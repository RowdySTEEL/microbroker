"""Causal news revision state, receipt cutoffs, atomic freeze and corruption gates."""
import contextlib
import io
import json
import unittest
from unittest.mock import patch
from microbroker.cli import main
from microbroker.config import Settings
from microbroker.data.news import normalize_news
from microbroker.data.news_replay import NewsReplayRequest,create_news_replay,replay_news,verify_news_replay
from microbroker.data.research import encode
from microbroker.data.storage.repository import Repository,StorageError
from microbroker.data.models import DataValidationError

class R9Tests(unittest.TestCase):
    def setUp(self):self.repo=Repository('sqlite:///:memory:')
    def tearDown(self):self.repo.close()
    def add(self, *, headline='Microsoft original report', updated='2026-01-05T10:00:00Z', received='2026-01-06T10:00:00Z', symbols=('MSFT',), id=1, content='',provider='alpaca'):
        n=normalize_news({'id':id,'headline':headline,'summary':'','created_at':'2026-01-05T10:00:00Z','updated_at':updated,'symbols':list(symbols),'content':content,'source':'synthetic','url':'https://example.com/test'},provider=provider)
        d=n['data'];self.repo.connection.execute('INSERT INTO news_versions VALUES (?,?,?,?,?,?,?)',(provider,id,n['revision_hash'],d['created_at'],d['updated_at'],received,encode(d)))
        for s,m in n['mappings'].items():self.repo.connection.execute('INSERT INTO news_symbols VALUES (?,?,?,?,?)',(provider,id,n['revision_hash'],s,m))
        self.repo.connection.commit();return n['revision_hash']
    def freeze(self, asof='2026-01-10T00:00:00Z', symbols=('MSFT',)):
        return create_news_replay(self.repo,NewsReplayRequest(asof,symbols))
    def play(self,snapshot,at=None):return replay_news(self.repo,snapshot['replay_id'],at)
    def test_empty_before_receipt_is_valid_not_backdated(self):
        self.add();s=self.freeze('2026-01-05T23:00:00Z');self.assertEqual(s['item_count'],0);self.assertEqual(self.play(s)['available_articles'],0)
        self.assertIn('empty_asof_state',s['quality']['warning_codes'])
    def test_old_revision_selected_before_later_receipt(self):
        old=self.add();self.add(headline='Microsoft correction',updated='2026-01-07T10:00:00Z',received='2026-01-07T11:00:00Z')
        s=self.freeze();p=self.play(s,'2026-01-06T12:00:00Z');self.assertEqual(p['samples'][0]['revision_hash'],old)
        self.assertEqual(self.play(s)['samples'][0]['headline'],'Microsoft correction')
    def test_future_provider_timestamp_not_visible_early(self):
        self.add(updated='2026-01-07T10:00:00Z');s=self.freeze()
        self.assertEqual(self.play(s,'2026-01-06T12:00:00Z')['available_articles'],0)
    def test_stale_arrival_cannot_downgrade(self):
        newest=self.add(updated='2026-01-07T10:00:00Z',received='2026-01-07T11:00:00Z')
        self.add(headline='Old late arrival',received='2026-01-08T11:00:00Z')
        self.assertEqual(self.play(self.freeze())['samples'][0]['revision_hash'],newest)
    def test_equal_updated_body_preserved(self):
        full=self.add(content='<p>Microsoft report body.</p>');self.add(headline='Contentless later metadata',received='2026-01-06T11:00:00Z')
        self.assertEqual(self.play(self.freeze())['samples'][0]['revision_hash'],full)
    def test_equal_updated_same_body_rank_later_receipt_wins(self):
        self.add();latest=self.add(headline='Metadata correction',received='2026-01-06T11:00:00Z')
        self.assertEqual(self.play(self.freeze())['samples'][0]['revision_hash'],latest)
    def test_ambiguous_identical_receipts_rejected_before_write(self):
        self.add();self.add(headline='Different same-time content')
        with self.assertRaises(StorageError):self.freeze()
        self.assertEqual(self.repo.connection.execute('SELECT COUNT(*) FROM replay_runs').fetchone()[0],0)
    def test_symbol_removed_revision_never_falls_back_to_old_tag(self):
        self.add();self.add(headline='Apple correction',symbols=('AAPL',),updated='2026-01-07T10:00:00Z',received='2026-01-07T11:00:00Z')
        s=self.freeze();self.assertEqual(s['item_count'],2);self.assertEqual(self.play(s)['available_articles'],0)
        self.assertEqual(self.play(s,'2026-01-06T12:00:00Z')['available_articles'],1)
    def test_symbol_added_later_not_visible_in_past(self):
        self.add(symbols=('AAPL',));self.add(headline='Microsoft added',updated='2026-01-07T10:00:00Z',received='2026-01-07T11:00:00Z')
        s=self.freeze();self.assertEqual(self.play(s,'2026-01-06T12:00:00Z')['available_articles'],0)
        self.assertEqual(self.play(s)['available_articles'],1)
    def test_snapshot_remains_identical_after_source_changes(self):
        self.add();s=self.freeze();before=self.play(s)
        self.add(headline='Future correction',updated='2026-01-11T10:00:00Z',received='2026-01-11T11:00:00Z')
        self.repo.connection.execute('DELETE FROM news_versions');self.repo.connection.commit()
        self.assertEqual(before,self.play(s))
    def test_nanosecond_receipt_cutoff(self):
        self.add(received='2026-01-06T10:00:00.000000002Z');s=self.freeze()
        self.assertEqual(self.play(s,'2026-01-06T10:00:00.000000001Z')['available_articles'],0)
        self.assertEqual(self.play(s,'2026-01-06T10:00:00.000000002Z')['available_articles'],1)
    def test_offset_cutoff_equivalence(self):
        self.add();self.assertEqual(self.play(self.freeze('2026-01-06T13:00:00+03:00'))['available_articles'],1)
    def test_after_cutoff_rejected(self):
        self.add();s=self.freeze()
        with self.assertRaises(DataValidationError):self.play(s,'2026-01-11T00:00:00Z')
    def test_provider_isolation(self):
        self.add(provider='alpaca-sandbox');self.assertEqual(self.freeze()['item_count'],0)
    def test_source_mapping_corruption_rejected(self):
        self.add();self.repo.connection.execute("UPDATE news_symbols SET method='forged'");self.repo.connection.commit()
        with self.assertRaises(StorageError):self.freeze()
    def test_source_hash_corruption_rejected(self):
        self.add();self.repo.connection.execute("UPDATE news_versions SET revision_hash='forged'");self.repo.connection.commit()
        with self.assertRaises(StorageError):self.freeze()
    def test_snapshot_payload_corruption_rejected(self):
        self.add();s=self.freeze();self.repo.connection.execute("UPDATE replay_items SET record_json='{}'");self.repo.connection.commit()
        with self.assertRaises(StorageError):self.play(s)
    def test_manifest_cutoff_corruption_rejected(self):
        self.add();s=self.freeze();row=self.repo.connection.execute('SELECT request_json FROM replay_runs').fetchone();m=json.loads(row[0]);m['as_of']='2027-01-01T00:00:00.000000000Z'
        self.repo.connection.execute('UPDATE replay_runs SET request_json=?',(encode(m),));self.repo.connection.commit()
        with self.assertRaises(StorageError):self.play(s)
    def test_snapshot_ordinal_gap_rejected(self):
        self.add();s=self.freeze();self.repo.connection.execute('UPDATE replay_items SET ordinal=9');self.repo.connection.commit()
        with self.assertRaises(StorageError):self.play(s)
    def test_version_limit_rollback(self):
        self.add();self.add(id=2)
        with patch('microbroker.data.news_replay.MAX_VERSIONS',1),self.assertRaises(StorageError):self.freeze()
        self.assertFalse(self.repo.connection.in_transaction)
    def test_input_byte_limit_rejected(self):
        self.add()
        with patch('microbroker.data.news_replay.MAX_BYTES',1),self.assertRaises(StorageError):self.freeze()
    def test_atomic_snapshot_insert_failure(self):
        self.add();self.repo.connection.execute("CREATE TRIGGER fail_item BEFORE INSERT ON replay_items BEGIN SELECT RAISE(ABORT,'injected'); END;");self.repo.connection.commit()
        with self.assertRaises(Exception):self.freeze()
        self.assertEqual(self.repo.connection.execute('SELECT COUNT(*) FROM replay_runs').fetchone()[0],0)
    def test_source_tables_and_analyses_untouched(self):
        self.add();before=self.repo.connection.execute('SELECT * FROM news_versions').fetchall();s=self.freeze();self.play(s)
        self.assertEqual(before,self.repo.connection.execute('SELECT * FROM news_versions').fetchall())
        self.assertFalse(self.play(s)['analyses_included']);self.assertFalse(self.play(s)['exact_live_point_in_time_validated'])
    def test_private_cli_verifier_without_env(self):
        with patch('microbroker.cli.Settings.from_env',side_effect=AssertionError('must not read env')),contextlib.redirect_stdout(io.StringIO()) as out:
            self.assertEqual(main(['verify-news-replay']),0)
        self.assertEqual(len(json.loads(out.getvalue())['checks']),9)
    def test_wrong_snapshot_kind_rejected(self):
        self.repo.connection.execute('INSERT INTO replay_runs VALUES (?,?,?,?,?,?)',('other','now',encode({'source':'bars'}),0,'fake','{}'));self.repo.connection.commit()
        with self.assertRaises(DataValidationError):replay_news(self.repo,'other')
    def test_invalid_cutoff_and_unknown_symbol(self):
        for time in ['2026-01-06T10:00:00','bad']:
            with self.assertRaises(DataValidationError):NewsReplayRequest(time)
        with self.assertRaises(DataValidationError):NewsReplayRequest('2026-01-06T10:00:00Z',('UNKNOWN',))
    def test_create_and_replay_cli_without_model_or_api(self):
        self.add()
        with patch('microbroker.cli.Settings.from_env',return_value=Settings(database_url='sqlite:///:memory:')),patch('microbroker.cli.Repository',return_value=self.repo),patch.object(self.repo,'close'):
            with contextlib.redirect_stdout(io.StringIO()) as out:
                self.assertEqual(main(['create-news-replay','--symbols','MSFT','--as-of','2026-01-10T00:00:00Z']),0)
            frozen=json.loads(out.getvalue())
            with contextlib.redirect_stdout(io.StringIO()) as out:
                self.assertEqual(main(['replay-news','--replay-id',frozen['replay_id'],'--at','2026-01-05T23:00:00Z']),0)
            self.assertEqual(json.loads(out.getvalue())['available_articles'],0)
    def test_invalid_stored_json_is_rejected(self):
        self.add();self.repo.connection.execute("UPDATE news_versions SET data_json='{invalid'");self.repo.connection.commit()
        with self.assertRaises(StorageError):self.freeze()
