"""Temporal analyses, frozen revision identity, tampering and transaction failures."""
import contextlib
import io
import json
import unittest
from unittest.mock import patch
from microbroker.cli import main
from microbroker.data.analysis_replay import create_analysis_replay, replay_analysis, verify_analysis_replay
from microbroker.data.news_replay import NewsReplayRequest, create_news_replay
from microbroker.data.research import encode
from microbroker.data.storage.repository import Repository, StorageError
from microbroker.data.models import DataValidationError
from microbroker.intelligence.analysis import compute_analysis
from microbroker.intelligence.policy import analyzer_metadata, digest
from microbroker.intelligence.sentiment import RulesEngine
import test_r9

class R10Tests(unittest.TestCase):
    add = test_r9.R9Tests.add
    def setUp(self):
        self.repo = Repository('sqlite:///:memory:')
        self.key = digest(analyzer_metadata(RulesEngine()))
        self.revision = self.add(headline='Microsoft reports strong growth.')
    def tearDown(self): self.repo.close()
    def analysis(self, revision=None, at='2026-01-07T10:00:00Z', key=None, engine='rules'):
        revision = revision or self.revision
        row = self.repo.connection.execute('SELECT data_json,first_seen_at FROM news_versions WHERE revision_hash=?',(revision,)).fetchone()
        mappings = dict(self.repo.connection.execute('SELECT symbol,method FROM news_symbols WHERE revision_hash=?',(revision,)).fetchall())
        result = compute_analysis({'data':json.loads(row[0]),'first_seen':row[1],'mappings':mappings},RulesEngine())
        self.repo.connection.execute('INSERT INTO news_analyses VALUES (?,?,?,?,?,?,?,?,?)',
            ('alpaca',1,revision,key or self.key,engine,result['input_sha256'],at,encode(result),digest(result)))
        self.repo.connection.commit()
    def freeze(self, cutoff='2026-01-10T00:00:00Z', key=None, engine='rules', news_cutoff='2026-01-08T00:00:00Z'):
        news = create_news_replay(self.repo,NewsReplayRequest(news_cutoff,('MSFT',)))
        return create_analysis_replay(self.repo,news['replay_id'],cutoff,key or self.key,engine)
    def play(self, snapshot, at=None):return replay_analysis(self.repo,snapshot['replay_id'],at)
    def test_analysis_waits_for_completion(self):
        self.analysis();s=self.freeze();p=self.play(s,'2026-01-06T12:00:00Z')
        self.assertEqual((p['available_articles'],p['pending_analyses'],p['available_analyses']),(1,1,0))
        self.assertIsNone(p['samples'][0]['sentiment'])
    def test_inclusive_nanosecond_completion(self):
        self.analysis(at='2026-01-07T10:00:00.000000001Z');s=self.freeze()
        self.assertEqual(self.play(s,'2026-01-07T10:00:00Z')['available_analyses'],0)
        self.assertEqual(self.play(s,'2026-01-07T10:00:00.000000001Z')['available_analyses'],1)
    def test_receipt_floor_even_if_analysis_clock_earlier(self):
        self.analysis(at='2026-01-05T11:00:00Z');s=self.freeze()
        self.assertEqual(self.play(s,'2026-01-06T09:59:59Z')['available_analyses'],0)
        self.assertEqual(self.play(s,'2026-01-06T10:00:00Z')['available_analyses'],1)
    def test_future_analysis_not_stored(self):
        self.analysis(at='2026-01-11T00:00:00Z');s=self.freeze()
        self.assertEqual(s['available_analyses'],0)
        raw=self.repo.connection.execute('SELECT record_json FROM replay_items WHERE replay_id=?',(s['replay_id'],)).fetchone()[0]
        self.assertIsNone(json.loads(raw)['analysis'])
    def test_exact_revision_no_old_analysis_fallback(self):
        self.analysis();self.add(headline='Microsoft corrected report.',updated='2026-01-07T12:00:00Z',received='2026-01-07T13:00:00Z')
        s=self.freeze();self.assertEqual(s['available_analyses'],0)
        self.assertEqual(self.play(s,'2026-01-07T11:00:00Z')['available_analyses'],1)
    def test_symbol_removed_does_not_reappear(self):
        self.analysis();self.add(headline='Apple corrected report.',updated='2026-01-07T12:00:00Z',received='2026-01-07T13:00:00Z',symbols=('AAPL',))
        s=self.freeze();self.assertEqual(s['available_articles'],0)
    def test_other_analyzer_not_used(self):
        self.analysis();s=self.freeze(key='0'*64);self.assertEqual(s['available_analyses'],0)
    def test_wrong_engine_rejected(self):
        self.analysis()
        with self.assertRaises(StorageError):self.freeze(engine='finbert')
    def test_source_input_corruption_rejected(self):
        self.analysis();self.repo.connection.execute("UPDATE news_analyses SET input_sha256='bad'");self.repo.connection.commit()
        with self.assertRaises(StorageError):self.freeze()
    def test_source_result_corruption_rejected(self):
        self.analysis();self.repo.connection.execute("UPDATE news_analyses SET result_json='{}'");self.repo.connection.commit()
        with self.assertRaises(StorageError):self.freeze()
    def test_source_change_and_deletion_cannot_change_frozen_state(self):
        self.analysis();s=self.freeze();before=self.play(s)
        self.repo.connection.execute('DELETE FROM news_analyses');self.repo.connection.execute('DELETE FROM news_versions');self.repo.connection.commit()
        self.assertEqual(self.play(s),before)
    def test_source_tables_preserved_and_no_inference(self):
        self.analysis();before=self.repo.connection.execute('SELECT * FROM news_analyses').fetchall()
        with patch('microbroker.intelligence.analysis.compute_analysis',side_effect=AssertionError('No inference')):
            s=self.freeze();self.play(s)
        self.assertEqual(self.repo.connection.execute('SELECT * FROM news_analyses').fetchall(),before)
        self.assertFalse(s['inference_performed']);self.assertFalse(s['source_tables_modified'])
    def test_snapshot_payload_corruption_rejected(self):
        self.analysis();s=self.freeze();self.repo.connection.execute("UPDATE replay_items SET record_json='{}' WHERE replay_id=?",(s['replay_id'],));self.repo.connection.commit()
        with self.assertRaises(StorageError):self.play(s)
    def test_manifest_corruption_rejected(self):
        s=self.freeze();self.repo.connection.execute("UPDATE replay_runs SET dataset_sha256='bad' WHERE id=?",(s['replay_id'],));self.repo.connection.commit()
        with self.assertRaises(StorageError):self.play(s)
    def test_future_cutoff_rejected(self):
        s=self.freeze()
        with self.assertRaises(DataValidationError):self.play(s,'2026-01-11T00:00:00Z')
    def test_cutoff_before_news_freeze_rejected(self):
        with self.assertRaises(DataValidationError):self.freeze(cutoff='2026-01-07T00:00:00Z')
    def test_invalid_key_rejected(self):
        with self.assertRaises(DataValidationError):self.freeze(key='latest')
    def test_wrong_snapshot_kind_rejected(self):
        s=create_news_replay(self.repo,NewsReplayRequest('2026-01-10T00:00:00Z'))
        with self.assertRaises(DataValidationError):self.play(s)
    def test_timezone_equivalence(self):
        self.analysis();s=self.freeze()
        self.assertEqual(self.play(s,'2026-01-07T13:00:00+03:00'),self.play(s,'2026-01-07T10:00:00Z'))
    def test_partial_insert_rolls_back(self):
        news=create_news_replay(self.repo,NewsReplayRequest('2026-01-08T00:00:00Z'))
        before=self.repo.connection.execute('SELECT COUNT(*) FROM replay_runs').fetchone()[0]
        self.repo.connection.execute("CREATE TRIGGER abort_analysis BEFORE INSERT ON replay_items BEGIN SELECT RAISE(ABORT,'fixture'); END");self.repo.connection.commit()
        with self.assertRaises(Exception):create_analysis_replay(self.repo,news['replay_id'],'2026-01-10T00:00:00Z',self.key,'rules')
        self.assertEqual(self.repo.connection.execute('SELECT COUNT(*) FROM replay_runs').fetchone()[0],before)
    def test_byte_bound_before_write(self):
        news=create_news_replay(self.repo,NewsReplayRequest('2026-01-08T00:00:00Z'))
        with patch('microbroker.data.analysis_replay.MAX_BYTES',1):
            with self.assertRaises(StorageError):create_analysis_replay(self.repo,news['replay_id'],'2026-01-10T00:00:00Z',self.key,'rules')
        self.assertEqual(self.repo.connection.execute('SELECT COUNT(*) FROM replay_runs').fetchone()[0],1)
    def test_analyzer_metadata_is_frozen_and_validated(self):
        self.analysis();meta=analyzer_metadata(RulesEngine())
        self.repo.connection.execute('INSERT INTO news_analysis_runs VALUES (?,?,?,?,?,?,?,?,?)',
            ('fixture','2026-01-07T10:00:00Z','2026-01-07T11:00:00Z','completed','alpaca','rules',self.key,encode({'analyzer':meta}),'{}'));self.repo.connection.commit()
        s=self.freeze();self.assertTrue(s['analyzer_metadata_available'])
        self.repo.connection.execute('DELETE FROM news_analysis_runs');self.repo.connection.commit()
        self.assertTrue(self.play(s)['analyzer_metadata_available'])
    def test_corrupt_analyzer_metadata_rejected(self):
        self.repo.connection.execute('INSERT INTO news_analysis_runs VALUES (?,?,?,?,?,?,?,?,?)',
            ('fixture','2026-01-07T10:00:00Z','2026-01-07T11:00:00Z','completed','alpaca','rules',self.key,encode({'analyzer':{}}),'{}'));self.repo.connection.commit()
        with self.assertRaises(StorageError):self.freeze()
    def test_verifier_and_cli_no_settings_or_model(self):
        self.assertEqual(len(verify_analysis_replay()['checks']),8)
        with contextlib.redirect_stdout(io.StringIO()),patch('microbroker.cli.Settings.from_env',side_effect=AssertionError('No env')):
            self.assertEqual(main(['verify-analysis-replay']),0)
    def test_pending_snapshot_does_not_pick_up_later_analysis(self):
        s=self.freeze();self.analysis();self.assertEqual(self.play(s)['available_analyses'],0)
        self.assertEqual(self.freeze()['available_analyses'],1)
    def test_frozen_timestamp_change_is_rejected(self):
        self.analysis();s=self.freeze()
        raw=self.repo.connection.execute('SELECT record_json FROM replay_items WHERE replay_id=?',(s['replay_id'],)).fetchone()[0]
        record=json.loads(raw);record['analysis']['analyzed_at']='2026-01-06T10:00:00.000000000Z'
        self.repo.connection.execute('UPDATE replay_items SET record_json=? WHERE replay_id=?',(encode(record),s['replay_id']));self.repo.connection.commit()
        with self.assertRaises(StorageError):self.play(s)
    def test_cli_create_and_play_without_model(self):
        from microbroker.config import Settings
        self.analysis();news=create_news_replay(self.repo,NewsReplayRequest('2026-01-08T00:00:00Z'))
        settings=Settings.from_env('/nonexistent')
        with patch('microbroker.cli.Settings.from_env',return_value=settings),patch('microbroker.cli.Repository',return_value=self.repo),patch.object(self.repo,'close'):
            out=io.StringIO()
            with contextlib.redirect_stdout(out):
                self.assertEqual(main(['create-analysis-replay','--news-replay-id',news['replay_id'],'--as-of','2026-01-10T00:00:00Z','--analyzer-key',self.key,'--engine','rules']),0)
            s=json.loads(out.getvalue());self.assertEqual(s['available_analyses'],1)
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(main(['replay-analysis','--replay-id',s['replay_id'],'--at','2026-01-06T12:00:00Z']),0)
    def test_malformed_snapshot_json_is_storage_error(self):
        s=self.freeze();self.repo.connection.execute("UPDATE replay_items SET record_json='[' WHERE replay_id=?",(s['replay_id'],));self.repo.connection.commit()
        with self.assertRaises(StorageError):self.play(s)
