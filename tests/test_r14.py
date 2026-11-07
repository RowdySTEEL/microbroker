import contextlib
from copy import deepcopy
import io
import json
import unittest
from unittest.mock import patch
from microbroker.cli import main
from microbroker.data.session_price_verification import price_fixture, empty_stats, apply_at, verify_session_prices
from microbroker.data.session_prices import create_session_prices, align_session_records, manifest
from microbroker.data.alignment import read_bars_snapshot, research_alignment
from microbroker.data.analysis_replay import replay_analysis
from microbroker.data.storage.repository import Repository, StorageError
from microbroker.data.session import session_fixture
from microbroker.data.session_coverage import session_coverage
from microbroker.data.stream_models import StreamRequest, normalize_event
from microbroker.data.research import encode, digest_records

class R14Tests(unittest.TestCase):
    def setUp(self):self.repo=Repository('sqlite:///:memory:')
    def tearDown(self):self.repo.close()
    def fixture(self):
        self.analysis,self.snap,self.run,self.stats,self.base=price_fixture(self.repo)
        self.bars,self.meta,_=read_bars_snapshot(self.repo,self.snap['replay_id'])
        self.selected,_,_=replay_analysis(self.repo,self.analysis,_include_records=True)
    def test_complete_private_verifier(self):self.assertEqual(len(verify_session_prices()['checks']),12)
    def test_verifier_cli_needs_no_environment(self):
        with patch('microbroker.cli.Settings.from_env',side_effect=AssertionError('No env')),contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['verify-session-prices']),0)
    def test_scope_ignores_shared_current_prices(self):
        self.fixture();self.repo.connection.execute('UPDATE bars SET close=999');self.repo.connection.commit()
        report=research_alignment(self.repo,self.analysis,self.snap['replay_id'])
        self.assertEqual(report['samples'][0]['price']['close'],'101')
        self.assertEqual(report['samples'][0]['price']['provenance']['run_id'],self.run)
    def test_duplicate_recorded_in_session_even_when_globally_known(self):
        self.fixture();self.assertEqual(self.stats['duplicates'],2)
        self.assertEqual(len(self.bars),3);self.assertEqual(self.stats['bar_receipts_recorded'],3)
    def test_unique_minutes_separate_from_correction_receipts(self):
        self.fixture();self.assertEqual(self.snap['unique_minute_bars'],1);self.assertEqual(self.snap['item_count'],3)
    def test_received_after_feature_is_excluded(self):
        self.fixture();self.selected[0]['analysis']['timestamp']='2026-01-07T10:00:59.999999999Z'
        row=align_session_records(self.selected,self.bars,['MSFT'],5)[0]
        self.assertEqual(row['status'],'no_session_bar_available_at_feature');self.assertIsNone(row['price'])
    def test_inclusive_receipt_time_and_future_correction_exclusion(self):
        self.fixture();row=align_session_records(self.selected,self.bars,['MSFT'],5)[0]
        self.assertEqual(row['price']['close'],'101');self.assertEqual(row['price']['provenance']['ordinal'],1)
    def test_later_update_not_downgraded_by_repeated_base(self):
        self.fixture();self.selected[0]['analysis']['timestamp']='2026-01-07T10:01:03.000000000Z'
        self.assertEqual(align_session_records(self.selected,self.bars,['MSFT'],5)[0]['price']['close'],'102')
    def test_unfinished_minute_excluded_even_if_received(self):
        self.fixture();bars=deepcopy(self.bars);selected=deepcopy(self.selected)
        selected[0]['analysis']['timestamp']='2026-01-07T10:00:30.000000000Z'
        for b in bars:b['receipt']['received_at']='2026-01-07T10:00:20.000000000Z'
        self.assertEqual(align_session_records(selected,bars,['MSFT'],5)[0]['status'],'no_session_bar_available_at_feature')
    def test_stale_session_price_not_carried_forward(self):
        self.fixture();self.selected[0]['analysis']['timestamp']='2026-01-07T10:06:00.000000001Z'
        self.assertEqual(align_session_records(self.selected,self.bars,['MSFT'],5)[0]['status'],'previous_bar_too_old')
    def test_exact_lag_cap_accepted(self):
        self.fixture();self.selected[0]['analysis']['timestamp']='2026-01-07T10:06:00.000000000Z'
        self.assertEqual(align_session_records(self.selected,self.bars,['MSFT'],5)[0]['status'],'matched')
    def test_missing_company_sentiment_remains_unscored(self):
        self.fixture();self.selected[0]['analysis']['result']['entity_sentiments']={}
        self.assertEqual(align_session_records(self.selected,self.bars,['MSFT'],5)[0]['status'],'company_sentiment_unavailable')
    def test_missing_analysis_stays_pending(self):
        self.fixture();self.selected[0]['analysis']=None
        self.assertEqual(align_session_records(self.selected,self.bars,['MSFT'],5)[0]['status'],'pending_analysis')
    def test_empty_run_cannot_fall_back_to_current_bars(self):
        self.fixture();request=StreamRequest(('MSFT',),channels=('bars','updatedBars'))
        run=self.repo.begin_stream(request,empty_stats())
        snapshot=create_session_prices(self.repo,run,('MSFT',),'iex','2026-01-07T10:00:00Z','2026-01-07T10:02:00Z')
        self.assertEqual(snapshot['status'],'empty');self.assertEqual(snapshot['unique_minute_bars'],0)
    def test_receipt_cutoff_excludes_later_update(self):
        self.fixture();snapshot=create_session_prices(self.repo,self.run,('MSFT',),'iex','2026-01-07T10:00:00Z','2026-01-07T10:01:00Z')
        bars,_,_=read_bars_snapshot(self.repo,snapshot['replay_id']);self.assertEqual(len(bars),1)
    def test_atomic_receipt_event_bar_audit_rollback(self):
        self.fixture();self.repo.capture_bar_receipts=True
        before=self.repo.connection.execute('SELECT COUNT(*) FROM stream_bar_receipts').fetchone()[0]
        old=self.repo.connection.execute('SELECT stats_json FROM stream_sessions WHERE id=?',(self.run,)).fetchone()[0];self.repo.connection.commit()
        update=normalize_event({**json.loads(self.base.payload_json),'T':'u','c':'103'},StreamRequest(('MSFT',),channels=('bars','updatedBars')))
        def broken():yield update;raise RuntimeError('fixture failure')
        with self.assertRaises(RuntimeError):self.repo.apply_stream_frame(self.run,broken(),self.stats)
        self.assertEqual(self.repo.connection.execute('SELECT COUNT(*) FROM stream_bar_receipts').fetchone()[0],before)
        self.assertEqual(self.repo.connection.execute('SELECT stats_json FROM stream_sessions WHERE id=?',(self.run,)).fetchone()[0],old)
        self.assertEqual(self.repo.connection.execute('SELECT close FROM bars WHERE symbol=? AND timestamp=?',('MSFT','2026-01-07T10:00:00+00:00')).fetchone()[0],'102')
    def test_manifest_metadata_tamper_rejected(self):
        self.fixture();meta={**self.meta,'stream_run_id':'foreign'}
        self.repo.connection.execute('UPDATE replay_runs SET request_json=? WHERE id=?',(encode(meta),self.snap['replay_id']));self.repo.connection.commit()
        with self.assertRaises(StorageError):read_bars_snapshot(self.repo,self.snap['replay_id'])
    def test_metadata_format_downgrade_rejected(self):
        self.fixture();meta={k:v for k,v in self.meta.items() if k!='format'}
        self.repo.connection.execute('UPDATE replay_runs SET request_json=? WHERE id=?',(encode(meta),self.snap['replay_id']));self.repo.connection.commit()
        with self.assertRaises(StorageError):read_bars_snapshot(self.repo,self.snap['replay_id'])
    def test_resealed_cross_run_record_still_rejected(self):
        self.fixture();bars=deepcopy(self.bars);bars[0]['receipt']['run_id']='foreign'
        sha=digest_records(bars);meta={**self.meta};meta['manifest_sha256']=manifest(meta,len(bars),sha)
        self.repo.connection.execute('UPDATE replay_runs SET request_json=?,dataset_sha256=? WHERE id=?',(encode(meta),sha,self.snap['replay_id']))
        self.repo.connection.execute('UPDATE replay_items SET record_json=? WHERE replay_id=? AND ordinal=0',(encode(bars[0]),self.snap['replay_id']));self.repo.connection.commit()
        with self.assertRaises(StorageError):read_bars_snapshot(self.repo,self.snap['replay_id'])
    def test_bounds_fail_without_creating_partial_snapshot(self):
        self.fixture();count=self.repo.connection.execute('SELECT COUNT(*) FROM replay_runs').fetchone()[0];self.repo.connection.commit()
        with patch('microbroker.data.session_prices.MAX_VERSIONS',1),self.assertRaises(StorageError):
            create_session_prices(self.repo,self.run,('MSFT',),'iex','2026-01-07T10:00:00Z','2026-01-07T10:02:00Z')
        self.assertEqual(self.repo.connection.execute('SELECT COUNT(*) FROM replay_runs').fetchone()[0],count)
    def test_capture_opt_in_only(self):
        request=StreamRequest(('MSFT',),channels=('bars','updatedBars'));run=self.repo.begin_stream(request,empty_stats())
        event=normalize_event({'T':'b','S':'MSFT','t':'2026-01-07T10:00:00Z','o':100,'h':102,'l':99,'c':101,'v':1,'n':1},request)
        self.repo.apply_stream_frame(run,[event],empty_stats())
        self.assertEqual(self.repo.connection.execute('SELECT COUNT(*) FROM stream_bar_receipts').fetchone()[0],0)
    def test_session_warmup_prices_precede_analysis(self):
        result,_,_=session_fixture();self.assertEqual(result['price_warmup']['status'],'ready')
        self.assertEqual(result['alignment']['price_scope'],'same_stream_run_receipts')
        provenance=result['alignment']['samples'][0]['price']['provenance']
        self.assertEqual(provenance['run_id'],result['stream']['run_id'])
    def test_empty_warmup_reports_timeout_without_false_price_claim(self):
        result,_,_=session_fixture(stream_empty=True)
        self.assertEqual(result['price_warmup']['status'],'timed_out')
        self.assertEqual(result['status'],'no_data')
        self.assertIn('price_warmup_timed_out',result['coverage']['reason_codes'])
    def test_duplicate_receipt_not_labeled_no_live_prices(self):
        report=session_coverage({'status':'completed','price_scope':'same_stream_run_receipts',
            'stream':{'bars_written':0,'bar_receipts_recorded':1},'snapshot_minute_bars':1})
        self.assertNotIn('no_stream_minute_bars',report['reason_codes'])
        self.assertIn('no_new_current_bar_writes',report['reason_codes'])

if __name__=='__main__':unittest.main()
