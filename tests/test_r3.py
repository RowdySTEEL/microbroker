from dataclasses import replace
import contextlib
import io
import json
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch
from microbroker.config import Settings
from microbroker.cli import main
from microbroker.data.models import DataValidationError,utc_datetime
from microbroker.data.request import BarsRequest
from microbroker.data.research import ResearchRequest,inspect_dataset,create_replay,replay,verify_research
from microbroker.data.storage.repository import Repository,StorageError
from microbroker.data.adapters.alpaca import normalize_bar
from microbroker.data.stream_models import StreamRequest,normalize_event
from test_r2 import BAR,TRADE,QUOTE,stats

class R3Tests(unittest.TestCase):
    def setUp(self):
        self.repo=Repository('sqlite:///:memory:')
        self.req=ResearchRequest(symbols=('AAPL',))
        self.bars_req=BarsRequest(('AAPL',),utc_datetime('2026-01-05T14:00:00Z'),utc_datetime('2026-01-05T15:00:00Z'),'1Min')
    def tearDown(self):self.repo.close()
    def bars(self,*rows):self.repo.collect([normalize_bar('AAPL',r,self.bars_req) for r in rows],self.bars_req)
    def events(self,*rows,feed='iex',symbol='AAPL'):
        req=StreamRequest((symbol,),feed=feed)
        run=self.repo.begin_stream(req,stats())
        self.repo.apply_stream_frame(run,[normalize_event(r,req) for r in rows],stats())
    def test_stable_sort_and_decimal_precision(self):
        self.bars({**BAR,'t':'2026-01-05T14:31:00Z'}, {**BAR,'o':'100.000000000000000001'})
        records,report=inspect_dataset(self.repo,self.req)
        self.assertEqual(report['status'],'passed')
        self.assertEqual(records[0]['open'],'100.000000000000000001')
        self.assertLess(records[0]['timestamp'],records[1]['timestamp'])
    def test_nanosecond_events_and_hash_validation(self):
        self.events(TRADE,{**TRADE,'i':2,'t':'2026-01-05T14:30:00.123456788Z'})
        records,report=inspect_dataset(self.repo,replace(self.req,source='events',order='event'))
        self.assertEqual(report['status'],'warning')
        self.assertEqual(report['issue_counts']['warning:out_of_order_event'],1)
        self.assertEqual(records[0]['timestamp'],'2026-01-05T14:30:00.123456788Z')
    def test_start_inclusive_end_exclusive(self):
        self.bars(BAR,{**BAR,'t':'2026-01-05T14:31:00Z'})
        r=replace(self.req,start=utc_datetime(BAR['t']),end=utc_datetime('2026-01-05T14:31:00Z'))
        records,_=inspect_dataset(self.repo,r)
        self.assertEqual(len(records),1)
    def test_test_feed_isolation(self):
        self.events(TRADE)
        self.events({**QUOTE,'S':'FAKEPACA'},feed='test',symbol='FAKEPACA')
        records,_=inspect_dataset(self.repo,replace(self.req,source='events'))
        self.assertEqual([r['symbol'] for r in records],['AAPL'])
        records,_=inspect_dataset(self.repo,ResearchRequest(source='events',feed='test'))
        self.assertEqual([r['symbol'] for r in records],['FAKEPACA'])
    def test_invalid_ohlc_blocks_snapshot(self):
        self.bars(BAR)
        self.repo.connection.execute("UPDATE bars SET low='200'");self.repo.connection.commit()
        _,report=inspect_dataset(self.repo,self.req)
        self.assertEqual(report['issue_counts']['error:invalid_bar'],1)
        with self.assertRaises(DataValidationError):create_replay(self.repo,self.req)
        self.assertEqual(self.repo.connection.execute('SELECT COUNT(*) FROM replay_runs').fetchone()[0],0)
    def test_invalid_timestamp_report(self):
        self.bars(BAR)
        self.repo.connection.execute("UPDATE bars SET timestamp='invalid'");self.repo.connection.commit()
        _,report=inspect_dataset(self.repo,self.req)
        self.assertEqual(report['issue_counts']['error:invalid_timestamp'],1)
    def test_nonfinite_price_report(self):
        self.bars(BAR)
        self.repo.connection.execute("UPDATE bars SET open='NaN'");self.repo.connection.commit()
        _,report=inspect_dataset(self.repo,self.req)
        self.assertEqual(report['status'],'failed')
    def test_crossed_quote_warning(self):
        self.events({**QUOTE,'bp':102,'ap':101})
        _,report=inspect_dataset(self.repo,replace(self.req,source='events'))
        self.assertEqual(report['issue_counts']['warning:crossed_quote'],1)
    def test_candidate_gap_no_synthetic_bars(self):
        self.bars(BAR,{**BAR,'t':'2026-01-05T14:33:00Z'})
        records,report=inspect_dataset(self.repo,self.req)
        self.assertEqual(len(records),2)
        self.assertEqual(report['issue_counts']['warning:candidate_minute_gap'],1)
        self.assertEqual(report['issues'][0]['absent_minutes'],2)
    def test_event_payload_hash_tampering_report(self):
        self.events(TRADE)
        self.repo.connection.execute("UPDATE stream_events SET event_key='invalid'");self.repo.connection.commit()
        _,report=inspect_dataset(self.repo,replace(self.req,source='events'))
        self.assertEqual(report['status'],'failed')
    def test_immutable_replay_survives_live_changes(self):
        self.bars(BAR)
        snap=create_replay(self.repo,self.req)
        self.bars({**BAR,'c':102})
        captured=[]
        result=replay(self.repo,snap['replay_id'],captured.append)
        self.assertEqual(captured[0]['close'],'101')
        self.assertEqual(result['dataset_sha256'],snap['dataset_sha256'])
        newer=create_replay(self.repo,self.req)
        self.assertNotEqual(newer['dataset_sha256'],snap['dataset_sha256'])
    def test_same_data_same_digest_and_repeat_replay(self):
        self.bars(BAR)
        a=create_replay(self.repo,self.req);b=create_replay(self.repo,self.req)
        self.assertNotEqual(a['replay_id'],b['replay_id'])
        self.assertEqual(a['dataset_sha256'],b['dataset_sha256'])
        self.assertEqual(replay(self.repo,a['replay_id']),replay(self.repo,a['replay_id']))
    def test_tamper_rejected_before_consumer(self):
        self.bars(BAR);snap=create_replay(self.repo,self.req)
        self.repo.connection.execute("UPDATE replay_items SET record_json='{}'");self.repo.connection.commit()
        called=[]
        with self.assertRaises(StorageError):replay(self.repo,snap['replay_id'],called.append)
        self.assertEqual(called,[])
    def test_missing_ordinal_rejected(self):
        self.bars(BAR);snap=create_replay(self.repo,self.req)
        self.repo.connection.execute('UPDATE replay_items SET ordinal=1');self.repo.connection.commit()
        with self.assertRaises(StorageError):replay(self.repo,snap['replay_id'])
    def test_snapshot_transaction_rollback(self):
        self.bars(BAR)
        self.repo.connection.execute("CREATE TRIGGER fail_item BEFORE INSERT ON replay_items BEGIN SELECT RAISE(ABORT,'test'); END");self.repo.connection.commit()
        with self.assertRaises(Exception):create_replay(self.repo,self.req)
        self.assertEqual(self.repo.connection.execute('SELECT COUNT(*) FROM replay_runs').fetchone()[0],0)
    def test_empty_and_missing_symbol(self):
        _,report=inspect_dataset(self.repo,self.req)
        self.assertEqual(report['missing_symbols'],['AAPL'])
        with self.assertRaises(DataValidationError):create_replay(self.repo,self.req)
    def test_backfilled_gap_still_warns_trade_quote_loss(self):
        self.bars(BAR);self.events(TRADE)
        req=StreamRequest(('AAPL',))
        gap=self.repo.begin_gap('test',req,utc_datetime(BAR['t']))
        self.repo.close_gap(gap,utc_datetime('2026-01-05T14:31:00Z'))
        self.repo.connection.execute("UPDATE stream_gaps SET status='backfilled'");self.repo.connection.commit()
        _,report=inspect_dataset(self.repo,replace(self.req,source='events'))
        self.assertEqual(report['issue_counts']['warning:unrecovered_trade_quote_interval'],1)
    def test_default_receipt_order_differs_from_event_order(self):
        self.events(TRADE,{**TRADE,'i':2,'t':'2026-01-05T14:29:00Z'})
        records,report=inspect_dataset(self.repo,replace(self.req,source='events'))
        self.assertEqual(records[0]['payload']['i'],1)
        self.assertEqual(report['replay_semantics'],'first_stored_receipt_order')
    def test_cli_research_without_keys(self):
        with tempfile.TemporaryDirectory() as folder:
            db='sqlite:///'+str(Path(folder)/'db')
            env=Path(folder)/'.env';env.write_text('DATABASE_URL='+db+'\n')
            capture=io.StringIO()
            with patch.dict('os.environ',{},clear=True),contextlib.redirect_stdout(capture):
                code=main(['--env-file',str(env),'data-quality','--symbols','AAPL'])
            self.assertEqual(code,3)
            self.assertEqual(json.loads(capture.getvalue())['status'],'warning')
    def test_verify_research_does_not_change_market_data(self):
        self.bars(BAR);self.events(TRADE)
        result=verify_research(self.repo,('AAPL',))
        self.assertEqual(result['status'],'passed')
        self.assertEqual(len(result['datasets']),2)
        self.assertEqual(self.repo.count(),1)
        self.assertEqual(self.repo.connection.execute('SELECT COUNT(*) FROM stream_events').fetchone()[0],1)
    def test_unknown_replay(self):
        with self.assertRaises(DataValidationError):replay(self.repo,'does-not-exist')
    def test_invalid_request(self):
        for kw in ({'source':'bad'},{'feed':'bad'},{'order':'bad'},{'timeframe':'2Min'}):
            with self.assertRaises(DataValidationError):ResearchRequest(**kw)
    def test_hard_row_bound(self):
        self.bars(BAR)
        with patch('microbroker.data.research.MAX_ROWS',0),self.assertRaises(StorageError):inspect_dataset(self.repo,self.req)
    def test_no_market_write_or_api_call_during_quality(self):
        self.bars(BAR);before=self.repo.connection.total_changes
        inspect_dataset(self.repo,self.req)
        self.assertEqual(self.repo.connection.total_changes,before)
    def test_range_filters_before_memory_bound(self):
        self.bars(BAR,{**BAR,'t':'2026-01-05T14:31:00Z'})
        r=replace(self.req,end=utc_datetime('2026-01-05T14:31:00Z'))
        with patch('microbroker.data.research.MAX_ROWS',1):
            records,_=inspect_dataset(self.repo,r)
        self.assertEqual(len(records),1)
    def test_sample_cap_still_counts_all_errors(self):
        self.events(*[{**TRADE,'i':i} for i in range(110)])
        self.repo.connection.execute("UPDATE stream_events SET event_key='bad-' || event_key");self.repo.connection.commit()
        _,report=inspect_dataset(self.repo,replace(self.req,source='events'))
        self.assertEqual(report['issue_counts']['error:invalid_event'],110)
        self.assertEqual(len(report['issues']),100)
    def test_daily_research_is_separate_from_minute_data(self):
        self.bars(BAR)
        req=replace(self.bars_req,timeframe='1Day')
        self.repo.collect([normalize_bar('AAPL',BAR,req)],req)
        records,_=inspect_dataset(self.repo,replace(self.req,timeframe='1Day'))
        self.assertEqual(len(records),1)
        self.assertEqual(records[0]['timeframe'],'1Day')
    def test_removed_item_rejected(self):
        self.bars(BAR);snap=create_replay(self.repo,self.req)
        self.repo.connection.execute('DELETE FROM replay_items');self.repo.connection.commit()
        with self.assertRaises(StorageError):replay(self.repo,snap['replay_id'])
    def test_decimal_equal_format_has_same_digest(self):
        self.bars(BAR);a=create_replay(self.repo,self.req)
        self.repo.connection.execute("UPDATE bars SET open='100.0000'");self.repo.connection.commit()
        b=create_replay(self.repo,self.req)
        self.assertEqual(a['dataset_sha256'],b['dataset_sha256'])
    def test_pending_transaction_is_not_discarded(self):
        self.bars(BAR)
        self.repo.connection.execute("UPDATE bars SET close='102'")
        with self.assertRaises(StorageError):inspect_dataset(self.repo,self.req)
        self.assertTrue(self.repo.connection.in_transaction)
        self.assertEqual(self.repo.connection.execute('SELECT close FROM bars').fetchone()[0],'102')
        self.repo.connection.rollback()
