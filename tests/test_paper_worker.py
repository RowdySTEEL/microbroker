"""Worker fencing, durable news claims, lifecycle and sampled equity semantics."""
import contextlib
from datetime import datetime,timezone,timedelta
import io
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
from microbroker.cli import main
from microbroker.config import Settings
from microbroker.data.session import SessionRequest,run_session,safe_error
from microbroker.data.storage.repository import Repository,StorageError
from microbroker.data.models import DataValidationError
from microbroker.data.news import normalize_news
from microbroker.data.stream_models import normalize_event,canonical_time
from microbroker.execution.paper_api import PaperBrokerError
from microbroker.execution.paper_worker import (PaperWorker,run_paper_session,utc_add,read_decision,
    current_records,append_decision,all_decisions)
from microbroker.execution.paper_worker_verification import WorkerFixture,verify_paper_session,demo_paper_session
from microbroker.execution.paper_broker_verification import FakePaper
from microbroker.execution import paper_broker as broker
from microbroker.execution.equity_history import capture_equity,performance_report
from microbroker.execution.strategy_verification import FixtureEngine
from microbroker.intelligence.analysis import AnalysisRequest,select_news

class WorkerTests(unittest.TestCase):
    def setUp(self):self.repo=Repository('sqlite:///:memory:');self.f=WorkerFixture(self.repo,'unittest',0)
    def tearDown(self):self.repo.close()
    def test_private_verification(self):
        r=verify_paper_session();self.assertEqual(len(r['checks']),30);self.assertEqual(r['orders_sent_to_alpaca'],0)
    def test_wait_then_buy_on_following_minute(self):
        f=self.f;f.news();f.bar(0);f.fake.at='2026-01-05T15:01:10.000000000Z';f.tick()
        self.assertEqual(f.fake.posts,0);self.assertEqual(f.decisions()[0][2]['status'],'pending')
        f.bar(1);f.fake.at='2026-01-05T15:02:10.000000000Z';f.tick();self.assertEqual(f.fake.posts,1)
        self.assertEqual(f.worker.summary()['post_attempts'],1)
    def test_session_buy_cap_applies_after_closed_roundtrip(self):
        f=self.f;f.worker.max_buys=1;f.news();f.ready();f.tick();f.fill();f.tick()
        f.news(1,'Microsoft shares fell.',at='2026-01-05T15:02:20Z');f.fake.at='2026-01-05T15:03:00.000000000Z';f.worker.lease()
        f.bar(3);f.fake.at='2026-01-05T15:04:10.000000000Z';f.tick();f.fill();f.tick()
        f.news(2,at='2026-01-05T15:04:20Z');f.fake.at='2026-01-05T15:05:00.000000000Z';f.worker.lease()
        f.bar(5);f.fake.at='2026-01-05T15:06:01.000000000Z';f.tick()
        self.assertEqual(f.fake.posts,2)
        self.assertEqual(next(r[2]['reason'] for r in f.decisions() if r[1]['record']['news']['article_id']==f.base+2),'session_buy_limit')
    def test_finishing_does_not_liquidate_open_position(self):
        f=self.f;f.news();f.ready();f.tick();f.fill();f.tick();f.worker.finish()
        self.assertEqual(f.fake.posts,1);self.assertEqual(f.fake.client.positions()['MSFT']['qty'],'0.1')
    def test_next_session_can_exit_previously_managed_entry(self):
        f=self.f;f.news();f.ready();f.tick();f.fill();f.tick();f.worker.finish()
        f.fake.at='2026-01-05T15:03:00.000000000Z'
        f.worker=PaperWorker(self.repo,f.fake.client,('MSFT',),'iex',f.key,'rules',_fixture=True);f.worker.lease(acquire=True)
        self.repo.capture_bar_receipts=True;f.worker.stream_run_id=self.repo.begin_stream(f.request,{k:v.copy() if isinstance(v,dict) else 0 for k,v in f.stats.items()});self.repo.capture_bar_receipts=False
        f.stats={'events_received':0,'events_inserted':0,'duplicates':0,'bars_written':0,'received_by_type':{},'received_by_symbol':{}}
        f.news(1,'Microsoft shares fell.',at='2026-01-05T15:03:01Z');f.fake.at='2026-01-05T15:04:00.000000000Z';f.worker.lease()
        f.bar(4);f.fake.at='2026-01-05T15:05:01.000000000Z';f.tick()
        self.assertEqual(f.fake.posts,2);self.assertEqual(list(f.fake.orders.values())[-1]['side'],'sell')
    def test_partial_buy_does_not_trigger_unreserved_exit(self):
        f=self.f;f.news();f.ready();f.tick();cid=next(iter(f.fake.orders));f.fake.update(cid,'partially_filled','0.04')
        f.news(1,'Microsoft shares fell.',at='2026-01-05T15:02:15Z');f.fake.at='2026-01-05T15:03:00.000000000Z';f.worker.lease()
        f.bar(3);f.fake.at='2026-01-05T15:04:01.000000000Z';f.tick()
        self.assertEqual(f.fake.posts,1);self.assertEqual(next(r[2]['reason'] for r in f.decisions() if r[1]['record']['news']['article_id']==f.base+1),'no_managed_worker_position')
    def test_simultaneous_conflict_across_polls(self):
        f=self.f;f.news();f.bar(0);f.fake.at='2026-01-05T15:01:10.000000000Z';f.tick()
        f.news(1,'Microsoft shares fell.');f.bar(1);f.fake.at='2026-01-05T15:02:10.000000000Z';f.tick()
        self.assertEqual(f.fake.posts,0);self.assertTrue(all(r[2]['reason']=='conflicting_simultaneous_signals' for r in f.decisions()))
    def test_signal_before_gap_recovery_is_withheld(self):
        f=self.f;f.news();f.fake.at='2026-01-05T15:01:00.000000000Z';f.worker.lease()
        f.fake.at='2026-01-05T15:02:00.000000000Z';f.worker.lease();f.fake.at='2026-01-05T15:03:00.000000000Z';f.worker.lease()
        f.bar(3);f.fake.at='2026-01-05T15:04:01.000000000Z';f.tick()
        self.assertEqual(f.fake.posts,0);self.assertEqual(f.decisions()[0][2]['reason'],'signal_before_price_recovery')
    def test_recovered_price_needs_newer_feature_and_following_bar(self):
        f=self.f
        for minute in (1,2,3):f.fake.at=f'2026-01-05T15:0{minute}:00.000000000Z';f.worker.lease()
        f.bar(3);f.news(at='2026-01-05T15:04:01Z');f.fake.at='2026-01-05T15:04:10.000000000Z';f.tick();self.assertEqual(f.fake.posts,0)
        f.bar(5);f.fake.at='2026-01-05T15:05:10.000000000Z';f.worker.lease();f.fake.at='2026-01-05T15:06:01.000000000Z';f.tick()
        self.assertEqual(f.fake.posts,1)
    def test_expired_feature_does_not_buy(self):
        f=self.f;f.news()
        for minute in range(1,7):f.fake.at=f'2026-01-05T15:0{minute}:10.000000000Z';f.worker.lease()
        f.bar(5);f.tick();self.assertEqual(f.fake.posts,0);self.assertEqual(f.decisions()[0][2]['reason'],'signal_expired_or_future')
    def test_session_deadline_never_claims_order(self):
        f=self.f;f.news();f.ready();f.worker.deadline=f.fake.at
        self.assertEqual(f.tick()['status'],'deadline');self.assertEqual(f.fake.posts,0)
    def test_pre_session_news_not_selected(self):
        f=self.f;f.news(at='2026-01-05T14:59:59Z');f.ready();f.tick();self.assertEqual(f.decisions(),[])
    def test_coarse_sql_filter_preserves_nanosecond_cutoff(self):
        f=self.f;f.news(first='2026-01-05T15:00:01.000000001Z');f.news(1,first='2026-01-05T15:00:01.000000002Z')
        rows=select_news(self.repo,AnalysisRequest(symbols=('MSFT',),first_seen_since='2026-01-05T15:00:01.000000002Z'))
        self.assertEqual([r['id'] for r in rows],[f.base+1])
    def test_no_new_analysis_uses_empty_recent_scan(self):
        f=self.f;f.ready();f.tick();self.assertEqual(f.worker.summary()['decisions'],0);self.assertEqual(f.fake.posts,0)
    def test_unmanaged_positions_fail_before_execution(self):
        f=self.f;f.news();f.ready();f.fake.extra_positions=[{'symbol':'MSFT','qty':'1','side':'long'}]
        with self.assertRaisesRegex(PaperBrokerError,'reconciliation'):f.tick()
        self.assertEqual(f.fake.posts,0)
    def test_account_changed_between_cycles_fail_closed(self):
        f=self.f;f.ready();f.fake.identity='00000000-0000-0000-0000-000000000002'
        with self.assertRaisesRegex(PaperBrokerError,'identity_changed'):f.tick()
        self.assertEqual(f.fake.posts,0)
    def test_lease_takeover_during_preflight_prevents_post(self):
        f=self.f;f.news();f.ready();original=f.fake.client.asset
        rival=PaperWorker(self.repo,f.fake.client,('MSFT',),'iex',f.key,'rules',_fixture=True)
        def asset(symbol):
            result=original(symbol);f.fake.at=utc_add(f.fake.at,121);rival.lease(acquire=True);return result
        with patch.object(f.fake.client,'asset',side_effect=asset),self.assertRaisesRegex(PaperBrokerError,'lease_lost'):f.tick()
        self.assertEqual(f.fake.posts,0);rival.finish()
    def test_deadline_crossed_during_preflight_prevents_post(self):
        f=self.f;f.news();f.ready();f.worker.deadline=utc_add(f.fake.at,2);original=f.fake.client.asset
        def asset(symbol):
            result=original(symbol);f.fake.at=utc_add(f.fake.at,3);return result
        with patch.object(f.fake.client,'asset',side_effect=asset):f.tick()
        self.assertEqual(f.fake.posts,0);self.assertEqual(f.decisions()[0][2]['reason'],'session_deadline')
    def test_crashed_prepared_intent_is_retired_on_takeover(self):
        f=self.f;f.news();f.ready();record=current_records(self.repo,('MSFT',),f.worker.started,f.key,'rules')[0]
        key,_=f.worker.create(record,'MSFT');row=f.decisions()[0];iid=row[5]
        price=f.worker.price_health(f.worker.read_prices(self.repo))['by_symbol']['MSFT'][-1];self.repo.connection.commit()
        broker.prepare_buy(self.repo,f.fake.client,iid,'MSFT')
        f.worker.transaction(lambda:append_decision(self.repo,key,{'kind':'prepared','price':price,'parent_intent_id':None},f.fake.at))
        f.fake.at=utc_add(f.fake.at,121);other=PaperWorker(self.repo,f.fake.client,('MSFT',),'iex',f.key,'rules',_fixture=True);other.lease(acquire=True)
        self.assertEqual(broker.report(self.repo,iid)['phase'],'retired');broker.submit(self.repo,f.fake.client,iid)
        self.assertEqual(f.fake.posts,0);other.finish()
    def test_retired_prepared_exit_releases_reservation(self):
        f=self.f;f.worker.finish();p=broker.prepare_buy(self.repo,f.fake.client,'r23-old-managed','MSFT');broker.submit(self.repo,f.fake.client,'r23-old-managed')
        f.fake.update(p['request']['client_order_id'],'filled','0.1');broker.reconcile(self.repo,f.fake.client,'r23-old-managed')
        broker.prepare_exit(self.repo,f.fake.client,'retired-exit','r23-old-managed')
        broker._record(self.repo,f.fake.client,{'id':'retired-exit','account_ref':f.worker.account_ref},'retired_unsent',{'reason':'worker_signal_retired'})
        p=broker.prepare_exit(self.repo,f.fake.client,'new-exit','r23-old-managed');self.assertEqual(p['request']['qty'],'0.1')
    def test_terminal_news_claim_tamper_rejected(self):
        f=self.f;f.news();f.ready();f.tick();key=f.decisions()[0][0]
        self.repo.connection.execute('UPDATE paper_news_events SET event_json=? WHERE decision_id=?',('{}',key));self.repo.connection.commit()
        with self.assertRaises(StorageError):f.worker.summary()
    def test_prepared_price_receipt_change_is_rejected(self):
        f=self.f;f.news();f.ready();original=f.fake.client.asset
        def asset(symbol):
            r=original(symbol);self.repo.connection.execute('UPDATE stream_bar_receipts SET payload_json=? WHERE run_id=? AND ordinal=2',('{}',f.worker.stream_run_id));self.repo.connection.commit();return r
        with patch.object(f.fake.client,'asset',side_effect=asset),self.assertRaises((StorageError,DataValidationError)):f.tick()
        self.assertEqual(f.fake.posts,0)
    def test_prior_clock_error_cannot_restore_old_positive(self):
        f=self.f;f.news();f.bar(0,receipt='2026-01-05T15:00:59.9Z');f.ready();f.tick()
        self.assertEqual(f.fake.posts,0);self.assertEqual(f.decisions()[0][2]['reason'],'signal_before_price_recovery')
    def test_later_equity_samples_not_backfilled_into_earlier_report(self):
        f=self.f;capture_equity(self.repo,f.fake.client);f.fake.at=utc_add(f.fake.at,86400);f.fake.equity='12000';capture_equity(self.repo,f.fake.client)
        r=performance_report(self.repo,f.worker.account_ref,end='2026-01-05T15:00:00Z');self.assertEqual(r['samples'],1);self.assertIsNone(r['equity_change'])
    def test_equity_tamper_blocks_report(self):
        f=self.f;capture_equity(self.repo,f.fake.client);self.repo.connection.execute("UPDATE paper_equity_samples SET snapshot_json='{}'");self.repo.connection.commit()
        with self.assertRaises(StorageError):performance_report(self.repo,f.worker.account_ref)
    def test_clock_regression_does_not_append_equity(self):
        f=self.f;capture_equity(self.repo,f.fake.client);f.fake.at=utc_add(f.fake.at,-1)
        with self.assertRaises(StorageError):capture_equity(self.repo,f.fake.client)
        self.assertEqual(self.repo.connection.execute('SELECT COUNT(*) FROM paper_equity_samples').fetchone()[0],1)
    def test_period_shift_preserves_nanoseconds(self):
        f=self.f;f.fake.at='2026-01-05T15:00:00.123456789Z';capture_equity(self.repo,f.fake.client)
        r=performance_report(self.repo,f.worker.account_ref,period='day');self.assertEqual(r['requested_start'],'2026-01-04T15:00:00.123456789Z')
        self.assertEqual(utc_add(f.fake.at,120),'2026-01-05T15:02:00.123456789Z')
    def test_empty_account_history_not_zero_profit(self):
        r=performance_report(self.repo);self.assertEqual(r['status'],'insufficient_observations');self.assertIsNone(r['equity_change']);self.assertIsNone(r['net_profit'])
    def test_equity_capture_once_per_five_minutes_in_worker(self):
        f=self.f
        for minute in range(6):
            f.fake.at=f'2026-01-05T15:0{minute}:00.000000000Z';f.tick()
        self.assertEqual(self.repo.connection.execute('SELECT COUNT(*) FROM paper_equity_samples').fetchone()[0],2)
    def test_unverified_deposit_is_not_net_profit(self):
        f=self.f;capture_equity(self.repo,f.fake.client);f.fake.advance();f.fake.equity='20000';capture_equity(self.repo,f.fake.client)
        r=performance_report(self.repo,f.worker.account_ref);self.assertEqual(r['equity_change'],'10000.00000000');self.assertIsNone(r['net_profit']);self.assertFalse(r['cashflows_verified'])
    def test_lease_survives_other_owners_cleanup(self):
        f=self.f;f.fake.at=utc_add(f.fake.at,121);other=PaperWorker(self.repo,f.fake.client,('MSFT',),'iex',f.key,'rules',_fixture=True);other.lease(acquire=True)
        with self.assertRaises(PaperBrokerError):f.worker.finish()
        other.lease();other.finish()
    def test_equity_insert_failure_rolls_back(self):
        f=self.f;base=self.repo.connection
        class Fail:
            def __getattr__(self,name):return getattr(base,name)
            def execute(self,q,p=()):
                result=base.execute(q,p)
                if q.startswith('INSERT INTO paper_equity_samples'):raise StorageError('controlled rollback')
                return result
        self.repo.connection=Fail()
        with self.assertRaises(StorageError):capture_equity(self.repo,f.fake.client)
        self.repo.connection=base;self.assertEqual(base.execute('SELECT COUNT(*) FROM paper_equity_samples').fetchone()[0],0)
    def test_worker_rejects_unverified_engine_and_bounds(self):
        for kw in ({'duration':0},{'duration':True},{'max_buys':11},{'notional':'101'},{'notional':'0.5'}):
            with self.assertRaises(DataValidationError):PaperWorker(self.repo,self.f.fake.client,('MSFT',),'iex',self.f.key,'rules',_fixture=True,**kw)
        with self.assertRaises(DataValidationError):PaperWorker(self.repo,self.f.fake.client,('MSFT',),'iex',self.f.key,'rules')
    def test_readonly_performance_cli_does_not_construct_api(self):
        with tempfile.TemporaryDirectory() as d:
            settings=Settings(database_url='sqlite:///'+str(Path(d)/'db'))
            with patch('microbroker.cli.Settings.from_env',return_value=settings),patch('microbroker.cli.PaperAPI',side_effect=AssertionError('No API')),contextlib.redirect_stdout(io.StringIO()) as out:
                self.assertEqual(main(['paper-performance','--period','month']),0)
            self.assertEqual(json.loads(out.getvalue())['status'],'insufficient_observations')
    def test_private_verifier_cli_ignores_env(self):
        with patch('microbroker.cli.Settings.from_env',side_effect=AssertionError('No credentials')),contextlib.redirect_stdout(io.StringIO()):self.assertEqual(main(['verify-paper-session']),0)
    def test_cli_sqlite_rejected_before_model_or_api(self):
        with patch('microbroker.cli.Settings.from_env',return_value=Settings(api_key='FAKE',api_secret='FAKE')),patch('microbroker.cli.make_engine',side_effect=AssertionError('No model')),contextlib.redirect_stderr(io.StringIO()):self.assertEqual(main(['run-paper-session']),2)
    def test_demo_never_uses_user_database(self):
        with tempfile.TemporaryDirectory() as d:
            result=demo_paper_session(Path(d)/'demo');self.assertEqual(result['orders_sent_to_alpaca'],0)
            self.assertTrue((Path(d)/'demo/scenario.sqlite3').is_file())
            with self.assertRaises(DataValidationError):demo_paper_session(Path(d)/'demo')
    def test_safe_paper_failure_category_without_raw_message(self):
        r=safe_error(PaperBrokerError('paper_worker_lease_lost'),'paper_cycle');self.assertEqual(r['category'],'paper_worker_lease_lost')
        self.assertNotIn('category',safe_error(PaperBrokerError('FAKE_SECRET=secret'),'paper_cycle'))

class WrapperTests(unittest.TestCase):

    def test_pg_style_implicit_read_transactions_and_locks(self):
        # SQLite transport shim: enforce psycopg implicit SELECT transactions and PG clean-entry guards.
        from microbroker.execution.paper_worker_verification import worker_checks
        from types import SimpleNamespace
        repo=Repository('sqlite:///:memory:');base=repo.connection
        class PgShim:
            @property
            def info(self):return SimpleNamespace(transaction_status=2 if base.in_transaction else 0)
            def __getattr__(self,name):return getattr(base,name)
            def execute(self,q,p=()):
                if q.startswith('SET TRANSACTION'):return base.execute('BEGIN')
                if q.startswith('SELECT') and not base.in_transaction:base.execute('BEGIN')
                return base.execute(q.replace('%s','?').replace(' FOR UPDATE',''),p)
        repo.connection=PgShim();repo.postgres=True;repo.marker='%s'
        try:self.assertEqual(len(worker_checks(repo)),30)
        finally:repo.close()

    def test_aborted_session_releases_lease_and_preserves_equity(self):
        with tempfile.TemporaryDirectory() as d:
            url='sqlite:///'+str(Path(d)/'db');settings=Settings(database_url=url,api_key='FAKE',api_secret='FAKE');f=FakePaper('wrapper-failure')
            def failed(*args,**kwargs):raise RuntimeError('controlled session failure')
            with self.assertRaises(RuntimeError):run_paper_session(settings,SessionRequest(('MSFT',)),FixtureEngine(),api_factory=lambda _:f.client,session_runner=failed,_fixture=True)
            repo=Repository(url)
            try:
                self.assertEqual(repo.connection.execute('SELECT COUNT(*) FROM paper_worker_leases').fetchone()[0],0)
                self.assertEqual(repo.connection.execute('SELECT COUNT(*) FROM paper_equity_samples').fetchone()[0],2)
                self.assertEqual(f.posts,0)
            finally:repo.close()
    def test_real_session_coordinator_hook_with_private_transports(self):
        # Real ingestion/analysis/thread shutdown and scoped snapshots, fake REST and stream.
        with tempfile.TemporaryDirectory() as d:
            url='sqlite:///'+str(Path(d)/'db');settings=Settings(database_url=url,api_key='FAKE',api_secret='FAKE')
            f=FakePaper('wrapper-real-coordinator');f.at=canonical_time(datetime.now(timezone.utc).isoformat());done=threading.Event()
            class News:
                pages=1
                def __init__(self,s):pass
                def iter_news(self,request):
                    now=datetime.now(timezone.utc)-timedelta(seconds=1)
                    yield normalize_news({'id':99923999,'headline':'Microsoft publishes an update.','symbols':['MSFT'],'created_at':now.isoformat(),'updated_at':now.isoformat()})
            class Stream:
                def __init__(self,s):pass
                def run(self,request,repository,on_event=None,stop_event=None):
                    stats={'events_received':0,'events_inserted':0,'duplicates':0,'bars_written':0,'received_by_type':{},'received_by_symbol':{}}
                    run=repository.begin_stream(request,stats);t=datetime.now(timezone.utc).replace(second=0,microsecond=0)-timedelta(minutes=1)
                    event=normalize_event({'T':'b','S':'MSFT','t':t.isoformat(),'o':100,'h':102,'l':99,'c':101,'v':100,'n':5},request)
                    stats=repository.apply_stream_frame(run,[event],stats)
                    if on_event:on_event(event)
                    done.set();stop_event.wait(2);return {'run_id':run,'status':'completed',**stats}
            class Clock:
                value=0
                def __call__(self):return self.value
                def wait(self,s):self.value+=s
            clock=Clock()
            def runner(s,r,e,**kw):return run_session(s,r,e,stream_factory=Stream,news_factory=News,clock=clock,wait=clock.wait,_warmup_wait=lambda _:done.wait(2),_allow_sqlite_fixture=True,**kw)
            result=run_paper_session(settings,SessionRequest(('MSFT',),duration=60,poll_seconds=30),FixtureEngine(),api_factory=lambda _:f.client,session_runner=runner,_fixture=True)
            self.assertNotEqual(result['status'],'failed',result)
            self.assertNotIn('orders_sent',result['research_session']);self.assertEqual(f.posts,0)
            self.assertEqual(result['research_session']['paper_worker_session_id'],result['paper']['session_id'])
            self.assertFalse(result['positions_liquidated_at_session_end']);self.assertNotIn('worker_cleanup_error',result)

if __name__=='__main__':unittest.main()
