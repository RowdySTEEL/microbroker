"""R28 blocked-network concurrency, deadline, integrity and offline reporting."""
import contextlib
import io
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
from microbroker.cli import main
from microbroker.config import Settings
from microbroker.data.session import SessionRequest
from microbroker.data.storage.repository import Repository,StorageError
from microbroker.execution.async_portfolio import AsyncPortfolioSampler
from microbroker.execution.observation_verification import verify_observation,observation_checks,concurrent_checks
from microbroker.execution.session_timing import Timings,TimedAPI,distribution
from microbroker.execution.paper_api import PaperBrokerError
from microbroker.execution.paper_portfolio import capture_portfolio,read_history
from microbroker.execution.paper_worker import run_paper_session,utc_add
from microbroker.execution.paper_worker_verification import WorkerFixture
from microbroker.execution.portfolio_verification import ValuedFakePaper,seeded
from microbroker.execution.latency_report import latency_report,render_latency
from microbroker.execution.paper_monitor import save_run,report_session,render_report
from microbroker.execution import paper_broker as broker
from microbroker.execution.strategy_verification import FixtureEngine


class AsyncObserverTests(unittest.TestCase):
    def setUp(self):
        self.folder=tempfile.TemporaryDirectory();self.addCleanup(self.folder.cleanup)
        self.settings=Settings(database_url='sqlite:///'+str(Path(self.folder.name)/'db'))
        self.repo=Repository(self.settings.database_url);self.addCleanup(self.repo.close)
        self.f,self.entry=seeded(self.repo,'r28-observer-tests');self.ref=self.f.client.account()['account_ref']
        import uuid
        self.sid=str(uuid.uuid4());self.timer=Timings();self.release=threading.Event();self.entered=threading.Event()
        self.addCleanup(self.release.set)
    def observer(self,api=None,factory=Repository):
        observer=AsyncPortfolioSampler(factory,lambda _:api or self.f.client,self.settings,self.ref,self.sid,self.f.at,self.timer)
        self.addCleanup(lambda:observer.finish(timeout=2));return observer
    def block(self):
        original=self.f.client.valued_positions
        def slow():
            self.entered.set()
            if not self.release.wait(3):raise PaperBrokerError('transport_failed')
            return original()
        self.f.client.valued_positions=slow
    def test_controlled_slow_observer_does_not_hold_writer_or_db(self):
        self.assertEqual(len(concurrent_checks()),4)
    def test_timeout_fences_late_capture_and_closes_on_owner_thread(self):
        self.block();observer=self.observer();observer.request('start');self.assertTrue(self.entered.wait(2))
        observer.finish(timeout=.01);self.assertTrue(observer.summary()['shutdown_timed_out'])
        self.assertTrue(observer.summary()['observer_thread_alive'])
        self.release.set();observer.thread.join(2)
        self.assertFalse(observer.thread.is_alive());self.assertTrue(observer.summary()['shutdown_timed_out'])
        self.assertEqual(self.repo.connection.execute('SELECT COUNT(*) FROM paper_portfolio_snapshots').fetchone()[0],0)
        self.assertEqual(self.f.posts,1)  # Only the seeded manual fixture order.
    def test_timeout_during_started_db_write_reports_possible_late_commit(self):
        def factory(url):
            repo=Repository(url);base=repo.connection
            test=self
            class BlockInsert:
                def __getattr__(self,name):return getattr(base,name)
                def execute(self,query,params=()):
                    if query.startswith('INSERT INTO paper_portfolio_snapshots'):
                        test.entered.set()
                        if not test.release.wait(3):raise StorageError('Controlled DB write release missing')
                    return base.execute(query,params)
            repo.connection=BlockInsert();return repo
        observer=self.observer(factory=factory);observer.request('start');self.assertTrue(self.entered.wait(2))
        observer.finish(timeout=.01)
        self.assertTrue(observer.summary()['commit_in_flight_at_timeout'])
        self.assertTrue(observer.summary()['already_started_database_write_may_finish'])
        self.release.set();observer.thread.join(2)
        self.assertFalse(observer.thread.is_alive());self.assertTrue(observer.summary()['sampling_degraded'])
        self.assertEqual(self.repo.connection.execute('SELECT COUNT(*) FROM paper_portfolio_snapshots').fetchone()[0],1)
    def test_final_observation_uses_new_stable_sources_after_race(self):
        self.block();observer=self.observer();observer.request('start');self.assertTrue(self.entered.wait(2))
        broker.prepare_buy(self.repo,self.f.client,'r28-new-source','AAPL','10')
        self.release.set();observer.finish(timeout=2)
        self.assertEqual(observer.summary()['recorded_snapshots'],1)
        self.assertIn('final',observer.summary()['last_snapshot_id'])
        self.assertTrue(observer.summary()['sampling_degraded'])
        self.assertEqual(self.repo.connection.execute('SELECT COUNT(*) FROM broker_paper_events WHERE intent_id=?',('r28-new-source',)).fetchone()[0],0)
    def test_transient_error_does_not_invent_snapshot_or_kill_observer(self):
        self.f.client.valued_positions=lambda:(_ for _ in ()).throw(PaperBrokerError('http_error',503))
        observer=self.observer();observer.request('start');observer.finish(timeout=2)
        self.assertEqual(len(observer.summary()['errors']),2);self.assertEqual(observer.summary()['recorded_snapshots'],0)
        self.assertNotIn('FAKE_SECRET',json.dumps(observer.summary()))
    def test_auth_failure_is_fatal_and_cannot_be_reported_as_successful_observation(self):
        self.f.client.valued_positions=lambda:(_ for _ in ()).throw(PaperBrokerError('http_error',401))
        observer=self.observer();observer.request('start')
        with self.assertRaises(PaperBrokerError):observer.finish(timeout=2)
        self.assertTrue(observer.summary()['sampling_degraded']);self.assertEqual(observer.summary()['recorded_snapshots'],0)
        # Cleanup may recheck the fatal error; consume it explicitly.
        self._cleanups.pop()
    def test_repository_start_failure_is_sanitized_and_propagated(self):
        def broken(url):raise StorageError('SENTINEL_SECRET')
        observer=self.observer(factory=broken)
        with self.assertRaises(StorageError):observer.finish(timeout=2)
        self.assertNotIn('SENTINEL_SECRET',json.dumps(observer.summary()));self._cleanups.pop()
    def test_fresh_broker_snapshot_does_not_rewrite_stale_local_order(self):
        before=broker.report(self.repo,self.entry)
        cid=before['request']['client_order_id'];self.f.update(cid,'filled','0.1','101')
        observer=self.observer();observer.request('start');observer.finish(timeout=2)
        self.assertEqual(broker.report(self.repo,self.entry),before)
        self.assertEqual(observer.summary()['recorded_snapshots'],2)
    def test_thread_close_failure_is_degraded(self):
        def factory(url):
            repo=Repository(url);original=repo.close
            def close():original();raise StorageError('SENTINEL_SECRET')
            repo.close=close;return repo
        observer=self.observer(factory=factory);observer.request('start');observer.finish(timeout=2)
        self.assertTrue(observer.summary()['sampling_degraded']);self.assertNotIn('SENTINEL_SECRET',json.dumps(observer.summary()))


class TimingCoverageTests(unittest.TestCase):
    def setUp(self):
        self.repo=Repository('sqlite:///:memory:');self.addCleanup(self.repo.close)
        self.f=WorkerFixture(self.repo,'r28-timing-unit',90)
    def tearDown(self):self.f.worker.finish()
    def test_private_checks(self):self.assertEqual(len(observation_checks(self.repo)),13)
    def test_previous_session_claim_is_not_a_new_unassessed_signal(self):
        self.f.news();self.f.fake.advance();self.f.tick();self.f.worker.finish()
        import uuid
        self.f.worker.session_id=str(uuid.uuid4());self.f.worker.lease(acquire=True)
        coverage=self.f.worker.coverage(self.f.fake.at)
        self.assertEqual(coverage['previous_account_claims'],1);self.assertEqual(coverage['unassessed_analyses'],0)
    def test_cutoff_excludes_future_analysis_and_future_publication(self):
        self.f.news(at='2026-01-05T15:00:10Z');self.f.fake.advance()
        self.assertEqual(self.f.worker.coverage(self.f.fake.at)['available_analyses'],0)
    def test_tampered_order_chain_rejected_before_latency_query(self):
        self.f.news();self.f.ready();self.f.tick();intent=self.f.decisions()[0][5]
        self.repo.connection.execute("UPDATE broker_paper_events SET event_json='{}' WHERE intent_id=?",(intent,));self.repo.connection.commit()
        with self.assertRaises(StorageError):self.f.worker.coverage(self.f.fake.at)
        # Keep tearDown's retirement audit valid.
        self.f.worker.lease_acquired=False
    def test_new_revision_with_prior_article_claim_not_falsely_new_trade(self):
        self.f.news();self.f.fake.advance();self.f.tick()
        c=self.f.worker.coverage(self.f.fake.at);self.assertEqual(c['claimed_by_this_session'],1)
        self.assertFalse(c['executable_opportunity_validated'])
    def test_monotonic_guard_blocks_prepared_order_even_when_wall_clock_is_before_deadline(self):
        self.f.news();self.f.ready();original=broker.submit
        def expire(*args,**kwargs):
            self.f.worker.monotonic_deadline=0
            return original(*args,**kwargs)
        with patch('microbroker.execution.paper_worker.broker.submit',side_effect=expire):self.f.tick()
        self.assertEqual(self.f.fake.posts,0)
        self.assertEqual(self.f.decisions()[0][2]['reason'],'session_deadline')
    def test_nested_timing_is_bounded_and_detached(self):
        values=iter([0,1,4,6]);t=Timings(clock=lambda:next(values))
        with t.measure('outer'):
            with t.measure('inner'):pass
        r=t.summary();self.assertEqual(r['stages']['inner']['median_seconds'],3)
        self.assertEqual(r['stages']['outer']['median_seconds'],6);r['stages'].clear();self.assertEqual(len(t.summary()['stages']),2)
    def test_record_limit_disclosed(self):
        t=Timings();t.values['test']=[1]*4096
        with t.measure('new'):pass
        self.assertEqual(t.summary()['dropped_samples'],1)
    def test_slow_reconciliation_crossing_deadline_does_not_create_wait_claims(self):
        self.f.news();original=broker.reconcile_account
        def slow(*args,**kwargs):
            result=original(*args,**kwargs);self.f.fake.at=self.f.worker.deadline;return result
        with patch('microbroker.execution.paper_worker.broker.reconcile_account',side_effect=slow):
            self.assertEqual(self.f.tick()['status'],'deadline')
        self.assertEqual(len(self.f.decisions()),0);self.assertEqual(self.f.fake.posts,0)
        self.assertEqual(self.f.worker.coverage(self.f.fake.at)['unassessed_analyses'],1)
        self.f.worker.lease_acquired=False
    def test_fatal_observer_failure_is_rechecked_by_order_guard(self):
        self.f.news();self.f.ready();original=broker.submit
        def submit(*args,**kwargs):
            self.f.worker.background_check=lambda:(_ for _ in ()).throw(PaperBrokerError('observer_failed'))
            return original(*args,**kwargs)
        with patch('microbroker.execution.paper_worker.broker.submit',side_effect=submit):self.f.tick()
        self.assertEqual(self.f.fake.posts,0);self.assertEqual(self.f.decisions()[0][2]['reason'],'observer_failed')
    def test_latency_cli_is_offline_and_honors_specific_session(self):
        save_run(self.repo,self.f.worker,{'status':'completed','paper':self.f.worker.summary()})
        settings=Settings(database_url='sqlite:///:memory:')
        with patch('microbroker.cli.Settings.from_env',return_value=settings),patch('microbroker.cli.Repository',return_value=self.repo),patch('microbroker.cli.PaperAPI',side_effect=AssertionError('No network')),contextlib.redirect_stdout(io.StringIO()) as out:
            self.assertEqual(main(['paper-session-latency','--session-id',self.f.worker.session_id,'--format','json']),0)
        self.assertEqual(json.loads(out.getvalue())['session_id'],self.f.worker.session_id)
        # main owns and closes its repository.
        self.f.worker.lease_acquired=False
    def test_sampler_cannot_get_raw_transport_or_delete_capability(self):
        api=TimedAPI(self.f.fake.client,Timings(),observer=True)
        for name in ('call','send','submit','cancel','lookup','positions','asset'):
            with self.subTest(name=name),self.assertRaises(AttributeError):getattr(api,name)
    def test_one_observation_does_not_invent_missing_order_stages(self):
        self.f.news();self.f.fake.advance();c=self.f.worker.coverage(self.f.fake.at)
        self.assertEqual(c['pipeline_latency']['stages']['publication_to_first_receipt']['samples'],1)
        self.assertIsNone(c['pipeline_latency']['stages']['post_claim_to_first_local_filled_observation']['p95_seconds'])
    def test_unassessed_analysis_not_headlined_as_all_decisions_waited(self):
        self.f.news();self.f.fake.advance();self.f.worker.monotonic_deadline=0
        coverage=self.f.worker.coverage(self.f.fake.at)
        save_run(self.repo,self.f.worker,{'status':'completed','paper':self.f.worker.summary(),
            'decision_coverage':coverage,'research_session':{'news_polls':1,'received_articles':1}})
        report=report_session(self.repo,self.f.worker.session_id)
        self.assertEqual(report['assessment'],'decision_coverage_incomplete')
        self.assertIn('incelenmemiş',render_report(report))
    def test_legacy_session_latency_is_unknown_and_readonly(self):
        save_run(self.repo,self.f.worker,{'status':'completed','paper':self.f.worker.summary()})
        before=self.repo.connection.total_changes;r=latency_report(self.repo,self.f.worker.session_id)
        self.assertEqual(r['status'],'not_recorded');self.assertIn('sıfır gecikme varsayılmadı',render_latency(r))
        self.assertEqual(self.repo.connection.total_changes,before)
    def test_public_private_cli_never_loads_env_or_model(self):
        with patch('microbroker.cli.Settings.from_env',side_effect=AssertionError('No env')),contextlib.redirect_stdout(io.StringIO()) as out:
            self.assertEqual(main(['verify-session-observation']),0)
        self.assertEqual(len(json.loads(out.getvalue())['checks']),17)


class WrapperTimelineTests(unittest.TestCase):
    def test_slow_preparation_excluded_and_no_orders_after_shared_deadline(self):
        with tempfile.TemporaryDirectory() as folder:
            settings=Settings(database_url='sqlite:///'+str(Path(folder)/'db'))
            f=ValuedFakePaper('r28-preparation');original=f.client.account;calls=0
            def account():
                nonlocal calls
                calls+=1
                if calls==2:f.at=utc_add(f.at,40)
                return original()
            f.client.account=account
            def runner(s,r,e,**kw):
                timeline=kw['_timeline'];self.assertEqual(timeline['started_at'],'2026-01-05T15:00:40.000000000Z')
                self.assertEqual(timeline['deadline'],'2026-01-05T15:01:40.000000000Z')
                f.at=utc_add(f.at,61);cycle=kw['_cycle_hook'](None,{},None)
                self.assertEqual(cycle['status'],'deadline');return {'status':'completed','execution_timeline':timeline}
            result=run_paper_session(settings,SessionRequest(('MSFT',),duration=60),FixtureEngine(),api_factory=lambda _:f.client,session_runner=runner,_fixture=True)
            self.assertEqual(result['paper']['post_attempts'],0);self.assertEqual(result['decision_coverage']['unassessed_analyses'],0)
            self.assertIn('timing',result);self.assertNotIn('worker_cleanup_error',result)
