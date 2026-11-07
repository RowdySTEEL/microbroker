"""R26 observation arithmetic, missing data, bounded persistence and worker fencing."""
import contextlib
from copy import deepcopy
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid
from microbroker.cli import main
from microbroker.config import Settings
from microbroker.data.alignment import nanos
from microbroker.data.models import DataValidationError
from microbroker.data.session import SessionRequest
from microbroker.data.storage.repository import Repository,StorageError
from microbroker.execution import paper_broker as broker
from microbroker.execution.paper_api import PaperBrokerError
from microbroker.execution.paper_monitor import report_session,render_report
from microbroker.execution.paper_portfolio import capture_portfolio,read_history,validate_sampling
from microbroker.execution.paper_worker import run_paper_session,utc_add
from microbroker.execution.paper_worker_verification import WorkerFixture
from microbroker.execution.portfolio_history import history_report,render_history
from microbroker.execution.portfolio_history_verification import verify_history,history_checks,demo_history
from microbroker.execution.portfolio_sampling import PortfolioSampler
from microbroker.execution.portfolio_verification import seeded,ValuedFakePaper
from microbroker.execution.strategy_verification import FixtureEngine


class HistoryTests(unittest.TestCase):
    def setUp(self):
        self.repo=Repository('sqlite:///:memory:');self.f,self.entry=seeded(self.repo,'history-unit')
        self.ref=self.f.client.account()['account_ref'];self.sid=str(uuid.uuid4())
        self.s=PortfolioSampler(self.repo,self.f.client,self.ref,self.sid,self.f.at)
    def tearDown(self):self.repo.close()
    def pair(self,mark='120'):
        self.s.capture('start');self.f.at=utc_add(self.f.at,300);self.f.mark=mark;self.s.capture();return history_report(self.repo,self.ref)
    def test_private_scenarios(self):self.assertEqual(len(verify_history()['checks']),17)
    def test_no_samples_is_insufficient_not_zero_profit(self):
        r=history_report(self.repo);self.assertEqual(r['status'],'insufficient_observations');self.assertIsNone(r['gross_total_change'])
    def test_legacy_manual_snapshots_remain_readable(self):
        capture_portfolio(self.repo,self.f.client,snapshot_id='legacy-one');self.f.advance();self.f.mark='120'
        capture_portfolio(self.repo,self.f.client,snapshot_id='legacy-two');r=history_report(self.repo,self.ref)
        self.assertEqual(r['gross_total_change'],'1');self.assertTrue(all(p['sampling'] is None for p in r['points']))
    def test_day_week_month_are_trailing_windows_preserving_nanos(self):
        self.s.capture('start')
        for period,days in [('day',1),('week',7),('month',30)]:
            r=history_report(self.repo,self.ref,period=period,end='2026-01-05T16:12:34.123456789Z')
            self.assertEqual(nanos(r['requested_end'])-nanos(r['requested_start']),days*86400*10**9)
            self.assertTrue(r['requested_start'].endswith('.123456789Z'))
    def test_decline_is_negative_not_zero(self):self.assertEqual(self.pair('90')['gross_total_change'],'-2')
    def test_unchanged_position_change_is_zero_not_sum_of_cumulative_profit(self):self.assertEqual(self.pair('110')['gross_total_change'],'0')
    def test_final_sale_does_not_double_count_open_profit(self):
        self.pair();plan=broker.prepare_exit(self.repo,self.f.client,'history-exit',self.entry);broker.submit(self.repo,self.f.client,'history-exit')
        self.f.update(plan['request']['client_order_id'],'filled','0.1','120');self.s.capture('final');r=history_report(self.repo,self.ref)
        self.assertEqual((r['gross_realized_change'],r['gross_unrealized_change'],r['gross_total_change']),('2','-1','1'))
        self.assertEqual(r['by_origin']['manual_paper']['gross_total_change'],'1');self.assertIsNone(r['net_profit'])
    def test_partial_sale_uses_remaining_basis(self):
        self.pair();plan=broker.prepare_exit(self.repo,self.f.client,'history-partial',self.entry);broker.submit(self.repo,self.f.client,'history-partial')
        self.f.update(plan['request']['client_order_id'],'partially_filled','0.04','120');self.s.capture('final');r=history_report(self.repo,self.ref)
        self.assertEqual((r['gross_realized_change'],r['gross_unrealized_change'],r['gross_total_change']),('0.8','0.2','1'))
    def test_incomplete_middle_point_withholds_even_if_endpoints_ready(self):
        self.s.capture('start');self.f.at=utc_add(self.f.at,300);self.f.missing_mark=True;self.s.capture()
        self.f.at=utc_add(self.f.at,300);self.f.missing_mark=False;self.s.capture('final');r=history_report(self.repo,self.ref)
        self.assertEqual(r['status'],'attention_required');self.assertIsNone(r['gross_total_change']);self.assertEqual(len(r['incomplete_snapshot_ids']),1)
    def test_same_timestamp_observations_do_not_claim_period(self):
        self.s.capture('start');self.s.capture('final');r=history_report(self.repo,self.ref)
        self.assertEqual(r['samples'],2);self.assertEqual(r['status'],'insufficient_observations')
    def test_requested_boundaries_are_inclusive_without_carry_forward(self):
        self.pair();r=history_report(self.repo,self.ref,start=utc_add(self.s.started_at,1))
        self.assertEqual(r['samples'],1);self.assertIsNone(r['gross_total_change']);self.assertFalse(r['exact_requested_boundaries_observed'])
    def test_display_limit_does_not_change_full_dataset_digest(self):
        r=self.pair();limited=history_report(self.repo,self.ref,limit=1)
        self.assertEqual(r['selected_dataset_sha256'],limited['selected_dataset_sha256']);self.assertEqual(limited['gross_total_change'],'1')
    def test_future_capture_changes_chain_but_not_earlier_selected_dataset(self):
        self.pair();end=self.f.at;a=history_report(self.repo,self.ref,end=end);self.f.advance();capture_portfolio(self.repo,self.f.client,snapshot_id='future')
        b=history_report(self.repo,self.ref,end=end);self.assertEqual(a['selected_dataset_sha256'],b['selected_dataset_sha256']);self.assertNotEqual(a['history_sha256'],b['history_sha256'])
    def test_session_filter_ignores_other_session_and_manual_capture(self):
        self.pair();self.f.advance();capture_portfolio(self.repo,self.f.client,snapshot_id='manual')
        other=PortfolioSampler(self.repo,self.f.client,self.ref,str(uuid.uuid4()),self.f.at);other.capture('start')
        r=history_report(self.repo,self.ref,session_id=self.sid);self.assertEqual(r['samples'],2);self.assertEqual(history_report(self.repo,self.ref)['samples'],4)
    def test_cash_or_equity_change_not_substituted_for_gross_pnl(self):
        self.s.capture('start');self.f.advance();self.f.cash='12000';self.f.equity='13000';self.s.capture('final');r=history_report(self.repo,self.ref)
        self.assertEqual(r['gross_total_change'],'0');self.assertNotEqual(r['equity_change'],'0');self.assertFalse(r['cashflows_verified'])
    def test_history_is_offline(self):
        self.pair();calls=len(self.f.calls);history_report(self.repo,self.ref);self.assertEqual(len(self.f.calls),calls)
    def test_invalid_query_rejected(self):
        for kw in ({'period':'year'},{'limit':0},{'limit':True},{'limit':101},{'session_id':'x'},{'account_ref':'x'}, {'start':'2026-01-06T00:00:00Z','end':'2026-01-05T00:00:00Z'}):
            with self.subTest(kw=kw),self.assertRaises(DataValidationError):history_report(self.repo,**kw)
    def test_multiple_accounts_require_explicit_ref(self):
        self.pair();other,e=seeded(self.repo,'another-history');capture_portfolio(self.repo,other.client,snapshot_id='other')
        with self.assertRaises(DataValidationError):history_report(self.repo)
        self.assertEqual(history_report(self.repo,self.ref)['samples'],2)
    def test_tampered_sampling_scope_rejected(self):
        self.pair();self.repo.connection.execute("UPDATE paper_portfolio_snapshots SET snapshot_json='{}'");self.repo.connection.commit()
        with self.assertRaises(StorageError):history_report(self.repo,self.ref)
    def test_text_discloses_actual_window_and_not_net_profit(self):
        text=render_history(self.pair());self.assertIn('Gözlenen UTC aralığı',text);self.assertIn('tam gün/hafta/ay',text);self.assertIn('Net kâr',text)
    def test_strict_pg_implicit_transaction_protocol(self):
        repo=Repository('sqlite:///:memory:');base=repo.connection
        class Shim:
            @property
            def info(self):return SimpleNamespace(transaction_status=2 if base.in_transaction else 0)
            def __getattr__(self,n):return getattr(base,n)
            def execute(self,q,p=()):
                if q.startswith('SET TRANSACTION'):return base.execute('BEGIN')
                if q.startswith('SELECT') and not base.in_transaction:base.execute('BEGIN')
                return base.execute(q.replace('%s','?').replace(' FOR UPDATE',''),p)
        repo.connection=Shim();repo.postgres=True;repo.marker='%s'
        try:self.assertEqual(len(history_checks(repo)),17)
        finally:repo.close()


class SamplingTests(unittest.TestCase):
    setUp=HistoryTests.setUp
    tearDown=HistoryTests.tearDown
    def test_start_and_final_are_different_slots(self):
        before=self.f.posts;self.s.capture('start');self.f.advance();self.s.capture('final')
        self.assertEqual(self.s.summary()['recorded_snapshots'],2);self.assertEqual(self.f.posts,before)
    def test_interval_boundary_and_no_duplicate(self):
        self.f.at=utc_add(self.f.at,299);self.assertIsNone(self.s.capture());self.f.advance();self.s.capture();self.s.capture()
        self.assertEqual([a['slot'] for a in self.s.summary()['attempts']],['interval-1'])
    def test_skipped_slots_not_backfilled(self):
        self.s.capture('start');self.f.at=utc_add(self.f.at,1000);self.s.capture()
        self.assertEqual([a['slot'] for a in self.s.summary()['attempts']],['start','interval-3']);self.assertFalse(self.s.summary()['missed_slots_backfilled'])
    def test_recreated_sampler_restores_successful_slots(self):
        self.s.capture('start');s=PortfolioSampler(self.repo,self.f.client,self.ref,self.sid,self.s.started_at);s.capture('start')
        self.assertEqual(s.summary()['recorded_snapshots'],1);self.assertEqual(s.summary()['attempts'],[])
    def test_transient_get_failure_does_not_fake_zero_sample_and_next_slot_recovers(self):
        original=self.f.client.valued_positions
        self.f.client.valued_positions=lambda:(_ for _ in ()).throw(PaperBrokerError('transport_failed'))
        self.assertIsNone(self.s.capture('start'));self.s.capture('start');r=self.s.summary()
        self.assertEqual(len(r['attempts']),1);self.assertEqual(r['recorded_snapshots'],0);self.assertTrue(r['sampling_degraded'])
        self.f.client.valued_positions=original;self.f.at=utc_add(self.f.at,300);self.s.capture()
        self.assertEqual(self.s.summary()['recorded_snapshots'],1);self.assertTrue(self.s.summary()['sampling_degraded'])
    def test_http_failure_contains_only_sanitized_diagnostics(self):
        def failed():raise PaperBrokerError('http_error',503)
        self.f.client.valued_positions=failed;self.s.capture('start');error=self.s.summary()['errors'][0]
        self.assertEqual(error['exception_type'],'PaperBrokerError');self.assertNotIn('headers',json.dumps(error));self.assertNotIn('raw',error)
    def test_authentication_failure_is_fatal_instead_of_transient_sampling_gap(self):
        self.f.client.valued_positions=lambda:(_ for _ in ()).throw(PaperBrokerError('http_error',403))
        with self.assertRaises(PaperBrokerError):self.s.capture('start')
        self.assertEqual(self.s.summary()['recorded_snapshots'],0)
    def test_integrity_failure_is_fatal_and_snapshot_not_written(self):
        self.f.client.valued_positions=lambda:(_ for _ in ()).throw(StorageError('TEST_SECRET'))
        with self.assertRaises(StorageError):self.s.capture('start')
        self.assertEqual(self.s.summary()['recorded_snapshots'],0);self.assertNotIn('TEST_SECRET',json.dumps(self.s.summary()))
    def test_wrong_account_never_captures_for_new_identity(self):
        self.f.identity='00000000-0000-0000-0000-000000000002'
        with self.assertRaises(PaperBrokerError):self.s.capture('start')
        self.assertEqual(self.repo.connection.execute('SELECT COUNT(*) FROM paper_portfolio_snapshots').fetchone()[0],0)
    def test_clock_regression_is_fatal(self):
        self.f.at=utc_add(self.f.at,-1)
        with self.assertRaises(StorageError):self.s.capture('start')
    def test_invalid_tag_types_and_scope_rejected(self):
        meta={'format':'R26','session_id':self.sid,'slot':'start','interval_seconds':300}
        for change in ({'interval_seconds':True},{'interval_seconds':1},{'slot':'interval-0'},{'slot':'x'},{'session_id':'x'},{'format':'R25'},{'unexpected':1}):
            with self.subTest(change=change),self.assertRaises(StorageError):validate_sampling({**meta,**change})
    def test_retry_cannot_relabel_existing_snapshot_scope(self):
        self.s.capture('start');meta={'format':'R26','session_id':self.sid,'slot':'final','interval_seconds':300}
        with self.assertRaises(StorageError):capture_portfolio(self.repo,self.f.client,snapshot_id='r26-'+self.sid+'-start',_sampling=meta)
    def test_insert_failure_rolls_back_but_previous_history_survives(self):
        self.s.capture('start');base=self.repo.connection
        class Fail:
            def __getattr__(self,n):return getattr(base,n)
            def execute(self,q,p=()):
                out=base.execute(q,p)
                if q.startswith('INSERT INTO paper_portfolio_snapshots'):raise StorageError('insert failed')
                return out
        self.repo.connection=Fail();self.f.at=utc_add(self.f.at,300)
        with self.assertRaises(StorageError):self.s.capture()
        self.repo.connection=base;self.assertEqual(history_report(self.repo,self.ref)['samples'],1)
    def test_capacity_failure_preserves_previous_samples(self):
        self.s.capture('start');self.f.advance()
        with patch('microbroker.execution.paper_portfolio.MAX_SAMPLES',1),self.assertRaises(StorageError):self.s.capture('final')
        self.assertEqual(history_report(self.repo,self.ref)['samples'],1)
    def test_summary_is_detached_from_internal_state(self):
        self.s.capture('start');r=self.s.summary();r['attempts'][0]['status']='fake'
        self.assertEqual(self.s.summary()['attempts'][0]['status'],'captured')
    def test_other_sampling_failure_cannot_send_previously_ready_positive_order(self):
        repo=Repository('sqlite:///:memory:')
        try:
            fixture=WorkerFixture(repo,'r26-fenced-positive',63);fixture.news();fixture.ready()
            sampler=PortfolioSampler(repo,fixture.fake.client,fixture.worker.account_ref,fixture.worker.session_id,fixture.worker.started)
            original=fixture.fake.client.valued_positions
            def slow():fixture.fake.at=utc_add(fixture.fake.at,121);return original()
            fixture.fake.client.valued_positions=slow;fixture.worker.lease();sampler.capture('start')
            with self.assertRaisesRegex(PaperBrokerError,'lease_lost'):fixture.tick()
            self.assertEqual(fixture.fake.posts,0)
        finally:repo.close()


class WrapperSamplingTests(unittest.TestCase):
    def run_case(self,runner,modify=None):
        self.folder=tempfile.TemporaryDirectory();self.addCleanup(self.folder.cleanup)
        self.url='sqlite:///'+str(Path(self.folder.name)/'db');self.f=ValuedFakePaper('wrapper-r26')
        if modify:modify(self.f)
        settings=Settings(database_url=self.url,api_key='FAKE',api_secret='FAKE')
        return run_paper_session(settings,SessionRequest(('MSFT',),duration=600,poll_seconds=60),FixtureEngine(),api_factory=lambda _:self.f.client,
            session_runner=lambda *a,**k:runner(self.f,*a,**k),_fixture=True)
    def inspect(self):
        repo=Repository(self.url);self.addCleanup(repo.close);return repo
    def test_wrapper_captures_start_interval_final_and_seals_monitor(self):
        def runner(f,s,r,e,**kw):
            hook=kw['_cycle_hook']
            for _ in range(5):f.at=utc_add(f.at,60);hook(None,{},None)
            return {'status':'completed'}
        result=self.run_case(runner);sampling=result['portfolio_sampling'];self.assertGreaterEqual(sampling['recorded_snapshots'],2);self.assertLessEqual(sampling['recorded_snapshots'],3)
        repo=self.inspect();report=report_session(repo,result['paper']['session_id'])
        self.assertEqual(report['portfolio_sampling'],sampling);self.assertIn('Portföy gözlemi: '+str(sampling['recorded_snapshots']),render_report(report))
        self.assertEqual(repo.connection.execute('SELECT COUNT(*) FROM paper_worker_leases').fetchone()[0],0);self.assertEqual(self.f.posts,0)
    def test_transient_portfolio_failure_is_visible_without_aborting_coordinator(self):
        def modify(f):f.client.valued_positions=lambda:(_ for _ in ()).throw(PaperBrokerError('transport_failed'))
        result=self.run_case(lambda *a,**kw:{'status':'completed'},modify)
        self.assertEqual(result['status'],'completed');self.assertTrue(result['portfolio_sampling']['sampling_degraded'])
        self.assertEqual(len(result['portfolio_sampling']['errors']),2);self.assertEqual(self.f.posts,0)
        self.assertIn('örneklemesi eksik',render_report(report_session(self.inspect(),result['paper']['session_id'])))
    def test_final_integrity_failure_disclosed_and_lease_released(self):
        def runner(f,*a,**kw):
            f.client.valued_positions=lambda:(_ for _ in ()).throw(StorageError('TEST_SECRET'))
            return {'status':'completed'}
        result=self.run_case(runner);self.assertIn('portfolio_capture_error',result);self.assertNotIn('TEST_SECRET',json.dumps(result))
        self.assertEqual(self.inspect().connection.execute('SELECT COUNT(*) FROM paper_worker_leases').fetchone()[0],0)
    def test_preparation_expired_lease_never_starts_coordinator(self):
        def modify(f):
            original=f.client.clock
            def slow():f.at=utc_add(f.at,121);return original()
            f.client.clock=slow
        with self.assertRaisesRegex(PaperBrokerError,'lease_lost'):self.run_case(lambda *a,**kw:self.fail('Coordinator must not start'),modify)
        self.assertEqual(self.f.posts,0);repo=self.inspect();self.assertEqual(repo.connection.execute('SELECT COUNT(*) FROM paper_worker_leases').fetchone()[0],0)
    def test_expired_owner_still_cannot_tick_after_background_revision(self):
        def runner(f,*a,**kw):
            f.at=utc_add(f.at,121);kw['_cycle_hook'](None,{},None)
            self.fail('Expired lease must stop')
        with self.assertRaisesRegex(PaperBrokerError,'lease_lost'):self.run_case(runner)
        self.assertEqual(self.f.posts,0)
    def test_failed_runner_keeps_start_and_final_observations(self):
        def runner(f,*a,**kw):f.advance();raise RuntimeError('synthetic failure')
        with self.assertRaises(RuntimeError):self.run_case(runner)
        repo=self.inspect();self.assertEqual(repo.connection.execute('SELECT COUNT(*) FROM paper_portfolio_snapshots').fetchone()[0],2)
        self.assertEqual(repo.connection.execute('SELECT COUNT(*) FROM paper_worker_leases').fetchone()[0],0)


class HistoryCliTests(unittest.TestCase):
    def test_private_cli_does_not_load_env(self):
        with patch('microbroker.cli.Settings.from_env',side_effect=AssertionError('No env')),contextlib.redirect_stdout(io.StringIO()) as out:
            self.assertEqual(main(['verify-portfolio-history']),0)
        self.assertEqual(json.loads(out.getvalue())['status'],'passed')
    def test_demo_is_isolated_and_explicitly_synthetic(self):
        with tempfile.TemporaryDirectory() as d:
            folder=Path(d)/'demo'
            with patch('microbroker.cli.Settings.from_env',side_effect=AssertionError('No env')),contextlib.redirect_stdout(io.StringIO()) as out:
                self.assertEqual(main(['demo-portfolio-history','--output',str(folder)]),0)
            r=json.loads(out.getvalue());self.assertEqual(r['snapshots'],3);self.assertEqual(r['gross_total_change'],'1');self.assertEqual(r['orders_sent_to_alpaca'],0)
            self.assertTrue((folder/'report.txt').read_text().startswith('SENTETİK DEMO'))
            with self.assertRaises(DataValidationError):demo_history(folder)
    def test_offline_cli_report_does_not_construct_api(self):
        with tempfile.TemporaryDirectory() as d:
            url='sqlite:///'+str(Path(d)/'db');repo=Repository(url);f,e=seeded(repo,'history-cli')
            capture_portfolio(repo,f.client,snapshot_id='one');f.advance();f.mark='120';capture_portfolio(repo,f.client,snapshot_id='two');repo.close()
            with patch('microbroker.cli.Settings.from_env',return_value=Settings(database_url=url)),patch('microbroker.cli.PaperAPI',side_effect=AssertionError('No network')),contextlib.redirect_stdout(io.StringIO()) as out:
                self.assertEqual(main(['paper-portfolio-history','--period','day','--format','text']),0)
            self.assertIn('Brüt toplam değişim: 1 USD',out.getvalue())
    def test_worker_cli_flags_sampling_degradation_as_nonzero(self):
        fake=SimpleNamespace(name='finbert')
        settings=Settings(database_url='postgresql://test/db',api_key='FAKE',api_secret='FAKE')
        with patch('microbroker.cli.Settings.from_env',return_value=settings),patch('microbroker.cli.make_engine',return_value=fake),patch('microbroker.cli.run_paper_session',return_value={'status':'completed','portfolio_sampling':{'sampling_degraded':True}}),contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['run-paper-session']),2)
