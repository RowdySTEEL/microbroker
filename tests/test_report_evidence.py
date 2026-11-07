"""R29 acceptance regressions, including legacy audits and interrupted sessions."""
from copy import deepcopy
import contextlib
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from microbroker.cli import main
from microbroker.config import Settings
from microbroker.data.session import SessionRequest
from microbroker.data.storage.repository import Repository,StorageError
from microbroker.execution.paper_api import PaperBrokerError
from microbroker.execution.paper_worker import run_paper_session,utc_add
from microbroker.execution.paper_worker_verification import WorkerFixture
from microbroker.execution.paper_monitor import save_run,report_session,render_report
from microbroker.execution.latency_report import latency_report,render_latency
from microbroker.execution.portfolio_sampling import PortfolioSampler
from microbroker.execution.portfolio_verification import ValuedFakePaper
from microbroker.execution.session_timing import Timings
from microbroker.execution.strategy_verification import FixtureEngine
from microbroker.execution.report_evidence import coverage_view,price_warning_history,timing_view,validate_warning_history
from microbroker.execution.report_price_health import price_health
from microbroker.execution.report_evidence_verification import evidence_checks,verify_evidence,revise,recovered_cycles


class CaptureTimingTests(unittest.TestCase):
    def setUp(self):
        self.repo=Repository('sqlite:///:memory:');self.addCleanup(self.repo.close)
        self.f=ValuedFakePaper('r29-timing');self.timer=Timings();self.clock=0.;self.timer.clock=lambda:self.clock
        self.sampler=PortfolioSampler(self.repo,self.f.client,self.f.client.account()['account_ref'],
            '00000000-0000-4000-8000-000000000029',self.f.at,_timing=self.timer)
    def test_failed_get_is_not_a_successful_capture_duration(self):
        def failed():self.clock+=4;raise PaperBrokerError('http_error',503)
        self.f.client.valued_positions=failed
        self.assertIsNone(self.sampler.capture('start'));self.sampler.capture('start')
        s=self.timer.summary()['stages'];self.assertNotIn('portfolio_capture',s)
        self.assertEqual(s['portfolio_capture_failed']['median_seconds'],4)
        self.assertEqual(s['portfolio_capture_attempt']['samples'],1);self.assertEqual(s['portfolio_slot_check']['samples'],2)
        self.assertEqual(self.sampler.summary()['recorded_snapshots'],0)
    def test_resumed_sampler_does_not_count_loaded_snapshots_as_new_captures(self):
        self.sampler.capture('start');self.sampler.capture('final');timer=Timings()
        other=PortfolioSampler(self.repo,self.f.client,self.sampler.account_ref,self.sampler.session_id,self.f.at,_timing=timer)
        other.capture('start');other.capture('final')
        self.assertNotIn('portfolio_capture',timer.summary()['stages'])
        self.assertEqual(other.summary()['new_captures_this_process'],0);self.assertEqual(other.summary()['recorded_snapshots'],2)
    def test_existing_response_has_separate_metric(self):
        self.sampler.capture('start');self.sampler.attempted.clear()
        self.sampler.capture('start');s=self.timer.summary()['stages']
        self.assertEqual(s['portfolio_capture']['samples'],1);self.assertEqual(s['portfolio_capture_existing']['samples'],1)
        self.assertEqual(s['portfolio_capture_attempt']['samples'],2)
    def test_real_success_after_failed_slot_has_only_one_success_sample(self):
        original=self.f.client.valued_positions
        self.f.client.valued_positions=lambda:(_ for _ in ()).throw(PaperBrokerError('transport_failed'))
        self.sampler.capture('start');self.f.client.valued_positions=original;self.sampler.capture('final')
        s=self.timer.summary()['stages'];self.assertEqual(s['portfolio_capture']['samples'],1)
        self.assertEqual(s['portfolio_capture_failed']['samples'],1);self.assertEqual(s['portfolio_capture_attempt']['samples'],2)


class RevisionEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.repo=Repository('sqlite:///:memory:');self.addCleanup(self.repo.close)
        self.f=WorkerFixture(self.repo,'r29-current',102);self.addCleanup(self.f.worker.finish)
    def test_private_verifier_all_checks(self):
        self.assertEqual(len(verify_evidence()['checks']),15)
    def test_old_claim_current_revision_and_withheld_context_separate(self):
        self.f.news();self.f.fake.advance();self.f.tick();revise(self.f)
        self.f.fake.at='2026-01-05T15:00:03.000000000Z';self.f.tick()
        c=self.f.worker.coverage(self.f.fake.at);view=coverage_view(c)
        self.assertEqual(view['current_revision_rows_not_assessed'],1)
        self.assertEqual(c['unassessed_reason_counts'],{'claim_for_prior_revision':1})
        self.assertIsNone(c['samples'][0]['current_company_label']);self.assertEqual(self.f.fake.posts,0)
        self.assertEqual(self.f.decisions()[0][2]['reason'],'revision_superseded')
    def test_same_revision_different_analyzer_is_not_counted_as_current_assessment(self):
        self.f.news();self.f.fake.advance();self.f.tick();m=self.repo.marker
        old=self.f.key;new='c'*64
        rows=self.repo.connection.execute(f'SELECT provider,article_id,revision_hash,engine,input_sha256,analyzed_at,result_json,result_sha256 FROM news_analyses WHERE analyzer_key={m}',(old,)).fetchall()
        for provider,article,rev,engine,inp,at,raw,sha in rows:
            self.repo.connection.execute(f'INSERT INTO news_analyses VALUES ({",".join([m]*9)})',(provider,article,rev,new,engine,inp,at,raw,sha))
        self.repo.connection.commit();self.f.worker.key=new
        c=self.f.worker.coverage(self.f.fake.at)
        self.assertEqual(c['claims_for_different_analysis'],1);self.assertEqual(c['claims_for_prior_revision'],0)
        self.assertEqual(c['current_analysis_claim_rows'],0);self.assertEqual(len(self.f.decisions()),1)
    def test_current_analysis_claim_does_not_imply_trade(self):
        self.f.news();self.f.fake.advance();self.f.fake.open=False;self.f.tick()
        c=self.f.worker.coverage(self.f.fake.at)
        self.assertTrue(coverage_view(c)['current_revision_assessment_complete']);self.assertEqual(self.f.fake.posts,0)
        self.assertFalse(c['executable_opportunity_validated'])
    def test_future_current_analysis_is_excluded_at_cutoff(self):
        self.f.news();self.f.fake.advance();self.f.tick();cutoff=self.f.fake.at;revise(self.f)
        c=self.f.worker.coverage(cutoff);self.assertEqual(c['available_analyses'],0)
        self.assertEqual(c['claims_for_prior_revision'],0)
    def test_legacy_zero_unclaimed_does_not_prove_current_revision_coverage(self):
        c={'format':'R28','available_analyses':2,'company_rows':2,'unassessed_company_rows':0,'claims_for_prior_revision':1,'samples':[]}
        v=coverage_view(c);self.assertIsNone(v['current_revision_rows_not_assessed'])
        self.assertEqual(v['current_revision_rows_not_assessed_at_least'],1);self.assertFalse(v['current_revision_assessment_complete'])
    def test_legacy_no_known_mismatch_still_not_full_analysis_match_claim(self):
        v=coverage_view({'format':'R28','available_analyses':1,'company_rows':1,'unassessed_company_rows':0,'claims_for_prior_revision':0})
        self.assertIsNone(v['current_revision_assessment_complete']);self.assertFalse(v['details_recorded'])
    def test_inconsistent_new_coverage_totals_rejected(self):
        self.f.news();self.f.fake.advance();c=self.f.worker.coverage(self.f.fake.at);c['claims_for_prior_revision']=1
        with self.assertRaises(StorageError):coverage_view(c)
    def test_legacy_lower_bound_exceeding_total_rejected(self):
        with self.assertRaises(StorageError):coverage_view({'format':'R28','available_analyses':1,'company_rows':1,'unassessed_company_rows':1,'claims_for_prior_revision':1})


class WarningHistoryTests(unittest.TestCase):
    def test_saved_warning_patterns_reinterpreted_without_mutation(self):
        fixtures=json.loads((Path(__file__).parent/'fixtures/report_warning_history.json').read_text())
        original=deepcopy(fixtures)
        opened,closed=fixtures
        health=price_health(opened['research'],opened['observation'])
        self.assertEqual(health['warning_history']['cycles_with_clock_flags'],4)
        self.assertEqual(health['warning_history']['cycles_with_gaps'],1)
        self.assertEqual(health['status'],'recovered_with_recorded_warnings')
        self.assertEqual(coverage_view(opened['coverage'])['current_revision_rows_not_assessed_at_least'],1)
        self.assertEqual(opened['timing']['stages']['portfolio_capture']['samples'],11);self.assertEqual(opened['new_captures'],3)
        self.assertEqual(price_health(closed['research'],closed['observation'])['status'],'missing_minute_bars')
        self.assertEqual(closed['timing']['stages']['portfolio_capture']['samples'],9);self.assertEqual(closed['new_captures'],3)
        timing_view(opened['timing']);self.assertEqual(fixtures,original)
    def test_supplied_cycle_pattern_retains_recovered_warnings(self):
        research=recovered_cycles();r=price_health(research,research['cycles'][-1]['paper'])
        self.assertEqual(r['warning_history']['cycles_with_any_warning'],4);self.assertEqual(r['warning_history']['cycles_with_gaps'],1)
        self.assertEqual(r['current_status'],'recorded_minute_data_present');self.assertEqual(r['status'],'recovered_with_recorded_warnings')
    def test_warning_samples_bounded_without_truncating_counts(self):
        r=recovered_cycles();r['cycles']=[deepcopy(r['cycles'][2]) for _ in range(25)]
        h=price_warning_history(r);self.assertEqual(h['cycles_with_clock_flags'],25)
        self.assertEqual(len(h['samples']),20);self.assertTrue(h['samples_truncated']);self.assertEqual(validate_warning_history(h),h)
    def test_missing_history_and_partial_fields_remain_unknown(self):
        self.assertIsNone(price_warning_history({})['warning_recorded'])
        h=price_warning_history({'cycles':[{'paper':{'price_gap_symbols':[]}}]})
        self.assertIsNone(h['cycles_with_clock_flags']);self.assertIsNone(h['warning_recorded']);self.assertFalse(h['whole_session_history_available'])
        self.assertEqual(validate_warning_history(h),h)
    def test_fallback_latest_observation_not_claimed_whole_session(self):
        h=price_warning_history({}, {'price_clock_flags':[],'price_gap_symbols':[]})
        self.assertFalse(h['whole_session_history_available']);self.assertIsNone(h['cycles_in_saved_history'])
    def test_original_cycle_indices_preserved_when_older_cycles_have_no_paper_metadata(self):
        r={'cycles':[{}, {'paper':{'price_clock_flags':['MSFT'],'price_gap_symbols':[]}}]}
        self.assertEqual(price_warning_history(r)['samples'][0]['cycle'],2)
    def test_corrupt_saved_summary_rejected_even_when_cycles_present(self):
        r=recovered_cycles();h=price_warning_history(r);h['cycles_with_any_warning']=0
        with self.assertRaises(StorageError):price_health(r,r['cycles'][-1]['paper'],h)
    def test_warning_symbols_are_union_not_incident_count(self):
        h=price_warning_history(recovered_cycles());self.assertEqual(len(h['clock_symbols']),4)
        self.assertTrue(h['warning_cycles_are_not_independent_incidents'])
    def test_cycle_bound_rejected(self):
        with self.assertRaises(StorageError):price_warning_history({'cycles':[{}]*257})
    def test_final_healthy_snapshot_does_not_hide_old_clock_flag(self):
        r=recovered_cycles();h=price_health(r,r['cycles'][-1]['paper']);self.assertTrue(h['acceptance_incomplete_from_recorded_price_evidence'])
        self.assertIn('price_clock_issue_recorded_during_session',h['reason_codes'])


class SavedAndWrapperTests(unittest.TestCase):
    def test_tracker_retains_warning_before_research_attaches_last_hook_result(self):
        research=recovered_cycles();tracker=price_warning_history(research)
        research['cycles']=[research['cycles'][-1]]
        health=price_health(research,research['cycles'][-1]['paper'],tracker)
        self.assertEqual(health['warning_history']['cycles_with_clock_flags'],4)
        self.assertEqual(health['status'],'recovered_with_recorded_warnings')
    def test_r28_timing_and_raw_digest_survive_offline_compatibility_view(self):
        repo=Repository('sqlite:///:memory:');self.addCleanup(repo.close)
        f=WorkerFixture(repo,'r29-legacy',103);self.addCleanup(f.worker.finish)
        legacy={'format':'R28','stages':{'portfolio_capture':{'samples':11,'median_seconds':0,'p95_seconds':3.244,'invalid_or_negative_samples':0}}}
        c={'format':'R28','available_analyses':2,'company_rows':2,'unassessed_company_rows':0,'claims_for_prior_revision':1}
        save_run(repo,f.worker,{'status':'completed','paper':f.worker.summary(),'research_session':recovered_cycles(),
            'decision_coverage':c,'timing':legacy})
        before=repo.connection.total_changes;first=report_session(repo,f.worker.session_id);r=latency_report(repo,f.worker.session_id)
        self.assertEqual(r['stored_observation_sha256'],first['stored_observation_sha256'])
        self.assertEqual(repo.connection.total_changes,before);self.assertEqual(first['timing'],legacy)
        self.assertNotIn('portfolio_capture',r['local_stage_durations']['stages'])
        self.assertIn('gerçek kayıt süresi geçmişten yeniden hesaplanamadı',render_latency(r))
        self.assertIn('en az 1 şirket satırı incelenmemiş',render_report(first))
        self.assertEqual(f.fake.posts,0)
    def test_new_private_cli_needs_no_env_model_or_provider(self):
        with patch('microbroker.cli.Settings.from_env',side_effect=AssertionError('No env')),patch('microbroker.cli.PaperAPI',side_effect=AssertionError('No API')),contextlib.redirect_stdout(io.StringIO()) as out:
            self.assertEqual(main(['verify-report-evidence']),0)
        self.assertEqual(len(json.loads(out.getvalue())['checks']),15)
    def test_warning_tracker_persists_when_research_raises_after_recovery(self):
        with TemporaryDirectory() as folder:
            settings=Settings(database_url='sqlite:///'+str(Path(folder)/'db'));f=ValuedFakePaper('r29-interrupted')
            def runner(s,r,e,**kw):
                for flags in (['MSFT'],[]):
                    f.at=utc_add(f.at,1)
                    observation={'status':'processed','observed_at':f.at,'price_clock_flags':flags,'price_gap_symbols':[]}
                    with patch('microbroker.execution.paper_worker.PaperWorker.tick',return_value=observation):
                        kw['_cycle_hook'](None,{'news_polls':1},None)
                raise RuntimeError('CONTROLLED_FAILURE')
            with self.assertRaises(RuntimeError):
                run_paper_session(settings,SessionRequest(('MSFT',),duration=60),FixtureEngine(),api_factory=lambda _:f.client,session_runner=runner,_fixture=True)
            repo=Repository(settings.database_url);self.addCleanup(repo.close)
            report=report_session(repo);history=report['price_health']['warning_history']
            self.assertEqual(history['cycles_with_clock_flags'],1);self.assertEqual(history['cycles_with_price_fields'],2)
            self.assertEqual(report['assessment'],'session_failed_or_interrupted');self.assertEqual(f.posts,0)
    def test_private_checks_support_strict_postgres_implicit_reads(self):
        repo=Repository('sqlite:///:memory:');self.addCleanup(repo.close);base=repo.connection
        class Shim:
            @property
            def info(self):return SimpleNamespace(transaction_status=2 if base.in_transaction else 0)
            def __getattr__(self,n):return getattr(base,n)
            def execute(self,q,p=()):
                q=q.replace('%s','?').replace(' FOR UPDATE','')
                if q.startswith('SET TRANSACTION'):
                    if base.in_transaction:raise StorageError('Nested implicit transaction')
                    return base.execute('BEGIN')
                if q.lstrip().startswith('SELECT') and not base.in_transaction:base.execute('BEGIN')
                return base.execute(q,p)
        repo.connection=Shim();repo.postgres=True;repo.marker='%s'
        self.assertEqual(len(evidence_checks(repo)),15)
