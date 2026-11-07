"""R30 deterministic outcome, revision, cutoff, benchmark and no-order regressions."""
from copy import deepcopy
import contextlib
from datetime import datetime,timedelta,timezone
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from microbroker.cli import main
from microbroker.config import Settings
from microbroker.data.event_study import event_study,session_study,render_study
from microbroker.data.event_study_calculation import calculate,event_rows,regular_minute,nanos,timestamp,MINUTE,POLICY
from microbroker.data.event_study_verification import study_fixture,event_checks,verify_event_study,add_news,START,END
from microbroker.data.event_collection import EventStudySessionRequest,run_event_collection
from microbroker.data.session import SessionRequest
from microbroker.data.storage.repository import Repository,StorageError
from microbroker.data.models import DataValidationError
from microbroker.data.analysis_replay import replay_analysis,create_analysis_replay
from microbroker.data.alignment import read_bars_snapshot,alignment_fixture
from microbroker.data.news_replay import NewsReplayRequest,create_news_replay
from microbroker.data.research import encode
from microbroker.data.session_price_verification import empty_stats
from microbroker.data.stream_models import normalize_event
from microbroker.execution.strategy_verification import FixtureEngine
from microbroker.execution.paper_worker_verification import WorkerFixture
from microbroker.execution.paper_monitor import save_run
from microbroker.data.news import normalize_news
from microbroker.intelligence.policy import digest,analyzer_metadata


class StudyTests(unittest.TestCase):
    def test_legacy_raw_label_not_treated_as_guarded_company_decision(self):
        records=deepcopy(self.records);records[0]['analysis']['result']['analysis_format']='R8'
        rows,groups,_=self.evaluate(records=records)
        self.assertEqual(rows[0]['status'],'legacy_analysis_not_supported')
        self.assertEqual(groups['positive']['1']['asset']['n'],0)
    def test_return_math_ignores_callers_decimal_context(self):
        from decimal import localcontext,ROUND_UP
        with localcontext() as context:
            context.prec=4;context.rounding=ROUND_UP
            report=event_study(self.repo,self.aid,self.bid)
        self.assertEqual(report,self.report)
    def setUp(self):
        self.repo=Repository('sqlite:///:memory:');self.addCleanup(self.repo.close)
        self.aid,self.bid=study_fixture(self.repo);self.report=event_study(self.repo,self.aid,self.bid)
        self.records,_,_=replay_analysis(self.repo,self.aid,_include_history=True)
        self.bars,_,_=read_bars_snapshot(self.repo,self.bid)
    def evaluate(self,records=None,bars=None,source=None):
        return calculate(records if records is not None else self.records,bars if bars is not None else self.bars,source or self.report['source'])
    def refresh(self):
        key=self.report['source']['analyzer_key']
        news=create_news_replay(self.repo,NewsReplayRequest(END,('MSFT',),first_seen_since=START))
        self.aid=create_analysis_replay(self.repo,news['replay_id'],END,key,'rules')['replay_id']
        return event_study(self.repo,self.aid,self.bid)
    def engine(self):
        engine=FixtureEngine();engine.metadata={**engine.metadata,'version':'r30-controlled-88400000'};return engine
    def mutate_price_time(self,symbol,minute,delay_ns):
        bars=deepcopy(self.bars)
        for b in bars:
            if b['symbol']==symbol and nanos(b['timestamp'])==nanos(START)+minute*MINUTE:
                b['receipt']['received_at']=timestamp(nanos(START)+(minute+1)*MINUTE+delay_ns)
        return sorted(bars,key=lambda b:b['receipt']['received_at'])
    def test_private_checks(self):self.assertEqual(len(verify_event_study()['checks']),19)
    def test_all_four_horizons_and_neutral_group(self):
        self.assertEqual([o['horizon_minutes'] for o in self.report['samples'][0]['outcomes']],[1,5,15,60])
        self.assertEqual(self.report['observed_absolute_outcomes'],12);self.assertEqual(self.report['benchmark_matched_outcomes'],12)
        self.assertEqual(self.report['groups']['neutral']['60']['asset']['n'],1)
    def test_reference_close_is_not_preanalysis_price(self):
        r=self.report['samples'][0];self.assertEqual(r['baseline']['close'],'101')
        self.assertGreaterEqual(r['baseline']['bar_start'],r['feature_available_at'])
        self.assertEqual(r['feature_to_reference_completion_seconds'],110)
    def test_market_difference_is_not_beta_alpha(self):
        o=self.report['samples'][0]['outcomes'][0]
        self.assertEqual(o['market_adjusted_return_bps'],'49.25865721')
        self.assertFalse(self.report['beta_estimated']);self.assertIn('beta_assumed_one',self.report['policy']['market_adjustment'])
    def test_spy_endpoints_have_identical_bar_times(self):
        r=self.report['samples'][0]
        for o in r['outcomes']:
            self.assertEqual(o['spy_baseline']['bar_start'],r['baseline']['bar_start'])
            self.assertEqual(o['spy_price']['bar_start'],o['price']['bar_start'])
    def test_negative_direction_is_not_short_fill_or_net_profit(self):
        r=self.report['samples'][1]['outcomes'][0]
        self.assertTrue(r['asset_return_bps'].startswith('97.'))
        self.assertTrue(r['directional_market_adjusted_bps'].startswith('-'))
        self.assertEqual(self.report['orders_sent'],0);self.assertIsNone(self.report['net_profit'])
        self.assertIn('short işlem kârı değildir',render_study(self.report))
    def test_withheld_not_neutral(self):
        self.assertEqual(self.report['event_status_counts']['company_decision_unavailable_or_withheld'],1)
        self.assertEqual(self.report['groups']['neutral']['1']['article_company_rows'],1)
    def test_missing_company_does_not_use_document_label(self):
        records=deepcopy(self.records);records[0]['analysis']['result']['entity_sentiments']={}
        rows,groups,_=self.evaluate(records=records)
        self.assertEqual(rows[0]['status'],'company_decision_unavailable_or_withheld')
        self.assertEqual(groups['positive']['1']['asset']['n'],0)
    def test_missing_analysis_is_explicit_not_zero(self):
        records=deepcopy(self.records);records[0]['analysis']=None
        rows,_,_=self.evaluate(records=records)
        self.assertEqual(rows[0]['status'],'analysis_unavailable');self.assertFalse(rows[0]['outcomes'])
    def test_future_revision_never_replaces_original_event_label(self):
        add_news(self.repo,self.engine(),88400000,'Microsoft shares fell.',timestamp(nanos(START)+10_000_000_000),
            updated=timestamp(nanos(START)+20_000_000_000))
        r=self.refresh();rows=[x for x in r['samples'] if x['article_id']==88400000]
        self.assertEqual([x['status'] for x in rows],['eligible','later_revision_not_independent_event'])
        self.assertEqual(rows[0]['company_label'],'positive')
        self.assertEqual(r['eligible_article_company_events'],3)
    def test_revision_superseded_before_analysis_completion_is_not_historical_signal(self):
        add_news(self.repo,self.engine(),88400000,'Microsoft shares fell.',timestamp(nanos(START)+10_000_000_000),
            updated=timestamp(nanos(START)+20_000_000_000))
        self.repo.connection.execute('UPDATE news_analyses SET analyzed_at=? WHERE article_id=? AND analyzed_at=?',
            (timestamp(nanos(START)+MINUTE),88400000,timestamp(nanos(START)+10_000_000_000)))
        self.repo.connection.commit();r=self.refresh();rows=[x for x in r['samples'] if x['article_id']==88400000]
        self.assertEqual([x['status'] for x in rows],['eligible','revision_not_current_at_feature'])
        self.assertEqual(rows[0]['company_label'],'negative')
    def test_first_withheld_revision_does_not_select_later_clear_label_for_same_event(self):
        at=timestamp(nanos(START)+10*MINUTE)
        add_news(self.repo,self.engine(),88400009,'Microsoft profits rose. Microsoft shares fell.',at)
        add_news(self.repo,self.engine(),88400009,'Microsoft profits rose.',at,updated=timestamp(nanos(at)+MINUTE))
        rows=[x for x in self.refresh()['samples'] if x['article_id']==88400009]
        self.assertEqual([x['status'] for x in rows],['company_decision_unavailable_or_withheld','later_revision_not_independent_event'])
    def test_end_cutoff_excludes_later_analysis(self):
        r=event_study(self.repo,self.aid,self.bid,end=timestamp(nanos(START)+3*MINUTE))
        self.assertEqual(r['eligible_article_company_events'],2)
    def test_right_censoring_not_zero_return(self):
        r=event_study(self.repo,self.aid,self.bid,end=timestamp(nanos(START)+10*MINUTE))
        self.assertIsNone(r['groups']['positive']['60']['asset']['mean_bps'])
        self.assertEqual(r['groups']['positive']['60']['status_counts'],{'right_censored_by_snapshot_end':1})
    def test_future_end_beyond_snapshot_rejected(self):
        with self.assertRaises(DataValidationError):event_study(self.repo,self.aid,self.bid,end='2026-02-03T00:00:00Z')
    def test_end_before_receipt_start_rejected(self):
        with self.assertRaises(DataValidationError):event_study(self.repo,self.aid,self.bid,end='2026-02-01T00:00:00Z')
    def test_missing_outcome_endpoint_not_interpolated(self):
        bars=[b for b in self.bars if not (b['symbol']=='MSFT' and nanos(b['timestamp'])==nanos(START)+2*MINUTE)]
        rows,_,_=self.evaluate(bars=bars);o=rows[0]['outcomes'][0]
        self.assertIsNone(o['asset_return_bps']);self.assertEqual(o['status'],'asset_window_minute_not_recorded_by_cutoff')
    def test_missing_intermediate_minute_does_not_erase_shorter_complete_horizon(self):
        bars=[b for b in self.bars if not (b['symbol']=='MSFT' and nanos(b['timestamp'])==nanos(START)+3*MINUTE)]
        rows,_,_=self.evaluate(bars=bars)
        self.assertEqual(rows[0]['outcomes'][0]['status'],'observed')
        self.assertIsNone(rows[0]['outcomes'][1]['asset_return_bps'])
    def test_clock_early_by_one_nanosecond_rejected(self):
        rows,_,counts=self.evaluate(bars=self.mutate_price_time('MSFT',1,-1))
        self.assertEqual(counts['receipt_before_completion'],1)
        self.assertEqual(rows[0]['outcomes'][0]['status'],'asset_reference_receipt_before_completion')
    def test_delay_boundary_inclusive_and_beyond_rejected(self):
        for delay,expected in [(120_000_000_000,'observed'),(120_000_000_001,'asset_reference_receipt_delay_exceeds_120_seconds')]:
            rows,_,_=self.evaluate(bars=self.mutate_price_time('MSFT',1,delay))
            self.assertEqual(rows[0]['outcomes'][1]['status'],expected)
    def test_reference_not_known_until_after_short_outcome(self):
        rows,_,_=self.evaluate(bars=self.mutate_price_time('MSFT',1,90_000_000_000))
        self.assertEqual(rows[0]['outcomes'][0]['status'],'asset_reference_observed_after_target_completion')
        self.assertEqual(rows[0]['outcomes'][1]['status'],'observed')
    def test_late_spy_reference_withholds_pair_not_asset_return(self):
        rows,_,_=self.evaluate(bars=self.mutate_price_time('SPY',1,90_000_000_000))
        o=rows[0]['outcomes'][0];self.assertEqual(o['status'],'observed')
        self.assertIsNone(o['market_adjusted_return_bps']);self.assertEqual(o['benchmark_status'],'paired_reference_observed_after_target_completion')
    def test_absent_spy_receipt_no_asset_fallback_for_market_adjustment(self):
        rows,_,_=self.evaluate(bars=[b for b in self.bars if b['symbol']!='SPY'])
        o=rows[0]['outcomes'][0];self.assertEqual(o['status'],'observed');self.assertIsNone(o['spy_return_bps'])
    def test_actual_snapshot_without_spy_reports_absolute_only(self):
        aid,bid=study_fixture(self.repo,base=88401000,spy=False)
        r=event_study(self.repo,aid,bid)
        self.assertEqual(r['assessment'],'absolute_returns_only');self.assertFalse(r['source']['benchmark_in_snapshot'])
        self.assertIn('SPY_missing_absolute_returns_only',r['warning_codes'])
    def test_spy_missing_interior_minute_withholds_only_benchmark(self):
        bars=[b for b in self.bars if not (b['symbol']=='SPY' and nanos(b['timestamp'])==nanos(START)+3*MINUTE)]
        rows,_,_=self.evaluate(bars=bars);o=rows[0]['outcomes'][1]
        self.assertEqual(o['status'],'observed');self.assertIsNone(o['market_adjusted_return_bps'])
    def test_updated_bars_do_not_replace_first_observed_base(self):
        bars=deepcopy(self.bars);update=deepcopy(bars[2]);update['receipt']['event_type']='u';update['close']='999'
        bars.insert(3,update);rows,_,counts=self.evaluate(bars=bars)
        self.assertEqual(rows[0]['baseline']['close'],'101');self.assertEqual(counts['updated_receipts_excluded'],1)
    def test_later_duplicate_base_does_not_change_reference(self):
        bars=deepcopy(self.bars);duplicate=deepcopy(bars[2]);duplicate['close']='999';bars.insert(3,duplicate)
        rows,_,counts=self.evaluate(bars=bars)
        self.assertEqual(rows[0]['baseline']['close'],'101');self.assertEqual(counts['later_base_receipts_excluded'],1)
    def test_nondecreasing_receipt_clock_required(self):
        bars=list(reversed(self.bars))
        with self.assertRaises(StorageError):self.evaluate(bars=bars)
    def test_limit_is_display_only(self):
        small=event_study(self.repo,self.aid,self.bid,limit=1)
        self.assertEqual(small['groups'],self.report['groups']);self.assertEqual(small['study_sha256'],self.report['study_sha256'])
        self.assertTrue(small['samples_truncated']);self.assertEqual(len(small['samples']),1)
    def test_report_policy_is_detached_from_global(self):
        self.report['policy']['horizons_minutes'].append(999)
        self.assertEqual(POLICY['horizons_minutes'],[1,5,15,60])
    def test_readonly_does_not_touch_ledger_or_broker_tables(self):
        before=self.repo.connection.total_changes
        again=event_study(self.repo,self.aid,self.bid)
        self.assertEqual(again,self.report);self.assertEqual(self.repo.connection.total_changes,before)
        for table in ('broker_paper_intents','paper_news_decisions','paper_accounts'):
            self.assertEqual(self.repo.connection.execute('SELECT COUNT(*) FROM '+table).fetchone()[0],0)
        self.repo.connection.commit()
    def test_price_payload_tampering_rejected(self):
        self.repo.connection.execute('UPDATE replay_items SET record_json=? WHERE replay_id=? AND ordinal=0',('{}',self.bid));self.repo.connection.commit()
        with self.assertRaises(StorageError):event_study(self.repo,self.aid,self.bid)
    def test_analysis_payload_tampering_rejected(self):
        self.repo.connection.execute('UPDATE replay_items SET record_json=? WHERE replay_id=? AND ordinal=0',('{}',self.aid));self.repo.connection.commit()
        with self.assertRaises(StorageError):event_study(self.repo,self.aid,self.bid)
    def test_legacy_current_bar_snapshot_rejected(self):
        aid,bid=alignment_fixture(self.repo)
        with self.assertRaises(DataValidationError):event_study(self.repo,aid,bid)
    def test_text_explains_horizon_and_missing_data(self):
        text=render_study(self.report)
        for phrase in ('referans kapanışından','eski fiyat taşınmadı','bağımsız örnek','emir göndermez','beta tahmini yoktur'):
            self.assertIn(phrase,text)
    def test_session_lookup_verifies_frozen_stream_identity(self):
        session={'session_id':'r30-session','status':'completed','stream':{'run_id':self.report['source']['stream_run_id']},
            'snapshots':{'analysis_replay_id':self.aid,'bars_replay_id':self.bid}}
        request={k:self.report['source'][k] for k in ('analyzer_key','engine')}
        self.repo.connection.execute('INSERT INTO research_sessions VALUES (?,?,?,?,?,?)',('r30-session',START,END,'completed',encode(request),encode(session)));self.repo.connection.commit()
        r=session_study(self.repo,'r30-session');self.assertEqual(r['study_sha256'],self.report['study_sha256'])
        self.assertEqual(session_study(self.repo)['study_sha256'],r['study_sha256'])
        session['stream']['run_id']='another-stream'
        self.repo.connection.execute('UPDATE research_sessions SET result_json=?',(encode(session),));self.repo.connection.commit()
        with self.assertRaises(StorageError):session_study(self.repo,'r30-session')
    def test_paper_session_uuid_resolves_existing_research_sources(self):
        import uuid
        sid=str(uuid.uuid4());source=self.report['source']
        session={'session_id':sid,'status':'completed','stream':{'run_id':source['stream_run_id']},
            'snapshots':{'analysis_replay_id':self.aid,'bars_replay_id':self.bid}}
        request={k:source[k] for k in ('analyzer_key','engine')}
        self.repo.connection.execute('INSERT INTO research_sessions VALUES (?,?,?,?,?,?)',(sid,START,END,'completed',encode(request),encode(session)));self.repo.connection.commit()
        f=WorkerFixture(self.repo,'r30-paper-link',104);self.addCleanup(f.worker.finish)
        save_run(self.repo,f.worker,{'status':'completed','paper':f.worker.summary(),
            'research_session':{'session_id':sid,'paper_worker_session_id':f.worker.session_id}})
        result=session_study(self.repo,f.worker.session_id)
        self.assertEqual(result['study_sha256'],self.report['study_sha256'])
        self.assertEqual(result['session_lookup']['research_session_id'],sid)


class ClockAndCollectionTests(unittest.TestCase):
    def test_synthetic_demo_is_marked_and_exclusive_without_config(self):
        with TemporaryDirectory() as folder:
            target=Path(folder)/'demo'
            with patch('microbroker.cli.Settings.from_env',side_effect=AssertionError('No env')),contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(main(['demo-event-study','--output',str(target)]),0)
                self.assertIn('SENTETİK DEMO', (target/'report.txt').read_text())
                before=(target/'report.json').read_bytes()
                with contextlib.redirect_stderr(io.StringIO()):self.assertEqual(main(['demo-event-study','--output',str(target)]),2)
                self.assertEqual(before,(target/'report.json').read_bytes())
    def test_concurrent_collection_keeps_company_news_and_spy_price_scope_separate(self):
        ready=threading.Event();base=datetime.now(timezone.utc);requests=[]
        class Clock:
            value=0
            def __call__(self):return self.value
            def wait(self,seconds):self.value+=seconds
        clock=Clock()
        class News:
            pages=1
            def __init__(self,settings):pass
            def iter_news(self,request):
                requests.append(('news',request.symbols))
                yield normalize_news({'id':88407000,'headline':'Microsoft profits rose.','content':'Microsoft profits rose.',
                    'created_at':(base-timedelta(minutes=1)).isoformat(),'updated_at':(base-timedelta(minutes=1)).isoformat(),
                    'symbols':['MSFT'],'source':'original-synthetic','url':'https://example.com/r30-concurrent'})
        class Stream:
            def __init__(self,settings):pass
            def run(self,request,repository,on_event=None,stop_event=None):
                requests.append(('prices',request.symbols));stats=empty_stats();run=repository.begin_stream(request,stats)
                at=base.replace(second=0,microsecond=0)-timedelta(minutes=1)
                for symbol in request.symbols:
                    event=normalize_event({'T':'b','S':symbol,'t':at.isoformat(),'o':100,'h':102,'l':99,'c':101,'v':100,'n':5},request)
                    stats=repository.apply_stream_frame(run,[event],stats)
                    if on_event:on_event(event)
                ready.set();stop_event.wait(2)
                return {'run_id':run,'status':'completed',**stats}
        with TemporaryDirectory() as folder:
            settings=Settings(database_url='sqlite:///'+str(Path(folder)/'db'),api_key='FAKE',api_secret='FAKE')
            with patch('microbroker.cli.PaperAPI',side_effect=AssertionError('No order capability')):
                result=run_event_collection(settings,EventStudySessionRequest(('MSFT',),duration=60),FixtureEngine(),
                    stream_factory=Stream,news_factory=News,clock=clock,wait=clock.wait,
                    utc_now=lambda:base+timedelta(seconds=clock.value),_allow_sqlite_fixture=True,
                    _warmup_wait=lambda _:self.assertTrue(ready.wait(2)))
            self.assertEqual(result['status'],'completed')
            self.assertTrue(all(set(symbols)=={'MSFT'} for kind,symbols in requests if kind=='news'))
            self.assertIn(('prices',('MSFT','SPY')),requests)
            repo=Repository(settings.database_url)
            try:
                report=session_study(repo,result['session_id']);self.assertTrue(report['source']['benchmark_in_snapshot'])
                self.assertEqual(repo.connection.execute('SELECT COUNT(*) FROM broker_paper_intents').fetchone()[0],0)
                repo.connection.commit()
            finally:repo.close()
    def test_ny_dst_and_regular_boundaries(self):
        for at,expected in [('2026-01-05T14:30:00Z',True),('2026-06-01T13:30:00Z',True),
                ('2026-06-01T13:29:00Z',False),('2026-01-05T20:59:00Z',True),
                ('2026-01-05T21:00:00Z',False),('2026-01-03T15:00:00Z',False)]:
            self.assertEqual(regular_minute(nanos(at)),expected,at)
    def test_closed_clock_reference_not_shifted_to_next_open(self):
        repo=Repository('sqlite:///:memory:');self.addCleanup(repo.close);aid,bid=study_fixture(repo)
        records,_,_=replay_analysis(repo,aid,_include_history=True);bars,_,_=read_bars_snapshot(repo,bid)
        source=event_study(repo,aid,bid)['source'];source={**source,'start':'2026-02-02T00:00:00Z'}
        records[0]['analysis']['timestamp']='2026-02-02T14:00:10.000000000Z'
        # Keep the first revision visible at the earlier controlled feature time.
        records[0]['news']['timestamp']='2026-02-02T14:00:00.000000000Z'
        rows,_,_=calculate(records,bars,source)
        self.assertTrue(all(o['status']=='reference_outside_regular_clock_hours' for o in rows[0]['outcomes']))
    def test_regular_close_does_not_roll_outcome_into_next_session(self):
        repo=Repository('sqlite:///:memory:');self.addCleanup(repo.close);aid,bid=study_fixture(repo)
        records,_,_=replay_analysis(repo,aid,_include_history=True);bars,_,_=read_bars_snapshot(repo,bid)
        source=event_study(repo,aid,bid)['source'];source={**source,'end':'2026-02-02T22:00:00Z','event_cutoff':'2026-02-02T22:00:00Z'}
        records=records[:1];records[0]['analysis']['timestamp']='2026-02-02T20:58:10.000000000Z'
        rows,_,_=calculate(records,bars,source)
        self.assertEqual(rows[0]['outcomes'][0]['status'],'horizon_outside_same_regular_clock_day')
    def test_long_request_only_for_orderless_collection(self):
        r=EventStudySessionRequest(('MSFT','SPY','AAPL'),duration=10800)
        self.assertEqual(r.news_symbols,('AAPL','MSFT'));self.assertEqual(r.symbols,('AAPL','MSFT','SPY'))
        self.assertEqual(r.metadata()['purpose'],'event_study')
        with self.assertRaises(DataValidationError):SessionRequest(('MSFT',),duration=5400)
        with self.assertRaises(DataValidationError):EventStudySessionRequest(('MSFT',),duration=10801)
        with self.assertRaises(DataValidationError):EventStudySessionRequest(('SPY',))
    def test_collection_wrapper_keeps_failure_and_no_orders(self):
        r=EventStudySessionRequest(('MSFT',))
        with patch('microbroker.data.event_collection.run_session',return_value={'status':'failed','snapshots':{}}):
            result=run_event_collection(Settings(),r,FixtureEngine())
        self.assertEqual(result['status'],'failed');self.assertFalse(result['event_study_collection']['orders_enabled'])
    def test_event_bound_does_not_return_silently_truncated_stats(self):
        repo=Repository('sqlite:///:memory:');self.addCleanup(repo.close);aid,_=study_fixture(repo)
        records,_,_=replay_analysis(repo,aid,_include_history=True)
        with patch('microbroker.data.event_study_calculation.MAX_EVENTS',0),self.assertRaises(StorageError):
            event_rows(records,['MSFT'],START,END)
    def test_private_cli_does_not_load_env_model_or_broker(self):
        with patch('microbroker.cli.Settings.from_env',side_effect=AssertionError('No env')),patch('microbroker.cli.make_engine',side_effect=AssertionError('No model')),patch('microbroker.cli.PaperAPI',side_effect=AssertionError('No broker')),contextlib.redirect_stdout(io.StringIO()) as out:
            self.assertEqual(main(['verify-event-study']),0)
        self.assertEqual(len(json.loads(out.getvalue())['checks']),19)
    def test_live_cli_uses_market_data_collector_and_finbert_only(self):
        settings=Settings(database_url='postgresql://fake/db',api_key='FAKE',api_secret='FAKE')
        with patch('microbroker.cli.Settings.from_env',return_value=settings),patch('microbroker.cli.make_engine',return_value=FixtureEngine()) as engine,patch('microbroker.cli.PaperAPI',side_effect=AssertionError('No broker')),patch('microbroker.data.event_collection.run_event_collection',return_value={'status':'completed'}) as collect,contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['run-event-study-session','--symbols','MSFT']),0)
        self.assertEqual(engine.call_args.args[0],'finbert');self.assertEqual(collect.call_args.args[1].symbols,('MSFT','SPY'))
    def test_offline_cli_export_is_exclusive_and_has_no_network(self):
        with TemporaryDirectory() as directory:
            url='sqlite:///'+str(Path(directory)/'db');repo=Repository(url);aid,bid=study_fixture(repo);repo.close()
            output=Path(directory)/'study.json';args=['event-study','--analysis-replay-id',aid,'--bars-replay-id',bid,'--format','text','--limit','1','--output',str(output)]
            with patch('microbroker.cli.Settings.from_env',return_value=Settings(database_url=url)),patch('microbroker.cli.PaperAPI',side_effect=AssertionError('No API')),patch('microbroker.cli.make_engine',side_effect=AssertionError('No model')),contextlib.redirect_stdout(io.StringIO()) as out:
                self.assertEqual(main(args),0);self.assertIn('SPY farkı',out.getvalue())
                exported=json.loads(output.read_text());self.assertEqual(len(exported['samples']),1);self.assertEqual(len(exported['event_rows']),4)
                original=output.read_bytes()
                with contextlib.redirect_stderr(io.StringIO()):self.assertEqual(main(args),2)
                self.assertEqual(output.read_bytes(),original)
    def test_strict_postgres_protocol(self):
        repo=Repository('sqlite:///:memory:');self.addCleanup(repo.close);base=repo.connection
        class Shim:
            @property
            def info(self):return SimpleNamespace(transaction_status=2 if base.in_transaction else 0)
            def __getattr__(self,n):return getattr(base,n)
            def cursor(self,name=None):
                owner=self
                class Cursor:
                    def execute(self,q,p=()):self.inner=owner.execute(q,p);return self
                    def __getattr__(self,n):return getattr(self.inner,n)
                return Cursor()
            def execute(self,q,p=()):
                q=q.replace('%s','?').replace(' FOR UPDATE','')
                if q.startswith('SET TRANSACTION'):
                    if base.in_transaction:raise StorageError('Nested read transaction')
                    return base.execute('BEGIN')
                if q.lstrip().startswith('SELECT') and not base.in_transaction:base.execute('BEGIN')
                return base.execute(q,p)
        repo.connection=Shim();repo.postgres=True;repo.marker='%s'
        self.assertEqual(len(event_checks(repo)),19)
