"""Read-only news diagnostics, bounded offline reports and durable session observations."""
import contextlib
from copy import deepcopy
from datetime import datetime,timezone
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
from microbroker.data.models import DataValidationError
from microbroker.data.news import normalize_news
from microbroker.data.news_probe import inspect_news
from microbroker.data.adapters.alpaca import Response,MarketDataError
from microbroker.data.news import AlpacaNews
from microbroker.data.research import encode
from microbroker.data.storage.repository import Repository,StorageError
from microbroker.execution.paper_monitor import (save_run,report_session,render_report,configuration,parsed)
from microbroker.execution.paper_monitor_verification import verify_paper_monitor,monitor_checks
from microbroker.execution.paper_worker_verification import WorkerFixture
from microbroker.execution.paper_worker import run_paper_session,utc_add
from microbroker.execution import paper_broker as broker
from microbroker.execution.equity_history import capture_equity
from microbroker.execution.paper_broker_verification import FakePaper
from microbroker.execution.strategy_verification import FixtureEngine
from microbroker.data.session import SessionRequest

class MonitorTests(unittest.TestCase):
    def setUp(self):self.repo=Repository('sqlite:///:memory:');self.f=WorkerFixture(self.repo,'r24-unit',0)
    def tearDown(self):self.repo.close()
    def value(self,status='running',received=0,observation=None):
        return {'status':status,'paper':self.f.worker.summary(),
            'research_session':{'session_id':str(uuid.uuid4()),'news_polls':1,'received_articles':received,'new_versions':received,'inserted_analyses':received},
            'observation':observation or {}}
    def save(self,**kw):
        value=self.value(**kw);save_run(self.repo,self.f.worker,value);return value
    def test_private_monitor_checks(self):
        result=verify_paper_monitor();self.assertEqual(len(result['checks']),11);self.assertEqual(result['orders_sent_to_alpaca'],0)
    def test_latest_no_news_report_has_zero_orders(self):
        self.save(status='no_data');r=report_session(self.repo)
        self.assertEqual(r['assessment'],'no_news_returned');self.assertEqual(r['session_id'],self.f.worker.session_id);self.assertEqual(r['paper']['post_attempts'],0)
    def test_saved_configuration_is_immutable(self):
        self.save();self.f.worker.notional='11'
        with self.assertRaises(StorageError):self.save()
    def test_audit_clock_regression_is_rejected(self):
        self.save();self.f.fake.at=utc_add(self.f.fake.at,-1)
        with self.assertRaises(StorageError):self.save()
    def test_previously_corrupt_audit_cannot_be_silently_overwritten(self):
        self.save();self.repo.connection.execute("UPDATE paper_worker_runs SET result_json='{}'");self.repo.connection.commit()
        with self.assertRaises(StorageError):self.save()
    def test_audit_root_tamper_rejected(self):
        self.save();self.repo.connection.execute("UPDATE paper_worker_runs SET request_json='{}'");self.repo.connection.commit()
        with self.assertRaises(StorageError):report_session(self.repo)
    def test_no_record_does_not_invent_session(self):
        with self.assertRaises(DataValidationError):report_session(self.repo)
    def test_uuid_and_sample_limit_validation(self):
        for value in ('','../db','SELECT *',None):
            if value is None:continue
            with self.assertRaises(DataValidationError):report_session(self.repo,value)
        for value in (0,101,True):
            with self.assertRaises(DataValidationError):report_session(self.repo,limit=value)
    def test_saved_observation_tamper_preserves_decisions(self):
        f=self.f;f.news();f.ready();tick=f.tick();self.save(received=1,observation=tick)
        self.repo.connection.execute("UPDATE paper_worker_runs SET status='no_data'");self.repo.connection.commit()
        with self.assertRaises(StorageError):report_session(self.repo)
        self.assertEqual(f.fake.posts,1)
    def test_running_record_is_not_process_liveness_claim(self):
        self.save();r=report_session(self.repo);self.assertEqual(r['recorded_session_status'],'running');self.assertTrue(r['running_status_is_last_saved_observation_not_process_liveness'])
    def test_pending_reason_and_verified_context_visible(self):
        f=self.f;f.news();f.bar(0);f.fake.at='2026-01-05T15:01:10.000000000Z';tick=f.tick();self.save(received=1,observation=tick)
        r=report_session(self.repo);s=r['samples'][0]
        self.assertEqual(s['last_observed_pending_reason'],'awaiting_following_completed_bar');self.assertTrue(s['context_preview_verified']);self.assertIsNone(s['order'])
        self.assertIn('tamamlanmış dakika bekleniyor',render_report(r));self.assertEqual(f.fake.posts,0)
    def test_conflict_has_no_raw_positive_fallback_in_report(self):
        f=self.f;f.news(text='Microsoft profits rose. Microsoft shares fell.');f.ready();tick=f.tick();self.save(status='completed',received=1,observation=tick)
        r=report_session(self.repo);self.assertIsNone(r['samples'][0]['company_label']);self.assertEqual(r['samples'][0]['decision_reason'],'company_decision_withheld')
        self.assertIn('karar yok',render_report(r));self.assertEqual(f.fake.posts,0)
    def test_closed_market_explained(self):
        f=self.f;f.news();f.ready();f.fake.open=False;tick=f.tick();self.save(status='completed',received=1,observation=tick)
        self.assertIn('Piyasa kapalı',render_report(report_session(self.repo)))
    def test_partial_fill_does_not_appear_as_full_fill(self):
        f=self.f;f.news();f.ready();tick=f.tick();cid=next(iter(f.fake.orders));f.fake.update(cid,'partially_filled','0.04');broker.reconcile_account(self.repo,f.fake.client)
        self.save(received=1,observation=tick);r=report_session(self.repo)
        self.assertEqual(r['samples'][0]['order']['filled_qty'],'0.04');self.assertEqual(r['assessment'],'paper_orders_attempted')
        self.assertIn('Kaydedilmiş filled emir: 0',render_report(r))
    def test_later_reconciliation_updates_report_without_resubmission(self):
        f=self.f;f.news();f.ready();tick=f.tick();self.save(status='completed',received=1,observation=tick)
        old=report_session(self.repo);f.fill();broker.reconcile_account(self.repo,f.fake.client);new=report_session(self.repo)
        self.assertEqual(old['stored_observation_sha256'],new['stored_observation_sha256']);self.assertEqual(new['assessment'],'paper_fills_recorded');self.assertEqual(f.fake.posts,1)
    def test_incomplete_analysis_explained(self):
        self.save(status='completed',received=1,observation={'analyses_pending':1});self.assertEqual(report_session(self.repo)['assessment'],'analysis_not_available')
    def test_only_existing_news_explained(self):
        self.save(status='completed',received=5);self.assertEqual(report_session(self.repo)['assessment'],'no_new_session_decisions')
    def test_cleanup_error_not_hidden_as_no_news(self):
        v=self.value('no_data');v['worker_cleanup_error']={'stage':'worker_cleanup','exception_type':'PaperBrokerError'};save_run(self.repo,self.f.worker,v)
        self.assertEqual(report_session(self.repo)['assessment'],'session_cleanup_failed')
    def test_audit_write_failure_rolls_back_to_previous_observation(self):
        self.save();old=report_session(self.repo);base=self.repo.connection
        class Fail:
            def __getattr__(self,name):return getattr(base,name)
            def execute(self,q,p=()):
                r=base.execute(q,p)
                if q.startswith('INSERT INTO paper_worker_runs'):raise StorageError('controlled failure')
                return r
        self.repo.connection=Fail()
        with self.assertRaises(StorageError):self.save(status='completed')
        self.repo.connection=base;self.assertEqual(report_session(self.repo)['stored_observation_sha256'],old['stored_observation_sha256'])
    def test_legacy_report_both_ids_and_no_upgrade(self):
        f=self.f;capture_equity(self.repo,f.fake.client,session_id=f.worker.session_id);rid=str(uuid.uuid4())
        body={'session_id':rid,'paper_worker_session_id':f.worker.session_id,'status':'no_data','started_at':f.worker.started,'finished_at':f.fake.at,'received_articles':0,'news_polls':18}
        self.repo.connection.execute('INSERT INTO research_sessions VALUES (?,?,?,?,?,?)',(rid,f.worker.started,f.fake.at,'no_data',encode({'symbols':['MSFT'],'feed':'iex'}),encode(body)));self.repo.connection.commit()
        a=report_session(self.repo,rid);b=report_session(self.repo,f.worker.session_id)
        self.assertEqual(a,b);self.assertFalse(a['audit_integrity_verified']);self.assertEqual(self.repo.connection.execute('SELECT COUNT(*) FROM paper_worker_runs').fetchone()[0],0)
    def test_r24_research_id_resolves_sealed_worker_report(self):
        v=self.save();rid=v['research_session']['session_id'];body={**v['research_session'],'paper_worker_session_id':self.f.worker.session_id,'status':'running','started_at':self.f.worker.started}
        self.repo.connection.execute('INSERT INTO research_sessions VALUES (?,?,?,?,?,?)',(rid,self.f.worker.started,None,'running','{}',encode(body)));self.repo.connection.commit()
        self.assertTrue(report_session(self.repo,rid)['audit_integrity_verified'])
    def test_legacy_without_account_evidence_rejected(self):
        rid=str(uuid.uuid4());body={'session_id':rid,'paper_worker_session_id':str(uuid.uuid4()),'status':'no_data','started_at':self.f.worker.started}
        self.repo.connection.execute('INSERT INTO research_sessions VALUES (?,?,?,?,?,?)',(rid,self.f.worker.started,None,'no_data','{}',encode(body)));self.repo.connection.commit()
        with self.assertRaises(StorageError):report_session(self.repo,rid)
    def test_oversized_audit_rejected(self):
        with self.assertRaises(StorageError):parsed(' '* (2*1024*1024+1))
    def test_report_cli_is_offline_and_format_text(self):
        with tempfile.TemporaryDirectory() as d:
            url='sqlite:///'+str(Path(d)/'db');repo=Repository(url);f=WorkerFixture(repo,'r24-cli',0)
            save_run(repo,f.worker,{'status':'no_data','paper':f.worker.summary(),'research_session':{'received_articles':0,'news_polls':1}});repo.close()
            with patch('microbroker.cli.Settings.from_env',return_value=Settings(database_url=url)),patch('microbroker.cli.PaperAPI',side_effect=AssertionError('No network')),contextlib.redirect_stdout(io.StringIO()) as out:
                self.assertEqual(main(['paper-session-report','--format','text']),0)
            self.assertIn('Haber sorguları boş döndü',out.getvalue())
    def test_readable_demo_four_decisions_and_no_real_orders(self):
        from microbroker.execution.paper_monitor_verification import demo_paper_monitor
        with tempfile.TemporaryDirectory() as d:
            folder=Path(d)/'demo';r=demo_paper_monitor(folder)
            self.assertEqual(r['orders_sent_to_alpaca'],0);self.assertEqual(r['fake_filled_orders'],2);self.assertEqual(r['wait_decisions'],2)
            text=(folder/'report.txt').read_text();self.assertIn('SENTETİK DEMO',text);self.assertIn('Kayıtlı şirket kararı: negatif',text)
            report=json.loads((folder/'report.json').read_text());self.assertEqual(report['paper']['post_attempts'],2)
            with self.assertRaises(DataValidationError):demo_paper_monitor(folder)
    def test_readable_demo_cli_without_env(self):
        with tempfile.TemporaryDirectory() as d,patch('microbroker.cli.Settings.from_env',side_effect=AssertionError('No user database')),contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['demo-paper-monitor','--output',str(Path(d)/'demo')]),0)
    def test_private_cli_does_not_read_credentials(self):
        with patch('microbroker.cli.Settings.from_env',side_effect=AssertionError('No credentials')),contextlib.redirect_stdout(io.StringIO()):self.assertEqual(main(['verify-paper-monitor']),0)
    def test_monitor_checks_under_pg_implicit_transactions(self):
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
        try:self.assertEqual(len(monitor_checks(repo)),11)
        finally:repo.close()

class ProbeTests(unittest.TestCase):
    settings=Settings(api_key='FAKE',api_secret='FAKE')
    now=datetime(2026,1,5,16,tzinfo=timezone.utc)
    def probe(self,payload):
        def adapter(s):return AlpacaNews(s,transport=lambda *a:Response(200,{},json.dumps(payload).encode()))
        return inspect_news(self.settings,('MSFT',),60,adapter_factory=adapter,now=self.now)
    def test_empty_200_is_described_as_provider_response(self):
        r=self.probe({'news':[]});self.assertEqual(r['response'],'empty_response');self.assertEqual(r['received_articles'],0)
        self.assertFalse(r['database_modified']);self.assertFalse(r['arrival_times_recorded']);self.assertEqual(r['orders_sent'],0)
    def test_provider_headlines_and_coverage_without_model(self):
        r=self.probe({'news':[{'id':1,'headline':'Microsoft update','symbols':['MSFT'],'created_at':'2026-01-05T15:20:00Z','updated_at':'2026-01-05T15:20:00Z'}]})
        self.assertEqual(r['samples'][0]['headline'],'Microsoft update');self.assertFalse(r['samples'][0]['has_content']);self.assertEqual(r['query_start'],'2026-01-05T15:00:00.000000000Z')
    def test_probe_displays_latest_headlines_and_recent_publication_count(self):
        rows=[{'id':i+1,'headline':f'Microsoft update {i}','symbols':['MSFT'],'created_at':f'2026-01-05T15:{i*7:02}:00Z','updated_at':f'2026-01-05T15:{i*7:02}:00Z'} for i in range(8)]
        r=self.probe({'news':rows});self.assertEqual([s['article_id'] for s in r['samples']],[4,5,6,7,8])
        self.assertEqual(r['versions_published_within_last_15_minutes'],1)
    def test_probe_response_count_bound_is_not_partial_success(self):
        class Oversized:
            pages=1
            def __init__(self,s):pass
            def iter_news(self,r):
                article=normalize_news({'id':1,'headline':'Microsoft update','symbols':['MSFT'],'created_at':'2026-01-05T15:59:00Z','updated_at':'2026-01-05T15:59:00Z'})
                for _ in range(251):yield article
        with self.assertRaises(DataValidationError):inspect_news(self.settings,adapter_factory=Oversized,now=self.now)
    def test_probe_invalid_bounds_or_symbols_no_request(self):
        for n in (0,1441,True):
            with self.assertRaises(DataValidationError):inspect_news(self.settings,lookback_minutes=n,adapter_factory=lambda _:self.fail('No API'))
        with self.assertRaises(DataValidationError):inspect_news(self.settings,('XYZ',),adapter_factory=lambda _:self.fail('No API'))
    def test_probe_unauthorized_response_not_empty_success(self):
        def adapter(s):return AlpacaNews(s,transport=lambda *a:Response(401,{},b'SECRET must not leak'),sleep=lambda _:None)
        with self.assertRaises(MarketDataError) as raised:inspect_news(self.settings,adapter_factory=adapter,now=self.now)
        self.assertNotIn('SECRET',str(raised.exception))
    def test_probe_pagination_bound_failure_not_partial_complete(self):
        count=0
        def adapter(s):
            def transport(*args):
                nonlocal count;count+=1;return Response(200,{},json.dumps({'news':[],'next_page_token':str(count)}).encode())
            return AlpacaNews(s,transport=transport)
        with self.assertRaises(MarketDataError):inspect_news(self.settings,adapter_factory=adapter,now=self.now)
        self.assertEqual(count,5)
    def test_probe_cli_does_not_open_database_or_model(self):
        result={'status':'queried','orders_sent':0}
        with patch('microbroker.cli.Settings.from_env',return_value=self.settings),patch('microbroker.cli.inspect_news',return_value=result),patch('microbroker.cli.Repository',side_effect=AssertionError('No DB')),patch('microbroker.cli.make_engine',side_effect=AssertionError('No model')),contextlib.redirect_stdout(io.StringIO()):self.assertEqual(main(['inspect-news-feed']),0)

class PersistenceTests(unittest.TestCase):
    def test_unexpected_session_failure_has_saved_sanitized_report(self):
        with tempfile.TemporaryDirectory() as d:
            url='sqlite:///'+str(Path(d)/'db');f=FakePaper('r24-crash');settings=Settings(database_url=url,api_key='FAKE',api_secret='FAKE')
            def failed(*a,**kw):raise RuntimeError('FAKE_SECRET must not leak')
            with self.assertRaises(RuntimeError):run_paper_session(settings,SessionRequest(('MSFT',)),FixtureEngine(),api_factory=lambda _:f.client,session_runner=failed,_fixture=True)
            repo=Repository(url)
            try:
                r=report_session(repo);self.assertEqual(r['recorded_session_status'],'failed');self.assertNotIn('FAKE_SECRET',json.dumps(r));self.assertEqual(f.posts,0)
            finally:repo.close()
    def test_interrupted_session_is_reportable_without_restart(self):
        with tempfile.TemporaryDirectory() as d:
            url='sqlite:///'+str(Path(d)/'db');f=FakePaper('r24-interrupt');settings=Settings(database_url=url,api_key='FAKE',api_secret='FAKE')
            def stopped(*a,**kw):raise KeyboardInterrupt()
            with self.assertRaises(KeyboardInterrupt):run_paper_session(settings,SessionRequest(('MSFT',)),FixtureEngine(),api_factory=lambda _:f.client,session_runner=stopped,_fixture=True)
            repo=Repository(url)
            try:self.assertEqual(report_session(repo)['recorded_session_status'],'interrupted')
            finally:repo.close()

if __name__=='__main__':unittest.main()
