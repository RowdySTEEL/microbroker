"""R27 reproduces zero-data reports and preserves fills, failures and old evidence."""
import contextlib
from copy import deepcopy
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from microbroker.cli import main
from microbroker.config import Settings
from microbroker.data.storage.repository import Repository
from microbroker.execution.paper_monitor import save_run,report_session,render_report,assessment
from microbroker.execution.paper_worker_verification import WorkerFixture
from microbroker.execution.portfolio_history import history_report,render_history
from microbroker.execution.portfolio_verification import seeded
from microbroker.execution.paper_portfolio import capture_portfolio
from microbroker.execution.report_price_health import price_health
from microbroker.execution.report_health_verification import no_price_research,verify_health,health_checks


class PriceHealthTests(unittest.TestCase):
    def setUp(self):
        self.paper={'decisions':2,'uncertain_intents':[],'post_attempts':0,'broker_order_status_counts':{},'status_counts':{'wait':2}}
        self.missing=no_price_research()
    def test_controlled_private_acceptance(self):
        r=verify_health();self.assertEqual(len(r['checks']),14);self.assertEqual(r['orders_sent_to_alpaca'],0)
    def test_supplied_zero_prices_not_hidden_by_company_waits_and_gap_flags(self):
        self.assertEqual(assessment('no_data',self.missing,self.paper,{'price_gap_symbols':['AAPL','MSFT','NVDA','TSLA']}),'price_data_unavailable')
    def test_zero_stream_but_no_news_still_identifies_price_issue(self):
        self.missing['received_articles']=0;self.assertEqual(assessment('no_data',self.missing,self.paper,{}),'price_data_unavailable')
    def test_quotes_and_trades_without_bars_do_not_satisfy_minute_requirement(self):
        self.missing['stream'].update(events_received=500,received_by_type={'q':490,'t':10})
        health=price_health(self.missing,{});self.assertEqual(health['status'],'missing_minute_bars');self.assertEqual(health['stream_events_received'],500)
    def test_no_snapshot_even_when_bars_written_is_incomplete(self):
        self.missing['stream'].update(events_received=5,bars_written=5)
        self.assertEqual(price_health(self.missing,{})['status'],'missing_snapshot_prices')
    def test_known_failure_priority_over_missing_price(self):
        self.assertEqual(assessment('failed',self.missing,self.paper,{}),'session_failed_or_interrupted')
    def test_uncertain_broker_order_priority_not_hidden_by_missing_price(self):
        self.paper['uncertain_intents']=['uncertain'];self.assertEqual(assessment('no_data',self.missing,self.paper,{}),'broker_reconciliation_required')
    def test_running_zero_data_described_as_saved_wait_not_terminal_failure(self):
        self.assertEqual(assessment('running',self.missing,self.paper,{}),'awaiting_price_data')
    def test_running_warmup_without_counters_does_not_invent_zero(self):
        r={'price_warmup':{'status':'pending'}};h=price_health(r,{})
        self.assertEqual(h['status'],'warmup_pending');self.assertIsNone(h['stream_events_received']);self.assertEqual(assessment('running',r,self.paper,{}),'awaiting_price_data')
    def test_stream_failed_does_not_look_like_no_new_signals(self):
        self.missing['stream'].update(status='failed',error_code='409')
        self.assertEqual(assessment('no_data',self.missing,self.paper,{}),'price_stream_failed')
    def test_existing_bars_with_latest_clock_issue_remain_incomplete(self):
        r={'stream':{'bars_written':4},'snapshot_minute_bars':4}
        h=price_health(r,{'price_clock_flags':['MSFT']});self.assertTrue(h['acceptance_incomplete_from_recorded_price_evidence'])
    def test_late_recovery_keeps_warmup_warning_but_not_missing_label(self):
        self.missing['stream'].update(events_received=4,bars_written=4);self.missing['snapshot_minute_bars']=4
        h=price_health(self.missing,{'fresh_price_symbols':['MSFT']})
        self.assertEqual(h['status'],'recorded_minute_data_present');self.assertIn('price_warmup_timed_out',h['reason_codes']);self.assertFalse(h['acceptance_passed'])
    def test_saved_prices_do_not_claim_current_price_or_provider_cause(self):
        h=price_health({'stream':{'bars_written':2}}, {})
        for key in ('broker_price_freshness_validated','provider_incident_checked','outage_cause_determined','network_required','point_in_time_validated'):
            self.assertFalse(h[key],key)
    def test_missing_legacy_counters_are_not_replaced_with_zero(self):
        h=price_health({},{});self.assertEqual(h['status'],'not_recorded');self.assertIsNone(h['snapshot_minute_bars'])
    def test_zero_writes_without_delivery_counters_cannot_prove_absent_duplicate_bars(self):
        h=price_health({'stream':{'bars_written':0}}, {})
        self.assertEqual(h['status'],'not_recorded');self.assertFalse(h['acceptance_incomplete_from_recorded_price_evidence'])
    def test_invalid_boolean_and_negative_counts_not_accepted_as_measurement(self):
        h=price_health({'stream':{'bars_written':False,'events_received':-1}}, {})
        self.assertIsNone(h['stream_events_received']);self.assertIsNone(h['stream_bars_written']);self.assertEqual(h['status'],'not_recorded')
    def test_positive_bar_event_avoids_false_missing_when_duplicate_not_written(self):
        h=price_health({'stream':{'bars_written':0,'received_by_type':{'b':2}},'snapshot_minute_bars':2},{})
        self.assertEqual(h['status'],'recorded_minute_data_present')
    def test_same_run_snapshot_cannot_be_present_with_zero_total_events(self):
        r={'price_scope':'same_stream_run_receipts','snapshot_minute_bars':2,'stream':{'events_received':0,'bars_written':0}}
        self.assertEqual(price_health(r,{})['status'],'inconsistent_evidence')
    def test_legacy_cross_writer_snapshot_not_same_run_contradiction(self):
        r={'snapshot_minute_bars':2,'stream':{'events_received':0,'bars_written':0}}
        self.assertEqual(price_health(r,{})['status'],'recorded_minute_data_present')


class SavedReportHealthTests(unittest.TestCase):
    def setUp(self):
        self.repo=Repository('sqlite:///:memory:');self.f=WorkerFixture(self.repo,'r27-unit-report',72)
    def tearDown(self):self.repo.close()
    def store(self,research=None,status='no_data',**extra):
        value={'status':status,'paper':self.f.worker.summary(),'research_session':research or no_price_research(),'observation':{},**extra}
        save_run(self.repo,self.f.worker,value);return report_session(self.repo,self.f.worker.session_id)
    def test_old_sealed_audit_read_with_new_report_without_rewriting(self):
        r=self.store();raw=self.repo.connection.execute('SELECT result_json,result_sha256 FROM paper_worker_runs').fetchone();self.repo.connection.commit();before=self.repo.connection.total_changes
        again=report_session(self.repo,self.f.worker.session_id)
        self.assertEqual(again,r);self.assertEqual(raw,self.repo.connection.execute('SELECT result_json,result_sha256 FROM paper_worker_runs').fetchone())
        self.assertEqual(before,self.repo.connection.total_changes);self.assertEqual(r['report_revision'],'R29')
    def test_text_displays_zero_as_zero_and_not_as_missing_metadata(self):
        text=render_report(self.store());self.assertIn('Fiyat akışı: 0 olay | 0 bar yazımı',text)
        self.assertIn('Isınma: timed_out',text);self.assertIn('oturum kabulü eksik',text)
    def test_unknown_legacy_metadata_explicitly_disclosed(self):
        text=render_report(self.store({'received_articles':0}));self.assertIn('fiyat kapsamı bilgisi yok',text)
    def test_cleanup_failure_remains_main_result_and_price_issue_is_also_visible(self):
        r=self.store(worker_cleanup_error={'stage':'worker_cleanup','exception_type':'PaperBrokerError'})
        self.assertEqual(r['assessment'],'session_cleanup_failed');text=render_report(r)
        self.assertIn('Oturum kapanışında hata oluştu',text);self.assertIn('Fiyat kabulü eksik',text)
    def test_report_has_no_key_or_raw_exception_and_performs_no_network(self):
        before=len(self.f.fake.calls);r=self.store();render_report(r)
        self.assertEqual(len(self.f.fake.calls),before);self.assertEqual(self.f.fake.posts,0);self.assertFalse(r['network_required'])
    def test_zero_observed_gross_change_is_not_claimed_fresh_price(self):
        f,e=seeded(self.repo,'r27-zero-change');capture_portfolio(self.repo,f.client,snapshot_id='before');f.advance();capture_portfolio(self.repo,f.client,snapshot_id='after')
        text=render_history(history_report(self.repo,f.client.account()['account_ref']))
        self.assertIn('0 USD yalnız kayıtlı brüt değerleme değişimidir',text)
    def test_private_cli_does_not_load_credentials_or_model(self):
        with patch('microbroker.cli.Settings.from_env',side_effect=AssertionError('No env')),patch('microbroker.cli.make_engine',side_effect=AssertionError('No model')),contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['verify-paper-report-health']),0)
    def test_strict_pg_implicit_read_protocol(self):
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
        try:self.assertEqual(len(health_checks(repo)),14)
        finally:repo.close()
    def test_cli_reads_old_audit_offline(self):
        with tempfile.TemporaryDirectory() as d:
            url='sqlite:///'+str(Path(d)/'db');repo=Repository(url);f=WorkerFixture(repo,'r27-cli-read',73)
            save_run(repo,f.worker,{'status':'no_data','paper':f.worker.summary(),'research_session':no_price_research()});repo.close()
            with patch('microbroker.cli.Settings.from_env',return_value=Settings(database_url=url)),patch('microbroker.cli.PaperAPI',side_effect=AssertionError('No network')),contextlib.redirect_stdout(io.StringIO()) as out:
                self.assertEqual(main(['paper-session-report','--session-id',f.worker.session_id,'--format','text']),0)
            self.assertIn('Fiyat verisi alınamadı',out.getvalue())
