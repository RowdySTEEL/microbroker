import contextlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from microbroker.cli import main
from microbroker.config import Settings, ConfigurationError
from microbroker.data.adapters.alpaca import AlpacaHistoricalData
from microbroker.data.adapters.alpaca_stream import AlpacaStream
from microbroker.data.session import run_session, SessionRequest, session_fixture, session_summary
from microbroker.data.session_coverage import session_coverage
from microbroker.data.storage.repository import Repository

class R13Tests(unittest.TestCase):
    def test_cyrillic_secret_rejected_before_http(self):
        settings=Settings(api_key='FIXTURE',api_secret='FIXTURE\u0410')
        with self.assertRaises(ConfigurationError) as caught:
            AlpacaHistoricalData(settings,transport=lambda *args:self.fail('No HTTP'))
        self.assertIn('ALPACA_API_SECRET',str(caught.exception))
        self.assertNotIn(settings.api_secret,str(caught.exception))
    def test_invalid_key_rejected_before_stream_connection(self):
        with self.assertRaises(ConfigurationError):
            AlpacaStream(Settings(api_key='FIXTURE\u043e',api_secret='FIXTURE'),connector=lambda *args:self.fail('No connection'))
    def test_whitespace_control_and_non_ascii_rejected(self):
        for char in (' ','\t','\r','\n','\x00','\x7f','\u00a0','\u200b','\u201d','\u0410'):
            for field in ('api_key','api_secret'):
                with self.subTest(char=ord(char),field=field),self.assertRaises(ConfigurationError):
                    Settings(**{'api_key':'FIXTURE','api_secret':'FIXTURE',field:'FIXTURE'+char}).require_credentials()
    def test_ascii_values_accepted_without_rewriting(self):
        settings=Settings(api_key='FIXTURE-key_123',api_secret='FIXTURE-secret_+/-=')
        settings.require_credentials()
        self.assertTrue(settings.credential_checks()['format_valid'])
        self.assertEqual(settings.api_secret,'FIXTURE-secret_+/-=')
        self.assertFalse(settings.credential_checks()['provider_authentication_checked'])
    def test_missing_credentials_reported_without_network(self):
        checks=Settings().credential_checks()
        self.assertFalse(checks['format_valid'])
        self.assertFalse(checks['fields']['ALPACA_API_SECRET']['present'])
    def test_doctor_invalid_secret_fails_without_exposing_value(self):
        settings=Settings(api_key='FIXTURE',api_secret='FIXTURE\u0410')
        out=io.StringIO()
        with patch('microbroker.cli.Settings.from_env',return_value=settings),contextlib.redirect_stdout(out):
            code=main(['doctor','--require-credentials'])
        self.assertEqual(code,2)
        report=json.loads(out.getvalue())
        self.assertTrue(report['credentials_present'])
        self.assertFalse(report['credential_checks']['fields']['ALPACA_API_SECRET']['format_valid'])
        self.assertNotIn('FIXTURE',out.getvalue())
    def test_non_strict_doctor_still_reports_bad_format(self):
        with patch('microbroker.cli.Settings.from_env',return_value=Settings(api_key='FIXTURE',api_secret='FIXTURE\u0410')),contextlib.redirect_stdout(io.StringIO()) as out:
            self.assertEqual(main(['doctor']),0)
        self.assertFalse(json.loads(out.getvalue())['credential_checks']['format_valid'])
    def test_session_cli_invalid_secret_before_model_and_db(self):
        settings=Settings(database_url='postgresql://fixture/db',api_key='FIXTURE',api_secret='FIXTURE\u0410')
        with patch('microbroker.cli.Settings.from_env',return_value=settings),patch('microbroker.cli.make_engine',side_effect=AssertionError('No model')),patch('microbroker.cli.Repository',side_effect=AssertionError('No DB')),contextlib.redirect_stderr(io.StringIO()) as err:
            self.assertEqual(main(['run-research-session']),2)
        self.assertNotIn('FIXTURE',err.getvalue());self.assertIn('ALPACA_API_SECRET',err.getvalue())
    def test_session_function_invalid_key_before_db(self):
        with self.assertRaises(ConfigurationError):
            run_session(Settings(api_key='FIXTURE\u0410',api_secret='FIXTURE'),SessionRequest(),None,
                repository_factory=lambda *args:self.fail('No DB'))
    def test_file_and_environment_precedence_validation(self):
        with tempfile.TemporaryDirectory() as directory:
            env=Path(directory)/'fixture.env';env.write_text('ALPACA_API_KEY=FIXTURE\nALPACA_API_SECRET="FIXTURE-secret"\n')
            with patch.dict(os.environ,{},clear=True):
                self.assertTrue(Settings.from_env(env).credential_checks()['format_valid'])
            with patch.dict(os.environ,{'ALPACA_API_SECRET':'FIXTURE\u0410'},clear=True):
                with self.assertRaises(ConfigurationError):Settings.from_env(env).require_credentials()
    def test_successful_session_reports_real_fixture_alignment(self):
        result,audit,_=session_fixture();coverage=result['coverage']
        self.assertEqual(coverage['assessment'],'aligned_research_rows')
        self.assertEqual(coverage['alignment']['matched_rows'],1)
        self.assertEqual(coverage['prices']['snapshot_minute_bars'],1)
        self.assertEqual(json.loads(audit[1]),result)
        self.assertFalse(coverage['trading_readiness_validated'])
    def test_empty_news_and_missing_prices_are_distinct(self):
        news,_,_=session_fixture(news_empty=True)
        price,_,_=session_fixture(stream_empty=True)
        self.assertIn('news_queries_returned_empty',news['coverage']['reason_codes'])
        self.assertNotIn('no_stream_minute_bars',news['coverage']['reason_codes'])
        self.assertIn('no_stream_minute_bars',price['coverage']['reason_codes'])
        self.assertNotIn('news_queries_returned_empty',price['coverage']['reason_codes'])
    def test_failed_news_not_reported_as_empty_success(self):
        result,_,_=session_fixture(news_failed=True)
        self.assertEqual(result['coverage']['assessment'],'failed')
        self.assertIn('no_successful_news_poll',result['coverage']['reason_codes'])
        self.assertNotIn('news_queries_returned_empty',result['coverage']['reason_codes'])
        self.assertIsNone(result['coverage']['news']['snapshot_articles'])
    def test_analysis_failure_preserves_successful_poll_counts(self):
        result,_,_=session_fixture(analysis_failed=True)
        self.assertEqual(result['coverage']['news']['received_articles_across_polls'],1)
        self.assertEqual(result['coverage']['news']['successful_polls'],1)
        self.assertNotIn('news_queries_returned_empty',result['coverage']['reason_codes'])
    def test_legacy_partial_cycles_do_not_claim_empty_api_result(self):
        coverage=session_coverage({'status':'failed','news_polls':1,'new_versions':1,'cycles':[]})
        self.assertIsNone(coverage['news']['received_articles_across_polls'])
        self.assertNotIn('news_queries_returned_empty',coverage['reason_codes'])
    def test_legacy_no_data_receipts_derive_without_calendar_claim(self):
        result={'status':'no_data','news_polls':2,'cycles':[{'received_articles':0},{'received_articles':0}],
            'new_versions':0,'session_articles':0,'available_analyses':0,
            'stream':{'authenticated':True,'subscribed':True,'events_received':0,'bars_written':0},
            'price_coverage':'no_minute_bars_in_session_window'}
        coverage=session_coverage(result)
        self.assertEqual(coverage['prices']['snapshot_minute_bars'],0)
        self.assertEqual(coverage['news']['received_articles_across_polls'],0)
        self.assertFalse(coverage['market_open_checked'])
        self.assertNotIn('coverage',result)
    def test_snapshot_prices_cannot_hide_empty_live_stream(self):
        report=session_coverage({'status':'completed','snapshot_minute_bars':20,'stream':{'bars_written':0},
            'alignment':{'matched_rows':1,'company_rows':1,'status_counts':{'matched':1}}})
        self.assertEqual(report['prices']['stream_bars_written'],0)
        self.assertEqual(report['prices']['snapshot_minute_bars'],20)
        self.assertIn('no_stream_minute_bars',report['reason_codes'])
    def test_pending_analyses_and_unmatched_reasons_retained(self):
        report=session_coverage({'status':'completed','session_articles':2,'available_analyses':1,
            'alignment':{'matched_rows':0,'company_rows':2,'status_counts':{'previous_bar_too_old':2}}})
        self.assertEqual(report['analysis']['snapshot_pending'],1)
        self.assertEqual(report['alignment']['status_counts'],{'previous_bar_too_old':2})
        self.assertIn('pending_snapshot_analyses',report['reason_codes'])
    def test_duplicates_are_not_new_articles(self):
        report=session_coverage({'status':'no_data','news_polls':1,'received_articles':101,'new_versions':0,'session_articles':0})
        self.assertIn('only_previously_stored_news_versions',report['reason_codes'])
        self.assertNotIn('news_queries_returned_empty',report['reason_codes'])
    def test_legacy_summary_readonly_and_repeatable(self):
        repo=Repository('sqlite:///:memory:')
        result={'session_id':'fixture','status':'no_data','news_polls':1,'cycles':[{'received_articles':0}]}
        try:
            repo.connection.execute('INSERT INTO research_sessions VALUES (?,?,?,?,?,?)',('fixture','start','end','no_data','{}',json.dumps(result)));repo.connection.commit()
            changes=repo.connection.total_changes
            first=session_summary(repo,'fixture');second=session_summary(repo,'fixture')
            self.assertEqual(first,second);self.assertEqual(first['session'],result)
            self.assertEqual(repo.connection.total_changes,changes)
            self.assertEqual(first['coverage']['assessment'],'no_data')
        finally:repo.close()

if __name__=='__main__':unittest.main()
