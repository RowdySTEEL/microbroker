"""Session lifecycle, independent stage commits, scoped snapshots and stream stop."""
import contextlib
from dataclasses import replace
from datetime import datetime,timezone,timedelta
import io
import threading
import unittest
from unittest.mock import patch
from microbroker.cli import main
from microbroker.config import Settings
from microbroker.data.models import DataValidationError
from microbroker.data.session import SessionRequest,session_fixture,verify_session,run_session
from microbroker.data.news_replay import NewsReplayRequest,create_news_replay,replay_news
from microbroker.data.storage.repository import Repository
import test_r9
import test_r2
from microbroker.data.adapters.alpaca_stream import AlpacaStream

class R12Tests(unittest.TestCase):
    def test_private_concurrent_session_success(self):
        result,audit,articles=session_fixture()
        self.assertEqual(result['status'],'completed');self.assertEqual(articles,1)
        self.assertEqual(result['alignment']['matched_rows'],1)
        self.assertEqual(audit[0],'completed');self.assertEqual(result['orders_sent'],0)
    def test_polling_reuses_immutable_analyses(self):
        result,_,_=session_fixture();self.assertEqual(result['new_versions'],1);self.assertEqual(result['inserted_analyses'],1)
        self.assertGreaterEqual(result['news_polls'],1)
    def test_empty_news_marks_no_data(self):
        result,audit,_=session_fixture(news_empty=True)
        self.assertEqual(result['status'],'no_data');self.assertEqual(result['session_articles'],0)
        self.assertEqual(audit[0],'no_data')
    def test_failed_stream_marks_failed_session(self):
        result,audit,_=session_fixture(stream_failed=True)
        self.assertEqual(result['status'],'failed');self.assertEqual(result['error_code'],'stream_failed');self.assertEqual(audit[0],'failed')
    def test_news_failure_sanitized(self):
        result,_,_=session_fixture(news_failed=True)
        self.assertEqual(result['error_code'],'news_poll_failed');self.assertNotIn('SYNTHETIC_SECRET',str(result))
    def test_analysis_failure_preserves_committed_news(self):
        result,_,articles=session_fixture(analysis_failed=True)
        self.assertEqual(result['error_code'],'analysis_failed');self.assertEqual(articles,1)
        self.assertTrue(result['committed_stages_preserved']);self.assertEqual(result['snapshots'],{})
    def test_invalid_session_bounds(self):
        for kw in ({'duration':0},{'duration':True},{'poll_seconds':1},{'lookback_minutes':61},{'feed':'test'},{'symbols':('XYZ',)}):
            with self.assertRaises(DataValidationError):SessionRequest(**kw)
    def test_sqlite_user_session_rejected(self):
        with self.assertRaises(DataValidationError):run_session(Settings(api_key='fixture',api_secret='fixture'),SessionRequest(),__import__('microbroker.intelligence.sentiment',fromlist=['RulesEngine']).RulesEngine())
    def test_sandbox_session_rejected(self):
        with self.assertRaises(DataValidationError):run_session(Settings(data_env='sandbox',auth_mode='broker',database_url='postgresql://test/db',api_key='fixture',api_secret='fixture'),SessionRequest(),None)
    def test_scope_excludes_pre_session_receipts(self):
        repository=Repository('sqlite:///:memory:');self.repo=repository
        try:
            test_r9.R9Tests.add(self,id=1,received='2026-01-06T10:00:00Z')
            test_r9.R9Tests.add(self,id=2,received='2026-01-07T10:00:00Z')
            snapshot=create_news_replay(repository,NewsReplayRequest('2026-01-08T00:00:00Z',('MSFT',),first_seen_since='2026-01-07T10:00:00Z'))
            result=replay_news(repository,snapshot['replay_id'])
            self.assertEqual(result['available_articles'],1);self.assertEqual(result['samples'][0]['id'],2)
            self.assertEqual(replay_news(repository,snapshot['replay_id'],'2026-01-06T12:00:00Z')['available_articles'],0)
        finally:repository.close()
    def test_scope_since_after_cutoff_rejected(self):
        with self.assertRaises(DataValidationError):NewsReplayRequest('2026-01-08T00:00:00Z',first_seen_since='2026-01-09T00:00:00Z')
    def test_legacy_r9_request_metadata_unchanged(self):
        self.assertNotIn('first_seen_since',NewsReplayRequest('2026-01-08T00:00:00Z').metadata())
    def test_stop_event_prevents_stream_connect(self):
        repository=Repository('sqlite:///:memory:');stop=threading.Event();stop.set()
        try:
            adapter=AlpacaStream(Settings(api_key='fixture',api_secret='fixture'),connector=lambda *args: self.fail('must not connect'))
            result=adapter.run(test_r2.req(duration=60),repository,stop_event=stop)
            self.assertEqual(result['stop_reason'],'session_stop');self.assertEqual(result['status'],'stopped')
        finally:repository.close()
    def test_stream_stop_after_frame_closes_socket(self):
        repository=Repository('sqlite:///:memory:');clock=test_r2.Clock();stop=threading.Event();req=test_r2.req(duration=60)
        socket=test_r2.socket_for(clock,req,test_r2.frame(test_r2.BAR))
        try:
            adapter=AlpacaStream(Settings(api_key='fixture',api_secret='fixture'),connector=lambda *args:socket,clock=clock,sleep=clock.sleep)
            result=adapter.run(req,repository,on_event=lambda event:stop.set(),stop_event=stop)
            self.assertEqual(result['events_received'],1);self.assertEqual(result['status'],'stopped');self.assertTrue(socket.closed)
        finally:repository.close()
    def test_verifier_without_environment_or_model(self):
        with contextlib.redirect_stdout(io.StringIO()),patch('microbroker.cli.Settings.from_env',side_effect=AssertionError('No env')):
            self.assertEqual(main(['verify-research-session']),0)
        self.assertEqual(len(verify_session()['checks']),15)
    def test_session_schema_preserves_existing_tables(self):
        repository=Repository('sqlite:///:memory:')
        try:
            self.assertIsNotNone(repository.connection.execute("SELECT name FROM sqlite_master WHERE name='research_sessions'").fetchone())
            self.assertIsNotNone(repository.connection.execute("SELECT name FROM sqlite_master WHERE name='news_analyses'").fetchone())
        finally:repository.close()
    def test_live_session_cli_rejects_sqlite_before_model_load(self):
        with patch('microbroker.cli.Settings.from_env',return_value=Settings(api_key='fixture',api_secret='fixture')),patch('microbroker.cli.make_engine',side_effect=AssertionError('Do not load model')),contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(main(['run-research-session']),2)
    def test_scope_does_not_resurrect_pre_session_revision(self):
        repository=Repository('sqlite:///:memory:');self.repo=repository
        try:
            test_r9.R9Tests.add(self,received='2026-01-06T10:00:00Z')
            test_r9.R9Tests.add(self,headline='Apple revised report',updated='2026-01-07T09:00:00Z',received='2026-01-07T10:00:00Z',symbols=('AAPL',))
            snapshot=create_news_replay(repository,NewsReplayRequest('2026-01-08T00:00:00Z',('MSFT',),first_seen_since='2026-01-07T00:00:00Z'))
            self.assertEqual(replay_news(repository,snapshot['replay_id'])['available_articles'],0)
        finally:repository.close()
    def test_optional_scope_inclusive_nanosecond(self):
        request=NewsReplayRequest('2026-01-08T00:00:00Z',first_seen_since='2026-01-07T13:00:00.000000001+03:00')
        self.assertEqual(request.first_seen_since,'2026-01-07T10:00:00.000000001Z')
    def test_stop_event_interrupts_reconnect_wait(self):
        repository=Repository('sqlite:///:memory:');stop=threading.Event();clock=test_r2.Clock()
        try:
            def connector(*args):stop.set();raise OSError('fixture network')
            adapter=AlpacaStream(Settings(api_key='fixture',api_secret='fixture'),connector=connector,clock=clock)
            result=adapter.run(test_r2.req(duration=60),repository,stop_event=stop)
            self.assertEqual(result['status'],'stopped');self.assertEqual(result['connections'],0)
        finally:repository.close()
    def test_session_summary_unknown_identity_rejected(self):
        from microbroker.data.session import session_summary
        repository=Repository('sqlite:///:memory:')
        try:
            with self.assertRaises(DataValidationError):session_summary(repository,'unknown')
        finally:repository.close()

    def test_no_price_data_marks_no_data_without_alignment(self):
        result,_,_=session_fixture(stream_empty=True)
        self.assertEqual(result['status'],'no_data');self.assertIsNone(result['alignment'])
        self.assertEqual(result['session_articles'],1)
        self.assertEqual(result['price_coverage'],'no_minute_bars_in_session_window')
    def test_session_summary_saved_audit_roundtrip(self):
        import json
        from microbroker.data.session import session_summary
        result,_,_=session_fixture();repository=Repository('sqlite:///:memory:')
        try:
            repository.connection.execute('INSERT INTO research_sessions VALUES (?,?,?,?,?,?)',(result['session_id'],result['started_at'],result['finished_at'],result['status'],'{}',json.dumps(result)));repository.connection.commit()
            report=session_summary(repository,result['session_id'])
            self.assertEqual(report['session'],result);self.assertFalse(report['database_modified'])
        finally:repository.close()
    def test_session_cli_no_data_exit_code(self):
        from microbroker.intelligence.sentiment import RulesEngine
        settings=Settings(database_url='postgresql://fixture/db',api_key='fixture',api_secret='fixture')
        with patch('microbroker.cli.Settings.from_env',return_value=settings),patch('microbroker.cli.make_engine',return_value=RulesEngine()),patch('microbroker.cli.run_session',return_value={'status':'no_data'}),contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['run-research-session','--engine','rules']),3)
