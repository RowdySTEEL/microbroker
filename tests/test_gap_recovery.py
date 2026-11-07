from datetime import timedelta
from dataclasses import replace
import json
import unittest
import tempfile
from pathlib import Path
from unittest.mock import patch
import contextlib
import io
from microbroker.config import Settings
from microbroker.data.models import utc_datetime
from microbroker.data.adapters.alpaca import normalize_bar, MarketDataError
from microbroker.data.adapters.alpaca_stream import AlpacaStream
from microbroker.data.ingestion.gap_recovery import recover_stream_gaps
from microbroker.data.storage.repository import Repository
from microbroker.data.stream_models import StreamRequest, normalize_event
from microbroker.cli import main
from test_r2 import Clock, socket_for, frame, BAR, TRADE, stats

class GapTests(unittest.TestCase):
    def setUp(self):
        self.repo = Repository('sqlite:///:memory:')
        self.settings = Settings(api_key='TEST', api_secret='TEST')
        self.request = StreamRequest(('AAPL',))
        self.run = self.repo.begin_stream(self.request, stats())
        self.start = utc_datetime('2026-01-05T14:30:50Z')
        self.end = utc_datetime('2026-01-05T14:32:10Z')
        self.gap = self.repo.begin_gap(self.run, self.request, self.start)
        self.repo.close_gap(self.gap, self.end)
        self.now = utc_datetime('2026-01-05T14:34:00Z')
        self.requests = []
    def tearDown(self): self.repo.close()
    def factory(self, rows, broken=False):
        owner = self
        class Adapter:
            def __init__(self, settings): pass
            def iter_bars(self, request):
                owner.requests.append(request)
                for row in rows:
                    yield normalize_bar('AAPL', row, request)
                if broken: raise MarketDataError('secret must not enter audit')
        return Adapter
    def recover(self, rows=(), broken=False, now=None):
        return recover_stream_gaps(self.settings, self.repo, self.run,
            adapter_factory=self.factory(rows, broken), now=now or self.now)
    def test_boundaries_overlap_maturity_and_scope(self):
        result = self.recover([BAR])
        r = self.requests[0]
        self.assertEqual(r.start.isoformat(), '2026-01-05T14:29:00+00:00')
        self.assertEqual(r.end.isoformat(), '2026-01-05T14:32:00+00:00')
        self.assertEqual((r.feed, r.timeframe, r.adjustment), ('iex','1Min','raw'))
        self.assertEqual(result['gaps'], {'backfilled':1})
        self.assertFalse(result['trades_quotes_recovered'])
        self.assertEqual(self.repo.connection.execute('SELECT COUNT(*) FROM stream_events').fetchone()[0],0)
    def test_unclosed_minute_defers_without_http(self):
        result = self.recover(now=self.now-timedelta(microseconds=1))
        self.assertEqual(result['gaps'], {'pending':1})
        self.assertEqual(self.requests, [])
    def test_preserve_live_updated_bar_and_idempotency(self):
        self.repo.apply_stream_frame(self.run,[normalize_event({**BAR,'T':'u','c':102},self.request)],stats())
        result = self.recover([BAR])
        self.assertEqual(result['outcomes'][0]['inserted_bars'],0)
        self.assertEqual(self.repo.connection.execute('SELECT close FROM bars').fetchone()[0],'102')
        self.assertEqual(self.recover([BAR])['outcomes'],[])
        self.assertEqual(self.repo.count(),1)
    def test_partial_page_rollback_and_retry(self):
        failed = self.recover([BAR],broken=True)
        self.assertEqual(failed['gaps'], {'failed':1})
        self.assertEqual(self.repo.count(),0)
        self.assertNotIn('secret',json.dumps(failed))
        result = self.recover([BAR])
        self.assertEqual(result['status'],'passed')
        self.assertEqual(self.repo.count(),1)
        self.assertEqual(self.repo.connection.execute('SELECT attempts FROM stream_gaps').fetchone()[0],2)
    def test_empty_response_does_not_invent_bars(self):
        result = self.recover()
        self.assertEqual(result['outcomes'][0]['coverage'],'provider_response_only')
        self.assertEqual(result['outcomes'][0]['received_by_symbol'],{'AAPL':0})
        self.assertEqual(self.repo.count(),0)
    def test_open_gap_is_retained_and_not_recovered(self):
        self.repo.begin_gap(self.run,self.request,self.start)
        result = self.recover([BAR])
        self.assertEqual(result['status'],'pending')
        self.assertEqual(result['gaps'], {'backfilled':1,'open':1})
    def test_other_session_not_recovered(self):
        result = recover_stream_gaps(self.settings,self.repo,'different',adapter_factory=self.factory([BAR]),now=self.now)
        self.assertEqual(result['outcomes'],[])
        self.assertEqual(self.repo.gap_summary(self.run),{'pending':1})
    def test_duplicate_rest_rows_insert_once(self):
        result = self.recover([BAR,BAR])
        self.assertEqual(result['outcomes'][0]['inserted_bars'],1)
        self.assertEqual(self.repo.count(),1)
    def test_invalid_bar_rolls_back(self):
        def adapter(settings):
            class A:
                def iter_bars(inner, request):
                    b = normalize_bar('AAPL',BAR,request)
                    yield b
                    yield replace(b,feed='sip')
            return A()
        result = recover_stream_gaps(self.settings,self.repo,self.run,adapter_factory=adapter,now=self.now)
        self.assertEqual(result['gaps'],{'failed':1})
        self.assertEqual(self.repo.count(),0)
    def test_reconnect_persists_gap_and_recovers_after_socket_close(self):
        clock = Clock()
        r = StreamRequest(('AAPL',),duration=180)
        sockets = [socket_for(clock,r,frame(TRADE),OSError('offline')),socket_for(clock,r,frame(BAR))]
        remaining = list(sockets)
        def connect(*args):
            if len(remaining) == 1:
                clock.value += 90
            return remaining.pop(0)
        def recovery(run):
            self.assertTrue(all(s.closed for s in sockets))
            return recover_stream_gaps(self.settings,self.repo,run,
                adapter_factory=self.factory([{**BAR,'t':'2026-01-05T14:31:00Z'}]),now=self.now+timedelta(minutes=1))
        result = AlpacaStream(self.settings,connector=connect,clock=clock,sleep=clock.sleep,
            utc_now=lambda:self.start+timedelta(seconds=clock.value)).run(r,self.repo,recovery=recovery)
        self.assertEqual(result['reconnects'],1)
        self.assertEqual(result['gaps'],{'backfilled':1})
        self.assertEqual(result['events_inserted'],2)
        self.assertEqual(result['recovery']['outcomes'][0]['inserted_bars'],1)
        self.assertEqual(self.repo.count(),2)
    def test_reconnect_exhaustion_keeps_open_gap(self):
        clock = Clock(); r=StreamRequest(('AAPL',),max_reconnects=0)
        sock = socket_for(clock,r,frame(TRADE),OSError('offline'))
        result = AlpacaStream(self.settings,connector=lambda *a:sock,clock=clock,sleep=clock.sleep).run(r,self.repo)
        self.assertEqual(result['status'],'failed')
        self.assertEqual(result['gaps'],{'open':1})
    def test_test_feed_does_not_create_real_gaps(self):
        clock = Clock(); r=StreamRequest(('FAKEPACA',),feed='test',max_reconnects=0)
        sock = socket_for(clock,r,OSError('offline'))
        result = AlpacaStream(self.settings,connector=lambda *a:sock,clock=clock,sleep=clock.sleep).run(r,self.repo)
        self.assertEqual(result['gaps'],{})
        self.assertEqual(result['gap_recovery'],'not_applicable_test')
    def test_success_audit_failure_rolls_back_bar(self):
        self.repo.connection.execute("CREATE TRIGGER reject_success BEFORE UPDATE ON stream_gaps WHEN NEW.status='backfilled' BEGIN SELECT RAISE(ABORT,'test'); END")
        self.repo.connection.commit()
        result = self.recover([BAR])
        self.assertEqual(self.repo.count(),0)
        self.assertEqual(result['gaps'],{'failed':1})
    def test_cli_and_restart_persisted_gap(self):
        with tempfile.TemporaryDirectory() as folder:
            url = 'sqlite:///'+str(Path(folder)/'data.db')
            repo = Repository(url)
            run = repo.begin_stream(self.request, stats())
            gap = repo.begin_gap(run, self.request, self.start)
            repo.close_gap(gap, self.end)
            repo.close()
            settings = replace(self.settings,database_url=url)
            captured = io.StringIO()
            # One real recovery implementation behind the CLI; only REST is replaced.
            def recover(settings,repository,run_id):
                return recover_stream_gaps(settings,repository,run_id,
                    adapter_factory=self.factory([BAR]),now=self.now)
            with patch('microbroker.cli.Settings.from_env',return_value=settings),patch('microbroker.cli.recover_stream_gaps',side_effect=recover),contextlib.redirect_stdout(captured):
                code = main(['recover-stream-gaps','--run-id',run])
            self.assertEqual(code,0)
            self.assertEqual(json.loads(captured.getvalue())['gaps'],{'backfilled':1})
            repo = Repository(url)
            self.assertEqual(repo.count(),1)
            self.assertEqual(repo.gap_summary(run),{'backfilled':1})
            repo.close()
    def test_cli_pending_exit_code(self):
        with patch('microbroker.cli.Settings.from_env',return_value=replace(self.settings,database_url='sqlite:///:memory:')),patch('microbroker.cli.recover_stream_gaps',return_value={'status':'pending'}),contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['recover-stream-gaps']),3)
