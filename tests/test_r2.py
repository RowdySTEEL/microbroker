from copy import deepcopy
import contextlib
import io
import json
import logging
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
from microbroker.config import Settings
from microbroker.data.adapters.alpaca_stream import AlpacaStream, StreamError, decode_frame
from microbroker.data.models import DataValidationError
from microbroker.data.stream_models import StreamRequest, normalize_event, canonical_time
from microbroker.data.storage.repository import Repository, StorageError
from microbroker.data.storage.postgres_verification import verify_postgres
from microbroker.cli import main

BAR={'T':'b','S':'AAPL','t':'2026-01-05T14:30:00Z','o':100,'h':102,'l':99,'c':101,'v':10,'n':3,'vw':100.5}
TRADE={'T':'t','S':'AAPL','t':'2026-01-05T14:30:00.123456789Z','p':100.5,'s':2,'i':1,'x':'V','c':['@'],'z':'C'}
QUOTE={'T':'q','S':'AAPL','t':'2026-01-05T14:30:00.123456789Z','bp':100,'ap':101,'bs':2,'as':3,'bx':'V','ax':'V'}
CORRECTION={'T':'c','S':'AAPL','t':TRADE['t'],'oi':1,'op':100,'os':2,'ci':2,'cp':101,'cs':3,'oc':['@'],'cc':['@'],'x':'V'}
CANCEL={'T':'x','S':'AAPL','t':TRADE['t'],'i':1,'p':100,'s':2,'a':'C','x':'V'}

def req(**kw):
    values={'symbols':('AAPL',)};values.update(kw);return StreamRequest(**values)
def stats():
    return {'events_received':0,'events_inserted':0,'duplicates':0,'bars_written':0,'received_by_type':{},'received_by_symbol':{}}
def frame(*rows): return json.dumps(list(rows))
def subscription(r): return {'T':'subscription',**{c:list(r.symbols) for c in r.channels}}

class Clock:
    def __init__(self): self.value=0;self.sleeps=[]
    def __call__(self): return self.value
    def sleep(self,n): self.sleeps.append(n);self.value+=n

class Socket:
    def __init__(self,clock,*frames): self.clock=clock;self.frames=list(frames);self.sent=[];self.closed=False
    def recv(self,timeout):
        if not self.frames:
            self.clock.value+=timeout
            raise TimeoutError
        value=self.frames.pop(0)
        if isinstance(value,BaseException): raise value
        return value
    def send(self,value): self.sent.append(json.loads(value))
    def close(self): self.closed=True

def socket_for(clock,r,*data):
    return Socket(clock,frame({'T':'success','msg':'connected'}),frame({'T':'success','msg':'authenticated'}),frame(subscription(r)),*data)

class R2Tests(unittest.TestCase):
    def setUp(self):
        self.repo=Repository('sqlite:///:memory:')
        self.settings=Settings(api_key='TEST-KEY',api_secret='TEST-SECRET')
        self.clock=Clock()
        self.logger=logging.getLogger('microbroker');self.logger.handlers=[logging.NullHandler()]
    def tearDown(self): self.repo.close()
    def run_stream(self,r,sockets,on_event=None):
        calls=[]
        queue=list(sockets)
        def connect(url,timeout):
            calls.append((url,timeout))
            value=queue.pop(0)
            if isinstance(value,BaseException):raise value
            return value
        result=AlpacaStream(self.settings,connector=connect,clock=self.clock,sleep=self.clock.sleep).run(r,self.repo,on_event)
        return result,calls
    def session(self,run):
        return self.repo.connection.execute('select status,stats_json,error_code from stream_sessions where id=?',(run,)).fetchone()

    def test_test_feed_validation(self):
        self.assertEqual(req(symbols=('FAKEPACA',),feed='test').feed,'test')
        for change in ({'feed':'test'},{'symbols':('FAKEPACA',)},{'channels':()},{'channels':('bogus',)},{'duration':float('nan')},{'duration':-1},{'max_events':-1},{'max_reconnects':101}):
            with self.subTest(change=change),self.assertRaises(DataValidationError):req(**change)

    def test_nanosecond_utc_preservation(self):
        self.assertEqual(canonical_time('2026-01-05T17:30:00.123456789+03:00'),'2026-01-05T14:30:00.123456789Z')
        a=normalize_event(TRADE,req());b=normalize_event({**TRADE,'t':'2026-01-05T14:30:00.123456788Z'},req())
        self.assertNotEqual(a.event_key,b.event_key)
        for t in ('2026-01-05','2026-01-05T14:30:00','2026-01-05T14:30:00.1234567890Z'):
            with self.assertRaises(DataValidationError):canonical_time(t)

    def test_all_event_types(self):
        for row in (BAR,{**BAR,'T':'u'},TRADE,QUOTE,CORRECTION,CANCEL):
            event=normalize_event(row,req())
            self.assertEqual(event.event_type,row['T'])
            self.assertEqual(event.provider,'alpaca')

    def test_test_provider_isolation(self):
        r=req(symbols=('FAKEPACA',),feed='test')
        event=normalize_event({**BAR,'S':'FAKEPACA'},r)
        self.assertEqual(event.provider,'alpaca-test');self.assertEqual(event.bar.feed,'test')

    def test_whitelist_prevents_raw_field_leak(self):
        event=normalize_event({**QUOTE,'secret':'TEST-SECRET','msg':'TEST-KEY'},req())
        self.assertNotIn('TEST-',event.payload_json)

    def test_unsubscribed_and_bad_events(self):
        for row in ({**BAR,'S':'MSFT'},{**BAR,'t':TRADE['t']},{**TRADE,'p':'NaN'},{**TRADE,'i':-1},{**QUOTE,'bs':1.5},{**CANCEL,'a':'bad'},{**QUOTE,'bx':1}):
            with self.subTest(row=row),self.assertRaises(DataValidationError):normalize_event(row,req())
        with self.assertRaises(DataValidationError):normalize_event(TRADE,req(channels=('bars',)))

    def test_trade_corrections_allowed_implicitly(self):
        self.assertEqual(normalize_event(CORRECTION,req(channels=('trades',))).event_type,'c')
        self.assertEqual(normalize_event(CANCEL,req(channels=('trades',))).event_type,'x')

    def test_decode_malformed_frames(self):
        for raw in ('{}','[]','[1]','not-json',b'\xff'):
            with self.assertRaises(StreamError):decode_frame(raw)

    def test_storage_duplicate_and_bar_revision(self):
        r=req();s=stats();run=self.repo.begin_stream(r,s)
        b=normalize_event(BAR,r);u=normalize_event({**BAR,'T':'u','c':102},r)
        s=self.repo.apply_stream_frame(run,[b,b],s)
        self.assertEqual(s['events_inserted'],1);self.assertEqual(s['duplicates'],1)
        s=self.repo.apply_stream_frame(run,[u],s)
        s=self.repo.apply_stream_frame(run,[normalize_event({**BAR,'c':101.5},r)],s)
        self.assertEqual(self.repo.count(),1)
        self.assertEqual(self.repo.connection.execute('select close from bars').fetchone()[0],'102')
        self.assertEqual(self.repo.connection.execute('select count(*) from stream_events').fetchone()[0],3)

    def test_frame_rollback_includes_stats(self):
        r=req();s=stats();run=self.repo.begin_stream(r,s)
        s=self.repo.apply_stream_frame(run,[normalize_event(BAR,r)],s)
        before=deepcopy(s)
        def fail():
            yield normalize_event({**BAR,'T':'u','c':102},r)
            raise ValueError('TEST-SECRET')
        with self.assertRaises(ValueError):self.repo.apply_stream_frame(run,fail(),s)
        self.assertEqual(s,before)
        self.assertEqual(self.repo.connection.execute('select close from bars').fetchone()[0],'101')
        self.assertEqual(self.repo.connection.execute('select count(*) from stream_events').fetchone()[0],1)
        self.assertEqual(json.loads(self.session(run)[1]),before)

    def test_schema_upgrade_preserves_existing_bars(self):
        r=req();run=self.repo.begin_stream(r,stats());self.repo.apply_stream_frame(run,[normalize_event(BAR,r)],stats())
        self.repo.migrate();self.assertEqual(self.repo.count(),1)

    def test_scoped_schema_rejects_public(self):
        with self.assertRaises(StorageError):Repository('postgresql://unused',schema='public')

    def test_postgres_verification_rejects_sqlite(self):
        with self.assertRaises(StorageError):verify_postgres(Settings())

    def test_happy_batch_and_auth(self):
        r=req(max_events=3);ws=socket_for(self.clock,r,frame(BAR,TRADE,QUOTE))
        result,calls=self.run_stream(r,[ws])
        self.assertEqual(result['status'],'completed');self.assertEqual(result['events_received'],3)
        self.assertEqual(result['bars_written'],1);self.assertEqual(result['stop_reason'],'max_events')
        self.assertEqual(ws.sent[0]['action'],'auth');self.assertEqual(ws.sent[1]['action'],'subscribe')
        self.assertTrue(ws.closed);self.assertEqual(calls[0][0],'wss://stream.data.alpaca.markets/v2/iex')
        self.assertEqual(self.session(result['run_id'])[0],'completed')

    def test_broker_sandbox_url(self):
        s=Settings(api_key='TEST-KEY',api_secret='TEST-SECRET',auth_mode='broker',data_env='sandbox')
        self.assertEqual(AlpacaStream(s).url(req()),'wss://stream.data.sandbox.alpaca.markets/v2/iex')

    def test_disconnect_reauth_and_resubscribe(self):
        r=req(max_events=2)
        a=socket_for(self.clock,r,frame(BAR),OSError('TEST-SECRET'))
        b=socket_for(self.clock,r,frame(BAR))
        result,calls=self.run_stream(r,[a,b])
        self.assertEqual(result['status'],'completed');self.assertEqual(result['reconnects'],1)
        self.assertEqual(result['connections'],2);self.assertEqual(result['duplicates'],1)
        self.assertEqual(self.repo.count(),1);self.assertEqual(b.sent[0]['action'],'auth')
        self.assertEqual(b.sent[1]['action'],'subscribe');self.assertEqual(len(calls),2)
        self.assertNotIn('TEST-',json.dumps(result))

    def test_fatal_provider_errors_are_not_retried(self):
        for code in (400,401,402,405,406,407,409,410):
            with self.subTest(code=code):
                ws=Socket(self.clock,frame({'T':'error','code':code,'msg':'TEST-SECRET'}))
                result,calls=self.run_stream(req(duration=10),[ws])
                self.assertEqual(result['status'],'failed');self.assertEqual(result['error_code'],str(code))
                self.assertEqual(len(calls),1);self.assertNotIn('TEST-',json.dumps(result))

    def test_transient_provider_error_reconnects(self):
        r=req(max_events=1)
        a=Socket(self.clock,frame({'T':'error','code':500,'msg':'TEST-SECRET'}))
        b=socket_for(self.clock,r,frame(BAR))
        result,_=self.run_stream(r,[a,b]);self.assertEqual(result['reconnects'],1)
        self.assertEqual(result['status'],'completed')

    def test_reconnect_exhaustion(self):
        result,calls=self.run_stream(req(max_reconnects=1),[OSError('TEST-SECRET'),OSError('TEST-KEY')])
        self.assertEqual(result['error_code'],'reconnect_exhausted');self.assertEqual(len(calls),2)

    def test_tls_error_does_not_retry(self):
        import ssl
        result,calls=self.run_stream(req(),[ssl.SSLCertVerificationError('TEST-SECRET')])
        self.assertEqual(result['error_code'],'tls_certificate');self.assertEqual(len(calls),1)

    def test_auth_timeout_is_bounded(self):
        r=req(duration=30,max_reconnects=0)
        result,_=self.run_stream(r,[Socket(self.clock)])
        self.assertEqual(result['error_code'],'reconnect_exhausted');self.assertLessEqual(self.clock.value,8)

    def test_subscription_mismatch(self):
        r=req(duration=10);ws=Socket(self.clock,frame({'T':'success','msg':'authenticated'}),frame({'T':'subscription','bars':['MSFT']}))
        result,calls=self.run_stream(r,[ws]);self.assertEqual(result['status'],'failed');self.assertEqual(len(calls),1)

    def test_no_data_has_explicit_status(self):
        r=req(duration=3);ws=socket_for(self.clock,r)
        result,_=self.run_stream(r,[ws]);self.assertEqual(result['status'],'no_data')
        self.assertEqual(result['reconnects'],0);self.assertTrue(result['subscribed'])

    def test_keyboard_interrupt_finishes_session(self):
        r=req();ws=socket_for(self.clock,r,frame(BAR),KeyboardInterrupt())
        result,_=self.run_stream(r,[ws]);self.assertEqual(result['status'],'stopped')
        self.assertEqual(result['events_inserted'],1);self.assertEqual(self.session(result['run_id'])[0],'stopped')

    def test_max_events_limits_batch(self):
        r=req(max_events=1);ws=socket_for(self.clock,r,frame(BAR,TRADE,QUOTE))
        result,_=self.run_stream(r,[ws]);self.assertEqual(result['events_received'],1)
        self.assertEqual(self.repo.connection.execute('select count(*) from stream_events').fetchone()[0],1)

    def test_bad_frame_preserves_prior_frames(self):
        r=req();ws=socket_for(self.clock,r,frame(BAR),'not-json')
        result,_=self.run_stream(r,[ws]);self.assertEqual(result['status'],'failed')
        self.assertEqual(result['events_received'],1);self.assertEqual(self.repo.count(),1)

    def test_cli_stream_mock_and_test_isolation(self):
        with tempfile.TemporaryDirectory() as tmp,patch.dict(os.environ,{},clear=True):
            env=Path(tmp)/'.env';env.write_text('ALPACA_API_KEY=TEST-KEY\nALPACA_API_SECRET=TEST-SECRET\nDATABASE_URL=sqlite:///:memory:\n')
            r=req(symbols=('FAKEPACA',),feed='test',channels=('bars','quotes'),max_events=1)
            ws=socket_for(self.clock,r,frame({**QUOTE,'S':'FAKEPACA'}))
            adapter=AlpacaStream(self.settings,connector=lambda *a:ws,clock=self.clock,sleep=self.clock.sleep)
            with patch('microbroker.cli.AlpacaStream',return_value=adapter),contextlib.redirect_stdout(io.StringIO()) as out,contextlib.redirect_stderr(io.StringIO()):
                code=main(['--env-file',str(env),'stream-market-data','--test-feed','--max-events','1'])
            result=json.loads(out.getvalue());self.assertEqual(code,0);self.assertEqual(result['feed'],'test')
            self.assertNotIn('TEST-',out.getvalue())

    def test_cli_invalid_request_fails_before_db_creation(self):
        with tempfile.TemporaryDirectory() as tmp,patch.dict(os.environ,{},clear=True):
            db=Path(tmp)/'test.db';env=Path(tmp)/'.env';env.write_text(f'DATABASE_URL=sqlite:///{db}\n')
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(main(['--env-file',str(env),'stream-market-data','--duration','-1']),2)
            self.assertFalse(db.exists())

    def test_error_and_auth_payloads_not_logged(self):
        out=io.StringIO();handler=logging.StreamHandler(out);self.logger.handlers=[handler];self.logger.setLevel(logging.DEBUG)
        result,_=self.run_stream(req(),[Socket(self.clock,frame({'T':'error','code':402,'msg':'TEST-SECRET TEST-KEY'}))])
        self.assertNotIn('TEST-',out.getvalue());self.assertNotIn('TEST-',json.dumps(result))

class LocalWebSocketIntegration(unittest.TestCase):
    def test_real_socket_auth_subscription_frames_sqlite(self):
        try:
            from websockets.sync.server import serve
            from websockets.sync.client import connect
        except ImportError:
            self.skipTest('Install websockets>=16,<17 for local transport integration')
        observed=[];failures=[]
        def handler(ws):
            try:
                ws.send(frame({'T':'success','msg':'connected'}))
                auth=json.loads(ws.recv(timeout=3));observed.append(auth['action'])
                if auth['key']!='TEST-KEY':raise ValueError('wrong fake auth')
                ws.send(frame({'T':'success','msg':'authenticated'}))
                sub=json.loads(ws.recv(timeout=3));observed.append(sub['action'])
                ws.send(frame({'T':'subscription',**{k:v for k,v in sub.items() if k!='action'}}))
                ws.send(frame(BAR,QUOTE,{**BAR,'T':'u','c':102}))
                try:ws.recv(timeout=3)
                except Exception:pass
            except Exception as exc:failures.append(type(exc).__name__)
        with serve(handler,'127.0.0.1',0,compression=None) as server:
            port=server.socket.getsockname()[1]
            thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
            repo=Repository('sqlite:///:memory:')
            try:
                adapter=AlpacaStream(Settings(api_key='TEST-KEY',api_secret='TEST-SECRET'),connector=lambda u,t:connect(f'ws://127.0.0.1:{port}',proxy=None,open_timeout=t))
                result=adapter.run(req(max_events=3,duration=10),repo)
                self.assertEqual(result['status'],'completed');self.assertEqual(result['events_received'],3)
                self.assertEqual(repo.count(),1);self.assertEqual(repo.connection.execute('select close from bars').fetchone()[0],'102')
                self.assertEqual(observed,['auth','subscribe']);self.assertEqual(failures,[])
            finally:
                repo.close();server.shutdown();thread.join(timeout=3)

    def test_real_socket_disconnect_reauth_resubscribe(self):
        try:
            from websockets.sync.server import serve
            from websockets.sync.client import connect
        except ImportError:
            self.skipTest('Install websockets>=16,<17 for local transport integration')
        observed=[]; failures=[]
        def handler(ws):
            try:
                auth=json.loads(ws.recv(timeout=3));observed.append(auth['action'])
                ws.send(frame({'T':'success','msg':'authenticated'}))
                sub=json.loads(ws.recv(timeout=3));observed.append(sub['action'])
                ws.send(frame({'T':'subscription',**{k:v for k,v in sub.items() if k!='action'}}))
                ws.send(frame(BAR))
                if len(observed)==2:
                    ws.close(code=1011,reason='simulated outage')
                else:
                    try:ws.recv(timeout=3)
                    except Exception:pass
            except Exception as exc:failures.append(type(exc).__name__)
        with serve(handler,'127.0.0.1',0,compression=None) as server:
            port=server.socket.getsockname()[1]
            thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
            repo=Repository('sqlite:///:memory:')
            try:
                adapter=AlpacaStream(Settings(api_key='TEST-KEY',api_secret='TEST-SECRET'),connector=lambda u,t:connect(f'ws://127.0.0.1:{port}',proxy=None,open_timeout=t),sleep=lambda n:None)
                result=adapter.run(req(max_events=2,duration=10),repo)
                self.assertEqual(result['status'],'completed');self.assertEqual(result['reconnects'],1)
                self.assertEqual(result['duplicates'],1);self.assertEqual(repo.count(),1)
                self.assertEqual(observed,['auth','subscribe','auth','subscribe']);self.assertEqual(failures,[])
            finally:
                repo.close();server.shutdown();thread.join(timeout=3)

class R1MigrationRegression(unittest.TestCase):
    def test_actual_r1_schema_and_data_survive_r2_init(self):
        import sqlite3
        with tempfile.TemporaryDirectory() as tmp:
            db=Path(tmp)/'legacy.db'
            with sqlite3.connect(db) as connection:
                connection.execute('''CREATE TABLE bars (
                provider TEXT NOT NULL,symbol TEXT NOT NULL,timestamp TEXT NOT NULL,
                timeframe TEXT NOT NULL,feed TEXT NOT NULL,adjustment TEXT NOT NULL,
                open TEXT NOT NULL,high TEXT NOT NULL,low TEXT NOT NULL,close TEXT NOT NULL,
                volume BIGINT NOT NULL,trade_count BIGINT NOT NULL,vwap TEXT,
                PRIMARY KEY(provider,symbol,timestamp,timeframe,feed,adjustment))''')
                connection.execute("INSERT INTO bars VALUES ('alpaca','AAPL','2026-01-05T05:00:00+00:00','1Day','iex','raw','100','102','99','101',10,3,'100.5')")
            repo=Repository(f'sqlite:///{db}')
            try:
                self.assertEqual(repo.count(),1)
                tables={row[0] for row in repo.connection.execute("select name from sqlite_master where type='table'")}
                self.assertTrue({'bars','collection_runs','stream_events','stream_sessions','stream_bar_state'}<=tables)
                self.assertEqual(repo.connection.execute('select close from bars').fetchone()[0],'101')
            finally:repo.close()
