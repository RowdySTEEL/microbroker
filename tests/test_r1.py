import contextlib
from decimal import Decimal
import io
import json
import logging
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import URLError
from urllib.parse import urlparse, parse_qs
from microbroker.config import Settings, ConfigurationError
from microbroker.data.adapters.alpaca import AlpacaHistoricalData, Response, MarketDataError, normalize_bar, NoRedirect
from microbroker.data.ingestion.collector import collect_historical
from microbroker.data.models import utc_datetime, DataValidationError
from microbroker.data.request import BarsRequest
from microbroker.data.storage.repository import Repository
from microbroker.cli import main

ROW = {'t':'2026-01-05T05:00:00Z','o':100.25,'h':102,'l':99,'c':101,'v':10,'n':3,'vw':100.5}

def request(**overrides):
    values = dict(symbols=('AAPL','MSFT'), start=utc_datetime('2026-01-05T00:00:00Z'), end=utc_datetime('2026-01-07T00:00:00Z'))
    values.update(overrides)
    return BarsRequest(**values)

def response(bars=None, token=None, status=200, headers=None, body=None):
    return Response(status, headers or {}, body if body is not None else json.dumps({'bars':bars or {},'next_page_token':token}).encode())

class FakeTransport:
    def __init__(self, *responses):
        self.responses=list(responses)
        self.calls=[]
    def __call__(self, url, headers, timeout):
        self.calls.append((url, headers, timeout))
        result=self.responses.pop(0)
        if isinstance(result, Exception): raise result
        return result

class R1Tests(unittest.TestCase):
    def setUp(self):
        logging.getLogger('microbroker').handlers=[]
        logging.getLogger('microbroker').addHandler(logging.NullHandler())
        self.repo=Repository('sqlite:///:memory:')
        self.settings=Settings(api_key='TEST-KEY',api_secret='TEST-SECRET')
    def tearDown(self): self.repo.close()
    def adapter(self, transport, sleep=lambda n:None, settings=None):
        return AlpacaHistoricalData(settings or self.settings,transport=transport,sleep=sleep)

    def test_pagination_multiple_symbols_and_utc_query(self):
        transport=FakeTransport(response({'AAPL':[ROW]},'next'),response({'MSFT':[ROW]}))
        bars=list(self.adapter(transport).iter_bars(request(limit=1)))
        self.assertEqual([b.symbol for b in bars],['AAPL','MSFT'])
        params=parse_qs(urlparse(transport.calls[1][0]).query)
        self.assertEqual(params['page_token'],['next'])
        self.assertEqual(params['asof'],['-'])
        self.assertEqual(params['sort'],['asc'])
        self.assertEqual(transport.calls[0][1]['APCA-API-KEY-ID'],'TEST-KEY')
        self.assertIsInstance(bars[0].open,Decimal)
        self.assertEqual(bars[0].timestamp.utcoffset().total_seconds(),0)

    def test_broker_sandbox_basic_auth(self):
        settings=Settings(api_key='TEST-KEY',api_secret='TEST-SECRET',auth_mode='broker',data_env='sandbox')
        transport=FakeTransport(response())
        list(self.adapter(transport,settings=settings).iter_bars(request()))
        self.assertEqual(urlparse(transport.calls[0][0]).hostname,'data.sandbox.alpaca.markets')
        self.assertTrue(transport.calls[0][1]['Authorization'].startswith('Basic '))
        self.assertNotIn('APCA-API-KEY-ID',transport.calls[0][1])

    def test_429_then_503_then_success(self):
        transport=FakeTransport(response(status=429,headers={'retry-after':'2'}),response(status=503),response({'AAPL':[ROW]}))
        delays=[]
        self.assertEqual(len(list(self.adapter(transport,sleep=delays.append).iter_bars(request()))),1)
        self.assertEqual(delays,[2,2])

    def test_network_retry(self):
        transport=FakeTransport(URLError('TEST-SECRET'),response())
        list(self.adapter(transport).iter_bars(request()))
        self.assertEqual(len(transport.calls),2)

    def test_network_retry_exhausted_redacted(self):
        transport=FakeTransport(*[URLError('TEST-SECRET')]*4)
        with self.assertRaises(MarketDataError) as caught:
            list(self.adapter(transport).iter_bars(request()))
        self.assertNotIn('TEST-SECRET',str(caught.exception))
        self.assertEqual(len(transport.calls),4)

    def test_retry_exhausted(self):
        transport=FakeTransport(*[response(status=429)]*4)
        with self.assertRaises(MarketDataError): list(self.adapter(transport).iter_bars(request()))
        self.assertEqual(len(transport.calls),4)

    def test_nonretryable_and_redacted_http_errors(self):
        for status in (400,401,403,302):
            with self.subTest(status=status):
                transport=FakeTransport(response(status=status,body=b'TEST-SECRET TEST-KEY'))
                with self.assertRaises(MarketDataError) as caught:
                    list(self.adapter(transport).iter_bars(request()))
                self.assertNotIn('TEST-',str(caught.exception))
                self.assertEqual(len(transport.calls),1)

    def test_retry_delay_clamp(self):
        adapter=self.adapter(FakeTransport())
        self.assertEqual(adapter._delay(0,{'Retry-After':'999'}),60)
        self.assertEqual(adapter._delay(0,{'Retry-After':'invalid'}),1)

    def test_redirect_blocked(self):
        self.assertIsNone(NoRedirect().redirect_request(None,None,302,'',{},'https://elsewhere.invalid'))

    def test_malformed_responses(self):
        for body in (b'not json',b'{"bars":[]}',b'{}'):
            with self.subTest(body=body), self.assertRaises(MarketDataError):
                list(self.adapter(FakeTransport(response(body=body))).iter_bars(request()))

    def test_unexpected_symbol_and_row_structure(self):
        for payload in ({'TSLA':[ROW]},{'AAPL':'bad'}):
            with self.assertRaises(MarketDataError):
                list(self.adapter(FakeTransport(response(payload))).iter_bars(request()))

    def test_repeated_pagination_token(self):
        transport=FakeTransport(response({'AAPL':[ROW]},'repeat'),response({'AAPL':[ROW]},'repeat'))
        with self.assertRaises(MarketDataError): self.repo.collect(self.adapter(transport).iter_bars(request()),request())
        self.assertEqual(self.repo.count(),0)

    def test_empty_page_with_token_continues(self):
        adapter=self.adapter(FakeTransport(response(token='next'),response({'AAPL':[ROW]})))
        self.assertEqual(len(list(adapter.iter_bars(request()))),1)
        self.assertEqual(adapter.pages,2)

    def test_decimal_precision(self):
        row={**ROW,'o':'100.250000000000000001'}
        bar=normalize_bar('AAPL',row,request())
        self.repo.collect([bar],request())
        self.assertEqual(self.repo.connection.execute('select open from bars').fetchone()[0],'100.250000000000000001')

    def test_optional_vwap(self):
        row={k:v for k,v in ROW.items() if k!='vw'}
        self.assertIsNone(normalize_bar('AAPL',row,request()).vwap)

    def test_invalid_bars_rejected(self):
        changes=({'h':90},{'l':110},{'o':'NaN'},{'v':-1},{'v':1.5},{'n':True},{'vw':'Infinity'},{'t':'2026-01-05'},{'t':'2020-01-01T00:00:00Z'})
        for change in changes:
            with self.subTest(change=change),self.assertRaises(DataValidationError):
                normalize_bar('AAPL',{**ROW,**change},request())
        with self.assertRaises(DataValidationError): normalize_bar('AAPL',{},request())

    def test_invalid_requests(self):
        for changes in ({'timeframe':'60Min'},{'timeframe':'24Hour'},{'feed':'other'},{'limit':0},{'limit':10001},{'asof':'2026-01-01'},{'adjustment':'other'},{'end':utc_datetime('2026-01-01T00:00:00Z')}):
            with self.subTest(changes=changes),self.assertRaises(DataValidationError): request(**changes)

    def test_no_timezone_rejected(self):
        with self.assertRaises(DataValidationError): request(start=request().start.replace(tzinfo=None))

    def test_idempotency_and_update(self):
        bar=normalize_bar('AAPL',ROW,request())
        self.repo.collect([bar],request())
        self.repo.collect([bar],request())
        self.assertEqual(self.repo.count(),1)
        new=normalize_bar('AAPL',{**ROW,'c':102},request())
        self.repo.collect([new],request())
        self.assertEqual(self.repo.count(),1)
        self.assertEqual(self.repo.connection.execute('select close from bars').fetchone()[0],'102')
        self.assertEqual(self.repo.connection.execute('select count(*) from collection_runs').fetchone()[0],3)

    def test_feed_adjustment_timeframe_separation(self):
        for r in (request(),request(feed='sip'),request(adjustment='split'),request(timeframe='1Hour')):
            self.repo.collect([normalize_bar('AAPL',ROW,r)],r)
        self.assertEqual(self.repo.count(),4)

    def test_rollback_preserves_existing_and_audits_failure(self):
        bar=normalize_bar('AAPL',ROW,request())
        self.repo.collect([bar],request())
        def broken():
            yield normalize_bar('AAPL',{**ROW,'c':102},request())
            yield normalize_bar('MSFT',ROW,request())
            raise MarketDataError('TEST-SECRET')
        with self.assertRaises(MarketDataError): self.repo.collect(broken(),request())
        self.assertEqual(self.repo.count(),1)
        self.assertEqual(self.repo.connection.execute('select close from bars').fetchone()[0],'101')
        status,result=self.repo.connection.execute("select status,result_json from collection_runs where status='failed'").fetchone()
        self.assertEqual(status,'failed')
        self.assertEqual(json.loads(result)['committed_bars'],0)
        self.assertNotIn('TEST-',result)

    def test_collection_audit_and_missing_symbols(self):
        adapter=self.adapter(FakeTransport(response({'AAPL':[ROW]})))
        result=collect_historical(adapter,self.repo,request())
        self.assertEqual(result['pages'],1)
        self.assertEqual(result['missing_symbols'],['MSFT'])
        metadata=json.loads(self.repo.connection.execute('select request_json from collection_runs').fetchone()[0])
        self.assertEqual(metadata['symbols'],['AAPL','MSFT'])
        self.assertNotIn('TEST-',json.dumps(metadata))

    def test_empty_result(self):
        result=self.repo.collect([],request())
        self.assertEqual(result['status'],'empty')
        self.assertEqual(result['missing_symbols'],['AAPL','MSFT'])

    def test_schema_initialization_idempotent(self):
        self.repo.migrate(); self.repo.migrate()
        self.assertEqual(self.repo.count(),0)

    def test_cli_end_to_end_mock_http_to_file_db(self):
        with tempfile.TemporaryDirectory() as tmp,patch.dict(os.environ,{},clear=True):
            path=Path(tmp)/'.env'; db=Path(tmp)/'test.db'
            path.write_text(f'ALPACA_API_KEY=TEST-KEY\nALPACA_API_SECRET=TEST-SECRET\nDATABASE_URL=sqlite:///{db}\n')
            transport=FakeTransport(response({'AAPL':[ROW]}))
            def factory(settings): return self.adapter(transport,settings=settings)
            with patch('microbroker.cli.AlpacaHistoricalData',side_effect=factory),contextlib.redirect_stdout(io.StringIO()) as out,contextlib.redirect_stderr(io.StringIO()):
                code=main(['--env-file',str(path),'collect-bars','--symbols','AAPL','--start','2026-01-05T00:00:00Z','--end','2026-01-07T00:00:00Z'])
            self.assertEqual(code,0)
            self.assertEqual(json.loads(out.getvalue())['received_bars'],1)
            repo=Repository(f'sqlite:///{db}')
            try: self.assertEqual(repo.count(),1)
            finally: repo.close()

    def test_cli_empty_exit_three(self):
        with tempfile.TemporaryDirectory() as tmp,patch.dict(os.environ,{},clear=True):
            path=Path(tmp)/'.env'
            path.write_text('ALPACA_API_KEY=TEST-KEY\nALPACA_API_SECRET=TEST-SECRET\nDATABASE_URL=sqlite:///:memory:\n')
            with patch('microbroker.cli.AlpacaHistoricalData',return_value=self.adapter(FakeTransport(response()))),contextlib.redirect_stdout(io.StringIO()),contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(main(['--env-file',str(path),'collect-bars','--start','2026-01-05T00:00:00Z','--end','2026-01-07T00:00:00Z']),3)

    def test_cli_credentials_missing_no_db_created(self):
        with tempfile.TemporaryDirectory() as tmp,patch.dict(os.environ,{},clear=True):
            path=Path(tmp)/'.env'; db=Path(tmp)/'test.db'
            path.write_text(f'DATABASE_URL=sqlite:///{db}\n')
            with contextlib.redirect_stdout(io.StringIO()),contextlib.redirect_stderr(io.StringIO()) as err:
                code=main(['--env-file',str(path),'collect-bars','--start','2026-01-05T00:00:00Z','--end','2026-01-07T00:00:00Z'])
            self.assertEqual(code,2)
            self.assertIn('ALPACA_API_KEY',err.getvalue())
            self.assertFalse(db.exists())

    def test_cli_doctor_secrets_not_exposed(self):
        with patch.dict(os.environ,{'ALPACA_API_KEY':'TEST-KEY','ALPACA_API_SECRET':'TEST-SECRET'},clear=True),contextlib.redirect_stdout(io.StringIO()) as out:
            self.assertEqual(main(['--env-file','/nonexistent','doctor']),0)
            self.assertNotIn('TEST-',out.getvalue())
