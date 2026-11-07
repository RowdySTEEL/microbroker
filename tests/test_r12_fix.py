import contextlib
import io
import unittest
from unittest.mock import patch, MagicMock
from urllib.parse import urlsplit, parse_qs
from urllib.error import URLError
from microbroker.data.adapters.alpaca import Response, http_get, MarketDataError
from microbroker.data.session import session_fixture, safe_error, parsed_session_result

class R12FixTests(unittest.TestCase):
    def test_native_adapter_empty_response_and_query(self):
        calls=[]
        def transport(url,headers,timeout):
            calls.append(url)
            self.assertEqual(timeout,30)
            self.assertEqual(headers['APCA-API-KEY-ID'],'FIXTURE')
            return Response(200,{},b'{"news":[],"next_page_token":null}')
        result,audit,_=session_fixture(native_transport=transport)
        self.assertEqual(result['status'],'no_data')
        self.assertGreaterEqual(result['news_polls'],1)
        self.assertEqual(parsed_session_result(audit[1]),result)
        parts=urlsplit(calls[0]);query=parse_qs(parts.query)
        self.assertEqual(parts.path,'/v1beta1/news')
        self.assertEqual(query['symbols'],['MSFT'])
        self.assertEqual(query['limit'],['50'])
        self.assertEqual(query['include_content'],['true'])
    def test_http_denial_audit_has_status_without_secret(self):
        for code in (400,401,403,429,503):
            with self.subTest(code=code):
                result,audit,_=session_fixture(native_transport=lambda *args:Response(code,{'secret':'SYNTHETIC_SECRET'},b'SYNTHETIC_SECRET'))
                self.assertEqual(result['error_details']['http_status'],code)
                self.assertEqual(result['error_details']['category'],'http_error')
                self.assertEqual(parsed_session_result(audit[1]),result)
                self.assertNotIn('SYNTHETIC_SECRET',str(result))
    def test_network_failure_diagnostic(self):
        def transport(*args):raise URLError('SYNTHETIC_SECRET')
        result,_,_=session_fixture(native_transport=transport)
        self.assertEqual(result['error_details']['category'],'network_error')
        self.assertNotIn('SYNTHETIC_SECRET',str(result))
    def test_invalid_payload_diagnostic(self):
        for body,category in ((b'SYNTHETIC_SECRET','invalid_json'),(b'{}','invalid_response')):
            result,_,_=session_fixture(native_transport=lambda *args:Response(200,{},body))
            self.assertEqual(result['error_details']['category'],category)
            self.assertNotIn('SYNTHETIC_SECRET',str(result))
    def test_local_request_error_type_retained_in_log_and_audit(self):
        def transport(*args):raise UnicodeEncodeError('ascii','SYNTHETIC_SECRET',0,1,'fixture')
        result,audit,_=session_fixture(native_transport=transport)
        self.assertEqual(result['error_details']['exception_type'],'UnicodeEncodeError')
        self.assertEqual(parsed_session_result(audit[1]),result)
        self.assertNotIn('SYNTHETIC_SECRET',str(result))
    def test_unknown_exception_type_and_untrusted_metadata_not_exposed(self):
        SecretError=type('SYNTHETIC_SECRET',(RuntimeError,),{})
        self.assertEqual(safe_error(SecretError('SYNTHETIC_SECRET'),'news_poll')['exception_type'],'OtherError')
        self.assertEqual(safe_error(MarketDataError('SYNTHETIC_SECRET',code='SYNTHETIC_SECRET',http_status='SYNTHETIC_SECRET'),'news_poll'),
            {'stage':'news_poll','exception_type':'MarketDataError'})
    def test_sqlstate_from_wrapped_database_error(self):
        exc=RuntimeError('SYNTHETIC_SECRET');inner=RuntimeError('SYNTHETIC_SECRET')
        inner.sqlstate='42501';exc.__context__=inner
        self.assertEqual(safe_error(exc,'news_poll')['sqlstate'],'42501')
        inner.sqlstate='SYNTHETIC_SECRET'
        self.assertNotIn('sqlstate',safe_error(exc,'news_poll'))
    def test_urllib_request_path_with_stub_opener(self):
        response=MagicMock();response.status=200;response.headers={};response.read.return_value=b'{"news":[]}'
        opener=MagicMock();opener.open.return_value.__enter__.return_value=response
        with patch('microbroker.data.adapters.alpaca.build_opener',return_value=opener):
            result=http_get('https://data.alpaca.markets/v1beta1/news?symbols=MSFT',{'APCA-API-KEY-ID':'FIXTURE'},30)
        request=opener.open.call_args.args[0]
        self.assertEqual(request.get_method(),'GET');self.assertEqual(result.status,200)
        self.assertEqual(opener.open.call_args.kwargs['timeout'],30)

if __name__=='__main__':unittest.main()
