import contextlib
import hashlib
from http.client import IncompleteRead
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
from urllib.request import Request, urlopen

from microbroker.cli import main
from microbroker.intelligence.model_files import prepare_finbert, verify_model_files
from microbroker.intelligence.sentiment import AnalysisError

class Response(io.BytesIO):
    def __init__(self, data, *, status=200, headers=None):
        super().__init__(data); self.status=status; self.headers=headers or {}

class ResumeTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.folder=Path(self.temp.name)
        self.value=b'0123456789abcdef'
        self.expected={'weights.bin':(len(self.value),hashlib.sha256(self.value).hexdigest())}
        self.part=self.folder/'weights.bin.part'
        self.patches=[patch('microbroker.intelligence.model_files.EXPECTED_FILES',self.expected),
                      patch('microbroker.intelligence.model_files.RANGE_BYTES',8)]
        for p in self.patches:p.start()
    def tearDown(self):
        for p in reversed(self.patches):p.stop()
        self.temp.cleanup()
    def range_response(self, request):
        start,end=map(int,request.get_header('Range').removeprefix('bytes=').split('-'))
        return Response(self.value[start:end+1],status=206,headers={'Content-Range':f'bytes {start}-{end}/{len(self.value)}'})
    def prepare(self, opener):return prepare_finbert(self.folder,opener=opener,sleep=lambda n:None)
    def test_existing_partial_resumes_from_exact_offset_in_bounded_ranges(self):
        self.part.write_bytes(self.value[:3]); seen=[]
        def opener(request,**kw):seen.append(request.get_header('Range'));return self.range_response(request)
        self.assertEqual(self.prepare(opener)['status'],'ready')
        self.assertEqual(seen,['bytes=3-10','bytes=11-15'])
        self.assertEqual((self.folder/'weights.bin').read_bytes(),self.value)
    def test_short_response_retries_after_saved_bytes(self):
        seen=[]
        def opener(request,**kw):
            seen.append(request.get_header('Range'))
            if len(seen)==1:return Response(self.value[:3],status=206,headers={'Content-Range':'bytes 0-7/16'})
            return self.range_response(request)
        self.prepare(opener)
        self.assertEqual(seen[:2],['bytes=0-7','bytes=3-10'])
    def test_incomplete_read_payload_is_saved_before_retry(self):
        seen=[]
        class Interrupted(Response):
            def read(self,n):raise IncompleteRead(b'012',5)
        def opener(request,**kw):
            seen.append(request.get_header('Range'))
            if len(seen)==1:return Interrupted(b'',status=206,headers={'Content-Range':'bytes 0-7/16'})
            return self.range_response(request)
        self.prepare(opener); self.assertEqual(seen[1],'bytes=3-10')
    def test_all_retries_fail_partial_survives_next_command(self):
        calls=[]
        def broken(request,**kw):
            calls.append(request.get_header('Range'))
            if len(calls)==1:return Response(self.value[:3],status=206,headers={'Content-Range':'bytes 0-7/16'})
            raise TimeoutError('TEST_SECRET_NETWORK_URL')
        with self.assertRaises(AnalysisError) as raised:self.prepare(broken)
        self.assertEqual(self.part.read_bytes(),self.value[:3]);self.assertNotIn('TEST_SECRET',str(raised.exception))
        seen=[]
        def repaired(request,**kw):seen.append(request.get_header('Range'));return self.range_response(request)
        self.prepare(repaired);self.assertEqual(seen[0],'bytes=3-10')
    def test_server_ignoring_range_restarts_without_appending_duplicate_bytes(self):
        self.part.write_bytes(self.value[:3])
        self.prepare(lambda *a,**kw:Response(self.value))
        self.assertEqual((self.folder/'weights.bin').read_bytes(),self.value)
    def test_wrong_range_is_rejected_without_changing_partial(self):
        self.part.write_bytes(self.value[:3])
        for content_range in ('bytes 0-7/16','bytes 3-10/17','bytes 3-15/16','bad'):
            with self.subTest(content_range=content_range),self.assertRaises(AnalysisError):
                self.prepare(lambda *a,**kw:Response(self.value[3:11],status=206,headers={'Content-Range':content_range}))
            self.assertEqual(self.part.read_bytes(),self.value[:3])
    def test_complete_partial_is_validated_and_promoted_without_network(self):
        self.part.write_bytes(self.value)
        self.prepare(lambda *a,**kw:self.fail('must not redownload complete valid partial'))
        self.assertFalse(self.part.exists());self.assertEqual((self.folder/'weights.bin').read_bytes(),self.value)
    def test_hidden_metadata_cannot_block_verified_model_files(self):
        self.prepare(lambda *a,**kw:Response(self.value))
        (self.folder/'.DS_Store').write_bytes(b'harmless-metadata')
        self.assertEqual(verify_model_files(self.folder)['model'],'ProsusAI/finbert')
    def test_partial_inference_error_tells_user_to_resume(self):
        self.part.write_bytes(b'012')
        with self.assertRaisesRegex(AnalysisError,'prepare-finbert to resume'):verify_model_files(self.folder)
    def test_checksum_failure_cannot_finalize_a_model(self):
        with self.assertRaises(AnalysisError):self.prepare(lambda *a,**kw:Response(b'x'*16))
        self.assertFalse(self.part.exists());self.assertFalse((self.folder/'weights.bin').exists())
    def test_keyboard_interrupt_preserves_partial(self):
        class Interrupted(Response):
            def __init__(self):super().__init__(b'');self.calls=0
            def read(self,n):
                self.calls+=1
                if self.calls==1:return b'012'
                raise KeyboardInterrupt
        with self.assertRaises(KeyboardInterrupt):self.prepare(lambda *a,**kw:Interrupted())
        self.assertEqual(self.part.read_bytes(),b'012')
    def test_compressed_response_rejected_without_changing_partial(self):
        self.part.write_bytes(b'012')
        with self.assertRaises(AnalysisError):self.prepare(lambda *a,**kw:Response(b'bad',headers={'Content-Encoding':'gzip'}))
        self.assertEqual(self.part.read_bytes(),b'012')
    def test_local_http_server_disconnect_and_real_range_resume(self):
        payload=self.value; seen=[]
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args):pass
            def do_GET(self):
                header=self.headers['Range'];seen.append(header)
                start,end=map(int,header.removeprefix('bytes=').split('-'))
                self.send_response(206);self.send_header('Content-Range',f'bytes {start}-{end}/16')
                self.send_header('Content-Length',str(end-start+1));self.end_headers()
                self.wfile.write(payload[start:(3 if len(seen)==1 else end+1)])
                self.wfile.flush();self.close_connection=True
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        def opener(request,timeout):
            return urlopen(Request(f'http://127.0.0.1:{server.server_port}/weights',headers=dict(request.header_items())),timeout=5)
        try:self.prepare(opener)
        finally:server.shutdown();server.server_close();thread.join(timeout=5)
        self.assertEqual(seen[:2],['bytes=0-7','bytes=3-10'])
        self.assertEqual((self.folder/'weights.bin').read_bytes(),payload)
    def test_doctor_reports_missing_env_path_without_credentials(self):
        output=io.StringIO();env=self.folder/'missing.env'
        with patch.dict(os.environ,{},clear=True),contextlib.redirect_stdout(output):
            self.assertEqual(main(['--env-file',str(env),'doctor']),0)
        data=json.loads(output.getvalue());self.assertFalse(data['env_file_found'])
        self.assertEqual(data['env_file_path'],str(env));self.assertFalse(data['credentials_present'])
    def test_strict_doctor_stops_when_postgres_config_not_loaded(self):
        with patch.dict(os.environ,{},clear=True),contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['--env-file',str(self.folder/'missing.env'),'doctor','--require-postgres']),2)
    def test_strict_doctor_accepts_complete_file_without_exposing_values(self):
        env=self.folder/'local.env'
        env.write_text('DATABASE_URL=postgresql://test:TEST_DB_SECRET@localhost/test\nALPACA_API_KEY=TEST_KEY\nALPACA_API_SECRET=TEST_API_SECRET\n')
        output=io.StringIO()
        with patch.dict(os.environ,{},clear=True),contextlib.redirect_stdout(output):
            self.assertEqual(main(['--env-file',str(env),'doctor','--require-postgres','--require-credentials']),0)
        self.assertNotIn('TEST_API_SECRET',output.getvalue());self.assertNotIn('TEST_DB_SECRET',output.getvalue())
