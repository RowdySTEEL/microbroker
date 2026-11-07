from dataclasses import replace
import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit,parse_qs
from microbroker.config import Settings
from microbroker.cli import main
from microbroker.data.models import utc_datetime,DataValidationError
from microbroker.data.adapters.alpaca import Response,MarketDataError
from microbroker.data.news import NewsRequest,normalize_news,AlpacaNews,collect_news,news_summary
from microbroker.data.storage.repository import Repository

NEWS={'id':1,'headline':'Test company announces update','summary':'Test summary',
      'created_at':'2026-01-05T14:30:00Z','updated_at':'2026-01-05T14:31:00Z',
      'content':'<p>Research test body.</p>','symbols':['AAPL','MSFT'],'author':'Test Author',
      'source':'test-source','url':'https://example.com/story?utm_source=test#section'}

def req(**kw):
    return NewsRequest(('AAPL','MSFT'),utc_datetime('2026-01-05T00:00:00Z'),utc_datetime('2026-01-07T00:00:00Z'),**kw)

class NewsAdapter:
    pages=1
    def __init__(self,*rows,fail=False):self.rows=rows;self.fail=fail
    def iter_news(self,request):
        for row in self.rows:yield normalize_news(row)
        if self.fail:raise MarketDataError('TEST_SECRET_RAW_HTTP')

class R4Tests(unittest.TestCase):
    def setUp(self):
        self.repo=Repository('sqlite:///:memory:')
        self.settings=Settings(api_key='TEST_KEY',api_secret='TEST_SECRET')
    def tearDown(self):self.repo.close()
    def collect(self,*rows,fail=False):return collect_news(NewsAdapter(*rows,fail=fail),self.repo,req())
    def summary(self):return news_summary(self.repo,symbols=('AAPL','MSFT'))
    def response(self,news,token=None):return Response(200,{},json.dumps({'news':news,'next_page_token':token}).encode())
    def test_normalization_whitelist_and_plaintext(self):
        article=normalize_news({**NEWS,'content':'<p>Text &amp; value</p><script>HIDE</script><style>HIDE</style><p>End</p>','credential':'SECRET'})
        self.assertEqual(article['data']['content_text'],'Text & value End')
        self.assertEqual(article['data']['url'],'https://example.com/story')
        self.assertNotIn('credential',article['data'])
    def test_tags_and_explicit_cashtags_no_company_guessing(self):
        article=normalize_news({**NEWS,'symbols':['MSFT','msft','UNKNOWN'],'headline':'Apple and $AAPL with $NVDA and $AAPLX'})
        self.assertEqual(article['mappings'],{'MSFT':'provider_tag','AAPL':'explicit_cashtag','NVDA':'explicit_cashtag'})
        article=normalize_news({**NEWS,'symbols':[],'headline':'Apple and Microsoft','content':''})
        self.assertEqual(article['mappings'],{})
    def test_idempotency_counts(self):
        a=self.collect(NEWS,NEWS)
        self.assertEqual(a['new_articles'],1);self.assertEqual(a['exact_duplicates'],1)
        b=self.collect(NEWS)
        self.assertEqual(b['new_versions'],0);self.assertEqual(b['exact_duplicates'],1)
        s=self.summary();self.assertEqual((s['articles_total'],s['versions_total']),(1,1))
    def test_revision_and_latest_symbol_mapping(self):
        self.collect(NEWS)
        updated={**NEWS,'updated_at':'2026-01-05T14:32:00Z','headline':'Updated title','symbols':['MSFT']}
        result=self.collect(updated)
        self.assertEqual(result['updated_articles'],1)
        s=self.summary();self.assertEqual(s['versions_total'],2);self.assertEqual(s['symbol_counts'],{'AAPL':0,'MSFT':1})
    def test_stale_revision_not_promoted(self):
        updated={**NEWS,'updated_at':'2026-01-05T14:32:00Z','headline':'Newest'}
        self.collect(updated)
        result=self.collect(NEWS)
        self.assertEqual(result['stale_versions'],1)
        self.assertEqual(self.summary()['samples'][0]['headline'],'Newest')
    def test_metadata_does_not_erase_same_timestamp_body(self):
        self.collect(NEWS)
        metadata={k:v for k,v in NEWS.items() if k!='content'}
        result=self.collect(metadata)
        self.assertEqual(result['stale_versions'],1)
        self.assertEqual(self.summary()['contentless_articles'],0)
    def test_same_timestamp_full_content_upgrades_metadata(self):
        self.collect({k:v for k,v in NEWS.items() if k!='content'})
        result=self.collect(NEWS)
        self.assertEqual(result['updated_articles'],1)
        self.assertEqual(self.summary()['contentless_articles'],0)
    def test_newer_contentless_revision_is_explicit(self):
        self.collect(NEWS)
        self.collect({**NEWS,'updated_at':'2026-01-05T14:32:00Z','content':None})
        self.assertEqual(self.summary()['contentless_articles'],1)
        self.assertEqual(self.summary()['versions_total'],2)
    def test_partial_page_rollback_includes_versions_and_mapping(self):
        self.collect(NEWS)
        with self.assertRaises(MarketDataError):self.collect({**NEWS,'id':2},fail=True)
        s=self.summary();self.assertEqual(s['articles_total'],1);self.assertEqual(s['versions_total'],1)
        audit=self.repo.connection.execute("SELECT result_json FROM news_collection_runs WHERE status='failed'").fetchone()[0]
        self.assertNotIn('TEST_SECRET',audit)
        self.assertEqual(json.loads(audit)['committed_articles'],0)
    def test_update_rollback_preserves_old_body_and_symbols(self):
        self.collect(NEWS)
        with self.assertRaises(MarketDataError):self.collect({**NEWS,'headline':'New','updated_at':'2026-01-05T14:32:00Z','symbols':[]},fail=True)
        s=self.summary();self.assertEqual(s['samples'][0]['headline'],NEWS['headline'])
        self.assertEqual(s['symbol_counts'],{'AAPL':1,'MSFT':1})
    def test_audit_failure_rolls_back_all_news(self):
        self.repo.connection.execute("CREATE TRIGGER fail_success BEFORE INSERT ON news_collection_runs WHEN NEW.status='completed' BEGIN SELECT RAISE(ABORT,'test'); END");self.repo.connection.commit()
        with self.assertRaises(Exception):self.collect(NEWS)
        self.assertEqual(self.summary()['articles_total'],0)
    def test_provider_isolation(self):
        self.collect(NEWS)
        class Sandbox:
            pages=1
            def iter_news(inner,request):yield normalize_news(NEWS,provider='alpaca-sandbox')
        collect_news(Sandbox(),self.repo,req())
        self.assertEqual(self.summary()['articles_total'],1)
        self.assertEqual(news_summary(self.repo,provider='alpaca-sandbox')['articles_total'],1)
    def test_duplicate_candidate_not_merged(self):
        self.collect(NEWS,{**NEWS,'id':2,'url':'https://example.com/story?utm_campaign=other'})
        s=self.summary();self.assertEqual(s['articles_total'],2)
        self.assertEqual(s['duplicate_candidate_groups'][0]['articles'],2)
        self.assertFalse(s['duplicate_candidates_merged'])
    def test_empty_and_unmapped(self):
        self.assertEqual(self.collect()['status'],'empty')
        result=self.collect({**NEWS,'symbols':[],'content':''})
        self.assertEqual(result['unmapped_received'],1)
        self.assertEqual(self.summary()['unmapped_articles'],1)
    def test_invalid_provider_row_fields(self):
        for change in ({'id':True},{'id':-1},{'id':'1'},{'headline':''},{'symbols':'AAPL'},{'symbols':['bad symbol']},
                       {'updated_at':'2026-01-04T00:00:00Z'},{'created_at':'no-time'},{'url':'javascript:bad'},
                       {'content':['bad']},{'url':'https://user:password@example.com'}):
            with self.subTest(change=change),self.assertRaises(DataValidationError):normalize_news({**NEWS,**change})
    def test_optional_fields_missing_and_utc(self):
        data=normalize_news({'id':2,'headline':'Test','created_at':'2026-01-05T17:30:00+03:00','updated_at':'2026-01-05T17:31:00+03:00'})['data']
        self.assertEqual(data['created_at'],'2026-01-05T14:30:00.000000000Z')
        self.assertEqual(data['content_text'],'')
    def test_provider_filter_not_local_created_time_clip(self):
        older={**NEWS,'created_at':'2025-12-01T00:00:00Z'}
        result=self.collect(older)
        self.assertEqual(result['new_articles'],1)
    def test_pagination_query_and_auth(self):
        calls=[];responses=[self.response([NEWS],'next'),self.response([{**NEWS,'id':2}])]
        def get(url,headers,timeout):calls.append((url,headers));return responses.pop(0)
        adapter=AlpacaNews(self.settings,transport=get)
        self.assertEqual(len(list(adapter.iter_news(req()))),2)
        query=parse_qs(urlsplit(calls[0][0]).query)
        self.assertEqual(urlsplit(calls[0][0]).path,'/v1beta1/news')
        self.assertEqual(query['include_content'],['true']);self.assertEqual(query['exclude_contentless'],['false'])
        self.assertEqual(parse_qs(urlsplit(calls[1][0]).query)['page_token'],['next'])
        self.assertEqual(calls[0][1]['APCA-API-SECRET-KEY'],'TEST_SECRET')
    def test_broker_auth_and_sandbox_host(self):
        settings=replace(self.settings,auth_mode='broker',data_env='sandbox')
        seen=[]
        def get(url,headers,timeout):seen.append((url,headers));return self.response([NEWS])
        article=list(AlpacaNews(settings,transport=get).iter_news(req()))[0]
        self.assertEqual(article['provider'],'alpaca-sandbox')
        self.assertEqual(urlsplit(seen[0][0]).hostname,'data.sandbox.alpaca.markets')
        self.assertIn('Authorization',seen[0][1])
    def test_http_forbidden_redacts_body(self):
        adapter=AlpacaNews(self.settings,transport=lambda *a:Response(403,{},b'TEST_SECRET'))
        with self.assertRaises(MarketDataError) as raised:list(adapter.iter_news(req()))
        self.assertNotIn('TEST_SECRET',str(raised.exception));self.assertIn('403',str(raised.exception))
    def test_rate_limit_retry(self):
        responses=[Response(429,{'Retry-After':'1'},b''),self.response([NEWS])];sleeps=[]
        adapter=AlpacaNews(self.settings,transport=lambda *a:responses.pop(0),sleep=sleeps.append)
        self.assertEqual(len(list(adapter.iter_news(req()))),1);self.assertEqual(sleeps,[1])
    def test_repeated_token_rollback(self):
        adapter=AlpacaNews(self.settings,transport=lambda *a:self.response([NEWS],'same'))
        with self.assertRaises(MarketDataError):collect_news(adapter,self.repo,req())
        self.assertEqual(self.summary()['articles_total'],0)
    def test_max_pages_rolls_back_instead_of_claiming_complete(self):
        adapter=AlpacaNews(self.settings,transport=lambda *a:self.response([NEWS],'next'))
        with self.assertRaises(MarketDataError):collect_news(adapter,self.repo,req(max_pages=1))
        self.assertEqual(self.summary()['articles_total'],0)
    def test_malformed_news_response(self):
        for body in (b'not json',b'{"news":{}}',b'[]'):
            adapter=AlpacaNews(self.settings,transport=lambda *a:Response(200,{},body))
            with self.assertRaises(MarketDataError):list(adapter.iter_news(req()))
    def test_request_validation(self):
        for values in ({'limit':0},{'limit':51},{'max_pages':0},{'limit':True}):
            with self.assertRaises(DataValidationError):req(**values)
    def test_cli_collection_and_summary(self):
        with tempfile.TemporaryDirectory() as folder:
            settings=replace(self.settings,database_url='sqlite:///'+str(Path(folder)/'db'))
            captured=io.StringIO()
            with patch('microbroker.cli.Settings.from_env',return_value=settings),patch('microbroker.cli.AlpacaNews',return_value=NewsAdapter(NEWS)),contextlib.redirect_stdout(captured):
                code=main(['collect-news','--symbols','AAPL,MSFT','--start','2026-01-05T00:00:00Z','--end','2026-01-07T00:00:00Z'])
            self.assertEqual(code,0);self.assertEqual(json.loads(captured.getvalue())['new_articles'],1)
            captured=io.StringIO()
            with patch('microbroker.cli.Settings.from_env',return_value=settings),contextlib.redirect_stdout(captured):
                self.assertEqual(main(['news-summary','--symbols','AAPL']),0)
            self.assertEqual(json.loads(captured.getvalue())['symbol_counts'],{'AAPL':1})
    def test_first_seen_revision_does_not_change_on_repeat(self):
        self.collect(NEWS)
        before=self.repo.connection.execute('SELECT first_seen_at FROM news_versions').fetchone()[0]
        self.repo.connection.commit()
        self.collect(NEWS)
        after=self.repo.connection.execute('SELECT first_seen_at FROM news_versions').fetchone()[0]
        self.assertEqual(before,after)
    def test_malformed_second_article_rolls_back(self):
        with self.assertRaises(DataValidationError):self.collect(NEWS,{**NEWS,'id':True})
        self.assertEqual(self.summary()['articles_total'],0)
