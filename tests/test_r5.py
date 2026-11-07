import contextlib
import hashlib
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch, MagicMock

from microbroker.cli import main
from microbroker.config import Settings
from microbroker.data.models import DataValidationError, utc_datetime
from microbroker.data.news import NewsRequest, normalize_news, collect_news
from microbroker.data.storage.repository import Repository, StorageError
from microbroker.intelligence.analysis import AnalysisRequest, analyze_news, analysis_summary, compute_analysis
from microbroker.intelligence.policy import prepare_text, relevance, article_flags, digest
from microbroker.intelligence.sentiment import RulesEngine, FinBertEngine, AnalysisError, validate_sentiment
from microbroker.intelligence.model_files import prepare_finbert, verify_model_files
from microbroker.intelligence.verification import verify_analysis

NEWS = {'id': 1, 'headline': 'Microsoft beats expectations and raises guidance.',
        'summary': '', 'content': '<p>Microsoft revenue rose.</p>', 'symbols': ['MSFT'],
        'created_at': '2026-01-05T14:30:00Z', 'updated_at': '2026-01-05T14:30:00Z',
        'source': 'test', 'url': 'https://example.com/test'}

class Fixture:
    pages = 1
    def __init__(self, rows, provider='alpaca'): self.rows = rows; self.provider = provider
    def iter_news(self, request):
        for row in self.rows: yield normalize_news(row, provider=self.provider)

class R5Tests(unittest.TestCase):
    def setUp(self):
        self.repo = Repository('sqlite:///:memory:')
        self.request = AnalysisRequest(symbols=('AAPL', 'MSFT'))
    def tearDown(self): self.repo.close()
    def collect(self, *rows, provider='alpaca'):
        collect_news(Fixture(rows, provider), self.repo, NewsRequest(('AAPL','MSFT'),
            utc_datetime('2026-01-05T00:00:00Z'), utc_datetime('2026-01-06T00:00:00Z')))
    def analyze(self, engine=None): return analyze_news(self.repo, self.request, engine or RulesEngine())
    def summary(self, **kwargs): return analysis_summary(self.repo, self.request, engine='rules', **kwargs)
    def article(self, row):
        normalized = normalize_news(row)
        return {'data': normalized['data'], 'mappings': normalized['mappings'], 'first_seen': '2026-01-06T00:00:00Z'}

    def test_positive_negative_and_unscored_are_distinct(self):
        engine = RulesEngine()
        self.assertEqual(engine.score('Revenue rose and sales grew.')['label'], 'positive')
        self.assertEqual(engine.score('Profit fell and revenue declined.')['label'], 'negative')
        result = engine.score('The company will publish a report next week.')
        self.assertEqual(result['label'], 'unscored'); self.assertIsNone(result['score'])
        self.assertIsNone(result['confidence'])
    def test_mixed_document_is_not_positive_for_both_companies(self):
        row = {**NEWS, 'headline': 'Microsoft raises guidance; Apple cuts guidance.', 'symbols': ['MSFT','AAPL']}
        result = compute_analysis(self.article(row), RulesEngine())
        self.assertEqual(result['sentiment']['label'], 'mixed')
        self.assertEqual(result['sentiment_scope'], 'document')
        for rel in result['relevance'].values():
            self.assertTrue(rel['entity_sentiment_available']); self.assertNotIn('sentiment', rel)
        self.assertEqual(result['entity_sentiments']['MSFT']['sentiment']['label'], 'positive')
        self.assertEqual(result['entity_sentiments']['AAPL']['sentiment']['label'], 'negative')
        self.assertIn('multiple_symbols', result['flags']); self.assertFalse(result['usable_for_trading'])
    def test_negation_suppresses_phrase_without_fabricated_inverse(self):
        for text in ('Microsoft did not beat expectations.', "Microsoft didn't beat expectations.", 'There was no upgrade.'):
            with self.subTest(text=text): self.assertEqual(RulesEngine().score(text)['label'], 'unscored')
        self.assertTrue(RulesEngine().score('Microsoft did not beat expectations.')['negated_evidence'])
    def test_headline_only_body_missing_is_reported(self):
        result = compute_analysis(self.article({**NEWS, 'content': '', 'summary': ''}), RulesEngine())
        self.assertEqual(result['text_basis'], 'headline'); self.assertIn('no_full_body', result['flags'])
    def test_summary_is_used_when_body_missing(self):
        result = compute_analysis(self.article({**NEWS, 'headline': 'Quarterly report', 'summary': 'Microsoft cuts guidance', 'content': ''}), RulesEngine())
        self.assertEqual(result['text_basis'], 'headline_summary')
        self.assertEqual(result['sentiment']['label'], 'negative')
        self.assertEqual(result['relevance']['MSFT']['basis'], 'summary_mention')
    def test_metadata_only_does_not_claim_company_mentioned(self):
        result = compute_analysis(self.article({**NEWS, 'headline': 'Anthropic launches a new program',
                'content': '', 'symbols': ['MSFT']}), RulesEngine())
        self.assertEqual(result['relevance']['MSFT']['basis'], 'metadata_only')
        self.assertIn('metadata_only_mapping', result['flags'])
    def test_four_mention_levels_and_ticker_boundaries(self):
        cases = [('Microsoft report', '', '', 'headline_mention'),
                 ('Report', 'Microsoft report', '', 'summary_mention'),
                 ('Report', '', 'Microsoft report', 'body_mention'),
                 ('Report', '', 'MSFTX and msft report', 'metadata_only')]
        for headline, summary, body, basis in cases:
            rel = relevance({'headline':headline,'summary':summary,'content_text':body}, {'MSFT':'provider_tag'})
            self.assertEqual(rel['MSFT']['basis'], basis)
    def test_relevance_does_not_add_unmapped_companies(self):
        rel = relevance({'headline':'Apple and Microsoft','summary':'','content_text':''}, {'MSFT':'provider_tag'})
        self.assertEqual(set(rel), {'MSFT'})
    def test_product_alias_and_historical_review(self):
        row = {**NEWS, 'headline': 'Here is how much you made owning iPhone maker stock in the last 20 years',
               'content': '', 'symbols': ['AAPL']}
        result = compute_analysis(self.article(row), RulesEngine())
        self.assertEqual(result['relevance']['AAPL']['basis'], 'headline_mention')
        self.assertIn('historical_review', result['flags']); self.assertFalse(result['backtest_ready'])
    def test_speculation_and_roundup_are_flags_not_impact_predictions(self):
        self.assertEqual(article_flags('Top 5 stocks to watch: Apple could rise'), ['speculative_language','market_roundup'])
    def test_long_text_is_bounded_and_truncation_explicit(self):
        text, fields, basis, truncated = prepare_text({'headline':'x'*5000, 'summary':'y'*7000, 'content_text':'z'*20000})
        self.assertLessEqual(len(text), 26002)
        self.assertEqual(truncated, ['headline','summary','content_text'])
    def test_same_field_text_is_not_counted_twice(self):
        text, _, basis, _ = prepare_text({'headline':'Microsoft raises guidance','summary':'Microsoft raises guidance','content_text':''})
        self.assertEqual(text, 'Microsoft raises guidance'); self.assertEqual(basis, 'headline_summary')
    def test_token_truncation_remains_visible(self):
        class LimitedRules(RulesEngine):
            def score(self, text): return {**super().score(text), 'token_truncated': True}
        result = compute_analysis(self.article(NEWS), LimitedRules())
        self.assertIn('token_truncated', result['flags'])
    def test_invalid_model_outputs_rejected(self):
        base = {'label':'positive','score':0.7,'probabilities':{'positive':0.8,'negative':0.1,'neutral':0.1},'confidence':0.8}
        validate_sentiment(base)
        for changed in ({'score':float('nan')}, {'score':True}, {'confidence':0.1}, {'label':'negative'},
                        {'probabilities':{'positive':0.8,'negative':0.2,'neutral':0.2}}, {'score':2}):
            with self.subTest(changed=changed), self.assertRaises(DataValidationError): validate_sentiment({**base, **changed})
    def test_first_analysis_and_repeat_preserve_news_and_results(self):
        self.collect(NEWS)
        before = self.repo.connection.execute('SELECT * FROM news_versions').fetchall(); self.repo.connection.commit()
        first = self.analyze(); result_before = self.repo.connection.execute('SELECT * FROM news_analyses').fetchall(); self.repo.connection.commit()
        with patch.object(RulesEngine, 'score', side_effect=AssertionError('repeat must use stored result')):
            second = self.analyze()
        self.assertEqual((first['inserted_analyses'],second['inserted_analyses'],second['existing_analyses']), (1,0,1))
        self.assertEqual(self.repo.connection.execute('SELECT * FROM news_versions').fetchall(), before)
        self.assertEqual(self.repo.connection.execute('SELECT * FROM news_analyses').fetchall(), result_before)
    def test_revision_change_retains_old_analysis_and_reports_pending(self):
        self.collect(NEWS); self.analyze()
        self.collect({**NEWS, 'updated_at':'2026-01-05T14:31:00Z','headline':'Microsoft cuts guidance','content':''})
        self.assertEqual(self.summary()['pending_articles'], 1)
        self.analyze()
        result = self.summary()
        self.assertEqual(result['label_counts'], {'negative':1})
        self.assertEqual(self.repo.connection.execute('SELECT COUNT(*) FROM news_analyses').fetchone()[0], 2)
    def test_engine_version_changes_create_separate_analysis(self):
        self.collect(NEWS); first = self.analyze()
        class OtherRules(RulesEngine): metadata = {**RulesEngine.metadata, 'version':'test-other'}
        second = self.analyze(OtherRules())
        self.assertNotEqual(first['analyzer_key'], second['analyzer_key'])
        self.assertEqual(second['inserted_analyses'], 1)
        self.assertEqual(self.summary(analyzer_key=first['analyzer_key'])['analyzed_current_articles'], 1)
    def test_provider_isolation_in_default_summary(self):
        self.collect(NEWS); first = self.analyze()
        self.collect(NEWS, provider='alpaca-sandbox')
        class OtherRules(RulesEngine): metadata = {**RulesEngine.metadata, 'version':'sandbox-only'}
        analyze_news(self.repo, AnalysisRequest('alpaca-sandbox'), OtherRules())
        self.assertEqual(self.summary()['analyzer_key'], first['analyzer_key'])
    def test_selection_analyzes_all_mappings_so_filter_does_not_change_cached_result(self):
        self.collect({**NEWS, 'symbols':['AAPL','MSFT']})
        first = analyze_news(self.repo, AnalysisRequest(symbols=('MSFT',)), RulesEngine())
        second = analyze_news(self.repo, AnalysisRequest(symbols=('AAPL',)), RulesEngine())
        self.assertEqual(first['analyzer_key'], second['analyzer_key']); self.assertEqual(second['existing_analyses'], 1)
        summary = analysis_summary(self.repo, AnalysisRequest(symbols=('AAPL',)), engine='rules')
        self.assertEqual(set(summary['samples'][0]['relevance']), {'AAPL'})
    def test_inference_failure_rolls_back_partial_analysis_and_audits_safely(self):
        self.collect(NEWS, {**NEWS,'id':2})
        class BrokenRules(RulesEngine):
            def __init__(self): self.calls = 0
            def score(self, text):
                self.calls += 1
                if self.calls == 2: raise AnalysisError('TEST_SECRET_MODEL_ERROR')
                return super().score(text)
        with self.assertRaises(AnalysisError): self.analyze(BrokenRules())
        self.assertEqual(self.repo.connection.execute('SELECT COUNT(*) FROM news_analyses').fetchone()[0], 0)
        raw = self.repo.connection.execute('SELECT result_json FROM news_analysis_runs').fetchone()[0]
        self.assertNotIn('TEST_SECRET', raw); self.assertEqual(json.loads(raw)['committed_analyses'], 0)
    def test_success_audit_failure_also_rolls_back_results(self):
        self.collect(NEWS)
        original = self.repo.connection
        class AuditFailure:
            def __getattr__(self, name): return getattr(original, name)
            def execute(self, query, params=()):
                if query.startswith('INSERT INTO news_analysis_runs') and params[3] != 'failed':
                    raise StorageError('injected audit failure')
                return original.execute(query, params)
        self.repo.connection = AuditFailure()
        with self.assertRaises(StorageError): self.analyze()
        self.assertEqual(original.execute('SELECT COUNT(*) FROM news_analyses').fetchone()[0], 0)
        self.assertEqual(original.execute('SELECT status FROM news_analysis_runs').fetchone()[0], 'failed')
    def test_modified_news_payload_rejected_before_inference(self):
        self.collect(NEWS)
        self.repo.connection.execute("UPDATE news_versions SET data_json='{}'"); self.repo.connection.commit()
        with self.assertRaises(StorageError): self.analyze()
        self.assertEqual(self.repo.connection.execute('SELECT COUNT(*) FROM news_analyses').fetchone()[0], 0)
    def test_modified_analysis_rejected_by_summary_and_repeat(self):
        self.collect(NEWS); self.analyze()
        self.repo.connection.execute("UPDATE news_analyses SET result_json='{}'"); self.repo.connection.commit()
        with self.assertRaises(StorageError): self.summary()
        with self.assertRaises(StorageError): self.analyze()
    def test_empty_selection_is_explicit_not_successful_analysis(self):
        self.assertEqual(self.analyze()['status'], 'empty')
        self.assertEqual(self.summary()['status'], 'empty')
    def test_symbol_and_key_validation(self):
        with self.assertRaises(DataValidationError): AnalysisRequest(symbols=('NOT-IN-UNIVERSE',))
        with self.assertRaises(DataValidationError): self.summary(analyzer_key='bad')
    def test_bounded_selection_rejects_partial_success(self):
        self.collect(NEWS,{**NEWS,'id':2})
        with patch('microbroker.intelligence.analysis.MAX_ARTICLES',1), self.assertRaises(StorageError): self.analyze()
        self.assertEqual(self.repo.connection.execute('SELECT COUNT(*) FROM news_analyses').fetchone()[0], 0)
    def test_rule_smoke_check_is_not_reported_as_model_accuracy(self):
        result = verify_analysis(RulesEngine())
        self.assertEqual(result['status'], 'passed'); self.assertFalse(result['model_accuracy_validated'])
    def test_cli_rules_works_without_api_credentials(self):
        with tempfile.TemporaryDirectory() as folder:
            settings = Settings(database_url='sqlite:///'+str(Path(folder)/'db'))
            repo = Repository(settings.database_url)
            collect_news(Fixture([NEWS]),repo,NewsRequest(('MSFT',),utc_datetime('2026-01-05T00:00:00Z'),utc_datetime('2026-01-06T00:00:00Z')))
            repo.close(); output = io.StringIO()
            with patch('microbroker.cli.Settings.from_env',return_value=settings), contextlib.redirect_stdout(output):
                self.assertEqual(main(['analyze-news','--symbols','MSFT','--engine','rules']),0)
                self.assertEqual(main(['analysis-summary','--symbols','MSFT','--engine','rules']),0)
            lines = [json.loads(line) for line in output.getvalue().splitlines()]
            self.assertEqual(lines[0]['inserted_analyses'], 1); self.assertEqual(lines[1]['analyzed_current_articles'], 1)

class ModelFileTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.folder = Path(self.temp.name)
        self.files = {'config.json': b'config', 'pytorch_model.bin': b'weights'}
        self.expected = {name:(len(data),hashlib.sha256(data).hexdigest()) for name,data in self.files.items()}
        self.patch = patch('microbroker.intelligence.model_files.EXPECTED_FILES',self.expected); self.patch.start()
    def tearDown(self): self.patch.stop(); self.temp.cleanup()
    def download(self, request, timeout):
        self.assertNotIn('Authorization',request.headers)
        self.assertIn('4556d13015211d73dccd3fdd39d39232506f3e43',request.full_url)
        return io.BytesIO(self.files[request.full_url.rsplit('/',1)[1]])
    def test_download_pins_revision_verifies_hash_and_reuses_valid_files(self):
        result = prepare_finbert(self.folder, opener=self.download)
        self.assertEqual(result['verified_files'], 2)
        again = prepare_finbert(self.folder, opener=lambda *a,**k: self.fail('must not redownload'))
        self.assertEqual(again['downloaded_files'], 0)
        self.assertEqual(verify_model_files(self.folder)['revision'], result['revision'])
    def test_modified_weight_fails_before_any_model_loading(self):
        prepare_finbert(self.folder, opener=self.download)
        (self.folder/'pytorch_model.bin').write_bytes(b'changed')
        with self.assertRaises(AnalysisError): FinBertEngine(self.folder)
    def test_untracked_tokenizer_file_cannot_change_model_identity(self):
        prepare_finbert(self.folder, opener=self.download)
        (self.folder/'tokenizer.json').write_text('{}')
        with self.assertRaises(AnalysisError): verify_model_files(self.folder)
    def test_bad_download_retries_without_finalizing_model(self):
        with self.assertRaises(AnalysisError): prepare_finbert(self.folder, opener=lambda *a,**k:io.BytesIO(b'bad'), sleep=lambda n:None)
        self.assertFalse((self.folder/'microbroker-model.json').exists())
        self.assertTrue((self.folder/'config.json.part').exists())
    def test_network_error_does_not_expose_exception_text(self):
        def broken(*a,**k): raise OSError('TEST_SECRET_NETWORK_URL')
        with self.assertRaises(AnalysisError) as raised: prepare_finbert(self.folder,opener=broken,sleep=lambda n:None)
        self.assertNotIn('TEST_SECRET',str(raised.exception))
    def test_missing_model_does_not_fallback_to_rules(self):
        with self.assertRaises(AnalysisError): FinBertEngine(self.folder)
    def test_inference_uses_cpu_local_only_checked_labels_and_softmax(self):
        prepare_finbert(self.folder, opener=self.download)
        torch = MagicMock(); torch.__version__ = '2.8.0'; torch.get_num_threads.return_value = 4
        torch.softmax.return_value.__getitem__.return_value.tolist.return_value = [0.8,0.1,0.1]
        tokenizer = MagicMock(); tokenizer.encode.return_value = [1]*600; tokenizer.return_value = {'input_ids':'fixture'}
        model = MagicMock(); model.config.id2label = {0:'positive',1:'negative',2:'neutral'}
        transformers = SimpleNamespace(__version__='4.57.3',AutoTokenizer=MagicMock(),AutoModelForSequenceClassification=MagicMock())
        transformers.AutoTokenizer.from_pretrained.return_value = tokenizer
        transformers.AutoModelForSequenceClassification.from_pretrained.return_value = model
        with patch.dict('sys.modules',{'torch':torch,'transformers':transformers}):
            engine = FinBertEngine(self.folder); result = engine.score('fixture input')
        self.assertEqual(result['label'],'positive'); self.assertAlmostEqual(result['score'],0.7)
        self.assertTrue(result['token_truncated']); self.assertEqual(result['tokens_used'],512)
        self.assertTrue(transformers.AutoModelForSequenceClassification.from_pretrained.call_args.kwargs['local_files_only'])
        self.assertTrue(transformers.AutoModelForSequenceClassification.from_pretrained.call_args.kwargs['weights_only'])
        self.assertFalse(transformers.AutoModelForSequenceClassification.from_pretrained.call_args.kwargs['trust_remote_code'])
        model.to.assert_called_once_with('cpu'); model.eval.assert_called_once()
    def test_unexpected_label_mapping_is_rejected(self):
        prepare_finbert(self.folder, opener=self.download)
        torch = MagicMock(); torch.__version__ = '2.8.0'
        transformers = SimpleNamespace(__version__='4.57.3',AutoTokenizer=MagicMock(),AutoModelForSequenceClassification=MagicMock())
        transformers.AutoModelForSequenceClassification.from_pretrained.return_value.config.id2label = {0:'negative',1:'positive',2:'neutral'}
        with patch.dict('sys.modules',{'torch':torch,'transformers':transformers}),self.assertRaises(AnalysisError): FinBertEngine(self.folder)
