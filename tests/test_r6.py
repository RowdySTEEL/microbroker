"""Coverage, attribution limits, cache isolation and transactional R6 regressions."""
import copy
import json
import unittest
from unittest.mock import patch, MagicMock
from microbroker.data.models import DataValidationError
from microbroker.intelligence.policy import company_contexts, digest, analyzer_metadata
from microbroker.intelligence.sentiment import (FinBertEngine, RulesEngine, AnalysisError,
                                               token_windows, aggregate_windows, validate_sentiment)
from microbroker.intelligence.analysis import compute_analysis
import test_r5 as baseline
NEWS = baseline.NEWS

def window(start, end, label):
    probs = {p: 0.1 for p in ('positive', 'negative', 'neutral')}; probs[label] = 0.8
    return dict(start_token=start, end_token=end, content_tokens=end-start, label=label,
                probabilities=probs, confidence=0.8, score=probs['positive']-probs['negative'])

class WindowTests(unittest.TestCase):
    def test_windows_cover_boundary_lengths_without_overlap_or_omission(self):
        for n in (1, 509, 510, 511, 1020, 1021, 32640):
            ranges = token_windows(list(range(n)))
            self.assertEqual([i for start,end in ranges for i in range(start,end)],list(range(n)))
            self.assertTrue(all(end-start<=510 for start,end in ranges))
    def test_over_capacity_and_empty_fail_without_prefix_success(self):
        for ids in ([], [0]*32641):
            with self.assertRaises(AnalysisError): token_windows(ids)
    def test_short_tail_weight_is_not_equal_to_full_window(self):
        result = aggregate_windows([window(0,510,'positive'),window(510,520,'negative')],520)
        self.assertAlmostEqual(result['probabilities']['positive'],(510*0.8+10*0.1)/520)
        self.assertTrue(result['mixed_chunk_polarity']); self.assertIsNone(result['confidence'])
        self.assertFalse(result['token_truncated']); self.assertEqual(result['covered_content_tokens'],520)
    def test_one_window_retains_softmax_confidence(self):
        self.assertEqual(aggregate_windows([window(0,10,'positive')],10)['confidence'],0.8)
    def test_gaps_overlaps_and_incomplete_coverage_rejected(self):
        for windows,total in (([window(1,10,'positive')],10),
                              ([window(0,10,'positive'),window(9,20,'negative')],20),
                              ([window(0,10,'positive')],11)):
            with self.assertRaises(DataValidationError): aggregate_windows(windows,total)
    def test_invalid_probability_still_rejected_for_aggregate_with_no_confidence(self):
        result=aggregate_windows([window(0,10,'positive'),window(10,20,'negative')],20)
        validate_sentiment(result)
        for changed in ({'score':float('nan')},{'label':'neutral'},
                        {'probabilities':{'positive':0.9,'negative':0.9,'neutral':0.9}}):
            with self.assertRaises(DataValidationError):validate_sentiment({**result,**changed})
    def test_exact_ids_are_sent_to_model_including_last_token(self):
        class Tokenizer:
            def encode(self,*args,**kwargs):return list(range(521))
            def prepare_for_model(self,ids,**kwargs):
                self.received.append(ids);return {'input_ids':ids}
            received=[]
        class Torch:
            from contextlib import nullcontext
            inference_mode=staticmethod(nullcontext)
            @staticmethod
            def softmax(logits,dim):
                class Value:
                    def __getitem__(self,index):return self
                    def tolist(self):return [0.8,0.1,0.1]
                return Value()
        class Model:
            def __call__(self,**inputs):
                from types import SimpleNamespace
                return SimpleNamespace(logits=None)
        engine=object.__new__(FinBertEngine);engine.tokenizer=Tokenizer();engine.torch=Torch()
        engine.model=Model();engine.labels={0:'positive',1:'negative',2:'neutral'}
        result=engine.score_document('ignored fixture')
        self.assertEqual(engine.tokenizer.received,[list(range(510)),list(range(510,521))])
        self.assertEqual(result['chunk_count'],2)

class ContextTests(unittest.TestCase):
    def fields(self,text):return {'headline':text,'summary':'','content_text':''}
    def test_semicolon_clauses_separate_opposite_company_results(self):
        contexts=company_contexts(self.fields('Microsoft raises guidance; Apple cuts guidance.'),{'MSFT':'tag','AAPL':'tag'})
        self.assertEqual(contexts['MSFT']['units'][0]['text'],'Microsoft raises guidance')
        self.assertEqual(contexts['AAPL']['units'][0]['text'],'Apple cuts guidance.')
    def test_shared_sentence_is_excluded_for_both(self):
        contexts=company_contexts(self.fields('Microsoft raises guidance while Apple cuts guidance.'),{'MSFT':'tag','AAPL':'tag'})
        for context in contexts.values():
            self.assertEqual(context['units'],[]);self.assertEqual(context['shared_units_excluded'],1)
    def test_unmapped_configured_company_also_makes_clause_shared(self):
        context=company_contexts(self.fields('Microsoft and Apple raise guidance.'),{'MSFT':'tag'})['MSFT']
        self.assertEqual(context['units'],[]);self.assertEqual(context['shared_units_excluded'],1)
    def test_pronouns_are_not_resolved_and_duplicates_not_weighted_twice(self):
        fields={'headline':'Microsoft publishes a report.','summary':'Microsoft publishes a report.',
                'content_text':'It cuts guidance. Apple raises guidance.'}
        context=company_contexts(fields,{'MSFT':'tag'})['MSFT']
        self.assertEqual(len(context['units']),1)
        self.assertNotIn('cuts guidance',context['units'][0]['text'])
    def test_no_mapping_is_invented_from_mentions(self):
        self.assertEqual(company_contexts(self.fields('Apple raises guidance.'),{}),{})
    def test_ticker_boundaries_and_case_remain_conservative(self):
        context=company_contexts(self.fields('msft and MSFTX raise guidance.'),{'MSFT':'tag'})['MSFT']
        self.assertEqual(context['units'],[])

class R6PersistenceTests(unittest.TestCase):
    setUp = baseline.R5Tests.setUp
    tearDown = baseline.R5Tests.tearDown
    collect = baseline.R5Tests.collect
    analyze = baseline.R5Tests.analyze
    summary = baseline.R5Tests.summary
    article = baseline.R5Tests.article
    def test_metadata_only_company_has_no_fabricated_label(self):
        result=compute_analysis(self.article({**NEWS,'headline':'Anthropic raises guidance.','content':''}),RulesEngine())
        company=result['entity_sentiments']['MSFT']
        self.assertEqual(company['status'],'no_explicit_mention');self.assertIsNone(company['sentiment'])
        self.assertFalse(result['relevance']['MSFT']['entity_sentiment_available'])
    def test_shared_only_company_has_explicit_unavailable_status(self):
        result=compute_analysis(self.article({**NEWS,'headline':'Microsoft and Apple raise guidance.',
                    'content':'','symbols':['MSFT','AAPL']}),RulesEngine())
        for company in result['entity_sentiments'].values():
            self.assertEqual(company['status'],'shared_context_only');self.assertIsNone(company['sentiment'])
        self.assertIn('entity_context_unavailable',result['flags'])
    def test_company_failure_rolls_back_document_and_success_audit(self):
        self.collect({**NEWS,'headline':'Market update.','content':'Microsoft raises guidance.'})
        class Broken(RulesEngine):
            def __init__(self):self.calls=0
            def score(self,text):
                self.calls+=1
                if self.calls==2:raise AnalysisError('private fixture error')
                return super().score(text)
        with self.assertRaises(AnalysisError):self.analyze(Broken())
        self.assertEqual(self.repo.connection.execute('SELECT COUNT(*) FROM news_analyses').fetchone()[0],0)
        status,raw=self.repo.connection.execute('SELECT status,result_json FROM news_analysis_runs').fetchone()
        self.assertEqual(status,'failed');self.assertEqual(json.loads(raw)['committed_analyses'],0)
    def test_second_model_window_failure_preserves_previous_analyzer_results(self):
        self.collect(NEWS);self.analyze()
        before=self.repo.connection.execute('SELECT * FROM news_analyses').fetchall()
        self.repo.connection.commit()
        engine=object.__new__(FinBertEngine);engine.metadata={'name':'finbert','test':'window-failure'}
        engine.tokenizer=MagicMock();engine.tokenizer.encode.return_value=list(range(600))
        engine.tokenizer.prepare_for_model.return_value={'input_ids':'fixture'}
        engine.torch=MagicMock()
        engine.torch.softmax.return_value.__getitem__.return_value.tolist.return_value=[0.8,0.1,0.1]
        engine.model=MagicMock(side_effect=[MagicMock(logits=None),RuntimeError('private failure')])
        engine.labels={0:'positive',1:'negative',2:'neutral'}
        with self.assertRaises(AnalysisError):self.analyze(engine)
        self.assertEqual(self.repo.connection.execute('SELECT * FROM news_analyses').fetchall(),before)
        self.assertEqual(engine.model.call_count,2)
        statuses=self.repo.connection.execute('SELECT status FROM news_analysis_runs ORDER BY started_at').fetchall()
        self.assertEqual(statuses,[('completed',),('failed',)])
    def test_previous_r5_snapshot_remains_queryable_after_r6(self):
        self.collect(NEWS)
        legacy=compute_analysis(self.article(NEWS),RulesEngine())
        legacy.pop('entity_sentiments');legacy.pop('analysis_format');legacy['entity_sentiment_implemented']=False
        with patch('microbroker.intelligence.analysis.analyzer_metadata',return_value={'version':'R5-fixture'}),\
             patch('microbroker.intelligence.analysis.compute_analysis',return_value=legacy):
            old=self.analyze()
        new=self.analyze()
        self.assertNotEqual(old['analyzer_key'],new['analyzer_key'])
        summary=self.summary(analyzer_key=old['analyzer_key'])
        self.assertEqual(summary['analysis_format_counts'],{'R5':1})
        self.assertFalse(summary['entity_sentiment_implemented'])
        self.assertEqual(self.repo.connection.execute('SELECT COUNT(*) FROM news_analyses').fetchone()[0],2)
    def test_summary_filters_company_labels_and_shows_pending_counts(self):
        self.collect({**NEWS,'headline':'Microsoft raises guidance; Apple cuts guidance.',
                    'content':'','symbols':['MSFT','AAPL']});self.analyze()
        from microbroker.intelligence.analysis import AnalysisRequest,analysis_summary
        summary=analysis_summary(self.repo,AnalysisRequest(symbols=('AAPL',)),engine='rules')
        self.assertEqual(summary['entity_label_counts'],{'AAPL':{'negative':1}})
        self.assertEqual(set(summary['samples'][0]['entity_sentiments']),{'AAPL'})
        self.assertEqual(summary['pending_articles'],0)
    def test_bounded_input_does_not_hide_character_truncation(self):
        result=compute_analysis(self.article({**NEWS,'content':'x '*20000}),RulesEngine())
        self.assertIn('character_truncated',result['flags'])
        self.assertEqual(result['character_truncated_fields'],['content_text'])
    def test_analyzer_identity_includes_policy_and_runtime_metadata(self):
        metadata=analyzer_metadata(RulesEngine())
        self.assertEqual(metadata['policy'],'r18-accepted-context-unit-evidence-v1')
        self.assertTrue(metadata['entity_sentiment_implemented'])
        self.assertFalse(metadata['entity_attribution_validated'])
        changed=copy.deepcopy(metadata);changed['entity_context_policy']='different'
        self.assertNotEqual(digest(metadata),digest(changed))
