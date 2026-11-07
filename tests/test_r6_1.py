"""Company abbreviation and empty ticker evidence regressions from real review."""
import unittest
from microbroker.intelligence.policy import context_units, company_contexts, substantive_context
from microbroker.intelligence.analysis import compute_analysis
from microbroker.intelligence.sentiment import RulesEngine
from microbroker.data.news import normalize_news

def fields(text):return {'headline':text,'summary':'','content_text':''}

def article(text):
    story={'id':1,'headline':text,'summary':'','content':'','symbols':['MSFT'],
           'created_at':'2026-01-05T00:00:00Z','updated_at':'2026-01-05T00:00:00Z',
           'source':'synthetic','url':'https://example.com/fixture'}
    row=normalize_news(story)
    return {'data':row['data'],'mappings':row['mappings'],'first_seen':'2026-01-06T00:00:00Z'}

class ContextEvidenceTests(unittest.TestCase):
    def test_corp_annotation_and_predicate_remain_together(self):
        text='Microsoft Corp. (NASDAQ:MSFT) cuts guidance. Apple raises guidance.'
        contexts=company_contexts(fields(text),{'MSFT':'tag','AAPL':'tag'})
        self.assertEqual(contexts['MSFT']['units'][0]['text'],'Microsoft Corp. (NASDAQ:MSFT) cuts guidance.')
        self.assertEqual(RulesEngine().score(contexts['MSFT']['units'][0]['text'])['label'],'negative')
    def test_inc_name_is_not_separated_from_revenue_clause(self):
        text='Apple Inc. reported record revenue. Microsoft cuts guidance.'
        context=company_contexts(fields(text),{'AAPL':'tag'})['AAPL']
        self.assertEqual(len(context['units']),1)
        self.assertIn('record revenue',context['units'][0]['text'])
    def test_other_corporate_abbreviations_are_retained(self):
        for name in ('Co.','Ltd.','plc.'):
            text='Microsoft '+name+' cuts guidance. Another report follows.'
            self.assertEqual(len(list(context_units(text))),2)
            self.assertIn('cuts guidance',list(context_units(text))[0][2])
    def test_us_and_titles_do_not_create_orphan_company_fragments(self):
        text='Dr. Smith said Microsoft will report U.S. revenue next week. Apple published a report.'
        self.assertEqual(len(list(context_units(text))),2)
        self.assertIn('next week',list(context_units(text))[0][2])
    def test_decimal_does_not_split_the_sentence(self):
        text='Microsoft revenue rose 6.6%. Apple revenue fell 3.2%.'
        self.assertEqual(len(list(context_units(text))),2)
    def test_ticker_only_context_does_not_get_a_neutral_score(self):
        result=compute_analysis(article('(NASDAQ:MSFT).'),RulesEngine())
        context=result['entity_sentiments']['MSFT']
        self.assertEqual(context['status'],'insufficient_context');self.assertIsNone(context['sentiment'])
        self.assertFalse(result['relevance']['MSFT']['entity_sentiment_available'])
        self.assertIn('insufficient_entity_context_excluded',result['flags'])
    def test_company_name_or_name_and_exchange_only_are_insufficient(self):
        for text in ('Microsoft Corp.','Microsoft Corp. (NASDAQ:MSFT).','MSFT','Microsoft Corporation.'):
            with self.subTest(text=text):self.assertFalse(substantive_context(text,'MSFT'))
    def test_known_multi_company_sentence_cannot_leak_ticker_fragment(self):
        text='Nvidia Corp. and Microsoft Corp. (NASDAQ:MSFT) publish a report.'
        context=company_contexts(fields(text),{'MSFT':'tag'})['MSFT']
        self.assertEqual(context['units'],[]);self.assertEqual(context['shared_units_excluded'],1)
    def test_neutral_factual_company_sentence_is_still_usable(self):
        text='Microsoft will publish its report next week.'
        self.assertTrue(substantive_context(text,'MSFT'))
        self.assertEqual(len(company_contexts(fields(text),{'MSFT':'tag'})['MSFT']['units']),1)
    def test_exact_offsets_match_each_evidence_excerpt(self):
        text='  Microsoft Corp. raises guidance;   Apple cuts guidance.  '
        for start,end,unit in context_units(text):
            self.assertEqual(text[start:end],unit)
            self.assertEqual(unit,unit.strip())
