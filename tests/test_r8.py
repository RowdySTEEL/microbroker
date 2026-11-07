"""Evidence diagnostics and narrow attribution exceptions, including rejection paths."""
import unittest
from microbroker.intelligence.policy import explicit_money_flow, company_contexts
from microbroker.intelligence.evidence import evidence_summary
from microbroker.intelligence.sentiment import RulesEngine, aggregate_windows
from microbroker.intelligence.analysis import compute_analysis
from test_r6_1 import article, fields

class R8Tests(unittest.TestCase):
    def test_same_direction_list(self):
        text='In the early trade, money flows are negative in Microsoft Corp. (NASDAQ:MSFT), Apple Inc (NASDAQ:AAPL), and Nvidia (NVDA).'
        c=company_contexts(fields(text),{'MSFT':'tag','AAPL':'tag'})
        for value in c.values():
            self.assertEqual(value['shared_units_excluded'],0)
            self.assertEqual(value['units'][0]['reported_money_flow'],'negative')
            self.assertEqual(value['units'][0]['text'],text)
    def test_neutral_and_mixed_not_collapsed(self):
        for direction in ('neutral','mixed'):
            self.assertEqual(explicit_money_flow('money flows are '+direction+' in Microsoft and Apple.'),direction)
    def test_negated_direction_rejected(self):
        self.assertIsNone(explicit_money_flow('money flows are not positive in Microsoft and Apple.'))
    def test_unknown_company_rejected(self):
        self.assertIsNone(explicit_money_flow('money flows are positive in Microsoft and Adobe.'))
    def test_unknown_ticker_rejected(self):
        self.assertIsNone(explicit_money_flow('money flows are positive in Microsoft and ADBE.'))
    def test_legal_name_and_known_alphabet_share_class(self):
        text='In the early trade, money flows are negative in Amazon.com, Inc. (NASDAQ:AMZN), Alphabet Inc Class C (NASDAQ:GOOG), and Microsoft Corp (MSFT).'
        self.assertEqual(explicit_money_flow(text),'negative')
        c=company_contexts(fields(text),{'MSFT':'tag'})['MSFT']
        self.assertEqual(len(c['units']),1)
    def test_single_letter_unknown_is_not_share_class(self):
        self.assertIsNone(explicit_money_flow('money flows are positive in Microsoft and C.'))
    def test_trailing_opposite_predicate_rejected(self):
        self.assertIsNone(explicit_money_flow('money flows are positive in Microsoft and Apple but Apple revenue fell.'))
    def test_speculative_flow_rejected(self):
        self.assertIsNone(explicit_money_flow('money flows may be positive in Microsoft and Apple.'))
    def test_general_joint_sentence_stays_excluded(self):
        c=company_contexts(fields('Microsoft and Apple announce an agreement.'),{'MSFT':'tag'})['MSFT']
        self.assertEqual(c['units'],[]);self.assertEqual(c['shared_units_excluded'],1)
    def test_offsets_and_duplicate_fields(self):
        text='  money flows are positive in Microsoft and Apple.  '
        f={'headline':text,'summary':text,'content_text':''}
        c=company_contexts(f,{'MSFT':'tag'})['MSFT']
        self.assertEqual(len(c['units']),1)
        u=c['units'][0];self.assertEqual(f[u['field']][u['start_char']:u['end_char']],u['text'])
    def test_opposite_phrases_without_changing_raw_label(self):
        d=evidence_summary('Microsoft raises guidance. Microsoft revenue fell.',{'label':'neutral'})
        self.assertEqual(d['status'],'mixed_evidence');self.assertEqual(d['raw_label'],'neutral')
        self.assertTrue(d['opposite_rule_phrases'])
    def test_negated_phrase_is_not_opposite_evidence(self):
        d=evidence_summary('Microsoft raises guidance. Microsoft did not cut guidance.',{'label':'positive'})
        self.assertFalse(d['opposite_rule_phrases'])
        self.assertTrue(d['rule_negated_samples'])
    def test_window_token_shares_and_opposite_labels(self):
        chunks=[{'label':'positive','content_tokens':510},{'label':'negative','content_tokens':30}]
        d=evidence_summary('A factual report.',{'label':'positive','chunks':chunks})
        self.assertTrue(d['opposite_window_labels']);self.assertEqual(d['window_label_counts'],{'negative':1,'positive':1})
        self.assertAlmostEqual(sum(d['window_content_token_share'].values()),1)
    def test_no_evidence_is_not_neutral_or_validated(self):
        d=evidence_summary('A report is forthcoming.',{'label':'negative'})
        self.assertEqual(d['raw_label'],'negative');self.assertEqual(d['status'],'no_opposite_evidence_detected')
        self.assertTrue(d['absence_is_not_neutral']);self.assertFalse(d['semantic_conflict_validated'])
    def test_stored_result_exposes_flow_separately_from_model(self):
        text='money flows are negative in Microsoft and Apple.'
        r=compute_analysis(article(text),RulesEngine())
        self.assertEqual(r['analysis_format'],'R18')
        self.assertEqual(r['entity_sentiments']['MSFT']['reported_money_flow_counts'],{'negative':1})
        self.assertEqual(r['entity_sentiments']['MSFT']['sentiment']['label'],'unscored')
        self.assertFalse(r['usable_for_trading'])
    def test_evidence_can_detect_opposition_beyond_sample_cap(self):
        d=evidence_summary(('Microsoft raises guidance. '*40)+'Microsoft cuts guidance.',{'label':'positive'})
        self.assertTrue(d['opposite_rule_phrases']);self.assertEqual(len(d['rule_evidence_samples']),30)
    def test_metadata_only_remains_unscored(self):
        r=compute_analysis(article('An external vendor raises guidance.'),RulesEngine())
        self.assertIsNone(r['entity_sentiments']['MSFT']['sentiment'])
        self.assertIsNone(r['entity_sentiments']['MSFT']['evidence_summary'])
