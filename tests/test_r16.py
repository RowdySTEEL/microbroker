from copy import deepcopy
import contextlib
import io
import json
from pathlib import Path
import unittest
from unittest.mock import patch
from microbroker.cli import main
from microbroker.data.storage.repository import Repository,StorageError
from microbroker.intelligence.attribution import ambiguity_reason
from microbroker.intelligence.attribution_verification import CASE,control_article,attribution_fixture,verify_attribution
from microbroker.intelligence.analysis import compute_analysis,analyze_news
from microbroker.intelligence.policy import ALIASES,company_contexts,company_contexts_v8,digest,analyzer_metadata
from microbroker.intelligence.sentiment import RulesEngine
from microbroker.intelligence.explanation import explain_analysis

IREN_UNIT='The company already has a $9.7 billion agreement with Microsoft Corp (NASDAQ:MSFT) for AI cloud capacity, giving it another major customer relationship as it expands its data-center operations.'

class R16Tests(unittest.TestCase):
    def fields(self,text):return {'headline':text,'summary':'','content_text':''}
    def test_reported_iren_sentence_excluded(self):
        context=company_contexts(self.fields(IREN_UNIT),{'MSFT':'tag'})['MSFT']
        self.assertEqual(context['units'],[])
        self.assertEqual(context['ambiguous_units'][0]['reason'],'unresolved_subject')
        self.assertEqual(context['ambiguous_units'][0]['text'],IREN_UNIT)
    def test_untracked_vendor_counterparty_rejected(self):
        for subject in ('IREN','An external vendor','A provider'):
            text=subject+' signed a contract with Microsoft.'
            self.assertEqual(ambiguity_reason(text,'MSFT',ALIASES),'counterparty_role_not_established')
    def test_pronoun_subjects_not_resolved(self):
        for text in ('It won a contract with Microsoft.','They raised guidance after the Microsoft deal.','Its partnership with Microsoft boosted sales.'):
            self.assertEqual(ambiguity_reason(text,'MSFT',ALIASES),'unresolved_subject')
    def test_third_party_subjects_not_target(self):
        for text in ("Microsoft's supplier raises guidance.",'Microsoft partner IREN wins a contract.',"Microsoft Corp's customer posts record profit."):
            self.assertEqual(ambiguity_reason(text,'MSFT',ALIASES),'third_party_subject')
    def test_explicit_named_subject_positive_preserved(self):
        result=compute_analysis(control_article('Microsoft beats expectations and raises guidance.'),RulesEngine())
        entity=result['entity_sentiments']['MSFT']
        self.assertEqual(entity['status'],'scored');self.assertEqual(entity['sentiment']['label'],'positive')
    def test_explicit_subject_negative_preserved(self):
        entity=compute_analysis(control_article('Microsoft cuts guidance.'),RulesEngine())['entity_sentiments']['MSFT']
        self.assertEqual(entity['sentiment']['label'],'negative')
    def test_analyst_action_not_contract_counterparty(self):
        self.assertIsNone(ambiguity_reason('Analysts upgraded Microsoft.','MSFT',ALIASES))
    def test_target_subject_can_receive_capacity_from_unknown_vendor(self):
        self.assertIsNone(ambiguity_reason('Microsoft secured cloud capacity from IREN.','MSFT',ALIASES))
    def test_raw_positive_retained_but_effective_none(self):
        entity=compute_analysis(control_article(CASE),RulesEngine())['entity_sentiments']['MSFT']
        self.assertEqual(entity['raw_mention_sentiment']['label'],'positive')
        self.assertIsNone(entity['sentiment']);self.assertEqual(entity['status'],'attribution_ambiguous')
        self.assertFalse(entity['raw_mention_sentiment_usable_for_alignment'])
    def test_all_ambiguous_context_not_called_neutral(self):
        entity=compute_analysis(control_article(CASE),RulesEngine())['entity_sentiments']['MSFT']
        self.assertIsNone(entity['sentiment'])
    def test_valid_and_ambiguous_units_are_separated(self):
        entity=compute_analysis(control_article(CASE+' Microsoft beats expectations.'),RulesEngine())['entity_sentiments']['MSFT']
        self.assertEqual(entity['context_unit_count'],1);self.assertEqual(entity['ambiguous_units_excluded'],1)
        self.assertEqual(entity['status'],'scored')
    def test_money_flow_exception_survives_guard(self):
        context=company_contexts(self.fields('money flows are positive in Microsoft and Apple.'),{'MSFT':'tag'})['MSFT']
        self.assertEqual(len(context['units']),1);self.assertEqual(context['ambiguous_units_excluded'],0)
    def test_shared_context_still_excluded(self):
        context=company_contexts(self.fields('Microsoft and Apple announce a partnership.'),{'MSFT':'tag'})['MSFT']
        self.assertEqual(context['units'],[]);self.assertEqual(context['shared_units_excluded'],1)
    def test_legacy_context_function_unchanged_for_case(self):
        context=company_contexts_v8(self.fields(IREN_UNIT),{'MSFT':'tag'})['MSFT']
        self.assertEqual(len(context['units']),1);self.assertNotIn('ambiguous_units',context)
    def test_new_namespace_differs_from_actual_r8_metadata(self):
        fixture=json.loads((Path(__file__).parent/'fixtures/r8_attribution_legacy.json').read_text())
        self.assertNotEqual(digest(analyzer_metadata(RulesEngine())),fixture['analyzer_key'])
    def seed_legacy(self,repo):
        fixture=json.loads((Path(__file__).parent/'fixtures/r8_attribution_legacy.json').read_text())
        a=fixture['article'];data=a['data'];rev=digest(data);result=fixture['result']
        repo.connection.execute('INSERT INTO news_versions VALUES (?,?,?,?,?,?,?)',('alpaca',99916,rev,data['created_at'],data['updated_at'],a['first_seen'],json.dumps(data)))
        repo.connection.execute('INSERT INTO news_articles VALUES (?,?,?,?,?,?,?,?)',('alpaca',99916,rev,data['updated_at'],a['first_seen'],a['first_seen'],'original-synthetic-fixture',int(bool(data['content_text']))))
        for symbol,method in a['mappings'].items():repo.connection.execute('INSERT INTO news_symbols VALUES (?,?,?,?,?)',('alpaca',99916,rev,symbol,method))
        repo.connection.execute('INSERT INTO news_analyses VALUES (?,?,?,?,?,?,?,?,?)',('alpaca',99916,rev,fixture['analyzer_key'],'rules',result['input_sha256'],'2026-01-06T10:00:00Z',json.dumps(result),digest(result)))
        repo.connection.commit();return fixture,rev
    def test_original_r8_analysis_explanation_still_reconstructs(self):
        repo=Repository('sqlite:///:memory:')
        try:
            fixture,rev=self.seed_legacy(repo)
            explanation=explain_analysis(repo,99916,revision=rev,analyzer_key=fixture['analyzer_key'],engine='rules')
            self.assertEqual(explanation['analysis_format'],'R8')
            self.assertEqual(explanation['companies']['MSFT']['sentiment']['label'],'positive')
            self.assertFalse(explanation['companies']['MSFT']['attribution_guard_applied'])
        finally:repo.close()
    def test_new_analysis_retains_old_row_and_digest(self):
        repo=Repository('sqlite:///:memory:')
        try:
            fixture,rev=self.seed_legacy(repo);request,explanation=attribution_fixture(repo)
            rows=repo.connection.execute('SELECT analyzer_key,result_sha256 FROM news_analyses WHERE article_id=99916').fetchall()
            self.assertEqual(len(rows),2);self.assertIn((fixture['analyzer_key'],digest(fixture['result'])),rows)
            self.assertNotEqual(explanation['analyzer_key'],fixture['analyzer_key'])
        finally:repo.close()
    def test_explanation_shows_excluded_text_and_raw_score(self):
        repo=Repository('sqlite:///:memory:')
        try:
            request,explanation=attribution_fixture(repo);company=explanation['companies']['MSFT']
            self.assertEqual(company['units'],[]);self.assertEqual(company['excluded_units'][0]['text'],CASE)
            self.assertTrue(company['raw_mention_context_verified'])
            self.assertEqual(company['raw_mention_sentiment']['label'],'positive')
        finally:repo.close()
    def test_excluded_evidence_tamper_rejected_even_rehashed(self):
        repo=Repository('sqlite:///:memory:')
        try:
            attribution_fixture(repo)
            raw=repo.connection.execute('SELECT result_json FROM news_analyses WHERE article_id=99916').fetchone()[0]
            result=json.loads(raw);result['entity_sentiments']['MSFT']['excluded_context_sha256']='changed'
            repo.connection.execute('UPDATE news_analyses SET result_json=?,result_sha256=?',(json.dumps(result),digest(result)));repo.connection.commit()
            with self.assertRaises(StorageError):explain_analysis(repo,99916,engine='rules')
        finally:repo.close()
    def test_private_offline_guard_verifier(self):
        self.assertEqual(len(verify_attribution()['checks']),10)
        with patch('microbroker.cli.Settings.from_env',side_effect=AssertionError('No env')),contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['verify-attribution']),0)

if __name__=='__main__':unittest.main()
