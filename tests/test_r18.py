from copy import deepcopy
import contextlib
import io
import json
from pathlib import Path
import unittest
from unittest.mock import patch
from microbroker.cli import main
from microbroker.data.storage.repository import Repository, StorageError
from microbroker.intelligence.unit_evidence import (context_hints, score_units, summarize_units,
    validate_unit_evidence, company_unit_decision, MAX_COMPANY_UNITS, MAX_ARTICLE_UNIT_TEXTS)
from microbroker.intelligence.unit_verification import UnitFixtureEngine, CASE, unit_fixture, verify_unit_evidence
from microbroker.intelligence.analysis import compute_analysis, analyze_news, AnalysisRequest
from microbroker.intelligence.sentiment import RulesEngine, AnalysisError
from microbroker.intelligence.attribution_verification import control_article
from microbroker.intelligence.policy import digest, analyzer_metadata
from microbroker.intelligence.research_decision import validate_research_decisions
from microbroker.intelligence.explanation import explain_analysis

class R18Tests(unittest.TestCase):
    def test_clearly_opposite_units_block_high_margin_aggregate(self):
        a=control_article(CASE);r=compute_analysis(a,UnitFixtureEngine());company=r['entity_sentiments']['MSFT']
        self.assertEqual(company['sentiment']['label'],'negative')
        self.assertEqual(company['sentiment']['confidence'],0.9)
        self.assertFalse(company['evidence_summary']['opposite_window_labels'])
        self.assertFalse(company['evidence_summary']['opposite_rule_phrases'])
        self.assertEqual(company['unit_evidence']['unit_label_counts'],{'negative':1,'positive':1})
        self.assertIsNone(company['research_decision']['label'])
    def test_temporal_hint_keeps_negative_raw_unit(self):
        c=compute_analysis(control_article(CASE),UnitFixtureEngine())['entity_sentiments']['MSFT']
        self.assertTrue(c['unit_evidence']['units'][0]['hints']['past_event_hint'])
        self.assertEqual(c['unit_evidence']['units'][0]['sentiment']['label'],'negative')
        self.assertFalse(c['unit_evidence']['temporal_resolution_validated'])
    def test_expectation_hint_does_not_assert_completion(self):
        hints=context_hints('Microsoft could expand capacity next year, analysts said.')
        self.assertTrue(hints['uncertainty_hint']);self.assertTrue(hints['reported_view_hint'])
        self.assertFalse(hints['past_event_hint'])
    def test_month_reference_does_not_claim_past(self):
        hints=context_hints('Microsoft expects a product in June.')
        self.assertTrue(hints['time_reference_hint']);self.assertFalse(hints['past_event_hint'])
    def test_shared_context_is_not_newly_scored(self):
        c=compute_analysis(control_article('Microsoft and Apple issue a statement.'),RulesEngine())['entity_sentiments']['MSFT']
        self.assertEqual(c['status'],'shared_context_only');self.assertEqual(c['unit_evidence']['units'],[])
        self.assertEqual(c['research_decision']['status'],'context_unavailable')
    def test_ambiguous_attribution_not_scored_as_accepted_units(self):
        c=compute_analysis(control_article('The company raises guidance after signing a deal with Microsoft.'),RulesEngine())['entity_sentiments']['MSFT']
        self.assertEqual(c['status'],'attribution_ambiguous');self.assertEqual(c['unit_evidence']['scored_unit_count'],0)
        self.assertEqual(c['raw_mention_sentiment']['label'],'positive')
    def context(self,n):
        return {'units':[{'text':'Microsoft publishes item '+str(i)+'.','field':'content_text','start_char':i*50,'end_char':i*50+40} for i in range(n)]}
    def test_company_budget_and_prefix_are_explicit(self):
        e=score_units(self.context(MAX_COMPANY_UNITS+2),UnitFixtureEngine().score,{},set())
        self.assertEqual(e['scored_unit_count'],MAX_COMPANY_UNITS);self.assertFalse(e['complete'])
        self.assertEqual([u['unit_index'] for u in e['units']],list(range(MAX_COMPANY_UNITS)))
    def test_article_score_budget_blocks_new_calls(self):
        engine=UnitFixtureEngine();calls=[]
        def score(text):calls.append(text);return engine.score(text)
        e=score_units(self.context(2),score,{},set(str(i) for i in range(MAX_ARTICLE_UNIT_TEXTS)))
        self.assertEqual(calls,[]);self.assertEqual(e['scored_unit_count'],0);self.assertFalse(e['complete'])
    def test_cached_unit_can_be_reused_after_budget_exhaustion(self):
        ctx=self.context(1);text=ctx['units'][0]['text'];cached={text:UnitFixtureEngine().score(text)}
        e=score_units(ctx,lambda _:self.fail('Should reuse score'),cached,set(str(i) for i in range(MAX_ARTICLE_UNIT_TEXTS)))
        self.assertTrue(e['complete']);self.assertEqual(e['scored_unit_count'],1)
    def test_incomplete_unit_coverage_not_called_neutral(self):
        e=score_units(self.context(65),UnitFixtureEngine().score,{},set())
        d=company_unit_decision(UnitFixtureEngine().score('aggregate'),None,'scored',e)
        self.assertEqual(d['status'],'incomplete_evidence');self.assertIsNone(d['label'])
    def test_empty_evidence_is_not_missing_for_no_company_context(self):
        e=summarize_units([],0);validate_unit_evidence(e,0)
        self.assertTrue(e['complete'])
        self.assertEqual(company_unit_decision(None,None,'shared_context_only',e)['status'],'context_unavailable')
    def test_invalid_summary_rejected(self):
        e=score_units(self.context(1),UnitFixtureEngine().score,{},set());e['complete']=False
        with self.assertRaises(StorageError):validate_unit_evidence(e,1)
    def test_invalid_unit_ordinal_rejected(self):
        e=score_units(self.context(1),UnitFixtureEngine().score,{},set());e['units'][0]['unit_index']=3
        with self.assertRaises(StorageError):validate_unit_evidence(e,1)
    def test_company_unit_digest_rejected(self):
        r=compute_analysis(control_article(CASE),UnitFixtureEngine());r['entity_sentiments']['MSFT']['unit_evidence_sha256']='0'*64
        with self.assertRaises(StorageError):validate_research_decisions(r)
    def test_rehashed_context_hint_tamper_rejected_by_explanation(self):
        repo=Repository('sqlite:///:memory:')
        try:
            unit_fixture(repo)
            r=json.loads(repo.connection.execute('SELECT result_json FROM news_analyses WHERE article_id=99918').fetchone()[0])
            c=r['entity_sentiments']['MSFT'];u=c['unit_evidence']['units'];u[0]['hints']['past_event_hint']=False
            c['unit_evidence']=summarize_units(u,c['context_unit_count']);c['unit_evidence_sha256']=digest(c['unit_evidence'])
            repo.connection.execute('UPDATE news_analyses SET result_json=?,result_sha256=?',(json.dumps(r),digest(r)));repo.connection.commit()
            with self.assertRaises(StorageError):explain_analysis(repo,99918,engine='rules')
        finally:repo.close()
    def test_single_clear_company_unit_still_accepted(self):
        c=compute_analysis(control_article('Microsoft beats expectations.'),RulesEngine())['entity_sentiments']['MSFT']
        self.assertEqual(c['research_decision']['label'],'positive');self.assertTrue(c['unit_evidence']['complete'])
    def test_raw_document_is_not_replaced_by_company_units(self):
        r=compute_analysis(control_article(CASE),UnitFixtureEngine())
        self.assertEqual(r['sentiment']['label'],'negative')
        self.assertEqual(r['research_decision']['label'],'negative')
        self.assertIsNone(r['entity_sentiments']['MSFT']['research_decision']['label'])
    def test_unit_failure_rolls_back_new_analyzer_keeps_news(self):
        class Failing(UnitFixtureEngine):
            metadata={'name':'rules','version':'synthetic-unit-failure','trained_model':False}
            def score(self,text):
                if text.startswith('Analysts') and '\n' not in text:raise AnalysisError('Synthetic unit failure')
                return super().score(text)
        repo=Repository('sqlite:///:memory:')
        try:
            request,engine,explanation=unit_fixture(repo)
            with self.assertRaises(AnalysisError):analyze_news(repo,request,Failing())
            self.assertEqual(repo.connection.execute('SELECT COUNT(*) FROM news_analyses').fetchone()[0],1)
            self.assertEqual(repo.connection.execute('SELECT COUNT(*) FROM news_versions').fetchone()[0],1)
            self.assertEqual(repo.connection.execute("SELECT COUNT(*) FROM news_analysis_runs WHERE status='failed'").fetchone()[0],1)
        finally:repo.close()
    def test_replay_checks_unit_source_even_when_result_is_rehashed(self):
        from microbroker.data.news_replay import create_news_replay,NewsReplayRequest
        from microbroker.data.analysis_replay import create_analysis_replay,validate_analysis,replay_analysis
        repo=Repository('sqlite:///:memory:')
        try:
            request,engine,explanation=unit_fixture(repo)
            news=create_news_replay(repo,NewsReplayRequest('2099-01-01T00:00:00Z',('MSFT',)))
            snapshot=create_analysis_replay(repo,news['replay_id'],'2099-01-01T00:00:00Z',explanation['analyzer_key'],'rules')
            first=replay_analysis(repo,snapshot['replay_id'])
            record=json.loads(repo.connection.execute('SELECT record_json FROM replay_items WHERE replay_id=?',(snapshot['replay_id'],)).fetchone()[0])
            company=record['analysis']['result']['entity_sentiments']['MSFT']
            company['unit_evidence']['units'][0]['text_sha256']='0'*64
            company['unit_evidence_sha256']=digest(company['unit_evidence'])
            record['analysis']['result_sha256']=digest(record['analysis']['result'])
            with self.assertRaises(StorageError):validate_analysis(record)
            repo.connection.execute('DELETE FROM news_analyses');repo.connection.commit()
            self.assertEqual(replay_analysis(repo,snapshot['replay_id']),first)
        finally:repo.close()
    def seed_r17(self,repo):
        f=json.loads((Path(__file__).parent/'fixtures/r17_unit_legacy.json').read_text());a=f['article'];d=a['data'];rev=digest(d);r=f['result']
        repo.connection.execute('INSERT INTO news_versions VALUES (?,?,?,?,?,?,?)',('alpaca',99918,rev,d['created_at'],d['updated_at'],a['first_seen'],json.dumps(d)))
        repo.connection.execute('INSERT INTO news_articles VALUES (?,?,?,?,?,?,?,?)',('alpaca',99918,rev,d['updated_at'],a['first_seen'],a['first_seen'],'synthetic',1))
        for symbol,method in a['mappings'].items():repo.connection.execute('INSERT INTO news_symbols VALUES (?,?,?,?,?)',('alpaca',99918,rev,symbol,method))
        repo.connection.execute('INSERT INTO news_analyses VALUES (?,?,?,?,?,?,?,?,?)',('alpaca',99918,rev,f['analyzer_key'],'rules',r['input_sha256'],'2026-01-06T10:01:00Z',json.dumps(r),digest(r)))
        repo.connection.commit();return f
    def test_actual_r17_fixture_remains_accepted_and_unmodified(self):
        repo=Repository('sqlite:///:memory:')
        try:
            f=self.seed_r17(repo);old=explain_analysis(repo,99918,engine='rules',analyzer_key=f['analyzer_key'])
            self.assertEqual(old['companies']['MSFT']['research_decision']['status'],'accepted')
            self.assertNotIn('unit_evidence',old['companies']['MSFT'])
            unit_fixture(repo)
            self.assertEqual(explain_analysis(repo,99918,engine='rules',analyzer_key=f['analyzer_key']),old)
            self.assertEqual(repo.connection.execute('SELECT COUNT(*) FROM news_analyses').fetchone()[0],2)
            self.assertNotEqual(digest(analyzer_metadata(UnitFixtureEngine())),f['analyzer_key'])
        finally:repo.close()
    def test_offline_verifier_without_credentials(self):
        self.assertEqual(len(verify_unit_evidence()['checks']),10)
        with patch('microbroker.cli.Settings.from_env',side_effect=AssertionError('No env')),contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['verify-unit-evidence']),0)

if __name__=='__main__':unittest.main()
