from copy import deepcopy
import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from microbroker.cli import main
from microbroker.data.storage.repository import Repository, StorageError
from microbroker.data.alignment import align_records
from microbroker.data.analysis_replay import validate_analysis, create_analysis_replay, replay_analysis
from microbroker.data.news_replay import create_news_replay, NewsReplayRequest
from microbroker.intelligence.research_decision import research_decision, validate_research_decisions
from microbroker.intelligence.decision_verification import controlled_output, ControlledEngine, decision_fixture, verify_research_decision
from microbroker.intelligence.sentiment import RulesEngine
from microbroker.intelligence.attribution_verification import control_article
from microbroker.intelligence.analysis import compute_analysis, analyze_news, AnalysisRequest
from microbroker.intelligence.explanation import explain_analysis
from microbroker.intelligence.policy import digest, analyzer_metadata
from microbroker.intelligence.review import export_review, evaluate_review

class R17Tests(unittest.TestCase):
    def test_supplied_burry_values_raw_positive_but_no_research_label(self):
        raw=controlled_output();before=deepcopy(raw);decision=research_decision(raw)
        self.assertEqual(decision['status'],'conflicting_evidence')
        self.assertIsNone(decision['label']);self.assertEqual(raw,before)
        self.assertEqual(decision['raw_label'],'positive')
        self.assertAlmostEqual(decision['top_label_margin'],0.0266488158797964)
        self.assertEqual(len(decision['reason_codes']),2)
    def test_no_conflict_but_low_margin_is_withheld(self):
        raw=controlled_output();raw['mixed_chunk_polarity']=False;raw['chunks']=[]
        self.assertEqual(research_decision(raw)['status'],'ambiguous_model_output')
    def test_opposite_windows_detected_even_without_flag(self):
        raw=controlled_output();raw['mixed_chunk_polarity']=False
        self.assertEqual(research_decision(raw)['status'],'conflicting_evidence')
    def test_boundary_margin_inclusive(self):
        raw={'label':'positive','probabilities':{'positive':0.55,'negative':0.4,'neutral':0.05}}
        self.assertEqual(research_decision(raw)['status'],'accepted')
        raw['probabilities']['positive']=0.549999;raw['probabilities']['neutral']=0.050001
        self.assertEqual(research_decision(raw)['status'],'ambiguous_model_output')
    def test_neutral_can_be_accepted_without_becoming_directional(self):
        d=research_decision({'label':'neutral','probabilities':{'positive':0.1,'negative':0.1,'neutral':0.8}})
        self.assertEqual(d['label'],'neutral');self.assertFalse(d['usable_for_trading'])
    def test_no_sentiment_is_not_neutral(self):
        self.assertEqual(research_decision(None,context_status='shared_context_only')['status'],'context_unavailable')
        self.assertIsNone(research_decision(None)['label'])
    def test_unscored_rules_not_accepted(self):
        self.assertEqual(research_decision(RulesEngine().score('A report is scheduled.'))['status'],'unscored')
    def test_rule_conflict_never_becomes_mixed_prediction(self):
        raw=RulesEngine().score('Microsoft raises guidance. Microsoft cuts guidance.')
        self.assertEqual(raw['label'],'mixed');self.assertEqual(research_decision(raw)['status'],'conflicting_evidence')
        self.assertIsNone(research_decision(raw)['label'])
    def test_company_conflict_no_document_label_fallback(self):
        r=compute_analysis(control_article('Microsoft publishes an earnings report.'),ControlledEngine())
        selected=[{'news':{'article_id':1,'revision_hash':'fixture','timestamp':'2026-01-06T10:01:00Z','mappings':{'MSFT':'tag'}},'analysis':{'timestamp':'2026-01-06T10:01:00Z','result':r}}]
        bar={'symbol':'MSFT','timestamp':'2026-01-06T10:00:00Z','close':'100','feed':'iex','adjustment':'raw'}
        row=align_records(selected,[bar],['MSFT'],5)[0]
        self.assertEqual(row['status'],'company_research_decision_withheld');self.assertIsNone(row['company_label']);self.assertIsNone(row['price'])
    def test_document_conflict_does_not_block_clear_company_context(self):
        a=control_article('Microsoft beats expectations. Apple cuts guidance.')
        r=compute_analysis(a,RulesEngine())
        self.assertIsNone(r['research_decision']['label'])
        self.assertEqual(r['entity_sentiments']['MSFT']['research_decision']['label'],'positive')
    def test_rehashed_inconsistent_decision_rejected(self):
        r=compute_analysis(control_article('Microsoft publishes an earnings report.'),ControlledEngine())
        r['research_decision']['label']='positive'
        with self.assertRaises(StorageError):validate_research_decisions(r)
    def test_missing_company_decision_rejected(self):
        r=compute_analysis(control_article('Microsoft beats expectations.'),RulesEngine())
        del r['entity_sentiments']['MSFT']['research_decision']
        with self.assertRaises(StorageError):validate_research_decisions(r)
    def test_invalid_probability_structure_rejected(self):
        r=compute_analysis(control_article('Microsoft beats expectations.'),RulesEngine())
        r['sentiment']['probabilities']={'positive':1}
        with self.assertRaises(StorageError):validate_research_decisions(r)
    def test_raw_attribution_score_cannot_bypass_decision(self):
        r=compute_analysis(control_article('The company raises guidance after securing an agreement with Microsoft.'),RulesEngine())
        c=r['entity_sentiments']['MSFT']
        self.assertEqual(c['raw_mention_sentiment']['label'],'positive');self.assertIsNone(c['research_decision']['label'])
    def seed_legacy(self,repo):
        f=json.loads((Path(__file__).parent/'fixtures/r16_research_legacy.json').read_text())
        a=f['article'];d=a['data'];rev=digest(d);result=f['result']
        repo.connection.execute('INSERT INTO news_versions VALUES (?,?,?,?,?,?,?)',('alpaca',99917,rev,d['created_at'],d['updated_at'],a['first_seen'],json.dumps(d)))
        repo.connection.execute('INSERT INTO news_articles VALUES (?,?,?,?,?,?,?,?)',('alpaca',99917,rev,d['updated_at'],a['first_seen'],a['first_seen'],'synthetic',0))
        for symbol,method in a['mappings'].items():repo.connection.execute('INSERT INTO news_symbols VALUES (?,?,?,?,?)',('alpaca',99917,rev,symbol,method))
        repo.connection.execute('INSERT INTO news_analyses VALUES (?,?,?,?,?,?,?,?,?)',('alpaca',99917,rev,f['analyzer_key'],'rules',result['input_sha256'],'2026-01-06T10:01:00Z',json.dumps(result),digest(result)))
        repo.connection.commit();return f,rev
    def test_actual_r16_source_fixture_not_reinterpreted_in_alignment(self):
        repo=Repository('sqlite:///:memory:')
        try:
            f,rev=self.seed_legacy(repo)
            selected=[{'news':{'article_id':99917,'revision_hash':rev,'timestamp':'2026-01-06T10:01:00Z','mappings':{'MSFT':'tag'}},'analysis':{'timestamp':'2026-01-06T10:01:00Z','result':f['result']}}]
            row=align_records(selected,[{'symbol':'MSFT','timestamp':'2026-01-06T10:00:00Z','close':'100','feed':'iex','adjustment':'raw'}],['MSFT'],5)[0]
            self.assertEqual(row['status'],'matched');self.assertNotIn('company_research_decision',row)
        finally:repo.close()
    def test_old_r16_explanation_and_sha_preserved_after_new_analysis(self):
        repo=Repository('sqlite:///:memory:')
        try:
            f,rev=self.seed_legacy(repo)
            old=explain_analysis(repo,99917,engine='rules',analyzer_key=f['analyzer_key'])
            self.assertNotIn('document_research_decision',old)
            analyze_news(repo,AnalysisRequest(symbols=('MSFT',)),ControlledEngine())
            self.assertEqual(explain_analysis(repo,99917,engine='rules',analyzer_key=f['analyzer_key']),old)
            self.assertEqual(repo.connection.execute('SELECT COUNT(*) FROM news_analyses').fetchone()[0],2)
            self.assertNotEqual(digest(analyzer_metadata(ControlledEngine())),f['analyzer_key'])
        finally:repo.close()
    def test_frozen_replay_repeats_and_retains_decisions_after_source_deletion(self):
        repo=Repository('sqlite:///:memory:')
        try:
            request,engine,explanation=decision_fixture(repo)
            news=create_news_replay(repo,NewsReplayRequest('2099-01-01T00:00:00Z',('MSFT',)))
            a=create_analysis_replay(repo,news['replay_id'],'2099-01-01T00:00:00Z',explanation['analyzer_key'],'rules')
            first=replay_analysis(repo,a['replay_id'])
            self.assertIsNone(first['samples'][0]['research_decision']['label'])
            repo.connection.execute('DELETE FROM news_analyses');repo.connection.commit()
            self.assertEqual(replay_analysis(repo,a['replay_id']),first)
        finally:repo.close()
    def test_explanation_rejects_semantically_inconsistent_rehashed_result(self):
        repo=Repository('sqlite:///:memory:')
        try:
            decision_fixture(repo)
            raw=repo.connection.execute('SELECT result_json FROM news_analyses WHERE article_id=99917').fetchone()[0]
            r=json.loads(raw);r['research_decision']['label']='positive'
            repo.connection.execute('UPDATE news_analyses SET result_json=?,result_sha256=?',(json.dumps(r),digest(r)));repo.connection.commit()
            with self.assertRaises(StorageError):explain_analysis(repo,99917,engine='rules')
        finally:repo.close()
    def test_legacy_review_evaluates_raw_predictions_without_new_gate(self):
        repo=Repository('sqlite:///:memory:')
        try:
            f,rev=self.seed_legacy(repo)
            with tempfile.TemporaryDirectory() as root:
                folder=Path(root)/'review';export_review(repo,AnalysisRequest(symbols=('MSFT',)),folder,analyzer_key=f['analyzer_key'],engine='rules')
                labels=json.loads((folder/'review.json').read_text());labels['articles'][0]['labels'].update(document='mixed',source='human',reviewer='test',prediction_seen=False)
                (folder/'review.json').write_text(json.dumps(labels))
                report=evaluate_review(folder)
                self.assertEqual(report['document']['predicted'],1)
                self.assertEqual(report['research_decision']['gated_articles'],0)
                self.assertEqual(report['research_decision']['document']['labeled'],0)
        finally:repo.close()
    def test_offline_verifier_does_not_load_credentials(self):
        self.assertEqual(len(verify_research_decision()['checks']),11)
        with patch('microbroker.cli.Settings.from_env',side_effect=AssertionError('No credentials')),contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['verify-research-decision']),0)

if __name__=='__main__':unittest.main()
