import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from microbroker.cli import main
from microbroker.data.models import DataValidationError
from microbroker.data.storage.repository import Repository, StorageError
from microbroker.intelligence.explanation import explain_analysis
from microbroker.intelligence.review import export_review,evaluate_review
from microbroker.intelligence.review_ui import review_html
from microbroker.intelligence.review_verification import review_fixture,verify_review_workflow
from microbroker.intelligence.policy import digest

class R15Tests(unittest.TestCase):
    def setUp(self):self.repo=Repository('sqlite:///:memory:');self.request=review_fixture(self.repo)
    def tearDown(self):self.repo.close()
    def bundle(self,root):
        folder=Path(root)/'review';export_review(self.repo,self.request,folder,engine='rules')
        self.review=json.loads((folder/'review.json').read_text());return folder
    def label(self,seen=None,source='human'):
        self.review['articles'][0]['labels'].update(document='mixed',companies={'AAPL':'negative','MSFT':'positive'},source=source,reviewer='simulated-reviewer',prediction_seen=seen)
    def write(self,path):Path(path).write_text(json.dumps(self.review));return path
    def test_explanation_exact_full_context_read_only(self):
        before=self.repo.connection.total_changes
        report=explain_analysis(self.repo,99915,symbol='MSFT',engine='rules')
        self.assertEqual(report['companies']['MSFT']['units'][0]['text'],'Microsoft beats expectations.')
        self.assertTrue(report['input_verified']);self.assertFalse(report['inference_performed'])
        self.assertEqual(self.repo.connection.total_changes,before)
    def test_explanation_keeps_document_company_label_difference(self):
        report=explain_analysis(self.repo,99915,symbol='MSFT',engine='rules')
        self.assertEqual(report['document_sentiment']['label'],'mixed')
        self.assertEqual(report['companies']['MSFT']['sentiment']['label'],'positive')
        self.assertTrue(report['companies']['MSFT']['document_company_label_difference'])
    def test_explanation_selects_exact_revision(self):
        report=explain_analysis(self.repo,99915,engine='rules')
        self.assertEqual(explain_analysis(self.repo,99915,revision=report['revision_hash'],engine='rules'),report)
    def test_explanation_wrong_symbol_or_engine_rejected(self):
        for kw in ({'symbol':'NVDA','engine':'rules'},{'engine':'finbert'},{'revision':'missing','engine':'rules'}):
            with self.assertRaises(DataValidationError):explain_analysis(self.repo,99915,**kw)
    def test_stored_analysis_tamper_rejected(self):
        self.repo.connection.execute("UPDATE news_analyses SET result_json='{}'");self.repo.connection.commit()
        with self.assertRaises(StorageError):explain_analysis(self.repo,99915,engine='rules')
    def test_rehashed_wrong_context_still_rejected(self):
        raw=self.repo.connection.execute('SELECT result_json FROM news_analyses WHERE article_id=99915').fetchone()[0]
        result=json.loads(raw);result['entity_sentiments']['MSFT']['context_sha256']='changed'
        self.repo.connection.execute('UPDATE news_analyses SET result_json=?,result_sha256=? WHERE article_id=99915',(json.dumps(result),digest(result)));self.repo.connection.commit()
        with self.assertRaises(StorageError):explain_analysis(self.repo,99915,engine='rules')
    def test_mapping_tamper_rejected(self):
        self.repo.connection.execute("UPDATE news_symbols SET method='changed' WHERE article_id=99915");self.repo.connection.commit()
        with self.assertRaises(StorageError):explain_analysis(self.repo,99915,engine='rules')
    def test_html_export_contains_blind_review_only(self):
        with tempfile.TemporaryDirectory() as root:
            folder=self.bundle(root);html=(folder/'review.html').read_text()
            self.assertNotIn('"predictions":',html)
            self.assertIn('review-labeled.json',html)
            self.assertTrue(all(a['labels']['prediction_seen'] is None for a in self.review['articles']))
    def test_html_script_escape_preserves_text_without_injection(self):
        review={'text':'</script><script>window.INJECTED=1</script>\u2028'}
        html=review_html(review)
        self.assertNotIn('</script><script>window.INJECTED',html)
        payload=html.split('<script id="review-data" type="application/json">',1)[1].split('</script>',1)[0]
        self.assertEqual(json.loads(payload),review)
    def test_downloaded_labels_evaluated_without_modifying_original(self):
        with tempfile.TemporaryDirectory() as root:
            folder=self.bundle(root);before=(folder/'review.json').read_bytes();self.label(False)
            path=self.write(Path(root)/'downloaded.json');report=evaluate_review(folder,path)
            self.assertEqual(report['human_blind']['document']['labeled'],1)
            self.assertEqual((folder/'review.json').read_bytes(),before)
    def test_seen_human_labels_excluded_from_blind_subset(self):
        with tempfile.TemporaryDirectory() as root:
            folder=self.bundle(root);self.label(True);self.write(folder/'review.json')
            report=evaluate_review(folder);self.assertEqual(report['human_only']['document']['labeled'],1)
            self.assertEqual(report['human_blind']['document']['labeled'],0)
            self.assertEqual(report['human_prediction_exposure_article_counts'],{'seen':1})
    def test_unknown_exposure_legacy_labels_not_assumed_blind(self):
        with tempfile.TemporaryDirectory() as root:
            folder=self.bundle(root);self.label();del self.review['articles'][0]['labels']['prediction_seen'];self.write(folder/'review.json')
            report=evaluate_review(folder);self.assertEqual(report['human_blind']['document']['labeled'],0)
            self.assertEqual(report['human_prediction_exposure_article_counts'],{'unknown':1})
    def test_ai_assisted_labels_not_in_blind_human_metrics(self):
        with tempfile.TemporaryDirectory() as root:
            folder=self.bundle(root);self.label(False,'ai_assisted');self.write(folder/'review.json')
            report=evaluate_review(folder);self.assertEqual(report['document']['labeled'],1)
            self.assertEqual(report['human_blind']['document']['labeled'],0)
    def test_invalid_exposure_values_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            folder=self.bundle(root)
            for value in (0,1,'false',[],{}):
                self.label(value);self.write(folder/'review.json')
                with self.assertRaises(DataValidationError):evaluate_review(folder)
    def test_downloaded_text_tamper_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            folder=self.bundle(root);self.review['articles'][0]['text']['headline']='changed';path=self.write(Path(root)/'labels.json')
            with self.assertRaises(DataValidationError):evaluate_review(folder,path)
    def test_unlabeled_ui_never_reports_accuracy(self):
        with tempfile.TemporaryDirectory() as root:
            folder=self.bundle(root);report=evaluate_review(folder)
            self.assertEqual(report['status'],'awaiting_labels')
            self.assertIsNone(report['human_blind']['document']['accuracy_when_predicted'])
    def test_cli_external_labels_needs_no_env_or_db(self):
        with tempfile.TemporaryDirectory() as root:
            folder=self.bundle(root);self.label(False);path=self.write(Path(root)/'labels.json')
            with patch('microbroker.cli.Settings.from_env',side_effect=AssertionError('No env')),contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(main(['evaluate-analysis-review','--review-dir',str(folder),'--labels-file',str(path)]),0)
    def test_verifier_offline_and_private(self):
        self.assertEqual(len(verify_review_workflow()['checks']),10)
        with patch('microbroker.cli.Settings.from_env',side_effect=AssertionError('No env')),contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['verify-analysis-review']),0)

if __name__=='__main__':unittest.main()
