import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import test_r5 as baseline
from microbroker.cli import main
from microbroker.data.models import DataValidationError
from microbroker.data.storage.repository import StorageError
from microbroker.intelligence.review import export_review,evaluate_review,metrics

class ReviewTests(unittest.TestCase):
    setUp=baseline.R5Tests.setUp
    tearDown=baseline.R5Tests.tearDown
    collect=baseline.R5Tests.collect
    analyze=baseline.R5Tests.analyze
    def bundle(self,folder,news=None):
        self.collect(news or baseline.NEWS);self.analyze()
        export_review(self.repo,self.request,folder,engine='rules')
        return Path(folder)
    def edit(self,folder,change):
        path=folder/'review.json';value=json.loads(path.read_text());change(value)
        path.write_text(json.dumps(value))
    def test_unlabeled_export_has_no_predictions_or_fake_accuracy(self):
        with tempfile.TemporaryDirectory() as root:
            folder=self.bundle(Path(root)/'review')
            value=json.loads((folder/'review.json').read_text())
            self.assertNotIn('predictions',value)
            self.assertEqual(value['articles'][0]['labels']['document'],None)
            report=evaluate_review(folder)
            self.assertEqual(report['status'],'awaiting_labels')
            self.assertIsNone(report['document']['accuracy_including_abstentions'])
    def test_correct_and_wrong_labels_are_compared_without_inference(self):
        with tempfile.TemporaryDirectory() as root:
            folder=self.bundle(Path(root)/'review')
            def label(value):value['articles'][0]['labels'].update(document='positive',companies={'MSFT':'negative'},source='human',reviewer='reviewer-1')
            self.edit(folder,label)
            with patch('microbroker.intelligence.sentiment.RulesEngine.score',side_effect=AssertionError):report=evaluate_review(folder)
            self.assertEqual(report['document']['accuracy_including_abstentions'],1)
            self.assertEqual(report['companies']['MSFT']['accuracy_including_abstentions'],0)
            self.assertFalse(report['model_accuracy_validated'])
    def test_ai_assisted_labels_not_counted_as_human_accuracy(self):
        with tempfile.TemporaryDirectory() as root:
            folder=self.bundle(Path(root)/'review')
            self.edit(folder,lambda v:v['articles'][0]['labels'].update(document='positive',source='ai_assisted',reviewer='assistant-1'))
            report=evaluate_review(folder)
            self.assertEqual(report['source_article_counts'],{'ai_assisted':1})
            self.assertEqual(report['human_only']['document']['labeled'],0)
    def test_abstention_counts_in_coverage_and_all_label_accuracy(self):
        result=metrics([('positive','positive'),('negative',None)])
        self.assertEqual(result['coverage'],0.5)
        self.assertEqual(result['accuracy_when_predicted'],1)
        self.assertEqual(result['accuracy_including_abstentions'],0.5)
    def test_company_metadata_only_is_model_abstention_not_neutral(self):
        with tempfile.TemporaryDirectory() as root:
            folder=self.bundle(Path(root)/'review',{**baseline.NEWS,'headline':'A vendor raises guidance.','content':''})
            self.edit(folder,lambda v:v['articles'][0]['labels'].update(companies={'MSFT':'neutral'},source='human',reviewer='r'))
            report=evaluate_review(folder)['companies']['MSFT']
            self.assertEqual(report['abstained'],1);self.assertEqual(report['coverage'],0)
    def test_tampered_frozen_text_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            folder=self.bundle(Path(root)/'review')
            self.edit(folder,lambda v:v['articles'][0]['text'].update(headline='altered'))
            with self.assertRaises(DataValidationError):evaluate_review(folder)
    def test_tampered_reference_predictions_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            folder=self.bundle(Path(root)/'review');path=folder/'reference.json';value=json.loads(path.read_text())
            value['payload']['predictions']['1']['document']='negative';path.write_text(json.dumps(value))
            with self.assertRaises(DataValidationError):evaluate_review(folder)
    def test_invalid_label_and_missing_provenance_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            folder=self.bundle(Path(root)/'review')
            for label in ('buy','positive'):
                self.edit(folder,lambda v:v['articles'][0]['labels'].update(document=label))
                with self.assertRaises(DataValidationError):evaluate_review(folder)
    def test_reference_labels_remain_bound_to_export_after_news_revision(self):
        with tempfile.TemporaryDirectory() as root:
            folder=self.bundle(Path(root)/'review')
            self.collect({**baseline.NEWS,'headline':'Microsoft cuts guidance.','updated_at':'2026-01-05T14:31:00Z'})
            self.edit(folder,lambda v:v['articles'][0]['labels'].update(document='positive',source='human',reviewer='r'))
            self.assertEqual(evaluate_review(folder)['document']['accuracy_including_abstentions'],1)
    def test_partial_coverage_is_explicit(self):
        with tempfile.TemporaryDirectory() as root:
            self.collect(baseline.NEWS,{**baseline.NEWS,'id':2});self.analyze()
            folder=Path(root)/'review';export_review(self.repo,self.request,folder,engine='rules')
            self.edit(folder,lambda v:v['articles'][0]['labels'].update(document='positive',source='synthetic',reviewer='test'))
            report=evaluate_review(folder)
            self.assertEqual(report['articles'],2);self.assertEqual(report['document']['labeled'],1)
    def test_existing_bundle_never_overwritten(self):
        with tempfile.TemporaryDirectory() as root:
            folder=self.bundle(Path(root)/'review');before=(folder/'review.json').read_bytes()
            with self.assertRaises(DataValidationError):export_review(self.repo,self.request,folder,engine='rules')
            self.assertEqual((folder/'review.json').read_bytes(),before)
    def test_pending_revision_export_fails_without_partial_bundle(self):
        with tempfile.TemporaryDirectory() as root:
            self.collect(baseline.NEWS);self.analyze()
            self.collect({**baseline.NEWS,'headline':'Microsoft cuts guidance.','updated_at':'2026-01-05T14:31:00Z'})
            folder=Path(root)/'review'
            with self.assertRaises(DataValidationError):export_review(self.repo,self.request,folder,engine='rules')
            self.assertFalse(folder.exists())
    def test_export_preserves_database_tables(self):
        with tempfile.TemporaryDirectory() as root:
            self.collect(baseline.NEWS);self.analyze()
            before=list(self.repo.connection.iterdump())
            export_review(self.repo,self.request,Path(root)/'review',engine='rules')
            self.assertEqual(list(self.repo.connection.iterdump()),before)
    def test_cli_evaluation_needs_no_environment_database_or_model(self):
        with tempfile.TemporaryDirectory() as root:
            folder=self.bundle(Path(root)/'review');output=io.StringIO()
            with patch('microbroker.cli.Settings.from_env',side_effect=AssertionError),contextlib.redirect_stdout(output):
                self.assertEqual(main(['evaluate-analysis-review','--review-dir',str(folder)]),0)
            self.assertEqual(json.loads(output.getvalue())['status'],'awaiting_labels')
    def test_macro_f1_for_reference_present_classes(self):
        result=metrics([('positive','positive'),('negative','positive')])
        self.assertAlmostEqual(result['macro_f1_over_reference_present_classes'],1/3)
    def test_export_integrity_failure_does_not_create_bundle(self):
        with tempfile.TemporaryDirectory() as root:
            self.collect(baseline.NEWS);self.analyze()
            self.repo.connection.execute("UPDATE news_analyses SET result_json='{}'");self.repo.connection.commit()
            folder=Path(root)/'review'
            with self.assertRaises(StorageError):export_review(self.repo,self.request,folder,engine='rules')
            self.assertFalse(folder.exists())
