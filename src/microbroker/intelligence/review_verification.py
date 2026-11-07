"""Synthetic workflow verification, never a model accuracy evaluation."""
import json
import tempfile
from pathlib import Path
from datetime import datetime, timezone, timedelta
from .analysis import AnalysisRequest, analyze_news
from .sentiment import RulesEngine
from .explanation import explain_analysis
from .review import export_review, evaluate_review
from ..data.news import normalize_news, collect_news, NewsRequest
from ..data.storage.repository import Repository, StorageError
from ..data.models import DataValidationError

def review_fixture(repository):
    now=datetime.now(timezone.utc)
    class Adapter:
        pages=1
        def iter_news(self,request):
            yield normalize_news({'id':99915,'headline':'Microsoft beats expectations. Apple cuts guidance.',
                'created_at':'2026-01-05T10:00:00Z','updated_at':'2026-01-05T10:00:00Z',
                'symbols':['MSFT','AAPL']})
    collect_news(Adapter(),repository,NewsRequest(('AAPL','MSFT'),now-timedelta(days=1),now))
    request=AnalysisRequest(symbols=('AAPL','MSFT'))
    analyze_news(repository,request,RulesEngine())
    return request

def verify_review_workflow():
    repo=Repository('sqlite:///:memory:');checks=[]
    def require(ok,name):
        if not ok:raise StorageError('Review workflow verification failed: '+name)
        checks.append(name)
    try:
        request=review_fixture(repo);before=repo.connection.total_changes
        explanation=explain_analysis(repo,99915,symbol='MSFT',engine='rules')
        company=explanation['companies']['MSFT']
        require(explanation['input_verified'] and company['context_verified'],'stored_input_and_context_hashes_verified')
        require(len(company['units'])==1 and company['units'][0]['text']=='Microsoft beats expectations.','full_company_context_units_exposed')
        require(repo.connection.total_changes==before and not explanation['inference_performed'],'explanation_no_database_changes_or_inference')
        with tempfile.TemporaryDirectory() as directory:
            folder=Path(directory)/'review';export_review(repo,request,folder,engine='rules')
            review=json.loads((folder/'review.json').read_text())
            html=(folder/'review.html').read_text()
            require('predictions' not in review and '"predictions":' not in html,'labeling_page_blind_to_frozen_predictions')
            require(evaluate_review(folder)['status']=='awaiting_labels','unlabeled_not_claimed_accuracy')
            review['articles'][0]['labels'].update(document='neutral',companies={'AAPL':'negative','MSFT':'positive'},source='human',reviewer='simulated-fixture-reviewer',prediction_seen=True)
            labels=Path(directory)/'review-labeled.json';labels.write_text(json.dumps(review))
            report=evaluate_review(folder,labels)
            require(report['human_only']['document']['labeled']==1 and report['human_blind']['document']['labeled']==0,'seen_predictions_excluded_from_blind_metrics')
            review['articles'][0]['labels']['prediction_seen']=False;labels.write_text(json.dumps(review))
            require(evaluate_review(folder,labels)['human_blind']['document']['labeled']==1,'self_reported_blind_human_metrics_separated')
            review['articles'][0]['labels']['source']='ai_assisted';labels.write_text(json.dumps(review))
            require(evaluate_review(folder,labels)['human_blind']['document']['labeled']==0,'ai_assisted_labels_not_human_blind')
            review['articles'][0]['text']['headline']='changed';labels.write_text(json.dumps(review))
            rejected=False
            try:evaluate_review(folder,labels)
            except DataValidationError:rejected=True
            require(rejected,'downloaded_label_file_identity_tamper_rejected')
            require(repo.connection.total_changes==before,'export_and_evaluation_read_only')
        return {'status':'passed','checks':checks,'fixture_source':'original_synthetic_text_and_simulated_reviewer_flags',
            'network_required':False,'model_required':False,'user_database_modified':False,
            'model_accuracy_validated':False,'human_reference_collected':False}
    finally:repo.close()
