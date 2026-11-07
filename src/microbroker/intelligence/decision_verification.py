"""Controlled outputs and private persistence checks; not FinBERT accuracy."""
from copy import deepcopy
from datetime import datetime, timezone, timedelta
import json
from pathlib import Path
import tempfile
from .research_decision import research_decision, validate_research_decisions
from .analysis import compute_analysis, analyze_news, AnalysisRequest, analysis_summary
from .sentiment import RulesEngine
from .explanation import explain_analysis
from .review import export_review, evaluate_review
from ..data.news import normalize_news, collect_news, NewsRequest
from ..data.storage.repository import Repository, StorageError
from ..data.alignment import align_records
from ..data.session_prices import align_session_records

def controlled_output():
    # Reproduce the supplied Burry output's aggregate/window values without model inference.
    probabilities = {'positive':0.40723707024932754, 'negative':0.38058825436953114,
                     'neutral':0.2121746284319154}
    return {'label':'positive', 'score':probabilities['positive']-probabilities['negative'],
            'probabilities':probabilities, 'confidence':None, 'confidence_calibrated':False,
            'aggregation':'content_token_weighted_probability_mean', 'chunk_count':2,
            'chunks':[{'label':'positive','content_tokens':510},
                      {'label':'negative','content_tokens':336}],
            'mixed_chunk_polarity':True, 'token_truncated':False}

class ControlledEngine(RulesEngine):
    metadata = {'name':'rules', 'version':'synthetic-r17-controlled-output', 'trained_model':False}
    def score(self, text):
        return controlled_output()

def decision_fixture(repository):
    now=datetime.now(timezone.utc)
    class Adapter:
        pages=1
        def iter_news(self, request):
            yield normalize_news({'id':99917,'headline':'Microsoft publishes an earnings report.',
                'content':'','symbols':['MSFT'], 'created_at':'2026-01-05T10:00:00Z',
                'updated_at':'2026-01-05T10:00:00Z'})
    collect_news(Adapter(),repository,NewsRequest(('MSFT',),now-timedelta(days=1),now))
    request=AnalysisRequest(symbols=('MSFT',));engine=ControlledEngine()
    stats=analyze_news(repository,request,engine)
    explanation=explain_analysis(repository,99917,symbol='MSFT',engine='rules',analyzer_key=stats['analyzer_key'])
    return request,engine,explanation

def decision_checks(repository):
    checks=[]
    def require(ok,name):
        if not ok:raise StorageError('Research decision verification failed: '+name)
        checks.append(name)
    request,engine,explanation=decision_fixture(repository)
    company=explanation['companies']['MSFT']
    require(explanation['document_sentiment']['label']=='positive' and
            explanation['document_research_decision']['label'] is None and
            explanation['document_research_decision']['status']=='conflicting_evidence',
            'opposite_windows_preserve_raw_label_withhold_decision')
    require(company['sentiment']['label']=='positive' and company['research_decision']['label'] is None,
            'company_conflict_withheld_separately')
    raw=repository.connection.execute(f'SELECT result_json FROM news_analyses WHERE article_id={repository.marker} AND analyzer_key={repository.marker}',(99917,explanation['analyzer_key'])).fetchone()[0]
    repository.connection.commit()
    result=json.loads(raw)
    selected=[{'news':{'article_id':99917,'revision_hash':'fixture','timestamp':'2026-01-06T10:01:00.000000000Z','mappings':{'MSFT':'tag'}},
        'analysis':{'timestamp':'2026-01-06T10:01:00.000000000Z','result':result}}]
    bar={'symbol':'MSFT','timestamp':'2026-01-06T10:00:00.000000000Z','close':'100','feed':'iex','adjustment':'raw',
         'receipt':{'received_at':'2026-01-06T10:01:00.000000000Z','run_id':'fixture','ordinal':1,'event_type':'b','event_key':'fixture'}}
    require(align_records(selected,[bar],['MSFT'],5)[0]['status']=='company_research_decision_withheld',
            'conflicting_company_not_price_aligned')
    require(align_session_records(selected,[bar],['MSFT'],5)[0]['price'] is None,
            'same_session_conflict_not_price_aligned')
    require(analyze_news(repository,request,engine)['inserted_analyses']==0,
            'research_decision_analysis_idempotency')
    summary=analysis_summary(repository,request,analyzer_key=explanation['analyzer_key'],engine='rules')
    require(summary['label_counts'].get('positive',0)>=1 and summary['research_decision_status_counts'].get('conflicting_evidence',0)>=1,
            'summary_raw_and_decision_counts_separated')
    with tempfile.TemporaryDirectory() as root:
        folder=Path(root)/'review'
        export_review(repository,request,folder,analyzer_key=explanation['analyzer_key'],engine='rules')
        labels=json.loads((folder/'review.json').read_text())
        for item in labels['articles']:
            if item['id']==99917:item['labels'].update(document='mixed',companies={'MSFT':'mixed'},source='human',reviewer='simulated',prediction_seen=False)
        (folder/'review.json').write_text(json.dumps(labels))
        report=evaluate_review(folder)
        require(report['document']['predicted']==1 and report['research_decision']['document']['abstained']==1 and
                report['research_decision']['document']['accuracy_when_predicted'] is None,
                'review_abstention_not_counted_as_correct')
        require(report['research_decision']['human_blind']['companies']['MSFT']['abstained']==1,
                'blind_company_decision_metrics')
    changed=deepcopy(result);changed['entity_sentiments']['MSFT']['research_decision']['label']='positive'
    rejected=False
    try:validate_research_decisions(changed)
    except StorageError:rejected=True
    require(rejected,'inconsistent_research_decision_rejected')
    return checks

def verify_research_decision():
    repo=Repository('sqlite:///:memory:')
    try:
        checks=decision_checks(repo)
        near=controlled_output();near['mixed_chunk_polarity']=False;near['chunks']=[]
        if research_decision(near)['status']!='ambiguous_model_output':
            raise StorageError('Research decision margin verification failed')
        checks.append('low_margin_without_opposite_windows_withheld')
        clean=RulesEngine().score('Microsoft beats expectations.')
        if research_decision(clean)['label']!='positive':
            raise StorageError('Research decision explicit polarity verification failed')
        checks.append('clear_polarity_preserved')
        return {'status':'passed','checks':checks,'fixture_source':'synthetic_controlled_outputs_and_supplied_score_values',
                'model_required':False,'network_required':False,'user_database_modified':False,
                'threshold_calibrated':False,'model_accuracy_validated':False,'orders_sent':0}
    finally:repo.close()
