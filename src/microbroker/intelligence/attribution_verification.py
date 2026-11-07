"""Original role/subject controls, not general entity attribution accuracy."""
from datetime import datetime,timezone,timedelta
from .analysis import compute_analysis,analyze_news,AnalysisRequest
from .sentiment import RulesEngine
from .policy import company_contexts, digest
from .explanation import explain_analysis
from ..data.news import normalize_news,collect_news,NewsRequest
from ..data.storage.repository import Repository,StorageError
from ..data.alignment import align_records

CASE='The company raises guidance after securing an agreement with Microsoft.'

def control_article(text):
    row=normalize_news({'id':99916,'headline':'External provider update','content':text,
        'created_at':'2026-01-05T10:00:00Z','updated_at':'2026-01-05T10:00:00Z','symbols':['MSFT']})
    return {'data':row['data'],'mappings':row['mappings'],'first_seen':'2026-01-06T00:00:00Z'}

def attribution_fixture(repository):
    now=datetime.now(timezone.utc)
    class Adapter:
        pages=1
        def iter_news(self,request):
            yield normalize_news({'id':99916,'headline':'External provider update','content':CASE,
                'created_at':'2026-01-05T10:00:00Z','updated_at':'2026-01-05T10:00:00Z','symbols':['MSFT']})
    collect_news(Adapter(),repository,NewsRequest(('MSFT',),now-timedelta(days=1),now))
    request=AnalysisRequest(symbols=('MSFT',));analyze_news(repository,request,RulesEngine())
    return request,explain_analysis(repository,99916,symbol='MSFT',engine='rules')

def verify_attribution():
    checks=[]
    def require(ok,name):
        if not ok:raise StorageError('Attribution verification failed: '+name)
        checks.append(name)
    result=compute_analysis(control_article(CASE),RulesEngine());entity=result['entity_sentiments']['MSFT']
    require(entity['status']=='attribution_ambiguous' and entity['sentiment'] is None,'unresolved_subject_not_company_score')
    require(entity['raw_mention_sentiment']['label']=='positive','raw_mention_model_output_preserved')
    fields=lambda text:{'headline':text,'summary':'','content_text':''}
    context=company_contexts(fields('An external vendor won a contract with Microsoft.'),{'MSFT':'tag'})['MSFT']
    require(context['ambiguous_units'][0]['reason']=='counterparty_role_not_established','unknown_vendor_counterparty_excluded')
    context=company_contexts(fields("Microsoft's supplier wins a contract."),{'MSFT':'tag'})['MSFT']
    require(context['ambiguous_units'][0]['reason']=='third_party_subject','third_party_possessive_subject_excluded')
    explicit=compute_analysis(control_article('Microsoft beats expectations and raises guidance.'),RulesEngine())
    require(explicit['entity_sentiments']['MSFT']['sentiment']['label']=='positive','explicit_target_subject_preserved')
    mixed=compute_analysis(control_article(CASE+' Microsoft beats expectations.'),RulesEngine())
    require(mixed['entity_sentiments']['MSFT']['status']=='scored' and mixed['entity_sentiments']['MSFT']['context_unit_count']==1,'valid_context_survives_ambiguous_unit_removal')
    context=company_contexts(fields('money flows are positive in Microsoft and Apple.'),{'MSFT':'tag'})['MSFT']
    require(len(context['units'])==1 and context['ambiguous_units_excluded']==0,'exact_money_flow_exception_preserved')
    selected=[{'news':{'article_id':99916,'revision_hash':'fixture','timestamp':'2026-01-06T10:01:00.000000000Z','mappings':{'MSFT':'tag'}},
        'analysis':{'timestamp':'2026-01-06T10:01:00.000000000Z','result':result}}]
    bar={'symbol':'MSFT','timestamp':'2026-01-06T10:00:00.000000000Z','close':'100','feed':'iex','adjustment':'raw'}
    row=align_records(selected,[bar],['MSFT'],5)[0]
    require(row['status']=='company_sentiment_unavailable' and row['price'] is None,'raw_positive_not_alignment_fallback')
    repo=Repository('sqlite:///:memory:')
    try:
        request,explanation=attribution_fixture(repo)
        require(explanation['companies']['MSFT']['excluded_units'][0]['reason']=='unresolved_subject','excluded_context_explanation_verified')
        repeat=analyze_news(repo,request,RulesEngine())
        require(repeat['inserted_analyses']==0,'repeat_analysis_idempotent')
    finally:repo.close()
    return {'status':'passed','checks':checks,'fixture_source':'original_synthetic_subject_and_role_cases',
        'model_required':False,'network_required':False,'user_database_modified':False,
        'model_accuracy_validated':False,'entity_attribution_validated':False}
