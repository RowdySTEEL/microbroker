"""Synthetic unit polarity/coverage and private persistence controls."""
from datetime import datetime, timezone, timedelta
import json
from .analysis import compute_analysis, analyze_news, AnalysisRequest
from .sentiment import RulesEngine
from .explanation import explain_analysis
from .policy import digest
from .unit_evidence import score_units, company_unit_decision, MAX_COMPANY_UNITS
from ..data.news import collect_news, normalize_news, NewsRequest
from ..data.alignment import align_records
from ..data.session_prices import align_session_records
from ..data.storage.repository import Repository, StorageError

CASE='Microsoft shares fell to a low last year. Analysts are increasingly optimistic about Microsoft today.'

class UnitFixtureEngine(RulesEngine):
    metadata={'name':'rules','version':'synthetic-r18-unit-controls','trained_model':False}
    def score(self,text):
        label='positive' if '\n' not in text and 'optimistic' in text else 'negative'
        probs={'positive':0.9 if label=='positive' else 0.05,
               'negative':0.9 if label=='negative' else 0.05,'neutral':0.05}
        return {'label':label,'score':probs['positive']-probs['negative'],
                'probabilities':probs,'confidence':0.9,'confidence_calibrated':False,
                'token_truncated':False}

def unit_fixture(repository):
    now=datetime.now(timezone.utc)
    class Adapter:
        pages=1
        def iter_news(self,request):
            yield normalize_news({'id':99918,'headline':'Company update','content':CASE,
                'created_at':'2026-01-05T10:00:00Z','updated_at':'2026-01-05T10:00:00Z','symbols':['MSFT']})
    collect_news(Adapter(),repository,NewsRequest(('MSFT',),now-timedelta(days=1),now))
    request=AnalysisRequest(symbols=('MSFT',));engine=UnitFixtureEngine()
    stats=analyze_news(repository,request,engine)
    explanation=explain_analysis(repository,99918,symbol='MSFT',analyzer_key=stats['analyzer_key'],engine='rules')
    return request,engine,explanation

def unit_checks(repository):
    checks=[]
    def require(ok,name):
        if not ok:raise StorageError('Unit evidence verification failed: '+name)
        checks.append(name)
    request,engine,explanation=unit_fixture(repository)
    company=explanation['companies']['MSFT'];units=company['unit_evidence']
    require(company['sentiment']['label']=='negative' and company['sentiment']['confidence']==0.9,
            'high_margin_aggregate_preserved')
    require(units['unit_label_counts']=={'negative':1,'positive':1} and units['opposite_unit_labels'],
            'opposite_unit_labels_recorded')
    require(units['units'][0]['hints']['past_event_hint'] and units['units'][0]['hints']['time_reference_hint'],
            'historical_lexical_hint_not_resolved_date')
    require(company['research_decision']['label'] is None and 'opposite_context_unit_labels' in company['research_decision']['reason_codes'],
            'high_margin_unit_conflict_withheld')
    require(company['unit_evidence_verified'] and len(company['units'])==units['scored_unit_count'],
            'unit_explanation_offsets_and_hashes_verified')
    row=repository.connection.execute(f'SELECT result_json FROM news_analyses WHERE article_id={repository.marker} AND analyzer_key={repository.marker}',(99918,explanation['analyzer_key'])).fetchone()
    repository.connection.commit();result=json.loads(row[0])
    selected=[{'news':{'article_id':99918,'revision_hash':'fixture','timestamp':'2026-01-06T10:01:00.000000000Z','mappings':{'MSFT':'tag'}},
               'analysis':{'timestamp':'2026-01-06T10:01:00.000000000Z','result':result}}]
    bar={'symbol':'MSFT','timestamp':'2026-01-06T10:00:00.000000000Z','close':'100','feed':'iex','adjustment':'raw',
         'receipt':{'received_at':'2026-01-06T10:01:00.000000000Z','run_id':'fixture','ordinal':1,'event_type':'b','event_key':'fixture'}}
    require(align_records(selected,[bar],['MSFT'],5)[0]['status']=='company_research_decision_withheld',
            'unit_conflict_not_price_aligned')
    require(align_session_records(selected,[bar],['MSFT'],5)[0]['price'] is None,
            'same_session_unit_conflict_not_price_aligned')
    require(analyze_news(repository,request,engine)['inserted_analyses']==0,
            'unit_scoring_analysis_idempotency')
    # The scoring prefix is disclosed; missing evidence must never imply acceptance.
    context={'units':[{'field':'content_text','start_char':i*50,'end_char':i*50+40,'text':'Microsoft publishes item '+str(i)+'.'} for i in range(MAX_COMPANY_UNITS+1)]}
    limited=score_units(context,engine.score,{},set())
    require(limited['scored_unit_count']==MAX_COMPANY_UNITS and not limited['complete'],
            'company_unit_budget_disclosed')
    require(company_unit_decision(engine.score('An aggregate report'),None,'scored',limited)['status']=='incomplete_evidence',
            'incomplete_unit_coverage_withheld')
    return checks

def verify_unit_evidence():
    repository=Repository('sqlite:///:memory:')
    try:
        return {'status':'passed','checks':unit_checks(repository),
                'fixture_source':'original_synthetic_controlled_aggregate_and_unit_scores',
                'model_required':False,'network_required':False,'user_database_modified':False,
                'temporal_resolution_validated':False,'model_accuracy_validated':False,'orders_sent':0}
    finally:repository.close()
