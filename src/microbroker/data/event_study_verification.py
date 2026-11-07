"""Original controlled company/benchmark prices, not empirical signal validation."""
from copy import deepcopy
from decimal import Decimal
from .event_study import event_study,render_study
from .event_study_calculation import calculate,timestamp,nanos,MINUTE
from .event_collection import EventStudySessionRequest
from .news import normalize_news
from .news_replay import NewsReplayRequest,create_news_replay
from .analysis_replay import create_analysis_replay,replay_analysis
from .session_prices import create_session_prices
from .session_price_verification import empty_stats,apply_at
from .stream_models import StreamRequest,normalize_event
from .alignment import read_bars_snapshot
from .research import encode
from .storage.repository import Repository,StorageError
from .models import DataValidationError
from ..execution.strategy_verification import FixtureEngine
from ..intelligence.analysis import compute_analysis
from ..intelligence.policy import digest,analyzer_metadata

START='2026-02-02T15:00:00Z'
END='2026-02-02T16:10:00Z'


def add_news(repository,engine,article_id,text,at,*,updated=None,analyzed=None):
    n=normalize_news({'id':article_id,'headline':'Microsoft research update','content':text,
        'created_at':at,'updated_at':updated or at,'symbols':['MSFT'],
        'source':'original-synthetic','url':'https://example.com/r30-fixture'})
    data=n['data'];first=updated or at;key=digest(analyzer_metadata(engine));m=repository.marker
    repository.connection.execute(f'INSERT INTO news_versions VALUES ({",".join([m]*7)})',
        ('alpaca',article_id,n['revision_hash'],data['created_at'],data['updated_at'],first,encode(data)))
    for symbol,method in n['mappings'].items():
        repository.connection.execute(f'INSERT INTO news_symbols VALUES ({",".join([m]*5)})',('alpaca',article_id,n['revision_hash'],symbol,method))
    result=compute_analysis({'data':data,'first_seen':first,'mappings':n['mappings']},engine)
    repository.connection.execute(f'INSERT INTO news_analyses VALUES ({",".join([m]*9)})',
        ('alpaca',article_id,n['revision_hash'],key,'rules',result['input_sha256'],analyzed or first,encode(result),digest(result)))
    repository.connection.commit();return n


def study_fixture(repository,*,base=88400000,spy=True,minutes=69):
    engine=FixtureEngine();engine.metadata={**engine.metadata,'version':'r30-controlled-'+str(base)}
    for i,(minute,text) in enumerate(((0,'Microsoft profits rose.'),(2,'Microsoft shares fell.'),
            (4,'Microsoft publishes an update.'),(5,'Microsoft profits rose. Microsoft shares fell.'))):
        add_news(repository,engine,base+i,text,timestamp(nanos(START)+minute*MINUTE+10_000_000_000))
    symbols=('MSFT','SPY') if spy else ('MSFT',)
    request=StreamRequest(symbols,channels=('bars','updatedBars'));prior=repository.capture_bar_receipts
    repository.capture_bar_receipts=True
    try:
        run=repository.begin_stream(request,empty_stats());stats=empty_stats()
        for minute in range(minutes):
            for index,symbol in enumerate(symbols):
                close=(100 if symbol=='MSFT' else 200)+minute
                payload={'T':'b','S':symbol,'t':timestamp(nanos(START)+minute*MINUTE),
                    'o':str(close),'h':str(close+1),'l':str(close-1),'c':str(close),'v':100,'n':5}
                event=normalize_event(payload,request)
                stats=apply_at(repository,run,event,stats,timestamp(nanos(START)+(minute+1)*MINUTE+(index+1)*100_000_000))
        prices=create_session_prices(repository,run,symbols,'iex',START,END)
    finally:repository.capture_bar_receipts=prior
    news=create_news_replay(repository,NewsReplayRequest(END,('MSFT',),first_seen_since=START))
    analysis=create_analysis_replay(repository,news['replay_id'],END,digest(analyzer_metadata(engine)),'rules')
    return analysis['replay_id'],prices['replay_id']


def event_checks(repository):
    checks=[]
    def require(ok,name):
        if not ok:raise StorageError('Event study verification failed: '+name)
        checks.append(name)
    aid,bid=study_fixture(repository);result=event_study(repository,aid,bid)
    require(result['eligible_article_company_events']==3 and result['observed_absolute_outcomes']==12,'event_three_labels_four_horizons')
    positive=result['samples'][0];first=positive['outcomes'][0]
    require(positive['baseline']['bar_start']=='2026-02-02T15:01:00.000000000Z' and
        positive['feature_to_reference_completion_seconds']==110,'event_reference_after_full_postfeature_minute')
    require(first['target_completed_at']=='2026-02-02T15:03:00.000000000Z' and
        first['feature_to_target_completion_seconds']==170,'event_horizon_anchor_disclosed_not_feature_plus_one')
    require(first['asset_return_bps']=='99.00990099' and first['spy_return_bps']=='49.75124378' and
        first['market_adjusted_return_bps']=='49.25865721','event_simple_returns_and_synchronous_spy_difference')
    negative=result['samples'][1]['outcomes'][0]
    require(Decimal(negative['directional_market_adjusted_bps'])==-Decimal(negative['market_adjusted_return_bps']),
        'event_negative_direction_not_actual_short_pnl')
    require(result['groups']['neutral']['1']['asset']['n']==1 and result['groups']['neutral']['1']['directional_market_adjusted']['n']==0,
        'event_neutral_separate_no_invented_direction')
    require(result['samples'][3]['status']=='company_decision_unavailable_or_withheld' and not result['samples'][3]['outcomes'],
        'event_withheld_not_document_or_raw_fallback')
    short=event_study(repository,aid,bid,end='2026-02-02T15:10:00Z')
    require(short['groups']['positive']['60']['asset']['n']==0 and
        short['groups']['positive']['60']['asset']['mean_bps'] is None,'event_unobserved_long_horizon_not_zero_return')
    records,_,_=replay_analysis(repository,aid,_include_history=True);bars,_,_=read_bars_snapshot(repository,bid)
    missing=[b for b in bars if b['symbol']!='SPY'];source={**result['source'],'benchmark_in_snapshot':False}
    _,groups,_=calculate(records,missing,source)
    require(groups['positive']['1']['asset']['n']==1 and groups['positive']['1']['market_adjusted']['n']==0,
        'event_missing_benchmark_keeps_absolute_only')
    gap=[b for b in bars if not (b['symbol']=='MSFT' and b['timestamp']=='2026-02-02T15:03:00.000000000Z')]
    _,groups,_=calculate(records,gap,result['source'])
    require(groups['positive']['5']['asset']['n']==0,'event_missing_intermediate_minute_not_carried_forward')
    early=deepcopy(bars)
    for b in early:
        if b['symbol']=='MSFT' and b['timestamp']=='2026-02-02T15:01:00.000000000Z':
            b['receipt']['received_at']='2026-02-02T15:01:59.999999999Z'
    early.sort(key=lambda b:b['receipt']['received_at'])
    rows,_,_=calculate(records,early,result['source'])
    require(rows[0]['outcomes'][0]['status']=='asset_reference_receipt_before_completion','event_early_receipt_no_clock_tolerance')
    updates=[{**deepcopy(b),'receipt':{**b['receipt'],'event_type':'u'}} for b in bars]
    _,groups,_=calculate(records,updates,result['source'])
    require(groups['positive']['1']['asset']['n']==0,'event_updates_never_retroactively_supply_base_close')
    require(event_study(repository,aid,bid,limit=1)['groups']==result['groups'] and
        event_study(repository,aid,bid,limit=1)['study_sha256']==result['study_sha256'],'event_display_limit_not_calculation_limit')
    before=event_study(repository,aid,bid)
    repository.connection.execute(f'DELETE FROM stream_bar_receipts WHERE run_id={repository.marker}',(result['source']['stream_run_id'],))
    repository.connection.commit()
    require(event_study(repository,aid,bid)==before,'event_frozen_sources_isolated_from_live_receipt_changes')
    require(not result['beta_estimated'] and not result['statistical_significance_calculated'] and
        result['orders_sent']==0 and result['net_profit'] is None,'event_descriptive_not_profit_or_beta_alpha_validation')
    request=EventStudySessionRequest(('MSFT',),duration=5400)
    require(request.news_symbols==('MSFT',) and request.symbols==('MSFT','SPY') and not request.metadata()['orders_enabled'],
        'event_collection_adds_spy_prices_not_spy_news_or_orders')
    from .session import SessionRequest
    try:SessionRequest(('MSFT',),duration=5400)
    except DataValidationError:rejected=True
    else:rejected=False
    require(rejected,'event_long_collection_does_not_extend_paper_session_limit')
    require('eksik' in render_study(result).lower() and 'beta tahmini yoktur' in render_study(result),
        'event_text_reports_measurement_limits')
    original=repository.connection.execute(f'SELECT request_json FROM replay_runs WHERE id={repository.marker}',(bid,)).fetchone()[0];repository.connection.commit()
    repository.connection.execute(f'UPDATE replay_runs SET request_json={repository.marker} WHERE id={repository.marker}',('{}',bid));repository.connection.commit()
    try:event_study(repository,aid,bid)
    except (StorageError,DataValidationError):rejected=True
    else:rejected=False
    require(rejected,'event_tampered_price_manifest_rejected')
    repository.connection.execute(f'UPDATE replay_runs SET request_json={repository.marker} WHERE id={repository.marker}',(original,bid));repository.connection.commit()
    return checks


def verify_event_study():
    repository=Repository('sqlite:///:memory:')
    try:checks=event_checks(repository)
    finally:repository.close()
    return {'status':'passed','checks':checks,'fixture_source':'original_synthetic_company_spy_prices_and_controlled_times',
        'network_required':False,'model_required':False,'user_database_modified':False,'orders_sent':0,
        'profitability_validated':False,'statistical_significance_calculated':False}


def demo_event_study(output):
    import json
    from pathlib import Path
    destination=Path(output)
    if destination.exists():raise DataValidationError('Demo output already exists; choose a new folder')
    repository=Repository('sqlite:///:memory:')
    try:
        aid,bid=study_fixture(repository)
        report=event_study(repository,aid,bid);report['fixture_scope']='synthetic_r30_prices_not_empirical'
    finally:repository.close()
    destination.mkdir(parents=True)
    (destination/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    (destination/'report.txt').write_text(render_study(report)+'\n',encoding='utf-8')
    return {'status':'created','output':str(destination),'synthetic':True,'orders_sent':0,'network_required':False}
