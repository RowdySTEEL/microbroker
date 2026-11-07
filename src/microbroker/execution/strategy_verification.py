"""Original controlled analyses and receipts, not downloaded news or model accuracy."""
from copy import deepcopy
import json
from pathlib import Path
from .strategy import run_strategy, strategy_report, load_inputs, simulate, strategy_policy
from . import paper_ledger as ledger
from ..data.news import normalize_news
from ..data.news_replay import create_news_replay, NewsReplayRequest
from ..data.analysis_replay import create_analysis_replay
from ..data.session_prices import create_session_prices
from ..data.session_price_verification import apply_at, empty_stats
from ..data.stream_models import StreamRequest, normalize_event
from ..data.storage.repository import Repository, StorageError
from ..data.models import DataValidationError
from ..data.research import encode
from ..intelligence.analysis import compute_analysis
from ..intelligence.policy import digest, analyzer_metadata
from ..intelligence.sentiment import RulesEngine

START='2026-01-05T10:00:00Z'
END='2026-01-05T10:06:00Z'

class FixtureEngine(RulesEngine):
    metadata={'name':'rules','version':'r20-original-controlled-sentiments','trained_model':False}
    def score(self,text):
        label='negative' if 'fell' in text else 'positive' if 'rose' in text else 'neutral'
        probs={k:0.9 if k==label else 0.05 for k in ('positive','negative','neutral')}
        return {'label':label,'probabilities':probs,'score':probs['positive']-probs['negative'],
                'confidence':0.9,'confidence_calibrated':False,'token_truncated':False}

def strategy_fixture(repository,article_base=999200):
    engine=FixtureEngine()
    if article_base!=999200:
        engine.metadata={**engine.metadata,'version':engine.metadata['version']+'-'+str(article_base)}
    key=digest(analyzer_metadata(engine));m=repository.marker
    scenarios=[('00','Microsoft profits rose sharply.'),('02','Microsoft shares fell sharply.'),
               ('04','Microsoft publishes an update.'),('05','Microsoft profits rose. Microsoft shares fell.')]
    for index,(minute,text) in enumerate(scenarios):
        n=normalize_news({'id':article_base+index,'headline':'Microsoft update','content':text,
            'created_at':'2026-01-05T10:'+minute+':00Z','updated_at':'2026-01-05T10:'+minute+':00Z',
            'symbols':['MSFT'],'source':'original-synthetic','url':'https://example.com/r20-fixture'})
        data=n['data'];first='2026-01-05T10:'+minute+':01Z'
        repository.connection.execute(f"INSERT INTO news_versions VALUES ({','.join([m]*7)})",
            ('alpaca',data['id'],n['revision_hash'],data['created_at'],data['updated_at'],first,encode(data)))
        for symbol,method in n['mappings'].items():
            repository.connection.execute(f"INSERT INTO news_symbols VALUES ({','.join([m]*5)})",
                ('alpaca',data['id'],n['revision_hash'],symbol,method))
        result=compute_analysis({'data':data,'first_seen':first,'mappings':n['mappings']},engine)
        repository.connection.execute(f"INSERT INTO news_analyses VALUES ({','.join([m]*9)})",
            ('alpaca',data['id'],n['revision_hash'],key,'rules',result['input_sha256'],
             '2026-01-05T10:'+minute+':10Z',encode(result),digest(result)))
    repository.connection.commit()
    request=StreamRequest(('MSFT',),channels=('bars','updatedBars'));prior=repository.capture_bar_receipts
    try:
        repository.capture_bar_receipts=True;run=repository.begin_stream(request,empty_stats());stats=empty_stats()
        for minute,close in [('00',90),('01',100),('02',105),('03',110),('04',111)]:
            payload={'T':'b','S':'MSFT','t':'2026-01-05T10:'+minute+':00Z',
                     'o':close,'h':close+1,'l':close-1,'c':close,'v':100,'n':5}
            receipt='2026-01-05T10:'+str(int(minute)+1).zfill(2)+':00.1Z'
            stats=apply_at(repository,run,normalize_event(payload,request),stats,receipt)
        prices=create_session_prices(repository,run,('MSFT',),'iex',START,END)
    finally:repository.capture_bar_receipts=prior
    news=create_news_replay(repository,NewsReplayRequest(END,('MSFT',)))
    analysis=create_analysis_replay(repository,news['replay_id'],END,key,'rules')
    return analysis['replay_id'],prices['replay_id']

def strategy_checks(repository):
    checks=[]
    def require(ok,name):
        if not ok:raise StorageError('Strategy verification failed: '+name)
        checks.append(name)
    def rejects(call):
        try:call()
        except (StorageError,DataValidationError):return True
        return False
    analysis_id,prices_id=strategy_fixture(repository)
    result=run_strategy(repository,'controlled-r20',analysis_id,prices_id)
    require(result['decision_counts']=={'buy':1,'sell':1,'wait':2},'strategy_positive_buy_negative_exit_neutral_wait')
    fills=[row for row in result['samples'] if row['action'] in {'buy','sell'}]
    require(fills[0]['reference_price']=='100' and fills[0]['bar_start']=='2026-01-05T10:01:00.000000000Z',
        'strategy_no_fill_at_preanalysis_bar_close')
    require(all(row['bar_start']>=row['available_at'] and row['execution_at']>row['available_at'] for row in fills),
        'strategy_only_following_completed_minute')
    require(result['reason_counts']['company_decision_withheld']==1,'strategy_raw_label_not_withheld_fallback')
    require(result['valuation']['positions']=={} and result['valuation']['fees_paid']=='2.00000000',
        'strategy_long_only_exit_and_costs')
    repeated=run_strategy(repository,'controlled-r20',analysis_id,prices_id)
    require(repeated['status']=='existing' and repeated['valuation']==result['valuation'],
        'strategy_same_run_no_double_fills')
    require(rejects(lambda:run_strategy(repository,'controlled-r20',analysis_id,prices_id,initial_cash='9999')),
        'strategy_changed_inputs_same_run_rejected')
    report=strategy_report(repository,'controlled-r20')
    require(report['status']=='verified' and report['events_sha256']==result['events_sha256'],
        'strategy_sealed_report_recomputed')
    require(rejects(lambda:ledger.append_event(repository,result['account_id'],ledger.fill_request('manual','MSFT','buy','1','100',END))),
        'strategy_account_manual_mutation_rejected')
    records,bars,source=load_inputs(repository,analysis_id,prices_id)
    config=ledger.account_config('10000',source['start'],scope='recorded_receipt_strategy_simulation',max_mark_age_seconds=120)
    policy=strategy_policy()
    def evaluate(r=records,b=bars,p=policy):return simulate(r,b,source,config,p)[0]
    expired=evaluate(p=strategy_policy(max_signal_age_seconds=1))
    require(expired['valuation']['fills']==0 and expired['reason_counts'].get('signal_expired',0)>0,
        'strategy_expired_signal_no_fill')
    stale=deepcopy(bars)
    for bar in stale:bar['receipt']['received_at']='2026-01-05T10:06:00.000000000Z'
    require(evaluate(b=stale)['price_counts'].get('stale_price_receipt',0)>0,
        'strategy_stale_price_no_execution')
    old=deepcopy(records)
    for item in old:item['news']['data']['created_at']='2026-01-04T10:00:00.000000000Z'
    require(evaluate(r=old)['valuation']['fills']==0,'strategy_old_news_no_buy')
    updates=deepcopy(bars)
    for bar in updates:bar['receipt']['event_type']='u'
    require(evaluate(b=updates)['valuation']['fills']==0,'strategy_updated_bars_not_fill_sources')
    duplicate=evaluate(b=bars+[deepcopy(bars[-1])])
    require(duplicate['valuation']==result['valuation'] and duplicate['price_counts']['duplicate_minute']==1,
        'strategy_duplicate_receipt_no_double_mark')
    no_prices=evaluate(b=[])
    require(no_prices['valuation']['fills']==0,'strategy_no_price_no_trade')
    # Insert failure occurs after a ledger row was written: whole owned account/audit must roll back.
    class FailAfterInsert:
        def __init__(self,base):self.base=base
        def __getattr__(self,name):return getattr(self.base,name)
        def execute(self,query,params=()):
            result=self.base.execute(query,params)
            if query.startswith('INSERT INTO paper_ledger'):raise StorageError('Controlled strategy insert failure')
            return result
    original=repository.connection;repository.connection=FailAfterInsert(original)
    try:failed=rejects(lambda:run_strategy(repository,'rollback-r20',analysis_id,prices_id))
    finally:repository.connection=original
    remaining=repository.connection.execute(f'SELECT COUNT(*) FROM paper_accounts WHERE id={repository.marker}',('r20-rollback-r20',)).fetchone()[0]
    repository.connection.commit()
    require(failed and remaining==0,'strategy_account_fills_audit_atomic_rollback')
    m=repository.marker
    saved=repository.connection.execute(f'SELECT result_json FROM strategy_simulations WHERE id={m}',('controlled-r20',)).fetchone()[0]
    repository.connection.commit()
    repository.connection.execute(f'UPDATE strategy_simulations SET result_json={m} WHERE id={m}',('{}','controlled-r20'));repository.connection.commit()
    try:require(rejects(lambda:strategy_report(repository,'controlled-r20')),'strategy_audit_tamper_rejected')
    finally:
        repository.connection.execute(f'UPDATE strategy_simulations SET result_json={m} WHERE id={m}',(saved,'controlled-r20'));repository.connection.commit()
    require(result['broker_connected'] is False and result['orders_sent']==0 and not result['profitability_validated'],
        'strategy_simulation_not_broker_profitability')
    same_time=deepcopy(records[:2])
    same_time[1]['analysis']['timestamp']=same_time[0]['analysis']['timestamp']
    same_time[1]['news']['timestamp']=same_time[0]['news']['timestamp']
    same_time[1]['news']['data']['created_at']=same_time[0]['news']['data']['created_at']
    simultaneous=evaluate(r=same_time)
    require(simultaneous['valuation']['fills']==0 and simultaneous['reason_counts']=={'conflicting_simultaneous_signals':2},
        'strategy_simultaneous_opposite_signals_withheld')
    additional=deepcopy(records[0]);additional['news']['article_id']=999210
    additional['analysis']['timestamp']='2026-01-05T10:02:05.000000000Z'
    require(evaluate(r=records+[additional])['reason_counts'].get('position_already_open')==1,
        'strategy_positive_does_not_pyramid_position')
    correction=deepcopy(records[0]);correction['news']['revision_hash']='a'*64
    correction['news']['data']['updated_at']='2026-01-05T10:04:00.000000000Z'
    correction['news']['timestamp']='2026-01-05T10:04:01.000000000Z';correction['analysis']=None
    require(evaluate(r=[records[0],correction])['valuation']['fills']==1,
        'strategy_future_revision_does_not_erase_past_fill')
    narrow=run_strategy(repository,'cutoff-r20',analysis_id,prices_id,end='2026-01-05T10:01:59Z')
    require(narrow['valuation']['fills']==0 and narrow['reason_counts']['no_eligible_following_bar']==1,
        'strategy_cutoff_never_uses_future_price')
    return checks

def verify_strategy():
    repository=Repository('sqlite:///:memory:')
    try:
        return {'status':'passed','checks':strategy_checks(repository),
            'fixture_source':'original_synthetic_guarded_analyses_and_recorded_receipts',
            'network_required':False,'model_required':False,'user_database_modified':False,
            'point_in_time_validated':False,'profitability_validated':False,'orders_sent':0}
    finally:repository.close()

def demo_strategy(output):
    folder=Path(output)
    if folder.exists():raise DataValidationError('Demo output already exists; choose a new folder')
    folder.mkdir(parents=True)
    repository=Repository('sqlite:///'+str((folder/'scenario.sqlite3').resolve()))
    try:
        analysis,prices=strategy_fixture(repository)
        result=run_strategy(repository,'demo',analysis,prices)
        report=strategy_report(repository,'demo')
        if report['events_sha256']!=result['events_sha256']:raise StorageError('Demo repeat verification failed')
        result.update(fixture_source='original_synthetic_prices_and_model_outputs_not_live_data',
                      user_database_modified=False,output=str(folder))
        (folder/'summary.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
        return result
    finally:repository.close()
