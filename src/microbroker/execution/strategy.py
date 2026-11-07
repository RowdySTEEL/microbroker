"""Bounded, sealed, receipt-time strategy simulation. Never connects to a broker."""
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal, localcontext, ROUND_DOWN
from itertools import groupby
import json
from . import paper_ledger as ledger
from .strategy_provenance import attach
from .risk_controls import RiskControls, validate_policy as validate_risk_policy, STRATEGY_METHOD
from ..data.analysis_replay import replay_analysis
from ..data.alignment import read_bars_snapshot, nanos
from ..data.research import begin_read, encode
from ..data.storage.repository import StorageError
from ..data.stream_models import canonical_time
from ..data.models import DataValidationError
from ..intelligence.policy import digest
from ..intelligence.research_decision import validate_research_decisions, company_research_label

METHOD='r20-long-only-next-complete-minute-v1'
MAX_SIGNALS=2000
SEMANTICS='recorded_receipt_strategy_simulation_not_exact_live_backtest'

def strategy_policy(entry_fraction='0.10',max_signal_age_seconds=300,
                    max_price_age_seconds=120,max_news_age_seconds=900,risk=None):
    fraction=ledger.decimal_value(entry_fraction,'entry fraction',maximum='1')
    for name,value,minimum,maximum in [('signal age',max_signal_age_seconds,1,3600),
        ('price age',max_price_age_seconds,0,600),('news age',max_news_age_seconds,1,86400)]:
        if type(value) is not int or not minimum<=value<=maximum:
            raise DataValidationError('Invalid strategy '+name)
    policy = {'method':METHOD,'entry_fraction':ledger.number_text(fraction),
        'max_signal_age_seconds':max_signal_age_seconds,'max_price_age_seconds':max_price_age_seconds,
        'max_news_age_seconds':max_news_age_seconds,'position_additions':False,'shorts':False,
        'one_article_symbol_decision_per_run':True,'thresholds_calibrated':False}
    if risk is not None:
        policy.update(method=STRATEGY_METHOD, risk=validate_risk_policy(risk))
    return policy

def load_inputs(repository,analysis_replay_id,bars_replay_id,start=None,end=None):
    records,meta,analysis_sha=replay_analysis(repository,analysis_replay_id,_include_history=True)
    bars,price_meta,bars_sha=read_bars_snapshot(repository,bars_replay_id)
    if meta['provider']!='alpaca' or price_meta.get('scope')!='same_stream_run_receipts' or price_meta.get('format')!='R14':
        raise DataValidationError('Strategy requires Alpaca analysis and same-stream receipt price snapshots')
    start=canonical_time(start) if start else price_meta['receipt_start']
    end=canonical_time(end) if end else min(meta['as_of'],price_meta['receipt_end'])
    if start>end or start<price_meta['receipt_start'] or end>min(meta['as_of'],price_meta['receipt_end']):
        raise DataValidationError('Strategy window must fit both frozen cutoffs and the price receipt window')
    if any(a['receipt']['received_at']>b['receipt']['received_at'] for a,b in zip(bars,bars[1:])):
        raise StorageError('Strategy requires nondecreasing recorded stream receipt times')
    symbols=sorted(set(price_meta['symbols']).intersection(meta['symbols'] or price_meta['symbols']))
    if not symbols:raise DataValidationError('Strategy snapshot symbols do not overlap')
    source={'analysis_replay_id':analysis_replay_id,'bars_replay_id':bars_replay_id,
        'analysis_dataset_sha256':analysis_sha,'bars_dataset_sha256':bars_sha,
        'price_manifest_sha256':price_meta['manifest_sha256'],'stream_run_id':price_meta['stream_run_id'],
        'analyzer_key':meta['analyzer_key'],'engine':meta['engine'],'feed':price_meta['feed'],
        'symbols':symbols,'start':start,'end':end}
    return records,bars,source

def simulate(records,bars,source,config,policy):
    """Walk version/feature/receipt times, preserving all earlier revision states."""
    state=ledger.initial_state(config);decisions=[];requests=[];pending={};current={};seen_articles=set()
    used_minutes=set();last_minute={};price_counts=Counter();record_counts=Counter()
    start,end=source['start'],source['end'];symbols=set(source['symbols']);timeline=[]
    risk = RiskControls(policy['risk'], config, source) if 'risk' in policy else None
    for record in records:
        news=record['news'];analysis=record['analysis']
        if news['timestamp']<=end:
            timeline.append((news['timestamp'],0,(news['article_id'],news['revision_hash']),record))
        if analysis and start<=analysis['timestamp']<=end:
            for symbol in sorted(symbols.intersection(news['mappings'])):
                row={'article_id':news['article_id'],'revision_hash':news['revision_hash'],'symbol':symbol,
                    'available_at':analysis['timestamp'],'company_label':None,'action':'wait','reason':'unprocessed',
                    'execution_at':None,'quantity':None,'reference_price':None}
                decisions.append(row)
                if len(decisions)>MAX_SIGNALS:raise StorageError('Strategy exceeds 2000 signal rows; narrow the window')
                timeline.append((analysis['timestamp'],1,(news['article_id'],news['revision_hash'],symbol),(record,row)))
        else:record_counts['pending_analysis' if analysis is None else 'analysis_outside_window']+=1
    for bar in bars:
        if start<=bar['receipt']['received_at']<=end and bar['symbol'] in symbols:
            timeline.append((bar['receipt']['received_at'],2,(bar['receipt']['ordinal'],),bar))
        else:price_counts['outside_window_or_symbols']+=1
    timeline.sort(key=lambda t:(t[0],t[1],t[2]))

    def hold(row,reason):
        row.update(action='wait',reason=reason)
        if risk and reason.startswith('risk_'):risk.blocked[reason]+=1
    def cancel_entries(symbol,reason):
        for key,(record,row) in list(pending.items()):
            if row['company_label']=='positive' and (symbol is None or key==symbol):
                hold(row,reason);pending.pop(key)
    def rank(news):return (news['data']['updated_at'],bool(news['data']['content_text']),news['first_seen_at'])
    def emit(request):
        nonlocal state
        ledger.validate_request(request);state=ledger.apply_request(config,state,request)
        requests.append(request)
        if risk:risk.emitted(request,state)
        if len(requests)>ledger.MAX_EVENTS:raise StorageError('Strategy paper ledger event bound exceeded')
    def queue(record,row):
        news,analysis=record['news'],record['analysis'];symbol=row['symbol'];identity=(news['article_id'],symbol)
        # A new feature cancels an older pending feature, including neutral/withheld arrivals.
        prior=pending.pop(symbol,None)
        if prior:hold(prior[1],'superseded_by_newer_feature')
        if identity in seen_articles:hold(row,'article_already_considered');return
        seen_articles.add(identity)
        visible=current.get(news['article_id'])
        if not visible or visible['revision_hash']!=news['revision_hash']:
            hold(row,'revision_not_current_at_feature');return
        result=analysis['result']
        if result.get('analysis_format') not in {'R17','R18'}:hold(row,'legacy_analysis_without_guarded_decision');return
        validate_research_decisions(result)
        company=result.get('entity_sentiments',{}).get(symbol)
        if not company or company.get('status')!='scored':hold(row,'company_context_unavailable');return
        label=company_research_label(result,company);row['company_label']=label
        if label not in {'positive','negative','neutral'}:hold(row,'company_decision_withheld');return
        age=nanos(row['available_at'])-nanos(news['data']['created_at'])
        if age<0 or age>policy['max_news_age_seconds']*1000000000:hold(row,'news_too_old');return
        if label=='neutral':hold(row,'neutral_company_decision');return
        if label=='positive' and symbol in state['positions']:hold(row,'position_already_open');return
        if label=='negative' and symbol not in state['positions']:hold(row,'no_position_to_sell');return
        if risk and label=='positive':
            blocked=risk.entry_interruption()
            if blocked:hold(row,blocked);return
        pending[symbol]=(record,row);row['reason']='awaiting_following_completed_bar'

    for at,group in groupby(timeline,key=lambda t:t[0]):
        if risk and at>=start:risk.tick(state,at,cancel_entries)
        events=list(group)
        for _,priority,_,record in events:
            if priority!=0:continue
            news=record['news'];prior=current.get(news['article_id'])
            if prior is None or rank(news)>rank(prior):
                current[news['article_id']]=news
                for symbol,(old,row) in list(pending.items()):
                    if old['news']['article_id']==news['article_id'] and row['revision_hash']!=news['revision_hash']:
                        hold(row,'revision_superseded_before_execution');pending.pop(symbol)
            elif rank(news)==rank(prior) and news['revision_hash']!=prior['revision_hash']:
                raise StorageError('Ambiguous equal-time strategy news revisions')
        simultaneous=defaultdict(list)
        for _,priority,_,value in events:
            if priority==1:simultaneous[value[1]['symbol']].append(value)
        for symbol,values in sorted(simultaneous.items()):
            for record,row in values:queue(record,row)
            # Inspect all same-time guarded directional candidates, even those canceled by queue order.
            labels={row['company_label'] for _,row in values if row['reason'] in
                {'awaiting_following_completed_bar','superseded_by_newer_feature','no_position_to_sell','position_already_open'}}
            if {'positive','negative'}.issubset(labels):
                pending.pop(symbol,None)
                for _,row in values:
                    if row['company_label'] in {'positive','negative'}:hold(row,'conflicting_simultaneous_signals')
        for _,priority,_,bar in events:
            if priority!=2:continue
            symbol=bar['symbol'];r=bar['receipt'];minute=bar['timestamp']
            if r['event_type']!='b':price_counts['updated_bar_not_execution_source']+=1;continue
            if (symbol,minute) in used_minutes:price_counts['duplicate_minute']+=1;continue
            if symbol in last_minute and minute<=last_minute[symbol]:price_counts['out_of_order_minute']+=1;continue
            age=nanos(at)-nanos(minute)-60000000000
            if age<0:
                price_counts['unfinished_bar']+=1
                if risk:risk.rejected_early(symbol,at,cancel_entries)
                continue
            if age>policy['max_price_age_seconds']*1000000000:price_counts['stale_price_receipt']+=1;continue
            price=ledger.number_text(ledger.decimal_value(bar['close'],'strategy reference price'))
            used_minutes.add((symbol,minute));last_minute[symbol]=minute;price_counts['eligible_base_receipts']+=1
            if risk:risk.price(symbol,at)
            if symbol in state['positions']:
                emit(attach(ledger.mark_request(digest({'mark':r['ordinal'],'run':r['run_id']}),symbol,price,at),source,bar))
            queued=pending.get(symbol)
            if not queued:continue
            record,row=queued
            lag=nanos(at)-nanos(row['available_at'])
            if lag>policy['max_signal_age_seconds']*1000000000:
                hold(row,'signal_expired');pending.pop(symbol);continue
            if nanos(at)-nanos(record['news']['data']['created_at'])>policy['max_news_age_seconds']*1000000000:
                hold(row,'news_expired_before_execution');pending.pop(symbol);continue
            if minute<row['available_at'] or at<=row['available_at']:continue
            side='buy' if row['company_label']=='positive' else 'sell'
            if risk and side=='buy':
                blocked=risk.before_buy(state,at)
                if blocked:hold(row,blocked);pending.pop(symbol);continue
            with localcontext() as ctx:
                ctx.prec=50
                if side=='buy':
                    if symbol in state['positions']:hold(row,'position_already_open');pending.pop(symbol);continue
                    budget=min(Decimal(config['initial_cash'])*Decimal(policy['entry_fraction']),Decimal(state['cash']),
                               Decimal(config['initial_cash'])*Decimal(config['max_position_cost_fraction']))
                    execution_price=ledger.money(Decimal(price)*(1+Decimal(config['slippage_bps'])/10000))
                    available=budget-Decimal(config['fee_per_fill'])
                    quantity=(min(available/execution_price,Decimal('1000000')).quantize(Decimal('0.000001'),rounding=ROUND_DOWN)
                              if available>0 and execution_price>0 else Decimal(0))
                    if quantity<=0:hold(row,'insufficient_cash_or_quantity');pending.pop(symbol);continue
                else:
                    if symbol not in state['positions']:hold(row,'no_position_to_sell');pending.pop(symbol);continue
                    quantity=Decimal(state['positions'][symbol]['quantity'])
            signal={'article_id':row['article_id'],'revision_hash':row['revision_hash'],'available_at':row['available_at'],
                    'company_label':row['company_label'],'analyzer_key':source['analyzer_key']}
            request=attach(ledger.fill_request(digest({'article':row['article_id'],'symbol':symbol,'method':policy['method']}),
                           symbol,side,ledger.number_text(quantity),price,at),source,bar,signal)
            if risk and side=='buy':
                try:projected=ledger.apply_request(config,state,request)
                except DataValidationError:hold(row,'portfolio_constraint_rejected');pending.pop(symbol);continue
                blocked=risk.before_buy(state,at,projected)
                if blocked:hold(row,blocked);pending.pop(symbol);continue
            try:emit(request)
            except DataValidationError:hold(row,'portfolio_constraint_rejected');pending.pop(symbol);continue
            row.update(action=side,reason='executed_scenario_fill',execution_at=at,quantity=request['quantity'],
                       reference_price=price,receipt_ordinal=r['ordinal'],bar_start=minute)
            pending.pop(symbol)
    if risk:risk.tick(state,end,cancel_entries)
    for _,row in pending.values():
        hold(row,'signal_expired' if nanos(end)-nanos(row['available_at'])>policy['max_signal_age_seconds']*1000000000
             else 'no_eligible_following_bar')
    final=ledger.valuation(config,state,end)
    result={'format':'R20','method':METHOD,'decisions':decisions,'decision_counts':dict(Counter(r['action'] for r in decisions)),
        'reason_counts':dict(Counter(r['reason'] for r in decisions)), 'price_counts':dict(price_counts),
        'record_counts':dict(record_counts),'ledger_event_count':len(requests),'events_sha256':digest(requests),
        'final_state':state,'valuation':final,'return_percent':ledger.money_text(Decimal(final['total_pnl'])/Decimal(config['initial_cash'])*100),
        'semantics':SEMANTICS,'warning_codes':['heuristic_strategy_not_calibrated','bar_close_not_executable_quote',
            'receipt_and_analysis_times_not_exact_commit_visibility','cross_stage_equal_time_order_assumed',
            'fees_and_slippage_are_scenario_assumptions','no_exchange_calendar_filter'],
        'point_in_time_validated':False,'model_accuracy_validated':False,'profitability_validated':False,
        'broker_connected':False,'orders_sent':0}
    if risk:
        result.update(format='R21',method=policy['method'],risk=risk.summary())
        result['warning_codes']+=['daily_risk_scope_is_independent_run_utc_day',
            'price_gaps_observed_only_at_recorded_events', 'news_poll_liveness_not_in_snapshots']
    if len(encode(result).encode())>ledger.MAX_BYTES:raise StorageError('Strategy result exceeds 64 MiB')
    return result,requests

def result_summary(run_id,account_id,result,request,status='completed'):
    return {'status':status,'run_id':run_id,'account_id':account_id,'source':request['source'],
        'policy':request['policy'],**{k:v for k,v in result.items() if k not in {'decisions','final_state'}},
        'decisions_total':len(result['decisions']),'samples':result['decisions'][:20],'sample_limit':20,
        'strategy_implemented':True,'source_tables_modified':False,'simulation_tables_modified':status=='completed',
        'inference_performed':False,'network_required':False,'ledger_integrity_verified':True}

def run_strategy(repository,run_id,analysis_replay_id,bars_replay_id,*,start=None,end=None,
                 initial_cash='10000',fee_per_fill='1',slippage_bps='5',max_position_fraction='0.25',
                 entry_fraction='0.10',max_signal_age_seconds=300,max_price_age_seconds=120,max_news_age_seconds=900,risk=None):
    ledger.identifier(run_id)
    if len(run_id)>60:raise DataValidationError('Strategy run-id must be at most 60 characters')
    account_id=('r21-' if risk is not None else 'r20-')+run_id
    records,bars,source=load_inputs(repository,analysis_replay_id,bars_replay_id,start,end)
    config=ledger.account_config(initial_cash,source['start'],fee_per_fill,slippage_bps,max_position_fraction,
        max_price_age_seconds,scope='recorded_receipt_strategy_simulation')
    policy=strategy_policy(entry_fraction,max_signal_age_seconds,max_price_age_seconds,max_news_age_seconds,risk)
    request={'run_id':run_id,'account_id':account_id,'source':source,'account_config':config,'policy':policy}
    result,events=simulate(records,bars,source,config,policy)
    created=canonical_time(datetime.now(timezone.utc).isoformat())
    root=digest({'id':account_id,'created_at':created,'config':config});m=repository.marker
    ledger.begin_write(repository)
    try:
        inserted=repository.connection.execute(f'INSERT INTO paper_accounts VALUES ({",".join([m]*4)}) ON CONFLICT (id) DO NOTHING',
            (account_id,created,encode(config),root)).rowcount
        actual_config,root=ledger.header(repository,account_id,True)
        existing=repository.connection.execute(f'SELECT request_json,request_sha256,result_json,result_sha256,ledger_sha256 FROM strategy_simulations WHERE id={m}',(run_id,)).fetchone()
        if existing:
            verify_saved(repository,account_id,request,result,events,existing,actual_config,root)
            repository.connection.commit();return result_summary(run_id,account_id,result,request,'existing')
        if not inserted:raise DataValidationError('Strategy account name already exists without this run audit')
        previous=root;state=ledger.initial_state(config);size=0
        for ordinal,event in enumerate(events):
            state=ledger.apply_request(config,state,event);raw=encode(event);state_raw=encode(state)
            size+=len(raw.encode())+len(state_raw.encode())
            if size>ledger.MAX_BYTES:raise StorageError('Strategy paper ledger exceeds 64 MiB')
            sha=ledger.event_digest(account_id,ordinal,previous,event,state)
            repository.connection.execute(f'INSERT INTO paper_ledger VALUES ({",".join([m]*9)})',
                (account_id,ordinal,event['event_key'],event['at'],raw,digest(event),previous,state_raw,sha))
            previous=sha
        ledger.valuation(config,state,source['end'])
        repository.connection.execute(f'INSERT INTO strategy_simulations VALUES ({",".join([m]*8)})',
            (run_id,account_id,created,encode(request),digest(request),encode(result),digest(result),previous))
        repository.connection.commit();return result_summary(run_id,account_id,result,request)
    except BaseException:repository.connection.rollback();raise

def verify_saved(repository,account_id,request,result,events,saved,config,root):
    try:
        if any(not isinstance(saved[i],str) or len(saved[i].encode())>ledger.MAX_BYTES for i in (0,2)):
            raise StorageError('Strategy audit text bound exceeded')
        saved_request,saved_result=json.loads(saved[0]),json.loads(saved[2])
    except (TypeError,KeyError,ValueError):raise StorageError('Invalid strategy audit') from None
    if digest(saved_request)!=saved[1] or digest(saved_result)!=saved[3]:
        raise StorageError('Strategy audit digest mismatch')
    if saved_request!=request:raise DataValidationError('Strategy run-id already has different inputs/settings')
    if saved_result!=result or config!=request['account_config']:
        raise StorageError('Strategy recomputed result/config mismatch')
    state,rows,sha,_=ledger.read_ledger(repository,account_id,config,root)
    if state!=result['final_state'] or [r['request'] for r in rows]!=events or sha!=saved[4]:
        raise StorageError('Strategy ledger/audit mismatch')

def strategy_report(repository,run_id):
    ledger.identifier(run_id);m=repository.marker;begin_read(repository)
    try:
        row=repository.connection.execute(f'SELECT account_id,request_json,request_sha256,result_json,result_sha256,ledger_sha256 FROM strategy_simulations WHERE id={m}',(run_id,)).fetchone()
        if not row:raise DataValidationError('Unknown strategy run-id')
        if not isinstance(row[1],str) or len(row[1].encode())>ledger.MAX_BYTES:
            raise StorageError('Strategy request text bound exceeded')
        try:
            request=json.loads(row[1])
            if not isinstance(request,dict) or digest(request)!=row[2]:raise StorageError('Strategy request digest mismatch')
            if (set(request)!={'run_id','account_id','source','account_config','policy'} or
                request.get('account_id')!=('r21-' if 'risk' in request.get('policy',{}) else 'r20-')+run_id or row[0]!=request.get('account_id')):
                raise StorageError('Strategy request identity/structure mismatch')
        except (ValueError,TypeError):raise StorageError('Invalid strategy request') from None
        repository.connection.commit()
    except BaseException:repository.connection.rollback();raise
    source=request['source'];records,bars,actual_source=load_inputs(repository,source['analysis_replay_id'],source['bars_replay_id'],source['start'],source['end'])
    if actual_source!=source:raise StorageError('Strategy frozen source mismatch')
    expected_policy=strategy_policy(request['policy']['entry_fraction'],request['policy']['max_signal_age_seconds'],
        request['policy']['max_price_age_seconds'],request['policy']['max_news_age_seconds'],request['policy'].get('risk'))
    if request['policy']!=expected_policy or request['run_id']!=run_id or request['account_id']!=row[0]:
        raise StorageError('Strategy policy/identity mismatch')
    c=request['account_config']
    expected_config=ledger.account_config(c['initial_cash'],source['start'],c['fee_per_fill'],c['slippage_bps'],
        c['max_position_cost_fraction'],expected_policy['max_price_age_seconds'],scope='recorded_receipt_strategy_simulation')
    if c!=expected_config:raise StorageError('Strategy account configuration mismatch')
    result,events=simulate(records,bars,source,request['account_config'],expected_policy)
    begin_read(repository)
    try:
        config,root=ledger.header(repository,row[0]);verify_saved(repository,row[0],request,result,events,row[1:],config,root)
        repository.connection.commit();return result_summary(run_id,row[0],result,request,'verified')
    except BaseException:repository.connection.rollback();raise
