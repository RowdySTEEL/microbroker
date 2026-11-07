"""Durable Paper intents and cumulative broker observations; not a simulated ledger."""
from copy import deepcopy
from decimal import Decimal, localcontext
import json
import re
from . import paper_ledger as ledger
from .paper_api import PaperBrokerError, BASE, TERMINAL, order, decimal_text
from ..config import UNIVERSE
from ..data.alignment import nanos
from ..data.models import DataValidationError
from ..data.stream_models import canonical_time
from ..data.research import begin_read, encode
from ..data.storage.repository import StorageError
from ..intelligence.policy import digest

MAX_EVENTS = 1000
MAX_INTENTS = 1000
MAX_NOTIONAL = Decimal('100')
MAX_DAILY_BUY_ATTEMPTS = 10


def intent_payload(intent_id, account_ref, symbol, side, amount, created_at, parent=None):
    ledger.identifier(intent_id)
    if canonical_time(created_at)!=created_at:raise DataValidationError('Paper intent time must be canonical UTC')
    if len(intent_id)>60 or not re.fullmatch('[0-9a-f]{64}',account_ref):raise DataValidationError('Invalid paper intent identity')
    if symbol not in UNIVERSE or side not in {'buy','sell'}:raise DataValidationError('Invalid paper order symbol/side')
    amount=ledger.decimal_value(amount,'paper order amount',maximum=str(MAX_NOTIONAL) if side=='buy' else '1000000',places=2 if side=='buy' else 9)
    if side=='buy' and (amount<1 or parent is not None):raise DataValidationError('Paper buy notional must be 1–100 USD')
    if side=='sell':ledger.identifier(parent)
    payload={'symbol':symbol,'side':side,'type':'market','time_in_force':'day','extended_hours':False,
             'client_order_id':'mb22-'+digest({'account':account_ref,'intent_id':intent_id})[:40],
             'notional' if side=='buy' else 'qty':ledger.number_text(amount)}
    return {'format':'R22','id':intent_id,'account_ref':account_ref,'created_at':created_at,
            'paper_origin':BASE,'parent_intent_id':parent,'order':payload}


def reduce_event(state,event,intent):
    state=deepcopy(state);kind,p=event['kind'],event['data']
    if kind=='post_claimed':
        if state['phase']!='prepared' or state['post_attempts']!=0 or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',p['day']):
            raise StorageError('Invalid paper send claim')
        state.update(phase='submitting',post_attempts=1,submission_day=p['day'])
    elif kind=='broker_observed':
        observed=order(p,intent)
        previous=state['order']
        if previous:
            if observed['id']!=previous['id']:raise PaperBrokerError('broker_order_id_changed')
            if observed['updated_at']<previous['updated_at']:return state
            if Decimal(observed['filled_qty'])<Decimal(previous['filled_qty']):raise PaperBrokerError('cumulative_fill_regressed')
            if previous['status']=='filled' and observed['status']!='filled':raise PaperBrokerError('filled_status_regressed')
            if previous['status'] in TERMINAL and observed['status'] not in TERMINAL:
                raise PaperBrokerError('terminal_status_regressed')
        state.update(phase='observed',order=observed,last_error=None)
    elif kind=='retired_unsent':
        if state['post_attempts'] or state['order'] is not None or p!={'reason':'worker_signal_retired'}:
            raise StorageError('Only an unsent worker intent may be retired')
        state['phase']='retired'
    elif kind=='uncertain':
        if set(p)!={'code','http_status'} or not re.fullmatch('[a-z_]{1,80}',p['code']):raise StorageError('Invalid paper diagnostic')
        if p['http_status'] is not None and (type(p['http_status']) is not int or not 100<=p['http_status']<=599):
            raise StorageError('Invalid paper diagnostic status')
        state.update(phase='uncertain',last_error=p)
    elif kind=='cancel_attempted':
        if p!={}:raise StorageError('Invalid cancel attempt')
        state['cancel_requested']=True
    elif kind=='cancel_requested':
        if p!={'acknowledged':True}:raise StorageError('Invalid paper cancellation acknowledgement')
        state['cancel_acknowledged']=True
    elif kind=='daily_loss_halt':
        if set(p)!={'day'} or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',p['day']):raise StorageError('Invalid paper loss halt')
        state['loss_halt_days']=sorted(set(state['loss_halt_days'])|{p['day']})
    else:raise StorageError('Invalid paper broker event kind')
    return state


def locked(repository,account_ref):
    ledger.begin_write(repository);m=repository.marker
    repository.connection.execute(f'INSERT INTO broker_paper_locks VALUES ({m}) ON CONFLICT (account_ref) DO NOTHING',(account_ref,))
    repository.connection.execute(f'SELECT account_ref FROM broker_paper_locks WHERE account_ref={m}'+(' FOR UPDATE' if repository.postgres else ''),(account_ref,)).fetchone()


def read_intent(repository,intent_id):
    ledger.identifier(intent_id);m=repository.marker
    header=repository.connection.execute(f'SELECT account_ref,created_at,intent_json,intent_sha256 FROM broker_paper_intents WHERE id={m}',(intent_id,)).fetchone()
    if not header:raise DataValidationError('Unknown paper intent-id')
    try:
        if len(header[2].encode())>8192:raise StorageError('Paper intent text bound exceeded')
        intent=json.loads(header[2]);payload=intent['order']
        expected=intent_payload(intent_id,header[0],payload['symbol'],payload['side'],
            payload['notional'] if payload['side']=='buy' else payload['qty'],header[1],intent['parent_intent_id'])
        if expected!=intent or digest(intent)!=header[3]:raise StorageError('Paper intent integrity mismatch')
        state={'phase':'prepared','post_attempts':0,'submission_day':None,'order':None,
               'cancel_requested':False,'cancel_acknowledged':False,'last_error':None,'loss_halt_days':[]}
        previous=header[3];last_at=header[1]
        rows=repository.connection.execute(f'SELECT ordinal,observed_at,event_json,previous_sha256,event_sha256 FROM broker_paper_events WHERE intent_id={m} ORDER BY ordinal LIMIT {MAX_EVENTS+1}',(intent_id,)).fetchall()
        if len(rows)>MAX_EVENTS:raise StorageError('Paper event count bound exceeded')
        for index,(ordinal,at,raw,prev,sha) in enumerate(rows):
            if len(raw.encode())>8192:raise StorageError('Paper event text bound exceeded')
            event=json.loads(raw)
            expected_sha=digest({'intent_id':intent_id,'ordinal':ordinal,'at':at,'event':event,'previous':previous})
            if ordinal!=index or prev!=previous or sha!=expected_sha or at<last_at:raise StorageError('Paper event chain mismatch')
            state=reduce_event(state,event,intent);previous=sha;last_at=at
        return intent,state,previous,len(rows),last_at
    except (KeyError,TypeError,ValueError,PaperBrokerError,DataValidationError):
        raise StorageError('Invalid stored paper intent or event') from None


def append(repository,intent_id,kind,data,at):
    intent,state,previous,count,last_at=read_intent(repository,intent_id)
    event={'kind':kind,'data':data};new=reduce_event(state,event,intent)
    if new==state:return state
    if count>=MAX_EVENTS or at<last_at:raise StorageError('Paper audit bound or local clock order failed')
    raw=encode(event)
    if len(raw.encode())>8192:raise StorageError('Paper event too large')
    sha=digest({'intent_id':intent_id,'ordinal':count,'at':at,'event':event,'previous':previous});m=repository.marker
    repository.connection.execute(f'INSERT INTO broker_paper_events VALUES ({",".join([m]*6)})',
        (intent_id,count,at,raw,previous,sha))
    return new


def report(repository,intent_id):
    begin_read(repository)
    try:
        intent,state,sha,count,last=read_intent(repository,intent_id)
        managed=repository.connection.execute(f'SELECT id FROM paper_news_decisions WHERE intent_id={repository.marker}',(intent_id,)).fetchone() is not None
        repository.connection.commit()
    except BaseException:repository.connection.rollback();raise
    return {'status':state['phase'],'intent_id':intent_id,'environment':'alpaca_paper','account_ref':intent['account_ref'],
            'request':intent['order'],'parent_intent_id':intent['parent_intent_id'],**state,
            'audit_events':count,'audit_sha256':sha,'last_meaningful_observation_at':last,
            'audit_integrity_verified':True,'broker_queried':False,'network_required':False,
            'post_requests_attempted_by_this_call':0,'live_trading_supported':False,
            'simulation_ledger_modified':False,'broker_freshness_validated':False,
            'pnl_calculated':False,'automatic_strategy_execution':managed}


def all_intents(repository,account_ref):
    rows=repository.connection.execute(f'SELECT id FROM broker_paper_intents WHERE account_ref={repository.marker} ORDER BY id LIMIT {MAX_INTENTS+1}',(account_ref,)).fetchall()
    if len(rows)>MAX_INTENTS:raise StorageError('Paper account exceeds 1000 intents')
    return [read_intent(repository,row[0]) for row in rows]


def prepare_buy(repository,api,intent_id,symbol,notional='10'):
    a=api.account()
    return prepare(repository,api,intent_payload(intent_id,a['account_ref'],symbol,'buy',notional,api.now()))


def prepare_exit(repository,api,intent_id,parent_id):
    a=api.account();parent=report(repository,parent_id)
    if parent['account_ref']!=a['account_ref']:raise DataValidationError('Paper account differs from entry account')
    p=parent['order']
    if parent['phase']!='observed' or not p or p['side']!='buy' or p['status'] not in TERMINAL or Decimal(p['filled_qty'])<=0:
        raise DataValidationError('Reconcile a terminal, filled entry before preparing an exit')
    # Prepare under the account lock so two exits cannot reserve the same entry shares.
    locked(repository,a['account_ref'])
    try:
        rows=all_intents(repository,a['account_ref'])
        existing=next((i for i,s,*_ in rows if i['id']==intent_id),None)
        if existing:
            if existing['parent_intent_id']!=parent_id:raise DataValidationError('Paper exit-id already has a different parent')
            repository.connection.commit()
            return {'status':'existing','intent_id':intent_id,'request':existing['order'],'account_ref':a['account_ref'],'orders_sent':0,'environment':'alpaca_paper'}
        parent_now=next(s for i,s,*_ in rows if i['id']==parent_id)
        if parent_now['phase']!='observed' or not parent_now['order'] or parent_now['order']['status'] not in TERMINAL:
            raise DataValidationError('Entry state changed; reconcile before exit')
        p=parent_now['order'];remaining=Decimal(p['filled_qty'])
        for i,s,*_ in rows:
            if i['id']==intent_id:continue
            if i['parent_intent_id']==parent_id and s['phase']!='retired':
                observed=s['order']
                remaining-=Decimal(observed['filled_qty']) if observed and observed['status'] in TERMINAL else Decimal(i['order']['qty'])
        if remaining<=0:raise DataValidationError('No unreserved filled shares remain for this entry')
        intent=intent_payload(intent_id,a['account_ref'],p['symbol'],'sell',ledger.number_text(remaining),api.now(),parent_id)
        result=_prepare_locked(repository,intent)
        repository.connection.commit()
    except BaseException:repository.connection.rollback();raise
    return result


def _prepare_locked(repository,intent):
    m=repository.marker
    found=repository.connection.execute(f'SELECT id FROM broker_paper_intents WHERE id={m}',(intent['id'],)).fetchone()
    if found:
        actual,*_=read_intent(repository,intent['id'])
        if {k:v for k,v in actual.items() if k!='created_at'}!={k:v for k,v in intent.items() if k!='created_at'}:
            raise DataValidationError('Paper intent-id already has different account/order parameters')
    else:
        if len(all_intents(repository,intent['account_ref']))>=MAX_INTENTS:raise StorageError('Paper account intent limit')
        repository.connection.execute(f'INSERT INTO broker_paper_intents VALUES ({",".join([m]*5)})',
            (intent['id'],intent['account_ref'],intent['created_at'],encode(intent),digest(intent)))
    return {'status':'existing' if found else 'prepared','intent_id':intent['id'],'request':intent['order'],
            'account_ref':intent['account_ref'],'orders_sent':0,'environment':'alpaca_paper'}


def prepare(repository,api,intent):
    locked(repository,intent['account_ref'])
    try:
        result=_prepare_locked(repository,intent);repository.connection.commit();return result
    except BaseException:repository.connection.rollback();raise


def _record(repository,api,intent,kind,data):
    locked(repository,intent['account_ref'])
    try:
        state=append(repository,intent['id'],kind,data,api.now());repository.connection.commit();return state
    except BaseException:repository.connection.rollback();raise


def _same_account(api,intent):
    a=api.account()
    if a['account_ref']!=intent['account_ref']:raise PaperBrokerError('paper_account_changed')
    return a


def _observe(repository,api,intent,raw):
    try:
        normalized=order(raw,intent)
        _record(repository,api,intent,'broker_observed',normalized)
    except PaperBrokerError as exc:
        _record(repository,api,intent,'uncertain',{'code':exc.code,'http_status':exc.http_status})
    result=report(repository,intent['id']);result.update(broker_queried=True,network_required=True)
    return result


def reconcile(repository,api,intent_id):
    before=report(repository,intent_id)
    if before['phase']=='retired':return before
    intent={'id':intent_id,'account_ref':before['account_ref'],'order':before['request']}
    _same_account(api,intent)
    try:raw=api.lookup(intent['order']['client_order_id'])
    except PaperBrokerError as exc:
        _record(repository,api,intent,'uncertain',{'code':exc.code,'http_status':exc.http_status})
        result=report(repository,intent_id);result.update(broker_queried=True,network_required=True);return result
    if raw is not None:return _observe(repository,api,intent,raw)
    if before['post_attempts'] or before['phase']!='prepared':
        _record(repository,api,intent,'uncertain',{'code':'order_not_found_after_attempt','http_status':404})
    result=report(repository,intent_id);result.update(broker_queried=True,network_required=True)
    return result


def preflight(api,intent,a):
    if any(a[k] for k in ('trading_blocked','account_blocked','trade_suspended_by_user')):
        raise PaperBrokerError('account_trading_blocked')
    payload=intent['order'];asset=api.asset(payload['symbol'])
    if not isinstance(asset,dict) or asset.get('symbol')!=payload['symbol'] or asset.get('class')!='us_equity' or asset.get('status')!='active' or asset.get('tradable') is not True or asset.get('fractionable') is not True:
        raise PaperBrokerError('asset_not_fractional_tradable_equity')
    positions,orders=api.positions(),api.open_orders()
    if any(o['symbol']==payload['symbol'] for o in orders):raise PaperBrokerError('symbol_has_open_broker_order')
    if payload['side']=='buy':
        if payload['symbol'] in positions:raise PaperBrokerError('position_already_exists')
        if Decimal(a['cash'])<Decimal(payload['notional'])*Decimal('1.01'):
            raise PaperBrokerError('insufficient_cash_buffer')
    else:
        held=positions.get(payload['symbol'])
        if not held or held['side']!='long' or Decimal(held['qty'])<Decimal(payload['qty']):
            raise PaperBrokerError('insufficient_long_position')
    c=api.clock()
    if abs(nanos(api.now())-nanos(c['timestamp']))>120000000000:raise PaperBrokerError('market_clock_not_fresh')
    if not c['is_open'] or nanos(c['next_close'])-nanos(c['timestamp'])<=30000000000:
        raise PaperBrokerError('regular_market_closed_or_closing')
    return c


def submit(repository,api,intent_id,*,_guard=None):
    before=report(repository,intent_id)
    if before['phase']!='prepared' or before['post_attempts']:
        return reconcile(repository,api,intent_id)  # Lookup only; never blind POST retry.
    intent={'id':intent_id,'account_ref':before['account_ref'],'order':before['request']}
    a=_same_account(api,intent)
    raw=api.lookup(intent['order']['client_order_id'])
    if raw is not None:return _observe(repository,api,intent,raw)
    c=preflight(api,intent,a);day=c['timestamp'][:10]
    locked(repository,intent['account_ref'])
    blocked=None;claimed=False
    try:
        actual,state,*_=read_intent(repository,intent_id)
        if state['phase']=='prepared' and state['post_attempts']==0:
            if _guard is not None:_guard(repository,actual)
            else:
                protected=repository.connection.execute(f'SELECT id FROM paper_news_decisions WHERE intent_id={repository.marker}',(intent_id,)).fetchone()
                leased=repository.connection.execute(f'SELECT token FROM paper_worker_leases WHERE account_ref={repository.marker} AND expires_at>{repository.marker}',(intent['account_ref'],api.now())).fetchone()
                if protected or leased:raise PaperBrokerError('paper_worker_owns_submission')
            rows=all_intents(repository,intent['account_ref'])
            for other,s,*_ in rows:
                if other['id']==intent_id:continue
                if s['phase'] in {'submitting','uncertain'}:blocked='unresolved_local_paper_order';break
                if other['order']['symbol']==intent['order']['symbol'] and s['order'] and s['order']['status'] not in TERMINAL:
                    blocked='symbol_has_open_local_order';break
            if intent['order']['side']=='buy':
                if any(day in s['loss_halt_days'] for _,s,*_ in rows):blocked='paper_daily_loss_latched'
                if Decimal(a['last_equity'])<=0 or Decimal(a['equity'])<=Decimal(a['last_equity'])*Decimal('0.98'):
                    append(repository,intent_id,'daily_loss_halt',{'day':day},api.now());blocked='paper_daily_loss_latched'
                attempts=sum(i['order']['side']=='buy' and s['submission_day']==day for i,s,*_ in rows)
                if attempts>=MAX_DAILY_BUY_ATTEMPTS:blocked='paper_daily_buy_attempt_limit'
                local_exposure=sum((Decimal(s['order']['filled_qty']) if i['order']['side']=='buy' else -Decimal(s['order']['filled_qty']))
                    for i,s,*_ in rows if i['order']['symbol']==intent['order']['symbol'] and s['order'])
                if local_exposure>0:blocked='managed_position_already_exists'
                # Reserve the full day's submitted notionals, even if broker cash already reflects them.
                # Conservative double reservation avoids spending against a preflight cash snapshot from before another local send.
                reserved=sum(Decimal(i['order']['notional']) for i,s,*_ in rows if i['order']['side']=='buy' and
                    (s['phase'] in {'submitting','uncertain'} or s['submission_day']==day))
                if Decimal(a['cash'])-reserved<Decimal(intent['order']['notional'])*Decimal('1.01'):blocked='insufficient_unreserved_cash'
            else:
                parent=next(((i,s) for i,s,*_ in rows if i['id']==actual['parent_intent_id']),None)
                if not parent or parent[1]['phase']!='observed' or not parent[1]['order'] or parent[1]['order']['status'] not in TERMINAL:
                    blocked='entry_not_reconciled_terminal'
                else:
                    sold=sum(Decimal(s['order']['filled_qty']) if s['order'] and s['order']['status'] in TERMINAL else Decimal(i['order']['qty'])
                             for i,s,*_ in rows if i['id']!=intent_id and i['parent_intent_id']==actual['parent_intent_id'] and s['phase']!='retired')
                    if Decimal(parent[1]['order']['filled_qty'])-sold<Decimal(intent['order']['qty']):blocked='entry_shares_already_reserved'
            if not blocked:
                append(repository,intent_id,'post_claimed',{'day':day},api.now());claimed=True
        repository.connection.commit()
    except BaseException:repository.connection.rollback();raise
    if blocked:raise PaperBrokerError(blocked)
    if not claimed:return reconcile(repository,api,intent_id)
    try:
        raw=api.submit(intent['order'])
    except PaperBrokerError as exc:
        _record(repository,api,intent,'uncertain',{'code':exc.code,'http_status':exc.http_status})
        result=report(repository,intent_id);result.update(broker_queried=True,network_required=True,post_requests_attempted_by_this_call=1)
        return result
    result=_observe(repository,api,intent,raw);result['post_requests_attempted_by_this_call']=1
    return result


def cancel(repository,api,intent_id):
    current=reconcile(repository,api,intent_id)
    if current['phase']!='observed' or not current['order'] or current['order']['status'] in TERMINAL:return current
    intent={'id':intent_id,'account_ref':current['account_ref'],'order':current['request']}
    _record(repository,api,intent,'cancel_attempted',{})
    try:ack=api.cancel(current['order']['id'])
    except PaperBrokerError as exc:
        _record(repository,api,intent,'uncertain',{'code':exc.code,'http_status':exc.http_status})
    else:_record(repository,api,intent,'cancel_requested',ack)
    # DELETE acknowledgement alone never means canceled or zero fills.
    return reconcile(repository,api,intent_id)


def reconcile_account(repository,api):
    a=api.account();begin_read(repository)
    try:
        before=all_intents(repository,a['account_ref']);repository.connection.commit()
    except BaseException:repository.connection.rollback();raise
    for intent,state,*_ in before:
        reconcile(repository,api,intent['id'])
    positions=api.positions();open_orders=api.open_orders();latest=api.account()
    if latest['account_ref']!=a['account_ref']:raise PaperBrokerError('paper_account_changed')
    begin_read(repository)
    try:
        rows=all_intents(repository,a['account_ref']);repository.connection.commit()
    except BaseException:repository.connection.rollback();raise
    expected={};uncertain=[]
    for intent,state,*_ in rows:
        if state['phase'] in {'uncertain','submitting'}:uncertain.append(intent['id'])
        o=state['order']
        if o:
            symbol=o['symbol'];expected.setdefault(symbol,Decimal(0))
            expected[symbol]+=Decimal(o['filled_qty'])*(1 if o['side']=='buy' else -1)
    observed_clients={i['order']['client_order_id'] for i,s,*_ in rows if s['phase']=='observed' and s['order'] and s['order']['status'] not in TERMINAL}
    unmatched_open=sum(o.get('client_order_id') not in observed_clients for o in open_orders)
    differences=[]
    for symbol in sorted(set(expected)|set(positions)):
        actual=Decimal(positions.get(symbol,{}).get('qty','0'));local=expected.get(symbol,Decimal(0))
        if actual!=local:differences.append({'symbol':symbol,'managed_filled_quantity':ledger.number_text(local),'broker_quantity':ledger.number_text(actual)})
    return {'status':'matched' if not differences and not uncertain and not unmatched_open else 'attention_required',
            'environment':'alpaca_paper','account_ref':a['account_ref'],'account':latest,
            'unmatched_open_broker_orders':unmatched_open,'intents':len(rows),'uncertain_intents':uncertain,'position_differences':differences,
            'positions':positions,'scope':'managed_cumulative_order_quantities_vs_broker_position_snapshot',
            'atomic_broker_snapshot':False,'cash_reconciled':False,'fees_reconciled':False,'pnl_calculated':False,
            'broker_queried':True,'post_requests_attempted_by_this_call':0,'live_trading_supported':False}
