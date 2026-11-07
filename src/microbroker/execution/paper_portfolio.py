"""Frozen cumulative-fill lot accounting. GET-only capture; no net-profit inference."""
from datetime import datetime,timezone
from decimal import Decimal,localcontext
import json
import re
import uuid
from . import paper_broker as broker
from .paper_api import PaperBrokerError,order,valued_positions,decimal_text,clock
from ..data.models import DataValidationError
from ..data.research import begin_read,encode
from ..data.stream_models import canonical_time
from ..data.alignment import nanos
from ..data.storage.repository import StorageError
from ..intelligence.policy import digest

MAX_SAMPLES=5000
MAX_BYTES=2*1024*1024
MAX_HISTORY_BYTES=64*1024*1024


def validate_sampling(value):
    try:
        if (set(value)!={'format','session_id','slot','interval_seconds'} or value['format']!='R26'
            or str(uuid.UUID(value['session_id']))!=value['session_id']
            or not re.fullmatch(r'start|final|interval-[1-9][0-9]{0,2}',value['slot'])
            or value['interval_seconds']!=300 or type(value['interval_seconds']) is not int):raise ValueError()
    except (ValueError,TypeError,KeyError,AttributeError):raise StorageError('Invalid portfolio sampling scope') from None


def number(value):
    text=format(value,'f')
    return (text.rstrip('0').rstrip('.') if '.' in text else text) if value else '0'


def identity(value):
    if not isinstance(value,str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,64}',value):
        raise DataValidationError('Invalid portfolio snapshot-id')
    return value


def validate(value,account_ref,at):
    """Validate frozen inputs independently of mutable source tables."""
    try:
        if (value['format']!='R25' or value['environment']!='alpaca_paper' or value['account']['account_ref']!=account_ref
            or canonical_time(at)!=at or canonical_time(value['started_at'])!=value['started_at']
            or value['started_at']>at or clock(value['broker_clock'])!=value['broker_clock']):raise ValueError()
        for k in ('cash','equity','last_equity'):
            if decimal_text(value['account'][k],signed=True)!=value['account'][k]:raise ValueError()
        if valued_positions(list(value['positions'].values()))!=value['positions']:raise ValueError()
        if not isinstance(value['sources'],list) or len(value['sources'])>broker.MAX_INTENTS:raise ValueError()
        ids=set()
        for s in value['sources']:
            i=s['intent'];p=i['order'];name=i['id']
            if (i!=broker.intent_payload(name,account_ref,p['symbol'],p['side'],p.get('notional') if p['side']=='buy' else p.get('qty'),i['created_at'],i['parent_intent_id'])
                or name in ids or type(s['automatic_news_entry']) is not bool
                or (s['automatic_news_entry'] and not re.fullmatch('[0-9a-f]{64}',s.get('origin_decision_sha256') or ''))
                or (not s['automatic_news_entry'] and s.get('origin_decision_sha256') is not None)
                or not re.fullmatch('[0-9a-f]{64}',s['audit_sha256'])
                or canonical_time(s['observed_at'])!=s['observed_at'] or s['observed_at']>at
                or s['phase'] not in {'prepared','retired','submitting','uncertain','observed'}):raise ValueError()
            if s['order'] is not None and order(s['order'],i)!=s['order']:raise ValueError()
            ids.add(name)
        if (type(value['unmatched_open_broker_orders']) is not int or value['unmatched_open_broker_orders']<0):raise ValueError()
        if 'sampling' in value:validate_sampling(value['sampling'])
        if 'collection_method' in value and value['collection_method']!='r28-passive-committed-local-orders':raise ValueError()
    except (KeyError,TypeError,ValueError,PaperBrokerError,DataValidationError):
        raise StorageError('Invalid portfolio snapshot inputs') from None


def read_history(repository,account_ref):
    rows=repository.connection.execute(f'SELECT ordinal,id,observed_at,snapshot_json,previous_sha256,snapshot_sha256 FROM paper_portfolio_snapshots WHERE account_ref={repository.marker} ORDER BY ordinal LIMIT {MAX_SAMPLES+1}',(account_ref,)).fetchall()
    if len(rows)>MAX_SAMPLES:raise StorageError('Portfolio history exceeds 5000 snapshots')
    previous=digest([]);last=None;result=[];size=0
    for index,(ordinal,name,at,raw,prev,sha) in enumerate(rows):
        size+=len(raw.encode())
        if len(raw.encode())>MAX_BYTES or size>MAX_HISTORY_BYTES:raise StorageError('Portfolio snapshot history size bound exceeded')
        try:
            identity(name);value=json.loads(raw);validate(value,account_ref,at)
            expected=digest({'account_ref':account_ref,'ordinal':ordinal,'id':name,'observed_at':at,'snapshot':value,'previous':previous})
            if ordinal!=index or prev!=previous or sha!=expected or (last is not None and at<last):raise ValueError()
        except (ValueError,TypeError,DataValidationError):raise StorageError('Portfolio snapshot chain mismatch') from None
        result.append({'snapshot_id':name,'observed_at':at,'sha256':sha,'inputs':value});previous=sha;last=at
    return result,previous


def capture_portfolio(repository,api,*,snapshot_id=None,_sampling=None,_account_ref=None,
                      _passive=False,_commit_gate=None):
    if _sampling is not None:validate_sampling(_sampling)
    name=identity(snapshot_id or str(uuid.uuid4()));started=canonical_time(api.now());a=api.account();ref=a['account_ref']
    if _account_ref is not None and _account_ref!=ref:raise PaperBrokerError('paper_account_changed')
    begin_read(repository)
    try:
        history,_=read_history(repository,ref);same=next((r for r in history if r['snapshot_id']==name),None)
        baseline=passive_sources(repository,ref) if _passive else None
        repository.connection.commit()
    except BaseException:repository.connection.rollback();raise
    if same:
        if _sampling is not None and same['inputs'].get('sampling')!=_sampling:raise StorageError('Portfolio snapshot sampling scope changed')
        return {'status':'existing','snapshot_id':name,'observed_at':same['observed_at'],'account_ref':ref,'orders_sent':0,'snapshot_refreshed':False}
    # Existing reconciliation uses GET only and commits truthful broker observations independently.
    # A background observer must not append broker observations concurrently with
    # the fenced order writer. Freeze only unchanged, already committed sources.
    reconciled=None if _passive else broker.reconcile_account(repository,api)
    open_orders=api.open_orders() if _passive else None
    positions=api.valued_positions();c=api.clock();a=api.account();at=canonical_time(api.now())
    if a['account_ref']!=ref or (reconciled is not None and reconciled['account_ref']!=ref):raise PaperBrokerError('paper_account_changed')
    broker.locked(repository,ref)
    try:
        if _passive and passive_sources(repository,ref)!=baseline:
            raise PaperBrokerError('portfolio_sources_changed')
        history,previous=read_history(repository,ref)
        same=next((r for r in history if r['snapshot_id']==name),None)
        if same:
            if _sampling is not None and same['inputs'].get('sampling')!=_sampling:raise StorageError('Portfolio snapshot sampling scope changed')
            repository.connection.commit()
            return {'status':'existing','snapshot_id':name,'observed_at':same['observed_at'],'account_ref':ref,'orders_sent':0,'snapshot_refreshed':False}
        if len(history)>=MAX_SAMPLES or (history and at<history[-1]['observed_at']):raise StorageError('Portfolio history bound or local clock regression')
        from .paper_worker import all_decisions
        # Verify each news claim and event chain before calling an entry strategy-generated.
        automatic={intent_id:(request,state,sha) for key,request,state,sha,count,intent_id in all_decisions(repository,ref)}
        for i,s,sha,count,last in broker.all_intents(repository,ref):
            if i['id'] in automatic:
                request,state,_=automatic[i['id']]
                if request['symbol']!=i['order']['symbol'] or not state['intent_prepared']:
                    raise StorageError('Portfolio news origin does not match prepared intent')
        sources=[{'intent':i,'phase':s['phase'],'order':s['order'],'audit_sha256':sha,'observed_at':last,
                  'automatic_news_entry':i['id'] in automatic,
                  'origin_decision_sha256':automatic[i['id']][2] if i['id'] in automatic else None}
                 for i,s,sha,count,last in broker.all_intents(repository,ref)]
        clients={s['intent']['order']['client_order_id'] for s in sources if s['order'] is not None and s['phase']=='observed' and s['order']['status'] not in broker.TERMINAL}
        unmatched=sum(o.get('client_order_id') not in clients for o in open_orders) if _passive else reconciled['unmatched_open_broker_orders']
        value={'format':'R25','environment':'alpaca_paper','started_at':started,'account':a,'broker_clock':c,
               'positions':positions,'sources':sources,'unmatched_open_broker_orders':unmatched}
        if _passive:value['collection_method']='r28-passive-committed-local-orders'
        if _sampling is not None:value['sampling']=dict(_sampling)
        validate(value,ref,at);calculate(value)  # Reject broken parent/quantity economics before persistence.
        raw=encode(value)
        if len(raw.encode())>MAX_BYTES:raise StorageError('Portfolio snapshot exceeds 2 MiB')
        if sum(len(encode(r['inputs']).encode()) for r in history)+len(raw.encode())>MAX_HISTORY_BYTES:raise StorageError('Portfolio snapshot history size bound exceeded')
        sha=digest({'account_ref':ref,'ordinal':len(history),'id':name,'observed_at':at,'snapshot':value,'previous':previous})
        from contextlib import nullcontext
        with _commit_gate() if _commit_gate is not None else nullcontext():
            repository.connection.execute(f'INSERT INTO paper_portfolio_snapshots VALUES ({",".join([repository.marker]*7)})',
                (ref,len(history),name,at,raw,previous,sha));repository.connection.commit()
    except BaseException:repository.connection.rollback();raise
    return {'status':'captured','snapshot_id':name,'observed_at':at,'account_ref':ref,'orders_sent':0,
        'broker_queried':True,'snapshot_refreshed':True,'broker_snapshot_atomic':False,'fees_reconciled':False}


def passive_sources(repository,ref):
    """Caller owns the read/write transaction. No network and no source writes."""
    from .paper_worker import all_decisions
    return digest({'orders':[(i['id'],sha) for i,s,sha,count,last in broker.all_intents(repository,ref)],
                   'decisions':[(key,sha) for key,r,s,sha,count,i in all_decisions(repository,ref)]})


def calculate(value):
    """Each exit allocates its actual cumulative proceeds to its explicit parent entry."""
    with localcontext() as context:
        context.prec=100
        return _calculate(value)


def _calculate(value):
    sources=value['sources'];by_id={s['intent']['id']:s for s in sources};lots=[];net_cash=Decimal(0)
    uncertain=[s['intent']['id'] for s in sources if s['phase'] in {'uncertain','submitting'}]
    active=[s['intent']['id'] for s in sources if s['order'] is not None and s['order']['status'] not in broker.TERMINAL]
    for s in sources:
        i=s['intent'];o=s['order']
        if i['order']['side']=='sell':
            parent=by_id.get(i['parent_intent_id'])
            if parent is None or parent['intent']['order']['side']!='buy' or parent['intent']['order']['symbol']!=i['order']['symbol']:
                raise StorageError('Portfolio exit parent mismatch')
            if o and Decimal(o['filled_qty']) and (parent['order'] is None or not Decimal(parent['order']['filled_qty'])):
                raise StorageError('Portfolio exit has no recorded entry fill')
        if o and Decimal(o['filled_qty']):net_cash+=Decimal(o['filled_qty'])*Decimal(o['filled_avg_price'])*(1 if o['side']=='sell' else -1)
    for s in sources:
        i=s['intent'];o=s['order']
        if i['order']['side']!='buy' or o is None or not Decimal(o['filled_qty']):continue
        qty=Decimal(o['filled_qty']);price=Decimal(o['filled_avg_price']);sold=Decimal(0);proceeds=Decimal(0);exits=[]
        for e in sources:
            if e['intent']['parent_intent_id']!=i['id'] or e['order'] is None:continue
            eo=e['order'];eq=Decimal(eo['filled_qty'])
            if eq and o['status'] not in broker.TERMINAL:raise StorageError('Portfolio exit requires a terminal entry')
            sold+=eq;proceeds+=eq*Decimal(eo['filled_avg_price'] or '0');exits.append(e['intent']['id'])
        if sold>qty:raise StorageError('Portfolio exits exceed entry cumulative fill')
        remaining=qty-sold;basis=remaining*price;realized=proceeds-sold*price
        lots.append({'entry_intent_id':i['id'],'symbol':o['symbol'],'origin':'automatic_news' if s['automatic_news_entry'] else 'manual_paper',
            'entry_quantity':number(qty),'entry_average_price':number(price),'sold_quantity':number(sold),'open_quantity':number(remaining),
            'remaining_cost_basis':number(basis),'gross_realized_pnl':number(realized),'exit_intent_ids':exits})
    symbols=sorted({r['symbol'] for r in lots}|set(value['positions']));rows=[];differences=[]
    for symbol in symbols:
        group=[l for l in lots if l['symbol']==symbol];qty=sum((Decimal(l['open_quantity']) for l in group),Decimal(0))
        basis=sum((Decimal(l['remaining_cost_basis']) for l in group),Decimal(0));p=value['positions'].get(symbol)
        actual=Decimal(p['qty']) if p else Decimal(0);match=qty==actual
        if not match:differences.append({'symbol':symbol,'managed_quantity':number(qty),'broker_quantity':number(actual)})
        mark=Decimal(p['current_price']) if p and p['current_price'] is not None and match and qty>0 else None
        for l in group:
            remaining=Decimal(l['open_quantity']);l['gross_unrealized_pnl']=number(remaining*mark-Decimal(l['remaining_cost_basis'])) if remaining and mark is not None else ('0' if not remaining else None)
        rows.append({'symbol':symbol,'managed_quantity':number(qty),'broker_quantity':number(actual),'quantities_match':match,
            'remaining_cost_basis':number(basis),'gross_realized_pnl':number(sum((Decimal(l['gross_realized_pnl']) for l in group),Decimal(0))),
            'valuation_price':number(mark) if mark is not None else None,'broker_position':p,
            'gross_unrealized_pnl':number(qty*mark-basis) if qty and mark is not None else ('0' if not qty and match else None)})
    realized=sum((Decimal(l['gross_realized_pnl']) for l in lots),Decimal(0));unknown=any(l['gross_unrealized_pnl'] is None for l in lots)
    unrealized=None if unknown else sum((Decimal(l['gross_unrealized_pnl']) for l in lots),Decimal(0))
    complete=not uncertain and not differences and not value['unmatched_open_broker_orders'] and not unknown
    origins={}
    for origin in ('manual_paper','automatic_news'):
        group=[l for l in lots if l['origin']==origin];r=sum((Decimal(l['gross_realized_pnl']) for l in group),Decimal(0))
        u=None if any(l['gross_unrealized_pnl'] is None for l in group) else sum((Decimal(l['gross_unrealized_pnl']) for l in group),Decimal(0))
        origins[origin]={'lots':len(group),'known_gross_realized_pnl':number(r),'known_gross_unrealized_pnl':number(u) if u is not None else None,
                         'gross_total_pnl':number(r+u) if complete and u is not None else None}
    return {'status':'ready' if complete else 'attention_required','lots':lots,'per_symbol':rows,'by_origin':origins,
        'known_gross_realized_pnl':number(realized),'known_gross_unrealized_pnl':number(unrealized) if unrealized is not None else None,
        'gross_total_pnl':number(realized+unrealized) if complete else None,'managed_trade_cash':number(net_cash),
        'position_differences':differences,'uncertain_intents':uncertain,'active_orders':active,
        'unmatched_open_broker_orders':value['unmatched_open_broker_orders']}


def portfolio_report(repository,account_ref=None,*,snapshot_id=None,now=None):
    if snapshot_id is not None:identity(snapshot_id)
    if account_ref is not None and not re.fullmatch('[0-9a-f]{64}',account_ref):raise DataValidationError('Invalid Paper account-ref')
    begin_read(repository)
    try:
        if snapshot_id is not None:
            refs=[r[0] for r in repository.connection.execute(f'SELECT account_ref FROM paper_portfolio_snapshots WHERE id={repository.marker}',(snapshot_id,)).fetchall()]
            if not refs or (account_ref is not None and account_ref not in refs):raise DataValidationError('Unknown portfolio snapshot-id')
        else:refs=[r[0] for r in repository.connection.execute('SELECT DISTINCT account_ref FROM paper_portfolio_snapshots LIMIT 101').fetchall()]
        if account_ref is None:
            if len(refs)!=1:raise DataValidationError('Capture a Paper portfolio first; multiple accounts require --account-ref')
            account_ref=refs[0]
        history,_=read_history(repository,account_ref);repository.connection.commit()
    except BaseException:repository.connection.rollback();raise
    selected=next((r for r in history if r['snapshot_id']==snapshot_id),None) if snapshot_id else (history[-1] if history else None)
    if selected is None:raise DataValidationError('Capture a Paper portfolio first')
    value=selected['inputs'];result=calculate(value);index=history.index(selected);previous=history[index-1] if index else None
    current=canonical_time(now or datetime.now(timezone.utc).isoformat());age=(nanos(current)-nanos(selected['observed_at']))/1e9
    warnings=['gross_not_net_profit','cumulative_average_fill_not_execution_activity_ledger','independent_gets_not_atomic',
              'broker_price_timestamp_unavailable','lot_basis_not_broker_tax_basis','fees_and_cashflows_not_reconciled']
    if value.get('collection_method')=='r28-passive-committed-local-orders':warnings.append('passive_capture_does_not_refresh_local_order_observations')
    if result['active_orders']:warnings.append('active_orders_can_change_cumulative_fills')
    if age<0:warnings.append('local_clock_before_snapshot')
    elif age>300:warnings.append('saved_observation_older_than_5_minutes')
    if not value['broker_clock']['is_open']:warnings.append('market_closed_recorded_valuation_not_executable_price')
    change={'baseline_snapshot_id':previous['snapshot_id'] if previous else None,'cash_change':None,'managed_trade_cash_change':None,
            'unexplained_cash_residual':None,'meaning':'observed_cash_change_minus_change_in_known_cumulative_managed_trade_cash_not_assigned_to_fees'}
    with localcontext() as ctx:
        ctx.prec=100
        if previous:
            old=calculate(previous['inputs']);cash=Decimal(value['account']['cash'])-Decimal(previous['inputs']['account']['cash'])
            trade=Decimal(result['managed_trade_cash'])-Decimal(old['managed_trade_cash'])
            change.update(cash_change=number(cash),managed_trade_cash_change=number(trade),unexplained_cash_residual=number(cash-trade))
        broker_mvs=[p['market_value'] for p in value['positions'].values()]
        residual=number(Decimal(value['account']['equity'])-Decimal(value['account']['cash'])-sum((Decimal(v) for v in broker_mvs),Decimal(0))) if all(v is not None for v in broker_mvs) else None
    return {**result,'format':'R25','environment':'alpaca_paper','account_ref':account_ref,'snapshot_id':selected['snapshot_id'],
        'observed_at':selected['observed_at'],'collection_started_at':value['started_at'],'account':value['account'],'broker_clock':value['broker_clock'],
        'observation_age_seconds':age,'saved_observation_stale':age<0 or age>300,'broker_price_freshness_validated':False,
        'cash_comparison':change,'account_equity_identity_residual':residual,'net_profit':None,'fees_paid':None,
        'fees_reconciled':False,'cashflows_verified':False,'cash_reconciled':False,'broker_snapshot_atomic':False,
        'scope':'lifetime_known_managed_cumulative_fills_explicit_parent_lots_and_recorded_broker_marks',
        'period_pnl_calculated':False,'tax_basis_reconciled':False,'strategy_performance_validated':False,'profitability_validated':False,
        'snapshot_sha256':selected['sha256'],'snapshot_integrity_verified':True,'integrity_semantics':'digest_chain_detects_changes_not_authenticity',
        'network_required':False,'database_modified':False,'orders_sent':0,'warning_codes':warnings}


def render_portfolio(value):
    lines=['Paper portföyü — kayıtlı brüt kâr-zarar',f"Kayıt zamanı: {value['observed_at']} (UTC)",
        f"Durum: {value['status']} | Nakit: {value['account']['cash']} USD | Özsermaye: {value['account']['equity']} USD",
        f"Bilinen brüt gerçekleşmiş: {value['known_gross_realized_pnl']} USD",
        f"Bilinen brüt gerçekleşmemiş: {value['known_gross_unrealized_pnl'] if value['known_gross_unrealized_pnl'] is not None else 'hesaplanamadı'} USD",
        f"Brüt toplam: {value['gross_total_pnl'] if value['gross_total_pnl'] is not None else 'eksik/uyumsuz veri nedeniyle verilmedi'} USD"]
    for row in value['per_symbol']:
        lines.append(f"{row['symbol']}: yönetilen {row['managed_quantity']} / broker {row['broker_quantity']} adet; gerçekleşmiş {row['gross_realized_pnl']}; gerçekleşmemiş {row['gross_unrealized_pnl'] if row['gross_unrealized_pnl'] is not None else 'bilinmiyor'} USD")
    for origin,title in [('manual_paper','Elle başlatılan Paper işlemleri'),('automatic_news','Otomatik haber işlemleri')]:
        r=value['by_origin'][origin];lines.append(f"{title}: {r['lots']} alım; brüt gerçekleşmiş {r['known_gross_realized_pnl']}; brüt gerçekleşmemiş {r['known_gross_unrealized_pnl'] if r['known_gross_unrealized_pnl'] is not None else 'bilinmiyor'} USD")
    c=value['cash_comparison'];lines.append('Önceki gözleme göre açıklanmamış nakit farkı: '+(c['unexplained_cash_residual'] if c['unexplained_cash_residual'] is not None else 'ilk kayıt; karşılaştırma yok')+' USD')
    if value['saved_observation_stale']:lines.append('Kayıt eski veya yerel saat tutarsız; yeni portföy gözlemi alın.')
    if value['position_differences'] or value['uncertain_intents'] or value['unmatched_open_broker_orders']:
        lines.append('Mutabakat eksik: miktar farkı / belirsiz emir / eşleşmeyen açık emir var; JSON raporunu inceleyin.')
    if value['active_orders']:lines.append('Açık emirler var; gerçekleşen miktarlar değişebilir.')
    lines.extend(['Fiyatın borsa zaman damgası yok; güncelliği doğrulanmadı. Bu komut broker sorgulamaz.',
        'Ücretler ve hesap hareketleri doğrulanmadı. Nakit farkı ücret veya net kâr sayılmadı.',
        'Bu rapor bilinen işlemlerin birikimli brüt sonucudur; haftalık/aylık net strateji kârı değildir.'])
    return '\n'.join(lines)
