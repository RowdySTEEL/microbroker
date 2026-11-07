"""Local long-only USD accounting with explicit manual price assumptions; no broker I/O."""
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal, localcontext, ROUND_HALF_EVEN, InvalidOperation
import json
import re
from ..config import UNIVERSE
from ..data.models import DataValidationError, utc_datetime
from ..data.stream_models import canonical_time
from ..data.research import begin_read, encode
from ..data.storage.repository import StorageError
from ..intelligence.policy import digest

MAX_EVENTS = 10000
MAX_BYTES = 64*1024*1024
MONEY_QUANTUM = Decimal('0.00000001')

def decimal_value(value, name, *, maximum='1000000000', places=6, zero=False):
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise DataValidationError(name+' must be a decimal string, not float/bool')
    text=str(value)
    if len(text)>40 or not re.fullmatch(r'(?:0|[1-9][0-9]*)(?:\.[0-9]+)?',text):
        raise DataValidationError('Invalid '+name)
    try:
        with localcontext() as ctx:
            ctx.prec=50
            number=Decimal(text)
            if (not number.is_finite() or number<0 or (not zero and number==0) or
                number>Decimal(maximum) or number!=number.quantize(Decimal(1).scaleb(-places))):
                raise DataValidationError('Invalid '+name+' range or precision')
            return number
    except InvalidOperation:
        raise DataValidationError('Invalid '+name) from None

def number_text(number):
    text=format(number,'f')
    return text.rstrip('0').rstrip('.') if '.' in text else text

def money(number):
    return number.quantize(MONEY_QUANTUM,rounding=ROUND_HALF_EVEN)

def money_text(number):
    return format(money(number),'f')

def identifier(value):
    if not isinstance(value,str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,63}',value):
        raise DataValidationError('Use a 1–64 character account/event key with letters, digits, _ or -')
    return value

def account_config(initial_cash, start_at, fee_per_fill='1', slippage_bps='5',
                   max_position_fraction='0.25', max_mark_age_seconds=300,
                   scope='local_manual_price_scenarios'):
    initial=decimal_value(initial_cash,'initial cash',places=2)
    fee=decimal_value(fee_per_fill,'fee per fill',maximum='10000',places=2,zero=True)
    slippage=decimal_value(slippage_bps,'slippage bps',maximum='1000',places=4,zero=True)
    fraction=decimal_value(max_position_fraction,'position cost fraction',maximum='1',places=6)
    if type(max_mark_age_seconds) is not int or not 0<=max_mark_age_seconds<=86400:
        raise DataValidationError('Invalid mark age limit')
    if scope not in {'local_manual_price_scenarios','recorded_receipt_strategy_simulation'}:
        raise DataValidationError('Invalid paper account scope')
    return {'format':'R19','currency':'USD','start_at':canonical_time(start_at),
            'initial_cash':number_text(initial),'fee_per_fill':number_text(fee),
            'slippage_bps':number_text(slippage),'max_position_cost_fraction':number_text(fraction),
            'max_mark_age_seconds':max_mark_age_seconds,
            'accounting':'USD_round_half_even_8_decimals_weighted_average_cost_includes_buy_fees',
            'scope':scope,'shorts_allowed':False,'leverage_allowed':False}

def initial_state(config):
    with localcontext() as ctx:
        ctx.prec=50
        return {'cash':money_text(Decimal(config['initial_cash'])),'positions':{},
                'realized_pnl':'0.00000000','fees_paid':'0.00000000','slippage_cost':'0.00000000',
                'fills':0,'marks':0,'last_at':config['start_at']}

def fill_request(event_key, symbol, side, quantity, reference_price, at):
    if symbol not in UNIVERSE or side not in {'buy','sell'}:
        raise DataValidationError('Invalid paper symbol/side')
    return {'kind':'fill','event_key':identifier(event_key),'symbol':symbol,'side':side,
            'quantity':number_text(decimal_value(quantity,'quantity',maximum='1000000')),
            'reference_price':number_text(decimal_value(reference_price,'reference price')),
            'at':canonical_time(at),'price_source':'manual_scenario_assumption'}

def mark_request(event_key, symbol, reference_price, at):
    if symbol not in UNIVERSE:raise DataValidationError('Invalid paper symbol')
    return {'kind':'mark','event_key':identifier(event_key),'symbol':symbol,
            'reference_price':number_text(decimal_value(reference_price,'reference price')),
            'at':canonical_time(at),'price_source':'manual_scenario_assumption'}

def validate_request(request):
    try:
        if request['kind']=='fill':
            expected=fill_request(request['event_key'],request['symbol'],request['side'],request['quantity'],request['reference_price'],request['at'])
        elif request['kind']=='mark':
            expected=mark_request(request['event_key'],request['symbol'],request['reference_price'],request['at'])
        else:raise StorageError('Invalid paper ledger event kind')
        if 'provenance' in request:
            from .strategy_provenance import validate_provenance
            validate_provenance(request)
            expected.update(price_source='recorded_receipt_simulation_assumption',provenance=request['provenance'])
        if request!=expected:raise StorageError('Paper ledger request is not canonical')
    except (KeyError,TypeError,ValueError,DataValidationError):
        raise StorageError('Invalid paper ledger request') from None

def apply_request(config, state, request):
    """Deterministic reducer; no outside prices, current news or model state."""
    if ('provenance' in request)!=(config['scope']=='recorded_receipt_strategy_simulation'):
        raise DataValidationError('Paper event provenance must match its account scope')
    if request['at']<state['last_at']:
        raise DataValidationError('Paper event is older than the latest committed scenario event')
    new=deepcopy(state);symbol=request['symbol']
    with localcontext() as ctx:
        ctx.prec=50
        reference=Decimal(request['reference_price'])
        if request['kind']=='mark':
            if symbol not in new['positions']:raise DataValidationError('Cannot mark a symbol without an open position')
            new['positions'][symbol].update(mark_price=money_text(reference),marked_at=request['at'])
            new['marks']+=1
        else:
            quantity=Decimal(request['quantity']);fee=Decimal(config['fee_per_fill'])
            slippage=Decimal(config['slippage_bps'])/Decimal(10000)
            price=money(reference*(1+slippage if request['side']=='buy' else 1-slippage))
            notional=money(price*quantity)
            if price<=0 or notional<=0:raise DataValidationError('Paper fill is below accounting precision')
            cash=Decimal(new['cash'])
            position=new['positions'].get(symbol,{'quantity':'0','cost_basis':'0.00000000'})
            old_quantity=Decimal(position['quantity']);basis=Decimal(position['cost_basis'])
            if request['side']=='buy':
                cost=notional+fee
                if cost>cash:raise DataValidationError('Insufficient paper cash')
                if basis+cost>Decimal(config['initial_cash'])*Decimal(config['max_position_cost_fraction']):
                    raise DataValidationError('Paper position cost cap exceeded')
                cash-=cost;basis+=cost;old_quantity+=quantity
            else:
                if quantity>old_quantity:raise DataValidationError('Paper sell exceeds held quantity; shorting is disabled')
                proceeds=notional-fee
                if cash+proceeds<0:raise DataValidationError('Paper sell fees exceed available cash')
                released=basis if quantity==old_quantity else money(basis*quantity/old_quantity)
                cash+=proceeds;basis-=released;old_quantity-=quantity
                new['realized_pnl']=money_text(Decimal(new['realized_pnl'])+proceeds-released)
            new['cash']=money_text(cash)
            if old_quantity:
                new['positions'][symbol]={'quantity':number_text(old_quantity),'cost_basis':money_text(basis),
                    'average_cost':money_text(basis/old_quantity),'mark_price':money_text(reference),'marked_at':request['at']}
            else:new['positions'].pop(symbol,None)
            new['fees_paid']=money_text(Decimal(new['fees_paid'])+fee)
            new['slippage_cost']=money_text(Decimal(new['slippage_cost'])+abs(notional-money(reference*quantity)))
            new['fills']+=1
        new['last_at']=request['at']
    return new

def begin_write(repository):
    if repository.postgres:
        if repository.connection.info.transaction_status!=0:raise StorageError('Paper writes require a clean transaction')
        repository.connection.execute('BEGIN')
    else:
        if repository.connection.in_transaction:raise StorageError('Paper writes require a clean transaction')
        repository.connection.execute('BEGIN IMMEDIATE')

def header(repository, account_id, lock=False):
    query=f'SELECT created_at,config_json,config_sha256 FROM paper_accounts WHERE id={repository.marker}'
    if lock and repository.postgres:query+=' FOR UPDATE'
    row=repository.connection.execute(query,(account_id,)).fetchone()
    if not row:raise DataValidationError('Unknown paper account')
    try:
        config=json.loads(row[1])
        expected=account_config(config['initial_cash'],config['start_at'],config['fee_per_fill'],
            config['slippage_bps'],config['max_position_cost_fraction'],config['max_mark_age_seconds'],config['scope'])
        if config!=expected or canonical_time(row[0])!=row[0] or digest({'id':account_id,'created_at':row[0],'config':config})!=row[2]:
            raise StorageError('Paper account configuration integrity check failed')
    except (ValueError,TypeError,KeyError,DataValidationError):
        raise StorageError('Invalid paper account configuration') from None
    return config,row[2]

def event_digest(account_id, ordinal, previous, request, state):
    return digest({'account_id':account_id,'ordinal':ordinal,'previous_sha256':previous,'request':request,'state':state})

def read_ledger(repository, account_id, config, root_sha):
    state=initial_state(config);previous=root_sha;rows=[];size=0;seen=set()
    cursor=repository.connection.execute(f'SELECT ordinal,event_key,event_at,request_json,request_sha256,previous_sha256,state_json,event_sha256 FROM paper_ledger WHERE account_id={repository.marker} ORDER BY ordinal LIMIT {MAX_EVENTS+1}',(account_id,))
    for record in cursor:
        ordinal,key,at,raw,request_sha,parent,state_raw,sha=record
        if not isinstance(raw,str) or not isinstance(state_raw,str):
            raise StorageError('Invalid stored paper event encoding')
        size+=len(raw.encode())+len(state_raw.encode())
        if len(rows)>=MAX_EVENTS or size>MAX_BYTES:raise StorageError('Paper ledger exceeds 10000 events/64 MiB; archive before extending')
        try:
            request=json.loads(raw);saved=json.loads(state_raw);validate_request(request)
            if ordinal!=len(rows) or key in seen or key!=request['event_key'] or at!=request['at'] or digest(request)!=request_sha or parent!=previous:
                raise StorageError('Paper ledger identity/hash chain mismatch')
            state=apply_request(config,state,request)
            if saved!=state or event_digest(account_id,ordinal,previous,request,state)!=sha:
                raise StorageError('Paper ledger state integrity check failed')
        except (ValueError,TypeError,KeyError,DataValidationError):
            raise StorageError('Invalid stored paper event') from None
        rows.append({'ordinal':ordinal,'request':request,'state':state,'sha256':sha})
        previous=sha;seen.add(key)
    return state,rows,previous,size

def valuation(config, state, at):
    strategy=config['scope']=='recorded_receipt_strategy_simulation'
    positions={};market_value=Decimal(0);unrealized=Decimal(0)
    warnings=['recorded_receipt_simulation_assumptions' if strategy else 'manual_price_assumptions'];stale=[]
    with localcontext() as ctx:
        ctx.prec=50
        for symbol,p in sorted(state['positions'].items()):
            value=money(Decimal(p['quantity'])*Decimal(p['mark_price']))
            pnl=value-Decimal(p['cost_basis']);market_value+=value;unrealized+=pnl
            age=(utc_datetime(at)-utc_datetime(p['marked_at'])).total_seconds()
            if age>config['max_mark_age_seconds']:stale.append(symbol)
            positions[symbol]={**p,'market_value':money_text(value),'unrealized_pnl':money_text(pnl),'mark_age_seconds':age}
        cash=Decimal(state['cash']);equity=cash+market_value;initial=Decimal(config['initial_cash'])
        if money(equity-initial)!=money(Decimal(state['realized_pnl'])+unrealized):
            raise StorageError('Paper accounting identity failed')
        if stale:warnings.append('stale_recorded_marks' if strategy else 'stale_manual_marks')
        if at>state['last_at']:warnings.append('valuation_carried_forward')
        return {'cash':money_text(cash),'positions':positions,'market_value':money_text(market_value),
                'equity':money_text(equity),'realized_pnl':state['realized_pnl'],
                'unrealized_pnl':money_text(unrealized),'total_pnl':money_text(equity-initial),
                'fees_paid':state['fees_paid'],'slippage_cost':state['slippage_cost'],
                'fills':state['fills'],'marks':state['marks'],'at':at,'latest_event_at':state['last_at'],
                'fresh_mark_coverage':not stale,'stale_symbols':stale,'warning_codes':warnings,
                'open_pnl_includes_hypothetical_exit_costs':False,'broker_connected':False,'orders_sent':0,
                'scope':config['scope'],'strategy_implemented':strategy,
                'ledger_integrity_semantics':'recomputed_state_and_digest_chain_not_authenticity',
                'profitability_validated':False}

def create_account(repository, account_id, initial_cash, start_at, **options):
    identifier(account_id);config=account_config(initial_cash,start_at,**options)
    created=canonical_time(datetime.now(timezone.utc).isoformat());sha=digest({'id':account_id,'created_at':created,'config':config})
    begin_write(repository)
    try:
        m=repository.marker
        inserted=repository.connection.execute(f'INSERT INTO paper_accounts VALUES ({",".join([m]*4)}) ON CONFLICT (id) DO NOTHING',(account_id,created,encode(config),sha)).rowcount
        existing,root=header(repository,account_id,True)
        if existing!=config:raise DataValidationError('Paper account exists with different configuration; choose another name')
        state,rows,last,size=read_ledger(repository,account_id,existing,root)
        repository.connection.commit()
        return {'status':'created' if inserted else 'existing','account_id':account_id,'config':config,'ledger_events':len(rows),'ledger_sha256':last,'scope':config['scope'],'orders_sent':0}
    except BaseException:repository.connection.rollback();raise

def append_event(repository, account_id, request):
    identifier(account_id);validate_request(request);begin_write(repository)
    try:
        config,root=header(repository,account_id,True)
        if config['scope']!='local_manual_price_scenarios':
            raise DataValidationError('Strategy simulation accounts are sealed; use a new run-id for another scenario')
        state,rows,last,size=read_ledger(repository,account_id,config,root)
        duplicate=next((r for r in rows if r['request']['event_key']==request['event_key']),None)
        if duplicate:
            if duplicate['request']!=request:raise DataValidationError('Paper event key was used for a different request')
            status='duplicate';ordinal=duplicate['ordinal']
        else:
            if len(rows)>=MAX_EVENTS:raise StorageError('Paper ledger reached its event limit')
            state=apply_request(config,state,request);ordinal=len(rows)
            raw=encode(request);state_raw=encode(state)
            if size+len(raw.encode())+len(state_raw.encode())>MAX_BYTES:raise StorageError('Paper ledger reached its byte limit')
            sha=event_digest(account_id,ordinal,last,request,state);m=repository.marker
            repository.connection.execute(f'INSERT INTO paper_ledger VALUES ({",".join([m]*9)})',
                (account_id,ordinal,request['event_key'],request['at'],raw,digest(request),last,state_raw,sha))
            last=sha;rows.append({'ordinal':ordinal,'request':request,'state':state,'sha256':sha});status='applied'
        current_valuation=valuation(config,state,state['last_at'])
        repository.connection.commit()
        return {'status':status,'account_id':account_id,'event_ordinal':ordinal,'ledger_events':len(rows),
                'ledger_sha256':last,'ledger_integrity_verified':True,**current_valuation}
    except BaseException:repository.connection.rollback();raise

def account_summary(repository, account_id, at=None):
    identifier(account_id);begin_read(repository)
    try:
        config,root=header(repository,account_id)
        state,rows,last,size=read_ledger(repository,account_id,config,root)
        cutoff=canonical_time(at) if at else state['last_at']
        if cutoff<config['start_at']:raise DataValidationError('Paper summary time precedes scenario start')
        selected=[r for r in rows if r['request']['at']<=cutoff]
        state=selected[-1]['state'] if selected else initial_state(config)
        result={'status':'ready','account_id':account_id,'config':config,'ledger_events':len(rows),
                'events_available_at':len(selected),'ledger_sha256':last,'ledger_integrity_verified':True,
                'market_tables_modified':False,**valuation(config,state,cutoff)}
        repository.connection.commit();return result
    except BaseException:repository.connection.rollback();raise

def period_report(repository, account_id, start, end):
    identifier(account_id);start=canonical_time(start);end=canonical_time(end)
    if end<start:raise DataValidationError('Paper period end precedes start')
    begin_read(repository)
    try:
        config,root=header(repository,account_id);latest,rows,last,size=read_ledger(repository,account_id,config,root)
        if start<config['start_at']:raise DataValidationError('Paper period precedes scenario start')
        def at(t):
            selected=[r for r in rows if r['request']['at']<=t]
            return valuation(config,selected[-1]['state'] if selected else initial_state(config),t)
        first,final=at(start),at(end)
        with localcontext() as ctx:
            ctx.prec=50
            pnl=Decimal(final['equity'])-Decimal(first['equity']);denominator=Decimal(first['equity'])
            result={'status':'ready','account_id':account_id,'start':start,'end':end,
                    'period_semantics':'equity_difference_between_inclusive_scenario_time_boundaries',
                    'period_pnl':money_text(pnl),'return_percent':money_text(pnl/denominator*100) if denominator else None,
                    'realized_pnl_change':money_text(Decimal(final['realized_pnl'])-Decimal(first['realized_pnl'])),
                    'unrealized_pnl_change':money_text(Decimal(final['unrealized_pnl'])-Decimal(first['unrealized_pnl'])),
                    'fees_in_period':money_text(Decimal(final['fees_paid'])-Decimal(first['fees_paid'])),
                    'fills_in_period':final['fills']-first['fills'],'start_valuation':first,'end_valuation':final,
                    'ledger_sha256':last,'ledger_integrity_verified':True,'market_tables_modified':False,
                    'scope':config['scope'],'strategy_implemented':config['scope']=='recorded_receipt_strategy_simulation',
                    'broker_connected':False,'profitability_validated':False,'orders_sent':0}
        repository.connection.commit();return result
    except BaseException:repository.connection.rollback();raise
