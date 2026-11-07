"""Immutable sampled Paper account equity; cash-flow-adjusted P&L is not inferred."""
from datetime import datetime,timezone,timedelta
from decimal import Decimal,localcontext
import json
import uuid
from .paper_broker import locked
from .paper_api import clock as normalize_clock, PaperBrokerError
from .paper_ledger import money_text
from ..data.models import DataValidationError
from ..data.stream_models import canonical_time
from ..data.research import begin_read,encode
from ..data.storage.repository import StorageError
from ..intelligence.policy import digest

MAX_SAMPLES=10000


def read_samples(repository,account_ref):
    root=digest({'format':'R23','account_ref':account_ref});previous=root;last=None;result=[];size=0
    rows=repository.connection.execute(f'SELECT ordinal,sample_id,observed_at,snapshot_json,previous_sha256,sample_sha256 FROM paper_equity_samples WHERE account_ref={repository.marker} ORDER BY ordinal LIMIT {MAX_SAMPLES+1}',(account_ref,)).fetchall()
    if len(rows)>MAX_SAMPLES:raise StorageError('Paper equity history exceeds 10000 samples')
    for index,(ordinal,sample_id,at,raw,prev,sha) in enumerate(rows):
        size+=len(raw.encode())
        if size>16*1024*1024:raise StorageError('Paper equity history exceeds 16 MiB')
        try:
            value=json.loads(raw)
            expected=digest({'account_ref':account_ref,'ordinal':ordinal,'sample_id':sample_id,'at':at,'snapshot':value,'previous':previous})
            if (ordinal!=index or prev!=previous or sha!=expected or canonical_time(at)!=at or
                (last is not None and at<last) or value['account']['account_ref']!=account_ref):
                raise StorageError('Paper equity sample chain mismatch')
            for k in ('cash','equity','last_equity'):
                from .paper_api import decimal_text
                if decimal_text(value['account'][k],signed=True)!=value['account'][k]:raise StorageError('Invalid equity number')
            if normalize_clock(value['broker_clock'])!=value['broker_clock'] or value['cashflows_verified'] is not False or value['atomic_broker_snapshot'] is not False:raise StorageError('Invalid equity observation semantics')
            if value['environment']!='alpaca_paper' or value['format']!='R23':raise StorageError('Invalid equity scope')
        except (KeyError,ValueError,TypeError,PaperBrokerError):raise StorageError('Invalid equity sample') from None
        result.append({'sample_id':sample_id,'observed_at':at,**value});previous=sha;last=at
    return result,previous


def capture_equity(repository,api,*,session_id=None,sample_id=None):
    a=api.account();clock=api.clock();at=canonical_time(api.now());sample_id=sample_id or str(uuid.uuid4())
    if not isinstance(sample_id,str) or not 1<=len(sample_id)<=64:raise DataValidationError('Invalid equity sample-id')
    value={'format':'R23','environment':'alpaca_paper','account':a,'broker_clock':clock,
           'session_id':session_id,'cashflows_verified':False,'atomic_broker_snapshot':False}
    locked(repository,a['account_ref'])
    try:
        rows,previous=read_samples(repository,a['account_ref'])
        same=next((r for r in rows if r['sample_id']==sample_id),None)
        if same:
            repository.connection.commit()
            return {'status':'existing','sample_id':sample_id,'observed_at':same['observed_at'],'account_ref':a['account_ref'],'post_requests_attempted':0}
        if len(rows)>=MAX_SAMPLES or (rows and at<rows[-1]['observed_at']):raise StorageError('Equity sample bound/local clock order failed')
        ordinal=len(rows);sha=digest({'account_ref':a['account_ref'],'ordinal':ordinal,'sample_id':sample_id,'at':at,'snapshot':value,'previous':previous})
        repository.connection.execute(f'INSERT INTO paper_equity_samples VALUES ({",".join([repository.marker]*7)})',
            (a['account_ref'],ordinal,sample_id,at,encode(value),previous,sha))
        repository.connection.commit()
        return {'status':'captured','sample_id':sample_id,'observed_at':at,'account_ref':a['account_ref'],
                'equity':a['equity'],'cash':a['cash'],'post_requests_attempted':0,'broker_queried':True}
    except BaseException:repository.connection.rollback();raise


def performance_report(repository,account_ref=None,*,period='week',start=None,end=None):
    if period not in {'day','week','month'}:raise DataValidationError('Paper period must be day, week or month')
    begin_read(repository)
    try:
        refs=[r[0] for r in repository.connection.execute('SELECT DISTINCT account_ref FROM paper_equity_samples LIMIT 101').fetchall()]
        if account_ref is None:
            if len(refs)>1:raise DataValidationError('Multiple Paper accounts; specify --account-ref')
            account_ref=refs[0] if refs else None
        rows,sha=read_samples(repository,account_ref) if account_ref else ([],digest([]))
        repository.connection.commit()
    except BaseException:repository.connection.rollback();raise
    cutoff=canonical_time(end) if end else (rows[-1]['observed_at'] if rows else canonical_time(datetime.now(timezone.utc).isoformat()))
    first=canonical_time(start) if start else (datetime.fromisoformat(cutoff[:19]+'+00:00')-timedelta(days={'day':1,'week':7,'month':30}[period])).strftime('%Y-%m-%dT%H:%M:%S')+cutoff[19:]
    if first>cutoff:raise DataValidationError('Paper report start must not exceed end')
    selected=[r for r in rows if first<=r['observed_at']<=cutoff]
    result={'status':'ready' if len(selected)>=2 else 'insufficient_observations','account_ref':account_ref,
        'period':period,'period_semantics':'trailing_1_7_30_days_anchored_at_latest_recorded_sample_unless_explicit_end',
        'requested_start':first,'requested_end':cutoff,'samples':len(selected),'history_samples':len(rows),
        'history_covers_requested_start':bool(rows and rows[0]['observed_at']<=first),
        'observed_start':selected[0]['observed_at'] if selected else None,
        'observed_end':selected[-1]['observed_at'] if selected else None,
        'equity_change':None,'equity_change_percent':None,'max_observed_drawdown_percent':None,
        'scope':'recorded_account_equity_change_not_cashflow_adjusted_strategy_profit',
        'net_profit':None,'cashflows_verified':False,'fees_reconciled':False,'realized_unrealized_split':False,
        'strategy_performance_validated':False,'profitability_validated':False,'account_history_sha256':sha,
        'history_integrity_verified':True,'network_required':False,'database_modified':False,
        'post_requests_attempted':0,'sampling_gaps_not_filled':True,'broker_snapshot_atomic':False}
    if len(selected)>=2:
        with localcontext() as ctx:
            ctx.prec=50
            values=[Decimal(r['account']['equity']) for r in selected];base,last=values[0],values[-1]
            peak=base;drawdown=None
            for value in values:
                peak=max(peak,value)
                if peak>0:drawdown=max(drawdown or Decimal(0),(peak-value)/peak*100)
            result.update(initial_equity=str(base),final_equity=str(last),equity_change=money_text(last-base),
                equity_change_percent=money_text((last-base)/base*100) if base>0 else None,
                max_observed_drawdown_percent=money_text(drawdown) if drawdown is not None else None)
    return result
