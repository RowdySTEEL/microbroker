"""Bounded quality checks and immutable research replay; no strategy or orders."""
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
import hashlib
import json
import uuid
from .models import DataValidationError, utc_datetime, validate_symbols
from .stream_models import canonical_time, normalize_event, StreamRequest
from .storage.repository import StorageError

MAX_ROWS = 200000

def encode(value): return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=True)

def price(value):
    n = Decimal(str(value))
    if not n.is_finite() or n < 0: raise ValueError
    value = format(n,'f')
    if '.' in value: value = value.rstrip('0').rstrip('.')
    return '0' if n == 0 else value

def count(value):
    n = Decimal(str(value))
    if isinstance(value,bool) or not n.is_finite() or n < 0 or n != n.to_integral_value(): raise ValueError
    return int(n)

def begin_read(repository):
    if repository.postgres:
        if repository.connection.info.transaction_status != 0:
            raise StorageError('Research reads require a clean transaction')
        # psycopg starts the transaction; SET applies before the first data query.
        repository.connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
    else:
        if repository.connection.in_transaction:
            raise StorageError('Research reads require a clean transaction')
        repository.connection.execute('BEGIN')

@dataclass(frozen=True)
class ResearchRequest:
    source: str = 'bars'
    symbols: tuple = ()
    feed: str = 'iex'
    timeframe: str = '1Min'
    start: datetime | None = None
    end: datetime | None = None
    order: str = 'received'

    def __post_init__(self):
        if self.symbols: object.__setattr__(self,'symbols',validate_symbols(self.symbols))
        if self.source not in {'bars','events'} or self.feed not in {'iex','sip','test'} or self.order not in {'received','event'}:
            raise DataValidationError('Invalid research source/feed/order')
        if self.timeframe not in {'1Min','1Day'}: raise DataValidationError('R3 research supports 1Min or 1Day')
        for dt in (self.start,self.end):
            if dt is not None and (dt.tzinfo is None or dt.utcoffset() is None):
                raise DataValidationError('Research boundaries must include a timezone')
        if self.start and self.end and self.start >= self.end: raise DataValidationError('start must precede end')

    def metadata(self):
        return {'source':self.source,'symbols':self.symbols,'feed':self.feed,'timeframe':self.timeframe,
                'start':self.start.isoformat() if self.start else None,'end':self.end.isoformat() if self.end else None,
                'order':self.order,'interval':'start_inclusive_end_exclusive','adjustment':'raw'}

def read_dataset(repository, request):
    """Read rows and gap audit in one database snapshot, with a hard memory bound."""
    m = repository.marker
    provider = 'alpaca-test' if request.feed == 'test' else 'alpaca'
    columns = ('symbol,timestamp,open,high,low,close,volume,trade_count,vwap' if request.source == 'bars'
               else 'symbol,timestamp,event_type,payload_json,event_key,received_at')
    table = 'bars' if request.source == 'bars' else 'stream_events'
    query = f'SELECT {columns} FROM {table} WHERE provider={m} AND feed={m}'
    params = [provider,request.feed]
    if request.source == 'bars':
        query += f' AND timeframe={m} AND adjustment={m}'
        params.extend([request.timeframe,'raw'])
    if request.symbols:
        query += f" AND symbol IN ({','.join([m]*len(request.symbols))})"
        params.extend(request.symbols)
    begin_read(repository)
    try:
        if repository.postgres:
            cursor=repository.connection.cursor(name='research_'+uuid.uuid4().hex)
            cursor.execute(query,params)
        else:
            cursor=repository.connection.execute(query,params)
        rows=[]
        try:
            while True:
                batch=cursor.fetchmany(1000)
                if not batch:break
                for row in batch:
                    try:
                        dt=utc_datetime(row[1])
                        if (request.start and dt<request.start) or (request.end and dt>=request.end):continue
                    except DataValidationError:
                        pass  # Invalid timestamps are retained so quality checks expose them.
                    rows.append(row)
                    if len(rows)>MAX_ROWS:raise StorageError('R3 dataset exceeds 200000 rows; narrow symbols/time interval')
        finally:
            cursor.close()
        gaps = repository.connection.execute(f'SELECT id,symbols_json,start_at,end_at,status FROM stream_gaps WHERE feed={m} LIMIT {MAX_ROWS+1}',(request.feed,)).fetchmany(MAX_ROWS+1)
        if len(gaps)>MAX_ROWS: raise StorageError('R3 gap audit exceeds 200000 rows')
        repository.connection.commit()
        return rows,gaps
    except BaseException:
        repository.connection.rollback()
        raise

def inspect_dataset(repository,request):
    rows,gaps = read_dataset(repository,request)
    records=[];issues=[];counts=Counter();valid_times=defaultdict(set);by_symbol=Counter();seen_bars=set()
    def issue(code,symbol,timestamp,severity='error',**detail):
        counts[severity+':'+code] += 1
        if len(issues)<100: issues.append({'code':code,'severity':severity,'symbol':symbol,'timestamp':str(timestamp),**detail})
    for row in rows:
        symbol, timestamp = row[:2]
        try:
            timestamp = canonical_time(timestamp)
            dt = utc_datetime(timestamp)
        except DataValidationError:
            issue('invalid_timestamp',symbol,timestamp);continue
        if (request.start and dt<request.start) or (request.end and dt>=request.end):continue
        by_symbol[symbol]+=1
        try:
            if request.source == 'bars':
                o,h,l,c = [price(v) for v in row[2:6]]
                if Decimal(l)>min(Decimal(o),Decimal(c)) or Decimal(h)<max(Decimal(o),Decimal(c)) or Decimal(l)>Decimal(h):raise ValueError
                if request.timeframe == '1Min' and timestamp[17:29] != '00.000000000':raise ValueError
                if (symbol,timestamp) in seen_bars:raise ValueError
                seen_bars.add((symbol,timestamp))
                record={'symbol':symbol,'timestamp':timestamp,'timeframe':request.timeframe,'feed':request.feed,
                        'provider':'alpaca-test' if request.feed=='test' else 'alpaca','adjustment':'raw',
                        'open':o,'high':h,'low':l,'close':c,'volume':count(row[6]),'trade_count':count(row[7]),
                        'vwap':price(row[8]) if row[8] is not None else None,'source':'bars'}
                valid_times[symbol].add(dt)
            else:
                kind,payload,key,received=row[2:]
                payload=json.loads(payload)
                channels=('bars','updatedBars','trades','quotes')
                event=normalize_event(payload,StreamRequest((symbol,),request.feed,channels))
                if event.event_key!=key or event.event_type!=kind or event.timestamp!=timestamp:raise ValueError
                received=canonical_time(received)
                record={'source':'events','symbol':symbol,'timestamp':timestamp,'received_at':received,
                        'event_type':kind,'event_key':key,'feed':request.feed,'provider':event.provider,
                        'payload':json.loads(event.payload_json)}
                if kind=='q' and Decimal(payload['bp'])>Decimal(payload['ap']):issue('crossed_quote',symbol,timestamp,'warning')
            records.append(record)
        except (ValueError,TypeError,KeyError,InvalidOperation,DataValidationError):
            issue('invalid_bar' if request.source=='bars' else 'invalid_event',symbol,timestamp)
    if request.source == 'bars' and request.timeframe=='1Min':
        for symbol,times in valid_times.items():
            ordered=sorted(times)
            for a,b in zip(ordered,ordered[1:]):
                # No calendar assumption: only report intraday candidate intervals.
                if a.date()==b.date() and b-a>timedelta(minutes=1):
                    issue('candidate_minute_gap',symbol,a.isoformat(),'warning',next_timestamp=b.isoformat(),
                          absent_minutes=int((b-a).total_seconds()//60)-1)
    relevant=[]
    for gap_id,symbols,start,end,status in gaps:
        symbols=json.loads(symbols)
        if request.symbols and not set(request.symbols).intersection(symbols):continue
        a=utc_datetime(start);b=utc_datetime(end) if end else None
        if (request.end and a>=request.end) or (request.start and b and b<request.start):continue
        relevant.append({'gap_id':gap_id,'status':status,'start':start,'end':end})
        if status in {'open','pending','failed'}:issue('unresolved_stream_gap',None,start,'warning',gap_id=gap_id,status=status)
        if request.source=='events':issue('unrecovered_trade_quote_interval',None,start,'warning',gap_id=gap_id,bar_recovery_status=status)
    if not records:issue('empty_dataset',None,'','warning')
    missing=[s for s in request.symbols if not any(r['symbol']==s for r in records)]
    for symbol in missing:issue('missing_symbol',symbol,'','warning')
    if request.source=='events':
        by_receipt=sorted(records,key=lambda r:(r['received_at'],r['event_key']))
        last={}
        for r in by_receipt:
            key=(r['symbol'],r['event_type'])
            if key in last and r['timestamp']<last[key]:issue('out_of_order_event',r['symbol'],r['timestamp'],'warning')
            last[key]=max(last.get(key,r['timestamp']),r['timestamp'])
    records.sort(key=lambda r:((r['received_at'] if request.source=='events' and request.order=='received' else r['timestamp']),r['symbol'],r.get('event_key','')))
    report={'status':'failed' if any(k.startswith('error:') for k in counts) else ('warning' if counts else 'passed'),
            'request':request.metadata(),'valid_records':len(records),'rows_in_interval':sum(by_symbol.values()),
            'received_by_symbol':dict(by_symbol),'missing_symbols':missing,'issue_counts':dict(counts),
            'issues':issues,'issue_sample_limit':100,'stream_gaps':relevant,
            'minute_gap_interpretation':'candidates_only_no_exchange_calendar_or_no_trade_inference',
            'replay_semantics':'current_bar_snapshot_not_point_in_time' if request.source=='bars' else
              ('first_stored_receipt_order' if request.order=='received' else 'event_time_order_not_causal'),
            'trades_quotes_gap_recovery':False}
    return records,report

def digest_records(records):
    h=hashlib.sha256()
    for record in records:h.update((encode(record)+'\n').encode())
    return h.hexdigest()

def create_replay(repository,request):
    records,report=inspect_dataset(repository,request)
    if report['status']=='failed':raise DataValidationError('Dataset has invalid records; inspect data-quality before creating a replay')
    if not records:raise DataValidationError('Cannot create an empty replay')
    replay_id=str(uuid.uuid4());digest=digest_records(records);m=repository.marker
    try:
        repository.connection.execute(f"INSERT INTO replay_runs VALUES ({','.join([m]*6)})",
            (replay_id,datetime.now(timezone.utc).isoformat(),encode(request.metadata()),len(records),digest,encode(report)))
        for i,record in enumerate(records):
            repository.connection.execute(f"INSERT INTO replay_items VALUES ({','.join([m]*3)})",(replay_id,i,encode(record)))
        repository.connection.commit()
    except BaseException:
        repository.connection.rollback();raise
    return {'replay_id':replay_id,'status':'ready','item_count':len(records),'dataset_sha256':digest,'quality_status':report['status'],
            'replay_semantics':report['replay_semantics'],
            'quality':{'status':report['status'],'issue_counts':report['issue_counts'],'missing_symbols':report['missing_symbols']}}

def replay(repository,replay_id,on_record=None):
    m=repository.marker
    begin_read(repository)
    try:
        info=repository.connection.execute(f'SELECT item_count,dataset_sha256 FROM replay_runs WHERE id={m}',(replay_id,)).fetchone()
        if not info:raise DataValidationError('Unknown replay_id')
        rows=repository.connection.execute(f'SELECT ordinal,record_json FROM replay_items WHERE replay_id={m} ORDER BY ordinal LIMIT {MAX_ROWS+1}',(replay_id,)).fetchmany(MAX_ROWS+1)
        if len(rows)!=info[0] or len(rows)>MAX_ROWS:raise StorageError('Replay item count mismatch')
        records=[]
        for i,(ordinal,raw) in enumerate(rows):
            if i!=ordinal:raise StorageError('Replay ordinal mismatch')
            records.append(json.loads(raw))
        digest=digest_records(records)
        if digest!=info[1]:raise StorageError('Replay digest mismatch')
        repository.connection.commit()
    except BaseException:
        repository.connection.rollback();raise
    # Validate the complete snapshot before invoking any consumer.
    for record in records:
        if on_record is not None:on_record(record)
    return {'replay_id':replay_id,'status':'passed','item_count':len(records),'dataset_sha256':digest,
            'first_timestamp':records[0]['timestamp'] if records else None,'last_timestamp':records[-1]['timestamp'] if records else None}

def verify_research(repository,symbols=('AAPL','MSFT'),feed='iex'):
    results=[]
    for source in ('bars','events'):
        request=ResearchRequest(source=source,symbols=tuple(symbols),feed=feed)
        snapshot=create_replay(repository,request)
        first=replay(repository,snapshot['replay_id'])
        second=replay(repository,snapshot['replay_id'])
        if first!=second:raise StorageError('Repeated replay results differ')
        results.append({'source':source,**snapshot,'repeat_replay':'passed'})
    return {'status':'passed','datasets':results,'market_tables_modified':False,
            'replay_snapshots_created':len(results),'trades_quotes_gap_recovery':False}
