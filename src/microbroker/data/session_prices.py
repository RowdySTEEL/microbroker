"""Frozen receipts from one stream run, independent of shared current bars."""
from datetime import datetime, timezone
import uuid
from .models import DataValidationError, validate_symbols
from .stream_models import StreamRequest, normalize_event, canonical_time
from .research import begin_read, encode, digest_records, price
from .news_replay import digest, parsed_json, MAX_BYTES, MAX_VERSIONS
from .storage.repository import StorageError

FORMAT='R14'
SCOPE='same_stream_run_receipts'
SEMANTICS='recorded_stream_receipt_alignment_not_exact_commit_visibility'

def receipt_record(payload, provider, feed, run_id, ordinal, received_at, symbols):
    event=normalize_event(payload,StreamRequest(tuple(symbols),feed,('bars','updatedBars')))
    if event.bar is None or event.provider!=provider or type(ordinal) is not int or ordinal<1:
        raise StorageError('Invalid session bar receipt')
    b=event.bar
    return {'source':'bars','symbol':event.symbol,'timestamp':event.timestamp,'timeframe':'1Min',
        'feed':feed,'provider':provider,'adjustment':'raw','open':price(b.open),'high':price(b.high),
        'low':price(b.low),'close':price(b.close),'volume':b.volume,'trade_count':b.trade_count,
        'vwap':price(b.vwap) if b.vwap is not None else None,
        'receipt':{'run_id':run_id,'ordinal':ordinal,'received_at':canonical_time(received_at),
            'event_type':event.event_type,'event_key':event.event_key,'payload':payload}}

def manifest(metadata,count,sha):
    return digest({'request':{k:v for k,v in metadata.items() if k!='manifest_sha256'},
        'item_count':count,'dataset_sha256':sha})

def validate_receipt_record(record,metadata):
    try:
        r=record['receipt'];received=r['received_at']
        if (r['run_id']!=metadata['stream_run_id'] or not metadata['receipt_start']<=received<=metadata['receipt_end'] or
            record!=receipt_record(r['payload'],'alpaca',metadata['feed'],r['run_id'],r['ordinal'],received,metadata['symbols'])):
            raise StorageError('Invalid frozen session bar receipt')
    except (KeyError,TypeError,ValueError,DataValidationError):
        raise StorageError('Invalid frozen session bar receipt') from None

def create_session_prices(repository,run_id,symbols,feed,start,end):
    symbols=tuple(sorted(validate_symbols(symbols)));start=canonical_time(start);end=canonical_time(end)
    if start>end or feed not in {'iex','sip'} or not isinstance(run_id,str) or not run_id:
        raise DataValidationError('Invalid session price scope')
    metadata={'source':'bars','symbols':list(symbols),'feed':feed,'timeframe':'1Min','adjustment':'raw',
        'format':FORMAT,'scope':SCOPE,'stream_run_id':run_id,'receipt_start':start,'receipt_end':end,
        'order':'stream_receipt_ordinal','replay_semantics':SEMANTICS}
    m=repository.marker;begin_read(repository)
    try:
        header=repository.connection.execute(f'SELECT request_json FROM stream_sessions WHERE id={m}',(run_id,)).fetchone()
        if not header:raise StorageError('Session stream audit missing')
        request=parsed_json(header[0])
        if request['feed']!=feed or set(request['symbols'])!=set(symbols):raise StorageError('Session stream scope mismatch')
        query=f'SELECT ordinal,provider,feed,payload_json,received_at FROM stream_bar_receipts WHERE run_id={m} ORDER BY ordinal LIMIT {MAX_VERSIONS+1}'
        if repository.postgres:
            cursor=repository.connection.cursor(name='session_prices_'+uuid.uuid4().hex);cursor.execute(query,(run_id,))
        else:cursor=repository.connection.execute(query,(run_id,))
        records=[];size=0;rows=0
        try:
            while True:
                batch=cursor.fetchmany(1)
                if not batch:break
                for ordinal,provider,row_feed,raw,received in batch:
                    rows+=1;size+=len(raw.encode())
                    if rows>MAX_VERSIONS or size>MAX_BYTES:raise StorageError('Session prices exceed 10000 receipts/64 MiB bound')
                    if provider!='alpaca' or row_feed!=feed:raise StorageError('Session receipt provider/feed mismatch')
                    received=canonical_time(received)
                    if not start<=received<=end:continue
                    record=receipt_record(parsed_json(raw),provider,feed,run_id,ordinal,received,symbols)
                    size+=len(encode(record).encode())
                    if size>MAX_BYTES:raise StorageError('Session prices exceed 64 MiB bound')
                    records.append(record)
        finally:cursor.close()
        repository.connection.commit()
    except BaseException:repository.connection.rollback();raise
    unique=len({(r['symbol'],r['timestamp']) for r in records})
    if not records:return {'status':'empty','item_count':0,'unique_minute_bars':0,'scope':SCOPE}
    sha=digest_records(records);metadata['manifest_sha256']=manifest(metadata,len(records),sha)
    replay_id=str(uuid.uuid4());quality={'status':'warning','replay_semantics':SEMANTICS,
        'scope':SCOPE,'receipt_count':len(records),'unique_minute_bars':unique,
        'point_in_time_validated':False,'receipt_times_are_commit_times':False}
    try:
        repository.connection.execute(f"INSERT INTO replay_runs VALUES ({','.join([m]*6)})",
            (replay_id,datetime.now(timezone.utc).isoformat(),encode(metadata),len(records),sha,encode(quality)))
        for i,record in enumerate(records):repository.connection.execute(f"INSERT INTO replay_items VALUES ({','.join([m]*3)})",(replay_id,i,encode(record)))
        repository.connection.commit()
    except BaseException:repository.connection.rollback();raise
    return {'status':'ready','replay_id':replay_id,'item_count':len(records),
        'unique_minute_bars':unique,'dataset_sha256':sha,'scope':SCOPE}

def align_session_records(selected,bars,symbols,max_lag_minutes):
    from .alignment import align_records
    rows=[]
    for item in selected:
        analysis=item['analysis'];available=analysis['timestamp'] if analysis else None
        current={}
        for bar in bars:
            r=bar['receipt']
            if available is None or r['received_at']>available:continue
            key=(bar['symbol'],bar['timestamp']);prior=current.get(key)
            if prior and prior['receipt']['event_type']=='u' and r['event_type']=='b':continue
            current[key]=bar
        for row in align_records([item],list(current.values()),symbols,max_lag_minutes):
            if row['status']=='no_completed_bar_at_or_before_feature':row['status']='no_session_bar_available_at_feature'
            if row['price'] is not None:
                bar=current[(row['symbol'],row['price']['bar_start'])]
                row['price']['provenance']={'scope':SCOPE,**{k:v for k,v in bar['receipt'].items() if k!='payload'}}
            rows.append(row)
    return rows
