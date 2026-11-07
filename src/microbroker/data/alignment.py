"""Read-only retrospective company sentiment/price coverage, never a backtest.

A bar start is not a completed close. Completion floor is start+one minute;
legacy corrected bar snapshots do not establish actual arrival visibility.
"""
from bisect import bisect_right
from collections import Counter, defaultdict
from datetime import timedelta
from decimal import Decimal, InvalidOperation
import uuid
from ..intelligence.research_decision import company_research_label, validate_research_decisions
from .analysis_replay import replay_analysis
from .news_replay import parsed_json, digest, MAX_BYTES, MAX_VERSIONS
from .research import begin_read, digest_records, encode
from .models import DataValidationError, utc_datetime
from .stream_models import canonical_time
from .storage.repository import StorageError

SEMANTICS = 'retrospective_snapshot_alignment_not_point_in_time_backtest'

def nanos(value):
    value = canonical_time(value)
    return int(utc_datetime(value[:19]+'Z').timestamp())*1000000000 + int(value[20:29])

def read_bars_snapshot(repository, replay_id):
    """Recheck the full frozen R3 payload before using any prices."""
    m = repository.marker; begin_read(repository)
    try:
        header = repository.connection.execute(f'SELECT request_json,item_count,dataset_sha256 FROM replay_runs WHERE id={m}',(replay_id,)).fetchone()
        if not header: raise DataValidationError('Unknown bars replay_id')
        metadata = parsed_json(header[0])
        if metadata.get('source') != 'bars' or metadata.get('timeframe') != '1Min' or metadata.get('feed') not in {'iex','sip'} or metadata.get('adjustment') != 'raw':
            raise DataValidationError('Alignment requires a raw IEX/SIP 1Min bars snapshot')
        receipt_scoped=metadata.get('format')=='R14'
        if receipt_scoped:
            from .session_prices import manifest, validate_receipt_record, SCOPE
            if (metadata.get('scope')!=SCOPE or metadata.get('manifest_sha256')!=manifest(metadata,header[1],header[2])):
                raise StorageError('Session price manifest integrity check failed')
        query = f'SELECT ordinal,record_json FROM replay_items WHERE replay_id={m} ORDER BY ordinal LIMIT {MAX_VERSIONS+1}'
        if repository.postgres:
            cursor = repository.connection.cursor(name='alignment_bars_'+uuid.uuid4().hex);cursor.execute(query,(replay_id,))
        else: cursor = repository.connection.execute(query,(replay_id,))
        records = []; size = 0; seen = set()
        while True:
            batch = cursor.fetchmany(1)
            if not batch: break
            for ordinal, raw in batch:
                size += len(raw.encode())
                if size > MAX_BYTES or len(records) >= MAX_VERSIONS: raise StorageError('Alignment bars exceed 10000 rows/64 MiB bound')
                if ordinal != len(records): raise StorageError('Alignment bars ordinal mismatch')
                record = parsed_json(raw)
                try:
                    t = canonical_time(record['timestamp']); identity=(record['symbol'], t)
                    if ('receipt' in record)!=receipt_scoped:raise StorageError('Session price scope mismatch')
                    if receipt_scoped:
                        validate_receipt_record(record,metadata)
                        identity=(record['receipt']['run_id'],record['receipt']['ordinal'])
                        if records and record['receipt']['ordinal']<=records[-1]['receipt']['ordinal']:
                            raise StorageError('Session receipt order mismatch')
                    prices = [Decimal(record[k]) for k in ('open','high','low','close')]
                    if (record['source'] != 'bars' or record['timeframe'] != '1Min' or
                            record['feed'] != metadata['feed'] or record['provider'] != 'alpaca' or
                            record['adjustment'] != 'raw' or t != record['timestamp'] or
                            t[17:29] != '00.000000000' or identity in seen or
                            any(not p.is_finite() or p <= 0 for p in prices) or
                            prices[2] > min(prices[0],prices[3]) or prices[1] < max(prices[0],prices[3]) or prices[2] > prices[1]):
                        raise StorageError('Invalid frozen alignment bar')
                except (KeyError,TypeError,ValueError,InvalidOperation,DataValidationError):
                    raise StorageError('Invalid frozen alignment bar') from None
                seen.add(identity); records.append(record)
        cursor.close()
        if len(records) != header[1] or digest_records(records) != header[2]:
            raise StorageError('Alignment bars snapshot integrity check failed')
        repository.connection.commit()
    except BaseException:
        repository.connection.rollback();raise
    return records,metadata,header[2]

def align_records(selected, bars, symbols, max_lag_minutes):
    """Latest completed bar at/before feature availability, with a strict lag cap."""
    by_symbol = defaultdict(list)
    for bar in bars:
        end = canonical_time((utc_datetime(bar['timestamp'])+timedelta(minutes=1)).isoformat())
        by_symbol[bar['symbol']].append((nanos(end),end,bar))
    for values in by_symbol.values():values.sort(key=lambda row:row[0])
    times = {s:[row[0] for row in values] for s,values in by_symbol.items()}
    rows = []
    for item in selected:
        news = item['news']; analysis = item['analysis']
        for symbol in sorted(set(news['mappings']).intersection(symbols)):
            row = {'article_id':news['article_id'],'revision_hash':news['revision_hash'],'symbol':symbol,
                   'news_available_at':news['timestamp'],'feature_available_at':None,
                   'document_label':None,'company_label':None,'company_context_status':None,
                   'price':None,'previous_bar_completed_at':None,'lag_seconds':None}
            if analysis is None:
                row['status']='pending_analysis';rows.append(row);continue
            row['feature_available_at']=analysis['timestamp']
            validate_research_decisions(analysis['result'])
            row['document_label']=analysis['result']['sentiment']['label']
            company = analysis['result'].get('entity_sentiments',{}).get(symbol)
            row['company_context_status']=company.get('status') if company else 'not_present'
            if analysis['result'].get('analysis_format') in {'R17','R18'} and company:
                row['company_research_decision'] = company['research_decision']
            if not company or company.get('status')!='scored' or not company.get('sentiment'):
                row['status']='company_sentiment_unavailable';rows.append(row);continue
            label = company_research_label(analysis['result'], company)
            if label not in {'positive','negative','neutral'}:
                row['status']='company_research_decision_withheld' if analysis['result'].get('analysis_format') in {'R17','R18'} else 'company_sentiment_unavailable';rows.append(row);continue
            row['company_label']=label
            available = nanos(analysis['timestamp']); values = by_symbol.get(symbol,[])
            position = bisect_right(times.get(symbol,[]),available)-1
            if position < 0:
                row['status']='no_completed_bar_at_or_before_feature';rows.append(row);continue
            end_ns, end, bar = values[position]
            lag = available-end_ns
            row['previous_bar_completed_at']=end
            row['lag_seconds']=lag/1000000000
            if lag > max_lag_minutes*60*1000000000:
                row['status']='previous_bar_too_old';rows.append(row);continue
            row['status']='matched'
            row['price']={'bar_start':bar['timestamp'],'completion_floor':end,'close':bar['close'],
                          'feed':bar['feed'],'adjustment':bar['adjustment']}
            rows.append(row)
    return rows

def research_alignment(repository, analysis_replay_id, bars_replay_id, at=None, max_lag_minutes=5):
    if type(max_lag_minutes) is not int or not 0 <= max_lag_minutes <= 1440:
        raise DataValidationError('max-lag-minutes must be an integer from 0 to 1440')
    selected, meta, analysis_sha = replay_analysis(repository,analysis_replay_id,at,_include_records=True)
    bars, bars_meta, bars_sha = read_bars_snapshot(repository,bars_replay_id)
    if meta['provider'] != 'alpaca':raise DataValidationError('Alignment provider must match Alpaca price snapshots')
    symbols = meta['symbols'] or sorted({s for item in selected for s in item['news']['mappings']})
    receipt_scoped=bars_meta.get('format')=='R14'
    if receipt_scoped:
        from .session_prices import align_session_records, SEMANTICS as receipt_semantics
        rows=align_session_records(selected,bars,symbols,max_lag_minutes)
    else:rows = align_records(selected,bars,symbols,max_lag_minutes)
    counts = Counter(row['status'] for row in rows); per_symbol = {}
    for symbol in symbols:
        symbol_bars = [r for r in bars if r['symbol']==symbol]
        symbol_rows = [r for r in rows if r['symbol']==symbol]
        per_symbol[symbol]={'company_rows':len(symbol_rows),'status_counts':dict(Counter(r['status'] for r in symbol_rows)),
                            'bars':len({r['timestamp'] for r in symbol_bars}),'first_bar_start':min((r['timestamp'] for r in symbol_bars),default=None),
                            'last_bar_start':max((r['timestamp'] for r in symbol_bars),default=None),
                            'first_feature_available_at':min((r['feature_available_at'] for r in symbol_rows if r['feature_available_at']),default=None),
                            'last_feature_available_at':max((r['feature_available_at'] for r in symbol_rows if r['feature_available_at']),default=None)}
    gap_intervals = []
    for symbol in symbols:
        ordered = sorted({r['timestamp'] for r in bars if r['symbol']==symbol})
        for a,b in zip(ordered,ordered[1:]):
            if a[:10]==b[:10] and nanos(b)-nanos(a)>60000000000:
                gap_intervals.append({'symbol':symbol,'previous_bar_start':a,'next_bar_start':b,
                                      'absent_minute_slots':(nanos(b)-nanos(a))//60000000000-1})
    request={'analysis_replay_id':analysis_replay_id,'bars_replay_id':bars_replay_id,
             'at':canonical_time(at) if at else meta['as_of'],'max_lag_minutes':max_lag_minutes,
             'analysis_dataset_sha256':analysis_sha,'bars_dataset_sha256':bars_sha,
             'symbols':symbols,'feed':bars_meta['feed'],'semantics':receipt_semantics if receipt_scoped else SEMANTICS}
    if receipt_scoped:
        request['price_scope']=bars_meta['scope'];request['stream_run_id']=bars_meta['stream_run_id']
        request['price_manifest_sha256']=bars_meta['manifest_sha256']
    return {'status':'completed','quality_status':'warning','analysis_replay_id':analysis_replay_id,
            'bars_replay_id':bars_replay_id,'at':request['at'],'feed':bars_meta['feed'],'timeframe':'1Min','available_articles':len(selected),
            'company_rows':len(rows),'matched_rows':counts['matched'],'status_counts':dict(counts),
            'per_symbol':per_symbol,'candidate_intraday_bar_gap_intervals':len(gap_intervals),
            'bar_gap_samples':gap_intervals[:20],
            'gap_interpretation':'candidate_only_no_exchange_calendar_or_missing_trade_inference',
            'max_lag_minutes':max_lag_minutes,'alignment_sha256':digest({'request':request,'rows':rows}),
            'analysis_dataset_sha256':analysis_sha,'bars_dataset_sha256':bars_sha,
            'replay_semantics':request['semantics'],'frozen_news_as_of':meta['news_as_of'],
            **({'price_scope':bars_meta['scope'],'stream_run_id':bars_meta['stream_run_id'],
                'price_receipts':len(bars),'recorded_receipt_cutoff_applied':True} if receipt_scoped else {}),
            'warning_codes':(['news_receipt_time_not_commit_time','analysis_time_not_commit_time',
                'stream_receipt_time_not_commit_time','receipt_digest_not_authenticity'] if receipt_scoped else
                ['legacy_receipt_time_not_commit_time','legacy_analysis_time_not_commit_time',
                             'current_bar_snapshot_not_point_in_time','bar_completion_floor_not_actual_receipt',
                             'legacy_bar_request_metadata_not_sealed']),
            'database_modified':False,'inference_performed':False,'network_required':False,
            'point_in_time_validated':False,'returns_calculated':False,'strategy_implemented':False,
            'usable_for_trading':False,'samples':rows[:20],'sample_limit':20}

def alignment_fixture(repository):
    """Original controlled news/analysis/bar times; no downloaded model."""
    from .news import normalize_news
    from .news_replay import create_news_replay, NewsReplayRequest
    from .analysis_replay import create_analysis_replay
    from .research import create_replay, ResearchRequest
    from ..intelligence.analysis import compute_analysis
    from ..intelligence.sentiment import RulesEngine
    from ..intelligence.policy import analyzer_metadata
    n=normalize_news({'id':9001,'headline':'Microsoft beats expectations.', 'summary':'','content':'',
        'created_at':'2026-01-05T10:00:00Z','updated_at':'2026-01-05T10:00:00Z',
        'symbols':['MSFT'],'source':'original-synthetic','url':'https://example.com/fixture'})
    data=n['data'];first='2026-01-06T10:00:00Z';key=digest(analyzer_metadata(RulesEngine()));m=repository.marker
    repository.connection.execute(f"INSERT INTO news_versions VALUES ({','.join([m]*7)})",
        ('alpaca',9001,n['revision_hash'],data['created_at'],data['updated_at'],first,encode(data)))
    for symbol,method in n['mappings'].items():
        repository.connection.execute(f"INSERT INTO news_symbols VALUES ({','.join([m]*5)})",('alpaca',9001,n['revision_hash'],symbol,method))
    result=compute_analysis({'data':data,'first_seen':first,'mappings':n['mappings']},RulesEngine())
    repository.connection.execute(f"INSERT INTO news_analyses VALUES ({','.join([m]*9)})",
        ('alpaca',9001,n['revision_hash'],key,'rules',result['input_sha256'],'2026-01-07T10:01:00Z',encode(result),digest(result)))
    for minute in ('09:59','10:00','10:01'):
        repository.connection.execute(f"INSERT INTO bars VALUES ({','.join([m]*13)})",
            ('alpaca','MSFT','2026-01-07T'+minute+':00Z','1Min','iex','raw','100','102','99','101',100,5,None))
    repository.connection.commit()
    news=create_news_replay(repository,NewsReplayRequest('2026-01-08T00:00:00Z',('MSFT',)))
    analysis=create_analysis_replay(repository,news['replay_id'],'2026-01-08T00:00:00Z',key,'rules')
    bars=create_replay(repository,ResearchRequest(symbols=('MSFT',)))
    return analysis['replay_id'],bars['replay_id']

def verify_alignment():
    from copy import deepcopy
    from .storage.repository import Repository
    repository=Repository('sqlite:///:memory:');checks=[]
    def require(ok,name):
        if not ok:raise StorageError('Alignment verification failed: '+name)
        checks.append(name)
    try:
        analysis_id,bars_id=alignment_fixture(repository)
        report=research_alignment(repository,analysis_id,bars_id)
        require(report['matched_rows']==1 and report['samples'][0]['price']['bar_start']=='2026-01-07T10:00:00.000000000Z',
                'completed_minute_boundary_inclusive')
        selected,_,_=replay_analysis(repository,analysis_id,_include_records=True)
        bars,_,_=read_bars_snapshot(repository,bars_id)
        prior=deepcopy(selected);prior[0]['analysis']['timestamp']='2026-01-07T10:00:59.999999999Z'
        require(align_records(prior,bars,['MSFT'],5)[0]['price']['bar_start']=='2026-01-07T09:59:00.000000000Z',
                'unfinished_minute_close_excluded')
        older=deepcopy(selected);older[0]['analysis']['timestamp']='2026-01-07T10:07:00.000000000Z'
        require(align_records(older,bars,['MSFT'],5)[0]['status']=='matched','lag_cap_boundary_inclusive')
        older[0]['analysis']['timestamp']='2026-01-07T10:07:00.000000001Z'
        require(align_records(older,bars,['MSFT'],5)[0]['status']=='previous_bar_too_old','stale_bar_not_carried_forward')
        pending=research_alignment(repository,analysis_id,bars_id,'2026-01-06T12:00:00Z')
        require(pending['status_counts']=={'pending_analysis':1},'future_company_sentiment_excluded')
        no_company=deepcopy(selected);no_company[0]['analysis']['result']['entity_sentiments']={}
        require(align_records(no_company,bars,['MSFT'],5)[0]['status']=='company_sentiment_unavailable','document_label_not_company_fallback')
        require(align_records(selected,[{**b,'symbol':'AAPL'} for b in bars],['MSFT'],5)[0]['price'] is None,'symbol_price_isolation')
        require(research_alignment(repository,analysis_id,bars_id)==report,'repeat_alignment_digest')
        repository.connection.execute('DELETE FROM bars');repository.connection.execute('DELETE FROM news_analyses');repository.connection.commit()
        require(research_alignment(repository,analysis_id,bars_id)==report,'frozen_source_change_isolation')
        repository.connection.execute("UPDATE replay_items SET record_json='{}' WHERE replay_id=?",(bars_id,));repository.connection.commit()
        rejected=False
        try:research_alignment(repository,analysis_id,bars_id)
        except StorageError:rejected=True
        require(rejected,'tampered_price_snapshot_rejected')
        return {'status':'passed','checks':checks,'fixture_source':'original_synthetic_controlled_times',
                'model_required':False,'network_required':False,'user_database_modified':False,
                'point_in_time_validated':False,'returns_calculated':False,'strategy_implemented':False}
    finally:repository.close()
