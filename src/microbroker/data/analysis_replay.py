"""Sealed revision-specific analyses, gated by recorded completion timestamps.

Legacy analyzed_at is written before commit. No exact commit visibility or
historically available trading signal is claimed. No inference or API calls.
"""
from datetime import datetime, timezone
import re
import uuid
from .news_replay import (replay_news, state_at, validate_record, parsed_json,
                          digest, MAX_BYTES, MAX_VERSIONS)
from .research import begin_read, encode, digest_records
from .stream_models import canonical_time
from .models import DataValidationError
from .storage.repository import StorageError
from ..intelligence.research_decision import validate_research_decisions
from ..intelligence.policy import prepare_text
from ..intelligence.sentiment import validate_sentiment, AnalysisError

SEMANTICS = 'recorded_analysis_completion_not_exact_commit_visibility'

def validate_analysis(record):
    try:
        news = validate_record(record['news'])
        analysis = record['analysis']
    except (KeyError, TypeError):
        raise StorageError('Invalid stored analysis replay record') from None
    if analysis is None: return record
    try:
        validate_sentiment(analysis['result']['sentiment'])
        validate_research_decisions(analysis['result'])
        text, fields, basis, _ = prepare_text(news['data'])
        if analysis['result'].get('analysis_format')=='R18':
            from ..intelligence.policy import company_contexts
            from ..intelligence.unit_evidence import verify_units_against_context
            contexts=company_contexts(fields,news['mappings'])
            if set(contexts)!=set(analysis['result']['entity_sentiments']):
                raise StorageError('Stored unit evidence company set mismatch')
            for symbol,context in contexts.items():
                verify_units_against_context(analysis['result']['entity_sentiments'][symbol],context)
        expected = digest({'text': text, 'basis': basis, 'mappings': news['mappings']})
        if (analysis['input_sha256'] != expected or analysis['result']['input_sha256'] != expected or
                digest(analysis['result']) != analysis['result_sha256'] or
                analysis['analyzed_at'] != canonical_time(analysis['analyzed_at']) or
                analysis['timestamp'] != max(news['timestamp'], analysis['analyzed_at']) or
                not re.fullmatch('[0-9a-f]{64}', analysis['analyzer_key']) or
                analysis['engine'] not in {'finbert', 'rules'}):
            raise StorageError('Analysis replay integrity check failed')
    except (KeyError, TypeError, ValueError, DataValidationError, AnalysisError):
        raise StorageError('Invalid stored analysis replay record') from None
    return record

def create_analysis_replay(repository, news_replay_id, as_of, analyzer_key, engine='finbert'):
    cutoff = canonical_time(as_of)
    if not re.fullmatch('[0-9a-f]{64}', analyzer_key) or engine not in {'finbert', 'rules'}:
        raise DataValidationError('Specify a valid explicit analyzer key and engine')
    news_request, news_records, news_sha = replay_news(repository, news_replay_id, _include_records=True)
    if cutoff < news_request.as_of:
        raise DataValidationError('Analysis cutoff must be at or after the frozen news cutoff')
    m = repository.marker
    records = []; size = 0
    begin_read(repository)
    try:
        audit = repository.connection.execute(
            f"SELECT request_json FROM news_analysis_runs WHERE provider={m} AND analyzer_key={m} AND engine={m} AND status IN ('completed','empty') ORDER BY finished_at LIMIT 1",
            (news_request.provider, analyzer_key, engine)).fetchone()
        if audit and len(audit[0].encode()) > MAX_BYTES:
            raise StorageError('Analyzer metadata exceeds snapshot bound')
        analyzer = parsed_json(audit[0]).get('analyzer') if audit else None
        if audit and digest(analyzer) != analyzer_key:
            raise StorageError('Analyzer metadata fingerprint mismatch')
        for news in news_records:
            row = repository.connection.execute(
                f'SELECT engine,input_sha256,analyzed_at,result_json,result_sha256 FROM news_analyses WHERE provider={m} AND article_id={m} AND revision_hash={m} AND analyzer_key={m}',
                (news['provider'], news['article_id'], news['revision_hash'], analyzer_key)).fetchone()
            analysis = None
            if row is not None:
                analyzed = canonical_time(row[2])
                if analyzed <= cutoff:
                    if len(row[3].encode()) > MAX_BYTES:
                        raise StorageError('Analysis result exceeds snapshot bound')
                    if row[0] != engine: raise StorageError('Analysis engine does not match explicit selection')
                    analysis = {'engine': row[0], 'analyzer_key': analyzer_key, 'input_sha256': row[1],
                                'analyzed_at': analyzed, 'timestamp': max(news['timestamp'], analyzed),
                                'result': parsed_json(row[3]), 'result_sha256': row[4]}
            record = validate_analysis({'news': news, 'analysis': analysis})
            size += len(encode(record).encode())
            if size > MAX_BYTES: raise StorageError('Analysis replay exceeds 64 MiB snapshot bound')
            records.append(record)
        repository.connection.commit()
    except BaseException:
        repository.connection.rollback(); raise
    metadata = {'source': 'news_analysis', 'format': 'R10', 'news_replay_id': news_replay_id,
                'news_dataset_sha256': news_sha, 'news_as_of': news_request.as_of,
                'provider': news_request.provider, 'symbols': list(news_request.symbols),
                'as_of': cutoff, 'analyzer_key': analyzer_key, 'engine': engine,
                'replay_semantics': SEMANTICS, 'analyzer_metadata': analyzer}
    sha = digest_records(records); replay_id = str(uuid.uuid4())
    report = {'manifest_sha256': digest({'request': metadata, 'item_count': len(records), 'dataset_sha256': sha}),
              'status': 'warning', 'warning_codes': ['legacy_receipt_time_not_commit_time', 'legacy_analysis_time_not_commit_time']}
    try:
        repository.connection.execute(f"INSERT INTO replay_runs VALUES ({','.join([m]*6)})",
            (replay_id, datetime.now(timezone.utc).isoformat(), encode(metadata), len(records), sha, encode(report)))
        for ordinal, record in enumerate(records):
            repository.connection.execute(f"INSERT INTO replay_items VALUES ({','.join([m]*3)})", (replay_id, ordinal, encode(record)))
        repository.connection.commit()
    except BaseException:
        repository.connection.rollback(); raise
    result = replay_analysis(repository, replay_id)
    return {**result, 'status': 'ready', 'replay_snapshots_created': 1}

def replay_analysis(repository, replay_id, at=None, *, _include_records=False, _include_history=False):
    m = repository.marker
    begin_read(repository)
    try:
        header = repository.connection.execute(f'SELECT request_json,item_count,dataset_sha256,report_json FROM replay_runs WHERE id={m}', (replay_id,)).fetchone()
        if not header: raise DataValidationError('Unknown analysis replay_id')
        meta = parsed_json(header[0]); report = parsed_json(header[3])
        if meta.get('source') != 'news_analysis' or meta.get('format') != 'R10':
            raise DataValidationError('replay-analysis requires an R10 analysis snapshot')
        if report.get('manifest_sha256') != digest({'request': meta, 'item_count': header[1], 'dataset_sha256': header[2]}):
            raise StorageError('Analysis replay manifest integrity check failed')
        if meta.get('analyzer_metadata') is not None and digest(meta['analyzer_metadata']) != meta['analyzer_key']:
            raise StorageError('Frozen analyzer metadata fingerprint mismatch')
        when = canonical_time(at) if at else meta['as_of']
        if when > meta['as_of']: raise DataValidationError('Replay time exceeds frozen analysis cutoff')
        query = f'SELECT ordinal,record_json FROM replay_items WHERE replay_id={m} ORDER BY ordinal LIMIT {MAX_VERSIONS+1}'
        if repository.postgres:
            cursor = repository.connection.cursor(name='analysis_frozen_'+uuid.uuid4().hex); cursor.execute(query, (replay_id,))
        else: cursor = repository.connection.execute(query, (replay_id,))
        records = []; size = 0
        while True:
            batch = cursor.fetchmany(1)
            if not batch: break
            for ordinal, raw in batch:
                size += len(raw.encode())
                if size > MAX_BYTES or len(records) >= MAX_VERSIONS: raise StorageError('Analysis replay snapshot bound exceeded')
                if ordinal != len(records): raise StorageError('Analysis replay ordinal mismatch')
                record = validate_analysis(parsed_json(raw)); news = record['news']; analysis = record['analysis']
                if news['timestamp'] > meta['news_as_of'] or news['provider'] != meta['provider']:
                    raise StorageError('News outside frozen analysis scope')
                if analysis and (analysis['timestamp'] > meta['as_of'] or analysis['analyzer_key'] != meta['analyzer_key'] or analysis['engine'] != meta['engine']):
                    raise StorageError('Analysis outside frozen selection')
                records.append(record)
        cursor.close()
        if len(records) != header[1] or digest_records(records) != header[2]:
            raise StorageError('Analysis replay dataset integrity check failed')
        if digest_records([r['news'] for r in records]) != meta['news_dataset_sha256']:
            raise StorageError('Analysis replay frozen news digest mismatch')
        selected_news = state_at([r['news'] for r in records], when, meta['symbols'])
        index = {(r['news']['article_id'],r['news']['revision_hash']):r['analysis'] for r in records}
        selected = []
        for news in selected_news:
            analysis = index[(news['article_id'],news['revision_hash'])]
            selected.append({'news': news, 'analysis': analysis if analysis and analysis['timestamp'] <= when else None})
        repository.connection.commit()
    except BaseException:
        repository.connection.rollback(); raise
    if _include_history: return records, meta, header[2]
    if _include_records: return selected, meta, header[2]
    return {'status': 'passed', 'replay_id': replay_id, 'item_count': len(records),
            'available_articles': len(selected), 'available_analyses': sum(r['analysis'] is not None for r in selected),
            'pending_analyses': sum(r['analysis'] is None for r in selected), 'at': when,
            'frozen_as_of': meta['as_of'], 'frozen_news_as_of': meta['news_as_of'],
            'analyzer_key': meta['analyzer_key'], 'engine': meta['engine'],
            'analyzer_metadata_available': meta['analyzer_metadata'] is not None,
            'dataset_sha256': header[2], 'state_sha256': digest_records(selected),
            'replay_semantics': SEMANTICS, 'exact_live_point_in_time_validated': False,
            'source_tables_modified': False, 'inference_performed': False,
            'model_accuracy_validated': False, 'usable_for_trading': False,
            'samples': [{'id': r['news']['article_id'], 'revision_hash': r['news']['revision_hash'],
                         'analysis_available': r['analysis'] is not None,
                         'analyzed_at': r['analysis']['analyzed_at'] if r['analysis'] else None,
                         'sentiment': r['analysis']['result']['sentiment'] if r['analysis'] else None,
                         **({'research_decision':r['analysis']['result']['research_decision']} if r['analysis'] and r['analysis']['result'].get('analysis_format') in {'R17','R18'} else {})}
                        for r in selected[:10]]}

def verify_analysis_replay():
    """Private original fixtures: observed receipt and analysis completion separation."""
    from .storage.repository import Repository
    from .news import normalize_news
    from .news_replay import NewsReplayRequest, create_news_replay
    from ..intelligence.analysis import compute_analysis
    from ..intelligence.sentiment import RulesEngine
    from ..intelligence.policy import analyzer_metadata
    repository = Repository('sqlite:///:memory:'); checks = []
    key = digest(analyzer_metadata(RulesEngine()))
    def require(ok, name):
        if not ok: raise StorageError('Analysis replay verification failed: '+name)
        checks.append(name)
    try:
        n = normalize_news({'id':1,'headline':'Microsoft reports strong growth.', 'summary':'', 'content':'',
            'created_at':'2026-01-05T10:00:00Z','updated_at':'2026-01-05T10:00:00Z',
            'symbols':['MSFT'],'source':'original-synthetic','url':'https://example.com/fixture'})
        first = '2026-01-06T10:00:00Z'; data = n['data']
        repository.connection.execute('INSERT INTO news_versions VALUES (?,?,?,?,?,?,?)',
            ('alpaca',1,n['revision_hash'],data['created_at'],data['updated_at'],first,encode(data)))
        for symbol, method in n['mappings'].items():
            repository.connection.execute('INSERT INTO news_symbols VALUES (?,?,?,?,?)',('alpaca',1,n['revision_hash'],symbol,method))
        result = compute_analysis({'data':data,'mappings':n['mappings'],'first_seen':first},RulesEngine())
        repository.connection.execute('INSERT INTO news_analyses VALUES (?,?,?,?,?,?,?,?,?)',
            ('alpaca',1,n['revision_hash'],key,'rules',result['input_sha256'],'2026-01-07T10:00:00Z',encode(result),digest(result)))
        repository.connection.commit()
        news = create_news_replay(repository, NewsReplayRequest('2026-01-08T00:00:00Z',('MSFT',)))
        frozen = create_analysis_replay(repository, news['replay_id'],'2026-01-09T00:00:00Z',key,'rules')
        early = replay_analysis(repository,frozen['replay_id'],'2026-01-05T23:00:00Z')
        require(early['available_articles']==0 and early['available_analyses']==0,'no_news_before_receipt')
        past = replay_analysis(repository,frozen['replay_id'],'2026-01-06T12:00:00Z')
        require(past['available_articles']==1 and past['available_analyses']==0,'future_analysis_excluded')
        boundary = replay_analysis(repository,frozen['replay_id'],'2026-01-07T10:00:00Z')
        require(boundary['available_analyses']==1,'inclusive_analysis_completion_boundary')
        require(replay_analysis(repository,frozen['replay_id'])['state_sha256']==frozen['state_sha256'],'repeat_state_digest')
        other = create_analysis_replay(repository,news['replay_id'],'2026-01-09T00:00:00Z','0'*64,'rules')
        require(other['available_analyses']==0,'explicit_analyzer_isolation')
        repository.connection.execute("UPDATE news_analyses SET result_json='{}'"); repository.connection.commit()
        require(replay_analysis(repository,frozen['replay_id'])['available_analyses']==1,'source_change_isolation')
        rejected = False
        try: replay_analysis(repository,frozen['replay_id'],'2026-01-10T00:00:00Z')
        except DataValidationError: rejected=True
        require(rejected,'time_after_frozen_cutoff_rejected')
        repository.connection.execute("UPDATE replay_items SET record_json='{}' WHERE replay_id=?",(frozen['replay_id'],));repository.connection.commit()
        rejected=False
        try: replay_analysis(repository,frozen['replay_id'])
        except StorageError: rejected=True
        require(rejected,'tampered_snapshot_rejected')
        return {'status':'passed','checks':checks,'fixture_source':'original_synthetic_controlled_times',
                'user_database_modified':False,'model_required':False,'network_required':False,
                'replay_semantics':SEMANTICS,'exact_live_point_in_time_validated':False}
    finally: repository.close()
