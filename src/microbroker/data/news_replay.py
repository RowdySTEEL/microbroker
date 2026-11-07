"""Frozen news version replay at recorded receipt cutoffs; never publication-only.

First-seen timestamps precede transaction commit in legacy ingestion. This is a
recorded-receipt reconstruction, not exact live/commit visibility or sentiment replay.
"""
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import uuid
from ..config import UNIVERSE
from .models import DataValidationError, validate_symbols
from .news import normalize_news
from .research import begin_read, encode, digest_records
from .stream_models import canonical_time
from .storage.repository import StorageError

MAX_VERSIONS = 10000
MAX_BYTES = 64 * 1024 * 1024
SEMANTICS = 'recorded_first_seen_version_state_not_exact_commit_visibility'

def digest(value):
    import hashlib
    return hashlib.sha256(encode(value).encode()).hexdigest()

def parsed_json(raw):
    try: return json.loads(raw)
    except (ValueError, TypeError):
        raise StorageError('Invalid stored news replay JSON') from None

@dataclass(frozen=True)
class NewsReplayRequest:
    as_of: str
    symbols: tuple = ()
    provider: str = 'alpaca'
    first_seen_since: str | None = None

    def __post_init__(self):
        object.__setattr__(self, 'as_of', canonical_time(self.as_of))
        if self.first_seen_since is not None:
            object.__setattr__(self, 'first_seen_since', canonical_time(self.first_seen_since))
            if self.first_seen_since > self.as_of:
                raise DataValidationError('first-seen-since must not exceed as-of')
        if self.provider not in {'alpaca', 'alpaca-sandbox'}:
            raise DataValidationError('Invalid news replay provider')
        if self.symbols:
            object.__setattr__(self, 'symbols', tuple(sorted(validate_symbols(self.symbols))))
            if not set(self.symbols).issubset(UNIVERSE):
                raise DataValidationError('News replay supports configured research symbols')

    def metadata(self):
        result = {'source': 'news', 'format': 'R9', 'provider': self.provider,
                'symbols': list(self.symbols), 'as_of': self.as_of,
                'cutoff_inclusive': True, 'replay_semantics': SEMANTICS,
                'analyses_included': False, 'exact_commit_visibility_available': False}
        if self.first_seen_since is not None: result['first_seen_since'] = self.first_seen_since
        return result

def validate_record(record):
    try:
        data = record['data']
        source = {**data, 'content': data['content_html']}
        if not data['content_provided']: source.pop('content')
        normalized = normalize_news(source, provider=record['provider'])
        first_seen = canonical_time(record['first_seen_at'])
        available = max(first_seen, data['created_at'], data['updated_at'])
        if (normalized['data'] != data or normalized['revision_hash'] != record['revision_hash'] or
                normalized['mappings'] != record['mappings'] or data['id'] != record['article_id'] or
                record['source'] != 'news' or record['timestamp'] != available or
                record['first_seen_at'] != first_seen):
            raise StorageError('News replay version integrity check failed')
        return record
    except (KeyError, TypeError, ValueError, DataValidationError):
        raise StorageError('Invalid stored news replay version') from None

def state_at(records, at, symbols=()):
    """Choose the newest visible provider revision; then apply its own symbol map.

    Body preservation at equal provider timestamps matches ingestion. Equal
    receipt/time/body ranks with different revisions have no observed ordering.
    """
    chosen = {}
    for record in records:
        if record['timestamp'] > at: continue
        identity = (record['provider'], record['article_id'])
        data = record['data']
        rank = (data['updated_at'], bool(data['content_text']), record['first_seen_at'])
        previous = chosen.get(identity)
        if previous is not None and previous[0] == rank and previous[1]['revision_hash'] != record['revision_hash']:
            raise StorageError('Ambiguous equal-time news revisions; receipt ordering unavailable')
        if previous is None or rank > previous[0]: chosen[identity] = (rank, record)
    return sorted((r for _, r in chosen.values() if not symbols or set(symbols).intersection(r['mappings'])),
                  key=lambda r: (r['timestamp'], r['article_id'], r['revision_hash']))

def read_versions(repository, request):
    m = repository.marker
    begin_read(repository)
    try:
        query = f'SELECT article_id,revision_hash,created_at,updated_at,first_seen_at,data_json FROM news_versions WHERE provider={m} LIMIT {MAX_VERSIONS+1}'
        if repository.postgres:
            cursor = repository.connection.cursor(name='news_replay_'+uuid.uuid4().hex)
            cursor.execute(query, (request.provider,))
        else: cursor = repository.connection.execute(query, (request.provider,))
        records = []; total_bytes = 0; seen = 0
        while True:
            batch = cursor.fetchmany(1)
            if not batch: break
            for article_id, revision, created, updated, first_seen, raw in batch:
                seen += 1
                if seen > MAX_VERSIONS: raise StorageError('News replay exceeds 10000 provider versions')
                total_bytes += len(raw.encode('utf-8'))
                if total_bytes > MAX_BYTES: raise StorageError('News replay exceeds 64 MiB input bound')
                first_seen = canonical_time(first_seen)
                # Still enforce row bounds for the entire provider; future rows do not enter the snapshot.
                if first_seen > request.as_of: continue
                data = parsed_json(raw)
                if data['created_at'] != created or data['updated_at'] != updated:
                    raise StorageError('News replay version timestamp mismatch')
                mappings = dict(repository.connection.execute(
                    f'SELECT symbol,method FROM news_symbols WHERE provider={m} AND article_id={m} AND revision_hash={m}',
                    (request.provider, article_id, revision)).fetchall())
                available = max(first_seen, canonical_time(created), canonical_time(updated))
                record = {'source': 'news', 'provider': request.provider, 'article_id': article_id,
                          'revision_hash': revision, 'first_seen_at': first_seen,
                          'timestamp': available, 'data': data, 'mappings': mappings}
                validate_record(record)
                if available <= request.as_of and (request.first_seen_since is None or first_seen >= request.first_seen_since):
                    records.append(record)
        cursor.close()
        repository.connection.commit()
    except BaseException:
        repository.connection.rollback(); raise
    # Retain every historical revision of any relevant article, including versions
    # whose symbol tags change away from the requested symbol later.
    identities = {r['article_id'] for r in records if not request.symbols or set(request.symbols).intersection(r['mappings'])}
    records = [r for r in records if r['article_id'] in identities]
    records.sort(key=lambda r: (r['timestamp'], r['article_id'], r['revision_hash']))
    state_at(records, request.as_of, request.symbols)  # reject unresolved ordering before persisting
    return records

def create_news_replay(repository, request):
    records = read_versions(repository, request)
    selected = state_at(records, request.as_of, request.symbols)
    if sum(len(encode(r).encode('utf-8')) for r in records) > MAX_BYTES:
        raise StorageError('News replay exceeds 64 MiB snapshot bound')
    missing = [s for s in request.symbols if not any(s in r['mappings'] for r in selected)]
    metadata = request.metadata(); sha = digest_records(records)
    report = {'status': 'warning', 'replay_semantics': SEMANTICS,
              'warning_codes': ['legacy_receipt_time_not_commit_time'] + (['empty_asof_state'] if not selected else []),
              'missing_symbols': missing, 'available_articles_as_of': len(selected),
              'manifest_sha256': digest({'request': metadata, 'item_count': len(records), 'dataset_sha256': sha})}
    replay_id = str(uuid.uuid4()); m = repository.marker
    try:
        repository.connection.execute(f"INSERT INTO replay_runs VALUES ({','.join([m]*6)})",
            (replay_id, datetime.now(timezone.utc).isoformat(), encode(metadata), len(records), sha, encode(report)))
        for i, record in enumerate(records):
            repository.connection.execute(f"INSERT INTO replay_items VALUES ({','.join([m]*3)})", (replay_id, i, encode(record)))
        repository.connection.commit()
    except BaseException:
        repository.connection.rollback(); raise
    return {'status': 'ready', 'replay_id': replay_id, 'item_count': len(records),
            'available_articles_as_of': len(selected), 'as_of': request.as_of,
            'dataset_sha256': sha, 'quality': report, 'news_tables_modified': False,
            'replay_snapshots_created': 1, 'analyses_included': False,
            'exact_live_point_in_time_validated': False}

def replay_news(repository, replay_id, at=None, *, _include_records=False):
    m = repository.marker
    begin_read(repository)
    try:
        info = repository.connection.execute(
            f'SELECT request_json,item_count,dataset_sha256,report_json FROM replay_runs WHERE id={m}', (replay_id,)).fetchone()
        if not info: raise DataValidationError('Unknown news replay_id')
        metadata = parsed_json(info[0]); report = parsed_json(info[3])
        if metadata.get('source') != 'news' or metadata.get('format') != 'R9':
            raise DataValidationError('replay-news requires an R9 news snapshot')
        request = NewsReplayRequest(metadata['as_of'], tuple(metadata['symbols']), metadata['provider'],metadata.get('first_seen_since'))
        if metadata != request.metadata() or report.get('manifest_sha256') != digest(
                {'request': metadata, 'item_count': info[1], 'dataset_sha256': info[2]}):
            raise StorageError('News replay manifest integrity check failed')
        when = canonical_time(at) if at else request.as_of
        if when > request.as_of: raise DataValidationError('Replay time exceeds frozen as-of cutoff')
        query = f'SELECT ordinal,record_json FROM replay_items WHERE replay_id={m} ORDER BY ordinal LIMIT {MAX_VERSIONS+1}'
        if repository.postgres:
            cursor = repository.connection.cursor(name='news_frozen_'+uuid.uuid4().hex)
            cursor.execute(query, (replay_id,))
        else: cursor = repository.connection.execute(query, (replay_id,))
        rows = []; size = 0
        while True:
            batch = cursor.fetchmany(1)
            if not batch: break
            for row in batch:
                size += len(row[1].encode('utf-8'))
                if size > MAX_BYTES: raise StorageError('News replay exceeds 64 MiB snapshot bound')
                rows.append(row)
        cursor.close()
        if len(rows) != info[1] or len(rows) > MAX_VERSIONS:
            raise StorageError('News replay item count mismatch')
        records = []
        for i, (ordinal, raw) in enumerate(rows):
            if i != ordinal: raise StorageError('News replay ordinal mismatch')
            record = validate_record(parsed_json(raw))
            if (record['provider'] != request.provider or record['timestamp'] > request.as_of or
                    (request.first_seen_since is not None and record['first_seen_at'] < request.first_seen_since)):
                raise StorageError('News replay contains data outside frozen cutoff')
            records.append(record)
        if digest_records(records) != info[2]: raise StorageError('News replay digest mismatch')
        selected = state_at(records, when, request.symbols)
        repository.connection.commit()
    except BaseException:
        repository.connection.rollback(); raise
    if _include_records: return request, records, info[2]
    return {'status': 'passed', 'replay_id': replay_id, 'item_count': len(records),
            'available_articles': len(selected), 'at': when, 'frozen_as_of': request.as_of,
            'dataset_sha256': info[2], 'state_sha256': digest_records(selected),
            'replay_semantics': SEMANTICS, 'exact_live_point_in_time_validated': False,
            'analyses_included': False, 'news_tables_modified': False,
            'missing_symbols': [s for s in request.symbols if not any(s in r['mappings'] for r in selected)],
            'samples': [{'id': r['article_id'], 'revision_hash': r['revision_hash'],
                         'headline': r['data']['headline'][:200], 'created_at': r['data']['created_at'],
                         'updated_at': r['data']['updated_at'], 'first_seen_at': r['first_seen_at'],
                         'available_at': r['timestamp'], 'symbols': sorted(r['mappings'])} for r in selected[:10]]}

def verify_news_replay():
    """Original controlled-time fixtures in a private in-memory database."""
    from .storage.repository import Repository
    repository = Repository('sqlite:///:memory:'); checks = []
    def require(condition, name):
        if not condition: raise StorageError('News replay verification failed: '+name)
        checks.append(name)
    def insert(headline, updated, received, symbols=('MSFT',), article_id=1):
        normalized = normalize_news({'id': article_id, 'headline': headline, 'summary': '', 'content': '',
            'created_at': '2026-01-05T10:00:00Z', 'updated_at': updated, 'symbols': list(symbols),
            'source': 'original-synthetic-fixture', 'url': 'https://example.com/fixture'})
        d = normalized['data']; revision = normalized['revision_hash']
        repository.connection.execute('INSERT INTO news_versions VALUES (?,?,?,?,?,?,?)',
            ('alpaca',article_id,revision,d['created_at'],d['updated_at'],received,encode(d)))
        for s, method in normalized['mappings'].items():
            repository.connection.execute('INSERT INTO news_symbols VALUES (?,?,?,?,?)',('alpaca',article_id,revision,s,method))
        repository.connection.commit()
    try:
        insert('Microsoft publishes its original report.', '2026-01-05T10:00:00Z', '2026-01-06T10:00:00Z')
        insert('Apple receives the corrected report.', '2026-01-07T10:00:00Z', '2026-01-07T11:00:00Z', ('AAPL',))
        snapshot = create_news_replay(repository, NewsReplayRequest('2026-01-08T00:00:00Z', ('MSFT',)))
        early = replay_news(repository, snapshot['replay_id'], '2026-01-05T23:00:00Z')
        require(early['available_articles']==0, 'publication_is_not_receipt_availability')
        past = replay_news(repository, snapshot['replay_id'], '2026-01-06T12:00:00Z')
        require(past['available_articles']==1 and 'original' in past['samples'][0]['headline'], 'future_correction_excluded')
        late = replay_news(repository, snapshot['replay_id'])
        require(late['available_articles']==0, 'current_revision_symbol_removal_no_stale_fallback')
        require(replay_news(repository,snapshot['replay_id'])==late, 'repeat_replay_digest')
        insert('Microsoft publishes a later new report.', '2026-01-09T10:00:00Z', '2026-01-09T11:00:00Z')
        require(replay_news(repository,snapshot['replay_id'])==late, 'snapshot_isolated_from_later_arrivals')
        at_receipt = replay_news(repository,snapshot['replay_id'],'2026-01-06T10:00:00Z')
        require(at_receipt['available_articles']==1, 'inclusive_receipt_boundary')
        require(not late['analyses_included'], 'future_sentiment_not_joined')
        rejected=False
        try: replay_news(repository,snapshot['replay_id'],'2026-01-10T00:00:00Z')
        except DataValidationError: rejected=True
        require(rejected,'time_after_snapshot_rejected')
        repository.connection.execute("UPDATE replay_items SET record_json='{}' WHERE replay_id=?",(snapshot['replay_id'],))
        repository.connection.commit(); rejected=False
        try: replay_news(repository,snapshot['replay_id'])
        except StorageError: rejected=True
        require(rejected,'snapshot_tamper_rejected')
        return {'status':'passed','checks':checks,'fixture_source':'original_synthetic_controlled_receipt_times',
                'replay_semantics':SEMANTICS,'exact_live_point_in_time_validated':False,
                'user_database_modified':False,'network_required':False,'model_required':False}
    finally: repository.close()
