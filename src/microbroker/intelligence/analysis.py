"""Immutable analysis per news revision and analyzer; bounded snapshot, atomic writes."""
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
import uuid
from ..config import UNIVERSE
from ..data.models import DataValidationError, validate_symbols
from ..data.research import begin_read, encode
from ..data.storage.repository import StorageError
from .policy import prepare_text, relevance, company_contexts, article_flags, analyzer_metadata, digest
from .sentiment import validate_sentiment, AnalysisError
from .evidence import evidence_summary
from .unit_evidence import score_units, company_unit_decision
from .research_decision import research_decision, validate_research_decisions

log = logging.getLogger('microbroker.analysis')
MAX_ARTICLES = 5000

@dataclass(frozen=True)
class AnalysisRequest:
    provider: str = 'alpaca'
    symbols: tuple = ()
    first_seen_since: str | None = None

    def __post_init__(self):
        if self.provider not in {'alpaca', 'alpaca-sandbox'}:
            raise DataValidationError('Invalid news provider')
        if self.first_seen_since is not None:
            from ..data.stream_models import canonical_time
            object.__setattr__(self,'first_seen_since',canonical_time(self.first_seen_since))
        if self.symbols:
            object.__setattr__(self, 'symbols', validate_symbols(self.symbols))
            if not set(self.symbols).issubset(UNIVERSE):
                raise DataValidationError('Analysis supports the 12 configured research symbols')

    def metadata(self):
        result={'provider': self.provider, 'symbols': self.symbols,
                'versions': 'current_snapshot', 'max_articles': MAX_ARTICLES}
        if self.first_seen_since is not None:result['first_seen_since']=self.first_seen_since
        return result

def select_news(repository, request):
    m = repository.marker
    query = ('SELECT a.article_id,a.revision_hash,v.first_seen_at,v.data_json '
             'FROM news_articles a JOIN news_versions v ON v.provider=a.provider '
             'AND v.article_id=a.article_id AND v.revision_hash=a.revision_hash '
             f'WHERE a.provider={m}')
    params = [request.provider]
    if request.symbols:
        query += (f" AND EXISTS (SELECT 1 FROM news_symbols s WHERE s.provider=a.provider "
                  "AND s.article_id=a.article_id AND s.revision_hash=a.revision_hash "
                  f"AND s.symbol IN ({','.join([m]*len(request.symbols))}))")
        params.extend(request.symbols)
    if request.first_seen_since is not None:
        # Coarse UTC-second filter preserves nanosecond boundary candidates; exact check below.
        query += f' AND SUBSTR(v.first_seen_at,1,19)>={m}'
        params.append(request.first_seen_since[:19])
    begin_read(repository)
    try:
        rows = repository.connection.execute(query+' ORDER BY a.article_id LIMIT 5001', params).fetchall()
        if len(rows) > MAX_ARTICLES:
            raise StorageError('Analysis selection exceeds 5000 articles; narrow --symbols')
        items = []
        for article_id, revision, first_seen, raw in rows:
            if request.first_seen_since is not None:
                from ..data.stream_models import canonical_time
                if canonical_time(first_seen)<request.first_seen_since:continue
            data = json.loads(raw)
            if digest(data) != revision or data['provider'] != request.provider or data['id'] != article_id:
                raise StorageError('News revision integrity check failed')
            mappings = dict(repository.connection.execute(
                f'SELECT symbol,method FROM news_symbols WHERE provider={m} AND article_id={m} AND revision_hash={m} ORDER BY symbol',
                (request.provider, article_id, revision)).fetchall())
            items.append({'id': article_id, 'revision': revision, 'first_seen': first_seen,
                          'data': data, 'mappings': mappings})
        repository.connection.commit()
        return items
    except BaseException:
        repository.connection.rollback()
        raise

def compute_analysis(article, engine):
    text, fields, basis, truncated = prepare_text(article['data'])
    if not text:
        raise DataValidationError('News analysis needs nonempty text')
    score = getattr(engine, 'score_document', engine.score)
    sentiment = score(text)
    validate_sentiment(sentiment)
    entity_relevance = relevance(fields, article['mappings'])
    contexts = company_contexts(fields, article['mappings'])
    from .policy import company_contexts_v8
    raw_contexts=company_contexts_v8(fields,article['mappings'])
    entity_sentiments = {}
    cache = {text: sentiment}
    article_unit_texts = set()
    for symbol, context in contexts.items():
        raw_text='\n'.join(unit['text'] for unit in raw_contexts[symbol]['units'])
        raw_sentiment=None
        if raw_text:
            if raw_text not in cache:cache[raw_text]=score(raw_text)
            raw_sentiment=cache[raw_text];validate_sentiment(raw_sentiment)
        context_text = '\n'.join(unit['text'] for unit in context['units'])
        if context_text:
            if context_text not in cache: cache[context_text] = score(context_text)
            company_sentiment = cache[context_text]
            validate_sentiment(company_sentiment)
            status = 'scored' if company_sentiment['label'] != 'unscored' else 'unscored'
        else:
            company_sentiment = None
            status = ('attribution_ambiguous' if context['ambiguous_units_excluded'] else
                      'insufficient_context' if context['insufficient_units_excluded'] else
                      'shared_context_only' if context['shared_units_excluded'] else 'no_explicit_mention')
        entity_sentiments[symbol] = {
            'status': status, 'scope': 'guarded_single_company_or_explicit_money_flow_context',
            'method': 'mention_clause_filter_with_subject_counterparty_guard', 'attribution_validated': False,
            'context_sha256': digest(context_text), 'context_unit_count': len(context['units']),
            'shared_units_excluded': context['shared_units_excluded'],
            'insufficient_units_excluded': context['insufficient_units_excluded'],
            'ambiguous_units_excluded':context['ambiguous_units_excluded'],
            'excluded_context_sha256':digest(context['ambiguous_units']),
            'raw_mention_context_sha256':digest(raw_text),'raw_mention_sentiment':raw_sentiment,
            'raw_mention_sentiment_usable_for_alignment':False,
            'evidence_samples': [{k: v for k, v in unit.items() if k != 'text'} |
                                 {'excerpt': unit['text'][:240], 'text_sha256': digest(unit['text'])}
                                 for unit in context['units'][:3]],
            'reported_money_flow_counts': dict(Counter(unit['reported_money_flow'] for unit in context['units'] if unit.get('reported_money_flow'))),
            'evidence_summary': evidence_summary(context_text, company_sentiment) if company_sentiment else None,
            'sentiment': company_sentiment}
        entity = entity_sentiments[symbol]
        entity['unit_evidence'] = score_units(context, score, cache, article_unit_texts)
        entity['unit_evidence_sha256'] = digest(entity['unit_evidence'])
        entity['research_decision'] = company_unit_decision(company_sentiment, entity['evidence_summary'], status, entity['unit_evidence'])
        entity_relevance[symbol]['entity_sentiment_available'] = entity['research_decision']['status'] == 'accepted'
    diagnostic = evidence_summary(text, sentiment)
    flags = article_flags(fields['headline'])
    if diagnostic['status'] == 'mixed_evidence': flags.append('mixed_evidence_detected')
    if not fields['content_text']: flags.append('no_full_body')
    if truncated: flags.append('character_truncated')
    if sentiment.get('token_truncated'): flags.append('token_truncated')
    if sentiment.get('mixed_chunk_polarity'): flags.append('mixed_chunk_polarity')
    if any(e['shared_units_excluded'] for e in entity_sentiments.values()): flags.append('shared_entity_context_excluded')
    if any(e['status'] in {'no_explicit_mention', 'shared_context_only', 'insufficient_context'} for e in entity_sentiments.values()):
        flags.append('entity_context_unavailable')
    if any(e['insufficient_units_excluded'] for e in entity_sentiments.values()): flags.append('insufficient_entity_context_excluded')
    if any(e['ambiguous_units_excluded'] for e in entity_sentiments.values()):flags.append('ambiguous_entity_attribution_excluded')
    if any(e['sentiment'] and e['sentiment'].get('mixed_chunk_polarity') for e in entity_sentiments.values()):
        flags.append('mixed_entity_chunk_polarity')
    if any(e['research_decision']['status'] in {'conflicting_evidence','ambiguous_model_output','incomplete_evidence'} for e in entity_sentiments.values()):
        flags.append('company_research_decision_withheld')
    if 'top_label_margin_below_heuristic' in research_decision(sentiment, diagnostic)['reason_codes']:
        flags.append('ambiguous_model_output')
    if len(entity_relevance) > 1: flags.append('multiple_symbols')
    if not entity_relevance: flags.append('unmapped_article')
    if any(r['basis'] == 'metadata_only' for r in entity_relevance.values()): flags.append('metadata_only_mapping')
    if sentiment['label'] == 'unscored': flags.append('no_rule_evidence')
    return {'input_sha256': digest({'text': text, 'basis': basis, 'mappings': article['mappings']}),
            'text_basis': basis, 'character_truncated_fields': truncated,
            'text_basis_semantics': 'bounded_fields_all_tokens_covered_for_finbert',
            'sentiment_scope': 'document', 'sentiment': sentiment, 'relevance': entity_relevance,
            'entity_sentiments': entity_sentiments, 'analysis_format': 'R18', 'unit_evidence_implemented': True,
            'evidence_summary': diagnostic, 'research_decision': research_decision(sentiment, diagnostic),
            'flags': sorted(flags), 'review_required': bool(flags),
            'language_assumption': 'English', 'language_verified': False,
            'article_first_seen_at': article['first_seen'],
            'backtest_ready': False, 'usable_for_trading': False,
            'entity_sentiment_implemented': True, 'entity_attribution_validated': False,
            'price_impact_implemented': False}

def analyze_news(repository, request, engine):
    metadata = analyzer_metadata(engine); key = digest(metadata)
    run_id = str(uuid.uuid4()); started = datetime.now(timezone.utc).isoformat(); m = repository.marker
    stats = {'run_id': run_id, 'status': 'completed', 'engine': engine.name, 'analyzer_key': key,
             'selected_articles': 0, 'inserted_analyses': 0, 'existing_analyses': 0,
             'news_tables_modified': False, 'sentiment_scope': 'document', 'analysis_format': 'R18',
             'entity_sentiment_scope': 'guarded_single_company_or_explicit_money_flow_context'}
    def audit(status, result):
        repository.connection.execute(f"INSERT INTO news_analysis_runs VALUES ({','.join([m]*9)})",
            (run_id, started, datetime.now(timezone.utc).isoformat(), status, request.provider, engine.name, key,
             encode({**request.metadata(), 'analyzer': metadata}), encode(result)))
    try:
        articles = select_news(repository, request)
        stats['selected_articles'] = len(articles)
        for ordinal, article in enumerate(articles, 1):
            identity = (request.provider, article['id'], article['revision'], key)
            existing = repository.connection.execute(
                f'SELECT result_json,result_sha256 FROM news_analyses WHERE provider={m} AND article_id={m} AND revision_hash={m} AND analyzer_key={m}', identity).fetchone()
            if existing is not None:
                if digest(json.loads(existing[0])) != existing[1]:
                    raise StorageError('Stored analysis integrity check failed')
                stats['existing_analyses'] += 1
                continue
            result = compute_analysis(article, engine)
            inserted = repository.connection.execute(
                f"INSERT INTO news_analyses VALUES ({','.join([m]*9)}) ON CONFLICT (provider,article_id,revision_hash,analyzer_key) DO NOTHING",
                identity+(engine.name, result['input_sha256'], datetime.now(timezone.utc).isoformat(), encode(result), digest(result))).rowcount
            stats['inserted_analyses' if inserted else 'existing_analyses'] += 1
            if ordinal % 10 == 0:
                log.info('News analysis progress: %d/%d engine=%s', ordinal, len(articles), engine.name)
        if not articles: stats['status'] = 'empty'
        audit(stats['status'], stats)
        repository.connection.commit()
        return stats
    except BaseException:
        repository.connection.rollback()
        try:
            audit('failed', {'committed_analyses': 0, 'error_code': 'analysis_failed'})
            repository.connection.commit()
        except Exception:
            repository.connection.rollback()
        raise

def analysis_summary(repository, request, *, analyzer_key=None, engine='finbert'):
    if engine not in {'finbert', 'rules'}: raise DataValidationError('Invalid analysis engine')
    if analyzer_key is not None and (len(analyzer_key) != 64 or any(c not in '0123456789abcdef' for c in analyzer_key)):
        raise DataValidationError('Invalid analyzer key')
    # select_news verifies a coherent current news snapshot. Summary runs a second, bounded
    # analysis snapshot against those exact revisions; later arrivals cannot mix revisions.
    articles = select_news(repository, request)
    m = repository.marker
    begin_read(repository)
    try:
        if analyzer_key is None:
            run = repository.connection.execute(
                f"SELECT analyzer_key FROM news_analysis_runs WHERE status='completed' AND provider={m} AND engine={m} ORDER BY finished_at DESC,id DESC LIMIT 1",
                (request.provider, engine)).fetchone()
            analyzer_key = run[0] if run else None
        labels = Counter(); flags = Counter(); bases = Counter(); entity_counts = Counter(); samples = []
        analyzed = 0; company_labels = Counter(); company_status = Counter(); chunk_counts = Counter(); formats = Counter(); decision_counts = Counter(); entity_decision_counts = Counter(); evidence_counts = Counter(); flow_counts = Counter()
        for article in reversed(articles):
            row = None if analyzer_key is None else repository.connection.execute(
                f'SELECT result_json,result_sha256 FROM news_analyses WHERE provider={m} AND article_id={m} AND revision_hash={m} AND analyzer_key={m} AND engine={m}',
                (request.provider, article['id'], article['revision'], analyzer_key, engine)).fetchone()
            if row is None: continue
            result = json.loads(row[0])
            if digest(result) != row[1]: raise StorageError('Stored analysis integrity check failed')
            validate_research_decisions(result)
            decision_counts[result.get('research_decision', {}).get('status', 'legacy_not_gated')] += 1
            analyzed += 1; labels[result['sentiment']['label']] += 1; bases[result['text_basis']] += 1
            flags.update(result['flags'])
            formats[result.get('analysis_format', 'R5')] += 1
            evidence_counts[result.get('evidence_summary', {}).get('status', 'legacy_not_available')] += 1
            chunk_counts[result['sentiment'].get('chunk_count', 1)] += 1
            for symbol, context in result.get('entity_sentiments', {}).items():
                if request.symbols and symbol not in request.symbols: continue
                company_status[(symbol, context['status'])] += 1
                entity_decision_counts[(symbol, context.get('research_decision', {}).get('status', 'legacy_not_gated'))] += 1
                flow_counts.update({(symbol, direction): count for direction, count in context.get('reported_money_flow_counts', {}).items()})
                if context['sentiment'] is not None: company_labels[(symbol, context['sentiment']['label'])] += 1
            for symbol, rel in result['relevance'].items():
                if not request.symbols or symbol in request.symbols: entity_counts[(symbol, rel['basis'])] += 1
            if len(samples) < 10:
                samples.append({'id': article['id'], 'headline': article['data']['headline'][:200],
                                'revision_hash': article['revision'], 'text_basis': result['text_basis'],
                                'sentiment': {k: v for k, v in result['sentiment'].items() if k not in {'evidence','negated_evidence','chunks'}},
                                'entity_sentiments': {s: {**context,
                                    **({'unit_evidence':{k:v for k,v in context['unit_evidence'].items() if k!='units'}} if 'unit_evidence' in context else {}), 'sentiment':
                                    {k: v for k, v in context['sentiment'].items() if k not in {'evidence','negated_evidence','chunks'}}
                                    if context['sentiment'] is not None else None}
                                    for s, context in result.get('entity_sentiments', {}).items() if not request.symbols or s in request.symbols},
                                'relevance': {s: r for s, r in result['relevance'].items() if not request.symbols or s in request.symbols},
                                'research_decision': result.get('research_decision'),
                                'evidence_summary': result.get('evidence_summary'),
                                'flags': result['flags'], 'review_required': result['review_required']})
        repository.connection.commit()
        return {'status': 'ready' if analyzed else 'empty', 'engine': engine, 'analyzer_key': analyzer_key,
                'selected_articles': len(articles), 'analyzed_current_articles': analyzed,
                'pending_articles': len(articles)-analyzed, 'label_counts': dict(sorted(labels.items())),
                'text_basis_counts': dict(sorted(bases.items())), 'flag_counts': dict(sorted(flags.items())),
                'analysis_format_counts': dict(sorted(formats.items())),
                'label_counts_semantics': 'raw_model_labels_not_research_decisions',
                'research_decision_status_counts': dict(sorted(decision_counts.items())),
                'entity_research_decision_status_counts': {s: {status: count for (symbol, status), count in sorted(entity_decision_counts.items()) if symbol == s} for s in sorted({symbol for symbol, _ in entity_decision_counts})},
                'evidence_status_counts': dict(sorted(evidence_counts.items())),
                'reported_money_flow_counts': {s: {direction: count for (symbol, direction), count in sorted(flow_counts.items()) if symbol == s} for s in sorted({symbol for symbol, _ in flow_counts})},
                'document_chunk_count_distribution': dict(sorted(chunk_counts.items())),
                'entity_label_counts': {s: {label: count for (symbol, label), count in sorted(company_labels.items()) if symbol == s}
                    for s in sorted({symbol for symbol, _ in company_status})},
                'entity_status_counts': {s: {status: count for (symbol, status), count in sorted(company_status.items()) if symbol == s}
                    for s in sorted({symbol for symbol, _ in company_status})},
                'text_basis_semantics': 'see_stored_analysis_format_R5_prefix_R6_R8_bounded_all_tokens',
                'relevance_counts': {s: {basis: count for (symbol, basis), count in sorted(entity_counts.items()) if symbol == s}
                    for s in sorted({symbol for symbol, _ in entity_counts})},
                'samples': samples, 'sentiment_scope': 'document', 'entity_sentiment_implemented': bool(formats.get('R6') or formats.get('R8') or formats.get('R16') or formats.get('R17') or formats.get('R18')),
                'entity_sentiment_scope': 'guarded_single_company_or_explicit_money_flow_context' if (formats.get('R16') or formats.get('R17') or formats.get('R18')) else 'single_company_or_explicit_money_flow_context', 'entity_attribution_validated': False,
                'confidence_calibrated': False, 'price_impact_implemented': False, 'backtest_ready': False,
                'news_tables_modified': False}
    except BaseException:
        repository.connection.rollback(); raise
