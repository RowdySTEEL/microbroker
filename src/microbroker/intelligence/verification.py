"""Small original engineering fixtures. These are smoke tests, not model accuracy."""
from ..data.models import utc_datetime
from ..data.news import NewsRequest, normalize_news, collect_news
from ..data.storage.repository import Repository
from .analysis import AnalysisRequest, analyze_news, analysis_summary, compute_analysis
from .policy import digest
from .sentiment import AnalysisError

SMOKE_CASES = (
    ('The company beats expectations and raises guidance.', 'positive', 'positive'),
    ('The company cuts guidance and revenue fell.', 'negative', 'negative'),
    ('The company will publish its quarterly report next week.', 'neutral', 'unscored'),
)

def verify_analysis(engine):
    checks = []; labels = []
    for text, expected_model, expected_rules in SMOKE_CASES:
        first = engine.score(text); second = engine.score(text)
        if digest(first) != digest(second):
            raise AnalysisError('Analysis smoke test failed: repeated inference changed')
        expected = expected_rules if engine.name == 'rules' else expected_model
        if first['label'] != expected:
            raise AnalysisError('Analysis smoke test failed: unexpected '+expected+' fixture label')
        labels.append(first['label'])
    checks.extend(['three_sentiment_smoke_cases', 'repeat_inference'])
    repository = Repository('sqlite:///:memory:')
    story = {'id': 1, 'headline': 'Microsoft beats expectations and raises guidance.',
             'summary': '', 'symbols': ['MSFT'], 'content': '', 'source': 'synthetic-test',
             'url': 'https://example.com/test', 'created_at': '2026-01-05T14:30:00Z',
             'updated_at': '2026-01-05T14:30:00Z'}
    class Fixture:
        pages = 1
        def iter_news(self, request): yield normalize_news(story)
    try:
        collect_news(Fixture(), repository, NewsRequest(('MSFT',), utc_datetime('2026-01-05T00:00:00Z'), utc_datetime('2026-01-06T00:00:00Z')))
        request = AnalysisRequest(symbols=('MSFT',))
        first = analyze_news(repository, request, engine)
        second = analyze_news(repository, request, engine)
        summary = analysis_summary(repository, request, engine=engine.name)
        if first['inserted_analyses'] != 1 or second['inserted_analyses'] != 0 or second['existing_analyses'] != 1:
            raise AnalysisError('Analysis smoke test failed: persistence idempotency')
        checks.append('analysis_idempotency')
        if (summary['analyzed_current_articles'] != 1 or
                summary['samples'][0]['relevance']['MSFT']['basis'] != 'headline_mention' or
                summary['samples'][0]['text_basis'] != 'headline' or
                'no_full_body' not in summary['samples'][0]['flags']):
            raise AnalysisError('Analysis smoke test failed: content and relevance')
        checks.append('headline_only_and_entity_mentions')
    finally:
        repository.close()
    def fixture(headline, content='', symbols=('MSFT',)):
        normalized = normalize_news({**story, 'headline': headline, 'content': content, 'symbols': list(symbols)})
        return {'data': normalized['data'], 'mappings': normalized['mappings'], 'first_seen': '2026-01-06T00:00:00Z'}
    opposite = compute_analysis(fixture(
        'Microsoft beats expectations and raises guidance. Apple cuts guidance and revenue fell.',
        symbols=('MSFT','AAPL')), engine)
    if (opposite['entity_sentiments']['MSFT']['sentiment']['label'] != 'positive' or
            opposite['entity_sentiments']['AAPL']['sentiment']['label'] != 'negative'):
        raise AnalysisError('Analysis smoke test failed: opposite company contexts')
    checks.append('opposite_company_contexts')
    if not opposite['evidence_summary']['opposite_rule_phrases']:
        raise AnalysisError('Analysis smoke test failed: opposing evidence')
    checks.append('mixed_evidence_preserves_raw_model_label')
    shared = compute_analysis(fixture('Microsoft and Apple publish a report.', symbols=('MSFT','AAPL')), engine)
    if any(e['status'] != 'shared_context_only' or e['sentiment'] is not None for e in shared['entity_sentiments'].values()):
        raise AnalysisError('Analysis smoke test failed: shared context exclusion')
    checks.append('shared_company_context_excluded')
    metadata_only = compute_analysis(fixture('A security vendor publishes a report.'), engine)
    if metadata_only['entity_sentiments']['MSFT']['sentiment'] is not None:
        raise AnalysisError('Analysis smoke test failed: metadata-only company sentiment')
    checks.append('metadata_only_company_not_scored')
    long = compute_analysis(fixture('Quarterly reports',
        ('The company will publish its report next week. '*80)+
        'Microsoft cuts guidance and revenue fell.'), engine)
    sentiment = long['sentiment']
    if long['entity_sentiments']['MSFT']['sentiment']['label'] != 'negative':
        raise AnalysisError('Analysis smoke test failed: company evidence after prefix')
    if engine.name == 'finbert' and (sentiment.get('chunk_count', 0) < 2 or sentiment['token_truncated'] or
            sentiment['covered_content_tokens'] != sentiment['content_token_count']):
        raise AnalysisError('Analysis smoke test failed: full token coverage')
    checks.append('long_document_tail_and_coverage')
    flow = compute_analysis(fixture('money flows are negative in Microsoft and Apple.', symbols=('MSFT','AAPL')), engine)
    if not all(e['reported_money_flow_counts']=={'negative':1} and e['context_unit_count']==1 for e in flow['entity_sentiments'].values()):
        raise AnalysisError('Analysis smoke test failed: explicit money flow attribution')
    checks.append('explicit_shared_money_flow_attribution')
    ambiguous=compute_analysis(fixture('External provider update',content='The company raises guidance after securing an agreement with Microsoft.'),engine)
    entity=ambiguous['entity_sentiments']['MSFT']
    if entity['status']!='attribution_ambiguous' or entity['sentiment'] is not None or entity['raw_mention_sentiment'] is None:
        raise AnalysisError('Analysis smoke test failed: ambiguous attribution')
    checks.append('ambiguous_subject_raw_output_preserved')
    counterparty=compute_analysis(fixture('External provider update',content='A vendor signed an agreement with Microsoft.'),engine)
    if counterparty['entity_sentiments']['MSFT']['status']!='attribution_ambiguous':
        raise AnalysisError('Analysis smoke test failed: counterparty attribution')
    checks.append('unresolved_counterparty_not_company_score')
    if opposite['research_decision']['label'] is not None:
        raise AnalysisError('Analysis smoke test failed: conflicting document decision')
    checks.append('conflicting_document_withheld_raw_preserved')
    if any(e['research_decision']['label'] != e['sentiment']['label'] for e in opposite['entity_sentiments'].values()):
        raise AnalysisError('Analysis smoke test failed: explicit company decisions')
    checks.append('document_conflict_not_company_fallback')
    if opposite['entity_sentiments']['MSFT']['unit_evidence']['scored_unit_count']<1:
        raise AnalysisError('Analysis smoke test failed: company unit inference')
    checks.append('company_unit_inference_recorded')
    past=compute_analysis(fixture('Company update',content='Microsoft revenue fell last year. Microsoft beats expectations and raises guidance.'),engine)
    company=past['entity_sentiments']['MSFT']
    if (not company['unit_evidence']['opposite_unit_labels'] or company['research_decision']['label'] is not None):
        raise AnalysisError('Analysis smoke test failed: company unit opposite polarity')
    checks.append('opposite_company_units_withheld')
    return {'status': 'passed', 'engine': engine.name, 'checks': checks,
            'fixture_labels': labels, 'fixture_source': 'three_original_synthetic_sentences',
            'additional_fixtures': 'eight_original_context_coverage_evidence_and_attribution_cases', 'analysis_format': 'R18',
            'model_accuracy_validated': False, 'confidence_calibrated': False,
            'entity_attribution_validated': False,
            'user_database_modified': False}
