"""Versioned, conservative input and entity-mention policy; no price prediction."""
import hashlib
import re
from ..data.research import encode

POLICY_VERSION = 'r18-accepted-context-unit-evidence-v1'
FIELD_LIMITS = {'headline': 4000, 'summary': 6000, 'content_text': 16000}
ALIASES = {
    'AAPL': ('Apple', 'Apple Inc', 'iPhone', 'MacBook', 'iPad'),
    'MSFT': ('Microsoft', 'Azure'), 'NVDA': ('Nvidia',),
    'AMZN': ('Amazon', 'Amazon.com', 'Amazon Web Services'),
    'GOOGL': ('Alphabet', 'Google'), 'META': ('Meta Platforms', 'Facebook', 'Instagram'),
    'TSLA': ('Tesla',), 'AMD': ('Advanced Micro Devices',),
    'NFLX': ('Netflix',), 'AVGO': ('Broadcom',),
    'SPY': ('SPDR S&P 500',), 'QQQ': ('Invesco QQQ',),
}

def digest(value):
    return hashlib.sha256(encode(value).encode()).hexdigest()

def prepare_text(data):
    fields = {}; truncated = []; parts = []
    for field, limit in FIELD_LIMITS.items():
        value = ' '.join(data.get(field, '').split())
        if len(value) > limit:
            truncated.append(field)
        fields[field] = value[:limit]
        if fields[field] and fields[field] not in parts:
            parts.append(fields[field])
    basis = '_'.join(name for name, field in (
        ('headline', 'headline'), ('summary', 'summary'), ('body', 'content_text')) if fields[field])
    return '\n'.join(parts), fields, basis, truncated

def mentions(text, symbol):
    found = []
    # Bare tickers are case sensitive (META/AMD can be ordinary words).
    if re.search(r'(?<![A-Za-z0-9])\$?' + re.escape(symbol) + r'(?![A-Za-z0-9])', text):
        found.append(symbol)
    for alias in ALIASES[symbol]:
        if re.search(r'(?<!\w)' + re.escape(alias) + r'(?!\w)', text, re.I):
            found.append(alias)
    return sorted(set(found))

def relevance(fields, mappings):
    results = {}
    for symbol, method in sorted(mappings.items()):
        evidence = {field: mentions(value, symbol) for field, value in fields.items()}
        if evidence['headline']:
            basis, rank = 'headline_mention', 4
        elif evidence['summary']:
            basis, rank = 'summary_mention', 3
        elif evidence['content_text']:
            basis, rank = 'body_mention', 2
        else:
            basis, rank = 'metadata_only', 1
        results[symbol] = {'basis': basis, 'rank': rank, 'mapping_method': method,
                           'mentions': evidence, 'entity_sentiment_available': False}
    return results

def article_flags(headline):
    flags = []
    if re.search(r'how much.*(?:made|owning|invested)|last \d+ years|\b(?:retrospective|historical return)\b', headline, re.I):
        flags.append('historical_review')
    if re.search(r'\b(?:could|may|might|rumou?r|reportedly|would)\b', headline, re.I):
        flags.append('speculative_language')
    if re.search(r'\b(?:stocks? to watch|gainers|losers|market movers|top \d+ stocks?)\b', headline, re.I):
        flags.append('market_roundup')
    return flags

def context_units(text):
    """Conservative punctuation spans, retaining company abbreviations and exact offsets."""
    start = 0
    for boundary in re.finditer(r'[;!?\n]|\.(?=\s|$)', text):
        if boundary.group() == '.' and re.search(
                r'(?<!\w)(?:Inc|Corp|Co|Ltd|Plc|Mr|Mrs|Ms|Dr|Prof|vs|e\.g|i\.e|(?:[A-Z]\.)*[A-Z])\.$',
                text[:boundary.end()], re.I):
            continue
        end = boundary.start() if boundary.group() in {';', '\n'} else boundary.end()
        left = start
        while left < end and text[left].isspace(): left += 1
        while end > left and text[end-1].isspace(): end -= 1
        if end > left: yield left, end, text[left:end]
        start = boundary.end()
    end = len(text)
    while start < end and text[start].isspace(): start += 1
    while end > start and text[end-1].isspace(): end -= 1
    if end > start: yield start, end, text[start:end]

def substantive_context(text, symbol):
    # Reject pure names/ticker annotations/price numerals, not factual neutral statements.
    remainder = re.sub(r'\b(?:NASDAQ|NYSE|AMEX|NYSEARCA|OTC|OTCMKTS)\s*:', ' ', text, flags=re.I)
    remainder = re.sub(r'(?<![A-Za-z0-9])\$?' + re.escape(symbol) + r'(?![A-Za-z0-9])', ' ', remainder)
    for alias in sorted(ALIASES[symbol], key=len, reverse=True):
        remainder = re.sub(r'(?<!\w)' + re.escape(alias) + r'(?!\w)', ' ', remainder, flags=re.I)
    words = re.findall(r'[A-Za-z]+', remainder.lower())
    boilerplate = {'inc', 'corp', 'corporation', 'co', 'company', 'ltd', 'limited', 'plc',
                   'nasdaq', 'nyse', 'amex', 'nysearca', 'otc', 'otcmkts', 'the', 'and', 'of', 'a', 'an'}
    return any(word not in boilerplate for word in words)

def company_contexts_v8(fields, mappings):
    """Explicit mention clauses only. No pronoun resolution or learned attribution."""
    contexts = {symbol: {'units': [], 'shared_units_excluded': 0, 'insufficient_units_excluded': 0}
                for symbol in sorted(mappings)}
    seen = set()
    for field, text in fields.items():
        for start, end, unit in context_units(text):
            if not unit or unit in seen: continue
            seen.add(unit)
            found = {symbol for symbol in ALIASES if mentions(unit, symbol)}
            flow = explicit_money_flow(unit)
            for symbol in found.intersection(mappings):
                if len(found) != 1 and flow is None:
                    contexts[symbol]['shared_units_excluded'] += 1
                elif not substantive_context(unit, symbol):
                    contexts[symbol]['insufficient_units_excluded'] += 1
                else:
                    contexts[symbol]['units'].append({'field': field, 'start_char': start,
                                                     'end_char': end, 'text': unit,
                                                     'attribution': 'explicit_same_direction_money_flow' if flow else 'single_company_mention',
                                                     'reported_money_flow': flow})
    return contexts

def company_contexts(fields,mappings):
    from .attribution import ambiguity_reason
    contexts=company_contexts_v8(fields,mappings)
    for symbol,context in contexts.items():
        accepted=[];excluded=[]
        for unit in context['units']:
            reason=None if unit['reported_money_flow'] else ambiguity_reason(unit['text'],symbol,ALIASES)
            if reason:excluded.append({**unit,'reason':reason})
            else:accepted.append(unit)
        context['units']=accepted
        context['ambiguous_units_excluded']=len(excluded)
        context['ambiguous_units']=excluded
    return contexts

def explicit_money_flow(text):
    """Accept only an entire same-direction money-flow sentence and a known list.

    Never interpret a buy/sell signal, forecast, negation, or company partnership.
    Unknown names and trailing predicates invalidate this narrow exception.
    """
    match = re.fullmatch(
        r'(?:In the early trade(?: today)?,?\s+)?money flows are '
        r'(positive|negative|neutral|mixed) in (.+?)\.?', text, re.I)
    if not match: return None
    remainder = match[2]
    found = [s for s in ALIASES if mentions(remainder, s)]
    if not found: return None
    remainder = re.sub(r'\b(?:NASDAQ|NYSE|NYSEARCA|AMEX|OTC)\s*:', ' ', remainder, flags=re.I)
    for symbol in found:
        tickers = (symbol, 'GOOG') if symbol == 'GOOGL' else (symbol,)
        for ticker in tickers:
            remainder = re.sub(r'(?<![A-Za-z0-9])\$?' + re.escape(ticker) + r'(?![A-Za-z0-9])', ' ', remainder)
        for alias in sorted(ALIASES[symbol], key=len, reverse=True):
            remainder = re.sub(r'(?<!\w)' + re.escape(alias) + r'(?!\w)', ' ', remainder, flags=re.I)
    remainder = re.sub(r'\bClass\s+[AC]\b', ' ', remainder, flags=re.I)
    words = re.findall(r'[A-Za-z0-9]+', remainder.lower())
    if any(word not in {'inc', 'corp', 'co', 'corporation', 'plc', 'and'} for word in words):
        return None
    return match[1].lower()

def analyzer_metadata(engine):
    # Code bytes are included so a changed rule/input/adapter never silently reuses old results.
    from pathlib import Path
    root = Path(__file__).parent
    code = {name: hashlib.sha256((root / name).read_bytes()).hexdigest()
            for name in ('policy.py', 'sentiment.py', 'analysis.py', 'evidence.py', 'attribution.py', 'research_decision.py', 'unit_evidence.py')}
    from .research_decision import METHOD, MIN_MARGIN
    from .unit_evidence import METHOD as UNIT_METHOD, MAX_COMPANY_UNITS, MAX_ARTICLE_UNIT_TEXTS
    return {'unit_evidence_policy': {'method':UNIT_METHOD,'max_company_units':MAX_COMPANY_UNITS,'max_article_unique_unit_texts':MAX_ARTICLE_UNIT_TEXTS}, 'research_decision_policy': {'method': METHOD, 'min_top_label_margin': MIN_MARGIN, 'threshold_calibrated': False}, 'policy': POLICY_VERSION, 'field_limits': FIELD_LIMITS, 'aliases': ALIASES,
            'engine': engine.metadata, 'code_sha256': code,
            'sentiment_scope': 'document_and_filtered_company_context', 'language_assumption': 'English',
            'entity_context_policy': 'mention_context_with_subject_counterparty_guard_no_pronoun_resolution',
            'entity_sentiment_implemented': True, 'entity_attribution_validated': False,
            'price_impact_implemented': False}
