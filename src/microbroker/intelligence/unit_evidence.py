"""Bounded accepted-context unit scores and lexical hints, not temporal resolution."""
from collections import Counter
import re
from .policy import digest
from .sentiment import validate_sentiment
from ..data.storage.repository import StorageError

METHOD = 'r18-accepted-context-unit-scores-v1'
MAX_COMPANY_UNITS = 64
MAX_ARTICLE_UNIT_TEXTS = 256

def context_hints(text):
    return {
        'time_reference_hint': bool(re.search(r'\b(?:yesterday|last (?:year|month|week|quarter)|20\d{2}|in (?:January|February|March|April|May|June|July|August|September|October|November|December))\b', text, re.I)),
        'past_event_hint': bool(re.search(r'\b(?:previously|last (?:year|month|week|quarter)|yesterday)\b|\bwhen\b.{0,100}\b(?:fell|dropped|declined)\b', text, re.I)),
        'uncertainty_hint': bool(re.search(r'\b(?:could|may|might|expects?|expected|forecasts?|potential|unclear|reportedly)\b', text, re.I)),
        'reported_view_hint': bool(re.search(r'\b(?:says?|said|analysts?|according to|believes?|highlighted|argues?|cited)\b', text, re.I)),
    }

def summarize_units(units, total):
    labels=Counter(u['sentiment']['label'] for u in units)
    markers=Counter(k for u in units for k,v in u['hints'].items() if v)
    return {'method':METHOD, 'selected_unit_count':total, 'scored_unit_count':len(units),
            'complete':len(units)==total, 'units':units,
            'unit_label_counts':dict(sorted(labels.items())),
            'opposite_unit_labels':{'positive','negative'}.issubset(labels),
            'contains_mixed_unit':bool(labels.get('mixed')),
            'hint_counts':dict(sorted(markers.items())),
            'scoring_limits':{'per_company':MAX_COMPANY_UNITS,'per_article_unique_unit_texts':MAX_ARTICLE_UNIT_TEXTS},
            'hint_semantics':'lexical_markers_not_resolved_dates_or_speakers',
            'temporal_resolution_validated':False, 'semantic_conflict_validated':False}

def score_units(context, score, cache, article_unit_texts):
    scored=[]
    for index,unit in enumerate(context['units'][:MAX_COMPANY_UNITS]):
        text=unit['text']
        if text not in cache:
            if len(article_unit_texts)>=MAX_ARTICLE_UNIT_TEXTS:
                break
            cache[text]=score(text)
            article_unit_texts.add(text)
        sentiment=cache[text];validate_sentiment(sentiment)
        scored.append({'unit_index':index, 'field':unit['field'],
                       'start_char':unit['start_char'], 'end_char':unit['end_char'],
                       'text_sha256':digest(text), 'hints':context_hints(text), 'sentiment':sentiment})
    return summarize_units(scored,len(context['units']))

def validate_unit_evidence(evidence, total):
    try:
        units=evidence['units']
        if type(total) is not int or total<0 or not isinstance(units,list) or len(units)>MAX_COMPANY_UNITS or len(units)>total:
            raise StorageError('Invalid company unit evidence coverage')
        for i,unit in enumerate(units):
            validate_sentiment(unit['sentiment'])
            if (type(unit['unit_index']) is not int or unit['unit_index']!=i or
                unit['field'] not in {'headline','summary','content_text'} or
                type(unit['start_char']) is not int or type(unit['end_char']) is not int or
                not 0<=unit['start_char']<unit['end_char'] or
                not re.fullmatch('[0-9a-f]{64}',unit['text_sha256']) or
                set(unit['hints'])!={'time_reference_hint','past_event_hint','uncertainty_hint','reported_view_hint'} or
                any(type(v) is not bool for v in unit['hints'].values())):
                raise StorageError('Invalid company unit evidence identity')
        if evidence!=summarize_units(units,total):
            raise StorageError('Company unit evidence summary mismatch')
    except (KeyError,TypeError,ValueError,AttributeError):
        raise StorageError('Invalid company unit evidence structure') from None

def verify_units_against_context(company, context):
    if (company['context_unit_count']!=len(context['units']) or
        company['context_sha256']!=digest('\n'.join(u['text'] for u in context['units']))):
        raise StorageError('Stored unit context cannot be reconstructed')
    for scored in company['unit_evidence']['units']:
        unit=context['units'][scored['unit_index']]
        if (any(scored[k]!=unit[k] for k in ('field','start_char','end_char')) or
            scored['text_sha256']!=digest(unit['text']) or scored['hints']!=context_hints(unit['text'])):
            raise StorageError('Stored unit evidence cannot be reconstructed')

def company_unit_decision(sentiment, evidence, context_status, units):
    from .research_decision import research_decision
    decision=research_decision(sentiment,evidence,context_status)
    decision={**decision,'method':'r18-company-unit-conflict-v1'}
    if context_status!='scored' or sentiment is None:
        return decision
    reasons=list(decision['reason_codes'])
    labels=set(units['unit_label_counts'])
    if units['opposite_unit_labels'] or units['contains_mixed_unit']:
        reasons.append('opposite_context_unit_labels')
    raw=sentiment['label']
    if (raw=='positive' and 'negative' in labels) or (raw=='negative' and 'positive' in labels):
        reasons.append('unit_polarity_opposes_aggregate')
    if not units['complete']:
        reasons.append('context_unit_scoring_incomplete')
    if 'context_unit_scoring_incomplete' in reasons:
        decision.update(status='incomplete_evidence',label=None)
    elif any(r in reasons for r in ('opposite_context_unit_labels','unit_polarity_opposes_aggregate')):
        decision.update(status='conflicting_evidence',label=None)
    decision['reason_codes']=reasons
    return decision
