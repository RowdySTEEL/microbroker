"""Conservative research eligibility; preserves model outputs, never a trading signal."""
from decimal import Decimal
from ..data.storage.repository import StorageError
from ..data.models import DataValidationError
from .sentiment import validate_sentiment

METHOD = 'r17-conflict-and-margin-abstention-v1'
MIN_MARGIN = 0.15  # Existing ambiguity flag threshold; heuristic, not calibrated.

def research_decision(sentiment, evidence=None, context_status='scored'):
    raw_label = sentiment.get('label') if sentiment else None
    probabilities = sentiment.get('probabilities') if sentiment else None
    margin = None
    if probabilities:
        values = sorted(probabilities.values(), reverse=True)
        margin = float(Decimal(str(values[0])) - Decimal(str(values[1])))
    reasons = []
    if context_status != 'scored' or sentiment is None:
        status = 'context_unavailable'; reasons.append(context_status)
    elif raw_label == 'mixed':
        status = 'conflicting_evidence'; reasons.append('opposite_evidence_detected')
    elif raw_label not in {'positive', 'negative', 'neutral'}:
        status = 'unscored'; reasons.append('no_single_model_label')
    else:
        opposite_windows = {'positive','negative'}.issubset({w['label'] for w in sentiment.get('chunks',[])})
        if opposite_windows or sentiment.get('mixed_chunk_polarity') or (evidence or {}).get('status') == 'mixed_evidence':
            reasons.append('opposite_evidence_detected')
        if margin is not None and margin < MIN_MARGIN:
            reasons.append('top_label_margin_below_heuristic')
        status = ('conflicting_evidence' if 'opposite_evidence_detected' in reasons else
                  'ambiguous_model_output' if reasons else 'accepted')
    return {'status': status, 'label': raw_label if status == 'accepted' else None,
            'raw_label': raw_label, 'reason_codes': reasons, 'top_label_margin': margin,
            'method': METHOD, 'min_top_label_margin': MIN_MARGIN,
            'threshold_calibrated': False, 'model_accuracy_validated': False,
            'usable_for_trading': False}

def validate_research_decisions(result):
    """Fail closed for R17; never apply the new policy to legacy stored results."""
    if result.get('analysis_format') not in {'R17','R18'}:
        return
    try:
        validate_sentiment(result['sentiment'])
        expected = research_decision(result['sentiment'], result.get('evidence_summary'))
        if result.get('research_decision') != expected:
            raise StorageError('Stored document research decision mismatch')
        for company in result.get('entity_sentiments', {}).values():
            if company.get('sentiment') is not None:
                validate_sentiment(company['sentiment'])
            if result.get('analysis_format')=='R18':
                from .unit_evidence import validate_unit_evidence, company_unit_decision
                from .policy import digest
                units=company['unit_evidence']
                validate_unit_evidence(units,company['context_unit_count'])
                if company.get('unit_evidence_sha256')!=digest(units):
                    raise StorageError('Company unit evidence digest mismatch')
                expected=company_unit_decision(company.get('sentiment'),company.get('evidence_summary'),company.get('status','not_present'),units)
            else:
                expected = research_decision(company.get('sentiment'), company.get('evidence_summary'),
                                             company.get('status', 'not_present'))
            if company.get('research_decision') != expected:
                raise StorageError('Stored company research decision mismatch')
    except (KeyError, TypeError, ValueError, AttributeError, IndexError, DataValidationError):
        raise StorageError('Invalid stored research decision structure') from None

def company_research_label(result, company):
    if result.get('analysis_format') in {'R17','R18'}:
        validate_research_decisions(result)
        return company['research_decision']['label']
    sentiment = company.get('sentiment')
    return sentiment.get('label') if sentiment else None
