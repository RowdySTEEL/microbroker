"""Uncalibrated evidence diagnostics; never replace the raw model label."""
from collections import Counter
from .sentiment import RulesEngine

def evidence_summary(text, sentiment):
    rules = RulesEngine().score(text)
    windows = sentiment.get('chunks', [])
    counts = Counter(w['label'] for w in windows)
    polarities = set(counts)
    opposite_windows = {'positive', 'negative'}.issubset(polarities)
    opposite_rules = rules['label'] == 'mixed'
    mixed = opposite_windows or opposite_rules
    return {
        'status': 'mixed_evidence' if mixed else 'no_opposite_evidence_detected',
        'method': 'r8-window-labels-and-narrow-phrase-rules-v1',
        'raw_label': sentiment['label'],
        'window_label_counts': dict(sorted(counts.items())),
        'window_content_token_share': {
            label: sum(w['content_tokens'] for w in windows if w['label'] == label) /
                   sum(w['content_tokens'] for w in windows)
            for label in sorted(counts)},
        'opposite_window_labels': opposite_windows,
        'opposite_rule_phrases': opposite_rules,
        'rule_label': rules['label'],
        'rule_evidence_samples': rules['evidence'],
        'rule_negated_samples': rules['negated_evidence'],
        'rule_sample_limit': 30,
        'absence_is_not_neutral': True,
        'semantic_conflict_validated': False,
        'confidence_calibrated': False,
        'usable_for_trading': False,
    }
