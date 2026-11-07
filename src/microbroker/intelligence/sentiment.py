"""Explicit rules baseline and local FinBERT inference, without silent fallback."""
import math
import os
import re
from ..data.models import DataValidationError

class AnalysisError(RuntimeError):
    pass

# Narrow phrase patterns are a baseline, not a trained model or calibrated probability.
PATTERNS = {
    'positive': (
        r'\bbeat(?:s)? (?:earnings |revenue )?(?:estimates|expectations)\b',
        r'\b(?:raise[sd]?|raising) (?:its |full.year )?(?:guidance|outlook)\b',
        r'\brecord (?:revenue|profit|sales)\b', r'\bupgraded?\b',
        r'\b(?:profit|revenue|sales) (?:rose|grew|surged|increased)\b',
    ),
    'negative': (
        r'\bmiss(?:es|ed)? (?:earnings |revenue )?(?:estimates|expectations)\b',
        r'\b(?:cuts?|cutting|lowers?|lowered) (?:its |full.year )?(?:guidance|outlook)\b',
        r'\bdowngraded?\b', r'\b(?:bankruptcy|fraud|lawsuit|recall)\b',
        r'\b(?:profit|revenue|sales) (?:fell|declined|dropped|decreased)\b',
    ),
}

class RulesEngine:
    name = 'rules'
    metadata = {'name': name, 'version': 'r5-rules-v1', 'patterns': PATTERNS,
                'confidence_calibrated': False, 'trained_model': False}

    def score(self, text):
        evidence = []; negated = []
        for polarity, patterns in PATTERNS.items():
            for pattern in patterns:
                for match in re.finditer(pattern, text, re.I):
                    words = re.findall(r"[A-Za-z]+(?:'[A-Za-z]+)?", text[max(0, match.start()-60):match.start()].lower())[-3:]
                    item = {'polarity': polarity, 'phrase': match.group(0)[:100], 'offset': match.start()}
                    if any(w in {'not', 'no', 'never', 'without'} or w.endswith("n't") for w in words):
                        negated.append(item)
                    else:
                        evidence.append(item)
        pos = sum(e['polarity'] == 'positive' for e in evidence)
        neg = len(evidence)-pos
        label = 'mixed' if pos and neg else ('positive' if pos else ('negative' if neg else 'unscored'))
        return {'label': label, 'score': round((pos-neg)/(pos+neg), 8) if evidence else None,
                'probabilities': None, 'confidence': None, 'confidence_calibrated': False,
                'evidence': evidence[:30], 'negated_evidence': negated[:30],
                'token_truncated': False}

class FinBertEngine:
    name = 'finbert'

    def __init__(self, model_dir):
        from .model_files import verify_model_files
        manifest = verify_model_files(model_dir)
        try:
            import torch
            import transformers
            from transformers import AutoTokenizer, AutoModelForSequenceClassification
            if tuple(int(x) for x in torch.__version__.split('+')[0].split('.')[:2]) < (2, 6):
                raise AnalysisError('FinBERT requires torch>=2.6')
            self.torch = torch
            torch.set_num_threads(min(4, os.cpu_count() or 1))
            self.tokenizer = AutoTokenizer.from_pretrained(str(model_dir), local_files_only=True,
                                                           trust_remote_code=False, token=False)
            self.model = AutoModelForSequenceClassification.from_pretrained(
                str(model_dir), local_files_only=True, trust_remote_code=False, token=False,
                weights_only=True, use_safetensors=False, attn_implementation='eager')
            self.model.to('cpu'); self.model.eval()
            labels = {int(k): v.lower() for k, v in self.model.config.id2label.items()}
            if labels != {0: 'positive', 1: 'negative', 2: 'neutral'}:
                raise AnalysisError('Unexpected FinBERT label mapping')
            self.labels = labels
            self.metadata = {'name': self.name, 'model': manifest['model'],
                             'revision': manifest['revision'], 'files_sha256': manifest['files_sha256'],
                             'torch': torch.__version__, 'transformers': transformers.__version__,
                             'device': 'cpu', 'max_tokens': 512, 'truncation': 'prefix',
                             'cpu_threads': torch.get_num_threads(),
                             'confidence_calibrated': False, 'trained_model': True}
            self.metadata.update(document_policy='r6-disjoint-token-windows-v1',
                                 window_content_tokens=510, max_document_windows=64,
                                 aggregation='content_token_weighted_probability_mean')
        except AnalysisError:
            raise
        except ImportError:
            raise AnalysisError('Install R5 model dependencies: python3 -m pip install -r requirements-r5.txt') from None
        except Exception:
            raise AnalysisError('Local FinBERT load failed; check model files and R5 dependencies') from None

    def score(self, text):
        try:
            total = len(self.tokenizer.encode(text, add_special_tokens=True, truncation=False, verbose=False))
            inputs = self.tokenizer(text, return_tensors='pt', truncation=True, max_length=512)
            with self.torch.inference_mode():
                probs = self.torch.softmax(self.model(**inputs).logits, dim=-1)[0].tolist()
            probabilities = {self.labels[i]: float(p) for i, p in enumerate(probs)}
            label = max(probabilities, key=probabilities.get)
            result = {'label': label, 'score': probabilities['positive']-probabilities['negative'],
                      'probabilities': probabilities, 'confidence': probabilities[label],
                      'confidence_calibrated': False, 'token_count': total,
                      'tokens_used': min(total, 512), 'token_truncated': total > 512}
            validate_sentiment(result)
            return result
        except AnalysisError:
            raise
        except Exception:
            raise AnalysisError('Local FinBERT inference failed; no partial analysis committed') from None

    def score_document(self, text):
        """Cover every token of the bounded input exactly once; never decode/re-tokenize."""
        try:
            ids = self.tokenizer.encode(text, add_special_tokens=False, truncation=False, verbose=False)
            windows = token_windows(ids)
            results = []
            for start, end in windows:
                inputs = self.tokenizer.prepare_for_model(
                    ids[start:end], add_special_tokens=True, return_tensors='pt',
                    prepend_batch_axis=True, truncation=False, return_attention_mask=True)
                with self.torch.inference_mode():
                    probs = self.torch.softmax(self.model(**inputs).logits, dim=-1)[0].tolist()
                probabilities = {self.labels[i]: float(p) for i, p in enumerate(probs)}
                label = max(probabilities, key=probabilities.get)
                result = {'label': label, 'score': probabilities['positive']-probabilities['negative'],
                          'probabilities': probabilities, 'confidence': probabilities[label]}
                validate_sentiment(result)
                results.append({'start_token': start, 'end_token': end,
                                'content_tokens': end-start, **result})
            return aggregate_windows(results, len(ids))
        except (AnalysisError, DataValidationError):
            raise
        except Exception:
            raise AnalysisError('Local FinBERT document inference failed; no partial analysis committed') from None

def token_windows(ids, width=510, max_windows=64):
    if not ids or width < 1 or max_windows < 1:
        raise AnalysisError('Document needs nonempty token input and valid window bounds')
    if len(ids) > width*max_windows:
        raise AnalysisError('Document exceeds bounded token window capacity; no tokens silently omitted')
    return [(start, min(start+width, len(ids))) for start in range(0, len(ids), width)]

def aggregate_windows(windows, token_count):
    if not windows or token_count < 1:
        raise DataValidationError('Empty window aggregate')
    position = 0
    for window in windows:
        validate_sentiment(window)
        if (window['start_token'] != position or window['end_token'] <= position or
                window['content_tokens'] != window['end_token']-position):
            raise DataValidationError('Invalid token window coverage')
        position = window['end_token']
    if position != token_count:
        raise DataValidationError('Incomplete token window coverage')
    probabilities = {label: sum(w['probabilities'][label]*w['content_tokens'] for w in windows)/token_count
                     for label in ('positive', 'negative', 'neutral')}
    label = max(probabilities, key=probabilities.get)
    result = {'label': label, 'score': probabilities['positive']-probabilities['negative'],
              'probabilities': probabilities,
              'probability_semantics': 'weighted_mean_of_window_softmax_not_document_probability',
              'confidence': probabilities[label] if len(windows) == 1 else None,
              'confidence_calibrated': False, 'aggregation': 'content_token_weighted_probability_mean',
              'content_token_count': token_count, 'covered_content_tokens': token_count,
              'token_count': token_count+2, 'tokens_used': token_count+2*len(windows),
              'token_truncated': False, 'chunk_count': len(windows), 'chunks': windows,
              'mixed_chunk_polarity': {'positive', 'negative'}.issubset({w['label'] for w in windows})}
    validate_sentiment(result)
    return result

def validate_sentiment(result):
    def finite(n, low, high):
        return isinstance(n, (int, float)) and not isinstance(n, bool) and math.isfinite(n) and low <= n <= high
    if result.get('label') not in {'positive', 'negative', 'neutral', 'mixed', 'unscored'}:
        raise DataValidationError('Invalid sentiment result')
    score = result.get('score')
    if result['label'] == 'unscored':
        if score is not None: raise DataValidationError('Unscored sentiment must have no score')
    elif not finite(score, -1, 1):
        raise DataValidationError('Invalid sentiment score')
    probs = result.get('probabilities')
    if probs is not None:
        aggregated_without_confidence = (result.get('confidence') is None and
            result.get('aggregation') == 'content_token_weighted_probability_mean' and
            result.get('chunk_count', 0) > 1)
        valid_confidence = (finite(result.get('confidence'), 0, 1) and
            finite(probs.get(result['label']), 0, 1) and
            abs(result['confidence']-probs.get(result['label'], float('inf'))) <= 1e-7)
        if (set(probs) != {'positive', 'negative', 'neutral'} or
                not all(finite(n, 0, 1) for n in probs.values()) or
                abs(sum(probs.values())-1) > 1e-5 or result['label'] != max(probs, key=probs.get) or
                abs(score-(probs['positive']-probs['negative'])) > 1e-7 or
                not (aggregated_without_confidence or valid_confidence)):
            raise DataValidationError('Invalid sentiment probabilities')
    elif result.get('confidence') is not None:
        raise DataValidationError('Rules confidence must be unspecified')

def make_engine(name, model_dir='models/finbert'):
    if name == 'rules': return RulesEngine()
    if name == 'finbert': return FinBertEngine(model_dir)
    raise DataValidationError('Unknown sentiment engine')
