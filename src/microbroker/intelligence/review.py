"""Frozen, model-blind review inputs and explicit reference-label metrics; no inference."""
from collections import Counter
import json
from pathlib import Path
import shutil
import tempfile
from .analysis import select_news, analysis_summary
from .policy import digest, prepare_text
from ..data.models import DataValidationError
from ..data.research import begin_read
from ..data.storage.repository import StorageError
from .review_ui import review_html
from .research_decision import validate_research_decisions

LABELS = ('positive', 'negative', 'neutral', 'mixed')
SOURCES = ('human', 'ai_assisted', 'synthetic')
MAX_FILE_BYTES = 32*1024*1024

def export_review(repository, request, output, *, analyzer_key=None, engine='finbert'):
    folder = Path(output)
    if folder.exists(): raise DataValidationError('Review output already exists; choose a new folder')
    key = analysis_summary(repository, request, analyzer_key=analyzer_key, engine=engine)['analyzer_key']
    if key is None: raise DataValidationError('No completed analysis available for review')
    articles = select_news(repository, request)
    if not articles: raise DataValidationError('No news available for review')
    inputs = []; predictions = {}; m = repository.marker
    begin_read(repository)
    try:
        for article in articles:
            row = repository.connection.execute(
                f'SELECT result_json,result_sha256 FROM news_analyses WHERE provider={m} AND article_id={m} AND revision_hash={m} AND analyzer_key={m} AND engine={m}',
                (request.provider,article['id'],article['revision'],key,engine)).fetchone()
            if row is None: raise DataValidationError('Review needs complete analyses for selected current revisions')
            result = json.loads(row[0])
            if digest(result) != row[1]: raise StorageError('Stored analysis integrity check failed')
            validate_research_decisions(result)
            _, fields, basis, truncated = prepare_text(article['data'])
            symbols = sorted(s for s in article['mappings'] if not request.symbols or s in request.symbols)
            item = {'id':article['id'],'revision_hash':article['revision'],'text':fields,
                    'text_basis':basis,'character_truncated_fields':truncated,'symbols':symbols,
                    'first_seen_at':article['first_seen']}
            inputs.append(item)
            predictions[str(article['id'])] = {
                'document':result['sentiment']['label'],
                **({'research_decision':result['research_decision']} if result.get('analysis_format') in {'R17','R18'} else {}),
                'evidence_summary':result.get('evidence_summary'),
                'companies':{s:{'status':context['status'],'label':context['sentiment']['label'] if context['sentiment'] else None,
                              **({'research_decision':context['research_decision']} if result.get('analysis_format') in {'R17','R18'} else {})}
                             for s,context in result.get('entity_sentiments',{}).items() if s in symbols}}
        repository.connection.commit()
    except BaseException:
        repository.connection.rollback();raise
    manifest = {'format':'microbroker-review-v1','provider':request.provider,'engine':engine,
                'analyzer_key':key,'inputs':inputs,'predictions':predictions,
                'selection':'all_selected_current_articles_not_random_population_sample',
                'sentiment_reference_scope':'displayed_bounded_document_text',
                'company_reference_scope':'reviewer_company_attribution_from_displayed_text',
                'model_accuracy_validated':False,'confidence_calibrated':False}
    reference = {'payload':manifest,'sha256':digest(manifest)}
    review = {'format':'microbroker-review-v1','reference_sha256':reference['sha256'],
              'instructions':'Label before opening reference.json. Null means not reviewed or uncertain; never means neutral. Source/reviewer required for every labeled article.',
              'articles':[{**item,'labels':{'document':None,'companies':{s:None for s in item['symbols']},
                           'source':None,'reviewer':None,'notes':'','prediction_seen':None}} for item in inputs]}
    encoded = [json.dumps(value,ensure_ascii=False,indent=2)+'\n' for value in (review,reference)]
    if any(len(value.encode()) > MAX_FILE_BYTES for value in encoded):
        raise DataValidationError('Review file exceeds 32 MiB limit; narrow symbols')
    page=review_html(review)
    if len(page.encode())>MAX_FILE_BYTES:
        raise DataValidationError('Review page exceeds 32 MiB limit; narrow symbols')
    folder.parent.mkdir(parents=True,exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix='.microbroker-review-',dir=folder.parent))
    try:
        for name,value in zip(('review.json','reference.json'),encoded):
            (temporary/name).write_text(value,encoding='utf-8')
        (temporary/'review.html').write_text(page,encoding='utf-8')
        # rename fails if a nonempty destination appeared; never overwrite an existing bundle.
        if folder.exists(): raise DataValidationError('Review output already exists')
        temporary.rename(folder)
    finally:
        if temporary.exists():shutil.rmtree(temporary)
    return {'status':'exported','articles':len(inputs),'analyzer_key':key,'output':str(folder),
            'reference_sha256':reference['sha256'],'model_predictions_in_review_file':False,
            'database_modified':False,'model_accuracy_validated':False,'labeling_page':'review.html',
            'model_predictions_in_labeling_page':False}

def read_json(path):
    path=Path(path)
    if not path.is_file() or path.stat().st_size > MAX_FILE_BYTES:
        raise DataValidationError('Review file missing or exceeds 32 MiB')
    try:return json.loads(path.read_text(encoding='utf-8'))
    except (ValueError,UnicodeError):raise DataValidationError('Invalid review JSON') from None

def metrics(pairs):
    total=len(pairs);answered=sum(predicted is not None for gold,predicted in pairs)
    correct=sum(gold==predicted for gold,predicted in pairs)
    matrix=Counter(pairs);per_class={}
    for label in LABELS:
        tp=matrix[(label,label)];support=sum(g==label for g,p in pairs)
        fp=sum(g!=label and p==label for g,p in pairs);fn=support-tp
        precision=tp/(tp+fp) if tp+fp else None
        recall=tp/support if support else None
        f1=2*tp/(2*tp+fp+fn) if 2*tp+fp+fn else None
        per_class[label]={'support':support,'precision':precision,'recall':recall,'f1':f1}
    supported=[v['f1'] or 0 for v in per_class.values() if v['support']]
    return {'labeled':total,'predicted':answered,'abstained':total-answered,
            'coverage':answered/total if total else None,'accuracy_including_abstentions':correct/total if total else None,
            'accuracy_when_predicted':correct/answered if answered else None,
            'macro_f1_over_reference_present_classes':sum(supported)/len(supported) if supported else None,
            'per_class':per_class,'confusion':[{ 'reference':g,'prediction':p,'count':n}
                for (g,p),n in sorted(matrix.items(),key=lambda x:(x[0][0],x[0][1] or ''))]}

def evaluate_review(folder, labels_file=None):
    folder=Path(folder);review=read_json(labels_file if labels_file is not None else folder/'review.json');reference=read_json(folder/'reference.json')
    try:
        payload=reference['payload']
        if (payload['format']!='microbroker-review-v1' or review['format']!=payload['format'] or
                digest(payload)!=reference['sha256'] or review['reference_sha256']!=reference['sha256']):
            raise DataValidationError('Review reference integrity check failed')
        articles=review['articles']
        if len(articles)!=len(payload['inputs']):raise DataValidationError('Review article set changed')
        documents=[];companies={};sources=Counter();human_documents=[];human_companies={}
        effective_documents=[];effective_companies={};effective_human_documents=[];effective_human_companies={}
        effective_blind_documents=[];effective_blind_companies={};decision_counts=Counter();gated_articles=0
        blind_documents=[];blind_companies={};exposure_counts=Counter();human_exposure_counts=Counter()
        for item,frozen in zip(articles,payload['inputs']):
            if {k:v for k,v in item.items() if k!='labels'}!=frozen:
                raise DataValidationError('Review text or article identity changed')
            labels=item['labels'];document=labels['document'];company=labels['companies']
            if set(company)!=set(frozen['symbols']):raise DataValidationError('Review company set changed')
            values=[document,*company.values()]
            if any(value is not None and value not in LABELS for value in values):
                raise DataValidationError('Unknown review label; use positive/negative/neutral/mixed or null')
            labeled=any(value is not None for value in values)
            seen=labels.get('prediction_seen')
            if seen is not None and type(seen) is not bool:raise DataValidationError('prediction_seen must be true/false/null')
            if labeled and (labels['source'] not in SOURCES or not isinstance(labels['reviewer'],str) or not labels['reviewer'].strip()):
                raise DataValidationError('Labeled articles require source and reviewer')
            prediction=payload['predictions'][str(item['id'])]
            if 'research_decision' in prediction:
                gated_articles += 1
                decision_counts[prediction['research_decision']['status']] += 1
            if labeled:
                sources[labels['source']]+=1
                exposure='seen' if seen is True else 'not_seen' if seen is False else 'unknown'
                exposure_counts[exposure]+=1
                if labels['source']=='human':human_exposure_counts[exposure]+=1
            if document is not None:
                guess=prediction['document']
                pair=(document,guess if guess in LABELS else None);documents.append(pair)
                if labels['source']=='human':human_documents.append(pair)
                if labels['source']=='human' and seen is False:blind_documents.append(pair)
                if 'research_decision' in prediction:
                    decision_label=prediction['research_decision']['label']
                    effective_pair=(document,decision_label if decision_label in LABELS else None)
                    effective_documents.append(effective_pair)
                    if labels['source']=='human':effective_human_documents.append(effective_pair)
                    if labels['source']=='human' and seen is False:effective_blind_documents.append(effective_pair)
            for symbol,gold in company.items():
                if gold is None:continue
                guessed=prediction['companies'].get(symbol,{}).get('label')
                pair=(gold,guessed if guessed in LABELS else None)
                companies.setdefault(symbol,[]).append(pair)
                if labels['source']=='human':human_companies.setdefault(symbol,[]).append(pair)
                if labels['source']=='human' and seen is False:blind_companies.setdefault(symbol,[]).append(pair)
                company_prediction=prediction['companies'].get(symbol,{})
                if 'research_decision' in company_prediction:
                    effective_label=company_prediction['research_decision']['label']
                    effective_pair=(gold,effective_label if effective_label in LABELS else None)
                    effective_companies.setdefault(symbol,[]).append(effective_pair)
                    if labels['source']=='human':effective_human_companies.setdefault(symbol,[]).append(effective_pair)
                    if labels['source']=='human' and seen is False:effective_blind_companies.setdefault(symbol,[]).append(effective_pair)
        return {'status':'evaluated' if sources else 'awaiting_labels','articles':len(articles),
                'document_label_coverage':len(documents)/len(articles) if articles else None,
                'reference_sha256':reference['sha256'],'analyzer_key':payload['analyzer_key'],
                'source_article_counts':dict(sources),'document':metrics(documents),
                'document_metrics_semantics':'raw_model_labels',
                'research_decision':{'gated_articles':gated_articles,'legacy_articles_not_reinterpreted':len(articles)-gated_articles,
                    'status_counts':dict(decision_counts),'document':metrics(effective_documents),
                    'companies':{s:metrics(pairs) for s,pairs in sorted(effective_companies.items())},
                    'human_only':{'document':metrics(effective_human_documents),'companies':{s:metrics(pairs) for s,pairs in sorted(effective_human_companies.items())}},
                    'human_blind':{'document':metrics(effective_blind_documents),'companies':{s:metrics(pairs) for s,pairs in sorted(effective_blind_companies.items())}},
                    'threshold_calibrated':False,'abstention_is_not_correct_prediction':True},
                'companies':{s:metrics(pairs) for s,pairs in sorted(companies.items())},
                'human_only':{'document':metrics(human_documents),'companies':{s:metrics(pairs) for s,pairs in sorted(human_companies.items())}},
                'human_blind':{'document':metrics(blind_documents),'companies':{s:metrics(pairs) for s,pairs in sorted(blind_companies.items())}},
                'prediction_exposure_article_counts':dict(exposure_counts),
                'human_prediction_exposure_article_counts':dict(human_exposure_counts),
                'prediction_exposure':'self_reported_not_independently_verified',
                'selection':payload['selection'],'provenance':'reviewer_source_self_reported_not_independently_verified',
                'reference_integrity':'digest_detects_changes_not_authenticity','database_modified':False,
                'model_accuracy_validated':False,'confidence_calibrated':False}
    except (KeyError,TypeError,AttributeError):
        raise DataValidationError('Invalid review bundle structure') from None
