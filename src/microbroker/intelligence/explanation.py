"""Inspect stored input/context evidence; does not explain model internals or infer."""
import json
from .research_decision import validate_research_decisions
from .policy import prepare_text, company_contexts, company_contexts_v8, digest
from ..data.models import DataValidationError
from ..config import UNIVERSE
from ..data.research import begin_read
from ..data.storage.repository import StorageError

def explain_analysis(repository, article_id, *, symbol=None, revision=None,
                     analyzer_key=None, engine='finbert', provider='alpaca'):
    if type(article_id) is not int or article_id<1 or provider not in {'alpaca','alpaca-sandbox'}:
        raise DataValidationError('Invalid article identity')
    if symbol is not None and symbol not in UNIVERSE:raise DataValidationError('Unknown explanation symbol')
    m=repository.marker;begin_read(repository)
    try:
        query=f'SELECT v.revision_hash,v.first_seen_at,v.data_json FROM news_versions v '
        if revision is None:
            query+='JOIN news_articles a ON a.provider=v.provider AND a.article_id=v.article_id AND a.revision_hash=v.revision_hash '
        query+=f'WHERE v.provider={m} AND v.article_id={m}'
        params=[provider,article_id]
        if revision is not None:query+=f' AND v.revision_hash={m}';params.append(revision)
        row=repository.connection.execute(query,params).fetchone()
        if not row:raise DataValidationError('Article revision not found')
        revision,first_seen,raw=row;data=json.loads(raw)
        if digest(data)!=revision or data.get('id')!=article_id or data.get('provider')!=provider:
            raise StorageError('News revision integrity check failed')
        mappings=dict(repository.connection.execute(f'SELECT symbol,method FROM news_symbols WHERE provider={m} AND article_id={m} AND revision_hash={m}',(provider,article_id,revision)).fetchall())
        if symbol is not None and symbol not in mappings:raise DataValidationError('Article has no mapping for requested symbol')
        query=f'SELECT analyzer_key,analyzed_at,result_json,result_sha256 FROM news_analyses WHERE provider={m} AND article_id={m} AND revision_hash={m} AND engine={m}'
        params=[provider,article_id,revision,engine]
        if analyzer_key is not None:query+=f' AND analyzer_key={m}';params.append(analyzer_key)
        analysis=repository.connection.execute(query+' ORDER BY analyzed_at DESC,analyzer_key LIMIT 1',params).fetchone()
        if not analysis:raise DataValidationError('No stored analysis for this revision and engine')
        key,analyzed_at,raw,sha=analysis;result=json.loads(raw)
        if digest(result)!=sha:raise StorageError('Stored analysis integrity check failed')
        text,fields,basis,truncated=prepare_text(data)
        if result.get('input_sha256')!=digest({'text':text,'basis':basis,'mappings':mappings}):
            raise StorageError('Stored analysis input cannot be reconstructed with current policy')
        validate_research_decisions(result)
        fmt=result.get('analysis_format')
        if fmt not in {'R8','R16','R17','R18'}:raise DataValidationError('Full context explanation requires R8/R16/R17/R18 stored analysis')
        contexts=company_contexts(fields,mappings) if fmt in {'R16','R17','R18'} else company_contexts_v8(fields,mappings)
        raw_contexts=company_contexts_v8(fields,mappings);companies={}
        for s,context in contexts.items():
            if symbol is not None and s!=symbol:continue
            stored=result.get('entity_sentiments',{}).get(s)
            context_text='\n'.join(unit['text'] for unit in context['units'])
            if (not stored or digest(context_text)!=stored.get('context_sha256') or
                len(context['units'])!=stored.get('context_unit_count')):
                raise StorageError('Stored company context cannot be reconstructed with current policy')
            if fmt in {'R16','R17','R18'}:
                raw_text='\n'.join(unit['text'] for unit in raw_contexts[s]['units'])
                if (digest(raw_text)!=stored.get('raw_mention_context_sha256') or
                    digest(context['ambiguous_units'])!=stored.get('excluded_context_sha256')):
                    raise StorageError('Stored attribution guard evidence cannot be reconstructed')
            if fmt=='R18':
                from .unit_evidence import verify_units_against_context
                verify_units_against_context(stored,context)
            companies[s]={'status':stored['status'],'sentiment':stored['sentiment'],
                'context_sha256':stored['context_sha256'],'context_verified':True,
                'context_unit_count':len(context['units']),'units':context['units'],
                'shared_units_excluded':context['shared_units_excluded'],
                'insufficient_units_excluded':context['insufficient_units_excluded'],
                'attribution_guard_applied':fmt in {'R16','R17','R18'},
                **({'ambiguous_units_excluded':context['ambiguous_units_excluded'],
                    'excluded_units':context['ambiguous_units'],
                    'raw_mention_sentiment':stored['raw_mention_sentiment'],
                    'raw_mention_context_verified':True,
                    'raw_mention_sentiment_usable_for_alignment':False} if fmt in {'R16','R17','R18'} else {}),
                **({'research_decision':stored['research_decision']} if fmt in {'R17','R18'} else {}),
                **({'unit_evidence':{**stored['unit_evidence'],'units':[{**scored,'text':context['units'][scored['unit_index']]['text']} for scored in stored['unit_evidence']['units']]}, 'unit_evidence_verified':True} if fmt=='R18' else {}),
                'stored_evidence_summary':stored.get('evidence_summary'),
                'document_company_label_difference':stored['sentiment']['label']!=result['sentiment']['label'] if stored['sentiment'] else None}
        repository.connection.commit()
    except BaseException:repository.connection.rollback();raise
    return {'status':'ready','article_id':article_id,'revision_hash':revision,'first_seen_at':first_seen,
        'analyzer_key':key,'analyzed_at':analyzed_at,'engine':engine,'analysis_format':fmt,
        'text':fields,'text_basis':basis,'character_truncated_fields':truncated,
        **({'document_research_decision':result['research_decision']} if fmt in {'R17','R18'} else {}),
        'document_sentiment':result['sentiment'],'companies':companies,'flags':result['flags'],
        'stored_result_sha256':sha,'input_verified':True,'database_modified':False,
        'inference_performed':False,'network_required':False,'model_required':False,
        'explanation_scope':'verified_model_input_context_not_internal_model_reasoning',
        'model_accuracy_validated':False,'entity_attribution_validated':False,'confidence_calibrated':False}
