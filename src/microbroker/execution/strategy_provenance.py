"""Canonical receipt provenance on simulation ledger events; no claim of real fills."""
import re
from ..data.stream_models import canonical_time
from ..data.alignment import nanos
from ..data.storage.repository import StorageError

def validate_provenance(request):
    try:
        p=request['provenance']
        keys={'format','analysis_replay_id','bars_replay_id','analysis_dataset_sha256',
              'bars_dataset_sha256','stream_run_id','receipt_ordinal','receipt_event_key',
              'received_at','bar_start','signal'}
        if set(p)!=keys or p['format']!='R20':raise ValueError
        for key in ('analysis_replay_id','bars_replay_id','stream_run_id'):
            if not isinstance(p[key],str) or not 1<=len(p[key])<=128:raise ValueError
        for key in ('analysis_dataset_sha256','bars_dataset_sha256','receipt_event_key'):
            if not isinstance(p[key],str) or not re.fullmatch('[0-9a-f]{64}',p[key]):raise ValueError
        if type(p['receipt_ordinal']) is not int or p['receipt_ordinal']<1:raise ValueError
        for key in ('received_at','bar_start'):
            if canonical_time(p[key])!=p[key]:raise ValueError
        if (p['bar_start'][17:29]!='00.000000000' or request['at']!=p['received_at'] or
            nanos(p['received_at'])<nanos(p['bar_start'])+60000000000):raise ValueError
        signal=p['signal']
        if request['kind']=='fill':
            if set(signal)!={'article_id','revision_hash','available_at','company_label','analyzer_key'}:raise ValueError
            if type(signal['article_id']) is not int or signal['article_id']<1:raise ValueError
            for key in ('revision_hash','analyzer_key'):
                if not re.fullmatch('[0-9a-f]{64}',signal[key]):raise ValueError
            if (canonical_time(signal['available_at'])!=signal['available_at'] or
                signal['available_at']>p['bar_start'] or signal['available_at']>=p['received_at'] or
                signal['company_label']!=('positive' if request['side']=='buy' else 'negative')):raise ValueError
        elif signal is not None:raise ValueError
    except (KeyError,TypeError,ValueError,AttributeError):
        raise StorageError('Invalid strategy price/signal provenance') from None

def attach(request,source,bar,signal=None):
    receipt=bar['receipt']
    p={'format':'R20',**{k:source[k] for k in ('analysis_replay_id','bars_replay_id',
       'analysis_dataset_sha256','bars_dataset_sha256','stream_run_id')},
       'receipt_ordinal':receipt['ordinal'],'receipt_event_key':receipt['event_key'],
       'received_at':receipt['received_at'],'bar_start':bar['timestamp'],'signal':signal}
    request={**request,'price_source':'recorded_receipt_simulation_assumption','provenance':p}
    validate_provenance(request)
    return request
