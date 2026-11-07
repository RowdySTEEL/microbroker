"""Bounded GET-only news-window inspection, without storing arrivals or running inference."""
from collections import Counter
from datetime import datetime,timezone,timedelta
from .news import AlpacaNews,NewsRequest
from .models import DataValidationError
from .stream_models import canonical_time
from .alignment import nanos
from ..config import UNIVERSE


def inspect_news(settings,symbols=('AAPL','MSFT'),lookback_minutes=60,*,adapter_factory=AlpacaNews,now=None):
    settings.require_credentials()
    if settings.data_env!='production':raise DataValidationError('News inspection requires production market data')
    if type(lookback_minutes) is not int or not 1<=lookback_minutes<=1440:raise DataValidationError('News inspection lookback must be 1..1440 minutes')
    end=now or datetime.now(timezone.utc);request=NewsRequest(tuple(symbols),end-timedelta(minutes=lookback_minutes),end,50,5)
    if not set(request.symbols)<=set(UNIVERSE):raise DataValidationError('Inspect configured research symbols only')
    adapter=adapter_factory(settings);seen=set();received=0;counts=Counter();samples=[];first=last=None;recent=0
    end_stamp=canonical_time(end.isoformat())
    for article in adapter.iter_news(request):
        received+=1
        if received>250:raise DataValidationError('News inspection exceeds 250 article-version responses; narrow the interval')
        data=article['data'];identity=(article['id'],article['revision_hash'])
        if identity in seen:continue
        seen.add(identity);counts.update(set(article['mappings'])&set(request.symbols))
        published=data['created_at'];first=min(first or published,published);last=max(last or published,published)
        if 0<=nanos(end_stamp)-nanos(published)<=900000000000:recent+=1
        samples.append({'article_id':article['id'],'headline':data['headline'][:500],
            'published_at':published,'updated_at':data['updated_at'],'symbols':sorted(set(article['mappings'])&set(request.symbols)),
            'provider_symbols':data['symbols'],'has_content':bool(data['content_text'])})
        samples=sorted(samples,key=lambda s:(s['published_at'],s['updated_at'],s['article_id']))[-5:]
    return {'status':'queried','response':'articles_returned' if received else 'empty_response',
        'provider':'alpaca','symbols':list(request.symbols),'query_start':canonical_time(request.start.isoformat()),
        'query_end':canonical_time(end.isoformat()),'lookback_minutes':lookback_minutes,'received_articles':received,
        'unique_articles':len({identity[0] for identity in seen}),'unique_article_versions':len(seen),'symbol_counts':dict(counts),'symbol_counts_unit':'unique_article_versions','pages':adapter.pages,
        'versions_published_within_last_15_minutes':recent,'first_publication_returned':first,'last_publication_returned':last,'samples':samples,
        'sample_limit':5,'sample_selection':'five_latest_publication_timestamps_in_returned_response',
        'database_modified':False,'arrival_times_recorded':False,'inference_performed':False,
        'network_required':True,'orders_sent':0,'scope':'provider_response_for_requested_window_not_independent_completeness_check'}
