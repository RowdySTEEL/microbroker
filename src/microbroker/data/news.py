"""Provider-tag/cashtag news ingestion. No sentiment, orders or webpage fetching."""
from dataclasses import dataclass
from datetime import datetime,timezone
from html.parser import HTMLParser
import hashlib
import json
import logging
import re
from urllib.parse import urlsplit,urlunsplit,parse_qsl,urlencode
import uuid
from ..config import UNIVERSE
from .models import DataValidationError,validate_symbols
from .stream_models import canonical_time
from .adapters.alpaca import AlpacaHistoricalData,MarketDataError
from .research import encode,begin_read

log=logging.getLogger('microbroker.news')

@dataclass(frozen=True)
class NewsRequest:
    symbols: tuple
    start: datetime
    end: datetime
    limit: int = 50
    max_pages: int = 100

    def __post_init__(self):
        object.__setattr__(self,'symbols',validate_symbols(self.symbols))
        if any(d.tzinfo is None or d.utcoffset() is None for d in (self.start,self.end)) or self.start>=self.end:
            raise DataValidationError('News requires start < end with timezone-aware boundaries')
        if any(isinstance(n,bool) or not isinstance(n,int) for n in (self.limit,self.max_pages)) or not 1<=self.limit<=50 or not 1<=self.max_pages<=200:
            raise DataValidationError('News limit 1..50, max_pages 1..200')
    def metadata(self):
        return {'symbols':self.symbols,'start':self.start.isoformat(),'end':self.end.isoformat(),
                'limit':self.limit,'max_pages':self.max_pages,'sort':'asc','include_content':True,
                'exclude_contentless':False,'interval':'provider_inclusive'}

class TextParser(HTMLParser):
    def __init__(self):super().__init__(convert_charrefs=True);self.parts=[];self.hidden=0
    def handle_starttag(self,tag,attrs):
        if tag in {'script','style'}:self.hidden+=1
        elif tag in {'p','br','div','li','h1','h2','h3'} and not self.hidden:self.parts.append(' ')
    def handle_endtag(self,tag):
        if tag in {'script','style'}:self.hidden=max(0,self.hidden-1)
        elif tag in {'p','div','li'} and not self.hidden:self.parts.append(' ')
    def handle_data(self,text):
        if not self.hidden:self.parts.append(text)

def text(value,limit,required=False):
    if value is None:value=''
    if not isinstance(value,str) or len(value)>limit:raise ValueError
    if required and not value.strip():raise ValueError
    return value.strip()

def plain_html(value):
    parser=TextParser();parser.feed(value);parser.close()
    return ' '.join(''.join(parser.parts).split())

def canonical_url(value):
    if not value:return ''
    parts=urlsplit(value)
    if parts.scheme not in {'http','https'} or not parts.hostname or parts.username or parts.password:raise ValueError
    query=[(k,v) for k,v in parse_qsl(parts.query,keep_blank_values=True) if not k.lower().startswith('utm_') and k.lower() not in {'fbclid','gclid'}]
    return urlunsplit((parts.scheme,parts.netloc.lower(),parts.path,urlencode(sorted(query)),''))

def normalize_news(row, *, provider='alpaca'):
    try:
        if provider not in {'alpaca','alpaca-sandbox'}:raise ValueError
        article_id=row['id']
        if isinstance(article_id,bool) or not isinstance(article_id,int) or not 1<=article_id<=9223372036854775807:raise ValueError
        created=canonical_time(row['created_at']);updated=canonical_time(row['updated_at'])
        if updated<created:raise ValueError
        headline=plain_html(text(row['headline'],10000,True))
        if not headline:raise ValueError
        summary=plain_html(text(row.get('summary'),100000))
        content=text(row.get('content'),2000000)
        symbols=row.get('symbols',[])
        if not isinstance(symbols,list) or len(symbols)>1000:raise ValueError
        normalized=[]
        for s in symbols:
            if not isinstance(s,str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9./_-]{0,31}',s):raise ValueError
            normalized.append(s.upper())
        symbols=sorted(set(normalized))
        source=text(row.get('source'),1000)
        url=canonical_url(text(row.get('url'),10000))
        data={'id':article_id,'provider':provider,'headline':headline,'summary':summary,
              'author':text(row.get('author'),2000),'source':source,'created_at':created,'updated_at':updated,
              'url':url,'symbols':symbols,'content_html':content,'content_text':plain_html(content),
              'content_provided':isinstance(row.get('content'),str)}
        mappings={s:'provider_tag' for s in symbols if s in UNIVERSE}
        for s in re.findall(r'(?<![A-Za-z0-9])\$([A-Z][A-Z0-9.-]{0,14})(?![A-Za-z0-9.-])',headline+' '+summary+' '+data['content_text']):
            if s in UNIVERSE:mappings.setdefault(s,'explicit_cashtag')
        fingerprint=hashlib.sha256(encode({'source':source.lower(),'url':url,'headline':headline.casefold(),
            'created_at':created}).encode()).hexdigest()
        return {'provider':provider,'id':article_id,'data':data,'revision_hash':hashlib.sha256(encode(data).encode()).hexdigest(),
                'fingerprint':fingerprint,'mappings':mappings}
    except (KeyError,TypeError,ValueError,DataValidationError):
        raise DataValidationError('Invalid Alpaca news article') from None

class AlpacaNews(AlpacaHistoricalData):
    def iter_news(self,request):
        self.pages=0
        params={**{k:v for k,v in request.metadata().items() if k in {'limit','sort'}},
                'symbols':','.join(request.symbols),'start':request.start.astimezone(timezone.utc).isoformat(),
                'end':request.end.astimezone(timezone.utc).isoformat(),'include_content':'true','exclude_contentless':'false'}
        seen=set()
        provider='alpaca-sandbox' if self.settings.data_env=='sandbox' else 'alpaca'
        while True:
            payload=self._get(params,endpoint='/v1beta1/news',mapping_field='news',mapping_type=list)
            self.pages+=1
            for row in payload['news']:yield normalize_news(row,provider=provider)
            token=payload.get('next_page_token')
            if token is None:break
            if not isinstance(token,str) or not token or token in seen:raise MarketDataError('Invalid or repeated news pagination token')
            if self.pages>=request.max_pages:raise MarketDataError('News max-pages reached; narrow the interval or raise the limit; load rolled back',code='pagination_limit')
            seen.add(token);params['page_token']=token

def collect_news(adapter,repository,request):
    run_id=str(uuid.uuid4());started=datetime.now(timezone.utc).isoformat();m=repository.marker
    result={'run_id':run_id,'status':'completed','received_articles':0,'new_articles':0,'new_versions':0,
            'exact_duplicates':0,'updated_articles':0,'stale_versions':0,'contentless_received':0,'unmapped_received':0}
    def audit(status,data):
        repository.connection.execute(f"INSERT INTO news_collection_runs VALUES ({','.join([m]*6)})",
            (run_id,started,datetime.now(timezone.utc).isoformat(),status,encode(request.metadata()),encode(data)))
    try:
        for article in adapter.iter_news(request):
            provider,article_id=article['provider'],article['id'];data=article['data'];rev=article['revision_hash']
            result['received_articles']+=1
            if not data['content_text']:result['contentless_received']+=1
            if not article['mappings']:result['unmapped_received']+=1
            now=datetime.now(timezone.utc).isoformat()
            current=repository.connection.execute(f'SELECT revision_hash,updated_at,has_content FROM news_articles WHERE provider={m} AND article_id={m}',(provider,article_id)).fetchone()
            inserted=repository.connection.execute(f"INSERT INTO news_versions VALUES ({','.join([m]*7)}) ON CONFLICT (provider,article_id,revision_hash) DO NOTHING",
                (provider,article_id,rev,data['created_at'],data['updated_at'],now,encode(data))).rowcount
            if not inserted:
                result['exact_duplicates']+=1
                repository.connection.execute(f'UPDATE news_articles SET last_seen_at={m} WHERE provider={m} AND article_id={m}',(now,provider,article_id))
                continue
            result['new_versions']+=1
            for symbol,method in sorted(article['mappings'].items()):
                repository.connection.execute(f"INSERT INTO news_symbols VALUES ({','.join([m]*5)}) ON CONFLICT (provider,article_id,revision_hash,symbol) DO NOTHING",
                    (provider,article_id,rev,symbol,method))
            # Same-timestamp metadata-only responses cannot replace an available full body.
            promote=current is None or data['updated_at']>current[1] or (data['updated_at']==current[1] and (bool(data['content_text']) or not current[2]))
            if not promote:
                result['stale_versions']+=1
                continue
            repository.connection.execute(f"INSERT INTO news_articles VALUES ({','.join([m]*8)}) ON CONFLICT (provider,article_id) DO UPDATE SET revision_hash=excluded.revision_hash,updated_at=excluded.updated_at,last_seen_at=excluded.last_seen_at,fingerprint=excluded.fingerprint,has_content=excluded.has_content",
                (provider,article_id,rev,data['updated_at'],now,now,article['fingerprint'],int(bool(data['content_text']))))
            result['new_articles' if current is None else 'updated_articles']+=1
        result['pages']=adapter.pages
        if not result['received_articles']:result['status']='empty'
        audit(result['status'],result);repository.connection.commit()
        log.info('News collection finished: articles=%d new=%d versions=%d duplicates=%d',result['received_articles'],result['new_articles'],result['new_versions'],result['exact_duplicates'])
        return result
    except BaseException:
        repository.connection.rollback()
        try:
            audit('failed',{'received_before_rollback':result['received_articles'],'committed_articles':0,'error_code':'collection_failed'})
            repository.connection.commit()
        except Exception:repository.connection.rollback()
        raise

def news_summary(repository, *, provider='alpaca', symbols=()):
    if provider not in {'alpaca','alpaca-sandbox'}:raise DataValidationError('Invalid news provider')
    if symbols:symbols=validate_symbols(symbols)
    m=repository.marker
    begin_read(repository)
    try:
        total=repository.connection.execute(f'SELECT COUNT(*) FROM news_articles WHERE provider={m}',(provider,)).fetchone()[0]
        versions=repository.connection.execute(f'SELECT COUNT(*) FROM news_versions WHERE provider={m}',(provider,)).fetchone()[0]
        contentless=repository.connection.execute(f'SELECT COUNT(*) FROM news_articles WHERE provider={m} AND has_content=0',(provider,)).fetchone()[0]
        counts=dict(repository.connection.execute(f'SELECT s.symbol,COUNT(*) FROM news_symbols s JOIN news_articles a ON a.provider=s.provider AND a.article_id=s.article_id AND a.revision_hash=s.revision_hash WHERE a.provider={m} GROUP BY s.symbol ORDER BY s.symbol',(provider,)).fetchall())
        unmapped=repository.connection.execute(f'SELECT COUNT(*) FROM news_articles a WHERE a.provider={m} AND NOT EXISTS (SELECT 1 FROM news_symbols s WHERE s.provider=a.provider AND s.article_id=a.article_id AND s.revision_hash=a.revision_hash)',(provider,)).fetchone()[0]
        groups=repository.connection.execute(f'SELECT fingerprint,COUNT(*) FROM news_articles WHERE provider={m} GROUP BY fingerprint HAVING COUNT(*)>1 ORDER BY fingerprint LIMIT 20',(provider,)).fetchall()
        query=f'SELECT a.article_id,v.data_json,v.first_seen_at FROM news_articles a JOIN news_versions v ON v.provider=a.provider AND v.article_id=a.article_id AND v.revision_hash=a.revision_hash WHERE a.provider={m}'
        params=[provider]
        if symbols:
            query+=f" AND EXISTS (SELECT 1 FROM news_symbols s WHERE s.provider=a.provider AND s.article_id=a.article_id AND s.revision_hash=a.revision_hash AND s.symbol IN ({','.join([m]*len(symbols))}))"
            params.extend(symbols)
        rows=repository.connection.execute(query+' ORDER BY a.updated_at DESC,a.article_id DESC LIMIT 5',params).fetchall()
        samples=[]
        for article_id,raw,first_seen in rows:
            data=json.loads(raw)
            samples.append({'id':article_id,'headline':data['headline'][:200],'created_at':data['created_at'],
                            'updated_at':data['updated_at'],'first_seen_at':first_seen,'symbols':data['symbols'],
                            'source':data['source'],'has_content':bool(data['content_text'])})
        repository.connection.commit()
        return {'provider':provider,'articles_total':total,'versions_total':versions,'contentless_articles':contentless,
                'unmapped_articles':unmapped,'symbol_counts':{s:counts.get(s,0) for s in (symbols or tuple(counts))},
                'duplicate_candidate_groups':[{'fingerprint':f,'articles':n} for f,n in groups],
                'duplicate_candidates_merged':False,'samples':samples,'matching':'provider_tag_or_explicit_cashtag',
                'sentiment_implemented':True,'sentiment_included_in_summary':False,
                'sentiment_command':'analysis-summary','news_replay_implemented':True,
                'news_replay_semantics':'recorded_first_seen_version_state_not_exact_commit_visibility',
                'sentiment_replay_implemented':False}
    except BaseException:
        repository.connection.rollback();raise
