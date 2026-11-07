"""Bounded guarded news-to-Paper coordinator, with persistent article claims and fencing."""
from collections import Counter,defaultdict
from datetime import datetime,timezone,timedelta
from decimal import Decimal
from itertools import groupby
import json
import logging
import time
import re
import uuid
from .paper_api import PaperAPI,PaperBrokerError,TERMINAL
from . import paper_broker as broker
from .equity_history import capture_equity
from ..config import UNIVERSE
from ..data.alignment import nanos
from ..data.analysis_replay import validate_analysis
from ..data.models import DataValidationError
from ..data.research import begin_read,encode
from ..data.session_prices import receipt_record
from ..data.stream_models import canonical_time
from ..data.storage.repository import Repository,StorageError
from ..intelligence.analysis import AnalysisRequest,select_news
from ..intelligence.policy import digest,analyzer_metadata
from ..intelligence.research_decision import company_research_label

log=logging.getLogger('microbroker.paper')

MAX_DECISIONS=5000
LEASE_SECONDS=120


def utc_add(at,seconds):
    at=canonical_time(at)
    return (datetime.fromisoformat(at[:19]+'+00:00')+timedelta(seconds=seconds)).strftime('%Y-%m-%dT%H:%M:%S')+at[19:]


def decision_id(account_ref,article_id,symbol):
    return digest({'account_ref':account_ref,'provider':'alpaca','article_id':article_id,'symbol':symbol})


def read_decision(repository,key):
    row=repository.connection.execute(f'SELECT account_ref,article_id,symbol,session_id,intent_id,request_json,request_sha256 FROM paper_news_decisions WHERE id={repository.marker}',(key,)).fetchone()
    if not row:raise StorageError('Unknown Paper news decision')
    try:
        if len(row[5].encode())>2*1024*1024:raise StorageError('Paper decision source exceeds 2 MiB')
        request=json.loads(row[5]);record=validate_analysis(request['record'])
        if (digest(request)!=row[6] or decision_id(row[0],row[1],row[2])!=key or
            row[4]!='r23-'+key[:48] or request['account_ref']!=row[0] or request['session_id']!=row[3] or
            request['symbol']!=row[2] or record['news']['article_id']!=row[1] or request['format']!='R23' or
            record['analysis'] is None or record['analysis']['analyzer_key']!=request['analyzer_key']):
            raise StorageError('Paper decision source integrity failed')
        state={'status':'pending','reason':'awaiting_following_completed_bar','intent_prepared':False,
               'price':None,'parent_intent_id':None}
        previous=row[6];events=repository.connection.execute(f'SELECT ordinal,observed_at,event_json,previous_sha256,event_sha256 FROM paper_news_events WHERE decision_id={repository.marker} ORDER BY ordinal LIMIT 101',(key,)).fetchall()
        if len(events)>100:raise StorageError('Paper decision event bound')
        last=request['created_at']
        for index,(ordinal,at,raw,prev,sha) in enumerate(events):
            event=json.loads(raw)
            expected=digest({'decision_id':key,'ordinal':ordinal,'at':at,'event':event,'previous':previous})
            if ordinal!=index or at<last or prev!=previous or sha!=expected:raise StorageError('Paper decision event chain failed')
            state=reduce_decision(state,event);previous=sha;last=at
        return request,state,previous,len(events),row[4]
    except (KeyError,TypeError,ValueError):raise StorageError('Invalid Paper decision record') from None


def reduce_decision(state,event):
    state=dict(state)
    if state['status']!='pending':raise StorageError('Paper decision is already consumed')
    if event['kind']=='wait':
        if not re.fullmatch('[a-z_]{1,80}',event['reason']):raise StorageError('Invalid Paper wait reason')
        state.update(status='wait',reason=event['reason'])
    elif event['kind']=='prepared':
        if state['intent_prepared']:raise StorageError('Paper decision already prepared')
        state.update(intent_prepared=True,price=event['price'],parent_intent_id=event['parent_intent_id'])
    elif event['kind']=='submitted':
        if not state['intent_prepared']:raise StorageError('Paper decision has no prepared intent')
        state.update(status='submitted',reason='broker_intent_consumed_not_necessarily_filled')
    else:raise StorageError('Unknown Paper decision event')
    return state


def append_decision(repository,key,event,at):
    request,state,previous,count,_=read_decision(repository,key)
    if state['status']!='pending':return state
    last=repository.connection.execute(f'SELECT observed_at FROM paper_news_events WHERE decision_id={repository.marker} ORDER BY ordinal DESC LIMIT 1',(key,)).fetchone()
    if canonical_time(at)!=at or at<(last[0] if last else request['created_at']):raise StorageError('Paper decision local clock order failed')
    changed=reduce_decision(state,event)
    sha=digest({'decision_id':key,'ordinal':count,'at':at,'event':event,'previous':previous})
    repository.connection.execute(f'INSERT INTO paper_news_events VALUES ({",".join([repository.marker]*6)})',
        (key,count,at,encode(event),previous,sha))
    return changed


def all_decisions(repository,account_ref):
    ids=repository.connection.execute(f'SELECT id FROM paper_news_decisions WHERE account_ref={repository.marker} ORDER BY id LIMIT {MAX_DECISIONS+1}',(account_ref,)).fetchall()
    if len(ids)>MAX_DECISIONS:raise StorageError('Paper account exceeds 5000 news decisions')
    rows=[];size=0
    for row in ids:
        value=read_decision(repository,row[0]);size+=len(encode(value[0]).encode())
        if size>64*1024*1024:raise StorageError('Paper decision account exceeds 64 MiB')
        rows.append((row[0],*value))
    return rows


def current_records(repository,symbols,started,key,engine,*,_articles=None):
    articles=_articles if _articles is not None else select_news(repository,AnalysisRequest(symbols=tuple(symbols),first_seen_since=started))
    begin_read(repository);records=[];m=repository.marker
    try:
        for article in articles:
            row=repository.connection.execute(f'SELECT analyzed_at,result_json,result_sha256,input_sha256 FROM news_analyses WHERE provider={m} AND article_id={m} AND revision_hash={m} AND analyzer_key={m} AND engine={m}',
                ('alpaca',article['id'],article['revision'],key,engine)).fetchone()
            if not row:continue
            if len(row[1].encode())>2*1024*1024:raise StorageError('Paper source analysis exceeds 2 MiB')
            news={'source':'news','provider':'alpaca','article_id':article['id'],'revision_hash':article['revision'],
                'first_seen_at':canonical_time(article['first_seen']),'data':article['data'],'mappings':article['mappings']}
            news['timestamp']=max(news['first_seen_at'],article['data']['created_at'],article['data']['updated_at'])
            analysis={'engine':engine,'analyzer_key':key,'analyzed_at':canonical_time(row[0]),
                'timestamp':max(news['timestamp'],canonical_time(row[0])),'result':json.loads(row[1]),
                'result_sha256':row[2],'input_sha256':row[3]}
            records.append(validate_analysis({'news':news,'analysis':analysis}))
        repository.connection.commit();return records
    except BaseException:repository.connection.rollback();raise


class PaperWorker:
    def __init__(self,repository,api,symbols,feed,analyzer_key,engine='finbert',*,duration=900,notional='10',max_buys=3,_fixture=False):
        if engine!='finbert' and not _fixture:raise DataValidationError('Paper news worker requires verified local FinBERT')
        if not symbols or not set(symbols)<=set(UNIVERSE) or feed not in {'iex','sip'}:raise DataValidationError('Invalid Paper worker symbols/feed')
        if type(duration) is not int or not 60<=duration<=3600 or type(max_buys) is not int or not 1<=max_buys<=10:
            raise DataValidationError('Paper worker duration 60..3600 and max-buys 1..10 required')
        from .paper_ledger import decimal_value,number_text
        self.notional=number_text(decimal_value(notional,'Paper worker notional',maximum='100',places=2))
        if Decimal(self.notional)<1:raise DataValidationError('Paper worker notional must be at least 1 USD')
        self.repository,self.api=repository,api;self.symbols=tuple(sorted(symbols));self.feed=feed
        self.key,self.engine=analyzer_key,engine;self.session_id=str(uuid.uuid4());self.token=uuid.uuid4().hex
        self.started=canonical_time(api.now());self.deadline=utc_add(self.started,duration);self.max_buys=max_buys
        self.account_ref=api.account()['account_ref'];self.stream_run_id=None;self.recovery_at=None
        self.last_equity_at=None;self.lease_acquired=False

    def transaction(self,call):
        broker.locked(self.repository,self.account_ref)
        try:
            result=call();self.repository.connection.commit();return result
        except BaseException:self.repository.connection.rollback();raise

    def lease(self,acquire=False):
        def operation():
            m=self.repository.marker;now=self.api.now()
            row=self.repository.connection.execute(f'SELECT token,expires_at FROM paper_worker_leases WHERE account_ref={m}',(self.account_ref,)).fetchone()
            if row and row[0]!=self.token and row[1]>now:raise PaperBrokerError('another_paper_worker_active')
            if not acquire and (not row or row[0]!=self.token or row[1]<=now):raise PaperBrokerError('paper_worker_lease_lost')
            self.repository.connection.execute(f'INSERT INTO paper_worker_leases VALUES ({",".join([m]*4)}) ON CONFLICT (account_ref) DO UPDATE SET token=excluded.token,session_id=excluded.session_id,expires_at=excluded.expires_at',
                (self.account_ref,self.token,self.session_id,utc_add(now,LEASE_SECONDS)))
        self.transaction(operation);self.lease_acquired=True
        if acquire:self.retire_pending(other_sessions=True)

    def assert_lease(self,repository):
        row=repository.connection.execute(f'SELECT token,expires_at FROM paper_worker_leases WHERE account_ref={repository.marker}',(self.account_ref,)).fetchone()
        if not row or row[0]!=self.token or row[1]<=self.api.now():raise PaperBrokerError('paper_worker_lease_lost')

    def retire_pending(self,other_sessions=False):
        def operation():
            self.assert_lease(self.repository)
            for key,request,state,*_ in all_decisions(self.repository,self.account_ref):
                selected=request['session_id']!=self.session_id if other_sessions else request['session_id']==self.session_id
                if selected and state['status']=='pending':self._wait_locked(key,'worker_session_ended')
        self.transaction(operation)

    def _wait_locked(self,key,reason):
        request,state,_,_,intent_id=read_decision(self.repository,key)
        if state['status']!='pending':return
        found=self.repository.connection.execute(f'SELECT id FROM broker_paper_intents WHERE id={self.repository.marker}',(intent_id,)).fetchone()
        if found:
            _,broker_state,*_=broker.read_intent(self.repository,intent_id)
            if broker_state['post_attempts'] or broker_state['order'] is not None:
                append_decision(self.repository,key,{'kind':'submitted'},self.api.now());return
            broker.append(self.repository,intent_id,'retired_unsent',{'reason':'worker_signal_retired'},self.api.now())
        append_decision(self.repository,key,{'kind':'wait','reason':reason},self.api.now())

    def wait(self,key,reason):return self.transaction(lambda:self._wait_locked(key,reason))

    def finish(self):
        if not self.lease_acquired:return
        try:self.retire_pending()
        finally:
            def release():
                self.repository.connection.execute(f'DELETE FROM paper_worker_leases WHERE account_ref={self.repository.marker} AND token={self.repository.marker}',(self.account_ref,self.token))
            self.transaction(release)

    def read_prices(self,repository):
        if not self.stream_run_id:return []
        m=repository.marker
        header=repository.connection.execute(f'SELECT request_json FROM stream_sessions WHERE id={m}',(self.stream_run_id,)).fetchone()
        if not header:raise StorageError('Paper worker stream audit missing')
        metadata=json.loads(header[0])
        if metadata['feed']!=self.feed or set(metadata['symbols'])!=set(self.symbols):raise StorageError('Paper worker stream scope mismatch')
        rows=repository.connection.execute(f'SELECT ordinal,provider,feed,payload_json,received_at FROM stream_bar_receipts WHERE run_id={m} ORDER BY ordinal LIMIT 10001',(self.stream_run_id,)).fetchall()
        if len(rows)>10000:raise StorageError('Paper worker receipt bound')
        records=[];last=None;size=0
        for expected,(ordinal,provider,feed,raw,at) in enumerate(rows,1):
            size+=len(raw.encode())
            if ordinal!=expected or size>64*1024*1024:raise StorageError('Paper receipt sequence/size bound failed')
            at=canonical_time(at)
            if last is not None and at<last:raise StorageError('Nonmonotonic Paper receipt clock')
            last=at
            if provider!='alpaca' or feed!=self.feed:raise StorageError('Paper worker receipt provider mismatch')
            if self.started<=at<=self.api.now():records.append(receipt_record(json.loads(raw),provider,feed,self.stream_run_id,ordinal,at,self.symbols))
        return records

    def price_health(self,records):
        now=self.api.now();by_symbol={};last_valid={};flags=set();recovery=None
        for bar in records:
            symbol=bar['symbol'];r=bar['receipt']
            if r['event_type']!='b':continue
            if symbol in last_valid and bar['timestamp']<=last_valid[symbol]['timestamp']:continue
            age=nanos(r['received_at'])-nanos(bar['timestamp'])-60000000000
            if age<0:flags.add(symbol);continue
            if age>120000000000:continue
            previous=last_valid.get(symbol)
            gap=nanos(r['received_at'])-nanos(previous['receipt']['received_at'] if previous else self.started)>180000000000
            if symbol in flags or gap:
                recovery=max(recovery or r['received_at'],r['received_at']);flags.discard(symbol)
            last_valid[symbol]=bar;by_symbol.setdefault(symbol,[]).append(bar)
        gaps={s for s in self.symbols if nanos(now)-nanos(last_valid[s]['receipt']['received_at'] if s in last_valid else self.started)>180000000000}
        fresh={s for s,b in last_valid.items() if 0<=nanos(now)-nanos(b['receipt']['received_at'])<=90000000000 and
               0<=nanos(now)-nanos(b['timestamp'])-60000000000<=120000000000}
        return {'by_symbol':by_symbol,'flags':flags,'gaps':gaps,'fresh':fresh,'recovery_at':recovery}

    def create(self,record,symbol):
        key=decision_id(self.account_ref,record['news']['article_id'],symbol);intent_id='r23-'+key[:48]
        request={'format':'R23','account_ref':self.account_ref,'session_id':self.session_id,'symbol':symbol,
            'record':record,'analyzer_key':self.key,'created_at':self.api.now(),'started_at':self.started,
            'deadline':self.deadline,'stream_run_id':self.stream_run_id,'feed':self.feed,
            'symbols':list(self.symbols),'notional':self.notional,'max_buys':self.max_buys}
        def operation():
            self.assert_lease(self.repository);m=self.repository.marker
            if self.repository.connection.execute(f'SELECT id FROM paper_news_decisions WHERE id={m}',(key,)).fetchone():return False
            if len(all_decisions(self.repository,self.account_ref))>=MAX_DECISIONS:raise StorageError('Paper decision account bound')
            self.repository.connection.execute(f'INSERT INTO paper_news_decisions VALUES ({",".join([m]*8)})',
                (key,self.account_ref,record['news']['article_id'],symbol,self.session_id,intent_id,encode(request),digest(request)))
            return True
        return key,self.transaction(operation)

    def label(self,request):
        result=request['record']['analysis']['result'];company=result.get('entity_sentiments',{}).get(request['symbol'])
        if result.get('analysis_format')!='R18' or not company or company.get('status')!='scored':return None
        return company_research_label(result,company)

    def gate(self,repository,key,health):
        request,state,*_=read_decision(repository,key);now=self.api.now();record=request['record'];symbol=request['symbol']
        if state['status']!='pending':return 'decision_already_consumed',None
        if (request['session_id']!=self.session_id or request['started_at']!=self.started or request['deadline']!=self.deadline or
            request['stream_run_id']!=self.stream_run_id or request['feed']!=self.feed or request['notional']!=self.notional or
            request['max_buys']!=self.max_buys or request['analyzer_key']!=self.key or tuple(request['symbols'])!=self.symbols):
            raise StorageError('Paper decision worker scope mismatch')
        if self.expired():return 'session_deadline',None
        available=record['analysis']['timestamp'];news=record['news']
        if news['first_seen_at']<self.started:return 'pre_session_news',None
        if not 0<=nanos(now)-nanos(available)<=300000000000:return 'signal_expired_or_future',None
        if not 0<=nanos(now)-nanos(news['data']['created_at'])<=900000000000:return 'news_expired_or_future',None
        current=repository.connection.execute(f'SELECT revision_hash FROM news_articles WHERE provider={repository.marker} AND article_id={repository.marker}',('alpaca',news['article_id'])).fetchone()
        if not current or current[0]!=news['revision_hash']:return 'revision_superseded',None
        analysis=repository.connection.execute(f'SELECT result_sha256,result_json,input_sha256,analyzed_at FROM news_analyses WHERE provider={repository.marker} AND article_id={repository.marker} AND revision_hash={repository.marker} AND analyzer_key={repository.marker}',
            ('alpaca',news['article_id'],news['revision_hash'],self.key)).fetchone()
        if (not analysis or analysis[0]!=record['analysis']['result_sha256'] or digest(json.loads(analysis[1]))!=analysis[0]
            or json.loads(analysis[1])!=record['analysis']['result'] or analysis[2]!=record['analysis']['input_sha256']
            or canonical_time(analysis[3])!=record['analysis']['analyzed_at']):raise StorageError('Paper signal analysis changed')
        version=repository.connection.execute(f'SELECT first_seen_at,data_json FROM news_versions WHERE provider={repository.marker} AND article_id={repository.marker} AND revision_hash={repository.marker}',('alpaca',news['article_id'],news['revision_hash'])).fetchone()
        mappings=dict(repository.connection.execute(f'SELECT symbol,method FROM news_symbols WHERE provider={repository.marker} AND article_id={repository.marker} AND revision_hash={repository.marker}',('alpaca',news['article_id'],news['revision_hash'])).fetchall())
        if not version or canonical_time(version[0])!=news['first_seen_at'] or json.loads(version[1])!=news['data'] or mappings!=news['mappings']:
            raise StorageError('Paper source news changed')
        label=self.label(request)
        if label not in {'positive','negative','neutral'}:return 'company_decision_withheld',None
        if label=='neutral':return 'neutral_company_decision',None
        if label=='positive':
            if health['flags']:return 'receipt_time_inconsistent',None
            if health['gaps']:return 'price_feed_gap',None
            if health['recovery_at'] and available<=health['recovery_at']:return 'signal_before_price_recovery',None
            if set(self.symbols)-health['fresh']:return 'awaiting_price_warmup',None
        if symbol not in health['fresh']:return 'awaiting_fresh_price',None
        eligible=[b for b in health['by_symbol'].get(symbol,[]) if b['timestamp']>=available and b['receipt']['received_at']>available and
                  0<=nanos(now)-nanos(b['receipt']['received_at'])<=90000000000]
        if not eligible:return 'awaiting_following_completed_bar',None
        return None,eligible[-1]

    def open_entry(self,symbol):
        rows=broker.all_intents(self.repository,self.account_ref);parents=[]
        for i,s,*_ in rows:
            if not i['id'].startswith('r23-') or i['order']['side']!='buy' or i['order']['symbol']!=symbol:continue
            if s['phase']!='observed' or not s['order'] or s['order']['status'] not in TERMINAL:continue
            remaining=Decimal(s['order']['filled_qty'])
            for exit_i,exit_s,*_ in rows:
                if exit_i['parent_intent_id']==i['id'] and exit_s['phase']!='retired':
                    remaining-=Decimal(exit_s['order']['filled_qty']) if exit_s['order'] and exit_s['order']['status'] in TERMINAL else Decimal(exit_i['order']['qty'])
            if remaining>0:parents.append(i['id'])
        if len(parents)>1:raise PaperBrokerError('multiple_managed_worker_entries')
        return parents[0] if parents else None

    def guard(self,key,chosen):
        def check(repository,intent):
            self.assert_lease(repository)
            if hasattr(self,'background_check'):self.background_check()
            records=self.read_prices(repository);health=self.price_health(records)
            reason,bar=self.gate(repository,key,health)
            if reason:raise PaperBrokerError(reason)
            if bar!=chosen:raise PaperBrokerError('price_changed_before_submit')
            source=chosen['receipt']
            exact=next((b for b in records if b['receipt']['ordinal']==source['ordinal']),None)
            if exact!=chosen:raise StorageError('Paper signal price receipt changed')
            request,state,*_=read_decision(repository,key)
            if not state['intent_prepared'] or state['price']!=chosen:raise StorageError('Paper intent lacks sealed price evidence')
            if intent['id']!='r23-'+key[:48]:raise StorageError('Paper intent/decision identity mismatch')
        return check

    def tick(self):
        self.lease()
        if self.expired():return {'status':'deadline','post_requests_attempted':0}
        comparison=broker.reconcile_account(self.repository,self.api)
        if comparison['account_ref']!=self.account_ref:raise PaperBrokerError('paper_account_identity_changed')
        if comparison['status']!='matched':raise PaperBrokerError('paper_account_needs_reconciliation')
        if self.expired():return {'status':'deadline','post_requests_attempted':0}
        if self.last_equity_at is None or nanos(self.api.now())-nanos(self.last_equity_at)>=300000000000:
            capture_equity(self.repository,self.api,session_id=self.session_id);self.last_equity_at=self.api.now()
        clock=self.api.clock()
        if self.expired():return {'status':'deadline','post_requests_attempted':0}
        if abs(nanos(clock['timestamp'])-nanos(self.api.now()))>120000000000:raise PaperBrokerError('market_clock_not_fresh')
        articles=select_news(self.repository,AnalysisRequest(symbols=self.symbols,first_seen_since=self.started))
        records=current_records(self.repository,self.symbols,self.started,self.key,self.engine,_articles=articles)
        ordered=[];reused=0
        for record in records:
            for symbol in set(record['news']['mappings'])&set(self.symbols):ordered.append((record['analysis']['timestamp'],symbol,record['news']['article_id'],record))
        ordered.sort(key=lambda item:item[:3])
        for (available,symbol),group in groupby(ordered,key=lambda item:(item[0],item[1])):
            if self.expired():break
            new=[]
            for _,_,_,record in group:
                if self.expired():break
                key,inserted=self.create(record,symbol)
                if inserted:new.append(key)
                else:reused+=1
            if not new:continue
            def supersede():
                entries=all_decisions(self.repository,self.account_ref)
                newest=max(r['record']['analysis']['timestamp'] for _,r,*_ in entries if r['symbol']==symbol)
                for key,r,s,*_ in entries:
                    if r['symbol']==symbol and s['status']=='pending' and (r['record']['analysis']['timestamp']<newest):
                        self._wait_locked(key,'superseded_by_newer_feature')
                same=[(key,r) for key,r,*_ in entries if r['symbol']==symbol and r['record']['analysis']['timestamp']==available]
                directions={self.label(r) for _,r in same}
                if {'positive','negative'}<=directions:
                    for key,_ in same:self._wait_locked(key,'conflicting_simultaneous_signals')
            self.transaction(supersede)
        begin_read(self.repository)
        try:
            prices=self.read_prices(self.repository);health=self.price_health(prices)
            pending=[row for row in all_decisions(self.repository,self.account_ref) if row[1]['session_id']==self.session_id and row[2]['status']=='pending']
            self.repository.connection.commit()
        except BaseException:self.repository.connection.rollback();raise
        attempted=0;pending_waits={}
        for key,request,state,_,_,intent_id in sorted(pending,key=lambda x:(x[1]['record']['analysis']['timestamp'],x[0])):
            begin_read(self.repository)
            try:
                reason,bar=self.gate(self.repository,key,health)
                parent=self.open_entry(request['symbol']) if self.label(request)=='negative' else None
                rows=broker.all_intents(self.repository,self.account_ref)
                own_ids={iid for _,r,_,_,_,iid in all_decisions(self.repository,self.account_ref) if r['session_id']==self.session_id}
                buys=sum(i['order']['side']=='buy' and s['post_attempts']>0 and i['id'] in own_ids for i,s,*_ in rows)
                self.repository.connection.commit()
            except BaseException:self.repository.connection.rollback();raise
            if not clock['is_open']:reason='market_closed'
            if reason:
                if not reason.startswith('awaiting_'):self.wait(key,reason)
                else:pending_waits[key]=reason
                continue
            label=self.label(request)
            if label=='negative' and parent is None:self.wait(key,'no_managed_worker_position');continue
            if label=='positive' and buys>=self.max_buys:self.wait(key,'session_buy_limit');continue
            try:
                self.lease()
                if label=='positive':broker.prepare_buy(self.repository,self.api,intent_id,request['symbol'],self.notional)
                else:broker.prepare_exit(self.repository,self.api,intent_id,parent)
                self.transaction(lambda:append_decision(self.repository,key,{'kind':'prepared','price':bar,'parent_intent_id':parent},self.api.now()))
                sent=broker.submit(self.repository,self.api,intent_id,_guard=self.guard(key,bar))
                attempted+=sent.get('post_requests_attempted_by_this_call',0)
                self.transaction(lambda:self.assert_lease(self.repository))
                if sent['post_attempts'] or sent['order'] is not None:
                    self.transaction(lambda:append_decision(self.repository,key,{'kind':'submitted'},self.api.now()))
                else:self.wait(key,'broker_intent_not_submitted')
                if sent['phase']=='uncertain':raise PaperBrokerError('paper_broker_order_uncertain')
            except PaperBrokerError as exc:
                self.wait(key,exc.code)
                if exc.code in {'paper_worker_lease_lost','paper_broker_order_uncertain','transport_failed'}:raise
        return {'status':'processed','observed_at':self.api.now(),'post_requests_attempted':attempted,'decisions_seen':len(ordered),
                'selected_recent_articles':len(articles),'analyses_recorded':len(records),
                'analyses_available':sum(r['analysis']['timestamp']<=self.api.now() for r in records),
                'analyses_pending':len(articles)-sum(r['analysis']['timestamp']<=self.api.now() for r in records),
                'existing_article_company_claims':reused,'pending_wait_reasons':pending_waits,
                'pending_wait_reason_counts':dict(Counter(pending_waits.values())),
                'fresh_price_symbols':sorted(health['fresh']),'price_gap_symbols':sorted(health['gaps']),
                'price_clock_flags':sorted(health['flags']),'price_recovered_at':health['recovery_at']}

    def expired(self):
        return self.api.now()>=self.deadline or (hasattr(self,'monotonic_deadline') and time.monotonic()>=self.monotonic_deadline)

    def coverage(self,cutoff):
        """Inspect committed inputs without creating claims or implying executable trades."""
        from .session_timing import pipeline_latency
        articles=select_news(self.repository,AnalysisRequest(symbols=self.symbols,first_seen_since=self.started))
        records=[r for r in current_records(self.repository,self.symbols,self.started,self.key,self.engine,_articles=articles)
                 if r['news']['timestamp']<=cutoff and r['analysis']['analyzed_at']<=cutoff]
        begin_read(self.repository)
        try:
            claims={key:(r,s,i) for key,r,s,sha,count,i in all_decisions(self.repository,self.account_ref)}
            broker.all_intents(self.repository,self.account_ref)  # Verify chains before timing their events.
            missing=[];missing_articles=set();unclaimed_articles=set();company_rows=0;owned=0;previous=0;prior_revision=0;different_analysis=0;current_claims=0;unclaimed=0;orders=[]
            for record in records:
                for symbol in sorted(set(record['news']['mappings'])&set(self.symbols)):
                    company_rows+=1;key=decision_id(self.account_ref,record['news']['article_id'],symbol)
                    claim=claims.get(key);reason=None
                    if claim is None:
                        unclaimed+=1;unclaimed_articles.add(record['news']['article_id']);reason='no_account_claim'
                    else:
                        saved=claim[0]
                        if saved['session_id']==self.session_id:owned+=1
                        else:previous+=1
                        if saved['record']['news']['revision_hash']!=record['news']['revision_hash']:
                            prior_revision+=1;reason='claim_for_prior_revision'
                        elif (saved['record']['analysis']['analyzer_key']!=record['analysis']['analyzer_key'] or
                              saved['record']['analysis']['engine']!=record['analysis']['engine'] or
                              saved['record']['analysis']['result_sha256']!=record['analysis']['result_sha256']):
                            different_analysis+=1;reason='claim_for_different_analysis'
                        else:current_claims+=1
                    if reason:
                        missing_articles.add(record['news']['article_id'])
                        entity=record['analysis']['result'].get('entity_sentiments',{}).get(symbol,{})
                        missing.append({'article_id':record['news']['article_id'],'revision_hash':record['news']['revision_hash'],
                            'symbol':symbol,'analysis_recorded_at':record['analysis']['analyzed_at'],'reason':reason,
                            'claimed_revision_hash':claim[0]['record']['news']['revision_hash'] if claim else None,
                            'claimed_analyzer_key':claim[0]['record']['analysis']['analyzer_key'] if claim else None,
                            'claim_session_id':claim[0]['session_id'] if claim else None,
                            'current_company_label':self.label({'record':record,'symbol':symbol}),
                            'current_company_context_status':entity.get('status'),
                            'current_research_decision_status':(entity.get('research_decision') or {}).get('status')})
            for request,state,intent_id in claims.values():
                if request['session_id']!=self.session_id:continue
                events=self.repository.connection.execute(f'SELECT observed_at,event_json FROM broker_paper_events WHERE intent_id={self.repository.marker} ORDER BY ordinal',(intent_id,)).fetchall()
                claimed=filled=None
                for at,raw in events:
                    event=json.loads(raw)
                    if event['kind']=='post_claimed' and claimed is None:claimed=at
                    if event['kind']=='broker_observed' and event['data']['status']=='filled' and filled is None:filled=at
                orders.append((request,claimed,filled))
            self.repository.connection.commit()
        except BaseException:self.repository.connection.rollback();raise
        return {'format':'R29','cutoff':cutoff,'available_analyses':len(records),'company_rows':company_rows,
            'claimed_by_this_session':owned,'previous_account_claims':previous,'claims_for_prior_revision':prior_revision,
            'claims_for_different_analysis':different_analysis,'current_analysis_claim_rows':current_claims,
            'unclaimed_analyses':len(unclaimed_articles),'unclaimed_company_rows':unclaimed,
            'unassessed_reason_counts':dict(Counter(row['reason'] for row in missing)),
            'unassessed_analyses':len(missing_articles),'unassessed_company_rows':len(missing),'samples':missing[:20],'sample_limit':20,
            'reason':'prior_claim_policy' if prior_revision or different_analysis else ('session_deadline' if self.expired() and missing else ('not_claimed' if missing else None)),
            'claim_presence_is_not_current_revision_assessment':True,
            'not_a_wait_decision':True,'executable_opportunity_validated':False,
            'scope':'current_revisions_first_received_in_session_time_window_not_collector_ownership',
            'pipeline_latency':pipeline_latency(records,orders)}

    def summary(self):
        begin_read(self.repository)
        try:
            rows=[r for r in all_decisions(self.repository,self.account_ref) if r[1]['session_id']==self.session_id]
            ids={r[5] for r in rows}
            linked=[(i,s) for i,s,*_ in broker.all_intents(self.repository,self.account_ref) if i['id'] in ids]
            self.repository.connection.commit()
        except BaseException:self.repository.connection.rollback();raise
        samples=[]
        rows.sort(key=lambda r:(r[1]['record']['analysis']['timestamp'],r[1]['record']['news']['article_id'],r[1]['symbol']))
        for key,request,state,_,_,intent_id in rows:
            samples.append({'article_id':request['record']['news']['article_id'],'symbol':request['symbol'],
                'headline':request['record']['news']['data']['headline'],
                'company_label':self.label(request),'status':state['status'],'reason':state['reason'],
                'intent_id':intent_id,'feature_available_at':request['record']['analysis']['timestamp']})
        return {'session_id':self.session_id,'account_ref':self.account_ref,'decisions':len(rows),
                'status_counts':dict(Counter(r[2]['status'] for r in rows)),
                'reason_counts':dict(Counter(r[2]['reason'] for r in rows)),'samples':samples[:20],
                'sample_limit':20,'post_attempts':sum(s['post_attempts'] for _,s in linked),
                'broker_order_status_counts':dict(Counter(s['order']['status'] for _,s in linked if s['order'])),
                'uncertain_intents':[i['id'] for i,s in linked if s['phase'] in {'submitting','uncertain'}],
                'automatic_strategy_execution':True,'environment':'alpaca_paper',
                'publication_scope':'versions_first_received_during_this_session',
                'article_symbol_dedup_scope':'account_and_current_database_across_sessions',
                'profitability_validated':False,'live_trading_supported':False}


def run_paper_session(settings,request,engine,*,notional='10',max_buys=3,
                      repository_factory=Repository,api_factory=PaperAPI,session_runner=None,_fixture=False):
    from ..data.session import run_session,safe_error
    from .paper_monitor import save_run
    from .async_portfolio import AsyncPortfolioSampler,ASYNC_POLICY
    from .session_timing import Timings,TimedAPI
    from .report_evidence import price_warning_history,MAX_CYCLES
    if not settings.database_url.startswith('postgresql://') and not _fixture:
        raise DataValidationError('Paper news sessions require PostgreSQL')
    if engine.name!='finbert' and not _fixture:raise DataValidationError('Paper news sessions require local FinBERT')
    repository=repository_factory(settings.database_url);worker=None;sampler=None;result=None;last_research={};last_observation={}
    last_processed={};price_observations=[];timing=Timings();preparation_started=time.monotonic()
    def warning_summary():return price_warning_history({"cycles":[{"paper":o} for o in price_observations]})
    try:
        api=TimedAPI(api_factory(settings),timing)
        worker=PaperWorker(repository,api,request.symbols,request.feed,digest(analyzer_metadata(engine)),engine.name,
            duration=request.duration,notional=notional,max_buys=max_buys,_fixture=_fixture)
        worker.lease(acquire=True)
        worker.portfolio_sampling_policy=dict(ASYNC_POLICY)
        capture_equity(repository,api,session_id=worker.session_id);worker.last_equity_at=api.now()
        worker.lease()  # Preparation never revives an expired lease.
        worker.started=canonical_time(api.now());worker.deadline=utc_add(worker.started,request.duration)
        worker.monotonic_deadline=time.monotonic()+request.duration
        timeline={'started_at':worker.started,'deadline':worker.deadline,'duration_seconds':request.duration,
            'monotonic_deadline':worker.monotonic_deadline,'preparation_seconds':time.monotonic()-preparation_started,
            'duration_includes_price_warmup':True,'finalization_can_extend_wall_time':True}
        sampler=AsyncPortfolioSampler(repository_factory,api_factory,settings,worker.account_ref,worker.session_id,worker.started,timing)
        sampler.request('start');worker.background_check=sampler.raise_if_failed
        save_run(repository,worker,{'status':'running','paper':worker.summary(),'portfolio_sampling':sampler.summary()})
        def factory(url):
            repo=repository_factory(url);original=repo.begin_stream
            def begin(*args,**kwargs):
                run_id=original(*args,**kwargs);worker.stream_run_id=run_id;return run_id
            repo.begin_stream=begin
            return repo
        def hook(repo,research,metadata):
            nonlocal last_research,last_observation,last_processed
            research['paper_worker_session_id']=worker.session_id
            last_research={k:research[k] for k in ('session_id','started_at','news_polls','received_articles','new_versions','inserted_analyses','price_warmup') if k in research}
            worker.lease();sampler.raise_if_failed()
            with timing.measure('worker_tick'):last_observation=worker.tick()
            if last_observation.get('status')=='processed':
                last_processed=last_observation
                if len(price_observations)>=MAX_CYCLES:raise StorageError('Paper price observation bound exceeded')
                price_observations.append({k:last_observation[k] for k in ('status','observed_at','price_gap_symbols','price_clock_flags')})
            sampler.request()
            paper=worker.summary()
            save_run(repository,worker,{'status':'running','research_session':last_research,'observation':last_observation,'last_processed_observation':last_processed,'paper':paper,'portfolio_sampling':sampler.summary(),
                'execution_timeline':{k:v for k,v in timeline.items() if k!='monotonic_deadline'},'timing':timing.summary(),
                'price_warning_history':warning_summary()})
            log.info('Paper cycle: selected=%d analysis_ready=%d decisions=%d post_attempts=%d',last_observation.get('selected_recent_articles',0),last_observation.get('analyses_available',0),paper['decisions'],paper['post_attempts'])
            return last_observation
        result=(session_runner or run_session)(settings,request,engine,repository_factory=factory,_cycle_hook=hook,_timeline=timeline,_timing=timing)
        result={'status':result['status'],'environment':'alpaca_paper','research_session':result,
                'paper':worker.summary(),'observation':last_observation,'last_processed_observation':last_processed,
                'execution_timeline':{k:v for k,v in timeline.items() if k!='monotonic_deadline'},'automatic_strategy_execution':True,'live_trading_supported':False,
                'profitability_validated':False,'positions_liquidated_at_session_end':False,'portfolio_sampling':sampler.summary()}
        return result
    except BaseException as exc:
        if worker is not None and worker.lease_acquired:
            result={'status':'interrupted' if isinstance(exc,KeyboardInterrupt) else 'failed','paper':worker.summary(),
                    'research_session':last_research,'observation':last_observation,'error_details':safe_error(exc,'paper_session')}
        raise
    finally:
        if worker is not None and worker.lease_acquired:
            try:worker.finish()
            except Exception as exc:
                if result is not None:result['worker_cleanup_error']=safe_error(exc,'worker_cleanup')
            if sampler is not None:
                try:sampler.finish()
                except Exception as exc:
                    if result is not None:result['portfolio_capture_error']=safe_error(exc,'portfolio_finalization')
                if result is not None:result['portfolio_sampling']=sampler.summary()
            try:
                capture_equity(repository,worker.api,session_id=worker.session_id)
            except Exception as exc:
                if result is not None:result['equity_capture_error']=safe_error(exc,'equity_capture')
            if result is not None:
                result['timing']=timing.summary();result['price_warning_history']=warning_summary()
                try:result['decision_coverage']=worker.coverage(canonical_time(result.get('research_session',{}).get('data_cutoff_at') or worker.api.now()))
                except Exception as exc:result['coverage_error']=safe_error(exc,'decision_coverage')
                try:result['paper']=worker.summary()
                except Exception:result['paper_audit_error']='paper_summary_failed'
                try:
                    save_run(repository,worker,result)
                    result['saved_paper_session_id']=worker.session_id
                except Exception as exc:result['paper_audit_error']=safe_error(exc,'paper_audit')
        repository.close()
