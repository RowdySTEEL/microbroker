"""Bounded live price worker + polling news/analysis coordinator, no orders.

Each worker owns its DB connection. Per-stage commits are retained on failure;
this is not one all-or-nothing session transaction or exact point-in-time data.
"""
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
import logging
import threading
import time
import uuid
from ..config import UNIVERSE
from .models import DataValidationError, validate_symbols
from .news import AlpacaNews, NewsRequest, collect_news
from .adapters.alpaca_stream import AlpacaStream
from .stream_models import StreamRequest
from .storage.repository import Repository, StorageError
from .news_replay import NewsReplayRequest, create_news_replay, digest
from .analysis_replay import create_analysis_replay
from .research import encode
from .session_prices import create_session_prices, SCOPE
from .alignment import research_alignment
from ..intelligence.analysis import analyze_news, AnalysisRequest
from ..intelligence.policy import analyzer_metadata
from .session_coverage import session_coverage

log=logging.getLogger('microbroker.session')

def safe_error(exc, stage):
    """Only fixed diagnostic identifiers, never exception messages or request data."""
    known_types={'MarketDataError','DataValidationError','StorageError','OperationalError',
        'ProgrammingError','IntegrityError','InterfaceError','DatabaseError','NameError',
        'TypeError','ValueError','AttributeError','RuntimeError','UnicodeEncodeError',
        'ImportError','ModuleNotFoundError','TimeoutError','ConnectionError','OSError','PaperBrokerError'}
    name=type(exc).__name__
    result={'stage':stage,'exception_type':name if name in known_types else 'OtherError'}
    from .adapters.alpaca import MarketDataError
    if isinstance(exc,MarketDataError):
        if exc.code in {'network_error','http_error','invalid_json','invalid_response','pagination_limit'}:
            result['category']=exc.code
        if type(exc.http_status) is int and 100<=exc.http_status<=599:
            result['http_status']=exc.http_status
    from ..execution.paper_api import PaperBrokerError
    if isinstance(exc,PaperBrokerError):
        import re
        if re.fullmatch('[a-z_]{1,80}',exc.code):result['category']=exc.code
    # PostgreSQL SQLSTATE is an identifier, not a server error message.
    for _ in range(4):
        state=getattr(exc,'sqlstate',None)
        if isinstance(state,str) and len(state)==5 and all(c in '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ' for c in state):
            result['sqlstate']=state;break
        exc=exc.__cause__ or exc.__context__
        if exc is None:break
    return result

@dataclass(frozen=True)
class SessionRequest:
    symbols: tuple = ('AAPL','MSFT')
    feed: str = 'iex'
    duration: int = 300
    poll_seconds: int = 60
    lookback_minutes: int = 10
    def __post_init__(self):
        object.__setattr__(self,'symbols',tuple(sorted(validate_symbols(self.symbols))))
        if not set(self.symbols).issubset(UNIVERSE) or self.feed not in {'iex','sip'}:
            raise DataValidationError('Session requires configured symbols and IEX/SIP feed')
        for value,lower,upper in ((self.duration,60,3600),(self.poll_seconds,30,300),(self.lookback_minutes,1,60)):
            if type(value) is not int or not lower<=value<=upper:
                raise DataValidationError('Session bounds: duration 60..3600, poll 30..300, lookback 1..60')
    def metadata(self):
        return {'symbols':list(self.symbols),'feed':self.feed,'duration':self.duration,
                'poll_seconds':self.poll_seconds,'lookback_minutes':self.lookback_minutes,
                'channels':['bars','updatedBars'],'news_max_pages':5,'format':'R14',
                'price_scope':SCOPE,'price_warmup_max_seconds':min(90,self.duration-30),
                'news_scope':'versions_first_received_during_session',
                'duration_semantics':'soft_deadline_inflight_poll_analysis_and_finalization_may_extend'}

def run_session(settings, request, engine, *, repository_factory=Repository,
                stream_factory=AlpacaStream, news_factory=AlpacaNews,
                clock=time.monotonic, utc_now=lambda:datetime.now(timezone.utc),
                wait=None, _allow_sqlite_fixture=False, _warmup_wait=None, _cycle_hook=None, _timeline=None, _timing=None):
    settings.require_credentials()
    if settings.data_env!='production' or (not settings.database_url.startswith('postgresql://') and not _allow_sqlite_fixture):
        raise DataValidationError('Research sessions require production market data and PostgreSQL')
    repository=repository_factory(settings.database_url)
    session_id=str(uuid.uuid4());started=utc_now();deadline=clock()+request.duration
    if _timeline is not None:
        started=datetime.fromisoformat(_timeline['started_at'].replace('Z','+00:00'))
        remaining=max(0,min(request.duration,(datetime.fromisoformat(_timeline['deadline'].replace('Z','+00:00'))-utc_now()).total_seconds()))
        deadline=_timeline['monotonic_deadline'] if clock is time.monotonic else clock()+remaining
    stop=threading.Event();done=threading.Event();prices_ready=threading.Event();price_symbols=set();worker_result={}
    metadata={**request.metadata(),'analyzer_key':digest(analyzer_metadata(engine)),'engine':engine.name}
    result={'session_id':session_id,'status':'running','started_at':started.isoformat(),
            'news_polls':0,'received_articles':0,'new_versions':0,'inserted_analyses':0,'cycles':[],
            'snapshots':{},'alignment':None,'error_code':None,'committed_stages_preserved':True,
            'point_in_time_validated':False,'orders_sent':0,'price_scope':SCOPE}
    if _cycle_hook is not None:
        result.pop('orders_sent')
        result['execution_scope']='alpaca_paper_guarded_news_worker'
    if _timeline is not None:result['execution_timeline']={k:v for k,v in _timeline.items() if k!='monotonic_deadline'}
    m=repository.marker
    def audit(finished=None):
        repository.connection.execute(
            f"INSERT INTO research_sessions VALUES ({','.join([m]*6)}) ON CONFLICT (id) DO UPDATE SET finished_at=excluded.finished_at,status=excluded.status,result_json=excluded.result_json",
            (session_id,started.isoformat(),finished,result['status'],encode(metadata),encode(result)))
        repository.connection.commit()
    def stream_worker():
        worker_repo=None
        try:
            worker_repo=repository_factory(settings.database_url)
            worker_repo.capture_bar_receipts=True
            adapter=stream_factory(settings)
            def on_price(event):
                if event.bar is not None:
                    age=(utc_now()-(event.bar.timestamp+timedelta(minutes=1))).total_seconds()
                    if 0<=age<=300:
                        price_symbols.add(event.symbol)
                        if set(request.symbols)<=price_symbols:prices_ready.set()
            worker_result['result']=adapter.run(StreamRequest(request.symbols,request.feed,
                ('bars','updatedBars'),request.duration),worker_repo,on_event=on_price,stop_event=stop)
        except Exception as exc:
            worker_result['result']={'status':'failed','error_code':'stream_worker_failed',
                'error_details':safe_error(exc,'stream_worker')}
        finally:
            try:
                if worker_repo is not None:worker_repo.close()
            except Exception:
                worker_result['result']={'status':'failed','error_code':'stream_close_failed'}
            finally:done.set()
    worker=threading.Thread(target=stream_worker,name='microbroker-price-worker',daemon=True)
    stage='start'
    try:
        audit();worker.start()
        stage='price_warmup';warmup_started=clock();warmup_seconds=min(90,request.duration-30)
        if _warmup_wait is not None:_warmup_wait(warmup_seconds)
        else:
            while not prices_ready.is_set() and not done.is_set() and clock()<warmup_started+warmup_seconds:
                prices_ready.wait(min(.25,max(0,warmup_started+warmup_seconds-clock())))
        result['price_warmup']={'status':'ready' if prices_ready.is_set() else ('stream_stopped' if done.is_set() else 'timed_out'),
            'ready_symbols':sorted(price_symbols),'elapsed_seconds':clock()-warmup_started,
            'max_seconds':warmup_seconds,'criteria':'committed_receipt_per_symbol_completed_within_5_minutes'}
        adapter=news_factory(settings)
        while clock()<deadline and not stop.is_set() and not done.is_set():
            stage='news_poll';end=utc_now()
            from contextlib import nullcontext
            with _timing.measure('news_poll') if _timing else nullcontext():
                collected=collect_news(adapter,repository,NewsRequest(getattr(request,'news_symbols',request.symbols),
                    end-timedelta(minutes=request.lookback_minutes),end,50,5))
            result['news_polls']+=1;result['new_versions']+=collected['new_versions']
            result['received_articles']+=collected['received_articles']
            analyzed=None
            if collected['new_versions']:
                stage='analysis'
                # Existing immutable analyses are reused; session snapshots below
                # exclude pre-session news even though this bounded current scan can include them.
                with _timing.measure('news_analysis_batch') if _timing else nullcontext():
                    analyzed=analyze_news(repository,AnalysisRequest(symbols=getattr(request,'news_symbols',request.symbols),
                        first_seen_since=started.isoformat() if _cycle_hook is not None else None),engine)
                result['inserted_analyses']+=analyzed['inserted_analyses']
            result['cycles'].append({'news_run_id':collected['run_id'],'new_versions':collected['new_versions'],
                'received_articles':collected['received_articles'],
                'analysis_run_id':analyzed['run_id'] if analyzed else None,
                'inserted_analyses':analyzed['inserted_analyses'] if analyzed else 0})
            if _cycle_hook is not None:
                stage='paper_cycle'
                result['cycles'][-1]['paper']=_cycle_hook(repository,result,metadata)
            audit();log.info('Research session progress: polls=%d new_versions=%d inserted_analyses=%d',
                result['news_polls'],result['new_versions'],result['inserted_analyses'])
            if done.is_set():break
            seconds=max(0,min(request.poll_seconds,deadline-clock()))
            if wait is not None:wait(seconds)
            else:done.wait(seconds)
        result['status']='completed'
    except KeyboardInterrupt:
        result['status']='stopped';result['error_code']='keyboard_interrupt'
    except Exception as exc:
        repository.connection.rollback();result['status']='failed';result['error_code']=stage+'_failed'
        result['error_details']=safe_error(exc,stage)
        log.error('Research session stage failed: %s; diagnostics=%s; raw exception suppressed',stage,encode(result['error_details']))
    finally:
        stop.set()
        if worker.ident is not None:worker.join(timeout=15)
        result['stream']=worker_result.get('result',{'status':'failed','error_code':'stream_shutdown_timeout'})
        if worker.is_alive():result['status']='failed';result['error_code']='stream_shutdown_timeout'
        elif result['stream']['status']=='failed':
            result['status']='failed';result['error_code']=result['error_code'] or 'stream_failed'
    try:
        finished=utc_now();result['data_cutoff_at']=finished.isoformat();result['finished_at']=finished.isoformat()
        result['elapsed_seconds']=(finished-started).total_seconds()
        if result['status'] in {'completed','stopped'}:
            stage='finalize'
            news=create_news_replay(repository,NewsReplayRequest(finished.isoformat(),getattr(request,'news_symbols',request.symbols),
                first_seen_since=started.isoformat()))
            analysis=create_analysis_replay(repository,news['replay_id'],finished.isoformat(),metadata['analyzer_key'],engine.name)
            result['snapshots'].update(news_replay_id=news['replay_id'],analysis_replay_id=analysis['replay_id'])
            result['session_articles']=analysis['available_articles'];result['available_analyses']=analysis['available_analyses']
            bars=create_session_prices(repository,result['stream']['run_id'],request.symbols,request.feed,
                started.isoformat(),finished.isoformat())
            result['snapshot_minute_bars']=bars['unique_minute_bars']
            result['snapshot_price_receipts']=bars['item_count']
            if bars['status']=='ready':
                result['snapshots']['bars_replay_id']=bars['replay_id']
                result['alignment']=research_alignment(repository,analysis['replay_id'],bars['replay_id'])
            else:result['price_coverage']='no_minute_bars_in_session_window'
            if result['status']=='completed' and (analysis['available_articles']==0 or bars['item_count']==0):
                result['status']='no_data'
        result['warning_codes']=['legacy_receipt_and_analysis_times_not_commit_times',
            'stream_receipt_time_not_commit_time','poll_window_can_miss_late_provider_updates',
            'inflight_operations_can_extend_duration','independent_stage_commits','minute_bar_gap_recovery_deferred']
        result['finished_at']=utc_now().isoformat()
        result['elapsed_seconds']=(datetime.fromisoformat(result['finished_at'])-started).total_seconds()
        result['coverage']=session_coverage(result)
        audit(result['finished_at'])
    except Exception as exc:
        repository.connection.rollback();result['status']='failed';result['error_code']='finalization_failed'
        result['error_details']=safe_error(exc,'finalize')
        result['coverage']=session_coverage(result)
        try:audit(utc_now().isoformat())
        except Exception:repository.connection.rollback()
    finally:repository.close()
    return result

def session_fixture(*, news_empty=False, stream_empty=False, stream_failed=False, news_failed=False, analysis_failed=False, native_transport=None):
    """Private temporary DB and fake price transport; exercises separate thread ownership."""
    import tempfile
    from pathlib import Path
    from ..config import Settings
    from .news import normalize_news
    from ..intelligence.sentiment import RulesEngine
    from unittest.mock import patch
    fixture_price_done=threading.Event()
    class FixtureNews:
        pages=1
        def __init__(self,settings):self.published=datetime.now(timezone.utc)-timedelta(minutes=1)
        def iter_news(self,request):
            if news_failed:raise RuntimeError('SYNTHETIC_SECRET_MUST_NOT_LEAK')
            if not news_empty:
                now=self.published
                yield normalize_news({'id':99001,'headline':'Microsoft beats expectations.','summary':'',
                    'content':'','created_at':now.isoformat(),'updated_at':now.isoformat(),
                    'symbols':['MSFT'],'source':'original-synthetic','url':'https://example.com/fixture'})
    class FixtureStream:
        def __init__(self,settings):pass
        def run(self,request,repository,on_event=None,stop_event=None):
            from .stream_models import normalize_event
            stats={'events_received':0,'events_inserted':0,'duplicates':0,'bars_written':0,
                'received_by_type':{},'received_by_symbol':{}}
            run_id=repository.begin_stream(request,stats)
            if stream_failed:
                fixture_price_done.set();return {'run_id':run_id,'status':'failed','error_code':'synthetic_transport',**stats}
            if stream_empty:
                fixture_price_done.set()
                stop_event.wait(2)
                return {'run_id':run_id,'status':'no_data',**stats}
            t=datetime.now(timezone.utc).replace(second=0,microsecond=0)-timedelta(minutes=1)
            event=normalize_event({'T':'b','S':'MSFT','t':t.isoformat(),'o':100,'h':102,'l':99,'c':101,'v':100,'n':5},request)
            stats=repository.apply_stream_frame(run_id,[event],stats)
            if on_event:on_event(event)
            fixture_price_done.set()
            stop_event.wait(2)
            return {'run_id':run_id,'status':'completed',**stats}
    class FixtureClock:
        value=0
        def __call__(self):return self.value
        def wait(self,seconds):self.value+=seconds
    with tempfile.TemporaryDirectory() as directory:
        url='sqlite:///'+str(Path(directory)/'session.db');settings=Settings(database_url=url,api_key='FIXTURE',api_secret='FIXTURE')
        clock=FixtureClock()
        def fixture_warmup(seconds):
            fixture_price_done.wait(2)
            if stream_empty:clock.wait(seconds)
        with patch.object(log,'disabled',True), (patch('microbroker.data.session.analyze_news',side_effect=RuntimeError('SYNTHETIC_SECRET')) if analysis_failed else __import__('contextlib').nullcontext()):
            result=run_session(settings,SessionRequest(('MSFT',),duration=60,poll_seconds=30),RulesEngine(),
                stream_factory=FixtureStream,
                news_factory=(lambda settings:AlpacaNews(settings,transport=native_transport,sleep=lambda _:None)) if native_transport else FixtureNews,
                clock=clock,wait=clock.wait,_allow_sqlite_fixture=True,_warmup_wait=fixture_warmup)
        repository=Repository(url)
        try:
            audit=repository.connection.execute('SELECT status,result_json FROM research_sessions').fetchone()
            articles=repository.connection.execute('SELECT COUNT(*) FROM news_versions').fetchone()[0]
            return result, audit, articles
        finally:repository.close()

def verify_session():
    checks=[]
    def require(ok,name):
        if not ok:raise StorageError('Session verification failed: '+name)
        checks.append(name)
    result,audit,_=session_fixture()
    require(result['status']=='completed' and result['news_polls']>=1,'concurrent_private_session_completed')
    require(result['new_versions']==1 and result['inserted_analyses']==1,'repeated_news_poll_idempotency')
    require(result['session_articles']==1 and result['available_analyses']==1,'session_receipt_scoped_snapshots')
    require(result['alignment']['matched_rows']==1,'same_session_company_price_alignment')
    require(audit[0]==result['status'] and parsed_session_result(audit[1])==result,'session_final_audit')
    empty,_,_=session_fixture(news_empty=True)
    require(empty['status']=='no_data' and empty['session_articles']==0,'empty_news_not_claimed_success')
    price_empty,_,_=session_fixture(stream_empty=True)
    require(price_empty['status']=='no_data' and price_empty['price_coverage']=='no_minute_bars_in_session_window','empty_prices_not_claimed_success')
    failed,_,_=session_fixture(stream_failed=True)
    require(failed['status']=='failed' and failed['error_code']=='stream_failed','stream_failure_propagated')
    failed,_,_=session_fixture(news_failed=True)
    require(failed['status']=='failed' and 'SYNTHETIC_SECRET' not in encode(failed),'news_failure_sanitized')
    failed,_,articles=session_fixture(analysis_failed=True)
    require(failed['status']=='failed' and articles==1 and failed['committed_stages_preserved'],'analysis_failure_keeps_committed_news')
    from .adapters.alpaca import Response
    published=datetime.now(timezone.utc)-timedelta(minutes=1)
    body=encode({'news':[{'id':99002,'headline':'Microsoft beats expectations.',
        'created_at':published.isoformat(),'updated_at':published.isoformat(),
        'symbols':['MSFT']}],'next_page_token':None}).encode()
    native,_,_=session_fixture(native_transport=lambda *args:Response(200,{},body))
    require(native['status']=='completed' and native['new_versions']==1,'native_news_adapter_session')
    denied,audit,_=session_fixture(native_transport=lambda *args:Response(401,{},b'SYNTHETIC_SECRET'))
    require(denied['error_details']=={'stage':'news_poll','exception_type':'MarketDataError',
        'category':'http_error','http_status':401} and parsed_session_result(audit[1])==denied,
        'native_http_failure_safe_diagnostics_and_audit')
    require(native['coverage']['assessment']=='aligned_research_rows' and
        native['coverage']['prices']['stream_bars_written']==1,'coverage_separates_stream_and_snapshot_prices')
    require('news_queries_returned_empty' in empty['coverage']['reason_codes'] and
        empty['coverage']['market_open_checked'] is False,'empty_news_coverage_no_market_calendar_claim')
    require(denied['coverage']['assessment']=='failed' and
        denied['coverage']['news']['snapshot_articles'] is None,'failed_stage_not_reported_as_empty_success')
    return {'status':'passed','checks':checks,'fixture_source':'original_synthetic_concurrent_transports',
            'network_required':False,'model_required':False,'user_database_modified':False,
            'point_in_time_validated':False,'orders_sent':0}

def parsed_session_result(raw):
    import json
    return json.loads(raw)


def session_summary(repository, session_id):
    from .research import begin_read
    from .news_replay import parsed_json
    begin_read(repository)
    try:
        row=repository.connection.execute(f'SELECT status,request_json,result_json FROM research_sessions WHERE id={repository.marker}',(session_id,)).fetchone()
        if not row:raise DataValidationError('Unknown research session_id')
        result=parsed_json(row[2]);request=parsed_json(row[1])
        if result.get('session_id')!=session_id or result.get('status')!=row[0]:
            raise StorageError('Research session audit identity mismatch')
        repository.connection.commit()
    except BaseException:
        repository.connection.rollback();raise
    return {'session':result,'request':request,'coverage':session_coverage(result),
            'database_modified':False,'audit_authenticity_verified':False}
