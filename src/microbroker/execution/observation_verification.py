"""Original controlled R28 fixtures. No provider calls, model or user database."""
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
from .paper_worker_verification import WorkerFixture
from .paper_broker_verification import FakePaper
from .paper_api import PaperBrokerError
from .paper_portfolio import capture_portfolio,read_history
from .paper_monitor import save_run,report_session
from .latency_report import latency_report,render_latency
from .session_timing import Timings,TimedAPI,distribution
from .async_portfolio import AsyncPortfolioSampler
from . import paper_broker as broker
from ..config import Settings
from ..data.research import begin_read
from ..data.storage.repository import Repository,StorageError


def observation_checks(repository):
    checks=[]
    def require(ok,name):
        if not ok:raise StorageError('R28 observation verification failed: '+name)
        checks.append(name)
    require(distribution([])['p95_seconds'] is None,'timing_empty_not_zero')
    d=distribution(list(range(1,21)))
    require(d['median_seconds']==10.5 and d['p95_seconds']==19,'timing_nearest_rank_p95')
    require(distribution([-1,2])['invalid_or_negative_samples']==1,'timing_negative_clock_delta_disclosed')
    f=WorkerFixture(repository,'r28-coverage',80);f.news();f.fake.at='2026-01-05T15:00:10.000000000Z'
    coverage=f.worker.coverage(f.fake.at)
    require(coverage['unassessed_analyses']==1 and coverage['unassessed_company_rows']==1 and len(f.decisions())==0,
        'coverage_read_does_not_create_wait_or_claim')
    f.tick();processed=f.worker.coverage(f.fake.at)
    require(processed['unassessed_analyses']==0 and processed['claimed_by_this_session']==1,'coverage_claimed_not_unassessed')
    f.news(index=1,at='2026-01-05T15:00:11Z');f.fake.at='2026-01-05T15:00:12.000000000Z'
    f.worker.monotonic_deadline=0
    require(f.tick()['status']=='deadline' and f.fake.posts==0,'monotonic_deadline_blocks_tick_with_earlier_wall_time')
    coverage=f.worker.coverage(f.fake.at)
    require(coverage['unassessed_analyses']==1 and coverage['reason']=='session_deadline' and not coverage['executable_opportunity_validated'],
        'coverage_deadline_not_missed_trade_claim')
    timer=Timings();value={'status':'completed','paper':f.worker.summary(),'observation':{'status':'deadline'},
        'last_processed_observation':{'status':'processed','fresh_price_symbols':['MSFT'],'price_gap_symbols':[]},
        'decision_coverage':coverage,'timing':timer.summary()}
    save_run(repository,f.worker,value);before=len(f.fake.calls);r=latency_report(repository,f.worker.session_id)
    require(r==latency_report(repository,f.worker.session_id) and len(f.fake.calls)==before and not r['network_required'],
        'timing_offline_frozen_repeat_no_api')
    require(r['pipeline']['stages']['analysis_recorded_to_post_claim']['samples']==0 and 'ölçülemedi' in render_latency(r),
        'timing_absent_order_not_zero_fill_latency')
    require(report_session(repository,f.worker.session_id)['latest_cycle']['status']=='processed','deadline_preserves_last_processed_observation')
    f.worker.finish()
    g=WorkerFixture(repository,'r28-fill-time',81);g.news();g.ready();g.tick();cid=g.fill()
    own=g.decisions();broker.reconcile(repository,g.fake.client,own[0][5]);r=g.worker.coverage(g.fake.at)
    require(r['pipeline_latency']['stages']['post_claim_to_first_local_filled_observation']['samples']==1 and
        not r['pipeline_latency']['actual_broker_fill_timestamp_measured'],'timing_first_local_fill_not_actual_execution_time')
    baseline=broker.report(repository,own[0][5])
    capture_portfolio(repository,g.fake.client,snapshot_id='r28-passive-core',_passive=True,_account_ref=g.worker.account_ref)
    require(broker.report(repository,own[0][5])==baseline,'passive_capture_never_reconciles_order_audit')
    api=TimedAPI(g.fake.client,timer,observer=True)
    require(not hasattr(api,'submit') and not hasattr(api,'cancel') and not hasattr(api,'lookup'),'observer_capability_excludes_order_operations')
    g.worker.finish()
    return checks


def concurrent_checks():
    checks=[]
    with TemporaryDirectory() as folder:
        settings=Settings(database_url='sqlite:///'+str(Path(folder)/'db'))
        repository=Repository(settings.database_url);f=WorkerFixture(repository,'r28-concurrent',82);f.news();f.ready()
        entered=threading.Event();release=threading.Event();closed=threading.Event();owners=[]
        original=f.fake.client.valued_positions
        def slow():
            entered.set()
            if not release.wait(5):raise AssertionError('Controlled observer was not released')
            return original()
        f.fake.client.valued_positions=slow
        def factory(url):
            repo=Repository(url);owners.append(threading.get_ident());close=repo.close
            def finish():
                if owners[-1]!=threading.get_ident():raise AssertionError('Connection crossed thread')
                close();closed.set()
            repo.close=finish;return repo
        observer=AsyncPortfolioSampler(factory,lambda _:f.fake.client,settings,f.worker.account_ref,f.worker.session_id,f.worker.started,Timings())
        try:
            observer.request('start')
            if not entered.wait(3):raise StorageError('Observer did not enter controlled slow GET')
            f.tick()
            if f.fake.posts!=1 or release.is_set():raise StorageError('Decision waited for observer')
            checks.append('slow_observer_does_not_block_single_order_writer')
            for _ in range(20):observer.request()
            if observer.summary()['coalesced_requests']<19:raise StorageError('Observer queue unbounded')
            checks.append('observer_queue_coalesces_without_backfill')
            release.set();observer.finish(timeout=5)
            summary=observer.summary()
            if not any(e.get('category')=='portfolio_sources_changed' for e in summary['errors']):raise StorageError('Changed sources were not rejected')
            checks.append('concurrent_order_source_change_rejects_observer_sample')
            if not closed.is_set() or owners==[threading.get_ident()] or observer.thread.is_alive():raise StorageError('Observer ownership/shutdown failed')
            checks.append('observer_owns_and_closes_separate_connection')
        finally:
            release.set();observer.finish(timeout=5);f.worker.finish();repository.close()
    return checks


def verify_observation():
    repository=Repository('sqlite:///:memory:')
    try:checks=observation_checks(repository)
    finally:repository.close()
    checks+=concurrent_checks()
    return {'status':'passed','checks':checks,'fixture_source':'original_synthetic_times_and_blocked_fake_GET',
        'network_required':False,'model_required':False,'user_database_modified':False,
        'orders_sent_to_alpaca':0,'real_postgresql_concurrency_validated':False,
        'actual_fill_latency_validated':False,'profitability_validated':False}
