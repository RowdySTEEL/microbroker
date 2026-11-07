"""Original deterministic news/Paper worker fixtures. No external network or model."""
from pathlib import Path
import json
from .paper_broker_verification import FakePaper
from .strategy_verification import FixtureEngine
from .paper_worker import PaperWorker,all_decisions,read_decision,current_records,utc_add
from .equity_history import capture_equity,performance_report
from . import paper_broker as broker
from .paper_api import PaperBrokerError
from ..data.news import normalize_news
from ..data.session_price_verification import apply_at,empty_stats
from ..data.stream_models import StreamRequest,normalize_event,canonical_time
from ..data.research import encode,begin_read
from ..data.storage.repository import Repository,StorageError
from ..data.models import DataValidationError
from ..intelligence.analysis import compute_analysis
from ..intelligence.policy import digest,analyzer_metadata


class WorkerFixture:
    def __init__(self,repository,name,index):
        self.repo=repository;self.fake=FakePaper('r23-'+name);self.base=99923000+index*100
        self.engine=FixtureEngine();self.engine.metadata={**self.engine.metadata,'version':'r23-worker-'+name}
        self.key=digest(analyzer_metadata(self.engine))
        self.worker=PaperWorker(repository,self.fake.client,('MSFT',),'iex',self.key,'rules',_fixture=True)
        self.worker.lease(acquire=True)
        self.request=StreamRequest(('MSFT',),channels=('bars','updatedBars'))
        prior=repository.capture_bar_receipts;repository.capture_bar_receipts=True
        self.worker.stream_run_id=repository.begin_stream(self.request,empty_stats());repository.capture_bar_receipts=prior
        self.stats=empty_stats()
    def news(self,index=0,text='Microsoft profits rose sharply.',at='2026-01-05T15:00:01Z',first=None,created=None):
        at=canonical_time(at);first=canonical_time(first or at);created=canonical_time(created or first)
        n=normalize_news({'id':self.base+index,'headline':'Microsoft update','content':text,'created_at':created,
            'updated_at':created,'symbols':['MSFT'],'source':'original-synthetic','url':'https://example.com/r23-original'})
        data=n['data'];m=self.repo.marker
        self.repo.connection.execute(f'INSERT INTO news_versions VALUES ({",".join([m]*7)})',
            ('alpaca',data['id'],n['revision_hash'],data['created_at'],data['updated_at'],first,encode(data)))
        for symbol,method in n['mappings'].items():
            self.repo.connection.execute(f'INSERT INTO news_symbols VALUES ({",".join([m]*5)})',('alpaca',data['id'],n['revision_hash'],symbol,method))
        self.repo.connection.execute(f'INSERT INTO news_articles VALUES ({",".join([m]*8)})',
            ('alpaca',data['id'],n['revision_hash'],data['updated_at'],first,first,n['fingerprint'],int(bool(data['content_text']))))
        result=compute_analysis({'data':data,'first_seen':first,'mappings':n['mappings']},self.engine)
        self.repo.connection.execute(f'INSERT INTO news_analyses VALUES ({",".join([m]*9)})',
            ('alpaca',data['id'],n['revision_hash'],self.key,'rules',result['input_sha256'],at,encode(result),digest(result)))
        self.repo.connection.commit();return n
    def bar(self,minute=1,receipt=None):
        event=normalize_event({'T':'b','S':'MSFT','t':f'2026-01-05T15:{minute:02}:00Z','o':100,'h':102,'l':99,'c':101,'v':100,'n':5},self.request)
        prior=self.repo.capture_bar_receipts;self.repo.capture_bar_receipts=True
        try:self.stats=apply_at(self.repo,self.worker.stream_run_id,event,self.stats,receipt or f'2026-01-05T15:{minute+1:02}:00.1Z')
        finally:self.repo.capture_bar_receipts=prior
    def ready(self):
        self.fake.at='2026-01-05T15:01:00.000000000Z';self.worker.lease()
        self.bar(0);self.bar(1);self.fake.at='2026-01-05T15:02:10.000000000Z'
    def tick(self):return self.worker.tick()
    def decisions(self):
        begin_read(self.repo)
        try:
            rows=[r for r in all_decisions(self.repo,self.worker.account_ref) if r[1]['session_id']==self.worker.session_id]
            self.repo.connection.commit();return rows
        except BaseException:self.repo.connection.rollback();raise
    def fill(self):
        cid=next(reversed(self.fake.orders));self.fake.update(cid,'filled','0.1');return cid


def worker_checks(repository):
    checks=[]
    def require(ok,name):
        if not ok:raise StorageError('Paper session verification failed: '+name)
        checks.append(name)
    def rejects(call):
        try:call()
        except (StorageError,PaperBrokerError,DataValidationError):return True
        return False
    f=WorkerFixture(repository,'roundtrip',0);f.news();f.ready();f.tick()
    require(f.fake.posts==1 and f.decisions()[0][2]['status']=='submitted','worker_new_positive_submits_one_paper_buy')
    root=f.decisions()[0];require(root[2]['price']['timestamp']>=root[1]['record']['analysis']['timestamp'],'worker_waits_for_minute_start_after_analysis')
    f.tick();require(f.fake.posts==1,'worker_repeated_news_poll_no_second_post')
    f.fill();f.tick();require(broker.reconcile_account(repository,f.fake.client)['status']=='matched','worker_cumulative_broker_fill_reconciled')
    f.news(1,'Microsoft shares fell sharply.',at='2026-01-05T15:02:20Z');f.fake.at='2026-01-05T15:03:00.000000000Z';f.worker.lease();f.bar(2);f.bar(3)
    f.fake.at='2026-01-05T15:04:10.000000000Z';f.tick()
    require(f.fake.posts==2 and list(f.fake.orders.values())[-1]['side']=='sell','worker_negative_exits_only_managed_filled_entry')
    f.fill();f.tick();require(broker.reconcile_account(repository,f.fake.client)['positions']=={},'worker_roundtrip_has_no_residual_position')
    f.worker.finish();require(repository.connection.execute(f'SELECT token FROM paper_worker_leases WHERE account_ref={repository.marker}',(f.worker.account_ref,)).fetchone() is None,'worker_session_releases_own_lease')
    repository.connection.commit()
    # Stable account/article/company claim survives sessions and analyzer revisions.
    f.fake.at=utc_add(f.fake.at,1);other=PaperWorker(repository,f.fake.client,('MSFT',),'iex',f.key,'rules',_fixture=True);other.lease(acquire=True)
    old=other.create(root[1]['record'],'MSFT');require(not old[1] and f.fake.posts==2,'worker_article_company_claim_survives_restart');other.finish()
    g=WorkerFixture(repository,'closed',1);g.news();g.ready();g.fake.open=False;g.tick()
    require(g.fake.posts==0 and g.decisions()[0][2]['reason']=='market_closed','worker_closed_market_records_wait_without_queue');g.worker.finish()
    g=WorkerFixture(repository,'neutral',2);g.news(text='Microsoft publishes an update.');g.ready();g.tick()
    require(g.fake.posts==0 and g.decisions()[0][2]['reason']=='neutral_company_decision','worker_neutral_news_does_not_trade');g.worker.finish()
    g=WorkerFixture(repository,'conflict',3);g.news(text='Microsoft profits rose. Microsoft shares fell.');g.ready();g.tick()
    require(g.fake.posts==0 and g.decisions()[0][2]['reason']=='company_decision_withheld','worker_withheld_company_no_raw_or_document_fallback');g.worker.finish()
    g=WorkerFixture(repository,'negative',4);g.news(text='Microsoft shares fell.');g.ready();g.tick()
    require(g.fake.posts==0 and g.decisions()[0][2]['reason']=='no_managed_worker_position','worker_negative_without_owned_position_no_short');g.worker.finish()
    g=WorkerFixture(repository,'oldpublication',5);g.news(created='2026-01-05T14:00:00Z');g.ready();g.tick()
    require(g.fake.posts==0 and g.decisions()[0][2]['reason']=='news_expired_or_future','worker_old_publication_not_revived_by_fresh_receipt');g.worker.finish()
    g=WorkerFixture(repository,'unfinished',6);g.news();g.fake.at='2026-01-05T15:01:10.000000000Z';g.bar(0);g.tick()
    require(g.fake.posts==0 and g.decisions()[0][2]['status']=='pending','worker_previous_minute_close_not_execution_price');g.worker.finish()
    require(g.decisions()[0][2]['reason']=='worker_session_ended','worker_pending_signal_retired_at_session_end')
    g=WorkerFixture(repository,'clock',7);g.news();g.ready();g.bar(2,receipt='2026-01-05T15:02:59.9Z');g.worker.lease();g.fake.at='2026-01-05T15:03:00.000000000Z';g.tick()
    require(g.fake.posts==0 and g.decisions()[0][2]['reason']=='receipt_time_inconsistent','worker_early_bar_receipt_blocks_new_buy');g.worker.finish()
    g=WorkerFixture(repository,'simultaneous',8);g.news();g.news(1,'Microsoft shares fell sharply.');g.ready();g.tick()
    require(g.fake.posts==0 and all(r[2]['reason']=='conflicting_simultaneous_signals' for r in g.decisions()),'worker_simultaneous_opposite_news_withheld');g.worker.finish()
    g=WorkerFixture(repository,'superseded',9);g.news();g.news(1,'Microsoft publishes an update.',at='2026-01-05T15:00:02Z');g.ready();g.tick()
    require(g.fake.posts==0 and 'superseded_by_newer_feature' in [r[2]['reason'] for r in g.decisions()],'worker_newer_feature_supersedes_pending_news');g.worker.finish()
    g=WorkerFixture(repository,'timeout',10);g.news();g.ready();g.fake.mode='timeout_after_accept'
    require(rejects(g.tick) and g.fake.posts==1,'worker_uncertain_post_stops_new_execution')
    g.fake.mode=None;g.tick();require(g.fake.posts==1,'worker_uncertain_post_recovered_by_lookup_only');g.worker.finish()
    g=WorkerFixture(repository,'lease',11)
    rival=PaperWorker(repository,g.fake.client,('MSFT',),'iex',g.key,'rules',_fixture=True)
    require(rejects(lambda:rival.lease(acquire=True)),'worker_account_single_owner_lease')
    g.fake.at=utc_add(g.fake.at,121);rival.lease(acquire=True)
    require(rejects(g.worker.lease),'worker_expired_owner_fenced_after_takeover');rival.finish()
    g=WorkerFixture(repository,'manualguard',12);broker.prepare_buy(repository,g.fake.client,'r23-test-manual','MSFT')
    require(rejects(lambda:broker.submit(repository,g.fake.client,'r23-test-manual')) and g.fake.posts==0,'worker_active_lease_blocks_manual_submit_bypass');g.worker.finish()
    g=WorkerFixture(repository,'sourcechange',13);g.news();g.ready()
    rec=current_records(repository,('MSFT',),g.worker.started,g.key,'rules')[0];key,_=g.worker.create(rec,'MSFT')
    repository.connection.execute(f'UPDATE news_analyses SET result_json={repository.marker} WHERE article_id={repository.marker}',('{}',g.base));repository.connection.commit()
    require(rejects(lambda:g.worker.transaction(lambda:g.worker.gate(repository,key,g.worker.price_health(g.worker.read_prices(repository))))) and g.fake.posts==0,'worker_changed_source_rejected_before_post')
    # Restore source so cleanup validates the sealed claim independently.
    repository.connection.execute(f'UPDATE news_analyses SET result_json={repository.marker} WHERE article_id={repository.marker}',(encode(rec['analysis']['result']),g.base));repository.connection.commit();g.worker.finish()
    g=WorkerFixture(repository,'equity',14);capture_equity(repository,g.fake.client,sample_id='r23-equity-initial')
    require(performance_report(repository,g.worker.account_ref)['status']=='insufficient_observations','equity_single_sample_not_period_profit')
    g.fake.at=utc_add(g.fake.at,86400);g.fake.equity='9900';capture_equity(repository,g.fake.client,sample_id='r23-equity-down')
    g.fake.at=utc_add(g.fake.at,86400);g.fake.equity='10100';capture_equity(repository,g.fake.client,sample_id='r23-equity-up')
    p=performance_report(repository,g.worker.account_ref,period='week')
    require(p['equity_change']=='100.00000000' and p['max_observed_drawdown_percent']=='1.00000000','equity_change_and_observed_drawdown_decimal_exact')
    require(p['net_profit'] is None and not p['cashflows_verified'] and not p['strategy_performance_validated'],'equity_change_not_claimed_cashflow_adjusted_strategy_profit')
    p=performance_report(repository,g.worker.account_ref,start='2026-01-06T15:00:00Z',end='2026-01-07T15:00:00Z')
    require(p['samples']==2 and p['initial_equity']=='9900','equity_report_inclusive_window_no_future_samples')
    before=performance_report(repository,g.worker.account_ref)['account_history_sha256'];capture_equity(repository,g.fake.client,sample_id='r23-equity-up')
    require(before==performance_report(repository,g.worker.account_ref)['account_history_sha256'],'equity_sample_retry_idempotent')
    require(rejects(lambda:performance_report(repository)),'equity_multiple_accounts_require_explicit_selection')
    g.worker.lease(acquire=True);g.worker.finish()
    return checks


def verify_paper_session():
    repo=Repository('sqlite:///:memory:')
    try:return {'status':'passed','checks':worker_checks(repo),'transport':'original_controlled_fake_paper_service',
        'network_required':False,'model_required':False,'user_database_modified':False,
        'actual_alpaca_connection_tested':False,'orders_sent_to_alpaca':0,'profitability_validated':False}
    finally:repo.close()


def demo_paper_session(output):
    folder=Path(output)
    if folder.exists():raise DataValidationError('Demo output already exists; choose a new folder')
    folder.mkdir(parents=True);repo=Repository('sqlite:///'+str((folder/'scenario.sqlite3').resolve()))
    try:
        result={'status':'completed','checks':worker_checks(repo),'scope':'original_fake_news_and_paper_not_real_performance',
                'orders_sent_to_alpaca':0,'user_database_modified':False,'output':str(folder)}
        (folder/'summary.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8');return result
    finally:repo.close()
