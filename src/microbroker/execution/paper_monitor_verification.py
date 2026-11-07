"""Private controlled news visibility and durable session-report acceptance."""
from datetime import datetime,timezone
from copy import deepcopy
import json
import uuid
from .paper_monitor import save_run,report_session
from .paper_worker_verification import WorkerFixture
from .equity_history import capture_equity
from . import paper_broker as broker
from ..data.storage.repository import Repository,StorageError
from ..data.research import encode
from ..data.models import DataValidationError
from ..data.news import normalize_news
from ..data.news_probe import inspect_news
from ..config import Settings


def monitor_checks(repository):
    checks=[]
    def require(ok,name):
        if not ok:raise StorageError('Paper monitor verification failed: '+name)
        checks.append(name)
    def reject(call):
        try:call()
        except (StorageError,DataValidationError):return True
        return False
    f=WorkerFixture(repository,'r24-monitor-buy',30);f.news();f.ready();tick=f.tick()
    research={'session_id':str(uuid.uuid4()),'news_polls':1,'received_articles':1,'new_versions':1,'inserted_analyses':1}
    value={'status':'running','research_session':research,'paper':f.worker.summary(),'observation':tick}
    save_run(repository,f.worker,value);report=report_session(repository,f.worker.session_id)
    require(report['paper']['post_attempts']==1 and report['samples'][0]['company_label']=='positive' and report['samples'][0]['order']['status']=='new',
            'monitor_headline_company_decision_and_unfilled_order_separated')
    require(any(u['text']=='Microsoft profits rose sharply.' for u in report['samples'][0]['context_preview']) and report['samples'][0]['context_preview_verified'],
            'monitor_context_preview_recomputed_from_verified_news')
    before=f.fake.posts;f.fill();broker.reconcile_account(repository,f.fake.client)
    later=report_session(repository,f.worker.session_id)
    require(later['assessment']=='paper_fills_recorded' and later['samples'][0]['order']['status']=='filled' and f.fake.posts==before,
            'monitor_latest_local_fill_after_get_reconciliation_no_submit')
    require(report_session(repository,f.worker.session_id)==later and not later['network_required'] and later['orders_submitted_by_this_call']==0,
            'monitor_repeat_offline_report_stable')
    require(later['last_saved_observation_at']==report['last_saved_observation_at'] and later['broker_observation_semantics']=='latest_locally_recorded_order_state_not_fresh_broker_query',
            'monitor_saved_observation_and_later_broker_cache_times_separated')
    m=repository.marker;raw=repository.connection.execute(f'SELECT result_json FROM paper_worker_runs WHERE id={m}',(f.worker.session_id,)).fetchone()[0]
    repository.connection.commit();repository.connection.execute(f'UPDATE paper_worker_runs SET result_json={m} WHERE id={m}',('{}',f.worker.session_id));repository.connection.commit()
    require(reject(lambda:report_session(repository,f.worker.session_id)),'monitor_session_audit_tamper_rejected')
    repository.connection.execute(f'UPDATE paper_worker_runs SET result_json={m} WHERE id={m}',(raw,f.worker.session_id));repository.connection.commit();f.worker.finish()
    g=WorkerFixture(repository,'r24-monitor-empty',31);capture_equity(repository,g.fake.client,session_id=g.worker.session_id)
    empty={'status':'no_data','research_session':{'session_id':str(uuid.uuid4()),'news_polls':18,'received_articles':0,'new_versions':0,'inserted_analyses':0},'paper':g.worker.summary()}
    save_run(repository,g.worker,empty);none=report_session(repository,g.worker.session_id)
    require(none['assessment']=='no_news_returned' and none['paper']['post_attempts']==0 and none['equity_observations_in_session']==1,
            'monitor_empty_news_explains_zero_orders_and_equity_samples')
    # Existing R23 zero-decision sessions are identified by their sealed equity account link.
    rid=str(uuid.uuid4());legacy={**empty['research_session'],'status':'no_data','started_at':g.worker.started,'finished_at':g.fake.at,
        'paper_worker_session_id':g.worker.session_id}
    repository.connection.execute(f'INSERT INTO research_sessions VALUES ({",".join([m]*6)})',
        (rid,g.worker.started,g.fake.at,'no_data',encode({'symbols':['MSFT'],'feed':'iex','analyzer_key':g.key,'engine':'rules'}),encode({**legacy,'session_id':rid})))
    repository.connection.commit()
    repository.connection.execute(f'DELETE FROM paper_worker_runs WHERE id={m}',(g.worker.session_id,));repository.connection.commit()
    previous=report_session(repository,rid)
    require(previous['assessment']=='no_news_returned' and not previous['audit_integrity_verified'] and previous['audit_semantics']=='legacy_research_audit_not_sealed',
            'monitor_r23_legacy_no_news_report_keeps_unsealed_provenance')
    require(reject(lambda:report_session(repository,str(uuid.uuid4()))),'monitor_unknown_session_rejected');g.worker.finish()
    class News:
        pages=1
        def __init__(self,s):pass
        def iter_news(self,request):
            article=normalize_news({'id':99924999,'headline':'Microsoft publishes an update.','symbols':['MSFT'],
                'created_at':'2026-01-05T15:00:00Z','updated_at':'2026-01-05T15:00:00Z'})
            yield article;yield article
    probe=inspect_news(Settings(api_key='FAKE',api_secret='FAKE'),('MSFT',),60,adapter_factory=News,now=datetime(2026,1,5,16,tzinfo=timezone.utc))
    require(probe['received_articles']==2 and probe['unique_article_versions']==1 and probe['symbol_counts']=={'MSFT':1},
            'news_probe_duplicate_versions_disclosed')
    require(not probe['database_modified'] and not probe['arrival_times_recorded'] and probe['orders_sent']==0 and not probe['inference_performed'],
            'news_probe_does_not_seed_tradable_arrivals_or_submit')
    return checks


def verify_paper_monitor():
    repository=Repository('sqlite:///:memory:')
    try:return {'status':'passed','checks':monitor_checks(repository),'network_required':False,'model_required':False,
        'user_database_modified':False,'orders_sent_to_alpaca':0,'transport':'original_controlled_fake_news_and_paper',
        'actual_alpaca_connection_tested':False,'profitability_validated':False}
    finally:repository.close()


def demo_paper_monitor(output):
    """Readable buy/fill/exit/neutral/conflict scenario in an isolated fake-service DB."""
    from pathlib import Path
    from .paper_monitor import render_report
    folder=Path(output)
    if folder.exists():raise DataValidationError('Demo output already exists; choose a new folder')
    folder.mkdir(parents=True);repository=Repository('sqlite:///'+str((folder/'scenario.sqlite3').resolve()))
    try:
        f=WorkerFixture(repository,'r24-readable-demo',40);scope='original_synthetic_news_and_fake_paper_service'
        capture_equity(repository,f.fake.client,session_id=f.worker.session_id)
        research={'session_id':str(uuid.uuid4()),'news_polls':0,'received_articles':0,'new_versions':0,'inserted_analyses':0}
        observation={}
        def save(status='running'):
            save_run(repository,f.worker,{'status':status,'fixture_scope':scope,'research_session':research,
                'observation':observation,'paper':f.worker.summary()})
        def cycle(count):
            nonlocal observation
            research.update(news_polls=count,received_articles=count,new_versions=count,inserted_analyses=count)
            observation=f.tick();save()
        save();f.news();f.ready();cycle(1);f.fill();broker.reconcile_account(repository,f.fake.client)
        f.news(1,'Microsoft shares fell sharply.',at='2026-01-05T15:02:20Z')
        f.fake.at='2026-01-05T15:03:00.000000000Z';f.worker.lease();f.bar(3)
        f.fake.at='2026-01-05T15:04:10.000000000Z';cycle(2);f.fill();broker.reconcile_account(repository,f.fake.client)
        f.news(2,'Microsoft publishes an update.',at='2026-01-05T15:04:20Z');f.fake.at='2026-01-05T15:04:30.000000000Z';cycle(3)
        f.news(3,'Microsoft profits rose. Microsoft shares fell.',at='2026-01-05T15:04:40Z');f.fake.at='2026-01-05T15:04:50.000000000Z';cycle(4)
        f.worker.finish();capture_equity(repository,f.fake.client,session_id=f.worker.session_id);save('completed')
        report=report_session(repository,f.worker.session_id)
        if report['paper']['broker_order_status_counts']!={'filled':2} or report['paper']['post_attempts']!=2 or report['paper']['status_counts']!={'submitted':2,'wait':2}:
            raise StorageError('Controlled readable demo outcome mismatch')
        (folder/'report.json').write_text(json.dumps(report,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
        (folder/'report.txt').write_text(render_report(report)+'\n',encoding='utf-8')
        result={'status':'completed','output':str(folder),'session_id':f.worker.session_id,'decisions':4,
            'fake_filled_orders':2,'wait_decisions':2,'scope':scope,'orders_sent_to_alpaca':0,'user_database_modified':False,'profitability_validated':False}
        (folder/'summary.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8');return result
    finally:repository.close()
