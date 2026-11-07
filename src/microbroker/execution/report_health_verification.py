"""Controlled saved-audit regressions for missing price data; no external API."""
from copy import deepcopy
from .paper_monitor import save_run,report_session,render_report,assessment
from .report_price_health import price_health
from .paper_worker_verification import WorkerFixture
from . import paper_broker as broker
from ..data.storage.repository import Repository,StorageError


def no_price_research():
    return {'status':'no_data','news_polls':6,'received_articles':7,'new_versions':2,'inserted_analyses':2,
        'price_scope':'same_stream_run_receipts','snapshot_minute_bars':0,
        'price_warmup':{'status':'timed_out','ready_symbols':[]},
        'stream':{'status':'stopped','authenticated':True,'subscribed':True,'events_received':0,
            'bars_written':0,'bar_receipts_recorded':0,'received_by_type':{},'error_code':None}}


def health_checks(repository):
    checks=[]
    def require(ok,name):
        if not ok:raise StorageError('Paper price report verification failed: '+name)
        checks.append(name)
    f=WorkerFixture(repository,'r27-no-price',70);f.news(text='Microsoft profits rose. Microsoft shares fell.')
    f.fake.advance();tick=f.tick()
    # This saved report fixture concerns this analyzer's completed article only.
    # The shared PG verifier also contains unrelated articles with other analyzers.
    tick.update(price_gap_symbols=['MSFT'],analyses_pending=0)
    sampling={'recorded_snapshots':3,'errors':[],'last_snapshot_id':'controlled-final','sampling_degraded':False}
    value={'status':'no_data','research_session':no_price_research(),'paper':f.worker.summary(),'observation':tick,'portfolio_sampling':sampling}
    save_run(repository,f.worker,value);r=report_session(repository,f.worker.session_id);text=render_report(r)
    require(r['assessment']=='price_data_unavailable' and 'Fiyat verisi alınamadı' in text,'health_missing_prices_override_wait_headline')
    require(r['decision_assessment']=='decisions_waited_without_order' and r['samples'][0]['decision_reason']=='company_decision_withheld',
        'health_company_wait_reason_preserved_separately')
    require(r['price_health']['stream_authenticated'] and r['price_health']['stream_subscribed'] and not r['price_health']['acceptance_passed'],
        'health_authentication_not_price_acceptance')
    before=repository.connection.total_changes if not repository.postgres else None
    repeated=report_session(repository,f.worker.session_id)
    require(repeated==r and f.fake.posts==0 and (before is None or before==repository.connection.total_changes),
        'health_offline_repeat_no_orders_or_database_changes')
    require(r['portfolio_sampling']==sampling and 'Portföy gözlemi: 3' in text,'health_sampling_success_independent_from_price_failure')
    require(not r['price_health']['provider_incident_checked'] and not r['price_health']['outage_cause_determined'] and
        not r['price_health']['broker_price_freshness_validated'],'health_no_provider_cause_or_freshness_invented')
    paper=value['paper'];missing=no_price_research();failed=deepcopy(missing);failed['stream'].update(status='failed',error_code='connection_closed')
    require(assessment('no_data',failed,paper,{})=='price_stream_failed','health_stream_failure_not_normal_wait')
    duplicate={'stream':{'bars_written':0,'bar_receipts_recorded':2,'events_received':2},'snapshot_minute_bars':2}
    require(price_health(duplicate,{})['status']=='recorded_minute_data_present','health_duplicate_bar_receipts_not_absent_prices')
    require(price_health({}, {})['status']=='not_recorded' and assessment('no_data',{'received_articles':0},paper,{})=='no_news_returned',
        'health_legacy_unknown_not_zero_prices')
    present={'stream':{'bars_written':2,'events_received':2},'snapshot_minute_bars':2}
    require(assessment('completed',present,paper,{'price_gap_symbols':['MSFT']})=='price_coverage_incomplete',
        'health_recorded_gap_not_hidden_by_prior_bars')
    require(assessment('completed',present,paper,{'price_clock_flags':['MSFT']})=='price_coverage_incomplete',
        'health_clock_issue_not_hidden_by_prior_bars')
    present['price_warmup']={'status':'timed_out'}
    require(not price_health(present,{'fresh_price_symbols':['MSFT']})['acceptance_incomplete_from_recorded_price_evidence'],
        'health_later_recorded_bars_not_permanently_failed_by_initial_warmup')
    f.worker.finish();g=WorkerFixture(repository,'r27-prior-fill',71);g.news();g.ready();g.tick();g.fill();broker.reconcile_account(repository,g.fake.client)
    save_run(repository,g.worker,{'status':'no_data','research_session':missing,'paper':g.worker.summary(),'observation':{}})
    filled=report_session(repository,g.worker.session_id)
    require(filled['assessment']=='price_data_unavailable' and filled['decision_assessment']=='paper_fills_recorded' and
        filled['samples'][0]['order']['status']=='filled' and filled['paper']['post_attempts']==1,'health_fill_preserved_when_price_report_incomplete')
    g.worker.finish();m=repository.marker
    raw=repository.connection.execute(f'SELECT result_json FROM paper_worker_runs WHERE id={m}',(f.worker.session_id,)).fetchone()[0];repository.connection.commit()
    repository.connection.execute(f'UPDATE paper_worker_runs SET result_json={m} WHERE id={m}',('{}',f.worker.session_id));repository.connection.commit()
    rejected=False
    try:report_session(repository,f.worker.session_id)
    except StorageError:rejected=True
    require(rejected,'health_old_session_tamper_still_rejected')
    repository.connection.execute(f'UPDATE paper_worker_runs SET result_json={m} WHERE id={m}',(raw,f.worker.session_id));repository.connection.commit()
    return checks


def verify_health():
    repository=Repository('sqlite:///:memory:')
    try:return {'status':'passed','checks':health_checks(repository),'network_required':False,'model_required':False,
        'user_database_modified':False,'orders_sent_to_alpaca':0,'fixture_source':'original_controlled_saved_price_audits',
        'actual_alpaca_connection_tested':False,'provider_incident_checked':False,'profitability_validated':False}
    finally:repository.close()
