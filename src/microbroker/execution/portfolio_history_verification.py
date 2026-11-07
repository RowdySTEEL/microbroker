"""Original controlled observations; sampled changes are not net strategy returns."""
from pathlib import Path
import json
import uuid
from .portfolio_verification import seeded
from .portfolio_sampling import PortfolioSampler
from .portfolio_history import history_report,render_history
from .paper_portfolio import capture_portfolio,read_history
from .paper_worker import utc_add
from . import paper_broker as broker
from ..data.research import begin_read
from ..data.models import DataValidationError
from ..data.storage.repository import Repository,StorageError


def history_checks(repository):
    checks=[]
    def require(ok,name):
        if not ok:raise StorageError('Portfolio history verification failed: '+name)
        checks.append(name)
    f,entry=seeded(repository,'r26-main');ref=f.client.account()['account_ref'];sid=str(uuid.uuid5(uuid.NAMESPACE_URL,'microbroker-r26-main'))
    s=PortfolioSampler(repository,f.client,ref,sid,f.at);before=f.posts;s.capture('start')
    require(s.summary()['recorded_snapshots']==1 and f.posts==before,'history_automatic_start_get_only')
    require(s.capture() is None and s.summary()['recorded_snapshots']==1,'history_interval_not_sampled_early')
    f.at=utc_add(f.at,300);f.mark='120';s.capture()
    require(s.summary()['recorded_snapshots']==2,'history_periodic_snapshot_recorded')
    s.capture();require(s.summary()['recorded_snapshots']==2,'history_same_interval_not_duplicated')
    restored=PortfolioSampler(repository,f.client,ref,sid,s.started_at);restored.capture('start');restored.capture()
    require(restored.summary()['recorded_snapshots']==2,'history_sampler_recreated_no_duplicate_slots')
    f.at=utc_add(f.at,300);f.mark='125';s.capture('final')
    require(s.summary()['recorded_snapshots']==3 and f.posts==before,'history_final_snapshot_no_liquidation')
    r=history_report(repository,ref,period='day',session_id=sid)
    require(r['gross_total_change']=='1.5' and r['gross_realized_change']=='0' and r['gross_unrealized_change']=='1.5','history_gross_delta_not_sum_of_lifetime_pnl')
    require(r['net_profit'] is None and not r['period_pnl_calculated'] and not r['fees_reconciled'],'history_period_net_and_execution_date_not_claimed')
    require(not r['history_reaches_requested_start'] and 'history_does_not_reach_requested_start' in r['warning_codes'],'history_short_sample_not_full_day_or_week')
    limited=history_report(repository,ref,session_id=sid,limit=1)
    require(limited['samples']==3 and len(limited['points'])==1 and limited['gross_total_change']=='1.5','history_display_limit_does_not_truncate_calculation')
    earlier=history_report(repository,ref,end=s.started_at,session_id=sid)
    require(earlier['samples']==1 and earlier['gross_total_change'] is None,'history_future_samples_excluded')
    exact=history_report(repository,ref,start=s.started_at,end=f.at,session_id=sid)
    require(exact['exact_requested_boundaries_observed'] and exact['samples']==3,'history_inclusive_exact_observation_boundaries')
    f.at=utc_add(f.at,1800);capture_portfolio(repository,f.client,snapshot_id='r26-manual-later')
    scoped=history_report(repository,ref,session_id=sid);unscoped=history_report(repository,ref)
    require(scoped['samples']==3 and unscoped['samples']==4,'history_worker_session_scope_excludes_manual_and_other_sessions')
    require(unscoped['gap_count']==1 and 'wall_clock_sampling_gaps_over_10_minutes' in unscoped['warning_codes'],'history_wall_clock_gaps_disclosed_not_backfilled')
    f.missing_mark=True;f.at=utc_add(f.at,300);capture_portfolio(repository,f.client,snapshot_id='r26-incomplete')
    bad=history_report(repository,ref)
    require(bad['status']=='attention_required' and bad['gross_total_change'] is None,'history_incomplete_observation_withholds_period_total')
    repeated=history_report(repository,ref,session_id=sid)
    require(repeated['selected_dataset_sha256']==scoped['selected_dataset_sha256'] and repeated['points']==scoped['points']
            and repeated['gross_total_change']==scoped['gross_total_change'],'history_repeat_scoped_report_stable_after_new_samples')
    m=repository.marker;raw=repository.connection.execute(f'SELECT snapshot_json FROM paper_portfolio_snapshots WHERE id={m}',('r26-incomplete',)).fetchone()[0];repository.connection.commit()
    repository.connection.execute(f'UPDATE paper_portfolio_snapshots SET snapshot_json={m} WHERE id={m}',('{}','r26-incomplete'));repository.connection.commit()
    rejected=False
    try:history_report(repository,ref)
    except StorageError:rejected=True
    require(rejected,'history_snapshot_chain_tamper_rejected')
    repository.connection.execute(f'UPDATE paper_portfolio_snapshots SET snapshot_json={m} WHERE id={m}',(raw,'r26-incomplete'));repository.connection.commit()
    return checks


def verify_history():
    repo=Repository('sqlite:///:memory:')
    try:return {'status':'passed','checks':history_checks(repo),'user_database_modified':False,'network_required':False,
        'model_required':False,'orders_sent_to_alpaca':0,'actual_alpaca_connection_tested':False,
        'fixture_source':'original_controlled_cumulative_fills_and_sampling_times','profitability_validated':False}
    finally:repo.close()


def demo_history(output):
    folder=Path(output)
    if folder.exists():raise DataValidationError('Use a new demo output directory')
    folder.mkdir(parents=True);repo=Repository('sqlite:///'+str((folder/'scenario.sqlite3').resolve()))
    try:
        f,entry=seeded(repo,'r26-readable-demo');ref=f.client.account()['account_ref'];sid=str(uuid.uuid5(uuid.NAMESPACE_URL,'r26-readable-demo'))
        s=PortfolioSampler(repo,f.client,ref,sid,f.at);s.capture('start')
        f.at=utc_add(f.at,300);f.mark='120';s.capture()
        plan=broker.prepare_exit(repo,f.client,'r26-demo-exit',entry);broker.submit(repo,f.client,'r26-demo-exit');f.update(plan['request']['client_order_id'],'filled','0.1','120')
        f.at=utc_add(f.at,300);s.capture('final');report=history_report(repo,ref,session_id=sid)
        (folder/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
        (folder/'report.txt').write_text("SENTETİK DEMO — Alpaca'ya emir gönderilmedi\n"+render_history(report)+'\n')
        result={'status':'completed','session_id':sid,'snapshots':s.summary()['recorded_snapshots'],
            'gross_realized_change':report['gross_realized_change'],'gross_unrealized_change':report['gross_unrealized_change'],
            'gross_total_change':report['gross_total_change'],'orders_sent_to_alpaca':0,'fake_broker_posts':f.posts,
            'profitability_validated':False,'output':str(folder)}
        (folder/'summary.json').write_text(json.dumps(result,indent=2)+'\n');return result
    finally:repo.close()
