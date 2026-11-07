"""Private R29 regressions: reporting must not create decisions, orders or past timings."""
from copy import deepcopy
import uuid
from .paper_worker_verification import WorkerFixture
from .portfolio_sampling import PortfolioSampler
from .session_timing import Timings
from .paper_monitor import save_run,report_session,render_report
from .latency_report import latency_report,render_latency
from .report_evidence import coverage_view,price_warning_history,timing_view
from .report_price_health import price_health
from ..data.news import normalize_news
from ..data.research import encode
from ..data.storage.repository import Repository,StorageError
from ..intelligence.analysis import compute_analysis
from ..intelligence.policy import digest


def revise(f,text='Microsoft profits rose. Microsoft shares fell.',at='2026-01-05T15:00:02Z'):
    """A genuine later article revision and analysis, leaving the prior claim sealed."""
    from .paper_worker import current_records
    old=current_records(f.repo,('MSFT',),f.worker.started,f.key,'rules')[0]['news']
    raw=old['data'];n=normalize_news({'id':raw['id'],'headline':raw['headline'],'content':text,
        'created_at':raw['created_at'],'updated_at':at,'symbols':['MSFT'],'source':raw['source'],'url':raw['url']})
    data=n['data'];m=f.repo.marker
    f.repo.connection.execute(f'INSERT INTO news_versions VALUES ({",".join([m]*7)})',
        ('alpaca',data['id'],n['revision_hash'],data['created_at'],data['updated_at'],at,encode(data)))
    for symbol,method in n['mappings'].items():
        f.repo.connection.execute(f'INSERT INTO news_symbols VALUES ({",".join([m]*5)})',('alpaca',data['id'],n['revision_hash'],symbol,method))
    f.repo.connection.execute(f'UPDATE news_articles SET revision_hash={m},updated_at={m},last_seen_at={m},fingerprint={m} WHERE provider={m} AND article_id={m}',
        (n['revision_hash'],data['updated_at'],at,n['fingerprint'],'alpaca',data['id']))
    result=compute_analysis({'data':data,'first_seen':at,'mappings':n['mappings']},f.engine)
    f.repo.connection.execute(f'INSERT INTO news_analyses VALUES ({",".join([m]*9)})',
        ('alpaca',data['id'],n['revision_hash'],f.key,'rules',result['input_sha256'],at,encode(result),digest(result)))
    f.repo.connection.commit();return n


def recovered_cycles():
    """Minimal price-health fields from the user's nine-cycle R28 acceptance output."""
    symbols=['AAPL','MSFT','NVDA','TSLA']
    clocks=[[],[],symbols,['MSFT','NVDA','TSLA'],symbols,[],['MSFT'],[],[]]
    return {'stream':{'events_received':40,'bars_written':40,'bar_receipts_recorded':40},
        'snapshot_minute_bars':40,'price_warmup':{'status':'ready'},
        'cycles':[{'paper':{'observed_at':f'2026-10-16T13:{42+i:02}:00Z','price_clock_flags':c,
            'price_gap_symbols':['MSFT','NVDA','TSLA'] if i==4 else []}} for i,c in enumerate(clocks)]}


def evidence_checks(repository):
    checks=[]
    def require(ok,name):
        if not ok:raise StorageError('R29 report evidence verification failed: '+name)
        checks.append(name)
    f=WorkerFixture(repository,'r29-measurement',100);timer=Timings();clock=[0.0];timer.clock=lambda:clock[0]
    sampler=PortfolioSampler(repository,f.fake.client,f.worker.account_ref,f.worker.session_id,f.worker.started,_timing=timer)
    original=f.fake.client.valued_positions
    def slow():clock[0]+=2;return original()
    f.fake.client.valued_positions=slow
    sampler.capture('start')
    f.worker.finish()  # Sampling is GET-only and does not need an order-writer lease.
    for _ in range(8):sampler.capture()
    f.fake.at='2026-01-05T15:05:01.000000000Z';sampler.capture();sampler.capture('final')
    stages=timer.summary()['stages']
    require(sampler.summary()['new_captures_this_process']==3 and stages['portfolio_capture']['samples']==3 and
        stages['portfolio_capture']['median_seconds']==2 and stages['portfolio_slot_check']['samples']==11,
        'capture_three_successes_not_eleven_slot_checks')
    require(stages['portfolio_capture_attempt']['samples']==3,'capture_attempts_exclude_empty_slots')
    before=timer.summary();sampler.capture('start');sampler.capture('final')
    require(timer.summary()['stages']['portfolio_capture']==before['stages']['portfolio_capture'],
        'capture_repeated_slot_never_fakes_new_duration')
    g=WorkerFixture(repository,'r29-revision',101);old=g.news();g.fake.advance();g.tick()
    before_claims=g.decisions();new=revise(g);g.fake.at='2026-01-05T15:00:03.000000000Z';g.tick()
    coverage=g.worker.coverage(g.fake.at)
    require(coverage['claims_for_prior_revision']==1 and coverage['unassessed_company_rows']==1 and
        coverage['unclaimed_company_rows']==0,'coverage_prior_revision_not_current_assessment')
    sample=coverage['samples'][0]
    require(sample['revision_hash']==new['revision_hash'] and sample['claimed_revision_hash']==old['revision_hash'] and
        sample['current_company_label'] is None and sample['current_research_decision_status']=='conflicting_evidence',
        'coverage_current_withheld_separated_from_prior_positive')
    require(len(g.decisions())==1 and g.decisions()[0][1]==before_claims[0][1] and g.fake.posts==0,
        'coverage_preserves_article_claim_no_resubmission')
    research=recovered_cycles();latest=research['cycles'][-1]['paper'];history=price_warning_history(research)
    require(history['cycles_with_clock_flags']==4 and history['cycles_with_gaps']==1 and history['cycles_with_any_warning']==4,
        'history_counts_all_nine_cycles_not_last_only')
    health=price_health(research,latest)
    require(health['status']=='recovered_with_recorded_warnings' and health['current_status']=='recorded_minute_data_present' and
        health['acceptance_incomplete_from_recorded_price_evidence'],'history_recovered_last_cycle_keeps_session_warning')
    save_run(repository,g.worker,{'status':'completed','paper':g.worker.summary(),'research_session':research,
        'last_processed_observation':latest,'decision_coverage':coverage,'timing':timer.summary()})
    before=len(g.fake.calls);r=report_session(repository,g.worker.session_id);text=render_report(r)
    require(r['assessment']=='price_coverage_incomplete' and 'zaman 4 döngü' in text and 'güncel şirket kararı: karar yok' in text,
        'report_shows_history_and_current_revision_together')
    raw=repository.connection.execute(f'SELECT result_json,result_sha256 FROM paper_worker_runs WHERE id={repository.marker}',(g.worker.session_id,)).fetchone();repository.connection.commit()
    again=report_session(repository,g.worker.session_id);latency_report(repository,g.worker.session_id)
    unchanged=repository.connection.execute(f'SELECT result_json,result_sha256 FROM paper_worker_runs WHERE id={repository.marker}',(g.worker.session_id,)).fetchone();repository.connection.commit()
    require(r==again and raw==unchanged and len(g.fake.calls)==before and g.fake.posts==0,'report_repeat_readonly_no_api_or_audit_rewrite')
    legacy={'format':'R28','available_analyses':2,'company_rows':2,'unassessed_analyses':0,
        'unassessed_company_rows':0,'claims_for_prior_revision':1,'samples':[]}
    view=coverage_view(legacy)
    require(view['current_revision_rows_not_assessed_at_least']==1 and view['current_revision_rows_not_assessed'] is None and
        view['claims_for_different_analysis'] is None,'legacy_coverage_lower_bound_not_fabricated_exact_count')
    oldtimer={'format':'R28','stages':{'portfolio_capture':{'samples':11,'median_seconds':0}}};copy=deepcopy(oldtimer)
    value=timing_view(oldtimer)
    require(oldtimer==copy and 'portfolio_capture' not in value['stages'] and not value['successful_capture_duration_reconstructed'],
        'legacy_mixed_capture_named_without_reconstruction')
    require(price_warning_history({})['warning_recorded'] is None,'legacy_unknown_history_not_zero_warnings')
    fallback=price_health({},latest,history)
    require(fallback['warning_history']==history and fallback['acceptance_incomplete_from_recorded_price_evidence'],
        'sealed_warning_summary_survives_missing_final_research_cycles')
    repository.connection.execute(f'UPDATE paper_worker_runs SET result_json={repository.marker} WHERE id={repository.marker}',('{}',g.worker.session_id));repository.connection.commit()
    try:report_session(repository,g.worker.session_id)
    except StorageError:rejected=True
    else:rejected=False
    require(rejected,'report_tampered_observation_rejected')
    # Restore the valid sealed record for the remainder of a shared private verifier.
    repository.connection.execute(f'UPDATE paper_worker_runs SET result_json={repository.marker} WHERE id={repository.marker}',(raw[0],g.worker.session_id));repository.connection.commit()
    g.worker.finish()
    return checks


def verify_evidence():
    repository=Repository('sqlite:///:memory:')
    try:checks=evidence_checks(repository)
    finally:repository.close()
    return {'status':'passed','checks':checks,'fixture_source':'synthetic_prices_durations_revisions_and_supplied_r28_cycle_pattern',
        'network_required':False,'model_required':False,'user_database_modified':False,'orders_sent_to_alpaca':0,
        'real_postgresql_validated':False,'profitability_validated':False}
