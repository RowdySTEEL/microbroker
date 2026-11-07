"""Controlled receipt tests in a private database; no API, model or user data."""
from unittest.mock import patch
from datetime import datetime
from .stream_models import StreamRequest, normalize_event
from .models import utc_datetime
from .storage.repository import Repository, StorageError
from .session_prices import create_session_prices, align_session_records
from .alignment import alignment_fixture, research_alignment, read_bars_snapshot
from .analysis_replay import replay_analysis
from .research import encode

def empty_stats():
    return {'events_received':0,'events_inserted':0,'duplicates':0,'bars_written':0,
        'received_by_type':{},'received_by_symbol':{}}

def apply_at(repository,run,event,stats,at):
    with patch('microbroker.data.storage.repository.datetime',wraps=datetime) as clock:
        clock.now.return_value=utc_datetime(at)
        return repository.apply_stream_frame(run,[event],stats)

def price_fixture(repository):
    analysis_id,_=alignment_fixture(repository)
    request=StreamRequest(('MSFT',),channels=('bars','updatedBars'))
    base=normalize_event({'T':'b','S':'MSFT','t':'2026-01-07T10:00:00Z',
        'o':100,'h':103,'l':99,'c':101,'v':100,'n':5},request)
    update=normalize_event({'T':'u','S':'MSFT','t':'2026-01-07T10:00:00Z',
        'o':100,'h':103,'l':99,'c':102,'v':101,'n':6},request)
    prior=repository.capture_bar_receipts
    try:
        repository.capture_bar_receipts=True
        foreign=repository.begin_stream(request,empty_stats())
        apply_at(repository,foreign,base,empty_stats(),'2026-01-07T10:00:59Z')
        run=repository.begin_stream(request,empty_stats())
        stats=apply_at(repository,run,base,empty_stats(),'2026-01-07T10:01:00Z')
        stats=apply_at(repository,run,update,stats,'2026-01-07T10:01:01Z')
        stats=apply_at(repository,run,base,stats,'2026-01-07T10:01:02Z')
        snap=create_session_prices(repository,run,('MSFT',),'iex','2026-01-07T10:00:00Z','2026-01-07T10:02:00Z')
        return analysis_id,snap,run,stats,base
    finally:repository.capture_bar_receipts=prior

def verify_session_prices():
    repo=Repository('sqlite:///:memory:');checks=[]
    def require(ok,name):
        if not ok:raise StorageError('Session price verification failed: '+name)
        checks.append(name)
    try:
        analysis_id,snap,run,stats,base=price_fixture(repo)
        bars,meta,_=read_bars_snapshot(repo,snap['replay_id'])
        report=research_alignment(repo,analysis_id,snap['replay_id'])
        require(len(bars)==3 and all(b['receipt']['run_id']==run for b in bars),'foreign_stream_receipts_excluded')
        require(stats['duplicates']==2 and len(bars)==3,'duplicate_global_event_keeps_session_receipt')
        require(snap['unique_minute_bars']==1,'correction_receipts_not_extra_minutes')
        require(report['matched_rows']==1 and report['samples'][0]['price']['close']=='101','future_correction_excluded')
        require(report['samples'][0]['price']['provenance']['received_at']=='2026-01-07T10:01:00.000000000Z','inclusive_receipt_boundary')
        selected,_,_=replay_analysis(repo,analysis_id,_include_records=True)
        selected[0]['analysis']['timestamp']='2026-01-07T10:01:03.000000000Z'
        require(align_session_records(selected,bars,['MSFT'],5)[0]['price']['close']=='102','base_bar_does_not_downgrade_update')
        selected[0]['analysis']['timestamp']='2026-01-07T10:00:59.999999999Z'
        require(align_session_records(selected,bars,['MSFT'],5)[0]['status']=='no_session_bar_available_at_feature','no_future_bar_fallback')
        require(research_alignment(repo,analysis_id,snap['replay_id'])==report,'repeat_scoped_alignment_digest')
        repo.connection.execute('DELETE FROM bars');repo.connection.execute('DELETE FROM stream_bar_receipts');repo.connection.commit()
        require(research_alignment(repo,analysis_id,snap['replay_id'])==report,'frozen_receipts_isolated_from_live_changes')
        bad={**meta,'stream_run_id':'foreign'}
        repo.connection.execute('UPDATE replay_runs SET request_json=? WHERE id=?',(encode(bad),snap['replay_id']));repo.connection.commit()
        rejected=False
        try:read_bars_snapshot(repo,snap['replay_id'])
        except StorageError:rejected=True
        require(rejected,'price_scope_metadata_tamper_rejected')
        repo.connection.execute('UPDATE replay_runs SET request_json=? WHERE id=?',(encode(meta),snap['replay_id']))
        repo.connection.execute("UPDATE replay_items SET record_json='{}' WHERE replay_id=?",(snap['replay_id'],));repo.connection.commit()
        rejected=False
        try:read_bars_snapshot(repo,snap['replay_id'])
        except StorageError:rejected=True
        require(rejected,'receipt_payload_tamper_rejected')
        require(report['point_in_time_validated'] is False and report['usable_for_trading'] is False,'recorded_receipt_not_exact_commit_visibility')
        return {'status':'passed','checks':checks,'fixture_source':'original_synthetic_controlled_receipts',
            'network_required':False,'model_required':False,'user_database_modified':False,
            'point_in_time_validated':False,'orders_sent':0}
    finally:repo.close()
