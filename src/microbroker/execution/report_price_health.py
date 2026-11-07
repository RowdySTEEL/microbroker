"""Describe recorded price evidence. Never probe the API or infer an outage cause."""


def price_health(research,observation,recorded_history=None):
    from .report_evidence import price_warning_history,validate_warning_history
    history=price_warning_history(research,observation)
    if recorded_history is not None:
        # The R29 tracker is sealed before the cycle hook returns. Its last
        # processed observation can survive a failure before research.cycles
        # receives that hook result. Legacy records use the saved cycles above.
        history=validate_warning_history(recorded_history)
    stream=research.get('stream') or {};warmup=research.get('price_warmup') or {}
    def count(value):return value if type(value) is int and value>=0 else None
    def symbols(key):
        value=observation.get(key)
        return sorted(set(value)) if isinstance(value,list) and all(isinstance(s,str) for s in value) else []
    writes=count(stream.get('bars_written'));receipts=count(stream.get('bar_receipts_recorded'))
    events=count(stream.get('events_received'));snapshot=count(research.get('snapshot_minute_bars'))
    if snapshot is None and research.get('price_coverage')=='no_minute_bars_in_session_window':snapshot=0
    by_type=stream.get('received_by_type') or {}
    minute_events=[count(by_type.get(k)) for k in ('b','u')]
    positive_stream=any(v is not None and v>0 for v in [writes,receipts,*minute_events])
    typed_counts=[count(v) for v in by_type.values()]
    complete_types=events is not None and bool(by_type) and all(v is not None for v in typed_counts) and sum(typed_counts)==events
    no_stream=writes==0 and not positive_stream and (events==0 or receipts==0 or complete_types)
    gap=symbols('price_gap_symbols');clock=symbols('price_clock_flags');fresh=symbols('fresh_price_symbols')
    reasons=[]
    if stream.get('status')=='failed' or stream.get('error_code'):reasons.append('stream_failure_recorded')
    if no_stream:reasons.append('no_recorded_stream_minute_bars')
    if snapshot==0:reasons.append('no_recorded_snapshot_minute_bars')
    if warmup.get('status')=='timed_out':reasons.append('price_warmup_timed_out')
    if gap:reasons.append('price_gap_recorded_in_last_cycle')
    if clock:reasons.append('price_clock_issue_recorded_in_last_cycle')
    if history.get('cycles_with_clock_flags'):reasons.append('price_clock_issue_recorded_during_session')
    if history.get('cycles_with_gaps'):reasons.append('price_gap_recorded_during_session')
    # Same-run snapshots cannot contain bars if that stream explicitly received zero events.
    inconsistent=research.get('price_scope')=='same_stream_run_receipts' and events==0 and snapshot is not None and snapshot>0
    if inconsistent:reasons.append('recorded_price_evidence_inconsistent')
    status=('stream_failed' if 'stream_failure_recorded' in reasons else
        'inconsistent_evidence' if inconsistent else
        'missing_minute_bars' if no_stream and (snapshot is None or snapshot==0) else
        'missing_snapshot_prices' if snapshot==0 else
        'clock_issue_recorded' if clock else
        'gap_recorded' if gap else
        'recorded_minute_data_present' if positive_stream or (snapshot is not None and snapshot>0) else
        'warmup_timed_out' if warmup.get('status')=='timed_out' else
        'warmup_pending' if warmup.get('status') in {'waiting','pending','in_progress'} else 'not_recorded')
    current_status=status
    if status=='recorded_minute_data_present' and history.get('warning_recorded'):
        status='recovered_with_recorded_warnings'
    blocked=history.get('warning_recorded') is True or status in {'stream_failed','inconsistent_evidence','clock_issue_recorded','gap_recorded',
        'missing_minute_bars','missing_snapshot_prices','warmup_timed_out'}
    return {'format':'R29','status':status,'current_status':current_status,'warning_history':history,
        'acceptance_incomplete_from_recorded_price_evidence':blocked,
        'reason_codes':reasons,'stream_events_received':events,'stream_bars_written':writes,
        'stream_bar_receipts_recorded':receipts,'snapshot_minute_bars':snapshot,
        'stream_authenticated':stream.get('authenticated'),'stream_subscribed':stream.get('subscribed'),
        'warmup_status':warmup.get('status'),'last_cycle_fresh_price_symbols':fresh,
        'last_cycle_price_gap_symbols':gap,'last_cycle_price_clock_symbols':clock,
        'source':'stored_session_audit_not_independent_price_reverification',
        'broker_price_freshness_validated':False,'point_in_time_validated':False,
        'provider_incident_checked':False,'outage_cause_determined':False,'network_required':False,
        'acceptance_passed':False}
