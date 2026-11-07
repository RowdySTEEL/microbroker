"""Coverage from a stored session audit; no model, market calendar or API calls."""

def session_coverage(result):
    stream=result.get('stream') or {}
    cycles=result.get('cycles')
    polls=result.get('news_polls')
    received=result.get('received_articles')
    if received is None and isinstance(cycles,list) and len(cycles)==polls and all('received_articles' in c for c in cycles):
        received=sum(c['received_articles'] for c in cycles)
    articles=result.get('session_articles')
    analyses=result.get('available_analyses')
    stream_bars=stream.get('bars_written')
    snapshot_bars=result.get('snapshot_minute_bars')
    if snapshot_bars is None and result.get('price_coverage')=='no_minute_bars_in_session_window':snapshot_bars=0
    alignment=result.get('alignment')
    matched=alignment.get('matched_rows') if isinstance(alignment,dict) else None
    reasons=[]
    if result.get('status')=='failed':reasons.append('session_failed')
    if polls==0:reasons.append('no_successful_news_poll')
    elif received==0:reasons.append('news_queries_returned_empty')
    elif result.get('new_versions')==0:reasons.append('only_previously_stored_news_versions')
    if articles==0:reasons.append('no_articles_in_session_snapshot')
    if articles is not None and analyses is not None and articles>analyses:reasons.append('pending_snapshot_analyses')
    if stream_bars==0:
        reasons.append('no_new_current_bar_writes' if stream.get('bar_receipts_recorded',0)>0 else 'no_stream_minute_bars')
    if snapshot_bars==0:reasons.append('no_minute_bars_in_snapshot_window')
    if matched==0:reasons.append('no_aligned_company_rows')
    if isinstance(alignment,dict) and alignment.get('status_counts',{}).get('no_session_bar_available_at_feature'):
        reasons.append('session_prices_unavailable_at_feature')
    if result.get('price_warmup',{}).get('status')=='timed_out':reasons.append('price_warmup_timed_out')
    terminal=result.get('status') in {'completed','stopped','no_data','failed'}
    assessment=('failed' if result.get('status')=='failed' else
        'in_progress' if not terminal else
        'aligned_research_rows' if matched is not None and matched>0 else
        'no_data' if result.get('status')=='no_data' else 'incomplete_research_coverage')
    return {'format':'R14' if result.get('price_scope')=='same_stream_run_receipts' else 'R13','assessment':assessment,'reason_codes':reasons,
        'news':{'successful_polls':polls,'received_articles_across_polls':received,
                'new_versions':result.get('new_versions'),'snapshot_articles':articles},
        'analysis':{'snapshot_available':analyses,
                'snapshot_pending':articles-analyses if articles is not None and analyses is not None else None,
                'inserted_analyses':result.get('inserted_analyses')},
        'prices':{'stream_authenticated':stream.get('authenticated'),
                'stream_subscribed':stream.get('subscribed'),
                'stream_events_received':stream.get('events_received'),
                'stream_bars_written':stream_bars,'snapshot_minute_bars':snapshot_bars,
                'snapshot_scope':result.get('price_scope','current_bars_in_session_window_may_include_other_writers'),
                **({'snapshot_receipts':result.get('snapshot_price_receipts'),
                    'stream_bar_receipts_recorded':stream.get('bar_receipts_recorded',0),
                    'warmup':result.get('price_warmup')} if result.get('price_scope') else {})},
        'alignment':{'performed':isinstance(alignment,dict),'matched_rows':matched,
                'company_rows':alignment.get('company_rows') if isinstance(alignment,dict) else None,
                'status_counts':alignment.get('status_counts') if isinstance(alignment,dict) else None},
        'market_open_checked':False,'point_in_time_validated':False,
        'model_accuracy_validated':False,'trading_readiness_validated':False,
        'source':'session_audit_fields_not_independent_reverification'}
