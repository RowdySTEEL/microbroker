"""Read-only reproducible outcome study. Frozen sources are revalidated on every read."""
from collections import Counter
from copy import deepcopy
from .analysis_replay import replay_analysis
from .alignment import read_bars_snapshot
from .event_study_calculation import POLICY,calculate
from .stream_models import canonical_time
from .models import DataValidationError
from .storage.repository import StorageError
from ..intelligence.policy import digest


def event_study(repository,analysis_replay_id,bars_replay_id,*,end=None,limit=20,_include_rows=False):
    if type(limit) is not int or not 1<=limit<=100:raise DataValidationError('Event study limit must be 1..100')
    records,meta,analysis_sha=replay_analysis(repository,analysis_replay_id,_include_history=True)
    bars,price_meta,bars_sha=read_bars_snapshot(repository,bars_replay_id)
    if meta['provider']!='alpaca' or price_meta.get('format')!='R14' or price_meta.get('scope')!='same_stream_run_receipts':
        raise DataValidationError('Event study requires Alpaca analysis and an R14 same-stream receipt price snapshot')
    end=canonical_time(end) if end else price_meta['receipt_end']
    start=price_meta['receipt_start']
    if not start<=end<=price_meta['receipt_end']:raise DataValidationError('Study end must fit frozen price receipt bounds')
    symbols=sorted(set(meta['symbols'] or price_meta['symbols'])-{'SPY'})
    source={'analysis_replay_id':analysis_replay_id,'bars_replay_id':bars_replay_id,
        'analysis_dataset_sha256':analysis_sha,'bars_dataset_sha256':bars_sha,
        'price_manifest_sha256':price_meta['manifest_sha256'],'stream_run_id':price_meta['stream_run_id'],
        'analyzer_key':meta['analyzer_key'],'engine':meta['engine'],'feed':price_meta['feed'],
        'start':start,'end':end,'event_cutoff':min(end,meta['as_of']),'frozen_analysis_as_of':meta['as_of'],
        'frozen_news_as_of':meta['news_as_of'],'symbols':symbols,'benchmark':'SPY',
        'benchmark_in_snapshot':'SPY' in price_meta['symbols']}
    rows,groups,price_counts=calculate(records,bars,source)
    observed=sum(o['status']=='observed' for r in rows for o in r['outcomes'])
    matched=sum(o['benchmark_status']=='matched' for r in rows for o in r['outcomes'])
    eligible=sum(r['status']=='eligible' for r in rows)
    warnings=['recorded_times_not_exact_commit_visibility','bar_closes_not_executable_quotes',
        'horizons_start_at_following_full_minute_close','raw_unadjusted_prices_no_dividend_or_split_normalization',
        'continuous_minute_filter_may_select_liquid_periods','same_news_event_across_articles_not_clustered',
        'descriptive_rows_not_independent_observations','no_beta_estimation_or_random_time_controls',
        'regular_clock_filter_not_exchange_calendar','no_fees_slippage_or_execution_model']
    if not source['benchmark_in_snapshot']:warnings.append('SPY_missing_absolute_returns_only')
    return {'status':'completed','format':'R30','assessment':('no_eligible_company_events' if not eligible else
            'no_observed_outcomes' if not observed else 'absolute_returns_only' if not matched else 'benchmarked_descriptive_outcomes'),
        'source':source,'policy':deepcopy(POLICY),'study_sha256':digest({'source':source,'policy':POLICY,'rows':rows}),
        'event_revision_rows':len(rows),'eligible_article_company_events':eligible,
        'event_status_counts':dict(Counter(r['status'] for r in rows)),
        'outcome_status_counts':dict(Counter(o['status'] for r in rows for o in r['outcomes'])),
        'observed_absolute_outcomes':observed,'benchmark_matched_outcomes':matched,'price_counts':price_counts,
        'groups':groups,'samples':rows[:limit],'sample_limit':limit,'samples_truncated':len(rows)>limit,
        **({'event_rows':rows} if _include_rows else {}),
        'warning_codes':warnings,'database_modified':False,'source_tables_modified':False,
        'network_required':False,'inference_performed':False,'orders_sent':0,'broker_connected':False,
        'returns_calculated':True,'strategy_policy_applied':False,'point_in_time_validated':False,
        'model_accuracy_validated':False,'profitability_validated':False,'usable_for_trading':False,
        'event_clusters_independent':False,'statistical_significance_calculated':False,
        'net_profit':None,'beta_estimated':False,'random_time_controls_included':False,
        'neutral_group_semantics':'accepted_neutral_company_decisions_not_random_or_causally_matched_controls'}


def session_study(repository,session_id=None,**kwargs):
    from .session import session_summary
    from .research import begin_read
    if session_id is None:
        begin_read(repository)
        try:
            rows=repository.connection.execute('SELECT id,result_json FROM research_sessions ORDER BY started_at DESC,id DESC LIMIT 100').fetchall()
            repository.connection.commit()
        except BaseException:repository.connection.rollback();raise
        from .news_replay import parsed_json
        session_id=next((sid for sid,raw in rows if all(k in parsed_json(raw).get('snapshots',{}) for k in ('analysis_replay_id','bars_replay_id'))),None)
        if session_id is None:raise DataValidationError('No recent research session with analysis and price snapshots')
    try:saved=session_summary(repository,session_id)
    except DataValidationError:
        from ..execution.paper_monitor import report_session
        paper=report_session(repository,session_id);research_id=paper.get('research_session_id')
        if not research_id:raise DataValidationError('Paper session has no saved research source') from None
        saved=session_summary(repository,research_id)
    session=saved['session'];snapshots=session.get('snapshots',{})
    if not all(k in snapshots for k in ('analysis_replay_id','bars_replay_id')):
        raise DataValidationError('Session has no complete frozen analysis/price sources')
    result=event_study(repository,snapshots['analysis_replay_id'],snapshots['bars_replay_id'],**kwargs)
    source=result['source'];request=saved['request']
    if (source['stream_run_id']!=session.get('stream',{}).get('run_id') or
        source['analyzer_key']!=request.get('analyzer_key') or source['engine']!=request.get('engine')):
        raise StorageError('Event study session source identity mismatch')
    result['session_lookup']={'research_session_id':session['session_id'],
        'session_audit_authenticity_verified':False,'snapshots_integrity_verified':True}
    return result


def render_study(report):
    labels={'positive':'pozitif','negative':'negatif','neutral':'nötr'};source=report['source']
    show=lambda value:'ölçülemedi' if value is None else value
    lines=['Haber olay çalışması — kayıtlı fiyatlardan betimsel getiri',
        f"Durum: {report['assessment']} | Şirket olayı: {report['eligible_article_company_events']}",
        f"Analiz snapshot: {source['analysis_replay_id']}",f"Fiyat snapshot: {source['bars_replay_id']}",
        f"Fiyat UTC aralığı: {source['start']} / {source['end']}",
        'Referans: analiz erişiminden sonra başlayan ilk tam dakika barının kapanışı.',
        '+1/+5/+15/+60 dakika bu referans kapanışından ölçülür; analiz/yayın anından değildir.',
        '1 baz puan (bps) = %0,01. SPY farkı = hisse getirisi − aynı aralıktaki SPY getirisi.',
        'Yönlü SPY farkı negatif kararda işareti ters çevirir; short işlem kârı değildir. Nötrde hesaplanmaz.',
        f"Hisse sonucu: {report['observed_absolute_outcomes']} | SPY ile eşleşen sonuç: {report['benchmark_matched_outcomes']}"]
    if report.get('fixture_scope'):lines.insert(0,'SENTETİK DEMO — gerçek piyasa ölçümü değildir; emir gönderilmedi.')
    if not source['benchmark_in_snapshot']:lines.append('Bu fiyat snapshot’ında SPY yok; piyasa karşılaştırması ölçülemedi.')
    lines.append('Haber revizyonu durumları: '+str(report['event_status_counts']))
    lines.append('Ufuk kapsamı: '+str(report['outcome_status_counts']))
    for label,group in report['groups'].items():
        lines.append('\nŞirket kararı: '+labels[label])
        for horizon,stats in group.items():
            a=stats['asset'];adjusted=stats['market_adjusted'];directional=stats['directional_market_adjusted']
            lines.append(f"  +{horizon} dk: hisse n={a['n']} ort={show(a['mean_bps'])} bps | SPY farkı n={adjusted['n']} ort={show(adjusted['mean_bps'])} medyan={show(adjusted['median_bps'])} bps | yönlü ort={show(directional['mean_bps'])} bps")
    for row in report['samples']:
        lines.append(f"\n{row['symbol']} / haber {row['article_id']}: {row['status']} | {labels.get(row['company_label'],'karar yok')}")
        lines.append(f"Yayın → ilk alım: {show(row['publication_to_receipt_seconds'])} sn | İlk alım → analiz erişimi: {show(row['receipt_to_feature_seconds'])} sn")
        if row.get('feature_to_reference_completion_seconds') is not None:
            lines.append(f"Analiz → referans tamamlanması: {row['feature_to_reference_completion_seconds']:.3f} sn")
        for o in row['outcomes']:
            lines.append(f"  +{o['horizon_minutes']} dk: {o['status']} | hisse {show(o['asset_return_bps'])} bps | SPY farkı {show(o['market_adjusted_return_bps'])} bps | kıyas {o['benchmark_status'] or 'hesaplanmadı'}")
    if report['samples_truncated']:lines.append(f"İlk {report['sample_limit']} revizyon satırı gösteriliyor; grup hesapları tüm satırları kullanır.")
    lines+=['\nEksik veya pencere sonrasındaki fiyatlar sıfır getiri sayılmadı; eski fiyat taşınmadı.',
        'Aynı olayı anlatan farklı haberler kümelenmedi; satırlar bağımsız örnek veya anlamlılık kanıtı değildir.',
        'Nötr grup yalnız kayıtlı nötr kararlardır; rastgele zaman kontrolü ve beta tahmini yoktur.',
        'Bar fiyatları uygulanabilir emir fiyatı değildir. Net kâr, model doğruluğu ve kârlılık doğrulanmadı.',
        'Bu rapor broker sorgulamaz, model çalıştırmaz ve emir göndermez.']
    return '\n'.join(lines)
