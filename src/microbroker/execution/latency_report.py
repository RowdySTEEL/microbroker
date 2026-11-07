"""Offline view of sealed measurements, without inventing missing legacy durations."""
from .paper_monitor import report_session
from .report_evidence import timing_view,render_coverage


def latency_report(repository,session_id=None):
    report=report_session(repository,session_id)
    coverage=report.get('decision_coverage')
    return {'status':'recorded' if report.get('timing') else 'not_recorded','format':'R29',
        'session_id':report['session_id'],'execution_timeline':report.get('execution_timeline'),
        'local_stage_durations':timing_view(report.get('timing')),
        'pipeline':coverage.get('pipeline_latency') if coverage else None,
        'decision_coverage':{k:v for k,v in coverage.items() if k!='pipeline_latency'} if coverage else None,
        'decision_coverage_view':report.get('decision_coverage_view'),
        'price_warning_history':report['price_health']['warning_history'],
        'coverage_error':report.get('coverage_error'),'stored_observation_sha256':report['stored_observation_sha256'],
        'database_modified':False,'network_required':False,'orders_sent':0,'model_required':False,
        'actual_broker_fill_latency_validated':False,'profitability_validated':False}


def render_latency(value):
    lines=['Paper oturum gecikmeleri — kayıtlı ölçümler',f"Oturum: {value['session_id']}"]
    if value['status']=='not_recorded':
        return '\n'.join(lines+['Bu eski kayıtta R28 süre ölçümü yok; sıfır gecikme varsayılmadı.'])
    timeline=value.get('execution_timeline')
    if timeline:lines.append(f"Ortak UTC başlangıç / bitiş: {timeline['started_at']} / {timeline['deadline']}")
    if value['local_stage_durations'].get('legacy_portfolio_capture_mixed'):
        lines.append('Eski portfolio_capture ölçümü boş slot kontrollerini de içeriyor; gerçek kayıt süresi geçmişten yeniden hesaplanamadı.')
    else:
        lines.append('portfolio_capture yalnız yeni snapshot başarılarını ölçer; boş kontroller portfolio_slot_check altında, hatalar ayrı gösterilir.')
    for name,group in [('Yerel süreler (monoton saat)',value['local_stage_durations']),('Zaman damgası farkları',value.get('pipeline'))]:
        lines.append(name+':')
        if not group:lines.append('Ölçüm kaydedilmemiş.');continue
        for stage,stats in group['stages'].items():
            show=lambda v:'ölçülemedi' if v is None else f'{v:.3f}'
            lines.append(f"  {stage}: n={stats['samples']} | medyan={show(stats['median_seconds'])} sn | p95={show(stats['p95_seconds'])} sn | geçersiz/negatif={stats['invalid_or_negative_samples']}")
    lines+=render_coverage(value.get('decision_coverage_view'))
    lines+=['Alım ve analiz zamanları kesin veritabanı commit zamanı değildir.',
        'Filled ölçüsü ilk yerel gerçekleşme gözlemidir; broker gerçekleşme anı ölçülmedi.',
        'İç içe ve eşzamanlı süreler toplanmamalı. Az örnekte p95 genelleştirilemez.',
        'Bu komut broker sorgulamaz veya emir göndermez. Gecikmenin getiriye etkisi henüz ölçülmedi.']
    return '\n'.join(lines)
