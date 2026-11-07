"""Observed cumulative gross P&L changes, never an execution-date net return ledger."""
from datetime import datetime,timedelta,timezone
from decimal import Decimal,localcontext
import re
import uuid
from .paper_portfolio import read_history,calculate,number,MAX_SAMPLES,MAX_HISTORY_BYTES
from ..data.research import begin_read
from ..data.models import DataValidationError
from ..data.stream_models import canonical_time
from ..data.alignment import nanos
from ..intelligence.policy import digest


def history_report(repository,account_ref=None,*,period='week',start=None,end=None,session_id=None,limit=20):
    if period not in {'day','week','month'}:raise DataValidationError('Portfolio period must be day, week or month')
    if type(limit) is not int or not 1<=limit<=100:raise DataValidationError('Portfolio history limit must be 1..100')
    if account_ref is not None and (not isinstance(account_ref,str) or not re.fullmatch('[0-9a-f]{64}',account_ref)):
        raise DataValidationError('Invalid Paper account-ref')
    if session_id is not None:
        try:
            if str(uuid.UUID(session_id))!=session_id:raise ValueError()
        except (ValueError,TypeError,AttributeError):raise DataValidationError('Specify a canonical Paper worker session UUID') from None
    begin_read(repository)
    try:
        refs=[r[0] for r in repository.connection.execute('SELECT DISTINCT account_ref FROM paper_portfolio_snapshots LIMIT 101').fetchall()]
        if account_ref is None:
            if len(refs)>1:raise DataValidationError('Multiple Paper accounts; specify --account-ref')
            account_ref=refs[0] if refs else None
        all_rows,sha=read_history(repository,account_ref) if account_ref else ([],None)
        repository.connection.commit()
    except BaseException:repository.connection.rollback();raise
    rows=[r for r in all_rows if session_id is None or r['inputs'].get('sampling',{}).get('session_id')==session_id]
    cutoff=canonical_time(end) if end else (rows[-1]['observed_at'] if rows else canonical_time(datetime.now(timezone.utc).isoformat()))
    first=canonical_time(start) if start else (datetime.fromisoformat(cutoff[:19]+'+00:00')-timedelta(days={'day':1,'week':7,'month':30}[period])).strftime('%Y-%m-%dT%H:%M:%S')+cutoff[19:]
    if first>cutoff:raise DataValidationError('Portfolio history start must not exceed end')
    selected=[r for r in rows if first<=r['observed_at']<=cutoff];points=[]
    for row in selected:
        c=calculate(row['inputs'])
        points.append({'snapshot_id':row['snapshot_id'],'observed_at':row['observed_at'],'status':c['status'],
            'known_gross_realized_pnl':c['known_gross_realized_pnl'],'known_gross_unrealized_pnl':c['known_gross_unrealized_pnl'],
            'gross_total_pnl':c['gross_total_pnl'],'by_origin':c['by_origin'],
            'cash':row['inputs']['account']['cash'],'equity':row['inputs']['account']['equity'],
            'sampling':row['inputs'].get('sampling'),'snapshot_sha256':row['sha256']})
    valid=bool(len(points)>=2 and points[0]['observed_at']<points[-1]['observed_at'])
    incomplete=[p['snapshot_id'] for p in points if p['status']!='ready' or p['gross_total_pnl'] is None]
    warnings=['gross_changes_not_net_profit','cumulative_observation_changes_not_execution_period_pnl',
              'sampling_gaps_not_filled','broker_price_timestamp_unavailable','independent_gets_not_atomic','fees_cashflows_not_reconciled']
    if rows and rows[0]['observed_at']>first:warnings.append('history_does_not_reach_requested_start')
    if points and (points[0]['observed_at']!=first or points[-1]['observed_at']!=cutoff):warnings.append('requested_boundary_samples_not_exact')
    if not valid:warnings.append('insufficient_distinct_observation_times')
    if incomplete:warnings.append('incomplete_portfolio_observations')
    gaps=[]
    for a,b in zip(points,points[1:]):
        seconds=(nanos(b['observed_at'])-nanos(a['observed_at']))/1e9
        if seconds>600:gaps.append({'from':a['observed_at'],'to':b['observed_at'],'seconds':seconds})
    if gaps:warnings.append('wall_clock_sampling_gaps_over_10_minutes')
    result={'format':'R26','status':'attention_required' if incomplete else ('ready' if valid else 'insufficient_observations'),
        'account_ref':account_ref,'session_id':session_id,'period':period,'requested_start':first,'requested_end':cutoff,
        'period_semantics':'trailing_1_7_30_days_anchored_at_latest_selected_recorded_sample_unless_explicit_end',
        'observed_start':points[0]['observed_at'] if points else None,'observed_end':points[-1]['observed_at'] if points else None,
        'history_samples':len(all_rows),'selected_history_samples':len(rows),'samples':len(points),
        'history_reaches_requested_start':bool(rows and rows[0]['observed_at']<=first),
        'exact_requested_boundaries_observed':bool(points and points[0]['observed_at']==first and points[-1]['observed_at']==cutoff),
        'gross_realized_change':None,'gross_unrealized_change':None,'gross_total_change':None,
        'cash_change':None,'equity_change':None,'by_origin':{},'incomplete_snapshot_ids':incomplete,
        'gap_count':len(gaps),'gap_samples':gaps[:20],'gap_semantics':'wall_clock_elapsed_no_exchange_calendar_or_expected_session_schedule',
        'points':points[-limit:],'point_limit':limit,'points_truncated':len(points)>limit,
        'history_sha256':sha,'selected_dataset_sha256':digest(points),
        'history_integrity_verified':True,'snapshot_capacity':MAX_SAMPLES,'history_byte_capacity':MAX_HISTORY_BYTES,
        'scope':'observed_snapshot_changes_within_requested_window_not_execution_date_profit',
        'period_pnl_calculated':False,'net_profit':None,'fees_reconciled':False,'cashflows_verified':False,
        'profitability_validated':False,'strategy_performance_validated':False,'broker_price_freshness_validated':False,
        'sampling_completeness_validated':False,'network_required':False,'database_modified':False,'orders_sent':0,'warning_codes':warnings}
    if valid:
        a,b=points[0],points[-1]
        with localcontext() as context:
            context.prec=100
            for key in ('cash','equity'):result[key+'_change']=number(Decimal(b[key])-Decimal(a[key]))
            if not incomplete:
                for target,source in [('gross_realized_change','known_gross_realized_pnl'),('gross_unrealized_change','known_gross_unrealized_pnl'),('gross_total_change','gross_total_pnl')]:
                    result[target]=number(Decimal(b[source])-Decimal(a[source]))
                for origin in ('manual_paper','automatic_news'):
                    before,after=a['by_origin'][origin],b['by_origin'][origin]
                    result['by_origin'][origin]={target:number(Decimal(after[source])-Decimal(before[source]))
                        for target,source in [('gross_realized_change','known_gross_realized_pnl'),('gross_unrealized_change','known_gross_unrealized_pnl'),('gross_total_change','gross_total_pnl')]}
    return result


def render_history(result):
    def show(key):return str(result[key])+' USD' if result[key] is not None else 'hesaplanamadı'
    lines=['Paper portföy geçmişi — gözlemler arası brüt değişim',f"Durum: {result['status']} | Dönem: {result['period']}",
        f"İstenen UTC aralığı: {result['requested_start']} / {result['requested_end']}",
        f"Gözlenen UTC aralığı: {result['observed_start']} / {result['observed_end']}",
        f"Gözlem: {result['samples']} | Miktar/fiyat/emir nedeniyle eksik: {len(result['incomplete_snapshot_ids'])}",
        'Brüt gerçekleşmiş değişim: '+show('gross_realized_change'),'Brüt gerçekleşmemiş değişim: '+show('gross_unrealized_change'),
        'Brüt toplam değişim: '+show('gross_total_change'),'Hesap özsermaye değişimi (ayrı ölçü): '+show('equity_change')]
    for origin,title in [('manual_paper','Elle başlatılan işlemler'),('automatic_news','Otomatik haber işlemleri')]:
        if origin in result['by_origin']:lines.append(title+': brüt değişim '+result['by_origin'][origin]['gross_total_change']+' USD')
    if not result['history_reaches_requested_start']:lines.append('Geçmiş istenen dönem başına ulaşmıyor; sonuç tam gün/hafta/ay olarak yorumlanmamalı.')
    if not result['exact_requested_boundaries_observed']:lines.append('Hesap yalnız gerçek gözlem uçlarını karşılaştırır; sınırlar için eski fiyat taşınmadı.')
    if result['gross_total_change']=='0':lines.append('0 USD yalnız kayıtlı brüt değerleme değişimidir; piyasa fiyatının sabit kaldığını veya fiyatın güncelliğini kanıtlamaz.')
    lines.append(f"10 dakikayı aşan duvar saati boşluğu: {result['gap_count']} (piyasa takvimi kontrolü değil).")
    lines.extend(['Net kâr, ücretler ve para hareketleri doğrulanmadı. Bu rapor gerçekleşme tarihli dönem kârı değildir.',
        'Terminal kapalıyken gözlem oluşmaz. Broker fiyatının zaman damgası ve örnekleme tamlığı doğrulanmadı.'])
    return '\n'.join(lines)
