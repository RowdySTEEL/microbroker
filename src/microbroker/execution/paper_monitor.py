"""Persistent bounded Paper session observations and offline, recomputed decision views."""
from collections import Counter
import json
import uuid
from . import paper_broker as broker
from .equity_history import read_samples
from .report_price_health import price_health
from ..data.research import begin_read,encode
from ..data.stream_models import canonical_time
from ..data.storage.repository import StorageError
from ..data.models import DataValidationError
from ..intelligence.policy import digest,prepare_text,company_contexts
from ..intelligence.research_decision import company_research_label

MAX_BYTES=2*1024*1024


def identifier(value):
    try:return str(uuid.UUID(value))
    except (ValueError,TypeError,AttributeError):raise DataValidationError('Specify a valid Paper/research session UUID') from None


def parsed(raw):
    if not isinstance(raw,str) or len(raw.encode())>MAX_BYTES:raise StorageError('Paper session audit exceeds 2 MiB')
    try:
        result=json.loads(raw)
        if not isinstance(result,dict):raise ValueError()
        return result
    except (ValueError,TypeError):raise StorageError('Invalid Paper session audit JSON') from None


def configuration(worker):
    result={'format':'R24','session_id':worker.session_id,'account_ref':worker.account_ref,
            'started_at':worker.started,'deadline':worker.deadline,'symbols':list(worker.symbols),
            'feed':worker.feed,'analyzer_key':worker.key,'engine':worker.engine,
            'notional':worker.notional,'max_buys':worker.max_buys,'environment':'alpaca_paper'}
    if hasattr(worker,'portfolio_sampling_policy'):result['portfolio_sampling_policy']=dict(worker.portfolio_sampling_policy)
    return result


def save_run(repository,worker,result):
    """Update only the observation. Immutable session configuration must match on every write."""
    config=configuration(worker);at=canonical_time(worker.api.now());raw=encode(result)
    if len(raw.encode())>MAX_BYTES:raise StorageError('Paper session audit exceeds 2 MiB')
    if result.get('status') not in {'running','completed','stopped','no_data','failed','interrupted'}:
        raise StorageError('Invalid Paper session audit status')
    if result.get('paper',{}).get('session_id')!=worker.session_id or result['paper'].get('account_ref')!=worker.account_ref:
        raise StorageError('Paper session observation scope mismatch')
    broker.locked(repository,worker.account_ref)
    try:
        m=repository.marker
        found=load_run(repository,worker.session_id)
        if found and (found[0]!=config or at<found[2]):
            raise StorageError('Paper session configuration/clock changed')
        if at<worker.started:raise StorageError('Paper session local clock order failed')
        sha=digest({'request_sha256':digest(config),'observed_at':at,'result':result})
        repository.connection.execute(f'INSERT INTO paper_worker_runs VALUES ({",".join([m]*9)}) ON CONFLICT (id) DO UPDATE SET updated_at=excluded.updated_at,status=excluded.status,result_json=excluded.result_json,result_sha256=excluded.result_sha256',
            (worker.session_id,worker.account_ref,worker.started,at,result['status'],encode(config),digest(config),raw,sha))
        repository.connection.commit()
    except BaseException:repository.connection.rollback();raise


def load_run(repository,session_id):
    m=repository.marker
    row=repository.connection.execute(f'SELECT account_ref,created_at,updated_at,status,request_json,request_sha256,result_json,result_sha256 FROM paper_worker_runs WHERE id={m}',(session_id,)).fetchone()
    if not row:return None
    account,created,updated,status,raw,sha,body,body_sha=row
    config=parsed(raw);result=parsed(body)
    try:
        if (config['format']!='R24' or config['environment']!='alpaca_paper' or config['session_id']!=session_id or
            config['account_ref']!=account or config['started_at']!=created or digest(config)!=sha or
            canonical_time(created)!=created or canonical_time(updated)!=updated or updated<created or
            digest({'request_sha256':sha,'observed_at':updated,'result':result})!=body_sha or
            result['status']!=status or status not in {'running','completed','stopped','no_data','failed','interrupted'} or
            result.get('research_session',{}).get('paper_worker_session_id',session_id)!=session_id or result['paper']['session_id']!=session_id or result['paper']['account_ref']!=account):
            raise StorageError('Paper session audit integrity mismatch')
    except (KeyError,TypeError,ValueError):raise StorageError('Invalid Paper session audit') from None
    return config,result,updated,body_sha,'sealed_r24_observation'


def legacy_run(repository,session_id):
    """Read existing R23 research audits without upgrading or rewriting their evidence."""
    m=repository.marker
    rows=repository.connection.execute(f'SELECT id,status,result_json,request_json FROM research_sessions WHERE id={m} OR result_json LIKE {m} LIMIT 3',
        (session_id,'%'+session_id+'%')).fetchall()
    matches=[]
    for rid,status,raw,metadata in rows:
        body=parsed(raw);meta=parsed(metadata);wid=body.get('paper_worker_session_id')
        if wid is None or (rid!=session_id and wid!=session_id):continue
        if body.get('session_id')!=rid or body.get('status')!=status:raise StorageError('Legacy Paper session identity mismatch')
        identifier(wid)
        sealed=load_run(repository,wid)
        if sealed is not None:
            matches.append(sealed);continue
        refs={r[0] for r in repository.connection.execute(f'SELECT DISTINCT account_ref FROM paper_news_decisions WHERE session_id={m}',(wid,)).fetchall()}
        for ref,snapshot in repository.connection.execute(f'SELECT DISTINCT account_ref,snapshot_json FROM paper_equity_samples WHERE snapshot_json LIKE {m} LIMIT 10001',('%'+wid+'%',)).fetchall():
            if parsed(snapshot).get('session_id')==wid:refs.add(ref)
        if len(refs)!=1:raise StorageError('Legacy Paper session account cannot be uniquely verified')
        account=next(iter(refs));read_samples(repository,account)
        config={'format':'R23_legacy','session_id':wid,'research_session_id':rid,'account_ref':account,
                'started_at':canonical_time(body['started_at']),'analyzer_key':meta.get('analyzer_key'),
                'engine':meta.get('engine'),'symbols':meta.get('symbols'),'feed':meta.get('feed'),'environment':'alpaca_paper'}
        matches.append((config,{'status':status,'research_session':body},body.get('finished_at',body['started_at']),None,'legacy_research_audit_not_sealed'))
    if len(matches)>1:raise StorageError('Multiple legacy Paper sessions match')
    return matches[0] if matches else None


def decision_assessment(status,research,paper,observation):
    if status in {'failed','interrupted'}:return 'session_failed_or_interrupted'
    if paper['uncertain_intents']:return 'broker_reconciliation_required'
    if paper['broker_order_status_counts'].get('filled',0):return 'paper_fills_recorded'
    if paper['post_attempts']:return 'paper_orders_attempted'
    if status=='running':return 'running'
    if research.get('received_articles')==0:return 'no_news_returned'
    if observation.get('analyses_pending',0)>0:return 'analysis_not_available'
    if paper['decisions']==0:return 'no_new_session_decisions'
    if paper['status_counts'].get('pending',0):return 'awaiting_eligible_conditions'
    return 'decisions_waited_without_order'


def assessment(status,research,paper,observation,*,health=None):
    base=decision_assessment(status,research,paper,observation)
    if base in {'session_failed_or_interrupted','broker_reconciliation_required'}:return base
    health=health or price_health(research,observation)
    if health['acceptance_incomplete_from_recorded_price_evidence']:
        if health['status']=='stream_failed':return 'price_stream_failed'
        if status=='running':return 'awaiting_price_data'
        return 'price_data_unavailable' if health['status'] in {'missing_minute_bars','missing_snapshot_prices','warmup_timed_out'} else 'price_coverage_incomplete'
    if health['status']=='warmup_pending' and status=='running':return 'awaiting_price_data'
    return base


def report_session(repository,session_id=None,*,limit=20):
    from .paper_worker import all_decisions
    if type(limit) is not int or not 1<=limit<=100:raise DataValidationError('Report limit must be 1..100')
    if session_id is not None:session_id=identifier(session_id)
    begin_read(repository)
    try:
        if session_id is None:
            row=repository.connection.execute('SELECT id FROM paper_worker_runs ORDER BY created_at DESC,id DESC LIMIT 1').fetchone()
            if row is None:raise DataValidationError('No R24 Paper session; specify a previous research --session-id for R23')
            session_id=row[0]
        loaded=load_run(repository,session_id) or legacy_run(repository,session_id)
        if loaded is None:raise DataValidationError('Unknown Paper session')
        config,result,observed,sha,semantics=loaded;wid=config['session_id'];account=config['account_ref']
        rows=[r for r in all_decisions(repository,account) if r[1]['session_id']==wid]
        ids={r[5] for r in rows};linked={i['id']:(i,s,at) for i,s,_,_,at in broker.all_intents(repository,account) if i['id'] in ids}
        equity,_=read_samples(repository,account);equity=[r for r in equity if r['session_id']==wid]
        repository.connection.commit()
    except BaseException:repository.connection.rollback();raise
    samples=[];pending_reasons=(result.get('last_processed_observation') or result.get('observation',{})).get('pending_wait_reasons',{})
    for key,request,state,_,_,intent_id in sorted(rows,key=lambda r:(r[1]['record']['analysis']['timestamp'],r[0])):
        record=request['record'];analysis=record['analysis']['result'];symbol=request['symbol']
        entity=analysis.get('entity_sentiments',{}).get(symbol,{})
        label=company_research_label(analysis,entity) if entity.get('status')=='scored' else None
        stored_order=linked.get(intent_id);order=stored_order[1]['order'] if stored_order else None
        _,fields,_,_=prepare_text(record['news']['data'])
        unit_evidence=company_contexts(fields,record['news']['mappings']).get(symbol,{}).get('units',[])
        if digest('\n'.join(u['text'] for u in unit_evidence))!=entity.get('context_sha256'):
            raise StorageError('Paper report context/source mismatch')
        samples.append({'article_id':record['news']['article_id'],'revision_hash':record['news']['revision_hash'],
            'symbol':symbol,'headline':record['news']['data']['headline'],'summary':record['news']['data']['summary'][:500],
            'published_at':record['news']['data']['created_at'],'first_seen_at':record['news']['first_seen_at'],
            'feature_available_at':record['analysis']['timestamp'],'company_label':label,
            'company_context_status':entity.get('status'),'company_research_decision':entity.get('research_decision'),
            'decision_status':state['status'],'decision_reason':state['reason'],
            'last_observed_pending_reason':pending_reasons.get(key),'intent_id':intent_id if stored_order else None,
            'order':({k:order[k] for k in ('status','filled_qty','filled_avg_price','updated_at')} if order else None),
            'order_phase':stored_order[1]['phase'] if stored_order else None,
            'post_attempts':stored_order[1]['post_attempts'] if stored_order else 0,
            'context_preview':[{'field':u['field'],'start_char':u['start_char'],'end_char':u['end_char'],'text':u['text'][:300]} for u in unit_evidence[:2]],
            'context_preview_verified':True,
            'context_preview_truncated':any(len(u['text'])>300 for u in unit_evidence[:2]) or len(unit_evidence)>2})
    paper={'decisions':len(rows),'status_counts':dict(Counter(r[2]['status'] for r in rows)),
        'reason_counts':dict(Counter(r[2]['reason'] for r in rows)),
        'post_attempts':sum(s['post_attempts'] for _,s,_ in linked.values()),
        'broker_order_status_counts':dict(Counter(s['order']['status'] for _,s,_ in linked.values() if s['order'])),
        'uncertain_intents':[i['id'] for i,s,_ in linked.values() if s['phase'] in {'uncertain','submitting'}]}
    research=result.get('research_session',{});observation=result.get('last_processed_observation') or result.get('observation',{})
    from .report_evidence import coverage_view
    coverage=coverage_view(result.get('decision_coverage'))
    health=price_health(research,observation,result.get('price_warning_history'))
    decision_status=decision_assessment(result['status'],research,paper,observation)
    headline='session_cleanup_failed' if result.get('worker_cleanup_error') else assessment(result['status'],research,paper,observation,health=health)
    if coverage and coverage['current_revision_rows_not_assessed_at_least']>0:
        if headline in {'decisions_waited_without_order','no_new_session_decisions','analysis_not_available','awaiting_eligible_conditions'}:
            headline='decision_coverage_incomplete'
        if decision_status in {'decisions_waited_without_order','no_new_session_decisions','analysis_not_available','awaiting_eligible_conditions'}:
            decision_status='decision_coverage_incomplete'
    return {'status':'reported','format':'R24','session_id':wid,'research_session_id':research.get('session_id',config.get('research_session_id')),
        'report_revision':'R29','decision_coverage':result.get('decision_coverage'),'decision_coverage_view':coverage,'coverage_error':result.get('coverage_error'),
        'timing':result.get('timing'),'execution_timeline':result.get('execution_timeline'),'final_cycle':result.get('observation'),
        'price_health':health,
        'decision_assessment':decision_status,
        'portfolio_sampling':result.get('portfolio_sampling'),'portfolio_capture_error':result.get('portfolio_capture_error'),
        'account_ref':account,'test_fixture':result.get('fixture_scope'),'recorded_session_status':result['status'],'assessment':headline,
        'configuration':config,'last_saved_observation_at':observed,'news':{k:research.get(k) for k in ('news_polls','received_articles','new_versions','inserted_analyses')},
        'latest_cycle':observation,'prices':research.get('stream') or research.get('price_warmup'),
        'paper':paper,'samples':samples[:limit],'sample_limit':limit,'samples_truncated':len(samples)>limit,
        'equity_observations_in_session':len(equity),'recorded_error':research.get('error_details') or result.get('error_details'),
        'cleanup_error':result.get('worker_cleanup_error'),'equity_capture_error':result.get('equity_capture_error'),
        'audit_semantics':semantics,'stored_observation_sha256':sha,
        'audit_integrity_verified':semantics=='sealed_r24_observation','decision_and_order_integrity_verified':True,
        'integrity_semantics':'digests_detect_changes_not_authenticity',
        'running_status_is_last_saved_observation_not_process_liveness':True,
        'broker_observation_semantics':'latest_locally_recorded_order_state_not_fresh_broker_query',
        'last_local_broker_observation_at':max((at for _,_,at in linked.values()),default=None),
        'database_modified':False,'network_required':False,'inference_performed':False,
        'post_requests_attempted_by_this_call':0,'orders_submitted_by_this_call':0,
        'profitability_validated':False,'net_profit':None,'live_trading_supported':False}


def render_report(report):
    descriptions={
        'decision_coverage_incomplete':'Karar kapsamı eksik; bazı hazır analizler incelenmedi',
        'no_news_returned':'Haber sorguları boş döndü',
        'paper_fills_recorded':'Kaydedilmiş Paper gerçekleşmesi var',
        'paper_orders_attempted':'Emir gönderim girişimi var; gerçekleşme durumunu kontrol et',
        'broker_reconciliation_required':'Belirsiz emir için broker mutabakatı gerekiyor',
        'running':'Son kayıtta oturum çalışıyordu',
        'session_failed_or_interrupted':'Oturum hata aldı veya kesildi',
        'session_cleanup_failed':'Oturum kapanışında hata oluştu',
        'price_data_unavailable':'Fiyat verisi alınamadı veya kullanılabilir fiyat yok; oturum kabulü eksik',
        'price_coverage_incomplete':'Fiyat kapsamında kesinti veya zaman tutarsızlığı kaydı var; oturum kabulü eksik',
        'price_stream_failed':'Fiyat akışı hata kaydetti; oturum kabulü eksik',
        'awaiting_price_data':'Son kayıtta kullanılabilir fiyat verisi bekleniyordu',
        'analysis_not_available':'Seçilen bazı haberlerin analizi henüz hazır değil',
        'no_new_session_decisions':'Bu oturuma ait yeni şirket kararı oluşmadı',
        'awaiting_eligible_conditions':'Uygun fiyat veya işlem koşulları bekleniyor',
        'decisions_waited_without_order':'Kararlar bekleme ile sonuçlandı'}
    reasons={
        'company_decision_withheld':'Şirket kararı belirsiz/çelişkili olduğu için kullanılmadı',
        'neutral_company_decision':'Şirket kararı nötr',
        'awaiting_price_warmup':'İzlenen sembollerde taze fiyat bekleniyor',
        'awaiting_fresh_price':'Bu sembolde taze fiyat bekleniyor',
        'awaiting_following_completed_bar':'Analizden sonra başlayan tamamlanmış dakika bekleniyor',
        'worker_session_ended':'Oturum kapandığı için bekleyen sinyal emekliye ayrıldı',
        'market_closed':'Piyasa kapalı',
        'no_managed_worker_position':'Çıkış yapılacak R23/R24 yönetimli pozisyon yok',
        'broker_intent_consumed_not_necessarily_filled':'Broker emri kaydedildi; gerçekleşme ayrı izlenir',
        'signal_expired_or_future':'Sinyal zamanı uygun değil',
        'news_expired_or_future':'Haber yayın zamanı uygun değil',
        'session_buy_limit':'Oturum alış sınırı doldu',
        'superseded_by_newer_feature':'Daha yeni haber kararı önceki sinyalin yerini aldı',
        'conflicting_simultaneous_signals':'Aynı zamandaki şirket kararları karşıt',
        'receipt_time_inconsistent':'Fiyat alım zamanı tutarsız',
        'price_feed_gap':'Fiyat akışında kesinti',
        'signal_before_price_recovery':'Sinyal fiyat akışı toparlanmadan önce oluşmuş'}
    reasons['revision_superseded']='Kararın haber revizyonu daha yeni bir revizyonla değişmiş'
    news=report['news'];paper=report['paper'];labels={'positive':'pozitif','negative':'negatif','neutral':'nötr',None:'karar yok'}
    lines=(["SENTETİK DEMO — Alpaca'ya emir gönderilmedi"] if report.get('test_fixture') else [])
    news_line=(f"Haber: {news.get('news_polls')} sorgu / {news.get('received_articles')} alınan kayıt" if news.get('news_polls') is not None else 'Haber: sorgu kaydı henüz yok')
    lines += [f"Paper oturumu: {report['session_id']}",
        'Sonuç: '+descriptions.get(report['assessment'],report['assessment']),
        news_line,
        f"Karar: {paper['decisions']} | Gönderim girişimi: {paper['post_attempts']} | Kaydedilmiş filled emir: {paper['broker_order_status_counts'].get('filled',0)}",
        f"Özsermaye gözlemi: {report['equity_observations_in_session']}",
        f"Son oturum kaydı: {report['last_saved_observation_at']} (UTC)"]
    health=report.get('price_health')
    if health:
        show=lambda v:'kaydedilmemiş' if v is None else str(v)
        lines.extend([f"Fiyat akışı: {show(health['stream_events_received'])} olay | {show(health['stream_bars_written'])} bar yazımı | {show(health['stream_bar_receipts_recorded'])} bar receipt",
            f"Fiyat snapshot barı: {show(health['snapshot_minute_bars'])} | Isınma: {health['warmup_status'] or 'kaydedilmemiş'}"])
        if health['acceptance_incomplete_from_recorded_price_evidence']:
            lines.append('Fiyat kabulü eksik: '+', '.join(health['reason_codes']))
        if report.get('decision_assessment') and report['decision_assessment']!=report['assessment']:
            lines.append('Karar sonucu (ayrı): '+descriptions.get(report['decision_assessment'],report['decision_assessment']))
        if health['status']=='not_recorded':lines.append('Bu kayıtta fiyat kapsamı bilgisi yok; sıfır veri veya başarılı fiyat kabulü varsayılmadı.')
        lines.append('Fiyat bilgileri son kayıtlı gözlemdir; bu rapor akışı yeniden sorgulamaz ve kesinti nedeni belirlemez.')
        history=health.get('warning_history')
        if history:
            if history['cycles_with_price_fields']:
                lines.append(f"Oturum içi fiyat uyarıları: zaman {show(history['cycles_with_clock_flags'])} döngü | boşluk {show(history['cycles_with_gaps'])} döngü | uyarılı toplam {show(history['cycles_with_any_warning'])} döngü")
                if health.get('current_status')=='recorded_minute_data_present' and history['warning_recorded']:
                    lines.append('Son fiyat gözlemi toparlanmış; oturum içindeki uyarılar kabul değerlendirmesinde korunuyor.')
                if not history['whole_session_history_available']:lines.append('Fiyat uyarısı geçmişi tüm oturumu kapsamıyor; sıfır uyarı varsayılmadı.')
            else:lines.append('Oturum içi fiyat uyarısı geçmişi kaydedilmemiş.')
    sampling=report.get('portfolio_sampling')
    if sampling:
        lines.extend([f"Portföy gözlemi: {sampling['recorded_snapshots']} | Başarısız örnekleme: {len(sampling['errors'])}",
            f"Son portföy snapshot: {sampling['last_snapshot_id']}"])
        if sampling.get('sampling_degraded') or sampling['errors'] or report.get('portfolio_capture_error'):lines.append('Portföy örneklemesi eksik; dönem raporunda boşlukları kontrol et.')
    from .report_evidence import render_coverage
    lines.extend(render_coverage(report.get('decision_coverage_view')))
    if report.get('coverage_error'):lines.append('Karar kapsamı hesaplanamadı; eksiksiz değerlendirme varsayılmadı.')
    timing=report.get('timing')
    if timing:
        duration=timing.get('stages',{}).get('worker_tick',{})
        lines.append(f"Karar döngüsü süresi: n={duration.get('samples',0)} | medyan={duration.get('median_seconds')} sn | p95={duration.get('p95_seconds')} sn")
    if sampling and sampling.get('observer_thread_alive'):
        lines.append('Portföy gözlemcisi son kayıtta kapanmamıştı; örnekleme tamamlandı varsayılmadı.')
    for sample in report['samples']:

        reason=sample['last_observed_pending_reason'] if sample['decision_status']=='pending' and sample['last_observed_pending_reason'] else sample['decision_reason']
        lines += ['',f"{sample['symbol']} / haber {sample['article_id']}: {sample['headline']}",
            'Kayıtlı şirket kararı: '+labels[sample['company_label']],
            'Neden: '+reasons.get(reason,reason)]
        if reason=='revision_superseded':
            lines.append('Bu satır eski revizyona aittir; güncel şirket kararı olarak kullanılmaz.')
        coverage=report.get('decision_coverage_view') or {}
        newer=next((r for r in coverage.get('samples',[]) if r.get('article_id')==sample['article_id'] and r.get('symbol')==sample['symbol'] and r.get('reason') in {'claim_for_prior_revision','claim_for_different_analysis'}),None)
        if newer:
            lines.append('Kapsam kesitindeki güncel şirket kararı: '+labels[newer['current_company_label']]+' | '+str(newer['current_research_decision_status']))
        if sample['decision_reason']=='worker_session_ended' and sample['last_observed_pending_reason']:
            lines.append('Son bekleme koşulu: '+reasons.get(sample['last_observed_pending_reason'],sample['last_observed_pending_reason']))
        for unit in sample['context_preview']:lines.append('Kullanılan bağlam: '+unit['text'])
        if sample['order']:
            lines.append(f"Broker son kaydı: {sample['order']['status']} | Gerçekleşen miktar: {sample['order']['filled_qty']} | Ortalama fiyat: {sample['order']['filled_avg_price'] or 'henüz yok'}")
    if report['samples_truncated']:lines.append(f"İlk {report['sample_limit']} karar gösteriliyor; --limit ile artırılabilir.")
    lines += ['', 'Broker durumları yerel son kayıtlardır; bu komut emir göndermez ve broker sorgulamaz.',
              'Net strateji kârı ve kârlılık henüz doğrulanmadı.']
    return '\n'.join(lines)
