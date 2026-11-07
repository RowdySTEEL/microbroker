"""Exercise real PostgreSQL using an owned temporary schema only."""
from decimal import Decimal
import json
import uuid
from pathlib import Path
import tempfile
from .repository import Repository, StorageError
from ..models import utc_datetime
from ..request import BarsRequest
from ..adapters.alpaca import normalize_bar, MarketDataError
from ..stream_models import StreamRequest, normalize_event
from ..ingestion.gap_recovery import recover_stream_gaps
from ..research import ResearchRequest,inspect_dataset,create_replay,replay
from ..news import NewsRequest,normalize_news,collect_news,news_summary
from ...intelligence.analysis import AnalysisRequest,analyze_news,analysis_summary
from ...intelligence.sentiment import RulesEngine,AnalysisError
from ...intelligence.review import export_review,evaluate_review

class VerificationFailure(StorageError):
    pass

def verify_postgres(settings):
    if not settings.database_url.startswith("postgresql://"):
        raise StorageError("verify-postgres requires a PostgreSQL DATABASE_URL in .env")
    try:
        import psycopg
        from psycopg import sql
    except ImportError:
        raise StorageError("Install PostgreSQL driver: python3 -m pip install 'psycopg[binary]>=3.2,<4'") from None
    schema = "mbtest_"+uuid.uuid4().hex
    checks = []
    repository = None
    created = False
    def require(condition, name):
        if not condition:
            raise VerificationFailure(f"PostgreSQL verification failed: {name}")
        checks.append(name)
    try:
        with psycopg.connect(settings.database_url,autocommit=True,connect_timeout=10) as admin:
            try:
                admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
                created = True
                repository = Repository(settings.database_url,schema=schema)
                req = BarsRequest(("AAPL","MSFT"),utc_datetime("2026-01-05T00:00:00Z"),utc_datetime("2026-01-07T00:00:00Z"))
                row = {"t":"2026-01-05T05:00:00Z","o":"100.250000000000000001","h":102,"l":99,"c":101,"v":10,"n":3,"vw":100.5}
                initial = normalize_bar("AAPL",row,req)
                repository.collect([initial],req)
                repository.collect([initial],req)
                require(repository.count()==1,"historical_idempotency")
                value=repository.connection.execute("SELECT open FROM bars").fetchone()[0]
                require(value==Decimal(row["o"]),"decimal_precision")
                def broken_page():
                    yield normalize_bar("AAPL",{**row,"c":102},req)
                    yield normalize_bar("MSFT",row,req)
                    raise MarketDataError("injected_test_failure")
                try:
                    repository.collect(broken_page(),req)
                    raise VerificationFailure("Injected page error did not propagate")
                except MarketDataError:
                    pass
                require(repository.count()==1,"historical_partial_insert_rollback")
                require(repository.connection.execute("SELECT close FROM bars").fetchone()[0]==Decimal("101"),"historical_update_rollback")
                audit=repository.connection.execute("SELECT result_json FROM collection_runs WHERE status='failed'").fetchone()
                require(audit is not None and json.loads(audit[0])["committed_bars"]==0,"failed_collection_audit")
                live=StreamRequest(("AAPL",),channels=("bars","updatedBars"))
                stats={"events_received":0,"events_inserted":0,"duplicates":0,"bars_written":0,"received_by_type":{},"received_by_symbol":{}}
                run=repository.begin_stream(live,stats)
                b=normalize_event({**row,"T":"b","S":"AAPL"},live)
                stats=repository.apply_stream_frame(run,[b,b],stats)
                require(stats["events_inserted"]==1 and stats["duplicates"]==1,"stream_exact_duplicate")
                u=normalize_event({**row,"T":"u","S":"AAPL","c":102},live)
                stats=repository.apply_stream_frame(run,[u],stats)
                replayed_base=normalize_event({**row,"T":"b","S":"AAPL","c":"101.5"},live)
                stats=repository.apply_stream_frame(run,[replayed_base],stats)
                require(repository.connection.execute("SELECT close FROM bars WHERE timeframe='1Min'").fetchone()[0]==Decimal("102"),"updated_bar_not_downgraded")
                count_before=repository.connection.execute("SELECT COUNT(*) FROM stream_events").fetchone()[0]
                def interrupted_frame():
                    yield normalize_event({**row,"T":"u","S":"AAPL","c":"101.75"},live)
                    raise MarketDataError("injected_frame_failure")
                try:
                    repository.apply_stream_frame(run,interrupted_frame(),stats)
                    raise VerificationFailure("Injected frame error did not propagate")
                except MarketDataError:
                    pass
                require(repository.connection.execute("SELECT COUNT(*) FROM stream_events").fetchone()[0]==count_before,"stream_frame_event_rollback")
                require(repository.connection.execute("SELECT close FROM bars WHERE timeframe='1Min'").fetchone()[0]==Decimal("102"),"stream_frame_bar_rollback")
                dbstats=json.loads(repository.connection.execute("SELECT stats_json FROM stream_sessions WHERE id=%s",(run,)).fetchone()[0])
                require(dbstats==stats,"stream_frame_audit_rollback")
                gap_start=utc_datetime('2026-01-05T05:00:10Z')
                gap_end=utc_datetime('2026-01-05T05:01:10Z')
                gap=repository.begin_gap(run,live,gap_start)
                repository.close_gap(gap,gap_end)
                back_req=BarsRequest(('AAPL',),utc_datetime('2026-01-05T04:59:00Z'),utc_datetime('2026-01-05T05:01:00Z'),'1Min')
                back_bar=normalize_bar('AAPL',row,back_req)
                new_bar=normalize_bar('AAPL',{**row,'t':'2026-01-05T05:01:00Z'},back_req)
                def broken_recovery():
                    yield new_bar
                    raise MarketDataError('injected_recovery_failure')
                before=repository.count()
                try:
                    repository.backfill_gap(gap,broken_recovery(),back_req)
                    raise VerificationFailure('Injected recovery error did not propagate')
                except MarketDataError:
                    pass
                require(repository.count()==before,'gap_partial_insert_rollback')
                require(repository.gap_summary(run)=={'pending':1},'gap_success_audit_rollback')
                repository.fail_gap(gap)
                require(repository.gap_summary(run)=={'failed':1},'gap_failure_audit')
                result=repository.backfill_gap(gap,[back_bar,new_bar,new_bar],back_req)
                require(result['inserted_bars']==1 and repository.count()==before+1,'gap_retry_missing_only')
                require(repository.connection.execute("SELECT close FROM bars WHERE timeframe='1Min' AND timestamp=%s",(back_bar.timestamp.isoformat(),)).fetchone()[0]==Decimal('102'),'gap_preserves_live_correction')
                require(repository.gap_summary(run)=={'backfilled':1},'gap_success_audit')
                again=recover_stream_gaps(settings,repository,run)
                require(again['outcomes']==[],'gap_recovery_idempotency')
                repository.connection.commit()
                research=ResearchRequest(symbols=('AAPL',))
                records,report=inspect_dataset(repository,research)
                require(len(records)==2 and report['status']!='failed','research_postgres_quality')
                snap=create_replay(repository,research)
                played=replay(repository,snap['replay_id'])
                require(played['dataset_sha256']==snap['dataset_sha256'],'research_postgres_snapshot_digest')
                require(replay(repository,snap['replay_id'])==played,'research_postgres_repeat_replay')
                repository.connection.execute("UPDATE bars SET close=101 WHERE timeframe='1Min'")
                repository.connection.commit()
                require(replay(repository,snap['replay_id'])==played,'research_postgres_snapshot_isolation')
                events_req=ResearchRequest(source='events',symbols=('AAPL',))
                event_snap=create_replay(repository,events_req)
                require(replay(repository,event_snap['replay_id'])['item_count']==event_snap['item_count'],'research_postgres_event_replay')
                repository.connection.execute("UPDATE replay_items SET record_json='{}' WHERE replay_id=%s AND ordinal=0",(snap['replay_id'],))
                repository.connection.commit()
                rejected=False
                try:replay(repository,snap['replay_id'])
                except StorageError:rejected=True
                require(rejected,'research_postgres_tamper_rejected')
                news_req=NewsRequest(('AAPL','MSFT'),req.start,req.end)
                story={'id':1,'headline':'Fixture news','summary':'Fixture summary','symbols':['AAPL','MSFT'],
                       'created_at':'2026-01-05T05:00:00Z','updated_at':'2026-01-05T05:01:00Z',
                       'content':'<p>Fixture body.</p>','source':'fixture','url':'https://example.com/fixture'}
                class NewsFixture:
                    pages=1
                    def __init__(self,*rows,fail=False):self.rows=rows;self.fail=fail
                    def iter_news(self,request):
                        for item in self.rows:yield normalize_news(item)
                        if self.fail:raise MarketDataError('injected_news_failure')
                collect_news(NewsFixture(story),repository,news_req)
                repeat=collect_news(NewsFixture(story),repository,news_req)
                summary=news_summary(repository)
                require(summary['articles_total']==1 and summary['versions_total']==1 and repeat['exact_duplicates']==1,'news_idempotency')
                newer={**story,'updated_at':'2026-01-05T05:02:00Z','headline':'Updated fixture','symbols':['MSFT']}
                collect_news(NewsFixture(newer),repository,news_req)
                require(news_summary(repository)['versions_total']==2,'news_revision_preserved')
                collect_news(NewsFixture(story),repository,news_req)
                summary=news_summary(repository)
                require(summary['samples'][0]['headline']=='Updated fixture','news_stale_not_downgraded')
                require(summary['symbol_counts']=={'MSFT':1},'news_symbol_versioning')
                partial={k:v for k,v in newer.items() if k!='content'}
                collect_news(NewsFixture(partial),repository,news_req)
                require(news_summary(repository)['contentless_articles']==0,'news_content_preserved')
                try:
                    collect_news(NewsFixture({**story,'id':2},fail=True),repository,news_req)
                    raise VerificationFailure('Injected news failure did not propagate')
                except MarketDataError:pass
                require(news_summary(repository)['articles_total']==1,'news_partial_page_rollback')
                audit=repository.connection.execute("SELECT result_json FROM news_collection_runs WHERE status='failed'").fetchone()
                require(audit is not None and json.loads(audit[0])['committed_articles']==0,'news_failure_audit')
                repository.connection.commit()
                analysis_request=AnalysisRequest(symbols=('AAPL','MSFT'))
                news_before=repository.connection.execute('SELECT data_json FROM news_versions ORDER BY revision_hash').fetchall()
                repository.connection.commit()
                analysis=analyze_news(repository,analysis_request,RulesEngine())
                require(analysis['inserted_analyses']==1,'analysis_inserted')
                again=analyze_news(repository,analysis_request,RulesEngine())
                require(again['inserted_analyses']==0 and again['existing_analyses']==1,'analysis_idempotency')
                require(repository.connection.execute('SELECT data_json FROM news_versions ORDER BY revision_hash').fetchall()==news_before,'analysis_news_preserved')
                repository.connection.commit()
                corrected={**newer,'headline':'Microsoft cuts guidance','updated_at':'2026-01-05T05:03:00Z'}
                collect_news(NewsFixture(corrected),repository,news_req)
                analyze_news(repository,analysis_request,RulesEngine())
                require(repository.connection.execute('SELECT COUNT(*) FROM news_analyses').fetchone()[0]==2,'analysis_revision_retained')
                repository.connection.commit()
                current=analysis_summary(repository,analysis_request,engine='rules')
                require(current['analyzed_current_articles']==1 and current['label_counts']=={'negative':1},'analysis_current_revision_only')
                class AlternateRules(RulesEngine):
                    metadata={**RulesEngine.metadata,'version':'pg-alternate-fixture'}
                alternative=analyze_news(repository,analysis_request,AlternateRules())
                require(alternative['inserted_analyses']==1 and alternative['analyzer_key']!=analysis['analyzer_key'],'analysis_engine_isolation')
                collect_news(NewsFixture({**corrected,'id':3}),repository,news_req)
                before=repository.connection.execute('SELECT COUNT(*) FROM news_analyses').fetchone()[0]
                repository.connection.commit()
                class BrokenRules(RulesEngine):
                    metadata={**RulesEngine.metadata,'version':'pg-failure-fixture'}
                    def __init__(self):self.calls=0
                    def score(self,text):
                        self.calls+=1
                        if self.calls==2:raise AnalysisError('injected_analysis_failure')
                        return super().score(text)
                try:
                    analyze_news(repository,analysis_request,BrokenRules())
                    raise VerificationFailure('Injected analysis failure did not propagate')
                except AnalysisError:pass
                require(repository.connection.execute('SELECT COUNT(*) FROM news_analyses').fetchone()[0]==before,'analysis_partial_rollback')
                failed=repository.connection.execute("SELECT result_json FROM news_analysis_runs WHERE status='failed'").fetchone()
                require(failed is not None and json.loads(failed[0])['committed_analyses']==0,'analysis_failure_audit')
                opposite={**corrected,'id':10,'symbols':['MSFT','AAPL'],
                    'headline':'Microsoft raises guidance; Apple cuts guidance.', 'content':'','summary':''}
                collect_news(NewsFixture(opposite),repository,news_req)
                analyze_news(repository,analysis_request,RulesEngine())
                sample=next(s for s in analysis_summary(repository,analysis_request,engine='rules')['samples'] if s['id']==10)
                require(sample['entity_sentiments']['MSFT']['sentiment']['label']=='positive' and
                        sample['entity_sentiments']['AAPL']['sentiment']['label']=='negative','analysis_entity_opposite_contexts')
                require(sample['evidence_summary']['opposite_rule_phrases'] and sample['evidence_summary']['raw_label']==sample['sentiment']['label'], 'analysis_mixed_evidence_preserves_raw_label')
                shared={**opposite,'id':11,'headline':'Microsoft and Apple publish a report.'}
                collect_news(NewsFixture(shared),repository,news_req)
                analyze_news(repository,analysis_request,RulesEngine())
                sample=next(s for s in analysis_summary(repository,analysis_request,engine='rules')['samples'] if s['id']==11)
                require(all(e['status']=='shared_context_only' and e['sentiment'] is None
                            for e in sample['entity_sentiments'].values()),'analysis_entity_shared_context_excluded')
                metadata_only={**opposite,'id':12,'symbols':['MSFT'],'headline':'A security vendor raises guidance.'}
                collect_news(NewsFixture(metadata_only),repository,news_req)
                analyze_news(repository,analysis_request,RulesEngine())
                sample=next(s for s in analysis_summary(repository,analysis_request,engine='rules')['samples'] if s['id']==12)
                require(sample['entity_sentiments']['MSFT']['status']=='no_explicit_mention' and
                        sample['entity_sentiments']['MSFT']['sentiment'] is None,'analysis_entity_metadata_only_unscored')
                repeated=analyze_news(repository,analysis_request,RulesEngine())
                require(repeated['inserted_analyses']==0 and repeated['existing_analyses']==5,
                        'analysis_entity_repeat_no_insert')
                before_review=repository.connection.execute('SELECT data_json FROM news_versions ORDER BY article_id,revision_hash').fetchall()
                audit_count=repository.connection.execute('SELECT COUNT(*) FROM news_analysis_runs').fetchone()[0]
                repository.connection.commit()
                with tempfile.TemporaryDirectory() as temporary:
                    folder=Path(temporary)/'review'
                    exported=export_review(repository,analysis_request,folder,engine='rules')
                    require(exported['articles']==5 and not exported['database_modified'] and
                        repository.connection.execute('SELECT data_json FROM news_versions ORDER BY article_id,revision_hash').fetchall()==before_review and
                        repository.connection.execute('SELECT COUNT(*) FROM news_analysis_runs').fetchone()[0]==audit_count,
                        'review_export_no_data_changes')
                    repository.connection.commit()
                    path=folder/'review.json';review=json.loads(path.read_text())
                    frozen=json.loads((folder/'reference.json').read_text())
                    first=review['articles'][0]
                    expected=frozen['payload']['predictions'][str(first['id'])]['document']
                    first['labels'].update(document=expected,source='synthetic',reviewer='pg-fixture')
                    path.write_text(json.dumps(review))
                    evaluated=evaluate_review(folder)
                    require(evaluated['document']['labeled']==1 and
                            evaluated['document']['accuracy_including_abstentions']==1 and
                            evaluated['human_only']['document']['labeled']==0,'review_frozen_reference_metrics')
                flow_story={**opposite,'id':13,'headline':'money flows are negative in Microsoft and Apple.'}
                collect_news(NewsFixture(flow_story),repository,news_req)
                analyze_news(repository,analysis_request,RulesEngine())
                sample=next(s for s in analysis_summary(repository,analysis_request,engine='rules')['samples'] if s['id']==13)
                require(all(e['reported_money_flow_counts']=={'negative':1} and e['context_unit_count']==1
                            for e in sample['entity_sentiments'].values()),'analysis_explicit_shared_money_flow')
                from ..news_replay import NewsReplayRequest,create_news_replay,replay_news
                unavailable=create_news_replay(repository,NewsReplayRequest('1999-01-01T00:00:00Z',('AAPL','MSFT')))
                require(replay_news(repository,unavailable['replay_id'])['available_articles']==0,
                        'news_replay_asof_excludes_future_receipts')
                frozen_news=create_news_replay(repository,NewsReplayRequest('2099-01-01T00:00:00Z',('AAPL','MSFT')))
                before_news_replay=replay_news(repository,frozen_news['replay_id'])
                collect_news(NewsFixture({**opposite,'id':14,'headline':'Another controlled news fixture.'}),repository,news_req)
                require(replay_news(repository,frozen_news['replay_id'])==before_news_replay and
                        replay_news(repository,frozen_news['replay_id'])==before_news_replay,
                        'news_replay_repeat_and_frozen_source_isolation')
                from ..analysis_replay import create_analysis_replay,replay_analysis
                frozen_analysis=create_analysis_replay(repository,frozen_news['replay_id'],
                    '2099-01-02T00:00:00Z',analysis['analyzer_key'],'rules')
                require(replay_analysis(repository,frozen_analysis['replay_id'],'1999-01-01T00:00:00Z')['available_analyses']==0,
                        'analysis_replay_future_completion_excluded')
                before_analysis_replay=replay_analysis(repository,frozen_analysis['replay_id'])
                require(before_analysis_replay['available_analyses']>0 and
                        replay_analysis(repository,frozen_analysis['replay_id'])==before_analysis_replay,
                        'analysis_replay_repeat_frozen_digest')
                from ..alignment import research_alignment
                alignment_bars=create_replay(repository,ResearchRequest(symbols=('AAPL','MSFT')))
                before_alignment_count=repository.connection.execute('SELECT COUNT(*) FROM replay_runs').fetchone()[0]
                repository.connection.commit()  # PostgreSQL SELECT starts a transaction; alignment requires a clean one.
                aligned=research_alignment(repository,frozen_analysis['replay_id'],alignment_bars['replay_id'])
                require(research_alignment(repository,frozen_analysis['replay_id'],alignment_bars['replay_id'])==aligned,
                        'alignment_frozen_snapshot_repeat')
                require(not aligned['database_modified'] and not aligned['point_in_time_validated'] and
                        not aligned['returns_calculated'] and
                        repository.connection.execute('SELECT COUNT(*) FROM replay_runs').fetchone()[0]==before_alignment_count,
                        'alignment_readonly_research_limits')
                repository.connection.commit()  # Close the alignment count verification read before the next snapshot.
                session_news=create_news_replay(repository,NewsReplayRequest('2099-01-02T00:00:00Z',
                    ('AAPL','MSFT'),first_seen_since='2099-01-01T00:00:00Z'))
                require(replay_news(repository,session_news['replay_id'])['available_articles']==0,
                        'session_receipt_scope_excludes_legacy')
                repository.connection.execute("INSERT INTO research_sessions VALUES (%s,%s,%s,%s,%s,%s)",
                    ('pg-session-fixture','2026-01-05T10:00:00Z','2026-01-05T10:01:00Z','completed','{}','{"orders_sent":0}'))
                repository.connection.commit()
                require(repository.connection.execute("SELECT status FROM research_sessions WHERE id=%s",('pg-session-fixture',)).fetchone()[0]=='completed',
                        'session_audit_roundtrip')
                repository.connection.commit()
                from ..session_price_verification import price_fixture
                from ..alignment import read_bars_snapshot
                receipt_analysis,receipt_snapshot,receipt_run,receipt_stats,receipt_base=price_fixture(repository)
                receipt_bars,receipt_meta,_=read_bars_snapshot(repository,receipt_snapshot['replay_id'])
                require(len(receipt_bars)==3 and receipt_stats['duplicates']==2 and
                    all(b['receipt']['run_id']==receipt_run for b in receipt_bars),'session_price_receipts_and_duplicate_scope')
                receipt_report=research_alignment(repository,receipt_analysis,receipt_snapshot['replay_id'])
                require(receipt_report['samples'][0]['price']['close']=='101', 'session_price_future_update_excluded')
                require(research_alignment(repository,receipt_analysis,receipt_snapshot['replay_id'])==receipt_report,
                    'session_price_repeat_frozen_scope')
                receipt_count=repository.connection.execute('SELECT COUNT(*) FROM stream_bar_receipts WHERE run_id=%s',(receipt_run,)).fetchone()[0]
                repository.connection.commit()
                repository.capture_bar_receipts=True
                def broken_receipt_frame():
                    yield receipt_base
                    raise MarketDataError('synthetic_frame_failure')
                try:
                    try:repository.apply_stream_frame(receipt_run,broken_receipt_frame(),receipt_stats)
                    except MarketDataError:pass
                finally:repository.capture_bar_receipts=False
                require(repository.connection.execute('SELECT COUNT(*) FROM stream_bar_receipts WHERE run_id=%s',(receipt_run,)).fetchone()[0]==receipt_count,
                    'session_price_frame_receipt_rollback')
                repository.connection.commit()
                from ..research import encode
                repository.connection.execute('UPDATE replay_runs SET request_json=%s WHERE id=%s',
                    (encode({**receipt_meta,'stream_run_id':'foreign'}),receipt_snapshot['replay_id']))
                repository.connection.commit()
                rejected=False
                try:read_bars_snapshot(repository,receipt_snapshot['replay_id'])
                except StorageError:rejected=True
                require(rejected,'session_price_manifest_tamper_rejected')
                from ...intelligence.review_verification import review_fixture
                from ...intelligence.explanation import explain_analysis
                review_request=review_fixture(repository)
                explained=explain_analysis(repository,99915,symbol='MSFT',engine='rules')
                require(explained['companies']['MSFT']['context_verified'] and not explained['inference_performed'],
                    'review_full_context_explanation_verified')
                with tempfile.TemporaryDirectory() as review_root:
                    folder=Path(review_root)/'r15-review'
                    exported=export_review(repository,review_request,folder,engine='rules')
                    require((folder/'review.html').is_file() and not exported['model_predictions_in_labeling_page'],
                        'review_blind_labeling_page')
                    labels=json.loads((folder/'review.json').read_text())
                    for item in labels['articles']:
                        if item['id']==99915:item['labels'].update(document='neutral',source='human',reviewer='simulated-fixture',prediction_seen=True)
                    (folder/'review.json').write_text(json.dumps(labels))
                    evaluated=evaluate_review(folder)
                    require(evaluated['human_only']['document']['labeled']==1 and evaluated['human_blind']['document']['labeled']==0,
                        'review_exposed_labels_excluded_from_blind_metrics')
                from ...intelligence.attribution_verification import attribution_fixture
                guard_request,guard_explanation=attribution_fixture(repository)
                guarded=guard_explanation['companies']['MSFT']
                require(guarded['status']=='attribution_ambiguous' and guarded['sentiment'] is None and
                    guarded['raw_mention_sentiment']['label']=='positive','attribution_raw_output_preserved_but_unavailable')
                guard_news=create_news_replay(repository,NewsReplayRequest('2099-01-02T00:00:00Z',('MSFT',)))
                guard_analysis=create_analysis_replay(repository,guard_news['replay_id'],'2099-01-02T00:00:00Z',guard_explanation['analyzer_key'],'rules')
                guarded_alignment=research_alignment(repository,guard_analysis['replay_id'],alignment_bars['replay_id'])
                guarded_row=next(r for r in guarded_alignment['samples'] if r['article_id']==99916)
                require(guarded_row['status']=='company_sentiment_unavailable' and guarded_row['price'] is None,
                    'attribution_raw_label_not_price_fallback')
                require(analyze_news(repository,guard_request,RulesEngine())['inserted_analyses']==0,
                    'attribution_repeat_analysis_idempotency')
                from ...intelligence.decision_verification import decision_checks
                checks.extend(decision_checks(repository))
                from ...intelligence.unit_verification import unit_checks
                checks.extend(unit_checks(repository))
                from ...execution.paper_verification import paper_checks
                checks.extend(paper_checks(repository))
                from ...execution.strategy_verification import strategy_checks
                checks.extend(strategy_checks(repository))
                from ...execution.risk_verification import risk_checks
                checks.extend(risk_checks(repository))
                from ...execution.paper_broker_verification import broker_checks
                checks.extend(broker_checks(repository))
                from ...execution.paper_worker_verification import worker_checks
                checks.extend(worker_checks(repository))
                from ...execution.paper_monitor_verification import monitor_checks
                checks.extend(monitor_checks(repository))
                from ...execution.portfolio_verification import portfolio_checks
                checks.extend(portfolio_checks(repository))
                from ...execution.portfolio_history_verification import history_checks
                checks.extend(history_checks(repository))
                from ...execution.report_health_verification import health_checks
                checks.extend(health_checks(repository))
                from ...execution.observation_verification import observation_checks
                checks.extend(observation_checks(repository))
                from ...execution.report_evidence_verification import evidence_checks
                checks.extend(evidence_checks(repository))
                from ..event_study_verification import event_checks
                checks.extend(event_checks(repository))
                repository.connection.execute("UPDATE news_analyses SET result_json='{}'")
                repository.connection.commit()
                rejected=False
                try:analysis_summary(repository,analysis_request,engine='rules')
                except StorageError:rejected=True
                require(rejected,'analysis_tamper_rejected')
            finally:
                if repository is not None:
                    repository.close()
                if created:
                    admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
            require(admin.execute("SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name=%s",(schema,)).fetchone()[0]==0,"temporary_schema_cleanup")
    except VerificationFailure:
        raise
    except Exception as exc:
        import traceback
        frames=traceback.extract_tb(exc.__traceback__)
        location=frames[-1].name if frames else 'connect'
        # Function names/error classes/codes only; never include exception text or DB URL.
        code=getattr(exc,'sqlstate',None)
        details=f"type={type(exc).__name__} stage={location} passed_checks={len(checks)}"
        if code:details+=f" sqlstate={code}"
        raise StorageError('PostgreSQL verification failed: '+details) from None
    return {"status":"passed", "checks":checks, "schema_cleaned":True, "public_tables_modified":False}
