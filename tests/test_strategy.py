from collections import Counter
from copy import deepcopy
import contextlib
from concurrent.futures import ThreadPoolExecutor
import io
import json
from decimal import Decimal
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from microbroker.cli import main
from microbroker.execution.strategy import (run_strategy,strategy_report,load_inputs,simulate,strategy_policy,MAX_SIGNALS)
from microbroker.execution.strategy_verification import strategy_fixture,verify_strategy,strategy_checks,START,END,FixtureEngine
from microbroker.execution import paper_ledger as ledger
from microbroker.execution.strategy_provenance import attach
from microbroker.data.analysis_replay import replay_analysis
from microbroker.data.research import create_replay,ResearchRequest
from microbroker.data.storage.repository import Repository,StorageError
from microbroker.data.models import DataValidationError
from microbroker.intelligence.policy import digest

class StrategyTests(unittest.TestCase):
    def setUp(self):
        self.repo=Repository('sqlite:///:memory:')
        self.analysis,self.prices=strategy_fixture(self.repo)
        self.records,self.bars,self.source=load_inputs(self.repo,self.analysis,self.prices)
        self.config=ledger.account_config('10000',self.source['start'],max_mark_age_seconds=120,scope='recorded_receipt_strategy_simulation')
    def tearDown(self):self.repo.close()
    def evaluate(self,records=None,bars=None,policy=None,config=None,source=None):
        return simulate(self.records if records is None else records,self.bars if bars is None else bars,
            self.source if source is None else source,self.config if config is None else config,policy or strategy_policy())
    def execute(self,run_id='unit',**options):return run_strategy(self.repo,run_id,self.analysis,self.prices,**options)
    def test_positive_negative_neutral_and_withheld(self):
        result,events=self.evaluate()
        self.assertEqual(result['decision_counts'],{'buy':1,'sell':1,'wait':2})
        self.assertEqual([e['kind'] for e in events],['fill','mark','mark','fill'])
        self.assertEqual(result['reason_counts']['company_decision_withheld'],1)
    def test_expected_quantity_and_costly_pnl(self):
        result,events=self.evaluate()
        self.assertEqual(events[0]['quantity'],'9.985007')
        self.assertEqual(result['valuation']['total_pnl'],'96.80164427')
        self.assertEqual(result['valuation']['cash'],'10096.80164427')
        self.assertEqual(result['valuation']['fees_paid'],'2.00000000')
        self.assertEqual(result['valuation']['positions'],{})
    def test_base_before_signal_not_used_for_fill(self):
        _,events=self.evaluate()
        first=events[0]
        self.assertEqual(first['provenance']['bar_start'],'2026-01-05T10:01:00.000000000Z')
        self.assertEqual(first['at'],'2026-01-05T10:02:00.100000000Z')
        self.assertEqual(first['reference_price'],'100')
    def test_future_price_not_used_at_cutoff(self):
        result=self.execute(end='2026-01-05T10:01:59Z')
        self.assertEqual(result['valuation']['fills'],0)
        self.assertEqual(result['reason_counts']['no_eligible_following_bar'],1)
    def test_analysis_before_window_not_backdated(self):
        result=self.execute(start='2026-01-05T10:02:00Z')
        self.assertEqual(result['valuation']['fills'],0)
        self.assertEqual(result['reason_counts']['no_position_to_sell'],1)
    def test_same_run_idempotent_and_other_run_independent(self):
        first=self.execute();second=self.execute();other=self.execute('other')
        self.assertEqual(second['status'],'existing')
        self.assertEqual(second['valuation'],first['valuation'])
        self.assertEqual(other['valuation'],first['valuation'])
        self.assertNotEqual(first['account_id'],other['account_id'])
    def test_changed_settings_run_key_rejected(self):
        self.execute()
        with self.assertRaises(DataValidationError):self.execute(entry_fraction='0.2')
        self.assertEqual(strategy_report(self.repo,'unit')['valuation']['fills'],2)
    def test_report_recomputes_exact_result(self):
        result=self.execute();report=strategy_report(self.repo,'unit')
        self.assertEqual(report['events_sha256'],result['events_sha256'])
        self.assertEqual(report['valuation'],result['valuation'])
        self.assertFalse(report['simulation_tables_modified'])
    def test_sealed_account_cannot_take_manual_fill(self):
        result=self.execute()
        with self.assertRaises(DataValidationError):
            ledger.append_event(self.repo,result['account_id'],ledger.fill_request('x','MSFT','buy','1','100',END))
        self.assertEqual(ledger.account_summary(self.repo,result['account_id'])['fills'],2)
    def test_scope_and_pnl_report_disclosure(self):
        result=self.execute();summary=ledger.account_summary(self.repo,result['account_id'])
        self.assertTrue(summary['strategy_implemented'])
        self.assertEqual(summary['scope'],'recorded_receipt_strategy_simulation')
        self.assertNotIn('manual_price_assumptions',summary['warning_codes'])
        report=ledger.period_report(self.repo,result['account_id'],START,END)
        self.assertEqual(report['period_pnl'],result['valuation']['total_pnl'])
        self.assertFalse(report['profitability_validated'])
    def test_signal_expiry_boundary_inclusive(self):
        records=deepcopy(self.records[:1]);records[0]['analysis']['timestamp']='2026-01-05T10:01:00.100000000Z'
        result,_=self.evaluate(records=records,policy=strategy_policy(max_signal_age_seconds=120))
        self.assertEqual(result['valuation']['fills'],1)
        records[0]['analysis']['timestamp']='2026-01-05T10:01:00.099999999Z'
        result,_=self.evaluate(records=records,policy=strategy_policy(max_signal_age_seconds=120))
        self.assertEqual(result['valuation']['fills'],0)
    def test_price_freshness_boundary_inclusive(self):
        bars=deepcopy(self.bars[:2]);bars[1]['receipt']['received_at']='2026-01-05T10:03:00.000000000Z'
        result,_=self.evaluate(records=self.records[:1],bars=bars,policy=strategy_policy(max_price_age_seconds=60))
        self.assertEqual(result['valuation']['fills'],1)
        bars[1]['receipt']['received_at']='2026-01-05T10:03:00.000000001Z'
        result,_=self.evaluate(records=self.records[:1],bars=bars,policy=strategy_policy(max_price_age_seconds=60))
        self.assertEqual(result['valuation']['fills'],0)
    def test_old_news_excluded(self):
        records=deepcopy(self.records)
        for r in records:r['news']['data']['created_at']='2026-01-04T10:00:00.000000000Z'
        result,_=self.evaluate(records=records)
        self.assertEqual(result['valuation']['fills'],0)
    def test_news_expires_while_waiting(self):
        result,_=self.evaluate(records=self.records[:1],policy=strategy_policy(max_news_age_seconds=30))
        self.assertEqual(result['reason_counts']['news_expired_before_execution'],1)
    def test_neutral_no_trade(self):
        result,_=self.evaluate(records=[self.records[2]])
        self.assertEqual(result['reason_counts'],{'neutral_company_decision':1})
    def test_negative_while_flat_does_not_short(self):
        result,_=self.evaluate(records=[self.records[1]])
        self.assertEqual(result['reason_counts'],{'no_position_to_sell':1})
        self.assertEqual(result['valuation']['cash'],'10000.00000000')
    def test_high_margin_raw_company_conflict_has_no_fallback(self):
        result,_=self.evaluate(records=[self.records[3]])
        self.assertEqual(result['reason_counts'],{'company_decision_withheld':1})
        self.assertEqual(result['valuation']['fills'],0)
    def test_document_positive_cannot_replace_missing_company(self):
        records=deepcopy(self.records[:1]);records[0]['analysis']['result']['entity_sentiments']={}
        result,_=self.evaluate(records=records)
        self.assertEqual(result['reason_counts'],{'company_context_unavailable':1})
    def test_legacy_analysis_not_raw_sentiment_fallback(self):
        records=deepcopy(self.records[:1]);records[0]['analysis']['result']['analysis_format']='R8'
        result,_=self.evaluate(records=records)
        self.assertEqual(result['reason_counts'],{'legacy_analysis_without_guarded_decision':1})
    def test_repeated_positive_does_not_add_to_position(self):
        extra=deepcopy(self.records[0]);extra['news']['article_id']=77701
        extra['analysis']['timestamp']='2026-01-05T10:02:05.000000000Z'
        result,_=self.evaluate(records=self.records+[extra])
        self.assertEqual(result['reason_counts']['position_already_open'],1)
        self.assertEqual(result['valuation']['fills'],2)
    def test_same_article_correction_not_second_trade(self):
        correction=deepcopy(self.records[0]);correction['news']['revision_hash']='a'*64
        correction['news']['data']['updated_at']='2026-01-05T10:02:00.000000000Z'
        correction['news']['timestamp']='2026-01-05T10:02:01.000000000Z'
        correction['analysis']['timestamp']='2026-01-05T10:02:05.000000000Z'
        result,_=self.evaluate(records=self.records+[correction])
        self.assertEqual(result['reason_counts']['article_already_considered'],1)
    def test_new_revision_cancels_pending_old_revision(self):
        correction=deepcopy(self.records[0]);correction['news']['revision_hash']='a'*64
        correction['news']['data']['updated_at']='2026-01-05T10:01:00.000000000Z'
        correction['news']['timestamp']='2026-01-05T10:01:01.000000000Z';correction['analysis']=None
        result,_=self.evaluate(records=[self.records[0],correction])
        self.assertEqual(result['reason_counts']['revision_superseded_before_execution'],1)
        self.assertEqual(result['valuation']['fills'],0)
    def test_future_revision_does_not_delete_past_fill(self):
        correction=deepcopy(self.records[0]);correction['news']['revision_hash']='a'*64
        correction['news']['data']['updated_at']='2026-01-05T10:04:00.000000000Z'
        correction['news']['timestamp']='2026-01-05T10:04:01.000000000Z';correction['analysis']=None
        result,_=self.evaluate(records=[self.records[0],correction])
        self.assertEqual(result['valuation']['fills'],1)
    def test_symbol_removed_by_revision_no_stale_fallback(self):
        correction=deepcopy(self.records[0]);correction['news']['revision_hash']='a'*64
        correction['news']['mappings']={}
        correction['news']['data']['updated_at']='2026-01-05T10:00:02.000000000Z'
        correction['news']['timestamp']='2026-01-05T10:00:03.000000000Z';correction['analysis']=None
        result,_=self.evaluate(records=[self.records[0],correction])
        self.assertEqual(result['reason_counts']['revision_not_current_at_feature'],1)
    def test_simultaneous_opposite_signals_hold(self):
        records=deepcopy(self.records[:2]);records[1]['analysis']['timestamp']=records[0]['analysis']['timestamp']
        records[1]['news']['timestamp']=records[0]['news']['timestamp']
        records[1]['news']['data']['created_at']=records[0]['news']['data']['created_at']
        result,_=self.evaluate(records=records)
        self.assertEqual(result['reason_counts'],{'conflicting_simultaneous_signals':2})
        self.assertEqual(result['valuation']['fills'],0)
    def test_newer_feature_cancels_older_pending(self):
        records=deepcopy(self.records[:2]);records[1]['analysis']['timestamp']='2026-01-05T10:00:20.000000000Z'
        records[1]['news']['timestamp']='2026-01-05T10:00:15.000000000Z'
        records[1]['news']['data']['created_at']='2026-01-05T10:00:10.000000000Z'
        result,_=self.evaluate(records=records)
        self.assertEqual(result['reason_counts']['superseded_by_newer_feature'],1)
        self.assertEqual(result['valuation']['fills'],0)
    def test_updated_duplicate_and_out_of_order_bars(self):
        updated=deepcopy(self.bars[0]);updated['receipt']['event_type']='u'
        late=deepcopy(self.bars[0]);late['receipt']['received_at']=self.bars[-1]['receipt']['received_at'];late['timestamp']='2026-01-05T09:59:00.000000000Z'
        result,_=self.evaluate(bars=self.bars+[updated,deepcopy(self.bars[-1]),late])
        self.assertEqual(result['price_counts']['updated_bar_not_execution_source'],1)
        self.assertEqual(result['price_counts']['duplicate_minute'],1)
        self.assertEqual(result['price_counts']['out_of_order_minute'],1)
    def test_unfinished_bar_excluded(self):
        bars=deepcopy(self.bars)
        for b in bars:b['receipt']['received_at']=b['timestamp']
        result,_=self.evaluate(bars=bars)
        self.assertEqual(result['valuation']['fills'],0)
        self.assertEqual(result['price_counts']['unfinished_bar'],5)
    def test_empty_signals_still_no_trades(self):
        result,events=self.evaluate(records=[])
        self.assertEqual(result['valuation']['fills'],0)
        self.assertEqual(events,[])
    def test_no_prices_no_trade(self):
        result,events=self.evaluate(bars=[])
        self.assertEqual(events,[])
        self.assertEqual(result['valuation']['total_pnl'],'0.00000000')
    def test_price_symbol_does_not_cross_match(self):
        bars=deepcopy(self.bars)
        for b in bars:b['symbol']='AAPL'
        result,_=self.evaluate(bars=bars)
        self.assertEqual(result['valuation']['fills'],0)
    def test_entry_budget_includes_costs_and_respects_cap(self):
        result,events=self.evaluate(records=self.records[:1],policy=strategy_policy(entry_fraction='1'))
        self.assertLessEqual(Decimal(result['final_state']['positions']['MSFT']['cost_basis']),Decimal('2500'))
        self.assertGreaterEqual(Decimal(result['valuation']['cash']),0)
    def test_tiny_cap_produces_wait(self):
        config=ledger.account_config('0.01',self.source['start'],scope='recorded_receipt_strategy_simulation')
        result,_=self.evaluate(records=self.records[:1],config=config)
        self.assertEqual(result['reason_counts']['insufficient_cash_or_quantity'],1)
    def test_signal_limit_is_not_silent_truncation(self):
        with patch('microbroker.execution.strategy.MAX_SIGNALS',1),self.assertRaises(StorageError):self.evaluate()
    def test_event_limit_fails_entire_plan(self):
        with patch.object(ledger,'MAX_EVENTS',1),self.assertRaises(StorageError):self.evaluate()
    def test_ledger_and_audit_insert_rollback(self):
        self.repo.connection.execute("CREATE TRIGGER fail_strategy BEFORE INSERT ON strategy_simulations BEGIN SELECT RAISE(ABORT,'controlled audit failure'); END")
        self.repo.connection.commit()
        with self.assertRaises(Exception):self.execute()
        self.assertEqual(self.repo.connection.execute("SELECT COUNT(*) FROM paper_accounts WHERE id='r20-unit'").fetchone()[0],0)
        self.assertEqual(self.repo.connection.execute("SELECT COUNT(*) FROM paper_ledger WHERE account_id='r20-unit'").fetchone()[0],0)
    def test_audit_json_and_semantic_tamper_rejected(self):
        self.execute();row=self.repo.connection.execute("SELECT result_json FROM strategy_simulations WHERE id='unit'").fetchone()[0]
        result=json.loads(row);result['valuation']['total_pnl']='9999'
        self.repo.connection.execute('UPDATE strategy_simulations SET result_json=?,result_sha256=?',(ledger.encode(result),digest(result)))
        self.repo.connection.commit()
        with self.assertRaises(StorageError):strategy_report(self.repo,'unit')
    def test_ledger_removed_or_modified_rejected(self):
        self.execute();self.repo.connection.execute("DELETE FROM paper_ledger WHERE account_id='r20-unit' AND ordinal=3");self.repo.connection.commit()
        with self.assertRaises(StorageError):strategy_report(self.repo,'unit')
    def test_source_snapshot_tamper_rejected(self):
        self.execute();self.repo.connection.execute("UPDATE replay_items SET record_json='{}' WHERE replay_id=?",(self.prices,));self.repo.connection.commit()
        with self.assertRaises(StorageError):strategy_report(self.repo,'unit')
    def test_source_live_tables_change_does_not_change_frozen_run(self):
        result=self.execute();self.repo.connection.execute('DELETE FROM news_versions');self.repo.connection.execute('DELETE FROM stream_bar_receipts');self.repo.connection.commit()
        self.assertEqual(strategy_report(self.repo,'unit')['valuation'],result['valuation'])
    def test_history_reader_exposes_all_frozen_versions_without_changing_default(self):
        history,meta,sha=replay_analysis(self.repo,self.analysis,_include_history=True)
        selected,_,_=replay_analysis(self.repo,self.analysis,_include_records=True)
        self.assertEqual(len(history),4)
        self.assertEqual(len(selected),4)
        self.assertEqual(replay_analysis(self.repo,self.analysis)['dataset_sha256'],sha)
    def test_real_frozen_future_revision_retains_earlier_signal_history(self):
        from microbroker.data.news import normalize_news
        from microbroker.data.news_replay import create_news_replay,NewsReplayRequest
        from microbroker.data.analysis_replay import create_analysis_replay
        from microbroker.intelligence.analysis import compute_analysis
        n=normalize_news({'id':999200,'headline':'Microsoft update','content':'Microsoft shares fell sharply.',
            'created_at':'2026-01-05T10:00:00Z','updated_at':'2026-01-05T10:04:00Z','symbols':['MSFT'],
            'source':'original-synthetic','url':'https://example.com/r20-fixture'})
        data=n['data'];first='2026-01-05T10:04:01Z';key=self.source['analyzer_key']
        result=compute_analysis({'data':data,'first_seen':first,'mappings':n['mappings']},FixtureEngine())
        self.repo.connection.execute('INSERT INTO news_versions VALUES (?,?,?,?,?,?,?)',
            ('alpaca',999200,n['revision_hash'],data['created_at'],data['updated_at'],first,ledger.encode(data)))
        self.repo.connection.execute('INSERT INTO news_symbols VALUES (?,?,?,?,?)',('alpaca',999200,n['revision_hash'],'MSFT',n['mappings']['MSFT']))
        self.repo.connection.execute('INSERT INTO news_analyses VALUES (?,?,?,?,?,?,?,?,?)',
            ('alpaca',999200,n['revision_hash'],key,'rules',result['input_sha256'],'2026-01-05T10:04:10Z',ledger.encode(result),digest(result)))
        self.repo.connection.commit()
        news=create_news_replay(self.repo,NewsReplayRequest(END,('MSFT',)))
        analysis=create_analysis_replay(self.repo,news['replay_id'],END,key,'rules')
        selected,_,_=replay_analysis(self.repo,analysis['replay_id'],_include_records=True)
        history,_,_=replay_analysis(self.repo,analysis['replay_id'],_include_history=True)
        self.assertEqual(len(selected),4)
        self.assertEqual(len(history),5)
        simulated=run_strategy(self.repo,'history',analysis['replay_id'],self.prices)
        self.assertEqual(simulated['decision_counts']['buy'],1)
        self.assertEqual(simulated['reason_counts']['article_already_considered'],1)
    def test_existing_unowned_account_name_is_not_reused(self):
        ledger.create_account(self.repo,'r20-unit','10000',START)
        with self.assertRaises(DataValidationError):self.execute()
        self.assertEqual(ledger.account_summary(self.repo,'r20-unit')['ledger_events'],0)
    def test_malformed_saved_request_is_rejected(self):
        self.execute()
        self.repo.connection.execute('UPDATE strategy_simulations SET request_json=?,request_sha256=?',('[]',digest([])))
        self.repo.connection.commit()
        with self.assertRaises(StorageError):strategy_report(self.repo,'unit')
    def test_legacy_price_snapshot_rejected(self):
        from microbroker.data.alignment import alignment_fixture
        _,legacy=alignment_fixture(self.repo)
        with self.assertRaises(DataValidationError):run_strategy(self.repo,'legacy',self.analysis,legacy)
    def test_invalid_policy_values(self):
        for opts in [dict(entry_fraction='0'),dict(entry_fraction='1.1'),dict(max_signal_age_seconds=True),dict(max_price_age_seconds=-1),dict(max_news_age_seconds=86401)]:
            with self.assertRaises(DataValidationError):strategy_policy(**opts)
    def test_invalid_window_and_run_id(self):
        for opts in [dict(start='2025-01-01T00:00:00Z'),dict(end='2027-01-01T00:00:00Z'),dict(start=END,end=START)]:
            with self.assertRaises(DataValidationError):self.execute(**opts)
        with self.assertRaises(DataValidationError):self.execute('x'*61)
    def test_event_price_provenance_verified(self):
        _,events=self.evaluate();request=deepcopy(events[0]);request['provenance']['available_at']='wrong'
        with self.assertRaises(StorageError):ledger.validate_request(request)
        request=deepcopy(events[0]);request['provenance']['bar_start']='2026-01-05T10:00:00.000000000Z'
        with self.assertRaises(StorageError):ledger.validate_request(request)
    def test_manual_account_cannot_import_receipt_scope(self):
        _,events=self.evaluate();config=ledger.account_config('10000',START)
        with self.assertRaises(DataValidationError):ledger.apply_request(config,ledger.initial_state(config),events[0])

class StrategyPersistenceTests(unittest.TestCase):
    def test_disk_restart_preserves_run_and_idempotency(self):
        with tempfile.TemporaryDirectory() as root:
            url='sqlite:///'+str(Path(root)/'db');repo=Repository(url);analysis,prices=strategy_fixture(repo)
            result=run_strategy(repo,'disk',analysis,prices);repo.close();repo=Repository(url)
            try:
                self.assertEqual(strategy_report(repo,'disk')['valuation'],result['valuation'])
                self.assertEqual(run_strategy(repo,'disk',analysis,prices)['status'],'existing')
            finally:repo.close()
    def test_concurrent_same_run_applied_once(self):
        with tempfile.TemporaryDirectory() as root:
            url='sqlite:///'+str(Path(root)/'db');repo=Repository(url);analysis,prices=strategy_fixture(repo);repo.close()
            barrier=threading.Barrier(2)
            def worker():
                local=Repository(url)
                try:
                    barrier.wait(timeout=10)
                    return run_strategy(local,'race',analysis,prices)['status']
                finally:local.close()
            with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(lambda _:worker(),range(2)))
            self.assertCountEqual(results,['completed','existing'])
            repo=Repository(url)
            try:self.assertEqual(ledger.account_summary(repo,'r20-race')['fills'],2)
            finally:repo.close()

class StrategyCLITests(unittest.TestCase):
    def call(self,args):
        stream=io.StringIO()
        with contextlib.redirect_stdout(stream):code=main(args)
        self.assertEqual(code,0)
        return json.loads(stream.getvalue())
    def test_verify_without_settings(self):
        with patch('microbroker.cli.Settings.from_env',side_effect=AssertionError('private verification')):
            self.assertEqual(len(self.call(['verify-strategy'])['checks']),22)
    def test_automatic_private_demo_and_existing_output_protected(self):
        with tempfile.TemporaryDirectory() as root:
            folder=Path(root)/'demo'
            with patch('microbroker.cli.Settings.from_env',side_effect=AssertionError('private demo')):
                result=self.call(['demo-strategy','--output',str(folder)])
            self.assertEqual(result['decision_counts'],{'buy':1,'sell':1,'wait':2})
            self.assertTrue((folder/'summary.json').is_file())
            self.assertTrue((folder/'scenario.sqlite3').is_file())
            with contextlib.redirect_stderr(io.StringIO()):self.assertEqual(main(['demo-strategy','--output',str(folder)]),2)

class StrategyPostgresProtocolTests(unittest.TestCase):
    """PostgreSQL transaction/query contracts, with SQLite transport; not a real server."""
    def test_full_checks_reach_postgres_branches(self):
        repo=Repository('sqlite:///:memory:')
        class Cursor:
            def __init__(self,base):self.base=base
            def execute(self,query,params=()):self.base.execute(query.replace('%s','?'),params)
            def __getattr__(self,name):return getattr(self.base,name)
        class Protocol:
            def __init__(self,base):self.base=base
            def __getattr__(self,name):return getattr(self.base,name)
            @property
            def info(self):return SimpleNamespace(transaction_status=2 if self.base.in_transaction else 0)
            def cursor(self,name=None):return Cursor(self.base.cursor())
            def execute(self,query,params=()):
                if query=='SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY':query='BEGIN'
                return self.base.execute(query.replace('%s','?').removesuffix(' FOR UPDATE'),params)
        repo.connection=Protocol(repo.connection);repo.postgres=True;repo.marker='%s'
        try:self.assertEqual(len(strategy_checks(repo)),22)
        finally:repo.close()
