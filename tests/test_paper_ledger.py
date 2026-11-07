import contextlib
import io
import json
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from microbroker.cli import main
from microbroker.data.models import DataValidationError
from microbroker.data.storage.repository import Repository, StorageError
from microbroker.execution import paper_ledger as ledger
from microbroker.execution.paper_verification import verify_paper_ledger

T0='2026-01-05T10:00:00Z'
T1='2026-01-05T10:01:00Z'
T2='2026-01-05T10:02:00Z'
T3='2026-01-05T10:03:00Z'

class PaperLedgerTests(unittest.TestCase):
    def setUp(self):
        self.repo=Repository('sqlite:///:memory:')
        ledger.create_account(self.repo,'test','10000',T0)
    def tearDown(self):self.repo.close()
    def fill(self,key='b',symbol='AAPL',side='buy',quantity='10',price='100',at=T1):
        return ledger.append_event(self.repo,'test',ledger.fill_request(key,symbol,side,quantity,price,at))
    def summary(self,at=None):return ledger.account_summary(self.repo,'test',at)
    def test_private_verifier(self):
        result=verify_paper_ledger()
        self.assertEqual(result['status'],'passed')
        self.assertEqual(len(result['checks']),22)
        self.assertFalse(result['user_database_modified'])
        self.assertFalse(result['profitability_validated'])
    def test_initial_balance(self):
        result=self.summary()
        self.assertEqual(result['cash'],'10000.00000000')
        self.assertEqual(result['total_pnl'],'0.00000000')
        self.assertEqual(result['positions'],{})
        self.assertEqual(result['orders_sent'],0)
    def test_costs_and_buy_basis(self):
        result=self.fill()
        self.assertEqual(result['cash'],'8998.50000000')
        self.assertEqual(result['positions']['AAPL']['average_cost'],'100.15000000')
        self.assertEqual(result['fees_paid'],'1.00000000')
        self.assertEqual(result['slippage_cost'],'0.50000000')
        self.assertEqual(result['total_pnl'],'-1.50000000')
    def test_weighted_average_and_partial_then_full_sale(self):
        self.fill()
        self.fill('b2',quantity='5',price='120',at=T2)
        after=self.fill('s',side='sell',quantity='6',price='130',at=T3)
        self.assertEqual(after['positions']['AAPL']['cost_basis'],'961.68000000')
        self.assertEqual(after['realized_pnl'],'137.49000000')
        final=self.fill('s2',side='sell',quantity='9',price='90',at=T3)
        self.assertEqual(final['positions'],{})
        self.assertEqual(final['realized_pnl'],'-15.59500000')
        self.assertEqual(final['cash'],'9984.40500000')
        self.assertEqual(final['total_pnl'],final['realized_pnl'])
    def test_fractional_position_basis_remainder_clears(self):
        ledger.create_account(self.repo,'fraction','100',T0,fee_per_fill='0',slippage_bps='0',max_position_fraction='1')
        for key,side,qty,price in [('b','buy','0.3','0.333333'),('s','sell','0.1','0.333333'),('s2','sell','0.2','0.333333')]:
            result=ledger.append_event(self.repo,'fraction',ledger.fill_request(key,'AAPL',side,qty,price,T1))
        self.assertEqual(result['cash'],'100.00000000')
        self.assertEqual(result['positions'],{})
        self.assertEqual(result['realized_pnl'],'0.00000000')
    def test_duplicate_after_later_events_returns_latest_state(self):
        self.fill()
        self.fill('b2',quantity='1',at=T2)
        duplicate=self.fill()
        self.assertEqual(duplicate['status'],'duplicate')
        self.assertEqual(duplicate['positions']['AAPL']['quantity'],'11')
        self.assertEqual(duplicate['event_ordinal'],0)
        self.assertEqual(duplicate['ledger_events'],2)
    def test_equivalent_numeric_request_is_idempotent(self):
        self.fill()
        self.assertEqual(self.fill(quantity='10.000000',price='100.0')['status'],'duplicate')
    def test_conflicting_duplicate_rejected_without_mutation(self):
        self.fill();before=self.summary()
        with self.assertRaises(DataValidationError):self.fill(price='101')
        self.assertEqual(self.summary(),before)
    def test_cash_limit(self):
        ledger.create_account(self.repo,'small','10',T0,max_position_fraction='1')
        with self.assertRaises(DataValidationError):
            ledger.append_event(self.repo,'small',ledger.fill_request('b','AAPL','buy','1','10',T1))
        self.assertEqual(ledger.account_summary(self.repo,'small')['ledger_events'],0)
    def test_position_cap_includes_buy_fee(self):
        with self.assertRaises(DataValidationError):self.fill(quantity='25')
        self.assertEqual(self.summary()['ledger_events'],0)
    def test_exact_position_cap_allowed(self):
        ledger.create_account(self.repo,'cap','1000',T0,fee_per_fill='0',slippage_bps='0')
        result=ledger.append_event(self.repo,'cap',ledger.fill_request('b','AAPL','buy','2.5','100',T1))
        self.assertEqual(result['positions']['AAPL']['cost_basis'],'250.00000000')
    def test_short_and_oversell_rejected(self):
        with self.assertRaises(DataValidationError):self.fill(side='sell')
        self.fill()
        with self.assertRaises(DataValidationError):self.fill('s',side='sell',quantity='11',at=T2)
        self.assertEqual(self.summary()['fills'],1)
    def test_mark_requires_open_position(self):
        with self.assertRaises(DataValidationError):
            ledger.append_event(self.repo,'test',ledger.mark_request('m','AAPL','110',T1))
    def test_mark_retry_and_conflict(self):
        self.fill();request=ledger.mark_request('m','AAPL','110',T2)
        ledger.append_event(self.repo,'test',request)
        self.assertEqual(ledger.append_event(self.repo,'test',request)['status'],'duplicate')
        with self.assertRaises(DataValidationError):
            ledger.append_event(self.repo,'test',ledger.mark_request('m','AAPL','111',T2))
        self.assertEqual(self.summary()['marks'],1)
    def test_time_order_and_equal_time_ordinal(self):
        self.fill()
        with self.assertRaises(DataValidationError):self.fill('old',at=T0)
        self.assertEqual(self.fill('same',quantity='1')['event_ordinal'],1)
    def test_nanosecond_cutoff_does_not_include_future_mark(self):
        self.fill(at='2026-01-05T10:01:00.000000001Z')
        self.assertEqual(self.summary(T1)['positions'],{})
        self.assertEqual(self.summary('2026-01-05T10:01:00.000000001Z')['fills'],1)
    def test_inclusive_report_start_and_end(self):
        self.fill()
        ledger.append_event(self.repo,'test',ledger.mark_request('m','AAPL','110',T2))
        report=ledger.period_report(self.repo,'test',T1,T2)
        self.assertEqual(report['period_pnl'],'100.00000000')
        self.assertEqual(report['fills_in_period'],0)
        self.assertEqual(report['fees_in_period'],'0.00000000')
        same=ledger.period_report(self.repo,'test',T2,T2)
        self.assertEqual(same['period_pnl'],'0.00000000')
    def test_invalid_report_boundaries(self):
        for start,end in [(T2,T1),('2025-12-31T00:00:00Z',T1)]:
            with self.assertRaises(DataValidationError):ledger.period_report(self.repo,'test',start,end)
        with self.assertRaises(DataValidationError):self.summary('2025-12-31T00:00:00Z')
    def test_stale_mark_boundary_and_carried_forward(self):
        self.fill()
        self.assertTrue(self.summary('2026-01-05T10:06:00Z')['fresh_mark_coverage'])
        stale=self.summary('2026-01-05T10:06:00.000001Z')
        self.assertFalse(stale['fresh_mark_coverage'])
        self.assertIn('valuation_carried_forward',stale['warning_codes'])
    def test_multi_symbol_and_account_isolation(self):
        self.fill();self.fill('m',symbol='MSFT',quantity='2',price='200',at=T2)
        ledger.create_account(self.repo,'second','500',T0)
        self.assertEqual(set(self.summary()['positions']),{'AAPL','MSFT'})
        self.assertEqual(ledger.account_summary(self.repo,'second')['cash'],'500.00000000')
    def test_insert_failure_rolls_back(self):
        before=self.summary()
        self.repo.connection.execute("CREATE TRIGGER fail_paper BEFORE INSERT ON paper_ledger BEGIN SELECT RAISE(ABORT,'controlled insert failure'); END")
        self.repo.connection.commit()
        with self.assertRaises(Exception):self.fill()
        self.assertEqual(self.summary(),before)
    def test_invariant_failure_before_commit_rolls_back(self):
        before=self.summary()
        with patch.object(ledger,'valuation',side_effect=StorageError('controlled invariant failure')):
            with self.assertRaises(StorageError):self.fill()
        self.assertEqual(self.summary(),before)
    def test_configuration_tamper_rejected(self):
        self.repo.connection.execute("UPDATE paper_accounts SET config_json='{}'");self.repo.connection.commit()
        with self.assertRaises(StorageError):self.summary()
    def test_request_hash_chain_and_semantic_state_tamper(self):
        self.fill()
        row=self.repo.connection.execute('SELECT request_json,state_json,previous_sha256 FROM paper_ledger').fetchone()
        self.repo.connection.commit();request=json.loads(row[0]);state=json.loads(row[1]);state['cash']='9999.00000000'
        sha=ledger.event_digest('test',0,row[2],request,state)
        self.repo.connection.execute('UPDATE paper_ledger SET state_json=?,event_sha256=?',(ledger.encode(state),sha));self.repo.connection.commit()
        with self.assertRaises(StorageError):self.summary()
    def test_parent_hash_tamper(self):
        self.fill();self.repo.connection.execute("UPDATE paper_ledger SET previous_sha256='foreign'");self.repo.connection.commit()
        with self.assertRaises(StorageError):self.summary()
    def test_event_limit_does_not_partially_insert(self):
        self.fill()
        with patch.object(ledger,'MAX_EVENTS',1):
            with self.assertRaises(StorageError):self.fill('b2',at=T2)
        self.assertEqual(self.summary()['ledger_events'],1)
    def test_byte_limit_does_not_insert(self):
        with patch.object(ledger,'MAX_BYTES',10):
            with self.assertRaises(StorageError):self.fill()
        self.assertEqual(self.summary()['ledger_events'],0)
    def test_tiny_fill_below_accounting_precision(self):
        with self.assertRaises(DataValidationError):self.fill(quantity='0.000001',price='0.000001')
    def test_sell_fees_cannot_make_cash_negative(self):
        ledger.create_account(self.repo,'fees','2',T0,slippage_bps='0',max_position_fraction='1')
        ledger.append_event(self.repo,'fees',ledger.fill_request('b','AAPL','buy','1','1',T1))
        with self.assertRaises(DataValidationError):
            ledger.append_event(self.repo,'fees',ledger.fill_request('s','AAPL','sell','1','0.1',T2))
        self.assertEqual(ledger.account_summary(self.repo,'fees')['cash'],'0.00000000')
    def test_zero_equity_return_is_undefined(self):
        ledger.create_account(self.repo,'zero','2',T0,slippage_bps='0',max_position_fraction='1')
        for key,side,at in [('b','buy',T1),('s','sell',T2)]:
            ledger.append_event(self.repo,'zero',ledger.fill_request(key,'AAPL',side,'1','1',at))
        report=ledger.period_report(self.repo,'zero',T2,T3)
        self.assertEqual(report['period_pnl'],'0.00000000')
        self.assertIsNone(report['return_percent'])
    def test_intermediate_event_deletion_is_rejected(self):
        self.fill();self.fill('b2',quantity='1',at=T2)
        self.repo.connection.execute('DELETE FROM paper_ledger WHERE ordinal=0');self.repo.connection.commit()
        with self.assertRaises(StorageError):self.summary()
    def test_mark_not_fee_bearing_and_exit_costs_disclosed(self):
        self.fill()
        marked=ledger.append_event(self.repo,'test',ledger.mark_request('m','AAPL','110',T2))
        self.assertEqual(marked['fees_paid'],'1.00000000')
        self.assertFalse(marked['open_pnl_includes_hypothetical_exit_costs'])
    def test_invalid_numeric_inputs(self):
        for value in ['NaN','Infinity','-1','1e3','0','0.0000001',1.1,True,' 10','1000000001']:
            with self.subTest(value=value),self.assertRaises(DataValidationError):
                ledger.fill_request('x','AAPL','buy','1',value,T1)
    def test_invalid_keys_symbols_and_config(self):
        for key in ['', '../x','a b','x'*65]:
            with self.assertRaises(DataValidationError):ledger.identifier(key)
        with self.assertRaises(DataValidationError):ledger.fill_request('x','UNKNOWN','buy','1','1',T1)
        for opts in [dict(slippage_bps='1001'),dict(max_position_fraction='0'),dict(fee_per_fill='1.001'),dict(max_mark_age_seconds=True)]:
            with self.assertRaises(DataValidationError):ledger.account_config('1000',T0,**opts)
    def test_market_tables_unchanged(self):
        tables=['bars','stream_events','news_articles','news_versions','news_analyses']
        before=[self.repo.connection.execute('SELECT COUNT(*) FROM '+t).fetchone()[0] for t in tables]
        self.repo.connection.commit();self.fill();self.summary();ledger.period_report(self.repo,'test',T0,T2)
        after=[self.repo.connection.execute('SELECT COUNT(*) FROM '+t).fetchone()[0] for t in tables]
        self.assertEqual(before,after)
    def test_clean_transaction_required(self):
        self.repo.connection.execute('BEGIN')
        with self.assertRaises(StorageError):self.fill()
        self.assertTrue(self.repo.connection.in_transaction)
        self.repo.connection.rollback()
    def test_decimal_rounding_identity_many_partial_fills(self):
        ledger.create_account(self.repo,'many','10000',T0,max_position_fraction='1')
        for i in range(13):
            ledger.append_event(self.repo,'many',ledger.fill_request('b'+str(i),'AAPL','buy','0.123456',str(100+i),T1))
        for i in range(12):
            result=ledger.append_event(self.repo,'many',ledger.fill_request('s'+str(i),'AAPL','sell','0.123456',str(90+i),T2))
            self.assertEqual(Decimal(result['total_pnl']),Decimal(result['realized_pnl'])+Decimal(result['unrealized_pnl']))
        final=ledger.append_event(self.repo,'many',ledger.fill_request('close','AAPL','sell','0.123456','105',T3))
        self.assertEqual(final['positions'],{})

class PaperPersistenceTests(unittest.TestCase):
    def test_restart_and_retry(self):
        with tempfile.TemporaryDirectory() as root:
            url='sqlite:///'+str(Path(root)/'paper.db');repo=Repository(url)
            ledger.create_account(repo,'persist','10000',T0)
            request=ledger.fill_request('buy','AAPL','buy','10','100',T1)
            ledger.append_event(repo,'persist',request);before=ledger.account_summary(repo,'persist');repo.close()
            repo=Repository(url)
            try:
                self.assertEqual(ledger.account_summary(repo,'persist'),before)
                self.assertEqual(ledger.append_event(repo,'persist',request)['status'],'duplicate')
                self.assertEqual(ledger.account_summary(repo,'persist')['fills'],1)
            finally:repo.close()
    def test_concurrent_duplicate_serialized(self):
        with tempfile.TemporaryDirectory() as root:
            url='sqlite:///'+str(Path(root)/'paper.db');repo=Repository(url)
            ledger.create_account(repo,'concurrent','10000',T0);repo.close()
            barrier=threading.Barrier(2)
            def worker():
                local=Repository(url)
                try:
                    barrier.wait(timeout=10)
                    return ledger.append_event(local,'concurrent',ledger.fill_request('same','AAPL','buy','10','100',T1))['status']
                finally:local.close()
            with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(lambda _:worker(),range(2)))
            self.assertCountEqual(results,['applied','duplicate'])
            repo=Repository(url)
            try:self.assertEqual(ledger.account_summary(repo,'concurrent')['cash'],'8998.50000000')
            finally:repo.close()
    def test_concurrent_cash_check_recomputed_after_lock(self):
        with tempfile.TemporaryDirectory() as root:
            url='sqlite:///'+str(Path(root)/'paper.db');repo=Repository(url)
            ledger.create_account(repo,'race','100',T0,fee_per_fill='0',slippage_bps='0',max_position_fraction='1');repo.close()
            barrier=threading.Barrier(2)
            def worker(symbol):
                local=Repository(url)
                try:
                    barrier.wait(timeout=10)
                    try:return ledger.append_event(local,'race',ledger.fill_request(symbol,symbol,'buy','1','80',T1))['status']
                    except DataValidationError:return 'rejected'
                finally:local.close()
            with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(worker,['AAPL','MSFT']))
            self.assertCountEqual(results,['applied','rejected'])
            repo=Repository(url)
            try:self.assertEqual(ledger.account_summary(repo,'race')['cash'],'20.00000000')
            finally:repo.close()

class PaperCLITests(unittest.TestCase):
    def call(self,args):
        output=io.StringIO()
        with contextlib.redirect_stdout(output):code=main(args)
        self.assertEqual(code,0)
        return json.loads(output.getvalue())
    def test_verifier_ignores_env(self):
        with patch('microbroker.cli.Settings.from_env',side_effect=AssertionError('No settings for private verifier')):
            self.assertEqual(self.call(['verify-paper-ledger'])['status'],'passed')
    def test_full_cli_scenario_without_credentials(self):
        with tempfile.TemporaryDirectory() as root:
            env=Path(root)/'env';env.write_text('DATABASE_URL=sqlite:///'+str(Path(root)/'db')+'\n')
            prefix=['--env-file',str(env)]
            self.call(prefix+['paper-create','--account','cli','--cash','10000','--start',T0])
            self.call(prefix+['paper-fill','--account','cli','--event-key','b','--symbol','AAPL','--side','buy','--quantity','10','--price','100','--at',T1])
            self.call(prefix+['paper-mark','--account','cli','--event-key','m','--symbol','AAPL','--price','110','--at',T2])
            self.assertEqual(self.call(prefix+['paper-summary','--account','cli'])['total_pnl'],'98.50000000')
            self.assertEqual(self.call(prefix+['paper-report','--account','cli','--start',T0,'--end',T2])['period_pnl'],'98.50000000')

class PaperPostgresProtocolTests(unittest.TestCase):
    """Validate PG branch/query contracts using SQLite, not actual PostgreSQL acceptance."""
    def setUp(self):
        self.repo=Repository('sqlite:///:memory:')
        class ProtocolConnection:
            def __init__(self,base):self.base=base;self.queries=[]
            def __getattr__(self,name):return getattr(self.base,name)
            @property
            def info(self):return SimpleNamespace(transaction_status=2 if self.base.in_transaction else 0)
            def execute(self,query,params=()):
                self.queries.append(query)
                if query=='SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY':query='BEGIN'
                return self.base.execute(query.replace('%s','?').removesuffix(' FOR UPDATE'),params)
        self.repo.connection=ProtocolConnection(self.repo.connection)
        self.repo.postgres=True;self.repo.marker='%s'
    def tearDown(self):self.repo.close()
    def test_pg_queries_lock_account_before_append_and_read_coherently(self):
        ledger.create_account(self.repo,'pg','10000',T0)
        self.repo.connection.queries=[]
        ledger.append_event(self.repo,'pg',ledger.fill_request('b','AAPL','buy','1','100',T1))
        queries=self.repo.connection.queries
        self.assertEqual(queries[0],'BEGIN')
        self.assertIn('FOR UPDATE',queries[1])
        self.assertTrue(queries[2].startswith('SELECT ordinal'))
        self.assertTrue(queries[3].startswith('INSERT INTO paper_ledger'))
        self.repo.connection.queries=[]
        self.assertEqual(ledger.account_summary(self.repo,'pg')['fills'],1)
        self.assertEqual(self.repo.connection.queries[0],'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
        self.assertFalse(self.repo.connection.in_transaction)
    def test_pg_pending_transaction_rejected_before_new_write(self):
        self.repo.connection.execute('BEGIN')
        with self.assertRaises(StorageError):ledger.create_account(self.repo,'pg','10000',T0)
        self.assertEqual(self.repo.connection.queries,['BEGIN'])
    def test_full_paper_checks_reach_pg_branch_and_rollback(self):
        from microbroker.execution.paper_verification import paper_checks
        self.assertEqual(len(paper_checks(self.repo)),22)
        self.assertFalse(self.repo.connection.in_transaction)
