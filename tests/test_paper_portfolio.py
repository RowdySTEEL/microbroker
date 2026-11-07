"""Financial invariants, bounded frozen observations and GET-only acceptance."""
import contextlib
from copy import deepcopy
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from microbroker.cli import main
from microbroker.config import Settings
from microbroker.data.models import DataValidationError
from microbroker.data.storage.repository import Repository,StorageError
from microbroker.execution import paper_broker as broker
from microbroker.execution.paper_api import valued_positions,PaperBrokerError
from microbroker.execution.paper_portfolio import capture_portfolio,portfolio_report,read_history,calculate,render_portfolio
from microbroker.execution.portfolio_verification import seeded,ValuedFakePaper,verify_portfolio,portfolio_checks


class PortfolioTests(unittest.TestCase):
    def setUp(self):self.repo=Repository('sqlite:///:memory:')
    def tearDown(self):self.repo.close()
    def sample(self,name='unit',**kw):
        f,entry=seeded(self.repo,name,**kw);r=capture_portfolio(self.repo,f.client,snapshot_id=name)
        return f,entry,portfolio_report(self.repo,snapshot_id=name,now=f.at)
    def inputs(self,name):
        r=self.repo.connection.execute('SELECT snapshot_json FROM paper_portfolio_snapshots WHERE id=?',(name,)).fetchone();self.repo.connection.commit()
        return json.loads(r[0])
    def test_private_financial_scenarios(self):self.assertEqual(len(verify_portfolio()['checks']),18)
    def test_actual_supplied_aapl_roundtrip_subcent_loss_is_preserved(self):
        f,entry,r=self.sample(price='346.36',quantity='0.028842822')
        p=broker.prepare_exit(self.repo,f.client,'unit-exit',entry);broker.submit(self.repo,f.client,'unit-exit')
        f.update(p['request']['client_order_id'],'filled','0.028842822','346.336');capture_portfolio(self.repo,f.client,snapshot_id='closed')
        r=portfolio_report(self.repo,snapshot_id='closed',now=f.at)
        self.assertEqual(r['known_gross_realized_pnl'],'-0.000692227728');self.assertEqual(r['gross_total_pnl'],'-0.000692227728')
        self.assertIsNone(r['net_profit']);self.assertIn('-0.000692227728',render_portfolio(r))
    def test_partial_entry_canceled_fill_is_accounted(self):
        f,entry,r=self.sample(status='canceled',quantity='0.04',price='99')
        self.assertEqual(r['lots'][0]['entry_quantity'],'0.04');self.assertEqual(r['known_gross_unrealized_pnl'],'0.44')
    def test_unfilled_rejected_order_no_phantom_basis(self):
        f,e,r=self.sample(status='rejected',quantity='0')
        self.assertEqual(r['lots'],[]);self.assertEqual(r['managed_trade_cash'],'0')
    def test_multiple_snapshots_do_not_sum_the_same_fill(self):
        f,e,r=self.sample()
        f.advance();capture_portfolio(self.repo,f.client,snapshot_id='next')
        later=portfolio_report(self.repo,snapshot_id='next',now=f.at)
        self.assertEqual(later['managed_trade_cash'],'-10');self.assertEqual(later['known_gross_unrealized_pnl'],'1')
        self.assertEqual(later['cash_comparison']['managed_trade_cash_change'],'0')
    def test_changed_quote_is_frozen_per_snapshot(self):
        f,e,r=self.sample();f.mark='90';f.advance();capture_portfolio(self.repo,f.client,snapshot_id='fall')
        self.assertEqual(portfolio_report(self.repo,snapshot_id='unit',now=f.at)['known_gross_unrealized_pnl'],'1')
        self.assertEqual(portfolio_report(self.repo,snapshot_id='fall',now=f.at)['known_gross_unrealized_pnl'],'-1')
    def test_broker_tax_basis_not_used_as_managed_entry_basis(self):
        f,e,r=self.sample(price='101')
        self.assertEqual(r['per_symbol'][0]['broker_position']['cost_basis'],'10');self.assertEqual(r['lots'][0]['remaining_cost_basis'],'10.1')
        self.assertFalse(r['tax_basis_reconciled'])
    def test_actual_cash_difference_not_assigned_to_fee(self):
        f,e,r=self.sample();f.cash='9999.99';f.advance();capture_portfolio(self.repo,f.client,snapshot_id='cash')
        r=portfolio_report(self.repo,snapshot_id='cash',now=f.at)
        self.assertEqual(r['cash_comparison']['unexplained_cash_residual'],'-0.01');self.assertIsNone(r['fees_paid']);self.assertIsNone(r['net_profit'])
    def test_missing_broker_market_value_withholds_equity_identity(self):
        f,e,r=self.sample();original=f.client.valued_positions
        def missing():
            p=original();p['AAPL']['market_value']=None;return p
        f.client.valued_positions=missing;f.advance();capture_portfolio(self.repo,f.client,snapshot_id='missing')
        self.assertIsNone(portfolio_report(self.repo,snapshot_id='missing',now=f.at)['account_equity_identity_residual'])
    def test_unmanaged_same_symbol_quantity_mismatch_withholds_valuation(self):
        f,e,r=self.sample();original=f.client.valued_positions
        def changed():
            p=original();p['AAPL']['qty']='0.2';return p
        f.client.valued_positions=changed;f.advance();capture_portfolio(self.repo,f.client,snapshot_id='mismatch')
        r=portfolio_report(self.repo,snapshot_id='mismatch',now=f.at)
        self.assertIsNone(r['gross_total_pnl']);self.assertIsNone(r['known_gross_unrealized_pnl']);self.assertEqual(r['status'],'attention_required')
    def test_short_unmanaged_position_is_disclosed(self):
        f,e,r=self.sample();f.extra_positions=[{'symbol':'MSFT','qty':'-1','side':'short'}];f.advance();capture_portfolio(self.repo,f.client,snapshot_id='short')
        r=portfolio_report(self.repo,snapshot_id='short',now=f.at)
        self.assertIsNone(r['gross_total_pnl']);self.assertEqual(r['position_differences'][0]['broker_quantity'],'-1')
    def test_missing_price_does_not_hide_known_realized(self):
        f,e,r=self.sample();p=broker.prepare_exit(self.repo,f.client,'exit',e);broker.submit(self.repo,f.client,'exit')
        f.update(p['request']['client_order_id'],'partially_filled','0.04','120');f.missing_mark=True;capture_portfolio(self.repo,f.client,snapshot_id='missing')
        r=portfolio_report(self.repo,snapshot_id='missing',now=f.at)
        self.assertEqual(r['known_gross_realized_pnl'],'0.8');self.assertIsNone(r['known_gross_unrealized_pnl'])
    def test_explicit_parent_lots_prevent_cross_lot_fifo_guess(self):
        f,e,r=self.sample();v=self.inputs('unit');first=v['sources'][0];second=deepcopy(first)
        second['intent']['id']='other';second['order']['filled_avg_price']='90';v['sources'].append(second)
        exit_source=deepcopy(first);exit_source['intent']['id']='sell';exit_source['intent']['order']['side']='sell';exit_source['intent']['parent_intent_id']='other'
        exit_source['order'].update(side='sell',filled_qty='0.1',filled_avg_price='110');v['sources'].append(exit_source)
        r=calculate(v);self.assertEqual(r['lots'][0]['gross_realized_pnl'],'0');self.assertEqual(r['lots'][1]['gross_realized_pnl'],'2')
    def test_broken_parent_is_rejected(self):
        f,e,r=self.sample();v=self.inputs('unit');s=deepcopy(v['sources'][0]);s['intent']['order']['side']='sell';s['intent']['parent_intent_id']='unknown';v['sources'].append(s)
        with self.assertRaises(StorageError):calculate(v)
    def test_exit_overallocation_is_rejected(self):
        f,e,r=self.sample();v=self.inputs('unit');s=deepcopy(v['sources'][0]);s['intent']['id']='sell';s['intent']['order']['side']='sell';s['intent']['parent_intent_id']=e;s['order'].update(side='sell',filled_qty='0.2');v['sources'].append(s)
        with self.assertRaises(StorageError):calculate(v)
    def test_two_origins_are_separate(self):
        f,e,r=self.sample();v=self.inputs('unit');v['sources'][0]['automatic_news_entry']=True;r=calculate(v)
        self.assertEqual(r['by_origin']['automatic_news']['known_gross_unrealized_pnl'],'1');self.assertEqual(r['by_origin']['manual_paper']['lots'],0)
    def test_real_worker_claim_links_to_automatic_origin(self):
        from microbroker.execution.paper_worker_verification import WorkerFixture
        f=WorkerFixture(self.repo,'r25-origin',50);f.news();f.ready();f.tick();f.fill()
        capture_portfolio(self.repo,f.fake.client,snapshot_id='auto')
        r=portfolio_report(self.repo,snapshot_id='auto',now=f.fake.at)
        self.assertEqual(r['by_origin']['automatic_news']['lots'],1);self.assertEqual(r['by_origin']['manual_paper']['lots'],0)
        self.assertIsNotNone(self.inputs('auto')['sources'][0]['origin_decision_sha256']);f.worker.finish()
    def test_corrupt_news_claim_cannot_silently_classify_origin(self):
        from microbroker.execution.paper_worker_verification import WorkerFixture
        f=WorkerFixture(self.repo,'r25-bad-origin',51);f.news();f.ready();f.tick();f.fill()
        self.repo.connection.execute("UPDATE paper_news_decisions SET request_json='{}'");self.repo.connection.commit()
        with self.assertRaises(StorageError):capture_portfolio(self.repo,f.fake.client,snapshot_id='bad-origin')
        self.assertEqual(self.repo.connection.execute('SELECT COUNT(*) FROM paper_portfolio_snapshots').fetchone()[0],0)
    def test_get_failure_does_not_create_partial_snapshot(self):
        f,e,r=self.sample();f.client.valued_positions=lambda:(_ for _ in ()).throw(PaperBrokerError('transport_failed'))
        with self.assertRaises(PaperBrokerError):capture_portfolio(self.repo,f.client,snapshot_id='failed')
        self.assertEqual(self.repo.connection.execute('SELECT COUNT(*) FROM paper_portfolio_snapshots').fetchone()[0],1)
    def test_insert_failure_rolls_back_and_prior_report_survives(self):
        f,e,r=self.sample();base=self.repo.connection
        class Fail:
            def __getattr__(self,n):return getattr(base,n)
            def execute(self,q,p=()):
                out=base.execute(q,p)
                if q.startswith('INSERT INTO paper_portfolio_snapshots'):raise StorageError('controlled insert failure')
                return out
        self.repo.connection=Fail()
        with self.assertRaises(StorageError):capture_portfolio(self.repo,f.client,snapshot_id='failed')
        self.repo.connection=base
        self.assertEqual(portfolio_report(self.repo,snapshot_id='unit',now=f.at)['gross_total_pnl'],'1')
        self.assertEqual(base.execute('SELECT COUNT(*) FROM paper_portfolio_snapshots').fetchone()[0],1)
    def test_tampered_chain_link_is_rejected(self):
        self.sample();self.repo.connection.execute("UPDATE paper_portfolio_snapshots SET previous_sha256='changed'");self.repo.connection.commit()
        with self.assertRaises(StorageError):portfolio_report(self.repo,snapshot_id='unit')
    def test_tampered_source_fill_is_rejected(self):
        self.sample();v=self.inputs('unit');v['sources'][0]['order']['filled_avg_price']='999'
        self.repo.connection.execute('UPDATE paper_portfolio_snapshots SET snapshot_json=?',(json.dumps(v),));self.repo.connection.commit()
        with self.assertRaises(StorageError):portfolio_report(self.repo,snapshot_id='unit')
    def test_no_snapshot_prompts_capture(self):
        with self.assertRaises(DataValidationError):portfolio_report(self.repo)
    def test_multiple_accounts_require_selection(self):
        f,e,r=self.sample();g,x,t=self.sample('other')
        with self.assertRaises(DataValidationError):portfolio_report(self.repo)
        self.assertEqual(portfolio_report(self.repo,snapshot_id='unit',now=f.at)['account_ref'],r['account_ref'])
    def test_unknown_and_invalid_ids_rejected(self):
        for s in ('../x','', 'x'*65):
            with self.assertRaises(DataValidationError):portfolio_report(self.repo,snapshot_id=s)
        with self.assertRaises(DataValidationError):portfolio_report(self.repo,snapshot_id='absent')
    def test_stale_observation_and_price_freshness_are_different(self):
        f,e,r=self.sample();self.assertFalse(r['saved_observation_stale']);self.assertFalse(r['broker_price_freshness_validated'])
        r=portfolio_report(self.repo,snapshot_id='unit',now='2026-01-05T15:10:00Z');self.assertTrue(r['saved_observation_stale'])
        self.assertIn('Kayıt eski',render_portfolio(r))
    def test_clock_regression_report_and_capture(self):
        f,e,r=self.sample();f.at='2026-01-05T14:59:00Z'
        self.assertTrue(portfolio_report(self.repo,snapshot_id='unit',now=f.at)['saved_observation_stale'])
        with self.assertRaises(StorageError):capture_portfolio(self.repo,f.client,snapshot_id='regressed')
    def test_market_closed_valuation_is_not_execution(self):
        f,e,r=self.sample();f.open=False;f.advance();capture_portfolio(self.repo,f.client,snapshot_id='closed-market')
        self.assertIn('market_closed_recorded_valuation_not_executable_price',portfolio_report(self.repo,snapshot_id='closed-market',now=f.at)['warning_codes'])
    def test_capture_and_retry_only_get_then_report_offline(self):
        f,e=seeded(self.repo,'getonly');before=len(f.calls);capture_portfolio(self.repo,f.client,snapshot_id='getonly')
        self.assertTrue(f.calls[before:]);self.assertTrue(all(c[0]=='GET' for c in f.calls[before:]))
        before=len(f.calls);portfolio_report(self.repo,snapshot_id='getonly',now=f.at);self.assertEqual(len(f.calls),before)
    def test_private_verification_without_credentials(self):
        with patch('microbroker.cli.Settings.from_env',side_effect=AssertionError('No env')),contextlib.redirect_stdout(io.StringIO()):self.assertEqual(main(['verify-paper-portfolio']),0)
    def test_report_cli_offline(self):
        with tempfile.TemporaryDirectory() as d:
            url='sqlite:///'+str(Path(d)/'db');repo=Repository(url);f,e=seeded(repo,'cli');capture_portfolio(repo,f.client,snapshot_id='cli');repo.close()
            with patch('microbroker.cli.Settings.from_env',return_value=Settings(database_url=url)),patch('microbroker.cli.PaperAPI',side_effect=AssertionError('No network')),contextlib.redirect_stdout(io.StringIO()) as out:
                self.assertEqual(main(['paper-portfolio-report','--format','text']),0)
            self.assertIn('Brüt toplam: 1 USD',out.getvalue())
    def test_pg_implicit_transaction_and_lock_protocol(self):
        repo=Repository('sqlite:///:memory:');base=repo.connection
        class Shim:
            @property
            def info(self):return SimpleNamespace(transaction_status=2 if base.in_transaction else 0)
            def __getattr__(self,n):return getattr(base,n)
            def execute(self,q,p=()):
                if q.startswith('SET TRANSACTION'):return base.execute('BEGIN')
                if q.startswith('SELECT') and not base.in_transaction:base.execute('BEGIN')
                return base.execute(q.replace('%s','?').replace(' FOR UPDATE',''),p)
        repo.connection=Shim();repo.postgres=True;repo.marker='%s'
        try:self.assertEqual(len(portfolio_checks(repo)),18)
        finally:repo.close()


class ValuationValidationTests(unittest.TestCase):
    def valid(self,**kw):return {'symbol':'AAPL','qty':'1','side':'long','current_price':'100','avg_entry_price':'90','cost_basis':'90','market_value':'100','unrealized_pl':'10',**kw}
    def test_optional_valuation_missing_is_unknown(self):
        self.assertIsNone(valued_positions([{'symbol':'AAPL','qty':'1','side':'long'}])['AAPL']['current_price'])
    def test_invalid_quantity_side_or_number_rejected(self):
        for kw in ({'qty':'-1'},{'qty':'0'},{'qty':True},{'current_price':'NaN'},{'current_price':'0'},{'market_value':1.2},{'current_price':'1e99'}):
            with self.subTest(kw=kw),self.assertRaises(PaperBrokerError):valued_positions([self.valid(**kw)])
    def test_duplicate_and_bounded_response(self):
        for raw in ({},[self.valid(),self.valid()],[self.valid()]*1001):
            with self.assertRaises(PaperBrokerError):valued_positions(raw)
    def test_unknown_symbol_allowed_for_mismatch_disclosure(self):
        self.assertIn('EXTERNAL',valued_positions([self.valid(symbol='EXTERNAL')]))
    def test_existing_positions_shape_stays_unchanged(self):
        f=ValuedFakePaper();f.extra_positions=[self.valid()]
        self.assertEqual(f.client.positions(),{'AAPL':{'qty':'1','side':'long'}})
