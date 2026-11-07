from copy import deepcopy
import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from microbroker.cli import main
from microbroker.execution import paper_ledger as ledger
from microbroker.execution.risk_controls import RiskControls, risk_policy
from microbroker.execution.risk_verification import verify_risk_controls, demo_risk_controls
from microbroker.execution.strategy import simulate, strategy_policy, run_strategy, strategy_report, load_inputs
from microbroker.execution.strategy_verification import strategy_fixture
from microbroker.data.storage.repository import Repository, StorageError
from microbroker.data.models import DataValidationError
from microbroker.intelligence.policy import digest


class RiskTests(unittest.TestCase):
    def setUp(self):
        self.repo = Repository('sqlite:///:memory:')
        self.analysis, self.prices = strategy_fixture(self.repo)
        self.records, self.bars, self.source = load_inputs(self.repo, self.analysis, self.prices)
        self.config = ledger.account_config('10000', self.source['start'], max_mark_age_seconds=120,
                                           scope='recorded_receipt_strategy_simulation')

    def tearDown(self):
        self.repo.close()

    def evaluate(self, records=None, bars=None, source=None, config=None, **risk):
        return simulate(self.records if records is None else records, self.bars if bars is None else bars,
                        source or self.source, config or self.config, strategy_policy(risk=risk_policy(**risk)))

    def execute(self, run_id='risk-unit', **risk):
        return run_strategy(self.repo, run_id, self.analysis, self.prices, risk=risk_policy(**risk))

    def positive(self, at, article=888001):
        r = deepcopy(self.records[0]);r['news']['article_id'] = article
        r['news']['timestamp'] = at;r['news']['data']['created_at'] = at
        r['news']['data']['updated_at'] = at;r['analysis']['timestamp'] = at
        return r

    def bar(self, minute, close, ordinal=50, symbol='MSFT'):
        b = deepcopy(self.bars[0]);b.update(symbol=symbol, close=str(close), timestamp=f'2026-01-05T10:{minute:02}:00.000000000Z')
        b['receipt'].update(received_at=f'2026-01-05T10:{minute+1:02}:00.100000000Z', ordinal=ordinal)
        return b

    def control(self, **risk):
        c = RiskControls(risk_policy(**risk), self.config, self.source)
        state = ledger.initial_state(self.config)
        c.tick(state, self.source['start'], lambda *args: None)
        c.price('MSFT', self.source['start'])
        return c, state

    def test_private_verifier(self):
        result = verify_risk_controls()
        self.assertEqual(len(result['checks']), 18)
        self.assertFalse(result['user_database_modified'])

    def test_r20_existing_results_are_unchanged_and_readable(self):
        old = run_strategy(self.repo, 'old', self.analysis, self.prices)
        self.assertEqual(old['format'], 'R20');self.assertNotIn('risk', old)
        new = self.execute()
        self.assertEqual(new['valuation'], old['valuation'])
        self.assertEqual(strategy_report(self.repo, 'old')['events_sha256'], old['events_sha256'])
        self.assertEqual(new['account_id'], 'r21-risk-unit')

    def test_daily_entry_cap_blocks_second_buy_but_not_sell(self):
        source = dict(self.source, end='2026-01-05T10:08:00.000000000Z')
        records = self.records[:2] + [self.positive('2026-01-05T10:04:10.000000000Z')]
        bars = self.bars + [self.bar(5, 112)]
        result, _ = self.evaluate(records=records, bars=bars, source=source, max_daily_entries=1)
        self.assertEqual(result['decision_counts'], {'buy': 1, 'sell': 1, 'wait': 1})
        self.assertEqual(result['reason_counts']['risk_daily_entry_limit'], 1)
        self.assertEqual(result['risk']['days']['2026-01-05']['entries'], 1)

    def test_loss_halts_new_buy_and_preserves_sale(self):
        bars = deepcopy(self.bars)
        for b in bars[2:]:b['close'] = '50'
        result, _ = self.evaluate(records=self.records[:2] + [self.positive('2026-01-05T10:04:10.000000000Z')], bars=bars)
        self.assertTrue(result['risk']['days']['2026-01-05']['loss_halted'])
        self.assertEqual(result['decision_counts']['sell'], 1)
        self.assertEqual(result['reason_counts']['risk_daily_loss_limit'], 1)

    def test_loss_latch_survives_price_recovery(self):
        bars = deepcopy(self.bars);bars[2]['close'] = '50'
        result, _ = self.evaluate(records=self.records[:1], bars=bars)
        day = result['risk']['days']['2026-01-05']
        self.assertTrue(day['loss_halted'])
        self.assertGreater(float(day['last_observed_equity']), 10000)
        self.assertEqual(sum(e['code'] == 'daily_loss_halt' for e in result['risk']['events']), 1)

    def test_daily_loss_boundary_inclusive(self):
        c, state = self.control()
        state['cash'] = '9800.00000000';state['realized_pnl'] = '-200.00000000'
        c.observe_loss(state, self.source['start'])
        self.assertTrue(c.days[c.day]['loss_halted'])
        self.assertEqual(c.before_buy(state, self.source['start']), 'risk_daily_loss_limit')

    def test_above_daily_floor_not_halted(self):
        c, state = self.control()
        state['cash'] = '9800.00000001';state['realized_pnl'] = '-199.99999999'
        c.observe_loss(state, self.source['start'])
        self.assertFalse(c.days[c.day]['loss_halted'])

    def test_projected_entry_costs_cannot_cross_loss_floor(self):
        result, _ = self.evaluate(records=self.records[:1], daily_loss_fraction='0.0001')
        self.assertEqual(result['valuation']['fills'], 0)
        self.assertEqual(result['reason_counts']['risk_projected_daily_loss'], 1)
        self.assertFalse(result['risk']['days']['2026-01-05']['loss_halted'])

    def test_gap_cancels_pending_and_requires_new_signal(self):
        result, _ = self.evaluate(records=self.records[:1], bars=[self.bars[0], self.bars[3], self.bars[4]], max_feed_gap_seconds=60)
        self.assertEqual(result['valuation']['fills'], 0)
        self.assertEqual(result['reason_counts']['risk_price_feed_gap'], 1)
        self.assertTrue(any(e['code'] == 'price_receipt_recovered' for e in result['risk']['events']))

    def test_signal_during_gap_cannot_execute_after_recovery(self):
        records = [self.positive('2026-01-05T10:03:00.200000000Z')]
        result, _ = self.evaluate(records=records, bars=[self.bars[0], self.bars[3], self.bars[4]], max_feed_gap_seconds=60)
        self.assertEqual(result['valuation']['fills'], 0)
        self.assertEqual(result['reason_counts']['risk_price_feed_gap'], 1)

    def test_new_signal_after_recovery_can_buy(self):
        source = dict(self.source, end='2026-01-05T10:07:00.000000000Z')
        records = [self.positive('2026-01-05T10:04:10.000000000Z')]
        bars = [self.bars[0], self.bars[3], self.bars[4], self.bar(5, 112)]
        result, _ = self.evaluate(records=records, bars=bars, source=source, max_feed_gap_seconds=60)
        self.assertEqual(result['valuation']['fills'], 1)

    def test_gap_boundary_inclusive_and_one_ns_past(self):
        c, state = self.control(max_feed_gap_seconds=60)
        c.tick(state, '2026-01-05T10:01:00.000000000Z', lambda *args: None)
        self.assertFalse(c.gapped)
        c.tick(state, '2026-01-05T10:01:00.000000001Z', lambda *args: None)
        self.assertEqual(c.gapped, {'MSFT'})

    def test_final_cutoff_detects_silence_without_new_events(self):
        result, _ = self.evaluate(records=[], bars=self.bars[:1], max_feed_gap_seconds=60)
        self.assertEqual(result['risk']['unrecovered_price_symbols'], ['MSFT'])
        self.assertEqual(result['valuation']['fills'], 0)

    def test_early_receipt_cancels_instead_of_shifting_timestamp(self):
        bars = deepcopy(self.bars);bars[1]['receipt']['received_at'] = '2026-01-05T10:01:59.999999999Z'
        result, _ = self.evaluate(records=self.records[:1], bars=bars)
        self.assertEqual(result['price_counts']['unfinished_bar'], 1)
        self.assertEqual(result['reason_counts']['risk_receipt_time_inconsistent'], 1)
        self.assertEqual(result['valuation']['fills'], 0)
        self.assertEqual(bars[1]['receipt']['received_at'], '2026-01-05T10:01:59.999999999Z')

    def test_early_other_symbol_blocks_portfolio_buy_until_recovered(self):
        source = dict(self.source, symbols=['AAPL', 'MSFT'])
        aapl = self.bar(0, 100, symbol='AAPL');aapl['receipt']['received_at'] = '2026-01-05T10:00:59.900000000Z'
        result, _ = self.evaluate(records=self.records[:1], bars=[aapl] + self.bars, source=source)
        self.assertEqual(result['valuation']['fills'], 0)
        self.assertEqual(result['risk']['inconsistent_receipt_symbols'], ['AAPL'])

    def test_all_requested_symbols_need_warmup(self):
        source = dict(self.source, symbols=['AAPL', 'MSFT'])
        result, _ = self.evaluate(records=self.records[:1], source=source)
        self.assertEqual(result['reason_counts']['risk_price_warmup_incomplete'], 1)
        self.assertEqual(result['valuation']['fills'], 0)

    def test_stale_held_position_blocks_purchase(self):
        c, state = self.control()
        state = ledger.apply_request(ledger.account_config('10000', self.source['start']), state,
                                    ledger.fill_request('buy', 'AAPL', 'buy', '1', '100', self.source['start']))
        at = '2026-01-05T10:02:00.000000001Z'
        c.price('MSFT', at)
        self.assertEqual(c.before_buy(state, at), 'risk_stale_position_mark')

    def test_utc_day_resets_loss_and_count_not_prices(self):
        c, state = self.control();c.days[c.day].update(entries=10, loss_halted=True)
        cancellations = []
        c.tick(state, '2026-01-06T00:00:00.000000000Z', lambda *args: cancellations.append(args))
        self.assertFalse(c.days[c.day]['loss_halted']);self.assertEqual(c.days[c.day]['entries'], 0)
        self.assertEqual(c.gapped, {'MSFT'})
        self.assertIn((None, 'risk_day_boundary'), cancellations)

    def test_daily_reference_uses_equity_not_initial_cash_on_next_day(self):
        c, state = self.control();state.update(cash='11000.00000000', realized_pnl='1000.00000000')
        c.tick(state, '2026-01-06T00:00:00.000000000Z', lambda *args: None)
        self.assertEqual(c.days[c.day]['reference_equity'], '11000.00000000')
        state.update(cash='10780.00000000', realized_pnl='780.00000000')
        c.observe_loss(state, '2026-01-06T00:00:01.000000000Z')
        self.assertTrue(c.days[c.day]['loss_halted'])

    def test_same_run_retry_and_report_no_double_fills(self):
        first = self.execute();again = self.execute();report = strategy_report(self.repo, 'risk-unit')
        self.assertEqual(again['status'], 'existing')
        self.assertEqual(first['risk'], report['risk'])
        self.assertEqual(first['events_sha256'], again['events_sha256'])

    def test_changed_risk_settings_same_id_rejected(self):
        self.execute()
        with self.assertRaises(DataValidationError):self.execute(max_daily_entries=1)
        self.assertEqual(strategy_report(self.repo, 'risk-unit')['policy']['risk']['max_daily_entries'], 10)

    def test_mutated_risk_audit_with_recomputed_digest_rejected(self):
        self.execute();m = self.repo.marker
        raw = self.repo.connection.execute('SELECT result_json FROM strategy_simulations').fetchone()[0]
        result = json.loads(raw);result['risk']['days']['2026-01-05']['entries'] = 900
        self.repo.connection.execute(f'UPDATE strategy_simulations SET result_json={m},result_sha256={m}', (json.dumps(result), digest(result)))
        self.repo.connection.commit()
        with self.assertRaises(StorageError):strategy_report(self.repo, 'risk-unit')

    def test_failed_risk_audit_write_rolls_back_account_and_ledger(self):
        base = self.repo.connection
        class Fail:
            def __getattr__(self, name):return getattr(base, name)
            def execute(self, q, p=()):
                answer = base.execute(q, p)
                if q.startswith('INSERT INTO strategy_simulations'):raise StorageError('controlled failure')
                return answer
        self.repo.connection = Fail()
        try:
            with self.assertRaises(StorageError):self.execute()
        finally:self.repo.connection = base
        self.assertEqual(base.execute('SELECT COUNT(*) FROM paper_accounts').fetchone()[0], 0)
        self.assertEqual(base.execute('SELECT COUNT(*) FROM paper_ledger').fetchone()[0], 0)
        self.repo.connection.commit()

    def test_risk_account_cannot_be_manually_modified(self):
        self.execute()
        with self.assertRaises(DataValidationError):
            ledger.append_event(self.repo, 'r21-risk-unit', ledger.mark_request('manual', 'MSFT', '1', self.source['end']))

    def test_invalid_risk_numbers_rejected(self):
        for options in ({'daily_loss_fraction': '0'}, {'daily_loss_fraction': '1.01'}, {'daily_loss_fraction': float('nan')},
                        {'daily_loss_fraction': True}, {'max_daily_entries': False}, {'max_daily_entries': 0},
                        {'max_feed_gap_seconds': 59}, {'max_feed_gap_seconds': 3601}, {'max_daily_entries': 1001}):
            with self.subTest(options=options), self.assertRaises(DataValidationError):risk_policy(**options)

    def test_mutated_risk_policy_rejected(self):
        policy = risk_policy();policy['day_timezone'] = 'Europe/Istanbul'
        with self.assertRaises(DataValidationError):strategy_policy(risk=policy)

    def test_separate_demo_no_environment_or_network(self):
        with tempfile.TemporaryDirectory() as temp, patch('microbroker.config.Settings.from_env', side_effect=AssertionError('no env')):
            result = demo_risk_controls(str(Path(temp) / 'demo'))
            self.assertEqual(result['baseline_fills'], 2);self.assertTrue(result['daily_loss_halted'])
            self.assertEqual(result['price_gap_fills'], 0);self.assertEqual(result['early_receipt_fills'], 0)
            with self.assertRaises(DataValidationError):demo_risk_controls(str(Path(temp) / 'demo'))

    def test_cli_verify_without_env(self):
        with patch('microbroker.config.Settings.from_env', side_effect=AssertionError('no env')), contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(main(['verify-risk-controls']), 0)
        self.assertEqual(json.loads(output.getvalue())['status'], 'passed')

    def test_cli_simulation_enables_r21_by_default(self):
        with patch('microbroker.cli.Repository', return_value=self.repo), contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(main(['run-strategy-simulation', '--run-id', 'cli-risk', '--analysis-replay-id', self.analysis,
                                   '--bars-replay-id', self.prices]), 0)
        result = json.loads(output.getvalue())
        self.assertEqual(result['format'], 'R21');self.assertEqual(result['policy']['risk']['daily_loss_fraction'], '0.02')

    def test_news_before_window_does_not_create_pre_window_daily_risk_state(self):
        records=deepcopy(self.records[:1])
        records[0]['news']['timestamp']='2026-01-04T10:00:00.000000000Z'
        result,_=self.evaluate(records=records)
        self.assertEqual(list(result['risk']['days']), ['2026-01-05'])
        self.assertTrue(all(e['at']>=self.source['start'] for e in result['risk']['events']))

    def test_new_negative_signal_during_price_gap_can_exit_on_fresh_price(self):
        records=deepcopy(self.records[:2])
        records[1]['analysis']['timestamp']='2026-01-05T10:05:00.200000000Z'
        bars=self.bars[:2]+[self.bar(5,110),self.bar(6,111,ordinal=51)]
        source=dict(self.source,end='2026-01-05T10:08:00.000000000Z')
        result,_=self.evaluate(records=records,bars=bars,source=source,max_feed_gap_seconds=180)
        self.assertEqual(result['decision_counts'], {'buy':1,'sell':1})
        self.assertTrue(any(e['code']=='price_feed_gap_observed' for e in result['risk']['events']))

    def test_no_fill_does_not_consume_entry_quota(self):
        result,_=self.evaluate(records=self.records[:1],bars=self.bars[:1],max_daily_entries=1)
        self.assertEqual(result['risk']['days']['2026-01-05']['entries'],0)
        self.assertEqual(result['valuation']['fills'],0)

    def test_independent_runs_cannot_claim_shared_daily_limits(self):
        a=self.execute('risk-a',max_daily_entries=1);b=self.execute('risk-b',max_daily_entries=1)
        self.assertEqual(a['risk']['days']['2026-01-05']['entries'],1)
        self.assertEqual(b['risk']['days']['2026-01-05']['entries'],1)
        self.assertFalse(a['risk']['cross_run_daily_limits'])

    def test_empty_news_is_not_profitability_or_data_outage(self):
        result, _ = self.evaluate(records=[])
        self.assertEqual(result['valuation']['fills'], 0)
        self.assertFalse(result['risk']['news_poll_health_checked'])
        self.assertFalse(result['profitability_validated'])


if __name__ == '__main__':unittest.main()
