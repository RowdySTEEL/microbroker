"""Original controlled scenarios for R21; no market/model accuracy claim."""
from copy import deepcopy
import json
from pathlib import Path
from .risk_controls import risk_policy
from .strategy import run_strategy, strategy_report, simulate, strategy_policy, load_inputs
from .strategy_verification import strategy_fixture
from . import paper_ledger as ledger
from ..data.storage.repository import Repository, StorageError
from ..data.models import DataValidationError


def risk_scenarios(repository):
    analysis, prices = strategy_fixture(repository, article_base=999300)
    records, bars, source = load_inputs(repository, analysis, prices)
    records = [r for r in records if 999300 <= r['news']['article_id'] < 999304]
    config = ledger.account_config('10000', source['start'], max_mark_age_seconds=120,
                                   scope='recorded_receipt_strategy_simulation')
    def evaluate(r=None, b=None, policy=None):
        return simulate(records if r is None else r, bars if b is None else b, source, config,
                        strategy_policy(risk=policy or risk_policy()))[0]
    baseline = run_strategy(repository, 'controlled-r21', analysis, prices, risk=risk_policy())
    loss_bars = deepcopy(bars)
    for bar in loss_bars[2:]:
        bar['close'] = '50'
    loss = evaluate(b=loss_bars)
    gap_bars = [bars[0], bars[3], bars[4]]
    gap = evaluate(r=records[:1], b=gap_bars, policy=risk_policy(max_feed_gap_seconds=60))
    early_bars = deepcopy(bars)
    early_bars[1]['receipt']['received_at'] = '2026-01-05T10:01:59.999999999Z'
    early = evaluate(r=records[:1], b=early_bars)
    return analysis, prices, records, bars, source, config, baseline, loss, gap, early


def risk_checks(repository):
    checks = []
    def require(ok, name):
        if not ok:
            raise StorageError('Risk verification failed: ' + name)
        checks.append(name)
    analysis, prices, records, bars, source, config, baseline, loss, gap, early = risk_scenarios(repository)
    require(baseline['format'] == 'R21' and baseline['valuation']['fills'] == 2,
            'risk_clear_scenario_keeps_buy_and_exit')
    repeated = run_strategy(repository, 'controlled-r21', analysis, prices, risk=risk_policy())
    require(repeated['status'] == 'existing' and repeated['events_sha256'] == baseline['events_sha256'],
            'risk_same_run_no_double_charge')
    require(strategy_report(repository, 'controlled-r21')['risk'] == baseline['risk'],
            'risk_saved_report_recomputes_risk_audit')
    require(loss['risk']['days']['2026-01-05']['loss_halted'], 'risk_unrealized_loss_triggers_daily_halt')
    require(loss['decision_counts'].get('sell') == 1, 'risk_loss_halt_preserves_signal_exit')
    require(gap['reason_counts'].get('risk_price_feed_gap') == 1 and gap['valuation']['fills'] == 0,
            'risk_price_interruption_cancels_pending_entry')
    require(any(e['code'] == 'price_receipt_recovered' for e in gap['risk']['events']),
            'risk_price_recovery_recorded_without_old_signal_reuse')
    require(early['price_counts']['unfinished_bar'] == 1 and early['valuation']['fills'] == 0,
            'risk_negative_receipt_delta_not_tolerated')
    require(early['reason_counts'].get('risk_receipt_time_inconsistent') == 1,
            'risk_inconsistent_receipt_cancels_entry')
    controls = baseline['risk']
    require(not controls['cross_run_daily_limits'] and not controls['broker_risk_enforced'],
            'risk_independent_run_scope_disclosed')
    require(not controls['news_poll_health_checked'] and not controls['clock_synchronization_validated'],
            'risk_unobserved_news_and_clock_health_not_claimed')
    from .risk_controls import RiskControls
    control = RiskControls(risk_policy(max_daily_entries=1), config, source)
    state = ledger.initial_state(config)
    control.tick(state, source['start'], lambda *args: None)
    for symbol in source['symbols']:
        control.price(symbol, source['start'])
    control.days[control.day]['entries'] = 1
    require(control.before_buy(state, source['start']) == 'risk_daily_entry_limit',
            'risk_daily_entry_cap_blocks_next_buy')
    control.tick(state, '2026-01-06T00:00:00.000000000Z', lambda *args: None)
    require(control.days['2026-01-06']['entries'] == 0 and not control.days['2026-01-06']['loss_halted'],
            'risk_new_utc_day_resets_daily_counters')
    source_extended = dict(source, end='2026-01-05T10:08:00.000000000Z')
    extra = deepcopy(records[0]);extra['news']['article_id'] = 999399
    for field in ('created_at','updated_at'):extra['news']['data'][field] = '2026-01-05T10:04:10.000000000Z'
    extra['news']['timestamp'] = extra['analysis']['timestamp'] = '2026-01-05T10:04:10.000000000Z'
    last = deepcopy(bars[-1]);last['timestamp'] = '2026-01-05T10:05:00.000000000Z'
    last['receipt'].update(received_at='2026-01-05T10:06:00.100000000Z', ordinal=90)
    cap = simulate(records[:2]+[extra], bars+[last], source_extended, config,
                   strategy_policy(risk=risk_policy(max_daily_entries=1)))[0]
    require(cap['decision_counts'] == {'buy':1,'sell':1,'wait':1} and
            cap['reason_counts'].get('risk_daily_entry_limit') == 1, 'risk_entry_cap_allows_exit_not_reentry')
    costs = simulate(records[:1], bars, source, config,
                     strategy_policy(risk=risk_policy(daily_loss_fraction='0.0001')))[0]
    require(costs['valuation']['fills'] == 0 and costs['reason_counts'].get('risk_projected_daily_loss') == 1,
            'risk_projected_costs_cannot_cross_daily_floor')
    recovered = deepcopy(bars);recovered[2]['close'] = '50'
    recovery = simulate(records[:1], recovered, source, config, strategy_policy(risk=risk_policy()))[0]
    require(recovery['risk']['days']['2026-01-05']['loss_halted'] and
            float(recovery['risk']['days']['2026-01-05']['last_observed_equity']) > 10000,
            'risk_loss_halt_latched_after_price_recovery')
    during = deepcopy(extra);during['news']['timestamp'] = during['analysis']['timestamp'] = '2026-01-05T10:03:00.200000000Z'
    during['news']['data']['created_at'] = during['news']['timestamp']
    outage = simulate([during], [bars[0],bars[3],bars[4]], source, config,
                      strategy_policy(risk=risk_policy(max_feed_gap_seconds=60)))[0]
    require(outage['valuation']['fills'] == 0 and outage['reason_counts'].get('risk_price_feed_gap') == 1,
            'risk_signal_during_outage_not_reused_after_recovery')
    # A result-only edit must fail even when the stored digest is not changed.
    m = repository.marker
    saved = repository.connection.execute(f'SELECT result_json FROM strategy_simulations WHERE id={m}', ('controlled-r21',)).fetchone()[0]
    repository.connection.commit()
    repository.connection.execute(f'UPDATE strategy_simulations SET result_json={m} WHERE id={m}', ('{}', 'controlled-r21'))
    repository.connection.commit()
    rejected = False
    try:
        strategy_report(repository, 'controlled-r21')
    except StorageError:
        rejected = True
    finally:
        repository.connection.execute(f'UPDATE strategy_simulations SET result_json={m} WHERE id={m}', (saved, 'controlled-r21'))
        repository.connection.commit()
    require(rejected, 'risk_audit_tamper_rejected')
    return checks


def verify_risk_controls():
    repository = Repository('sqlite:///:memory:')
    try:
        return {'status': 'passed', 'checks': risk_checks(repository),
                'fixture_source': 'original_synthetic_loss_cap_and_receipt_interruption_scenarios',
                'user_database_modified': False, 'network_required': False, 'model_required': False,
                'broker_connected': False, 'orders_sent': 0, 'profitability_validated': False}
    finally:
        repository.close()


def demo_risk_controls(output):
    folder = Path(output)
    if folder.exists():
        raise DataValidationError('Demo output already exists; choose a new folder')
    folder.mkdir(parents=True)
    repository = Repository('sqlite:///' + str((folder / 'scenario.sqlite3').resolve()))
    try:
        *_, baseline, loss, gap, early = risk_scenarios(repository)
        result = {'status': 'completed', 'format': 'R21', 'baseline': baseline,
                  'daily_loss': loss, 'price_gap': gap, 'early_receipt': early,
                  'scope': 'original_synthetic_scenarios_not_live_profit',
                  'scenario_storage': 'baseline_sealed_in_sqlite_other_cases_pure_simulation',
                  'user_database_modified': False, 'broker_connected': False, 'orders_sent': 0}
        (folder / 'summary.json').write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
        return {k: result[k] for k in ('status', 'format', 'scope', 'user_database_modified', 'orders_sent')} | {
            'output': str(folder), 'baseline_fills': baseline['valuation']['fills'],
            'daily_loss_halted': loss['risk']['days']['2026-01-05']['loss_halted'],
            'price_gap_fills': gap['valuation']['fills'], 'early_receipt_fills': early['valuation']['fills']}
    finally:
        repository.close()
