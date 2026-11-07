"""Deterministic entry gates over recorded data, never broker stop orders."""
from collections import Counter
from decimal import Decimal, localcontext
from . import paper_ledger as ledger
from ..data.alignment import nanos
from ..data.models import DataValidationError
from ..data.storage.repository import StorageError

METHOD = 'r21-entry-risk-gates-v1'
STRATEGY_METHOD = 'r21-risk-guarded-next-complete-minute-v1'


def risk_policy(daily_loss_fraction='0.02', max_daily_entries=10, max_feed_gap_seconds=180):
    loss = ledger.decimal_value(daily_loss_fraction, 'daily loss fraction', maximum='1')
    for name, value, minimum, maximum in (
        ('daily entries', max_daily_entries, 1, 1000),
        ('feed gap', max_feed_gap_seconds, 60, 3600),
    ):
        if type(value) is not int or not minimum <= value <= maximum:
            raise DataValidationError('Invalid risk ' + name)
    return {'method': METHOD, 'daily_loss_fraction': ledger.number_text(loss),
            'max_daily_entries': max_daily_entries, 'max_feed_gap_seconds': max_feed_gap_seconds,
            'day_timezone': 'UTC', 'daily_reference': 'first_observed_equity_in_run_day',
            'loss_includes_recorded_unrealized_and_costs': True,
            'loss_latched_until_next_utc_day': True, 'exits_exempt_from_entry_gates': True,
            'price_recovery_requires_new_signal': True, 'thresholds_calibrated': False}


def validate_policy(value):
    if not isinstance(value, dict):
        raise DataValidationError('Invalid risk policy')
    try:
        expected = risk_policy(value['daily_loss_fraction'], value['max_daily_entries'], value['max_feed_gap_seconds'])
    except KeyError:
        raise DataValidationError('Incomplete risk policy') from None
    if value != expected:
        raise DataValidationError('Risk policy structure mismatch')
    return expected


class RiskControls:
    def __init__(self, policy, config, source):
        self.policy = validate_policy(policy)
        self.config, self.source = config, source
        self.day = None
        self.days, self.events, self.blocked = {}, [], Counter()
        self.last_prices, self.clock_flags, self.gapped = {}, set(), set()

    def event(self, at, code, **details):
        if len(self.events) >= ledger.MAX_EVENTS:
            raise StorageError('Risk audit exceeds 10000 events; narrow the window')
        self.events.append({'at': at, 'code': code, **details})

    def equity(self, state, at):
        return Decimal(ledger.valuation(self.config, state, at)['equity'])

    def observe_loss(self, state, at):
        day = self.days[self.day]
        equity = self.equity(state, at)
        day['last_observed_equity'] = ledger.money_text(equity)
        day['lowest_observed_equity'] = ledger.money_text(min(equity, Decimal(day['lowest_observed_equity'])))
        with localcontext() as ctx:
            ctx.prec = 50
            floor = Decimal(day['reference_equity']) * (1 - Decimal(self.policy['daily_loss_fraction']))
        if equity <= floor and not day['loss_halted']:
            day['loss_halted'] = True
            self.event(at, 'daily_loss_halt', equity=ledger.money_text(equity),
                       reference_equity=day['reference_equity'])

    def tick(self, state, at, cancel):
        day = at[:10]  # All inputs are canonical UTC timestamps.
        if day != self.day:
            if self.day is not None:
                cancel(None, 'risk_day_boundary')
            self.day = day
            equity = ledger.money_text(self.equity(state, at))
            self.days[day] = {'reference_at': at, 'reference_equity': equity,
                              'last_observed_equity': equity, 'lowest_observed_equity': equity,
                              'entries': 0, 'loss_halted': False}
            self.event(at, 'utc_day_reference', equity=equity)
        self.observe_loss(state, at)
        if self.days[day]['loss_halted']:
            cancel(None, 'risk_daily_loss_limit')
        limit = self.policy['max_feed_gap_seconds'] * 1000000000
        for symbol in self.source['symbols']:
            last = self.last_prices.get(symbol, self.source['start'])
            if nanos(at) - nanos(last) > limit and symbol not in self.gapped:
                self.gapped.add(symbol)
                self.event(at, 'price_feed_gap_observed', symbol=symbol, last_eligible_receipt=last)
                # Any watched symbol losing price coverage invalidates pending entries.
                cancel(None, 'risk_price_feed_gap')

    def rejected_early(self, symbol, at, cancel):
        if symbol not in self.clock_flags:
            self.event(at, 'receipt_before_completion_floor', symbol=symbol)
        self.clock_flags.add(symbol)
        cancel(None, 'risk_receipt_time_inconsistent')

    def price(self, symbol, at):
        self.last_prices[symbol] = at
        if symbol in self.gapped or symbol in self.clock_flags:
            self.event(at, 'price_receipt_recovered', symbol=symbol)
        self.gapped.discard(symbol)
        self.clock_flags.discard(symbol)

    def entry_interruption(self):
        if self.days[self.day]['loss_halted']:
            return 'risk_daily_loss_limit'
        if self.days[self.day]['entries'] >= self.policy['max_daily_entries']:
            return 'risk_daily_entry_limit'
        if self.clock_flags:
            return 'risk_receipt_time_inconsistent'
        if self.gapped:
            return 'risk_price_feed_gap'
        return None

    def before_buy(self, state, at, proposed_state=None):
        day = self.days[self.day]
        interrupted=self.entry_interruption()
        if interrupted:return interrupted
        if any(s not in self.last_prices for s in self.source['symbols']):
            return 'risk_price_warmup_incomplete'
        # Existing holdings with a stale mark cannot justify a new purchase.
        if any(nanos(at)-nanos(p['marked_at']) > self.config['max_mark_age_seconds']*1000000000
               for p in state['positions'].values()):
            return 'risk_stale_position_mark'
        if proposed_state is not None:
            with localcontext() as ctx:
                ctx.prec = 50
                floor = Decimal(day['reference_equity']) * (1 - Decimal(self.policy['daily_loss_fraction']))
            if self.equity(proposed_state, at) <= floor:
                return 'risk_projected_daily_loss'
        return None

    def emitted(self, request, state):
        if request['kind'] == 'fill' and request['side'] == 'buy':
            self.days[self.day]['entries'] += 1
        self.observe_loss(state, request['at'])

    def summary(self):
        return {'format': 'R21', 'method': METHOD, 'days': self.days, 'events': self.events,
                'blocked_entry_counts': dict(self.blocked),
                'unrecovered_price_symbols': sorted(self.gapped),
                'inconsistent_receipt_symbols': sorted(self.clock_flags),
                'observation_scope': 'recorded_timeline_events_and_final_cutoff',
                'news_poll_health_checked': False, 'exchange_calendar_checked': False,
                'clock_synchronization_validated': False, 'forced_liquidation': False,
                'cross_run_daily_limits': False, 'broker_risk_enforced': False}
