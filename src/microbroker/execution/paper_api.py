"""Fixed-host Alpaca Paper Trading REST adapter. No POST retry or redirects."""
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import json
import re
import uuid
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, build_opener
from ..config import ConfigurationError, UNIVERSE
from ..data.adapters.alpaca import NoRedirect
from ..data.stream_models import canonical_time
from ..intelligence.policy import digest

BASE = 'https://paper-api.alpaca.markets'
MAX_BYTES = 1024 * 1024
TERMINAL = {'filled', 'canceled', 'expired', 'rejected'}
STATUSES = TERMINAL | {'accepted', 'pending_new', 'new', 'partially_filled', 'pending_cancel',
                       'accepted_for_bidding', 'done_for_day', 'stopped', 'suspended', 'calculated'}


class PaperBrokerError(RuntimeError):
    def __init__(self, code, http_status=None):
        self.code, self.http_status = code, http_status
        super().__init__('Paper broker: ' + code + (f' (HTTP {http_status})' if http_status else ''))


@dataclass
class Reply:
    status: int
    body: bytes


def transport(method, url, headers, body, timeout):
    opener = build_opener(NoRedirect())
    try:
        with opener.open(Request(url, data=body, headers=headers, method=method), timeout=timeout) as response:
            return Reply(response.status, response.read(MAX_BYTES + 1))
    except HTTPError as exc:
        return Reply(exc.code, b'')  # Never retain provider messages/echoed credentials.


def decimal_text(value, *, signed=False, positive=False):
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise PaperBrokerError('invalid_numeric_response')
    text = str(value)
    if len(text) > 40 or not re.fullmatch(r'-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?', text):
        raise PaperBrokerError('invalid_numeric_response')
    try:
        number = Decimal(text)
        if not number.is_finite() or abs(number) > Decimal('1000000000000') or (not signed and number < 0) or (positive and number <= 0):
            raise PaperBrokerError('invalid_numeric_response')
    except InvalidOperation:
        raise PaperBrokerError('invalid_numeric_response') from None
    result=format(number,'f')
    return result.rstrip('0').rstrip('.') if '.' in result else result


def uuid_text(value):
    try:
        if not isinstance(value, str):raise ValueError()
        return str(uuid.UUID(value))
    except (ValueError, AttributeError):
        raise PaperBrokerError('invalid_broker_identifier') from None


def account(raw):
    try:
        identity = uuid_text(raw['id'])
        flags = {k: raw[k] for k in ('trading_blocked', 'account_blocked', 'trade_suspended_by_user')}
        if any(type(v) is not bool for v in flags.values()):raise ValueError()
        if raw['currency'] != 'USD' or raw['status'] != 'ACTIVE':raise PaperBrokerError('account_not_active_usd')
        return {'account_ref': digest({'paper_origin': BASE, 'account_id': identity}),
                'currency': 'USD', 'status': 'ACTIVE', **flags,
                **{k: decimal_text(raw[k], signed=True) for k in ('cash', 'equity', 'last_equity')},
                'buying_power': decimal_text(raw['buying_power'])}
    except (KeyError, TypeError, ValueError):
        raise PaperBrokerError('invalid_account_response') from None


def clock(raw):
    try:
        if type(raw['is_open']) is not bool:raise ValueError()
        return {'is_open': raw['is_open'], **{k: canonical_time(raw[k]) for k in ('timestamp', 'next_open', 'next_close')}}
    except (KeyError, TypeError, ValueError):
        raise PaperBrokerError('invalid_clock_response') from None


def order(raw, intent):
    """Match economics and client identity before recording cumulative broker fills."""
    try:
        payload = intent['order']
        if (not isinstance(raw, dict) or any(raw.get(k) != payload[k] for k in ('client_order_id', 'symbol', 'side', 'type', 'time_in_force'))
            or raw.get('extended_hours') is not False or raw.get('asset_class') != 'us_equity'
            or raw.get('order_class') not in ('', 'simple') or raw.get('legs') not in (None, [])):
            raise PaperBrokerError('order_identity_or_economics_mismatch')
        amount = 'notional' if payload['side'] == 'buy' else 'qty'
        if Decimal(decimal_text(raw.get(amount), positive=True)) != Decimal(payload[amount]):
            raise PaperBrokerError('order_amount_mismatch')
        filled = decimal_text(raw['filled_qty'])
        qty = decimal_text(raw['qty']) if raw.get('qty') is not None else None
        if qty is not None and Decimal(filled) > Decimal(qty):raise PaperBrokerError('filled_quantity_exceeds_order')
        if raw['status'] not in STATUSES:raise PaperBrokerError('unsupported_broker_order_status')
        if raw['status'] == 'filled' and (Decimal(filled) == 0 or (qty is not None and Decimal(filled) != Decimal(qty))):
            raise PaperBrokerError('inconsistent_filled_status')
        avg = decimal_text(raw['filled_avg_price'], positive=True) if Decimal(filled) > 0 else None
        return {'id': uuid_text(raw['id']), 'client_order_id': payload['client_order_id'],
                'symbol': payload['symbol'], 'side': payload['side'], 'type': 'market', 'time_in_force': 'day',
                'status': raw['status'], 'filled_qty': filled, 'filled_avg_price': avg,
                'qty': qty, 'notional': decimal_text(raw['notional']) if raw.get('notional') is not None else None,
                'updated_at': canonical_time(raw['updated_at']),
                'asset_class': 'us_equity', 'extended_hours': False, 'order_class': 'simple'}
    except (KeyError, TypeError, ValueError, InvalidOperation):
        raise PaperBrokerError('invalid_order_response') from None


class PaperAPI:
    def __init__(self, settings, send=transport, now=None):
        settings.require_credentials()
        if settings.auth_mode != 'trading' or settings.data_env != 'production':
            raise ConfigurationError('Paper Trading requires trading API keys and production market data settings')
        self.settings, self.send = settings, send
        self.now = now or (lambda: canonical_time(datetime.now(timezone.utc).isoformat()))

    def call(self, method, path, payload=None, missing=False):
        # No user supplied endpoint, redirect, cookies, POST retries or alternate live host.
        allowed = ((method == 'GET' and (path in {'/v2/account', '/v2/clock', '/v2/positions', '/v2/orders?status=open&limit=500'}
                    or re.fullmatch(r'/v2/assets/(?:'+'|'.join(UNIVERSE)+r')', path)
                    or re.fullmatch(r'/v2/orders:by_client_order_id\?client_order_id=mb22-[0-9a-f]{40}', path)))
                   or (method == 'POST' and path == '/v2/orders')
                   or (method == 'DELETE' and re.fullmatch(r'/v2/orders/[0-9a-f-]{36}', path)))
        if not allowed:raise PaperBrokerError('unsupported_paper_route')
        headers = {'APCA-API-KEY-ID': self.settings.api_key, 'APCA-API-SECRET-KEY': self.settings.api_secret,
                   'Accept': 'application/json', 'Content-Type': 'application/json'}
        body = json.dumps(payload).encode('ascii') if payload is not None else None
        try:
            reply = self.send(method, BASE + path, headers, body, self.settings.timeout)
        except Exception:
            raise PaperBrokerError('transport_failed') from None
        if type(reply.status) is not int or not 100<=reply.status<=599:raise PaperBrokerError('invalid_http_status')
        if reply.status == 404 and missing:return None
        if not 200 <= reply.status < 300:raise PaperBrokerError('http_error', reply.status)
        if method == 'DELETE':return {'acknowledged': True}
        if not isinstance(reply.body, bytes) or len(reply.body) > MAX_BYTES:raise PaperBrokerError('response_size_or_type')
        try:
            return json.loads(reply.body.decode('utf-8'), parse_float=Decimal,
                              parse_constant=lambda value: (_ for _ in ()).throw(ValueError()))
        except (ValueError, UnicodeError, RecursionError):
            raise PaperBrokerError('invalid_json_response') from None

    def account(self):return account(self.call('GET', '/v2/account'))
    def clock(self):return clock(self.call('GET', '/v2/clock'))
    def lookup(self, client_id):return self.call('GET', '/v2/orders:by_client_order_id?' + urlencode({'client_order_id': client_id}), missing=True)
    def submit(self, payload):return self.call('POST', '/v2/orders', payload)
    def cancel(self, order_id):return self.call('DELETE', '/v2/orders/' + uuid_text(order_id))
    def asset(self, symbol):return self.call('GET', '/v2/assets/' + symbol)
    def positions(self):
        raw = self.call('GET', '/v2/positions')
        if not isinstance(raw, list) or len(raw) > 1000:raise PaperBrokerError('invalid_positions_response')
        result = {}
        for p in raw:
            try:
                symbol = p['symbol']
                if not isinstance(symbol, str) or len(symbol)>32 or symbol in result:raise ValueError()
                result[symbol] = {'qty': decimal_text(p['qty'], signed=True), 'side': p['side']}
                if p['side'] not in {'long', 'short'}:raise ValueError()
            except (KeyError, TypeError, ValueError):raise PaperBrokerError('invalid_position_response') from None
        return result
    def valued_positions(self):
        """Separate valuation view; existing risk/reconciliation position contract is unchanged."""
        return valued_positions(self.call('GET', '/v2/positions'))
    def open_orders(self):
        raw = self.call('GET', '/v2/orders?status=open&limit=500')
        if not isinstance(raw, list) or len(raw) >= 500:raise PaperBrokerError('open_order_list_incomplete')
        if any(not isinstance(o, dict) or not isinstance(o.get('symbol'), str) for o in raw):
            raise PaperBrokerError('invalid_open_orders_response')
        return raw


def check_connection(api):
    a, c = api.account(), api.clock()
    return {'status': 'connected', 'environment': 'alpaca_paper', 'account': a, 'market_clock': c,
            'broker_connected': True, 'orders_sent': 0, 'database_modified': False,
            'live_trading_supported': False, 'profitability_validated': False}


def valued_positions(raw):
    if not isinstance(raw, list) or len(raw)>1000:raise PaperBrokerError('invalid_positions_response')
    result={}
    for p in raw:
        try:
            symbol=p['symbol'];side=p['side'];qty=decimal_text(p['qty'],signed=True)
            if (not isinstance(symbol,str) or not re.fullmatch(r'[A-Za-z0-9./_-]{1,32}',symbol)
                or symbol in result or side not in {'long','short'} or Decimal(qty)==0
                or (side=='long')!=(Decimal(qty)>0)):raise ValueError()
            value={'symbol':symbol,'qty':qty,'side':side}
            for key in ('current_price','avg_entry_price','cost_basis','market_value','unrealized_pl'):
                value[key]=decimal_text(p[key],signed=key in {'cost_basis','market_value','unrealized_pl'},
                                        positive=key in {'current_price','avg_entry_price'}) if p.get(key) is not None else None
            result[symbol]=value
        except (KeyError,TypeError,ValueError):raise PaperBrokerError('invalid_valued_position_response') from None
    return result
