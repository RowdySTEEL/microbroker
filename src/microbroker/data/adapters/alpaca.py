"""Read-only historical bars adapter. Only fixed HTTPS market-data hosts."""
import base64
from dataclasses import dataclass
from datetime import timezone
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
import json
import math
import socket
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, build_opener, HTTPRedirectHandler
from ..models import Bar, DataValidationError, utc_datetime

class MarketDataError(RuntimeError):
    def __init__(self, message, *, code=None, http_status=None):
        super().__init__(message)
        self.code = code
        self.http_status = http_status

@dataclass
class Response:
    status: int
    headers: dict
    body: bytes

class NoRedirect(HTTPRedirectHandler):
    # Never forward API credentials to a redirect target.
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None

def http_get(url, headers, timeout):
    opener = build_opener(NoRedirect())
    try:
        with opener.open(Request(url, headers=headers, method="GET"), timeout=timeout) as response:
            return Response(response.status, dict(response.headers), response.read())
    except HTTPError as exc:
        return Response(exc.code, dict(exc.headers), exc.read())

class AlpacaHistoricalData:
    def __init__(self, settings, transport=http_get, sleep=time.sleep):
        self.settings, self.transport, self.sleep = settings, transport, sleep
        settings.require_credentials()
        self.pages = 0

    def _headers(self):
        if self.settings.auth_mode == "broker":
            token = base64.b64encode(f"{self.settings.api_key}:{self.settings.api_secret}".encode()).decode()
            return {"Authorization": f"Basic {token}", "Accept": "application/json"}
        return {"APCA-API-KEY-ID": self.settings.api_key, "APCA-API-SECRET-KEY": self.settings.api_secret,
                "Accept": "application/json"}

    def _delay(self, attempt, headers):
        value = next((v for k, v in headers.items() if k.lower() == "retry-after"), None)
        if value:
            try:
                delay = float(value)
                if math.isfinite(delay):
                    return min(60, max(0, delay))
            except ValueError:
                try:
                    return min(60, max(0, parsedate_to_datetime(value).timestamp() - time.time()))
                except (TypeError, ValueError, OverflowError):
                    pass
        return min(30, 2 ** attempt)

    def _get(self, params, *, endpoint="/v2/stocks/bars", mapping_field="bars", mapping_type=dict):
        url = self.settings.data_url + endpoint + "?" + urlencode(params)
        for attempt in range(self.settings.max_retries + 1):
            try:
                response = self.transport(url, self._headers(), self.settings.timeout)
            except (URLError, TimeoutError, socket.timeout, ConnectionError, OSError):
                if attempt == self.settings.max_retries:
                    raise MarketDataError("Market data network request failed after retries", code="network_error") from None
                self.sleep(self._delay(attempt, {}))
                continue
            if response.status == 429 or 500 <= response.status <= 599:
                if attempt < self.settings.max_retries:
                    self.sleep(self._delay(attempt, response.headers))
                    continue
            if response.status != 200:
                hints = {400: "Invalid query parameters", 401: "Invalid credentials", 403: "Check credentials, environment and feed permissions",
                         429: "Rate limit exceeded"}
                # Server body and headers can contain secrets; never log or surface them.
                raise MarketDataError(f"Alpaca HTTP {response.status}: {hints.get(response.status, 'Market data request failed')}", code="http_error", http_status=response.status)
            try:
                payload = json.loads(response.body, parse_float=Decimal)
            except (ValueError, UnicodeDecodeError):
                raise MarketDataError("Alpaca returned invalid JSON", code="invalid_json") from None
            if not isinstance(payload, dict) or not isinstance(payload.get(mapping_field), mapping_type):
                raise MarketDataError(f"Alpaca response missing {mapping_field} mapping", code="invalid_response")
            return payload
        raise AssertionError("Unreachable retry state")

    def iter_bars(self, request):
        self.pages = 0
        params = {"symbols": ",".join(request.symbols), "start": request.start.astimezone(timezone.utc).isoformat(),
                  "end": request.end.astimezone(timezone.utc).isoformat(), "timeframe": request.timeframe,
                  "feed": request.feed, "adjustment": request.adjustment, "limit": request.limit,
                  "sort": "asc", "asof": request.asof}
        seen = set()
        while True:
            payload = self._get(params)
            self.pages += 1
            for symbol, rows in payload["bars"].items():
                if symbol not in request.symbols or not isinstance(rows, list):
                    raise MarketDataError("Unexpected symbol or bars structure")
                for row in rows:
                    yield normalize_bar(symbol, row, request)
            token = payload.get("next_page_token")
            if token is None:
                break
            if not isinstance(token, str) or not token or token in seen:
                raise MarketDataError("Invalid or repeated pagination token")
            seen.add(token)
            params["page_token"] = token

def normalize_bar(symbol, row, request):
    try:
        prices = [Decimal(str(row[k])) for k in ("o", "h", "l", "c")]
        o, h, l, c = prices
        if any(not n.is_finite() or n < 0 for n in prices) or l > min(o, c) or h < max(o, c) or l > h:
            raise ValueError
        counts = [row[k] for k in ("v", "n")]
        if any(isinstance(n, bool) or Decimal(str(n)) != Decimal(str(n)).to_integral_value() or Decimal(str(n)) < 0 for n in counts):
            raise ValueError
        volume, count = map(int, counts)
        vw = Decimal(str(row["vw"])) if row.get("vw") is not None else None
        if vw is not None and (not vw.is_finite() or vw < 0):
            raise ValueError
        timestamp = utc_datetime(row["t"])
        # Alpaca may include the end timestamp; keep provider's inclusive interval.
        if not request.start <= timestamp <= request.end:
            raise ValueError
        return Bar(symbol, timestamp, request.timeframe, request.feed, request.adjustment, o, h, l, c, volume, count, vw)
    except (KeyError, TypeError, ValueError, InvalidOperation, OverflowError, DataValidationError):
        raise DataValidationError("Invalid Alpaca OHLCV bar") from None
