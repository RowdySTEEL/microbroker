"""Whitelisted market events. Preserve trade/quote nanoseconds as UTC text."""
from dataclasses import dataclass, replace
from datetime import timedelta
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
import re
from types import SimpleNamespace
from .models import Bar, DataValidationError, utc_datetime, validate_symbols
from .adapters.alpaca import normalize_bar

CHANNELS = {"b": "bars", "u": "updatedBars", "t": "trades", "q": "quotes", "c": "corrections", "x": "cancelErrors"}

@dataclass(frozen=True)
class StreamRequest:
    symbols: tuple[str, ...]
    feed: str = "iex"
    channels: tuple[str, ...] = ("bars", "updatedBars", "trades", "quotes")
    duration: float = 0
    max_events: int = 0
    max_reconnects: int = 10

    def __post_init__(self):
        object.__setattr__(self, "symbols", validate_symbols(self.symbols))
        object.__setattr__(self, "channels", tuple(dict.fromkeys(self.channels)))
        if self.feed not in {"iex", "sip", "test"}:
            raise DataValidationError("Stream feed must be iex, sip or test")
        if self.feed == "test" and self.symbols != ("FAKEPACA",):
            raise DataValidationError("Test feed requires only FAKEPACA")
        if self.feed != "test" and "FAKEPACA" in self.symbols:
            raise DataValidationError("FAKEPACA requires test feed")
        if not self.channels or any(c not in {"bars", "updatedBars", "trades", "quotes"} for c in self.channels):
            raise DataValidationError("Invalid stream channels")
        if any(isinstance(n, bool) or not isinstance(n, int) for n in (self.max_events, self.max_reconnects)):
            raise DataValidationError("Event/reconnect limits must be integers")
        if not math.isfinite(self.duration) or self.duration < 0 or self.max_events < 0 or not 0 <= self.max_reconnects <= 100:
            raise DataValidationError("Invalid stream duration/event/reconnect limits")

    def metadata(self):
        return {"symbols": self.symbols, "feed": self.feed, "channels": self.channels,
                "duration": self.duration, "max_events": self.max_events, "max_reconnects": self.max_reconnects}

@dataclass(frozen=True)
class MarketEvent:
    provider: str
    feed: str
    symbol: str
    event_type: str
    timestamp: str
    payload_json: str
    event_key: str
    bar: Bar | None = None

def canonical_time(value):
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})", value):
        raise DataValidationError("Invalid stream timestamp")
    dt = utc_datetime(value)
    match = re.search(r"\.(\d+)", value)
    fraction = (match.group(1) if match else "").ljust(9, "0")
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + "." + fraction + "Z"

def decimal_price(value):
    try:
        number = Decimal(str(value))
        if not number.is_finite() or number < 0:
            raise ValueError
        return str(number)
    except (ValueError, InvalidOperation):
        raise DataValidationError("Invalid stream price") from None

def integer(value):
    try:
        n = Decimal(str(value))
        if isinstance(value, bool) or not n.is_finite() or n != n.to_integral_value() or not 0 <= n <= 9223372036854775807:
            raise ValueError
        return int(n)
    except (ValueError, InvalidOperation):
        raise DataValidationError("Invalid stream integer") from None

def normalize_event(row, request):
    try:
        kind, symbol = row["T"], row["S"]
        if kind not in CHANNELS or symbol not in request.symbols:
            raise ValueError
        channel = CHANNELS[kind]
        if channel not in request.channels and not (kind in {"c", "x"} and "trades" in request.channels):
            raise ValueError
        timestamp = canonical_time(row["t"])
        payload = {"T": kind, "S": symbol, "t": timestamp}
        bar = None
        provider = "alpaca-test" if request.feed == "test" else "alpaca"
        if kind in {"b", "u"}:
            dt = utc_datetime(row["t"])
            if dt.second or dt.microsecond or timestamp[20:29] != "000000000":
                raise ValueError
            spec = SimpleNamespace(start=dt-timedelta(seconds=1), end=dt+timedelta(seconds=1), timeframe="1Min", feed=request.feed, adjustment="raw")
            bar = replace(normalize_bar(symbol, row, spec), provider=provider)
            payload.update({k: decimal_price(row[k]) for k in ("o", "h", "l", "c")})
            payload.update({k: integer(row[k]) for k in ("v", "n")})
            payload["vw"] = decimal_price(row["vw"]) if row.get("vw") is not None else None
        else:
            prices = {"t": ("p",), "q": ("bp", "ap"), "c": ("op", "cp"), "x": ("p",)}[kind]
            ints = {"t": ("i", "s"), "q": ("bs", "as"), "c": ("oi", "os", "ci", "cs"), "x": ("i", "s")}[kind]
            payload.update({k: decimal_price(row[k]) for k in prices})
            payload.update({k: integer(row[k]) for k in ints})
            if kind == "x":
                if row["a"] not in {"C", "E"}:
                    raise ValueError
                payload["a"] = row["a"]
            for k in ("x", "bx", "ax", "z"):
                if k in row:
                    if not isinstance(row[k], str) or len(row[k]) > 16:
                        raise ValueError
                    payload[k] = row[k]
            for k in ("c", "oc", "cc"):
                if k in row:
                    if not isinstance(row[k], list) or any(not isinstance(v, str) or len(v) > 16 for v in row[k]):
                        raise ValueError
                    payload[k] = row[k]
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        key = hashlib.sha256(encoded.encode()).hexdigest()
        return MarketEvent(provider, request.feed, symbol, kind, timestamp, encoded, key, bar)
    except (KeyError, TypeError, ValueError, DataValidationError, OverflowError):
        raise DataValidationError("Invalid or unsubscribed stream event") from None
