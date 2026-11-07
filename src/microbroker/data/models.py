from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import re

class DataValidationError(ValueError):
    pass

def utc_datetime(value: str) -> datetime:
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None or dt.utcoffset() is None:
            raise ValueError
        return dt.astimezone(timezone.utc)
    except (TypeError, ValueError, AttributeError):
        raise DataValidationError("Timestamp must be timezone-aware ISO 8601") from None

def validate_symbols(symbols):
    result = tuple(dict.fromkeys(str(s).strip().upper() for s in symbols))
    if not result or any(not re.fullmatch(r"[A-Z][A-Z0-9.-]{0,14}", s) for s in result):
        raise DataValidationError("Provide valid stock symbols")
    return result

@dataclass(frozen=True)
class Bar:
    symbol: str
    timestamp: datetime
    timeframe: str
    feed: str
    adjustment: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    trade_count: int
    vwap: Decimal | None
    provider: str = "alpaca"
