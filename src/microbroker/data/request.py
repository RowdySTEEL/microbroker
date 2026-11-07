from dataclasses import dataclass
from datetime import datetime
import re
from .models import DataValidationError, validate_symbols

@dataclass(frozen=True)
class BarsRequest:
    symbols: tuple[str, ...]
    start: datetime
    end: datetime
    timeframe: str = "1Day"
    feed: str = "iex"
    adjustment: str = "raw"
    limit: int = 1000
    asof: str = "-"

    def __post_init__(self):
        object.__setattr__(self, "symbols", validate_symbols(self.symbols))
        if any(d.tzinfo is None or d.utcoffset() is None for d in (self.start, self.end)):
            raise DataValidationError("start and end must include a timezone")
        if self.start >= self.end:
            raise DataValidationError("start must be before end")
        if not re.fullmatch(r"(?:[1-9]|[1-5][0-9])Min|(?:[1-9]|1[0-9]|2[0-3])Hour|1Day", self.timeframe):
            raise DataValidationError("Supported timeframes: 1-59Min, 1-23Hour, 1Day")
        if self.feed not in {"iex", "sip"} or self.adjustment not in {"raw", "split", "dividend", "all"}:
            raise DataValidationError("Unsupported feed or adjustment")
        if not 1 <= self.limit <= 10000:
            raise DataValidationError("Page limit must be 1..10000")
        if self.asof != "-":
            raise DataValidationError("R1 uses asof=- for stable symbol identity")

    def metadata(self):
        return {"symbols": self.symbols, "start": self.start.isoformat(), "end": self.end.isoformat(),
                "timeframe": self.timeframe, "feed": self.feed, "adjustment": self.adjustment,
                "limit": self.limit, "asof": self.asof}
