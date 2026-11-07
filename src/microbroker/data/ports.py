from collections.abc import Iterable
from typing import Protocol
from .models import Bar

class HistoricalMarketData(Protocol):
    def iter_bars(self, request) -> Iterable[Bar]: ...

class BarRepository(Protocol):
    def collect(self, bars: Iterable[Bar], request) -> dict: ...
