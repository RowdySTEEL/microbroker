"""Deferred REST recovery of missing minute bars; does not invent trades or quotes."""
from datetime import datetime, timedelta, timezone
import json
import logging
from ..adapters.alpaca import AlpacaHistoricalData
from ..models import utc_datetime
from ..request import BarsRequest

log = logging.getLogger('microbroker.recovery')

def recover_stream_gaps(settings, repository, run_id=None, *, adapter_factory=AlpacaHistoricalData, now=None):
    now = now or datetime.now(timezone.utc)
    outcomes = []
    for gap_id, feed, symbols_json, start, end, status in repository.recoverable_gaps(run_id):
        # Include an overlap minute before the last healthy receipt and the reconnection minute.
        start = utc_datetime(start).replace(second=0, microsecond=0)-timedelta(minutes=1)
        end = utc_datetime(end).replace(second=0, microsecond=0)
        # Wait for the last minute to close plus 60 s for initial late-trade updates.
        # Provider corrections can still arrive later; this is not a finality guarantee.
        if now < end+timedelta(minutes=2):
            outcomes.append({"gap_id":gap_id, "status":"pending", "reason":"bar_not_mature"})
            continue
        try:
            request = BarsRequest(tuple(json.loads(symbols_json)), start, end, '1Min', feed, 'raw')
            adapter = adapter_factory(settings)
            result = repository.backfill_gap(gap_id, adapter.iter_bars(request), request)
            outcomes.append({"gap_id":gap_id, "status":"backfilled", **result})
            log.info('Gap recovery finished: fetched=%d inserted=%d', result['received_bars'], result['inserted_bars'])
        except Exception:
            repository.fail_gap(gap_id)
            outcomes.append({"gap_id":gap_id, "status":"failed", "error_code":"recovery_failed"})
            log.warning('Gap recovery failed; retained for explicit retry')
    summary = repository.gap_summary(run_id)
    return {"status":"passed" if not any(summary.get(s,0) for s in ('open','pending','failed')) else 'pending',
            "gaps":summary, "outcomes":outcomes, "scope":"missing_minute_bars_only",
            "trades_quotes_recovered":False}
