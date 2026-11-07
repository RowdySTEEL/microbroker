"""Bounded reconnect, explicit auth/subscription acknowledgement, no payload logging."""
import json
import logging
import ssl
import time
from datetime import datetime, timezone
from decimal import Decimal
from .alpaca import MarketDataError
from ..models import DataValidationError
from ..stream_models import normalize_event

log = logging.getLogger("microbroker.stream")

class StreamError(MarketDataError):
    def __init__(self, message, code="protocol", retryable=False):
        super().__init__(message)
        self.code, self.retryable = code, retryable

class StreamDependencyError(StreamError):
    pass

def open_socket(url, timeout):
    try:
        from websockets.sync.client import connect
        from websockets.exceptions import InvalidStatus
    except ImportError:
        raise StreamDependencyError("Install stream dependency: python3 -m pip install 'websockets>=16,<17'", "dependency") from None
    # Library debug messages would expose outgoing auth frames. Use an isolated silent logger.
    private_log = logging.Logger("microbroker.private_transport", level=logging.CRITICAL+1)
    private_log.addHandler(logging.NullHandler())
    try:
        return connect(url, open_timeout=timeout, close_timeout=2, ping_interval=20,
                       ping_timeout=20, max_size=2**20, max_queue=16, logger=private_log,
                       additional_headers={"Content-Type": "application/json"})
    except InvalidStatus as exc:
        code = exc.response.status_code
        raise StreamError(f"WebSocket handshake HTTP {code}", f"http_{code}", code == 429 or code >= 500) from None

def decode_frame(raw):
    try:
        rows = json.loads(raw, parse_float=Decimal)
        if not isinstance(rows, list) or not rows or any(not isinstance(row, dict) for row in rows):
            raise ValueError
        return rows
    except (TypeError, ValueError, UnicodeDecodeError):
        raise StreamError("Invalid stream JSON frame") from None

def check_error(row):
    if row.get("T") == "error":
        code = row.get("code")
        if not isinstance(code, int):
            raise StreamError("Invalid stream error code")
        hints = {400:"Invalid subscription", 401:"Not authenticated", 402:"Invalid credentials",
                 403:"Already authenticated", 404:"Authentication timeout", 405:"Symbol limit exceeded",
                 406:"Connection limit exceeded; close other stream clients", 407:"Slow consumer",
                 409:"Feed permission denied", 410:"Unsupported channel", 500:"Provider internal error"}
        raise StreamError(f"Alpaca stream {code}: {hints.get(code, 'Provider error')}", str(code), code in {404, 500})

class AlpacaStream:
    def __init__(self, settings, connector=open_socket, clock=time.monotonic, sleep=time.sleep,
                 utc_now=lambda: datetime.now(timezone.utc)):
        settings.require_credentials()
        self.settings, self.connector, self.clock, self.sleep = settings, connector, clock, sleep
        self.utc_now = utc_now

    def url(self, request):
        host = "stream.data.sandbox.alpaca.markets" if self.settings.data_env == "sandbox" else "stream.data.alpaca.markets"
        return f"wss://{host}/v2/{request.feed}"

    def run(self, request, repository, on_event=None, recovery=None, stop_event=None):
        stats = {"events_received":0, "events_inserted":0, "duplicates":0, "bars_written":0,
                 "reconnects":0, "connections":0, "authenticated":False, "subscribed":False,
                 "received_by_type":{}, "received_by_symbol":{},
                 "gap_recovery":"not_applicable_test" if request.feed == 'test' else "minute_bars_deferred",
                 "trades_quotes_recovered":False}
        run_id = repository.begin_stream(request, stats)
        started = self.clock()
        deadline = started + request.duration if request.duration else float("inf")
        status, error_code = "completed", None
        reason = "duration"
        last_progress = started
        last_healthy = self.utc_now()
        had_subscription = False
        gap_id = None
        def record_gap():
            nonlocal gap_id
            if had_subscription and gap_id is None and request.feed != 'test':
                gap_id = repository.begin_gap(run_id, request, last_healthy)
        def remaining():
            return 0 if stop_event is not None and stop_event.is_set() else deadline-self.clock()
        def receive(ws, timeout):
            if remaining() <= 0:
                raise StopIteration
            return decode_frame(ws.recv(timeout=min(timeout, remaining())))
        try:
            while remaining() > 0:
                ws = None
                try:
                    log.info("Stream connecting: feed=%s symbols=%d", request.feed, len(request.symbols))
                    ws = self.connector(self.url(request), min(10, remaining()))
                    stats["connections"] += 1
                    stats["authenticated"], stats["subscribed"] = False, False
                    ws.send(json.dumps({"action":"auth", "key":self.settings.api_key, "secret":self.settings.api_secret}))
                    auth_deadline = min(deadline, self.clock()+8)
                    while not stats["authenticated"]:
                        rows = receive(ws, max(0.001, auth_deadline-self.clock()))
                        for row in rows:
                            check_error(row)
                            if row.get("T") == "success" and row.get("msg") == "authenticated":
                                stats["authenticated"] = True
                            elif not (row.get("T") == "success" and row.get("msg") == "connected"):
                                raise StreamError("Unexpected authentication response")
                        if self.clock() >= auth_deadline and not stats["authenticated"]:
                            raise StreamError("Authentication acknowledgement timeout", "auth_timeout", True)
                    ws.send(json.dumps({"action":"subscribe", **{c:list(request.symbols) for c in request.channels}}))
                    sub_deadline = min(deadline, self.clock()+8)
                    while not stats["subscribed"]:
                        for row in receive(ws, max(0.001, sub_deadline-self.clock())):
                            check_error(row)
                            if row.get("T") != "subscription":
                                raise StreamError("Expected subscription acknowledgement")
                            if any(not isinstance(row.get(c), list) or set(row[c]) != set(request.symbols) for c in request.channels):
                                raise StreamError("Subscription acknowledgement mismatch")
                            stats["subscribed"] = True
                        if self.clock() >= sub_deadline and not stats["subscribed"]:
                            raise StreamError("Subscription acknowledgement timeout", "subscription_timeout", True)
                    repository.update_stream(run_id, "running", stats)
                    log.info("Stream authenticated and subscribed")
                    if gap_id is not None:
                        repository.close_gap(gap_id, self.utc_now())
                        gap_id = None
                    had_subscription = True
                    last_healthy = self.utc_now()
                    while remaining() > 0:
                        try:
                            rows = receive(ws, 1)
                        except TimeoutError:
                            rows = []  # Sparse/closed market is not a transport failure; native ping monitors the socket.
                        events = []
                        for row in rows:
                            check_error(row)
                            if row.get("T") in {"success", "subscription"}:
                                raise StreamError("Unexpected control message after subscription")
                            events.append(normalize_event(row, request))
                        if request.max_events:
                            events = events[:request.max_events-stats["events_received"]]
                        if events:
                            result = repository.apply_stream_frame(run_id, events, stats)
                            stats = result
                            last_healthy = self.utc_now()
                            for event in events:
                                if on_event is not None:
                                    on_event(event)
                        if self.clock()-last_progress >= 5:
                            log.info("Stream progress: events=%d bars=%d reconnects=%d", stats["events_received"], stats["bars_written"], stats["reconnects"])
                            last_progress = self.clock()
                        if request.max_events and stats["events_received"] >= request.max_events:
                            reason = "max_events"
                            raise StopIteration
                except StreamError as exc:
                    if not exc.retryable:
                        record_gap()
                        raise
                    record_gap()
                    self._reconnect(request, stats, exc.code, remaining, repository, run_id, stop_event)
                except ssl.SSLCertVerificationError:
                    raise StreamError("TLS certificate verification failed; check Python root certificates", "tls_certificate") from None
                except (OSError, TimeoutError) as exc:
                    record_gap()
                    self._reconnect(request, stats, "network", remaining, repository, run_id, stop_event)
                except Exception as exc:
                    from websockets.exceptions import ConnectionClosed
                    if isinstance(exc, ConnectionClosed):
                        record_gap()
                        self._reconnect(request, stats, "connection_closed", remaining, repository, run_id, stop_event)
                    else:
                        raise
                finally:
                    if ws is not None:
                        try:
                            ws.close()
                        except Exception:
                            pass
        except StopIteration:
            pass
        except KeyboardInterrupt:
            status, reason = "stopped", "keyboard_interrupt"
        except StreamError as exc:
            status, error_code, reason = "failed", exc.code, "error"
            log.error("%s", str(exc))
        except Exception:
            status, error_code, reason = "failed", "processing_or_storage", "error"
            log.error("Stream processing/storage failed; raw exception suppressed")
        if status == "completed" and stats["events_received"] == 0:
            status = "no_data"
        if stop_event is not None and stop_event.is_set() and reason == "duration":
            status, reason = "stopped", "session_stop"
        stats["stop_reason"] = reason
        stats['gaps'] = repository.gap_summary(run_id)
        # Socket is closed before REST recovery, so a slow request cannot block live receiving.
        if recovery is not None and request.feed != 'test':
            stats['recovery'] = recovery(run_id)
            stats['gaps'] = stats['recovery']['gaps']
        repository.update_stream(run_id, status, stats, error_code=error_code, finished=True)
        log.info("Stream finished: status=%s events=%d bars=%d", status, stats["events_received"], stats["bars_written"])
        return {"run_id":run_id, "status":status, "feed":request.feed, "error_code":error_code, **stats}

    def _reconnect(self, request, stats, code, remaining, repository, run_id, stop_event=None):
        if remaining() <= 0:
            raise StopIteration
        if stats["reconnects"] >= request.max_reconnects:
            raise StreamError("Reconnect limit reached", "reconnect_exhausted")
        stats["reconnects"] += 1
        stats["authenticated"], stats["subscribed"] = False, False
        repository.update_stream(run_id, "reconnecting", stats, error_code=code)
        log.warning("Stream interrupted: code=%s reconnect=%d; minute-bar recovery deferred", code, stats["reconnects"])
        delay = min(30, 2**(stats["reconnects"]-1), remaining())
        if stop_event is None:self.sleep(delay)
        else:stop_event.wait(delay)
