"""Transactional ingestion; composite key isolates feed and price adjustments."""
from datetime import datetime, timezone
import json
import re
from copy import deepcopy
from pathlib import Path
import sqlite3
import uuid

class StorageError(RuntimeError):
    pass

class Repository:
    capture_bar_receipts = False
    def __init__(self, database_url, *, schema=None):
        if schema is not None and not re.fullmatch(r"mbtest_[a-f0-9]{32}", schema):
            raise StorageError("Only generated test schemas are supported")
        self.postgres = database_url.startswith("postgresql://")
        if self.postgres:
            try:
                import psycopg
            except ImportError:
                raise StorageError("Install PostgreSQL driver: python3 -m pip install \"psycopg[binary]>=3.2,<4\"") from None
            try:
                self.connection = psycopg.connect(database_url, connect_timeout=10)
                if schema is not None:
                    from psycopg import sql
                    self.connection.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
            except psycopg.Error:
                raise StorageError("PostgreSQL connection failed; check local DATABASE_URL") from None
        elif database_url.startswith("sqlite:///"):
            path = database_url.removeprefix("sqlite:///")
            if path != ":memory:":
                Path(path).parent.mkdir(parents=True, exist_ok=True)
            self.connection = sqlite3.connect(path)
        else:
            raise StorageError("Unsupported database backend")
        self.marker = "%s" if self.postgres else "?"
        try:
            self.migrate()
        except BaseException:
            self.connection.close()
            raise

    def migrate(self):
        price_type = "NUMERIC" if self.postgres else "TEXT"
        try:
            self.connection.execute("""CREATE TABLE IF NOT EXISTS paper_portfolio_snapshots (
                account_ref TEXT NOT NULL, ordinal INTEGER NOT NULL, id TEXT NOT NULL UNIQUE,
                observed_at TEXT NOT NULL, snapshot_json TEXT NOT NULL, previous_sha256 TEXT NOT NULL,
                snapshot_sha256 TEXT NOT NULL, PRIMARY KEY (account_ref,ordinal))""")
            self.connection.execute("""CREATE TABLE IF NOT EXISTS paper_worker_runs (
                id TEXT PRIMARY KEY, account_ref TEXT NOT NULL, created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL, status TEXT NOT NULL, request_json TEXT NOT NULL,
                request_sha256 TEXT NOT NULL, result_json TEXT NOT NULL, result_sha256 TEXT NOT NULL)""")
            self.connection.execute("""CREATE TABLE IF NOT EXISTS paper_worker_leases (
                account_ref TEXT PRIMARY KEY, token TEXT NOT NULL, session_id TEXT NOT NULL,
                expires_at TEXT NOT NULL)""")
            self.connection.execute("""CREATE TABLE IF NOT EXISTS paper_news_decisions (
                id TEXT PRIMARY KEY, account_ref TEXT NOT NULL, article_id BIGINT NOT NULL,
                symbol TEXT NOT NULL, session_id TEXT NOT NULL, intent_id TEXT NOT NULL UNIQUE,
                request_json TEXT NOT NULL, request_sha256 TEXT NOT NULL,
                UNIQUE (account_ref,article_id,symbol))""")
            self.connection.execute("""CREATE TABLE IF NOT EXISTS paper_news_events (
                decision_id TEXT NOT NULL, ordinal BIGINT NOT NULL, observed_at TEXT NOT NULL,
                event_json TEXT NOT NULL, previous_sha256 TEXT NOT NULL, event_sha256 TEXT NOT NULL,
                PRIMARY KEY (decision_id,ordinal))""")
            self.connection.execute("""CREATE TABLE IF NOT EXISTS paper_equity_samples (
                account_ref TEXT NOT NULL, ordinal BIGINT NOT NULL, sample_id TEXT NOT NULL UNIQUE,
                observed_at TEXT NOT NULL, snapshot_json TEXT NOT NULL,
                previous_sha256 TEXT NOT NULL, sample_sha256 TEXT NOT NULL,
                PRIMARY KEY (account_ref,ordinal))""")
            self.connection.execute("""CREATE TABLE IF NOT EXISTS broker_paper_locks (
                account_ref TEXT PRIMARY KEY)""")
            self.connection.execute("""CREATE TABLE IF NOT EXISTS broker_paper_intents (
                id TEXT PRIMARY KEY, account_ref TEXT NOT NULL, created_at TEXT NOT NULL,
                intent_json TEXT NOT NULL, intent_sha256 TEXT NOT NULL)""")
            self.connection.execute("""CREATE TABLE IF NOT EXISTS broker_paper_events (
                intent_id TEXT NOT NULL, ordinal BIGINT NOT NULL, observed_at TEXT NOT NULL,
                event_json TEXT NOT NULL, previous_sha256 TEXT NOT NULL, event_sha256 TEXT NOT NULL,
                PRIMARY KEY (intent_id,ordinal))""")
            self.connection.execute("""CREATE TABLE IF NOT EXISTS strategy_simulations (
                id TEXT PRIMARY KEY, account_id TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL,
                request_json TEXT NOT NULL, request_sha256 TEXT NOT NULL,
                result_json TEXT NOT NULL, result_sha256 TEXT NOT NULL,
                ledger_sha256 TEXT NOT NULL)""")
            self.connection.execute("""CREATE TABLE IF NOT EXISTS paper_accounts (
                id TEXT PRIMARY KEY, created_at TEXT NOT NULL,
                config_json TEXT NOT NULL, config_sha256 TEXT NOT NULL)""")
            self.connection.execute("""CREATE TABLE IF NOT EXISTS paper_ledger (
                account_id TEXT NOT NULL, ordinal BIGINT NOT NULL, event_key TEXT NOT NULL,
                event_at TEXT NOT NULL, request_json TEXT NOT NULL, request_sha256 TEXT NOT NULL,
                previous_sha256 TEXT NOT NULL, state_json TEXT NOT NULL, event_sha256 TEXT NOT NULL,
                PRIMARY KEY (account_id,ordinal), UNIQUE (account_id,event_key))""")
            self.connection.execute("""CREATE TABLE IF NOT EXISTS stream_bar_receipts (
                run_id TEXT NOT NULL, ordinal BIGINT NOT NULL, provider TEXT NOT NULL,
                feed TEXT NOT NULL, payload_json TEXT NOT NULL, received_at TEXT NOT NULL,
                PRIMARY KEY (run_id,ordinal))""")
            self.connection.execute(f"""CREATE TABLE IF NOT EXISTS bars (
                provider TEXT NOT NULL, symbol TEXT NOT NULL, timestamp TEXT NOT NULL,
                timeframe TEXT NOT NULL, feed TEXT NOT NULL, adjustment TEXT NOT NULL,
                open {price_type} NOT NULL, high {price_type} NOT NULL, low {price_type} NOT NULL,
                close {price_type} NOT NULL, volume BIGINT NOT NULL, trade_count BIGINT NOT NULL, vwap {price_type},
                PRIMARY KEY (provider,symbol,timestamp,timeframe,feed,adjustment))""")
            self.connection.execute("""CREATE TABLE IF NOT EXISTS collection_runs (
                id TEXT PRIMARY KEY, started_at TEXT NOT NULL, finished_at TEXT NOT NULL,
                status TEXT NOT NULL, request_json TEXT NOT NULL, result_json TEXT NOT NULL)""")
            self.connection.execute("""CREATE TABLE IF NOT EXISTS stream_events (
                provider TEXT NOT NULL, feed TEXT NOT NULL, event_key TEXT NOT NULL,
                symbol TEXT NOT NULL, event_type TEXT NOT NULL, timestamp TEXT NOT NULL,
                payload_json TEXT NOT NULL, received_at TEXT NOT NULL,
                PRIMARY KEY (provider,feed,event_key))""")
            self.connection.execute("""CREATE TABLE IF NOT EXISTS stream_sessions (
                id TEXT PRIMARY KEY, started_at TEXT NOT NULL, finished_at TEXT,
                status TEXT NOT NULL, request_json TEXT NOT NULL, stats_json TEXT NOT NULL, error_code TEXT)""")
            self.connection.execute("""CREATE TABLE IF NOT EXISTS stream_bar_state (
                provider TEXT NOT NULL, symbol TEXT NOT NULL, timestamp TEXT NOT NULL,
                timeframe TEXT NOT NULL, feed TEXT NOT NULL, adjustment TEXT NOT NULL,
                event_type TEXT NOT NULL, PRIMARY KEY (provider,symbol,timestamp,timeframe,feed,adjustment))""")
            self.connection.execute("""CREATE TABLE IF NOT EXISTS stream_gaps (
                id TEXT PRIMARY KEY, run_id TEXT NOT NULL, feed TEXT NOT NULL,
                symbols_json TEXT NOT NULL, start_at TEXT NOT NULL, end_at TEXT,
                status TEXT NOT NULL, attempts BIGINT NOT NULL, result_json TEXT NOT NULL)""")
            self.connection.execute("""CREATE TABLE IF NOT EXISTS replay_runs (
                id TEXT PRIMARY KEY, created_at TEXT NOT NULL, request_json TEXT NOT NULL,
                item_count BIGINT NOT NULL, dataset_sha256 TEXT NOT NULL, report_json TEXT NOT NULL)""")
            self.connection.execute("""CREATE TABLE IF NOT EXISTS replay_items (
                replay_id TEXT NOT NULL, ordinal BIGINT NOT NULL, record_json TEXT NOT NULL,
                PRIMARY KEY (replay_id,ordinal))""")
            self.connection.execute("""CREATE TABLE IF NOT EXISTS news_articles (
                provider TEXT NOT NULL, article_id BIGINT NOT NULL, revision_hash TEXT NOT NULL,
                updated_at TEXT NOT NULL, first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL,
                fingerprint TEXT NOT NULL, has_content BIGINT NOT NULL,
                PRIMARY KEY (provider,article_id))""")
            self.connection.execute("""CREATE TABLE IF NOT EXISTS news_versions (
                provider TEXT NOT NULL, article_id BIGINT NOT NULL, revision_hash TEXT NOT NULL,
                created_at TEXT NOT NULL, updated_at TEXT NOT NULL, first_seen_at TEXT NOT NULL,
                data_json TEXT NOT NULL, PRIMARY KEY (provider,article_id,revision_hash))""")
            self.connection.execute("""CREATE TABLE IF NOT EXISTS news_symbols (
                provider TEXT NOT NULL, article_id BIGINT NOT NULL, revision_hash TEXT NOT NULL,
                symbol TEXT NOT NULL, method TEXT NOT NULL,
                PRIMARY KEY (provider,article_id,revision_hash,symbol))""")
            self.connection.execute("""CREATE TABLE IF NOT EXISTS news_collection_runs (
                id TEXT PRIMARY KEY, started_at TEXT NOT NULL, finished_at TEXT NOT NULL,
                status TEXT NOT NULL, request_json TEXT NOT NULL, result_json TEXT NOT NULL)""")
            self.connection.execute('CREATE INDEX IF NOT EXISTS news_symbol_lookup ON news_symbols (provider,symbol,article_id,revision_hash)')
            self.connection.execute("""CREATE TABLE IF NOT EXISTS news_analyses (
                provider TEXT NOT NULL, article_id BIGINT NOT NULL, revision_hash TEXT NOT NULL,
                analyzer_key TEXT NOT NULL, engine TEXT NOT NULL, input_sha256 TEXT NOT NULL,
                analyzed_at TEXT NOT NULL, result_json TEXT NOT NULL, result_sha256 TEXT NOT NULL,
                PRIMARY KEY (provider,article_id,revision_hash,analyzer_key))""")
            self.connection.execute("""CREATE TABLE IF NOT EXISTS news_analysis_runs (
                id TEXT PRIMARY KEY, started_at TEXT NOT NULL, finished_at TEXT NOT NULL,
                status TEXT NOT NULL, provider TEXT NOT NULL, engine TEXT NOT NULL,
                analyzer_key TEXT NOT NULL, request_json TEXT NOT NULL, result_json TEXT NOT NULL)""")
            self.connection.execute("""CREATE TABLE IF NOT EXISTS research_sessions (
                id TEXT PRIMARY KEY, started_at TEXT NOT NULL, finished_at TEXT,
                status TEXT NOT NULL, request_json TEXT NOT NULL, result_json TEXT NOT NULL)""")
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

    def close(self):
        self.connection.close()

    def begin_gap(self, run_id, request, start):
        gap_id = str(uuid.uuid4())
        self.connection.execute(f"INSERT INTO stream_gaps VALUES ({','.join([self.marker]*9)})",
            (gap_id, run_id, request.feed, json.dumps(request.symbols), start.isoformat(),
             None, "open", 0, "{}"))
        self.connection.commit()
        return gap_id

    def close_gap(self, gap_id, end):
        m = self.marker
        self.connection.execute(f"UPDATE stream_gaps SET end_at={m},status='pending' WHERE id={m} AND status='open'",
                                (end.isoformat(), gap_id))
        self.connection.commit()

    def recoverable_gaps(self, run_id=None):
        query = "SELECT id,feed,symbols_json,start_at,end_at,status FROM stream_gaps WHERE status IN ('pending','failed')"
        params = ()
        if run_id is not None:
            query += f" AND run_id={self.marker}"
            params = (run_id,)
        return self.connection.execute(query+" ORDER BY start_at LIMIT 100", params).fetchall()

    def gap_summary(self, run_id=None):
        query = "SELECT status,COUNT(*) FROM stream_gaps"
        params = ()
        if run_id is not None:
            query += f" WHERE run_id={self.marker}"
            params = (run_id,)
        return dict(self.connection.execute(query+" GROUP BY status", params).fetchall())

    def backfill_gap(self, gap_id, bars, request):
        """Insert missing bars and success audit atomically; never overwrite live corrections."""
        received = inserted = 0
        counts = {s: 0 for s in request.symbols}
        m = self.marker
        try:
            for b in bars:
                if (b.provider != 'alpaca' or b.feed != request.feed or b.timeframe != '1Min'
                    or b.adjustment != 'raw' or b.symbol not in request.symbols
                    or not request.start <= b.timestamp <= request.end
                    or b.timestamp.second or b.timestamp.microsecond):
                    raise StorageError("Invalid recovery bar")
                received += 1
                counts[b.symbol] += 1
                key = (b.provider,b.symbol,b.timestamp.astimezone(timezone.utc).isoformat(),b.timeframe,b.feed,b.adjustment)
                inserted += self.connection.execute(f"INSERT INTO bars VALUES ({','.join([m]*13)}) ON CONFLICT (provider,symbol,timestamp,timeframe,feed,adjustment) DO NOTHING",
                    key+(str(b.open),str(b.high),str(b.low),str(b.close),b.volume,b.trade_count,str(b.vwap) if b.vwap is not None else None)).rowcount
            result = {"received_bars":received, "inserted_bars":inserted, "existing_bars_preserved":received-inserted,
                      "received_by_symbol":counts, "request":request.metadata(),
                      "coverage":"provider_response_only", "trades_quotes_recovered":False}
            cursor = self.connection.execute(f"UPDATE stream_gaps SET status='backfilled',attempts=attempts+1,result_json={m} WHERE id={m} AND status IN ('pending','failed')",
                                            (json.dumps(result),gap_id))
            if cursor.rowcount != 1:
                raise StorageError("Recovery gap is not pending")
            self.connection.commit()
            return result
        except BaseException:
            self.connection.rollback()
            raise

    def fail_gap(self, gap_id):
        m = self.marker
        self.connection.execute(f"UPDATE stream_gaps SET status='failed',attempts=attempts+1,result_json={m} WHERE id={m} AND status IN ('pending','failed')",
            (json.dumps({"error_code":"recovery_failed", "committed_bars":0}),gap_id))
        self.connection.commit()

    def collect(self, bars, request):
        started = datetime.now(timezone.utc).isoformat()
        run_id = str(uuid.uuid4())
        received = 0
        counts = {s: 0 for s in request.symbols}
        columns = "provider,symbol,timestamp,timeframe,feed,adjustment,open,high,low,close,volume,trade_count,vwap"
        sql = f"INSERT INTO bars ({columns}) VALUES ({','.join([self.marker]*13)}) ON CONFLICT (provider,symbol,timestamp,timeframe,feed,adjustment) DO UPDATE SET open=excluded.open,high=excluded.high,low=excluded.low,close=excluded.close,volume=excluded.volume,trade_count=excluded.trade_count,vwap=excluded.vwap"
        try:
            for b in bars:
                self.connection.execute(sql, (b.provider,b.symbol,b.timestamp.astimezone(timezone.utc).isoformat(),b.timeframe,b.feed,b.adjustment,
                                             str(b.open),str(b.high),str(b.low),str(b.close),b.volume,b.trade_count,str(b.vwap) if b.vwap is not None else None))
                received += 1
                counts[b.symbol] += 1
            result = {"run_id": run_id, "status": "completed" if received else "empty", "received_bars": received,
                      "received_by_symbol": counts, "missing_symbols": [s for s,n in counts.items() if not n]}
            self._audit(run_id, started, result["status"], request, result)
            self.connection.commit()
            return result
        except BaseException:
            self.connection.rollback()
            # No exception message/body enters the audit record.
            try:
                self._audit(run_id, started, "failed", request, {"received_before_rollback": received, "committed_bars": 0})
                self.connection.commit()
            except Exception:
                self.connection.rollback()
            raise

    def _audit(self, run_id, started, status, request, result):
        self.connection.execute(f"INSERT INTO collection_runs VALUES ({','.join([self.marker]*6)})",
                                (run_id,started,datetime.now(timezone.utc).isoformat(),status,json.dumps(request.metadata()),json.dumps(result)))

    def count(self):
        return self.connection.execute("SELECT COUNT(*) FROM bars").fetchone()[0]


    def begin_stream(self, request, stats):
        run_id = str(uuid.uuid4())
        self.connection.execute(f"INSERT INTO stream_sessions VALUES ({','.join([self.marker]*7)})",
            (run_id, datetime.now(timezone.utc).isoformat(), None, "running", json.dumps(request.metadata()), json.dumps(stats), None))
        self.connection.commit()
        return run_id

    def update_stream(self, run_id, status, stats, *, error_code=None, finished=False, commit=True):
        m = self.marker
        self.connection.execute(f"UPDATE stream_sessions SET status={m},stats_json={m},error_code={m},finished_at={m} WHERE id={m}",
            (status,json.dumps(stats),error_code,datetime.now(timezone.utc).isoformat() if finished else None,run_id))
        if commit:
            self.connection.commit()

    def apply_stream_frame(self, run_id, events, previous_stats):
        stats = deepcopy(previous_stats)
        m = self.marker
        try:
            for event in events:
                stats["events_received"] += 1
                received_at = datetime.now(timezone.utc).isoformat()
                if self.capture_bar_receipts and event.bar is not None:
                    self.connection.execute(f"INSERT INTO stream_bar_receipts VALUES ({','.join([m]*6)})",
                        (run_id,stats['events_received'],event.provider,event.feed,event.payload_json,received_at))
                    stats['bar_receipts_recorded']=stats.get('bar_receipts_recorded',0)+1
                for key, value in (("received_by_type", event.event_type), ("received_by_symbol",event.symbol)):
                    stats[key][value] = stats[key].get(value,0)+1
                inserted = self.connection.execute(f"INSERT INTO stream_events VALUES ({','.join([m]*8)}) ON CONFLICT (provider,feed,event_key) DO NOTHING",
                    (event.provider,event.feed,event.event_key,event.symbol,event.event_type,event.timestamp,event.payload_json,received_at)).rowcount
                if not inserted:
                    stats["duplicates"] += 1
                    continue
                stats["events_inserted"] += 1
                if event.bar is not None:
                    b = event.bar
                    key = (b.provider,b.symbol,b.timestamp.astimezone(timezone.utc).isoformat(),b.timeframe,b.feed,b.adjustment)
                    state = self.connection.execute(f"SELECT event_type FROM stream_bar_state WHERE provider={m} AND symbol={m} AND timestamp={m} AND timeframe={m} AND feed={m} AND adjustment={m}",key).fetchone()
                    # A replayed base bar must not downgrade a previously received updated bar.
                    if event.event_type == "b" and state and state[0] == "u":
                        continue
                    sql = f"INSERT INTO bars VALUES ({','.join([m]*13)}) ON CONFLICT (provider,symbol,timestamp,timeframe,feed,adjustment) DO UPDATE SET open=excluded.open,high=excluded.high,low=excluded.low,close=excluded.close,volume=excluded.volume,trade_count=excluded.trade_count,vwap=excluded.vwap"
                    self.connection.execute(sql,key+(str(b.open),str(b.high),str(b.low),str(b.close),b.volume,b.trade_count,str(b.vwap) if b.vwap is not None else None))
                    self.connection.execute(f"INSERT INTO stream_bar_state VALUES ({','.join([m]*7)}) ON CONFLICT (provider,symbol,timestamp,timeframe,feed,adjustment) DO UPDATE SET event_type=excluded.event_type",key+(event.event_type,))
                    stats["bars_written"] += 1
            self.update_stream(run_id,"running",stats,commit=False)
            self.connection.commit()
            return stats
        except BaseException:
            self.connection.rollback()
            raise
