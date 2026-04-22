"""
analytics_db.py — Comprehensive analytics database for Power BI reporting.

All tables are written with flat, denormalised columns so Power BI can
query them directly without complex joins.  Foreign keys are kept for
integrity but every table also carries the most-used dimensions
(ticker, sport, engine, session_id) so single-table slices work in PBI.

Tables
------
sessions            — one row per bot run (start → stop)
market_scans        — every fetch_active_sports_markets() call result
scanned_markets     — every individual market seen in a scan
signals             — every signal fired (ENTRY only now; extensible)
orders              — every order attempt (success or failure)
positions           — one row per opened position (filled on manual close)
position_ticks      — every price tick while a position is open
rejected_markets    — markets that failed _is_valid_market() with reason
engine_switches     — every time the user toggles between engines in the GUI
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, List, Tuple

logger = logging.getLogger(__name__)

DB_PATH = Path("kalshi_analytics.db")


# ─────────────────────────────────────────────────────────────────────
# SCHEMA
# ─────────────────────────────────────────────────────────────────────

SCHEMA = """
-- ── Sessions ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sessions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at      REAL    NOT NULL,           -- unix ts
    ended_at        REAL,
    paper_trade     INTEGER NOT NULL DEFAULT 1, -- 0=live 1=paper
    engine          TEXT,                       -- active engine at start
    notes           TEXT
);

-- ── Market Scans ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS market_scans (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      INTEGER NOT NULL REFERENCES sessions(id),
    scanned_at      REAL    NOT NULL,
    total_fetched   INTEGER,                    -- raw markets from API
    sports_found    INTEGER,                    -- after is_sports_market filter
    passed_filter   INTEGER,                    -- after _is_valid_market filter
    elapsed_secs    REAL
);

-- ── Scanned Markets ───────────────────────────────────────────────────
-- Every market seen in every scan — lets you track liquidity over time
CREATE TABLE IF NOT EXISTS scanned_markets (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_id         INTEGER NOT NULL REFERENCES market_scans(id),
    session_id      INTEGER NOT NULL,
    scanned_at      REAL    NOT NULL,
    ticker          TEXT    NOT NULL,
    sport           TEXT,                       -- NBA / NFL / MLB etc
    best_bid        REAL,
    best_ask        REAL,
    spread          REAL,
    last_price      REAL,
    volume          REAL,
    passed_filter   INTEGER NOT NULL DEFAULT 0, -- 1 if selected for trading
    reject_reason   TEXT                        -- NULL if passed
);
CREATE INDEX IF NOT EXISTS idx_sm_ticker    ON scanned_markets(ticker);
CREATE INDEX IF NOT EXISTS idx_sm_scan      ON scanned_markets(scan_id);
CREATE INDEX IF NOT EXISTS idx_sm_sport     ON scanned_markets(sport);

-- ── Rejected Markets (fast write path) ───────────────────────────────
-- Dedicated table for failed filter log — high write volume
CREATE TABLE IF NOT EXISTS rejected_markets (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      INTEGER NOT NULL,
    rejected_at     REAL    NOT NULL,
    ticker          TEXT    NOT NULL,
    sport           TEXT,
    best_bid        REAL,
    best_ask        REAL,
    spread          REAL,
    reject_reason   TEXT    NOT NULL            -- 'settled','spread','no_bid' etc
);
CREATE INDEX IF NOT EXISTS idx_rm_ticker    ON rejected_markets(ticker);
CREATE INDEX IF NOT EXISTS idx_rm_reason    ON rejected_markets(reject_reason);

-- ── Signals ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS signals (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      INTEGER NOT NULL,
    fired_at        REAL    NOT NULL,
    ticker          TEXT    NOT NULL,
    sport           TEXT,
    engine          TEXT    NOT NULL,           -- 'momentum' | 'resolution'
    signal_type     TEXT    NOT NULL,           -- 'ENTRY'
    price           REAL    NOT NULL,
    prev_price      REAL,
    price_move      REAL,                       -- price - prev_price
    best_bid        REAL,
    best_ask        REAL,
    spread          REAL,
    acted_on        INTEGER NOT NULL DEFAULT 0, -- 1 if an order was placed
    order_id        INTEGER REFERENCES orders(id)
);
CREATE INDEX IF NOT EXISTS idx_sig_ticker   ON signals(ticker);
CREATE INDEX IF NOT EXISTS idx_sig_engine   ON signals(engine);
CREATE INDEX IF NOT EXISTS idx_sig_sport    ON signals(sport);

-- ── Orders ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS orders (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      INTEGER NOT NULL,
    signal_id       INTEGER REFERENCES signals(id),
    placed_at       REAL    NOT NULL,
    ticker          TEXT    NOT NULL,
    sport           TEXT,
    engine          TEXT    NOT NULL,
    side            TEXT    NOT NULL,           -- 'BUY' | 'SELL'
    order_type      TEXT    NOT NULL DEFAULT 'limit',
    requested_price REAL    NOT NULL,
    requested_qty   INTEGER NOT NULL,
    filled_price    REAL,
    filled_qty      INTEGER,
    kalshi_order_id TEXT,                       -- ID returned by API
    success         INTEGER NOT NULL DEFAULT 0,
    error_msg       TEXT,
    paper_trade     INTEGER NOT NULL DEFAULT 1,
    attempt_count   INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_ord_ticker   ON orders(ticker);
CREATE INDEX IF NOT EXISTS idx_ord_engine   ON orders(engine);

-- ── Positions ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS positions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      INTEGER NOT NULL,
    opened_at       REAL    NOT NULL,
    closed_at       REAL,
    ticker          TEXT    NOT NULL,
    sport           TEXT,
    engine          TEXT    NOT NULL,
    entry_order_id  INTEGER REFERENCES orders(id),
    exit_order_id   INTEGER REFERENCES orders(id),
    entry_price     REAL    NOT NULL,
    exit_price      REAL,
    qty             INTEGER NOT NULL,
    -- computed on close
    gross_pnl       REAL,                       -- (exit - entry) * qty
    pnl_pct         REAL,                       -- gross_pnl / (entry * qty)
    peak_price      REAL,                       -- highest price seen while open
    peak_pnl        REAL,                       -- (peak - entry) * qty
    ticks_held      INTEGER,
    secs_held       REAL,
    close_reason    TEXT    DEFAULT 'manual',   -- 'manual' | 'expiry' etc
    paper_trade     INTEGER NOT NULL DEFAULT 1,
    notes           TEXT
);
CREATE INDEX IF NOT EXISTS idx_pos_ticker   ON positions(ticker);
CREATE INDEX IF NOT EXISTS idx_pos_engine   ON positions(engine);
CREATE INDEX IF NOT EXISTS idx_pos_sport    ON positions(sport);
CREATE INDEX IF NOT EXISTS idx_pos_opened   ON positions(opened_at);

-- ── Position Ticks ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS position_ticks (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    position_id         INTEGER NOT NULL REFERENCES positions(id),
    session_id          INTEGER NOT NULL,
    ticker              TEXT    NOT NULL,
    sport               TEXT,
    engine              TEXT    NOT NULL,
    ts                  REAL    NOT NULL,
    price               REAL    NOT NULL,
    best_bid            REAL,
    best_ask            REAL,
    spread              REAL,
    move_from_entry     REAL,                   -- price - entry_price
    pct_from_entry      REAL,                   -- move_from_entry / entry_price
    ticks_since_entry   INTEGER,
    secs_since_entry    REAL
);
CREATE INDEX IF NOT EXISTS idx_pt_position  ON position_ticks(position_id);
CREATE INDEX IF NOT EXISTS idx_pt_ticker    ON position_ticks(ticker);
CREATE INDEX IF NOT EXISTS idx_pt_engine    ON position_ticks(engine);

-- ── Engine Switches ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS engine_switches (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      INTEGER NOT NULL,
    switched_at     REAL    NOT NULL,
    from_engine     TEXT    NOT NULL,
    to_engine       TEXT    NOT NULL
);

-- ── Power BI friendly flat views ──────────────────────────────────────
-- These pre-join the most common combinations so PBI can use them directly
-- without needing to understand the schema.

CREATE VIEW IF NOT EXISTS vw_position_summary AS
SELECT
    p.id                                        AS position_id,
    p.ticker,
    p.sport,
    p.engine,
    p.paper_trade,
    datetime(p.opened_at, 'unixepoch')          AS opened_at_utc,
    datetime(p.closed_at, 'unixepoch')          AS closed_at_utc,
    p.entry_price,
    p.exit_price,
    p.qty,
    p.gross_pnl,
    p.pnl_pct,
    p.peak_price,
    p.peak_pnl,
    p.ticks_held,
    p.secs_held,
    ROUND(p.secs_held / 60.0, 1)               AS mins_held,
    p.close_reason,
    -- how much was left on the table vs peak
    CASE WHEN p.peak_price IS NOT NULL AND p.exit_price IS NOT NULL
         THEN (p.peak_price - p.exit_price) * p.qty
         ELSE NULL END                          AS missed_pnl,
    s.started_at                                AS session_start
FROM positions p
JOIN sessions s ON s.id = p.session_id;

CREATE VIEW IF NOT EXISTS vw_tick_profile AS
-- Average price path across all closed positions, per tick offset
-- Use this in PBI to build the "average position journey" line chart
SELECT
    pt.engine,
    pt.sport,
    pt.ticks_since_entry,
    COUNT(*)                                    AS n_observations,
    AVG(pt.move_from_entry)                     AS avg_move,
    AVG(pt.pct_from_entry)                      AS avg_pct_move,
    MAX(pt.move_from_entry)                     AS max_move,
    MIN(pt.move_from_entry)                     AS min_move,
    AVG(pt.spread)                              AS avg_spread
FROM position_ticks pt
JOIN positions p ON p.id = pt.position_id
WHERE p.closed_at IS NOT NULL   -- only completed positions
GROUP BY pt.engine, pt.sport, pt.ticks_since_entry;

CREATE VIEW IF NOT EXISTS vw_signal_funnel AS
-- Conversion funnel: signals fired → orders placed → fills → positions
SELECT
    engine,
    sport,
    DATE(fired_at, 'unixepoch')                 AS trade_date,
    COUNT(*)                                    AS signals_fired,
    SUM(acted_on)                               AS orders_placed,
    SUM(CASE WHEN order_id IS NOT NULL THEN 1 ELSE 0 END) AS fills
FROM signals
GROUP BY engine, sport, trade_date;

CREATE VIEW IF NOT EXISTS vw_daily_pnl AS
SELECT
    DATE(opened_at, 'unixepoch')                AS trade_date,
    engine,
    sport,
    paper_trade,
    COUNT(*)                                    AS trades,
    SUM(gross_pnl)                              AS total_pnl,
    AVG(gross_pnl)                              AS avg_pnl_per_trade,
    SUM(CASE WHEN gross_pnl > 0 THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN gross_pnl <= 0 THEN 1 ELSE 0 END) AS losses
FROM positions
WHERE closed_at IS NOT NULL
GROUP BY trade_date, engine, sport, paper_trade;
"""


# ─────────────────────────────────────────────────────────────────────
# ANALYTICS DB MANAGER
# ─────────────────────────────────────────────────────────────────────

class AnalyticsDB:
    """
    Thread-safe SQLite analytics database.

    Uses WAL mode so Power BI can read while the bot is writing.
    All writes go through a single serialised connection to avoid
    SQLite's write-lock contention from multiple worker threads.
    """

    def __init__(self, path: Path = DB_PATH) -> None:
        self._path = path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._apply_pragmas()
        self._create_schema()
        self.session_id: Optional[int] = None
        logger.info("AnalyticsDB initialised at %s", path.resolve())

    def _apply_pragmas(self) -> None:
        cur = self._conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")       # allow concurrent reads
        cur.execute("PRAGMA synchronous=NORMAL")     # safe + fast
        cur.execute("PRAGMA foreign_keys=ON")
        cur.execute("PRAGMA cache_size=-32000")      # 32 MB cache
        self._conn.commit()

    def _create_schema(self) -> None:
        cur = self._conn.cursor()
        cur.executescript(SCHEMA)
        self._conn.commit()

    @contextmanager
    def _tx(self):
        """Context manager for serialised write transactions."""
        with self._lock:
            cur = self._conn.cursor()
            try:
                yield cur
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    # ── Sessions ──────────────────────────────────────────────────────

    def start_session(self, paper_trade: bool, engine: str) -> int:
        with self._tx() as cur:
            cur.execute(
                "INSERT INTO sessions (started_at, paper_trade, engine) VALUES (?,?,?)",
                (time.time(), int(paper_trade), engine),
            )
            self.session_id = cur.lastrowid
        logger.info("Analytics session started: id=%d", self.session_id)
        return self.session_id

    def end_session(self) -> None:
        if not self.session_id:
            return
        with self._tx() as cur:
            cur.execute(
                "UPDATE sessions SET ended_at=? WHERE id=?",
                (time.time(), self.session_id),
            )

    # ── Market Scans ──────────────────────────────────────────────────

    def log_scan(
            self,
            total_fetched: int,
            sports_found: int,
            passed_filter: int,
            elapsed_secs: float,
            markets: list,              # list of snapshot dicts (passed + rejected)
            passed_tickers: set,        # which tickers made it through
            rejected: list,             # list of (snapshot, reason) tuples
    ) -> int:
        """Log a full market scan cycle."""
        now = time.time()
        with self._tx() as cur:
            cur.execute(
                """INSERT INTO market_scans
                   (session_id, scanned_at, total_fetched, sports_found,
                    passed_filter, elapsed_secs)
                   VALUES (?,?,?,?,?,?)""",
                (self.session_id, now, total_fetched, sports_found,
                 passed_filter, elapsed_secs),
            )
            scan_id = cur.lastrowid

            # Scanned markets (passed)
            for snap in markets:
                ticker = snap.get("ticker", "")
                bid    = snap.get("best_bid")
                ask    = snap.get("best_ask")
                cur.execute(
                    """INSERT INTO scanned_markets
                       (scan_id, session_id, scanned_at, ticker, sport,
                        best_bid, best_ask, spread, last_price, volume,
                        passed_filter, reject_reason)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        scan_id, self.session_id, now, ticker,
                        _sport_from_ticker(ticker),
                        bid, ask,
                        round(ask - bid, 6) if bid and ask else None,
                        snap.get("last_price"), snap.get("volume"),
                        1 if ticker in passed_tickers else 0,
                        None if ticker in passed_tickers else "filtered",
                    ),
                )

            # Rejected markets
            for snap, reason in rejected:
                ticker = snap.get("ticker", "")
                bid    = snap.get("best_bid")
                ask    = snap.get("best_ask")
                cur.execute(
                    """INSERT INTO rejected_markets
                       (session_id, rejected_at, ticker, sport,
                        best_bid, best_ask, spread, reject_reason)
                       VALUES (?,?,?,?,?,?,?,?)""",
                    (
                        self.session_id, now, ticker,
                        _sport_from_ticker(ticker),
                        bid, ask,
                        round(ask - bid, 6) if bid and ask else None,
                        reason,
                    ),
                )

        return scan_id

    # ── Signals ───────────────────────────────────────────────────────

    def log_signal(
            self,
            ticker: str,
            engine: str,
            signal_type: str,
            price: float,
            metadata: dict,
    ) -> int:
        with self._tx() as cur:
            cur.execute(
                """INSERT INTO signals
                   (session_id, fired_at, ticker, sport, engine, signal_type,
                    price, prev_price, price_move, best_bid, best_ask, spread,
                    acted_on)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,0)""",
                (
                    self.session_id, time.time(), ticker,
                    _sport_from_ticker(ticker), engine, signal_type,
                    price,
                    metadata.get("prev_price"),
                    metadata.get("price_move"),
                    metadata.get("best_bid"),
                    metadata.get("best_ask"),
                    metadata.get("spread"),
                ),
            )
            return cur.lastrowid

    def mark_signal_acted(self, signal_id: int, order_id: int) -> None:
        with self._tx() as cur:
            cur.execute(
                "UPDATE signals SET acted_on=1, order_id=? WHERE id=?",
                (order_id, signal_id),
            )

    # ── Orders ────────────────────────────────────────────────────────

    def log_order(
            self,
            ticker: str,
            engine: str,
            side: str,
            requested_price: float,
            requested_qty: int,
            filled_price: Optional[float],
            filled_qty: Optional[int],
            kalshi_order_id: Optional[str],
            success: bool,
            paper_trade: bool,
            error_msg: Optional[str] = None,
            signal_id: Optional[int] = None,
            attempt_count: int = 1,
    ) -> int:
        with self._tx() as cur:
            cur.execute(
                """INSERT INTO orders
                   (session_id, signal_id, placed_at, ticker, sport, engine,
                    side, requested_price, requested_qty, filled_price,
                    filled_qty, kalshi_order_id, success, error_msg,
                    paper_trade, attempt_count)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    self.session_id, signal_id, time.time(), ticker,
                    _sport_from_ticker(ticker), engine, side,
                    requested_price, requested_qty,
                    filled_price, filled_qty, kalshi_order_id,
                    int(success), error_msg, int(paper_trade), attempt_count,
                ),
            )
            return cur.lastrowid

    # ── Positions ─────────────────────────────────────────────────────

    def open_position(
            self,
            ticker: str,
            engine: str,
            entry_price: float,
            qty: int,
            paper_trade: bool,
            entry_order_id: Optional[int] = None,
    ) -> int:
        with self._tx() as cur:
            cur.execute(
                """INSERT INTO positions
                   (session_id, opened_at, ticker, sport, engine,
                    entry_order_id, entry_price, qty, paper_trade)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (
                    self.session_id, time.time(), ticker,
                    _sport_from_ticker(ticker), engine,
                    entry_order_id, entry_price, qty, int(paper_trade),
                ),
            )
            return cur.lastrowid

    def close_position(
            self,
            position_id: int,
            exit_price: float,
            peak_price: Optional[float],
            ticks_held: int,
            secs_held: float,
            exit_order_id: Optional[int] = None,
            close_reason: str = "manual",
            notes: Optional[str] = None,
    ) -> None:
        with self._tx() as cur:
            # Fetch entry details for P&L calc
            row = cur.execute(
                "SELECT entry_price, qty FROM positions WHERE id=?", (position_id,)
            ).fetchone()
            if not row:
                logger.warning("close_position: position %d not found", position_id)
                return

            entry_price = row["entry_price"]
            qty         = row["qty"]
            gross_pnl   = round((exit_price - entry_price) * qty, 6)
            pnl_pct     = round(gross_pnl / (entry_price * qty), 6) if entry_price else None
            peak_pnl    = round((peak_price - entry_price) * qty, 6) if peak_price else None

            cur.execute(
                """UPDATE positions SET
                   closed_at=?, exit_order_id=?, exit_price=?,
                   gross_pnl=?, pnl_pct=?, peak_price=?, peak_pnl=?,
                   ticks_held=?, secs_held=?, close_reason=?, notes=?
                   WHERE id=?""",
                (
                    time.time(), exit_order_id, exit_price,
                    gross_pnl, pnl_pct, peak_price, peak_pnl,
                    ticks_held, secs_held, close_reason, notes,
                    position_id,
                ),
            )

    # ── Position Ticks ────────────────────────────────────────────────

    def flush_ticks(
            self,
            position_id: int,
            ticker: str,
            engine: str,
            entry_price: float,
            entry_time: float,
            ticks: List[Tuple[float, float]],       # (ts, price)
            tick_base: int = 0,
            bid_ask_history: Optional[List[Tuple[float, float]]] = None,
    ) -> None:
        """
        Bulk insert tick history for a position.
        bid_ask_history: optional list of (bid, ask) parallel to ticks.
        """
        if not ticks:
            return
        sport = _sport_from_ticker(ticker)
        rows = []
        for i, (ts, price) in enumerate(ticks):
            idx   = tick_base + i
            bid   = bid_ask_history[i][0] if bid_ask_history else None
            ask   = bid_ask_history[i][1] if bid_ask_history else None
            move  = round(price - entry_price, 6)
            pct   = round(move / entry_price, 6) if entry_price else None
            secs  = round(ts - entry_time, 2)
            spread = round(ask - bid, 6) if bid and ask else None
            rows.append((
                position_id, self.session_id, ticker, sport, engine,
                ts, price, bid, ask, spread, move, pct, idx, secs,
            ))

        with self._tx() as cur:
            cur.executemany(
                """INSERT INTO position_ticks
                   (position_id, session_id, ticker, sport, engine,
                    ts, price, best_bid, best_ask, spread,
                    move_from_entry, pct_from_entry,
                    ticks_since_entry, secs_since_entry)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                rows,
            )

    # ── Engine Switches ───────────────────────────────────────────────

    def log_engine_switch(self, from_engine: str, to_engine: str) -> None:
        with self._tx() as cur:
            cur.execute(
                """INSERT INTO engine_switches
                   (session_id, switched_at, from_engine, to_engine)
                   VALUES (?,?,?,?)""",
                (self.session_id, time.time(), from_engine, to_engine),
            )

    # ── Utility ───────────────────────────────────────────────────────

    def execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        """Direct execute for ad-hoc queries from other modules."""
        with self._lock:
            return self._conn.execute(sql, params)

    def executemany(self, sql: str, params) -> None:
        with self._tx() as cur:
            cur.executemany(sql, params)


# ─────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────

_SPORT_MAP = {
    "NBA": "NBA", "NFL": "NFL", "MLB": "MLB", "NHL": "NHL",
    "NCAAB": "NCAAB", "NCAAF": "NCAAF",
    "MMA": "MMA", "SOCCER": "Soccer",
}

def _sport_from_ticker(ticker: str) -> Optional[str]:
    t = ticker.upper()
    for key, label in _SPORT_MAP.items():
        if key in t:
            return label
    return "Other"