-- kalshi_trader schema
-- SQLite compatible. For PostgreSQL, replace AUTOINCREMENT with SERIAL.

CREATE TABLE IF NOT EXISTS markets (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker     TEXT NOT NULL UNIQUE,
    event      TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ticks (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id  INTEGER NOT NULL REFERENCES markets(id),
    timestamp  TEXT NOT NULL,
    best_bid   REAL,
    best_ask   REAL,
    last_price REAL,
    volume     REAL
);

CREATE INDEX IF NOT EXISTS idx_ticks_market_ts ON ticks(market_id, timestamp);

CREATE TABLE IF NOT EXISTS signals (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id   INTEGER NOT NULL REFERENCES markets(id),
    timestamp   TEXT NOT NULL,
    signal_type TEXT NOT NULL,  -- ENTRY | EXIT | STOP_LOSS
    price       REAL NOT NULL,
    metadata    TEXT            -- JSON blob
);

CREATE TABLE IF NOT EXISTS trades (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id  INTEGER NOT NULL REFERENCES markets(id),
    side       TEXT NOT NULL,   -- BUY | SELL
    price      REAL NOT NULL,
    quantity   INTEGER NOT NULL,
    timestamp  TEXT NOT NULL,
    pnl        REAL             -- NULL until position closed
);

CREATE TABLE IF NOT EXISTS positions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id   INTEGER NOT NULL REFERENCES markets(id),
    entry_price REAL NOT NULL,
    quantity    INTEGER NOT NULL,
    status      TEXT NOT NULL DEFAULT 'OPEN',  -- OPEN | CLOSED
    stop_loss   REAL NOT NULL,
    created_at  TEXT NOT NULL,
    closed_at   TEXT
);
