"""
migration_add_position_ticks.py

Run once to add the position_ticks table to your existing kalshi_trader.db.

Usage:
    python migration_add_position_ticks.py
"""

import sqlite3

DB_PATH = "kalshi_trader.db"

CREATE_POSITION_TICKS = """
CREATE TABLE IF NOT EXISTS position_ticks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    position_id     INTEGER NOT NULL,           -- FK to positions.id
    ticker          TEXT    NOT NULL,
    ts              REAL    NOT NULL,            -- unix timestamp (time.time())
    price           REAL    NOT NULL,            -- 0–1 float
    move_from_entry REAL,                        -- price - entry_price at this tick
    ticks_since_entry INTEGER,                   -- tick index within the position
    FOREIGN KEY (position_id) REFERENCES positions(id)
);

CREATE INDEX IF NOT EXISTS idx_pt_position ON position_ticks(position_id);
CREATE INDEX IF NOT EXISTS idx_pt_ticker   ON position_ticks(ticker);
"""

ALTER_POSITIONS = """
-- Add sell-analysis columns to the positions table (safe: IF NOT EXISTS via try/except)
ALTER TABLE positions ADD COLUMN peak_price     REAL;
ALTER TABLE positions ADD COLUMN ticks_held     INTEGER;
ALTER TABLE positions ADD COLUMN secs_held      REAL;
ALTER TABLE positions ADD COLUMN close_reason   TEXT DEFAULT 'manual';
"""

def run():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    print("Creating position_ticks table...")
    for stmt in CREATE_POSITION_TICKS.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            cur.execute(stmt)

    print("Adding columns to positions table (skipping if already exist)...")
    for stmt in ALTER_POSITIONS.strip().split(";"):
        stmt = stmt.strip()
        if not stmt:
            continue
        try:
            cur.execute(stmt)
        except sqlite3.OperationalError as e:
            print(f"  Skipped (already exists): {e}")

    conn.commit()
    conn.close()
    print("Done.")

if __name__ == "__main__":
    run()