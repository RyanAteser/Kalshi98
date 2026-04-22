"""
worker_tick_flush.py

Paste this into your MarketWorker wherever it calls signal_engine.process_tick().
It drains state.price_history into the position_ticks DB table after every tick
so you have a full price timeline for every position, queryable later.

Assumes:
  - self._db       : Database instance with a .conn or .execute() method
  - self._signal_engine : SignalEngine
  - ticker, market_id  : available on self
"""

# ── Inside your tick loop, AFTER calling process_tick() ──────────────

def _flush_price_history(self, ticker: str, state) -> None:
    """
    Drain state.price_history into position_ticks table.
    Called every tick while a position is open.
    Clears the buffer after writing to avoid double-inserts.
    """
    if not state.has_position or not state.price_history:
        return

    position_id  = state.position_id
    entry_price  = state.entry_price or 0.0
    tick_base    = (state.ticks_held or 0) - len(state.price_history)

    rows = [
        (
            position_id,
            ticker,
            ts,
            price,
            round(price - entry_price, 6),   # move_from_entry
            tick_base + i,                    # ticks_since_entry
        )
        for i, (ts, price) in enumerate(state.price_history)
    ]

    try:
        self._db.executemany(
            """
            INSERT INTO position_ticks
                (position_id, ticker, ts, price, move_from_entry, ticks_since_entry)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    except Exception as exc:
        logger.warning("[%s] Failed to flush price history: %s", ticker, exc)
        return

    # Clear the buffer — already written
    state.price_history.clear()


# ── Analysis query (run in sqlite3 shell or a notebook) ──────────────
#
# Best tick (highest price) reached after entry, per position:
#
#   SELECT
#       p.id,
#       p.ticker,
#       p.entry_price,
#       MAX(pt.price)                               AS peak_price,
#       MAX(pt.price) - p.entry_price               AS best_possible_gain,
#       pt_best.ticks_since_entry                   AS ticks_to_peak,
#       pt_best.ts - p.entry_time                   AS secs_to_peak
#   FROM positions p
#   JOIN position_ticks pt       ON pt.position_id = p.id
#   JOIN position_ticks pt_best  ON pt_best.position_id = p.id
#                                AND pt_best.price = (
#                                    SELECT MAX(price) FROM position_ticks
#                                    WHERE position_id = p.id
#                                )
#   WHERE p.close_reason = 'manual'
#   GROUP BY p.id
#   ORDER BY best_possible_gain DESC;
#
#
# Distribution of price at each tick offset across all positions:
#
#   SELECT
#       ticks_since_entry,
#       COUNT(*)                        AS n,
#       AVG(move_from_entry)            AS avg_move,
#       AVG(move_from_entry) * 100      AS avg_move_cents,
#       MAX(move_from_entry)            AS best_move,
#       MIN(move_from_entry)            AS worst_move
#   FROM position_ticks
#   GROUP BY ticks_since_entry
#   ORDER BY ticks_since_entry;