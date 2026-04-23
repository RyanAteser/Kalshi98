"""
shadow_tracker.py — Simulates alternative stop loss thresholds in parallel with live trading.

When a real BUY fills, a shadow position is opened for each threshold in THRESHOLDS.
Each tick is evaluated against every open shadow position independently.
When a shadow position's threshold is crossed, it closes as 'stop_loss'.
When the real position closes (via settlement or stop loss), all remaining shadow
positions for that ticker close at the same exit price.

Results live in the shadow_trades table and never touch the real trading flow.
"""

from __future__ import annotations

import logging
from typing import Optional

from db.db import Database

logger = logging.getLogger(__name__)

THRESHOLDS = [0.70, 0.75, 0.80, 0.85, 0.90, 0.95]


class ShadowTracker:

    def __init__(self, db: Database) -> None:
        self._db = db
        self._open_tickers: set[str] = set()

    def open(self, ticker: str, side: str, entry_price: float) -> None:
        for threshold in THRESHOLDS:
            self._db.open_shadow_position(ticker, side, entry_price, threshold)
        self._open_tickers.add(ticker)
        logger.debug("[%s] Shadow positions opened: %d thresholds", ticker, len(THRESHOLDS))

    def process_tick(self, ticker: str, bid: Optional[float], ask: Optional[float]) -> None:
        if ticker not in self._open_tickers:
            return

        rows = self._db.get_open_shadow_positions(ticker)
        if not rows:
            self._open_tickers.discard(ticker)
            return

        for row_id, threshold, entry_price, side in rows:
            if side == "YES":
                check_price = bid
            else:
                check_price = round(1.0 - ask, 6) if ask is not None else None

            if check_price is not None and check_price <= threshold:
                pnl = check_price - entry_price
                self._db.close_shadow_position(row_id, check_price, "stop_loss", pnl)
                logger.debug(
                    "[%s] Shadow stop at threshold=%.2f price=%.4f pnl=%+.4f",
                    ticker, threshold, check_price, pnl,
                )

    def close_all(self, ticker: str, exit_price: float, reason: str) -> None:
        if ticker not in self._open_tickers:
            return

        rows = self._db.get_open_shadow_positions(ticker)
        for row_id, threshold, entry_price, side in rows:
            pnl = exit_price - entry_price
            self._db.close_shadow_position(row_id, exit_price, reason, pnl)

        self._open_tickers.discard(ticker)
        logger.debug(
            "[%s] Shadow positions closed: %d remaining → %s @ %.4f",
            ticker, len(rows), reason, exit_price,
        )
