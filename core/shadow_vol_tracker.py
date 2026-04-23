"""
shadow_vol_tracker.py — Simulates alternative volume spike stop loss multipliers in parallel.

When a real BUY fills, a shadow_vol position is opened for each multiplier in MULTIPLIERS.
On every tick, the volume delta is computed once and checked against each open multiplier.
When a multiplier's threshold is crossed AND price is against us, that position closes as
'vol_spike'. When the real position closes for any other reason, remaining open positions
close at the real exit price.

Results land in shadow_vol_trades. After enough trades accumulate, query:

    SELECT multiplier, AVG(pnl_per_contract), COUNT(*),
           COUNT(CASE WHEN exit_reason='vol_spike' THEN 1 END) AS vol_stops
    FROM shadow_vol_trades WHERE exit_price IS NOT NULL
    GROUP BY multiplier ORDER BY AVG(pnl_per_contract) DESC;
"""

from __future__ import annotations

import logging
from collections import deque
from typing import Optional

from db.db import Database

logger = logging.getLogger(__name__)

MULTIPLIERS           = [1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 5.0]
VOLUME_ROLLING_WINDOW = 20
VOLUME_MIN_BASELINE   = 1.0   # ignore spikes until rolling avg exceeds this


class ShadowVolTracker:

    def __init__(self, db: Database) -> None:
        self._db = db
        self._open_tickers: set[str]               = set()
        self._last_volume:  dict[str, float]        = {}
        self._vol_deltas:   dict[str, deque[float]] = {}

    def open(self, ticker: str, side: str, entry_price: float) -> None:
        for multiplier in MULTIPLIERS:
            self._db.open_shadow_vol_position(ticker, side, entry_price, multiplier)
        self._open_tickers.add(ticker)
        self._vol_deltas[ticker] = deque(maxlen=VOLUME_ROLLING_WINDOW)
        self._last_volume.pop(ticker, None)
        logger.debug("[%s] Shadow vol positions opened: %d multipliers", ticker, len(MULTIPLIERS))

    def process_tick(
        self,
        ticker: str,
        bid:    Optional[float],
        ask:    Optional[float],
        vol:    Optional[float],
    ) -> None:
        if ticker not in self._open_tickers:
            return

        rows = self._db.get_open_shadow_vol_positions(ticker)
        if not rows:
            self._open_tickers.discard(ticker)
            return

        # Update volume history
        vol_delta: Optional[float] = None
        rolling_avg: float = 0.0
        if vol is not None:
            prev = self._last_volume.get(ticker)
            if prev is not None:
                delta = vol - prev
                if delta > 0:
                    self._vol_deltas[ticker].append(delta)
                    vol_delta = delta
            self._last_volume[ticker] = vol

        if vol_delta is not None and len(self._vol_deltas[ticker]) >= 3:
            rolling_avg = sum(self._vol_deltas[ticker]) / len(self._vol_deltas[ticker])

        for row_id, multiplier, entry_price, side in rows:
            # Compute check_price the same way the real engine does
            if side == "YES":
                check_price = bid
            else:
                check_price = round(1.0 - ask, 6) if ask is not None else None

            if check_price is None:
                continue

            price_against_us = check_price < entry_price

            spike = (
                vol_delta is not None
                and rolling_avg >= VOLUME_MIN_BASELINE
                and vol_delta > multiplier * rolling_avg
            )

            if spike and price_against_us:
                pnl = check_price - entry_price
                self._db.close_shadow_vol_position(row_id, check_price, "vol_spike", pnl)
                logger.debug(
                    "[%s] Shadow vol stop: multiplier=%.1f delta=%.2f avg=%.2f price=%.4f pnl=%+.4f",
                    ticker, multiplier, vol_delta, rolling_avg, check_price, pnl,
                )

    def close_all(self, ticker: str, exit_price: float, reason: str) -> None:
        if ticker not in self._open_tickers:
            return

        rows = self._db.get_open_shadow_vol_positions(ticker)
        for row_id, _multiplier, entry_price, _side in rows:
            pnl = exit_price - entry_price
            self._db.close_shadow_vol_position(row_id, exit_price, reason, pnl)

        self._open_tickers.discard(ticker)
        self._last_volume.pop(ticker, None)
        self._vol_deltas.pop(ticker, None)
        logger.debug(
            "[%s] Shadow vol positions closed: %d remaining → %s @ %.4f",
            ticker, len(rows), reason, exit_price,
        )
