"""
signal_engine.py — Stateful per-market signal detection.

Strategy: entry-only signals. NO automatic stop-loss or take-profit exits.
All sells are manual. Every tick while in a position is recorded to the DB
so optimal sell timing can be determined from historical data.

Tracks per-position:
  - entry_price, entry_time
  - peak_price (highest seen since entry)
  - ticks_held (number of price ticks observed while in position)
  - price_history (list of (timestamp, price) tuples for full replay)
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Optional

from core.config import Config
from core.models import MarketState, Signal, SignalType

logger = logging.getLogger(__name__)


class SignalEngine:
    """
    Manages signal detection state for all active markets.

    Only fires ENTRY signals. Exits are 100% manual.
    While a position is open, process_tick() records every price movement
    so you can later query the DB and determine the optimal sell window.
    """

    def __init__(self, config: Config) -> None:
        self._config = config
        self._states: dict[str, MarketState] = {}
        self._locks:  dict[str, threading.Lock] = {}
        self._global_lock = threading.Lock()

    # ──────────────────────────────────────────────────────────────────
    # STATE MANAGEMENT
    # ──────────────────────────────────────────────────────────────────

    def get_or_create_state(self, ticker: str, market_id: int) -> MarketState:
        with self._global_lock:
            if ticker not in self._states:
                self._states[ticker] = MarketState(ticker=ticker, market_id=market_id)
                self._locks[ticker] = threading.Lock()
            return self._states[ticker]

    def mark_position_open(
            self,
            ticker: str,
            position_id: int,
            entry_price: float,
    ) -> None:
        """Called by execution engine after a confirmed fill."""
        state = self._states.get(ticker)
        if state:
            with self._locks[ticker]:
                state.has_position     = True
                state.position_id      = position_id
                state.entry_price      = entry_price
                state.entry_time       = time.time()
                state.peak_price       = entry_price
                state.ticks_held       = 0
                state.price_history    = [(time.time(), entry_price)]
                logger.info(
                    "[%s] Position opened: entry=%.4f  id=%d",
                    ticker, entry_price, position_id,
                )

    def mark_position_closed(self, ticker: str) -> None:
        """Called after a manual sell is confirmed."""
        state = self._states.get(ticker)
        if state:
            with self._locks[ticker]:
                held_secs = (
                    round(time.time() - state.entry_time, 1)
                    if state.entry_time else None
                )
                logger.info(
                    "[%s] Position closed manually: entry=%.4f  peak=%.4f  "
                    "ticks_held=%d  secs_held=%s",
                    ticker,
                    state.entry_price  or 0.0,
                    state.peak_price   or 0.0,
                    state.ticks_held   or 0,
                    held_secs,
                    )
                state.has_position    = False
                state.position_id     = None
                state.entry_price     = None
                state.entry_time      = None
                state.peak_price      = None
                state.ticks_held      = 0
                state.price_history   = []
                state.crossover_fired = False  # allow re-entry on same market

    def get_position_snapshot(self, ticker: str) -> Optional[dict]:
        """
        Return a dict of live position stats for dashboard display or DB writes.
        Returns None if no open position.
        """
        state = self._states.get(ticker)
        if not state or not state.has_position:
            return None
        with self._locks[ticker]:
            now = time.time()
            return {
                "ticker":       ticker,
                "position_id":  state.position_id,
                "entry_price":  state.entry_price,
                "entry_time":   state.entry_time,
                "peak_price":   state.peak_price,
                "ticks_held":   state.ticks_held,
                "secs_held":    round(now - state.entry_time, 1) if state.entry_time else None,
                "price_history": list(state.price_history),  # snapshot copy
            }

    # ──────────────────────────────────────────────────────────────────
    # TICK PROCESSING
    # ──────────────────────────────────────────────────────────────────

    def process_tick(
            self,
            ticker: str,
            market_id: int,
            price: float,
            best_bid: Optional[float],
            best_ask: Optional[float],
    ) -> Optional[Signal]:
        """
        Per-tick logic:

        1. If in a position — record the tick for analysis, update peak. No exits.
        2. If no position — watch for momentum crossover entry signal.

        Entry conditions (all must pass):
          - Price in 38¢–62¢ range  (coin-flip game)
          - Previous tick below entry_threshold, current tick at/above it
          - Price ≤ 55¢              (don't chase)
          - Move ≥ 2¢               (real momentum, not noise)
          - Spread within max_spread (liquid enough to fill)
        """
        state = self.get_or_create_state(ticker, market_id)

        with self._locks[ticker]:
            prev = state.last_price
            state.prev_price = prev
            state.last_price = price

            if prev is None:
                return None

            cfg = self._config

            # ── OPEN POSITION: track only, never auto-exit ─────────────
            if state.has_position:
                state.ticks_held += 1

                # Update peak
                if state.peak_price is None or price > state.peak_price:
                    state.peak_price = price

                # Record tick for sell-timing analysis
                # price_history is read by worker and flushed to DB
                state.price_history.append((time.time(), price))

                # Log notable moves for easy log-scraping
                if state.entry_price is not None:
                    move = price - state.entry_price
                    if abs(move) >= 0.05:   # log every 5¢ move from entry
                        logger.info(
                            "[%s] In position — price=%.4f  entry=%.4f  "
                            "move=%+.4f  peak=%.4f  ticks=%d",
                            ticker, price, state.entry_price,
                            move, state.peak_price or 0.0, state.ticks_held,
                                  )

                return None   # ← no automatic exit signal ever

            # ── NO POSITION: look for entry crossover ──────────────────
            if state.crossover_fired:
                return None   # already entered once, wait for manual close/reset

            # Price must be in coin-flip zone
            if not (0.38 <= price <= 0.62):
                return None

            # Must be a true upward crossover
            if not (prev < cfg.entry_threshold <= price):
                return None

            # Don't chase
            if price > 0.55:
                return None

            # Require real momentum (raised from 0.8¢ to 2¢)
            price_move = price - prev
            if price_move < 0.02:
                logger.debug("[%s] Crossover too weak: move=%.4f", ticker, price_move)
                return None

            # Spread guard
            spread = _spread(best_bid, best_ask)
            if spread is None or spread > cfg.max_spread:
                logger.debug("[%s] Crossover blocked: spread=%s", ticker, spread)
                return None

            state.crossover_fired = True
            logger.info(
                "[%s] ENTRY SIGNAL: %.4f → %.4f  move=%.4f  spread=%.4f",
                ticker, prev, price, price_move, spread or 0.0,
                                                 )

            return Signal(
                ticker=ticker,
                market_id=market_id,
                signal_type=SignalType.ENTRY,
                price=price,
                metadata={
                    "prev_price": prev,
                    "price_move": price_move,
                    "best_bid":   best_bid,
                    "best_ask":   best_ask,
                    "spread":     spread,
                },
            )

        return None


# ──────────────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────────────

def _spread(bid: Optional[float], ask: Optional[float]) -> Optional[float]:
    if bid is not None and ask is not None and ask > 0 and bid >= 0:
        return ask - bid
    return None