"""
signal_engine_resolution.py — Near-certain resolution scalper.

Strategy:
  Buy YES when price reaches 97¢+ — the market is pricing ~97% probability
  of YES resolution. Hold until manual exit or contract settles at 100¢,
  capturing the final 3¢ with very low risk.

Why this works:
  At 97¢ the market is almost certainly resolved in your favour.
  The remaining 3¢ upside sounds small but on e.g. 100 contracts that's
  $3 profit on $97 at-risk — a 3% return in minutes/hours.
  The key edge is that the bid/ask spread at this level is often 1–2¢,
  so a limit buy at 97¢ frequently fills and the downside is capped by
  how quickly you can manually exit if something reverses.

Entry conditions:
  - ask <= RESOLUTION_ENTRY_MAX  (default 0.97)
  - bid >= RESOLUTION_BID_MIN    (default 0.95)  — confirms genuine liquidity
  - spread <= RESOLUTION_MAX_SPREAD (default 0.03)
  - NOT already in a position on this ticker
  - NOT already fired entry on this ticker (reset on manual close)

No automatic exits. All sells are manual.
Every tick while in position is recorded to price_history for DB analysis.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Optional

from core.config import Config
from core.models import MarketState, Signal, SignalType

logger = logging.getLogger(__name__)

# ── Strategy constants ────────────────────────────────────────────────
RESOLUTION_ENTRY_MAX  = 0.97   # buy when ask is AT or BELOW this
RESOLUTION_BID_MIN    = 0.90   # require a real bid (not a ghost market)
RESOLUTION_MAX_SPREAD = 0.04   # max 4¢ spread at this level
# ─────────────────────────────────────────────────────────────────────


class ResolutionSignalEngine:
    """
    Fires ENTRY signals when a market reaches near-certain YES resolution.
    Drop-in replacement for SignalEngine — identical public interface.
    """

    def __init__(self, config: Config) -> None:
        self._config = config
        self._states: dict[str, MarketState] = {}
        self._locks:  dict[str, threading.Lock] = {}
        self._global_lock = threading.Lock()

    # ──────────────────────────────────────────────────────────────────
    # STATE MANAGEMENT  (identical to SignalEngine)
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
        state = self._states.get(ticker)
        if state:
            with self._locks[ticker]:
                state.has_position  = True
                state.position_id   = position_id
                state.entry_price   = entry_price
                state.entry_time    = time.time()
                state.peak_price    = entry_price
                state.ticks_held    = 0
                state.price_history = [(time.time(), entry_price)]
                logger.info(
                    "[%s] [RESOLUTION] Position opened: entry=%.4f  id=%d",
                    ticker, entry_price, position_id,
                )

    def mark_position_closed(self, ticker: str) -> None:
        state = self._states.get(ticker)
        if state:
            with self._locks[ticker]:
                held_secs = (
                    round(time.time() - state.entry_time, 1)
                    if state.entry_time else None
                )
                logger.info(
                    "[%s] [RESOLUTION] Position closed manually: "
                    "entry=%.4f  peak=%.4f  ticks=%d  secs=%s",
                    ticker,
                    state.entry_price or 0.0,
                    state.peak_price  or 0.0,
                    state.ticks_held  or 0,
                    held_secs,
                    )
                state.has_position    = False
                state.position_id     = None
                state.entry_price     = None
                state.entry_time      = None
                state.peak_price      = None
                state.ticks_held      = 0
                state.price_history   = []
                state.crossover_fired = False

    def get_position_snapshot(self, ticker: str) -> Optional[dict]:
        state = self._states.get(ticker)
        if not state or not state.has_position:
            return None
        with self._locks[ticker]:
            now = time.time()
            return {
                "ticker":        ticker,
                "position_id":   state.position_id,
                "entry_price":   state.entry_price,
                "entry_time":    state.entry_time,
                "peak_price":    state.peak_price,
                "ticks_held":    state.ticks_held,
                "secs_held":     round(now - state.entry_time, 1) if state.entry_time else None,
                "price_history": list(state.price_history),
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
        Resolution scalper tick logic:

        IN POSITION  → track price, update peak, never auto-exit.
        NO POSITION  → fire ENTRY if ask <= 97¢ with healthy bid and tight spread.
        """
        state = self.get_or_create_state(ticker, market_id)

        with self._locks[ticker]:
            prev = state.last_price
            state.prev_price = prev
            state.last_price = price

            if prev is None:
                return None

            # ── IN POSITION: record tick, no exits ────────────────────
            if state.has_position:
                state.ticks_held += 1
                if state.peak_price is None or price > state.peak_price:
                    state.peak_price = price
                state.price_history.append((time.time(), price))

                if state.entry_price is not None:
                    move = price - state.entry_price
                    if abs(move) >= 0.01:  # log every 1¢ move (tighter at this level)
                        logger.info(
                            "[%s] [RESOLUTION] price=%.4f  entry=%.4f  "
                            "move=%+.4f  peak=%.4f  ticks=%d",
                            ticker, price, state.entry_price,
                            move, state.peak_price or 0.0, state.ticks_held,
                                  )
                return None

            # ── NO POSITION: look for near-resolution entry ───────────
            if state.crossover_fired:
                return None

            # Need a real ask at 97¢ or below
            if best_ask is None or best_ask > RESOLUTION_ENTRY_MAX:
                return None

            # Need a real bid confirming the market is alive
            if best_bid is None or best_bid < RESOLUTION_BID_MIN:
                logger.debug(
                    "[%s] Resolution level reached but bid too low: bid=%s ask=%s",
                    ticker, best_bid, best_ask,
                )
                return None

            # Spread must be tight — wide spread at 97¢ means illiquid/ghost market
            spread = best_ask - best_bid
            if spread > RESOLUTION_MAX_SPREAD:
                logger.debug(
                    "[%s] Resolution spread too wide: %.4f > %.4f",
                    ticker, spread, RESOLUTION_MAX_SPREAD,
                )
                return None

            state.crossover_fired = True
            logger.info(
                "[%s] RESOLUTION ENTRY: ask=%.4f  bid=%.4f  spread=%.4f",
                ticker, best_ask, best_bid, spread,
            )

            return Signal(
                ticker=ticker,
                market_id=market_id,
                signal_type=SignalType.ENTRY,
                price=best_ask,
                metadata={
                    "engine":    "resolution",
                    "best_bid":  best_bid,
                    "best_ask":  best_ask,
                    "spread":    spread,
                    "prev_price": prev,
                },
            )

        return None