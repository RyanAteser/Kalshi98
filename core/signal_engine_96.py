"""
signal_engine_96.py — Buy YES or NO contracts at 97–99c. Stop out at 90c.

Hardened version:
  - Strict threshold validation with explicit bounds
  - External cooldown support — risk_manager can call mark_cooldown(ticker, until_ts)
    to prevent signal spam while in cooldown
  - Debug logging of every tick with exact thresholds used
  - Re-entry allowed after stop loss or close

FIX (stop loss never firing):
  Ticks from OTHER tickers no longer reset the engine's position state.
  Previously, whenever a tick arrived for a ticker other than the one we
  held, _reset() would wipe the position and the next tick for our held
  ticker would skip the stop loss check entirely (because _has_position
  had been set to False). Now mismatched-ticker ticks are ignored and
  market rotation/settlement is handled exclusively by
  mark_position_closed() from portfolio_poller / risk_manager.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Optional

from core.models import Signal, SignalType

logger = logging.getLogger(__name__)

# ── Thresholds ────────────────────────────────────────────────────────
BUY_MIN   = 0.97      # lower bound: price must be >= this to trigger entry
BUY_MAX   = 0.99      # upper bound: price must be <= this to trigger entry
STOP_LOSS = 0.90      # exit if price drops to this or below


class Simple96Engine:

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._reset()
        self._cooldown_until: dict[str, float] = {}

    def _reset(self) -> None:
        self._has_position     = False
        self._pending_entry    = False
        self._position_ticker: Optional[str]   = None
        self._position_side:   Optional[str]   = None
        self._entry_price:     Optional[float] = None
        self._position_id:     Optional[int]   = None

    # ── Cooldown (called by risk_manager on failed orders) ────────────

    def mark_cooldown(self, ticker: str, duration: float = 30.0) -> None:
        """Block new entry signals for this ticker for `duration` seconds."""
        with self._lock:
            self._cooldown_until[ticker] = time.time() + duration
        logger.info("[97c] Cooldown: %s blocked for %.0fs", ticker, duration)

    def _in_cooldown(self, ticker: str) -> bool:
        until = self._cooldown_until.get(ticker, 0)
        return time.time() < until

    # ── Position tracking ─────────────────────────────────────────────

    def mark_position_open(
            self, ticker: str, position_id: int, entry_price: float,
            side: Optional[str] = None,
    ) -> None:
        with self._lock:
            self._has_position    = True
            self._pending_entry   = False
            self._position_ticker = ticker
            self._entry_price     = entry_price
            self._position_id     = position_id
            if side is not None:
                self._position_side = side
        logger.info(
            "[97c] IN: %s @ %.4f  side=%s  id=%d",
            ticker, entry_price, self._position_side, position_id,
        )

    def mark_position_closed(self, ticker: str) -> None:
        with self._lock:
            self._reset()
        logger.info("[97c] CLOSED: %s — re-armed", ticker)

    def get_position_snapshot(self, ticker: str) -> Optional[dict]:
        with self._lock:
            if not self._has_position or self._position_ticker != ticker:
                return None
            return {
                "ticker":      ticker,
                "side":        self._position_side,
                "entry_price": self._entry_price,
                "position_id": self._position_id,
            }

    @property
    def current_side(self) -> Optional[str]:
        with self._lock:
            return self._position_side

    # ── Tick processing ───────────────────────────────────────────────

    def process_tick(
            self,
            ticker: str,
            market_id: int,
            price: float,
            best_bid: Optional[float],
            best_ask: Optional[float],
    ) -> Optional[Signal]:

        with self._lock:
            # ── IN POSITION: stop loss check ──────────────────────────
            if self._has_position:
                if self._position_ticker != ticker:
                    # Tick is for a different market — IGNORE it.
                    #
                    # We only hold one position at a time. Ticks for our held
                    # ticker will arrive from its own worker and drive the
                    # stop loss check below. Market rotation and settlement
                    # are handled by mark_position_closed() from
                    # portfolio_poller / risk_manager — NOT by resetting here.
                    #
                    # The previous implementation called self._reset() in this
                    # branch, which nuked position state every time any other
                    # ticker sent a tick, making stop loss effectively
                    # impossible to trigger in a multi-worker setup.
                    return None

                # Side-aware stop loss using LIVE book prices (not stale last trade)
                if self._position_side == "NO":
                    # NO bid = 1 - YES ask. That's what we could sell our NO for.
                    no_bid = round(1.0 - best_ask, 6) if best_ask is not None else None
                    check_price = no_bid
                    should_stop = no_bid is not None and no_bid <= STOP_LOSS
                else:
                    # For YES: check YES bid (what we could actually sell at).
                    # Never use last_price — it can be stale by minutes.
                    check_price = best_bid
                    should_stop = best_bid is not None and best_bid <= STOP_LOSS

                if should_stop:
                    side = self._position_side
                    logger.warning(
                        "[97c] STOP LOSS: %s  %s_bid=%.4f  entry=%.4f  side=%s",
                        ticker,
                        "no" if side == "NO" else "yes",
                        check_price, self._entry_price or 0, side,
                    )
                    return Signal(
                        ticker=ticker,
                        market_id=market_id,
                        signal_type=SignalType.STOP_LOSS,
                        price=check_price,
                        metadata={
                            "engine":      "96c",
                            "side":        side,
                            "entry_price": self._entry_price,
                        },
                    )
                return None

            # ── STANDING BY: suppress during cooldown or pending entry ───
            # _pending_entry stays True from signal generation until
            # mark_position_open() is called, blocking duplicate signals
            # in the window before the optimistic lock takes effect.
            if self._pending_entry or self._in_cooldown(ticker):
                return None

            # ── Check YES side (buy YES when BTC is going UP) ─────────
            yes_ask_valid = (
                    best_ask is not None
                    and BUY_MIN <= best_ask <= BUY_MAX
            )

            # ── Check NO side (buy NO when BTC is going DOWN) ─────────
            no_ask = None
            no_ask_valid = False
            if best_bid is not None and best_bid > 0:
                no_ask = round(1.0 - best_bid, 6)
                no_ask_valid = BUY_MIN <= no_ask <= BUY_MAX

            # Strictly require one side to pass thresholds
            if yes_ask_valid:
                side     = "YES"
                entry_px = best_ask
            elif no_ask_valid:
                side     = "NO"
                entry_px = no_ask
            else:
                # Nothing actionable — silent return, no log noise
                return None

            # ── Defensive double-check: reject if bounds violated ─────
            # Catches any future threshold-config mistakes
            if not (BUY_MIN <= entry_px <= BUY_MAX):
                logger.error(
                    "[97c] REJECTED bad entry_px=%.4f  side=%s  yes_ask=%s  no_ask=%s  "
                    "(bounds %.2f-%.2f)",
                    entry_px, side, best_ask, no_ask, BUY_MIN, BUY_MAX,
                )
                return None

            self._position_side  = side
            self._pending_entry  = True   # block further signals until mark_position_open()

        logger.info(
            "[97c] SIGNAL: %s  side=%s  entry=%.4f  (yes_ask=%s no_ask=%s)",
            ticker, side, entry_px, best_ask, no_ask,
        )

        return Signal(
            ticker=ticker,
            market_id=market_id,
            signal_type=SignalType.ENTRY,
            price=entry_px,
            metadata={
                "engine":   "96c",
                "side":     side,
                "best_ask": best_ask,
                "best_bid": best_bid,
                "yes_ask":  best_ask,
                "no_ask":   no_ask,
            },
        )
