"""
signal_engine_t2t.py — Time-to-Target physics engine.

Entry logic:
  1. Compute distance = |btc_current - btc_target|
  2. Compute time_remaining = close_ts - now  (seconds)
  3. velocity_needed = distance / time_remaining
  4. If velocity_needed > MAX_BTC_VELOCITY → physically impossible to reach target.
     → The LOSING side (contracts betting BTC hits the target) should crash to near 0.
     → The WINNING side (contracts betting BTC stays away from target) should be near 1.
  5. Enter on the WINNING side when ask <= MAX_WINNING_ASK (market hasn't fully priced it in).

Only fires in the final MAX_ENTRY_SECS seconds of the contract window.
Stop loss: entry - FIXED_RISK (same proportional logic as simple96).
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Optional, TYPE_CHECKING

from core.models import Signal, SignalType

if TYPE_CHECKING:
    from core.btc_feed import BtcFeed

logger = logging.getLogger(__name__)

MAX_BTC_VELOCITY = 10.0   # $/s — max plausible BTC movement speed
MAX_ENTRY_SECS   = 120    # only enter in the last 2 minutes of the contract
MIN_SECS_TO_FILL = 3      # need at least 3s remaining after entry
MAX_WINNING_ASK  = 0.95   # winning side must be below this (market under-pricing certainty)
FIXED_RISK       = 0.02   # stop = entry - FIXED_RISK


class TimeToTargetEngine:

    def __init__(self, btc_feed: "BtcFeed") -> None:
        self._btc_feed = btc_feed
        self._lock     = threading.Lock()
        self._reset()
        self._cooldown_until: dict[str, float] = {}
        # Per-ticker context (set by update_context)
        self._btc_target: dict[str, Optional[float]] = {}
        self._close_ts:   dict[str, Optional[datetime]] = {}

    def _reset(self) -> None:
        self._has_position     = False
        self._pending_entry    = False
        self._position_ticker: Optional[str]   = None
        self._position_side:   Optional[str]   = None
        self._entry_price:     Optional[float] = None
        self._stop_price:      Optional[float] = None
        self._position_id:     Optional[int]   = None

    # ── Context updates (called by worker after each snapshot) ────────

    def update_context(
            self, ticker: str,
            btc_target: Optional[float],
            close_ts: Optional[datetime],
    ) -> None:
        with self._lock:
            self._btc_target[ticker] = btc_target
            self._close_ts[ticker]   = close_ts

    # ── Cooldown ──────────────────────────────────────────────────────

    def mark_cooldown(self, ticker: str, duration: float = 30.0) -> None:
        with self._lock:
            self._cooldown_until[ticker] = time.time() + duration
        logger.info("[T2T] Cooldown: %s blocked for %.0fs", ticker, duration)

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
            self._stop_price      = round(entry_price - FIXED_RISK, 6)
            self._position_id     = position_id
            if side is not None:
                self._position_side = side
        logger.info(
            "[T2T] IN: %s @ %.4f  stop=%.4f  side=%s  id=%d",
            ticker, entry_price, self._stop_price or 0, self._position_side, position_id,
        )

    def get_stop_price(self) -> Optional[float]:
        with self._lock:
            return self._stop_price

    def mark_position_closed(self, ticker: str) -> None:
        with self._lock:
            self._reset()
        logger.info("[T2T] CLOSED: %s — re-armed", ticker)

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

    # ── Physics helpers ───────────────────────────────────────────────

    def _secs_remaining(self, ticker: str) -> Optional[float]:
        close = self._close_ts.get(ticker)
        if close is None:
            return None
        now = datetime.now(tz=timezone.utc)
        if close.tzinfo is None:
            close = close.replace(tzinfo=timezone.utc)
        return (close - now).total_seconds()

    def _velocity_needed(self, btc_current: float, btc_target: float, secs: float) -> float:
        return abs(btc_current - btc_target) / secs

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
                    logger.info(
                        "[T2T] Auto-release: active ticker=%s but got tick for %s — clearing",
                        self._position_ticker, ticker,
                    )
                    self._reset()
                    # fall through to entry check
                else:
                    stop = self._stop_price
                    if self._position_side == "NO":
                        no_bid    = round(1.0 - best_ask, 6) if best_ask is not None else None
                        check_price = no_bid
                        should_stop = stop is not None and no_bid is not None and no_bid <= stop
                    else:
                        check_price = best_bid
                        should_stop = stop is not None and best_bid is not None and best_bid <= stop

                    if should_stop:
                        side = self._position_side
                        logger.warning(
                            "[T2T] STOP LOSS: %s  bid=%.4f  entry=%.4f  side=%s",
                            ticker, check_price, self._entry_price or 0, side,
                        )
                        return Signal(
                            ticker=ticker,
                            market_id=market_id,
                            signal_type=SignalType.STOP_LOSS,
                            price=check_price,
                            metadata={
                                "engine":      "t2t",
                                "side":        side,
                                "entry_price": self._entry_price,
                            },
                        )
                    return None

            if self._pending_entry or self._in_cooldown(ticker):
                return None

            # ── Physics check ─────────────────────────────────────────
            btc_target = self._btc_target.get(ticker)
            if btc_target is None or btc_target == 0.0:
                return None

            btc_current = self._btc_feed.current_price
            if btc_current is None:
                return None

            secs = self._secs_remaining(ticker)
            if secs is None or secs < MIN_SECS_TO_FILL or secs > MAX_ENTRY_SECS:
                return None

            velocity = self._velocity_needed(btc_current, btc_target, secs)
            if velocity <= MAX_BTC_VELOCITY:
                # Target is still reachable — no edge
                return None

            # ── Target is physically impossible to reach ──────────────
            # Determine which side WINS (the side betting BTC stays away from target)
            # If btc_current > btc_target → DOWN move needed → YES (above) is winning
            # If btc_current < btc_target → UP move needed → NO (below) is winning
            btc_above_target = btc_current > btc_target
            winning_side = "YES" if btc_above_target else "NO"

            if winning_side == "YES":
                entry_px = best_ask
                if entry_px is None or entry_px > MAX_WINNING_ASK:
                    return None
            else:
                if best_bid is None or best_bid <= 0:
                    return None
                no_ask   = round(1.0 - best_bid, 6)
                entry_px = no_ask
                if entry_px > MAX_WINNING_ASK:
                    return None

            side = winning_side
            self._position_side = side
            self._pending_entry = True

        logger.info(
            "[T2T] SIGNAL: %s  side=%s  entry=%.4f  btc=%.2f  target=%.2f  "
            "secs=%.1f  velocity_needed=%.2f$/s",
            ticker, side, entry_px, btc_current, btc_target, secs, velocity,
        )

        return Signal(
            ticker=ticker,
            market_id=market_id,
            signal_type=SignalType.ENTRY,
            price=entry_px,
            metadata={
                "engine":           "t2t",
                "side":             side,
                "btc_current":      btc_current,
                "btc_target":       btc_target,
                "secs_remaining":   secs,
                "velocity_needed":  velocity,
                "best_ask":         best_ask,
                "best_bid":         best_bid,
            },
        )
