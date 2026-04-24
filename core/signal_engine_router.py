"""
signal_engine_router.py — Hot-swappable wrapper around all signal engines.

Engines:
  momentum   — Momentum crossover at 52c (coin-flip games)
  resolution — Near-certain resolution scalper at 97c
  simple96   — Buy YES or NO at 97c, proportional stop loss
  t2t        — Time-to-target physics engine (parallel, not swappable)

T2T runs in parallel with whichever engine is active: it fires independently
on every tick and its signals are returned alongside the active engine's signals.
"""

from __future__ import annotations

import logging
import threading
from typing import Optional, Literal, TYPE_CHECKING

from core.config import Config
from core.models import Signal
from core.signal_engine import SignalEngine
from core.signal_engine_resolution import ResolutionSignalEngine
from core.signal_engine_96 import Simple96Engine
from core.signal_engine_t2t import TimeToTargetEngine

if TYPE_CHECKING:
    from core.btc_feed import BtcFeed

logger = logging.getLogger(__name__)

EngineKey = Literal["momentum", "resolution", "simple96"]


class SignalEngineRouter:

    ENGINE_LABELS = {
        "momentum":   "Momentum Crossover (52c)",
        "resolution": "Resolution Scalper (97c)",
        "simple96":   "Simple Buy (97c)",
    }

    def __init__(self, config: Config) -> None:
        self._config     = config
        self._lock       = threading.Lock()
        self._momentum   = SignalEngine(config)
        self._resolution = ResolutionSignalEngine(config)
        self._simple96   = Simple96Engine()
        self._t2t:       Optional[TimeToTargetEngine] = None
        self._active_key: EngineKey = "simple96"
        self._active     = self._simple96
        logger.info("SignalEngineRouter initialised — active: simple96")

    def set_t2t_engine(self, btc_feed: "BtcFeed") -> None:
        """Wire up the T2T engine. Called once at startup after BtcFeed is ready."""
        self._t2t = TimeToTargetEngine(btc_feed)
        logger.info("T2T engine armed")

    def set_engine(self, key: EngineKey) -> None:
        with self._lock:
            if key == self._active_key:
                return
            self._active = {
                "momentum":   self._momentum,
                "resolution": self._resolution,
                "simple96":   self._simple96,
            }[key]
            self._active_key = key
            logger.info("Engine switched -> %s", self.ENGINE_LABELS[key])

    @property
    def active_key(self) -> EngineKey:
        return self._active_key

    @property
    def active_label(self) -> str:
        return self.ENGINE_LABELS[self._active_key]

    def update_t2t_context(self, ticker: str, btc_target, close_ts) -> None:
        if self._t2t is not None:
            self._t2t.update_context(ticker, btc_target, close_ts)

    def process_tick(self, ticker, market_id, price, best_bid, best_ask) -> Optional[Signal]:
        with self._lock:
            engine = self._active

        sig = engine.process_tick(ticker, market_id, price, best_bid, best_ask)
        if sig is not None:
            return sig

        # T2T runs in parallel — only fires when the active engine is silent
        if self._t2t is not None:
            return self._t2t.process_tick(ticker, market_id, price, best_bid, best_ask)

        return None

    def get_or_create_state(self, ticker: str, market_id: int):
        with self._lock:
            engine = self._active
        if hasattr(engine, "get_or_create_state"):
            return engine.get_or_create_state(ticker, market_id)
        return None

    def mark_position_open(
            self, ticker: str, position_id: int, entry_price: float,
            side: Optional[str] = None,
    ) -> None:
        for e in (self._momentum, self._resolution):
            e.mark_position_open(ticker, position_id, entry_price)
        self._simple96.mark_position_open(ticker, position_id, entry_price, side=side)
        if self._t2t is not None:
            self._t2t.mark_position_open(ticker, position_id, entry_price, side=side)

    def mark_position_closed(self, ticker: str) -> None:
        for e in (self._momentum, self._resolution, self._simple96):
            e.mark_position_closed(ticker)
        if self._t2t is not None:
            self._t2t.mark_position_closed(ticker)

    def get_position_snapshot(self, ticker: str) -> Optional[dict]:
        with self._lock:
            engine = self._active
        return engine.get_position_snapshot(ticker)

    def get_stop_price(self) -> Optional[float]:
        stop = self._simple96.get_stop_price()
        if stop is not None:
            return stop
        if self._t2t is not None:
            return self._t2t.get_stop_price()
        return None
