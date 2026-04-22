"""
signal_engine_router.py — Hot-swappable wrapper around all signal engines.

Engines:
  momentum   — Momentum crossover at 52c (coin-flip games)
  resolution — Near-certain resolution scalper at 97c
  simple96   — Buy YES or NO at 97–99c, stop loss at 90c
"""

from __future__ import annotations

import logging
import threading
from typing import Optional, Literal

from core.config import Config
from core.models import Signal
from core.signal_engine import SignalEngine
from core.signal_engine_resolution import ResolutionSignalEngine
from core.signal_engine_96 import Simple96Engine

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
        self._active_key: EngineKey = "simple96"
        self._active     = self._simple96
        logger.info("SignalEngineRouter initialised — active: simple96")

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

    def process_tick(self, ticker, market_id, price, best_bid, best_ask) -> Optional[Signal]:
        with self._lock:
            engine = self._active
        return engine.process_tick(ticker, market_id, price, best_bid, best_ask)

    def get_or_create_state(self, ticker: str, market_id: int):
        with self._lock:
            engine = self._active
        if hasattr(engine, "get_or_create_state"):
            return engine.get_or_create_state(ticker, market_id)
        return None

    def mark_position_open(self, ticker: str, position_id: int, entry_price: float) -> None:
        for e in (self._momentum, self._resolution, self._simple96):
            e.mark_position_open(ticker, position_id, entry_price)

    def mark_position_closed(self, ticker: str) -> None:
        for e in (self._momentum, self._resolution, self._simple96):
            e.mark_position_closed(ticker)

    def get_position_snapshot(self, ticker: str) -> Optional[dict]:
        with self._lock:
            engine = self._active
        return engine.get_position_snapshot(ticker)