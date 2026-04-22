"""event_bus.py — Lightweight in-process pub/sub event bus.

Publishers call push_*(). Subscribers call subscribe_*().
All callbacks are fired synchronously on the publishing thread.
Thread-safe.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from typing import Callable, List, Optional

logger = logging.getLogger(__name__)


# ── Event types ───────────────────────────────────────────────────────

@dataclass
class MarketUpdate:
    ticker:    str
    market_id: int
    price:     Optional[float] = None
    bid:       Optional[float] = None
    ask:       Optional[float] = None
    volume:    Optional[float] = None
    target:    Optional[float] = None  # BTC strike price (e.g. 45000.0)


@dataclass
class TradeEvent:
    ticker: str
    side:   str        # "BUY" or "SELL"
    price:  float
    qty:    int
    pnl:    Optional[float] = None


@dataclass
class SignalEvent:
    ticker:      str
    signal_type: str   # "ENTRY", "STOP_LOSS", etc.
    price:       float


# ── Internal registry ─────────────────────────────────────────────────

_lock = threading.Lock()

_market_callbacks: List[Callable[[MarketUpdate], None]] = []
_trade_callbacks:  List[Callable[[TradeEvent],   None]] = []
_signal_callbacks: List[Callable[[SignalEvent],  None]] = []


# ── Subscribe ─────────────────────────────────────────────────────────

def subscribe_market(cb: Callable[[MarketUpdate], None]) -> None:
    with _lock:
        _market_callbacks.append(cb)

def subscribe_trade(cb: Callable[[TradeEvent], None]) -> None:
    with _lock:
        _trade_callbacks.append(cb)

def subscribe_signal(cb: Callable[[SignalEvent], None]) -> None:
    with _lock:
        _signal_callbacks.append(cb)


# ── Publish ───────────────────────────────────────────────────────────

def push_market(event: MarketUpdate) -> None:
    with _lock:
        cbs = list(_market_callbacks)
    for cb in cbs:
        try:
            cb(event)
        except Exception as e:
            logger.warning("market callback error: %s", e)

def push_trade(event: TradeEvent) -> None:
    with _lock:
        cbs = list(_trade_callbacks)
    for cb in cbs:
        try:
            cb(event)
        except Exception as e:
            logger.warning("trade callback error: %s", e)

def push_signal(event: SignalEvent) -> None:
    with _lock:
        cbs = list(_signal_callbacks)
    for cb in cbs:
        try:
            cb(event)
        except Exception as e:
            logger.warning("signal callback error: %s", e)