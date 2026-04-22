"""Data Sharing Across All Module"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class SignalType(str, Enum):
    ENTRY = "ENTRY"
    EXIT = "EXIT"
    STOP_LOSS = "STOP_LOSS"


class PositionStatus(str, Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass
class Tick:
    """Normalized market tick."""
    ticker: str
    best_bid: Optional[float]
    best_ask: Optional[float]
    last_price: Optional[float]
    volume: Optional[float]
    ts: int  # unix epoch from feed


@dataclass
class MarketState:
    """Per-market state tracked by signal engine."""
    ticker: str
    market_id: int
    prev_price: Optional[float] = None
    last_price: Optional[float] = None
    has_position: bool = False
    position_id: Optional[int] = None
    entry_price: Optional[float] = None
    crossover_fired: bool = False  # prevent duplicate entry signals


@dataclass
class Signal:
    ticker: str
    market_id: int
    signal_type: SignalType
    price: float
    metadata: dict = field(default_factory=dict)


@dataclass
class OrderResult:
    success: bool
    order_id: Optional[str]
    filled_price: Optional[float]
    filled_qty: int
    error: Optional[str] = None