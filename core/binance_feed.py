"""
binance_feed.py — Binance BTC/USDT spot WebSocket feed.

Provides:
  - Rolling CVD (Cumulative Volume Delta) from aggTrade stream
  - Current mid price from bookTicker stream
  - Price change direction for cross_asset_boost in EVSignalEngine

No API key required — uses public Binance WebSocket streams.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import deque
from typing import Optional

import websocket

logger = logging.getLogger(__name__)

WS_URL = (
    "wss://stream.binance.com:9443/stream"
    "?streams=btcusdt@aggTrade/btcusdt@bookTicker"
)
CVD_WINDOW  = 200   # rolling trade count for CVD calculation
MAX_ERRORS  = 20    # stop reconnecting after this many consecutive failures
RECONNECT_DELAY = 5.0

_instance: Optional["BinanceFeed"] = None
_instance_lock = threading.Lock()


def get_instance() -> "BinanceFeed":
    """Return the singleton BinanceFeed, starting it on first call."""
    global _instance
    with _instance_lock:
        if _instance is None:
            _instance = BinanceFeed()
            _instance.start()
    return _instance


class BinanceFeed(threading.Thread):
    """
    Background thread streaming Binance BTC spot data.

    aggTrade stream: computes rolling CVD.
      - m=False → taker buy  (bullish) → +qty
      - m=True  → taker sell (bearish) → -qty

    bookTicker stream: maintains current best bid/ask and mid price.
    """

    def __init__(self) -> None:
        super().__init__(daemon=True, name="binance-spot-feed")
        self._lock        = threading.Lock()
        self._stop_event  = threading.Event()
        self._error_count = 0

        # bookTicker state
        self._bid: Optional[float] = None
        self._ask: Optional[float] = None
        self._mid: Optional[float] = None

        # aggTrade CVD — rolling deque of signed quantities
        self._trade_qtys: deque = deque(maxlen=CVD_WINDOW)
        self._cvd_sum: float = 0.0        # running sum of deque contents

        self._ws: Optional[websocket.WebSocketApp] = None

    # ── Public properties ─────────────────────────────────────────────

    @property
    def mid_price(self) -> Optional[float]:
        """Latest BTC/USDT mid price from bookTicker."""
        with self._lock:
            return self._mid

    @property
    def cvd(self) -> float:
        """
        Cumulative Volume Delta over last CVD_WINDOW trades.
        Positive = net taker buying (bullish).
        Normalized to [-1, 1] by dividing by sum of absolute values.
        """
        with self._lock:
            if not self._trade_qtys:
                return 0.0
            abs_sum = sum(abs(q) for q in self._trade_qtys)
            return self._cvd_sum / abs_sum if abs_sum > 0 else 0.0

    @property
    def is_connected(self) -> bool:
        with self._lock:
            return self._mid is not None

    # ── Thread lifecycle ──────────────────────────────────────────────

    def stop(self) -> None:
        self._stop_event.set()
        if self._ws:
            self._ws.close()

    def run(self) -> None:
        logger.info("BinanceFeed started")
        while not self._stop_event.is_set():
            if self._error_count >= MAX_ERRORS:
                logger.error("BinanceFeed stopping — too many errors (%d)", MAX_ERRORS)
                break
            try:
                self._connect_and_run()
            except Exception as exc:
                self._error_count += 1
                logger.warning(
                    "BinanceFeed error #%d: %s — reconnecting in %.0fs",
                    self._error_count, exc, RECONNECT_DELAY,
                )
            if not self._stop_event.is_set():
                self._stop_event.wait(RECONNECT_DELAY)
        logger.info("BinanceFeed stopped")

    def _connect_and_run(self) -> None:
        self._ws = websocket.WebSocketApp(
            WS_URL,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
            on_open=self._on_open,
        )
        self._ws.run_forever(ping_interval=20, ping_timeout=10)

    # ── WebSocket callbacks ───────────────────────────────────────────

    def _on_open(self, ws) -> None:
        self._error_count = 0
        logger.info("BinanceFeed WebSocket connected")

    def _on_error(self, ws, error) -> None:
        logger.warning("BinanceFeed WS error: %s", error)

    def _on_close(self, ws, code, msg) -> None:
        logger.info("BinanceFeed WS closed (code=%s)", code)

    def _on_message(self, ws, raw: str) -> None:
        try:
            outer  = json.loads(raw)
            stream = outer.get("stream", "")
            data   = outer.get("data", {})
        except Exception:
            return

        if "bookTicker" in stream:
            self._handle_book_ticker(data)
        elif "aggTrade" in stream:
            self._handle_agg_trade(data)

    # ── Message handlers ──────────────────────────────────────────────

    def _handle_book_ticker(self, data: dict) -> None:
        try:
            bid = float(data["b"])
            ask = float(data["a"])
            mid = (bid + ask) / 2.0
            with self._lock:
                self._bid = bid
                self._ask = ask
                self._mid = mid
        except (KeyError, ValueError, TypeError):
            pass

    def _handle_agg_trade(self, data: dict) -> None:
        """
        m=True  → market sell (taker is seller) → bearish → -qty
        m=False → market buy  (taker is buyer)  → bullish → +qty
        """
        try:
            qty          = float(data["q"])
            is_sell_side = bool(data["m"])
            signed_qty   = -qty if is_sell_side else qty

            with self._lock:
                # Maintain running sum: subtract evicted element if deque is full
                if len(self._trade_qtys) == CVD_WINDOW:
                    evicted = self._trade_qtys[0]
                    self._cvd_sum -= evicted
                self._trade_qtys.append(signed_qty)
                self._cvd_sum += signed_qty
        except (KeyError, ValueError, TypeError):
            pass
