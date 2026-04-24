"""
btc_feed.py — Fetches BTC/USD 15-minute OHLCV candles from Coinbase.

Runs as a background thread. Every POLL_SECONDS it hits the Coinbase
Exchange public API and pushes updated candles to any registered callbacks.

Coinbase candle format (each row):
  [timestamp_unix, price_low, price_high, price_open, price_close, volume]
  Rows are returned newest-first.

No API key needed — candles endpoint is public.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import urllib.request
from dataclasses import dataclass
from typing import Callable, List, Optional

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────
COINBASE_URL  = "https://api.exchange.coinbase.com/products/BTC-USD/candles"
GRANULARITY   = 900      # 15 minutes in seconds
CANDLE_LIMIT  = 60       # fetch last 60 candles (~15 hours of history)
POLL_SECONDS  = 15       # re-fetch every 15s to get fresh current candle
MAX_ERRORS    = 10


@dataclass
class Candle:
    ts:     int     # unix timestamp of candle open
    low:    float
    high:   float
    open:   float
    close:  float
    volume: float

    @property
    def is_bullish(self) -> bool:
        return self.close >= self.open

    @property
    def body_size(self) -> float:
        return abs(self.close - self.open)

    @property
    def range_size(self) -> float:
        return self.high - self.low


class BtcFeed(threading.Thread):
    """
    Background thread that polls Coinbase for BTC 15m candles.

    Usage:
        feed = BtcFeed()
        feed.on_update(my_callback)   # called with List[Candle] newest-first
        feed.start()
        ...
        feed.stop()
    """

    def __init__(self) -> None:
        super().__init__(daemon=True, name="btc-feed")
        self._stop_event  = threading.Event()
        self._callbacks:  List[Callable[[List[Candle]], None]] = []
        self._last_candles: List[Candle] = []
        self._lock        = threading.Lock()
        self._error_count = 0

    def on_update(self, callback: Callable[[List[Candle]], None]) -> None:
        """Register a callback that receives the full candle list on every update."""
        self._callbacks.append(callback)

    @property
    def latest_candles(self) -> List[Candle]:
        with self._lock:
            return list(self._last_candles)

    @property
    def current_price(self) -> Optional[float]:
        """Most recent close price."""
        with self._lock:
            return self._last_candles[0].close if self._last_candles else None

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        logger.info("BTC feed started (15m candles, poll=%.0fs)", POLL_SECONDS)
        while not self._stop_event.is_set():
            try:
                candles = self._fetch()
                if candles:
                    with self._lock:
                        self._last_candles = candles
                    for cb in self._callbacks:
                        try:
                            cb(candles)
                        except Exception as e:
                            logger.warning("BTC feed callback error: %s", e)
                self._error_count = 0
            except Exception as exc:
                self._error_count += 1
                logger.warning(
                    "BTC feed fetch failed (%d/%d): %s",
                    self._error_count, MAX_ERRORS, exc,
                )
                if self._error_count >= MAX_ERRORS:
                    logger.error("BTC feed stopping — too many errors")
                    break

            self._stop_event.wait(POLL_SECONDS)

        logger.info("BTC feed stopped")

    def _fetch(self) -> List[Candle]:
        """
        Fetch candles from Coinbase Exchange public API.
        Returns list of Candle objects, newest-first.
        Handles both old list-of-lists format and new dict-with-candles format.
        """
        url = f"{COINBASE_URL}?granularity={GRANULARITY}"
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; btc-trader/1.0)",
                "Accept":     "application/json",
            }
        )

        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = json.loads(resp.read().decode())

        candles = self._parse_candles(raw)

        if not candles:
            logger.warning(
                "BTC feed: 0 candles parsed — raw type=%s snippet=%.200s",
                type(raw).__name__, str(raw),
            )
        else:
            logger.info(
                "BTC feed: %d candles, latest close=%.2f",
                len(candles), candles[0].close,
            )
        return candles

    @staticmethod
    def _parse_candles(raw) -> List[Candle]:
        """Parse Coinbase response into Candle list (newest-first)."""
        if isinstance(raw, list):
            rows = raw
        elif isinstance(raw, dict) and "candles" in raw:
            rows = raw["candles"]
        else:
            return []

        candles: List[Candle] = []
        for row in rows:
            try:
                if isinstance(row, dict):
                    candles.append(Candle(
                        ts=int(row["start"]),
                        low=float(row["low"]),
                        high=float(row["high"]),
                        open=float(row["open"]),
                        close=float(row["close"]),
                        volume=float(row["volume"]),
                    ))
                elif hasattr(row, "__len__") and len(row) >= 6:
                    candles.append(Candle(
                        ts=int(row[0]),
                        low=float(row[1]),
                        high=float(row[2]),
                        open=float(row[3]),
                        close=float(row[4]),
                        volume=float(row[5]),
                    ))
            except (KeyError, TypeError, ValueError, IndexError):
                continue

        # Normalise to newest-first regardless of source format
        candles.sort(key=lambda c: c.ts, reverse=True)
        return candles