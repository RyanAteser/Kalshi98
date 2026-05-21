"""
binance_futures_feed.py — Binance BTC/USDT futures data feed.

Provides via REST polling (no API key required):
  - Mark price and funding rate from premiumIndex endpoint
  - Open Interest delta (OI change between polls)

Used for cross_asset_boost and cvd_boost enrichment in EVSignalEngine.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import urllib.request
from collections import deque
from typing import Optional

logger = logging.getLogger(__name__)

FUTURES_MARK_URL = "https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT"
FUTURES_OI_URL   = "https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT"
MARK_POLL_INTERVAL = 10.0    # seconds between mark price polls
OI_POLL_INTERVAL   = 30.0    # seconds between OI polls
OI_HISTORY_LEN     = 6       # keep 6 OI samples (3 min at 30s cadence)
MAX_ERRORS         = 20

_instance: Optional["BinanceFuturesFeed"] = None
_instance_lock = threading.Lock()


def get_instance() -> "BinanceFuturesFeed":
    """Return the singleton BinanceFuturesFeed, starting it on first call."""
    global _instance
    with _instance_lock:
        if _instance is None:
            _instance = BinanceFuturesFeed()
            _instance.start()
    return _instance


class BinanceFuturesFeed(threading.Thread):
    """
    Background thread that polls Binance futures endpoints.

    mark_price: latest futures mark price for BTC/USDT
    funding_rate: current 8-hour funding rate (positive = longs pay shorts)
    oi_delta: fractional OI change over the OI_HISTORY_LEN window
              (positive = open interest growing = momentum)
    """

    def __init__(self) -> None:
        super().__init__(daemon=True, name="binance-futures-feed")
        self._lock        = threading.Lock()
        self._stop_event  = threading.Event()
        self._error_count = 0

        self._mark_price:   Optional[float] = None
        self._funding_rate: float = 0.0
        self._oi_history:   deque = deque(maxlen=OI_HISTORY_LEN)
        self._oi_delta:     float = 0.0

    # ── Public properties ─────────────────────────────────────────────

    @property
    def mark_price(self) -> Optional[float]:
        with self._lock:
            return self._mark_price

    @property
    def funding_rate(self) -> float:
        """Current 8-hour funding rate. Positive = contango (longs bullish)."""
        with self._lock:
            return self._funding_rate

    @property
    def oi_delta(self) -> float:
        """
        Fractional OI change over recent history window.
        Positive = growing OI (new positions being opened, momentum building).
        Clamped to [-0.10, 0.10].
        """
        with self._lock:
            return self._oi_delta

    # ── Thread lifecycle ──────────────────────────────────────────────

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        logger.info("BinanceFuturesFeed started")
        last_oi_poll = 0.0

        while not self._stop_event.is_set():
            if self._error_count >= MAX_ERRORS:
                logger.error("BinanceFuturesFeed stopping — too many errors")
                break

            now = time.time()

            try:
                self._poll_mark_price()
                self._error_count = 0
            except Exception as exc:
                self._error_count += 1
                logger.debug("Futures mark price poll failed: %s", exc)

            if now - last_oi_poll >= OI_POLL_INTERVAL:
                try:
                    self._poll_oi()
                    last_oi_poll = now
                except Exception as exc:
                    logger.debug("Futures OI poll failed: %s", exc)

            self._stop_event.wait(MARK_POLL_INTERVAL)

        logger.info("BinanceFuturesFeed stopped")

    # ── Polling methods ───────────────────────────────────────────────

    def _fetch_json(self, url: str) -> dict:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "kalshi-trader/1.0",
                "Accept":     "application/json",
            },
        )
        with urllib.request.urlopen(req, timeout=8) as resp:
            return json.loads(resp.read().decode())

    def _poll_mark_price(self) -> None:
        data = self._fetch_json(FUTURES_MARK_URL)
        mp   = float(data["markPrice"])
        fr   = float(data.get("lastFundingRate", 0.0))
        with self._lock:
            self._mark_price   = mp
            self._funding_rate = fr

    def _poll_oi(self) -> None:
        data = self._fetch_json(FUTURES_OI_URL)
        oi   = float(data["openInterest"])
        with self._lock:
            self._oi_history.append(oi)
            if len(self._oi_history) >= 2:
                oldest = self._oi_history[0]
                if oldest > 0:
                    raw_delta = (oi - oldest) / oldest
                    # Clamp to [-0.10, 0.10]
                    self._oi_delta = max(-0.10, min(0.10, raw_delta))
                else:
                    self._oi_delta = 0.0
