from __future__ import annotations

import logging
import threading
import time
from typing import Optional, ClassVar, Set, Any

from pykalshi import KalshiClient, Feed, TickerMessage

from core.config import Config
from core import event_bus
from core.event_bus import MarketUpdate
from core.market_fetcher import get_market_snapshot
from core.models import Tick
from core.risk_manager import RiskManager
from core.signal_engine_router import SignalEngineRouter
from db.db import Database

logger = logging.getLogger(__name__)

_SENTINEL_PRICES = {0.0, 1.0}

def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        return v if v != 0.0 else None
    except (TypeError, ValueError):
        return None

def parse_btc_target(ticker: str) -> Optional[float]:
    """Extracts the BTC strike price from ticker (e.g. KXBTC15M-26APR171015-45 → 45000.0)."""
    try:
        parts = ticker.split('-')
        if len(parts) >= 3:
            raw_target = parts[-1]  # last segment is the strike in thousands
            return float(raw_target) * 1000.0
    except Exception:
        pass
    return None


def resolve_price_from_dict(snapshot: dict) -> Optional[float]:
    for key in ("last_price", "best_ask", "best_bid"):
        v = snapshot.get(key)
        if v is not None and v not in _SENTINEL_PRICES:
            return v
    return None

class MarketWorker(threading.Thread):
    _shared_feed: Optional[Feed] = None
    _subscribed_tickers: ClassVar[Set[str]] = set()
    _feed_lock = threading.Lock()

    # Track last update globally per ticker so workers know if WS is alive
    _last_ws_update: ClassVar[dict[str, float]] = {}

    # Maps ticker → active worker so the shared WS callback can dispatch
    # to whichever worker currently owns that market after rotations.
    _ticker_to_worker: ClassVar[dict[str, 'MarketWorker']] = {}

    def __init__(
            self,
            client: KalshiClient,
            ticker: str,
            market_id: int,
            db: Database,
            signal_engine: SignalEngineRouter,
            risk_manager: RiskManager,
            config: Config,
    ) -> None:
        super().__init__(daemon=True, name=f"worker-{ticker[:20]}")
        self._client = client
        self._ticker = ticker
        self._market_id = market_id
        self._db = db
        self._signal_engine = signal_engine
        self._risk_manager = risk_manager
        self._config = config
        self._stop_event = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        MarketWorker._ticker_to_worker[self._ticker] = self
        delay = self._config.worker_restart_delay
        try:
            while not self._stop_event.is_set():
                try:
                    logger.info("[%s] Worker starting", self._ticker)
                    self._run_stream()
                except Exception as exc:
                    if self._stop_event.is_set():
                        break
                    logger.error("[%s] Worker crashed: %s", self._ticker, exc)
                    time.sleep(delay)
        finally:
            MarketWorker._ticker_to_worker.pop(self._ticker, None)

    def _run_stream(self) -> None:
        self._push_initial_snapshot()
        self._setup_shared_feed()

        last_poll = time.time()
        # Only poll REST if the WebSocket hasn't sent an update in 10 seconds
        WATCHDOG_TIMEOUT = 10.0
        POLL_INTERVAL = 3.5

        while not self._stop_event.is_set():
            time.sleep(0.5)
            now = time.time()

            last_ws = MarketWorker._last_ws_update.get(self._ticker, 0)
            ws_stale = (now - last_ws) > WATCHDOG_TIMEOUT

            if ws_stale and (now - last_poll >= POLL_INTERVAL):
                last_poll = now
                logger.debug("[%s] WS stale, falling back to REST poll", self._ticker)
                self._do_safe_poll()

    def _setup_shared_feed(self) -> None:
        with MarketWorker._feed_lock:
            if MarketWorker._shared_feed is None:
                try:
                    feed = Feed(self._client)
                    MarketWorker._shared_feed = feed

                    def handle_msg(msg: TickerMessage) -> None:
                        # Extract ticker - V2 Feed might nest this
                        ticker = getattr(msg, "market_ticker", None) or getattr(msg, "ticker", None)
                        if not ticker or ticker not in MarketWorker._subscribed_tickers:
                            return

                        # Update watchdog
                        MarketWorker._last_ws_update[ticker] = time.time()

                        # Helper to pull fields from msg or nested msg.market
                        def get_v2(m, attr):
                            val = getattr(m, attr, None)
                            if val is None and hasattr(m, 'market'):
                                val = getattr(m.market, attr, None)
                            return val

                        raw_bid   = _safe_float(get_v2(msg, "yes_bid_dollars"))
                        raw_ask   = _safe_float(get_v2(msg, "yes_ask_dollars"))
                        volume    = _safe_float(get_v2(msg, "volume_fp"))

                        # Resolve Price logic
                        raw_price = None
                        for attr in ("price_dollars", "last_price_dollars", "yes_ask_dollars", "yes_bid_dollars"):
                            v = _safe_float(get_v2(msg, attr))
                            if v is not None and v not in _SENTINEL_PRICES:
                                raw_price = v
                                break

                        target_px = parse_btc_target(ticker)

                        event_bus.push_market(MarketUpdate(
                            ticker=ticker,
                            market_id=0,
                            price=raw_price,
                            bid=raw_bid,
                            ask=raw_ask,
                            volume=volume,
                            target=target_px,
                        ))

                        # Dispatch to whichever worker currently owns this ticker.
                        # Using a class-level dict instead of closing over self._ticker
                        # so that market rotations work correctly — after a rotation,
                        # the new worker is the registered owner and receives ticks.
                        target = MarketWorker._ticker_to_worker.get(ticker)
                        if target is not None and raw_price is not None:
                            target._on_tick(raw_price, raw_bid, raw_ask, volume)

                    feed.on("ticker")(handle_msg)
                    feed.start()
                    logger.info("Shared WebSocket started.")
                except Exception as e:
                    logger.warning("Failed to start shared feed: %s", e)
                    MarketWorker._shared_feed = None
                    return

            if self._ticker not in MarketWorker._subscribed_tickers:
                try:
                    MarketWorker._subscribed_tickers.add(self._ticker)
                    MarketWorker._shared_feed.subscribe("ticker", market_ticker=self._ticker)
                except Exception as e:
                    logger.warning("Failed to subscribe %s: %s", self._ticker, e)

    def _push_initial_snapshot(self) -> None:
        snap = get_market_snapshot(self._client, self._ticker)
        if snap:
            self._handle_snapshot(snap)

    def _do_safe_poll(self) -> None:
        try:
            snap = get_market_snapshot(self._client, self._ticker)
            if snap:
                self._handle_snapshot(snap)
        except Exception as e:
            logger.debug("[%s] Poll failed: %s", self._ticker, e)

    def _handle_snapshot(self, snap: dict) -> None:
        target_px = parse_btc_target(self._ticker)
        event_bus.push_market(MarketUpdate(
            ticker=self._ticker,
            market_id=self._market_id,
            price=snap.get("last_price"),
            bid=snap.get("best_bid"),
            ask=snap.get("best_ask"),
            volume=snap.get("volume"),
            target=target_px,
        ))
        sig_price = resolve_price_from_dict(snap)
        if sig_price is not None:
            self._on_tick(sig_price, snap.get("best_bid"), snap.get("best_ask"), snap.get("volume"))

    def _on_tick(self, price: float, bid: Optional[float], ask: Optional[float], vol: Optional[float]) -> None:
        try:
            self._db.insert_tick(self._market_id, bid, ask, price, vol)
            sig = self._signal_engine.process_tick(self._ticker, self._market_id, price, bid, ask)
            if sig:
                self._risk_manager.handle_signal(sig, bid, ask)
        except Exception as exc:
            logger.error("[%s] Tick processing error: %s", self._ticker, exc)