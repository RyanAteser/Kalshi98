"""
main.py — Entry point for the Kalshi momentum trading system.

Startup sequence:
  1. Load config from environment
  2. Initialize DB + schema
  3. Fetch active sports markets
  4. Start one MarketWorker per market
  5. Block until KeyboardInterrupt, then graceful shutdown
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import time
from typing import Optional

from dotenv import load_dotenv
from pykalshi import KalshiClient

from core.config import load_config
from core.execution_engine import ExecutionEngine
from core.market_fetcher import fetch_active_sports_markets
from core.risk_manager import RiskManager
from core.shadow_tracker import ShadowTracker
from core.shadow_vol_tracker import ShadowVolTracker
from core.signal_engine import SignalEngine
from core.worker import MarketWorker
from db.db import Database

load_dotenv()


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        handlers=[
            logging.StreamHandler(
                open(sys.stdout.fileno(), mode="w", encoding="utf-8", closefd=False)
            ),
            logging.FileHandler("kalshi_trader.log", encoding="utf-8"),
        ],
    )


def main() -> None:
    config = load_config()
    setup_logging(config.log_level)

    logger = logging.getLogger(__name__)
    logger.info(
        "Starting Kalshi Trader | paper_mode=%s max_markets=%d",
        config.paper_trade, config.max_markets,
    )

    # ── Database ─────────────────────────────────────────────────────
    db = Database()
    db.create_schema()

    # ── Kalshi client ────────────────────────────────────────────────
    client = KalshiClient.from_env()

    # ── Core components ──────────────────────────────────────────────
    signal_engine = SignalEngine(config)
    execution_engine = ExecutionEngine(client, config)
    risk_manager = RiskManager(db, signal_engine, execution_engine, config, client)
    risk_manager.set_shadow_tracker(ShadowTracker(db))
    risk_manager.set_shadow_vol_tracker(ShadowVolTracker(db))

    # ── Fetch markets ────────────────────────────────────────────────
    markets = []
    while not markets:
        markets = fetch_active_sports_markets(client, config)
        if not markets:
            logger.warning("No live binary sports markets found — retrying in 60s...")
            time.sleep(60)

    # ── Upsert markets into DB + start workers ───────────────────────
    workers: list[MarketWorker] = []

    for m in markets:
        ticker = m["ticker"]
        event = m.get("event", "")

        market_id = db.upsert_market(ticker, event)

        worker = MarketWorker(
            client=client,
            ticker=ticker,
            market_id=market_id,
            db=db,
            signal_engine=signal_engine,
            risk_manager=risk_manager,
            config=config,
        )
        workers.append(worker)
        worker.start()
        logger.info("Started worker: %s (market_id=%d)", ticker, market_id)

    logger.info("All %d workers running. Press Ctrl+C to stop.", len(workers))

    # ── Graceful shutdown ────────────────────────────────────────────
    def shutdown(sig, frame) -> None:
        logger.info("Shutdown signal received. Stopping workers...")
        for w in workers:
            w.stop()
        # Give threads a moment to exit cleanly
        for w in workers:
            w.join(timeout=3.0)
        logger.info("Shutdown complete.")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Keep main thread alive
    while True:
        alive = sum(1 for w in workers if w.is_alive())
        logger.debug("Workers alive: %d/%d", alive, len(workers))
        time.sleep(30)


if __name__ == "__main__":
    main()