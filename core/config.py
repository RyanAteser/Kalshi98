"""
config.py — All configuration loaded from environment variables.
Never hardcode secrets. Use .env file + python-dotenv for local dev.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    # Kalshi API
    kalshi_api_key_id: str
    kalshi_private_key_path: str

    # Database
    database_url: str

    # Strategy
    entry_threshold: float       # price >= this → consider entry (e.g. 0.50)
    stop_loss: float             # price <= this → force exit (e.g. 0.48)
    max_spread: float            # skip market if ask - bid > this (e.g. 0.10)
    min_liquidity_dollars: float # skip if orderbook depth < this (e.g. 5.0)
    position_size: int           # contracts per trade

    # Concurrency
    max_markets: int             # max simultaneous market workers
    worker_restart_delay: float  # seconds before restarting crashed worker

    # Misc
    log_level: str
    paper_trade: bool            # if True, skip real order placement


def load_config() -> Config:
    """Load and validate config from environment. Raises on missing required keys."""

    def require(key: str) -> str:
        val = os.getenv(key)
        if not val:
            raise EnvironmentError(f"Required environment variable '{key}' is not set.")
        return val

    def get_float(key: str, default: float) -> float:
        return float(os.getenv(key, str(default)))

    def get_int(key: str, default: int) -> int:
        return int(os.getenv(key, str(default)))

    def get_bool(key: str, default: bool) -> bool:
        return os.getenv(key, str(default)).lower() in ("1", "true", "yes")

    return Config(
        kalshi_api_key_id=require("KALSHI_API_KEY_ID"),
        kalshi_private_key_path=require("KALSHI_PRIVATE_KEY_PATH"),
        database_url=os.getenv("DATABASE_URL", "sqlite:///kalshi_trader.db"),
        entry_threshold=get_float("ENTRY_THRESHOLD", 0.50),
        stop_loss=get_float("STOP_LOSS", 0.48),
        max_spread=get_float("MAX_SPREAD", 0.10),
        min_liquidity_dollars=get_float("MIN_LIQUIDITY_DOLLARS", 5.0),
        position_size=get_int("POSITION_SIZE", 1),
        max_markets=get_int("MAX_MARKETS", 10),
        worker_restart_delay=get_float("WORKER_RESTART_DELAY", 5.0),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        paper_trade=get_bool("PAPER_TRADE", True),
    )
