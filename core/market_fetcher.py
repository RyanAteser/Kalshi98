"""
market_fetcher.py — Fetches Kalshi BTC 15-minute prediction markets only.

Queries the KXBTC15M series directly.
Extracts the BTC Reference/Target price from the ticker for GUI display.
"""

from __future__ import annotations

import logging
import time
import re
from datetime import datetime, timezone


def _parse_ts(val) -> int:
    """Parse a timestamp value into unix int. Returns 0 on failure."""
    if val is None:
        return 0
    try:
        if isinstance(val, (int, float)):
            return int(val)
        s = str(val)
        for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S+00:00", "%Y-%m-%dT%H:%M:%S"):
            try:
                return int(datetime.strptime(s, fmt).replace(tzinfo=timezone.utc).timestamp())
            except ValueError:
                pass
    except Exception:
        pass
    return 0
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Any

from pykalshi import KalshiClient
from pykalshi.models import MarketStatus

from core.config import Config

logger = logging.getLogger(__name__)

BTC_SERIES        = "KXBTC15M"
CANDIDATE_POOL    = 50
MAX_WORKERS       = 10
SETTLED_THRESHOLD = 0.99


def _extract_strike(m, ticker: str) -> float:
    """
    Extract BTC strike price using multi-field approach.
    Subtitle → numeric fields → settlement_timer_values → title → ticker suffix.
    """
    md = getattr(m, "data", m)
    if isinstance(md, dict) and "market" in md:
        md = md["market"]
    elif hasattr(md, "market") and getattr(md, "market") is not None:
        md = getattr(md, "market")

    def _get(obj, attr):
        return obj.get(attr) if isinstance(obj, dict) else getattr(obj, attr, None)

    def _parse_text(text) -> Optional[float]:
        if not text or "tbd" in str(text).lower():
            return None
        for pat in (
                r"(?:target price|above|below|over|under)[:\s]+\$?([\d,]+(?:\.\d+)?)",
                r"\$([\d,]+(?:\.\d+)?)",
        ):
            hit = re.search(pat, str(text), re.IGNORECASE)
            if hit:
                try:
                    v = float(hit.group(1).replace(",", ""))
                    if v > 1000:
                        return v
                except Exception:
                    pass
        return None

    # 1. subtitle (~30s after market open — most reliable)
    for attr in ("yes_sub_title", "no_sub_title", "subtitle", "sub_title"):
        v = _parse_text(_get(md, attr) or _get(m, attr))
        if v:
            return v

    # 2. numeric strike fields
    for attr in ("floor_strike_dollars", "cap_strike_dollars", "reference_price",
                 "floor_strike", "strike_price", "cap_strike", "strike",
                 "settlement_price", "price_to_beat"):
        raw = _get(md, attr) or _get(m, attr)
        try:
            v = float(raw) if raw is not None else None
            if v and v > 1000:
                return v
        except Exception:
            pass

    # 3. settlement_timer_values
    stv = _get(md, "settlement_timer_values") or _get(m, "settlement_timer_values")
    if isinstance(stv, dict):
        for val in stv.values():
            try:
                v = float(val)
                if v > 1000:
                    return v
            except Exception:
                pass

    # 4. title / rules_primary
    for attr in ("title", "rules_primary"):
        v = _parse_text(_get(md, attr) or _get(m, attr))
        if v:
            return v

    return 0.0


def _extract_field(m, *keys):
    # Pykalshi wraps single-market responses; unwrap to reach the real data payload
    md = getattr(m, "data", m)

    if isinstance(md, dict) and "market" in md:
        md = md["market"]
    elif hasattr(md, "market") and getattr(md, "market") is not None:
        md = getattr(md, "market")

    for key in keys:
        val = md.get(key) if isinstance(md, dict) else getattr(md, key, None)
        if val is not None:
            return val

        val = m.get(key) if isinstance(m, dict) else getattr(m, key, None)
        if val is not None:
            return val
    return None


def _to_dollars(val) -> Optional[float]:
    if val is None:
        return None
    try:
        f = float(val)
    except (TypeError, ValueError):
        return None
    # Convert cents (98) to dollars (0.98)
    return round(f / 100.0 if f > 1.0 else f, 6)


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _fetch_btc_markets(client: KalshiClient) -> list:
    markets = []
    cursor  = None
    for page in range(5):
        try:
            resp = client.get_markets(
                status=MarketStatus.OPEN,
                limit=200,
                cursor=cursor,
                series_ticker=BTC_SERIES,
            )
        except Exception as e:
            logger.warning("BTC fetch page %d failed: %s", page, e)
            break

        if hasattr(resp, "markets"):
            batch  = resp.markets or []
            cursor = getattr(resp, "cursor", None)
        elif isinstance(resp, dict):
            batch  = resp.get("markets") or []
            cursor = resp.get("cursor")
        else:
            batch  = list(resp) if resp else []
            cursor = None

        markets.extend(batch)
        if not cursor or not batch:
            break

    logger.info("Fetched %d KXBTC15M markets", len(markets))
    return markets


def get_market_snapshot(
        client: KalshiClient,
        ticker: str,
        retries: int = 3,
) -> Optional[Dict[str, Any]]:
    delay = 0.5
    for attempt in range(retries):
        try:
            m = client.get_market(ticker)

            raw_bid  = _extract_field(m, "yes_bid_dollars", "yes_bid",  "bid_dollars", "bid")
            raw_ask  = _extract_field(m, "yes_ask_dollars", "yes_ask",  "ask_dollars", "ask")
            raw_last = _extract_field(m, "last_price_dollars", "last_price", "price_dollars", "price")

            # Extract target BTC price from the ticker string
            btc_target = _extract_strike(m, ticker)

            # Extract close timestamp for rotator
            raw_close = _extract_field(m, "close_ts", "close_time", "close_time_ts",
                                       "close_timestamp", "closeTime", "close")
            return {
                "ticker":     ticker,
                "best_bid":   _to_dollars(raw_bid),
                "best_ask":   _to_dollars(raw_ask),
                "last_price": _to_dollars(raw_last),
                "volume":     _safe_float(_extract_field(m, "volume", "dollar_volume")),
                "btc_target": btc_target,
                "close_ts":   _parse_ts(raw_close),
            }
        except Exception as exc:
            s = str(exc).lower()
            if "429" in s or "too many" in s:
                time.sleep(delay)
                delay = min(delay * 2.0, 5.0)
            else:
                if attempt == retries - 1:
                    return None
                time.sleep(0.3)
    return None


def _is_valid_market(snapshot: Dict[str, Any]) -> bool:
    bid = snapshot.get("best_bid")
    ask = snapshot.get("best_ask")
    if (ask is not None and ask >= SETTLED_THRESHOLD) or \
            (bid is not None and bid >= SETTLED_THRESHOLD):
        return False
    bid_live = bid is not None and bid > 0.0
    ask_live = ask is not None and ask > 0.0
    return bid_live or ask_live


def fetch_active_sports_markets(
        client: KalshiClient,
        config: Config,
) -> List[Dict[str, Any]]:
    """Fetch live KXBTC15M markets. Name kept for drop-in compatibility."""
    start = time.time()
    raw   = _fetch_btc_markets(client)

    if not raw:
        logger.warning("No BTC 15m markets found — series '%s' may be off-hours", BTC_SERIES)
        return []

    live: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for m in raw:
            t = m.get("ticker") if isinstance(m, dict) else getattr(m, "ticker", None)
            if t:
                futures[executor.submit(get_market_snapshot, client, t)] = t

        for future in as_completed(futures):
            ticker = futures[future]
            try:
                snap = future.result()
            except Exception as exc:
                logger.warning("[%s] snapshot error: %s", ticker, exc)
                continue

            if snap and _is_valid_market(snap):
                live.append(snap)
            if len(live) >= CANDIDATE_POOL:
                break

    logger.info("BTC fetcher: %d live markets in %.2fs", len(live), time.time() - start)
    return live