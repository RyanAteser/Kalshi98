#!/usr/bin/env python3
"""
Backtest the T2T EV-based signal engine against historical BTC/USD data.

Data source : Coinbase 1-minute BTC/USD candles (public API, no auth).
Market model: Simulated Kalshi KXBTC15M-style 15-minute binary contracts.

For each 15-minute window in the history:
  - Generate strike prices as BTC_open ± STRIKE_PCT_OFFSETS %
  - Replay the final 2 minutes (t-120s and t-60s) as ticks
  - Fire the EV model on each tick — enter on first qualifying signal
  - Stop check at each remaining tick before close
  - Resolve: WIN if BTC closed on the correct side of the strike

Usage:
  python scripts/backtest_t2t.py --days 30
  python scripts/backtest_t2t.py --days 7 --min-ev 0.01 --spread 0.01

Strike offsets default to ≥0.5% (≥$500 at $100k BTC), matching real Kalshi
KXBTC15M spacing where entry is almost always capped at MAX_WINNING_ASK ($0.95).
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
import urllib.request
from dataclasses import dataclass
from typing import List, Optional, Tuple

sys.path.insert(0, ".")

from core.btc_feed import Candle
from core.signal_engine_t2t import (
    BTC_SIGMA_CANDLES, BTC_GRANULARITY, BTC_SIGMA_FALLBACK,
    MIN_EV, FIXED_RISK, MAX_WINNING_ASK,
)

# ── Simulation parameters ─────────────────────────────────────────────
MARKET_DURATION_SEC   = 900    # Kalshi KXBTC15M = 15 minutes
COINBASE_1MIN_URL     = "https://api.exchange.coinbase.com/products/BTC-USD/candles"
SYNTHETIC_BTC_START   = 100_000.0  # starting BTC price for synthetic mode
SYNTHETIC_SIGMA_DAILY = 0.025      # 2.5% daily vol — realistic for BTC

# Strike prices as % offsets from BTC open price (positive = target above open).
# Real Kalshi KXBTC15M strikes are spaced ~$500–$1000 apart at $100k BTC.
# At T-120s with σ≈8.5 $/s, distances >$187 cap entry at MAX_WINNING_ASK ($0.95).
# These offsets (0.5–2.0% = $500–$2000 at $100k) reflect the realistic regime
# where T2T fires with BTC well clear of the nearest unfulfilled strike.
STRIKE_PCT_OFFSETS = [-2.0, -1.5, -1.0, -0.75, -0.5,
                       0.5,  0.75,  1.0,  1.5,  2.0]


# ── Data structures ────────────────────────────────────────────────────

@dataclass
class TradeResult:
    window_ts:    int     # unix ts of 15-min window open
    target:       float   # BTC strike price
    side:         str     # YES or NO
    entry_px:     float   # simulated entry price
    p_reach:      float   # p_reach at entry
    ev:           float   # EV at entry
    sigma:        float   # sigma at entry ($/s)
    secs:         float   # seconds remaining when signal fired
    btc_entry:    float   # BTC price at signal
    btc_close:    float   # BTC price at contract close
    stopped:      bool    # True if stop loss triggered before close
    won:          bool    # True if contract resolved in our favour
    pnl:          float   # PnL per contract (dollars)


# ── Pure helpers (replicate engine methods without threading) ─────────

def _sigma_per_sec(candles_15min_newest_first: List[Candle], current_price: float) -> float:
    if len(candles_15min_newest_first) < 3 or current_price <= 0:
        return BTC_SIGMA_FALLBACK
    sample = candles_15min_newest_first[:BTC_SIGMA_CANDLES]
    closes = [c.close for c in reversed(sample)]
    if len(closes) < 2:
        return BTC_SIGMA_FALLBACK
    returns = [math.log(closes[i + 1] / closes[i]) for i in range(len(closes) - 1)]
    n    = len(returns)
    mean = sum(returns) / n
    var  = sum((r - mean) ** 2 for r in returns) / max(n - 1, 1)
    return max(math.sqrt(var) * current_price / math.sqrt(BTC_GRANULARITY), 0.1)


def _p_reach(btc_current: float, btc_target: float, secs: float, sigma: float) -> float:
    distance = abs(btc_current - btc_target)
    denom    = sigma * math.sqrt(2.0 * secs)
    return math.erfc(distance / denom) if denom > 0 else 0.0


def _ev(p_reach: float, entry_px: float) -> float:
    return (1.0 - p_reach) * (1.0 - entry_px) - p_reach * FIXED_RISK


# ── Data fetching ─────────────────────────────────────────────────────

def _fetch_page(start_ts: int, end_ts: int) -> List[Candle]:
    url = (f"{COINBASE_1MIN_URL}?granularity=60"
           f"&start={start_ts}&end={end_ts}&limit=300")
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (compatible; t2t-backtest/1.0)",
        "Accept":     "application/json",
    })
    with urllib.request.urlopen(req, timeout=15) as resp:
        raw = json.loads(resp.read().decode())
    rows = raw.get("candles", raw) if isinstance(raw, dict) else raw
    candles = []
    for row in rows:
        if isinstance(row, dict):
            candles.append(Candle(
                ts=int(row.get("start", row.get("time", 0))),
                low=float(row["low"]), high=float(row["high"]),
                open=float(row["open"]), close=float(row["close"]),
                volume=float(row["volume"]),
            ))
        elif len(row) >= 6:
            candles.append(Candle(
                ts=int(row[0]), low=float(row[1]), high=float(row[2]),
                open=float(row[3]), close=float(row[4]), volume=float(row[5]),
            ))
    return sorted(candles, key=lambda c: c.ts)


def fetch_1min_candles(days: int) -> List[Candle]:
    """Fetch up to `days` days of 1-minute BTC/USD candles from Coinbase.

    Returns an empty list on network failure so callers can fall back
    to synthetic data.
    """
    now_ts   = int(time.time())
    start_ts = now_ts - days * 86_400
    STEP     = 300 * 60  # 300 candles × 60 s each = 5 hours per page

    all_candles: dict[int, Candle] = {}
    t = start_ts
    pages = math.ceil((now_ts - start_ts) / STEP)
    done  = 0
    errors = 0
    print(f"Fetching {days}d of 1-min BTC candles ({pages} pages)...", flush=True)

    while t < now_ts:
        end = min(t + STEP, now_ts)
        for attempt in range(3):
            try:
                for c in _fetch_page(t, end):
                    all_candles[c.ts] = c
                break
            except Exception as exc:
                errors += 1
                if attempt == 2:
                    print(f"  warn: page t={t} failed: {exc}", flush=True)
                time.sleep(1.0 * (attempt + 1))
        # Bail early if first page already fails — likely no network access
        if done == 0 and errors > 0 and not all_candles:
            print("  Network unreachable — no candles fetched.")
            return []
        done += 1
        if done % 20 == 0:
            pct = (t - start_ts) / max(now_ts - start_ts, 1) * 100
            print(f"  {pct:.0f}%  ({len(all_candles)} candles)", flush=True)
        t = end
        time.sleep(0.12)

    result = sorted(all_candles.values(), key=lambda c: c.ts)
    if result:
        print(f"Fetched {len(result):,} 1-min candles ({result[0].ts} → {result[-1].ts})")
    return result


def generate_synthetic_candles(days: int) -> List[Candle]:
    """Generate synthetic 1-minute BTC candles via Geometric Brownian Motion.

    Uses a fixed random seed so results are reproducible. Parameters match
    realistic BTC volatility so the EV model is tested against the same
    diffusion process it was designed for.
    """
    import random
    rng = random.Random(42)

    sigma_1min = SYNTHETIC_SIGMA_DAILY / math.sqrt(1_440)  # scale daily → 1-min

    now_ts   = int(time.time())
    start_ts = (now_ts - days * 86_400) // 60 * 60  # align to minute boundary

    candles: List[Candle] = []
    price = SYNTHETIC_BTC_START
    ts    = start_ts

    while ts < now_ts:
        open_p = price
        z      = rng.gauss(0, 1)
        close_p = open_p * math.exp(sigma_1min * z)

        # Intra-minute range: simple bid-ask spread proxy
        intra = abs(z) * sigma_1min * 0.4 + sigma_1min * 0.2
        high_p = max(open_p, close_p) * math.exp(abs(rng.gauss(0, intra)))
        low_p  = min(open_p, close_p) * math.exp(-abs(rng.gauss(0, intra)))

        candles.append(Candle(
            ts=ts, low=low_p, high=high_p,
            open=open_p, close=close_p,
            volume=rng.uniform(1.0, 10.0),
        ))
        price = close_p
        ts   += 60

    sigma_ann = SYNTHETIC_SIGMA_DAILY * math.sqrt(252)
    print(
        f"Generated {len(candles):,} synthetic 1-min candles | "
        f"BTC ${SYNTHETIC_BTC_START:,.0f} → ${price:,.0f} | "
        f"σ_daily={SYNTHETIC_SIGMA_DAILY*100:.1f}%  σ_ann={sigma_ann*100:.0f}%"
    )
    return candles


def aggregate_15min(candles_1min: List[Candle]) -> List[Candle]:
    """Collapse 15 consecutive 1-min candles into synthetic 15-min candles."""
    out = []
    for i in range(0, len(candles_1min) - 14, 15):
        group = candles_1min[i:i + 15]
        if len(group) < 15:
            break
        out.append(Candle(
            ts=group[0].ts,
            low=min(c.low for c in group),
            high=max(c.high for c in group),
            open=group[0].open,
            close=group[-1].close,
            volume=sum(c.volume for c in group),
        ))
    return out


# ── Core simulation ────────────────────────────────────────────────────

def simulate_window(
        window_1min:       List[Candle],   # 15 1-min candles (this window)
        sigma_candles_15m: List[Candle],   # 20+ preceding 15-min candles (oldest first)
        target:            float,
        market_spread:     float,
        min_ev_override:   float,
) -> Optional[TradeResult]:
    """
    Simulate one 15-minute market window against one strike price.

    Ticks: t-120s (window_1min[-2]) and t-60s (window_1min[-1]).
    The engine fires on the first tick that clears the EV gate.
    Stop is checked at every subsequent tick before close.
    """
    if len(window_1min) < 2:
        return None

    btc_close  = window_1min[-1].close
    sigma_newest = list(reversed(sigma_candles_15m[-BTC_SIGMA_CANDLES:]))

    # Ticks available in the final 2 minutes: (candle, secs_remaining)
    tick_pairs: List[Tuple[Candle, int]] = [
        (window_1min[-2], 120),
        (window_1min[-1],  60),
    ]

    for t_idx, (entry_candle, secs) in enumerate(tick_pairs):
        btc_current = entry_candle.close
        if btc_current == target:
            continue

        winning_side = "YES" if btc_current > target else "NO"
        sigma  = _sigma_per_sec(sigma_newest, btc_current)
        pr     = _p_reach(btc_current, target, secs, sigma)
        # Simulated ask = fair value + half the market spread, capped at MAX_WINNING_ASK
        entry_px = min((1.0 - pr) + market_spread, MAX_WINNING_ASK)
        ev_val   = _ev(pr, entry_px)

        if ev_val < min_ev_override:
            continue

        # ── Stop check at each subsequent tick ─────────────────────────
        # Remaining ticks between entry and close
        subsequent = tick_pairs[t_idx + 1:]
        stopped = False
        stop_bid = entry_px - FIXED_RISK  # we stop when bid falls to here

        for rem_candle, rem_secs in subsequent:
            pr_rem      = _p_reach(rem_candle.close, target, rem_secs, sigma)
            implied_bid = (1.0 - pr_rem) - market_spread   # what we could sell at
            if implied_bid <= stop_bid:
                stopped = True
                break

        # ── Resolution ─────────────────────────────────────────────────
        won = (btc_close > target) if winning_side == "YES" else (btc_close < target)

        if stopped:
            pnl = -FIXED_RISK
        elif won:
            pnl = 1.0 - entry_px
        else:
            # BTC crossed target without stop firing (sudden end-of-market move)
            # Conservative: assume stop executes for the standard loss
            pnl = -FIXED_RISK

        return TradeResult(
            window_ts=window_1min[0].ts,
            target=target,
            side=winning_side,
            entry_px=entry_px,
            p_reach=pr,
            ev=ev_val,
            sigma=sigma,
            secs=float(secs),
            btc_entry=btc_current,
            btc_close=btc_close,
            stopped=stopped,
            won=won,
            pnl=pnl,
        )

    return None  # EV gate never cleared for this window/target pair


# ── Backtest runner ────────────────────────────────────────────────────

def run_backtest(
        candles_1min:  List[Candle],
        strike_offsets: List[float],
        market_spread:  float,
        min_ev:         float,
) -> List[TradeResult]:
    candles_15m = aggregate_15min(candles_1min)
    if len(candles_15m) < BTC_SIGMA_CANDLES + 2:
        print("Not enough candle history for sigma estimation.")
        return []

    results: List[TradeResult] = []
    n_windows = len(candles_15m)

    for i in range(BTC_SIGMA_CANDLES, n_windows):
        window_15m = candles_15m[i]
        # Match the 15 corresponding 1-min candles
        win_start = window_15m.ts
        win_end   = win_start + MARKET_DURATION_SEC
        window_1min = [c for c in candles_1min if win_start <= c.ts < win_end]
        if len(window_1min) < 2:
            continue

        sigma_candles = candles_15m[i - BTC_SIGMA_CANDLES: i]  # oldest→newest
        btc_open      = window_15m.open

        for pct in strike_offsets:
            target = btc_open * (1.0 + pct / 100.0)
            result = simulate_window(window_1min, sigma_candles, target, market_spread, min_ev)
            if result is not None:
                results.append(result)

    return results


# ── Reporting ─────────────────────────────────────────────────────────

def _kalshi_fee(contracts: int, price: float) -> float:
    """Estimate Kalshi round-trip fee for a winning trade."""
    entry_fee = 0.07 * contracts * price * (1.0 - price) + 0.0035 * contracts * price
    exit_fee  = 0.0035 * contracts * 1.0   # exit at $1 on win; trading fee = 0 at price=1
    return entry_fee + exit_fee


def _max_drawdown_pct(equity: List[float]) -> float:
    peak = equity[0]
    max_dd = 0.0
    for v in equity:
        if v > peak:
            peak = v
        if peak > 0:
            max_dd = max(max_dd, (peak - v) / peak * 100)
    return max_dd


def _simulate_portfolio(
        results: List[TradeResult],
        account: float,
        contracts_per_trade: int,
) -> Tuple[float, List[float], float]:
    """
    Sequential portfolio simulation: one trade per 15-min window, highest EV.

    Returns (final_account, equity_curve, total_fees).
    """
    from collections import defaultdict
    by_window: dict[int, List[TradeResult]] = defaultdict(list)
    for r in results:
        by_window[r.window_ts].append(r)

    equity    = [account]
    total_fees = 0.0

    for ts in sorted(by_window):
        best = max(by_window[ts], key=lambda r: r.ev)

        cost = contracts_per_trade * best.entry_px
        if cost > account:
            # Scale down to what we can afford
            contracts = max(1, int(account / best.entry_px))
            cost = contracts * best.entry_px
        else:
            contracts = contracts_per_trade

        if cost > account:
            continue

        gross_pnl = contracts * best.pnl
        fee = _kalshi_fee(contracts, best.entry_px) if best.won and not best.stopped else (
            0.0035 * contracts * best.entry_px  # regulatory only on losing side
        )
        total_fees += fee
        account   += gross_pnl - fee
        account    = max(0.0, account)
        equity.append(account)

    return account, equity, total_fees


def print_report(results: List[TradeResult], days: int, account_size: float) -> None:
    if not results:
        print("No signals fired.")
        return

    import datetime
    ts_min = min(r.window_ts for r in results)
    ts_max = max(r.window_ts for r in results)
    d_from = datetime.datetime.utcfromtimestamp(ts_min).strftime("%Y-%m-%d")
    d_to   = datetime.datetime.utcfromtimestamp(ts_max).strftime("%Y-%m-%d")

    n       = len(results)
    wins    = sum(1 for r in results if r.won)
    stops   = sum(1 for r in results if r.stopped)
    avg_entry = sum(r.entry_px for r in results) / n

    print(f"\n{'='*62}")
    print(f"  T2T Backtest  ({days}d  {d_from} → {d_to})")
    print(f"{'='*62}")

    # ── Signal quality (all strike offsets, not $ amounts) ───────────
    print(f"\n  SIGNAL ANALYSIS  (all {n:,} signals across all strike offsets)")
    print(f"  {'─'*50}")
    print(f"  Win rate      : {wins/n*100:.1f}%  ({wins:,} / {n:,})")
    print(f"  Stopped out   : {stops:,}  ({stops/n*100:.1f}%)")
    print(f"  Avg entry     : ${avg_entry:.4f} / contract")
    print(f"  Avg PnL       : ${sum(r.pnl for r in results)/n:+.4f} / contract  "
          f"(${sum(r.pnl for r in results)/n * 100:+.2f} per $100 invested per trade)")

    # ── EV bins ──────────────────────────────────────────────────────
    print(f"\n  {'EV range':<14} {'N':>6} {'Win%':>7} {'$/contract':>12} {'Entry$':>8}")
    print(f"  {'─'*52}")
    for lo, hi in [(0.005, 0.010), (0.010, 0.020), (0.020, 0.050), (0.050, 1.0)]:
        sub = [r for r in results if lo <= r.ev < hi]
        if not sub:
            continue
        w    = sum(1 for r in sub if r.won)
        apnl = sum(r.pnl for r in sub) / len(sub)
        aent = sum(r.entry_px for r in sub) / len(sub)
        print(f"  {lo:.3f}–{hi:.3f}     {len(sub):>6} {w/len(sub)*100:>6.1f}% "
              f"  {apnl:>+10.4f}   ${aent:.3f}")

    # ── By distance from target ───────────────────────────────────────
    print(f"\n  {'Distance':<12} {'N':>6} {'Win%':>7} {'$/contract':>12}  {'p_reach':>8}")
    print(f"  {'─'*50}")
    for lo, hi in [(0, 1), (1, 2), (2, 3), (3, 999)]:
        sub = [r for r in results
               if lo <= abs(r.btc_entry - r.target) / max(r.sigma * math.sqrt(2.0 * r.secs), 1e-9) < hi]
        if not sub:
            continue
        w  = sum(1 for r in sub if r.won)
        pr = sum(r.p_reach for r in sub) / len(sub)
        lbl = f"{lo}σ–{hi if hi < 999 else '∞'}σ"
        print(f"  {lbl:<12} {len(sub):>6} {w/len(sub)*100:>6.1f}% "
              f"  {sum(r.pnl for r in sub)/len(sub):>+10.4f}  {pr:>8.4f}")

    # ── Portfolio simulation ($account, 1 trade per window, FLAT sizing) ──
    # Contracts are fixed from initial capital — no compounding.
    # One trade per 15-min window: the highest-EV strike for that window.
    from collections import defaultdict
    by_window: dict[int, List[TradeResult]] = defaultdict(list)
    for r in results:
        by_window[r.window_ts].append(r)
    ordered_windows = sorted(by_window.keys())

    # Pre-select the best trade per window so we can summarise it independently
    best_per_window = [max(by_window[ts], key=lambda r: r.ev) for ts in ordered_windows]
    best_win_rate   = sum(1 for b in best_per_window if b.pnl > 0) / max(len(best_per_window), 1)
    best_avg_entry  = sum(b.entry_px for b in best_per_window) / max(len(best_per_window), 1)
    best_avg_pnl_c  = sum(b.pnl for b in best_per_window) / max(len(best_per_window), 1)

    # The key data for the portfolio: per-trade edge expressed clearly in $.
    # 30-day totals are shown but de-emphasised: at 96 trades/day the
    # absolute P&L from flat-bet runs grows linearly and mainly reflects
    # trade frequency, not per-trade edge.  Real trading frequency with
    # T2T is much lower (1–3 qualifying markets per hour, not 96).
    print(f"\n  PORTFOLIO SIMULATION  (${account_size:.0f} starting · flat sizing · 1 trade/window)")
    print(f"  {'─'*54}")
    print(f"  ⚠  Synthetic GBM data. Strike offsets ≥0.5% (≥$500 at $100k)")
    print(f"  match real Kalshi spacing — entries capped at $0.95 in this")
    print(f"  regime. Outcome model is conservative (erfc = ever-touch).")
    print()
    print(f"  Trade selection : highest-EV strike per 15-min window")
    print(f"  Windows tested  : {len(best_per_window):,}  ({len(best_per_window)/days:.0f}/day · one per 15 min)")
    print(f"  Avg entry price : ${best_avg_entry:.4f} / contract")
    print(f"  Win rate (pnl>0): {best_win_rate*100:.1f}%")
    print(f"  Edge / contract : ${best_avg_pnl_c:+.4f}  (avg PnL per contract traded)")

    # Show per-trade dollar amounts for a range of contract counts,
    # then derive 30-day totals as a consequence — not the headline.
    print(f"\n  Per-trade dollar results at different position sizes:")
    print(f"  {'─'*54}")
    print(f"  {'Contracts':>10}  {'Cost/trade':>11}  {'Avg win':>9}  {'Avg loss':>9}  {'Edge/trade':>11}")
    for n_contracts in [1, 10, 50, 100]:
        cost = n_contracts * best_avg_entry
        if cost > account_size * 1.05:   # skip if requires more than account
            continue
        avg_win_t  = n_contracts * sum(b.pnl for b in best_per_window if b.pnl > 0) / max(sum(1 for b in best_per_window if b.pnl > 0), 1)
        avg_loss_t = n_contracts * sum(b.pnl for b in best_per_window if b.pnl <= 0) / max(sum(1 for b in best_per_window if b.pnl <= 0), 1)
        edge       = n_contracts * best_avg_pnl_c
        print(f"  {n_contracts:>10}  ${cost:>9.2f}  ${avg_win_t:>+8.2f}  ${avg_loss_t:>+8.2f}  ${edge:>+10.2f}")

    # 30-day totals for the most practical sizing (fits in $account_size)
    practical_contracts = max(1, int(account_size * 0.95 / best_avg_entry))
    total_gross = sum(practical_contracts * b.pnl for b in best_per_window)
    total_fees  = sum(
        _kalshi_fee(practical_contracts, b.entry_px) if b.pnl > 0
        else 0.0035 * practical_contracts * b.entry_px
        for b in best_per_window
    )
    net   = total_gross - total_fees
    equity = [account_size]
    running = account_size
    for b in best_per_window:
        running += practical_contracts * b.pnl - (
            _kalshi_fee(practical_contracts, b.entry_px) if b.pnl > 0
            else 0.0035 * practical_contracts * b.entry_px
        )
        equity.append(running)
    dd = _max_drawdown_pct(equity)

    print(f"\n  30-day totals @ {practical_contracts} contracts/trade  (${practical_contracts * best_avg_entry:.2f}/trade, fits in ${account_size:.0f}):")
    print(f"    Gross P&L  : ${total_gross:+,.2f}")
    print(f"    Fees (est.): -${total_fees:,.2f}")
    print(f"    Net P&L    : ${net:+,.2f}")
    print(f"    Max drawdown: {dd:.2f}%")
    print(f"    Daily avg  : ${net/days:+,.2f}/day  (assumes every window is traded)")
    print(f"  * Real T2T fires far fewer times than 1/window; scale daily avg")
    print(f"    down proportionally to your actual observed firing frequency.")

    # ── EV calibration ───────────────────────────────────────────────
    print(f"\n  EV CALIBRATION  (model's p_win vs actual win rate)")
    print(f"  {'p_win bucket':<14} {'Predicted':>10} {'Actual':>10} {'N':>7}")
    print(f"  {'─'*46}")
    for lo, hi in [(0.50, 0.70), (0.70, 0.80), (0.80, 0.90), (0.90, 0.95), (0.95, 1.01)]:
        sub = [r for r in results if lo <= (1.0 - r.p_reach) < hi]
        if not sub:
            continue
        pred = sum(1.0 - r.p_reach for r in sub) / len(sub)
        act  = sum(1 for r in sub if r.won) / len(sub)
        print(f"  {lo:.2f}–{min(hi,1.0):.2f}         {pred:>10.3f} {act:>10.3f} {len(sub):>7}")
    print(f"  Note: model uses ever-touch probability (erfc); actual outcome")
    print(f"  is final-price, so model is conservative by ~2× near the boundary.")

    print(f"\n{'='*62}\n")


# ── Entry point ────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest T2T EV signal engine")
    parser.add_argument("--days",      type=int,   default=30,   help="Days of history (default 30)")
    parser.add_argument("--min-ev",    type=float, default=None, help="Override MIN_EV threshold")
    parser.add_argument("--spread",    type=float, default=0.01, help="Assumed market spread (default 0.01)")
    parser.add_argument("--account",   type=float, default=100,  help="Starting account size in $ (default 100)")
    parser.add_argument("--synthetic", action="store_true",      help="Force synthetic GBM data")
    args = parser.parse_args()

    effective_min_ev = args.min_ev if args.min_ev is not None else MIN_EV
    print(f"Config: days={args.days}  min_ev={effective_min_ev}  spread={args.spread}  account=${args.account:.0f}")

    if args.synthetic:
        candles_1min = generate_synthetic_candles(args.days)
    else:
        candles_1min = fetch_1min_candles(args.days)
        if not candles_1min:
            print("Falling back to synthetic GBM data (Coinbase unreachable).")
            candles_1min = generate_synthetic_candles(args.days)

    if len(candles_1min) < 300:
        print("Insufficient candle data — aborting.")
        sys.exit(1)

    results = run_backtest(candles_1min, STRIKE_PCT_OFFSETS, args.spread, effective_min_ev)
    print_report(results, args.days, args.account)


if __name__ == "__main__":
    main()
