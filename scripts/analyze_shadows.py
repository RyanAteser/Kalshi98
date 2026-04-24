#!/usr/bin/env python3
"""
analyze_shadows.py — Analyze shadow tracker results.

Shows which entry price threshold and volume spike multiplier would have
performed best on all closed trades in the DB.

Usage:
    python scripts/analyze_shadows.py
    python scripts/analyze_shadows.py --csv          # also write CSV files
    python scripts/analyze_shadows.py --db /path/to/other.db
"""

from __future__ import annotations

import argparse
import csv
import os
import sqlite3
import sys
from pathlib import Path


# ── Helpers ───────────────────────────────────────────────────────────────────

def _connect(db_path: str) -> sqlite3.Connection:
    if not Path(db_path).exists():
        print(f"ERROR: DB not found at {db_path}")
        sys.exit(1)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _hr(char: str = "─", width: int = 72) -> str:
    return char * width


def _pct(n: int, total: int) -> str:
    return f"{n / total * 100:.0f}%" if total else "—"


# ── Price threshold analysis ───────────────────────────────────────────────────

PRICE_SQL = """
SELECT
    threshold,
    COUNT(*)                                                  AS trades,
    SUM(CASE WHEN pnl_per_contract > 0 THEN 1 ELSE 0 END)   AS wins,
    SUM(CASE WHEN pnl_per_contract <= 0 THEN 1 ELSE 0 END)  AS losses,
    ROUND(SUM(pnl_per_contract), 4)                          AS total_pnl,
    ROUND(AVG(pnl_per_contract), 4)                          AS avg_pnl,
    ROUND(MIN(pnl_per_contract), 4)                          AS worst_pnl,
    ROUND(MAX(pnl_per_contract), 4)                          AS best_pnl,
    COUNT(DISTINCT exit_reason)                              AS reason_count
FROM shadow_trades
WHERE exit_price IS NOT NULL
GROUP BY threshold
ORDER BY avg_pnl DESC
"""

PRICE_REASONS_SQL = """
SELECT
    threshold,
    exit_reason,
    COUNT(*) AS n
FROM shadow_trades
WHERE exit_price IS NOT NULL
GROUP BY threshold, exit_reason
ORDER BY threshold, n DESC
"""


def analyze_price_thresholds(conn: sqlite3.Connection, write_csv: bool, out_dir: Path) -> None:
    rows = conn.execute(PRICE_SQL).fetchall()
    if not rows:
        print("\n[Price Thresholds]  No data yet in shadow_trades.\n")
        return

    reason_map: dict[float, list[str]] = {}
    for r in conn.execute(PRICE_REASONS_SQL).fetchall():
        reason_map.setdefault(r["threshold"], []).append(f"{r['exit_reason']}×{r['n']}")

    print()
    print(_hr("═"))
    print("  PRICE THRESHOLD SHADOW RESULTS")
    print(_hr("═"))
    header = f"{'Threshold':>10}  {'Trades':>6}  {'Win%':>5}  {'Avg PnL':>8}  {'Total PnL':>10}  {'Worst':>7}  {'Best':>7}  Exit Reasons"
    print(header)
    print(_hr())
    for r in rows:
        reasons = "  ".join(reason_map.get(r["threshold"], []))
        win_pct = _pct(r["wins"], r["trades"])
        print(
            f"  {r['threshold']:>8.2f}  {r['trades']:>6}  {win_pct:>5}  "
            f"{r['avg_pnl']:>+8.4f}  {r['total_pnl']:>+10.4f}  "
            f"{r['worst_pnl']:>+7.4f}  {r['best_pnl']:>+7.4f}  {reasons}"
        )
    print(_hr())
    best = rows[0]
    print(f"  BEST: threshold={best['threshold']:.2f}  avg_pnl={best['avg_pnl']:+.4f}  win%={_pct(best['wins'], best['trades'])}")
    print()

    if write_csv:
        path = out_dir / "shadow_price_thresholds.csv"
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["threshold", "trades", "wins", "losses", "win_pct",
                        "avg_pnl", "total_pnl", "worst_pnl", "best_pnl"])
            for r in rows:
                w.writerow([r["threshold"], r["trades"], r["wins"], r["losses"],
                            _pct(r["wins"], r["trades"]),
                            r["avg_pnl"], r["total_pnl"], r["worst_pnl"], r["best_pnl"]])
        print(f"  CSV written: {path}")


# ── Volume multiplier analysis ─────────────────────────────────────────────────

VOL_SQL = """
SELECT
    multiplier,
    COUNT(*)                                                        AS trades,
    SUM(CASE WHEN pnl_per_contract > 0 THEN 1 ELSE 0 END)         AS wins,
    SUM(CASE WHEN pnl_per_contract <= 0 THEN 1 ELSE 0 END)        AS losses,
    ROUND(SUM(pnl_per_contract), 4)                                AS total_pnl,
    ROUND(AVG(pnl_per_contract), 4)                                AS avg_pnl,
    ROUND(MIN(pnl_per_contract), 4)                                AS worst_pnl,
    ROUND(MAX(pnl_per_contract), 4)                                AS best_pnl,
    COUNT(CASE WHEN exit_reason = 'vol_spike' THEN 1 END)         AS vol_stops,
    COUNT(CASE WHEN exit_reason = 'real_exit' THEN 1 END)         AS real_exits,
    COUNT(CASE WHEN exit_reason = 'settlement' THEN 1 END)        AS settlements
FROM shadow_vol_trades
WHERE exit_price IS NOT NULL
GROUP BY multiplier
ORDER BY avg_pnl DESC
"""

# For each multiplier: avg ticks to stop (when vol_spike fired)
VOL_TIMING_SQL = """
SELECT
    multiplier,
    ROUND(AVG(
        (julianday(exit_ts) - julianday(entry_ts)) * 86400
    ), 1) AS avg_secs_to_exit
FROM shadow_vol_trades
WHERE exit_reason = 'vol_spike'
GROUP BY multiplier
"""


def analyze_vol_multipliers(conn: sqlite3.Connection, write_csv: bool, out_dir: Path) -> None:
    rows = conn.execute(VOL_SQL).fetchall()
    if not rows:
        print("\n[Volume Multipliers]  No data yet in shadow_vol_trades.\n")
        return

    timing: dict[float, float] = {
        r["multiplier"]: r["avg_secs_to_exit"]
        for r in conn.execute(VOL_TIMING_SQL).fetchall()
    }

    print()
    print(_hr("═"))
    print("  VOLUME SPIKE MULTIPLIER SHADOW RESULTS")
    print(_hr("═"))
    header = (
        f"{'Mult':>6}  {'Trades':>6}  {'Win%':>5}  {'Avg PnL':>8}  "
        f"{'Total':>9}  {'Worst':>7}  {'VolStops':>8}  {'AvgSecs':>8}"
    )
    print(header)
    print(_hr())
    for r in rows:
        win_pct  = _pct(r["wins"], r["trades"])
        avg_secs = timing.get(r["multiplier"])
        secs_str = f"{avg_secs:.0f}s" if avg_secs is not None else "—"
        print(
            f"  {r['multiplier']:>4.1f}  {r['trades']:>6}  {win_pct:>5}  "
            f"{r['avg_pnl']:>+8.4f}  {r['total_pnl']:>+9.4f}  "
            f"{r['worst_pnl']:>+7.4f}  {r['vol_stops']:>8}  {secs_str:>8}"
        )
    print(_hr())
    best = rows[0]
    vol_stop_rate = _pct(best["vol_stops"], best["trades"])
    print(
        f"  BEST: multiplier={best['multiplier']:.1f}  avg_pnl={best['avg_pnl']:+.4f}  "
        f"win%={_pct(best['wins'], best['trades'])}  vol_stop_rate={vol_stop_rate}"
    )
    print()

    if write_csv:
        path = out_dir / "shadow_vol_multipliers.csv"
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["multiplier", "trades", "wins", "losses", "win_pct",
                        "avg_pnl", "total_pnl", "worst_pnl", "best_pnl",
                        "vol_stops", "real_exits", "settlements", "avg_secs_to_exit"])
            for r in rows:
                w.writerow([
                    r["multiplier"], r["trades"], r["wins"], r["losses"],
                    _pct(r["wins"], r["trades"]),
                    r["avg_pnl"], r["total_pnl"], r["worst_pnl"], r["best_pnl"],
                    r["vol_stops"], r["real_exits"], r["settlements"],
                    timing.get(r["multiplier"], ""),
                ])
        print(f"  CSV written: {path}")


# ── Summary ────────────────────────────────────────────────────────────────────

SUMMARY_SQL = """
SELECT
    COUNT(DISTINCT ticker)       AS markets,
    COUNT(*)                     AS total_trades,
    SUM(CASE WHEN exit_reason = 'real_exit'   THEN 1 ELSE 0 END) AS real_exits,
    SUM(CASE WHEN exit_reason = 'settlement'  THEN 1 ELSE 0 END) AS settlements,
    MIN(entry_ts)                AS first_trade,
    MAX(exit_ts)                 AS last_trade
FROM shadow_trades
WHERE exit_price IS NOT NULL
"""


def print_summary(conn: sqlite3.Connection) -> None:
    r = conn.execute(SUMMARY_SQL).fetchone()
    if not r or r["total_trades"] == 0:
        print("\nNo closed shadow trades yet. Run the bot to collect data.\n")
        return
    print()
    print(_hr("─"))
    print(f"  Data range : {r['first_trade']} → {r['last_trade']}")
    print(f"  Markets    : {r['markets']}   Trades: {r['total_trades']}")
    print(f"  Exit types : real_exit={r['real_exits']}  settlement={r['settlements']}")
    print(_hr("─"))


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze shadow tracker results")
    parser.add_argument(
        "--db", default=os.getenv("DATABASE_URL", "kalshi_trader.db").replace("sqlite:///", ""),
        help="Path to SQLite DB (default: kalshi_trader.db)",
    )
    parser.add_argument("--csv", action="store_true", help="Write CSV output files")
    args = parser.parse_args()

    db_path = args.db or "kalshi_trader.db"
    print(f"\nDB: {db_path}")
    conn = _connect(db_path)
    out_dir = Path(db_path).parent

    print_summary(conn)
    analyze_price_thresholds(conn, write_csv=args.csv, out_dir=out_dir)
    analyze_vol_multipliers(conn, write_csv=args.csv, out_dir=out_dir)
    conn.close()


if __name__ == "__main__":
    main()
