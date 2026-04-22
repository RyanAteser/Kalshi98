"""
Shows a live Coinbase BTC/USD candlestick chart.
Trades Kalshi KXBTC15M binary markets using the 96-98c engine.
Auto-scales position size from cash balance.
Shuts down after 2 consecutive losses.
"""

from __future__ import annotations

import logging
import sys
import time
import threading
import tkinter as tk
import tkinter.ttk as ttk

from dotenv import load_dotenv
from pykalshi import KalshiClient

from core.config import load_config
from core.btc_feed import BtcFeed
from core.execution_engine import ExecutionEngine
from core.market_fetcher import fetch_active_sports_markets
from core.market_rotator import MarketRotator
from core.portfolio_poller import PortfolioPoller
from core.risk_manager import RiskManager
from core.signal_engine_router import SignalEngineRouter
from core.worker import MarketWorker
from db.db import Database
from core.btc_chart import BtcChart
from core.event_bus import MarketUpdate
import core.event_bus as event_bus

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
    logger.info("Starting BTC 15m Trader | paper_mode=%s", config.paper_trade)

    # ── Backend ──────────────────────────────────────────────────────
    db = Database()
    db.create_schema()

    client           = KalshiClient.from_env()
    signal_engine    = SignalEngineRouter(config)
    execution_engine = ExecutionEngine(client, config)

    markets:      list[dict]        = []
    workers:      list[MarketWorker] = []
    workers_lock  = threading.Lock()

    # ── GUI ──────────────────────────────────────────────────────────
    root = tk.Tk()
    root.title("BTC 15m Trader — Full Dashboard")
    root.configure(bg="#0a0c0f")
    root.geometry("1480x820")
    root.minsize(1100, 600)

    # Top toggle frame (unchanged)
    toggle_frame = tk.Frame(root, bg="#0a0c0f", pady=4)
    toggle_frame.pack(side=tk.TOP, fill=tk.X, padx=12)

    ENGINE_COLORS = {
        "simple96":   "#00e676",
        "resolution": "#f0a500",
        "momentum":   "#29b6f6",
    }
    toggle_label = tk.Label(
        toggle_frame, text="Engine: Simple Buy (96c)",
        bg="#0a0c0f", fg=ENGINE_COLORS["simple96"],
        font=("Consolas", 10, "bold"), anchor="w",
    )
    toggle_label.pack(side=tk.LEFT, padx=(0, 16))

    buttons = {}
    def _switch(key: str):
        signal_engine.set_engine(key)
        toggle_label.config(
            text=f"Engine: {signal_engine.ENGINE_LABELS[key]}",
            fg=ENGINE_COLORS[key],
        )
        for k, b in buttons.items():
            b.config(relief=tk.SUNKEN if k == key else tk.FLAT)

    for key, label in [
        ("simple96",   "Simple Buy (96-98c)"),
        ("resolution", "Resolution (97c)"),
        ("momentum",   "Momentum (52c)"),
    ]:
        b = tk.Button(
            toggle_frame, text=label,
            bg="#1a1d23", fg="#ffffff",
            activebackground="#2a2d33",
            relief=tk.SUNKEN if key == "simple96" else tk.FLAT,
            font=("Consolas", 9),
            command=lambda k=key: _switch(k),
        )
        b.pack(side=tk.LEFT, padx=4)
        buttons[key] = b

    # ── Market Info Header (Target + Orderbook) ─────────────────────
    info_frame = tk.Frame(root, bg="#0a0c0f", pady=8, height=38)
    info_frame.pack(fill=tk.X, padx=12, pady=(0, 6))
    info_frame.pack_propagate(False)

    target_label = tk.Label(
        info_frame, text="BTC Target: —", bg="#0a0c0f", fg="#00e6d6",
        font=("Consolas", 12, "bold"), anchor="w"
    )
    target_label.pack(side=tk.LEFT, padx=12)

    book_label = tk.Label(
        info_frame, text="BID: —c | ASK: —c", bg="#0a0c0f", fg="#ffffff",
        font=("Consolas", 12, "bold"), anchor="w"
    )
    book_label.pack(side=tk.LEFT, padx=12)

    # Balance & status (live from sizer + quick poll)
    status_frame = tk.Frame(info_frame, bg="#0a0c0f")
    status_frame.pack(side=tk.RIGHT, padx=12)
    balance_lbl = tk.Label(status_frame, text="Balance: $—.——", bg="#0a0c0f", fg="#00e676", font=("Consolas", 11, "bold"))
    balance_lbl.pack(side=tk.RIGHT, padx=8)
    mode_lbl = tk.Label(status_frame, text="● PAPER", bg="#0a0c0f", fg="#f0a500", font=("Consolas", 10, "bold"))
    mode_lbl.pack(side=tk.RIGHT, padx=8)

    if not config.paper_trade:
        mode_lbl.config(text="● LIVE", fg="#f44336")

    # ── Sidebar: Full Active Markets Table ───────────────────────────
    sidebar = tk.Frame(root, bg="#0a0c0f", width=340)
    sidebar.pack(side=tk.LEFT, fill=tk.Y, padx=(12, 0), pady=8)
    sidebar.pack_propagate(False)

    tk.Label(
        sidebar, text="ACTIVE KXBTC15M MARKETS", bg="#0a0c0f", fg="#c8ccd4",
        font=("Consolas", 10, "bold")
    ).pack(anchor="w", padx=12, pady=(8, 4))

    market_tree = ttk.Treeview(
        sidebar,
        columns=("ticker", "target", "yes_ask", "no_ask", "last"),
        show="headings",
        height=20
    )
    market_tree.heading("ticker",  text="Ticker")
    market_tree.heading("target",  text="BTC Target")
    market_tree.heading("yes_ask", text="YES ▲")
    market_tree.heading("no_ask",  text="NO  ▼")
    market_tree.heading("last",    text="Last")
    market_tree.column("ticker",  width=130, anchor="w")
    market_tree.column("target",  width=90,  anchor="center")
    market_tree.column("yes_ask", width=65,  anchor="center")
    market_tree.column("no_ask",  width=65,  anchor="center")
    market_tree.column("last",    width=55,  anchor="center")
    market_tree.pack(fill=tk.BOTH, expand=True, padx=8, pady=4)

    # ── Chart Area ───────────────────────────────────────────────────
    chart = BtcChart(root)

    if not config.paper_trade:
        chart.set_live_mode()

    # ── Live market data cache ───────────────────────────────────────
    latest_market_data: dict[str, dict] = {}

    def refresh_market_tree():
        for item in market_tree.get_children():
            market_tree.delete(item)
        for ticker, data in sorted(latest_market_data.items()):
            target  = data.get("target")
            bid     = data.get("bid")
            ask     = data.get("ask")
            tgt_str = f"${target:,.0f}" if target else "—"
            # YES ask = what you pay to bet "above target"
            yes_str = f"{int(round(ask*100))}c"  if ask  is not None else "—"
            # NO  ask = 1 - YES bid = what you pay to bet "below target"
            no_str  = f"{int(round((1.0-bid)*100))}c" if bid is not None else "—"
            last_str = f"{data.get('price'):.4f}" if data.get('price') is not None else "—"
            market_tree.insert("", "end", values=(
                ticker[-14:], tgt_str, yes_str, no_str, last_str,
            ))

    def update_header_labels(event: MarketUpdate):
        nonlocal target_label, book_label
        if event.target:
            target_label.config(text=f"BTC Target: ${event.target:,.0f}")
        yes_c = int(round((event.ask or 0) * 100)) if event.ask is not None else 0
        no_c  = int(round((1.0 - (event.bid or 1.0)) * 100)) if event.bid is not None else 0
        book_label.config(text=f"YES {yes_c:2d}c ▲   NO {no_c:2d}c ▼")
        # Amber if either side is in entry zone
        if (96 <= yes_c <= 98) or (96 <= no_c <= 98):
            book_label.config(fg="#f0a500")
        else:
            book_label.config(fg="#ffffff")

    def on_market_update(event: MarketUpdate):
        latest_market_data[event.ticker] = {
            "target": event.target,
            "price":  event.price,
            "bid":    event.bid,
            "ask":    event.ask,
        }
        root.after(0, refresh_market_tree)
        root.after(0, lambda e=event: update_header_labels(e))
        # Update chart YES/NO pills and target price
        root.after(0, lambda e=event: chart.update_orderbook(e.ask, e.bid))
        if event.target:
            root.after(0, lambda e=event: chart.update_target(e.target))

    event_bus.subscribe_market(on_market_update)

    # ── Shutdown callback ─────────────────────────────────────────────
    def _on_shutdown():
        logger.critical("SHUTDOWN: 2 consecutive losses — halting")
        root.after(0, on_close)

    # ── Risk manager ──────────────────────────────────────────────────
    risk_manager = RiskManager(
        db=db,
        signal_engine=signal_engine,
        execution_engine=execution_engine,
        config=config,
        client=client,
        on_shutdown=_on_shutdown,
    )

    # Forward trade events to chart
    def _on_trade(event):
        root.after(0, lambda: chart.add_trade(
            side=event.side,
            price=event.price,
            qty=event.qty,
            pnl=getattr(event, "pnl", None),
        ))

    event_bus.subscribe_trade(_on_trade)

    # ── Portfolio poller ──────────────────────────────────────────────
    portfolio_poller = PortfolioPoller(
        client=client,
        signal_engine=signal_engine,
        db=db,
        sizer=risk_manager._sizer,
    )
    risk_manager.set_poller(portfolio_poller)   # ← wire grace period + double-buy guard
    portfolio_poller.start()

    # ── BTC candle feed ───────────────────────────────────────────────
    btc_feed = BtcFeed()

    def _on_candles(candles):
        root.after(0, lambda: chart.update_candles(candles))
        root.after(0, lambda: chart.update_sizer_stats(risk_manager._sizer.stats))

    btc_feed.on_update(_on_candles)
    btc_feed.start()

    # ── Market loader ─────────────────────────────────────────────────
    def load_markets():
        nonlocal markets
        while not markets:
            try:
                logger.info("Fetching KXBTC15M markets...")
                markets = fetch_active_sports_markets(client, config)
                if not markets:
                    logger.warning("No BTC markets found — retrying in 30s")
                    time.sleep(30)
            except Exception as e:
                logger.error("Market fetch failed: %s", e)
                time.sleep(10)

    threading.Thread(target=load_markets, daemon=True).start()

    # ── Worker startup ────────────────────────────────────────────────
    def start_workers():
        nonlocal workers
        logger.info("Starting workers for %d BTC markets", len(markets))
        for m in markets:
            ticker    = m["ticker"]
            market_id = db.upsert_market(ticker, m.get("event", ""))
            worker    = MarketWorker(
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
            # Tell rotator when this market closes so it can roll on time
            close_ts = m.get("close_ts") or m.get("close_time") or 0
            if close_ts:
                rotator.register_market(ticker, int(close_ts))
        logger.info("All %d workers running", len(workers))

    def wait_for_markets():
        if not markets:
            root.after(500, wait_for_markets)
            return
        start_workers()

    root.after(100, wait_for_markets)

    # ── Market rotator ────────────────────────────────────────────────
    rotator = MarketRotator(
        client=client,
        db=db,
        signal_engine=signal_engine,
        risk_manager=risk_manager,
        config=config,
        workers=workers,
        workers_lock=workers_lock,
        on_remove=lambda ticker: None,
        on_add=lambda ticker, mid: None,
    )
    rotator.start()

    # ── Shutdown ──────────────────────────────────────────────────────
    def on_close() -> None:
        logger.info("Shutting down...")
        btc_feed.stop()
        portfolio_poller.stop()
        rotator.stop()

        with workers_lock:
            snapshot = list(workers)
        for w in snapshot:
            w.stop()
        for w in snapshot:
            w.join(timeout=2.0)

        root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_close)
    root.mainloop()
    logger.info("Shutdown complete.")


if __name__ == "__main__":
    main()