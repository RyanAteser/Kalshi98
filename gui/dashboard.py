"""
gui/dashboard.py — Full trading terminal dashboard.

Bloomberg-style dark terminal aesthetic.
Panels: market ticker table, open positions, trade log, PnL summary.
Polls event_bus every 100ms for live updates from workers.
"""

from __future__ import annotations

import tkinter as tk
from tkinter import ttk, font as tkfont
from datetime import datetime
from typing import Optional
import threading

from core import event_bus
from core.event_bus import MarketUpdate, TradeEvent, SignalEvent

# ─── Palette ──────────────────────────────────────────────────────────────────
BG         = "#0a0c0f"       # near-black
BG2        = "#111418"       # panel background
BG3        = "#181c22"       # table row alt
BORDER     = "#1e2530"       # subtle borders
FG         = "#c8d0dc"       # primary text
FG_DIM     = "#4a5568"       # dimmed text
FG_HEADER  = "#7a8899"       # column headers

GREEN      = "#00e676"       # price up / buy
GREEN_DIM  = "#1a3d2b"       # buy row tint
RED        = "#ff5252"       # price down / sell
RED_DIM    = "#3d1a1a"       # sell row tint
AMBER      = "#ffab40"       # signals / warnings
AMBER_DIM  = "#3d2a10"       # signal row tint
CYAN       = "#40c4ff"       # position / info
CYAN_DIM   = "#0d2a3d"       # position row tint
WHITE      = "#e8edf3"

FONT_MONO  = ("Consolas", 11)
FONT_MONO_SM = ("Consolas", 9)
FONT_MONO_LG = ("Consolas", 13, "bold")
FONT_LABEL = ("Consolas", 10)
FONT_HEADER = ("Consolas", 9)
FONT_TITLE = ("Consolas", 16, "bold")
FONT_BIG   = ("Consolas", 24, "bold")

POLL_MS    = 100   # GUI refresh interval


def _fmt_price(v: Optional[float]) -> str:
    if v is None:
        return "  ---  "
    return f"{v * 100:6.2f}¢"


def _fmt_pnl(v: Optional[float]) -> str:
    if v is None:
        return "  ---  "
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.4f}"


def _short_ticker(ticker: str) -> str:
    """Shorten ticker to last two segments for display."""
    parts = ticker.split("-")
    if len(parts) >= 3:
        return f"…{parts[-2][:8]}-{parts[-1][:8]}"
    return ticker[:22]


def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


class TradingDashboard:
    """Main application window."""

    def __init__(self, root: tk.Tk, tickers: list[str]) -> None:
        self.root = root
        self.tickers = tickers

        # State
        self._market_data: dict[str, dict] = {t: {} for t in tickers}
        self._positions: dict[str, dict] = {}   # ticker -> position info
        self._realized_pnl: float = 0.0
        self._trade_count: int = 0
        self._start_time = datetime.now()

        # Lock for state mutations from event drain
        self._lock = threading.Lock()

        self._rotator = None   # set after rotator starts via set_rotator()
        self._build_ui()
        self._poll()

    # ─── UI construction ──────────────────────────────────────────────────────

    def _build_ui(self) -> None:
        root = self.root
        root.title("KALSHI TRADING TERMINAL")
        root.configure(bg=BG)
        root.minsize(1200, 700)

        self._style_ttk()

        # ── Top bar ─────────────────────────────────────────────────────
        topbar = tk.Frame(root, bg=BG, height=48)
        topbar.pack(fill="x", padx=0, pady=0)
        topbar.pack_propagate(False)

        tk.Label(
            topbar, text="◈  KALSHI TERMINAL", font=FONT_TITLE,
            fg=CYAN, bg=BG, padx=20,
        ).pack(side="left", pady=12)

        self._lbl_time = tk.Label(
            topbar, text="", font=FONT_MONO_SM, fg=FG_DIM, bg=BG, padx=12,
        )
        self._lbl_time.pack(side="right", pady=14)

        self._lbl_mode = tk.Label(
            topbar, text="● PAPER TRADE", font=("Consolas", 9, "bold"),
            fg=AMBER, bg=BG, padx=12,
        )
        self._lbl_mode.pack(side="right", pady=14)

        self._lbl_uptime = tk.Label(
            topbar, text="UPTIME  00:00:00", font=FONT_MONO_SM,
            fg=FG_DIM, bg=BG, padx=12,
        )
        self._lbl_uptime.pack(side="right", pady=14)

        # ── Divider ──────────────────────────────────────────────────────
        tk.Frame(root, bg=BORDER, height=1).pack(fill="x")

        # ── Main content ─────────────────────────────────────────────────
        main = tk.Frame(root, bg=BG)
        main.pack(fill="both", expand=True, padx=0, pady=0)

        # Left column: markets table (wide)
        left = tk.Frame(main, bg=BG)
        left.pack(side="left", fill="both", expand=True)

        # Right column: positions + log
        right = tk.Frame(main, bg=BG, width=360)
        right.pack(side="right", fill="y")
        right.pack_propagate(False)

        tk.Frame(main, bg=BORDER, width=1).pack(side="right", fill="y")

        self._build_market_table(left)
        self._build_right_panel(right)

        # ── Status bar ───────────────────────────────────────────────────
        tk.Frame(root, bg=BORDER, height=1).pack(fill="x")
        statusbar = tk.Frame(root, bg=BG2, height=28)
        statusbar.pack(fill="x")
        statusbar.pack_propagate(False)

        self._lbl_status = tk.Label(
            statusbar, text="● LIVE  |  Monitoring markets...",
            font=FONT_MONO_SM, fg=GREEN, bg=BG2, padx=12,
        )
        self._lbl_status.pack(side="left")

        self._lbl_pnl_bar = tk.Label(
            statusbar, text="TOTAL PnL  +0.0000",
            font=("Consolas", 9, "bold"), fg=FG_DIM, bg=BG2, padx=12,
        )
        self._lbl_pnl_bar.pack(side="right")

    def _style_ttk(self) -> None:
        s = ttk.Style()
        s.theme_use("clam")
        s.configure("Treeview",
                    background=BG2, foreground=FG,
                    fieldbackground=BG2,
                    rowheight=26,
                    font=FONT_MONO_SM,
                    borderwidth=0,
                    )
        s.configure("Treeview.Heading",
                    background=BG, foreground=FG_HEADER,
                    font=FONT_HEADER,
                    borderwidth=0, relief="flat",
                    )
        s.map("Treeview",
              background=[("selected", "#1e2d40")],
              foreground=[("selected", WHITE)],
              )

    def _build_market_table(self, parent: tk.Frame) -> None:
        # Section header
        hdr = tk.Frame(parent, bg=BG, height=36)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)

        tk.Label(
            hdr, text="MARKETS", font=("Consolas", 10, "bold"),
            fg=FG_DIM, bg=BG, padx=16,
        ).pack(side="left", pady=10)

        self._lbl_market_count = tk.Label(
            hdr, text=f"{len(self.tickers)} ACTIVE",
            font=FONT_MONO_SM, fg=CYAN, bg=BG, padx=8,
        )
        self._lbl_market_count.pack(side="left")

        # Table frame
        frame = tk.Frame(parent, bg=BG2)
        frame.pack(fill="both", expand=True, padx=8, pady=(0, 8))

        cols = ("ticker", "yes_bid", "yes_ask", "no_bid", "no_ask", "last", "spread", "vol", "signal", "updated")
        self._mkt_tree = ttk.Treeview(frame, columns=cols, show="headings", selectmode="browse")

        col_cfg = [
            ("ticker",  "MARKET",    200, "w"),
            ("yes_bid", "YES BID",    72, "e"),
            ("yes_ask", "YES ASK",    72, "e"),
            ("no_bid",  "NO BID",     72, "e"),
            ("no_ask",  "NO ASK",     72, "e"),
            ("last",    "LAST",       72, "e"),
            ("spread",  "SPREAD",     64, "e"),
            ("vol",     "VOL",        64, "e"),
            ("signal",  "SIGNAL",     80, "center"),
            ("updated", "UPDATED",    68, "center"),
        ]
        for cid, label, width, anchor in col_cfg:
            self._mkt_tree.heading(cid, text=label, anchor=anchor)
            self._mkt_tree.column(cid, width=width, anchor=anchor, stretch=(cid == "ticker"))

        # Tags for row coloring
        self._mkt_tree.tag_configure("buy",    background=GREEN_DIM)
        self._mkt_tree.tag_configure("sell",   background=RED_DIM)
        self._mkt_tree.tag_configure("signal", background=AMBER_DIM)
        self._mkt_tree.tag_configure("alt",    background=BG3)
        self._mkt_tree.tag_configure("normal", background=BG2)

        scrollbar = ttk.Scrollbar(frame, orient="vertical", command=self._mkt_tree.yview)
        self._mkt_tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side="right", fill="y")
        self._mkt_tree.pack(fill="both", expand=True)

        # Pre-populate rows
        self._mkt_rows: dict[str, str] = {}  # ticker -> iid
        for i, ticker in enumerate(self.tickers):
            tag = "alt" if i % 2 else "normal"
            iid = self._mkt_tree.insert(
                "", "end",
                values=(_short_ticker(ticker), "---", "---", "---", "---", "---", "---", "---", "WAITING", "---"),
                tags=(tag,)
            )
            self._mkt_rows[ticker] = iid

    def _build_right_panel(self, parent: tk.Frame) -> None:
        # ── PnL summary ───────────────────────────────────────────────
        pnl_frame = tk.Frame(parent, bg=BG2)
        pnl_frame.pack(fill="x", padx=8, pady=(8, 4))

        tk.Frame(pnl_frame, bg=BORDER, height=1).pack(fill="x")
        inner = tk.Frame(pnl_frame, bg=BG2)
        inner.pack(fill="x", padx=12, pady=8)

        tk.Label(inner, text="REALIZED PnL", font=FONT_HEADER,
                 fg=FG_DIM, bg=BG2).pack(anchor="w")
        self._lbl_pnl = tk.Label(
            inner, text="+$0.0000",
            font=("Consolas", 22, "bold"), fg=GREEN, bg=BG2,
        )
        self._lbl_pnl.pack(anchor="w", pady=(2, 6))

        stats = tk.Frame(inner, bg=BG2)
        stats.pack(fill="x")

        self._lbl_trades = tk.Label(stats, text="0 TRADES",
                                    font=FONT_MONO_SM, fg=FG_DIM, bg=BG2)
        self._lbl_trades.pack(side="left")

        self._lbl_open_pos = tk.Label(stats, text="0 OPEN",
                                      font=FONT_MONO_SM, fg=CYAN, bg=BG2)
        self._lbl_open_pos.pack(side="right")

        tk.Frame(pnl_frame, bg=BORDER, height=1).pack(fill="x")

        # ── Open positions ─────────────────────────────────────────────
        pos_hdr = tk.Frame(parent, bg=BG, height=32)
        pos_hdr.pack(fill="x", padx=8)
        pos_hdr.pack_propagate(False)
        tk.Label(pos_hdr, text="OPEN POSITIONS", font=("Consolas", 9, "bold"),
                 fg=FG_DIM, bg=BG).pack(side="left", pady=8)

        pos_frame = tk.Frame(parent, bg=BG2)
        pos_frame.pack(fill="x", padx=8, pady=(0, 4))

        pcols = ("market", "entry", "curr", "unreal")
        self._pos_tree = ttk.Treeview(pos_frame, columns=pcols, show="headings",
                                      height=4, selectmode="none")
        pos_col_cfg = [
            ("market", "MARKET",  120, "w"),
            ("entry",  "ENTRY",    72, "e"),
            ("curr",   "LAST",     72, "e"),
            ("unreal", "UNREAL",   80, "e"),
        ]
        for cid, lbl, w, a in pos_col_cfg:
            self._pos_tree.heading(cid, text=lbl, anchor=a)
            self._pos_tree.column(cid, width=w, anchor=a)
        self._pos_tree.tag_configure("pos_open", background=CYAN_DIM, foreground=CYAN)
        self._pos_tree.pack(fill="x")

        # ── Activity log ───────────────────────────────────────────────
        log_hdr = tk.Frame(parent, bg=BG, height=32)
        log_hdr.pack(fill="x", padx=8)
        log_hdr.pack_propagate(False)
        tk.Label(log_hdr, text="ACTIVITY LOG", font=("Consolas", 9, "bold"),
                 fg=FG_DIM, bg=BG).pack(side="left", pady=8)

        log_frame = tk.Frame(parent, bg=BG2)
        log_frame.pack(fill="both", expand=True, padx=8, pady=(0, 8))

        self._log_text = tk.Text(
            log_frame,
            bg=BG2, fg=FG, font=FONT_MONO_SM,
            state="disabled", wrap="none",
            relief="flat", borderwidth=0,
            insertbackground=FG, selectbackground="#1e2d40",
        )
        log_scroll = ttk.Scrollbar(log_frame, orient="vertical", command=self._log_text.yview)
        self._log_text.configure(yscrollcommand=log_scroll.set)
        log_scroll.pack(side="right", fill="y")
        self._log_text.pack(fill="both", expand=True)

        # Text tags for coloring
        self._log_text.tag_configure("buy",    foreground=GREEN)
        self._log_text.tag_configure("sell",   foreground=RED)
        self._log_text.tag_configure("signal", foreground=AMBER)
        self._log_text.tag_configure("dim",    foreground=FG_DIM)
        self._log_text.tag_configure("cyan",   foreground=CYAN)

        self._log("SYSTEM", "Trading terminal online", "cyan")
        self._log("SYSTEM", f"Monitoring {len(self.tickers)} markets", "cyan")

    # ─── Logging ──────────────────────────────────────────────────────────────

    def _log(self, tag: str, msg: str, color: str = "dim") -> None:
        ts = _ts()
        line = f"{ts}  [{tag:<8}]  {msg}\n"
        self._log_text.configure(state="normal")
        self._log_text.insert("end", line, color)
        # Keep log to last 500 lines
        lines = int(self._log_text.index("end-1c").split(".")[0])
        if lines > 500:
            self._log_text.delete("1.0", "50.0")
        self._log_text.see("end")
        self._log_text.configure(state="disabled")

    # ─── Event processing ─────────────────────────────────────────────────────

    def _poll(self) -> None:
        """Drain event bus and update UI. Called every POLL_MS. Never raises."""
        try:
            events = event_bus.drain(max_items=500)
            for ev in events:
                try:
                    if isinstance(ev, MarketUpdate):
                        self._on_market_update(ev)
                    elif isinstance(ev, TradeEvent):
                        self._on_trade(ev)
                    elif isinstance(ev, SignalEvent):
                        self._on_signal(ev)
                except Exception as e:
                    # Per-event error — don't let one bad event kill the whole poll
                    self._log("ERROR", f"Event processing failed: {str(e)[:80]}", "sell")

            # Always update clocks and positions (even if no new events)
            self._update_clocks()
            self._update_positions_panel()

        except Exception as e:
            # Outer safety net — never let _poll die
            try:
                self._log("GUI-ERR", f"Poll exception: {str(e)[:100]}", "sell")
            except Exception:
                pass  # last resort protection

        finally:
            # CRITICAL: Always reschedule the next poll
            self.root.after(POLL_MS, self._poll)
    def _on_market_update(self, ev: MarketUpdate) -> None:
        if ev.ticker not in self._mkt_rows:
            return

        # Update state
        prev_price = self._market_data[ev.ticker].get("price")
        if self._rotator:
            self._rotator.update_price(ev.ticker, ev.price, ev.ask, ev.bid)
        self._market_data[ev.ticker] = {
            "price": ev.price,
            "bid": ev.bid,
            "ask": ev.ask,
            "spread": ev.spread,
            "volume": ev.volume,
            "updated": _ts(),
        }

        # Update position current price
        if ev.ticker in self._positions and ev.price is not None:
            self._positions[ev.ticker]["curr"] = ev.price

        # Pick row color
        signal_val = self._market_data[ev.ticker].get("signal", "WAITING")
        if signal_val in ("ENTRY", "BUY"):
            tag = "buy"
        elif signal_val in ("STOP_LOSS", "SELL"):
            tag = "sell"
        elif ev.ticker in self._positions:
            tag = "buy"  # has open position
        else:
            iid = self._mkt_rows[ev.ticker]
            idx = list(self._mkt_rows.keys()).index(ev.ticker)
            tag = "alt" if idx % 2 else "normal"

        # Price color indicator
        price_str = _fmt_price(ev.price)
        if prev_price is not None and ev.price is not None:
            if ev.price > prev_price:
                price_str = "▲ " + _fmt_price(ev.price)
            elif ev.price < prev_price:
                price_str = "▼ " + _fmt_price(ev.price)

        iid = self._mkt_rows[ev.ticker]
        spread_str = f"{ev.spread * 100:5.2f}¢" if ev.spread is not None else "  ---"
        vol_str = f"{ev.volume:6.0f}" if ev.volume is not None else "  ---"

        # NO side: NO bid = 1 - YES ask, NO ask = 1 - YES bid
        no_bid = (1.0 - ev.ask) if ev.ask is not None else None
        no_ask = (1.0 - ev.bid) if ev.bid is not None else None

        self._mkt_tree.item(iid, values=(
            _short_ticker(ev.ticker),
            _fmt_price(ev.bid),
            _fmt_price(ev.ask),
            _fmt_price(no_bid),
            _fmt_price(no_ask),
            price_str,
            spread_str,
            vol_str,
            self._market_data[ev.ticker].get("signal", "WAITING"),
            _ts(),
        ), tags=(tag,))

    def _on_signal(self, ev: SignalEvent) -> None:
        if ev.ticker in self._market_data:
            self._market_data[ev.ticker]["signal"] = ev.signal_type

        color = "buy" if ev.signal_type == "ENTRY" else "sell" if ev.signal_type in ("STOP_LOSS", "EXIT") else "signal"
        self._log(ev.signal_type, f"{_short_ticker(ev.ticker)}  @{_fmt_price(ev.price)}", color)

        # Update market table signal column
        if ev.ticker in self._mkt_rows:
            iid = self._mkt_rows[ev.ticker]
            vals = list(self._mkt_tree.item(iid, "values"))
            vals[8] = ev.signal_type
            self._mkt_tree.item(iid, values=vals)

    def _on_trade(self, ev: TradeEvent) -> None:
        self._trade_count += 1

        if ev.side == "BUY":
            self._positions[ev.ticker] = {
                "entry": ev.price,
                "curr": ev.price,
                "qty": ev.qty,
            }
            self._log("BUY", f"{_short_ticker(ev.ticker)}  entry={_fmt_price(ev.price)}  qty={ev.qty}", "buy")

        elif ev.side == "SELL":
            self._positions.pop(ev.ticker, None)
            pnl = ev.pnl or 0.0
            self._realized_pnl += pnl
            pnl_str = _fmt_pnl(ev.pnl)
            self._log("SELL", f"{_short_ticker(ev.ticker)}  exit={_fmt_price(ev.price)}  PnL={pnl_str}", "sell")
            self._update_pnl_display()

        self._lbl_trades.config(text=f"{self._trade_count} TRADES")

    # ─── Periodic panel updates ───────────────────────────────────────────────

    def _update_positions_panel(self) -> None:
        # Clear and re-render positions treeview
        for row in self._pos_tree.get_children():
            self._pos_tree.delete(row)

        for ticker, pos in self._positions.items():
            entry = pos.get("entry")
            curr  = pos.get("curr")
            unreal = (curr - entry) * pos.get("qty", 1) if entry and curr else None
            self._pos_tree.insert("", "end", values=(
                _short_ticker(ticker),
                _fmt_price(entry),
                _fmt_price(curr),
                _fmt_pnl(unreal),
            ), tags=("pos_open",))

        self._lbl_open_pos.config(text=f"{len(self._positions)} OPEN")

    def _update_pnl_display(self) -> None:
        pnl = self._realized_pnl
        color = GREEN if pnl >= 0 else RED
        sign = "+" if pnl >= 0 else ""
        self._lbl_pnl.config(text=f"{sign}${pnl:.4f}", fg=color)
        self._lbl_pnl_bar.config(
            text=f"REALIZED PnL  {sign}{pnl:.4f}",
            fg=color,
        )

    def _update_clocks(self) -> None:
        now = datetime.now()
        self._lbl_time.config(text=now.strftime("%Y-%m-%d  %H:%M:%S"))
        elapsed = now - self._start_time
        h, rem = divmod(int(elapsed.total_seconds()), 3600)
        m, s = divmod(rem, 60)
        self._lbl_uptime.config(text=f"UPTIME  {h:02d}:{m:02d}:{s:02d}")

    def set_live_mode(self) -> None:
        """Call this when PAPER_TRADE=false."""
        self._lbl_mode.config(text="● LIVE TRADING", fg=RED)

    def set_rotator(self, rotator) -> None:
        """Inject rotator reference so dashboard can feed prices back to it."""
        self._rotator = rotator

    def add_ticker(self, ticker: str) -> None:
        """Dynamically add a new market row with duplicate protection."""
        # 1. If it already exists, remove it first to ensure a fresh UI state
        if ticker in self._mkt_rows:
            self.remove_ticker(ticker)

        self.tickers.append(ticker)
        self._market_data[ticker] = {}

        idx = len(self._mkt_rows)
        tag = "alt" if idx % 2 else "normal"

        iid = self._mkt_tree.insert(
            "", "end",
            values=(_short_ticker(ticker), "---", "---", "---", "---", "---", "---", "---", "NEW", "---"),
            tags=(tag,)
        )
        self._mkt_rows[ticker] = iid
        self._lbl_market_count.config(text=f"{len(self.tickers)} ACTIVE")
        self._log("ROTATE", f"Added {_short_ticker(ticker)}", "cyan")

    def remove_ticker(self, ticker: str) -> None:
        """Forcefully remove a ticker from the UI and internal tracking."""
        # Remove from internal list
        self.tickers = [t for t in self.tickers if t != ticker]

        # Remove from the treeview
        iid = self._mkt_rows.pop(ticker, None)
        if iid:
            try:
                if self._mkt_tree.exists(iid):
                    self._mkt_tree.delete(iid)
            except Exception as e:
                self._log("GUI-ERR", f"Failed to delete {ticker}: {e}", "red")

        # Clear the cached data
        self._market_data.pop(ticker, None)
        self._lbl_market_count.config(text=f"{len(self.tickers)} ACTIVE")