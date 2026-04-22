"""
btc_chart.py — Bloomberg-style BTC 15m candlestick chart for tkinter.

Features:
  - OHLC candlestick chart with wick lines
  - Volume bars at the bottom
  - Current price line + label
  - Entry markers (green triangle) when bot buys
  - Trade log panel on the right
  - Auto-scrolls to keep latest candle visible
  - Dark terminal theme matching existing dashboard
"""

from __future__ import annotations

import tkinter as tk
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from core.btc_feed import Candle

# ── Theme ─────────────────────────────────────────────────────────────
BG          = "#0a0c0f"
PANEL_BG    = "#111318"
GRID        = "#1a1d24"
TEXT        = "#c8ccd4"
TEXT_DIM    = "#555a66"
GREEN       = "#00e676"
RED         = "#f44336"
AMBER       = "#f0a500"
BLUE        = "#29b6f6"
WHITE       = "#ffffff"

CANDLE_W    = 10    # candle body width px
CANDLE_GAP  = 3     # gap between candles
CHART_PAD_L = 70    # left padding for price axis
CHART_PAD_R = 20
CHART_PAD_T = 30
CHART_PAD_B = 60    # bottom for volume + time axis
VOL_HEIGHT  = 50    # volume panel height px


@dataclass
class TradeMarker:
    candle_ts: int       # timestamp of the candle when trade fired
    price:     float
    side:      str       # "BUY" or "SELL"
    qty:       int
    pnl:       Optional[float] = None


class BtcChart(tk.Frame):
    """
    Full BTC 15m candlestick chart widget.
    Drop-in replacement for TradingDashboard.
    """

    def __init__(self, parent: tk.Widget, **kwargs) -> None:
        super().__init__(parent, bg=BG, **kwargs)

        self._candles:  List[Candle]      = []
        self._markers:  List[TradeMarker] = []
        self._offset    = 0    # scroll offset (candles from right)
        self._mode_label = "PAPER"

        self._build_ui()

    # ── Public API ────────────────────────────────────────────────────

    def update_candles(self, candles: List[Candle]) -> None:
        """Called from BtcFeed callback (may be off main thread — use after())."""
        self._candles = candles
        self._redraw()

    def update_orderbook(
            self,
            yes_ask: Optional[float],
            yes_bid: Optional[float],
    ) -> None:
        """
        Update the YES/NO price pills in the header.
        yes_ask  = cost to buy YES (BTC goes UP)   e.g. 0.97 → "97c ▲"
        yes_bid  = best bid for YES
        no_ask   = 1 - yes_bid  (cost to buy NO, BTC goes DOWN)
        """
        if yes_ask is not None:
            yes_c = int(round(yes_ask * 100))
            # Highlight if in entry zone
            in_zone = 96 <= yes_c <= 98
            self._yes_lbl.config(
                text=f"YES {yes_c:2d}c ▲",
                bg="#1a4a2a" if in_zone else "#0d2b1a",
                fg="#00ff88" if in_zone else GREEN,
            )

        if yes_bid is not None:
            no_ask = 1.0 - yes_bid
            no_c   = int(round(no_ask * 100))
            in_zone = 96 <= no_c <= 98
            self._no_lbl.config(
                text=f"NO  {no_c:2d}c ▼",
                bg="#4a1a1a" if in_zone else "#2b0d0d",
                fg="#ff6666" if in_zone else RED,
            )

    def update_target(self, btc_target: float) -> None:
        """Update the BTC strike price display."""
        if btc_target and btc_target > 1000:
            self._target_lbl.config(text=f"TARGET ${btc_target:,.0f}")
        else:
            self._target_lbl.config(text="TARGET $——,———")

    def add_trade(self, side: str, price: float, qty: int,
                  pnl: Optional[float] = None) -> None:
        """Record a trade marker on the chart."""
        ts = self._candles[0].ts if self._candles else 0
        self._markers.append(TradeMarker(
            candle_ts=ts, price=price, side=side, qty=qty, pnl=pnl,
        ))
        self._add_trade_log(side, price, qty, pnl)
        self._redraw()

    def set_live_mode(self) -> None:
        self._mode_label = "LIVE"
        self._mode_lbl.config(text="● LIVE", fg=RED)

    def set_rotator(self, _) -> None:
        pass   # not needed for BTC chart

    def add_ticker(self, _) -> None:
        pass

    def remove_ticker(self, _) -> None:
        pass

    # ── UI construction ───────────────────────────────────────────────

    def _build_ui(self) -> None:
        self.pack(fill=tk.BOTH, expand=True)

        # ── Header bar ────────────────────────────────────────────────
        header = tk.Frame(self, bg=PANEL_BG, height=36)
        header.pack(fill=tk.X, side=tk.TOP)
        header.pack_propagate(False)

        tk.Label(
            header, text="BTC/USD  15m", bg=PANEL_BG, fg=WHITE,
            font=("Consolas", 13, "bold"), anchor="w",
        ).pack(side=tk.LEFT, padx=12, pady=6)

        self._price_lbl = tk.Label(
            header, text="$——,———.——", bg=PANEL_BG, fg=GREEN,
            font=("Consolas", 13, "bold"),
        )
        self._price_lbl.pack(side=tk.LEFT, padx=16)

        self._change_lbl = tk.Label(
            header, text="", bg=PANEL_BG, fg=TEXT_DIM,
            font=("Consolas", 10),
        )
        self._change_lbl.pack(side=tk.LEFT, padx=4)

        # YES / NO price pills — the key trading info
        self._yes_lbl = tk.Label(
            header,
            text="YES ——c ▲",
            bg="#0d2b1a", fg=GREEN,
            font=("Consolas", 10, "bold"),
            padx=8, pady=2, relief=tk.FLAT,
        )
        self._yes_lbl.pack(side=tk.LEFT, padx=(12, 2))

        self._no_lbl = tk.Label(
            header,
            text="NO  ——c ▼",
            bg="#2b0d0d", fg=RED,
            font=("Consolas", 10, "bold"),
            padx=8, pady=2, relief=tk.FLAT,
        )
        self._no_lbl.pack(side=tk.LEFT, padx=(2, 12))

        # Target strike price
        self._target_lbl = tk.Label(
            header, text="TARGET $——,———", bg=PANEL_BG, fg="#00e6d6",
            font=("Consolas", 10, "bold"),
        )
        self._target_lbl.pack(side=tk.LEFT, padx=8)

        self._mode_lbl = tk.Label(
            header, text="● PAPER", bg=PANEL_BG, fg=AMBER,
            font=("Consolas", 10, "bold"),
        )
        self._mode_lbl.pack(side=tk.RIGHT, padx=12)

        self._sizer_lbl = tk.Label(
            header, text="qty: —", bg=PANEL_BG, fg=TEXT_DIM,
            font=("Consolas", 10),
        )
        self._sizer_lbl.pack(side=tk.RIGHT, padx=12)

        # ── Main area: chart + trade log ──────────────────────────────
        body = tk.Frame(self, bg=BG)
        body.pack(fill=tk.BOTH, expand=True)

        # Chart canvas
        self._canvas = tk.Canvas(
            body, bg=BG, highlightthickness=0, cursor="crosshair",
        )
        self._canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self._canvas.bind("<Configure>", lambda _: self._redraw())
        self._canvas.bind("<MouseWheel>", self._on_scroll)
        self._canvas.bind("<Button-4>",   self._on_scroll)
        self._canvas.bind("<Button-5>",   self._on_scroll)

        # Trade log panel
        log_frame = tk.Frame(body, bg=PANEL_BG, width=220)
        log_frame.pack(side=tk.RIGHT, fill=tk.Y)
        log_frame.pack_propagate(False)

        tk.Label(
            log_frame, text="TRADE LOG", bg=PANEL_BG, fg=TEXT_DIM,
            font=("Consolas", 9, "bold"), anchor="w",
        ).pack(fill=tk.X, padx=8, pady=(8, 2))

        self._log_text = tk.Text(
            log_frame, bg=PANEL_BG, fg=TEXT,
            font=("Consolas", 9), state=tk.DISABLED,
            borderwidth=0, highlightthickness=0,
            wrap=tk.WORD,
        )
        self._log_text.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)
        self._log_text.tag_config("buy",  foreground=GREEN)
        self._log_text.tag_config("sell", foreground=RED)
        self._log_text.tag_config("win",  foreground=GREEN)
        self._log_text.tag_config("loss", foreground=RED)
        self._log_text.tag_config("dim",  foreground=TEXT_DIM)

        # Periodic redraw — ensures chart stays live even between candle updates
        self.after(2000, self._schedule_refresh)

    # ── Periodic refresh ──────────────────────────────────────────────

    def _schedule_refresh(self) -> None:
        self._redraw()
        self.after(2000, self._schedule_refresh)

    # ── Scrolling ─────────────────────────────────────────────────────

    def _on_scroll(self, event) -> None:
        if event.num == 4 or event.delta > 0:
            self._offset = min(self._offset + 3, max(0, len(self._candles) - 5))
        else:
            self._offset = max(self._offset - 3, 0)
        self._redraw()

    # ── Drawing ───────────────────────────────────────────────────────

    def _redraw(self) -> None:
        c = self._canvas
        c.delete("all")

        if not self._candles:
            c.create_text(
                c.winfo_width() // 2, c.winfo_height() // 2,
                text="Waiting for BTC data...",
                fill=TEXT_DIM, font=("Consolas", 12),
                )
            return

        W = c.winfo_width()
        H = c.winfo_height()
        if W < 50 or H < 50:
            return

        chart_h = H - CHART_PAD_T - CHART_PAD_B - VOL_HEIGHT
        chart_w = W - CHART_PAD_L - CHART_PAD_R
        x0      = CHART_PAD_L
        y0      = CHART_PAD_T
        y_vol   = y0 + chart_h + 10

        # ── Visible candles ───────────────────────────────────────────
        step        = CANDLE_W + CANDLE_GAP
        n_visible   = max(1, chart_w // step)
        end_idx     = self._offset
        start_idx   = min(end_idx + n_visible, len(self._candles))
        visible     = self._candles[end_idx:start_idx]

        if not visible:
            return

        # Update header price
        latest = visible[0]
        prev   = visible[1] if len(visible) > 1 else latest
        chg    = ((latest.close - prev.close) / prev.close * 100) if prev.close else 0
        self._price_lbl.config(
            text=f"${latest.close:,.2f}",
            fg=GREEN if chg >= 0 else RED,
        )
        self._change_lbl.config(
            text=f"{'▲' if chg >= 0 else '▼'} {abs(chg):.2f}%",
            fg=GREEN if chg >= 0 else RED,
        )

        # ── Price range ───────────────────────────────────────────────
        lo   = min(c.low  for c in visible) * 0.9995
        hi   = max(c.high for c in visible) * 1.0005
        rng  = hi - lo or 1.0

        def px(price: float) -> float:
            return y0 + chart_h - (price - lo) / rng * chart_h

        def vx(vol: float, max_vol: float) -> float:
            return VOL_HEIGHT * (vol / max_vol) if max_vol else 0

        max_vol = max((c.volume for c in visible), default=1)

        # ── Grid lines ────────────────────────────────────────────────
        n_grid = 5
        for i in range(n_grid + 1):
            gy = y0 + (chart_h / n_grid) * i
            c.create_line(x0, gy, x0 + chart_w, gy, fill=GRID, dash=(2, 4))
            price_at = hi - (rng / n_grid) * i
            c.create_text(
                x0 - 5, gy, text=f"{price_at:,.0f}",
                fill=TEXT_DIM, font=("Consolas", 8), anchor="e",
                )

        # ── Candles ───────────────────────────────────────────────────
        for i, candle in enumerate(reversed(visible)):
            cx   = x0 + chart_w - (i + 1) * step + CANDLE_W // 2
            bull = candle.is_bullish
            col  = GREEN if bull else RED

            body_top = px(max(candle.open, candle.close))
            body_bot = px(min(candle.open, candle.close))
            wick_top = px(candle.high)
            wick_bot = px(candle.low)

            # Ensure minimum body height
            if body_bot - body_top < 1:
                body_top -= 0.5
                body_bot += 0.5

            # Wick
            c.create_line(cx, wick_top, cx, wick_bot, fill=col, width=1)

            # Body
            c.create_rectangle(
                cx - CANDLE_W // 2, body_top,
                cx + CANDLE_W // 2, body_bot,
                fill=col, outline=col,
                )

            # Volume bar
            vh = vx(candle.volume, max_vol)
            c.create_rectangle(
                cx - CANDLE_W // 2, y_vol + VOL_HEIGHT - vh,
                cx + CANDLE_W // 2, y_vol + VOL_HEIGHT,
                fill=col, outline="",
                )

            # Time label every 4 candles
            if i % 4 == 0:
                t = datetime.fromtimestamp(candle.ts, tz=timezone.utc)
                c.create_text(
                    cx, H - 8,
                    text=t.strftime("%H:%M"),
                    fill=TEXT_DIM, font=("Consolas", 7),
                        )

        # ── Current price line ────────────────────────────────────────
        cur_y = px(latest.close)
        c.create_line(
            x0, cur_y, x0 + chart_w, cur_y,
            fill=BLUE, dash=(4, 3), width=1,
                       )
        c.create_rectangle(
            x0 + chart_w, cur_y - 9,
            x0 + chart_w + CHART_PAD_R + 60, cur_y + 9,
            fill=BLUE, outline="",
            )
        c.create_text(
            x0 + chart_w + 4, cur_y,
            text=f"{latest.close:,.0f}",
            fill=BG, font=("Consolas", 8, "bold"), anchor="w",
            )

        # ── Trade markers ─────────────────────────────────────────────
        for marker in self._markers:
            # Find which visible candle this trade belongs to
            for i, candle in enumerate(reversed(visible)):
                if candle.ts == marker.candle_ts:
                    cx = x0 + chart_w - (i + 1) * step + CANDLE_W // 2
                    my = px(marker.price)
                    if marker.side == "BUY":
                        # Green upward triangle
                        c.create_polygon(
                            cx, my - 10,
                                cx - 7, my + 2,
                                cx + 7, my + 2,
                            fill=GREEN, outline=GREEN,
                                )
                    else:
                        # Red downward triangle
                        c.create_polygon(
                            cx, my + 10,
                                cx - 7, my - 2,
                                cx + 7, my - 2,
                            fill=RED, outline=RED,
                                )
                    break

        # ── Axis lines ────────────────────────────────────────────────
        c.create_line(x0, y0, x0, y0 + chart_h, fill=GRID, width=1)
        c.create_line(x0, y0 + chart_h, x0 + chart_w, y0 + chart_h, fill=GRID, width=1)

    # ── Trade log ─────────────────────────────────────────────────────

    def _add_trade_log(
            self, side: str, price: float, qty: int, pnl: Optional[float],
    ) -> None:
        t  = datetime.now().strftime("%H:%M:%S")
        self._log_text.config(state=tk.NORMAL)

        self._log_text.insert(tk.END, f"{t}  ", "dim")

        if side == "BUY":
            self._log_text.insert(
                tk.END, f"BUY  {qty}@ ${price:.2f}\n", "buy",
            )
        else:
            tag = "win" if (pnl or 0) > 0 else "loss"
            pnl_str = f"  {'+' if (pnl or 0)>=0 else ''}{(pnl or 0):.2f}"
            self._log_text.insert(
                tk.END, f"SELL {qty}@ ${price:.2f}{pnl_str}\n", tag,
            )

        self._log_text.see(tk.END)
        self._log_text.config(state=tk.DISABLED)

    def update_sizer_stats(self, stats: dict) -> None:
        """Call from gui_main to show sizer state in header."""
        qty    = stats.get("current_qty", "—")
        streak = stats.get("consecutive_wins", 0)
        losses = stats.get("consecutive_losses", 0)
        self._sizer_lbl.config(
            text=f"qty:{qty}  streak:{streak}W/{losses}L",
            fg=GREEN if streak > 0 else (RED if losses > 0 else TEXT_DIM),
        )