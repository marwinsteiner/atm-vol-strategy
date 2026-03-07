# atm-vol-strategy

Systematic equity long/short factor model using ATM implied volatility term structure and quantitative momentum, executed via vertical spreads on Tastytrade.

## Strategy

**Factor 1 — ATM IV Term Structure**

Compare at-the-money call IV vs put IV at three time buckets (30, 60, 90 DTE, nearest available expiry). All three must agree:

- Call IV > Put IV across all three tenors → LONG signal
- Call IV < Put IV across all three tenors → SHORT signal

**Factor 2 — Momentum Confirmation**

12-1 month price momentum (skip the most recent month to avoid short-term reversal). Signal must confirm the IV direction:

- LONG: IV long + positive momentum
- SHORT: IV short + negative momentum

**Execution**

- LONG → Bull call spread (buy ATM call, sell ~5% OTM call) at ~45 DTE
- SHORT → Bear put spread (buy ATM put, sell ~5% OTM put) at ~45 DTE
- Limit orders at mid-price when available, market fallback

**Universe**

Top 500 US equities by market cap (NYSE/NASDAQ) via yfinance screener. Stocks paying dividends are excluded using Tastytrade market metrics.

**Rebalance**

Monthly, business days 5–8 of each month (avoids first-of-month rebalancing flows). Max 20 simultaneous positions (10 long, 10 short).

## Modes

| Mode | Env Vars | Behavior |
|------|----------|----------|
| **Paper** | `PAPER_MODE=true` | Full TT data pipeline, no orders routed. Fills assumed at TT mid-price. Tracks P&L month-by-month with $1/contract/leg commissions. |
| **Dry-run** | `DRY_RUN=true` | Sends orders to TT dry-run endpoint (validation only, no execution). |
| **Live** | `DRY_RUN=false` | Real order placement. |

## Setup

```bash
cp .env.example .env
# Fill in TT_SECRET and TT_REFRESH (Tastytrade OAuth credentials)
uv sync
uv run python main.py
```

Runs as a long-lived process, checking hourly whether a rebalance is due.

## Bookkeeping

SQLite database (`strategy.db`) with three tables:

- `positions` — open/closed positions with entry/exit prices and commissions
- `rebalances` — monthly rebalance log
- `monthly_pnl` — gross, net, cumulative P&L per month (paper mode)
