import asyncio
import logging
import os
import sqlite3
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import yfinance as yf
from dotenv import load_dotenv
from tastytrade import Account, Session
from tastytrade.instruments import NestedOptionChain
from tastytrade.market_data import get_market_data_by_type
from tastytrade.metrics import get_market_metrics
from tastytrade.order import (
    InstrumentType,
    Leg,
    NewOrder,
    OrderAction,
    OrderTimeInForce,
    OrderType,
    PriceEffect,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("atm-vol")

# --- Config ---
DB_PATH = os.getenv("DB_PATH", "strategy.db")
DTE_TARGETS = [30, 60, 90]
EXECUTION_DTE = 45
SPREAD_WIDTH_PCT = 0.05
MAX_POSITIONS = 20
CONTRACTS_PER_TRADE = 1
REBALANCE_DAY_RANGE = (5, 8)  # business days 5-8 of the month
SCAN_BATCH_SIZE = 20  # how many symbols to scan at once via tastytrade metrics
LOOP_INTERVAL_HOURS = 1
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
TT_ACCOUNT = os.getenv("TT_ACCOUNT", "")
PAPER_MODE = os.getenv("PAPER_MODE", "false").lower() == "true"
COMMISSION_PER_CONTRACT = 1.00  # per leg per contract

# --- Database ---

def init_db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            direction TEXT NOT NULL,
            long_symbol TEXT NOT NULL,
            short_symbol TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            entry_debit REAL,
            exit_credit REAL,
            commissions REAL,
            opened_at TEXT NOT NULL,
            closed_at TEXT
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS rebalances (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rebalance_date TEXT NOT NULL UNIQUE,
            tickers_long TEXT,
            tickers_short TEXT,
            completed_at TEXT
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS monthly_pnl (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month TEXT NOT NULL UNIQUE,
            gross_pnl REAL NOT NULL,
            commissions REAL NOT NULL,
            net_pnl REAL NOT NULL,
            num_trades INTEGER NOT NULL,
            cumulative_pnl REAL NOT NULL
        )
    """)
    con.commit()
    con.close()


def get_open_positions():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    rows = con.execute(
        "SELECT * FROM positions WHERE closed_at IS NULL"
    ).fetchall()
    con.close()
    return [dict(r) for r in rows]


def record_position(ticker, direction, long_sym, short_sym, qty, entry_debit=None):
    comms = qty * 2 * COMMISSION_PER_CONTRACT  # 2 legs
    con = sqlite3.connect(DB_PATH)
    con.execute(
        "INSERT INTO positions (ticker, direction, long_symbol, short_symbol, quantity, entry_debit, commissions, opened_at) VALUES (?,?,?,?,?,?,?,?)",
        (ticker, direction, long_sym, short_sym, qty, entry_debit, comms, datetime.now(timezone.utc).isoformat()),
    )
    con.commit()
    con.close()


def mark_position_closed(position_id, exit_credit=None):
    close_comms = COMMISSION_PER_CONTRACT * 2  # 2 legs to close
    con = sqlite3.connect(DB_PATH)
    con.execute(
        "UPDATE positions SET closed_at = ?, exit_credit = ?, commissions = commissions + ? WHERE id = ?",
        (datetime.now(timezone.utc).isoformat(), exit_credit, close_comms, position_id),
    )
    con.commit()
    con.close()


def was_rebalanced_this_month():
    con = sqlite3.connect(DB_PATH)
    today = date.today()
    first_of_month = today.replace(day=1).isoformat()
    row = con.execute(
        "SELECT 1 FROM rebalances WHERE rebalance_date >= ? LIMIT 1",
        (first_of_month,),
    ).fetchone()
    con.close()
    return row is not None


def record_rebalance(longs, shorts):
    con = sqlite3.connect(DB_PATH)
    con.execute(
        "INSERT OR IGNORE INTO rebalances (rebalance_date, tickers_long, tickers_short, completed_at) VALUES (?,?,?,?)",
        (
            date.today().isoformat(),
            ",".join(longs),
            ",".join(shorts),
            datetime.now(timezone.utc).isoformat(),
        ),
    )
    con.commit()
    con.close()


# --- Universe ---

def get_sp500_tickers():
    """Top 500 US equities by market cap via yfinance screener (S&P 500 proxy)."""
    query = yf.EquityQuery("and", [
        yf.EquityQuery("eq", ["region", "us"]),
        yf.EquityQuery("is-in", ["exchange", "NMS", "NYQ"]),
        yf.EquityQuery("gte", ["intradaymarketcap", 2_000_000_000]),
    ])
    tickers = []
    for offset in range(0, 500, 250):
        result = yf.screen(query, sortField="intradaymarketcap", sortAsc=False, size=250, offset=offset)
        tickers.extend(q["symbol"] for q in result.get("quotes", []))
    log.info(f"Fetched {len(tickers)} large-cap US tickers via yfinance screener")
    return tickers


async def filter_no_dividends_tt(session, tickers):
    clean = []
    for i in range(0, len(tickers), SCAN_BATCH_SIZE):
        batch = tickers[i : i + SCAN_BATCH_SIZE]
        try:
            metrics = await get_market_metrics(session, batch)
            for m in metrics:
                if (m.dividend_rate_per_share or 0) == 0 and (m.dividend_yield or 0) == 0:
                    clean.append(m.symbol)
        except Exception as e:
            log.warning(f"Metrics batch error: {e}")
    log.info(f"{len(clean)} tickers after removing dividend payers (tastytrade)")
    return clean


# --- Signals ---

def get_iv_signal_yf(ticker):
    """Check ATM call IV vs put IV at 30/60/90 DTE using yfinance.
    Returns 1 (long), -1 (short), or 0 (no signal).
    """
    try:
        tk = yf.Ticker(ticker)
        price = tk.info.get("regularMarketPrice") or tk.info.get("currentPrice")
        if not price:
            return 0
        expirations = tk.options
        if not expirations:
            return 0

        today = date.today()
        exp_dates = [datetime.strptime(e, "%Y-%m-%d").date() for e in expirations]

        votes = []
        for target_dte in DTE_TARGETS:
            target_date = today + timedelta(days=target_dte)
            closest = min(exp_dates, key=lambda d: abs((d - target_date).days))
            if abs((closest - target_date).days) > 14:
                continue
            exp_str = closest.strftime("%Y-%m-%d")
            chain = tk.option_chain(exp_str)

            # find ATM strike
            all_strikes = sorted(set(chain.calls["strike"].tolist()))
            atm_strike = min(all_strikes, key=lambda s: abs(s - price))

            call_row = chain.calls[chain.calls["strike"] == atm_strike]
            put_row = chain.puts[chain.puts["strike"] == atm_strike]

            if call_row.empty or put_row.empty:
                continue

            call_iv = call_row["impliedVolatility"].iloc[0]
            put_iv = put_row["impliedVolatility"].iloc[0]

            if call_iv > put_iv:
                votes.append(1)
            elif call_iv < put_iv:
                votes.append(-1)
            else:
                votes.append(0)

        if len(votes) < 2:
            return 0
        if all(v == 1 for v in votes):
            return 1
        if all(v == -1 for v in votes):
            return -1
        return 0

    except Exception as e:
        log.debug(f"IV signal error for {ticker}: {e}")
        return 0


def get_momentum_signal(ticker, lookback=252, skip=21):
    """12-1 month momentum. Returns 1 (bullish), -1 (bearish), or 0."""
    try:
        hist = yf.Ticker(ticker).history(period="15mo")
        if len(hist) < lookback:
            return 0
        ret = (hist["Close"].iloc[-skip] / hist["Close"].iloc[-lookback]) - 1
        if ret > 0.02:
            return 1
        elif ret < -0.02:
            return -1
        return 0
    except Exception as e:
        log.debug(f"Momentum error for {ticker}: {e}")
        return 0


def generate_signals(universe):
    longs, shorts = [], []
    total = len(universe)
    for i, ticker in enumerate(universe):
        if (i + 1) % 50 == 0:
            log.info(f"Scanning signals: {i + 1}/{total}")
        iv = get_iv_signal_yf(ticker)
        if iv == 0:
            continue
        mom = get_momentum_signal(ticker)
        if iv == 1 and mom == 1:
            longs.append(ticker)
            log.info(f"  LONG signal: {ticker} (iv={iv}, mom={mom})")
        elif iv == -1 and mom == -1:
            shorts.append(ticker)
            log.info(f"  SHORT signal: {ticker} (iv={iv}, mom={mom})")
    log.info(f"Signals complete: {len(longs)} long, {len(shorts)} short")
    return longs[:MAX_POSITIONS // 2], shorts[:MAX_POSITIONS // 2]


# --- Execution ---

def find_closest_expiry(expirations, target_dte):
    today = date.today()
    target = today + timedelta(days=target_dte)
    return min(expirations, key=lambda e: abs((e.expiration_date - target).days))


def pick_spread_strikes(strikes, price, direction):
    """Pick two strikes for a vertical spread.
    LONG (bull call): buy ATM call, sell OTM call above.
    SHORT (bear put): buy ATM put, sell OTM put below.
    """
    strike_prices = sorted(s.strike_price for s in strikes)
    atm = min(strike_prices, key=lambda s: abs(float(s) - price))
    width = float(atm) * SPREAD_WIDTH_PCT

    if direction == "long":
        otm_candidates = [s for s in strike_prices if s > atm]
        if not otm_candidates:
            return None, None
        otm = min(otm_candidates, key=lambda s: abs(float(s) - float(atm) - width))
        return atm, otm
    else:
        otm_candidates = [s for s in strike_prices if s < atm]
        if not otm_candidates:
            return None, None
        otm = min(otm_candidates, key=lambda s: abs(float(atm) - float(s) - width))
        return atm, otm


async def get_spread_mid_price(session, buy_sym, sell_sym):
    """Get the net debit mid-price for a vertical spread."""
    try:
        data = await get_market_data_by_type(session, options=[buy_sym, sell_sym])
        by_sym = {d.symbol: d for d in data}
        buy_md = by_sym.get(buy_sym)
        sell_md = by_sym.get(sell_sym)
        if buy_md and sell_md and buy_md.mid and sell_md.mid:
            return abs(buy_md.mid - sell_md.mid)
    except Exception as e:
        log.debug(f"Mid-price fetch failed: {e}")
    return None


async def close_all_positions(session, account):
    open_pos = get_open_positions()
    if not open_pos:
        log.info("No open positions to close")
        return

    for pos in open_pos:
        buy_sym, sell_sym = pos["long_symbol"], pos["short_symbol"]
        exit_credit = await get_spread_mid_price(session, buy_sym, sell_sym)
        exit_val = float(exit_credit) if exit_credit else None

        if not PAPER_MODE:
            broker_positions = await account.get_positions(
                session, instrument_type=InstrumentType.EQUITY_OPTION
            )
            broker_syms = {p.symbol: p for p in broker_positions if p.quantity != 0}

            legs = []
            for sym, action in [
                (buy_sym, OrderAction.SELL_TO_CLOSE),
                (sell_sym, OrderAction.BUY_TO_CLOSE),
            ]:
                if sym in broker_syms:
                    legs.append(Leg(
                        instrument_type=InstrumentType.EQUITY_OPTION,
                        symbol=sym,
                        action=action,
                        quantity=pos["quantity"],
                    ))

            if legs:
                order = NewOrder(
                    time_in_force=OrderTimeInForce.DAY,
                    order_type=OrderType.MARKET,
                    legs=legs,
                )
                try:
                    resp = await account.place_order(session, order, dry_run=DRY_RUN)
                    log.info(f"{'[DRY] ' if DRY_RUN else ''}Closed {pos['ticker']} {pos['direction']}: {resp}")
                except Exception as e:
                    log.error(f"Failed to close {pos['ticker']}: {e}")

        tag = "[PAPER] " if PAPER_MODE else ""
        log.info(
            f"{tag}Closed {pos['ticker']} {pos['direction']}: "
            f"entry={pos['entry_debit']} exit={exit_val}"
        )
        mark_position_closed(pos["id"], exit_credit=exit_val)


async def get_stock_price(session, ticker):
    """Get current stock price via tastytrade market data, yfinance fallback."""
    try:
        data = await get_market_data_by_type(session, equities=[ticker])
        if data and data[0].mark:
            return float(data[0].mark)
    except Exception:
        pass
    try:
        info = yf.Ticker(ticker).info
        return float(info.get("regularMarketPrice") or info.get("currentPrice") or 0)
    except Exception:
        return 0.0


async def open_vertical_spread(session, account, ticker, direction):
    try:
        chains = await NestedOptionChain.get(session, ticker)
        if not chains:
            log.warning(f"No option chain for {ticker}")
            return

        chain = chains[0]
        exp = find_closest_expiry(chain.expirations, EXECUTION_DTE)
        strikes = exp.strikes
        if not strikes:
            return

        price = await get_stock_price(session, ticker)
        if price <= 0:
            return

        atm_strike, otm_strike = pick_spread_strikes(strikes, price, direction)
        if atm_strike is None:
            log.warning(f"Could not find spread strikes for {ticker}")
            return

        strike_map = {s.strike_price: s for s in strikes}
        atm_s = strike_map.get(atm_strike)
        otm_s = strike_map.get(otm_strike)
        if not atm_s or not otm_s:
            return

        if direction == "long":
            buy_sym = atm_s.call
            sell_sym = otm_s.call
        else:
            buy_sym = atm_s.put
            sell_sym = otm_s.put

        mid = await get_spread_mid_price(session, buy_sym, sell_sym)
        entry = float(mid) if mid else None

        if PAPER_MODE:
            if entry is None:
                log.warning(f"No mid-price for {ticker}, skipping paper fill")
                return
            record_position(ticker, direction, buy_sym, sell_sym, CONTRACTS_PER_TRADE, entry_debit=entry)
            log.info(
                f"[PAPER] Filled {direction} spread on {ticker}: "
                f"{buy_sym}/{sell_sym} @ {exp.expiration_date} "
                f"strikes {atm_strike}/{otm_strike} debit={entry:.4f}"
            )
            return

        legs = [
            Leg(
                instrument_type=InstrumentType.EQUITY_OPTION,
                symbol=buy_sym,
                action=OrderAction.BUY_TO_OPEN,
                quantity=CONTRACTS_PER_TRADE,
            ),
            Leg(
                instrument_type=InstrumentType.EQUITY_OPTION,
                symbol=sell_sym,
                action=OrderAction.SELL_TO_OPEN,
                quantity=CONTRACTS_PER_TRADE,
            ),
        ]

        if mid and mid > 0:
            order = NewOrder(
                time_in_force=OrderTimeInForce.DAY,
                order_type=OrderType.LIMIT,
                price=Decimal(str(round(float(mid), 2))),
                price_effect=PriceEffect.DEBIT,
                legs=legs,
            )
        else:
            order = NewOrder(
                time_in_force=OrderTimeInForce.DAY,
                order_type=OrderType.MARKET,
                legs=legs,
            )

        resp = await account.place_order(session, order, dry_run=DRY_RUN)
        log.info(
            f"{'[DRY] ' if DRY_RUN else ''}Opened {direction} spread on {ticker}: "
            f"{buy_sym}/{sell_sym} @ {exp.expiration_date} "
            f"strikes {atm_strike}/{otm_strike}"
        )
        if not DRY_RUN:
            record_position(ticker, direction, buy_sym, sell_sym, CONTRACTS_PER_TRADE, entry_debit=entry)

    except Exception as e:
        log.error(f"Failed to open spread for {ticker}: {e}")


# --- P&L Tracking ---

def compute_monthly_pnl():
    """Compute and store P&L for positions closed this month."""
    con = sqlite3.connect(DB_PATH)
    today = date.today()
    month_str = today.strftime("%Y-%m")
    first_of_month = today.replace(day=1).isoformat()

    rows = con.execute(
        "SELECT entry_debit, exit_credit, commissions, quantity FROM positions WHERE closed_at >= ?",
        (first_of_month,),
    ).fetchall()

    if not rows:
        con.close()
        return

    gross_pnl = 0.0
    total_comms = 0.0
    for entry_debit, exit_credit, comms, qty in rows:
        if entry_debit is not None and exit_credit is not None:
            # P&L = (exit - entry) * 100 * qty for options
            gross_pnl += (exit_credit - entry_debit) * 100 * qty
        total_comms += comms or 0

    net_pnl = gross_pnl - total_comms

    # get cumulative
    prev = con.execute(
        "SELECT cumulative_pnl FROM monthly_pnl ORDER BY month DESC LIMIT 1"
    ).fetchone()
    cum = (prev[0] if prev else 0.0) + net_pnl

    con.execute(
        "INSERT OR REPLACE INTO monthly_pnl (month, gross_pnl, commissions, net_pnl, num_trades, cumulative_pnl) VALUES (?,?,?,?,?,?)",
        (month_str, round(gross_pnl, 2), round(total_comms, 2), round(net_pnl, 2), len(rows), round(cum, 2)),
    )
    con.commit()
    con.close()

    log.info(
        f"Monthly P&L [{month_str}]: gross=${gross_pnl:+.2f} comms=${total_comms:.2f} "
        f"net=${net_pnl:+.2f} trades={len(rows)} cumulative=${cum:+.2f}"
    )


def print_paper_summary():
    """Print full walk-forward results."""
    con = sqlite3.connect(DB_PATH)
    rows = con.execute("SELECT * FROM monthly_pnl ORDER BY month").fetchall()
    con.close()
    if not rows:
        log.info("No paper results yet")
        return
    log.info("=" * 70)
    log.info(f"{'Month':<10} {'Gross':>10} {'Comms':>10} {'Net':>10} {'Trades':>7} {'Cumulative':>12}")
    log.info("-" * 70)
    for row in rows:
        _, month, gross, comms, net, trades, cum = row
        log.info(f"{month:<10} {gross:>+10.2f} {comms:>10.2f} {net:>+10.2f} {trades:>7} {cum:>+12.2f}")
    log.info("=" * 70)


# --- Rebalance Logic ---

def is_rebalance_window():
    today = date.today()
    if today.weekday() >= 5:  # skip weekends
        return False
    # count business days from 1st of month to today
    bday_count = 0
    d = today.replace(day=1)
    while d <= today:
        if d.weekday() < 5:
            bday_count += 1
        d += timedelta(days=1)
    return REBALANCE_DAY_RANGE[0] <= bday_count <= REBALANCE_DAY_RANGE[1]


async def rebalance():
    if was_rebalanced_this_month():
        log.info("Already rebalanced this month, skipping")
        return
    if not is_rebalance_window():
        log.info("Not in rebalance window")
        return

    tag = "[PAPER] " if PAPER_MODE else ""
    log.info(f"=== {tag}REBALANCE START ===")

    # 1. get universe
    tickers = get_sp500_tickers()

    # 2. connect to tastytrade
    session = Session()
    async with session:
        if TT_ACCOUNT:
            account = await Account.get(session, TT_ACCOUNT)
        else:
            accounts = await Account.get(session)
            if not accounts:
                log.error("No tastytrade accounts found")
                return
            account = accounts[0]
        log.info(f"Using account {account.account_number} (paper={PAPER_MODE}, dry_run={DRY_RUN})")

        # 3. filter dividend payers via tastytrade metrics
        universe = await filter_no_dividends_tt(session, tickers)

        # 4. generate signals
        longs, shorts = generate_signals(universe)
        log.info(f"Final targets: LONG {longs}, SHORT {shorts}")

        # 5. close existing positions (paper: mark-to-market at TT mid)
        had_positions = bool(get_open_positions())
        await close_all_positions(session, account)

        if PAPER_MODE and had_positions:
            compute_monthly_pnl()

        # 6. open new positions (paper: record fill at TT mid)
        for ticker in longs:
            await open_vertical_spread(session, account, ticker, "long")
        for ticker in shorts:
            await open_vertical_spread(session, account, ticker, "short")

        record_rebalance(longs, shorts)

        if PAPER_MODE:
            print_paper_summary()

        log.info(f"=== {tag}REBALANCE COMPLETE ===")


# --- Main Loop ---

async def main_loop():
    init_db()
    mode = "PAPER" if PAPER_MODE else ("DRY-RUN" if DRY_RUN else "LIVE")
    log.info(f"Strategy process started (mode={mode})")

    while True:
        try:
            await rebalance()
        except Exception as e:
            log.error(f"Rebalance error: {e}", exc_info=True)

        log.info(f"Sleeping {LOOP_INTERVAL_HOURS}h until next check...")
        await asyncio.sleep(LOOP_INTERVAL_HOURS * 3600)


if __name__ == "__main__":
    asyncio.run(main_loop())
