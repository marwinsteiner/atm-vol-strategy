import asyncio
import logging
import os
import sqlite3
import time
from datetime import date, datetime, timedelta
from decimal import Decimal

import numpy as np
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from tastytrade import Account, Session
from tastytrade.instruments import NestedOptionChain
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
    con.commit()
    con.close()


def get_open_positions():
    con = sqlite3.connect(DB_PATH)
    rows = con.execute(
        "SELECT * FROM positions WHERE closed_at IS NULL"
    ).fetchall()
    con.close()
    cols = ["id", "ticker", "direction", "long_symbol", "short_symbol",
            "quantity", "opened_at", "closed_at"]
    return [dict(zip(cols, r)) for r in rows]


def record_position(ticker, direction, long_sym, short_sym, qty):
    con = sqlite3.connect(DB_PATH)
    con.execute(
        "INSERT INTO positions (ticker, direction, long_symbol, short_symbol, quantity, opened_at) VALUES (?,?,?,?,?,?)",
        (ticker, direction, long_sym, short_sym, qty, datetime.utcnow().isoformat()),
    )
    con.commit()
    con.close()


def mark_position_closed(position_id):
    con = sqlite3.connect(DB_PATH)
    con.execute(
        "UPDATE positions SET closed_at = ? WHERE id = ?",
        (datetime.utcnow().isoformat(), position_id),
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
            datetime.utcnow().isoformat(),
        ),
    )
    con.commit()
    con.close()


# --- Universe ---

def get_sp500_tickers():
    table = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
    tickers = table["Symbol"].str.replace(".", "-", regex=False).tolist()
    log.info(f"Fetched {len(tickers)} S&P 500 tickers")
    return tickers


def filter_no_dividends_yf(tickers):
    clean = []
    for t in tickers:
        try:
            info = yf.Ticker(t).info
            div_yield = info.get("dividendYield") or 0
            if div_yield == 0:
                clean.append(t)
        except Exception:
            pass
    log.info(f"{len(clean)} tickers after removing dividend payers (yfinance)")
    return clean


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
    for ticker in universe:
        iv = get_iv_signal_yf(ticker)
        mom = get_momentum_signal(ticker)
        if iv == 1 and mom == 1:
            longs.append(ticker)
        elif iv == -1 and mom == -1:
            shorts.append(ticker)
    log.info(f"Signals: {len(longs)} long, {len(shorts)} short")
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


async def close_all_positions(session, account):
    open_pos = get_open_positions()
    if not open_pos:
        log.info("No open positions to close")
        return

    broker_positions = await account.get_positions(
        session, instrument_type=InstrumentType.EQUITY_OPTION
    )
    broker_syms = {p.symbol: p for p in broker_positions if p.quantity != 0}

    for pos in open_pos:
        legs = []
        for sym, action in [
            (pos["long_symbol"], OrderAction.SELL_TO_CLOSE),
            (pos["short_symbol"], OrderAction.BUY_TO_CLOSE),
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
                resp = await account.place_order(session, order, dry_run=False)
                log.info(f"Closed {pos['ticker']} {pos['direction']}: order {resp}")
            except Exception as e:
                log.error(f"Failed to close {pos['ticker']}: {e}")

        mark_position_closed(pos["id"])


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

        # get current price from market data
        metrics = await get_market_metrics(session, [ticker])
        if not metrics:
            return
        # use the mark or last price from yfinance as fallback
        price = float(yf.Ticker(ticker).info.get("regularMarketPrice", 0))
        if price <= 0:
            return

        atm_strike, otm_strike = pick_spread_strikes(strikes, price, direction)
        if atm_strike is None:
            log.warning(f"Could not find spread strikes for {ticker}")
            return

        # find the OCC symbols from the strikes list
        strike_map = {s.strike_price: s for s in strikes}
        atm_s = strike_map.get(atm_strike)
        otm_s = strike_map.get(otm_strike)
        if not atm_s or not otm_s:
            return

        if direction == "long":
            # bull call spread: buy ATM call, sell OTM call
            buy_sym = atm_s.call
            sell_sym = otm_s.call
        else:
            # bear put spread: buy ATM put, sell OTM put
            buy_sym = atm_s.put
            sell_sym = otm_s.put

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

        order = NewOrder(
            time_in_force=OrderTimeInForce.DAY,
            order_type=OrderType.MARKET,
            legs=legs,
        )

        resp = await account.place_order(session, order, dry_run=False)
        log.info(f"Opened {direction} spread on {ticker}: {buy_sym}/{sell_sym} -> {resp}")
        record_position(ticker, direction, buy_sym, sell_sym, CONTRACTS_PER_TRADE)

    except Exception as e:
        log.error(f"Failed to open spread for {ticker}: {e}")


# --- Rebalance Logic ---

def is_rebalance_window():
    today = date.today()
    if today.weekday() >= 5:  # skip weekends
        return False
    first = today.replace(day=1)
    bdays = pd.bdate_range(first, today)
    bday_of_month = len(bdays)
    return REBALANCE_DAY_RANGE[0] <= bday_of_month <= REBALANCE_DAY_RANGE[1]


async def rebalance():
    if was_rebalanced_this_month():
        log.info("Already rebalanced this month, skipping")
        return
    if not is_rebalance_window():
        log.info("Not in rebalance window")
        return

    log.info("=== REBALANCE START ===")

    # 1. get universe
    tickers = get_sp500_tickers()

    # 2. connect to tastytrade
    session = Session()
    async with session:
        accounts = await Account.get(session)
        if not accounts:
            log.error("No tastytrade accounts found")
            return
        account = accounts[0]
        log.info(f"Using account {account.account_number}")

        # 3. filter dividend payers via tastytrade metrics
        universe = await filter_no_dividends_tt(session, tickers)

        # 4. generate signals
        longs, shorts = generate_signals(universe)
        log.info(f"Final targets: LONG {longs}, SHORT {shorts}")

        # 5. close existing positions
        await close_all_positions(session, account)

        # 6. open new positions
        for ticker in longs:
            await open_vertical_spread(session, account, ticker, "long")
        for ticker in shorts:
            await open_vertical_spread(session, account, ticker, "short")

        record_rebalance(longs, shorts)
        log.info("=== REBALANCE COMPLETE ===")


# --- Main Loop ---

async def main_loop():
    init_db()
    log.info("Strategy process started")

    while True:
        try:
            await rebalance()
        except Exception as e:
            log.error(f"Rebalance error: {e}", exc_info=True)

        log.info(f"Sleeping {LOOP_INTERVAL_HOURS}h until next check...")
        await asyncio.sleep(LOOP_INTERVAL_HOURS * 3600)


if __name__ == "__main__":
    asyncio.run(main_loop())
