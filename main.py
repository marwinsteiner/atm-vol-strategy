import asyncio
import logging
import os
import sqlite3
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import yfinance as yf
from dotenv import load_dotenv
from tastytrade import Account, DXLinkStreamer, Session
from tastytrade.dxfeed import Greeks
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


def get_db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA foreign_keys=ON")
    return con


def migrate_db(con):
    """Migrate old rebalances schema to new schema with status tracking."""
    cols = con.execute("PRAGMA table_info(rebalances)").fetchall()
    if not cols:
        return  # table doesn't exist yet, will be created fresh
    col_names = [c["name"] for c in cols]
    if "status" in col_names:
        return  # already migrated

    log.info("Migrating rebalances table to new schema...")
    con.execute("ALTER TABLE rebalances RENAME TO rebalances_old")
    con.execute("""
        CREATE TABLE rebalances (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rebalance_date TEXT NOT NULL,
            month TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL DEFAULT 'PENDING',
            tickers_long TEXT,
            tickers_short TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            completed_at TEXT
        )
    """)
    con.execute("""
        INSERT INTO rebalances (rebalance_date, month, status, tickers_long, tickers_short,
                                created_at, updated_at, completed_at)
        SELECT rebalance_date,
               substr(rebalance_date, 1, 7),
               'COMPLETED',
               tickers_long, tickers_short,
               COALESCE(completed_at, rebalance_date || 'T00:00:00+00:00'),
               COALESCE(completed_at, rebalance_date || 'T00:00:00+00:00'),
               completed_at
        FROM rebalances_old
    """)
    con.execute("DROP TABLE rebalances_old")
    con.commit()
    log.info("Migration complete")


def init_db():
    con = get_db()
    migrate_db(con)
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
            rebalance_date TEXT NOT NULL,
            month TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL DEFAULT 'PENDING',
            tickers_long TEXT,
            tickers_short TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            completed_at TEXT
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS rebalance_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rebalance_id INTEGER NOT NULL REFERENCES rebalances(id),
            ticker TEXT NOT NULL,
            operation TEXT NOT NULL,
            direction TEXT,
            status TEXT NOT NULL DEFAULT 'PENDING',
            position_id INTEGER,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
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


def get_open_positions(con=None):
    db = con or get_db()
    rows = db.execute(
        "SELECT * FROM positions WHERE closed_at IS NULL"
    ).fetchall()
    if not con:
        db.close()
    return [dict(r) for r in rows]


def record_position(ticker, direction, long_sym, short_sym, qty, entry_debit=None, con=None):
    comms = qty * 2 * COMMISSION_PER_CONTRACT  # 2 legs
    db = con or get_db()
    cur = db.execute(
        "INSERT INTO positions (ticker, direction, long_symbol, short_symbol, "
        "quantity, entry_debit, commissions, opened_at) VALUES (?,?,?,?,?,?,?,?)",
        (ticker, direction, long_sym, short_sym, qty, entry_debit, comms,
         datetime.now(timezone.utc).isoformat()),
    )
    if not con:
        db.commit()
        db.close()
    return cur.lastrowid


def mark_position_closed(position_id, exit_credit=None, con=None):
    close_comms = COMMISSION_PER_CONTRACT * 2  # 2 legs to close
    db = con or get_db()
    db.execute(
        "UPDATE positions SET closed_at = ?, exit_credit = ?, commissions = commissions + ? WHERE id = ?",
        (datetime.now(timezone.utc).isoformat(), exit_credit, close_comms, position_id),
    )
    if not con:
        db.commit()
        db.close()


def was_rebalanced_this_month():
    con = get_db()
    month_str = date.today().strftime("%Y-%m")
    row = con.execute(
        "SELECT 1 FROM rebalances WHERE month = ? AND status = 'COMPLETED' LIMIT 1",
        (month_str,),
    ).fetchone()
    con.close()
    return row is not None


def create_rebalance_record(longs, shorts):
    con = get_db()
    now = datetime.now(timezone.utc).isoformat()
    month_str = date.today().strftime("%Y-%m")
    cur = con.execute(
        "INSERT INTO rebalances (rebalance_date, month, status, tickers_long, "
        "tickers_short, created_at, updated_at) VALUES (?,?,?,?,?,?,?)",
        (date.today().isoformat(), month_str, "PENDING",
         ",".join(longs), ",".join(shorts), now, now),
    )
    con.commit()
    rb_id = cur.lastrowid
    con.close()
    return rb_id


def update_rebalance_status(rb_id, status, con=None):
    db = con or get_db()
    now = datetime.now(timezone.utc).isoformat()
    if status == "COMPLETED":
        db.execute(
            "UPDATE rebalances SET status = ?, updated_at = ?, completed_at = ? WHERE id = ?",
            (status, now, now, rb_id),
        )
    else:
        db.execute(
            "UPDATE rebalances SET status = ?, updated_at = ? WHERE id = ?",
            (status, now, rb_id),
        )
    if not con:
        db.commit()
        db.close()


def plan_close_items(rb_id, open_positions, con=None):
    db = con or get_db()
    now = datetime.now(timezone.utc).isoformat()
    for pos in open_positions:
        db.execute(
            "INSERT INTO rebalance_items (rebalance_id, ticker, operation, direction, "
            "status, position_id, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?)",
            (rb_id, pos["ticker"], "CLOSE", pos["direction"], "PENDING",
             pos["id"], now, now),
        )
    if not con:
        db.commit()
        db.close()


def plan_open_items(rb_id, longs, shorts, con=None):
    db = con or get_db()
    now = datetime.now(timezone.utc).isoformat()
    for ticker in longs:
        db.execute(
            "INSERT INTO rebalance_items (rebalance_id, ticker, operation, direction, "
            "status, created_at, updated_at) VALUES (?,?,?,?,?,?,?)",
            (rb_id, ticker, "OPEN", "long", "PENDING", now, now),
        )
    for ticker in shorts:
        db.execute(
            "INSERT INTO rebalance_items (rebalance_id, ticker, operation, direction, "
            "status, created_at, updated_at) VALUES (?,?,?,?,?,?,?)",
            (rb_id, ticker, "OPEN", "short", "PENDING", now, now),
        )
    if not con:
        db.commit()
        db.close()


def get_pending_rebalance():
    con = get_db()
    month_str = date.today().strftime("%Y-%m")
    row = con.execute(
        "SELECT * FROM rebalances WHERE month = ? AND status != 'COMPLETED' LIMIT 1",
        (month_str,),
    ).fetchone()
    con.close()
    return dict(row) if row else None


def get_rebalance_items(rb_id, operation=None, status=None):
    con = get_db()
    query = "SELECT * FROM rebalance_items WHERE rebalance_id = ?"
    params = [rb_id]
    if operation:
        query += " AND operation = ?"
        params.append(operation)
    if status:
        query += " AND status = ?"
        params.append(status)
    rows = con.execute(query, params).fetchall()
    con.close()
    return [dict(r) for r in rows]


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


async def gather_iv_symbols(session, universe):
    """Get ATM streamer symbols for each ticker at each DTE target.

    Returns (symbol_map, all_symbols) where symbol_map is
    {ticker: {dte: (call_streamer_sym, put_streamer_sym)}}
    and all_symbols is the flat set of all streamer symbols to subscribe to.
    """
    symbol_map = {}
    all_symbols = set()

    async def process_ticker(ticker):
        try:
            chains = await NestedOptionChain.get(session, ticker)
            if not chains:
                return
            chain = chains[0]
            price = await get_stock_price(session, ticker)
            if price <= 0:
                return
            ticker_map = {}
            for target_dte in DTE_TARGETS:
                try:
                    exp = find_closest_expiry(chain.expirations, target_dte)
                    actual_dte = (exp.expiration_date - date.today()).days
                    if abs(actual_dte - target_dte) > 14:
                        continue
                    strikes = exp.strikes
                    if not strikes:
                        continue
                    atm = min(strikes, key=lambda s: abs(float(s.strike_price) - price))
                    call_sym = atm.call_streamer_symbol
                    put_sym = atm.put_streamer_symbol
                    ticker_map[target_dte] = (call_sym, put_sym)
                    all_symbols.add(call_sym)
                    all_symbols.add(put_sym)
                except Exception:
                    continue
            if ticker_map:
                symbol_map[ticker] = ticker_map
        except Exception as e:
            log.debug(f"IV chain error for {ticker}: {e}")

    for i in range(0, len(universe), SCAN_BATCH_SIZE):
        batch = universe[i : i + SCAN_BATCH_SIZE]
        await asyncio.gather(*[process_ticker(t) for t in batch])
        if (i + SCAN_BATCH_SIZE) % 100 < SCAN_BATCH_SIZE:
            log.info(f"Gathered IV symbols: {min(i + SCAN_BATCH_SIZE, len(universe))}/{len(universe)}")

    log.info(f"Gathered {len(all_symbols)} streamer symbols for {len(symbol_map)} tickers")
    return symbol_map, all_symbols


async def get_iv_signals_tt(session, universe):
    """Get IV signals using TT Greeks streaming.

    Opens a single DXLinkStreamer, subscribes to Greeks for all ATM options,
    then compares call IV vs put IV at each DTE to vote on direction.
    """
    symbol_map, all_symbols = await gather_iv_symbols(session, universe)
    if not all_symbols:
        return {}

    iv_data = {}  # {streamer_symbol: volatility}

    async with DXLinkStreamer(session) as streamer:
        await streamer.subscribe(Greeks, list(all_symbols))

        expected = len(all_symbols)

        async def collect_greeks():
            async for greeks in streamer.listen(Greeks):
                if greeks.volatility is not None:
                    iv_data[greeks.event_symbol] = float(greeks.volatility)
                if len(iv_data) >= expected:
                    return

        try:
            await asyncio.wait_for(collect_greeks(), timeout=30.0)
        except asyncio.TimeoutError:
            log.info(f"Greeks timeout: got {len(iv_data)}/{expected} symbols")

    signals = {}
    for ticker, dte_map in symbol_map.items():
        votes = []
        for _dte, (call_sym, put_sym) in dte_map.items():
            call_iv = iv_data.get(call_sym)
            put_iv = iv_data.get(put_sym)
            if call_iv is None or put_iv is None:
                continue
            if call_iv > put_iv:
                votes.append(1)
            elif call_iv < put_iv:
                votes.append(-1)
            else:
                votes.append(0)
        if len(votes) < 2:
            continue
        if all(v == 1 for v in votes):
            signals[ticker] = 1
        elif all(v == -1 for v in votes):
            signals[ticker] = -1

    long_count = sum(1 for v in signals.values() if v == 1)
    short_count = sum(1 for v in signals.values() if v == -1)
    log.info(f"IV signals: {long_count} long, {short_count} short")
    return signals


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


async def generate_signals(session, universe):
    longs, shorts = [], []
    iv_signals = await get_iv_signals_tt(session, universe)
    for ticker, iv in iv_signals.items():
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
    return longs[: MAX_POSITIONS // 2], shorts[: MAX_POSITIONS // 2]


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


async def close_single_position(session, account, pos):
    """Close a single position via broker. Returns exit_credit float or None."""
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
    return exit_val


async def execute_close_phase(session, account, rb_id):
    """Execute all pending close items for a rebalance."""
    items = get_rebalance_items(rb_id, operation="CLOSE", status="PENDING")
    if not items:
        log.info("No pending close items")
        return

    for item in items:
        con = get_db()
        pos_row = con.execute(
            "SELECT * FROM positions WHERE id = ?", (item["position_id"],)
        ).fetchone()
        con.close()
        if not pos_row:
            log.warning(f"Position {item['position_id']} not found for close item {item['id']}")
            continue
        pos = dict(pos_row)

        exit_val = await close_single_position(session, account, pos)

        # atomically update position + rebalance item
        con = get_db()
        try:
            mark_position_closed(pos["id"], exit_credit=exit_val, con=con)
            now = datetime.now(timezone.utc).isoformat()
            con.execute(
                "UPDATE rebalance_items SET status = 'COMPLETED', updated_at = ? WHERE id = ?",
                (now, item["id"]),
            )
            con.commit()
        finally:
            con.close()


async def open_vertical_spread(session, account, ticker, direction):
    """Build and place a vertical spread. Returns position data dict or None."""
    try:
        chains = await NestedOptionChain.get(session, ticker)
        if not chains:
            log.warning(f"No option chain for {ticker}")
            return None

        chain = chains[0]
        exp = find_closest_expiry(chain.expirations, EXECUTION_DTE)
        strikes = exp.strikes
        if not strikes:
            return None

        price = await get_stock_price(session, ticker)
        if price <= 0:
            return None

        atm_strike, otm_strike = pick_spread_strikes(strikes, price, direction)
        if atm_strike is None:
            log.warning(f"Could not find spread strikes for {ticker}")
            return None

        strike_map = {s.strike_price: s for s in strikes}
        atm_s = strike_map.get(atm_strike)
        otm_s = strike_map.get(otm_strike)
        if not atm_s or not otm_s:
            return None

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
                return None
            log.info(
                f"[PAPER] Filled {direction} spread on {ticker}: "
                f"{buy_sym}/{sell_sym} @ {exp.expiration_date} "
                f"strikes {atm_strike}/{otm_strike} debit={entry:.4f}"
            )
            return {
                "ticker": ticker, "direction": direction,
                "buy_sym": buy_sym, "sell_sym": sell_sym,
                "qty": CONTRACTS_PER_TRADE, "entry_debit": entry,
            }

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
        if DRY_RUN:
            return None
        return {
            "ticker": ticker, "direction": direction,
            "buy_sym": buy_sym, "sell_sym": sell_sym,
            "qty": CONTRACTS_PER_TRADE, "entry_debit": entry,
        }

    except Exception as e:
        log.error(f"Failed to open spread for {ticker}: {e}")
        return None


async def execute_open_phase(session, account, rb_id):
    """Execute all pending open items for a rebalance."""
    items = get_rebalance_items(rb_id, operation="OPEN", status="PENDING")
    if not items:
        log.info("No pending open items")
        return

    for item in items:
        result = await open_vertical_spread(session, account, item["ticker"], item["direction"])

        # atomically record position + update rebalance item
        con = get_db()
        try:
            now = datetime.now(timezone.utc).isoformat()
            if result:
                pos_id = record_position(
                    result["ticker"], result["direction"],
                    result["buy_sym"], result["sell_sym"],
                    result["qty"], entry_debit=result["entry_debit"],
                    con=con,
                )
                con.execute(
                    "UPDATE rebalance_items SET status = 'COMPLETED', position_id = ?, "
                    "updated_at = ? WHERE id = ?",
                    (pos_id, now, item["id"]),
                )
            else:
                con.execute(
                    "UPDATE rebalance_items SET status = 'FAILED', updated_at = ? WHERE id = ?",
                    (now, item["id"]),
                )
            con.commit()
        finally:
            con.close()


# --- P&L Tracking ---


def compute_monthly_pnl():
    """Compute and store P&L for positions closed this month."""
    con = get_db()
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
    for r in rows:
        entry_debit, exit_credit, comms, qty = r["entry_debit"], r["exit_credit"], r["commissions"], r["quantity"]
        if entry_debit is not None and exit_credit is not None:
            # P&L = (exit - entry) * 100 * qty for options
            gross_pnl += (exit_credit - entry_debit) * 100 * qty
        total_comms += comms or 0

    net_pnl = gross_pnl - total_comms

    # get cumulative
    prev = con.execute(
        "SELECT cumulative_pnl FROM monthly_pnl ORDER BY month DESC LIMIT 1"
    ).fetchone()
    cum = (prev["cumulative_pnl"] if prev else 0.0) + net_pnl

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
    con = get_db()
    rows = con.execute("SELECT * FROM monthly_pnl ORDER BY month").fetchall()
    con.close()
    if not rows:
        log.info("No paper results yet")
        return
    log.info("=" * 70)
    log.info(f"{'Month':<10} {'Gross':>10} {'Comms':>10} {'Net':>10} {'Trades':>7} {'Cumulative':>12}")
    log.info("-" * 70)
    for row in rows:
        log.info(
            f"{row['month']:<10} {row['gross_pnl']:>+10.2f} {row['commissions']:>10.2f} "
            f"{row['net_pnl']:>+10.2f} {row['num_trades']:>7} {row['cumulative_pnl']:>+12.2f}"
        )
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


async def _get_account(session):
    if TT_ACCOUNT:
        return await Account.get(session, TT_ACCOUNT)
    accounts = await Account.get(session)
    if not accounts:
        raise RuntimeError("No tastytrade accounts found")
    return accounts[0]


async def resume_rebalance(rb, session, account):
    """Resume an interrupted rebalance from its current status."""
    rb_id = rb["id"]
    status = rb["status"]
    log.info(f"Resuming rebalance {rb_id} from status={status}")

    if status == "PENDING":
        close_items = get_rebalance_items(rb_id, operation="CLOSE")
        if close_items:
            update_rebalance_status(rb_id, "CLOSING")
            await execute_close_phase(session, account, rb_id)
            if PAPER_MODE:
                compute_monthly_pnl()
        update_rebalance_status(rb_id, "OPENING")
        await execute_open_phase(session, account, rb_id)
    elif status == "CLOSING":
        await execute_close_phase(session, account, rb_id)
        if PAPER_MODE:
            compute_monthly_pnl()
        update_rebalance_status(rb_id, "OPENING")
        await execute_open_phase(session, account, rb_id)
    elif status == "OPENING":
        await execute_open_phase(session, account, rb_id)

    update_rebalance_status(rb_id, "COMPLETED")

    if PAPER_MODE:
        print_paper_summary()
    log.info(f"=== Resumed rebalance {rb_id} COMPLETE ===")


async def rebalance():
    # 1. check for interrupted rebalance
    pending = get_pending_rebalance()
    if pending:
        log.info(f"Found interrupted rebalance {pending['id']} (status={pending['status']})")
        session = Session()
        async with session:
            account = await _get_account(session)
            log.info(f"Using account {account.account_number} (paper={PAPER_MODE}, dry_run={DRY_RUN})")
            await resume_rebalance(pending, session, account)
        return

    # 2. check if already done or not in window
    if was_rebalanced_this_month():
        log.info("Already rebalanced this month, skipping")
        return
    if not is_rebalance_window():
        log.info("Not in rebalance window")
        return

    tag = "[PAPER] " if PAPER_MODE else ""
    log.info(f"=== {tag}REBALANCE START ===")

    # 3. get universe
    tickers = get_sp500_tickers()

    # 4. connect to tastytrade
    session = Session()
    async with session:
        account = await _get_account(session)
        log.info(f"Using account {account.account_number} (paper={PAPER_MODE}, dry_run={DRY_RUN})")

        # 5. filter dividend payers
        universe = await filter_no_dividends_tt(session, tickers)

        # 6. generate signals via TT Greeks
        longs, shorts = await generate_signals(session, universe)
        log.info(f"Final targets: LONG {longs}, SHORT {shorts}")

        # 7. plan the rebalance
        open_positions = get_open_positions()
        rb_id = create_rebalance_record(longs, shorts)
        plan_close_items(rb_id, open_positions)
        plan_open_items(rb_id, longs, shorts)

        # 8. execute close phase
        update_rebalance_status(rb_id, "CLOSING")
        await execute_close_phase(session, account, rb_id)

        # 9. compute P&L if paper mode
        if PAPER_MODE and open_positions:
            compute_monthly_pnl()

        # 10. execute open phase
        update_rebalance_status(rb_id, "OPENING")
        await execute_open_phase(session, account, rb_id)

        # 11. mark completed
        update_rebalance_status(rb_id, "COMPLETED")

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
