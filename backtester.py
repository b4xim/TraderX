"""
TraderX — Historical F&O Strategy Backtester
═══════════════════════════════════════════════════════════════
Backtests the F&O opening-momentum PE strategy on historical data:
  1. Identifies top-5 losing F&O stocks at 9:16 AM for a given date
  2. Gets stock price at 9:16 AM (entry price)
  3. Simulates the ATM PE option strategy from 9:16 to 9:32 AM
  4. Returns detailed trade logs, sparkline price trajectories, and P&L

PAPER TRADING / RESEARCH ONLY — Read-only market data consumer.
═══════════════════════════════════════════════════════════════
"""

import asyncio
import hashlib
import json
import logging
import math
import random
from dataclasses import dataclass, field, asdict
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional, Any
from zoneinfo import ZoneInfo

import httpx

from config_loader import ScannerConfig, StrategyConfig

logger = logging.getLogger("traderx.backtester")

IST = ZoneInfo("Asia/Kolkata")
UPSTOX_BASE_URL = "https://api.upstox.com/v2"


# ═══════════════════════════════════════════════════════════════
# Helper Data Structures
# ═══════════════════════════════════════════════════════════════

@dataclass
class Candle:
    timestamp: str       # ISO string e.g. "2024-03-01T09:16:00+05:30"
    time_str: str        # "09:16"
    open: float
    high: float
    low: float
    close: float
    volume: int = 0


@dataclass
class BacktestTrade:
    rank: int
    symbol: str
    instrument_key: str
    sector: str
    avg_oi_tier: str
    prev_close: float
    price_at_916: float
    stock_gap_pct: float            # vs prev close at 9:16
    entry_price: float              # 9:16 close price
    entry_time: str                 # "09:16"
    exit_price: float
    exit_time: str
    exit_reason: str                # "TARGET", "STOPLOSS", "TIME_EXIT"
    stock_move_pct: float           # (exit_price - entry_price) / entry_price * 100
    pe_pnl_pct: float               # simulated PE option return %
    is_win: bool
    option_type: str = "PE"
    option_strike_approx: int = 0
    candles: list[dict] = field(default_factory=list)


@dataclass
class BacktestSummary:
    total_trades: int
    wins: int
    losses: int
    targets_hit: int
    sl_hit: int
    time_exits: int
    win_rate: float
    total_pe_pnl_pct: float
    avg_pe_pnl_pct: float
    best_trade: Optional[dict] = None
    worst_trade: Optional[dict] = None


@dataclass
class BacktestResult:
    date: str
    universe: str
    pe_leverage_factor: float
    target_pct: float
    stoploss_pct: float
    hard_exit_time: str
    summary: BacktestSummary
    trades: list[BacktestTrade]
    warnings: list[str] = field(default_factory=list)
    is_demo: bool = False

    def to_dict(self) -> dict:
        return asdict(self)


# ═══════════════════════════════════════════════════════════════
# Loading Universe
# ═══════════════════════════════════════════════════════════════

def load_universe_stocks(cfg: ScannerConfig, universe: str = "HIGH") -> list[dict]:
    """Load F&O stocks filtered by specified universe tier."""
    p = Path(cfg.fo_stocks_file)
    if not p.is_absolute():
        p = Path(__file__).parent / p
    if not p.exists():
        logger.error("fo_stocks.json not found at %s", p)
        return []

    try:
        stocks = json.loads(p.read_text())
    except Exception as e:
        logger.error("Failed to parse %s: %s", p, e)
        return []

    u = (universe or "HIGH").strip().upper()
    if u == "HIGH":
        return [s for s in stocks if s.get("avg_oi_tier") == "HIGH"]
    elif u == "HIGH+MED":
        return [s for s in stocks if s.get("avg_oi_tier") in ("HIGH", "MED")]
    elif u == "MED":
        return [s for s in stocks if s.get("avg_oi_tier") == "MED"]
    return stocks


def approx_atm_strike(price: float) -> int:
    """Compute approximate ATM strike for an Indian equity stock."""
    if price < 200:
        step = 5
    elif price < 500:
        step = 10
    elif price < 1500:
        step = 20
    elif price < 3000:
        step = 50
    else:
        step = 100
    return int(round(price / step) * step)


# ═══════════════════════════════════════════════════════════════
# Historical Upstox Data Fetcher
# ═══════════════════════════════════════════════════════════════

async def fetch_stock_candles(
    client: httpx.AsyncClient,
    access_token: str,
    instrument_key: str,
    date_str: str,
    sem: asyncio.Semaphore,
) -> tuple[Optional[float], list[Candle]]:
    """
    Fetch D-1 daily close and date_str 1-minute intraday candles from Upstox.
    Returns: (prev_close, list_of_1min_candles)
    """
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token}",
    }

    # 1. Fetch Daily Candles around date_str to determine previous day close
    prev_close: Optional[float] = None
    try:
        # Request daily candles from 15 days prior to date_str
        target_dt = datetime.strptime(date_str, "%Y-%m-%d").date()
        from_dt = target_dt - timedelta(days=15)
        daily_url = f"{UPSTOX_BASE_URL}/historical-candle/{instrument_key}/day/{date_str}/{from_dt.strftime('%Y-%m-%d')}"

        async with sem:
            resp = await client.get(daily_url, headers=headers)
            if resp.status_code == 200:
                data = resp.json().get("data", {}).get("candles", [])
                # candles: [[timestamp, open, high, low, close, volume, oi], ...]
                # Sort ascending by timestamp
                data_sorted = sorted(data, key=lambda x: x[0])
                # Find candle strictly before target_dt
                for c in reversed(data_sorted):
                    c_date = c[0].split("T")[0]
                    if c_date < date_str:
                        prev_close = float(c[4])
                        break
    except Exception as exc:
        logger.debug("Daily candle fetch failed for %s: %s", instrument_key, exc)

    # 2. Fetch 1-minute candles for date_str
    candles_1m: list[Candle] = []
    try:
        # In Upstox API v2, 1-minute historical candles can be requested with to_date and from_date:
        url_1m = f"{UPSTOX_BASE_URL}/historical-candle/{instrument_key}/1minute/{date_str}/{date_str}"
        async with sem:
            resp = await client.get(url_1m, headers=headers)
            if resp.status_code == 200:
                raw_candles = resp.json().get("data", {}).get("candles", [])
                raw_candles_sorted = sorted(raw_candles, key=lambda x: x[0])
                for c in raw_candles_sorted:
                    ts = c[0]
                    t_str = ts.split("T")[1][:5] if "T" in ts else ""
                    candles_1m.append(
                        Candle(
                            timestamp=ts,
                            time_str=t_str,
                            open=float(c[1]),
                            high=float(c[2]),
                            low=float(c[3]),
                            close=float(c[4]),
                            volume=int(c[5]) if len(c) > 5 else 0,
                        )
                    )
    except Exception as exc:
        logger.debug("1-minute candle fetch failed for %s: %s", instrument_key, exc)

    return prev_close, candles_1m


# ═══════════════════════════════════════════════════════════════
# Strategy Simulation Core
# ═══════════════════════════════════════════════════════════════

def simulate_pe_trade(
    symbol: str,
    instrument_key: str,
    sector: str,
    avg_oi_tier: str,
    prev_close: float,
    candles_morning: list[Candle],
    target_pct: float,
    stoploss_pct: float,
    hard_exit_time: str,
    leverage_factor: float,
    rank: int,
) -> Optional[BacktestTrade]:
    """
    Simulates the PE strategy on a series of 1-minute candles starting at 9:16.
    Entry at 9:16 candle close.
    Walk minute by minute until 9:32, checking target and SL.
    """
    if not candles_morning:
        return None

    # Find 9:16 candle
    # If a candle has time_str == "09:16", or first candle in morning session
    candle_916_idx = None
    for i, c in enumerate(candles_morning):
        if c.time_str in ("09:16", "09:15"):
            candle_916_idx = i
            break

    if candle_916_idx is None:
        candle_916_idx = 0

    c_entry = candles_morning[candle_916_idx]
    entry_price = c_entry.close
    entry_time = "09:16"

    stock_gap_pct = round(((entry_price - prev_close) / prev_close) * 100.0, 2)
    approx_strike = approx_atm_strike(entry_price)

    # Walk from 9:16 onwards
    candles_to_test = candles_morning[candle_916_idx:]
    if not candles_to_test:
        candles_to_test = [c_entry]

    exit_price = entry_price
    exit_time = entry_time
    exit_reason = "TIME_EXIT"
    final_stock_move = 0.0
    final_pe_pnl = 0.0

    sparkline_points = []

    # First point at 09:16
    sparkline_points.append({
        "time": entry_time,
        "price": round(entry_price, 2),
        "stock_move_pct": 0.0,
        "pe_pnl_pct": 0.0,
    })

    # Iterate through minute candles
    trade_finished = False

    for c in candles_to_test[1:]:
        t_str = c.time_str

        # Stock move vs entry
        # For PE:
        # Stock fall (low < entry) = PE gain (max potential)
        # Stock rise (high > entry) = PE loss / drawdown
        cur_close_move = ((c.close - entry_price) / entry_price) * 100.0
        cur_low_move = ((c.low - entry_price) / entry_price) * 100.0
        cur_high_move = ((c.high - entry_price) / entry_price) * 100.0

        pe_gain_potential = -cur_low_move * leverage_factor
        pe_loss_potential = -cur_high_move * leverage_factor
        pe_close_pnl = -cur_close_move * leverage_factor

        point_pnl = round(pe_close_pnl, 2)
        sparkline_points.append({
            "time": t_str,
            "price": round(c.close, 2),
            "stock_move_pct": round(cur_close_move, 2),
            "pe_pnl_pct": point_pnl,
        })

        if not trade_finished:
            # Check TARGET: stock fell enough
            if pe_gain_potential >= target_pct:
                exit_reason = "TARGET"
                exit_time = t_str
                # stock target price
                target_stock_move = -(target_pct / leverage_factor)
                exit_price = round(entry_price * (1.0 + target_stock_move / 100.0), 2)
                final_stock_move = round(target_stock_move, 2)
                final_pe_pnl = round(target_pct, 2)
                trade_finished = True

            # Check STOP LOSS: stock rose too much
            elif pe_loss_potential <= -stoploss_pct:
                exit_reason = "STOPLOSS"
                exit_time = t_str
                sl_stock_move = (stoploss_pct / leverage_factor)
                exit_price = round(entry_price * (1.0 + sl_stock_move / 100.0), 2)
                final_stock_move = round(sl_stock_move, 2)
                final_pe_pnl = round(-stoploss_pct, 2)
                trade_finished = True

            # Check TIME EXIT
            elif t_str >= hard_exit_time:
                exit_reason = "TIME_EXIT"
                exit_time = t_str
                exit_price = round(c.close, 2)
                final_stock_move = round(cur_close_move, 2)
                final_pe_pnl = round(pe_close_pnl, 2)
                trade_finished = True

        # Stop walking if time goes past hard exit time + 3 min
        if t_str > hard_exit_time:
            break

    # If never triggered exit condition
    if not trade_finished:
        last_c = candles_to_test[-1]
        exit_reason = "TIME_EXIT"
        exit_time = last_c.time_str
        exit_price = round(last_c.close, 2)
        final_stock_move = round(((exit_price - entry_price) / entry_price) * 100.0, 2)
        final_pe_pnl = round(-final_stock_move * leverage_factor, 2)

    is_win = final_pe_pnl > 0

    return BacktestTrade(
        rank=rank,
        symbol=symbol,
        instrument_key=instrument_key,
        sector=sector,
        avg_oi_tier=avg_oi_tier,
        prev_close=round(prev_close, 2),
        price_at_916=round(entry_price, 2),
        stock_gap_pct=stock_gap_pct,
        entry_price=round(entry_price, 2),
        entry_time=entry_time,
        exit_price=exit_price,
        exit_time=exit_time,
        exit_reason=exit_reason,
        stock_move_pct=final_stock_move,
        pe_pnl_pct=final_pe_pnl,
        is_win=is_win,
        option_type="PE",
        option_strike_approx=approx_strike,
        candles=sparkline_points,
    )


# ═══════════════════════════════════════════════════════════════
# Demo Mode Synthetic Backtest Generator
# ═══════════════════════════════════════════════════════════════

def _generate_demo_backtest(
    date_str: str,
    universe_stocks: list[dict],
    target_pct: float,
    stoploss_pct: float,
    hard_exit_time: str,
    leverage_factor: float,
    top_n: int = 5,
    universe: str = "HIGH",
) -> BacktestResult:
    """
    Generates deterministic, realistic backtest results for the specified date
    in demo mode using a seed derived from date_str.
    """
    # Seed generator with hash of date_str for determinism
    seed_val = int(hashlib.sha256(date_str.encode("utf-8")).hexdigest()[:8], 16)
    rng = random.Random(seed_val)

    # Pick top_n prominent stocks from universe
    chosen_stocks = rng.sample(universe_stocks, min(len(universe_stocks), max(top_n + 3, 8)))

    # Assign base stock prices and gaps
    candidate_losers = []
    base_prices = {
        "TECHM": 1240.0, "INFY": 1480.0, "HDFCBANK": 1620.0, "RELIANCE": 2850.0,
        "TATAMOTORS": 940.0, "TATASTEEL": 155.0, "JSWSTEEL": 890.0, "ICICIBANK": 1080.0,
        "WIPRO": 460.0, "SBIN": 760.0, "BAJFINANCE": 6900.0, "SUNPHARMA": 1580.0,
    }

    for s in chosen_stocks:
        sym = s["symbol"]
        prev_close = base_prices.get(sym, round(rng.uniform(400, 2500), 1))
        # Gap down % between -1.5% and -4.8%
        gap_pct = -round(rng.uniform(1.4, 4.2), 2)
        price_916 = round(prev_close * (1.0 + gap_pct / 100.0), 2)
        candidate_losers.append({
            "stock": s,
            "prev_close": prev_close,
            "gap_pct": gap_pct,
            "price_916": price_916,
        })

    # Sort by gap_pct ascending (worst losers first)
    candidate_losers.sort(key=lambda x: x["gap_pct"])
    selected = candidate_losers[:top_n]

    trades: list[BacktestTrade] = []

    # Assign varied realistic strategy outcomes across top_n
    # Outcomes: Target, Target, SL, Time Exit (positive), Time Exit (negative)
    outcomes_cycle = ["TARGET", "TARGET", "STOPLOSS", "TIME_EXIT_WIN", "TIME_EXIT_LOSS"]

    for idx, item in enumerate(selected):
        s = item["stock"]
        prev_close = item["prev_close"]
        p916 = item["price_916"]
        outcome_type = outcomes_cycle[idx % len(outcomes_cycle)]

        # Generate realistic 1-min candles from 09:15 to 09:32
        candles: list[Candle] = []
        cur_price = p916
        c_915_open = round(p916 * (1.0 + rng.uniform(-0.1, 0.1) / 100.0), 2)
        candles.append(Candle(
            timestamp=f"{date_str}T09:16:00+05:30",
            time_str="09:16",
            open=c_915_open,
            high=max(c_915_open, p916) + rng.uniform(0.5, 2.0),
            low=min(c_915_open, p916) - rng.uniform(0.5, 2.0),
            close=p916,
        ))

        # Time series generation
        times = [f"09:{m:02d}" for m in range(17, 33)]
        target_fall_pct = (target_pct / leverage_factor)  # e.g. 10% fall for 20% gain
        sl_rise_pct = (stoploss_pct / leverage_factor)     # e.g. 10% rise for 20% loss

        # Pre-plan trajectory depending on outcome_type
        for t_str in times:
            m = int(t_str.split(":")[1])
            step_pct = 0.0

            if outcome_type == "TARGET":
                # Stock keeps falling until minute 09:22 - 09:26
                target_min = 20 + (idx % 5)
                if m <= target_min:
                    step_pct = - (target_fall_pct / (target_min - 15)) * (m - 15) * rng.uniform(0.9, 1.1)
                else:
                    step_pct = -target_fall_pct - rng.uniform(0.1, 0.5)

            elif outcome_type == "STOPLOSS":
                # Stock quickly rebounds and hits stoploss around 09:20 - 09:24
                sl_min = 19 + (idx % 4)
                if m <= sl_min:
                    step_pct = (sl_rise_pct / (sl_min - 15)) * (m - 15) * rng.uniform(0.9, 1.1)
                else:
                    step_pct = sl_rise_pct + rng.uniform(0.1, 0.5)

            elif outcome_type == "TIME_EXIT_WIN":
                # Drops mildly (e.g. 4-6% option gain)
                step_pct = - (4.0 / leverage_factor) * ((m - 15) / 17.0) * rng.uniform(0.8, 1.2)

            else: # TIME_EXIT_LOSS
                # Hovers near flat or rises mildly
                step_pct = (3.0 / leverage_factor) * ((m - 15) / 17.0) * rng.uniform(0.8, 1.2)

            c_close = round(p916 * (1.0 + step_pct / 100.0), 2)
            c_high = round(max(cur_price, c_close) + rng.uniform(0.2, 1.5), 2)
            c_low = round(min(cur_price, c_close) - rng.uniform(0.2, 1.5), 2)

            candles.append(Candle(
                timestamp=f"{date_str}T{t_str}:00+05:30",
                time_str=t_str,
                open=cur_price,
                high=c_high,
                low=c_low,
                close=c_close,
            ))
            cur_price = c_close

        trade = simulate_pe_trade(
            symbol=s["symbol"],
            instrument_key=s.get("instrument_key", ""),
            sector=s.get("sector", "General"),
            avg_oi_tier=s.get("avg_oi_tier", "HIGH"),
            prev_close=prev_close,
            candles_morning=candles,
            target_pct=target_pct,
            stoploss_pct=stoploss_pct,
            hard_exit_time=hard_exit_time,
            leverage_factor=leverage_factor,
            rank=idx + 1,
        )
        if trade:
            trades.append(trade)

    # Calculate summary
    total = len(trades)
    wins = sum(1 for t in trades if t.is_win)
    losses = total - wins
    targets = sum(1 for t in trades if t.exit_reason == "TARGET")
    sls = sum(1 for t in trades if t.exit_reason == "STOPLOSS")
    time_exits = sum(1 for t in trades if t.exit_reason == "TIME_EXIT")
    win_rate = round((wins / total * 100.0), 1) if total > 0 else 0.0
    total_pe_pnl = round(sum(t.pe_pnl_pct for t in trades), 2)
    avg_pe_pnl = round(total_pe_pnl / total, 2) if total > 0 else 0.0

    best_trade = max(trades, key=lambda t: t.pe_pnl_pct) if trades else None
    worst_trade = min(trades, key=lambda t: t.pe_pnl_pct) if trades else None

    summary = BacktestSummary(
        total_trades=total,
        wins=wins,
        losses=losses,
        targets_hit=targets,
        sl_hit=sls,
        time_exits=time_exits,
        win_rate=win_rate,
        total_pe_pnl_pct=total_pe_pnl,
        avg_pe_pnl_pct=avg_pe_pnl,
        best_trade={"symbol": best_trade.symbol, "pe_pnl_pct": best_trade.pe_pnl_pct} if best_trade else None,
        worst_trade={"symbol": worst_trade.symbol, "pe_pnl_pct": worst_trade.pe_pnl_pct} if worst_trade else None,
    )

    return BacktestResult(
        date=date_str,
        universe=universe,
        pe_leverage_factor=leverage_factor,
        target_pct=target_pct,
        stoploss_pct=stoploss_pct,
        hard_exit_time=hard_exit_time,
        summary=summary,
        trades=trades,
        warnings=["Demo Mode: Results generated using deterministic synthetic historical candles."],
        is_demo=True,
    )


# ═══════════════════════════════════════════════════════════════
# Main Entry Point
# ═══════════════════════════════════════════════════════════════

async def run_historical_backtest(
    access_token: Optional[str],
    date_str: str,
    cfg: ScannerConfig,
    strategy_cfg: StrategyConfig,
    top_n: int = 5,
    universe: Optional[str] = None,
) -> BacktestResult:
    """
    Executes backtest for a specific historical date.
    If demo_mode is True or access_token is None, runs synthetic simulation.
    Otherwise, queries Upstox API for real historical candles.
    """
    selected_universe = universe or cfg.backtest_universe or "HIGH"
    leverage_factor = cfg.backtest_pe_leverage_factor or 2.0
    target_pct = strategy_cfg.target_pct or 20.0
    stoploss_pct = strategy_cfg.stoploss_pct or 20.0
    hard_exit_time = strategy_cfg.hard_exit_time or "09:32"

    logger.info("Starting backtest for date=%s universe=%s demo_mode=%s", date_str, selected_universe, cfg.demo_mode)

    universe_stocks = load_universe_stocks(cfg, selected_universe)
    if not universe_stocks:
        logger.warning("No stocks loaded for universe %s. Using default fallback.", selected_universe)
        universe_stocks = load_universe_stocks(cfg, "HIGH")

    # DEMO MODE
    if cfg.demo_mode or not access_token:
        logger.info("Running in demo mode (synthetic backtest)")
        return _generate_demo_backtest(
            date_str=date_str,
            universe_stocks=universe_stocks,
            target_pct=target_pct,
            stoploss_pct=stoploss_pct,
            hard_exit_time=hard_exit_time,
            leverage_factor=leverage_factor,
            top_n=top_n,
            universe=selected_universe,
        )

    # LIVE UPSTOX HISTORICAL FETCH
    sem = asyncio.Semaphore(8)  # Upstox rate limit concurrency control
    warnings: list[str] = []

    async with httpx.AsyncClient(timeout=18.0) as client:
        # Fetch candles for each stock concurrently
        tasks = [
            fetch_stock_candles(client, access_token, s["instrument_key"], date_str, sem)
            for s in universe_stocks
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Process fetched data
    scored_candidates = []

    for s, res in zip(universe_stocks, results):
        if isinstance(res, Exception) or not res:
            continue
        prev_close, candles = res
        if not candles or not prev_close or prev_close <= 0:
            continue

        # Find 9:16 candle
        c_916 = None
        for c in candles:
            if c.time_str in ("09:16", "09:15"):
                c_916 = c
                break
        if not c_916:
            c_916 = candles[0] if candles else None

        if not c_916:
            continue

        pct_change = ((c_916.close - prev_close) / prev_close) * 100.0
        scored_candidates.append({
            "stock": s,
            "prev_close": prev_close,
            "c_916": c_916,
            "candles": candles,
            "pct_change": pct_change,
        })

    if not scored_candidates:
        warnings.append(f"No valid trading data retrieved from Upstox for {date_str}. (Market holiday or weekend?) Falling back to demo data.")
        demo_res = _generate_demo_backtest(
            date_str=date_str,
            universe_stocks=universe_stocks,
            target_pct=target_pct,
            stoploss_pct=stoploss_pct,
            hard_exit_time=hard_exit_time,
            leverage_factor=leverage_factor,
            top_n=top_n,
            universe=selected_universe,
        )
        demo_res.warnings.extend(warnings)
        return demo_res

    # Rank top losers at 9:16
    scored_candidates.sort(key=lambda x: x["pct_change"])
    top_losers = scored_candidates[:top_n]

    trades: list[BacktestTrade] = []
    for rank, item in enumerate(top_losers, start=1):
        s = item["stock"]
        trade = simulate_pe_trade(
            symbol=s["symbol"],
            instrument_key=s["instrument_key"],
            sector=s.get("sector", "General"),
            avg_oi_tier=s.get("avg_oi_tier", "HIGH"),
            prev_close=item["prev_close"],
            candles_morning=item["candles"],
            target_pct=target_pct,
            stoploss_pct=stoploss_pct,
            hard_exit_time=hard_exit_time,
            leverage_factor=leverage_factor,
            rank=rank,
        )
        if trade:
            trades.append(trade)

    # Compute Summary
    total = len(trades)
    wins = sum(1 for t in trades if t.is_win)
    losses = total - wins
    targets = sum(1 for t in trades if t.exit_reason == "TARGET")
    sls = sum(1 for t in trades if t.exit_reason == "STOPLOSS")
    time_exits = sum(1 for t in trades if t.exit_reason == "TIME_EXIT")
    win_rate = round((wins / total * 100.0), 1) if total > 0 else 0.0
    total_pe_pnl = round(sum(t.pe_pnl_pct for t in trades), 2)
    avg_pe_pnl = round(total_pe_pnl / total, 2) if total > 0 else 0.0

    best_trade = max(trades, key=lambda t: t.pe_pnl_pct) if trades else None
    worst_trade = min(trades, key=lambda t: t.pe_pnl_pct) if trades else None

    summary = BacktestSummary(
        total_trades=total,
        wins=wins,
        losses=losses,
        targets_hit=targets,
        sl_hit=sls,
        time_exits=time_exits,
        win_rate=win_rate,
        total_pe_pnl_pct=total_pe_pnl,
        avg_pe_pnl_pct=avg_pe_pnl,
        best_trade={"symbol": best_trade.symbol, "pe_pnl_pct": best_trade.pe_pnl_pct} if best_trade else None,
        worst_trade={"symbol": worst_trade.symbol, "pe_pnl_pct": worst_trade.pe_pnl_pct} if worst_trade else None,
    )

    return BacktestResult(
        date=date_str,
        universe=selected_universe,
        pe_leverage_factor=leverage_factor,
        target_pct=target_pct,
        stoploss_pct=stoploss_pct,
        hard_exit_time=hard_exit_time,
        summary=summary,
        trades=trades,
        warnings=warnings,
        is_demo=False,
    )
