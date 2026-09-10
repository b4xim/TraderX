"""
TraderX — Main FastAPI Application
═══════════════════════════════════════════════════════════════
██████╗  █████╗ ██████╗ ███████╗██████╗     ████████╗██████╗  █████╗ ██████╗ ██╗███╗   ██╗ ██████╗
██╔══██╗██╔══██╗██╔══██╗██╔════╝██╔══██╗    ╚══██╔══╝██╔══██╗██╔══██╗██╔══██╗██║████╗  ██║██╔════╝
██████╔╝███████║██████╔╝█████╗  ██████╔╝       ██║   ██████╔╝███████║██║  ██║██║██╔██╗ ██║██║  ███╗
██╔═══╝ ██╔══██║██╔═══╝ ██╔══╝  ██╔══██╗       ██║   ██╔══██║██╔══██║██║  ██║██║██║╚██╗██║██║   ██║
██║     ██║  ██║██║     ███████╗██║  ██║       ██║   ██║  ██║██║  ██║██████╔╝██║██║ ╚████║╚██████╔╝
╚═╝     ╚═╝  ╚═╝╚═╝     ╚══════╝╚═╝  ╚═╝       ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝╚═════╝ ╚═╝╚═╝  ╚═══╝ ╚═════╝
═══════════════════════════════════════════════════════════════
PAPER TRADING MODE — NO LIVE ORDERS WILL BE PLACED.
This application ONLY reads market data from Upstox and simulates
trades in a local SQLite database. It NEVER calls any order-placement
endpoint. This is a read-only market data consumer.
═══════════════════════════════════════════════════════════════
"""

import asyncio
import json
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, date, time, timedelta
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Query
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from config_loader import load_config
from database import init_db, insert_trade, close_trade, mark_actual_pick, get_stats, get_today_trades, get_trades_for_date, get_all_trade_dates
from stock_picker import run_stage1_2, run_stage3, format_result_text, ScanResult, log_scan_result
from backtester import run_historical_backtest, BacktestResult
from upstox_client import UpstoxClient

IST = ZoneInfo("Asia/Kolkata")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("traderx")

# ─── Global state ───────────────────────────────────────────
cfg = load_config()

# Active positions for today (reset each day)
# Key: instrument_key, Value: position dict
active_positions: dict[str, dict] = {}

# Connected dashboard WebSocket clients
dashboard_clients: set[WebSocket] = set()

# Upstox client instance (initialized after import)
upstox: Optional[UpstoxClient] = None

# Feed connection status
feed_connected = False

# WebSocket feed task
ws_feed_task: Optional[asyncio.Task] = None

# Stock picker scan state
last_scan_result: Optional[ScanResult] = None
scanner_task: Optional[asyncio.Task] = None

# Hard exit scheduler task
exit_scheduler_task: Optional[asyncio.Task] = None



# ─── Position management ────────────────────────────────────

def create_position(stock: str, instrument_key: str, entry_price: float) -> dict:
    """Create a new paper trading position."""
    now = datetime.now(IST)
    target = entry_price * (1 + cfg.strategy.target_pct / 100)
    stoploss = entry_price * (1 - cfg.strategy.stoploss_pct / 100)

    trade_id = insert_trade(
        stock=stock,
        option_symbol=instrument_key,
        entry_price=entry_price,
        entry_time=now,
    )

    position = {
        "trade_id": trade_id,
        "stock": stock,
        "instrument_key": instrument_key,
        "entry_price": entry_price,
        "entry_time": now.isoformat(),
        "current_ltp": entry_price,
        "target": round(target, 2),
        "stoploss": round(stoploss, 2),
        "pnl_pct": 0.0,
        "status": "OPEN",
        "exit_price": None,
        "exit_time": None,
        "exit_reason": None,
        "is_actual_pick": False,
    }
    logger.info(
        f"POSITION OPENED: {stock} ({instrument_key}) @ ₹{entry_price:.2f} | "
        f"Target: ₹{target:.2f} | SL: ₹{stoploss:.2f}"
    )
    return position


def check_exit_conditions(position: dict, ltp: float) -> Optional[str]:
    """Check if target or stop-loss is hit. Returns exit reason or None."""
    if position["status"] != "OPEN":
        return None

    if ltp >= position["target"]:
        return "TARGET"
    elif ltp <= position["stoploss"]:
        return "SL"
    return None


def close_position(position: dict, exit_price: float, reason: str):
    """Close a position and log to database."""
    now = datetime.now(IST)
    pnl_pct = ((exit_price - position["entry_price"]) / position["entry_price"]) * 100

    position["current_ltp"] = exit_price
    position["exit_price"] = exit_price
    position["exit_time"] = now.isoformat()
    position["exit_reason"] = reason
    position["pnl_pct"] = round(pnl_pct, 2)
    position["status"] = reason  # TARGET / SL / TIME_EXIT

    close_trade(
        trade_id=position["trade_id"],
        exit_price=exit_price,
        exit_time=now,
        exit_reason=reason,
        pnl_percent=round(pnl_pct, 2),
    )

    logger.info(
        f"POSITION CLOSED [{reason}]: {position['stock']} @ ₹{exit_price:.2f} | "
        f"P&L: {pnl_pct:+.2f}%"
    )


# ─── Dashboard broadcast ────────────────────────────────────

async def broadcast_state():
    """Push current state to all connected dashboard clients."""
    state = build_dashboard_state()
    message = json.dumps(state)
    dead = set()
    for ws in dashboard_clients:
        try:
            await ws.send_text(message)
        except Exception:
            dead.add(ws)
    dashboard_clients -= dead


def build_dashboard_state() -> dict:
    """Build the full state object for the dashboard."""
    now = datetime.now(IST)

    # Calculate countdown to hard exit
    exit_parts = cfg.strategy.hard_exit_time.split(":")
    exit_time = now.replace(
        hour=int(exit_parts[0]),
        minute=int(exit_parts[1]),
        second=0,
        microsecond=0,
    )
    if now >= exit_time:
        countdown_seconds = 0
    else:
        countdown_seconds = int((exit_time - now).total_seconds())

    positions_list = list(active_positions.values())

    return {
        "type": "state_update",
        "positions": positions_list,
        "countdown_seconds": countdown_seconds,
        "hard_exit_time": cfg.strategy.hard_exit_time,
        "feed_connected": feed_connected,
        "current_time": now.strftime("%H:%M:%S"),
        "stats_all": get_stats(actual_only=False),
        "stats_actual": get_stats(actual_only=True),
    }


# ─── LTP tick handler ───────────────────────────────────────

async def on_ltp_tick(instrument_key: str, ltp: float, timestamp: int):
    """Called for every LTP update from the WebSocket feed."""
    if instrument_key not in active_positions:
        return

    pos = active_positions[instrument_key]
    if pos["status"] != "OPEN":
        return

    pos["current_ltp"] = ltp
    pos["pnl_pct"] = round(
        ((ltp - pos["entry_price"]) / pos["entry_price"]) * 100, 2
    )

    # Check exit conditions
    exit_reason = check_exit_conditions(pos, ltp)
    if exit_reason:
        close_position(pos, ltp, exit_reason)

    # Broadcast update to dashboard
    await broadcast_state()


# ─── Hard exit scheduler ────────────────────────────────────

async def schedule_hard_exit():
    """Wait until hard_exit_time and close all open positions."""
    global exit_scheduler_task
    now = datetime.now(IST)
    exit_parts = cfg.strategy.hard_exit_time.split(":")
    exit_time = now.replace(
        hour=int(exit_parts[0]),
        minute=int(exit_parts[1]),
        second=0,
        microsecond=0,
    )

    if now >= exit_time:
        logger.info("Hard exit time already passed for today.")
        return

    wait_seconds = (exit_time - now).total_seconds()
    logger.info(f"Hard exit scheduled in {wait_seconds:.0f}s at {cfg.strategy.hard_exit_time} IST")

    await asyncio.sleep(wait_seconds)

    logger.info("⏰ HARD EXIT TIME REACHED — closing all open positions")
    for key, pos in active_positions.items():
        if pos["status"] == "OPEN":
            close_position(pos, pos["current_ltp"], "TIME_EXIT")

    await broadcast_state()

    # Stop the WebSocket feed after hard exit
    global ws_feed_task
    if ws_feed_task and not ws_feed_task.done():
        ws_feed_task.cancel()
        logger.info("WebSocket feed stopped after hard exit.")


# ─── WebSocket feed management ──────────────────────────────

async def start_feed(instrument_keys: list[str]):
    """Start the Upstox WebSocket feed for given instruments."""
    global ws_feed_task, feed_connected

    if not upstox or not upstox.access_token:
        logger.error("Cannot start feed: not authenticated with Upstox")
        return

    # Import here to avoid circular dependency
    from upstox_client import stream_ltp

    async def feed_callback(inst_key: str, ltp: float, ts: int):
        await on_ltp_tick(inst_key, ltp, ts)

    async def on_connect():
        global feed_connected
        feed_connected = True
        await broadcast_state()
        logger.info("📡 Market data feed connected")

    async def on_disconnect():
        global feed_connected
        feed_connected = False
        await broadcast_state()
        logger.warning("⚠️ Market data feed disconnected")

    async def run_feed():
        try:
            await stream_ltp(
                access_token=upstox.access_token,
                instrument_keys=instrument_keys,
                callback=feed_callback,
                on_connect=on_connect,
                on_disconnect=on_disconnect,
            )
        except asyncio.CancelledError:
            logger.info("Feed task cancelled")
        except Exception as e:
            logger.error(f"Feed error: {e}")
            feed_connected = False
            await broadcast_state()

    # Cancel existing feed if running
    if ws_feed_task and not ws_feed_task.done():
        ws_feed_task.cancel()
        try:
            await ws_feed_task
        except (asyncio.CancelledError, Exception):
            pass

    ws_feed_task = asyncio.create_task(run_feed())


# ─── Stock Picker Schedulers ────────────────────────────────

async def _auto_scan_stage1_2():
    """Automatically run Stage 1+2 scan at 9:15 IST (if authenticated)."""
    global last_scan_result
    if not upstox or not upstox.access_token:
        logger.info("Scanner: skipping Stage 1+2 — not authenticated")
        return
    try:
        logger.info("⏰ 9:15 Auto-trigger: Stock Picker Stage 1+2")
        result = await run_stage1_2(
            access_token=upstox.access_token,
            cfg=cfg.scanner,
        )
        last_scan_result = result
        logger.info(format_result_text(result))
        # Broadcast to dashboard WebSocket clients
        await _broadcast_scan(result)
    except Exception as exc:
        logger.error("Auto Stage 1+2 scan failed: %s", exc)


async def _auto_scan_stage3():
    """Automatically run Stage 3 confirmation at 9:16:30 IST."""
    global last_scan_result
    if not upstox or not upstox.access_token:
        logger.info("Scanner: skipping Stage 3 — not authenticated")
        return
    if not last_scan_result or not last_scan_result.selected:
        logger.info("Scanner: skipping Stage 3 — no Stage 1+2 candidates")
        return
    try:
        logger.info("⏰ 9:16:30 Auto-trigger: Stock Picker Stage 3")
        result = await run_stage3(
            access_token=upstox.access_token,
            candidates=last_scan_result.selected,
            cfg=cfg.scanner,
        )
        last_scan_result = result
        logger.info(format_result_text(result))
        await _broadcast_scan(result)
    except Exception as exc:
        logger.error("Auto Stage 3 scan failed: %s", exc)


async def schedule_scanner():
    """Wait for 9:15 IST → run Stage 1+2; wait for 9:16:30 → run Stage 3."""
    global scanner_task
    now = datetime.now(IST)

    def _next_fire(h: int, m: int, s: int = 0) -> float:
        target = now.replace(hour=h, minute=m, second=s, microsecond=0)
        delta = (target - now).total_seconds()
        return delta

    s1_wait = _next_fire(9, 15, 0)
    s3_wait = _next_fire(9, 16, 30)

    if s1_wait > 0:
        logger.info("Scanner scheduled: Stage 1+2 in %.0fs, Stage 3 in %.0fs", s1_wait, s3_wait)
        await asyncio.sleep(s1_wait)
        await _auto_scan_stage1_2()
    else:
        logger.info("Stage 1+2 time already passed today — skipping auto-trigger")

    now2 = _next_fire(9, 16, 30)
    if now2 > 0:
        await asyncio.sleep(now2)
        await _auto_scan_stage3()
    else:
        logger.info("Stage 3 time already passed today — skipping auto-trigger")


async def _broadcast_scan(result: ScanResult):
    """Push scan update to all connected dashboard WebSocket clients."""
    global dashboard_clients
    payload = json.dumps({"type": "scan_update", **result.to_dict()})
    dead = set()
    for ws in dashboard_clients:
        try:
            await ws.send_text(payload)
        except Exception:
            dead.add(ws)
    dashboard_clients -= dead



# ─── App lifecycle ──────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("=" * 60)
    logger.info("  TraderX — Paper Trading Dashboard")
    logger.info("  PAPER TRADING MODE — NO LIVE ORDERS WILL BE PLACED")
    logger.info("=" * 60)

    init_db()

    # Initialize Upstox client
    global upstox
    from upstox_client import UpstoxClient
    upstox = UpstoxClient(
        api_key=cfg.upstox.api_key,
        api_secret=cfg.upstox.api_secret,
        redirect_uri=cfg.upstox.redirect_uri,
    )

    if upstox.access_token:
        logger.info("✓ Upstox access token loaded from cache")
    else:
        logger.info("⚠ No Upstox access token — login required via dashboard")

    # Start scanner scheduler (auto-fires at 9:15 and 9:16:30 IST)
    global scanner_task
    scanner_task = asyncio.create_task(schedule_scanner())

    yield

    # Shutdown
    if ws_feed_task and not ws_feed_task.done():
        ws_feed_task.cancel()
    if exit_scheduler_task and not exit_scheduler_task.done():
        exit_scheduler_task.cancel()
    if scanner_task and not scanner_task.done():
        scanner_task.cancel()
    logger.info("TraderX shut down.")


# ─── FastAPI app ────────────────────────────────────────────

app = FastAPI(title="TraderX", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


# ─── Routes ─────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Serve the main dashboard page."""
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={"config": cfg},
    )


@app.get("/instructions", response_class=HTMLResponse)
async def instructions(request: Request):
    """Serve the setup & instructions guide page."""
    return templates.TemplateResponse(
        request=request,
        name="instructions.html",
        context={"config": cfg},
    )


@app.api_route("/favicon.ico", methods=["GET", "HEAD"], include_in_schema=False)
async def favicon():
    """Serve the favicon for browsers directly requesting /favicon.ico."""
    return FileResponse("static/favicon.ico")


@app.get("/auth/login")
async def auth_login():
    """Redirect to Upstox OAuth login page."""
    if not upstox:
        return JSONResponse({"error": "Upstox client not initialized"}, status_code=500)
    url = upstox.get_auth_url()
    return RedirectResponse(url)


@app.get("/callback")
async def auth_callback(code: str = Query(...)):
    """Handle Upstox OAuth callback, exchange code for token."""
    if not upstox:
        return JSONResponse({"error": "Upstox client not initialized"}, status_code=500)
    try:
        token = await upstox.exchange_code(code)
        logger.info("✓ Upstox authentication successful")
        return RedirectResponse("/")
    except Exception as e:
        logger.error(f"Auth error: {e}")
        return JSONResponse({"error": str(e)}, status_code=400)


@app.get("/api/auth-status")
async def auth_status():
    """Check if we have a valid Upstox access token."""
    return {
        "authenticated": bool(upstox and upstox.access_token),
        "api_key_configured": bool(cfg.upstox.api_key),
    }


@app.post("/api/submit-stocks")
async def submit_stocks(request: Request):
    """
    Accept stock names or instrument keys, resolve ATM PE,
    fetch entry LTP, and start tracking positions.
    """
    global exit_scheduler_task

    if not upstox or not upstox.access_token:
        return JSONResponse(
            {"error": "Not authenticated. Please login to Upstox first."},
            status_code=401,
        )

    body = await request.json()
    inputs = body.get("stocks", [])

    if not inputs:
        return JSONResponse({"error": "No stocks provided"}, status_code=400)

    if len(inputs) > cfg.strategy.max_positions:
        return JSONResponse(
            {"error": f"Maximum {cfg.strategy.max_positions} positions allowed"},
            status_code=400,
        )

    # Clear any existing positions for today
    active_positions.clear()

    results = []
    instrument_keys = []

    for inp in inputs:
        inp = inp.strip()
        try:
            if "|" in inp:
                # User provided an exact instrument key (e.g., NSE_FO|51060)
                instrument_key = inp
                stock = inp.split("|")[-1]
                ltp = await upstox.get_ltp(instrument_key)
            else:
                # User provided a stock name — resolve ATM PE
                stock, instrument_key, ltp = await upstox.get_atm_pe(inp)

            pos = create_position(stock, instrument_key, ltp)
            active_positions[instrument_key] = pos
            instrument_keys.append(instrument_key)
            results.append({
                "stock": stock,
                "instrument_key": instrument_key,
                "entry_price": ltp,
                "target": pos["target"],
                "stoploss": pos["stoploss"],
            })

        except Exception as e:
            logger.error(f"Error resolving {inp}: {e}")
            results.append({"stock": inp, "error": str(e)})

    # Start WebSocket feed for all resolved instruments
    if instrument_keys:
        await start_feed(instrument_keys)

        # Schedule hard exit
        if exit_scheduler_task and not exit_scheduler_task.done():
            exit_scheduler_task.cancel()
        exit_scheduler_task = asyncio.create_task(schedule_hard_exit())

    await broadcast_state()
    return {"results": results}


@app.post("/api/mark-actual/{trade_id}")
async def toggle_actual_pick(trade_id: int):
    """Toggle whether a trade is marked as the user's actual pick."""
    # Find the position
    for pos in active_positions.values():
        if pos["trade_id"] == trade_id:
            pos["is_actual_pick"] = not pos["is_actual_pick"]
            mark_actual_pick(trade_id, pos["is_actual_pick"])
            await broadcast_state()
            return {"ok": True, "is_actual_pick": pos["is_actual_pick"]}

    # Not in active positions — just update the DB
    mark_actual_pick(trade_id, True)
    return {"ok": True, "is_actual_pick": True}


@app.get("/api/stats")
async def api_stats(actual_only: bool = False):
    """Get aggregate trading stats."""
    return get_stats(actual_only=actual_only)


@app.get("/api/history")
async def api_history(date_str: Optional[str] = None):
    """Get trade history for a given date or today."""
    if date_str:
        trades = get_trades_for_date(date_str)
    else:
        trades = get_today_trades()
    return {"trades": trades}


@app.post("/api/backtest")
async def api_backtest(req: Request):
    """
    Run historical strategy backtest on top-5 losing F&O stocks at 9:16 AM
    for a specified date.
    Body JSON: {"date": "YYYY-MM-DD", "top_n": 5, "universe": "HIGH"}
    """
    try:
        body = await req.json()
    except Exception:
        body = {}

    date_str = body.get("date")
    if not date_str:
        return JSONResponse({"error": "Missing 'date' parameter (format: YYYY-MM-DD)"}, status_code=400)

    top_n = int(body.get("top_n", 5))
    universe = body.get("universe", cfg.scanner.backtest_universe)

    token = upstox.access_token if (upstox and upstox.access_token) else None

    try:
        res = await run_historical_backtest(
            access_token=token,
            date_str=date_str,
            cfg=cfg.scanner,
            strategy_cfg=cfg.strategy,
            top_n=top_n,
            universe=universe,
        )
        return res.to_dict()
    except Exception as exc:
        logger.error("Backtest failed for date %s: %s", date_str, exc, exc_info=True)
        return JSONResponse({"error": str(exc)}, status_code=500)


@app.get("/api/state")
async def api_state():
    """Get current state snapshot (for initial page load)."""
    return build_dashboard_state()


# ─── Stock Picker API ────────────────────────────────────────

@app.post("/api/scan/stage1-2")
async def api_scan_stage1_2():
    """
    Manually trigger Stage 1+2 (pre-open candidate pool + structure filter).
    In demo_mode=True this works at any time with synthetic data.
    In live mode this should be called at or after 9:00 IST.
    """
    global last_scan_result
    if not upstox or not upstox.access_token:
        # Allow in demo mode without auth
        if not cfg.scanner.demo_mode:
            return JSONResponse(
                {"error": "Not authenticated. Login to Upstox first."},
                status_code=401,
            )
    try:
        result = await run_stage1_2(
            access_token=upstox.access_token if upstox else None,
            cfg=cfg.scanner,
        )
        last_scan_result = result
        logger.info(format_result_text(result))
        await _broadcast_scan(result)
        return result.to_dict()
    except Exception as exc:
        logger.error("Stage 1+2 scan error: %s", exc)
        return JSONResponse({"error": str(exc)}, status_code=500)


@app.post("/api/scan/stage3")
async def api_scan_stage3():
    """
    Manually trigger Stage 3 (first-candle confirmation at 9:16).
    Uses the candidates from the last Stage 1+2 run.
    In demo_mode this works at any time.
    """
    global last_scan_result
    if not last_scan_result:
        return JSONResponse(
            {"error": "No Stage 1+2 result found. Run Stage 1+2 first."},
            status_code=400,
        )
    if not upstox or not upstox.access_token:
        if not cfg.scanner.demo_mode:
            return JSONResponse(
                {"error": "Not authenticated. Login to Upstox first."},
                status_code=401,
            )
    try:
        result = await run_stage3(
            access_token=upstox.access_token if upstox else None,
            candidates=last_scan_result.selected,
            cfg=cfg.scanner,
        )
        last_scan_result = result
        logger.info(format_result_text(result))
        await _broadcast_scan(result)
        return result.to_dict()
    except Exception as exc:
        logger.error("Stage 3 scan error: %s", exc)
        return JSONResponse({"error": str(exc)}, status_code=500)


@app.get("/api/scan/state")
async def api_scan_state():
    """Return the last scan result (for Tab 2 initial load)."""
    if last_scan_result:
        return last_scan_result.to_dict()
    return {"stage": None, "selected": [], "excluded": [], "warnings": [], "demo_mode": cfg.scanner.demo_mode}


@app.get("/api/config")
async def api_config():
    """Expose current strategy + scanner config to the UI (read-only)."""
    from dataclasses import asdict
    return {
        "strategy": {
            "target_pct": cfg.strategy.target_pct,
            "stoploss_pct": cfg.strategy.stoploss_pct,
            "hard_exit_time": cfg.strategy.hard_exit_time,
            "entry_window_start": cfg.strategy.entry_window_start,
            "max_positions": cfg.strategy.max_positions,
        },
        "scanner": {
            "pool_size": cfg.scanner.pool_size,
            "min_avg_oi_tier": cfg.scanner.min_avg_oi_tier,
            "gap_min_pct": cfg.scanner.gap_min_pct,
            "gap_max_pct": cfg.scanner.gap_max_pct,
            "max_preopen_flips": cfg.scanner.max_preopen_flips,
            "top_n_losers_at_open": cfg.scanner.top_n_losers_at_open,
            "preopen_poll_interval_s": cfg.scanner.preopen_poll_interval_s,
            "demo_mode": cfg.scanner.demo_mode,
        },
    }


@app.get("/api/history-dates")
async def api_history_dates():
    """Return all dates that have recorded trades (for Backtester date picker)."""
    return {"dates": get_all_trade_dates()}


@app.get("/api/scan/log")
async def api_scan_log(date_str: Optional[str] = None):
    """Return the JSONL scan audit log for a given date (or today)."""
    from pathlib import Path
    today_str = date_str or date.today().strftime("%Y-%m-%d")
    log_path = Path(cfg.scanner.scan_log_dir) / f"{today_str}.jsonl"
    if not log_path.exists():
        return {"date": today_str, "entries": []}
    try:
        entries = [json.loads(line) for line in log_path.read_text().splitlines() if line.strip()]
        return {"date": today_str, "entries": entries}
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time dashboard updates."""
    await websocket.accept()
    dashboard_clients.add(websocket)
    logger.info(f"Dashboard client connected ({len(dashboard_clients)} total)")

    try:
        # Send initial state
        state = build_dashboard_state()
        await websocket.send_text(json.dumps(state))

        # Keep connection alive — listen for pings
        while True:
            data = await websocket.receive_text()
            # Client can send "ping" to keep alive
            if data == "ping":
                await websocket.send_text(json.dumps({"type": "pong"}))
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        dashboard_clients.discard(websocket)
        logger.info(f"Dashboard client disconnected ({len(dashboard_clients)} total)")


# ─── Entry point ────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host=cfg.server.host,
        port=cfg.server.port,
        reload=False,
        log_level="info",
    )
