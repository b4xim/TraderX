"""
TraderX — F&O Opening-Momentum Stock Picker
═══════════════════════════════════════════════════════════════
PAPER TRADING MODULE — Read-only market data consumer.
This module NEVER places orders or calls any order endpoint.

Purpose:
  Deterministic 3-stage filtering pipeline that takes pre-open and
  opening-market data and outputs a ranked shortlist of ATM PE option
  candidates for the opening-momentum strategy.

Invocation schedule:
  - Stage 1+2: called at 09:15 IST (end of pre-open session)
  - Stage 3:   called at 09:16 IST (after first 1-minute candle closes)

All thresholds are sourced exclusively from ScannerConfig (config.yaml).
No threshold is hardcoded here. No self-adjustment. No LLM calls.
Every stock that enters Stage 1 is logged with the exact rule that
excluded it, or "SELECTED" if it passed all stages.
═══════════════════════════════════════════════════════════════
"""

import asyncio
import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import httpx

from config_loader import ScannerConfig

logger = logging.getLogger("traderx.picker")

BASE_URL = "https://api.upstox.com/v2"

# ─── OI tier ordering ────────────────────────────────────────
OI_TIER_ORDER = {"LOW": 0, "MED": 1, "HIGH": 2}

# ─── Sector index display names ──────────────────────────────
SECTOR_INDEX_NAMES = {
    "NSE_INDEX|Nifty Bank":                "Nifty Bank",
    "NSE_INDEX|Nifty IT":                  "Nifty IT",
    "NSE_INDEX|Nifty Auto":                "Nifty Auto",
    "NSE_INDEX|Nifty Energy":              "Nifty Energy",
    "NSE_INDEX|Nifty Pharma":              "Nifty Pharma",
    "NSE_INDEX|Nifty Metal":               "Nifty Metal",
    "NSE_INDEX|Nifty FMCG":               "Nifty FMCG",
    "NSE_INDEX|Nifty Realty":              "Nifty Realty",
    "NSE_INDEX|Nifty PSU Bank":            "Nifty PSU Bank",
    "NSE_INDEX|Nifty Financial Services":  "Nifty Fin Services",
}


# ═══════════════════════════════════════════════════════════════
# Data Structures
# ═══════════════════════════════════════════════════════════════

@dataclass
class PreopenSnapshot:
    """Single pre-open LTP snapshot recorded during the polling window."""
    timestamp: str     # ISO 8601
    ltp: float
    direction: str     # "UP" | "DOWN" | "FLAT" vs previous snapshot


@dataclass
class StockCandidate:
    """Full annotation for one stock through the pipeline."""
    symbol: str
    instrument_key: str             # NSE_EQ key
    sector: str
    sector_index_key: str
    avg_oi_tier: str                # "HIGH" | "MED" | "LOW"

    # Price data
    prev_close: float
    pre_open_ltp: float
    gap_pct: float                  # negative = gap down; positive = gap up

    # Stage 2 fields
    prev_low: float = 0.0
    prev_high: float = 0.0
    prev_open: float = 0.0
    distance_from_prev_low_pct: float = 0.0   # positive = below prev low (preferred)
    preopen_snapshots: list = field(default_factory=list)
    preopen_flip_count: int = 0

    # Sector context
    sector_gap_pct: Optional[float] = None    # sector index gap % on the day
    sector_is_negative: bool = False

    # Corporate action flag
    has_corp_action: bool = False
    corp_action_note: str = ""

    # Stage 3 fields (populated at 9:16)
    live_change_pct: Optional[float] = None
    live_rank_among_losers: Optional[int] = None   # 1 = biggest loser
    candle_open: Optional[float] = None
    candle_high: Optional[float] = None
    candle_low: Optional[float] = None
    candle_close: Optional[float] = None
    is_red_candle: Optional[bool] = None
    circuit_hit: bool = False

    # ATM PE resolved at the end (Stage 3 selects → resolve PE)
    atm_pe_key: Optional[str] = None
    atm_pe_ltp: Optional[float] = None
    atm_strike: Optional[float] = None


@dataclass
class ExclusionRecord:
    """Audit record for every stock excluded at any stage."""
    symbol: str
    instrument_key: str
    stage: str          # "STAGE1" | "STAGE2" | "STAGE3"
    rule: str           # machine-readable rule name
    detail: str         # human-readable explanation with actual values


@dataclass
class ScanResult:
    """Full output of one complete pipeline run."""
    run_id: str                         # ISO timestamp of this run
    stage: str                          # "STAGE1_2" | "STAGE3"
    demo_mode: bool
    selected: list = field(default_factory=list)    # list[StockCandidate]
    excluded: list = field(default_factory=list)    # list[ExclusionRecord]
    warnings: list = field(default_factory=list)    # corp-action flags, sector notes
    config_snapshot: dict = field(default_factory=dict)  # scanner config at run time

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "stage": self.stage,
            "demo_mode": self.demo_mode,
            "selected": [asdict(s) for s in self.selected],
            "excluded": [asdict(e) for e in self.excluded],
            "warnings": self.warnings,
            "config_snapshot": self.config_snapshot,
        }


# ═══════════════════════════════════════════════════════════════
# Demo Data (used when demo_mode=True)
# ═══════════════════════════════════════════════════════════════

DEMO_PRE_OPEN_OVERRIDES = {
    # symbol → (prev_close, pre_open_ltp, flip_count, prev_low, prev_high)
    "TATASTEEL":  (135.50, 130.10, 0, 132.00, 140.00),   # gap -4.0%, below prev low ✓
    "HINDUNILVR": (2580.00, 2505.00, 1, 2550.00, 2610.00), # gap -2.9%, flip=1 ✓
    "MARUTI":     (10500.00, 10150.00, 0, 10350.00, 10700.00), # gap -3.3% ✓
    "BAJFINANCE": (6900.00, 6550.00, 3, 6750.00, 7100.00), # gap -5.1%, flips=3 → EXCLUDED
    "SUNPHARMA":  (1560.00, 1490.00, 0, 1530.00, 1590.00), # gap -4.5% ✓
    "WIPRO":      (445.00, 438.50, 1, 440.00, 458.00),     # gap -1.5%, barely in band ✓
    "IDEA":       (12.50, 10.00, 0, 11.00, 13.50),         # LOW OI tier → EXCLUDED Stage 1
    "TATAPOWER":  (420.00, 395.00, 0, 405.00, 430.00),     # gap -6.0%, at band edge ✓
    "NYKAA":      (180.00, 170.00, 2, 172.00, 185.00),     # LOW OI tier → EXCLUDED Stage 1
    "INFY":       (1820.00, 1755.00, 0, 1800.00, 1860.00), # gap -3.6% ✓
}

DEMO_STAGE3_OVERRIDES = {
    # symbol → (candle_open, candle_close, live_change_pct, circuit_hit)
    "TATASTEEL":  (130.20, 129.50, -4.2, False),   # red candle ✓
    "HINDUNILVR": (2507.00, 2520.00, -2.3, False),  # GREEN candle → EXCLUDED
    "MARUTI":     (10160.00, 10080.00, -3.9, False), # red candle ✓
    "SUNPHARMA":  (1492.00, 1481.00, -5.0, False),   # red candle ✓, top loser ✓
    "WIPRO":      (438.00, 434.00, -2.5, False),     # red candle ✓
    "TATAPOWER":  (396.00, 393.00, -6.8, False),     # red candle ✓
    "INFY":       (1756.00, 1742.00, -4.1, False),   # red candle ✓
}


# ═══════════════════════════════════════════════════════════════
# Upstox API Helpers
# ═══════════════════════════════════════════════════════════════

def _auth_headers(access_token: str) -> dict:
    return {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }


async def _bulk_ltp(access_token: str, instrument_keys: list[str], batch_size: int = 50) -> dict:
    """
    Fetch LTP for a large list of instrument keys in batches of `batch_size`.
    Returns dict: { instrument_key → { "ltp": float, "close_price": float } }
    """
    results: dict = {}
    for i in range(0, len(instrument_keys), batch_size):
        batch = instrument_keys[i: i + batch_size]
        keys_param = ",".join(batch)
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(
                f"{BASE_URL}/market-quote/ltp",
                params={"instrument_key": keys_param},
                headers=_auth_headers(access_token),
            )
            resp.raise_for_status()
            data = resp.json().get("data", {})
            for key, quote in data.items():
                results[key] = {
                    "ltp": float(quote.get("last_price", 0.0)),
                    "close_price": float(quote.get("close_price", 0.0)),
                }
        await asyncio.sleep(0.15)   # gentle rate-limit guard
    return results


async def _ohlc_prev_day(access_token: str, instrument_keys: list[str]) -> dict:
    """
    Fetch previous-day OHLC for a list of NSE equity instrument keys.
    Returns dict: { instrument_key → { open, high, low, close } }
    """
    results: dict = {}
    batch_size = 50
    for i in range(0, len(instrument_keys), batch_size):
        batch = instrument_keys[i: i + batch_size]
        keys_param = ",".join(batch)
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(
                f"{BASE_URL}/market-quote/ohlc",
                params={"instrument_key": keys_param, "interval": "1day"},
                headers=_auth_headers(access_token),
            )
            resp.raise_for_status()
            data = resp.json().get("data", {})
            for key, quote in data.items():
                ohlc = quote.get("ohlc", {})
                results[key] = {
                    "open":  float(ohlc.get("open", 0.0)),
                    "high":  float(ohlc.get("high", 0.0)),
                    "low":   float(ohlc.get("low", 0.0)),
                    "close": float(ohlc.get("close", 0.0)),
                }
        await asyncio.sleep(0.15)
    return results


async def _fetch_1min_candle(access_token: str, instrument_key: str) -> Optional[dict]:
    """
    Fetch the 9:15–9:16 first 1-minute candle for an instrument.
    Returns { open, high, low, close } or None if unavailable.
    Falls back to live quote open vs ltp if candle endpoint returns nothing.
    """
    today = date.today().strftime("%Y-%m-%d")
    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            resp = await client.get(
                f"{BASE_URL}/historical-candle/intraday/{instrument_key}/1minute",
                params={"to_date": today},
                headers=_auth_headers(access_token),
            )
            resp.raise_for_status()
            candles = resp.json().get("data", {}).get("candles", [])
            if candles:
                # candles format: [[timestamp, open, high, low, close, volume, oi], ...]
                # First candle in the list is the earliest (9:15)
                first = candles[-1] if len(candles) == 1 else candles[0]
                return {
                    "open":  float(first[1]),
                    "high":  float(first[2]),
                    "low":   float(first[3]),
                    "close": float(first[4]),
                }
        except Exception as exc:
            logger.warning("Candle fetch failed for %s: %s — falling back to LTP quote", instrument_key, exc)
    return None


async def _sector_ltp(access_token: str, sector_keys: list[str]) -> dict:
    """
    Fetch LTP + prev-close for all sector indices at once.
    Returns dict: { sector_key → { ltp, close_price, gap_pct } }
    """
    results: dict = {}
    try:
        keys_param = ",".join(sector_keys)
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(
                f"{BASE_URL}/market-quote/ltp",
                params={"instrument_key": keys_param},
                headers=_auth_headers(access_token),
            )
            resp.raise_for_status()
            data = resp.json().get("data", {})
            for key, quote in data.items():
                ltp = float(quote.get("last_price", 0.0))
                close = float(quote.get("close_price", 0.0))
                gap = ((ltp - close) / close * 100) if close else 0.0
                results[key] = {"ltp": ltp, "close_price": close, "gap_pct": round(gap, 2)}
    except Exception as exc:
        logger.warning("Sector LTP fetch failed: %s", exc)
    return results


# ═══════════════════════════════════════════════════════════════
# Static File Loaders
# ═══════════════════════════════════════════════════════════════

_BASE_DIR = Path(__file__).parent


def load_fo_stock_list(cfg: ScannerConfig) -> list[dict]:
    """Load the NSE F&O stock universe from the bundled JSON file."""
    path = _BASE_DIR / cfg.fo_stocks_file
    if not path.exists():
        logger.error("F&O stocks file not found: %s", path)
        return []
    try:
        stocks = json.loads(path.read_text())
        # Deduplicate by instrument_key
        seen = set()
        deduped = []
        for s in stocks:
            if s["instrument_key"] not in seen:
                seen.add(s["instrument_key"])
                deduped.append(s)
        logger.info("Loaded %d F&O stocks from %s", len(deduped), path.name)
        return deduped
    except Exception as exc:
        logger.error("Failed to load F&O stocks: %s", exc)
        return []


def load_corporate_actions(cfg: ScannerConfig) -> dict[str, str]:
    """
    Load today's corporate action flags.
    Returns dict: { symbol.upper() → action note string }
    """
    path = _BASE_DIR / cfg.corporate_actions_file
    today_str = date.today().strftime("%Y-%m-%d")
    actions: dict[str, str] = {}
    if not path.exists():
        return actions
    try:
        data = json.loads(path.read_text())
        for entry in data.get(today_str, []):
            sym = entry.get("symbol", "").upper()
            note = f"{entry.get('action', '')} — {entry.get('detail', '')}"
            if sym:
                actions[sym] = note
    except Exception as exc:
        logger.warning("Could not load corporate actions: %s", exc)
    return actions


# ═══════════════════════════════════════════════════════════════
# Pre-open polling (called by scheduler during 9:00–9:15)
# ═══════════════════════════════════════════════════════════════

# In-process store: { instrument_key → list[PreopenSnapshot] }
_preopen_history: dict[str, list[PreopenSnapshot]] = {}


def record_preopen_snapshot(instrument_key: str, ltp: float) -> None:
    """
    Record a pre-open LTP snapshot for a stock.
    Call this from the polling scheduler every `preopen_poll_interval_s` seconds.
    """
    now_str = datetime.now().isoformat()
    history = _preopen_history.setdefault(instrument_key, [])

    if not history:
        direction = "FLAT"
    else:
        prev_ltp = history[-1].ltp
        if ltp > prev_ltp:
            direction = "UP"
        elif ltp < prev_ltp:
            direction = "DOWN"
        else:
            direction = "FLAT"

    history.append(PreopenSnapshot(timestamp=now_str, ltp=ltp, direction=direction))
    logger.debug("Pre-open snapshot: %s ltp=%.2f dir=%s", instrument_key, ltp, direction)


def compute_flip_count(snapshots: list[PreopenSnapshot]) -> int:
    """
    Count direction changes (flips) in the pre-open price history.
    A flip is when direction changes from UP→DOWN or DOWN→UP (FLAT is ignored).
    E.g. UP, DOWN, UP = 2 flips.
    """
    flips = 0
    prev_dir = None
    for snap in snapshots:
        d = snap.direction
        if d == "FLAT":
            continue
        if prev_dir is not None and d != prev_dir:
            flips += 1
        prev_dir = d
    return flips


def clear_preopen_history() -> None:
    """Clear the pre-open history store (call at start of each trading day)."""
    _preopen_history.clear()


def get_preopen_history() -> dict:
    """Return current pre-open history for all tracked stocks."""
    return dict(_preopen_history)


# ═══════════════════════════════════════════════════════════════
# DEMO DATA generator
# ═══════════════════════════════════════════════════════════════

def _build_demo_candidates(fo_stocks: list[dict], corp_actions: dict[str, str]) -> list[StockCandidate]:
    """Build synthetic StockCandidate objects for demo mode (Stage 1+2 inputs)."""
    candidates = []
    for stock in fo_stocks:
        sym = stock["symbol"]
        if sym not in DEMO_PRE_OPEN_OVERRIDES:
            continue
        prev_close, pre_open_ltp, flips, prev_low, prev_high = DEMO_PRE_OPEN_OVERRIDES[sym]
        gap_pct = round((pre_open_ltp - prev_close) / prev_close * 100, 2)
        dist = round((prev_low - pre_open_ltp) / prev_low * 100, 2) if prev_low else 0.0
        demo_snaps = _build_demo_snapshots(prev_close, pre_open_ltp, flips)
        c = StockCandidate(
            symbol=sym,
            instrument_key=stock["instrument_key"],
            sector=stock["sector"],
            sector_index_key=stock["sector_index_key"],
            avg_oi_tier=stock["avg_oi_tier"],
            prev_close=prev_close,
            pre_open_ltp=pre_open_ltp,
            gap_pct=gap_pct,
            prev_low=prev_low,
            prev_high=prev_high,
            prev_open=prev_close,
            distance_from_prev_low_pct=dist,
            preopen_snapshots=[asdict(s) for s in demo_snaps],
            preopen_flip_count=flips,
            has_corp_action=sym in corp_actions,
            corp_action_note=corp_actions.get(sym, ""),
        )
        candidates.append(c)
    return candidates


def _build_demo_snapshots(prev_close: float, final_ltp: float, n_flips: int) -> list[PreopenSnapshot]:
    """Generate synthetic pre-open snapshots that produce `n_flips` direction changes."""
    snaps: list[PreopenSnapshot] = []
    if n_flips == 0:
        # Monotonically declining
        mid = (prev_close + final_ltp) / 2
        for ltp, dir_ in [(prev_close * 0.998, "DOWN"), (mid, "DOWN"), (final_ltp, "DOWN")]:
            snaps.append(PreopenSnapshot(timestamp=datetime.now().isoformat(), ltp=round(ltp, 2), direction=dir_))
    else:
        # Build a path that flips exactly n_flips times
        snaps.append(PreopenSnapshot(timestamp=datetime.now().isoformat(), ltp=prev_close, direction="FLAT"))
        for flip in range(n_flips + 1):
            if flip % 2 == 0:
                ltp = prev_close * 0.99
                dir_ = "DOWN"
            else:
                ltp = prev_close * 1.005
                dir_ = "UP"
            snaps.append(PreopenSnapshot(timestamp=datetime.now().isoformat(), ltp=round(ltp, 2), direction=dir_))
        snaps.append(PreopenSnapshot(timestamp=datetime.now().isoformat(), ltp=final_ltp, direction="DOWN"))
    return snaps


# ═══════════════════════════════════════════════════════════════
# Audit Logging
# ═══════════════════════════════════════════════════════════════

def log_scan_result(result: ScanResult, cfg: ScannerConfig) -> None:
    """
    Write the full scan result to a JSONL audit log.
    Each line is one stock entry. One file per day.
    """
    log_dir = _BASE_DIR / cfg.scan_log_dir
    log_dir.mkdir(exist_ok=True)
    today_str = date.today().strftime("%Y-%m-%d")
    log_path = log_dir / f"{today_str}.jsonl"

    run_ts = result.run_id
    entries = []

    for s in result.selected:
        entries.append({
            "run_id": run_ts,
            "stage": result.stage,
            "symbol": s.symbol,
            "outcome": "SELECTED",
            "stage_excluded": None,
            "rule_excluded": None,
            "detail": None,
            "gap_pct": s.gap_pct,
            "preopen_flips": s.preopen_flip_count,
            "distance_from_prev_low_pct": s.distance_from_prev_low_pct,
            "sector": s.sector,
            "sector_gap_pct": s.sector_gap_pct,
            "is_red_candle": s.is_red_candle,
            "circuit_hit": s.circuit_hit,
        })

    for e in result.excluded:
        entries.append({
            "run_id": run_ts,
            "stage": result.stage,
            "symbol": e.symbol,
            "outcome": "EXCLUDED",
            "stage_excluded": e.stage,
            "rule_excluded": e.rule,
            "detail": e.detail,
            "gap_pct": None,
            "preopen_flips": None,
            "distance_from_prev_low_pct": None,
            "sector": None,
            "sector_gap_pct": None,
            "is_red_candle": None,
            "circuit_hit": None,
        })

    try:
        with open(log_path, "a") as f:
            for entry in entries:
                f.write(json.dumps(entry) + "\n")
        logger.info("Scan result logged to %s (%d entries)", log_path.name, len(entries))
    except Exception as exc:
        logger.error("Failed to write scan log: %s", exc)


# ═══════════════════════════════════════════════════════════════
# STAGE 1 + 2  — Candidate Pool & Structure Filter
# ═══════════════════════════════════════════════════════════════

async def run_stage1_2(
    access_token: Optional[str],
    cfg: ScannerConfig,
) -> ScanResult:
    """
    Stage 1 (Candidate Pool) + Stage 2 (Structure Filter).
    Called at 9:15 IST (end of pre-open session).

    Returns ScanResult with .selected (passed both stages) and
    .excluded (full audit trail of every stock that didn't make it).
    """
    run_id = datetime.now().isoformat()
    logger.info("=" * 60)
    logger.info("  STOCK PICKER — Stage 1+2 starting  [run_id=%s]", run_id)
    logger.info("  demo_mode=%s", cfg.demo_mode)
    logger.info("=" * 60)

    fo_stocks = load_fo_stock_list(cfg)
    corp_actions = load_corporate_actions(cfg)

    config_snapshot = {
        "pool_size": cfg.pool_size,
        "min_avg_oi_tier": cfg.min_avg_oi_tier,
        "gap_min_pct": cfg.gap_min_pct,
        "gap_max_pct": cfg.gap_max_pct,
        "max_preopen_flips": cfg.max_preopen_flips,
        "top_n_losers_at_open": cfg.top_n_losers_at_open,
    }

    excluded: list[ExclusionRecord] = []
    warnings: list[str] = []

    # ── DEMO MODE: skip API calls, use synthetic data ────────────
    if cfg.demo_mode:
        logger.info("[DEMO] Generating synthetic pre-open data")
        all_candidates = _build_demo_candidates(fo_stocks, corp_actions)

        # Add sector context (dummy values in demo)
        sector_data = {
            "NSE_INDEX|Nifty IT":   {"gap_pct": -2.1, "ltp": 32000},
            "NSE_INDEX|Nifty Bank": {"gap_pct": -0.8, "ltp": 47000},
            "NSE_INDEX|Nifty Auto": {"gap_pct": -1.5, "ltp": 21000},
            "NSE_INDEX|Nifty Energy": {"gap_pct": -3.2, "ltp": 9800},
            "NSE_INDEX|Nifty Pharma": {"gap_pct": -0.4, "ltp": 18500},
            "NSE_INDEX|Nifty Metal": {"gap_pct": -4.1, "ltp": 8900},
            "NSE_INDEX|Nifty FMCG": {"gap_pct": -1.8, "ltp": 55000},
            "NSE_INDEX|Nifty Realty": {"gap_pct": +0.2, "ltp": 950},
            "NSE_INDEX|Nifty PSU Bank": {"gap_pct": -2.7, "ltp": 6200},
            "NSE_INDEX|Nifty Financial Services": {"gap_pct": -1.2, "ltp": 23000},
        }
        for c in all_candidates:
            idx_data = sector_data.get(c.sector_index_key, {})
            c.sector_gap_pct = idx_data.get("gap_pct")
            c.sector_is_negative = (c.sector_gap_pct is not None and c.sector_gap_pct < 0)

    else:
        # ── LIVE MODE: fetch real pre-open data ──────────────────
        all_eq_keys = [s["instrument_key"] for s in fo_stocks]
        logger.info("Fetching pre-open LTP for %d F&O stocks (batched)", len(all_eq_keys))

        try:
            ltp_data = await _bulk_ltp(access_token, all_eq_keys)
        except Exception as exc:
            logger.error("Bulk LTP fetch failed: %s", exc)
            ltp_data = {}

        # Fetch sector index context
        sector_data_raw = await _sector_ltp(access_token, cfg.sector_indices)

        all_candidates = []
        for stock in fo_stocks:
            key = stock["instrument_key"]
            sym = stock["symbol"]
            quote = ltp_data.get(key)
            if not quote:
                logger.debug("No LTP data for %s — skipping", sym)
                continue

            pre_ltp = quote["ltp"]
            prev_close = quote["close_price"]
            if prev_close <= 0:
                continue

            gap_pct = round((pre_ltp - prev_close) / prev_close * 100, 2)

            # Attach pre-open history from polling store
            snaps = _preopen_history.get(key, [])
            flips = compute_flip_count(snaps)

            # Sector context
            sector_key = stock["sector_index_key"]
            idx_info = sector_data_raw.get(sector_key, {})
            sector_gap = idx_info.get("gap_pct")

            c = StockCandidate(
                symbol=sym,
                instrument_key=key,
                sector=stock["sector"],
                sector_index_key=sector_key,
                avg_oi_tier=stock["avg_oi_tier"],
                prev_close=prev_close,
                pre_open_ltp=pre_ltp,
                gap_pct=gap_pct,
                preopen_snapshots=[asdict(s) for s in snaps],
                preopen_flip_count=flips,
                sector_gap_pct=sector_gap,
                sector_is_negative=(sector_gap is not None and sector_gap < 0),
                has_corp_action=sym in corp_actions,
                corp_action_note=corp_actions.get(sym, ""),
            )
            all_candidates.append(c)

    logger.info("Stage 1 pool entry: %d stocks from F&O universe", len(all_candidates))

    # ════════════════════════════════════════════════════════════
    # STAGE 1 — Rule 1: Liquidity Gate
    # ════════════════════════════════════════════════════════════
    liquidity_passed: list[StockCandidate] = []
    for c in all_candidates:
        if cfg.tier_passes(c.avg_oi_tier):
            liquidity_passed.append(c)
        else:
            reason = f"OI tier '{c.avg_oi_tier}' below minimum '{cfg.min_avg_oi_tier}'"
            excluded.append(ExclusionRecord(
                symbol=c.symbol, instrument_key=c.instrument_key,
                stage="STAGE1", rule="liquidity_gate", detail=reason,
            ))
            logger.info("[STAGE1] EXCLUDED %s — liquidity_gate: %s", c.symbol, reason)

    logger.info("Stage 1 after liquidity gate: %d stocks remain", len(liquidity_passed))

    # ════════════════════════════════════════════════════════════
    # STAGE 1 — Rule 2: Rank by pre-open gap%, take top pool_size
    # ════════════════════════════════════════════════════════════
    # Sort by gap_pct ascending (most negative = biggest gap-down first)
    liquidity_passed.sort(key=lambda c: c.gap_pct)
    pool = liquidity_passed[: cfg.pool_size]
    pool_keys = {c.instrument_key for c in pool}

    for c in liquidity_passed[cfg.pool_size:]:
        reason = f"gap_pct={c.gap_pct:+.2f}% (ranked outside top {cfg.pool_size})"
        excluded.append(ExclusionRecord(
            symbol=c.symbol, instrument_key=c.instrument_key,
            stage="STAGE1", rule="pool_size_cut", detail=reason,
        ))
        logger.info("[STAGE1] EXCLUDED %s — pool_size_cut: %s", c.symbol, reason)

    logger.info("Stage 1 pool after ranking: %d stocks  |  gaps: %s",
                len(pool), [f"{c.symbol}={c.gap_pct:+.2f}%" for c in pool])

    # ════════════════════════════════════════════════════════════
    # STAGE 1 — Rule 3: Corporate-action flag (surface, don't exclude)
    # ════════════════════════════════════════════════════════════
    for c in pool:
        if c.has_corp_action:
            msg = f"⚠ {c.symbol} has a corporate action today: {c.corp_action_note} — review before trading"
            warnings.append(msg)
            logger.warning("[STAGE1] Corp-action flag: %s", msg)

    # ════════════════════════════════════════════════════════════
    # STAGE 2 — Fetch previous-day OHLC for pool (live mode only)
    # ════════════════════════════════════════════════════════════
    if not cfg.demo_mode and pool:
        try:
            ohlc_data = await _ohlc_prev_day(access_token, [c.instrument_key for c in pool])
            for c in pool:
                ohlc = ohlc_data.get(c.instrument_key, {})
                c.prev_low  = ohlc.get("low", 0.0)
                c.prev_high = ohlc.get("high", 0.0)
                c.prev_open = ohlc.get("open", 0.0)
                if c.prev_low > 0:
                    c.distance_from_prev_low_pct = round(
                        (c.prev_low - c.pre_open_ltp) / c.prev_low * 100, 2
                    )
        except Exception as exc:
            logger.error("OHLC fetch failed: %s", exc)

    stage2_passed: list[StockCandidate] = []

    for c in pool:
        gap_abs = abs(c.gap_pct)

        # ── Stage 2, Rule 1: Gap-size band ───────────────────────
        if gap_abs < cfg.gap_min_pct:
            reason = (
                f"gap={c.gap_pct:+.2f}% — below minimum gap threshold "
                f"of {cfg.gap_min_pct:.1f}% (noise filter)"
            )
            excluded.append(ExclusionRecord(
                symbol=c.symbol, instrument_key=c.instrument_key,
                stage="STAGE2", rule="gap_too_small", detail=reason,
            ))
            logger.info("[STAGE2] EXCLUDED %s — gap_too_small: %s", c.symbol, reason)
            continue

        if gap_abs > cfg.gap_max_pct:
            reason = (
                f"gap={c.gap_pct:+.2f}% — above maximum gap threshold "
                f"of {cfg.gap_max_pct:.1f}% (likely extended/circuit risk)"
            )
            excluded.append(ExclusionRecord(
                symbol=c.symbol, instrument_key=c.instrument_key,
                stage="STAGE2", rule="gap_too_large", detail=reason,
            ))
            logger.info("[STAGE2] EXCLUDED %s — gap_too_large: %s", c.symbol, reason)
            continue

        # ── Stage 2, Rule 2: Pre-open stability ──────────────────
        if c.preopen_flip_count > cfg.max_preopen_flips:
            reason = (
                f"pre-open direction flipped {c.preopen_flip_count} times "
                f"(max allowed: {cfg.max_preopen_flips}) — erratic/unclear setup"
            )
            excluded.append(ExclusionRecord(
                symbol=c.symbol, instrument_key=c.instrument_key,
                stage="STAGE2", rule="preopen_erratic", detail=reason,
            ))
            logger.info("[STAGE2] EXCLUDED %s — preopen_erratic: %s", c.symbol, reason)
            continue

        # ── Stage 2, Rule 3: Distance from prev low (surface only) ─
        if c.distance_from_prev_low_pct < 0:
            # Stock is above prev low — note it as a data point but do NOT exclude
            msg = (
                f"ℹ {c.symbol} is {abs(c.distance_from_prev_low_pct):.2f}% ABOVE prev low "
                f"(prev_low=₹{c.prev_low:.2f}, pre_open=₹{c.pre_open_ltp:.2f}) — "
                "retesting a prior level, not fresh weakness"
            )
            warnings.append(msg)
            logger.info("[STAGE2] Prev-low note for %s: %s", c.symbol, msg)

        # ── Stage 2, Rule 4: Sector context (surface only) ────────
        if c.sector_gap_pct is not None:
            sector_name = SECTOR_INDEX_NAMES.get(c.sector_index_key, c.sector_index_key)
            sign = "↓" if c.sector_gap_pct < 0 else "↑"
            msg = (
                f"ℹ {c.symbol} sector ({sector_name}): "
                f"{sign}{abs(c.sector_gap_pct):.2f}% today"
            )
            warnings.append(msg)

        stage2_passed.append(c)
        logger.info("[STAGE2] PASSED %s  gap=%+.2f%%  flips=%d  dist_from_low=%+.2f%%",
                    c.symbol, c.gap_pct, c.preopen_flip_count, c.distance_from_prev_low_pct)

    logger.info("Stage 2 complete: %d stocks passed", len(stage2_passed))

    result = ScanResult(
        run_id=run_id,
        stage="STAGE1_2",
        demo_mode=cfg.demo_mode,
        selected=stage2_passed,
        excluded=excluded,
        warnings=warnings,
        config_snapshot=config_snapshot,
    )
    log_scan_result(result, cfg)
    return result


# ═══════════════════════════════════════════════════════════════
# STAGE 3 — Final Confirmation (9:16)
# ═══════════════════════════════════════════════════════════════

async def run_stage3(
    access_token: Optional[str],
    candidates: list[StockCandidate],
    cfg: ScannerConfig,
) -> ScanResult:
    """
    Stage 3 final confirmation. Called at ~9:16:30 IST.

    Rules (all deterministic, no LLM):
    1. Must still be in top-N F&O losers at 9:16 (real rank check).
    2. First 1-minute candle (9:15–9:16) must be RED (close < open).
    3. Circuit check: exclude any stock frozen at lower circuit.

    For stocks that pass all three: resolves ATM PE instrument_key + LTP.
    """
    run_id = datetime.now().isoformat()
    logger.info("=" * 60)
    logger.info("  STOCK PICKER — Stage 3 starting  [run_id=%s]", run_id)
    logger.info("  demo_mode=%s  |  candidates=%d", cfg.demo_mode, len(candidates))
    logger.info("=" * 60)

    if not candidates:
        logger.info("No Stage 2 candidates to confirm.")
        return ScanResult(run_id=run_id, stage="STAGE3", demo_mode=cfg.demo_mode)

    excluded: list[ExclusionRecord] = []
    warnings: list[str] = []
    final: list[StockCandidate] = []

    # ── Fetch live change % and 1-min candles ───────────────────
    if cfg.demo_mode:
        logger.info("[DEMO] Injecting synthetic 9:16 data")
        for c in candidates:
            if c.symbol in DEMO_STAGE3_OVERRIDES:
                o, cl, chg, circuit = DEMO_STAGE3_OVERRIDES[c.symbol]
                c.candle_open = o
                c.candle_close = cl
                c.candle_high = max(o, cl) * 1.002
                c.candle_low  = min(o, cl) * 0.998
                c.is_red_candle = cl < o
                c.live_change_pct = chg
                c.circuit_hit = circuit
            else:
                # Default: simulate red candle
                c.candle_open  = c.pre_open_ltp * 1.001
                c.candle_close = c.pre_open_ltp * 0.998
                c.candle_high  = c.pre_open_ltp * 1.002
                c.candle_low   = c.pre_open_ltp * 0.997
                c.is_red_candle = True
                c.live_change_pct = c.gap_pct - 0.3
                c.circuit_hit = False

    else:
        # Bulk live LTP to get real change %
        keys = [c.instrument_key for c in candidates]
        try:
            ltp_data = await _bulk_ltp(access_token, keys)
            for c in candidates:
                quote = ltp_data.get(c.instrument_key, {})
                ltp = quote.get("ltp", c.pre_open_ltp)
                prev = quote.get("close_price", c.prev_close)
                c.live_change_pct = round((ltp - prev) / prev * 100, 2) if prev else None
        except Exception as exc:
            logger.error("Live LTP fetch failed in Stage 3: %s", exc)

        # Fetch 1-minute candles
        for c in candidates:
            candle = await _fetch_1min_candle(access_token, c.instrument_key)
            if candle:
                c.candle_open  = candle["open"]
                c.candle_high  = candle["high"]
                c.candle_low   = candle["low"]
                c.candle_close = candle["close"]
                c.is_red_candle = candle["close"] < candle["open"]
            else:
                # Fallback: use live_ltp vs pre_open as proxy
                if c.live_change_pct is not None:
                    c.candle_open  = c.pre_open_ltp
                    c.candle_close = c.pre_open_ltp * (1 + c.live_change_pct / 100)
                    c.is_red_candle = c.candle_close < c.candle_open
                    warnings.append(
                        f"⚠ {c.symbol}: candle data unavailable — using LTP proxy for red-candle check"
                    )
                else:
                    c.is_red_candle = None  # unknown

    # ── Rank by live_change_pct (ascending → biggest losers first) ─
    ranked = sorted(
        [c for c in candidates if c.live_change_pct is not None],
        key=lambda c: c.live_change_pct
    )
    for rank, c in enumerate(ranked, start=1):
        c.live_rank_among_losers = rank

    # Include any with unknown live_change_pct at the end
    unknown_pct = [c for c in candidates if c.live_change_pct is None]
    all_ordered = ranked + unknown_pct

    # ── Apply Stage 3 rules ──────────────────────────────────────
    for c in all_ordered:
        # Rule 1: Still in top-N losers?
        rank = c.live_rank_among_losers
        if rank is None or rank > cfg.top_n_losers_at_open:
            chg_str = f"{c.live_change_pct:+.2f}%" if c.live_change_pct is not None else "unknown"
            reason = (
                f"live rank #{rank if rank else '?'} at 9:16 (change_pct={chg_str}) "
                f"— fell outside top-{cfg.top_n_losers_at_open} F&O losers"
            )
            excluded.append(ExclusionRecord(
                symbol=c.symbol, instrument_key=c.instrument_key,
                stage="STAGE3", rule="not_top_n_loser", detail=reason,
            ))
            logger.info("[STAGE3] EXCLUDED %s — not_top_n_loser: %s", c.symbol, reason)
            continue

        # Rule 2: First 1-minute candle must be RED
        if c.is_red_candle is False:
            reason = (
                f"first candle is GREEN/DOJI "
                f"(open=₹{c.candle_open:.2f}, close=₹{c.candle_close:.2f})"
            )
            excluded.append(ExclusionRecord(
                symbol=c.symbol, instrument_key=c.instrument_key,
                stage="STAGE3", rule="green_candle", detail=reason,
            ))
            logger.info("[STAGE3] EXCLUDED %s — green_candle: %s", c.symbol, reason)
            continue

        if c.is_red_candle is None:
            warnings.append(
                f"⚠ {c.symbol}: red-candle check inconclusive (no candle data) — human review required"
            )

        # Rule 3: Circuit check
        if c.circuit_hit:
            reason = "stock has hit lower circuit — no real liquidity, frozen bid"
            excluded.append(ExclusionRecord(
                symbol=c.symbol, instrument_key=c.instrument_key,
                stage="STAGE3", rule="circuit_hit", detail=reason,
            ))
            logger.info("[STAGE3] EXCLUDED %s — circuit_hit", c.symbol)
            continue

        # All rules passed — this stock is SELECTED
        final.append(c)
        logger.info(
            "[STAGE3] SELECTED %s  rank=#%d  change=%+.2f%%  red_candle=%s",
            c.symbol, rank,
            c.live_change_pct or 0.0,
            c.is_red_candle,
        )

    # ── For final picks: resolve ATM PE (skip in demo, just populate dummy) ─
    if not cfg.demo_mode and access_token and final:
        from upstox_client import UpstoxClient
        from config_loader import load_config
        _cfg = load_config()
        _client = UpstoxClient(
            api_key=_cfg.upstox.api_key,
            api_secret=_cfg.upstox.api_secret,
            redirect_uri=_cfg.upstox.redirect_uri,
        )
        _client._access_token = access_token
        for c in final:
            try:
                _, pe_key, pe_ltp = await _client.get_atm_pe(c.symbol)
                c.atm_pe_key = pe_key
                c.atm_pe_ltp = pe_ltp
                logger.info("Resolved ATM PE for %s: %s @ ₹%.2f", c.symbol, pe_key, pe_ltp)
            except Exception as exc:
                logger.error("ATM PE resolution failed for %s: %s", c.symbol, exc)
    else:
        for c in final:
            # Demo: synthetic PE values
            c.atm_pe_key = f"NSE_FO|{c.symbol}_PE_DEMO"
            c.atm_pe_ltp = round(abs(c.gap_pct) * 12, 2)

    logger.info("Stage 3 complete: %d stocks SELECTED, %d EXCLUDED", len(final), len(excluded))

    result = ScanResult(
        run_id=run_id,
        stage="STAGE3",
        demo_mode=cfg.demo_mode,
        selected=final,
        excluded=excluded,
        warnings=warnings,
        config_snapshot={
            "top_n_losers_at_open": cfg.top_n_losers_at_open,
        },
    )
    log_scan_result(result, cfg)
    return result


# ═══════════════════════════════════════════════════════════════
# Convenience: format output as readable text (for logging)
# ═══════════════════════════════════════════════════════════════

def format_result_text(result: ScanResult) -> str:
    """Format a ScanResult into the human-readable ranked list output."""
    lines: list[str] = []
    lines.append(f"\n{'='*60}")
    lines.append(f"  STOCK PICKER OUTPUT — {result.stage}  [{result.run_id[:19]}]")
    if result.demo_mode:
        lines.append("  ⚠ DEMO MODE — synthetic data")
    lines.append(f"{'='*60}")

    if not result.selected:
        lines.append("  No stocks passed all filters.")
    else:
        for rank, c in enumerate(result.selected, start=1):
            reasons = []
            reasons.append(f"gap {c.gap_pct:+.2f}%")
            if c.distance_from_prev_low_pct is not None:
                direction = "below" if c.distance_from_prev_low_pct > 0 else "above"
                reasons.append(f"{abs(c.distance_from_prev_low_pct):.2f}% {direction} prev low")
            if c.sector_gap_pct is not None:
                sector_name = SECTOR_INDEX_NAMES.get(c.sector_index_key, c.sector)
                reasons.append(f"sector ({sector_name}) {c.sector_gap_pct:+.2f}%")
            reasons.append(f"OI tier: {c.avg_oi_tier}")
            if c.atm_pe_key:
                reasons.append(f"ATM PE: {c.atm_pe_key} @ ₹{c.atm_pe_ltp:.2f}")
            if c.has_corp_action:
                reasons.append(f"⚠ corp-action: {c.corp_action_note}")
            lines.append(f"\n{rank}. {c.symbol} — Selected.")
            lines.append(f"   Reasons: {', '.join(reasons)}")

    if result.excluded:
        lines.append("\n\nExcluded from pool:")
        for e in result.excluded:
            lines.append(f"  - {e.symbol} — [{e.stage}/{e.rule}] {e.detail}")

    if result.warnings:
        lines.append("\n\nNotes:")
        for w in result.warnings:
            lines.append(f"  {w}")

    lines.append("")
    return "\n".join(lines)
