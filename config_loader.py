"""
TraderX — Configuration loader
Reads config.yaml and exposes typed settings.
"""

import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import List
import yaml


CONFIG_PATH = Path(__file__).parent / "config.yaml"

# OI tier ordering (higher index = better liquidity)
OI_TIER_ORDER = {"LOW": 0, "MED": 1, "HIGH": 2}


@dataclass
class UpstoxConfig:
    api_key: str = ""
    api_secret: str = ""
    redirect_uri: str = "http://127.0.0.1:8000/callback"


@dataclass
class StrategyConfig:
    target_pct: float = 20.0
    stoploss_pct: float = 20.0
    hard_exit_time: str = "09:32"       # HH:MM IST
    entry_window_start: str = "09:15"
    max_positions: int = 3


@dataclass
class ScannerConfig:
    # Stage 1
    pool_size: int = 6
    min_avg_oi_tier: str = "MED"        # "HIGH" | "MED" | "LOW"
    preopen_poll_interval_s: int = 120

    # Stage 2
    gap_min_pct: float = 1.5
    gap_max_pct: float = 6.0
    max_preopen_flips: int = 2

    # Stage 3
    top_n_losers_at_open: int = 3

    # Output & logging
    scan_log_dir: str = "scan_log"
    fo_stocks_file: str = "fo_stocks.json"
    corporate_actions_file: str = "corporate_actions.json"

    # Sector indices (list of Upstox instrument keys)
    sector_indices: List[str] = field(default_factory=lambda: [
        "NSE_INDEX|Nifty Bank",
        "NSE_INDEX|Nifty IT",
        "NSE_INDEX|Nifty Auto",
        "NSE_INDEX|Nifty Energy",
        "NSE_INDEX|Nifty Pharma",
        "NSE_INDEX|Nifty Metal",
        "NSE_INDEX|Nifty FMCG",
        "NSE_INDEX|Nifty Realty",
        "NSE_INDEX|Nifty PSU Bank",
        "NSE_INDEX|Nifty Financial Services",
    ])

    # Backtesting
    backtest_universe: str = "HIGH"
    backtest_pe_leverage_factor: float = 2.0

    # Demo / dry-run mode (replays mock pre-open data outside market hours)
    demo_mode: bool = True

    def tier_passes(self, stock_tier: str) -> bool:
        """Return True if stock_tier meets the minimum OI tier threshold."""
        return OI_TIER_ORDER.get(stock_tier, 0) >= OI_TIER_ORDER.get(self.min_avg_oi_tier, 0)


@dataclass
class ServerConfig:
    host: str = "127.0.0.1"
    port: int = 8000


@dataclass
class DatabaseConfig:
    path: str = "traderx.db"


@dataclass
class AppConfig:
    upstox: UpstoxConfig = field(default_factory=UpstoxConfig)
    strategy: StrategyConfig = field(default_factory=StrategyConfig)
    scanner: ScannerConfig = field(default_factory=ScannerConfig)
    server: ServerConfig = field(default_factory=ServerConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)


DOTENV_PATH = Path(__file__).parent / ".env"


def _load_dotenv(path: Path = DOTENV_PATH) -> None:
    """Simple .env loader that populates os.environ if not already set."""
    if path.exists():
        try:
            for line in path.read_text().splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip("\"'")
                if k and k not in os.environ:
                    os.environ[k] = v
        except Exception:
            pass


def load_config(path: Path = CONFIG_PATH) -> AppConfig:
    """Load configuration from YAML file, with env var overrides."""
    _load_dotenv()
    cfg = AppConfig()

    if path.exists():
        with open(path) as f:
            raw = yaml.safe_load(f) or {}

        if "upstox" in raw:
            cfg.upstox = UpstoxConfig(**raw["upstox"])
        if "strategy" in raw:
            cfg.strategy = StrategyConfig(**raw["strategy"])
        if "server" in raw:
            cfg.server = ServerConfig(**raw["server"])
        if "database" in raw:
            cfg.database = DatabaseConfig(**raw["database"])
        if "scanner" in raw:
            scanner_raw = dict(raw["scanner"])  # copy to avoid mutating parsed YAML
            # sector_indices is a list — extract separately
            sector_indices = scanner_raw.pop("sector_indices", None)
            # Remove comment-only keys that aren't dataclass fields
            valid_fields = {f.name for f in ScannerConfig.__dataclass_fields__.values()}
            scanner_raw = {k: v for k, v in scanner_raw.items() if k in valid_fields}
            cfg.scanner = ScannerConfig(**scanner_raw)
            if sector_indices is not None:
                cfg.scanner.sector_indices = sector_indices

    # Environment variable overrides
    cfg.upstox.api_key    = os.environ.get("UPSTOX_API_KEY",    cfg.upstox.api_key)
    cfg.upstox.api_secret = os.environ.get("UPSTOX_API_SECRET", cfg.upstox.api_secret)

    return cfg
