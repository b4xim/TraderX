"""
TraderX — Configuration loader
Reads config.yaml and exposes typed settings.
"""

import os
from pathlib import Path
from dataclasses import dataclass, field
import yaml


CONFIG_PATH = Path(__file__).parent / "config.yaml"


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
    server: ServerConfig = field(default_factory=ServerConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)


def load_config(path: Path = CONFIG_PATH) -> AppConfig:
    """Load configuration from YAML file, with env var overrides."""
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

    # Environment variable overrides (useful for CI / secrets)
    cfg.upstox.api_key = os.environ.get("UPSTOX_API_KEY", cfg.upstox.api_key)
    cfg.upstox.api_secret = os.environ.get("UPSTOX_API_SECRET", cfg.upstox.api_secret)

    return cfg
