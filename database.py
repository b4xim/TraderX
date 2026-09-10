"""
TraderX — SQLite database layer
Stores all paper trades for history and stats.
"""

import sqlite3
from datetime import datetime, date
from pathlib import Path
from typing import Optional

from config_loader import load_config


DB_PATH = Path(load_config().database.path)


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    """Create the trades table if it doesn't exist."""
    conn = get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date      TEXT NOT NULL,           -- YYYY-MM-DD
            stock           TEXT NOT NULL,            -- e.g. RELIANCE
            option_symbol   TEXT NOT NULL,            -- Upstox instrument key
            entry_price     REAL NOT NULL,
            entry_time      TEXT NOT NULL,            -- ISO 8601
            exit_price      REAL,
            exit_time       TEXT,                     -- ISO 8601
            exit_reason     TEXT,                     -- TARGET / SL / TIME_EXIT
            pnl_percent     REAL,
            is_actual_pick  INTEGER DEFAULT 0         -- 1 if user marked as their real trade
        )
    """)
    conn.commit()
    conn.close()


def insert_trade(
    stock: str,
    option_symbol: str,
    entry_price: float,
    entry_time: datetime,
) -> int:
    """Record a new paper trade entry. Returns the trade ID."""
    conn = get_connection()
    cur = conn.execute(
        """
        INSERT INTO trades (trade_date, stock, option_symbol, entry_price, entry_time)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            entry_time.strftime("%Y-%m-%d"),
            stock,
            option_symbol,
            entry_price,
            entry_time.isoformat(),
        ),
    )
    trade_id = cur.lastrowid
    conn.commit()
    conn.close()
    return trade_id


def close_trade(
    trade_id: int,
    exit_price: float,
    exit_time: datetime,
    exit_reason: str,
    pnl_percent: float,
):
    """Record exit details for a trade."""
    conn = get_connection()
    conn.execute(
        """
        UPDATE trades
        SET exit_price = ?, exit_time = ?, exit_reason = ?, pnl_percent = ?
        WHERE id = ?
        """,
        (exit_price, exit_time.isoformat(), exit_reason, pnl_percent, trade_id),
    )
    conn.commit()
    conn.close()


def mark_actual_pick(trade_id: int, is_actual: bool = True):
    """Toggle whether this trade was the user's actual pick for the day."""
    conn = get_connection()
    # Clear any other actual pick for the same date first
    row = conn.execute("SELECT trade_date FROM trades WHERE id = ?", (trade_id,)).fetchone()
    if row and is_actual:
        conn.execute(
            "UPDATE trades SET is_actual_pick = 0 WHERE trade_date = ?",
            (row["trade_date"],),
        )
    conn.execute(
        "UPDATE trades SET is_actual_pick = ? WHERE id = ?",
        (1 if is_actual else 0, trade_id),
    )
    conn.commit()
    conn.close()


def get_trades_for_date(trade_date: str) -> list[dict]:
    """Get all trades for a given date."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM trades WHERE trade_date = ? ORDER BY entry_time",
        (trade_date,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_stats(actual_only: bool = False) -> dict:
    """Compute aggregate stats from trade history."""
    conn = get_connection()
    where = "WHERE pnl_percent IS NOT NULL"
    if actual_only:
        where += " AND is_actual_pick = 1"

    rows = conn.execute(f"SELECT pnl_percent FROM trades {where}").fetchall()
    conn.close()

    if not rows:
        return {
            "total_trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": 0.0,
            "avg_win_pct": 0.0,
            "avg_loss_pct": 0.0,
            "expectancy": 0.0,
        }

    pnls = [r["pnl_percent"] for r in rows]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]

    total = len(pnls)
    win_count = len(wins)
    loss_count = len(losses)
    win_rate = (win_count / total) * 100 if total else 0.0
    avg_win = sum(wins) / win_count if wins else 0.0
    avg_loss = sum(losses) / loss_count if losses else 0.0

    # Expectancy = (win_rate * avg_win) - (loss_rate * |avg_loss|)
    # Expressed as average expected % per trade
    loss_rate = (loss_count / total) * 100 if total else 0.0
    expectancy = (win_rate / 100 * avg_win) - (loss_rate / 100 * abs(avg_loss))

    return {
        "total_trades": total,
        "wins": win_count,
        "losses": loss_count,
        "win_rate": round(win_rate, 1),
        "avg_win_pct": round(avg_win, 2),
        "avg_loss_pct": round(avg_loss, 2),
        "expectancy": round(expectancy, 2),
    }




def get_today_trades() -> list[dict]:
    """Get trades for today."""
    return get_trades_for_date(date.today().strftime("%Y-%m-%d"))


def get_all_trade_dates() -> list[str]:
    """Get all distinct dates that have trade records, sorted descending (newest first)."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT DISTINCT trade_date FROM trades ORDER BY trade_date DESC"
    ).fetchall()
    conn.close()
    return [r["trade_date"] for r in rows]


