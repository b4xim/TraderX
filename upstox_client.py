"""
TraderX — Upstox API Client
============================
PAPER TRADING MODE — this module only reads market data, never places orders.
No order-placement endpoint is called or even imported.

Handles:
  - OAuth 2.0 authorization-code flow (login → token)
  - Instrument search (stock name → instrument key)
  - Option chain lookup (stock → ATM PE instrument key + LTP)
  - REST LTP fallback
  - WebSocket V3 live market data feed (protobuf-encoded)
"""

import asyncio
import json
import logging
import ssl
import time
import uuid
from datetime import date
from pathlib import Path
from typing import Callable, Awaitable
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

_IST = ZoneInfo("Asia/Kolkata")

import httpx

try:
    import websockets
except ImportError:
    websockets = None  # type: ignore[assignment]

# Protobuf decode — fall back gracefully if the compiled module is missing.
_PROTO_AVAILABLE = False
try:
    from MarketDataFeed_pb2 import FeedResponse as PbFeedResponse  # type: ignore[import-untyped]
    _PROTO_AVAILABLE = True
except Exception:
    PbFeedResponse = None

logger = logging.getLogger("traderx.upstox")

# ──────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────
BASE_URL = "https://api.upstox.com/v2"
AUTH_DIALOG_URL = f"{BASE_URL}/login/authorization/dialog"
TOKEN_URL = f"{BASE_URL}/login/authorization/token"
INSTRUMENT_SEARCH_URL = f"{BASE_URL}/instruments/search"
OPTION_CHAIN_URL = f"{BASE_URL}/option/chain"
LTP_URL = f"{BASE_URL}/market-quote/ltp"
WS_AUTH_URL = f"{BASE_URL}/feed/market-data-feed/authorize"

TOKEN_FILE = Path(__file__).parent / ".upstox_token"


def nearest_monthly_expiry(ref: date | None = None) -> str:
    """Return the nearest NSE monthly-expiry date as YYYY-MM-DD.

    NSE stock options expire on the **last Thursday of the month**.
    If the last Thursday of the current month is today or still in the
    future we return it; otherwise we return next month's last Thursday.
    """
    return _monthly_expiry_candidates(ref)[0]


def _monthly_expiry_candidates(ref: date | None = None, n: int = 12) -> list[str]:
    """Return up to *n* monthly NSE expiry date candidates sorted ascending.

    NSE expiry rules (as of 2024-2026):
      - NIFTY 50 monthly options  : last **Tuesday** of the month
      - NIFTY 50 weekly options   : every Tuesday
      - Stock options (monthly)   : last **Thursday** of the month
      - Holiday rule: if that day is a trading holiday, expiry shifts
        to the preceding trading day (Friday/Wednesday respectively)

    To cover all cases without a holiday calendar we generate:
      last Tuesday, Thursday, Friday, Saturday
    for the current month + next 2 months, filter to >= today,
    deduplicate, sort ascending, and return the nearest *n*.
    """
    import calendar
    from datetime import timedelta

    def last_weekday(y: int, m: int, weekday: int) -> date:
        """Last occurrence of *weekday* (0=Mon…6=Sun) in the given month."""
        last_day = calendar.monthrange(y, m)[1]
        d = date(y, m, last_day)
        offset = (d.weekday() - weekday) % 7
        return d - timedelta(days=offset)

    today = ref or date.today()
    candidates: set[date] = set()
    y, m = today.year, today.month

    for _ in range(3):                  # current + next 2 months
        for wd in (1, 3, 4, 5):        # Tue=1, Thu=3, Fri=4, Sat=5
            candidates.add(last_weekday(y, m, wd))
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1

    upcoming = sorted(c for c in candidates if c >= today)
    return [d.isoformat() for d in upcoming[:n]]


# ──────────────────────────────────────────────────────────────
# Upstox REST Client
# ──────────────────────────────────────────────────────────────
class UpstoxClient:
    """Read-only Upstox API client.  PAPER TRADING — no orders."""

    def __init__(self, api_key: str, api_secret: str, redirect_uri: str) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.redirect_uri = redirect_uri
        self._access_token: str | None = None
        self._load_token()

    # ── token persistence ────────────────────────────────────

    def _load_token(self) -> None:
        """Load a previously saved access token from disk.

        Discards the cached token if it was saved on a previous calendar
        day (IST) — Upstox tokens expire at the end of each trading day.
        """
        if TOKEN_FILE.exists():
            try:
                data = json.loads(TOKEN_FILE.read_text())
                saved_date = data.get("saved_date")  # e.g. "2026-09-17"
                today = date.today().isoformat()
                if saved_date != today:
                    logger.info(
                        "Cached token is from %s — discarding (today is %s)",
                        saved_date, today,
                    )
                    TOKEN_FILE.unlink(missing_ok=True)
                    self._access_token = None
                    return
                self._access_token = data.get("access_token")
                logger.info("Loaded cached access token from %s", TOKEN_FILE)
            except (json.JSONDecodeError, OSError):
                self._access_token = None

    def _save_token(self, token: str) -> None:
        """Persist the access token with today's IST date for expiry checking."""
        TOKEN_FILE.write_text(json.dumps({
            "access_token": token,
            "saved_date": date.today().isoformat(),
        }))
        logger.info("Access token saved to %s", TOKEN_FILE)

    def _clear_token(self) -> None:
        """Invalidate the in-memory token and remove the token file."""
        self._access_token = None
        TOKEN_FILE.unlink(missing_ok=True)
        logger.warning("Access token cleared — re-authentication required.")

    @property
    def access_token(self) -> str | None:
        return self._access_token

    @access_token.setter
    def access_token(self, value: str) -> None:
        self._access_token = value
        self._save_token(value)

    @property
    def is_authenticated(self) -> bool:
        return bool(self._access_token)

    def _auth_headers(self) -> dict[str, str]:
        if not self._access_token:
            raise RuntimeError("Not authenticated — complete the OAuth flow first.")
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Accept": "application/json",
        }

    # ── OAuth flow ───────────────────────────────────────────

    def get_auth_url(self) -> str:
        """Return the browser URL the user must visit to authorize the app."""
        params = urlencode({
            "client_id": self.api_key,
            "redirect_uri": self.redirect_uri,
            "response_type": "code",
        })
        return f"{AUTH_DIALOG_URL}?{params}"

    async def exchange_code(self, code: str) -> str:
        """Exchange the authorization code for an access token.

        Returns the access token string and persists it to disk.
        """
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                TOKEN_URL,
                data={
                    "code": code,
                    "client_id": self.api_key,
                    "client_secret": self.api_secret,
                    "redirect_uri": self.redirect_uri,
                    "grant_type": "authorization_code",
                },
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "application/json",
                },
            )
            if resp.is_error:
                error_detail = resp.text
                try:
                    err_json = resp.json()
                    if "errors" in err_json and isinstance(err_json["errors"], list):
                        error_detail = "; ".join(e.get("message", str(e)) for e in err_json["errors"])
                    elif "message" in err_json:
                        error_detail = err_json["message"]
                except Exception:
                    pass
                logger.error("Upstox token exchange failed (%s): %s", resp.status_code, error_detail)
                raise RuntimeError(f"Upstox API error ({resp.status_code}): {error_detail}")
            data = resp.json()

        token = data["access_token"]
        self.access_token = token  # also saves to disk
        logger.info("OAuth token acquired successfully.")
        return token

    # ── Instrument search ────────────────────────────────────

    # Known index instruments that live in NSE_INDEX, not NSE_EQ
    _INDEX_KEYS: dict[str, str] = {
        "NIFTY":       "NSE_INDEX|Nifty 50",
        "NIFTY50":     "NSE_INDEX|Nifty 50",
        "NIFTY 50":    "NSE_INDEX|Nifty 50",
        "BANKNIFTY":   "NSE_INDEX|Nifty Bank",
        "BANK NIFTY":  "NSE_INDEX|Nifty Bank",
        "FINNIFTY":    "NSE_INDEX|Nifty Fin Service",
        "MIDCPNIFTY":  "NSE_INDEX|NIFTY MID SELECT",
        "SENSEX":      "BSE_INDEX|SENSEX",
    }

    async def search_instrument(self, stock_name: str) -> str:
        """Search for an NSE equity or index instrument key by name.

        Indices (NIFTY, BANKNIFTY, etc.) are resolved directly from a
        built-in map without an API call.  Equity stocks are resolved
        via the Upstox instrument search API.

        Returns an instrument_key (e.g. ``NSE_EQ|INE002A01018`` or
        ``NSE_INDEX|Nifty 50``).
        """
        # Fast-path: known index names
        index_key = self._INDEX_KEYS.get(stock_name.strip().upper())
        if index_key:
            logger.info("Index instrument resolved: %s → %s", stock_name, index_key)
            return index_key

        async with httpx.AsyncClient() as client:
            resp = await client.get(
                INSTRUMENT_SEARCH_URL,
                params={
                    "query": stock_name.upper(),
                    "segments": "EQ",
                    "exchanges": "NSE",
                    "records": 5,
                },
                headers=self._auth_headers(),
            )
            if resp.status_code == 401:
                self._clear_token()
                raise RuntimeError(
                    "Upstox token expired or invalid — please login again via the dashboard."
                )
            resp.raise_for_status()
            data = resp.json()

        instruments = data.get("data", [])
        if not instruments:
            raise ValueError(f"No NSE equity instrument found for '{stock_name}'")

        # Prefer exact trading_symbol match
        for inst in instruments:
            if inst.get("trading_symbol", "").upper() == stock_name.upper():
                logger.info("Instrument found: %s → %s", stock_name, inst["instrument_key"])
                return inst["instrument_key"]

        # Fall back to first result
        key = instruments[0]["instrument_key"]
        logger.info("Instrument found (first match): %s → %s", stock_name, key)
        return key

    # ── Option chain — ATM PE lookup ─────────────────────────

    async def get_atm_pe(self, stock_name: str) -> tuple[str, str, float]:
        """Find the ATM Put Option for *stock_name* (nearest monthly expiry).

        Tries the nearest 3 monthly expiry dates in sequence (last Thursday
        of each month) and uses the first one that returns a non-empty option
        chain. This handles cases where a contract hasn't been admitted to
        trading for the nearest computed Thursday yet.

        Returns ``(stock_name, pe_instrument_key, pe_ltp)``.
        """
        # Step 1 — resolve stock name to equity instrument key
        eq_key = await self.search_instrument(stock_name)

        # Step 2 — try expiry candidates until one yields a non-empty chain
        candidates = _monthly_expiry_candidates()
        last_error: Exception | None = None

        async with httpx.AsyncClient() as client:
            for expiry_date in candidates:
                logger.info("Trying expiry %s for %s (%s)", expiry_date, stock_name, eq_key)
                resp = await client.get(
                    OPTION_CHAIN_URL,
                    params={
                        "instrument_key": eq_key,
                        "expiry_date": expiry_date,
                    },
                    headers=self._auth_headers(),
                )
                if resp.status_code == 401:
                    self._clear_token()
                    raise RuntimeError(
                        "Upstox token expired or invalid — please login again via the dashboard."
                    )
                if resp.is_error:
                    last_error = ValueError(
                        f"Option chain API error {resp.status_code} for "
                        f"{stock_name} expiry={expiry_date}: {resp.text[:200]}"
                    )
                    logger.warning(str(last_error))
                    continue

                chain = resp.json()
                rows = chain.get("data", [])
                if not rows:
                    # Log full response to help diagnose why chain is empty
                    logger.warning(
                        "Empty option chain for %s expiry=%s | status=%s | response=%s",
                        stock_name, expiry_date, resp.status_code, resp.text[:500],
                    )
                if rows:
                    # Found a valid chain — proceed
                    spot = rows[0].get("underlying_spot_price", 0)
                    if spot == 0:
                        raise ValueError("underlying_spot_price missing from option chain response")

                    best_row = min(rows, key=lambda r: abs(r["strike_price"] - spot))
                    pe = best_row.get("put_options", {})
                    pe_key = pe.get("instrument_key", "")
                    pe_ltp = pe.get("market_data", {}).get("ltp", 0.0)
                    strike  = int(best_row["strike_price"])

                    # Build human-readable NSE option symbol: TECHM26SEP1540PE
                    exp = date.fromisoformat(expiry_date)
                    mon = exp.strftime("%b").upper()          # SEP
                    yy  = exp.strftime("%y")                  # 26
                    option_symbol = f"{stock_name.upper()}{yy}{mon}{strike}PE"

                    logger.info(
                        "ATM PE for %s (expiry=%s): %s key=%s ltp=%.2f (spot=%.2f)",
                        stock_name, expiry_date, option_symbol, pe_key, pe_ltp, spot,
                    )
                    return stock_name.upper(), pe_key, pe_ltp, option_symbol

                logger.warning(
                    "Empty option chain for %s expiry=%s — trying next expiry",
                    stock_name, expiry_date,
                )
                last_error = ValueError(
                    f"{stock_name} has no tradeable options for any of the tried "
                    f"expiry dates: {candidates}. Check if it is in the F&O segment."
                )

        raise last_error or ValueError(f"No option chain found for {stock_name}")

    # ── REST LTP fallback ────────────────────────────────────

    async def get_ltp(self, instrument_key: str) -> float:
        """Fetch the last traded price via REST (single instrument)."""
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                LTP_URL,
                params={"instrument_key": instrument_key},
                headers=self._auth_headers(),
            )
            if resp.status_code == 401:
                self._clear_token()
                raise RuntimeError(
                    "Upstox token expired or invalid — please login again via the dashboard."
                )
            resp.raise_for_status()
            data = resp.json()

        # The response nests under data → <instrument_key> → ltp
        quotes = data.get("data", {})
        for _key, quote in quotes.items():
            return float(quote.get("ltp", 0.0))

        raise ValueError(f"No LTP data returned for {instrument_key}")

    # ── WebSocket authorized URL ─────────────────────────────

    async def get_ws_url(self) -> str:
        """Get the single-use authorized WebSocket redirect URI."""
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                WS_AUTH_URL,
                headers=self._auth_headers(),
            )
            resp.raise_for_status()
            data = resp.json()

        ws_url = data["data"]["authorizedRedirectUri"]
        logger.info("WebSocket authorized URL obtained.")
        return ws_url


# ──────────────────────────────────────────────────────────────
# WebSocket — live LTP streaming
# ──────────────────────────────────────────────────────────────

# Type alias for the tick callback
TickCallback = Callable[[str, float, int], Awaitable[None] | None]


def _decode_protobuf(raw: bytes) -> dict:
    """Decode a protobuf-encoded FeedResponse into a plain dict.

    Falls back to JSON parsing if the protobuf module isn't available.
    """
    if _PROTO_AVAILABLE and PbFeedResponse is not None:
        feed = PbFeedResponse()
        feed.ParseFromString(raw)
        # Convert to dict manually for the fields we care about
        result: dict = {"type": feed.type, "feeds": {}, "currentTs": feed.currentTs}
        for key, f in feed.feeds.items():
            entry: dict = {}
            if f.HasField("ltpc"):
                entry["ltp"] = f.ltpc.ltp
                entry["ltt"] = f.ltpc.ltt
                entry["ltq"] = f.ltpc.ltq
                entry["cp"] = f.ltpc.cp
            elif f.HasField("fullFeed"):
                ff = f.fullFeed
                if ff.HasField("marketFF") and ff.marketFF.HasField("ltpc"):
                    entry["ltp"] = ff.marketFF.ltpc.ltp
                    entry["ltt"] = ff.marketFF.ltpc.ltt
                    entry["ltq"] = ff.marketFF.ltpc.ltq
                    entry["cp"] = ff.marketFF.ltpc.cp
                elif ff.HasField("indexFF") and ff.indexFF.HasField("ltpc"):
                    entry["ltp"] = ff.indexFF.ltpc.ltp
                    entry["ltt"] = ff.indexFF.ltpc.ltt
                    entry["ltq"] = ff.indexFF.ltpc.ltq
                    entry["cp"] = ff.indexFF.ltpc.cp
            elif f.HasField("firstLevelWithGreeks"):
                flg = f.firstLevelWithGreeks
                if flg.HasField("ltpc"):
                    entry["ltp"] = flg.ltpc.ltp
                    entry["ltt"] = flg.ltpc.ltt
            result["feeds"][key] = entry
        return result

    # Fallback: try JSON (Upstox may send JSON on certain error paths)
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        logger.warning("Could not decode message (protobuf module not loaded and not JSON)")
        return {}


async def stream_ltp(
    access_token: str,
    instrument_keys: list[str],
    callback: TickCallback,
    *,
    max_retries: int = 5,
    on_connect: Callable[[], Awaitable[None] | None] | None = None,
    on_disconnect: Callable[[], Awaitable[None] | None] | None = None,
    on_reconnect: Callable[[], Awaitable[None] | None] | None = None,
) -> None:
    """Connect to Upstox WebSocket V3 and stream LTP ticks.

    PAPER TRADING MODE — read-only market data streaming.

    Parameters
    ----------
    access_token : str
        Valid Upstox OAuth access token.
    instrument_keys : list[str]
        Instrument keys to subscribe (e.g. ``["NSE_FO|51060"]``).
    callback : TickCallback
        Called with ``(instrument_key, ltp, timestamp_ms)`` on each tick.
    max_retries : int
        Maximum reconnection attempts with exponential backoff.
    on_connect : callable, optional
        Called when the connection is established.
    on_disconnect : callable, optional
        Called when the connection drops.
    on_reconnect : callable, optional
        Called when the connection is re-established.
    """
    if websockets is None:
        raise RuntimeError("The 'websockets' package is required for live streaming. pip install websockets")

    retry_count = 0
    backoff = 1.0

    while retry_count <= max_retries:
        try:
            # Get a fresh single-use authorized WebSocket URL
            async with httpx.AsyncClient() as http:
                resp = await http.get(
                    WS_AUTH_URL,
                    headers={
                        "Authorization": f"Bearer {access_token}",
                        "Accept": "application/json",
                    },
                )
                resp.raise_for_status()
                ws_url = resp.json()["data"]["authorizedRedirectUri"]

            ssl_ctx = ssl.create_default_context()

            async with websockets.connect(ws_url, ssl=ssl_ctx) as ws:
                logger.info("WebSocket connected.")
                if on_connect:
                    result = on_connect()
                    if asyncio.iscoroutine(result):
                        await result
                if on_reconnect and retry_count > 0:
                    result = on_reconnect()
                    if asyncio.iscoroutine(result):
                        await result
                retry_count = 0  # reset on successful connection
                backoff = 1.0

                # Subscribe to instruments in ltpc mode
                sub_msg = json.dumps({
                    "guid": str(uuid.uuid4()),
                    "method": "sub",
                    "data": {
                        "mode": "ltpc",
                        "instrumentKeys": instrument_keys,
                    },
                })
                await ws.send(sub_msg.encode("utf-8"))
                logger.info(
                    "Subscribed to %d instrument(s): %s",
                    len(instrument_keys),
                    ", ".join(instrument_keys),
                )

                # Receive loop
                async for raw_msg in ws:
                    decoded = _decode_protobuf(raw_msg)
                    msg_type = decoded.get("type", -1)

                    # type 2 = market_info (first message) — skip
                    if msg_type == 2:
                        logger.debug("Received market_info message.")
                        continue

                    feeds = decoded.get("feeds", {})
                    ts = decoded.get("currentTs", 0)

                    if feeds:
                        logger.info(
                            "📡 WS FEED keys received: %s",
                            {k: v.get("ltp") for k, v in feeds.items()},
                        )

                    for inst_key, feed_data in feeds.items():
                        ltp = feed_data.get("ltp")
                        tick_ts = feed_data.get("ltt", ts)
                        if ltp is not None:
                            result = callback(inst_key, float(ltp), int(tick_ts))
                            if asyncio.iscoroutine(result):
                                await result

        except (
            asyncio.CancelledError,
            KeyboardInterrupt,
        ):
            logger.info("WebSocket stream cancelled.")
            raise

        except Exception as exc:
            retry_count += 1
            logger.warning(
                "WebSocket disconnected (%s). Retry %d/%d in %.1fs…",
                exc, retry_count, max_retries, backoff,
            )

            if on_disconnect:
                result = on_disconnect()
                if asyncio.iscoroutine(result):
                    await result

            if retry_count > max_retries:
                logger.error("Max WebSocket retries exceeded. Giving up.")
                raise

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)  # cap at 30s

    logger.error("WebSocket stream ended after exhausting retries.")


# ──────────────────────────────────────────────────────────────
# Convenience: quick CLI test
# ──────────────────────────────────────────────────────────────
async def _test_connection(token: str, instrument_key: str = "NSE_INDEX|Nifty 50") -> None:
    """Quick smoke test — connect and print a few ticks then exit."""
    tick_count = 0

    async def _print_tick(key: str, ltp: float, ts: int) -> None:
        nonlocal tick_count
        tick_count += 1
        print(f"  [{tick_count}] {key}  LTP={ltp:.2f}  ts={ts}")
        if tick_count >= 5:
            raise KeyboardInterrupt  # exit after 5 ticks

    print(f"Connecting to Upstox WebSocket for {instrument_key}…")
    try:
        await stream_ltp(token, [instrument_key], _print_tick, max_retries=1)
    except KeyboardInterrupt:
        print("✓ Test complete — received ticks successfully.")


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s  %(message)s")

    # ── PAPER TRADING MODE — no live orders will be placed ──
    logger.info("PAPER TRADING MODE — no live orders will be placed.")

    from config_loader import load_config
    cfg = load_config()

    client = UpstoxClient(cfg.upstox.api_key, cfg.upstox.api_secret, cfg.upstox.redirect_uri)

    if not client.is_authenticated:
        print("\n⚠  No access token found. Complete OAuth first:")
        print(f"   1. Open this URL in your browser:\n      {client.get_auth_url()}")
        print(f"   2. After login, you'll be redirected to {cfg.upstox.redirect_uri}?code=XXXX")
        print("   3. Run:  python upstox_client.py --code XXXX\n")

        if len(sys.argv) >= 3 and sys.argv[1] == "--code":
            code = sys.argv[2]
            token = asyncio.run(client.exchange_code(code))
            print(f"✓ Token acquired: {token[:12]}…")
        else:
            sys.exit(1)

    if len(sys.argv) >= 2 and sys.argv[1] == "--test":
        instrument = sys.argv[2] if len(sys.argv) >= 3 else "NSE_INDEX|Nifty 50"
        asyncio.run(_test_connection(client.access_token, instrument))
    elif len(sys.argv) >= 2 and sys.argv[1] == "--atm-pe":
        stock = sys.argv[2] if len(sys.argv) >= 3 else "RELIANCE"

        async def _show_atm():
            name, key, ltp = await client.get_atm_pe(stock)
            print(f"ATM PE for {name}: {key}  LTP={ltp:.2f}")

        asyncio.run(_show_atm())
