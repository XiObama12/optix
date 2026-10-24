#!/usr/bin/env python3
"""Longbridge data fetcher for Optix.

Mirrors the CLI contract of ../yfinance/fetcher.py so the Go adapter can use
either interchangeably.

    fetcher.py <quote|bars|option_chain> <SYMBOL> [args...]

Data comes from the Longbridge MCP endpoint (https://mcp.longbridge.com) via
JSON-RPC over HTTPS. That endpoint is reached through an HTTP proxy on this
machine (see LONG_BRIDGE_PROXY below), and authenticates with a bearer token
read from LONG_BRIDGE_TOKEN_FILE.

Why MCP and not the `longport` SDK: the SDK requires an app_key/app_secret
pair to sign requests, which this machine does not have. The MCP endpoint
authenticates with a single JWT that is already present and working.
"""

import json
import os
import sys
import urllib.request
import urllib.error
from datetime import datetime, timezone

MCP_URL = "https://mcp.longbridge.com"
TOKEN_FILE = os.environ.get(
    "LONG_BRIDGE_TOKEN_FILE",
    "/Users/qin/YianTradingAgents/cli/.lb_token",
)
PROXY = os.environ.get("LONG_BRIDGE_PROXY", "http://127.0.0.1:7897")
TIMEOUT = int(os.environ.get("LONG_BRIDGE_TIMEOUT", "30"))

# Optix timeframe token -> Longbridge candle period
PERIOD_MAP = {
    "1m": "MINUTE",
    "5m": "FIVE_MINUTES",
    "15m": "FIFTEEN_MINUTES",
    "30m": "THIRTY_MINUTES",
    "60m": "HOUR",
    "1h": "HOUR",
    "1d": "DAY",
    "1w": "WEEK",
    "1wk": "WEEK",
}


def _token() -> str:
    try:
        with open(TOKEN_FILE) as fh:
            return fh.read().strip()
    except OSError as exc:
        raise RuntimeError(
            f"Longbridge token not readable at {TOKEN_FILE}: {exc}. "
            "Set LONG_BRIDGE_TOKEN_FILE to point at a valid token."
        )


def _mcp(tool: str, args: dict):
    """Call one Longbridge MCP tool and return its decoded payload."""
    payload = json.dumps({
        "jsonrpc": "2.0",
        "method": "tools/call",
        "id": 1,
        "params": {"name": tool, "arguments": args},
    }).encode()

    req = urllib.request.Request(
        MCP_URL,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "Authorization": f"Bearer {_token()}",
        },
        method="POST",
    )

    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({"http": PROXY, "https": PROXY})
    )

    try:
        with opener.open(req, timeout=TIMEOUT) as resp:
            raw = resp.read().decode().strip()
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"MCP HTTP {exc.code} calling {tool}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(
            f"MCP unreachable calling {tool} (proxy {PROXY}): {exc.reason}"
        ) from exc

    if not raw:
        raise RuntimeError(f"MCP returned empty response for {tool}")
    if raw.startswith("data:"):
        raw = raw.split("data:", 1)[1].strip()

    doc = json.loads(raw)
    if "error" in doc:
        raise RuntimeError(f"MCP error for {tool}: {doc['error']}")

    content = doc.get("result", {}).get("content", [])
    if not content:
        raise RuntimeError(f"MCP returned no content for {tool}")

    text = content[0].get("text", "")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"_text": text}


def _qualify(symbol: str) -> str:
    """Optix passes bare tickers; Longbridge wants SYMBOL.MARKET."""
    s = symbol.strip().upper()
    return s if "." in s else f"{s}.US"


def _f(v, default=0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


def _i(v, default=0) -> int:
    try:
        if v is None or v == "":
            return default
        return int(float(v))
    except (TypeError, ValueError):
        return default


def fetch_quote(symbol: str) -> dict:
    rows = _mcp("quote", {"symbols": [_qualify(symbol)]})
    if not isinstance(rows, list) or not rows:
        raise RuntimeError(f"no quote returned for {symbol}")
    q = rows[0]

    last = _f(q.get("last_done"))
    prev_close = _f(q.get("prev_close"))
    change = last - prev_close if last and prev_close else 0.0
    change_pct = (change / prev_close * 100) if prev_close else 0.0

    return {
        "symbol": symbol.upper(),
        "last": last,
        "bid": 0.0,
        "ask": 0.0,
        "volume": _i(q.get("volume")),
        "change": round(change, 4),
        "changePct": round(change_pct, 4),
        "high": _f(q.get("high")),
        "low": _f(q.get("low")),
        "open": _f(q.get("open")),
        "close": prev_close,
        "high52w": 0.0,
        "low52w": 0.0,
        "avgVolume": 0.0,
        "timestamp": q.get("timestamp") or datetime.now(timezone.utc).isoformat(),
    }


def fetch_bars(symbol: str, timeframe: str, days: int) -> list:
    period = PERIOD_MAP.get(timeframe.lower(), "DAY")

    # Rough bar count from the requested window.
    per_day = {"MINUTE": 390, "FIVE_MINUTES": 78, "FIFTEEN_MINUTES": 26,
               "THIRTY_MINUTES": 13, "HOUR": 7}.get(period, 1)
    count = max(1, min(int(days * per_day), 1000))

    rows = _mcp("candlesticks", {
        "symbol": _qualify(symbol),
        "period": period,
        "count": count,
    })
    if not isinstance(rows, list):
        return []

    bars = []
    for r in rows:
        bars.append({
            "timestamp": r.get("timestamp"),
            "open": round(_f(r.get("open")), 4),
            "high": round(_f(r.get("high")), 4),
            "low": round(_f(r.get("low")), 4),
            "close": round(_f(r.get("close")), 4),
            "volume": _i(r.get("volume")),
        })
    return bars


def fetch_option_chain(symbol: str, expiration: str = "") -> dict:
    """Return {"underlying": ..., "expirations": [{expiration, calls[], puts[]}]}.

    `expiration` accepts YYYYMMDD (Optix's normalised form) or YYYY-MM-DD.
    Empty means the nearest expiry.
    """
    underlying = _qualify(symbol)

    dates = _mcp("option_chain_expiry_date_list", {"symbol": underlying})
    if not isinstance(dates, list):
        return {"underlying": symbol.upper(), "expirations": []}

    # The endpoint returns past expiries too; drop anything before today.
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    future = [d for d in dates if d >= today] or dates

    if expiration:
        want = expiration.replace("-", "")
        # Longbridge returns YYYY-MM-DD
        match = [d for d in future if d.replace("-", "") == want]
        target = match[0] if match else future[0]
    else:
        target = future[0]

    raw = _mcp("option_chain_info_by_date", {"symbol": underlying, "date": target})
    if not isinstance(raw, list):
        return {"underlying": symbol.upper(), "expirations": []}

    # Collect contract symbols, then batch-quote them for bid/ask/OI/IV.
    call_syms, put_syms, strikes = [], [], []
    for s in raw:
        cs, ps = s.get("call_symbol"), s.get("put_symbol")
        if cs and ps:
            call_syms.append(cs)
            put_syms.append(ps)
            strikes.append(_f(s.get("price")))

    quotes = {}
    batch = call_syms + put_syms
    for i in range(0, len(batch), 200):
        q = _mcp("option_quote", {"symbols": batch[i:i + 200]})
        if isinstance(q, list):
            for c in q:
                quotes[c.get("symbol")] = c

    # option_quote carries no bid/ask, so pull the book separately — but only
    # for the strikes near the money, since each `depth` call is one round trip.
    spot = _f((call_syms and quotes.get(call_syms[0], {}).get("underlying_symbol")) or 0)
    if not spot:
        try:
            spot = fetch_quote(symbol)["last"]
        except Exception:
            spot = 0.0

    near = int(os.environ.get("LONG_BRIDGE_DEPTH_STRIKES", "20"))
    books = {}
    if spot > 0:
        from concurrent.futures import ThreadPoolExecutor

        by_dist = sorted(range(len(strikes)), key=lambda i: abs(strikes[i] - spot))
        targets = []
        for idx in by_dist[:near]:
            targets += [call_syms[idx], put_syms[idx]]

        def one(sym):
            try:
                d = _mcp("depth", {"symbol": sym})
                bids, asks = d.get("bids", []), d.get("asks", [])
                return sym, (
                    _f(bids[0]["price"]) if bids else 0.0,
                    _f(asks[0]["price"]) if asks else 0.0,
                )
            except Exception:
                return sym, (0.0, 0.0)

        # Parallel: each depth call is an independent round trip.
        with ThreadPoolExecutor(max_workers=8) as pool:
            for sym, b in pool.map(one, targets):
                books[sym] = b

    def row(contract_sym, strike):
        c = quotes.get(contract_sym, {})
        bid, ask = books.get(contract_sym, (0.0, 0.0))
        return {
            "strike": strike,
            "bid": bid,
            "ask": ask,
            "last": _f(c.get("last_done")),
            "volume": _i(c.get("volume")),
            "openInterest": _i(c.get("open_interest")),
            "iv": _f(c.get("implied_volatility")),
            "delta": _f(c.get("delta")),
            "gamma": _f(c.get("gamma")),
            "theta": _f(c.get("theta")),
            "vega": _f(c.get("vega")),
        }

    calls = [row(cs, k) for cs, k in zip(call_syms, strikes)]
    puts = [row(ps, k) for ps, k in zip(put_syms, strikes)]

    return {
        "underlying": symbol.upper(),
        "expirations": [{
            "expiration": target.replace("-", ""),
            "calls": calls,
            "puts": puts,
        }],
    }


def main():
    if len(sys.argv) < 3:
        print(
            "Usage: fetcher.py <quote|bars|option_chain> <SYMBOL> [args...]",
            file=sys.stderr,
        )
        sys.exit(1)

    command = sys.argv[1]
    symbol = sys.argv[2].upper()

    try:
        if command == "quote":
            result = fetch_quote(symbol)
        elif command == "bars":
            timeframe = sys.argv[3] if len(sys.argv) > 3 else "1d"
            days = int(sys.argv[4]) if len(sys.argv) > 4 else 365
            result = fetch_bars(symbol, timeframe, days)
        elif command == "option_chain":
            expiration = sys.argv[3] if len(sys.argv) > 3 else ""
            result = fetch_option_chain(symbol, expiration)
        else:
            print(f"Unknown command: {command}", file=sys.stderr)
            sys.exit(1)

        json.dump(result, sys.stdout)
    except Exception as exc:
        print(f"Error fetching {command} for {symbol}: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
