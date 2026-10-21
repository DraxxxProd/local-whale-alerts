"""Alchemy EVM Transfers API -> Discord. Python 3.12+, standard library only.

Coverage is the intersection of Alchemy's published mainnets, networks enabled
for the app key, Transfers API support, and CoinGecko's contract registry.
Non-EVM chains and CEX order books are NOT covered. See README.md.
"""
from __future__ import annotations

import argparse
import collections
import html
import json
import os
from pathlib import Path
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal
from html.parser import HTMLParser


STABLE_USD = Decimal(os.getenv("STABLE_MIN_USD", "100000000"))
ETH_USD = Decimal(os.getenv("ETH_MIN_USD", "25000000"))
ROOT = Path(os.getenv("MONITOR_DATA_DIR", ".whale-data"))
STATE_FILE = ROOT / "state.json"
CATALOG_FILE = ROOT / "catalog.json"
COVERAGE_FILE = ROOT / "coverage.json"
DEADLINE_SECONDS = int(os.getenv("MAX_RUN_SECONDS", "1000"))
CHUNK_BLOCKS = int(os.getenv("CHUNK_BLOCKS", "40"))
BOOTSTRAP_BLOCKS = int(os.getenv("BOOTSTRAP_BLOCKS", "30"))
OVERLAP_BLOCKS = int(os.getenv("OVERLAP_BLOCKS", "12"))
MAX_CALLS = int(os.getenv("MAX_RPC_CALLS", "2500"))
MAINNET_DOC = "https://www.alchemy.com/docs/reference/node-supported-chains"
CG = "https://api.coingecko.com/api/v3"
ZERO = "0x" + "0" * 40
INTERNAL_NETWORKS = {"eth-mainnet", "polygon-mainnet", "base-mainnet", "arc-mainnet"}
API_KEY = os.getenv("ALCHEMY_API_KEY", "")
WEBHOOK = os.getenv("DISCORD_WEBHOOK", "")
DRY_RUN = False
calls = 0
deadline = float("inf")


class MonitorError(RuntimeError):
    pass


class BudgetReached(MonitorError):
    pass


def read_json(path, default=None):
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
    temporary.replace(path)


def request(url, payload=None, headers=None, method=None, attempts=4, json_response=True):
    """Never include URLs, API responses, or urllib exceptions in errors."""
    body = None if payload is None else json.dumps(payload).encode()
    request_headers = {"User-Agent": "WhaleMonitor/1.0", "Accept": "application/json"}
    if body is not None:
        request_headers["Content-Type"] = "application/json"
    request_headers.update(headers or {})
    for attempt in range(attempts):
        if time.monotonic() >= deadline:
            raise BudgetReached("Run time budget reached")
        req = urllib.request.Request(url, data=body, headers=request_headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=40) as response:
                data = response.read()
                return json.loads(data) if json_response and data else data
        except urllib.error.HTTPError as error:
            status = error.code
            wait = error.headers.get("Retry-After", "")
            try:
                wait = min(30, max(1, float(wait)))
            except ValueError:
                wait = 2 ** attempt
            if status not in (429, 500, 502, 503, 504) or attempt == attempts - 1:
                raise MonitorError(f"HTTP {status}") from None
            time.sleep(wait)
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError):
            if attempt == attempts - 1:
                raise MonitorError("Network request failed") from None
            time.sleep(2 ** attempt)
        except (ValueError, UnicodeError):
            raise MonitorError("Invalid JSON response") from None
    raise MonitorError("Request failed")


def rpc(network, method, params):
    global calls
    if calls >= MAX_CALLS or time.monotonic() >= deadline:
        raise BudgetReached("Run budget reached; remaining blocks kept for next run")
    calls += 1
    # Also keep aggregate throughput below Free plan's 300 CU/s.
    time.sleep(0.5)
    result = request(
        f"https://{network}.g.alchemy.com/v2/{urllib.parse.quote(API_KEY, safe='')}",
        {"jsonrpc": "2.0", "id": calls, "method": method, "params": params},
    )
    if "error" in result:
        code = result["error"].get("code", "unknown")
        raise MonitorError(f"RPC {method} failed (code {code})")
    if "result" not in result or result["result"] is None:
        raise MonitorError(f"RPC {method} returned no result")
    return result["result"]


def cg(path, params=None):
    key = os.getenv("COINGECKO_DEMO_API_KEY")
    headers = {"x-cg-demo-api-key": key} if key else {}
    query = "?" + urllib.parse.urlencode(params) if params else ""
    # CoinGecko public access is independently rate-limited.
    time.sleep(2)
    return request(CG + path + query, headers=headers)


class TableParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.rows = []
        self.row = None
        self.cell = None

    def handle_starttag(self, tag, attrs):
        if tag == "tr":
            self.row = []
        elif tag in ("td", "th") and self.row is not None:
            self.cell = []

    def handle_data(self, value):
        if self.cell is not None:
            self.cell.append(value)

    def handle_endtag(self, tag):
        if tag in ("td", "th") and self.cell is not None:
            self.row.append("".join(self.cell))
            self.cell = None
        elif tag == "tr" and self.row is not None:
            self.rows.append(self.row)
            self.row = None


def published_networks(document):
    parser = TableParser()
    parser.feed(document)
    networks = set()
    for row in parser.rows:
        text = " ".join(row)
        if "mainnet" not in text.lower():
            continue
        networks.update(re.findall(r"https://([a-z0-9-]+)\.g\.alchemy\.com/v2/", text))
    if "eth-mainnet" not in networks:
        raise MonitorError("Could not parse Alchemy mainnet catalogue; refusing a partial default list")
    return sorted(networks)


def refresh_catalog(state):
    cached = read_json(CATALOG_FILE)
    now = int(time.time())
    if cached and now - cached["updated_at"] < 86400:
        return cached
    document = request(MAINNET_DOC, json_response=False).decode("utf-8")
    networks = published_networks(html.unescape(document))
    platforms = cg("/asset_platforms")
    by_chain = {int(p["chain_identifier"]): p for p in platforms if p.get("chain_identifier")}
    coins = cg("/coins/list", {"include_platform": "true"})
    markets = []
    for page in range(1, 30):
        rows = cg("/coins/markets", {
            "vs_currency": "usd", "category": "stablecoins", "per_page": 250, "page": page,
        })
        if not isinstance(rows, list):
            raise MonitorError("Invalid stablecoin catalogue")
        markets.extend(rows)
        if len(rows) < 250:
            break
    else:
        raise MonitorError("Stablecoin catalogue pagination exceeded safety bound")
