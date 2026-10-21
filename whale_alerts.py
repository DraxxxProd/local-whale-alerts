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
    stable_ids = {row["id"] for row in markets}
    if not {"tether", "usd-coin"}.issubset(stable_ids):
        raise MonitorError("Incomplete stablecoin catalogue")
    tokens_by_platform = collections.defaultdict(dict)
    for coin in coins:
        kind = "stable" if coin["id"] in stable_ids else "eth" if coin["id"] == "weth" else None
        if kind is None:
            continue
        for platform, address in (coin.get("platforms") or {}).items():
            if isinstance(address, str) and re.fullmatch(r"0x[0-9a-fA-F]{40}", address):
                tokens_by_platform[platform][address.lower()] = {
                    "id": coin["id"], "symbol": coin["symbol"].upper(), "kind": kind,
                }
    enabled = []
    excluded = []
    for network in networks:
        try:
            chain_id = int(rpc(network, "eth_chainId", []), 16)
            platform = by_chain.get(chain_id)
            if not platform:
                raise MonitorError("No CoinGecko platform/contract registry for chain ID")
            tokens = tokens_by_platform.get(platform["id"], {})
            if not tokens and platform.get("native_coin_id") != "ethereum":
                raise MonitorError("No registered stablecoin or WETH contracts")
            # Probe the actual method, not merely whether an RPC hostname exists.
            height = int(rpc(network, "eth_blockNumber", []), 16)
            rpc(network, "alchemy_getAssetTransfers", [{
                "fromBlock": hex(max(0, height - 2)), "toBlock": hex(max(0, height - 2)),
                "category": ["erc20"], "maxCount": "0x1",
            }])
            enabled.append({
                "network": network, "chain_id": chain_id,
                "platform": platform["id"], "native_eth": platform.get("native_coin_id") == "ethereum",
                "internal": network in INTERNAL_NETWORKS,
                "tokens": tokens,
                "explorer": f"https://blockscan.com/tx/",
            })
        except BudgetReached:
            raise
        except MonitorError as error:
            excluded.append({"network": network, "reason": str(error)})
    if not enabled:
        raise MonitorError("No accessible Transfers API mainnets discovered")
    catalog = {"updated_at": now, "networks": enabled, "excluded": excluded}
    write_json(CATALOG_FILE, catalog)
    return catalog


def get_prices(catalog):
    addresses = collections.defaultdict(list)
    for chain in catalog["networks"]:
        for address in chain["tokens"]:
            addresses[chain["network"]].append(address)
    prices = {}
    # Resolve prices by contract, not symbol. No stablecoin is assumed to equal $1.
    batch = [{"network": net, "address": addr} for net, items in addresses.items() for addr in items]
    for offset in range(0, len(batch), 25):
        try:
            reply = request(
                "https://api.g.alchemy.com/prices/v1/"
                + urllib.parse.quote(API_KEY, safe="") + "/tokens/by-address",
                {"addresses": batch[offset:offset + 25]},
            )
        except BudgetReached:
            raise
        except MonitorError:
            # Isolate unsupported price networks/contracts instead of dropping
            # prices for every otherwise valid item in a rejected batch.
            rows = []
            for candidate in batch[offset:offset + 25]:
                try:
                    single = request(
                        "https://api.g.alchemy.com/prices/v1/"
                        + urllib.parse.quote(API_KEY, safe="") + "/tokens/by-address",
                        {"addresses": [candidate]}, attempts=2,
                    )
                    rows.extend(single.get("data", []))
                except BudgetReached:
                    raise
                except MonitorError:
                    pass
                time.sleep(0.5)
            reply = {"data": rows}
        for item in reply.get("data", []):
            for price in item.get("prices", []):
                if price.get("currency", "").lower() == "usd":
                    value = Decimal(str(price["value"]))
                    stamp = datetime.fromisoformat(price["lastUpdatedAt"].replace("Z", "+00:00"))
                    if value.is_finite() and value > 0 and abs(time.time() - stamp.timestamp()) < 3600:
                        prices[(item["network"], item["address"].lower())] = value
        time.sleep(0.5)
    # Native ETH price by symbol. WETH still uses its own contract price above.
    response = request(
        "https://api.g.alchemy.com/prices/v1/" + urllib.parse.quote(API_KEY, safe="")
        + "/tokens/by-symbol?symbols=ETH"
    )
    for item in response.get("data", []):
        if item.get("symbol") == "ETH":
            for price in item.get("prices", []):
                if price.get("currency", "").lower() == "usd":
                    value = Decimal(str(price["value"]))
                    stamp = datetime.fromisoformat(price["lastUpdatedAt"].replace("Z", "+00:00"))
                    if value.is_finite() and value > 0 and abs(time.time() - stamp.timestamp()) < 3600:
                        prices[("native", "ETH")] = value
    return prices


def transfers(network, start, end, categories, contracts=None):
    params = {
        "fromBlock": hex(start), "toBlock": hex(end), "category": categories,
        "excludeZeroValue": True, "withMetadata": True, "order": "asc", "maxCount": "0x3e8",
    }
    if contracts:
        params["contractAddresses"] = contracts
    page_keys = set()
    results = []
    while True:
        reply = rpc(network, "alchemy_getAssetTransfers", [params])
        results.extend(reply.get("transfers", []))
        key = reply.get("pageKey")
        if not key:
            return results
        if key in page_keys:
            raise MonitorError("Repeated Transfers API pageKey")
        page_keys.add(key)
        params["pageKey"] = key


def normalize(row, chain, prices):
    raw = row.get("rawContract") or {}
    address = (raw.get("address") or "").lower()
    if row.get("category") in ("external", "internal"):
        if not chain["native_eth"] or row.get("asset") != "ETH":
            return None
        token = {"symbol": "ETH", "kind": "eth"}
        price = prices.get(("native", "ETH"))
        identity = "native:ETH"
    else:
        token = chain["tokens"].get(address)
        if not token:
            return None
        price = prices.get((chain["network"], address))
        identity = address
    if price is None:
        return None  # Recorded explicitly in coverage.json before scanning.
    raw_value = raw.get("value")
    decimals = raw.get("decimal")
    if raw_value is not None and decimals is not None:
        exponent = int(decimals, 16) if isinstance(decimals, str) else int(decimals)
        amount = Decimal(int(raw_value, 16)) / (Decimal(10) ** exponent)
    elif row.get("value") is not None:
        # Alchemy internal transfers may expose only a decimal quantity.
        amount = Decimal(str(row["value"]))
    else:
        raise MonitorError("Transfer is missing amount; cursor was not advanced")
    if not amount.is_finite() or amount < 0:
        raise MonitorError("Invalid transfer amount")
    return {
        "tx": row["hash"], "block": int(row["blockNum"], 16),
        "id": row.get("uniqueId"), "asset": identity, "symbol": token["symbol"], "kind": token["kind"],
        "amount": amount, "usd": amount * price, "price": price,
        "from": (row.get("from") or ZERO).lower(), "to": (row.get("to") or ZERO).lower(),
        "time": (row.get("metadata") or {}).get("blockTimestamp"),
    }


def aggregate(rows):
    """One alert per tx+asset. Do not add both sides or routed hops as volume.

    Threshold basis: larger of the largest individual transfer and largest
    absolute non-zero-address net movement. This also catches split routes
    where one sender pays multiple pools within the SAME transaction.
    """
    groups = collections.defaultdict(list)
    for row in rows:
        if row and row["from"] != row["to"] and row["amount"] > 0:
            groups[(row["tx"], row["asset"])].append(row)
    events = []
    for (tx, asset), legs in groups.items():
        net = collections.defaultdict(Decimal)
        for leg in legs:
            if leg["from"] != ZERO:
                net[leg["from"]] -= leg["amount"]
            if leg["to"] != ZERO:
                net[leg["to"]] += leg["amount"]
        largest = max(legs, key=lambda leg: leg["amount"])
        amount = max(largest["amount"], max((abs(value) for value in net.values()), default=Decimal(0)))
        usd = amount * largest["price"]
        threshold = STABLE_USD if largest["kind"] == "stable" else ETH_USD
        if usd >= threshold:
            events.append({**largest, "amount": amount, "usd": usd, "legs": len(legs), "tx": tx, "asset": asset})
    return sorted(events, key=lambda event: (event["block"], event["tx"], event["asset"]))


def keccak256(data):
    """Legacy Keccak-256 (Ethereum), NOT FIPS SHA3-256. Pure Python."""
    rotations = (
        (0, 36, 3, 41, 18), (1, 44, 10, 45, 2), (62, 6, 43, 15, 61),
        (28, 55, 25, 21, 56), (27, 20, 39, 8, 14),
    )
    constants = (
        0x1, 0x8082, 0x800000000000808A, 0x8000000080008000,
        0x808B, 0x80000001, 0x8000000080008081, 0x8000000000008009,
        0x8A, 0x88, 0x80008009, 0x8000000A, 0x8000808B,
        0x800000000000008B, 0x8000000000008089, 0x8000000000008003,
        0x8000000000008002, 0x8000000000000080, 0x800A,
        0x800000008000000A, 0x8000000080008081, 0x8000000000008080,
        0x80000001, 0x8000000080008008,
    )
    mask = (1 << 64) - 1
    def rotate(value, shift):
        return ((value << shift) | (value >> ((64 - shift) % 64))) & mask
    padding = 136 - len(data) % 136
    data += bytes([0x81]) if padding == 1 else b"\x01" + bytes(padding - 2) + b"\x80"
    state = [0] * 25
    for offset in range(0, len(data), 136):
        for i in range(17):
            state[i] ^= int.from_bytes(data[offset + i * 8:offset + i * 8 + 8], "little")
        for rc in constants:
            columns = [state[x] ^ state[x + 5] ^ state[x + 10] ^ state[x + 15] ^ state[x + 20] for x in range(5)]
            differences = [columns[(x - 1) % 5] ^ rotate(columns[(x + 1) % 5], 1) for x in range(5)]
            for x in range(5):
                for y in range(5):
                    state[x + 5 * y] ^= differences[x]
            changed = [0] * 25
            for x in range(5):
                for y in range(5):
                    changed[y + 5 * ((2 * x + 3 * y) % 5)] = rotate(state[x + 5 * y], rotations[x][y])
            for x in range(5):
                for y in range(5):
                    state[x + 5 * y] = changed[x + 5 * y] ^ ((~changed[(x + 1) % 5 + 5 * y]) & changed[(x + 2) % 5 + 5 * y])
            state[0] ^= rc
    return b"".join(word.to_bytes(8, "little") for word in state)[:32].hex()


def signature_topics():
    signatures = {
        "Uniswap V2 compatible": "Swap(address,uint256,uint256,uint256,uint256,address)",
        "Uniswap V3 compatible": "Swap(address,address,int256,int256,uint160,uint128,int24)",
        "Uniswap V4 compatible": "Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24)",
        "Curve compatible": "TokenExchange(address,int128,uint256,int128,uint256)",
        "Curve NG compatible": "TokenExchange(address,uint256,uint256,uint256,uint256)",
        "Balancer compatible": "Swap(bytes32,address,address,uint256,uint256)",
    }
    return {"0x" + keccak256(signature.encode()): label for label, signature in signatures.items()}


def receipt_info(network, tx, topics):
    receipt = rpc(network, "eth_getTransactionReceipt", [tx])
    if int(receipt.get("status", "0x0"), 16) != 1:
        return False, []
    labels = sorted({topics[log["topics"][0].lower()] for log in receipt.get("logs", [])
                     if log.get("topics") and log["topics"][0].lower() in topics})
    return True, labels


def send_discord(chain, event, swap_labels):
    signal = "Есть событие Swap" if swap_labels else "Крупное движение средств"
    details = (
        f"**{event['symbol']} · {event['amount']:,.4f}**\n"
        f"Оценка: **${event['usd']:,.0f}**\n"
        f"Сеть: `{chain['network']}`\n"
        f"Крупнейший отдельный перевод:\n"
        f"От: `{event['from']}`\nКому: `{event['to']}`\n"
        f"Переводов актива в транзакции: {event['legs']}\n"
        "Объём: max(один перевод, чистое движение по адресу).\n"
    )
    if swap_labels:
        details += "Сигнатуры: " + ", ".join(swap_labels) + "\n"
        details += "Событие Swap найдено в транзакции; это не подтверждение объёма сделки.\n"
    details += f"[Открыть транзакцию](https://blockscan.com/tx/{event['tx']})"
    embed = {
        "title": "🐋 " + signal,
        "description": details,
        "color": 15844367 if swap_labels else 3447003,
        "footer": {"text": "Alchemy • USD по свежей котировке на момент проверки • не торговый сигнал"},
    }
    if event.get("time"):
        embed["timestamp"] = event["time"]
    payload = {"allowed_mentions": {"parse": []}, "embeds": [embed]}
    if DRY_RUN:
        print(json.dumps(payload, ensure_ascii=False))
        return
    url = WEBHOOK + ("&" if "?" in WEBHOOK else "?") + "wait=true"
    # A timeout after Discord accepted the request has an ambiguous result.
    # Do not blindly retry POSTs: rerunning may still produce a duplicate.
    request(url, payload, attempts=1)


def scan_chain(chain, prices, state, topics):
    network = chain["network"]
    head = int(rpc(network, "eth_blockNumber", []), 16)
    # 'indexed' is a Transfers API tag, NOT a standard eth_getBlockByNumber tag.
    indexed = rpc(network, "alchemy_getAssetTransfers", [{
        "fromBlock": "0x0", "toBlock": "indexed", "category": ["external", "erc20"],
        "order": "desc", "maxCount": "0x1", "excludeZeroValue": True,
    }])
    indexed_rows = indexed.get("transfers", [])
    if not indexed_rows:
        raise MonitorError("No indexed transfers; cursor left unchanged")
    safe = rpc(network, "eth_getBlockByNumber", ["safe", False])
    if not isinstance(safe, dict) or "number" not in safe:
        raise MonitorError("Safe block tag unavailable; cursor left unchanged")
    # Stop one block BEFORE the last observed indexed transfer, for a stable
    # lower bound; also replay recent blocks to tolerate indexing corrections.
    end = min(int(indexed_rows[0]["blockNum"], 16) - 1, int(safe["number"], 16), head - 12)
    cursors = state.setdefault("cursors", {})
    if network not in cursors:
        print(f"::warning::{network}: first run starts at last {BOOTSTRAP_BLOCKS} confirmed blocks; older history is not scanned")
        cursors[network] = max(0, end - BOOTSTRAP_BLOCKS)
    previous = int(cursors[network])
    start = max(0, previous - OVERLAP_BLOCKS + 1)
    sent = state.setdefault("sent", {})
    addresses = sorted(chain["tokens"])
    while start <= end:
        stop = min(end, start + CHUNK_BLOCKS - 1)
        rows = []
        if addresses:
            for offset in range(0, len(addresses), 100):
                rows.extend(transfers(network, start, stop, ["erc20"], addresses[offset:offset + 100]))
        if chain["native_eth"]:
            categories = ["external"] + (["internal"] if chain["internal"] else [])
            rows.extend(transfers(network, start, stop, categories))
        normalized = []
        seen = set()
        for row in rows:
            identity = row.get("uniqueId")
            if not identity:
                raise MonitorError("Missing unique transfer ID; refusing unsafe deduplication")
            if identity not in seen:
                seen.add(identity)
                normalized.append(normalize(row, chain, prices))
        receipts = {}
        for event in aggregate(normalized):
            identity = f"{network}:{event['tx']}:{event['asset']}"
            if identity in sent:
                continue
            if event["tx"] not in receipts:
                receipts[event["tx"]] = receipt_info(network, event["tx"], topics)
            success, labels = receipts[event["tx"]]
            if not success:
                continue
            send_discord(chain, event, labels)
            if not DRY_RUN:
                sent[identity] = int(time.time())
                write_json(STATE_FILE, state)
        if not DRY_RUN:
            cursors[network] = max(previous, stop)
            previous = cursors[network]
            write_json(STATE_FILE, state)
        start = stop + 1
    print(f"{network}: scanned through block {end}")


def github_headers():
    token = os.getenv("GITHUB_TOKEN")
    repo = os.getenv("GITHUB_REPOSITORY")
    if not token or not repo:
        raise MonitorError("GitHub state needs GITHUB_TOKEN and GITHUB_REPOSITORY")
    return repo, {"Authorization": f"Bearer {token}", "X-GitHub-Api-Version": "2022-11-28"}


def github_state(mode):
    """Persist state on a dedicated branch; do not modify the default branch."""
    import base64
    repo, headers = github_headers()
    base = f"https://api.github.com/repos/{repo}"
    branch = "whale-alert-state"
    endpoint = base + "/contents/whale_state.json"
    if mode == "restore":
        try:
            data = request(endpoint + "?ref=" + branch, headers=headers)
        except MonitorError as error:
            if str(error) != "HTTP 404":
                raise
            if os.getenv("REQUIRE_EXISTING_STATE", "false").lower() == "true":
                raise MonitorError("Persistent state missing; restore before restarting") from None
            print("::warning::No saved state branch; first run will bootstrap recent blocks")
            # Verify write permission BEFORE the first notification. Otherwise a
            # read-only token could repeatedly notify with no durable checkpoint.
            if os.getenv("MONITOR_DRY_RUN", "false").lower() != "true":
                write_json(STATE_FILE, {"version": 1, "cursors": {}, "sent": {}})
                github_state("save")
            return
        write_json(STATE_FILE, json.loads(base64.b64decode(data["content"])))
        return
    if not STATE_FILE.exists() or DRY_RUN:
        return
    try:
        existing = request(endpoint + "?ref=" + branch, headers=headers)
    except MonitorError as error:
        if str(error) != "HTTP 404":
            raise
        # Create the branch from the current checked out revision.
        try:
            request(base + "/git/refs", {
                "ref": "refs/heads/" + branch, "sha": os.environ["GITHUB_SHA"],
            }, headers=headers)
        except MonitorError as ref_error:
            if str(ref_error) != "HTTP 422":
                raise
        existing = None
    content = STATE_FILE.read_bytes()
    if existing and base64.b64decode(existing["content"]) == content:
        return
    payload = {
        "message": "Update whale monitor checkpoint [skip ci]", "branch": branch,
        "content": base64.b64encode(content).decode(),
    }
    if existing:
        payload["sha"] = existing["sha"]
    request(endpoint, payload, headers=headers, method="PUT")


def main():
    global DRY_RUN, deadline
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--restore-state", action="store_true")
    parser.add_argument("--save-state", action="store_true")
    args = parser.parse_args()
    DRY_RUN = args.dry_run
    ROOT.mkdir(parents=True, exist_ok=True)
    if args.restore_state or args.save_state:
        github_state("restore" if args.restore_state else "save")
        return
    if not API_KEY or (not WEBHOOK and not DRY_RUN):
        raise MonitorError("Set ALCHEMY_API_KEY and DISCORD_WEBHOOK")
    deadline = time.monotonic() + DEADLINE_SECONDS
    state = read_json(STATE_FILE, {"version": 1, "cursors": {}, "sent": {}})
    if state.get("version") != 1:
        raise MonitorError("Unknown checkpoint version")
    catalog = refresh_catalog(state)
    selected = set(filter(None, os.getenv("NETWORKS", "").split(",")))
    chains = [chain for chain in catalog["networks"] if not selected or chain["network"] in selected]
    if not chains:
        raise MonitorError("No matching networks")
    prices = get_prices(catalog)
    topics = signature_topics()
    coverage = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "excluded": catalog["excluded"], "chains": [],
        "scope": "EVM Transfers API + CoinGecko stablecoin/WETH registry; not all Alchemy chains",
        "limitations": [
            "No non-EVM chain adapter", "No CEX trades", "Swap signatures are a heuristic, not decoded trade volume",
            "Internal ETH supported only on explicitly listed networks", "No aggregation across transactions or chains",
        ],
    }
    errors = []
    # Round-robin order between runs prevents the same networks starving on a budget cap.
    offset = int(state.get("round_robin", 0)) % len(chains)
    chains = chains[offset:] + chains[:offset]
    state["round_robin"] = offset + 1
    for chain in chains:
        unpriced = [{"address": addr, "symbol": token["symbol"]} for addr, token in chain["tokens"].items()
                    if (chain["network"], addr) not in prices]
        item = {"network": chain["network"], "registered_tokens": len(chain["tokens"]),
                "internal_eth": chain["internal"], "native_eth": chain["native_eth"],
                "unpriced_assets_not_monitored": unpriced}
        if chain["native_eth"] and ("native", "ETH") not in prices:
            raise MonitorError("Missing fresh ETH price")
        try:
            scan_chain(chain, prices, state, topics)
            item["status"] = "partial" if unpriced else "ok"
            if unpriced:
                print(f"::warning::{chain['network']}: {len(unpriced)} assets have no fresh USD price; see coverage.json")
        except BudgetReached as error:
            item.update(status="budget", error=str(error))
            errors.append(f"{chain['network']}: {error}")
        except MonitorError as error:
            item.update(status="error", error=str(error))
            errors.append(f"{chain['network']}: {error}")
        coverage["chains"].append(item)
    if not DRY_RUN:
        cutoff = time.time() - 30 * 86400
        state["sent"] = {key: stamp for key, stamp in state["sent"].items() if stamp >= cutoff}
        write_json(STATE_FILE, state)
    write_json(COVERAGE_FILE, coverage)
    for error in errors:
        print("::error::" + error)
    print(f"RPC calls: {calls}; networks: {len(chains)}; exclusions: {len(catalog['excluded'])}")
    if errors:
        raise MonitorError("Some networks were not scanned; see coverage.json. Their cursors were preserved.")


if __name__ == "__main__":
    try:
        main()
    except MonitorError as error:
        print("ERROR:", str(error), file=sys.stderr)
        sys.exit(1)
    except Exception as error:
        # Tracebacks from HTTP libraries may contain secrets in URLs.
        print("ERROR:", type(error).__name__, "(details redacted)", file=sys.stderr)
        sys.exit(1)
