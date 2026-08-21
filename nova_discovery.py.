#!/usr/bin/env python3
"""
Nova - Discovery Layer (v0 prototype)
======================================
Polls Birdeye's free-tier "New Listing" endpoint for newly created Solana
tokens, applies a basic liquidity/quality filter, and logs candidates that
are worth a human look. This is layer 1 only (discovery) - no wallet
tracking, no auto-buying, no money moves on its own.

WHY POLLING, NOT A LIVE STREAM:
Birdeye's free Standard tier does not include websockets (that's a paid
tier feature). Polling on an interval is the zero-cost way to approximate
"live" on this budget. See BUDGET NOTES below before changing the interval.

BUDGET NOTES (read before running):
Free tier = 30,000 compute units (CU) / month.
The new_listing endpoint costs 30 CU per call.
=> 30,000 / 30 = 1,000 calls/month MAX if this is the only endpoint you hit.
=> Spread evenly across 30 days, that is ~33 calls/day, i.e. one call every
   ~43 minutes. The default POLL_INTERVAL_SECONDS below (2700s = 45 min)
   stays safely under that with some headroom. If you also add the
   trending-tokens endpoint later, cut the new_listing call frequency
   accordingly so the two don't blow the monthly budget together.

Do NOT drop this to something like "every 30 seconds" on the free tier -
you'll exhaust the month's quota in under a day and Birdeye will start
rejecting requests until it resets.

SETUP:
1. Sign up free at https://bds.birdeye.so (no card required for the free tier)
2. Generate an API key from the Security / API Keys section of the dashboard
3. Set it as an environment variable rather than pasting it into this file:
     export BIRDEYE_API_KEY="your_key_here"
4. pip install requests
5. python3 nova_discovery.py

Try --dry-run first to see the script work end-to-end against fake data,
with no API key and no network calls, before spending any real quota.
"""

import argparse
import csv
import json
import os
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, date
from pathlib import Path

# ----------------------------------------------------------------------
# Configuration - override any of these with environment variables
# ----------------------------------------------------------------------

API_BASE = "https://public-api.birdeye.so"
NEW_LISTING_PATH = "/defi/v2/tokens/new_listing"
CU_COST_PER_CALL = 30
FREE_TIER_MONTHLY_CU = 30_000

CHAIN = os.environ.get("NOVA_CHAIN", "solana")
POLL_INTERVAL_SECONDS = int(os.environ.get("NOVA_POLL_INTERVAL_SECONDS", 2700))  # 45 min default
MIN_LIQUIDITY_USD = float(os.environ.get("NOVA_MIN_LIQUIDITY_USD", 5000))
LISTING_LIMIT = int(os.environ.get("NOVA_LISTING_LIMIT", 20))  # max 20 per Birdeye docs
MEME_PLATFORM_ONLY = os.environ.get("NOVA_MEME_PLATFORM_ONLY", "true").lower() == "true"

DATA_DIR = Path(__file__).parent / "data"
CANDIDATES_JSON = DATA_DIR / "candidates.json"
CANDIDATES_CSV = DATA_DIR / "candidates.csv"
USAGE_FILE = DATA_DIR / "cu_usage.json"


# ----------------------------------------------------------------------
# Compute-unit budget tracking, so the script never gets you locked out
# ----------------------------------------------------------------------

def _load_usage():
    if USAGE_FILE.exists():
        data = json.loads(USAGE_FILE.read_text())
        current_month = date.today().strftime("%Y-%m")
        if data.get("month") == current_month:
            return data
    return {"month": date.today().strftime("%Y-%m"), "cu_spent": 0, "calls": 0}


def _save_usage(usage):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    USAGE_FILE.write_text(json.dumps(usage, indent=2))


def _record_call(usage):
    usage["cu_spent"] += CU_COST_PER_CALL
    usage["calls"] += 1
    _save_usage(usage)
    remaining = FREE_TIER_MONTHLY_CU - usage["cu_spent"]
    pct = usage["cu_spent"] / FREE_TIER_MONTHLY_CU * 100
    log(f"CU used this month: {usage['cu_spent']}/{FREE_TIER_MONTHLY_CU} ({pct:.1f}%), {remaining} remaining")
    if remaining < CU_COST_PER_CALL * 5:
        log("WARNING: fewer than 5 calls' worth of compute units left this month. "
            "Consider raising POLL_INTERVAL_SECONDS or waiting for the monthly reset.")


# ----------------------------------------------------------------------
# Candidate store â candidates.json is the single source of truth.
# The dashboard (nova-dashboard.html) loads this file directly via its
# "Load export" button, so its shape matters: a flat JSON array of rows,
# same fields as to_row() below.
# ----------------------------------------------------------------------

def _load_candidates():
    if CANDIDATES_JSON.exists():
        return json.loads(CANDIDATES_JSON.read_text())
    return []


def _save_candidates(candidates):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CANDIDATES_JSON.write_text(json.dumps(candidates, indent=2))


# ----------------------------------------------------------------------
# Candidate log
# ----------------------------------------------------------------------

def _append_candidates(rows):
    if not rows:
        return
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    is_new = not CANDIDATES_CSV.exists()
    with open(CANDIDATES_CSV, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "discovered_at", "address", "symbol", "name", "liquidity_usd", "source_listed_at",
        ])
        if is_new:
            writer.writeheader()
        writer.writerows(rows)


# ----------------------------------------------------------------------
# Birdeye API call
# ----------------------------------------------------------------------

def fetch_new_listings(api_key, chain=CHAIN, limit=LISTING_LIMIT, meme_only=MEME_PLATFORM_ONLY):
    params = f"limit={limit}&meme_platform_enabled={'true' if meme_only else 'false'}"
    url = f"{API_BASE}{NEW_LISTING_PATH}?{params}"
    req = urllib.request.Request(url)
    req.add_header("X-API-KEY", api_key)
    req.add_header("x-chain", chain)
    req.add_header("accept", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        log(f"ERROR: Birdeye returned HTTP {e.code}: {e.read().decode('utf-8', 'ignore')[:300]}")
        return None
    except urllib.error.URLError as e:
        log(f"ERROR: network error reaching Birdeye: {e.reason}")
        return None

    if not body.get("success", False):
        log(f"ERROR: Birdeye reported failure: {body}")
        return None

    return body.get("data", {}).get("items", [])


def _mock_new_listings():
    """Fake data for --dry-run, so you can see the pipeline work with zero API calls."""
    now = int(time.time())
    return [
        {"address": "MockAddr1111111111111111111111111111111",
         "symbol": "MOCKA", "name": "Mock Alpha", "liquidity": 12500, "liquidityAddedAt": now},
        {"address": "MockAddr2222222222222222222222222222222",
         "symbol": "MOCKB", "name": "Mock Beta (too thin)", "liquidity": 800, "liquidityAddedAt": now},
    ]


# ----------------------------------------------------------------------
# Filtering
# ----------------------------------------------------------------------

def filter_candidates(items, seen, min_liquidity=MIN_LIQUIDITY_USD):
    fresh = []
    for item in items:
        addr = item.get("address")
        if not addr or addr in seen:
            continue
        liquidity = item.get("liquidity") or 0
        if liquidity < min_liquidity:
            continue
        fresh.append(item)
    return fresh


def to_row(item):
    listed_ts = item.get("liquidityAddedAt")
    listed_iso = (
        datetime.fromtimestamp(listed_ts, tz=timezone.utc).isoformat()
        if listed_ts else ""
    )
    return {
        "discovered_at": datetime.now(timezone.utc).isoformat(),
        "address": item.get("address", ""),
        "symbol": item.get("symbol", ""),
        "name": item.get("name", ""),
        "liquidity_usd": item.get("liquidity", ""),
        "source_listed_at": listed_iso,
    }


# ----------------------------------------------------------------------
# Main loop
# ----------------------------------------------------------------------

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def run_once(api_key, candidates, usage, dry_run=False):
    seen = {c["address"] for c in candidates if c.get("address")}

    if dry_run:
        items = _mock_new_listings()
    else:
        items = fetch_new_listings(api_key)
        if items is None:
            return candidates  # error already logged; skip this cycle
        _record_call(usage)

    fresh = filter_candidates(items, seen, MIN_LIQUIDITY_USD)
    if fresh:
        rows = [to_row(item) for item in fresh]
        candidates.extend(rows)
        _save_candidates(candidates)
        _append_candidates(rows)
        for row in rows:
            log(f"CANDIDATE  {row['symbol']:<12} liquidity=${row['liquidity_usd']:<10} {row['address']}")
        log(f"{len(candidates)} total candidates in data/candidates.json â "
            f"load that file into nova-dashboard.html to review them.")
    else:
        log(f"No new candidates above ${MIN_LIQUIDITY_USD:,.0f} liquidity this poll "
            f"({len(items)} listings checked).")

    return candidates


def main():
    parser = argparse.ArgumentParser(description="Nova discovery layer - Birdeye new-listing poller")
    parser.add_argument("--dry-run", action="store_true",
                         help="Run against fake data, no API key or network calls needed.")
    parser.add_argument("--once", action="store_true",
                         help="Run a single poll cycle and exit, instead of looping forever.")
    args = parser.parse_args()

    api_key = os.environ.get("BIRDEYE_API_KEY")
    if not args.dry_run and not api_key:
        log("ERROR: BIRDEYE_API_KEY is not set. Export it, or run with --dry-run to test without one.")
        sys.exit(1)

    log(f"Starting Nova discovery layer | chain={CHAIN} | min_liquidity=${MIN_LIQUIDITY_USD:,.0f} "
        f"| poll_interval={POLL_INTERVAL_SECONDS}s | dry_run={args.dry_run}")

    candidates = _load_candidates()
    usage = _load_usage()

    if args.once:
        run_once(api_key, candidates, usage, dry_run=args.dry_run)
        return

    while True:
        candidates = run_once(api_key, candidates, usage, dry_run=args.dry_run)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
