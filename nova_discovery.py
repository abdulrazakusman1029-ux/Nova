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

RUG-CHECK LAYER (liquidity re-poll):
Each poll also re-checks a small, bounded batch of already-discovered
candidates via /defi/price (3 CU/call, much cheaper than new_listing's 30)
to see if liquidity has collapsed since they were first seen â the classic
"liquidity pull" rug. Bounded by NOVA_RUG_CHECK_MAX_PER_POLL (default 3
tokens/poll) and NOVA_RUG_CHECK_WINDOW_HOURS (default 72 â candidates older
than that stop getting re-checked, since a rug that hasn't happened by then
is unlikely to be this kind). At the default hourly schedule that's at most
72 re-checks/day = 216 CU/day = ~6,480 CU/month, comfortably inside the
~8,400 CU/month left over after discovery's ~21,600 (total ~28,080/30,000,
leaving headroom rather than cutting it exactly to the wire). This is NOT
real-time â a rug that happens between polls is only caught after the
fact, on the next poll that reaches it.

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
from datetime import datetime, timezone, date, timedelta
from pathlib import Path

# ----------------------------------------------------------------------
# Configuration - override any of these with environment variables
# ----------------------------------------------------------------------

API_BASE = "https://public-api.birdeye.so"
NEW_LISTING_PATH = "/defi/v2/tokens/new_listing"
PRICE_PATH = "/defi/price"
CU_COST_NEW_LISTING = 30
CU_COST_PRICE = 3
FREE_TIER_MONTHLY_CU = 30_000

CHAIN = os.environ.get("NOVA_CHAIN", "solana")
POLL_INTERVAL_SECONDS = int(os.environ.get("NOVA_POLL_INTERVAL_SECONDS", 2700))  # 45 min default
MIN_LIQUIDITY_USD = float(os.environ.get("NOVA_MIN_LIQUIDITY_USD", 5000))
LISTING_LIMIT = int(os.environ.get("NOVA_LISTING_LIMIT", 20))  # max 20 per Birdeye docs
MEME_PLATFORM_ONLY = os.environ.get("NOVA_MEME_PLATFORM_ONLY", "true").lower() == "true"

# --- Rug-check (liquidity re-poll) ---------------------------------------
# Layer 1.5: re-checks liquidity on candidates already discovered, to catch
# the classic "liquidity pull" rug â not a real-time guarantee (a rug can
# happen faster than an hourly poll), just an honest after-the-fact flag.
RUG_CHECK_ENABLED = os.environ.get("NOVA_RUG_CHECK_ENABLED", "true").lower() == "true"
RUG_CHECK_WINDOW_HOURS = float(os.environ.get("NOVA_RUG_CHECK_WINDOW_HOURS", 72))
RUG_CHECK_MAX_PER_POLL = int(os.environ.get("NOVA_RUG_CHECK_MAX_PER_POLL", 3))
RUG_LIQUIDITY_DROP_PCT = float(os.environ.get("NOVA_RUG_LIQUIDITY_DROP_PCT", 0.6))  # 60% drop from peak
RUG_MIN_PEAK_USD = float(os.environ.get("NOVA_RUG_MIN_PEAK_USD", 500))  # ignore noise below this

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


def _record_call(usage, cu_cost=CU_COST_NEW_LISTING):
    usage["cu_spent"] += cu_cost
    usage["calls"] += 1
    _save_usage(usage)
    remaining = FREE_TIER_MONTHLY_CU - usage["cu_spent"]
    pct = usage["cu_spent"] / FREE_TIER_MONTHLY_CU * 100
    log(f"CU used this month: {usage['cu_spent']}/{FREE_TIER_MONTHLY_CU} ({pct:.1f}%), {remaining} remaining")
    if remaining < CU_COST_NEW_LISTING * 5:
        log("WARNING: fewer than 5 discovery calls' worth of compute units left this month. "
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
        # This CSV is a write-once discovery log, not the live record â the
        # rug-check's bookkeeping fields (peak_liquidity_usd, last_checked_at,
        # rug_flag) change after a row is written, so they'd go stale here
        # immediately. candidates.json is the one source of truth for those;
        # ignore them here rather than write values that mislead later.
        writer = csv.DictWriter(f, fieldnames=[
            "discovered_at", "address", "symbol", "name", "liquidity_usd", "source_listed_at",
        ], extrasaction="ignore")
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


def fetch_token_liquidity(api_key, address, chain=CHAIN):
    """Re-check a single already-known token's current liquidity. Uses
    /defi/price (3 CU) rather than /defi/token_overview (20 CU) â all we
    need for the rug check is the liquidity number, not the full profile.
    Returns a float, or None on any failure (never raises â a re-check
    that can't reach Birdeye should skip that token this round, not crash
    the whole poll)."""
    params = f"address={address}&include_liquidity=true"
    url = f"{API_BASE}{PRICE_PATH}?{params}"
    req = urllib.request.Request(url)
    req.add_header("X-API-KEY", api_key)
    req.add_header("x-chain", chain)
    req.add_header("accept", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        log(f"ERROR: Birdeye price check for {address} returned HTTP {e.code}")
        return None
    except urllib.error.URLError as e:
        log(f"ERROR: network error checking price for {address}: {e.reason}")
        return None

    if not body.get("success", False):
        return None

    data = body.get("data") or {}
    return _as_float(data.get("liquidity"), default=None)


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

def _as_float(value, default=0.0):
    """Birdeye's fields aren't consistently typed across endpoints/tokens â
    numbers sometimes arrive as JSON numbers, sometimes as strings. Coerce
    defensively instead of trusting the type."""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def filter_candidates(items, seen, min_liquidity=MIN_LIQUIDITY_USD):
    fresh = []
    for item in items:
        addr = item.get("address")
        if not addr or addr in seen:
            continue
        liquidity = _as_float(item.get("liquidity"))
        if liquidity < min_liquidity:
            continue
        fresh.append(item)
    return fresh


def to_row(item):
    listed_ts = item.get("liquidityAddedAt")
    listed_iso = ""
    if listed_ts:
        try:
            # Expected shape: a Unix timestamp (int/float, or a numeric string).
            listed_iso = datetime.fromtimestamp(float(listed_ts), tz=timezone.utc).isoformat()
        except (TypeError, ValueError):
            # Birdeye sent something else (e.g. already an ISO string) â keep it
            # as-is rather than crashing the whole poll over a display field.
            listed_iso = str(listed_ts)
    liquidity = _as_float(item.get("liquidity"))
    return {
        "discovered_at": datetime.now(timezone.utc).isoformat(),
        "address": item.get("address", ""),
        "symbol": item.get("symbol", ""),
        "name": item.get("name", ""),
        "liquidity_usd": liquidity,
        "source_listed_at": listed_iso,
        # Rug-check bookkeeping â peak starts equal to the discovery-time
        # reading; recheck_liquidity() updates all three as it re-polls.
        "peak_liquidity_usd": liquidity,
        "last_checked_at": "",
        "rug_flag": False,
    }


# ----------------------------------------------------------------------
# Rug check â re-poll liquidity on already-discovered candidates
# ----------------------------------------------------------------------

def _parse_iso(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def recheck_liquidity(api_key, candidates, usage):
    """Re-poll liquidity for a small, bounded batch of already-discovered
    candidates, to catch a liquidity-pull rug after the fact. Bounded by
    RUG_CHECK_MAX_PER_POLL and RUG_CHECK_WINDOW_HOURS so this can't quietly
    balloon in cost as the candidate list grows over weeks/months â see the
    BUDGET NOTES at the top of this file for the math."""
    if not RUG_CHECK_ENABLED or not candidates:
        return 0, 0

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=RUG_CHECK_WINDOW_HOURS)

    eligible = []
    for c in candidates:
        discovered = _parse_iso(c.get("discovered_at"))
        if discovered is None or discovered < cutoff:
            continue  # outside the window â either too old to bother, or an unparseable timestamp
        eligible.append(c)

    # Never-checked candidates first, then whichever was checked longest ago.
    eligible.sort(key=lambda c: c.get("last_checked_at") or "")
    batch = eligible[:RUG_CHECK_MAX_PER_POLL]

    checked = 0
    newly_flagged = 0
    for c in batch:
        remaining = FREE_TIER_MONTHLY_CU - usage["cu_spent"]
        if remaining < CU_COST_PRICE:
            log("Rug-check paused: out of compute units for this month.")
            break

        liquidity = fetch_token_liquidity(api_key, c["address"])
        _record_call(usage, cu_cost=CU_COST_PRICE)
        checked += 1

        if liquidity is None:
            # Couldn't get a fresh reading â still stamp last_checked_at so a
            # persistently-failing token doesn't hog every future poll's batch.
            c["last_checked_at"] = now.isoformat()
            continue

        peak = max(_as_float(c.get("peak_liquidity_usd")), _as_float(c.get("liquidity_usd")), liquidity)
        was_flagged = bool(c.get("rug_flag"))
        c["liquidity_usd"] = liquidity
        c["peak_liquidity_usd"] = peak
        c["last_checked_at"] = now.isoformat()

        drop_pct = (1 - liquidity / peak) if peak > 0 else 0
        should_flag = peak >= RUG_MIN_PEAK_USD and drop_pct >= RUG_LIQUIDITY_DROP_PCT
        c["rug_flag"] = should_flag

        if should_flag and not was_flagged:
            newly_flagged += 1
            log(f"RUG FLAG   {c.get('symbol', '?'):<12} liquidity ${peak:,.0f} -> ${liquidity:,.0f} "
                f"({drop_pct * 100:.0f}% drop) {c['address']}")

    return checked, newly_flagged


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
    found_new = False
    if fresh:
        rows = [to_row(item) for item in fresh]
        candidates.extend(rows)
        _append_candidates(rows)
        found_new = True
        for row in rows:
            log(f"CANDIDATE  {row['symbol']:<12} liquidity=${row['liquidity_usd']:<10} {row['address']}")
        log(f"{len(candidates)} total candidates in data/candidates.json â "
            f"load that file into nova-dashboard.html to review them.")
    else:
        log(f"No new candidates above ${MIN_LIQUIDITY_USD:,.0f} liquidity this poll "
            f"({len(items)} listings checked).")

    checked = 0
    if not dry_run:
        checked, newly_flagged = recheck_liquidity(api_key, candidates, usage)
        if checked:
            log(f"Rug-check: re-polled {checked} existing candidate(s)"
                + (f", {newly_flagged} newly flagged" if newly_flagged else ""))

    if found_new or checked:
        _save_candidates(candidates)

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
