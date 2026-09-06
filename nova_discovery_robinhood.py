#!/usr/bin/env python3
"""
Nova - Discovery Layer for Robinhood Chain (v0 prototype)
===========================================================
Polls GeckoTerminal's free public API for newly created liquidity pools on
Robinhood Chain (Robinhood's Arbitrum-based Layer 2, launched July 2026 for
tokenized stocks — but permissionless token deployment means it now also
carries heavy memecoin volume; see README notes), applies a liquidity/quality
filter, and logs candidates worth a human look. Discovery only — no wallet
tracking, no auto-buying, no money moves on its own.

THIS IS A SEPARATE, FULLY ISOLATED SCRIPT from nova_discovery.py (the Solana/
Birdeye one) — separate data files, separate rate-limit tracking, separate
process. Nothing in this file imports from or writes to the Solana script's
files. Point nova-dashboard.html at whichever candidates JSON you want to
review; both scripts write the same row shape (plus a "chain" field) so the
one dashboard reads either.

WHY GECKOTERMINAL, NOT BIRDEYE:
Birdeye's coverage is Solana/major-EVM-chain focused and does not (yet) index
Robinhood Chain. GeckoTerminal's public API does — it lists Robinhood Chain
under the network slug "robinhood" — and unlike Birdeye it needs NO API key
and NO signup for the free tier. That also means there's no monthly quota to
budget against, only a requests-per-minute rate limit.

BUDGET / RATE-LIMIT NOTES (read before changing the interval):
GeckoTerminal's public (keyless) API is documented at ~30 requests/minute.
Each poll here makes 1 call for new pools plus up to RUG_CHECK_MAX_PER_POLL
calls for re-checking existing candidates' liquidity — a handful of calls,
nowhere near the per-minute ceiling, EVEN if you dropped the poll interval
a lot. The default POLL_INTERVAL_SECONDS below (900s = 15 min) is chosen for
politeness and to match the "discovery feed, not a live feed" spirit of the
Solana script, not because the rate limit forces it. Do not go below ~30s
between polls without also spacing out the individual calls inside a poll —
see MIN_SECONDS_BETWEEN_CALLS.

RUG-CHECK LAYER (liquidity re-poll):
Same idea as the Solana script: each poll re-checks a small, bounded batch of
already-discovered candidates' current liquidity (reserve_in_usd on the pool)
to catch a liquidity-pull rug after the fact. Bounded by
NOVA_RH_RUG_CHECK_MAX_PER_POLL and NOVA_RH_RUG_CHECK_WINDOW_HOURS.

ANALYZER + PATTERN ENGINE:
Identical in spirit to the Solana script — risk score, momentum, depth tier,
and a pattern_score comparing a candidate's early liquidity growth against
Nova's own history of resolved blow-ups/fades. Duplicated here rather than
imported, on purpose, to keep this script standalone. If you tune the
scoring in one file, tune it in the other too if you want them to agree.

THIN LIQUIDITY WARNING:
Robinhood Chain's memecoin pools have been reported as thin even when volume
looks large — a single sizeable buy/sell can move price sharply. That is
exactly the kind of thing MIN_LIQUIDITY_USD and the rug-check drop threshold
below are trying to filter for/flag — treat the verdict as a faster first
look, not a substitute for checking the pool yourself.

TELEGRAM ALERTS (optional):
Reuses the same TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID as the Solana script —
it's the same person's Nova project, so one bot is enough. Alert text is
prefixed with the chain name so you can tell which script fired. See the
Solana script's docstring for the one-time bot setup steps if you haven't
done that yet.

SETUP:
No signup, no API key. Just:
  python3 nova_discovery_robinhood.py --dry-run   (fake data, no network calls)
  python3 nova_discovery_robinhood.py --once      (one real poll, then exit)
  python3 nova_discovery_robinhood.py             (loops forever)
Uses only the Python standard library — no pip install needed.
"""

import argparse
import csv
import json
import os
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ----------------------------------------------------------------------
# Configuration - override any of these with environment variables
# ----------------------------------------------------------------------

CHAIN_LABEL = "robinhood"          # value stored in each row's "chain" field
GECKOTERMINAL_NETWORK = "robinhood"  # GeckoTerminal's network slug for Robinhood Chain
API_BASE = "https://api.geckoterminal.com/api/v2"
NEW_POOLS_PATH = f"/networks/{GECKOTERMINAL_NETWORK}/new_pools"
POOL_DETAIL_PATH = f"/networks/{GECKOTERMINAL_NETWORK}/pools/{{address}}"

POLL_INTERVAL_SECONDS = int(os.environ.get("NOVA_RH_POLL_INTERVAL_SECONDS", 900))  # 15 min default
MIN_LIQUIDITY_USD = float(os.environ.get("NOVA_RH_MIN_LIQUIDITY_USD", 5000))
NEW_POOLS_PAGES = int(os.environ.get("NOVA_RH_NEW_POOLS_PAGES", 1))  # 20 pools/page
MIN_SECONDS_BETWEEN_CALLS = float(os.environ.get("NOVA_RH_MIN_SECONDS_BETWEEN_CALLS", 1.5))

# --- Rug-check (liquidity re-poll) ---------------------------------------
RUG_CHECK_ENABLED = os.environ.get("NOVA_RH_RUG_CHECK_ENABLED", "true").lower() == "true"
RUG_CHECK_WINDOW_HOURS = float(os.environ.get("NOVA_RH_RUG_CHECK_WINDOW_HOURS", 72))
RUG_CHECK_MAX_PER_POLL = int(os.environ.get("NOVA_RH_RUG_CHECK_MAX_PER_POLL", 5))
RUG_LIQUIDITY_DROP_PCT = float(os.environ.get("NOVA_RH_RUG_LIQUIDITY_DROP_PCT", 0.6))  # 60% drop from peak
RUG_MIN_PEAK_USD = float(os.environ.get("NOVA_RH_RUG_MIN_PEAK_USD", 500))

# --- Pattern engine ---------------------------------------------------------
LIQUIDITY_HISTORY_MAX = int(os.environ.get("NOVA_RH_LIQUIDITY_HISTORY_MAX", 40))
BLOWUP_MULTIPLE = 3.0
FADE_AGE_HOURS = 24.0
FADE_MULTIPLE = 1.5
PATTERN_WINDOW_HOURS = 8.0
MIN_PATTERN_SAMPLES = 5

# --- Telegram alerts (shared with the Solana script) ------------------------
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
TELEGRAM_ALERTS_ENABLED = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)

DATA_DIR = Path(__file__).parent / "data"
CANDIDATES_JSON = DATA_DIR / "candidates_robinhood.json"
CANDIDATES_CSV = DATA_DIR / "candidates_robinhood.csv"
USAGE_FILE = DATA_DIR / "rate_usage_robinhood.json"


def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


# ----------------------------------------------------------------------
# Call-rate tracking (no monthly quota here, just a per-poll call log —
# kept mainly so a poll's log output is honest about how many calls it made)
# ----------------------------------------------------------------------

def _load_usage():
    if USAGE_FILE.exists():
        try:
            return json.loads(USAGE_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    return {"calls_total": 0, "last_poll_calls": 0}


def _save_usage(usage):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    USAGE_FILE.write_text(json.dumps(usage, indent=2))


# ----------------------------------------------------------------------
# Candidate store — candidates_robinhood.json is the single source of
# truth. Same row shape as the Solana script's candidates.json (plus a
# "chain" field), so nova-dashboard.html's "Load export" can open either.
# ----------------------------------------------------------------------

def _load_candidates():
    if CANDIDATES_JSON.exists():
        try:
            return json.loads(CANDIDATES_JSON.read_text())
        except (json.JSONDecodeError, OSError):
            log("WARNING: candidates_robinhood.json was unreadable, starting fresh.")
    return []


def _save_candidates(candidates):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CANDIDATES_JSON.write_text(json.dumps(candidates, indent=2))


def _append_candidates(rows):
    if not rows:
        return
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    is_new = not CANDIDATES_CSV.exists()
    with open(CANDIDATES_CSV, "a", newline="") as f:
        # Write-once discovery log — rug-check bookkeeping fields go stale
        # here immediately, so they're deliberately left out (same choice
        # as the Solana script's CSV).
        writer = csv.DictWriter(f, fieldnames=[
            "discovered_at", "chain", "dex", "address", "token_address",
            "symbol", "name", "liquidity_usd", "pool_created_at",
        ], extrasaction="ignore")
        if is_new:
            writer.writeheader()
        writer.writerows(rows)


# ----------------------------------------------------------------------
# GeckoTerminal API calls
# ----------------------------------------------------------------------

def _get_json(path, params=None):
    """Shared GET helper. Returns the parsed JSON body, or None on any
    failure (logged, never raised) — a bad response should skip this
    call's work for the current poll, not crash it."""
    url = f"{API_BASE}{path}"
    if params:
        url += "?" + "&".join(f"{k}={v}" for k, v in params.items())
    req = urllib.request.Request(url)
    req.add_header("accept", "application/json;version=20230302")
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", "ignore")[:300]
        except Exception:
            pass
        log(f"ERROR: GeckoTerminal returned HTTP {e.code} for {path}: {body}")
        return None
    except urllib.error.URLError as e:
        log(f"ERROR: network error reaching GeckoTerminal ({path}): {e.reason}")
        return None
    except Exception as e:
        log(f"ERROR: unexpected failure calling GeckoTerminal ({path}): {e}")
        return None


def _index_included(body):
    """GeckoTerminal responses are JSON:API style — pool objects reference
    their base/quote tokens and dex by id under relationships, with the
    actual token/dex attributes sitting in a separate top-level "included"
    list. Build a quick {type:id -> attributes} lookup so callers don't
    have to re-scan "included" for every pool."""
    index = {}
    for item in (body.get("included") or []):
        key = (item.get("type"), item.get("id"))
        index[key] = item.get("attributes") or {}
    return index


def _resolve_base_token(pool_obj, included_index):
    rel = ((pool_obj.get("relationships") or {}).get("base_token") or {}).get("data") or {}
    key = (rel.get("type"), rel.get("id"))
    return included_index.get(key, {})


def _resolve_dex_name(pool_obj, included_index):
    rel = ((pool_obj.get("relationships") or {}).get("dex") or {}).get("data") or {}
    key = (rel.get("type"), rel.get("id"))
    attrs = included_index.get(key, {})
    return attrs.get("name") or rel.get("id") or "unknown"


def fetch_new_pools(pages=NEW_POOLS_PAGES):
    """Fetch newest pools on Robinhood Chain, most-recent first. Returns a
    list of (pool_attributes, pool_address, token_address, symbol, name,
    dex_name) tuples, or None if the call failed outright."""
    results = []
    for page in range(1, pages + 1):
        body = _get_json(NEW_POOLS_PATH, params={"page": page})
        if body is None:
            return None if page == 1 else results  # first-page failure = whole call failed
        included_index = _index_included(body)
        for pool_obj in (body.get("data") or []):
            attrs = pool_obj.get("attributes") or {}
            token = _resolve_base_token(pool_obj, included_index)
            dex_name = _resolve_dex_name(pool_obj, included_index)
            pool_address = attrs.get("address") or (pool_obj.get("id", "").split("_", 1)[-1])
            token_address = token.get("address", "")
            symbol = token.get("symbol") or attrs.get("name", "?")
            name = token.get("name") or attrs.get("name", "?")
            results.append({
                "pool_address": pool_address,
                "token_address": token_address,
                "symbol": symbol,
                "name": name,
                "dex": dex_name,
                "liquidity_usd": _as_float(attrs.get("reserve_in_usd")),
                "pool_created_at": attrs.get("pool_created_at") or "",
            })
        if page < pages:
            time.sleep(MIN_SECONDS_BETWEEN_CALLS)
    return results


def fetch_pool_liquidity(pool_address):
    """Re-check a single already-known pool's current liquidity
    (reserve_in_usd). Returns a float, or None on any failure — never
    raises, matching fetch_new_pools' defensive stance."""
    body = _get_json(POOL_DETAIL_PATH.format(address=pool_address))
    if not isinstance(body, dict):
        return None
    attrs = ((body.get("data") or {}).get("attributes")) or {}
    return _as_float(attrs.get("reserve_in_usd"), default=None)


def send_telegram_message(text):
    """Fire a Telegram message via the bot API. Never raises. No-op if
    Telegram isn't configured."""
    if not TELEGRAM_ALERTS_ENABLED:
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = json.dumps({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }).encode("utf-8")
    req = urllib.request.Request(url, data=payload, method="POST")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        if not isinstance(body, dict) or not body.get("ok", False):
            log(f"ERROR: Telegram rejected the alert: {body}")
            return False
        return True
    except urllib.error.HTTPError as e:
        log(f"ERROR: Telegram send failed with HTTP {e.code}")
        return False
    except urllib.error.URLError as e:
        log(f"ERROR: network error sending Telegram alert: {e.reason}")
        return False
    except Exception as e:
        log(f"ERROR: unexpected failure sending Telegram alert: {e}")
        return False


def _esc_html(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _format_rug_alert(c, drop_pct):
    name = _esc_html(c.get("name") or c.get("symbol") or "?")
    symbol = _esc_html(c.get("symbol") or "?")
    peak = _as_float(c.get("peak_liquidity_usd"))
    liquidity = _as_float(c.get("liquidity_usd"))
    address = c.get("address", "")
    return (
        f"⚠️ <b>[Robinhood Chain] Possible rug: {name}</b> ({symbol})\n"
        f"Liquidity ${peak:,.0f} → ${liquidity:,.0f} ({drop_pct * 100:.0f}% drop)\n"
        f"pool <code>{address}</code>\n"
        f"Not confirmed — review before acting."
    )


def _format_new_candidates_alert(rows):
    MAX_LISTED = 10
    header = f"🆕 <b>[Robinhood Chain] {len(rows)} new candidate{'s' if len(rows) != 1 else ''}</b>\n"
    lines = []
    for row in rows[:MAX_LISTED]:
        name = _esc_html(row.get("name") or row.get("symbol") or "?")
        symbol = _esc_html(row.get("symbol") or "?")
        liquidity = _as_float(row.get("liquidity_usd"))
        address = row.get("address", "")
        a = row.get("analysis") or {}
        risk = a.get("risk_score")
        read = f" · {_esc_html(a.get('depth_tier', '?'))}, risk {risk}/100" if risk is not None else ""
        lines.append(f"\n<b>{symbol}</b> — {name}\n${liquidity:,.0f}{read} · <code>{address}</code>")
    remainder = len(rows) - MAX_LISTED
    footer = f"\n\n…and {remainder} more on the dashboard." if remainder > 0 else ""
    return header + "".join(lines) + footer


def _mock_new_pools():
    """Fake data for --dry-run — see the pipeline work with zero API calls."""
    now_iso = datetime.now(timezone.utc).isoformat()
    return [
        {"pool_address": "0xMockPool1111111111111111111111111111111",
         "token_address": "0xMockToken1111111111111111111111111111111",
         "symbol": "MOCKRH", "name": "Mock Robinhood Meme", "dex": "mock-dex",
         "liquidity_usd": 18500, "pool_created_at": now_iso},
        {"pool_address": "0xMockPool2222222222222222222222222222222",
         "token_address": "0xMockToken2222222222222222222222222222222",
         "symbol": "THINRH", "name": "Mock Thin Pool (too thin)", "dex": "mock-dex",
         "liquidity_usd": 900, "pool_created_at": now_iso},
    ]


# ----------------------------------------------------------------------
# Filtering / row shape
# ----------------------------------------------------------------------

def _as_float(value, default=0.0):
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def filter_candidates(items, seen, min_liquidity=MIN_LIQUIDITY_USD):
    fresh = []
    for item in items:
        addr = item.get("pool_address")
        if not addr or addr in seen:
            continue
        if _as_float(item.get("liquidity_usd")) < min_liquidity:
            continue
        fresh.append(item)
    return fresh


def to_row(item):
    liquidity = _as_float(item.get("liquidity_usd"))
    discovered_iso = datetime.now(timezone.utc).isoformat()
    row = {
        "discovered_at": discovered_iso,
        "chain": CHAIN_LABEL,
        "dex": item.get("dex", "unknown"),
        "address": item.get("pool_address", ""),        # pool address — used for liquidity re-checks
        "token_address": item.get("token_address", ""),  # underlying token contract address
        "symbol": item.get("symbol", ""),
        "name": item.get("name", ""),
        "liquidity_usd": liquidity,
        "pool_created_at": item.get("pool_created_at", ""),
        "peak_liquidity_usd": liquidity,
        "first_liquidity_usd": liquidity,
        "liquidity_history": [{"at": discovered_iso, "liquidity_usd": liquidity}],
        "last_checked_at": "",
        "rug_flag": False,
    }
    row["analysis"] = analyze_candidate(row)
    return row


def _parse_iso(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


# ----------------------------------------------------------------------
# Rug check — re-poll liquidity on already-discovered candidates
# ----------------------------------------------------------------------

def recheck_liquidity(candidates):
    """Re-poll liquidity for a small, bounded batch of already-discovered
    candidates. Returns (checked, newly_flagged)."""
    if not RUG_CHECK_ENABLED or not candidates:
        return 0, []

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=RUG_CHECK_WINDOW_HOURS)

    eligible = []
    for c in candidates:
        discovered = _parse_iso(c.get("discovered_at"))
        if discovered is None or discovered < cutoff:
            continue
        eligible.append(c)

    def _priority(c):
        last_checked = c.get("last_checked_at") or ""
        if last_checked:
            return (1, last_checked)
        discovered = _parse_iso(c.get("discovered_at"))
        newest_first = -discovered.timestamp() if discovered else 0
        return (0, newest_first)

    eligible.sort(key=_priority)
    batch = eligible[:RUG_CHECK_MAX_PER_POLL]

    checked = 0
    newly_flagged = []
    for c in batch:
        try:
            liquidity = fetch_pool_liquidity(c["address"])
            time.sleep(MIN_SECONDS_BETWEEN_CALLS)
            checked += 1

            if liquidity is None:
                c["last_checked_at"] = now.isoformat()
                continue

            peak = max(_as_float(c.get("peak_liquidity_usd")), _as_float(c.get("liquidity_usd")), liquidity)
            was_flagged = bool(c.get("rug_flag"))
            c["liquidity_usd"] = liquidity
            c["peak_liquidity_usd"] = peak
            c["last_checked_at"] = now.isoformat()

            history = c.setdefault("liquidity_history", [])
            history.append({"at": c["last_checked_at"], "liquidity_usd": liquidity})
            if len(history) > LIQUIDITY_HISTORY_MAX:
                del history[:len(history) - LIQUIDITY_HISTORY_MAX]

            drop_pct = (1 - liquidity / peak) if peak > 0 else 0
            should_flag = peak >= RUG_MIN_PEAK_USD and drop_pct >= RUG_LIQUIDITY_DROP_PCT
            c["rug_flag"] = should_flag

            if should_flag and not was_flagged:
                newly_flagged.append(c)
                log(f"RUG FLAG   {c.get('symbol', '?'):<12} liquidity ${peak:,.0f} -> ${liquidity:,.0f} "
                    f"({drop_pct * 100:.0f}% drop) {c['address']}")
                if TELEGRAM_ALERTS_ENABLED:
                    sent = send_telegram_message(_format_rug_alert(c, drop_pct))
                    if not sent:
                        log(f"WARNING: rug flag on {c.get('symbol', '?')} didn't reach Telegram.")
        except Exception as e:
            log(f"ERROR: rug-check bookkeeping failed for {c.get('address')}: {e}")
            c["last_checked_at"] = now.isoformat()
            continue

    return checked, newly_flagged


# ----------------------------------------------------------------------
# Analyzer — identical logic to the Solana script's analyzer, duplicated
# here on purpose (this file is meant to stand alone). Tune both together
# if you want the two chains scored on the same scale.
# ----------------------------------------------------------------------

def analyze_candidate(c):
    try:
        liquidity = _as_float(c.get("liquidity_usd"))
        first_liquidity = _as_float(c.get("first_liquidity_usd")) or liquidity
        peak = _as_float(c.get("peak_liquidity_usd")) or liquidity
        rug_flag = bool(c.get("rug_flag"))
        checked = bool(c.get("last_checked_at"))
        discovered = _parse_iso(c.get("discovered_at"))
        age_hours = (
            (datetime.now(timezone.utc) - discovered).total_seconds() / 3600
            if discovered else None
        )
        drop_from_peak = (1 - liquidity / peak) if peak > 0 else 0

        if liquidity >= 500_000:
            depth_tier = "Breakout"
        elif liquidity >= 150_000:
            depth_tier = "Established"
        elif liquidity >= 50_000:
            depth_tier = "Building"
        elif liquidity >= 15_000:
            depth_tier = "Early"
        else:
            depth_tier = "Nascent"

        if not checked:
            momentum = "Too new to tell"
        elif rug_flag:
            momentum = "Collapsed"
        else:
            growth_pct = ((liquidity - first_liquidity) / first_liquidity) if first_liquidity > 0 else 0
            if growth_pct >= 0.50:
                momentum = "Rising fast"
            elif growth_pct >= 0.10:
                momentum = "Growing"
            elif growth_pct >= -0.10:
                momentum = "Steady"
            elif growth_pct >= -0.40:
                momentum = "Cooling off"
            else:
                momentum = "Fading"

        if rug_flag:
            risk_score = 97
        else:
            risk = 0.0
            risk += max(0.0, 35 - liquidity / 3000)
            risk += max(0.0, drop_from_peak) * 45
            if age_hours is not None:
                if age_hours < 1:
                    risk += 20
                elif age_hours < 6:
                    risk += 12
                elif age_hours < 24:
                    risk += 5
            risk_score = int(round(min(96, max(3, risk))))

        conditions = [
            liquidity >= MIN_LIQUIDITY_USD,
            not rug_flag,
            drop_from_peak < 0.4,
            depth_tier != "Nascent",
        ]
        conditions_met = sum(1 for ok in conditions if ok)
        conditions_total = len(conditions)

        if rug_flag:
            verdict, tone = "Flagged — liquidity collapsed", "critical"
        elif risk_score >= 65:
            verdict, tone = "High risk", "critical"
        elif risk_score >= 35:
            if momentum in ("Rising fast", "Growing"):
                verdict, tone = "Risky, but gaining traction", "warning"
            else:
                verdict, tone = "Mixed signals", "warning"
        else:
            if momentum in ("Rising fast", "Growing"):
                verdict, tone = "Promising", "good"
            elif momentum == "Too new to tell":
                verdict, tone = "Clean so far, too early to call", "good"
            else:
                verdict, tone = "Holding steady", "good"

        return {
            "risk_score": risk_score,
            "momentum": momentum,
            "depth_tier": depth_tier,
            "conditions_met": conditions_met,
            "conditions_total": conditions_total,
            "verdict": verdict,
            "verdict_tone": tone,
        }
    except Exception as e:
        log(f"ERROR: analysis failed for {c.get('address')}: {e}")
        return {
            "risk_score": None,
            "momentum": "Unknown",
            "depth_tier": "Unknown",
            "conditions_met": None,
            "conditions_total": 4,
            "verdict": "Analysis unavailable",
            "verdict_tone": "warning",
        }


def classify_pattern(c):
    if c.get("rug_flag"):
        return "rugged"
    first = _as_float(c.get("first_liquidity_usd")) or _as_float(c.get("liquidity_usd"))
    peak = _as_float(c.get("peak_liquidity_usd")) or first
    if first <= 0:
        return "undetermined"
    if peak >= first * BLOWUP_MULTIPLE:
        return "blew_up"
    discovered = _parse_iso(c.get("discovered_at"))
    age_hours = (datetime.now(timezone.utc) - discovered).total_seconds() / 3600 if discovered else 0
    if age_hours >= FADE_AGE_HOURS and peak < first * FADE_MULTIPLE:
        return "faded"
    return "undetermined"


def _early_velocity(c, window_hours=PATTERN_WINDOW_HOURS):
    discovered = _parse_iso(c.get("discovered_at"))
    if discovered is None:
        return None
    history = c.get("liquidity_history") or []
    in_window = []
    for entry in history:
        at = _parse_iso(entry.get("at"))
        if at is None:
            continue
        hrs = (at - discovered).total_seconds() / 3600
        if 0 <= hrs <= window_hours:
            in_window.append((hrs, _as_float(entry.get("liquidity_usd"))))
    if len(in_window) < 2:
        return None
    in_window.sort(key=lambda pair: pair[0])
    t0, l0 = in_window[0]
    t1, l1 = in_window[-1]
    hours_elapsed = t1 - t0
    if hours_elapsed <= 0 or l0 <= 0:
        return None
    return ((l1 - l0) / l0) / hours_elapsed


def build_pattern_profile(candidates):
    blowup_v, faded_v = [], []
    for c in candidates:
        v = _early_velocity(c)
        if v is None:
            continue
        outcome = classify_pattern(c)
        if outcome == "blew_up":
            blowup_v.append(v)
        elif outcome in ("faded", "rugged"):
            faded_v.append(v)
    if len(blowup_v) < MIN_PATTERN_SAMPLES:
        return None
    return {
        "blowup_avg_velocity": sum(blowup_v) / len(blowup_v),
        "faded_avg_velocity": (sum(faded_v) / len(faded_v)) if faded_v else 0.0,
        "n_blowup": len(blowup_v),
        "n_faded": len(faded_v),
    }


def pattern_score_for(c, profile):
    if profile is None:
        return None, "Not enough resolved history yet to compare against"
    v = _early_velocity(c)
    if v is None:
        return None, "Too early — needs a couple of liquidity re-checks first"
    lo, hi = profile["faded_avg_velocity"], profile["blowup_avg_velocity"]
    if hi <= lo:
        hi = lo + abs(lo) * 0.5 + 0.01
    frac = (v - lo) / (hi - lo)
    score = int(round(min(100, max(0, frac * 100))))
    if score >= 70:
        label = f"Tracking like past blow-ups ({profile['n_blowup']} on file)"
    elif score >= 40:
        label = "Mixed — resembles both winners and fades so far"
    else:
        label = f"Tracking like past fades/rugs ({profile['n_faded']} on file)"
    return score, label


def recompute_analysis(candidates):
    profile = build_pattern_profile(candidates)
    for c in candidates:
        c["analysis"] = analyze_candidate(c)
        try:
            score, label = pattern_score_for(c, profile)
            c["analysis"]["pattern_outcome"] = classify_pattern(c)
            c["analysis"]["pattern_score"] = score
            c["analysis"]["pattern_label"] = label
        except Exception as e:
            log(f"ERROR: pattern scoring failed for {c.get('address')}: {e}")
            c["analysis"]["pattern_outcome"] = "undetermined"
            c["analysis"]["pattern_score"] = None
            c["analysis"]["pattern_label"] = "Pattern scoring unavailable"


# ----------------------------------------------------------------------
# Main loop
# ----------------------------------------------------------------------

def run_once(candidates, usage, dry_run=False):
    seen = {c["address"] for c in candidates if c.get("address")}
    calls_this_poll = 0

    if dry_run:
        items = _mock_new_pools()
    else:
        items = fetch_new_pools()
        calls_this_poll += NEW_POOLS_PAGES
        if items is None:
            return candidates  # error already logged; skip this cycle

    fresh = filter_candidates(items, seen, MIN_LIQUIDITY_USD)
    found_new = False
    if fresh:
        rows = [to_row(item) for item in fresh]
        candidates.extend(rows)
        _append_candidates(rows)
        found_new = True
        for row in rows:
            log(f"CANDIDATE  {row['symbol']:<12} liquidity=${row['liquidity_usd']:<10} "
                f"dex={row['dex']} pool={row['address']}")
        log(f"{len(candidates)} total Robinhood Chain candidates in "
            f"data/candidates_robinhood.json — load that file into nova-dashboard.html to review them.")
        if TELEGRAM_ALERTS_ENABLED and not dry_run:
            sent = send_telegram_message(_format_new_candidates_alert(rows))
            if not sent:
                log("WARNING: new-candidate alert didn't reach Telegram.")
    else:
        log(f"No new candidates above ${MIN_LIQUIDITY_USD:,.0f} liquidity this poll "
            f"({len(items)} pools checked).")

    checked = 0
    if not dry_run:
        try:
            checked, newly_flagged = recheck_liquidity(candidates)
            calls_this_poll += checked
            if checked:
                log(f"Rug-check: re-polled {checked} existing candidate(s)"
                    + (f", {len(newly_flagged)} newly flagged" if newly_flagged else ""))
        except Exception as e:
            log(f"ERROR: rug-check layer failed this poll, skipping it: {e}")

    try:
        recompute_analysis(candidates)
    except Exception as e:
        log(f"ERROR: analyzer layer failed this poll, skipping it: {e}")

    if not dry_run:
        usage["calls_total"] = usage.get("calls_total", 0) + calls_this_poll
        usage["last_poll_calls"] = calls_this_poll
        _save_usage(usage)

    if found_new or checked:
        _save_candidates(candidates)

    return candidates


def main():
    parser = argparse.ArgumentParser(
        description="Nova discovery layer - Robinhood Chain (GeckoTerminal new-pools poller)")
    parser.add_argument("--dry-run", action="store_true",
                         help="Run against fake data, no network calls needed.")
    parser.add_argument("--once", action="store_true",
                         help="Run a single poll cycle and exit, instead of looping forever.")
    args = parser.parse_args()

    log(f"Starting Nova discovery layer | chain={CHAIN_LABEL} | min_liquidity=${MIN_LIQUIDITY_USD:,.0f} "
        f"| poll_interval={POLL_INTERVAL_SECONDS}s | dry_run={args.dry_run}")

    candidates = _load_candidates()
    usage = _load_usage()

    if args.once:
        run_once(candidates, usage, dry_run=args.dry_run)
        return

    while True:
        candidates = run_once(candidates, usage, dry_run=args.dry_run)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
