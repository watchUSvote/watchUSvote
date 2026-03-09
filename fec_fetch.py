"""
VoteWatch USA — FEC Cache Fetcher
Fetches FEC campaign finance data for one chamber and merges it into
data/fec_cache.json.

Usage:
  python fec_fetch.py senate         # fetch senators only
  python fec_fetch.py house          # fetch representatives only

Called by:
  .github/workflows/fec-senators.yml        (runs Mondays)
  .github/workflows/fec-representatives.yml (runs Tuesdays)

Required env vars:
  CONGRESS_API_KEY  — from https://api.congress.gov/sign-up/
  FEC_API_KEY       — from https://api.data.gov/signup/ (free, instant)
"""

import os
import sys
import json
import time
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone

CONGRESS_KEY  = os.environ.get("CONGRESS_API_KEY", "")
FEC_KEY       = os.environ.get("FEC_API_KEY", "DEMO_KEY")

CONGRESS_BASE = "https://api.congress.gov/v3"
FEC_BASE      = "https://api.open.fec.gov/v1"

FEC_CACHE = os.path.join(os.path.dirname(__file__), "data", "fec_cache.json")
FEC_CYCLE = 2024


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def http_get(url, label=""):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "VoteWatch/1.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"  [WARN] {label or url[:80]} -> {e}")
        return {}


def congress_get(path, extra_params=""):
    url = f"{CONGRESS_BASE}{path}?format=json&limit=250{extra_params}"
    req = urllib.request.Request(url, headers={
        "User-Agent": "VoteWatch/1.0",
        "X-Api-Key": CONGRESS_KEY,
    })
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"  [WARN] Congress GET {path} -> {e}")
        return {}


def fmt_usd(n):
    try:
        n = float(n)
    except (TypeError, ValueError):
        return "N/A"
    if n >= 1_000_000:
        return f"${n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"${n/1_000:.0f}K"
    return f"${n:.0f}"


# ─────────────────────────────────────────────────────────────
# CONGRESS — fetch member names for one chamber
# ─────────────────────────────────────────────────────────────

def fetch_members(chamber):
    print(f"  Fetching {chamber} members from Congress.gov ...")
    members = []
    offset  = 0

    while True:
        url = (
            f"{CONGRESS_BASE}/member/congress/119"
            f"?format=json&limit=250&currentMember=true&offset={offset}"
        )
        req = urllib.request.Request(url, headers={
            "User-Agent": "VoteWatch/1.0",
            "X-Api-Key":  CONGRESS_KEY,
        })
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                data = json.loads(r.read())
        except Exception as e:
            print(f"  [WARN] Members page offset={offset} -> {e}")
            break

        page  = data.get("members") or []
        members += page
        total = (data.get("pagination") or {}).get("count", 0)
        if len(members) >= total or not page:
            break
        offset += 250
        time.sleep(0.1)

    # Split by chamber using terms field (same as fetch_data.py)
    chamber_key = "Senate" if chamber == "Senate" else "House of Representatives"
    bio_to_name = {}
    for m in members:
        terms = m.get("terms") or []
        if isinstance(terms, dict):
            terms = terms.get("item") or []
        if not terms:
            continue
        last = terms[-1] if isinstance(terms, list) else terms
        if (last.get("chamber") or "").strip() != chamber_key:
            continue

        bio  = m.get("bioguideId") or m.get("memberId") or ""
        name = (
            m.get("name") or
            " ".join(filter(None, [m.get("firstName"), m.get("lastName")]))
        ).strip()
        if bio and name:
            bio_to_name[bio] = name

    print(f"  -> {len(bio_to_name)} {chamber} members")
    return bio_to_name


# ─────────────────────────────────────────────────────────────
# FEC — fetch totals for a set of members
# ─────────────────────────────────────────────────────────────

def fetch_fec_totals(bio_id_to_name):
    print(f"  Fetching FEC data for {len(bio_id_to_name)} members ...")
    if FEC_KEY == "DEMO_KEY":
        print("  [WARN] Using DEMO_KEY — set FEC_API_KEY for full results")

    results = {}
    count   = 0

    for bio_id, name in bio_id_to_name.items():
        q   = urllib.parse.quote(name)
        url = (
            f"{FEC_BASE}/candidates/search/"
            f"?q={q}&election_year={FEC_CYCLE}&per_page=3&sort=-total_receipts"
            f"&api_key={FEC_KEY}"
        )
        data       = http_get(url, f"FEC search: {name}")
        candidates = data.get("results") or []

        if not candidates:
            results[bio_id] = None
            count += 1
            if count % 50 == 0:
                print(f"    ... {count}/{len(bio_id_to_name)}")
            time.sleep(0.05)
            continue

        cand    = candidates[0]
        cand_id = cand.get("candidate_id", "")

        totals_url  = (
            f"{FEC_BASE}/candidates/totals/"
            f"?candidate_id={cand_id}&cycle={FEC_CYCLE}&api_key={FEC_KEY}"
        )
        totals_data = http_get(totals_url, f"FEC totals: {name}")
        totals      = (totals_data.get("results") or [{}])[0]

        results[bio_id] = {
            "pac_total":        fmt_usd(totals.get("other_political_committee_contributions", 0)),
            "individual_total": fmt_usd(totals.get("individual_contributions", 0)),
            "total_raised":     fmt_usd(totals.get("receipts", 0)),
            "fec_candidate_id": cand_id,
            "fec_url":          f"https://www.fec.gov/data/candidate/{cand_id}/",
        }
        count += 1
        if count % 50 == 0:
            print(f"    ... {count}/{len(bio_id_to_name)}")
        time.sleep(0.12)

    found = sum(1 for v in results.values() if v)
    print(f"  -> FEC data found for {found}/{len(bio_id_to_name)} members")
    return results


# ─────────────────────────────────────────────────────────────
# Cache read / write
# ─────────────────────────────────────────────────────────────

def load_cache():
    if os.path.exists(FEC_CACHE):
        with open(FEC_CACHE, encoding="utf-8") as f:
            return json.load(f)
    return {"updated_at": "", "data": {}}


def save_cache(cache):
    os.makedirs(os.path.dirname(FEC_CACHE), exist_ok=True)
    with open(FEC_CACHE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)
    print(f"  -> Saved {FEC_CACHE}")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    if not CONGRESS_KEY:
        print("ERROR: CONGRESS_API_KEY is not set.")
        raise SystemExit(1)
    if FEC_KEY == "DEMO_KEY":
        print("WARNING: FEC_API_KEY not set — using DEMO_KEY (very low rate limit)")

    if len(sys.argv) < 2 or sys.argv[1].lower() not in ("senate", "house"):
        print("Usage: python fec_fetch.py senate|house")
        raise SystemExit(1)

    chamber = sys.argv[1].capitalize()
    print(f"=== VoteWatch FEC Fetch — {chamber} ===\n")

    bio_to_name = fetch_members(chamber)

    print(f"\nFetching FEC totals ...")
    new_data = fetch_fec_totals(bio_to_name)

    print(f"\nMerging into cache ...")
    cache = load_cache()
    cache["data"].update(new_data)
    cache["updated_at"] = datetime.now(timezone.utc).isoformat()
    save_cache(cache)

    total_in_cache = sum(1 for v in cache["data"].values() if v)
    print(f"\n  {chamber} records updated: {len(new_data)}")
    print(f"  Total members in cache:   {len(cache['data'])}")
    print(f"  Members with FEC data:    {total_in_cache}")


if __name__ == "__main__":
    main()
