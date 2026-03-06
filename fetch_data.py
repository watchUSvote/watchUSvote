"""
VoteWatch USA — Daily Data Fetcher
Fetches from:
  1. Congress.gov  — members (filtered by chamber), votes, bills
  2. FEC cache     — reads data/fec_cache.json written by fec_fetch.py

Key fixes vs prior version:
  - Chamber filter (&chamber=Senate) is IGNORED by the list endpoint.
    We now fetch /member/congress/119 (all members) and split by
    terms[].chamber ("Senate" vs "House of Representatives").
  - participation is derived from the individual member detail endpoint
    (/member/{bioguideId}) which returns missedVotesPercent.
  - state is always the 2-letter abbreviation (stateCode).
  - Vote positions are fetched per-vote via the rollcall detail endpoint.

Required env vars:
  CONGRESS_API_KEY  — from https://api.congress.gov/sign-up/

Run manually:  python fetch_data.py
Auto-runs via: .github/workflows/daily-fetch.yml
"""

import os
import json
import time
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone

CONGRESS_KEY  = os.environ.get("CONGRESS_API_KEY", "")
CONGRESS_BASE = "https://api.congress.gov/v3"

OUTPUT    = os.path.join(os.path.dirname(__file__), "data", "data.json")
FEC_CACHE = os.path.join(os.path.dirname(__file__), "data", "fec_cache.json")

CONGRESS_NUM = 119


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def congress_get(path, params=None):
    """GET from Congress.gov API, returns parsed JSON or {}."""
    base_params = f"format=json&limit=250"
    if params:
        base_params += "&" + params
    url = f"{CONGRESS_BASE}{path}?{base_params}"
    req = urllib.request.Request(url, headers={
        "User-Agent": "VoteWatch/1.0",
        "X-Api-Key":  CONGRESS_KEY,
    })
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"  [WARN] Congress GET {path} -> {e}")
        return {}


def normalize_party(raw):
    r = (raw or "").strip().upper()
    if r.startswith("D"): return "D"
    if r.startswith("R"): return "R"
    return "I"


def normalize_vote(pos):
    p = (pos or "").upper()
    if p in ("YEA", "YES", "AYE"):  return "YEA"
    if p in ("NAY", "NO", "NAYE"): return "NAY"
    if "PRESENT" in p:              return "PRESENT"
    return "NV"


def fmt_usd(n):
    try:
        n = float(n)
    except (TypeError, ValueError):
        return "N/A"
    if n >= 1_000_000: return f"${n/1_000_000:.1f}M"
    if n >= 1_000:     return f"${n/1_000:.0f}K"
    return f"${n:.0f}"


def member_chamber(m):
    """
    Determine a member's current chamber from their terms list.
    Returns 'Senate', 'House', or None.
    Congress.gov term chamber values:
      'Senate' | 'House of Representatives'
    We look at the most recent term (last in list).
    """
    terms = m.get("terms") or []
    if isinstance(terms, dict):
        # Older API shape: {"item": [...]}
        terms = terms.get("item") or []
    if not terms:
        return None
    # Most recent term is last
    last = terms[-1] if isinstance(terms, list) else terms
    chamber = (last.get("chamber") or "").strip()
    if chamber == "Senate":
        return "Senate"
    if chamber == "House of Representatives":
        return "House"
    return None


# ─────────────────────────────────────────────────────────────
# 1. CONGRESS.GOV — All members, paginated, split by chamber
# ─────────────────────────────────────────────────────────────

def fetch_all_members():
    """
    Fetch all current members of the 119th Congress.
    Returns (senate_list, house_list) — each a list of raw member dicts.
    """
    print("  Fetching all members (paginated) ...")
    members = []
    offset  = 0

    while True:
        data  = congress_get(
            f"/member/congress/{CONGRESS_NUM}",
            f"currentMember=true&offset={offset}"
        )
        page  = data.get("members") or []
        members += page

        pagination = data.get("pagination") or {}
        total      = pagination.get("count", 0)
        if len(members) >= total or not page:
            break
        offset += 250
        time.sleep(0.1)

    print(f"  -> {len(members)} total members fetched")

    senate = []
    house  = []
    for m in members:
        ch = member_chamber(m)
        if ch == "Senate":
            senate.append(m)
        elif ch == "House":
            house.append(m)
        # else: ignore (delegates, non-voting members, etc.)

    print(f"  -> {len(senate)} senators, {len(house)} representatives (after chamber split)")
    return senate, house


# ─────────────────────────────────────────────────────────────
# 2. CONGRESS.GOV — Member detail (participation rate)
# ─────────────────────────────────────────────────────────────

def fetch_member_details(members, label="members"):
    """
    Fetch individual member detail pages to get missedVotesPercent.
    Returns { bioguideId: participation_int }
    """
    print(f"  Fetching member detail pages for {len(members)} {label} ...")
    result = {}
    for i, m in enumerate(members):
        bio_id = m.get("bioguideId") or ""
        if not bio_id:
            continue
        detail = congress_get(f"/member/{bio_id}")
        member = detail.get("member") or {}
        missed = member.get("missedVotesPercent") or member.get("missedVotesPct")
        if missed is not None:
            try:
                result[bio_id] = max(0, min(100, round(100 - float(missed))))
            except (TypeError, ValueError):
                result[bio_id] = 85
        else:
            result[bio_id] = 85
        if (i + 1) % 50 == 0:
            print(f"    ... {i+1}/{len(members)}")
        time.sleep(0.05)

    found = sum(1 for v in result.values() if v != 85)
    print(f"  -> participation data found for {found}/{len(members)} {label}")
    return result


# ─────────────────────────────────────────────────────────────
# 3. CONGRESS.GOV — Recent votes with member positions
# ─────────────────────────────────────────────────────────────

def fetch_recent_votes(chamber, limit=5):
    """
    Fetch recent votes for a chamber, then pull the full rollcall
    detail for each so we have member-level positions.
    Returns (votes_with_positions, vote_bill_labels)
    """
    print(f"  Fetching {chamber} votes ...")
    chamber_lower = chamber.lower()
    votes_raw     = []

    for session in ["1", "2"]:
        data  = congress_get(
            f"/vote/{CONGRESS_NUM}/{chamber_lower}/{session}",
            "limit=10&sort=date+desc"
        )
        votes_raw = data.get("votes") or []
        if votes_raw:
            print(f"  -> {len(votes_raw)} {chamber} votes found (session {session})")
            break

    if not votes_raw:
        print(f"  [WARN] No {chamber} votes found")
        return [], []

    # Take the 5 most recent
    votes_raw = votes_raw[:limit]

    # Fetch full rollcall detail for each vote
    votes_detailed = []
    for v in votes_raw:
        roll_url = v.get("url") or ""
        if not roll_url:
            votes_detailed.append({"meta": v, "positions": {}})
            continue

        # The vote URL points to the rollcall detail
        # e.g. https://api.congress.gov/v3/vote/119/senate/1/1
        # Strip base and re-fetch via congress_get
        path = roll_url.replace(CONGRESS_BASE, "").split("?")[0]
        detail = congress_get(path)
        vote_detail = detail.get("vote") or {}
        positions_raw = vote_detail.get("positions") or []

        # Build { bioguideId: position }
        positions = {}
        for p in positions_raw:
            bio = p.get("member", {}).get("bioguideId") or ""
            pos = p.get("votePosition") or p.get("position") or ""
            if bio:
                positions[bio] = normalize_vote(pos)

        votes_detailed.append({"meta": v, "positions": positions})
        time.sleep(0.1)

    # Build bill labels
    vote_bill_labels = []
    for vd in votes_detailed:
        v = vd["meta"]
        vote_bill_labels.append({
            "id":    str(v.get("voteNumber") or v.get("rollNumber") or "?"),
            "title": v.get("question") or v.get("title") or "Vote",
            "date":  (v.get("date") or "")[:10],
        })

    print(f"  -> roll-call positions loaded for {len(votes_detailed)} {chamber} votes")
    return votes_detailed, vote_bill_labels


# ─────────────────────────────────────────────────────────────
# 4. CONGRESS.GOV — Bills with sponsor + cosponsors
# ─────────────────────────────────────────────────────────────

def fetch_bills():
    print("  Fetching recent bills ...")
    data      = congress_get(f"/bill/{CONGRESS_NUM}", "sort=updateDate+desc&limit=20")
    bills_raw = data.get("bills") or []

    bills = []
    for b in bills_raw:
        latest      = b.get("latestAction") or {}
        bill_type   = b.get("type", "")
        bill_number = b.get("number", "")
        bill_id     = f"{bill_type}.{bill_number}".strip(".")

        sponsor_name  = "Unknown"
        sponsor_party = "?"
        sponsor_state = "?"
        cosponsors    = []

        if bill_type and bill_number:
            detail      = congress_get(f"/bill/{CONGRESS_NUM}/{bill_type.lower()}/{bill_number}")
            bill_detail = detail.get("bill") or {}

            for sp in (bill_detail.get("sponsors") or [])[:1]:
                sponsor_name  = sp.get("fullName") or sp.get("name") or "Unknown"
                sponsor_party = normalize_party(sp.get("party") or "")
                sponsor_state = sp.get("state") or "?"

            cosponsor_data = congress_get(
                f"/bill/{CONGRESS_NUM}/{bill_type.lower()}/{bill_number}/cosponsors",
                "limit=10"
            )
            for cs in (cosponsor_data.get("cosponsors") or [])[:8]:
                cosponsors.append({
                    "name":  cs.get("fullName") or cs.get("name") or "Unknown",
                    "party": normalize_party(cs.get("party") or ""),
                    "state": cs.get("state") or "?",
                })
            time.sleep(0.15)

        bills.append({
            "id":            bill_id,
            "title":         b.get("title") or b.get("shortTitle") or "Untitled",
            "date":          (latest.get("actionDate") or b.get("updateDate") or "")[:10],
            "result":        "pending",
            "yea":           0,
            "nay":           0,
            "sponsor":       sponsor_name,
            "sponsor_party": sponsor_party,
            "sponsor_state": sponsor_state,
            "cosponsors":    cosponsors,
            "url":           b.get("url") or (
                f"https://www.congress.gov/bill/{CONGRESS_NUM}th-congress/"
                f"{bill_type.lower()}-bill/{bill_number}"
            ),
        })

    print(f"  -> {len(bills)} bills (with sponsors)")
    return bills


# ─────────────────────────────────────────────────────────────
# 5. FEC cache
# ─────────────────────────────────────────────────────────────

def load_fec_cache():
    if not os.path.exists(FEC_CACHE):
        print("  [WARN] fec_cache.json not found — FEC data will show N/A")
        print("         Trigger fec-senators.yml and fec-representatives.yml to populate it")
        return {}
    with open(FEC_CACHE, encoding="utf-8") as f:
        cache = json.load(f)
    updated = cache.get("updated_at", "unknown")
    data    = cache.get("data", {})
    print(f"  -> FEC cache loaded ({len(data)} members, last updated {updated[:10]})")
    return data


# ─────────────────────────────────────────────────────────────
# 6. Assemble member objects
# ─────────────────────────────────────────────────────────────

def map_members(raw_members, votes_detailed, participation_map, fec_data):
    out = []
    for m in raw_members:
        name = (
            m.get("name") or
            " ".join(filter(None, [m.get("firstName"), m.get("lastName")]))
        ).strip()

        party    = normalize_party(m.get("partyName") or m.get("party") or "")
        # Always use the 2-letter state abbreviation
        state    = m.get("stateCode") or m.get("state") or "?"
        if len(state) > 2:
            # Fallback: map full name to abbreviation if needed
            state = state[:2].upper()
        district = str(m.get("district") or "")
        bio_id   = m.get("bioguideId") or m.get("memberId") or ""

        participation = participation_map.get(bio_id, 85)

        # Build vote positions from detailed rollcall data
        votes = []
        for vd in votes_detailed:
            positions = vd.get("positions") or {}
            votes.append(positions.get(bio_id, "NV"))
        while len(votes) < 5:
            votes.append("NV")

        fec = fec_data.get(bio_id)

        out.append({
            "name":          name,
            "party":         party,
            "state":         state,
            "district":      district,
            "votes":         votes,
            "participation": participation,
            "id":            bio_id,
            "congress_url":  f"https://www.congress.gov/member/{bio_id}",
            "finance": {
                "pac_total":        (fec or {}).get("pac_total",        "N/A"),
                "individual_total": (fec or {}).get("individual_total", "N/A"),
                "total_raised":     (fec or {}).get("total_raised",     "N/A"),
                "fec_url":          (fec or {}).get("fec_url",          ""),
            },
        })
    return out


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    if not CONGRESS_KEY:
        print("ERROR: CONGRESS_API_KEY is not set.")
        raise SystemExit(1)

    print("=== VoteWatch Daily Fetch ===\n")

    # ── 1. Members (correctly split by chamber) ──────────────
    print("[1/5] Congress.gov members ...")
    senate_raw, house_raw = fetch_all_members()

    # ── 2. Member detail pages (participation rates) ─────────
    print("\n[2/5] Member participation rates ...")
    senate_participation = fetch_member_details(senate_raw, "senators")
    house_participation  = fetch_member_details(house_raw,  "representatives")

    # ── 3. Votes with full roll-call positions ────────────────
    print("\n[3/5] Congress.gov votes (with roll-call positions) ...")
    senate_votes, senate_vote_labels = fetch_recent_votes("Senate")
    house_votes,  house_vote_labels  = fetch_recent_votes("House")

    # ── 4. Bills ──────────────────────────────────────────────
    print("\n[4/5] Congress.gov bills ...")
    bills = fetch_bills()

    # ── 5. FEC cache + assemble ───────────────────────────────
    print("\n[5/5] Loading FEC cache & assembling output ...")
    fec_data = load_fec_cache()

    senators = map_members(senate_raw, senate_votes, senate_participation, fec_data)
    reps     = map_members(house_raw,  house_votes,  house_participation,  fec_data)

    payload = {
        "updated_at":        datetime.now(timezone.utc).isoformat(),
        "senators":          senators,
        "representatives":   reps,
        "bills":             bills,
        "senate_vote_bills": senate_vote_labels,
        "house_vote_bills":  house_vote_labels,
    }

    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    finance_count = sum(
        1 for m in senators + reps
        if m["finance"]["total_raised"] != "N/A"
    )
    real_participation = sum(
        1 for m in senators + reps
        if m["participation"] != 85
    )
    print(f"\n  Senators:              {len(senators)}")
    print(f"  Representatives:       {len(reps)}")
    print(f"  Bills:                 {len(bills)}")
    print(f"  FEC data:              {finance_count} members")
    print(f"  Real participation:    {real_participation} members")
    print(f"\nSaved -> {OUTPUT}")


if __name__ == "__main__":
    main()
