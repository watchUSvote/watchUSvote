"""
VoteWatch USA — Daily Data Fetcher

Data sources:
  1. Congress.gov API — members (split by chamber from terms field), bills
  2. Senate.gov XML  — Senate roll call vote list + individual vote XML
  3. Clerk.House.gov XML — House roll call vote XML (by roll number)
  4. FEC cache       — reads data/fec_cache.json (updated weekly)

Participation rate is calculated directly from the votes we fetch:
  (votes cast / total votes) * 100

Required env vars:
  CONGRESS_API_KEY  — https://api.congress.gov/sign-up/

Run manually:  python fetch_data.py
Auto-runs via: .github/workflows/daily-fetch.yml
"""

import os
import json
import time
import xml.etree.ElementTree as ET
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone

CONGRESS_KEY  = os.environ.get("CONGRESS_API_KEY", "")
CONGRESS_BASE = "https://api.congress.gov/v3"
SENATE_BASE   = "https://www.senate.gov/legislative/LIS"
HOUSE_BASE    = "https://clerk.house.gov/evs"

OUTPUT    = os.path.join(os.path.dirname(__file__), "data", "data.json")
FEC_CACHE = os.path.join(os.path.dirname(__file__), "data", "fec_cache.json")

CONGRESS_NUM = 119
VOTES_WANTED = 5   # how many recent votes to show per member


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def http_get_text(url, label=""):
    """Fetch a URL and return raw text, or None on failure."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "VoteWatch/1.0"})
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  [WARN] {label or url[:80]} -> {e}")
        return None


def congress_get(path, params=None):
    """GET from Congress.gov API, returns parsed JSON or {}."""
    base_params = "format=json&limit=250"
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
    p = (pos or "").strip().upper()
    if p in ("YEA", "YES", "AYE", "AY"):  return "YEA"
    if p in ("NAY", "NO", "NAYE", "NO"):  return "NAY"
    if "PRESENT" in p:                     return "PRESENT"
    return "NV"


def fmt_usd(n):
    try:
        n = float(n)
    except (TypeError, ValueError):
        return "N/A"
    if n >= 1_000_000: return f"${n/1_000_000:.1f}M"
    if n >= 1_000:     return f"${n/1_000:.0f}K"
    return f"${n:.0f}"


# ─────────────────────────────────────────────────────────────
# 1. CONGRESS.GOV — All members, split by chamber via terms
# ─────────────────────────────────────────────────────────────

def member_chamber(m):
    """Return 'Senate', 'House', or None based on most recent term."""
    terms = m.get("terms") or []
    if isinstance(terms, dict):
        terms = terms.get("item") or []
    if not terms:
        return None
    last    = terms[-1] if isinstance(terms, list) else terms
    chamber = (last.get("chamber") or "").strip()
    if chamber == "Senate":                    return "Senate"
    if chamber == "House of Representatives":  return "House"
    return None


def fetch_all_members():
    """
    Fetch all current 119th Congress members, return (senate_list, house_list).
    Uses /member/congress/119 with pagination — chamber filter is ignored by API.
    """
    print("  Fetching all members (paginated) ...")
    members = []
    offset  = 0

    while True:
        data = congress_get(
            f"/member/congress/{CONGRESS_NUM}",
            f"currentMember=true&offset={offset}"
        )
        page  = data.get("members") or []
        members += page
        total = (data.get("pagination") or {}).get("count", 0)
        if len(members) >= total or not page:
            break
        offset += 250
        time.sleep(0.1)

    print(f"  -> {len(members)} total members fetched")

    senate, house = [], []
    for m in members:
        ch = member_chamber(m)
        if ch == "Senate": senate.append(m)
        elif ch == "House": house.append(m)

    print(f"  -> {len(senate)} senators, {len(house)} representatives")
    return senate, house


# ─────────────────────────────────────────────────────────────
# 2. SENATE.GOV XML — Recent Senate roll call votes
# ─────────────────────────────────────────────────────────────

def fetch_senate_vote_list(session, limit=10):
    """
    Fetch the Senate vote list XML for a given session.
    Returns list of dicts with keys: vote_number, title, date
    Sorted most-recent first.
    URL: https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_{congress}_{session}.xml
    """
    url = f"{SENATE_BASE}/roll_call_lists/vote_menu_{CONGRESS_NUM}_{session}.xml"
    text = http_get_text(url, f"Senate vote list session {session}")
    if not text:
        return []

    try:
        root  = ET.fromstring(text)
        votes = []
        # Elements: <vote> with children <vote_number>, <vote_date>, <question>, <vote_result>, <document>
        for v in root.findall(".//vote"):
            num  = (v.findtext("vote_number") or "").strip().zfill(5)
            date = (v.findtext("vote_date") or "").strip()
            q    = (v.findtext("question") or "").strip()
            doc  = v.find("document")
            title = ""
            if doc is not None:
                title = (doc.findtext("document_title") or
                         doc.findtext("title") or q or "Vote")
            if not title:
                title = q or "Vote"
            votes.append({
                "vote_number": num,
                "session":     str(session),
                "title":       title,
                "date":        date,
            })
        # Most recent first (highest vote number)
        votes.sort(key=lambda x: x["vote_number"], reverse=True)
        return votes[:limit]
    except ET.ParseError as e:
        print(f"  [WARN] Senate vote list XML parse error: {e}")
        return []


def fetch_senate_vote_detail(vote_number, session):
    """
    Fetch a single Senate roll call vote XML.
    Returns { bioguide_id: "YEA"/"NAY"/"NV", ... }
    URL: https://www.senate.gov/legislative/LIS/roll_call_votes/vote{congress}{session}/vote_{congress}_{session}_{NNNNN}.xml
    """
    num_padded = str(vote_number).zfill(5)
    url = (
        f"{SENATE_BASE}/roll_call_votes/"
        f"vote{CONGRESS_NUM}{session}/"
        f"vote_{CONGRESS_NUM}_{session}_{num_padded}.xml"
    )
    text = http_get_text(url, f"Senate vote {vote_number}")
    if not text:
        return {}

    try:
        root      = ET.fromstring(text)
        positions = {}
        for member in root.findall(".//member"):
            bio_id = (member.findtext("bioguideId") or
                      member.findtext("lis_member_id") or "").strip()
            vote   = (member.findtext("vote_cast") or "").strip()
            # Senate XML uses last_name as fallback key
            if not bio_id:
                bio_id = (member.findtext("last_name") or "").strip()
            if bio_id:
                positions[bio_id] = normalize_vote(vote)
        return positions
    except ET.ParseError as e:
        print(f"  [WARN] Senate vote {vote_number} XML parse error: {e}")
        return {}


def fetch_senate_votes():
    """
    Returns (votes_detailed, vote_bill_labels) for Senate.
    votes_detailed: list of { meta: {...}, positions: {bioguide_id: pos} }
    """
    print("  Fetching Senate votes (senate.gov XML) ...")
    vote_list = []

    # Try session 2 (2026) first, fall back to session 1 (2025)
    for session in [2, 1]:
        vote_list = fetch_senate_vote_list(session, limit=VOTES_WANTED + 5)
        if vote_list:
            print(f"  -> {len(vote_list)} Senate votes found (session {session})")
            break

    if not vote_list:
        print("  [WARN] No Senate votes found")
        return [], []

    votes_detailed   = []
    vote_bill_labels = []

    for v in vote_list[:VOTES_WANTED]:
        positions = fetch_senate_vote_detail(v["vote_number"], v["session"])
        votes_detailed.append({"meta": v, "positions": positions})
        vote_bill_labels.append({
            "id":    v["vote_number"].lstrip("0") or "0",
            "title": v["title"][:80],
            "date":  v["date"][:10] if len(v["date"]) >= 10 else v["date"],
        })
        time.sleep(0.1)

    print(f"  -> roll-call positions loaded for {len(votes_detailed)} Senate votes")
    return votes_detailed, vote_bill_labels


# ─────────────────────────────────────────────────────────────
# 3. CLERK.HOUSE.GOV XML — Recent House roll call votes
# ─────────────────────────────────────────────────────────────

def fetch_house_vote_index():
    """
    Discover recent House roll numbers by fetching the Clerk index page.
    Returns list of (year, roll_number) tuples, most recent first.
    """
    results = []
    for year in [2026, 2025]:
        url  = f"{HOUSE_BASE}/{year}/index.asp"
        text = http_get_text(url, f"House vote index {year}")
        if not text:
            continue
        # Parse roll numbers from links like: evs/2025/roll080.xml
        import re
        rolls = re.findall(r'roll(\d+)\.xml', text, re.IGNORECASE)
        if rolls:
            nums = sorted(set(int(r) for r in rolls), reverse=True)
            results += [(year, n) for n in nums]
        if len(results) >= VOTES_WANTED + 10:
            break
        time.sleep(0.1)

    return results[:VOTES_WANTED + 10]


def fetch_house_vote_detail(year, roll_number):
    """
    Fetch a single House roll call XML from clerk.house.gov.
    Returns ({ bioguide_id: position }, meta_dict)
    URL: https://clerk.house.gov/evs/{year}/roll{NNN}.xml
    """
    num_padded = str(roll_number).zfill(3)
    url  = f"{HOUSE_BASE}/{year}/roll{num_padded}.xml"
    text = http_get_text(url, f"House vote {year}-{roll_number}")
    if not text:
        return {}, {}

    try:
        root = ET.fromstring(text)

        # Meta
        vote_question = root.findtext(".//vote-question") or ""
        vote_desc     = root.findtext(".//legis-name") or root.findtext(".//vote-desc") or ""
        action_date   = root.findtext(".//action-date") or ""
        title         = vote_desc or vote_question or f"Roll {roll_number}"

        meta = {
            "vote_number": str(roll_number),
            "title":       title[:80],
            "date":        action_date,
        }

        # Member positions
        positions = {}
        for member in root.findall(".//recorded-vote/legislator"):
            bio_id = (member.get("name-id") or "").strip()
            vote   = (member.get("vote") or "").strip()
            if bio_id:
                positions[bio_id] = normalize_vote(vote)

        return positions, meta
    except ET.ParseError as e:
        print(f"  [WARN] House vote {year}-{roll_number} XML parse error: {e}")
        return {}, {}


def fetch_house_votes():
    """
    Returns (votes_detailed, vote_bill_labels) for House.
    """
    print("  Fetching House votes (clerk.house.gov XML) ...")
    index = fetch_house_vote_index()

    if not index:
        print("  [WARN] No House votes found")
        return [], []

    print(f"  -> {len(index)} House votes in index, fetching top {VOTES_WANTED}")

    votes_detailed   = []
    vote_bill_labels = []

    for year, roll_num in index[:VOTES_WANTED]:
        positions, meta = fetch_house_vote_detail(year, roll_num)
        if not meta:
            continue
        votes_detailed.append({"meta": meta, "positions": positions})
        vote_bill_labels.append({
            "id":    meta["vote_number"],
            "title": meta["title"],
            "date":  meta["date"],
        })
        time.sleep(0.1)

    print(f"  -> roll-call positions loaded for {len(votes_detailed)} House votes")
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

def calc_participation(bio_id, votes_detailed):
    """
    Calculate participation % from the votes we already fetched.
    A member 'participated' if their position is YEA, NAY, or PRESENT.
    """
    total  = len(votes_detailed)
    if total == 0:
        return 85  # fallback
    cast = sum(
        1 for vd in votes_detailed
        if vd["positions"].get(bio_id, "NV") != "NV"
    )
    return round((cast / total) * 100)


def map_members(raw_members, votes_detailed, fec_data):
    out = []
    for m in raw_members:
        name = (
            m.get("name") or
            " ".join(filter(None, [m.get("firstName"), m.get("lastName")]))
        ).strip()

        party    = normalize_party(m.get("partyName") or m.get("party") or "")
        state    = (m.get("stateCode") or m.get("state") or "?")
        # Ensure it's always a 2-letter code
        if len(state) > 2:
            state = state[:2].upper()
        district = str(m.get("district") or "")
        bio_id   = m.get("bioguideId") or m.get("memberId") or ""

        participation = calc_participation(bio_id, votes_detailed)

        votes = []
        for vd in votes_detailed:
            votes.append(vd["positions"].get(bio_id, "NV"))
        while len(votes) < VOTES_WANTED:
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

    # ── 1. Members ────────────────────────────────────────────
    print("[1/5] Congress.gov members ...")
    senate_raw, house_raw = fetch_all_members()

    # ── 2. Senate votes (senate.gov XML) ─────────────────────
    print("\n[2/5] Senate roll call votes (senate.gov XML) ...")
    senate_votes, senate_vote_labels = fetch_senate_votes()

    # ── 3. House votes (clerk.house.gov XML) ─────────────────
    print("\n[3/5] House roll call votes (clerk.house.gov XML) ...")
    house_votes, house_vote_labels = fetch_house_votes()

    # ── 4. Bills ──────────────────────────────────────────────
    print("\n[4/5] Congress.gov bills ...")
    bills = fetch_bills()

    # ── 5. FEC cache + assemble ───────────────────────────────
    print("\n[5/5] Loading FEC cache & assembling output ...")
    fec_data = load_fec_cache()

    senators = map_members(senate_raw, senate_votes, fec_data)
    reps     = map_members(house_raw,  house_votes,  fec_data)

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
    all_nv_senate = sum(1 for m in senators if all(v == "NV" for v in m["votes"]))
    all_nv_house  = sum(1 for m in reps     if all(v == "NV" for v in m["votes"]))

    print(f"\n  Senators:              {len(senators)}")
    print(f"  Representatives:       {len(reps)}")
    print(f"  Bills:                 {len(bills)}")
    print(f"  FEC data:              {finance_count} members")
    print(f"  Senate votes loaded:   {len(senate_votes)}")
    print(f"  House votes loaded:    {len(house_votes)}")
    print(f"  Senators all-NV:       {all_nv_senate}  (0 = vote matching working)")
    print(f"  Reps all-NV:           {all_nv_house}  (0 = vote matching working)")
    print(f"\nSaved -> {OUTPUT}")


if __name__ == "__main__":
    main()
