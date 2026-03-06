"""
VoteWatch USA — Daily Data Fetcher

Data sources:
  1. Congress.gov API       — members (split by chamber via terms field), bills
  2. unitedstates/congress-legislators — LIS ID ↔ bioguide ID mapping for Senate
  3. Senate.gov XML         — Senate roll call votes (keyed by lis_member_id)
  4. Clerk.House.gov XML    — House roll call votes (keyed by name-id / bioguide)
  5. FEC cache              — reads data/fec_cache.json (updated weekly)

Senate vote matching:
  Senate XML uses lis_member_id, NOT bioguide ID.
  We fetch the unitedstates/congress-legislators current-legislators JSON to
  build a lis_id → bioguide_id lookup table.

House vote matching:
  House XML uses name-id which IS the bioguide ID.
  We discover the latest roll number by counting down from a high number.

Required env vars:
  CONGRESS_API_KEY  — https://api.congress.gov/sign-up/

Run manually:  python fetch_data.py
Auto-runs via: .github/workflows/daily-fetch.yml
"""

import os
import json
import re
import time
import xml.etree.ElementTree as ET
import urllib.request
import urllib.error
from datetime import datetime, timezone

CONGRESS_KEY  = os.environ.get("CONGRESS_API_KEY", "")
CONGRESS_BASE = "https://api.congress.gov/v3"
SENATE_BASE   = "https://www.senate.gov/legislative/LIS"
HOUSE_BASE    = "https://clerk.house.gov/evs"

# unitedstates/congress-legislators — current members with all IDs
LEGISLATORS_URL = (
    "https://unitedstates.github.io/congress-legislators/"
    "legislators-current.json"
)

OUTPUT    = os.path.join(os.path.dirname(__file__), "data", "data.json")
FEC_CACHE = os.path.join(os.path.dirname(__file__), "data", "fec_cache.json")

CONGRESS_NUM  = 119
VOTES_WANTED  = 5


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def http_get_text(url, label=""):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "VoteWatch/1.0"})
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  [WARN] {label or url[:80]} -> {e}")
        return None


def http_get_json(url, label=""):
    text = http_get_text(url, label)
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        print(f"  [WARN] JSON parse error for {label or url[:60]}: {e}")
        return None


def congress_get(path, params=None):
    base = "format=json&limit=250"
    if params:
        base += "&" + params
    url = f"{CONGRESS_BASE}{path}?{base}"
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
    if p in ("YEA", "YES", "AYE", "AY"):   return "YEA"
    if p in ("NAY", "NO", "NAYE"):          return "NAY"
    if "PRESENT" in p:                      return "PRESENT"
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
# 1. LIS ID → Bioguide ID mapping (for Senate vote matching)
# ─────────────────────────────────────────────────────────────

def fetch_lis_to_bioguide_map():
    """
    Fetch the unitedstates/congress-legislators JSON and build:
      { lis_id: bioguide_id }
    Also returns last_name → bioguide_id as a fallback.
    """
    print("  Fetching LIS ID → bioguide mapping ...")
    data = http_get_json(LEGISLATORS_URL, "congress-legislators")
    if not data:
        print("  [WARN] Could not fetch legislators — Senate vote matching will use name fallback")
        return {}, {}

    lis_map  = {}
    name_map = {}
    for leg in data:
        ids      = leg.get("id") or {}
        bio_id   = ids.get("bioguide") or ""
        lis_id   = ids.get("lis") or ""
        name     = (leg.get("name") or {})
        last     = (name.get("last") or "").upper()
        if bio_id and lis_id:
            lis_map[lis_id] = bio_id
        if bio_id and last:
            name_map[last] = bio_id

    print(f"  -> {len(lis_map)} LIS→bioguide mappings loaded")
    return lis_map, name_map


# ─────────────────────────────────────────────────────────────
# 2. CONGRESS.GOV — All members, split by chamber
# ─────────────────────────────────────────────────────────────

def member_chamber(m):
    terms = m.get("terms") or []
    if isinstance(terms, dict):
        terms = terms.get("item") or []
    if not terms:
        return None
    last    = terms[-1] if isinstance(terms, list) else terms
    chamber = (last.get("chamber") or "").strip()
    if chamber == "Senate":                   return "Senate"
    if chamber == "House of Representatives": return "House"
    return None


def fetch_all_members():
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
        total = (data.get("pagination") or {}).get("count", 0)
        if len(members) >= total or not page:
            break
        offset += 250
        time.sleep(0.1)

    print(f"  -> {len(members)} total members fetched")

    senate, house = [], []
    for m in members:
        ch = member_chamber(m)
        if ch == "Senate":  senate.append(m)
        elif ch == "House": house.append(m)

    print(f"  -> {len(senate)} senators, {len(house)} representatives")
    return senate, house


# ─────────────────────────────────────────────────────────────
# 3. SENATE.GOV XML — Recent Senate roll call votes
# ─────────────────────────────────────────────────────────────

def fetch_senate_vote_list(session, limit=10):
    url  = f"{SENATE_BASE}/roll_call_lists/vote_menu_{CONGRESS_NUM}_{session}.xml"
    text = http_get_text(url, f"Senate vote list session {session}")
    if not text:
        return []
    try:
        root  = ET.fromstring(text)
        votes = []
        for v in root.findall(".//vote"):
            num  = (v.findtext("vote_number") or "").strip()
            date = (v.findtext("vote_date")   or "").strip()
            q    = (v.findtext("question")    or "").strip()
            doc  = v.find("document")
            title = ""
            if doc is not None:
                title = (doc.findtext("document_title") or
                         doc.findtext("title") or "")
            title = title or q or f"Vote {num}"
            votes.append({
                "vote_number": num.zfill(5),
                "session":     str(session),
                "title":       title,
                "date":        date,
            })
        votes.sort(key=lambda x: x["vote_number"], reverse=True)
        return votes[:limit]
    except ET.ParseError as e:
        print(f"  [WARN] Senate vote list parse error: {e}")
        return []


def fetch_senate_vote_detail(vote_number, session, lis_map, name_map):
    """
    Fetch one Senate vote XML and return { bioguide_id: position }.
    Uses lis_map (lis_id→bioguide) with name_map as fallback.
    """
    num = str(vote_number).zfill(5)
    url = (
        f"{SENATE_BASE}/roll_call_votes/"
        f"vote{CONGRESS_NUM}{session}/"
        f"vote_{CONGRESS_NUM}_{session}_{num}.xml"
    )
    text = http_get_text(url, f"Senate vote {vote_number}")
    if not text:
        return {}
    try:
        root      = ET.fromstring(text)
        positions = {}
        for member in root.findall(".//member"):
            lis_id    = (member.findtext("lis_member_id") or "").strip()
            last_name = (member.findtext("last_name")     or "").strip().upper()
            vote_cast = (member.findtext("vote_cast")     or "").strip()

            # Resolve to bioguide: try lis_id first, then last name
            bio_id = lis_map.get(lis_id) or name_map.get(last_name) or ""
            if bio_id:
                positions[bio_id] = normalize_vote(vote_cast)
        return positions
    except ET.ParseError as e:
        print(f"  [WARN] Senate vote {vote_number} parse error: {e}")
        return {}


def fetch_senate_votes(lis_map, name_map):
    print("  Fetching Senate votes (senate.gov XML) ...")
    vote_list = []

    for session in [2, 1]:
        vote_list = fetch_senate_vote_list(session, limit=VOTES_WANTED + 5)
        if vote_list:
            print(f"  -> {len(vote_list)} Senate votes in list (session {session})")
            break

    if not vote_list:
        print("  [WARN] No Senate votes found")
        return [], []

    votes_detailed   = []
    vote_bill_labels = []

    for v in vote_list[:VOTES_WANTED]:
        positions = fetch_senate_vote_detail(
            v["vote_number"], v["session"], lis_map, name_map
        )
        votes_detailed.append({"meta": v, "positions": positions})
        vote_bill_labels.append({
            "id":    v["vote_number"].lstrip("0") or "0",
            "title": v["title"][:80],
            "date":  v["date"][:10] if len(v["date"]) >= 10 else v["date"],
        })
        time.sleep(0.1)

    matched = sum(1 for vd in votes_detailed if len(vd["positions"]) > 0)
    print(f"  -> {len(votes_detailed)} Senate votes loaded, {matched} with member positions")
    return votes_detailed, vote_bill_labels


# ─────────────────────────────────────────────────────────────
# 4. CLERK.HOUSE.GOV XML — Recent House roll call votes
# ─────────────────────────────────────────────────────────────

def find_latest_house_roll(year=None):
    """
    Discover the latest roll number for a given year by binary-search-style
    probing: start high and count down until we find one that exists.
    """
    if year is None:
        year = datetime.now().year
    # Start from a generous upper bound and work down
    for roll in range(500, 0, -1):
        num = str(roll).zfill(3)
        url = f"{HOUSE_BASE}/{year}/roll{num}.xml"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "VoteWatch/1.0"})
            urllib.request.urlopen(req, timeout=10).close()
            return roll
        except urllib.error.HTTPError as e:
            if e.code == 404:
                continue
            break
        except Exception:
            break
    return None


def fetch_house_vote_detail(year, roll_number):
    num  = str(roll_number).zfill(3)
    url  = f"{HOUSE_BASE}/{year}/roll{num}.xml"
    text = http_get_text(url, f"House vote {year}-{roll_number}")
    if not text:
        return {}, {}
    try:
        root          = ET.fromstring(text)
        vote_question = (root.findtext(".//vote-question") or "").strip()
        legis_name    = (root.findtext(".//legis-name")    or "").strip()
        action_date   = (root.findtext(".//action-date")   or "").strip()
        title         = legis_name or vote_question or f"Roll {roll_number}"

        meta = {
            "vote_number": str(roll_number),
            "title":       title[:80],
            "date":        action_date,
        }

        positions = {}
        for legislator in root.findall(".//recorded-vote/legislator"):
            # name-id IS the bioguide ID for House members
            bio_id = (legislator.get("name-id") or "").strip()
            vote   = (legislator.get("vote")    or "").strip()
            if bio_id:
                positions[bio_id] = normalize_vote(vote)

        return positions, meta
    except ET.ParseError as e:
        print(f"  [WARN] House vote {year}-{roll_number} parse error: {e}")
        return {}, {}


def fetch_house_votes():
    print("  Fetching House votes (clerk.house.gov XML) ...")
    year         = datetime.now().year
    latest_roll  = find_latest_house_roll(year)

    # If early in year and no rolls yet, try previous year
    if latest_roll is None and year > 2025:
        year        = year - 1
        latest_roll = find_latest_house_roll(year)

    if latest_roll is None:
        print("  [WARN] Could not find any House roll call votes")
        return [], []

    print(f"  -> Latest House roll: {year}-{latest_roll}, fetching top {VOTES_WANTED}")

    votes_detailed   = []
    vote_bill_labels = []
    roll             = latest_roll

    while len(votes_detailed) < VOTES_WANTED and roll > 0:
        positions, meta = fetch_house_vote_detail(year, roll)
        if meta:
            votes_detailed.append({"meta": meta, "positions": positions})
            vote_bill_labels.append({
                "id":    meta["vote_number"],
                "title": meta["title"],
                "date":  meta["date"],
            })
            time.sleep(0.05)
        roll -= 1

    print(f"  -> {len(votes_detailed)} House votes loaded")
    return votes_detailed, vote_bill_labels


# ─────────────────────────────────────────────────────────────
# 5. CONGRESS.GOV — Bills
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
# 6. FEC cache
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
# 7. Assemble members
# ─────────────────────────────────────────────────────────────

def calc_participation(bio_id, votes_detailed):
    total = len(votes_detailed)
    if total == 0:
        return 85
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
        state    = m.get("stateCode") or m.get("state") or "?"
        if len(state) > 2:
            state = state[:2].upper()
        district = str(m.get("district") or "")
        bio_id   = m.get("bioguideId") or m.get("memberId") or ""

        participation = calc_participation(bio_id, votes_detailed)

        votes = [vd["positions"].get(bio_id, "NV") for vd in votes_detailed]
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

    # ── 0. LIS ID mapping (needed for Senate vote matching) ──
    print("[0/6] Fetching LIS → bioguide ID mapping ...")
    lis_map, name_map = fetch_lis_to_bioguide_map()

    # ── 1. Members ────────────────────────────────────────────
    print("\n[1/6] Congress.gov members ...")
    senate_raw, house_raw = fetch_all_members()

    # ── 2. Senate votes ───────────────────────────────────────
    print("\n[2/6] Senate roll call votes (senate.gov XML) ...")
    senate_votes, senate_vote_labels = fetch_senate_votes(lis_map, name_map)

    # ── 3. House votes ────────────────────────────────────────
    print("\n[3/6] House roll call votes (clerk.house.gov XML) ...")
    house_votes, house_vote_labels = fetch_house_votes()

    # ── 4. Bills ──────────────────────────────────────────────
    print("\n[4/6] Congress.gov bills ...")
    bills = fetch_bills()

    # ── 5. FEC + assemble ─────────────────────────────────────
    print("\n[5/6] Loading FEC cache ...")
    fec_data = load_fec_cache()

    print("\n[6/6] Assembling output ...")
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

    finance_count    = sum(1 for m in senators + reps if m["finance"]["total_raised"] != "N/A")
    sen_all_nv       = sum(1 for m in senators if all(v == "NV" for v in m["votes"]))
    rep_all_nv       = sum(1 for m in reps     if all(v == "NV" for v in m["votes"]))
    sen_part_nonzero = sum(1 for m in senators if m["participation"] not in (0, 85))
    rep_part_nonzero = sum(1 for m in reps     if m["participation"] not in (0, 85))

    print(f"\n  Senators:                    {len(senators)}")
    print(f"  Representatives:             {len(reps)}")
    print(f"  Bills:                       {len(bills)}")
    print(f"  FEC data:                    {finance_count} members")
    print(f"  Senate votes loaded:         {len(senate_votes)}")
    print(f"  House votes loaded:          {len(house_votes)}")
    print(f"  Senators all-NV:             {sen_all_nv}  (target: 0)")
    print(f"  Reps all-NV:                 {rep_all_nv}  (target: 0)")
    print(f"  Senators real participation: {sen_part_nonzero}  (target: ~100)")
    print(f"  Reps real participation:     {rep_part_nonzero}  (target: ~438)")
    print(f"\nSaved -> {OUTPUT}")


if __name__ == "__main__":
    main()
