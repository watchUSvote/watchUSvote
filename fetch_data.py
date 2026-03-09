"""
VoteWatch USA — Daily Data Fetcher

Reads pre-fetched local files saved by fetch_votes.py (runs 1hr earlier):
  data/votes/senate/  — Senate roll call XMLs
  data/votes/house/   — House roll call JSONs
  data/bills/         — recent, committee, upcoming bill JSONs
  data/fec_cache.json — FEC finance cache (updated weekly)

Also fetches live from Congress.gov:
  - Member list (chamber split via terms field)
  - LIS ID → bioguide ID mapping (for Senate vote matching)

Required env vars:
  CONGRESS_API_KEY  — https://api.congress.gov/sign-up/

Run manually:  python fetch_data.py
Auto-runs via: .github/workflows/daily-fetch.yml (after fetch-votes.yml)
"""

import os
import json
import time
import xml.etree.ElementTree as ET
import urllib.request
import urllib.error
from datetime import datetime, timezone

CONGRESS_KEY  = os.environ.get("CONGRESS_API_KEY", "")
CONGRESS_BASE = "https://api.congress.gov/v3"

LEGISLATORS_URL = (
    "https://unitedstates.github.io/congress-legislators/"
    "legislators-current.json"
)

DATA_DIR      = os.path.join(os.path.dirname(__file__), "data")
OUTPUT        = os.path.join(DATA_DIR, "data.json")
FEC_CACHE     = os.path.join(DATA_DIR, "fec_cache.json")
VOTES_SENATE  = os.path.join(DATA_DIR, "votes", "senate")
VOTES_HOUSE   = os.path.join(DATA_DIR, "votes", "house")
BILLS_DIR     = os.path.join(DATA_DIR, "bills")

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
# 3. LOCAL FILES — Senate votes (written by fetch_votes.py)
# ─────────────────────────────────────────────────────────────

def parse_senate_xml(text, lis_map, name_map):
    """Parse Senate vote XML and return { bioguide_id: position }."""
    try:
        root      = ET.fromstring(text)
        positions = {}
        for member in root.findall(".//member"):
            lis_id    = (member.findtext("lis_member_id") or "").strip()
            last_name = (member.findtext("last_name")     or "").strip().upper()
            vote_cast = (member.findtext("vote_cast")     or "").strip()
            bio_id    = lis_map.get(lis_id) or name_map.get(last_name) or ""
            if bio_id:
                positions[bio_id] = normalize_vote(vote_cast)
        return positions
    except ET.ParseError:
        return {}


def load_senate_votes(lis_map, name_map):
    """
    Read the N most recent Senate vote XMLs from data/votes/senate/.
    Returns (votes_detailed, vote_bill_labels).
    """
    print(f"  Reading Senate vote XMLs from {VOTES_SENATE} ...")
    if not os.path.isdir(VOTES_SENATE):
        print("  [WARN] data/votes/senate/ not found — run fetch_votes.py first")
        return [], []

    files = sorted(
        [f for f in os.listdir(VOTES_SENATE) if f.endswith(".xml")],
        reverse=True
    )[:VOTES_WANTED]

    if not files:
        print("  [WARN] No Senate vote XMLs found")
        return [], []

    votes_detailed   = []
    vote_bill_labels = []

    for fname in files:
        fpath = os.path.join(VOTES_SENATE, fname)
        with open(fpath, encoding="utf-8") as f:
            text = f.read()

        positions = parse_senate_xml(text, lis_map, name_map)

        # Extract meta from filename: vote_119_2_00087.xml
        parts = fname.replace(".xml", "").split("_")
        session     = parts[2] if len(parts) > 2 else "?"
        vote_number = parts[3] if len(parts) > 3 else "?"

        # Try to get title from XML
        try:
            root  = ET.fromstring(text)
            title = (root.findtext(".//question") or
                     root.findtext(".//document_title") or
                     f"Vote {vote_number}")
            date  = (root.findtext(".//vote_date") or "")[:10]
        except ET.ParseError:
            title = f"Vote {vote_number}"
            date  = ""

        meta = {"vote_number": vote_number, "session": session,
                "title": title, "date": date}
        votes_detailed.append({"meta": meta, "positions": positions})
        vote_bill_labels.append({
            "id":    vote_number.lstrip("0") or "0",
            "title": title[:80],
            "date":  date,
        })

    matched = sum(1 for vd in votes_detailed if vd["positions"])
    print(f"  -> {len(votes_detailed)} Senate votes loaded, {matched} with positions")
    return votes_detailed, vote_bill_labels


# ─────────────────────────────────────────────────────────────
# 4. LOCAL FILES — House votes (written by fetch_votes.py)
# ─────────────────────────────────────────────────────────────

def load_house_votes():
    """
    Read the N most recent House vote JSONs from data/votes/house/.
    Returns (votes_detailed, vote_bill_labels).
    """
    print(f"  Reading House vote JSONs from {VOTES_HOUSE} ...")
    if not os.path.isdir(VOTES_HOUSE):
        print("  [NOTE] data/votes/house/ not found — House votes unavailable")
        return [], []

    files = sorted(
        [f for f in os.listdir(VOTES_HOUSE) if f.endswith(".json")],
        reverse=True
    )[:VOTES_WANTED]

    if not files:
        print("  [NOTE] No House vote data yet — 119th Congress not in API, check back later")
        return [], []

    votes_detailed   = []
    vote_bill_labels = []

    for fname in files:
        fpath = os.path.join(VOTES_HOUSE, fname)
        with open(fpath, encoding="utf-8") as f:
            data = json.load(f)

        meta    = data.get("meta") or {}
        members = data.get("members") or []

        positions = {}
        for m in members:
            bio_id = (
                (m.get("member") or {}).get("bioguideId") or
                m.get("bioguideId") or ""
            ).strip()
            pos = m.get("votePosition") or m.get("position") or ""
            if bio_id:
                positions[bio_id] = normalize_vote(pos)

        votes_detailed.append({"meta": meta, "positions": positions})
        vote_bill_labels.append({
            "id":    meta.get("vote_number", "?"),
            "title": (meta.get("title") or "Vote")[:80],
            "date":  meta.get("date", ""),
        })

    matched = sum(1 for vd in votes_detailed if vd["positions"])
    print(f"  -> {len(votes_detailed)} House votes loaded, {matched} with positions")
    return votes_detailed, vote_bill_labels


# ─────────────────────────────────────────────────────────────
# 5. LOCAL FILES — Bills (written by fetch_votes.py)
# ─────────────────────────────────────────────────────────────

def load_bills():
    """
    Read bill data from local JSON files saved by fetch_votes.py.
    Returns { recent, committee, upcoming } dicts.
    """
    result = {"recent": [], "committee": [], "upcoming": {"house_bills": [], "senate_bills": []}}

    for key, fname in [("recent", "recent.json"), ("committee", "committee.json"), ("upcoming", "upcoming.json")]:
        fpath = os.path.join(BILLS_DIR, fname)
        if not os.path.exists(fpath):
            print(f"  [WARN] {fname} not found — run fetch_votes.py first")
            continue
        with open(fpath, encoding="utf-8") as f:
            data = json.load(f)
        if key == "upcoming":
            result[key] = data
        else:
            result[key] = data.get("bills") or []

    print(f"  -> {len(result['recent'])} recent, {len(result['committee'])} committee, "
          f"{len(result['upcoming'].get('house_bills', []))} house + "
          f"{len(result['upcoming'].get('senate_bills', []))} senate upcoming bills loaded")
    return result


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
    found   = sum(1 for v in data.values() if v)
    nones   = sum(1 for v in data.values() if v is None)
    print(f"  -> FEC cache loaded ({len(data)} members, last updated {updated[:10]})")
    print(f"     With FEC data: {found}, No FEC match (None): {nones}")
    if found:
        sample_key = next(k for k, v in data.items() if v)
        print(f"     Sample key: {sample_key} → {data[sample_key]}")
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

    # ── 0. LIS ID mapping (Senate vote matching) ──────────────
    print("[0/6] Fetching LIS → bioguide ID mapping ...")
    lis_map, name_map = fetch_lis_to_bioguide_map()

    # ── 1. Members (live from Congress.gov) ───────────────────
    print("\n[1/6] Congress.gov members ...")
    senate_raw, house_raw = fetch_all_members()

    # ── 2. Senate votes (from local cache) ────────────────────
    print("\n[2/6] Senate votes (local cache) ...")
    senate_votes, senate_vote_labels = load_senate_votes(lis_map, name_map)

    # ── 3. House votes (from local cache) ─────────────────────
    print("\n[3/6] House votes (local cache) ...")
    house_votes, house_vote_labels = load_house_votes()

    # ── 4. Bills (from local cache) ───────────────────────────
    print("\n[4/6] Bills (local cache) ...")
    bills_data = load_bills()

    # ── 5. FEC cache ──────────────────────────────────────────
    print("\n[5/6] Loading FEC cache ...")
    fec_data = load_fec_cache()

    # ── 6. Assemble ───────────────────────────────────────────
    print("\n[6/6] Assembling output ...")
    senators = map_members(senate_raw, senate_votes, fec_data)
    reps     = map_members(house_raw,  house_votes,  fec_data)

    payload = {
        "updated_at":        datetime.now(timezone.utc).isoformat(),
        "senators":          senators,
        "representatives":   reps,
        "bills":             bills_data["recent"],
        "bills_committee":   bills_data["committee"],
        "bills_upcoming":    bills_data["upcoming"],
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

    upcoming = bills_data["upcoming"]
    print(f"\n  Senators:                    {len(senators)}")
    print(f"  Representatives:             {len(reps)}")
    print(f"  Bills (recent):              {len(bills_data['recent'])}")
    print(f"  Bills (committee):           {len(bills_data['committee'])}")
    print(f"  Bills (upcoming House):      {len(upcoming.get('house_bills', []))}")
    print(f"  Bills (upcoming Senate):     {len(upcoming.get('senate_bills', []))}")
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
