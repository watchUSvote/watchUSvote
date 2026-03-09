"""
VoteWatch USA — Vote & Bill Cache Fetcher

Saves raw data into the repo so fetch_data.py never hits live URLs directly.
Run daily at 5am UTC (one hour before fetch_data.py).

Saves:
  data/votes/senate/vote_119_{session}_{NNNNN}.xml  — one per Senate vote
  data/votes/house/vote_119_{session}_{NNN}.json    — one per House vote
  data/bills/recent.json                            — 50 most recently updated
  data/bills/committee.json                         — passed committee recently
  data/bills/upcoming.json                          — floor schedules (both chambers)

Pruning: any file in data/votes/ or data/bills/ older than 30 days is deleted.

Required env vars:
  CONGRESS_API_KEY  — https://api.congress.gov/sign-up/

Run manually:  python fetch_votes.py
Auto-runs via: .github/workflows/fetch-votes.yml
"""

import os
import json
import time
import xml.etree.ElementTree as ET
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta

CONGRESS_KEY  = os.environ.get("CONGRESS_API_KEY", "")
CONGRESS_BASE = "https://api.congress.gov/v3"
SENATE_BASE   = "https://www.senate.gov/legislative/LIS"
DOCS_HOUSE    = "https://docs.house.gov"

CONGRESS_NUM  = 119
VOTES_TO_KEEP = 10   # fetch latest N votes per chamber (keep rolling 30 days by date)
BILLS_RECENT  = 50
BILLS_COMMITTEE = 30
PRUNE_DAYS    = 30

DATA_DIR        = os.path.join(os.path.dirname(__file__), "data")
VOTES_SENATE    = os.path.join(DATA_DIR, "votes", "senate")
VOTES_HOUSE     = os.path.join(DATA_DIR, "votes", "house")
BILLS_DIR       = os.path.join(DATA_DIR, "bills")


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


def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def save_text(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def prune_old_files(directory, days=PRUNE_DAYS):
    """Delete files older than `days` days in directory."""
    if not os.path.isdir(directory):
        return 0
    cutoff = datetime.now() - timedelta(days=days)
    pruned = 0
    for fname in os.listdir(directory):
        fpath = os.path.join(directory, fname)
        if os.path.isfile(fpath):
            mtime = datetime.fromtimestamp(os.path.getmtime(fpath))
            if mtime < cutoff:
                os.remove(fpath)
                pruned += 1
    return pruned


# ─────────────────────────────────────────────────────────────
# 1. SENATE VOTES — save raw XML files
# ─────────────────────────────────────────────────────────────

def fetch_and_save_senate_votes():
    print("  Fetching Senate vote list ...")
    os.makedirs(VOTES_SENATE, exist_ok=True)
    saved = 0

    for session in [2, 1]:
        list_url = f"{SENATE_BASE}/roll_call_lists/vote_menu_{CONGRESS_NUM}_{session}.xml"
        text     = http_get_text(list_url, f"Senate vote list session {session}")
        if not text:
            continue

        try:
            root  = ET.fromstring(text)
            votes = root.findall(".//vote")
            votes_sorted = sorted(
                votes,
                key=lambda v: (v.findtext("vote_number") or "0").zfill(5),
                reverse=True
            )

            for v in votes_sorted[:VOTES_TO_KEEP]:
                num = (v.findtext("vote_number") or "").strip().zfill(5)
                if not num:
                    continue

                fname = f"vote_{CONGRESS_NUM}_{session}_{num}.xml"
                fpath = os.path.join(VOTES_SENATE, fname)

                # Skip if already cached today
                if os.path.exists(fpath):
                    today = datetime.now().date()
                    mtime = datetime.fromtimestamp(os.path.getmtime(fpath)).date()
                    if mtime == today:
                        saved += 1
                        continue

                # Fetch individual vote XML
                vote_url = (
                    f"{SENATE_BASE}/roll_call_votes/"
                    f"vote{CONGRESS_NUM}{session}/"
                    f"vote_{CONGRESS_NUM}_{session}_{num}.xml"
                )
                vote_xml = http_get_text(vote_url, f"Senate vote {num}")
                if vote_xml:
                    save_text(fpath, vote_xml)
                    saved += 1
                time.sleep(0.1)

            print(f"  -> {saved} Senate vote XMLs saved (session {session})")
            break  # Use whichever session has votes

        except ET.ParseError as e:
            print(f"  [WARN] Senate vote list parse error: {e}")
            continue

    return saved


# ─────────────────────────────────────────────────────────────
# 2. HOUSE VOTES — save via Congress.gov API as JSON
# ─────────────────────────────────────────────────────────────

def fetch_and_save_house_votes():
    print("  Fetching House votes via Congress.gov API ...")
    os.makedirs(VOTES_HOUSE, exist_ok=True)
    saved = 0

    # Try 119th Congress first (sessions 2 then 1), fall back to 118th
    attempts = [
        (CONGRESS_NUM, 2),
        (CONGRESS_NUM, 1),
        (118, 2),
        (118, 1),
    ]

    for congress, session in attempts:
        data  = congress_get(
            f"/vote/{congress}/house/{session}",
            f"limit={VOTES_TO_KEEP}&sort=date+desc"
        )
        votes = data.get("votes") or []
        if not votes:
            continue

        label = f"{congress}th Congress" if congress != CONGRESS_NUM else f"session {session}"
        print(f"  -> {len(votes)} House votes found ({label})")
        if congress != CONGRESS_NUM:
            print(f"  [NOTE] Using 118th Congress House votes as fallback — 119th not yet in API")

        for v in votes[:VOTES_TO_KEEP]:
            roll = v.get("voteNumber") or v.get("rollNumber") or ""
            if not roll:
                continue

            roll_str = str(roll).zfill(3)
            fname    = f"vote_{congress}_{session}_{roll_str}.json"
            fpath    = os.path.join(VOTES_HOUSE, fname)

            # Skip if already cached today
            if os.path.exists(fpath):
                today = datetime.now().date()
                mtime = datetime.fromtimestamp(os.path.getmtime(fpath)).date()
                if mtime == today:
                    saved += 1
                    continue

            # Fetch member positions
            members_data = congress_get(
                f"/vote/{congress}/house/{session}/{roll}/members"
            )
            members = members_data.get("members") or []

            payload = {
                "meta": {
                    "vote_number": str(roll),
                    "session":     str(session),
                    "congress":    str(congress),
                    "fallback":    congress != CONGRESS_NUM,
                    "title":       (v.get("question") or v.get("title") or f"Vote {roll}")[:80],
                    "date":        (v.get("date") or "")[:10],
                },
                "members": members,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
            save_json(fpath, payload)
            saved += 1
            time.sleep(0.1)

        break  # Stop at first congress/session that has votes

    print(f"  -> {saved} House vote JSONs saved")
    return saved


# ─────────────────────────────────────────────────────────────
# 3. BILLS — recent (50 most recently updated)
# ─────────────────────────────────────────────────────────────

def fetch_and_save_recent_bills():
    print(f"  Fetching {BILLS_RECENT} recent bills ...")
    bills = []
    offset = 0

    while len(bills) < BILLS_RECENT:
        data      = congress_get(
            f"/bill/{CONGRESS_NUM}",
            f"sort=updateDate+desc&limit=50&offset={offset}"
        )
        page = data.get("bills") or []
        if not page:
            break

        for b in page:
            bill_type   = b.get("type", "")
            bill_number = b.get("number", "")
            latest      = b.get("latestAction") or {}

            # Fetch sponsor detail
            sponsor_name  = "Unknown"
            sponsor_party = "?"
            sponsor_state = "?"
            cosponsors    = []

            if bill_type and bill_number:
                detail      = congress_get(f"/bill/{CONGRESS_NUM}/{bill_type.lower()}/{bill_number}")
                bill_detail = detail.get("bill") or {}
                for sp in (bill_detail.get("sponsors") or [])[:1]:
                    sponsor_name  = sp.get("fullName") or sp.get("name") or "Unknown"
                    sponsor_party = sp.get("party") or "?"
                    sponsor_state = sp.get("state") or "?"
                cosponsor_data = congress_get(
                    f"/bill/{CONGRESS_NUM}/{bill_type.lower()}/{bill_number}/cosponsors",
                    "limit=10"
                )
                for cs in (cosponsor_data.get("cosponsors") or [])[:8]:
                    cosponsors.append({
                        "name":  cs.get("fullName") or cs.get("name") or "Unknown",
                        "party": cs.get("party") or "?",
                        "state": cs.get("state") or "?",
                    })
                time.sleep(0.1)

            bills.append({
                "id":            f"{bill_type}.{bill_number}".strip("."),
                "title":         b.get("title") or b.get("shortTitle") or "Untitled",
                "date":          (latest.get("actionDate") or b.get("updateDate") or "")[:10],
                "latest_action": latest.get("text") or "",
                "status":        "active",
                "sponsor":       sponsor_name,
                "sponsor_party": sponsor_party,
                "sponsor_state": sponsor_state,
                "cosponsors":    cosponsors,
                "url":           b.get("url") or (
                    f"https://www.congress.gov/bill/{CONGRESS_NUM}th-congress/"
                    f"{bill_type.lower()}-bill/{bill_number}"
                ),
            })

            if len(bills) >= BILLS_RECENT:
                break

        offset += 50
        if len(page) < 50:
            break

    fpath = os.path.join(BILLS_DIR, "recent.json")
    save_json(fpath, {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "bills":      bills[:BILLS_RECENT],
    })
    print(f"  -> {len(bills[:BILLS_RECENT])} recent bills saved")


# ─────────────────────────────────────────────────────────────
# 4. BILLS — passed committee recently
# ─────────────────────────────────────────────────────────────

def fetch_and_save_committee_bills():
    """
    Fetch bills that have recently passed out of committee.
    Congress.gov action type 'ReportedToHouse' or 'ReportedToSenate'
    indicates a bill was reported out of committee.
    """
    print("  Fetching committee-passed bills ...")
    bills = []

    for chamber_action in ["ReportedToHouse", "ReportedToSenate"]:
        data = congress_get(
            f"/bill/{CONGRESS_NUM}",
            f"sort=updateDate+desc&limit={BILLS_COMMITTEE}&actionCode={chamber_action}"
        )
        for b in data.get("bills") or []:
            bill_type   = b.get("type", "")
            bill_number = b.get("number", "")
            latest      = b.get("latestAction") or {}

            sponsor_name  = "Unknown"
            sponsor_party = "?"
            sponsor_state = "?"

            if bill_type and bill_number:
                detail      = congress_get(f"/bill/{CONGRESS_NUM}/{bill_type.lower()}/{bill_number}")
                bill_detail = detail.get("bill") or {}
                for sp in (bill_detail.get("sponsors") or [])[:1]:
                    sponsor_name  = sp.get("fullName") or sp.get("name") or "Unknown"
                    sponsor_party = sp.get("party") or "?"
                    sponsor_state = sp.get("state") or "?"
                time.sleep(0.1)

            bills.append({
                "id":            f"{bill_type}.{bill_number}".strip("."),
                "title":         b.get("title") or b.get("shortTitle") or "Untitled",
                "date":          (latest.get("actionDate") or b.get("updateDate") or "")[:10],
                "latest_action": latest.get("text") or "",
                "status":        "reported_from_committee",
                "chamber":       "House" if "House" in chamber_action else "Senate",
                "sponsor":       sponsor_name,
                "sponsor_party": sponsor_party,
                "sponsor_state": sponsor_state,
                "url":           b.get("url") or (
                    f"https://www.congress.gov/bill/{CONGRESS_NUM}th-congress/"
                    f"{bill_type.lower()}-bill/{bill_number}"
                ),
            })

    fpath = os.path.join(BILLS_DIR, "committee.json")
    save_json(fpath, {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "bills":      bills,
    })
    print(f"  -> {len(bills)} committee-passed bills saved")


# ─────────────────────────────────────────────────────────────
# 5. UPCOMING — House floor schedule (docs.house.gov XML)
# ─────────────────────────────────────────────────────────────

def fetch_house_floor_schedule():
    """
    Fetch the weekly House floor schedule XML from docs.house.gov.
    URL pattern: docs.house.gov/billsthisweek/{YYYYMMDD}/floorschedule.xml
    Tries current week's Monday, then next Monday, then previous Monday as fallback.
    """
    today   = datetime.now()
    mondays = []

    # Current week Monday
    current_monday = today - timedelta(days=today.weekday())
    mondays.append(current_monday)

    # Next Monday (if today is Friday/weekend, schedule may already be posted)
    mondays.append(current_monday + timedelta(weeks=1))

    # Previous Monday as fallback
    mondays.append(current_monday - timedelta(weeks=1))

    bills = []
    for monday in mondays:
        date_str = monday.strftime("%Y%m%d")
        url      = f"{DOCS_HOUSE}/billsthisweek/{date_str}/floorschedule.xml"
        text     = http_get_text(url, f"House floor schedule {date_str}")
        if not text:
            continue

        try:
            root = ET.fromstring(text)
            for item in root.findall(".//*[@bill-number]"):
                bill_num    = item.get("bill-number") or ""
                bill_type   = item.get("bill-type") or ""
                title       = item.findtext("legis-name") or item.findtext("title") or bill_num
                description = item.findtext("floor-text") or item.findtext("description") or ""
                bills.append({
                    "id":          f"{bill_type}.{bill_num}".strip(".") if bill_type else bill_num,
                    "title":       title[:120],
                    "description": description[:200],
                    "chamber":     "House",
                    "week_of":     monday.strftime("%Y-%m-%d"),
                    "url":         f"https://www.congress.gov/bill/{CONGRESS_NUM}th-congress/house-bill/{bill_num}" if bill_num else "",
                })
            if bills:
                print(f"    House floor schedule found for week of {monday.strftime('%Y-%m-%d')}")
                break
        except ET.ParseError as e:
            print(f"  [WARN] House floor schedule XML parse error: {e}")
            continue

    return bills


# ─────────────────────────────────────────────────────────────
# 6. UPCOMING — Senate floor schedule (senate.gov XML)
# ─────────────────────────────────────────────────────────────

def fetch_senate_floor_schedule():
    """
    Fetch the Senate floor schedule XML from senate.gov.
    URL: https://www.senate.gov/legislative/schedule/floor_schedule.xml
    The XML contains schedule items with start/end times and descriptions.
    """
    url  = "https://www.senate.gov/legislative/schedule/floor_schedule.xml"
    text = http_get_text(url, "Senate floor schedule")

    bills = []
    if not text:
        return bills

    try:
        root = ET.fromstring(text)
        # Try multiple possible element structures
        items = (
            root.findall(".//floor_action") or
            root.findall(".//item") or
            root.findall(".//meeting") or
            list(root)  # fallback: top-level children
        )
        for item in items:
            # Extract any text content as description
            description = " ".join(
                (t.strip() for t in item.itertext() if t.strip())
            )[:200]
            if not description:
                continue

            # Try to find a date field
            date = ""
            for tag in ["start", "date", "meeting_date", "action_date"]:
                val = item.findtext(tag) or item.get(tag) or ""
                if val:
                    date = val[:10]
                    break

            bills.append({
                "id":          "",
                "title":       description[:120],
                "description": description,
                "chamber":     "Senate",
                "date":        date,
                "url":         "",
            })

    except ET.ParseError as e:
        print(f"  [WARN] Senate floor schedule XML parse error: {e}")

    return bills


def fetch_and_save_upcoming_bills():
    print("  Fetching upcoming floor schedules ...")

    house_bills  = fetch_house_floor_schedule()
    senate_bills = fetch_senate_floor_schedule()
    all_bills    = house_bills + senate_bills

    fpath = os.path.join(BILLS_DIR, "upcoming.json")
    save_json(fpath, {
        "fetched_at":   datetime.now(timezone.utc).isoformat(),
        "house_bills":  house_bills,
        "senate_bills": senate_bills,
        "total":        len(all_bills),
    })
    print(f"  -> {len(house_bills)} House + {len(senate_bills)} Senate upcoming bills saved")


# ─────────────────────────────────────────────────────────────
# 7. PRUNING — delete files older than 30 days
# ─────────────────────────────────────────────────────────────

def prune_all():
    print("  Pruning old files ...")
    total = 0
    for directory in [VOTES_SENATE, VOTES_HOUSE]:
        pruned = prune_old_files(directory)
        if pruned:
            print(f"    Pruned {pruned} files from {os.path.basename(directory)}/")
        total += pruned
    print(f"  -> {total} old files pruned")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    if not CONGRESS_KEY:
        print("ERROR: CONGRESS_API_KEY is not set.")
        raise SystemExit(1)

    print("=== VoteWatch Vote & Bill Cache Fetch ===\n")

    print("[1/6] Senate vote XMLs ...")
    fetch_and_save_senate_votes()

    print("\n[2/6] House vote JSONs ...")
    fetch_and_save_house_votes()

    print("\n[3/6] Recent bills (50) ...")
    fetch_and_save_recent_bills()

    print("\n[4/6] Committee-passed bills ...")
    fetch_and_save_committee_bills()

    print("\n[5/6] Upcoming floor schedules ...")
    fetch_and_save_upcoming_bills()

    print("\n[6/6] Pruning old files ...")
    prune_all()

    print(f"\nDone. Files saved to data/votes/ and data/bills/")


if __name__ == "__main__":
    main()
