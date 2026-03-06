"""
VoteWatch USA — Daily Data Fetcher
Fetches from three sources:
  1. Congress.gov   — members, votes, bills (sponsors + cosponsors)
  2. FEC API        — PAC / outside money totals per member (no key needed)
  3. OpenSecrets    — top donor industries per member (requires free key)

Required env vars:
  CONGRESS_API_KEY    — from https://api.congress.gov/sign-up/
  OPENSECRETS_API_KEY — from https://www.opensecrets.org/api/admin/index.php
                        (free, takes ~1 business day to receive)

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

CONGRESS_KEY     = os.environ.get("CONGRESS_API_KEY", "")
OPENSECRETS_KEY  = os.environ.get("OPENSECRETS_API_KEY", "")

CONGRESS_BASE    = "https://api.congress.gov/v3"
FEC_BASE         = "https://api.open.fec.gov/v1"
OPENSECRETS_BASE = "https://www.opensecrets.org/api"

OUTPUT = os.path.join(os.path.dirname(__file__), "data", "data.json")

# Current election cycle (FEC uses 2-year cycles ending in even years)
FEC_CYCLE = 2024


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def http_get(url, label=""):
    """Fetch a URL and return parsed JSON, or {} on failure."""
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


def normalize_party(raw):
    r = (raw or "").strip().upper()
    if r.startswith("D"): return "D"
    if r.startswith("R"): return "R"
    return "I"


def normalize_vote(pos):
    p = (pos or "").upper()
    if p in ("YEA", "YES", "AYE"): return "YEA"
    if p in ("NAY", "NO", "NAYE"): return "NAY"
    if "PRESENT" in p:             return "PRESENT"
    return "NV"


def fmt_usd(n):
    """Format a number as $1.2M / $450K / $12K."""
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
# 1. CONGRESS.GOV — Members
# ─────────────────────────────────────────────────────────────

def fetch_members(chamber):
    print(f"  Fetching {chamber} members ...")
    data = congress_get("/member", f"&chamber={chamber}&congress=119")
    members = data.get("members") or []

    nxt = (data.get("pagination") or {}).get("next", "")
    if nxt:
        try:
            req2 = urllib.request.Request(
                nxt + "&format=json",
                headers={"X-Api-Key": CONGRESS_KEY, "User-Agent": "VoteWatch/1.0"}
            )
            with urllib.request.urlopen(req2, timeout=20) as r:
                page2 = json.loads(r.read())
                members += page2.get("members") or []
        except Exception as e:
            print(f"  [WARN] Page 2 -> {e}")

    print(f"  -> {len(members)} {chamber} members")
    return members


# ─────────────────────────────────────────────────────────────
# 2. CONGRESS.GOV — Recent votes
# ─────────────────────────────────────────────────────────────

def fetch_recent_votes(chamber):
    print(f"  Fetching {chamber} votes ...")
    data = congress_get(f"/vote/{chamber.lower()}/119", "&limit=10")
    votes = data.get("votes") or []
    print(f"  -> {len(votes)} {chamber} votes")
    return votes


# ─────────────────────────────────────────────────────────────
# 3. CONGRESS.GOV — Bills with sponsor + cosponsors
# ─────────────────────────────────────────────────────────────

def fetch_bills():
    print("  Fetching recent bills ...")
    data = congress_get("/bill/119", "&sort=updateDate+desc&limit=20")
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
            detail      = congress_get(f"/bill/119/{bill_type.lower()}/{bill_number}")
            bill_detail = detail.get("bill") or {}

            sponsors_list = bill_detail.get("sponsors") or []
            if sponsors_list:
                sp = sponsors_list[0]
                sponsor_name  = sp.get("fullName") or sp.get("name") or "Unknown"
                sponsor_party = normalize_party(sp.get("party") or "")
                sponsor_state = sp.get("state") or "?"

            cosponsor_data = congress_get(
                f"/bill/119/{bill_type.lower()}/{bill_number}/cosponsors",
                "&limit=10"
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
                f"https://www.congress.gov/bill/119th-congress/"
                f"{bill_type.lower()}-bill/{bill_number}"
            ),
        })

    print(f"  -> {len(bills)} bills (with sponsors)")
    return bills


# ─────────────────────────────────────────────────────────────
# 4. FEC API — PAC / outside money totals per candidate
# ─────────────────────────────────────────────────────────────

def fetch_fec_totals(bio_id_to_name):
    """
    Query FEC for each member by name.
    Returns { bioguide_id: { pac_total, individual_total, total_raised, fec_url } }
    No API key required for basic use (uses DEMO_KEY for higher limits).
    Rate limit: 1000 req/hour unauthenticated.
    """
    print(f"  Fetching FEC finance data for {len(bio_id_to_name)} members ...")
    results = {}
    count   = 0

    for bio_id, name in bio_id_to_name.items():
        q   = urllib.parse.quote(name)
        url = (
            f"{FEC_BASE}/candidates/search/"
            f"?q={q}&election_year={FEC_CYCLE}&per_page=3&sort=-total_receipts"
            f"&api_key=DEMO_KEY"
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
            f"?candidate_id={cand_id}&cycle={FEC_CYCLE}&api_key=DEMO_KEY"
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
# 5. OpenSecrets — Top donor industries per member
# ─────────────────────────────────────────────────────────────

def fetch_opensecrets_cids(bioguide_ids):
    """Map bioguide IDs to OpenSecrets CIDs via getLegislators (one API call)."""
    if not OPENSECRETS_KEY:
        print("  [SKIP] OPENSECRETS_API_KEY not set -- skipping industry data")
        return {b: None for b in bioguide_ids}

    print("  Fetching OpenSecrets CID mapping ...")
    url = (
        f"{OPENSECRETS_BASE}/?method=getLegislators"
        f"&id=&output=json&apikey={OPENSECRETS_KEY}"
    )
    data        = http_get(url, "OpenSecrets getLegislators")
    legislators = (
        (data.get("response") or {})
        .get("legislators") or {}
    ).get("legislator") or []

    cid_map = {}
    for leg in legislators:
        attrs = leg.get("@attributes") or leg
        bio   = attrs.get("bioguide_id") or ""
        cid   = attrs.get("cid") or ""
        if bio:
            cid_map[bio] = cid

    result = {b: cid_map.get(b) for b in bioguide_ids}
    found  = sum(1 for v in result.values() if v)
    print(f"  -> CIDs found for {found}/{len(bioguide_ids)} members")
    return result


def fetch_opensecrets_industries(cid_map):
    """Returns { bioguide_id: [ {name, total, indivs, pacs}, ... ] }"""
    if not OPENSECRETS_KEY:
        return {}

    print(f"  Fetching OpenSecrets industry data ...")
    results = {}

    for bio_id, cid in cid_map.items():
        if not cid:
            continue
        url = (
            f"{OPENSECRETS_BASE}/?method=candIndustry"
            f"&cid={cid}&cycle={FEC_CYCLE}"
            f"&output=json&apikey={OPENSECRETS_KEY}"
        )
        data         = http_get(url, f"OpenSecrets industries: {cid}")
        industries_raw = (
            (data.get("response") or {})
            .get("industries") or {}
        ).get("industry") or []

        industries = []
        for ind in industries_raw[:6]:
            attrs = ind.get("@attributes") or ind
            industries.append({
                "name":   attrs.get("industry_name") or attrs.get("industry_code") or "Unknown",
                "total":  fmt_usd(attrs.get("total", 0)),
                "indivs": fmt_usd(attrs.get("indivs", 0)),
                "pacs":   fmt_usd(attrs.get("pacs", 0)),
            })

        results[bio_id] = industries
        time.sleep(0.2)

    print(f"  -> OpenSecrets industry data for {len(results)} members")
    return results


# ─────────────────────────────────────────────────────────────
# 6. Assemble member objects
# ─────────────────────────────────────────────────────────────

def map_members(raw_members, recent_votes, fec_data, industry_data):
    out = []
    for m in raw_members:
        name = (
            m.get("name") or
            " ".join(filter(None, [m.get("firstName"), m.get("lastName")]))
        ).strip()
        party         = normalize_party(m.get("partyName") or m.get("party") or "")
        state         = m.get("state") or m.get("stateCode") or "?"
        district      = str(m.get("district") or "")
        bio_id        = m.get("bioguideId") or m.get("memberId") or ""
        missed        = m.get("missedVotesPct")
        participation = round(100 - float(missed)) if missed is not None else 85

        votes = []
        for v in recent_votes[:5]:
            pos_list  = v.get("members") or []
            pos_entry = next(
                (p for p in pos_list
                 if (p.get("bioguideId") or p.get("memberId")) == bio_id),
                None
            )
            votes.append(normalize_vote(
                (pos_entry or {}).get("votePosition") or
                (pos_entry or {}).get("position") or ""
            ))
        while len(votes) < 5:
            votes.append("NV")

        fec    = fec_data.get(bio_id)
        indust = industry_data.get(bio_id) or []

        out.append({
            "name":          name,
            "party":         party,
            "state":         state,
            "district":      district,
            "votes":         votes,
            "participation": participation,
            "id":            bio_id,
            "congress_url":  m.get("url") or f"https://www.congress.gov/member/{bio_id}",
            "finance": {
                "pac_total":        (fec or {}).get("pac_total",        "N/A"),
                "individual_total": (fec or {}).get("individual_total", "N/A"),
                "total_raised":     (fec or {}).get("total_raised",     "N/A"),
                "fec_url":          (fec or {}).get("fec_url",          ""),
                "top_industries":   indust,
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

    print("[1/5] Congress.gov members & votes")
    senate_raw   = fetch_members("Senate")
    house_raw    = fetch_members("House")
    senate_votes = fetch_recent_votes("Senate")
    house_votes  = fetch_recent_votes("House")

    print("\n[2/5] Congress.gov bills (sponsors + cosponsors)")
    bills = fetch_bills()

    all_raw     = senate_raw + house_raw
    bio_to_name = {}
    for m in all_raw:
        bio  = m.get("bioguideId") or m.get("memberId") or ""
        name = (
            m.get("name") or
            " ".join(filter(None, [m.get("firstName"), m.get("lastName")]))
        ).strip()
        if bio and name:
            bio_to_name[bio] = name

    print("\n[3/5] FEC campaign finance totals")
    fec_data = fetch_fec_totals(bio_to_name)

    print("\n[4/5] OpenSecrets donor industries")
    cid_map       = fetch_opensecrets_cids(list(bio_to_name.keys()))
    industry_data = fetch_opensecrets_industries(cid_map)

    print("\n[5/5] Assembling output ...")
    senators = map_members(senate_raw, senate_votes, fec_data, industry_data)
    reps     = map_members(house_raw,  house_votes,  fec_data, industry_data)

    def vote_bill_labels(votes):
        return [{
            "id":    str(v.get("voteNumber") or v.get("rollNumber") or "?"),
            "title": v.get("question") or v.get("title") or "Vote",
            "date":  (v.get("date") or "")[:10],
        } for v in votes[:5]]

    payload = {
        "updated_at":        datetime.now(timezone.utc).isoformat(),
        "senators":          senators,
        "representatives":   reps,
        "bills":             bills,
        "senate_vote_bills": vote_bill_labels(senate_votes),
        "house_vote_bills":  vote_bill_labels(house_votes),
    }

    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    finance_count = sum(
        1 for m in senators + reps
        if m["finance"]["total_raised"] != "N/A"
    )
    industry_count = sum(
        1 for m in senators + reps
        if m["finance"]["top_industries"]
    )
    print(f"\n  Senators:        {len(senators)}")
    print(f"  Representatives: {len(reps)}")
    print(f"  Bills:           {len(bills)}")
    print(f"  FEC data:        {finance_count} members")
    print(f"  Industry data:   {industry_count} members")
    print(f"\nSaved -> {OUTPUT}")


if __name__ == "__main__":
    main()
