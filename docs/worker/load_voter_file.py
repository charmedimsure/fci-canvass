#!/usr/bin/env python3
"""
FCI FieldMap — Fresh Voter File Loader (Fairfield County)
Reads Ohio SOS voter export, groups into households, scores, uploads to D1.

Usage:
    python load_voter_file.py [path_to_voter_file.txt]
"""

import csv, sys, time, hashlib, requests
from collections import defaultdict

# ── CONFIG ────────────────────────────────────────────────────────────────────
API_URL    = 'https://fci-canvass.fci-canvass.workers.dev'
API_KEY    = 'ohiofcicanvass7312'
ADMIN_KEY  = '2026ad#min#oh#fci#LA'
COUNTY_NUM = '23'
BATCH_SIZE = 20
INPUT_FILE = sys.argv[1] if len(sys.argv) > 1 else 'FAIRFIELD__5_.txt'
# ──────────────────────────────────────────────────────────────────────────────

HEADERS       = {'X-FCI-Key': API_KEY, 'Content-Type': 'application/json'}
ADMIN_HEADERS = {**HEADERS, 'X-FCI-Admin': ADMIN_KEY}

# ── Scoring election lists ────────────────────────────────────────────────────
# Recent generals — weighted most heavily (1.5 pts each, max 6)
RECENT_GENERALS = [
    'GENERAL-11/05/2024',
    'GENERAL-11/04/2025',
    'GENERAL-11/08/2022',
    'GENERAL-11/07/2023',
]
# Mid-tier generals (0.5 pts each)
MID_GENERALS = [
    'GENERAL-11/08/2016',
    'GENERAL-11/06/2018',
    'GENERAL-11/03/2020',
    'GENERAL-11/02/2021',
]
# Recent primaries for last_primary tracking
RECENT_PRIMARIES = [
    'PRIMARY-05/05/2026',
    'PRIMARY-09/09/2025',
    'PRIMARY-05/06/2025',
    'PRIMARY-03/19/2024',
    'PRIMARY-05/02/2023',
    'PRIMARY-05/03/2022',
    'PRIMARY-05/04/2021',
    'PRIMARY-03/17/2020',
    'PRIMARY-05/07/2019',
    'PRIMARY-05/08/2018',
]
# All elections used for vote count
ALL_SCORED = RECENT_GENERALS + MID_GENERALS + RECENT_PRIMARIES

PARTY_MAP = {'D':'D','R':'R','L':'L','G':'G','N':'N','':''}


def make_id(addr, city, zip5):
    key = f"{addr.upper()}|{city.upper()}|{zip5}".encode()
    return hashlib.md5(key).hexdigest()[:16]

def address_key(row):
    return (
        row['RESIDENTIAL_ADDRESS1'].strip().upper(),
        row['RESIDENTIAL_CITY'].strip().upper(),
        row['RESIDENTIAL_ZIP'].strip()[:5],
    )

def last_primary(row):
    for col in RECENT_PRIMARIES:
        if row.get(col,'').strip() == 'X':
            return col.replace('PRIMARY-','')
    return ''

def vote_count(row):
    return sum(1 for c in ALL_SCORED if row.get(c,'').strip() == 'X')

def score_voter(row):
    s = 0.0

    # Active status
    if row.get('VOTER_STATUS','').strip().upper() == 'ACTIVE':
        s += 1.0

    # Recent generals — 1.5 pts each, capped at 6
    recent_voted = sum(1 for g in RECENT_GENERALS if row.get(g,'').strip() == 'X')
    s += min(recent_voted * 1.5, 6.0)

    # Consistency bonus — voted 3+ of last 4 recent generals
    if recent_voted >= 3:
        s += 0.5

    # Mid-tier generals — 0.5 pts each
    for g in MID_GENERALS:
        if row.get(g,'').strip() == 'X':
            s += 0.5

    # Issue voter bonus — voted Nov 2023 (abortion/marijuana) but skipped Nov 2022 midterm
    # These are issue-motivated unaffiliated voters, high persuasion value
    voted_2023 = row.get('GENERAL-11/07/2023','').strip() == 'X'
    voted_2022 = row.get('GENERAL-11/08/2022','').strip() == 'X'
    if voted_2023 and not voted_2022:
        s += 1.0

    # Recent primary bonus
    if row.get('PRIMARY-05/05/2026','').strip() == 'X':
        s += 1.0
    elif row.get('PRIMARY-09/09/2025','').strip() == 'X':
        s += 0.75
    elif row.get('PRIMARY-05/06/2025','').strip() == 'X':
        s += 0.75
    elif row.get('PRIMARY-03/19/2024','').strip() == 'X':
        s += 0.5
    elif row.get('PRIMARY-05/02/2023','').strip() == 'X':
        s += 0.25

    # Party bonus
    if row.get('PARTY_AFFILIATION','').strip().upper() == 'D':
        s += 1.0

    return min(round(s), 10)

def build_vns(row):
    return [
        row['FIRST_NAME'].strip(),
        row['LAST_NAME'].strip(),
        row['MIDDLE_NAME'].strip(),
        row['SUFFIX'].strip(),
        row['DATE_OF_BIRTH'].strip(),
        PARTY_MAP.get(row['PARTY_AFFILIATION'].strip(), ''),
        last_primary(row),
        vote_count(row),
        row['SOS_VOTERID'].strip(),
        row['VOTER_STATUS'].strip(),
    ]

def build_household(addr_tuple, rows):
    addr, city, zip5 = addr_tuple
    r0  = rows[0]
    sec = r0['RESIDENTIAL_SECONDARY_ADDR'].strip()
    full_addr = f"{r0['RESIDENTIAL_ADDRESS1'].strip()} {sec}".strip() if sec else r0['RESIDENTIAL_ADDRESS1'].strip()

    pc = defaultdict(int)
    for r in rows:
        p = r.get('PARTY_AFFILIATION','').strip()
        if p: pc[p] += 1
    hh_party = max(pc, key=pc.get) if pc else ''

    return {
        'id':           make_id(r0['RESIDENTIAL_ADDRESS1'].strip(), city, zip5),
        'a':            full_addr,
        'city':         r0['RESIDENTIAL_CITY'].strip(),
        'state':        r0['RESIDENTIAL_STATE'].strip(),
        'zip':          zip5,
        'precinct':     r0['PRECINCT_CODE'].strip(),
        'precinctName': r0['PRECINCT_NAME'].strip(),
        'pCode':        r0['PRECINCT_CODE'].strip(),
        'congDist':     r0['CONGRESSIONAL_DISTRICT'].strip(),
        'stHouse':      r0['STATE_REPRESENTATIVE_DISTRICT'].strip(),
        'stSenate':     r0['STATE_SENATE_DISTRICT'].strip(),
        'township':     r0['TOWNSHIP'].strip(),
        'village':      r0['VILLAGE'].strip(),
        'ward':         r0['WARD'].strip(),
        'party':        hh_party,
        'score':        max(score_voter(r) for r in rows),
        'vns':          [build_vns(r) for r in rows],
        'mailOnly':     False,
        'lat':          None,
        'lon':          None,
        'countyNum':    COUNTY_NUM,
        # True if any household member voted Nov 2023 (Issue 1/2) but skipped Nov 2022
        'issueVoter':   any(
            r.get('GENERAL-11/07/2023','').strip()=='X' and r.get('GENERAL-11/08/2022','').strip()!='X'
            for r in rows
        ),
    }


# ── READ ──────────────────────────────────────────────────────────────────────
print(f"Reading {INPUT_FILE}...")
by_addr = defaultdict(list)
total   = 0

with open(INPUT_FILE, newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        total += 1
        if total % 25000 == 0:
            print(f"  Read {total:,} rows...", end='\r')
        if row.get('COUNTY_NUMBER','').strip() == COUNTY_NUM or row.get('COUNTY_NUMBER','').strip() == '':
            by_addr[address_key(row)].append(row)

households   = [build_household(k, v) for k, v in by_addr.items()]
total_voters = sum(len(hh['vns']) for hh in households)
print(f"\nTotal rows     : {total:,}")
print(f"FC households  : {len(households):,}")
print(f"FC voters      : {total_voters:,}")

# Quick score distribution check
from collections import Counter
score_dist = Counter(hh['score'] for hh in households)
print("\nScore distribution:")
for s in sorted(score_dist):
    bar = '█' * (score_dist[s] // 200)
    print(f"  {s:2d}: {score_dist[s]:6,}  {bar}")

# ── UPLOAD ────────────────────────────────────────────────────────────────────
print(f"\nUploading to D1 (replace=True on first batch)...")
pushed = 0
errors = 0

for i in range(0, len(households), BATCH_SIZE):
    chunk        = households[i:i + BATCH_SIZE]
    replace_flag = (i == 0)
    r = requests.post(
        f"{API_URL}/api/admin/load-voters",
        headers=ADMIN_HEADERS,
        json={'voters': chunk, 'replace': replace_flag},
        timeout=120,
    )
    if r.ok:
        pushed += len(chunk)
    else:
        half = len(chunk) // 2
        for sub in [chunk[:half], chunk[half:]]:
            if not sub: continue
            r2 = requests.post(
                f"{API_URL}/api/admin/load-voters",
                headers=ADMIN_HEADERS,
                json={'voters': sub, 'replace': False},
                timeout=120,
            )
            if r2.ok:
                pushed += len(sub)
            else:
                print(f"\n  ❌ {r2.status_code}: {r2.text[:120]}")
                errors += 1
        time.sleep(0.2)

    pct = min(i + BATCH_SIZE, len(households)) / len(households) * 100
    print(f"  {pushed:,}/{len(households):,} ({pct:.1f}%)  errors={errors}   ", end='\r')
    time.sleep(0.15)

print(f"\n\n{'✅' if errors == 0 else '⚠️ '} Done.")
print(f"  Pushed : {pushed:,} households")
print(f"  Voters : {total_voters:,}")
print(f"  Errors : {errors}")
