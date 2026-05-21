#!/usr/bin/env python3
"""
FCI FieldMap — Ohio SOS Voter Migration Script (CD-12 Full District)
Reads Ohio SOS voter export, groups into households, geocodes Fairfield County,
and uploads all parties/counties to the worker API.

Usage:
    python migrate_csv.py \
        --files CONGRESSIONAL_DISTRICT_12.txt \
        --url https://fci-canvass.fci-canvass.workers.dev \
        --key ohiofcicanvass7312 \
        --admin-key "2026ad#min#oh#fci#LA"

    # Dry run (no upload, shows sample record):
    python migrate_csv.py --files CONGRESSIONAL_DISTRICT_12.txt ... --dry-run

    # Skip geocoding (faster, for testing):
    python migrate_csv.py --files CONGRESSIONAL_DISTRICT_12.txt ... --skip-geocode
"""

import argparse
import csv
import hashlib
import json
import os
import re
import sys
import time
import glob
import urllib.request
import urllib.error
import io
from collections import defaultdict

# ── Presidential election years (for presOnly flag) ──────────────────────────
PRES_YEARS = {2024, 2020, 2016, 2012, 2008}

# ── Ohio SOS county number → county name ─────────────────────────────────────
COUNTY_NAMES = {
    '1':  'Adams',       '2':  'Allen',       '3':  'Ashland',
    '4':  'Ashtabula',   '5':  'Athens',      '6':  'Auglaize',
    '7':  'Belmont',     '8':  'Brown',       '9':  'Butler',
    '10': 'Carroll',     '11': 'Champaign',   '12': 'Clark',
    '13': 'Clermont',    '14': 'Clinton',     '15': 'Columbiana',
    '16': 'Coshocton',   '17': 'Crawford',    '18': 'Cuyahoga',
    '19': 'Darke',       '20': 'Defiance',    '21': 'Delaware',
    '22': 'Erie',        '23': 'Fairfield',   '24': 'Fayette',
    '25': 'Franklin',    '26': 'Fulton',      '27': 'Gallia',
    '28': 'Geauga',      '29': 'Greene',      '30': 'Guernsey',
    '31': 'Hamilton',    '32': 'Hancock',     '33': 'Hardin',
    '34': 'Harrison',    '35': 'Henry',       '36': 'Highland',
    '37': 'Hocking',     '38': 'Holmes',      '39': 'Huron',
    '40': 'Jackson',     '41': 'Jefferson',   '42': 'Knox',
    '43': 'Lake',        '44': 'Lawrence',    '45': 'Licking',
    '46': 'Logan',       '47': 'Lorain',      '48': 'Lucas',
    '49': 'Madison',     '50': 'Mahoning',    '51': 'Marion',
    '52': 'Medina',      '53': 'Meigs',       '54': 'Mercer',
    '55': 'Miami',       '56': 'Monroe',      '57': 'Montgomery',
    '58': 'Morgan',      '59': 'Morrow',      '60': 'Muskingum',
    '61': 'Noble',       '62': 'Ottawa',      '63': 'Paulding',
    '64': 'Perry',       '65': 'Pickaway',    '66': 'Pike',
    '67': 'Portage',     '68': 'Preble',      '69': 'Putnam',
    '70': 'Richland',    '71': 'Ross',        '72': 'Sandusky',
    '73': 'Scioto',      '74': 'Seneca',      '75': 'Shelby',
    '76': 'Stark',       '77': 'Summit',      '78': 'Trumbull',
    '79': 'Tuscarawas',  '80': 'Union',       '81': 'Van Wert',
    '82': 'Vinton',      '83': 'Warren',      '84': 'Washington',
    '85': 'Wayne',       '86': 'Williams',    '87': 'Wood',
    '88': 'Wyandot',
}

# ── Fairfield County number (only county geocoded for door-to-door) ───────────
GEOCODE_COUNTY = '23'


# ─────────────────────────────────────────────────────────────────────────────
# FIELD EXTRACTION HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def s(val):
    """Safely coerce a value (including NaN/None) to a stripped string."""
    if val is None:
        return ''
    v = str(val).strip()
    return '' if v.lower() == 'nan' else v


def parse_vote_year(col):
    """Extract year from a column name like 'GENERAL-11/03/2020' or 'PRIMARY-03/17/2020'."""
    m = re.search(r'(\d{4})', col)
    return int(m.group(1)) if m else None


def normalize_address(row):
    addr = s(row.get('RESIDENTIAL_ADDRESS1', ''))
    if not addr:
        # Legacy fallback
        parts = [s(row.get('STNUM', '')), s(row.get('STDIR', '')), s(row.get('STNAME', ''))]
        addr = ' '.join(p for p in parts if p)
        apt = s(row.get('APT', ''))
        if apt:
            addr += ' APT ' + apt
    else:
        apt = s(row.get('RESIDENTIAL_SECONDARY_ADDR', ''))
        if apt:
            addr += ' ' + apt
    addr = addr.upper()
    # Normalize LOT → APT LOT so trailer park residents group as one household
    addr = re.sub(r'\s+LOT\s+(\S+)', r' APT LOT \1', addr)
    return addr


def normalize_city(row):
    return s(row.get('RESIDENTIAL_CITY', '') or row.get('CITY', '')).upper()


def get_party(row):
    return s(row.get('PARTY_AFFILIATION', '') or row.get('PARTYAFFIL', '')).upper()


def get_status(row):
    return s(row.get('VOTER_STATUS', '') or row.get('VOTERSTAT', '')).upper()


def get_zip(row):
    return s(row.get('RESIDENTIAL_ZIP', '') or row.get('ZIP', ''))[:5]


def get_names(row):
    fn = s(row.get('FIRST_NAME', '') or row.get('FIRSTN', '')).title()
    ln = s(row.get('LAST_NAME',  '') or row.get('LASTN',  '')).title()
    mn = s(row.get('MIDDLE_NAME', '')).title()
    sf = s(row.get('SUFFIX', '')).title()
    return fn, ln, mn, sf


def get_dob(row):
    """Return full DOB string and birth year int."""
    dob = s(row.get('DATE_OF_BIRTH', '') or row.get('BIRTHYEAR', ''))
    year = None
    try:
        year = int(dob[:4]) if len(dob) >= 4 and dob[:4].isdigit() else None
    except Exception:
        pass
    return dob, year


def get_sos_voterid(row):
    return s(row.get('SOS_VOTERID', '') or row.get('SOSID', ''))


def get_county_num(row):
    return s(row.get('COUNTY_NUMBER', '') or row.get('CNTYIDNUM', ''))


def get_districts(row):
    """Extract all available district/jurisdiction fields."""
    return {
        'stHouse':           s(row.get('STATE_REPRESENTATIVE_DISTRICT', '') or row.get('OH HOUSE', '') or row.get('STATE_HOUSE', '')),
        'stSenate':          s(row.get('STATE_SENATE_DISTRICT', '')         or row.get('OH SENATE', '') or row.get('STATE_SENATE', '')),
        'congDist':          s(row.get('CONGRESSIONAL_DISTRICT', '')        or row.get('US CONG', '') or row.get('US_CONGRESS', '')),
        'precinctName':      s(row.get('PRECINCT_NAME', '')  or row.get('PRECNAME', '')).upper(),
        'precinctCode':      s(row.get('PRECINCT_CODE', '')).upper(),
        'township':          s(row.get('TOWNSHIP', '')).upper(),
        'municipality':      s(row.get('CITY', '') or row.get('RESIDENTIAL_CITY', '')).upper(),
        'village':           s(row.get('VILLAGE', '')).upper(),
        'ward':              s(row.get('WARD', '') or row.get('CITY WARD', '')).upper(),
        'localSchool':       s(row.get('LOCAL_SCHOOL_DISTRICT', '')   or row.get('SCHOOL DISTRICT', '')),
        'citySchool':        s(row.get('CITY_SCHOOL_DISTRICT', '')),
        'careerCenter':      s(row.get('CAREER_CENTER', '')),
        'library':           s(row.get('LIBRARY', '')),
        'countyCourtDist':   s(row.get('COUNTY_COURT_DISTRICT', '')),
        'courtOfAppeals':    s(row.get('COURT_OF_APPEALS', '')),
        'eduServiceCenter':  s(row.get('EDU_SERVICE_CENTER_DISTRICT', '')),
        'exemptedVillSchool': s(row.get('EXEMPTED_VILL_SCHOOL_DISTRICT', '')),
        'municipalCourt':    s(row.get('MUNICIPAL_COURT_DISTRICT', '')),
        'stateBoardEd':      s(row.get('STATE_BOARD_OF_EDUCATION', '')),
        'countyNum':         get_county_num(row),
    }


def get_mailing_address(row, res_addr, city, zip5):
    """Return mailing address if different from residential, else None."""
    m1 = s(row.get('MAILING_ADDRESS1', '') or row.get('MADDR1', '')).upper()
    m2 = s(row.get('MAILING_SECONDARY_ADDRESS', '') or row.get('MADDR2', '')).upper()
    mc = s(row.get('MAILING_CITY', '') or row.get('MCITY', '')).upper()
    mz = s(row.get('MAILING_ZIP', '') or row.get('MZIP', ''))[:5]
    if not m1 or m1 == res_addr.split(',')[0].strip():
        return None
    full = m1
    if m2:
        full += ' ' + m2
    full += ', ' + (mc or city)
    if mz:
        full += ' ' + mz
    return full or None


def household_key(addr, city, zip5):
    """Each unique full address (including unit) = one household."""
    return (addr.strip().upper(), city.strip().upper(), str(zip5 or '').strip()[:5])


def make_id(display_name, addr, city):
    raw = f"{display_name}|{addr}|{city}".lower()
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def is_lot_address(addr):
    return bool(re.search(r'\bLOT\b', addr.upper()))


# ─────────────────────────────────────────────────────────────────────────────
# PROCESSING
# ─────────────────────────────────────────────────────────────────────────────

def process_files(csv_files):
    """Read one or more Ohio SOS voter files. Keeps ALL active voters, all parties."""
    all_voters = []
    skipped_inactive = 0

    for filepath in csv_files:
        print(f"\nReading {os.path.basename(filepath)}...")
        try:
            with open(filepath, newline='', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                headers = reader.fieldnames
                if not headers:
                    print("  WARNING: No headers found, skipping")
                    continue

                # Detect election history columns automatically
                election_cols = []
                for h in headers:
                    yr = parse_vote_year(h)
                    if yr and yr >= 2000:
                        election_cols.append((h, yr))

                print(f"  Found {len(election_cols)} election history columns")
                print(f"  Newest election: {election_cols[-1][0] if election_cols else 'none'}")

                file_voters = 0
                for row in reader:
                    status = get_status(row)
                    if status not in ('A', 'ACTIVE'):
                        skipped_inactive += 1
                        continue
                    all_voters.append((row, election_cols))
                    file_voters += 1

                print(f"  {file_voters:,} active voters (all parties)")

        except Exception as e:
            print(f"  ERROR reading {filepath}: {e}")
            continue

    print(f"\nTotal active voters: {len(all_voters):,}")
    print(f"Skipped (inactive/purged): {skipped_inactive:,}")
    return all_voters


def build_households(raw_voters):
    """Group voters into households by address."""
    households = defaultdict(list)
    for (row, election_cols) in raw_voters:
        addr  = normalize_address(row)
        city  = normalize_city(row)
        zip5  = get_zip(row)
        key   = household_key(addr, city, zip5)
        households[key].append((row, election_cols, addr, city, zip5))
    print(f"Grouped {len(raw_voters):,} voters into {len(households):,} households")
    return households


def build_voter_record(hh_key, members):
    """Build a single household record from its members."""
    addr, city, zip5 = hh_key
    row0, election_cols, _, _, _ = members[0]

    # ── Names ────────────────────────────────────────────────────────────────
    last_names = []
    seen_last  = set()
    for (row, _, _, _, _) in members:
        _, ln, _, _ = get_names(row)
        if ln and ln not in seen_last:
            last_names.append(ln)
            seen_last.add(ln)
    display_name = '/'.join(last_names[:3]) or 'Unknown'

    # ── Parties ──────────────────────────────────────────────────────────────
    member_parties = [get_party(row) for (row, _, _, _, _) in members]
    unique_parties  = sorted(set(p for p in member_parties if p))
    parties_str     = ','.join(unique_parties)  # e.g. "D,R" or "R" or "D"

    # Household "dominant" party for backward-compat party field
    # Priority: D > I > G > L > R > ''
    PARTY_PRIORITY = {'D': 0, 'I': 1, 'G': 2, 'L': 3, 'R': 4}
    hh_party = min(unique_parties, key=lambda p: PARTY_PRIORITY.get(p, 99)) if unique_parties else ''

    # ── Vote history ─────────────────────────────────────────────────────────
    years_voted = set()
    for (row, election_cols, _, _, _) in members:
        for (col, yr) in election_cols:
            val = s(row.get(col, ''))
            if val and val not in ('0', 'N', 'NO'):
                years_voted.add(yr)

    yrs_sorted    = sorted(years_voted, reverse=True)
    yrs_str       = ','.join(str(y) for y in yrs_sorted)
    generals_6    = [2024, 2022, 2020, 2018, 2016, 2014]
    generals_voted = sum(1 for y in generals_6 if y in years_voted)

    # ── Last primary (most recent primary any member voted in) ────────────────
    primary_cols = sorted(
        [(col, yr) for (col, yr) in election_cols if col.upper().startswith('PRIMARY')],
        key=lambda x: x[1], reverse=True
    )
    last_primary      = ''
    last_primary_year = None
    for (row, _, _, _, _) in members:
        for (col, yr) in primary_cols:
            val = s(row.get(col, '')).upper()
            if val and val not in ('0', 'N', 'NO'):
                last_primary      = 'D' if val == 'D' else ('R' if val == 'R' else 'D')
                last_primary_year = yr
                break
        if last_primary:
            break

    # ── Per-member VNS array ─────────────────────────────────────────────────
    # Format: [firstName, lastName, age, presOnly, party, lastPrimary, voteCount, dob, sosVoterId]
    current_year = 2026
    vns = []
    for (row, election_cols, _, _, _) in members:
        fn, ln, mn, sf = get_names(row)
        dob_str, birth_year = get_dob(row)
        age     = current_year - birth_year if birth_year else None
        sos_id  = get_sos_voterid(row)
        m_party = get_party(row)

        member_years = set()
        for (col, yr) in election_cols:
            val = s(row.get(col, ''))
            if val and val not in ('0', 'N', 'NO'):
                member_years.add(yr)

        pres_only = bool(member_years) and all(y in PRES_YEARS for y in member_years)

        # Member's last primary
        m_last_primary = ''
        for (col, yr) in primary_cols:
            val = s(row.get(col, '')).upper()
            if val and val not in ('0', 'N', 'NO'):
                m_last_primary = 'D' if val == 'D' else ('R' if val == 'R' else 'D')
                break

        m_vote_count = sum(
            1 for (col, yr) in election_cols
            if s(row.get(col, '')) not in ('', '0', 'N', 'NO')
        )

        vns.append([
            fn, ln, age, pres_only,
            m_party, m_last_primary, m_vote_count,
            dob_str, sos_id
        ])

    ages_str = ','.join(str(v[2]) for v in vns if v[2] is not None)

    # ── Districts ─────────────────────────────────────────────────────────────
    dists      = get_districts(row0)
    county_num = dists['countyNum']
    county_name = COUNTY_NAMES.get(county_num, f'County {county_num}')

    # ── mailOnly flag ─────────────────────────────────────────────────────────
    # Non-Fairfield counties = mail only (no geocoding = no door canvass)
    # Also: LOT addresses and unusually large households
    mail_only = (
        county_num != GEOCODE_COUNTY or
        is_lot_address(addr) or
        len(members) > 8
    )

    # ── Full address string ───────────────────────────────────────────────────
    full_addr = addr
    if city:  full_addr += ', ' + city
    if zip5:  full_addr += ' ' + zip5

    # ── Mailing address ───────────────────────────────────────────────────────
    mail_addr = get_mailing_address(row0, addr, city, zip5)

    # ── Household ID ─────────────────────────────────────────────────────────
    vid = make_id(display_name, addr, city)

    return {
        # ── Identity ──────────────────────────────────────────────────────────
        'id':              vid,
        'n':               display_name,
        'a':               full_addr,
        'hh':              len(members),

        # ── Party ────────────────────────────────────────────────────────────
        'party':           hh_party,
        'pty':             hh_party,
        'parties':         parties_str,          # NEW: all unique parties e.g. "D,R"

        # ── Vote history ──────────────────────────────────────────────────────
        'yrs':             yrs_str,
        'yearsVoted':      yrs_str,
        'voting':          str(generals_voted),
        'lp':              last_primary,
        'lastPrimary':     last_primary,
        'lastPrimaryYear': last_primary_year,
        'ages':            ages_str,

        # ── Per-member data ───────────────────────────────────────────────────
        'vns':             vns,

        # ── Geocoding (populated later for Fairfield Co. only) ────────────────
        'lat':             None,
        'lon':             None,

        # ── Flags ────────────────────────────────────────────────────────────
        'mailOnly':        mail_only,

        # ── Mailing ──────────────────────────────────────────────────────────
        'mailAddr':        mail_addr,

        # ── Scores / donations (set later) ───────────────────────────────────
        'score':           None,
        'donations':       [],

        # ── Legislative districts ─────────────────────────────────────────────
        'stHouse':         dists['stHouse'],
        'stSenate':        dists['stSenate'],
        'congDist':        dists['congDist'],

        # ── Precinct ─────────────────────────────────────────────────────────
        'precinct':        dists['precinctCode'],
        'precinctName':    dists['precinctName'],
        'precinctCode':    dists['precinctCode'],

        # ── Local jurisdictions ───────────────────────────────────────────────
        'township':        dists['township'],
        'municipality':    dists['municipality'],
        'village':         dists['village'],
        'ward':            dists['ward'],
        'localSchool':     dists['localSchool'],
        'citySchool':      dists['citySchool'],
        'careerCenter':    dists['careerCenter'],
        'library':         dists['library'],
        'countyCourtDist': dists['countyCourtDist'],
        'courtOfAppeals':  dists['courtOfAppeals'],
        'eduServiceCenter': dists['eduServiceCenter'],
        'exemptedVillSchool': dists['exemptedVillSchool'],
        'municipalCourt':  dists['municipalCourt'],
        'stateBoardEd':    dists['stateBoardEd'],

        # ── County ───────────────────────────────────────────────────────────
        'countyNum':       county_num,
        'countyName':      county_name,
    }


# ─────────────────────────────────────────────────────────────────────────────
# GEOCODING (Fairfield County only via Census Geocoder batch API)
# ─────────────────────────────────────────────────────────────────────────────

def geocode_census_batch(records, batch_size=1000):
    results = {}
    total   = len(records)
    print(f"\nGeocoding {total:,} Fairfield County addresses via Census Geocoder...")

    for i in range(0, total, batch_size):
        batch     = records[i:i + batch_size]
        batch_num = i // batch_size + 1
        print(f"  Batch {batch_num}: rows {i+1}–{min(i+batch_size, total):,}...", end=' ', flush=True)

        buf = io.StringIO()
        for r in batch:
            buf.write(f'"{r["id"]}","{r["address"]}","{r["city"]}","OH","{r["zip"]}"\n')

        boundary = '----CensusBoundary7312'
        body = (
            f'--{boundary}\r\n'
            f'Content-Disposition: form-data; name="addressFile"; filename="addresses.csv"\r\n'
            f'Content-Type: text/plain\r\n\r\n'
            + buf.getvalue()
            + f'\r\n--{boundary}\r\n'
            f'Content-Disposition: form-data; name="benchmark"\r\n\r\n'
            f'Public_AR_Current\r\n'
            f'--{boundary}--\r\n'
        ).encode('utf-8')

        url = 'https://geocoding.geo.census.gov/geocoder/locations/addressbatch'
        req = urllib.request.Request(url, data=body, method='POST')
        req.add_header('Content-Type', f'multipart/form-data; boundary={boundary}')

        matched_batch = 0
        success = False
        for attempt in range(3):
            try:
                with urllib.request.urlopen(req, timeout=90) as resp:
                    text   = resp.read().decode('utf-8', errors='replace')
                    reader = csv.reader(io.StringIO(text))
                    for row in reader:
                        if len(row) >= 6 and row[2].strip().upper() == 'MATCH':
                            vid    = row[0].strip()
                            coords = row[5].strip()
                            if coords:
                                parts = coords.split(',')
                                if len(parts) == 2:
                                    try:
                                        lon, lat = float(parts[0]), float(parts[1])
                                        results[vid] = (lat, lon)
                                        matched_batch += 1
                                    except ValueError:
                                        pass
                success = True
                print(f"matched {matched_batch}/{len(batch)}")
                break
            except Exception as e:
                print(f"\n    attempt {attempt+1} failed: {e}")
                time.sleep(5)

        if not success:
            print(f"\n    SKIPPING batch {batch_num} after 3 attempts")

        time.sleep(0.5)  # be polite to Census API

    pct = len(results) / total * 100 if total else 0
    print(f"  Geocoded {len(results):,}/{total:,} addresses ({pct:.0f}% match rate)")
    return results


# ─────────────────────────────────────────────────────────────────────────────
# UPLOAD
# ─────────────────────────────────────────────────────────────────────────────

def upload_voters(voters, worker_url, api_key, admin_key, batch_size=200):
    url     = worker_url.rstrip('/') + '/api/admin/load-voters'
    headers = {
        'Content-Type': 'application/json',
        'X-FCI-Key':    api_key,
        'X-FCI-Admin':  admin_key,
        'User-Agent':   'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Origin':       worker_url,
        'Referer':      worker_url + '/',
    }

    total  = len(voters)
    loaded = 0
    errors = 0

    print(f"\nUploading {total:,} households in batches of {batch_size}...")
    print(f"  → {url}\n")

    for i in range(0, total, batch_size):
        batch    = voters[i:i + batch_size]
        batch_n  = i // batch_size + 1
        is_first = (i == 0)  # first batch triggers table replacement

        payload = json.dumps({'replace': is_first, 'voters': batch}).encode('utf-8')
        req     = urllib.request.Request(url, data=payload, headers=headers, method='POST')

        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                result  = json.loads(resp.read())
                loaded += result.get('loaded', len(batch))
                pct     = loaded / total * 100
                print(f"  Batch {batch_n:>4}: {loaded:>7,}/{total:,} ({pct:.0f}%)")
        except urllib.error.HTTPError as e:
            body = e.read().decode()[:300]
            print(f"  ERROR batch {batch_n}: HTTP {e.code} — {body}")
            errors += 1
            if errors > 5:
                print("Too many errors, stopping.")
                sys.exit(1)
        except Exception as e:
            print(f"  ERROR batch {batch_n}: {e}")
            errors += 1

        time.sleep(0.15)

    print(f"\n✅  Done! {loaded:,} households uploaded. Errors: {errors}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description='FCI FieldMap voter file migration')
    ap.add_argument('--files',       nargs='+', required=True, help='Voter file(s) to process')
    ap.add_argument('--url',         required=True,            help='Worker base URL')
    ap.add_argument('--key',         required=True,            help='FCI API key')
    ap.add_argument('--admin-key',   required=True,            help='FCI admin key')
    ap.add_argument('--batch-size',  type=int, default=200,    help='Upload batch size (default 200)')
    ap.add_argument('--dry-run',     action='store_true',      help='Parse only, do not upload')
    ap.add_argument('--skip-geocode',action='store_true',      help='Skip Census geocoding')
    args = ap.parse_args()

    # Expand globs
    files = []
    for pattern in args.files:
        expanded = glob.glob(pattern)
        files.extend(expanded if expanded else [pattern])
    if not files:
        print("No files found.")
        sys.exit(1)

    print(f"Processing {len(files)} file(s): {[os.path.basename(f) for f in files]}")

    # ── Step 1: Read ─────────────────────────────────────────────────────────
    raw = process_files(files)
    if not raw:
        print("No voters found. Check file path and format.")
        sys.exit(1)

    # ── Step 2: Group into households ────────────────────────────────────────
    households = build_households(raw)

    # ── Step 3: Build records ────────────────────────────────────────────────
    print("\nBuilding household records...")
    records = []
    warn    = 0
    for key, members in households.items():
        try:
            records.append(build_voter_record(key, members))
        except Exception as e:
            warn += 1
            if warn <= 5:
                print(f"  WARNING: skipped household {key}: {e}")
    print(f"Built {len(records):,} household records ({warn} skipped)")

    # County breakdown
    from collections import Counter
    county_counts = Counter(r['countyName'] for r in records)
    print("\nHouseholds by county:")
    for name, cnt in sorted(county_counts.items(), key=lambda x: -x[1]):
        mail_flag = '' if name == 'Fairfield' else ' (mail only)'
        print(f"  {name:<15} {cnt:>7,}{mail_flag}")

    # Party breakdown
    party_counts = Counter(r['party'] for r in records)
    print("\nHouseholds by dominant party:")
    for p, cnt in sorted(party_counts.items(), key=lambda x: -x[1]):
        label = {'D': 'Democrat', 'R': 'Republican', 'I': 'Independent',
                 'G': 'Green', 'L': 'Libertarian', '': 'Unknown'}.get(p, p)
        print(f"  {label:<12} {cnt:>7,}")

    if args.dry_run:
        print("\n── DRY RUN — sample record ──")
        print(json.dumps(records[0], indent=2, default=str))
        print(f"\nWould upload {len(records):,} records. Remove --dry-run to proceed.")
        return

    # ── Step 4: Geocode Fairfield County ─────────────────────────────────────
    if not args.skip_geocode:
        fc_records = [r for r in records if r['countyNum'] == GEOCODE_COUNTY]
        print(f"\nPreparing to geocode {len(fc_records):,} Fairfield County households...")

        geo_input = []
        for r in fc_records:
            addr_full = r.get('a', '')
            parts     = addr_full.split(',')
            street    = parts[0].strip() if parts else ''
            city_zip  = parts[1].strip() if len(parts) > 1 else ''
            city_parts = city_zip.rsplit(' ', 1)
            g_city    = city_parts[0].strip() if city_parts else ''
            g_zip     = city_parts[1].strip() if len(city_parts) > 1 else ''
            geo_input.append({'id': r['id'], 'address': street, 'city': g_city, 'state': 'OH', 'zip': g_zip})

        geo_results = geocode_census_batch(geo_input)

        # Apply results
        geo_map = {r['id']: r for r in fc_records}
        for vid, (lat, lon) in geo_results.items():
            if vid in geo_map:
                geo_map[vid]['lat'] = lat
                geo_map[vid]['lon'] = lon

        geocoded = sum(1 for r in fc_records if r.get('lat'))
        print(f"Applied coordinates to {geocoded:,}/{len(fc_records):,} Fairfield households")
    else:
        print("\nSkipping geocoding (--skip-geocode)")

    # ── Step 5: Upload ───────────────────────────────────────────────────────
    upload_voters(records, args.url, args.key, args.admin_key, args.batch_size)
    print("\nNext step: run merge_donors.py to re-apply FEC + Ohio SOS donor data")


if __name__ == '__main__':
    main()
