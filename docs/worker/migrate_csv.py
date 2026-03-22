#!/usr/bin/env python3
"""
FCI FieldMap — Ohio SOS CSV Voter Migration Script
Reads one or more Ohio county voter CSV files, filters to active non-Republican voters,
groups into households, and uploads to Cloudflare D1 via the worker API.

Usage:
    python migrate_csv.py --files fairfield.csv licking.csv perry.csv ... 
                          --url https://fci-canvass.fci-canvass.workers.dev
                          --key ohiofcicanvass7312
                          --admin-key "2026ad#min#oh#fci#LA"

    # Or use a glob:
    python migrate_csv.py --files *.csv --url ... --key ... --admin-key ...
"""

import argparse, csv, hashlib, json, re, sys, time, os
import urllib.request, urllib.error
from collections import defaultdict

# ── Election history columns to scan for voting years ────────────────────────
# Ohio SOS files have columns like "11/03/2020" or "PRIMARY_03/17/2020" etc.
# We detect these automatically from the header.

PRES_YEARS = {2024, 2020, 2016, 2012, 2008}

def parse_vote_year(col):
    """Extract year from a column name like '11/03/2020' or 'PRIMARY_03/17/2020'"""
    m = re.search(r'(\d{4})', col)
    return int(m.group(1)) if m else None

def normalize_address(row):
    # Ohio SOS format: RESIDENTIAL_ADDRESS1 is a full pre-formatted address
    addr = str(row.get('RESIDENTIAL_ADDRESS1', '') or '').strip()
    if not addr:
        # Fallback: legacy split fields
        parts = [str(row.get('STNUM','') or '').strip(),
                 str(row.get('STDIR','') or '').strip(),
                 str(row.get('STNAME','') or '').strip()]
        addr = ' '.join(p for p in parts if p)
        apt = str(row.get('APT','') or '').strip()
        if apt:
            addr += ' APT ' + apt
    else:
        apt = str(row.get('RESIDENTIAL_SECONDARY_ADDR', '') or '').strip()
        if apt:
            addr += ' ' + apt
    return addr.upper()

def normalize_city(row):
    city = row.get('RESIDENTIAL_CITY', '') or row.get('CITY', '') or ''
    return str(city).strip().upper()

def household_key(addr, city, zip_code):
    return (addr.strip().upper(), city.strip().upper(), str(zip_code or '').strip()[:5])

def make_id(name, addr, city):
    raw = f"{name}|{addr}|{city}".lower()
    return hashlib.md5(raw.encode()).hexdigest()[:16]

def process_files(csv_files):
    all_voters = []
    skipped_inactive = 0

    for filepath in csv_files:
        print(f"\nReading {os.path.basename(filepath)}...")
        try:
            with open(filepath, newline='', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                headers = reader.fieldnames
                if not headers:
                    print(f"  WARNING: No headers found, skipping")
                    continue

                # Detect election history columns
                election_cols = []
                for h in headers:
                    yr = parse_vote_year(h)
                    if yr and yr >= 2000:
                        election_cols.append((h, yr))

                print(f"  Found {len(election_cols)} election history columns")

                file_voters = 0
                file_skipped = 0
                for row in reader:
                    # Filter: active voters only
                    status = str(row.get('VOTERSTAT', '') or row.get('VOTER_STATUS', '')).strip().upper()
                    if status not in ('A', 'ACTIVE'):
                        skipped_inactive += 1
                        continue

                    # Keep ALL parties — R exclusion handled by worker/frontend
                    party = str(row.get('PARTYAFFIL', '') or row.get('PARTY_AFFILIATION', '')).strip().upper()

                    all_voters.append((row, election_cols, party))
                    file_voters += 1

                print(f"  {file_voters:,} active voters")

        except Exception as e:
            print(f"  ERROR reading {filepath}: {e}")
            continue

    print(f"\nTotal active voters: {len(all_voters):,}")
    print(f"Skipped (inactive): {skipped_inactive:,}")
    return all_voters

def build_households(raw_voters):
    """Group individual voters into households by address."""
    households = defaultdict(list)

    for (row, election_cols, party) in raw_voters:
        addr = normalize_address(row)
        city = normalize_city(row)
        zip5 = str(row.get('RESIDENTIAL_ZIP', '') or row.get('ZIP', '') or '').strip()[:5]
        key  = household_key(addr, city, zip5)
        households[key].append((row, election_cols, party, addr, city, zip5))

    print(f"Grouped into {len(households):,} households")
    return households

def build_voter_record(hh_key, members):
    """Build a single voter record for a household."""
    addr, city, zip5 = hh_key

    # Use first member for address/district fields
    row0, election_cols, party0, _, _, _ = members[0]

    # Last names for household display name
    last_names = []
    seen_last = set()
    for (row, _, _, _, _, _) in members:
        ln = str(row.get('LAST_NAME', '') or row.get('LASTN', '') or '').strip().title()
        if ln and ln not in seen_last:
            last_names.append(ln)
            seen_last.add(ln)
    display_name = '/'.join(last_names[:3])
    if not display_name:
        display_name = 'Unknown'

    # Party: D if any D, else NP
    parties = [m[2] for m in members]
    if any(p in ('D', 'DEM', 'DEMOCRATIC') for p in parties):
        hh_party = 'D'
    else:
        hh_party = ''

    # Collect all years voted across all household members
    years_voted = set()
    for (row, election_cols, _, _, _, _) in members:
        for (col, yr) in election_cols:
            val = str(row.get(col, '') or '').strip()
            if val and val not in ('', '0', 'N', 'NO'):
                years_voted.add(yr)

    # Sort years descending
    yrs_sorted = sorted(years_voted, reverse=True)
    yrs_str = ','.join(str(y) for y in yrs_sorted)

    # General election count (last 6 generals: 2024,2022,2020,2018,2016,2014)
    generals = [2024, 2022, 2020, 2018, 2016, 2014]
    generals_voted = sum(1 for y in generals if y in years_voted)

    # Last primary
    last_primary = ''
    for (row, election_cols, _, _, _, _) in members:
        for (col, yr) in election_cols:
            if 'PRIM' in col.upper() or 'PRIMARY' in col.upper():
                val = str(row.get(col, '') or '').strip().upper()
                if val and val not in ('', '0', 'N', 'NO'):
                    # Try to detect D/R primary
                    if 'D' in val:
                        last_primary = 'D'
                        break
                    elif 'R' in val:
                        last_primary = 'R'
                        break
                    else:
                        last_primary = 'D'  # non-partisan counts as D-leaning
                        break
        if last_primary:
            break

    # Build vns (voter name array) - [firstName, lastName, age, presOnly]
    current_year = 2026
    vns = []
    for (row, election_cols, _, _, _, _) in members:
        fn  = str(row.get('FIRST_NAME', '') or row.get('FIRSTN', '') or '').strip().title()
        ln  = str(row.get('LAST_NAME',  '') or row.get('LASTN',  '') or '').strip().title()
        dob = row.get('DATE_OF_BIRTH', '') or row.get('BIRTHYEAR', '')
        try:
            by = str(dob).strip()[:4]   # works for YYYY-MM-DD or plain YYYY
            age = current_year - int(by) if by.isdigit() else None
        except:
            age = None

        # presOnly: only voted in presidential years
        member_years = set()
        for (col, yr) in election_cols:
            val = str(row.get(col, '') or '').strip()
            if val and val not in ('', '0', 'N', 'NO'):
                member_years.add(yr)
        pres_only = bool(member_years) and all(y in PRES_YEARS for y in member_years)
        vns.append([fn, ln, age, pres_only])

    # Ages string
    ages_list = []
    for v in vns:
        if v[2]: ages_list.append(str(v[2]))
    ages_str = ','.join(ages_list)

    # District info from first member
    def g(k): return str(row0.get(k, '') or '').strip()

    precinct_name = g('PRECINCT_NAME') or g('PRECNAME')
    st_house      = g('STATE_HOUSE') or g('OH HOUSE')
    st_senate     = g('STATE_SENATE') or g('OH SENATE')
    cong_dist     = g('US_CONGRESS') or g('US CONG')
    township      = g('TOWNSHIP')
    municipality  = g('RESIDENTIAL_CITY') or g('CITY')
    village       = g('VILLAGE')
    school_dist   = g('SCHOOL_DISTRICT') or g('SCHOOL DISTRICT')

    # Generate ID
    vid = make_id(display_name, addr, city)

    # Full address string
    full_addr = f"{addr}, {city}"
    if zip5:
        full_addr += f" {zip5}"

    record = {
        'id':           vid,
        'n':            display_name,
        'a':            full_addr,
        'party':        hh_party,
        'pty':          hh_party,
        'yrs':          yrs_str,
        'yearsVoted':   yrs_str,
        'voting':       str(generals_voted),
        'lp':           last_primary,
        'lastPrimary':  last_primary,
        'ages':         ages_str,
        'vns':          vns,
        'hh':           len(members),
        'lat':          None,
        'lon':          None,
        'precinct':     '',
        'precinctName': precinct_name,
        'stHouse':      st_house,
        'stSenate':     st_senate,
        'congDist':     cong_dist,
        'township':     township.upper() if township else '',
        'municipality': municipality.upper() if municipality else '',
        'village':      village.upper() if village else '',
        'ward':         '',
        'score':        None,
        'donations':    [],
        'countyNum':    str(row0.get('COUNTY_NUMBER', '') or '').strip(),
    }
    return record

def upload_voters(voters, worker_url, api_key, admin_key, batch_size=200):
    url = worker_url.rstrip('/') + '/api/admin/load-voters'
    headers = {
        'Content-Type': 'application/json',
        'X-FCI-Key':    api_key,
        'X-FCI-Admin':  admin_key,
        'User-Agent':   'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Origin':       worker_url,
        'Referer':      worker_url + '/',
    }
    total   = len(voters)
    loaded  = 0
    errors  = 0
    print(f"\nUploading {total:,} households in batches of {batch_size}...")
    print(f"Target: {url}\n")

    for i in range(0, total, batch_size):
        batch   = voters[i:i+batch_size]
        batch_n = i // batch_size + 1
        is_first = (i == 0)
        payload = json.dumps({'replace': is_first, 'voters': batch}).encode('utf-8')
        req = urllib.request.Request(url, data=payload, headers=headers, method='POST')
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                result = json.loads(resp.read())
                loaded += result.get('loaded', len(batch))
                pct = loaded / total * 100
                print(f"  Batch {batch_n}: {loaded:,}/{total:,} ({pct:.0f}%)")
        except urllib.error.HTTPError as e:
            body = e.read().decode()[:200]
            print(f"  ERROR batch {batch_n}: HTTP {e.code} — {body}")
            errors += 1
            if errors > 5:
                print("Too many errors, stopping.")
                sys.exit(1)
        except Exception as e:
            print(f"  ERROR batch {batch_n}: {e}")
            errors += 1
        time.sleep(0.4)

    print(f"\n✅ Done! {loaded:,} households uploaded. Errors: {errors}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--files',     nargs='+', required=True, help='CSV file(s) to process')
    ap.add_argument('--url',       required=True)
    ap.add_argument('--key',       required=True)
    ap.add_argument('--admin-key', required=True)
    ap.add_argument('--batch-size',type=int, default=200)
    ap.add_argument('--dry-run',   action='store_true', help='Parse only, do not upload')
    args = ap.parse_args()

    # Expand any globs
    import glob
    files = []
    for pattern in args.files:
        expanded = glob.glob(pattern)
        files.extend(expanded if expanded else [pattern])

    if not files:
        print("No files found.")
        sys.exit(1)

    print(f"Processing {len(files)} file(s): {[os.path.basename(f) for f in files]}")

    raw = process_files(files)
    households = build_households(raw)

    print("\nBuilding household records...")
    records = []
    for key, members in households.items():
        try:
            rec = build_voter_record(key, members)
            records.append(rec)
        except Exception as e:
            print(f"  WARNING: skipped household {key}: {e}")

    print(f"Built {len(records):,} household records")

    if args.dry_run:
        print("\nDry run — sample record:")
        print(json.dumps(records[0], indent=2, default=str))
        print(f"\nWould upload {len(records):,} records. Run without --dry-run to upload.")
        return

    upload_voters(records, args.url, args.key, args.admin_key, args.batch_size)
    print("\nNext step: run merge_donors.py to re-apply FEC donor data")

if __name__ == '__main__':
    main()
