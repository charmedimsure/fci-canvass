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
    import re as _re
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
    addr = addr.upper()
    # Normalize LOT numbers to APT format so trailer park residents group as one household
    # e.g. "2445 COLUMBUS-LANCASTER RD NW LOT 42" → base: "2445 COLUMBUS-LANCASTER RD NW" apt: "LOT 42"
    addr = _re.sub(r'\s+LOT\s+(\S+)', r' APT LOT \1', addr)
    return addr

def normalize_city(row):
    city = row.get('RESIDENTIAL_CITY', '') or row.get('CITY', '') or ''
    return str(city).strip().upper()

def get_party(row):
    return str(row.get('PARTYAFFIL', '') or row.get('PARTY_AFFILIATION', '')).strip().upper()

def get_status(row):
    return str(row.get('VOTERSTAT', '') or row.get('VOTER_STATUS', '')).strip().upper()

def get_zip(row):
    return str(row.get('RESIDENTIAL_ZIP', '') or row.get('ZIP', '') or '').strip()[:5]

def get_names(row):
    fn = str(row.get('FIRST_NAME', '') or row.get('FIRSTN', '') or '').strip().title()
    ln = str(row.get('LAST_NAME',  '') or row.get('LASTN',  '') or '').strip().title()
    return fn, ln

def get_dob_year(row):
    dob = row.get('DATE_OF_BIRTH', '') or row.get('BIRTHYEAR', '')
    try:
        by = str(dob).strip()[:4]
        return int(by) if by.isdigit() else None
    except:
        return None

def get_districts(row):
    return {
        'stHouse':  str(row.get('STATE_REPRESENTATIVE_DISTRICT','') or row.get('OH HOUSE','') or row.get('STATE_HOUSE','') or '').strip(),
        'stSenate': str(row.get('STATE_SENATE_DISTRICT','') or row.get('OH SENATE','') or row.get('STATE_SENATE','') or '').strip(),
        'congDist': str(row.get('CONGRESSIONAL_DISTRICT','') or row.get('US CONG','') or row.get('US_CONGRESS','') or '').strip(),
        'township': str(row.get('TOWNSHIP',   '') or '').strip().upper(),
        'municipality': str(row.get('CITY',   '') or row.get('RESIDENTIAL_CITY','') or '').strip().upper(),
        'village':  str(row.get('VILLAGE',    '') or '').strip().upper(),
        'ward':     str(row.get('WARD','') or row.get('CITY WARD','') or '').strip().upper(),
        'precinctName': str(row.get('PRECINCT_NAME','') or row.get('PRECNAME','') or '').strip().upper(),
        'countyNum': str(row.get('COUNTY_NUMBER','') or row.get('CNTYIDNUM','') or '').strip(),
        'schoolDist': str(row.get('LOCAL_SCHOOL_DISTRICT','') or row.get('CITY_SCHOOL_DISTRICT','') or row.get('SCHOOL DISTRICT','') or '').strip(),
    }

def household_key(addr, city, zip_code):
    import re as _re
    # Keep full address including unit/lot — each unit is its own household
    # Just normalize spacing and case
    full_addr = addr.strip().upper()
    return (full_addr, city.strip().upper(), str(zip_code or '').strip()[:5])

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
                    status = get_status(row)
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
        zip5 = get_zip(row)
        key  = household_key(addr, city, zip5)
        households[key].append((row, election_cols, party, addr, city, zip5))

    print(f"Grouped into {len(households):,} households")
    return households

def _get_mail_addr(row, res_addr, city, zip5):
    """Return mailing address if different from residential, else None."""
    m1 = str(row.get('MADDR1','') or '').strip().upper()
    m2 = str(row.get('MADDR2','') or '').strip().upper()
    mc = str(row.get('MCITY','') or '').strip().upper()
    mz = str(row.get('MZIP','') or '').strip()[:5]
    if not m1 or m1 == res_addr.split(',')[0].strip():
        return None  # same as residential, no need to store
    full = m1
    if m2: full += ' ' + m2
    mc_part = mc or city
    if mc_part: full += ', ' + mc_part
    if mz: full += ' ' + mz
    return full or None

def build_voter_record(hh_key, members):
    """Build a single voter record for a household."""
    addr, city, zip5 = hh_key

    # Use first member for address/district fields
    row0, election_cols, party0, _, _, _ = members[0]

    # Last names for household display name
    last_names = []
    seen_last = set()
    for (row, _, _, _, _, _) in members:
        _, ln = get_names(row)
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

    # Last primary — scan from most recent year backwards
    last_primary = ''
    last_primary_year = None
    # Sort election cols by year descending to find the most recent primary
    primary_cols = sorted(
        [(col, yr) for (col, yr) in election_cols if 'PRIM' in col.upper() or 'PRIMARY' in col.upper()],
        key=lambda x: x[1], reverse=True
    )
    for (row, election_cols, _, _, _, _) in members:
        for (col, yr) in primary_cols:
            val = str(row.get(col, '') or '').strip().upper()
            if val and val not in ('', '0', 'N', 'NO'):
                if 'D' in val:
                    last_primary = 'D'
                    last_primary_year = yr
                    break
                elif 'R' in val:
                    last_primary = 'R'
                    last_primary_year = yr
                    break
                else:
                    last_primary = 'D'
                    last_primary_year = yr
                    break
        if last_primary:
            break

    # Build vns (voter name array) - [firstName, lastName, age, presOnly]
    current_year = 2026
    vns = []
    for (row, election_cols, _, _, _, _) in members:
        fn, ln = get_names(row)
        birth_year = get_dob_year(row)
        age = current_year - birth_year if birth_year else None

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
    dists = get_districts(row0)
    precinct_name = dists['precinctName']
    st_house      = dists['stHouse']
    st_senate     = dists['stSenate']
    cong_dist     = dists['congDist']
    township      = dists['township']
    municipality  = dists['municipality']
    village       = dists['village']
    school_dist   = dists.get('schoolDist','') or str(row0.get('LOCAL_SCHOOL_DISTRICT','') or row0.get('SCHOOL DISTRICT','') or '').strip()

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
        'lastPrimaryYear': last_primary_year,
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
        'schoolDist':   school_dist,
        'mailAddr':     _get_mail_addr(row0, addr, city, zip5),
        'mailOnly':     len(members) > 8,  # large households = assisted living/dorms → mail only
        'countyNum':    dists['countyNum'] or str(row0.get('COUNTY_NUMBER','') or row0.get('CNTYIDNUM','') or '').strip(),
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
        time.sleep(0.15)

    print(f"\n✅ Done! {loaded:,} households uploaded. Errors: {errors}")


# ── Census Geocoder ──────────────────────────────────────────────────────────
def geocode_census_batch(records, batch_size=1000):
    """
    Geocode a list of {'id', 'address', 'city', 'state', 'zip'} dicts
    using the Census Geocoder batch API.
    Returns dict of id -> (lat, lon).
    """
    import io, csv as _csv
    results = {}
    total = len(records)
    print(f"\nGeocoding {total:,} addresses via Census Geocoder...")

    for i in range(0, total, batch_size):
        batch = records[i:i+batch_size]
        batch_num = i // batch_size + 1
        print(f"  Batch {batch_num}: {i+1}–{min(i+batch_size, total):,}...")

        # Build CSV payload
        buf = io.StringIO()
        for r in batch:
            # Census format: ID, Street, City, State, ZIP
            buf.write(f'"{r["id"]}","{r["address"]}","{r["city"]}","OH","{r["zip"]}"\n')

        import urllib.request, urllib.parse
        import time
        boundary = '----CensusBoundary'
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

        success = False
        for attempt in range(3):
            try:
                with urllib.request.urlopen(req, timeout=60) as resp:
                    text = resp.read().decode('utf-8', errors='replace')
                reader = _csv.reader(io.StringIO(text))
                for row in reader:
                    if len(row) >= 6 and row[2].strip().upper() == 'MATCH':
                        vid = row[0].strip()
                        coords = row[5].strip()  # "lon,lat"
                        if coords:
                            parts = coords.split(',')
                            if len(parts) == 2:
                                try:
                                    lon, lat = float(parts[0]), float(parts[1])
                                    results[vid] = (lat, lon)
                                except ValueError:
                                    pass
                success = True
                break
            except Exception as e:
                print(f"  WARNING: Geocoding batch {batch_num} attempt {attempt+1} failed: {e}")
                time.sleep(5)
        if not success:
            print(f"  SKIPPING batch {batch_num} after 3 attempts")

    matched = len(results)
    print(f"  Geocoded {matched:,}/{total:,} addresses ({matched/total*100:.0f}%)")
    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--files',     nargs='+', required=True, help='CSV file(s) to process')
    ap.add_argument('--url',       required=True)
    ap.add_argument('--key',       required=True)
    ap.add_argument('--admin-key', required=True)
    ap.add_argument('--batch-size',type=int, default=200)
    ap.add_argument('--dry-run',     action='store_true', help='Parse only, do not upload')
    ap.add_argument('--skip-geocode', action='store_true', dest='skip_geocode', help='Skip Census geocoding step')
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
        # Show district field sample
        print("\nDistrict field samples from first 3 records:")
        for r in records[:3]:
            print(f"  stHouse={repr(r.get('stHouse',''))} stSenate={repr(r.get('stSenate',''))} congDist={repr(r.get('congDist',''))} countyNum={repr(r.get('countyNum',''))}")
        # Also dump raw CSV keys that match district columns
        # Find the original row by checking all_voters
        if raw:
            sample_row = raw[0][0]
            dist_keys = [k for k in sample_row.keys() if any(x in k.upper() for x in ['HOUSE','SENATE','CONG','CNTY','COUNTY','DISTRICT','WARD','PRECINCT','PRECNAME','TOWNSHIP','VILLAGE','MUNICIPAL'])]
            print(f"  All district-related columns:")
            for k in dist_keys:
                print(f"    {repr(k)}: {repr(sample_row.get(k,''))}")
        print(f"\nWould upload {len(records):,} records. Run without --dry-run to upload.")
        return

    # ── Geocode all records via Census Geocoder ──────────────────────────────
    if not args.skip_geocode:
        geo_input = []
        for r in records:
            # Parse address back into components for Census API
            addr_full = r.get('a', '')
            # Format: "123 MAIN ST, LANCASTER 43130"
            parts = addr_full.split(',')
            street = parts[0].strip() if parts else ''
            city_zip = parts[1].strip() if len(parts) > 1 else ''
            city_parts = city_zip.rsplit(' ', 1)
            city = city_parts[0].strip() if city_parts else ''
            zipcode = city_parts[1].strip() if len(city_parts) > 1 else ''
            geo_input.append({'id': r['id'], 'address': street, 'city': city, 'state': 'OH', 'zip': zipcode})

        geo_results = geocode_census_batch(geo_input)
        for r in records:
            if r['id'] in geo_results:
                r['lat'], r['lon'] = geo_results[r['id']]
        geocoded = sum(1 for r in records if r.get('lat'))
        print(f"Geocoded {geocoded:,}/{len(records):,} household records")

    upload_voters(records, args.url, args.key, args.admin_key, args.batch_size)
    print("\nNext step: run merge_donors.py to re-apply FEC donor data")

if __name__ == '__main__':
    main()
