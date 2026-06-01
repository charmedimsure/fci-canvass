#!/usr/bin/env python3
"""
FCI FieldMap — Geocode Fairfield County Voters
Fetches all FC households from D1, geocodes via Census batch API, pushes coords back.

Usage:
    python geocode_voters.py
"""

import csv, io, time, urllib.request, requests, sys
from collections import defaultdict

# ── CONFIG ────────────────────────────────────────────────────────────────────
API_URL    = 'https://fci-canvass.fci-canvass.workers.dev'
API_KEY    = 'ohiofcicanvass7312'
ADMIN_KEY  = '2026ad#min#oh#fci#LA'
COUNTY_NUM = '23'
FETCH_BATCH = 10000   # voters per fetch call
GEO_BATCH   = 1000   # addresses per Census API call
PUSH_BATCH  = 20     # households per load-voters call
# ──────────────────────────────────────────────────────────────────────────────

HEADERS = {
    'X-FCI-Key':    API_KEY,
    'Content-Type': 'application/json',
}
ADMIN_HEADERS = {**HEADERS, 'X-FCI-Admin': ADMIN_KEY}


# ── STEP 1: Fetch all Fairfield County households from D1 ─────────────────────
print("Fetching Fairfield County households from D1...")
all_voters = []
offset = 0

while True:
    r = requests.get(
        f"{API_URL}/api/voters",
        params={'county_num': COUNTY_NUM, 'limit': FETCH_BATCH, 'offset': offset},
        headers=HEADERS,
        timeout=60,
    )
    if not r.ok:
        print(f"Error fetching voters: {r.status_code} {r.text[:200]}")
        sys.exit(1)
    batch = r.json().get('voters', [])
    if not batch:
        break
    all_voters.extend(batch)
    offset += len(batch)
    print(f"  Fetched {len(all_voters):,}...", end='\r')
    if len(batch) < FETCH_BATCH:
        break

print(f"\nTotal households fetched: {len(all_voters):,}")

# Only geocode those missing coordinates
to_geocode = [v for v in all_voters if not v.get('lat')]
print(f"Households needing geocoding: {len(to_geocode):,}")

if not to_geocode:
    print("All households already geocoded!")
    sys.exit(0)


# ── STEP 2: Census batch geocoder ─────────────────────────────────────────────
def geocode_census_batch(records):
    """
    records: list of dicts with keys: id, address, city, state, zip
    returns: dict of id -> (lat, lon)
    """
    results = {}
    total   = len(records)
    batch_num = 0

    for i in range(0, total, GEO_BATCH):
        batch     = records[i:i + GEO_BATCH]
        batch_num += 1
        print(f"  Census batch {batch_num} ({i+1}-{min(i+GEO_BATCH, total)}/{total})...", end=' ')

        # Build multipart CSV body
        boundary = 'fci_geo_boundary_xk39'
        csv_lines = ['ID,Street,City,State,ZIP']
        for rec in batch:
            addr = rec['address'].replace('"', '').replace(',', ' ')
            city = rec['city'].replace('"', '').replace(',', ' ')
            csv_lines.append(f'"{rec["id"]}","{addr}","{city}","{rec["state"]}","{rec["zip"]}"')
        csv_content = '\n'.join(csv_lines)

        body = (
            f'--{boundary}\r\n'
            f'Content-Disposition: form-data; name="addressFile"; filename="addresses.csv"\r\n'
            f'Content-Type: text/csv\r\n\r\n'
            f'{csv_content}\r\n'
            f'--{boundary}\r\n'
            f'Content-Disposition: form-data; name="benchmark"\r\n\r\n'
            f'Public_AR_Current\r\n'
            f'--{boundary}--\r\n'
        ).encode('utf-8')

        url = 'https://geocoding.geo.census.gov/geocoder/locations/addressbatch'
        req = urllib.request.Request(url, data=body, method='POST')
        req.add_header('Content-Type', f'multipart/form-data; boundary={boundary}')

        matched = 0
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
                                        matched += 1
                                    except ValueError:
                                        pass
                print(f"matched {matched}/{len(batch)}")
                break
            except Exception as e:
                print(f"\n    attempt {attempt+1} failed: {e}")
                time.sleep(5)

        time.sleep(0.5)

    pct = len(results) / total * 100 if total else 0
    print(f"\nGeocoded {len(results):,}/{total:,} ({pct:.1f}%)")
    return results


# Build geocoder input
geo_input = []
for v in to_geocode:
    vid  = v.get('id', '')
    addr = v.get('a', '')
    city = v.get('city', '')
    zip5 = v.get('zip', '')
    if vid and addr:
        geo_input.append({'id': vid, 'address': addr, 'city': city, 'state': 'OH', 'zip': zip5})

print(f"\nSending {len(geo_input):,} addresses to Census geocoder...")
geo_results = geocode_census_batch(geo_input)


# ── STEP 3: Apply coordinates and re-push ─────────────────────────────────────
# Build lookup from id -> full voter record
voter_map = {v['id']: v for v in all_voters if v.get('id')}

# Apply geocoded coords
updated = []
for vid, (lat, lon) in geo_results.items():
    if vid in voter_map:
        record = dict(voter_map[vid])
        record['lat'] = lat
        record['lon'] = lon
        updated.append(record)

print(f"\nPushing {len(updated):,} geocoded households back to D1...")
pushed = 0
errors = 0

for i in range(0, len(updated), PUSH_BATCH):
    chunk = updated[i:i + PUSH_BATCH]
    r = requests.post(
        f"{API_URL}/api/admin/load-voters",
        headers=ADMIN_HEADERS,
        json={'voters': chunk, 'replace': False},
        timeout=120,
    )
    if r.ok:
        pushed += len(chunk)
    else:
        # Retry one at a time
        for hh in chunk:
            r2 = requests.post(
                f"{API_URL}/api/admin/load-voters",
                headers=ADMIN_HEADERS,
                json={'voters': [hh], 'replace': False},
                timeout=120,
            )
            if r2.ok:
                pushed += 1
            else:
                print(f"\n  ❌ {r2.status_code}: {r2.text[:100]}")
                errors += 1
        time.sleep(0.2)

    pct = min(i + PUSH_BATCH, len(updated)) / len(updated) * 100
    print(f"  {pushed:,}/{len(updated):,} ({pct:.1f}%)  errors={errors}   ", end='\r')
    time.sleep(0.15)

print(f"\n\n{'✅' if errors == 0 else '⚠️ '} Done.")
print(f"  Geocoded  : {len(geo_results):,} households")
print(f"  Pushed    : {pushed:,} households")
print(f"  Errors    : {errors}")
print(f"  No match  : {len(geo_input) - len(geo_results):,} addresses (rural/new construction)")
