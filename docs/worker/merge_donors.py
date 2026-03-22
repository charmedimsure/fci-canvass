"""
FCI FieldMap — Donor Cross-Reference Migration Script
Matches FEC donor data to D1 voter records and updates voter JSON blobs with donations.

Usage:
  pip install requests --break-system-packages
  python merge_donors.py

Requires environment variables (or edit the constants below):
  FCI_API_URL   — your Cloudflare Worker URL
  FCI_API_KEY   — your API key
  FCI_ADMIN_KEY — your admin key
"""

import json, re, sys, os, time
import requests

# ── CONFIG ────────────────────────────────────────────────────────────────────
API_URL   = os.environ.get('FCI_API_URL',   'https://fci-canvass.fci-canvass.workers.dev')
API_KEY   = os.environ.get('FCI_API_KEY',   'ohiofcicanvass7312')
ADMIN_KEY = os.environ.get('FCI_ADMIN_KEY', '2026ad#min#oh#fci#LA')
BATCH     = 20  # Smaller batches to avoid Cloudflare payload limits
# ──────────────────────────────────────────────────────────────────────────────

HEADERS = {
    'X-FCI-Key':   API_KEY,
    'Content-Type':'application/json',
}

def norm_str(s):
    return re.sub(r'[^a-z0-9]', '', str(s or '').lower())

def norm_addr(s):
    s = str(s or '').upper()
    for old, new in [('STREET','ST'),('AVENUE','AVE'),('ROAD','RD'),('DRIVE','DR'),('LANE','LN'),('COURT','CT'),('BOULEVARD','BLVD'),('NORTHEAST','NE'),('NORTHWEST','NW'),('SOUTHEAST','SE'),('SOUTHWEST','SW')]:
        s = re.sub(r'\b'+old+r'\b', new, s)
    return re.sub(r'[^A-Z0-9]', '', s)

# Load the donor map built by the analysis script
print("Loading donor map...")
# donor_map.json should be in the same folder as this script
import os
_script_dir = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(_script_dir, 'donor_map.json')) as f:
    raw_map = json.load(f)

# Convert string keys back to tuples
donor_map = {}
for k, v in raw_map.items():
    # Key format: "('last', 'normstreet')"
    try:
        parts = k.strip("()").split(", ")
        last   = parts[0].strip("'\"")
        street = parts[1].strip("'\"") if len(parts) > 1 else ''
        donor_map[(last, street)] = v
    except:
        pass

print(f"Donor keys loaded: {len(donor_map)}")

# Fetch all voters in batches
print("\nFetching voters from D1...")
all_voters = []
offset = 0
while True:
    r = requests.get(f"{API_URL}/api/voters",
        params={'limit': 1000, 'offset': offset},
        headers=HEADERS)
    if not r.ok:
        print(f"Error fetching voters: {r.status_code} {r.text[:200]}")
        sys.exit(1)
    batch = r.json().get('voters', [])
    if not batch:
        break
    all_voters.extend(batch)
    offset += len(batch)
    print(f"  Fetched {len(all_voters)} voters...", end='\r')
    if len(batch) < 1000:
        break

print(f"\nTotal voters: {len(all_voters)}")

# Match voters to donors
matched = 0
updated_voters = []

for v in all_voters:
    # Build match keys from voter address
    raw_addr = str(v.get('a') or v.get('address') or '')
    # Extract street number + street name (before the comma)
    street_part = raw_addr.split(',')[0] if ',' in raw_addr else raw_addr
    norm_street = norm_addr(street_part)

    # Try matching by last name extracted from voter name
    full_name = str(v.get('n') or v.get('name') or '')
    # Name format in D1 is typically "LAST, FIRST" or "FIRST LAST"
    if ',' in full_name:
        last_name = norm_str(full_name.split(',')[0])
    else:
        parts = full_name.strip().split()
        last_name = norm_str(parts[-1]) if parts else ''

    if not last_name or not norm_street:
        continue

    key = (last_name, norm_street)
    donations = donor_map.get(key)

    if donations:
        # Summarize donations: dedupe by committee, keep lean + total per committee
        # Cap at 20 entries to keep payload small
        by_committee = {}
        for d in donations:
            k = d['committee'][:50]  # truncate long names
            if k not in by_committee:
                by_committee[k] = {'voter': d['voter'], 'committee': k, 'lean': d['lean'], 'amount': 0}
            by_committee[k]['amount'] += d.get('amount', 0)
        
        # Sort by amount desc, keep top 20
        summarized = sorted(by_committee.values(), key=lambda x: -x['amount'])[:20]
        
        v_data = dict(v)
        v_data['donations'] = summarized
        updated_voters.append(v_data)
        matched += 1

print(f"Matched {matched} voters to donor records")

if not updated_voters:
    print("No matches found — check name/address format in D1")
    sys.exit(0)

# Push updates back to D1 via admin load-voters endpoint (upsert)
print(f"\nPushing {len(updated_voters)} updated voter records to D1...")
admin_headers = {**HEADERS, 'X-FCI-Admin': ADMIN_KEY}

for i in range(0, len(updated_voters), BATCH):
    chunk = updated_voters[i:i+BATCH]
    r = requests.post(f"{API_URL}/api/admin/load-voters",
        headers=admin_headers,
        json={'voters': chunk, 'replace': False}
    )
    if not r.ok:
        # Retry once with smaller chunk on error
        print(f"  Batch {i//BATCH} failed ({r.status_code}), retrying in halves...")
        half = len(chunk) // 2
        for sub in [chunk[:half], chunk[half:]]:
            if not sub: continue
            r2 = requests.post(f"{API_URL}/api/admin/load-voters",
                headers=admin_headers, json={'voters': sub, 'replace': False})
            if not r2.ok:
                print(f"  Sub-batch error: {r2.status_code} {r2.text[:100]}")
    else:
        print(f"  Pushed {min(i+BATCH, len(updated_voters))}/{len(updated_voters)}...", end='\r')
    time.sleep(0.2)

print(f"\n✅ Done! {matched} voter records updated with donation history.")
print("Lean breakdown:")
leans = {'D': 0, 'R': 0, 'O': 0}
for v in updated_voters:
    for d in v.get('donations', []):
        leans[d.get('lean','O')] = leans.get(d.get('lean','O'), 0) + 1
for lean, count in leans.items():
    print(f"  {lean}: {count} donations")
