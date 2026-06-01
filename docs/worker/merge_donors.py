"""
FCI FieldMap — Donor Cross-Reference Migration Script
Matches FEC donor data to D1 voter records and updates voter JSON blobs with donations.

Usage:
  python merge_donors.py
"""

import json, re, sys, os, time
import requests

# ── CONFIG ────────────────────────────────────────────────────────────────────
API_URL   = os.environ.get('FCI_API_URL',   'https://fci-canvass.fci-canvass.workers.dev')
API_KEY   = os.environ.get('FCI_API_KEY',   'ohiofcicanvass7312')
ADMIN_KEY = os.environ.get('FCI_ADMIN_KEY', '2026ad#min#oh#fci#LA')
BATCH     = 20
# ──────────────────────────────────────────────────────────────────────────────

HEADERS = {
    'X-FCI-Key':    API_KEY,
    'Content-Type': 'application/json',
}
ADMIN_HEADERS = {**HEADERS, 'X-FCI-Admin': ADMIN_KEY}

def norm_str(s):
    return re.sub(r'[^a-z0-9]', '', str(s or '').lower())

def norm_addr(s):
    s = str(s or '').upper()
    for old, new in [
        ('STREET','ST'),('AVENUE','AVE'),('ROAD','RD'),('DRIVE','DR'),
        ('LANE','LN'),('COURT','CT'),('BOULEVARD','BLVD'),
        ('NORTHEAST','NE'),('NORTHWEST','NW'),('SOUTHEAST','SE'),('SOUTHWEST','SW'),
    ]:
        s = re.sub(r'\b' + old + r'\b', new, s)
    return re.sub(r'[^A-Z0-9]', '', s)


# ── Load donor map ─────────────────────────────────────────────────────────────
print("Loading donor map...")
_script_dir = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(_script_dir, 'donor_map.json')) as f:
    raw_map = json.load(f)

donor_map = {}
for k, v in raw_map.items():
    try:
        parts  = k.strip("()").split(", ")
        last   = parts[0].strip("'\"")
        street = parts[1].strip("'\"") if len(parts) > 1 else ''
        donor_map[(last, street)] = v
    except:
        pass

print(f"Donor keys loaded: {len(donor_map)}")

# Quick sample to understand key format
sample_keys = list(donor_map.keys())[:5]
print(f"Sample donor keys: {sample_keys}")


# ── Fetch all voters from D1 ───────────────────────────────────────────────────
print("\nFetching voters from D1...")
all_voters = []
offset = 0
while True:
    r = requests.get(
        f"{API_URL}/api/voters",
        params={'limit': 10000, 'offset': offset},
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
    if len(batch) < 10000:
        break

print(f"\nTotal voters: {len(all_voters):,}")


# ── Match voters to donors ─────────────────────────────────────────────────────
matched = 0
updated_voters = []

for v in all_voters:
    # Address key from 'a' field
    raw_addr   = str(v.get('a') or '')
    norm_street = norm_addr(raw_addr.split()[0:3] and ' '.join(raw_addr.split()))

    # Extract last names from vns array
    # vns entry: [first, last, middle, suffix, dob, party, lastPrimary, voteCount, sosId, status]
    vns = v.get('vns', [])
    last_names = set()
    for member in vns:
        if isinstance(member, list) and len(member) >= 2:
            last_names.add(norm_str(member[1]))  # index 1 = last name

    if not last_names or not norm_street:
        continue

    # Try each last name in the household
    all_donations = []
    for last_name in last_names:
        key = (last_name, norm_street)
        donations = donor_map.get(key)
        if donations:
            all_donations.extend(donations)

    if all_donations:
        # Dedupe by committee, keep lean + total
        by_committee = {}
        for d in all_donations:
            k = d['committee'][:50]
            if k not in by_committee:
                by_committee[k] = {'voter': d['voter'], 'committee': k, 'lean': d['lean'], 'amount': 0}
            by_committee[k]['amount'] += d.get('amount', 0)

        summarized = sorted(by_committee.values(), key=lambda x: -x['amount'])[:20]
        v_data = dict(v)
        v_data['donations'] = summarized
        updated_voters.append(v_data)
        matched += 1

print(f"Matched {matched:,} voters to donor records")

if not updated_voters:
    # Print some debug info to help diagnose
    print("\nDebug — sample voter address + vns:")
    for v in all_voters[:3]:
        print(f"  a={v.get('a')}  vns={v.get('vns', [])[:1]}")
    print("\nDebug — sample donor keys:")
    for k in list(donor_map.keys())[:5]:
        print(f"  {k}")
    print("\nNo matches found — address/name format mismatch.")
    sys.exit(0)


# ── Push updates back to D1 ────────────────────────────────────────────────────
print(f"\nPushing {len(updated_voters):,} updated voter records to D1...")

for i in range(0, len(updated_voters), BATCH):
    chunk = updated_voters[i:i + BATCH]
    r = requests.post(
        f"{API_URL}/api/admin/load-voters",
        headers=ADMIN_HEADERS,
        json={'voters': chunk, 'replace': False},
        timeout=120,
    )
    if not r.ok:
        half = len(chunk) // 2
        for sub in [chunk[:half], chunk[half:]]:
            if not sub: continue
            r2 = requests.post(
                f"{API_URL}/api/admin/load-voters",
                headers=ADMIN_HEADERS,
                json={'voters': sub, 'replace': False},
                timeout=120,
            )
            if not r2.ok:
                print(f"  Sub-batch error: {r2.status_code} {r2.text[:100]}")
    else:
        print(f"  Pushed {min(i+BATCH, len(updated_voters)):,}/{len(updated_voters):,}...", end='\r')
    time.sleep(0.2)

print(f"\n✅ Done! {matched:,} voter records updated with donation history.")
leans = {}
for v in updated_voters:
    for d in v.get('donations', []):
        lean = d.get('lean', 'O')
        leans[lean] = leans.get(lean, 0) + 1
for lean, count in sorted(leans.items()):
    print(f"  {lean}: {count} donations")
