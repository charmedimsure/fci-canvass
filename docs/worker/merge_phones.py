#!/usr/bin/env python3
"""
FCI FieldMap — Phone Number Merge
Matches phone numbers from protest/rally exports to voter records in D1.
Labels all matched voters as score=strong, party=D.

Usage:
    python merge_phones.py
"""

import csv, re, hashlib, time, requests
from collections import defaultdict

# ── CONFIG ────────────────────────────────────────────────────────────────────
API_URL    = 'https://fci-canvass.fci-canvass.workers.dev'
API_KEY    = 'ohiofcicanvass7312'
ADMIN_KEY  = '2026ad#min#oh#fci#LA'
VOTER_FILE = 'FAIRFIELD (5).txt'
PHONE_FILES = [
    ('2026-06-05.d968f6.people_export.csv',       'protest_signup'),
    ('2026-06-05.602d0f.participation_export.csv', 'no_kings_rally'),
]
BATCH_SIZE = 20
# ──────────────────────────────────────────────────────────────────────────────

HEADERS       = {'X-FCI-Key': API_KEY, 'Content-Type': 'application/json'}
ADMIN_HEADERS = {**HEADERS, 'X-FCI-Admin': ADMIN_KEY}

def norm(s):
    return re.sub(r'[^a-z]', '', (s or '').lower())

def norm_phone(p):
    digits = re.sub(r'\D', '', p or '')
    return digits[-10:] if len(digits) >= 10 else ''

def make_id(addr, city, zip5):
    key = f"{addr.upper()}|{city.upper()}|{zip5}".encode()
    return hashlib.md5(key).hexdigest()[:16]

# ── Load both phone files, dedupe by phone ────────────────────────────────────
all_people = {}

# File 1: people export (protest signups)
print(f"Loading {PHONE_FILES[0][0]}...")
with open(PHONE_FILES[0][0], newline='', encoding='utf-8') as f:
    for r in csv.DictReader(f):
        phone = norm_phone(r.get('phone',''))
        if not phone: continue
        all_people[phone] = {
            'first':     norm(r.get('first_name','')),
            'last':      norm(r.get('last_name','')),
            'zip':       (r.get('zipcode','') or '')[:5],
            'phone':     phone,
            'email':     r.get('email','').strip(),
            'raw_first': r.get('first_name','').strip(),
            'raw_last':  r.get('last_name','').strip(),
            'sms_opt_in': r.get('sms_opt_in_status','').strip(),
            'source':    PHONE_FILES[0][1],
        }

# File 2: participation export (NO KINGS rally)
print(f"Loading {PHONE_FILES[1][0]}...")
with open(PHONE_FILES[1][0], newline='', encoding='utf-8') as f:
    for r in csv.DictReader(f):
        phone = norm_phone(r.get('Mobile number',''))
        if not phone: continue
        if phone not in all_people:
            all_people[phone] = {
                'first':     norm(r.get('First name','')),
                'last':      norm(r.get('Last name','')),
                'zip':       (r.get('ZIP','') or '')[:5],
                'phone':     phone,
                'email':     r.get('Email','').strip(),
                'raw_first': r.get('First name','').strip(),
                'raw_last':  r.get('Last name','').strip(),
                'sms_opt_in': '',
                'source':    PHONE_FILES[1][1],
            }

phones = list(all_people.values())
print(f"Total unique phone records: {len(phones)}")

# ── Load voter file ───────────────────────────────────────────────────────────
print(f"\nLoading {VOTER_FILE}...")
voter_idx = defaultdict(list)
with open(VOTER_FILE, newline='', encoding='utf-8') as f:
    for row in csv.DictReader(f):
        key = (norm(row['FIRST_NAME']), norm(row['LAST_NAME']), row['RESIDENTIAL_ZIP'][:5])
        voter_idx[key].append(row)
print(f"Voter index: {len(voter_idx):,} keys")

# ── Match ─────────────────────────────────────────────────────────────────────
matched_households = {}
matched_log = []
unmatched   = []

for p in phones:
    key  = (p['first'], p['last'], p['zip'])
    hits = voter_idx.get(key, [])
    if not hits:
        for (f, l, z), rows in voter_idx.items():
            if f == p['first'] and l == p['last']:
                hits.extend(rows)

    if hits:
        for v in hits:
            hid = make_id(v['RESIDENTIAL_ADDRESS1'].strip(),
                          v['RESIDENTIAL_CITY'].strip(),
                          v['RESIDENTIAL_ZIP'][:5])
            if hid not in matched_households:
                matched_households[hid] = {
                    'voter':    v,
                    'phone':    p['phone'],
                    'email':    p['email'],
                    'sms_opt_in': p['sms_opt_in'],
                    'source':   p['source'],
                }
            matched_log.append({
                'name':     f"{p['raw_first']} {p['raw_last']}",
                'phone':    p['phone'],
                'email':    p['email'],
                'source':   p['source'],
                'address':  v['RESIDENTIAL_ADDRESS1'].strip(),
                'city':     v['RESIDENTIAL_CITY'].strip(),
                'zip':      v['RESIDENTIAL_ZIP'][:5],
                'precinct': v['PRECINCT_NAME'].strip(),
                'party':    v['PARTY_AFFILIATION'].strip(),
                'sos_id':   v['SOS_VOTERID'].strip(),
            })
    else:
        unmatched.append(p)

print(f"\nMatched  : {len(matched_households)} unique households")
print(f"Unmatched: {len(unmatched)} (out-of-area or partial name)")

# ── Write match report ────────────────────────────────────────────────────────
report_path = 'phone_match_report.csv'
with open(report_path, 'w', newline='', encoding='utf-8') as f:
    w = csv.DictWriter(f, fieldnames=['name','phone','email','source','address','city','zip','precinct','party','sos_id'])
    w.writeheader()
    w.writerows(matched_log)
print(f"Match report written to: {report_path}")

# ── Push to D1 — score=strong, party=D ───────────────────────────────────────
print(f"\nPushing {len(matched_households)} households to D1 (score=strong, party=D)...")
pushed = 0
errors = 0
hh_list = list(matched_households.values())

for i in range(0, len(hh_list), BATCH_SIZE):
    chunk = hh_list[i:i+BATCH_SIZE]
    updates = []
    for hh in chunk:
        v   = hh['voter']
        hid = make_id(v['RESIDENTIAL_ADDRESS1'].strip(),
                      v['RESIDENTIAL_CITY'].strip(),
                      v['RESIDENTIAL_ZIP'][:5])
        updates.append({
            'id':       hid,
            'phone':    hh['phone'],
            'email':    hh['email'],
            'smsOptIn': hh['sms_opt_in'],
            'score':    'strong',
            'party':    'D',
        })

    r = requests.post(
        f"{API_URL}/api/admin/update-voters",
        headers=ADMIN_HEADERS,
        json={'voters': updates},
        timeout=60,
    )
    if r.ok:
        pushed += len(chunk)
    else:
        for upd in updates:
            r2 = requests.post(
                f"{API_URL}/api/admin/update-voters",
                headers=ADMIN_HEADERS,
                json={'voters': [upd]},
                timeout=60,
            )
            if r2.ok: pushed += 1
            else:
                print(f"\n  ❌ {r2.status_code}: {r2.text[:80]}")
                errors += 1
        time.sleep(0.2)

    pct = min(i+BATCH_SIZE, len(hh_list)) / len(hh_list) * 100
    print(f"  {pushed}/{len(hh_list)} ({pct:.0f}%)  errors={errors}   ", end='\r')
    time.sleep(0.15)

print(f"\n\n✅ Done.")
print(f"  Households updated : {pushed}")
print(f"  Errors             : {errors}")
print(f"  Match report       : {report_path}")
