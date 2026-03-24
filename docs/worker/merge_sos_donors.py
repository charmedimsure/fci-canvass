#!/usr/bin/env python3
"""
FCI FieldMap — Ohio SOS State/Local Donor Merge Script

Rules:
  - Donated to LARE FOR OHIO or CITIZENS FOR PERALES → lean='R', add to R donations,
    mark voter party='R' so they get excluded from canvassing
  - Donated to SCARMACK → lean='D', add to D donations, boost score to 'strong'

Only updates voters already in D1. Matches by last name + address fuzzy match.
"""

import csv, json, sys, time, os
import urllib.request, urllib.error
import argparse

WORKER_URL = None
API_KEY    = None
ADMIN_KEY  = None

def _headers():
    return {
        'Content-Type': 'application/json',
        'X-FCI-Key':    API_KEY,
        'X-FCI-Admin':  ADMIN_KEY,
        'User-Agent':   'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Origin':       WORKER_URL,
        'Referer':      WORKER_URL + '/',
    }

def api_get(path, params={}):
    url = WORKER_URL.rstrip('/') + path
    if params:
        qs = '&'.join(f"{k}={urllib.parse.quote(str(v))}" for k,v in params.items())
        url += '?' + qs
    req = urllib.request.Request(url, headers=_headers())
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())

def api_post(path, data):
    import urllib.parse
    url = WORKER_URL.rstrip('/') + path
    body = json.dumps(data).encode('utf-8')
    req = urllib.request.Request(url, data=body, headers=_headers(), method='POST')
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())

def norm(s):
    return (s or '').upper().replace(' ','').replace('.','').replace(',','').replace('-','')

def addr_match(donor_addr, voter_addr):
    """Fuzzy address match — check if street number and first word of street match"""
    da = donor_addr.upper().split()
    va = voter_addr.upper().split()
    if not da or not va: return False
    # Street number must match
    if da[0] != va[0]: return False
    # At least one more word must match
    da_words = set(da[1:])
    va_words = set(va[1:])
    return bool(da_words & va_words)

def load_donors(csv_path):
    donors = []
    with open(csv_path, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            committee = row.get('Committee','').strip().upper()
            fn = row.get('Contributor First Name','').strip().title()
            ln = row.get('Contributor Last Name','').strip().title()
            addr = row.get('Address','').strip().upper()
            city = row.get('City','').strip().upper()
            amount_str = row.get('Amount','').replace('$','').replace(',','').strip()
            try:
                amount = float(amount_str) if amount_str else 0.0
            except:
                amount = 0.0
            date = row.get('Contribution Date','').strip()

            if not ln: continue

            if 'LARE FOR OHIO' in committee or 'CITIZENS FOR PERALES' in committee:
                lean = 'R'
            elif 'SCARMACK' in committee:
                lean = 'D'
            else:
                continue

            donors.append({
                'fn': fn, 'ln': ln, 'addr': addr, 'city': city,
                'amount': amount, 'date': date,
                'committee': row.get('Committee','').strip(),
                'lean': lean
            })
    return donors

def main():
    global WORKER_URL, API_KEY, ADMIN_KEY
    import urllib.parse

    ap = argparse.ArgumentParser()
    ap.add_argument('--csv',       required=True, help='Ohio SOS contribution CSV')
    ap.add_argument('--url',       required=True)
    ap.add_argument('--key',       required=True)
    ap.add_argument('--admin-key', required=True)
    ap.add_argument('--dry-run',   action='store_true')
    args = ap.parse_args()

    WORKER_URL = args.url
    API_KEY    = args.key
    ADMIN_KEY  = args.admin_key

    print(f"Loading donors from {args.csv}...")
    donors = load_donors(args.csv)
    print(f"  {len(donors)} individual donors ({sum(1 for d in donors if d['lean']=='R')} R, {sum(1 for d in donors if d['lean']=='D')} D)")

    # Group by last name
    by_last = {}
    for d in donors:
        key = d['ln'].upper()
        if key not in by_last:
            by_last[key] = []
        by_last[key].append(d)

    print(f"\nQuerying D1 for {len(by_last)} last names...")
    matched = 0
    updated = 0
    updates = []

    for i, (last_name, donor_list) in enumerate(by_last.items()):
        if i % 50 == 0 and i > 0:
            print(f"  {i}/{len(by_last)} names checked, {matched} matches so far...")
        try:
            result = api_get('/api/voters', {'last_name': last_name, 'limit': 20})
            voters = result.get('voters', [])
        except Exception as e:
            print(f"  WARNING: lookup failed for {last_name}: {e}")
            time.sleep(0.5)
            continue

        for voter in voters:
            voter_addr = (voter.get('a') or voter.get('address') or '').upper()
            voter_name = (voter.get('n') or voter.get('name') or '').upper()

            for d in donor_list:
                # Match: last name already matched by API, check address
                if not addr_match(d['addr'], voter_addr):
                    continue
                # Also verify first name if available
                if d['fn'] and d['fn'].upper() not in voter_name:
                    continue

                matched += 1
                donation = {
                    'lean':      d['lean'],
                    'amount':    d['amount'],
                    'date':      d['date'],
                    'committee': d['committee'],
                    'voter':     d['fn'] + ' ' + d['ln'],
                }

                # Determine what to update
                existing_donations = voter.get('donations', [])
                # Avoid duplicates
                already = any(
                    x.get('committee') == donation['committee'] and
                    x.get('amount') == donation['amount'] and
                    x.get('date') == donation['date']
                    for x in existing_donations
                )
                if already:
                    continue

                new_donations = existing_donations + [donation]

                # Determine new party/score
                new_party = voter.get('party', '')
                new_score = voter.get('score')
                if d['lean'] == 'R':
                    new_party = 'R'  # mark as Republican → excluded from canvassing
                elif d['lean'] == 'D':
                    new_score = 'strong'  # Scarmack donor → strong

                updates.append({
                    'id':        voter.get('id'),
                    'donations': new_donations,
                    'party':     new_party,
                    'score':     new_score,
                    'address':   voter_addr,
                    'name':      voter_name,
                    'lean':      d['lean'],
                    'committee': d['committee'],
                })

        time.sleep(0.15)

    print(f"\nMatched {matched} donor records to {len(updates)} voter updates")

    if args.dry_run:
        print("\nDry run — sample updates:")
        for u in updates[:5]:
            print(f"  {u['name']} | {u['address']} | {u['lean']} | {u['committee']} → party={u['party']}, score={u['score']}")
        return

    # Push updates
    print(f"\nPushing {len(updates)} voter updates to D1...")
    pushed = 0
    errors = 0
    BATCH = 100
    for i in range(0, len(updates), BATCH):
        batch = updates[i:i+BATCH]
        try:
            result = api_post('/api/admin/update-voters', {'voters': batch})
            pushed += result.get('updated', len(batch))
            print(f"  Pushed {pushed}/{len(updates)}...")
        except Exception as e:
            print(f"  ERROR batch {i//BATCH + 1}: {e}")
            errors += 1
        time.sleep(0.4)

    print(f"\n✅ Done! {pushed} voters updated. Errors: {errors}")
    r_count = sum(1 for u in updates if u['lean']=='R')
    d_count = sum(1 for u in updates if u['lean']=='D')
    print(f"  R donors (excluded from canvassing): {r_count}")
    print(f"  D donors (scored strong):             {d_count}")

if __name__ == '__main__':
    main()
