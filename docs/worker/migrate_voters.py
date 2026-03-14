#!/usr/bin/env python3
"""
FCI FieldMap — Voter Data Migration to Cloudflare D1
=====================================================
Extracts embedded voter data from fieldmap.html and uploads it to
your Cloudflare Worker via the /api/admin/load-voters endpoint.

Usage:
    python3 migrate_voters.py --html fieldmap.html \
                               --url https://fci-canvass.YOUR_NAME.workers.dev \
                               --key YOUR_API_KEY \
                               --admin-key YOUR_ADMIN_KEY

Run once after deploying the Worker. Safe to re-run (uses INSERT OR REPLACE).
"""

import argparse, json, re, sys, time
import urllib.request, urllib.error

def extract_voters(html_path):
    print(f"Reading {html_path} ...")
    with open(html_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Find all _VDn chunks
    chunks = re.findall(r'const _VD\d+=(\[.*?\]);', content, re.DOTALL)
    if not chunks:
        print("ERROR: No voter data chunks (_VD0.._VD9) found in HTML.")
        print("Make sure you're pointing at the correct fieldmap.html file.")
        sys.exit(1)

    print(f"Found {len(chunks)} data chunks, parsing...")
    all_voters = []
    for i, chunk in enumerate(chunks):
        voters = json.loads(chunk)
        all_voters.extend(voters)
        print(f"  Chunk {i}: {len(voters)} households")

    print(f"\nTotal households: {len(all_voters):,}")
    return all_voters

def upload_voters(voters, worker_url, api_key, admin_key, batch_size=500):
    url = worker_url.rstrip('/') + '/api/admin/load-voters'
    headers = {
        'Content-Type':  'application/json',
        'X-FCI-Key':     api_key,
        'X-FCI-Admin':   admin_key,
    }

    total   = len(voters)
    batches = (total + batch_size - 1) // batch_size
    loaded  = 0

    print(f"\nUploading {total:,} households in {batches} batches of {batch_size}...")
    print(f"Target: {url}\n")

    for i in range(0, total, batch_size):
        batch    = voters[i:i + batch_size]
        batch_n  = i // batch_size + 1
        payload  = json.dumps({
            'replace': i == 0,   # only clear on first batch
            'voters':  batch,
        }).encode('utf-8')

        req = urllib.request.Request(url, data=payload, headers=headers, method='POST')
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                result = json.loads(resp.read())
                loaded += result.get('loaded', len(batch))
                pct = loaded / total * 100
                print(f"  Batch {batch_n}/{batches} — {loaded:,}/{total:,} ({pct:.0f}%)")
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            print(f"\nERROR on batch {batch_n}: HTTP {e.code} — {body}")
            sys.exit(1)
        except Exception as e:
            print(f"\nERROR on batch {batch_n}: {e}")
            sys.exit(1)

        # Small delay to avoid hammering the Worker
        if i + batch_size < total:
            time.sleep(0.2)

    print(f"\n✅ Done! {loaded:,} households loaded into D1.")

def main():
    ap = argparse.ArgumentParser(description='Migrate FCI voter data to Cloudflare D1')
    ap.add_argument('--html',       required=True,  help='Path to fieldmap.html')
    ap.add_argument('--url',        required=True,  help='Worker URL, e.g. https://fci-canvass.you.workers.dev')
    ap.add_argument('--key',        required=True,  help='FCI API key (FCI_API_KEY secret)')
    ap.add_argument('--admin-key',  required=True,  help='FCI Admin key (FCI_ADMIN_KEY secret)')
    ap.add_argument('--batch-size', type=int, default=500, help='Households per upload batch (default 500)')
    args = ap.parse_args()

    voters = extract_voters(args.html)
    upload_voters(voters, args.url, args.key, args.admin_key, args.batch_size)

if __name__ == '__main__':
    main()
