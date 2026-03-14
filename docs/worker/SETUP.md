# FCI FieldMap v4 — Cloudflare Backend Setup Guide

## What you need before starting
- A free Cloudflare account (cloudflare.com — no credit card needed)
- Node.js installed (nodejs.org — download the LTS version)
- Python 3 (you likely already have this)
- The `fci-worker/` folder from this package
- Your current `fieldmap.html` (with voter data still embedded)

---

## Step 1 — Install Wrangler (Cloudflare's CLI tool)

Open a terminal and run:

```bash
npm install -g wrangler
```

Then log in to Cloudflare:

```bash
wrangler login
```

A browser window will open — click Allow.

---

## Step 2 — Create your D1 database

```bash
wrangler d1 create fci-canvass-db
```

This prints something like:

```
✅ Successfully created DB 'fci-canvass-db'
  database_id = "abc123-def456-..."
```

**Copy that database_id.** Open `fci-worker/wrangler.toml` and replace
`REPLACE_WITH_YOUR_DB_ID` with it.

---

## Step 3 — Run the database schema

From inside the `fci-worker/` folder:

```bash
wrangler d1 execute fci-canvass-db --file=schema.sql
```

You should see "100% complete" with no errors.

---

## Step 4 — Set your secret keys

These are passwords that protect your API. Make them strong (20+ random characters).
You'll need them again in Step 6.

```bash
# The key volunteers' devices use to talk to the API
wrangler secret put FCI_API_KEY
# (type or paste your key, press Enter)

# A separate key only used for loading voter data — keep this extra private
wrangler secret put FCI_ADMIN_KEY
# (type or paste a different key, press Enter)
```

---

## Step 5 — Deploy the Worker

From inside the `fci-worker/` folder:

```bash
wrangler deploy
```

You'll see output like:

```
✅ Deployed fci-canvass to https://fci-canvass.YOUR_SUBDOMAIN.workers.dev
```

**Copy that URL** — you'll need it in Steps 6 and 7.

Test it works:
```bash
curl https://fci-canvass.YOUR_SUBDOMAIN.workers.dev/api/ping
# Should return: {"ok":true,"ts":"..."}
```

---

## Step 6 — Load voter data into D1

From the `fci-worker/` folder, run the migration script pointing at your
existing fieldmap.html (the old one with voter data still in it):

```bash
python3 migrate_voters.py \
  --html /path/to/fieldmap.html \
  --url  https://fci-canvass.YOUR_SUBDOMAIN.workers.dev \
  --key  YOUR_FCI_API_KEY \
  --admin-key YOUR_FCI_ADMIN_KEY
```

This will upload all ~49,000 households in batches. Takes 3–5 minutes.
You should see progress like:

```
Found 10 data chunks, parsing...
Total households: 49,052

Uploading 49,052 households in 99 batches of 500...
  Batch 1/99 — 500/49,052 (1%)
  Batch 2/99 — 1,000/49,052 (2%)
  ...
✅ Done! 49,052 households loaded into D1.
```

**After this succeeds, the voter data lives in D1 — not in the HTML file.**

---

## Step 7 — Configure the new app

Open the new `fieldmap.html` (the API-connected version) and find this
block near the top of the `<script>` section:

```javascript
const FCI_API_URL = 'https://fci-canvass.YOUR_SUBDOMAIN.workers.dev';
const FCI_API_KEY = 'REPLACE_WITH_YOUR_FCI_API_KEY';
```

Replace both values with your Worker URL and API key from Steps 4–5.

---

## Step 8 — Deploy the new app

Upload the new (slim, ~2MB) `fieldmap.html` to wherever you host it.
The voter data is no longer in the file — it lives in D1.

---

## How sync works

- Volunteer opens app → fetches voter data for their campaign/district (~2–5 sec)
- Volunteer marks a door → immediately syncs to D1
- Every 30 seconds, all active volunteer devices pull latest contact updates
- If two volunteers are in the same area, one will see the other's contacts
  within 30 seconds — well before they'd physically reach the same door
- If a device loses internet mid-canvass, updates are queued locally and
  sync automatically when connectivity returns

## Updating voter data

If you get a new voter file, just re-run the migration script with
`--replace` behavior (the script does this automatically on first batch).
You don't need to redeploy the Worker or app.

## Cost

Free tier covers your usage comfortably:
- Workers: 100,000 requests/day free (you'll use ~100)
- D1: 5GB storage, 25M row reads/day free
- No credit card required, no project pausing

---

## Troubleshooting

**"Unauthorized" error from Worker**
→ Check that FCI_API_KEY in the app matches what you set with `wrangler secret put`

**Migration script fails on batch N**
→ Re-run the script — it uses INSERT OR REPLACE so it's safe to restart

**App shows "Unable to load voter data"**
→ Check the Worker URL in the app config (Step 7)
→ Run `curl https://YOUR_WORKER/api/ping` to confirm the Worker is up

**Wrangler login doesn't work**
→ Try `wrangler login --browser` to force browser auth
