# FCI FieldMap v4 — Deploy Guide

## Files
- index.html  — the entire app (self-contained, ~14 MB with voter data)
- manifest.json — PWA manifest
- sw.js         — service worker for offline caching
- icon-192.png / icon-512.png — home screen icons

## Hosting (any static host works)
1. Upload all files to the same folder on your web server / Netlify / GitHub Pages
2. Serve over HTTPS (required for PWA install + camera access)
3. First load caches everything via service worker — works offline after that

## Install as PWA
- **iOS Safari**: tap Share → "Add to Home Screen"
- **Android Chrome**: tap ⋮ menu → "Add to Home Screen" / "Install App"

## Admin Setup
1. Open the app → tap "Admin / Candidate" → "Admin Login"
2. First time: enter password `admin` (no email required)
3. Once in → go to Admin Panel → "Admin Accounts" → add your email + new password
4. The default `admin` password is disabled once real accounts are added

## Candidate Setup
Create a campaign from the Admin Panel or directly via "Candidate Login" → "New Campaign".

## What's New (v4 — March 2026)
- 📰 Lit Drop mode (wizard + candidate campaign type)
- 🗺 Google Maps walking route export per route
- 🔐 Multi-admin system with email + password
- 📬 Voter history sidebar on label print screen
- 🏘 Street landmarks in neighborhood picker
- ⚠️ Small routes now show (SMALL badge) instead of being dropped to mail
- ✅ Presidential-election-only voter filter
- ✅ Age range filter for candidates
- ✅ OH-73 / OH-69 / SD-20 / CD-12 always visible for independents
- ✅ "Change" canvass type button fixed
- ✅ Walk sheet + label print open in proper popup window
