# FCI Canvass — Deployment Guide

**Fairfield County Indivisible Canvassing Tool**

This app installs on any phone as a home screen app (no app store needed).
All voter data stays on-device — nothing is sent to any server.

---

## Quickest: Deploy with Netlify Drop (2 minutes, free)

1. Go to **https://app.netlify.com/drop**
2. Drag the entire `fci-canvass` folder onto the page
3. Netlify gives you a live HTTPS URL immediately (e.g. `https://random-name-123.netlify.app`)
4. Share that URL with your volunteers

**To set a custom URL like `https://fci-canvass.netlify.app`:**
- Create a free Netlify account
- Go to Site Settings → General → Change site name

---

## Option 2: GitHub Pages (free, permanent, your own URL)

1. Create a free account at **https://github.com**
2. Click **New Repository** → name it `fci-canvass` → Public → Create
3. Click **uploading an existing file** → drag all files in this folder
4. Commit the files
5. Go to **Settings → Pages → Source: main branch, / (root) → Save**
6. Your URL will be `https://YOUR-USERNAME.github.io/fci-canvass`

---

## How Volunteers Install It on Their Phone

### Android (Chrome)
1. Open the URL in Chrome
2. Tap the **⋮ menu → Add to Home Screen**
3. Tap **Add** — the FCI Canvass icon appears on their home screen
4. Chrome may also show an automatic "Install App" banner at the bottom

### iPhone / iPad (Safari)
1. Open the URL in **Safari** (must be Safari, not Chrome)
2. Tap the **Share button** (box with arrow at bottom)
3. Scroll down → **Add to Home Screen**
4. Tap **Add** — icon appears on home screen

### After Installing
- The app works **fully offline** after first load
- All data is saved locally on the device
- Map tiles need internet, but all canvassing/contact features work without it

---

## Files in this Package

| File | Purpose |
|------|---------|
| `index.html` | The entire app (self-contained) |
| `manifest.json` | PWA metadata for phone installation |
| `sw.js` | Service worker — enables offline use |
| `icon-192.png` | App icon (Android home screen) |
| `icon-512.png` | App icon (splash screen / iOS) |

---

## Updating the App

When you upload a new version of `index.html`, update the cache version in `sw.js`:

```js
const CACHE = 'fci-canvass-v2';  // increment this number
```

This forces volunteers' phones to download the new version on next open.

---

*Fairfield County Indivisible · fairfieldcountyindivisible.org*
