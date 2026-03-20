// FCI Canvass — Service Worker v4
const CACHE = 'fci-canvass-20260320111816'; // auto-bumped on deploy

const APP_SHELL = [
  './',
  './index.html',
  './manifest.json',
  './icon-192.png',
  './icon-512.png',
];

self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE)
      .then(cache => cache.addAll(APP_SHELL))
      .then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys()
      .then(keys => Promise.all(
        keys.filter(k => k !== CACHE).map(k => caches.delete(k))
      ))
      .then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', event => {
  const url = new URL(event.request.url);

  // Always network-first for:
  // - non-GET requests (POST, DELETE etc — never cache these)
  // - external APIs and CDNs
  if (event.request.method !== 'GET' ||
      url.hostname.includes('tile.openstreetmap') ||
      url.hostname.includes('nominatim') ||
      url.hostname.includes('cdnjs') ||
      url.hostname.includes('unpkg') ||
      url.hostname.includes('workers.dev') ||
      url.hostname.includes('cloudflare') ||
      url.pathname.startsWith('/api/')) {
    event.respondWith(fetch(event.request));
    return;
  }

  // Cache-first for app shell
  event.respondWith(
    caches.match(event.request).then(cached => {
      if (cached) return cached;
      return fetch(event.request).then(response => {
        if (response.ok && url.origin === self.location.origin) {
          caches.open(CACHE).then(c => c.put(event.request, response.clone()));
        }
        return response;
      }).catch(() => {
        if (event.request.mode === 'navigate') return caches.match('./index.html');
      });
    })
  );
});
