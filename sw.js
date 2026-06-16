// sw.js — Viking Invest dashboard Service Worker (2026-06-16).
//
// Goal: make 2nd-and-onwards visits to the dashboard instant on slow /
// flaky mobile networks. Caches the dashboard HTML + lazy-loaded JS
// chunks so the browser doesn't have to refetch them after the first
// successful load. Data files (.json) are never cached — they must
// always be fresh so the dashboard shows live signals + kill-switch
// state.
//
// Strategy:
//   - HTML: network-first with cache fallback. Online users always get
//     the latest deploy. Offline users get the last good copy.
//   - JS / CSS: cache-first with background revalidate. Lazy-loaded
//     chunks (dashboard-tests.js, dashboard-backtest-ui.js) use cache-
//     bust query strings; `ignoreSearch:true` matches them to a single
//     cached entry per file so we don't balloon storage.
//   - JSON / data feeds: bypass entirely.
//   - Cross-origin: bypass entirely (Supabase, jsDelivr, etc.).
//
// Safety net: if anything goes wrong, the user can unregister via
// chrome://serviceworker-internals (desktop) or Settings → Site
// Settings → Storage (mobile). The dashboard also exposes
// window._unregisterSW() for quick console recovery.
//
// VERSION is replaced at deploy time by the strip workflow's stamp
// step so each deploy produces a new cache name. Old caches are
// cleared on activate.

const VERSION = 'BUILD_TIMESTAMP_UTC';
const CACHE_NAME = 'viking-invest-' + VERSION;

const PRECACHE = [
  '/dashboard.html',
  '/dashboard-tests.js',
  '/dashboard-backtest-ui.js',
];

self.addEventListener('install', event => {
  self.skipWaiting();
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache =>
      Promise.all(PRECACHE.map(url =>
        cache.add(url).catch(err => console.warn('[sw] precache miss:', url, err))
      ))
    )
  );
});

self.addEventListener('activate', event => {
  event.waitUntil(Promise.all([
    self.clients.claim(),
    caches.keys().then(keys => Promise.all(
      keys.filter(k => k.startsWith('viking-invest-') && k !== CACHE_NAME)
          .map(k => caches.delete(k))
    )),
  ]));
});

self.addEventListener('fetch', event => {
  const req = event.request;
  if (req.method !== 'GET') return;
  const url = new URL(req.url);
  if (url.origin !== self.location.origin) return;       // cross-origin: bypass
  if (url.pathname.endsWith('.json')) return;            // data feeds: bypass

  // HTML — network-first so deploys propagate; cache fallback for offline.
  if (url.pathname === '/' || url.pathname.endsWith('.html')) {
    event.respondWith(
      fetch(req).then(resp => {
        if (resp && resp.ok && resp.type !== 'opaque') {
          const copy = resp.clone();
          caches.open(CACHE_NAME).then(cache => cache.put(req, copy));
        }
        return resp;
      }).catch(() => caches.match(req, { ignoreSearch: true }))
    );
    return;
  }

  // JS / CSS — cache-first with background revalidate. Lazy-loaded
  // chunks use cache-bust query strings; ignoreSearch matches them all
  // to a single cached entry per file.
  if (url.pathname.endsWith('.js') || url.pathname.endsWith('.css')) {
    event.respondWith(
      caches.match(req, { ignoreSearch: true }).then(cached => {
        const fetchPromise = fetch(req).then(resp => {
          if (resp && resp.ok && resp.type !== 'opaque') {
            const copy = resp.clone();
            caches.open(CACHE_NAME).then(cache => cache.put(req, copy));
          }
          return resp;
        }).catch(() => cached);
        return cached || fetchPromise;
      })
    );
    return;
  }
});
