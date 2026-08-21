const POD_CACHE = "miplay-pod-shell";
const POD_SHELL = [
  "/pod",
  "/pod/manifest.webmanifest",
  "/static/icon.png",
  "/static/poster.png",
  "/static/lib/lucide.min.js"
];

self.addEventListener("install", (event) => {
  event.waitUntil(caches.open(POD_CACHE).then((cache) => cache.addAll(POD_SHELL)));
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) => Promise.all(
      keys
        .filter((key) => key.startsWith("miplay-pod-") && key !== POD_CACHE)
        .map((key) => caches.delete(key))
    ))
  );
  self.clients.claim();
});

self.addEventListener("fetch", (event) => {
  const request = event.request;
  if (request.method !== "GET") return;

  const url = new URL(request.url);
  if (url.origin !== self.location.origin) return;
  if (url.pathname.startsWith("/api/") || url.pathname.startsWith("/stream/")) return;

  if (request.mode === "navigate" && url.pathname === "/pod") {
    event.respondWith(
      fetch(request)
        .then((response) => {
          const copy = response.clone();
          caches.open(POD_CACHE).then((cache) => cache.put("/pod", copy));
          return response;
        })
        .catch(() => caches.match("/pod"))
    );
    return;
  }

  if (POD_SHELL.includes(url.pathname)) {
    event.respondWith(
      caches.match(request).then((cached) => cached || fetch(request).then((response) => {
        const copy = response.clone();
        caches.open(POD_CACHE).then((cache) => cache.put(request, copy));
        return response;
      }))
    );
  }
});
