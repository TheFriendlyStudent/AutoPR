/**
 * youtube-proxy.js — Cloudflare Worker
 *
 * Proxies YouTube Data API v3 requests so the API key is NEVER exposed
 * to the frontend. Deploy this as a Cloudflare Worker and store your
 * YouTube Data API key as a Worker Secret named `YOUTUBE_API_KEY`.
 *
 * DEPLOY STEPS:
 *   1. npm install -g wrangler
 *   2. wrangler login
 *   3. wrangler secret put YOUTUBE_API_KEY   (paste your key)
 *   4. wrangler deploy
 *   5. Update PROXY_BASE in index.html to your worker's URL
 *
 * ENDPOINTS:
 *   GET /livestreams?ids=UCxxx,UCyyy   → returns live status + thumbnails for channel IDs
 *   GET /health                        → returns { ok: true, ts: <timestamp> }
 *
 * All requests are logged with timestamp, IP (hashed), and result summary.
 */

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // ── CORS headers so GitHub Pages can call this ──────────────────────────
    const cors = {
      "Access-Control-Allow-Origin": "*",        // restrict to your domain in prod
      "Access-Control-Allow-Methods": "GET, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
      "Cache-Control": "no-store",
    };

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: cors });
    }

    // ── Health check ────────────────────────────────────────────────────────
    if (url.pathname === "/health") {
      return Response.json({ ok: true, ts: Date.now() }, { headers: cors });
    }

    // ── /livestreams ────────────────────────────────────────────────────────
    if (url.pathname === "/livestreams") {
      const ids = url.searchParams.get("ids");
      if (!ids) {
        return Response.json(
          { error: "Missing required param: ids" },
          { status: 400, headers: cors }
        );
      }

      const channelIds = ids
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean)
        .slice(0, 20); // hard cap — protect quota

      const apiKey = env.YOUTUBE_API_KEY;
      if (!apiKey) {
        log("ERROR", "YOUTUBE_API_KEY secret not set", request);
        return Response.json(
          { error: "Server misconfiguration" },
          { status: 500, headers: cors }
        );
      }

      try {
        const results = await Promise.all(
          channelIds.map((id) => fetchChannelData(id, apiKey))
        );

        log("OK", `Fetched ${channelIds.length} channels`, request);

        return Response.json({ channels: results }, { headers: cors });
      } catch (err) {
        log("ERROR", err.message, request);
        return Response.json(
          { error: "YouTube API error", detail: err.message },
          { status: 502, headers: cors }
        );
      }
    }

    return Response.json({ error: "Not found" }, { status: 404, headers: cors });
  },
};

// ── Fetch channel metadata + live stream info ────────────────────────────────
async function fetchChannelData(channelId, apiKey) {
  const base = "https://www.googleapis.com/youtube/v3";

  // 1. Channel snippet + thumbnails
  const chanUrl = `${base}/channels?part=snippet&id=${channelId}&key=${apiKey}`;
  const chanRes = await fetch(chanUrl);
  if (!chanRes.ok) throw new Error(`channels API ${chanRes.status}`);
  const chanData = await chanRes.json();

  const channel = chanData.items?.[0];
  const snippet = channel?.snippet ?? {};
  const thumb =
    snippet.thumbnails?.default?.url ??
    snippet.thumbnails?.medium?.url ??
    null;

  // 2. Active live streams for this channel
  const liveUrl =
    `${base}/search?part=snippet&channelId=${channelId}` +
    `&eventType=live&type=video&maxResults=1&key=${apiKey}`;
  const liveRes = await fetch(liveUrl);
  if (!liveRes.ok) throw new Error(`search API ${liveRes.status}`);
  const liveData = await liveRes.json();

  const liveItem = liveData.items?.[0];
  const isLive = !!liveItem;
  const videoId = liveItem?.id?.videoId ?? null;
  const liveTitle = liveItem?.snippet?.title ?? null;

  return {
    channelId,
    name: snippet.title ?? channelId,
    thumb,
    isLive,
    videoId,
    liveTitle,
  };
}

// ── Structured log (visible in Cloudflare Worker logs dashboard) ─────────────
function log(level, message, request) {
  const ip = request.headers.get("CF-Connecting-IP") ?? "unknown";
  // Hash IP for privacy — never log raw IPs
  const ipHash = simpleHash(ip);
  console.log(
    JSON.stringify({
      level,
      message,
      ipHash,
      ts: new Date().toISOString(),
      path: new URL(request.url).pathname,
    })
  );
}

function simpleHash(str) {
  let h = 0;
  for (let i = 0; i < str.length; i++) {
    h = (Math.imul(31, h) + str.charCodeAt(i)) | 0;
  }
  return (h >>> 0).toString(16);
}
