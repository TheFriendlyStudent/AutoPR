/**
 * youtube-proxy.js — Cloudflare Worker
 *
 * Detects live streams using YouTube's public RSS feeds — no search.list calls,
 * no quota burn. Only channels.list is used (1 unit per channel) for channel
 * name + thumbnail. Live detection is completely quota-free via RSS.
 *
 * DEPLOY STEPS:
 *   1. npm install -g wrangler
 *   2. wrangler login
 *   3. wrangler secret put YOUTUBE_API_KEY   (paste your key — only needed for thumbnails/names)
 *   4. wrangler deploy
 *   5. Update PROXY_BASE in index.html to your worker's URL
 *
 * ENDPOINTS:
 *   GET /livestreams?ids=UCxxx,UCyyy   → returns live status + thumbnails for channel IDs
 *   GET /health                        → returns { ok: true, ts: <timestamp> }
 */

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    const cors = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
      "Cache-Control": "no-store",
    };

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: cors });
    }

    if (url.pathname === "/health") {
      return Response.json({ ok: true, ts: Date.now() }, { headers: cors });
    }

    if (url.pathname === "/livestreams") {
      const ids = url.searchParams.get("ids");
      if (!ids) {
        return Response.json({ error: "Missing required param: ids" }, { status: 400, headers: cors });
      }

      const channelIds = ids.split(",").map(s => s.trim()).filter(Boolean).slice(0, 20);

      // Optional: per-channel keyword filters passed as JSON
      // e.g. ?keywords={"UCxxx":["basketball","hoops"]}
      let keywordsMap = {};
      try {
        const kw = url.searchParams.get("keywords");
        if (kw) keywordsMap = JSON.parse(kw);
      } catch (e) { /* malformed JSON — ignore */ }

      const apiKey = env.YOUTUBE_API_KEY;

      try {
        const results = [];
        for (const id of channelIds) {
          results.push(await fetchChannelData(id, apiKey, keywordsMap[id] ?? []));
        }
        log("OK", `Fetched ${channelIds.length} channels (RSS live detection)`, request);
        return Response.json({ channels: results }, { headers: cors });
      } catch (err) {
        log("ERROR", err.message, request);
        return Response.json({ error: "Fetch error", detail: err.message }, { status: 502, headers: cors });
      }
    }

    return Response.json({ error: "Not found" }, { status: 404, headers: cors });
  },
};

// ── Fetch channel data ───────────────────────────────────────────────────────
// Live detection: parses the channel's public RSS feed for <yt:isLiveContent> flags.
// No quota used. Channel name + thumbnail: one channels.list call (1 unit).
async function fetchChannelData(channelId, apiKey, keywords = []) {

  // Run RSS (quota-free) and channel metadata (1 unit) in parallel
  const [liveResult, metaResult] = await Promise.allSettled([
    detectLiveViaRSS(channelId, keywords),
    fetchChannelMeta(channelId, apiKey),
  ]);

  const live = liveResult.status === "fulfilled" ? liveResult.value : { isLive: false, videoId: null, liveTitle: null };
  const meta = metaResult.status === "fulfilled" ? metaResult.value : { name: channelId, thumb: null };

  // Log individual failures without aborting the whole request
  if (liveResult.status === "rejected") {
    console.log(JSON.stringify({ level: "WARN", message: `RSS failed for ${channelId}: ${liveResult.reason}` }));
  }
  if (metaResult.status === "rejected") {
    console.log(JSON.stringify({ level: "WARN", message: `Meta failed for ${channelId}: ${metaResult.reason}` }));
  }

  return {
    channelId,
    name: meta.name,
    thumb: meta.thumb,
    isLive: live.isLive,
    videoId: live.videoId,
    liveTitle: live.liveTitle,
  };
}

// ── RSS live detection (no API key, no quota) ────────────────────────────────
// YouTube exposes a public Atom feed for every channel. Live videos appear in
// the feed with a <yt:isLiveContent> element. We grab the feed and scan for it.
async function detectLiveViaRSS(channelId, keywords = []) {
  const livePageUrl = `https://www.youtube.com/channel/${channelId}/live`;

  try {
    const res = await fetch(livePageUrl, {
      headers: {
        "User-Agent": "Mozilla/5.0 (compatible; NutmegSports/1.0)",
        "Accept-Language": "en-US,en;q=0.9",
      },
      redirect: "follow",
    });

    if (!res.ok) throw new Error(`live page ${res.status}`);
    const html = await res.text();

    // Verify this page actually belongs to the channel we requested.
    // YouTube can redirect /live to unrelated content.
    if (!html.includes(channelId)) {
      console.log(JSON.stringify({ level: "DEBUG", channelId, signal: "channel-mismatch" }));
      return { isLive: false, videoId: null, liveTitle: null };
    }

    // "isLive":true appears for both active and scheduled streams.
    // "isLiveNow":true is only active broadcasts but isn't always present in HTML.
    // Use isLive as primary signal, but cross-check with absence of "isUpcoming":true
    // which YouTube sets on scheduled-but-not-started streams.
    const isLive     = /"isLive"\s*:\s*true/.test(html);
    const isUpcoming = /"isUpcoming"\s*:\s*true/.test(html);
    const isLiveNow  = /"isLiveNow"\s*:\s*true/.test(html);

    const liveNow = (isLive && !isUpcoming) || isLiveNow;

    if (!liveNow) {
      console.log(JSON.stringify({ level: "DEBUG", channelId, signal: "not-live-now", isLive, isUpcoming, isLiveNow }));
      return { isLive: false, videoId: null, liveTitle: null };
    }

    // Extract video ID from the canonical watch URL in the page.
    // This is more reliable than matching "videoId" in JSON which can
    // pick up UUIDs or other non-video-ID strings.
    const videoIdMatch = html.match(/watch\?v=([\w-]{11})(?:[^-\w]|$)/)
                      || html.match(/"videoId"\s*:\s*"([a-zA-Z0-9_-]{11})(?:[^a-zA-Z0-9_-])/);
    const videoId = videoIdMatch?.[1] ?? null;

    // Extract title
    const titleMatch = html.match(/"title"\s*:\s*\{"runs"\s*:\s*\[\{"text"\s*:\s*"([^"]+)"/)
                    || html.match(/<title>([^<|]+)/);
    const title = (titleMatch?.[1] ?? "").trim()
      .replace(/&amp;/g,"&").replace(/&lt;/g,"<").replace(/&gt;/g,">");

    // Apply keyword filter
    if (keywords.length > 0) {
      const matched = keywords.some(kw => title.toLowerCase().includes(kw.toLowerCase()));
      if (!matched) {
        console.log(JSON.stringify({
          level: "DEBUG", channelId, signal: "keyword-skip",
          videoId, title: title.slice(0, 80), keywords,
        }));
        return { isLive: false, videoId: null, liveTitle: null };
      }
    }

    console.log(JSON.stringify({
      level: "DEBUG", channelId, signal: "live-confirmed",
      videoId, title: title.slice(0, 80),
    }));

    return { isLive: true, videoId, liveTitle: title };

  } catch (e) {
    throw new Error(`live page check failed for ${channelId}: ${e.message}`);
  }
}

// ── Channel metadata via API (name + thumbnail, 1 unit per channel) ──────────
async function fetchChannelMeta(channelId, apiKey) {
  if (!apiKey) return { name: channelId, thumb: null };

  const url = `https://www.googleapis.com/youtube/v3/channels?part=snippet&id=${channelId}&key=${apiKey}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`channels API ${res.status}`);
  const data = await res.json();

  const snippet = data.items?.[0]?.snippet ?? {};
  return {
    name:  snippet.title ?? channelId,
    thumb: snippet.thumbnails?.default?.url ?? snippet.thumbnails?.medium?.url ?? null,
  };
}

// ── Structured log ───────────────────────────────────────────────────────────
function log(level, message, request) {
  const ip = request.headers.get("CF-Connecting-IP") ?? "unknown";
  console.log(JSON.stringify({
    level,
    message,
    ipHash: simpleHash(ip),
    ts: new Date().toISOString(),
    path: new URL(request.url).pathname,
  }));
}

function simpleHash(str) {
  let h = 0;
  for (let i = 0; i < str.length; i++) h = (Math.imul(31, h) + str.charCodeAt(i)) | 0;
  return (h >>> 0).toString(16);
}

