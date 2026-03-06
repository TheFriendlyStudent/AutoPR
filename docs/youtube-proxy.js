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
      const apiKey = env.YOUTUBE_API_KEY;

      try {
        // Process sequentially to avoid Cloudflare's concurrent fetch limit,
        // which causes the "stalled HTTP response" deadlock warning.
        const results = [];
        for (const id of channelIds) {
          results.push(await fetchChannelData(id, apiKey));
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
async function fetchChannelData(channelId, apiKey) {

  // Run RSS (quota-free) and channel metadata (1 unit) in parallel
  const [liveResult, metaResult] = await Promise.allSettled([
    detectLiveViaRSS(channelId),
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
async function detectLiveViaRSS(channelId) {
  const feedUrl = `https://www.youtube.com/feeds/videos.xml?channel_id=${channelId}`;
  const res = await fetch(feedUrl, {
    headers: { "User-Agent": "Mozilla/5.0 (compatible; NutmegSports/1.0)" },
  });

  if (!res.ok) throw new Error(`RSS ${res.status} for ${channelId}`);
  const xml = await res.text();

  const entries = [...xml.matchAll(/<entry>([\s\S]*?)<\/entry>/g)];

  // Log a digest of every entry for this channel so we can see what signals
  // are present — video IDs, titles, and every live-related attribute.
  const digest = entries.map(([, entry]) => {
    const videoId    = entry.match(/<yt:videoId>([\w-]+)<\/yt:videoId>/)?.[1] ?? "?";
    const title      = entry.match(/<title>([\s\S]*?)<\/title>/)?.[1]?.slice(0,60) ?? "?";
    const isLiveTag  = /<yt:isLiveContent>1<\/yt:isLiveContent>/i.test(entry);
    const mediaState = entry.match(/media:status[^>]*state="([^"]+)"/)?.[1] ?? null;
    const liveBroadcastContent = entry.match(/liveBroadcastContent[^>]*>([^<]+)/)?.[1] ?? null;
    return { videoId, title, isLiveTag, mediaState, liveBroadcastContent };
  });

  console.log(JSON.stringify({
    level: "DEBUG",
    channelId,
    entryCount: entries.length,
    digest,
    // Also log the raw first 2000 chars of the feed in case the structure is unexpected
    rawSnippet: xml.slice(0, 2000),
  }));

  for (const [, entry] of entries) {
    const isLive = /<yt:isLiveContent>1<\/yt:isLiveContent>/i.test(entry)
                || /<media:status state="active"[^>]*>/i.test(entry);

    if (isLive) {
      const videoIdMatch = entry.match(/<yt:videoId>([\w-]+)<\/yt:videoId>/);
      const titleMatch   = entry.match(/<title>([\s\S]*?)<\/title>/);
      return {
        isLive: true,
        videoId:   videoIdMatch?.[1] ?? null,
        liveTitle: titleMatch?.[1]?.replace(/&amp;/g,"&").replace(/&lt;/g,"<").replace(/&gt;/g,">") ?? null,
      };
    }
  }

  return { isLive: false, videoId: null, liveTitle: null };
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

