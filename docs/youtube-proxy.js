/**
 * youtube-proxy.js — Cloudflare Worker
 *
 * Uses YouTube Data API v3 search.list (eventType=live) to accurately detect
 * live streams and return the correct video IDs.
 *
 * WHY search.list instead of page scraping:
 *   Page scraping /channel/<id>/live is unreliable — YouTube redirects that URL
 *   to whatever it considers relevant for the channel, not necessarily a live stream.
 *   The videoId regex can match non-live videos, and channels streaming multiple
 *   events return whichever video happens to be first in the page HTML.
 *   search.list?eventType=live is the authoritative source: it only returns
 *   videos that are actively live RIGHT NOW, and gives you the exact video ID.
 *
 * QUOTA NOTE:
 *   search.list costs 100 units/call. With 10 channels = 1000 units/refresh.
 *   YouTube's default daily quota is 10,000 units, so you get ~10 full refreshes/day.
 *   channels.list costs 1 unit/call (used for thumbnails) — negligible.
 *   To stay within quota, the frontend auto-refreshes every 3 minutes — adjust if needed.
 *
 * DEPLOY STEPS:
 *   1. npm install -g wrangler
 *   2. wrangler login
 *   3. wrangler secret put YOUTUBE_API_KEY
 *   4. wrangler deploy
 *   5. PROXY_BASE in index.html should already point to your worker URL.
 *
 * ENDPOINTS:
 *   GET /livestreams?ids=UCxxx,UCyyy   → live status + video IDs for each channel
 *   GET /health                        → { ok: true, ts: <timestamp> }
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

      // Optional per-channel keyword filters (JSON map: channelId → string[])
      let keywordsMap = {};
      try {
        const kw = url.searchParams.get("keywords");
        if (kw) keywordsMap = JSON.parse(kw);
      } catch (e) { /* ignore malformed JSON */ }

      const apiKey = env.YOUTUBE_API_KEY;

      if (!apiKey) {
        log("ERROR", "YOUTUBE_API_KEY secret not set", request);
        return Response.json(
          { error: "Server misconfiguration: API key not set" },
          { status: 500, headers: cors }
        );
      }

      try {
        // Step 1: Fetch all live streams for all channels in parallel using search.list.
        //         Each call is per-channel (channelId param only accepts one ID at a time).
        //         We run them in parallel to minimise latency.
        const liveResults = await Promise.allSettled(
          channelIds.map(id => fetchLiveViaSearchAPI(id, apiKey, keywordsMap[id] ?? []))
        );

        // Step 2: Fetch channel metadata (name + thumbnail) in a single batched call.
        //         channels.list accepts a comma-separated list of up to 50 IDs.
        const metaMap = await fetchChannelMetaBatch(channelIds, apiKey);

        const channels = channelIds.map((id, i) => {
          const liveResult = liveResults[i];
          const live = liveResult.status === "fulfilled"
            ? liveResult.value
            : { isLive: false, videoId: null, liveTitle: null };

          if (liveResult.status === "rejected") {
            console.log(JSON.stringify({
              level: "WARN",
              message: `Live check failed for ${id}: ${liveResult.reason?.message ?? liveResult.reason}`,
            }));
          }

          const meta = metaMap[id] ?? { name: id, thumb: null };

          return {
            channelId: id,
            name: meta.name,
            thumb: meta.thumb,
            isLive: live.isLive,
            videoId: live.videoId,
            liveTitle: live.liveTitle,
          };
        });

        const liveCount = channels.filter(c => c.isLive).length;
        log("OK", `${liveCount}/${channelIds.length} channels live`, request);

        return Response.json({ channels }, { headers: cors });

      } catch (err) {
        log("ERROR", err.message, request);
        return Response.json({ error: "Fetch error", detail: err.message }, { status: 502, headers: cors });
      }
    }

    return Response.json({ error: "Not found" }, { status: 404, headers: cors });
  },
};

// ── Live detection via search.list (accurate, returns correct video ID) ───────
//
// search.list?part=id,snippet&channelId=<id>&eventType=live&type=video
//   → Only returns videos that are ACTIVELY live right now for that channel.
//   → Returns the exact video ID — no regex guesswork, no wrong-video issues.
//   → Costs 100 quota units per channel.
//
async function fetchLiveViaSearchAPI(channelId, apiKey, keywords = []) {
  const params = new URLSearchParams({
    part: "id,snippet",
    channelId,
    eventType: "live",
    type: "video",
    maxResults: "10",   // get up to 10 in case channel has multiple concurrent streams
    key: apiKey,
  });

  const res = await fetch(`https://www.googleapis.com/youtube/v3/search?${params}`);

  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`search.list ${res.status}: ${body.slice(0, 200)}`);
  }

  const data = await res.json();

  if (!data.items || data.items.length === 0) {
    console.log(JSON.stringify({ level: "DEBUG", channelId, signal: "not-live" }));
    return { isLive: false, videoId: null, liveTitle: null };
  }

  // If keyword filter is set, find the first item whose title matches
  let chosenItem = null;

  if (keywords.length > 0) {
    for (const item of data.items) {
      const title = item.snippet?.title ?? "";
      const matched = keywords.some(kw => title.toLowerCase().includes(kw.toLowerCase()));
      if (matched) {
        chosenItem = item;
        break;
      }
    }
    if (!chosenItem) {
      // No item matched keywords — channel is live but not for the relevant sport
      console.log(JSON.stringify({
        level: "DEBUG", channelId, signal: "keyword-skip",
        titles: data.items.map(i => (i.snippet?.title ?? "").slice(0, 60)),
        keywords,
      }));
      return { isLive: false, videoId: null, liveTitle: null };
    }
  } else {
    // No keyword filter — use the first (most relevant) live stream
    chosenItem = data.items[0];
  }

  const videoId = chosenItem.id?.videoId ?? null;
  const liveTitle = chosenItem.snippet?.title ?? "";

  console.log(JSON.stringify({
    level: "DEBUG", channelId, signal: "live-confirmed",
    videoId, title: liveTitle.slice(0, 80),
    totalLiveStreams: data.items.length,
  }));

  return { isLive: true, videoId, liveTitle };
}

// ── Channel metadata — batched single API call (1 quota unit total) ───────────
async function fetchChannelMetaBatch(channelIds, apiKey) {
  if (!channelIds.length) return {};

  const params = new URLSearchParams({
    part: "snippet",
    id: channelIds.join(","),
    key: apiKey,
  });

  const res = await fetch(`https://www.googleapis.com/youtube/v3/channels?${params}`);
  if (!res.ok) {
    console.log(JSON.stringify({
      level: "WARN",
      message: `channels.list failed with status ${res.status}`,
    }));
    return {};
  }

  const data = await res.json();
  const map = {};

  for (const item of (data.items ?? [])) {
    const snippet = item.snippet ?? {};
    map[item.id] = {
      name:  snippet.title ?? item.id,
      thumb: snippet.thumbnails?.default?.url
          ?? snippet.thumbnails?.medium?.url
          ?? null,
    };
  }

  return map;
}

// ── Structured log ────────────────────────────────────────────────────────────
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