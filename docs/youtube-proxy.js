/**
 * youtube-proxy.js — Optimized Cloudflare Worker
 *
 * Improvements:
 * - Edge cache (10 min) prevents duplicate YouTube API calls
 * - Cache key normalization ignores frontend timestamps
 * - Stale cache fallback if YouTube API fails
 *
 * Quota impact:
 * Old: 10 channels × 100 = 1000 units every refresh
 * New: 1000 units every 10 minutes globally
 * ≈ 144,000/day worst case (usually far less)
 */

const CACHE_SECONDS = 600; // 10 minutes

export default {
  async fetch(request, env, ctx) {

    const url = new URL(request.url);

    const cors = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
    };

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: cors });
    }

    if (url.pathname === "/get_hls") {
      const ytUrl = url.searchParams.get("url");
      if (!ytUrl) return Response.json({ error: "Missing url" }, { status: 400, headers: cors });

      try {
        // Fetch the raw HTML of the YouTube page
        const ytResp = await fetch(ytUrl, {
          headers: { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)" }
        });
        const html = await ytResp.text();

        // Regex to find the hidden HLS manifest URL YouTube uses for its web player
        const match = html.match(/"hlsManifestUrl":"([^"]+)"/);
        
        if (match && match[1]) {
          return Response.json({ hls_url: match[1] }, { headers: cors });
        } else {
          return Response.json({ error: "Stream is not live or HLS not found" }, { status: 404, headers: cors });
        }
      } catch (err) {
        return Response.json({ error: err.message }, { status: 500, headers: cors });
      }
    }

    if (url.pathname === "/health") {
      return Response.json({ ok: true, ts: Date.now() }, { headers: cors });
    }

    if (url.pathname === "/livestreams") {

      const ids = url.searchParams.get("ids");
      if (!ids) {
        return Response.json(
          { error: "Missing required param: ids" },
          { status: 400, headers: cors }
        );
      }

      const apiKey = env.YOUTUBE_API_KEY;
      if (!apiKey) {
        return Response.json(
          { error: "Server misconfiguration: API key not set" },
          { status: 500, headers: cors }
        );
      }

      const channelIds = ids
        .split(",")
        .map(s => s.trim())
        .filter(Boolean)
        .slice(0, 20);

      // Parse keyword filters
      let keywordsMap = {};
      try {
        const kw = url.searchParams.get("keywords");
        if (kw) keywordsMap = JSON.parse(kw);
      } catch {}

      // ─────────────────────────────
      // EDGE CACHE
      // ─────────────────────────────

      const cache = caches.default;

      // Normalize cache key (ignore timestamps)
      const cacheUrl = new URL(request.url);
      cacheUrl.searchParams.delete("t");

      const cacheKey = new Request(cacheUrl.toString(), request);

      const cached = await cache.match(cacheKey);

      if (cached) {
        return addCors(cached, cors);
      }

      // ─────────────────────────────
      // FETCH LIVE STATUS
      // ─────────────────────────────

      try {

        const liveResults = await Promise.allSettled(
          channelIds.map(id =>
            fetchLiveViaSearchAPI(
              id,
              apiKey,
              keywordsMap[id] ?? []
            )
          )
        );

        const metaMap = await fetchChannelMetaBatch(channelIds, apiKey);

        const channels = channelIds.map((id, i) => {

          const liveResult = liveResults[i];

          const live =
            liveResult.status === "fulfilled"
              ? liveResult.value
              : { isLive: false, videoId: null, liveTitle: null };

          const meta = metaMap[id] ?? { name: id, thumb: null };

          return {
            channelId: id,
            name: meta.name,
            thumb: meta.thumb,
            isLive: live.isLive,
            videoId: live.videoId,
            liveTitle: live.liveTitle
          };
        });

        const response = Response.json(
          { channels },
          {
            headers: {
              ...cors,
              "Cache-Control": `public, max-age=${CACHE_SECONDS}`
            }
          }
        );

        ctx.waitUntil(cache.put(cacheKey, response.clone()));

        return response;

      } catch (err) {

        // Fallback: return stale cache if available
        const stale = await cache.match(cacheKey);
        if (stale) {
          return addCors(stale, cors);
        }

        return Response.json(
          { error: "Fetch error", detail: err.message },
          { status: 502, headers: cors }
        );
      }
    }

    return Response.json({ error: "Not found" }, { status: 404, headers: cors });
  }
};

// ─────────────────────────────
// SEARCH API LIVE DETECTION
// ─────────────────────────────

async function fetchLiveViaSearchAPI(channelId, apiKey, keywords = []) {

  const params = new URLSearchParams({
    part: "id,snippet",
    channelId,
    eventType: "live",
    type: "video",
    maxResults: "10",
    key: apiKey
  });

  const res = await fetch(
    `https://www.googleapis.com/youtube/v3/search?${params}`
  );

  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`search.list ${res.status}: ${body.slice(0, 200)}`);
  }

  const data = await res.json();

  if (!data.items || data.items.length === 0) {
    return { isLive: false, videoId: null, liveTitle: null };
  }

  let chosen = null;

  if (keywords.length > 0) {

    for (const item of data.items) {

      const title = item.snippet?.title ?? "";

      const matched = keywords.some(kw =>
        title.toLowerCase().includes(kw.toLowerCase())
      );

      if (matched) {
        chosen = item;
        break;
      }
    }

    if (!chosen) {
      return { isLive: false, videoId: null, liveTitle: null };
    }

  } else {

    chosen = data.items[0];

  }

  return {
    isLive: true,
    videoId: chosen.id?.videoId ?? null,
    liveTitle: chosen.snippet?.title ?? ""
  };
}

// ─────────────────────────────
// CHANNEL METADATA (BATCHED)
// ─────────────────────────────

async function fetchChannelMetaBatch(channelIds, apiKey) {

  if (!channelIds.length) return {};

  const params = new URLSearchParams({
    part: "snippet",
    id: channelIds.join(","),
    key: apiKey
  });

  const res = await fetch(
    `https://www.googleapis.com/youtube/v3/channels?${params}`
  );

  if (!res.ok) return {};

  const data = await res.json();

  const map = {};

  for (const item of data.items ?? []) {

    const snippet = item.snippet ?? {};

    map[item.id] = {
      name: snippet.title ?? item.id,
      thumb:
        snippet.thumbnails?.default?.url ??
        snippet.thumbnails?.medium?.url ??
        null
    };
  }

  return map;
}

// ─────────────────────────────
// ADD CORS TO CACHED RESPONSES
// ─────────────────────────────

function addCors(resp, cors) {

  const newHeaders = new Headers(resp.headers);

  for (const k in cors) {
    newHeaders.set(k, cors[k]);
  }

  return new Response(resp.body, {
    status: resp.status,
    statusText: resp.statusText,
    headers: newHeaders
  });
}