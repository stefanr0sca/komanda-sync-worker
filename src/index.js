// src/index.js
// Fragplace catalog → R2 sync worker
// Endpoints:
//   GET  /                          -> health + endpoint list
//   GET  /api/status                -> R2 listing summary + last rebuild state
//   POST /api/sync-brands           -> fetch & write a brand batch to R2
//   POST /api/rebuild-index         -> trigger rebuild (fire-and-forget, returns immediately)
//
// Also runs automatically every 6 hours via cron trigger (see wrangler.toml).

const FRAGPLACE_URL = "https://fragrance-api.p.rapidapi.com/multi-search";
const RAPIDAPI_HOST = "fragrance-api.p.rapidapi.com";
const REBUILD_CHUNK_SIZE = 400;

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Content-Type": "application/json",
};

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders });
    }

    if (url.pathname === "/api/sync-brands" && request.method === "POST") {
      return handleSyncBrands(request, env);
    }

    if (url.pathname === "/api/status" && request.method === "GET") {
      return handleStatus(env);
    }

    if (url.pathname === "/api/rebuild-index" && request.method === "POST") {
      return handleRebuildTrigger(env, ctx);
    }

    if (url.pathname === "/" || url.pathname === "/health") {
      return json({
        worker: "komanda-sync-worker",
        status: "ok",
        endpoints: [
          "GET  /api/status",
          "POST /api/sync-brands",
          "POST /api/rebuild-index  (fire-and-forget; poll /api/status for progress)",
        ],
        cron: "Rebuild runs automatically every 6 hours.",
      });
    }

    return json({ error: "Not found" }, 404);
  },

  // Cron trigger — Cloudflare calls this on the schedule defined in wrangler.toml
  async scheduled(event, env, ctx) {
    console.log("Cron trigger fired:", event.cron);
    ctx.waitUntil(rebuildIndexCore(env, "cron"));
  },
};

// ---------------------------------------------------------------------------
// /api/sync-brands
// ---------------------------------------------------------------------------
async function handleSyncBrands(request, env) {
  if (!env.RAPIDAPI_KEY) return json({ error: "RAPIDAPI_KEY not configured" }, 500);
  if (!env.MASTER_DB) return json({ error: "MASTER_DB R2 binding not configured" }, 500);

  let body = {};
  try { body = await request.json(); } catch {}
  const { offset = 0, limit = 500, dryRun = false } = body;

  try {
    const res = await fetch(FRAGPLACE_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-rapidapi-host": RAPIDAPI_HOST,
        "x-rapidapi-key": env.RAPIDAPI_KEY,
      },
      body: JSON.stringify({
        queries: [{ indexUid: "brands", q: "", limit, offset }],
      }),
    });

    if (!res.ok) {
      const errText = await res.text();
      return json({ error: `Fragplace error ${res.status}`, detail: errText.slice(0, 300) }, res.status);
    }

    const data = await res.json();
    const hits = data?.results?.[0]?.hits || [];
    const estimatedTotal = data?.results?.[0]?.estimatedTotalHits ?? hits.length;

    if (dryRun) {
      return json({ dryRun: true, received: hits.length, estimatedTotal, sample: hits.slice(0, 3) });
    }

    const normalizedRecords = hits.map((b) => ({
      id: b.id,
      name: b.name || "",
      slug: slugify(b.name),
      popularityScore: b.popularityScore ?? null,
      description: b.description || "",
      logoUrl: b.image?.url || "",
      status: b.status || "",
      syncedAt: new Date().toISOString(),
      raw: b,
    }));

    const writePromises = normalizedRecords.map((n) =>
      env.MASTER_DB.put(`brands/${n.id}.json`, JSON.stringify(n), {
        httpMetadata: { contentType: "application/json" },
      })
    );
    await Promise.all(writePromises);

    const hasMore = hits.length === limit;
    const manifest = {
      syncedAt: new Date().toISOString(),
      type: "brands",
      offset,
      limit,
      writtenThisRun: normalizedRecords.length,
      estimatedTotal,
      hasMore,
      nextOffset: offset + hits.length,
      note: "Index is derived — run POST /api/rebuild-index after syncs, or wait for cron.",
    };

    await env.MASTER_DB.put(
      `manifest/brands/${Date.now()}.json`,
      JSON.stringify(manifest),
      { httpMetadata: { contentType: "application/json" } }
    );

    return json({ success: true, ...manifest });
  } catch (err) {
    return json({ error: err.message }, 500);
  }
}

// ---------------------------------------------------------------------------
// /api/status — includes the latest rebuild-state record so we can poll progress
// ---------------------------------------------------------------------------
async function handleStatus(env) {
  if (!env.MASTER_DB) return json({ error: "MASTER_DB R2 binding not configured" }, 500);

  try {
    const brandKeys = await listAllKeys(env, "brands/");
    const brandFileKeys = brandKeys.filter((k) => k !== "brands/index.json");

    let indexLength = null;
    try {
      const existing = await env.MASTER_DB.get("brands/index.json");
      if (existing) {
        const parsed = JSON.parse(await existing.text());
        indexLength = Array.isArray(parsed) ? parsed.length : null;
      }
    } catch {}

    const manifestKeys = await listAllKeys(env, "manifest/brands/");

    let rebuildState = null;
    try {
      const stateObj = await env.MASTER_DB.get("state/rebuild.json");
      if (stateObj) rebuildState = JSON.parse(await stateObj.text());
    } catch {}

    return json({
      brands: {
        fileCount: brandFileKeys.length,
        indexLength,
        drift: indexLength === null ? "no index yet" : brandFileKeys.length - indexLength,
        firstKey: brandFileKeys[0] ?? null,
        lastKey: brandFileKeys[brandFileKeys.length - 1] ?? null,
      },
      manifests: {
        count: manifestKeys.length,
        latest: manifestKeys[manifestKeys.length - 1] ?? null,
      },
      rebuild: rebuildState,
    });
  } catch (err) {
    return json({ error: err.message }, 500);
  }
}

// ---------------------------------------------------------------------------
// /api/rebuild-index — trigger only. Returns instantly; real work runs async.
// ---------------------------------------------------------------------------
async function handleRebuildTrigger(env, ctx) {
  if (!env.MASTER_DB) return json({ error: "MASTER_DB R2 binding not configured" }, 500);

  const startedAt = new Date().toISOString();
  await env.MASTER_DB.put(
    "state/rebuild.json",
    JSON.stringify({ status: "running", startedAt, trigger: "http" }),
    { httpMetadata: { contentType: "application/json" } }
  );

  ctx.waitUntil(rebuildIndexCore(env, "http"));

  return json({
    accepted: true,
    status: "rebuild started in background",
    startedAt,
    pollFor: "GET /api/status -> rebuild.status will flip from 'running' to 'done' (or 'error')",
  });
}

// ---------------------------------------------------------------------------
// Core rebuild — called from waitUntil() or cron. Writes progress to state file.
// ---------------------------------------------------------------------------
async function rebuildIndexCore(env, trigger) {
  const startedAt = new Date().toISOString();
  try {
    const allKeys = await listAllKeys(env, "brands/");
    const brandKeys = allKeys.filter((k) => k !== "brands/index.json");

    const valid = [];
    let skipped = 0;

    for (let i = 0; i < brandKeys.length; i += REBUILD_CHUNK_SIZE) {
      const slice = brandKeys.slice(i, i + REBUILD_CHUNK_SIZE);
      const records = await Promise.all(
        slice.map(async (key) => {
          try {
            const obj = await env.MASTER_DB.get(key);
            if (!obj) return null;
            return JSON.parse(await obj.text());
          } catch {
            return null;
          }
        })
      );
      for (const r of records) {
        if (r && typeof r.id !== "undefined") valid.push(r);
        else skipped++;
      }
    }

    const indexEntries = valid
      .map((r) => ({
        id: r.id,
        name: r.name,
        slug: r.slug,
        popularityScore: r.popularityScore ?? null,
      }))
      .sort((a, b) => a.id - b.id);

    await env.MASTER_DB.put(
      "brands/index.json",
      JSON.stringify(indexEntries),
      { httpMetadata: { contentType: "application/json" } }
    );

    await env.MASTER_DB.put(
      "state/rebuild.json",
      JSON.stringify({
        status: "done",
        trigger,
        startedAt,
        finishedAt: new Date().toISOString(),
        filesScanned: brandKeys.length,
        validRecords: valid.length,
        skipped,
        indexLength: indexEntries.length,
      }),
      { httpMetadata: { contentType: "application/json" } }
    );

    console.log("Rebuild done:", indexEntries.length, "entries");
  } catch (err) {
    console.error("Rebuild failed:", err.message);
    await env.MASTER_DB.put(
      "state/rebuild.json",
      JSON.stringify({
        status: "error",
        trigger,
        startedAt,
        finishedAt: new Date().toISOString(),
        error: err.message,
      }),
      { httpMetadata: { contentType: "application/json" } }
    );
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function listAllKeys(env, prefix) {
  const keys = [];
  let cursor = undefined;
  let truncated = true;
  while (truncated) {
    const res = await env.MASTER_DB.list({ prefix, cursor, limit: 1000 });
    for (const obj of res.objects) keys.push(obj.key);
    truncated = res.truncated;
    cursor = res.cursor;
  }
  return keys;
}

function slugify(s) {
  return (s || "")
    .toLowerCase()
    .replace(/'/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function json(obj, status = 200) {
  return new Response(JSON.stringify(obj, null, 2), { status, headers: corsHeaders });
}
