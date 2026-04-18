// src/index.js
// Fragplace catalog → R2 sync worker
// POST /api/sync-brands with { offset, limit, dryRun }

const FRAGPLACE_URL = "https://fragrance-api.p.rapidapi.com/multi-search";
const RAPIDAPI_HOST = "fragrance-api.p.rapidapi.com";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Content-Type": "application/json",
};

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders });
    }

    if (url.pathname === "/api/sync-brands" && request.method === "POST") {
      return handleSyncBrands(request, env);
    }

    if (url.pathname === "/" || url.pathname === "/health") {
      return json({
        worker: "komanda-sync-worker",
        status: "ok",
        endpoints: ["POST /api/sync-brands"],
      });
    }

    return json({ error: "Not found" }, 404);
  },
};

async function handleSyncBrands(request, env) {
  if (!env.RAPIDAPI_KEY) {
    return json({ error: "RAPIDAPI_KEY not configured" }, 500);
  }
  if (!env.MASTER_DB) {
    return json({ error: "MASTER_DB R2 binding not configured" }, 500);
  }

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
      return json({
        error: `Fragplace error ${res.status}`,
        detail: errText.slice(0, 300),
      }, res.status);
    }

    const data = await res.json();
    const hits = data?.results?.[0]?.hits || [];
    const estimatedTotal = data?.results?.[0]?.estimatedTotalHits ?? hits.length;

    if (dryRun) {
      return json({
        dryRun: true,
        received: hits.length,
        estimatedTotal,
        sample: hits.slice(0, 3),
      });
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
    const written = normalizedRecords.length;

    const indexEntries = normalizedRecords.map((n) => ({
      id: n.id,
      name: n.name,
      slug: n.slug,
      popularityScore: n.popularityScore,
    }));

    let existingIndex = [];
    try {
      const existing = await env.MASTER_DB.get("brands/index.json");
      if (existing) existingIndex = JSON.parse(await existing.text());
    } catch {}

    const byId = new Map(existingIndex.map((e) => [e.id, e]));
    for (const entry of indexEntries) byId.set(entry.id, entry);
    const mergedIndex = Array.from(byId.values()).sort((a, b) => a.id - b.id);

    const hasMore = hits.length === limit;

    const manifest = {
      syncedAt: new Date().toISOString(),
      type: "brands",
      offset,
      limit,
      writtenThisRun: written,
      totalInIndex: mergedIndex.length,
      estimatedTotal,
      hasMore,
      nextOffset: offset + hits.length,
    };

    await Promise.all([
      env.MASTER_DB.put("brands/index.json", JSON.stringify(mergedIndex), {
        httpMetadata: { contentType: "application/json" },
      }),
      env.MASTER_DB.put(
        `manifest/brands/${Date.now()}.json`,
        JSON.stringify(manifest),
        { httpMetadata: { contentType: "application/json" } }
      ),
    ]);

    return json({ success: true, ...manifest });
  } catch (err) {
    return json({ error: err.message }, 500);
  }
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
