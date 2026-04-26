// src/index.js
// Fragplace catalog → R2 sync worker
// Endpoints:
//   GET  /                      -> health + endpoint list
//   GET  /api/status            -> R2 listing summary + last rebuild state
//   POST /api/sync-brands       -> fetch & write a brand batch to R2
//   POST /api/rebuild-index     -> process ONE chunk of rebuild, return continuation
//   POST /api/probe-fragplace   -> read-only Fragplace schema/EAN probe
//   POST /api/probe-fragella    -> read-only Fragella search probe by scent name

const FRAGPLACE_URL = "https://fragrance-api.p.rapidapi.com/multi-search";
const RAPIDAPI_HOST = "fragrance-api.p.rapidapi.com";
const FRAGELLA_BASE = "https://api.fragella.com/api/v1";

const REBUILD_CHUNK_SIZE = 200;

// Brand family keywords — used to filter fuzzy Fragella search results
const BRAND_FAMILIES = {
  "Lattafa":         ["lattafa", "asdaaf", "rave"],
  "Armaf":           ["armaf", "sterling"],
  "Paris Corner":    ["paris corner", "emir", "pendora"],
  "Zimaya":          ["zimaya"],
  "Fragrance World": ["fragrance world", "french avenue"],
  "Alhambra":        ["alhambra"],
  "Khadlaj":         ["khadlaj"],
  "Mancera":         ["mancera"],
  "Montale":         ["montale"],
  "Tom Ford":        ["tom ford"],
  "Dior":            ["dior", "christian dior"],
  "Chanel":          ["chanel"],
};

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
      return handleRebuildChunk(request, env);
    }

    if (url.pathname === "/api/probe-fragplace" && request.method === "POST") {
      return handleProbeFragplace(request, env);
    }

    if (url.pathname === "/api/probe-fragella" && request.method === "POST") {
      return handleProbeFragella(request, env);
    }

    if (url.pathname === "/" || url.pathname === "/health") {
      return json({
        worker: "komanda-sync-worker",
        status: "ok",
        endpoints: [
          "GET  /api/status",
          "POST /api/sync-brands",
          "POST /api/rebuild-index     (processes one chunk; loop until done: true)",
          "POST /api/probe-fragplace   (read-only schema/EAN probe; brandName optional)",
          "POST /api/probe-fragella    (read-only scent search; scentName required, brandName optional)",
        ],
        cron: "Rebuild runs automatically every 6 hours.",
      });
    }

    return json({ error: "Not found" }, 404);
  },

  async scheduled(event, env, ctx) {
    console.log("Cron fired:", event.cron);
    ctx.waitUntil(rebuildFullLoop(env, "cron"));
  },
};

// ---------------------------------------------------------------------------
// /api/probe-fragella — read-only Fragella search probe.
//
// Searches Fragella by scent name (normalized), optionally filters results
// by brand family to remove fuzzy-match noise. Returns best match candidate,
// all raw hits, and field schema seen.
//
// POST body: {
//   scentName: string,    // required — e.g. "Raed Oud" or "Ra`ed Oud"
//   brandName?: string,   // optional — e.g. "Lattafa" (used for family filter)
//   limit?: number        // default 5
// }
// ---------------------------------------------------------------------------
async function handleProbeFragella(request, env) {
  if (!env.FRAGELLA_API_KEY) {
    return json({
      error: "FRAGELLA_API_KEY not configured",
      hint: "Add it in Cloudflare Dashboard → Workers → komanda-sync-worker → Settings → Variables",
    }, 500);
  }

  let body = {};
  try { body = await request.json(); } catch {}

  const scentName = (body.scentName || "").trim();
  const brandName = (body.brandName || "").trim();
  const limit = body.limit || 5;

  if (!scentName) {
    return json({ error: "scentName is required" }, 400);
  }

  // Normalize: strip backticks/apostrophes (Arabic ain transliteration),
  // collapse whitespace, lowercase. Applied to both query and result matching.
  function normalize(str) {
    return (str || "")
      .toLowerCase()
      .replace(/[`''']/g, "")
      .replace(/&/g, "and")
      .replace(/[^\w\s]/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  // Include brand name in search query for precision — "Lattafa Yara" beats "Yara" alone
  const searchQuery = brandName
    ? normalize(brandName + " " + scentName)
    : normalize(scentName);

  try {
    const url = `${FRAGELLA_BASE}/fragrances?search=${encodeURIComponent(searchQuery)}&limit=${limit}`;
    const res = await fetch(url, {
      headers: { "x-api-key": env.FRAGELLA_API_KEY },
    });

    if (!res.ok) {
      const errText = await res.text();
      return json({
        error: `Fragella error ${res.status}`,
        detail: errText.slice(0, 500),
        searchQuery,
      }, res.status);
    }

    const raw = await res.json();
    const hits = Array.isArray(raw) ? raw : (raw.data || raw.results || []);

    // Filter by brand family if brandName provided
    const families = brandName
      ? (BRAND_FAMILIES[brandName] || [brandName.toLowerCase()])
      : null;

    const filtered = families
      ? hits.filter(h => {
          const b = (h.Brand || h.brand || "").toLowerCase();
          return families.some(fam => b.includes(fam));
        })
      : hits;

    // Best match: exact normalized name → partial → first result
    const normScent = normalize(scentName);
    const bestMatch =
      filtered.find(h => normalize(h.Name || h.name) === normScent) ||
      filtered.find(h => normalize(h.Name || h.name).includes(normScent)) ||
      filtered[0] ||
      null;

    // Collect all field names seen across hits
    const fieldsSeen = new Set();
    for (const h of hits) Object.keys(h).forEach(k => fieldsSeen.add(k));

    return json({
      scentName,
      brandName: brandName || null,
      searchQuery,
      totalHits: hits.length,
      filteredHits: filtered.length,
      exactMatch: bestMatch ? normalize(bestMatch.Name || bestMatch.name) === normScent : false,
      bestMatch,
      allFilteredHits: filtered,
      fieldsSeen: Array.from(fieldsSeen).sort(),
      urlUsed: url,
    });
  } catch (err) {
    return json({ error: err.message, searchQuery }, 500);
  }
}

// ---------------------------------------------------------------------------
// /api/probe-fragplace — read-only Fragplace schema/EAN probe.
// ---------------------------------------------------------------------------
async function handleProbeFragplace(request, env) {
  if (!env.RAPIDAPI_KEY) return json({ error: "RAPIDAPI_KEY not configured" }, 500);

  let body = {};
  try { body = await request.json(); } catch {}
  const rawBrandName = typeof body.brandName === "string" ? body.brandName.trim() : "";
  const limit = body.limit || 100;

  const query = {
    indexUid: "fragrances",
    q: "",
    limit,
    offset: 0,
  };
  if (rawBrandName) {
    query.filter = [`brand.name = "${rawBrandName}"`];
  }

  try {
    const res = await fetch(FRAGPLACE_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-rapidapi-host": RAPIDAPI_HOST,
        "x-rapidapi-key": env.RAPIDAPI_KEY,
      },
      body: JSON.stringify({ queries: [query] }),
    });

    if (!res.ok) {
      const errText = await res.text();
      return json({
        error: `Fragplace error ${res.status}`,
        detail: errText.slice(0, 500),
        queryUsed: query,
      }, res.status);
    }

    const data = await res.json();
    const firstResult = data?.results?.[0] || {};
    const hits = firstResult.hits || [];
    const estimatedTotal = firstResult.estimatedTotalHits ?? hits.length;

    let withEAN = 0, withBarcode = 0, withUPC = 0;
    const eanValues = [];
    const fieldNamesSeen = new Set();

    for (const h of hits) {
      for (const k of Object.keys(h)) fieldNamesSeen.add(k);
      const ean = h.ean || h.EAN || h.Ean;
      const barcode = h.barcode || h.Barcode;
      const upc = h.upc || h.UPC || h.Upc;
      if (ean) { withEAN++; eanValues.push({ id: h.id, name: h.name, ean }); }
      if (barcode) withBarcode++;
      if (upc) withUPC++;
    }

    return json({
      brandName: rawBrandName || null,
      filterApplied: Boolean(rawBrandName),
      sampled: hits.length,
      estimatedTotalForBrand: estimatedTotal,
      coverage: {
        withEAN,
        withBarcode,
        withUPC,
        percentEAN: hits.length ? Math.round((withEAN / hits.length) * 100) : 0,
        percentBarcode: hits.length ? Math.round((withBarcode / hits.length) * 100) : 0,
        percentUPC: hits.length ? Math.round((withUPC / hits.length) * 100) : 0,
      },
      topLevelFieldsSeen: Array.from(fieldNamesSeen).sort(),
      firstSampleRaw: hits[0] || null,
      eanSamples: eanValues.slice(0, 10),
      queryUsed: query,
    });
  } catch (err) {
    return json({ error: err.message, queryUsed: query }, 500);
  }
}

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
      note: "Index is derived — run POST /api/rebuild-index (loops until done:true).",
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

async function handleRebuildChunk(request, env) {
  if (!env.MASTER_DB) return json({ error: "MASTER_DB R2 binding not configured" }, 500);

  let body = {};
  try { body = await request.json(); } catch {}
  const { reset = false, cursor = null } = body;

  try {
    let staging = { entries: [], scanned: 0, startedAt: new Date().toISOString() };
    if (!reset) {
      const existing = await env.MASTER_DB.get("state/rebuild-staging.json");
      if (existing) {
        try { staging = JSON.parse(await existing.text()); } catch {}
      }
    }

    const listRes = await env.MASTER_DB.list({
      prefix: "brands/",
      cursor: cursor || undefined,
      limit: REBUILD_CHUNK_SIZE,
    });

    const chunkKeys = listRes.objects
      .map((o) => o.key)
      .filter((k) => k !== "brands/index.json");

    const records = await Promise.all(
      chunkKeys.map(async (key) => {
        try {
          const obj = await env.MASTER_DB.get(key);
          if (!obj) return null;
          return JSON.parse(await obj.text());
        } catch {
          return null;
        }
      })
    );

    let added = 0;
    for (const r of records) {
      if (r && typeof r.id !== "undefined") {
        staging.entries.push({
          id: r.id,
          name: r.name,
          slug: r.slug,
          popularityScore: r.popularityScore ?? null,
        });
        added++;
      }
    }
    staging.scanned += chunkKeys.length;

    const done = !listRes.truncated;

    if (done) {
      const finalEntries = staging.entries.sort((a, b) => a.id - b.id);

      await env.MASTER_DB.put(
        "brands/index.json",
        JSON.stringify(finalEntries),
        { httpMetadata: { contentType: "application/json" } }
      );

      await env.MASTER_DB.delete("state/rebuild-staging.json");

      const finishedAt = new Date().toISOString();
      await env.MASTER_DB.put(
        "state/rebuild.json",
        JSON.stringify({
          status: "done",
          startedAt: staging.startedAt,
          finishedAt,
          filesScanned: staging.scanned,
          indexLength: finalEntries.length,
        }),
        { httpMetadata: { contentType: "application/json" } }
      );

      return json({
        done: true,
        processed: added,
        totalSoFar: staging.scanned,
        indexLength: finalEntries.length,
        finishedAt,
      });
    }

    await env.MASTER_DB.put(
      "state/rebuild-staging.json",
      JSON.stringify(staging),
      { httpMetadata: { contentType: "application/json" } }
    );

    await env.MASTER_DB.put(
      "state/rebuild.json",
      JSON.stringify({
        status: "running",
        startedAt: staging.startedAt,
        filesScanned: staging.scanned,
        lastChunkAt: new Date().toISOString(),
      }),
      { httpMetadata: { contentType: "application/json" } }
    );

    return json({
      done: false,
      processed: added,
      totalSoFar: staging.scanned,
      nextCursor: listRes.cursor,
    });
  } catch (err) {
    try {
      await env.MASTER_DB.put(
        "state/rebuild.json",
        JSON.stringify({
          status: "error",
          error: err.message,
          failedAt: new Date().toISOString(),
        }),
        { httpMetadata: { contentType: "application/json" } }
      );
    } catch {}
    return json({ error: err.message }, 500);
  }
}

async function rebuildFullLoop(env, trigger) {
  try {
    await env.MASTER_DB.delete("state/rebuild-staging.json");
    let cursor = null;
    let safety = 100;
    while (safety-- > 0) {
      const res = await doOneChunkInternal(env, cursor);
      if (res.done) {
        console.log("Cron rebuild done:", res.indexLength, "entries");
        return;
      }
      cursor = res.nextCursor;
      if (!cursor) return;
    }
  } catch (err) {
    console.error("Cron rebuild failed:", err.message);
  }
}

async function doOneChunkInternal(env, cursor) {
  let staging = { entries: [], scanned: 0, startedAt: new Date().toISOString() };
  const existing = await env.MASTER_DB.get("state/rebuild-staging.json");
  if (existing) {
    try { staging = JSON.parse(await existing.text()); } catch {}
  }

  const listRes = await env.MASTER_DB.list({
    prefix: "brands/",
    cursor: cursor || undefined,
    limit: REBUILD_CHUNK_SIZE,
  });

  const chunkKeys = listRes.objects
    .map((o) => o.key)
    .filter((k) => k !== "brands/index.json");

  const records = await Promise.all(
    chunkKeys.map(async (key) => {
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
    if (r && typeof r.id !== "undefined") {
      staging.entries.push({
        id: r.id,
        name: r.name,
        slug: r.slug,
        popularityScore: r.popularityScore ?? null,
      });
    }
  }
  staging.scanned += chunkKeys.length;

  const done = !listRes.truncated;

  if (done) {
    const finalEntries = staging.entries.sort((a, b) => a.id - b.id);
    await env.MASTER_DB.put("brands/index.json", JSON.stringify(finalEntries), {
      httpMetadata: { contentType: "application/json" },
    });
    await env.MASTER_DB.delete("state/rebuild-staging.json");
    await env.MASTER_DB.put(
      "state/rebuild.json",
      JSON.stringify({
        status: "done",
        startedAt: staging.startedAt,
        finishedAt: new Date().toISOString(),
        filesScanned: staging.scanned,
        indexLength: finalEntries.length,
        trigger,
      }),
      { httpMetadata: { contentType: "application/json" } }
    );
    return { done: true, indexLength: finalEntries.length };
  }

  await env.MASTER_DB.put(
    "state/rebuild-staging.json",
    JSON.stringify(staging),
    { httpMetadata: { contentType: "application/json" } }
  );

  return { done: false, nextCursor: listRes.cursor };
}

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
