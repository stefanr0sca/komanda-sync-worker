import { WorkflowEntrypoint } from "cloudflare:workers";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Content-Type": "application/json"
};

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders });
    }
    if (url.pathname === "/" || url.pathname === "/health") {
      return json({ worker: "fragrances-catalog-worker", status: "ok", endpoints: [
        "POST /api/workflow/start            — trigger all brands",
        "POST /api/workflow/test             — test one brand {brandSlug?}",
        "GET  /api/workflow/status?id=<id>   — instance status",
        "GET  /api/search                    — search products",
        "GET  /api/product/{category}/{id}   — single product",
        "POST /api/shopify/sync              — sync to Shopify"
      ] });
    }
    if (url.pathname === "/api/workflow/start" && request.method === "POST") {
      ctx.waitUntil(startAllWorkflows(env));
      return json({ status: "triggering", message: "Creating/restarting workflow instances in background." });
    }
    if (url.pathname === "/api/workflow/test" && request.method === "POST") {
      return handleTest(request, env);
    }
    if (url.pathname === "/api/workflow/status" && request.method === "GET") {
      return handleStatus(url, env);
    }
    if (url.pathname === "/api/reindex" && request.method === "POST") {
      return handleReindex(request, env, ctx);
    }
    if (url.pathname === "/api/gemini/test" && request.method === "POST") {
      return handleGeminiTest(request, env);
    }
    if (url.pathname.startsWith("/api/export/") && request.method === "GET") {
      return handleExport(url, env);
    }
    if (url.pathname === "/api/search" && request.method === "GET") {
      return handleSearch(url, env);
    }
    if (url.pathname.startsWith("/api/product/") && request.method === "GET") {
      return handleProduct(url, env);
    }
    if (url.pathname === "/api/shopify/sync" && request.method === "POST") {
      return handleShopifySync(request, env, ctx);
    }
    return json({ error: "Not found" }, 404);
  },

  async scheduled(event, env, ctx) {
    console.log("Cron fired:", event.cron);
    ctx.waitUntil(cronReindex(env));
  }
};

// ─── D1 record builder ────────────────────────────────────────────────────────

function buildD1Record(r) {
  const fe = r.fragella || {};
  const fp = r.fragplace || {};

  const notesTop    = (fe.notes?.Top    || []).map(n => n.name);
  const notesMiddle = (fe.notes?.Middle || []).map(n => n.name);
  const notesBase   = (fe.notes?.Base   || []).map(n => n.name);
  const fpNotes     = (fp.notes || []).map(n => n.name);

  const allNotesSet = new Set([
    ...notesTop, ...notesMiddle, ...notesBase, ...fpNotes,
    ...(fe.generalNotes || [])
  ].map(n => n.toLowerCase()));

  const perfumers = (fp.perfumers || []).map(p => p.name).filter(Boolean);

  return {
    id:               String(r.id),
    category:         "fragrances",
    brand:            r.brand || "",
    brand_slug:       r.brandSlug || "",
    name:             r.name || "",
    slug:             r.slug || "",
    gender:           fe.gender || null,
    year:             fe.year || null,
    country:          fe.country || null,
    longevity:        fe.longevity || null,
    sillage:          fe.sillage || null,
    popularity:       fe.popularity || null,
    rating:           fe.rating || null,
    main_accords:     fe.mainAccords?.length ? JSON.stringify(fe.mainAccords) : null,
    image_url:        fe.imageUrl || fp.imageUrl || null,
    has_fragplace:    fp && Object.keys(fp).length > 0 ? 1 : 0,
    has_fragella:     fe && Object.keys(fe).length > 0 ? 1 : 0,
    has_image:        (fe.imageUrl || fp.imageUrl) ? 1 : 0,
    synced_at:        r.syncedAt || new Date().toISOString(),
    oil_type:         fe.oilType || null,
    notes_top:        notesTop.length    ? JSON.stringify(notesTop)    : null,
    notes_middle:     notesMiddle.length ? JSON.stringify(notesMiddle) : null,
    notes_base:       notesBase.length   ? JSON.stringify(notesBase)   : null,
    notes_all:        allNotesSet.size   ? JSON.stringify([...allNotesSet]) : null,
    accord_weights:   fe.mainAccordsPercentage && Object.keys(fe.mainAccordsPercentage).length
                        ? JSON.stringify(fe.mainAccordsPercentage) : null,
    occasion_scores:  fe.occasionRanking?.length ? JSON.stringify(fe.occasionRanking) : null,
    season_scores:    fe.seasonRanking?.length   ? JSON.stringify(fe.seasonRanking)   : null,
    perfumers:        perfumers.length ? JSON.stringify(perfumers) : null,
    fp_rating:        fp.reviewsScoreAvg || null,
    fp_reviews_count: fp.reviewsCount   || null,
  };
}

function makeStmt(db) {
  return db.prepare(`
    INSERT OR REPLACE INTO products (
      id, category, brand, brand_slug, name, slug,
      gender, year, country, longevity, sillage, popularity, rating,
      main_accords, image_url, has_fragplace, has_fragella, has_image, synced_at,
      oil_type, notes_top, notes_middle, notes_base, notes_all,
      accord_weights, occasion_scores, season_scores,
      perfumers, fp_rating, fp_reviews_count
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  `);
}

function bindRecord(stmt, d) {
  return stmt.bind(
    d.id, d.category, d.brand, d.brand_slug, d.name, d.slug,
    d.gender, d.year, d.country, d.longevity, d.sillage, d.popularity, d.rating,
    d.main_accords, d.image_url, d.has_fragplace, d.has_fragella, d.has_image, d.synced_at,
    d.oil_type, d.notes_top, d.notes_middle, d.notes_base, d.notes_all,
    d.accord_weights, d.occasion_scores, d.season_scores,
    d.perfumers, d.fp_rating, d.fp_reviews_count
  );
}

// ─── Search ───────────────────────────────────────────────────────────────────

async function handleSearch(url, env) {
  const q = url.searchParams.get("q") || "";
  const brand = url.searchParams.get("brand") || "";
  const gender = url.searchParams.get("gender") || "";
  const category = url.searchParams.get("category") || "fragrances";
  const minRating = parseFloat(url.searchParams.get("minRating") || "0");
  const minPopularity = parseFloat(url.searchParams.get("minPopularity") || "0");
  const limit = Math.min(parseInt(url.searchParams.get("limit") || "20"), 100);
  const offset = parseInt(url.searchParams.get("offset") || "0");
  const sortBy = url.searchParams.get("sortBy") || "popularity";

  let conditions = ["category = ?"];
  let params = [category];

  if (q) {
    conditions.push("(name LIKE ? OR brand LIKE ? OR main_accords LIKE ? OR notes_all LIKE ?)");
    params.push(`%${q}%`, `%${q}%`, `%${q}%`, `%${q}%`);
  }
  if (brand) { conditions.push("brand_slug LIKE ?"); params.push(`%${slugify(brand)}%`); }
  if (gender) { conditions.push("gender = ?"); params.push(gender); }
  if (minRating > 0) { conditions.push("rating >= ?"); params.push(minRating); }
  if (minPopularity > 0) { conditions.push("popularity >= ?"); params.push(minPopularity); }

  const orderCol = sortBy === "rating" ? "rating" : sortBy === "name" ? "name" : "popularity";
  const where = conditions.join(" AND ");
  const sql = `SELECT * FROM products WHERE ${where} ORDER BY ${orderCol} DESC LIMIT ? OFFSET ?`;
  params.push(limit, offset);
  const countSql = `SELECT COUNT(*) as total FROM products WHERE ${where}`;

  try {
    const [results, countResult] = await Promise.all([
      env.CATALOG_DB.prepare(sql).bind(...params).all(),
      env.CATALOG_DB.prepare(countSql).bind(...params.slice(0, -2)).first()
    ]);
    return json({
      total: countResult?.total || 0,
      limit, offset,
      results: results.results.map(r => parseProductRow(r))
    });
  } catch (err) {
    return json({ error: err.message }, 500);
  }
}

function parseProductRow(r) {
  return {
    ...r,
    main_accords:    r.main_accords    ? JSON.parse(r.main_accords)    : [],
    notes_top:       r.notes_top       ? JSON.parse(r.notes_top)       : [],
    notes_middle:    r.notes_middle    ? JSON.parse(r.notes_middle)    : [],
    notes_base:      r.notes_base      ? JSON.parse(r.notes_base)      : [],
    notes_all:       r.notes_all       ? JSON.parse(r.notes_all)       : [],
    accord_weights:  r.accord_weights  ? JSON.parse(r.accord_weights)  : {},
    occasion_scores: r.occasion_scores ? JSON.parse(r.occasion_scores) : [],
    season_scores:   r.season_scores   ? JSON.parse(r.season_scores)   : [],
    perfumers:       r.perfumers       ? JSON.parse(r.perfumers)       : [],
  };
}

// ─── Product ──────────────────────────────────────────────────────────────────

async function handleProduct(url, env) {
  const parts = url.pathname.split("/").filter(Boolean);
  const category = parts[2] || "fragrances";
  const id = parts[3];
  if (!id) return json({ error: "id required" }, 400);

  try {
    const row = await env.CATALOG_DB.prepare(
      "SELECT * FROM products WHERE id = ? AND category = ?"
    ).bind(id, category).first();
    if (!row) return json({ error: "Not found" }, 404);

    const fullRecord = await env.MASTER_DB.get(`catalog/${row.brand_slug}/${id}.json`)
      .then(async (o) => o ? JSON.parse(await o.text()) : null)
      .catch(() => null);

    return json({ ...parseProductRow(row), full: fullRecord });
  } catch (err) {
    return json({ error: err.message }, 500);
  }
}

// ─── Workflows ────────────────────────────────────────────────────────────────

async function startAllWorkflows(env) {
  let allBrands = [];
  try {
    const indexObj = await env.MASTER_DB.get("brands/index.json");
    if (!indexObj) return;
    allBrands = JSON.parse(await indexObj.text());
  } catch { return; }

  const fragellaKeys = new Set();
  try {
    let cursor;
    do {
      const listRes = await env.MASTER_DB.list({ prefix: "fragella-brands/", limit: 1000, cursor });
      for (const obj of listRes.objects) {
        fragellaKeys.add(obj.key.replace("fragella-brands/", "").replace(".json", ""));
      }
      cursor = listRes.truncated ? listRes.cursor : null;
    } while (cursor);
  } catch { return; }

  const brandsToStart = allBrands.filter(b => fragellaKeys.has(slugify(b.name)));
  const results = await Promise.allSettled(brandsToStart.map(async (brand) => {
    const slug = slugify(brand.name);
    const instanceId = `brand-${slug}`;
    try {
      const existing = await env.CATALOG_WORKFLOW.get(instanceId);
      const status = await existing.status();
      if (status.status === "errored") { await existing.restart(); return "restarted"; }
      if (["complete", "running", "queued"].includes(status.status)) return status.status;
    } catch {}
    try {
      await env.CATALOG_WORKFLOW.create({ id: instanceId, params: { brandName: brand.name, brandSlug: slug } });
      return "started";
    } catch { return "skipped"; }
  }));

  const counts = { started: 0, restarted: 0, running: 0, complete: 0, skipped: 0 };
  for (const r of results) if (r.status === "fulfilled") counts[r.value] = (counts[r.value] || 0) + 1;
  console.log(`Workflows: ${JSON.stringify(counts)} / ${brandsToStart.length} total`);
}

async function handleTest(request, env) {
  let body = {};
  try { body = await request.json(); } catch {}
  const testSlug = body.brandSlug || "dior";
  let brandName = testSlug;

  try {
    const indexObj = await env.MASTER_DB.get("brands/index.json");
    if (indexObj) {
      const brands = JSON.parse(await indexObj.text());
      const match = brands.find(b => slugify(b.name) === testSlug);
      if (match) brandName = match.name;
    }
  } catch {}

  const feObj = await env.MASTER_DB.get(`fragella-brands/${testSlug}.json`).catch(() => null);
  if (!feObj) return json({ error: `No fragella data for "${testSlug}"` }, 400);

  const testInstanceId = `test-${testSlug}-${Date.now()}`;
  let instance;
  try {
    instance = await env.CATALOG_WORKFLOW.create({ id: testInstanceId, params: { brandName, brandSlug: testSlug } });
  } catch (err) {
    return json({ error: `Failed to create test instance: ${err.message}` }, 500);
  }

  const start = Date.now();
  let finalStatus;
  while (Date.now() - start < 90000) {
    await new Promise(r => setTimeout(r, 2000));
    try {
      const s = await instance.status();
      if (s.status === "complete" || s.status === "errored") { finalStatus = s; break; }
    } catch {}
  }

  if (!finalStatus) return json({ status: "timeout", instanceId: testInstanceId });

  const d1Count = await env.CATALOG_DB.prepare(
    "SELECT COUNT(*) as cnt FROM products WHERE brand_slug = ? AND category = 'fragrances'"
  ).bind(testSlug).first().catch(() => null);

  return json({ test: testSlug, brandName, instanceId: testInstanceId, workflowStatus: finalStatus.status, workflowOutput: finalStatus.output, workflowError: finalStatus.error || null, d1IndexedRecords: d1Count?.cnt || 0 });
}

async function handleStatus(url, env) {
  const id = url.searchParams.get("id");
  if (!id) return json({ error: "id query parameter required" }, 400);
  try {
    const instance = await env.CATALOG_WORKFLOW.get(id);
    const status = await instance.status();
    return json({ id, ...status });
  } catch (err) {
    return json({ error: err.message }, 404);
  }
}

// ─── Catalog Workflow ─────────────────────────────────────────────────────────

export class CatalogWorkflow extends WorkflowEntrypoint {
  async run(event, step) {
    const { brandName, brandSlug } = event.payload;

    const mergeResult = await step.do(`merge-${brandSlug}`, async () => {
      const feObj = await this.env.MASTER_DB.get(`fragella-brands/${brandSlug}.json`);
      if (!feObj) return { skipped: true, reason: "no fragella data" };
      const fragellaData = JSON.parse(await feObj.text());
      if (!fragellaData.scents?.length) return { skipped: true, reason: "empty scents" };

      const fragellaIndex = {};
      for (const h of fragellaData.scents) {
        const normName = normalize(h.Name || h.name || "");
        if (normName) fragellaIndex[normName] = h;
      }

      let catalogKeys = [];
      let cursor;
      do {
        const listRes = await this.env.MASTER_DB.list({ prefix: `catalog/${brandSlug}/`, limit: 1000, cursor });
        catalogKeys = catalogKeys.concat(listRes.objects.map(o => o.key));
        cursor = listRes.truncated ? listRes.cursor : null;
      } while (cursor);

      if (catalogKeys.length === 0) return { skipped: true, reason: "no catalog records" };

      let enriched = 0, noMatch = 0;
      await Promise.all(catalogKeys.map(async (key) => {
        try {
          const obj = await this.env.MASTER_DB.get(key);
          if (!obj) return;
          const record = JSON.parse(await obj.text());
          if (record.fragella != null) return;

          const normName = normalize(record.name || "");
          const fe = fragellaIndex[normName]
            || Object.entries(fragellaIndex).find(([k]) => k.includes(normName) && normName.length > 4)?.[1]
            || Object.entries(fragellaIndex).find(([k]) => normName.includes(k) && k.length > 4)?.[1]
            || null;

          if (!fe) { noMatch++; return; }

          record.fragella = {
            matchType: "workflow-merge",
            year: fe.Year || null, country: fe.Country || null, gender: fe.Gender || null,
            oilType: fe.OilType || null, longevity: fe.Longevity || null, sillage: fe.Sillage || null,
            popularity: fe.Popularity || null, rating: fe.rating || null,
            priceValue: fe["Price Value"] || null, price: fe.Price || null,
            imageUrl: fe["Image URL"] || null, imageFallbacks: fe["Image Fallbacks"] || [],
            purchaseUrl: fe["Purchase URL"] || null,
            mainAccords: fe["Main Accords"] || [],
            mainAccordsPercentage: fe["Main Accords Percentage"] || {},
            generalNotes: fe["General Notes"] || [],
            notes: fe.Notes || {},
            seasonRanking: fe["Season Ranking"] || [],
            occasionRanking: fe["Occasion Ranking"] || []
          };
          record.syncedAt = new Date().toISOString();

          await Promise.all([
            this.env.MASTER_DB.put(key, JSON.stringify(record), { httpMetadata: { contentType: "application/json" } }),
            this.env.MASTER_DB.put(`sources/fragella/${record.id}.json`, JSON.stringify(fe), { httpMetadata: { contentType: "application/json" } })
          ]);
          enriched++;
        } catch {}
      }));

      return { catalogRecords: catalogKeys.length, enriched, noMatch };
    });

    const imageResult = await step.do(`images-${brandSlug}`, async () => {
      let catalogKeys = [];
      let cursor;
      do {
        const listRes = await this.env.MASTER_DB.list({ prefix: `catalog/${brandSlug}/`, limit: 1000, cursor });
        catalogKeys = catalogKeys.concat(listRes.objects.map(o => o.key));
        cursor = listRes.truncated ? listRes.cursor : null;
      } while (cursor);

      let mirrored = 0, skipped = 0, failed = 0;
      for (let i = 0; i < catalogKeys.length; i += 20) {
        await Promise.all(catalogKeys.slice(i, i + 20).map(async (key) => {
          try {
            const obj = await this.env.MASTER_DB.get(key);
            if (!obj) return;
            const record = JSON.parse(await obj.text());
            const id = record.id;
            if (!id) return;
            const tasks = [];
            const fpUrl = record.fragplace?.imageUrl;
            if (fpUrl) {
              const fpKey = `fragplace/${id}.webp`;
              const exists = await this.env.IMAGES_DB.get(fpKey).catch(() => null);
              if (!exists) {
                tasks.push(fetch(fpUrl).then(async r => {
                  if (r.ok) { await this.env.IMAGES_DB.put(fpKey, await r.arrayBuffer(), { httpMetadata: { contentType: r.headers.get("content-type") || "image/jpeg" } }); mirrored++; }
                  else failed++;
                }).catch(() => { failed++; }));
              } else skipped++;
            }
            const feUrl = record.fragella?.imageUrl;
            if (feUrl && feUrl !== fpUrl) {
              const feKey = `fragella/${id}.webp`;
              const exists = await this.env.IMAGES_DB.get(feKey).catch(() => null);
              if (!exists) {
                tasks.push(fetch(feUrl).then(async r => {
                  if (r.ok) { await this.env.IMAGES_DB.put(feKey, await r.arrayBuffer(), { httpMetadata: { contentType: r.headers.get("content-type") || "image/jpeg" } }); mirrored++; }
                  else failed++;
                }).catch(() => { failed++; }));
              } else skipped++;
            }
            await Promise.all(tasks);
          } catch { failed++; }
        }));
      }
      return { catalogRecords: catalogKeys.length, mirrored, skipped, failed };
    });

    const indexResult = await step.do(`index-${brandSlug}`, async () => {
      let catalogKeys = [];
      let cursor;
      do {
        const listRes = await this.env.MASTER_DB.list({ prefix: `catalog/${brandSlug}/`, limit: 1000, cursor });
        catalogKeys = catalogKeys.concat(listRes.objects.map(o => o.key));
        cursor = listRes.truncated ? listRes.cursor : null;
      } while (cursor);

      let indexed = 0, errors = 0;
      for (let i = 0; i < catalogKeys.length; i += 50) {
        const batch = catalogKeys.slice(i, i + 50);
        const records = await Promise.all(batch.map(async (key) => {
          try { const obj = await this.env.MASTER_DB.get(key); if (!obj) return null; return JSON.parse(await obj.text()); }
          catch { return null; }
        }));
        const stmt = makeStmt(this.env.CATALOG_DB);
        const d1batch = records.filter(r => r && r.id).map(r => bindRecord(stmt, buildD1Record(r)));
        if (d1batch.length > 0) {
          try { await this.env.CATALOG_DB.batch(d1batch); indexed += d1batch.length; }
          catch { errors += d1batch.length; }
        }
      }
      return { catalogRecords: catalogKeys.length, indexed, errors };
    });

    return { brandName, brandSlug, merge: mergeResult, images: imageResult, index: indexResult, completedAt: new Date().toISOString() };
  }
}

// ─── Cron reindex ─────────────────────────────────────────────────────────────

async function cronReindex(env) {
  const DEADLINE = Date.now() + 12 * 60 * 1000;
  let allBrands = [];
  try {
    const indexObj = await env.MASTER_DB.get("brands/index.json");
    if (!indexObj) return;
    allBrands = JSON.parse(await indexObj.text());
  } catch { return; }

  let state = { currentOffset: 0, totalIndexed: 0, totalBrands: allBrands.length, status: "running" };
  try {
    const stateObj = await env.MASTER_DB.get("state/reindex.json");
    if (stateObj) {
      const saved = JSON.parse(await stateObj.text());
      if (saved.status === "done") { console.log("Reindex already complete"); return; }
      state = saved;
    }
  } catch {}

  let brandsProcessed = 0;
  while (state.currentOffset < allBrands.length && Date.now() < DEADLINE) {
    const brand = allBrands[state.currentOffset];
    const brandSlug = slugify(brand.name);
    let catalogKeys = [];
    try {
      let cursor;
      do {
        const listRes = await env.MASTER_DB.list({ prefix: `catalog/${brandSlug}/`, limit: 1000, cursor });
        catalogKeys = catalogKeys.concat(listRes.objects.map(o => o.key));
        cursor = listRes.truncated ? listRes.cursor : null;
      } while (cursor);
    } catch {}

    let indexed = 0;
    for (let i = 0; i < catalogKeys.length; i += 50) {
      const batch = catalogKeys.slice(i, i + 50);
      const records = await Promise.all(batch.map(async (key) => {
        try { const obj = await env.MASTER_DB.get(key); return obj ? JSON.parse(await obj.text()) : null; }
        catch { return null; }
      }));
      const stmt = makeStmt(env.CATALOG_DB);
      const d1batch = records.filter(r => r && r.id).map(r => bindRecord(stmt, buildD1Record(r)));
      if (d1batch.length > 0) {
        try { await env.CATALOG_DB.batch(d1batch); indexed += d1batch.length; }
        catch {}
      }
    }

    state.currentOffset++;
    state.totalIndexed += indexed;
    brandsProcessed++;
    if (brandsProcessed % 10 === 0) {
      try { await env.MASTER_DB.put("state/reindex.json", JSON.stringify(state), { httpMetadata: { contentType: "application/json" } }); } catch {}
    }
  }

  const done = state.currentOffset >= allBrands.length;
  state.status = done ? "done" : "running";
  if (done) state.finishedAt = new Date().toISOString();
  try { await env.MASTER_DB.put("state/reindex.json", JSON.stringify(state), { httpMetadata: { contentType: "application/json" } }); } catch {}
  console.log(`Reindex cron: ${brandsProcessed} brands, ${state.totalIndexed} total, offset ${state.currentOffset}/${allBrands.length}, done: ${done}`);
}

// ─── Manual reindex ───────────────────────────────────────────────────────────

async function handleReindex(request, env, ctx) {
  if (!env.MASTER_DB) return json({ error: "MASTER_DB not configured" }, 500);
  if (!env.CATALOG_DB) return json({ error: "CATALOG_DB not configured" }, 500);

  let body = {};
  try { body = await request.json(); } catch {}
  const reset = body.reset === true;

  let allBrands = [];
  try {
    const indexObj = await env.MASTER_DB.get("brands/index.json");
    if (!indexObj) return json({ error: "brands/index.json not found" }, 500);
    allBrands = JSON.parse(await indexObj.text());
  } catch (err) {
    return json({ error: `Brand index load failed: ${err.message}` }, 500);
  }

  let state = { currentOffset: 0, totalIndexed: 0, totalBrands: allBrands.length, status: "running" };
  if (!reset) {
    try {
      const stateObj = await env.MASTER_DB.get("state/reindex.json");
      if (stateObj) {
        const saved = JSON.parse(await stateObj.text());
        if (saved.status !== "done") state = saved;
      }
    } catch {}
  }

  const offset = state.currentOffset;
  if (offset >= allBrands.length) {
    return json({ done: true, totalBrands: allBrands.length, totalIndexed: state.totalIndexed });
  }

  const brand = allBrands[offset];
  const brandSlug = slugify(brand.name);
  let catalogKeys = [];
  try {
    let cursor;
    do {
      const listRes = await env.MASTER_DB.list({ prefix: `catalog/${brandSlug}/`, limit: 1000, cursor });
      catalogKeys = catalogKeys.concat(listRes.objects.map(o => o.key));
      cursor = listRes.truncated ? listRes.cursor : null;
    } while (cursor);
  } catch {}

  let indexed = 0;
  for (let i = 0; i < catalogKeys.length; i += 50) {
    const batch = catalogKeys.slice(i, i + 50);
    const records = await Promise.all(batch.map(async (key) => {
      try { const obj = await env.MASTER_DB.get(key); return obj ? JSON.parse(await obj.text()) : null; }
      catch { return null; }
    }));
    const stmt = makeStmt(env.CATALOG_DB);
    const d1batch = records.filter(r => r && r.id).map(r => bindRecord(stmt, buildD1Record(r)));
    if (d1batch.length > 0) {
      try { await env.CATALOG_DB.batch(d1batch); indexed += d1batch.length; }
      catch {}
    }
  }

  state.currentOffset = offset + 1;
  state.totalIndexed += indexed;
  const done = state.currentOffset >= allBrands.length;
  state.status = done ? "done" : "running";
  if (done) state.finishedAt = new Date().toISOString();
  await env.MASTER_DB.put("state/reindex.json", JSON.stringify(state), { httpMetadata: { contentType: "application/json" } });

  return json({ done, brandName: brand.name, offset, nextOffset: done ? null : state.currentOffset, totalBrands: allBrands.length, totalIndexed: state.totalIndexed, thisBrand: { catalogRecords: catalogKeys.length, indexed }, finishedAt: done ? state.finishedAt : null });
}

// ─── Gemini ───────────────────────────────────────────────────────────────────

async function handleGeminiTest(request, env) {
  if (!env.GEMINI_API_KEY) return json({ error: "GEMINI_API_KEY not configured" }, 500);
  if (!env.CATALOG_DB) return json({ error: "CATALOG_DB not configured" }, 500);

  let body = {};
  try { body = await request.json(); } catch {}
  const brandSlug = body.brandSlug || "dior";
  const scentId = body.scentId || null;

  let scent;
  try {
    scent = scentId
      ? await env.CATALOG_DB.prepare("SELECT * FROM products WHERE id = ?").bind(scentId).first()
      : await env.CATALOG_DB.prepare("SELECT * FROM products WHERE brand_slug = ? AND has_fragella = 1 ORDER BY popularity DESC LIMIT 1").bind(brandSlug).first();
  } catch (err) { return json({ error: `D1 query failed: ${err.message}` }, 500); }
  if (!scent) return json({ error: `No scent found for brand "${brandSlug}"` }, 404);

  const accords = scent.main_accords ? JSON.parse(scent.main_accords) : [];
  const prompt = `You are a luxury fragrance copywriter. Generate multilingual product descriptions for this fragrance.

FRAGRANCE DATA:
Brand: ${scent.brand}
Name: ${scent.name}
Gender: ${scent.gender || "unisex"}
Year: ${scent.year || "unknown"}
Country: ${scent.country || "unknown"}
Longevity: ${scent.longevity || "unknown"}
Sillage: ${scent.sillage || "unknown"}
Main Accords: ${accords.join(", ") || "unknown"}
Rating: ${scent.rating || "unknown"}/5

Generate descriptions in ALL of the following languages. Each description should be 2-3 sentences, evocative, and feel native to that language — not translated. Also generate one short signature sentence per language (max 15 words) that captures the essence of the scent poetically.

Languages: English (en), Romanian (ro), French (fr), German (de), Spanish (es), Italian (it), Portuguese (pt), Arabic (ar), Russian (ru), Chinese Simplified (zh), Japanese (ja), Korean (ko), Hindi (hi), Bengali (bn), Turkish (tr), Dutch (nl), Polish (pl), Swedish (sv), Norwegian (no), Danish (da), Finnish (fi), Greek (el), Hebrew (he), Persian (fa), Indonesian (id), Malay (ms), Thai (th), Vietnamese (vi), Ukrainian (uk), Czech (cs), Slovak (sk), Hungarian (hu), Bulgarian (bg), Croatian (hr), Serbian (sr), Swahili (sw), Hausa (ha), Amharic (am), Tamil (ta), Telugu (te)

Also generate 3 universal occasion tags from: [date, office, casual, special, evening, sport, beach, winter, summer, spring, autumn, romantic, wedding, travel, formal]

Return ONLY valid JSON in this exact structure, no markdown, no preamble:
{
  "scent": "${scent.name}",
  "brand": "${scent.brand}",
  "descriptions": {
    "en": { "desc": "...", "signature": "..." }
  },
  "occasions": ["tag1", "tag2", "tag3"]
}`;

  const geminiUrl = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=" + env.GEMINI_API_KEY;
  try {
    const res = await fetch(geminiUrl, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ contents: [{ parts: [{ text: prompt }] }], generationConfig: { temperature: 0.7, maxOutputTokens: 8192, responseMimeType: "application/json" } }) });
    if (!res.ok) { const err = await res.text(); return json({ error: `Gemini API error ${res.status}`, detail: err.slice(0, 500) }, 500); }
    const data = await res.json();
    const text = data?.candidates?.[0]?.content?.parts?.[0]?.text || "";
    try {
      const geminiResponse = JSON.parse(text);
      return json({ scentId: scent.id, scentName: scent.name, brand: scent.brand, accords, longevity: scent.longevity, sillage: scent.sillage, gender: scent.gender, languagesGenerated: Object.keys(geminiResponse.descriptions || {}).length, result: geminiResponse });
    } catch { return json({ error: "Gemini returned invalid JSON", raw: text.slice(0, 1000) }, 500); }
  } catch (err) { return json({ error: `Gemini fetch failed: ${err.message}` }, 500); }
}

// ─── Export ───────────────────────────────────────────────────────────────────

async function handleExport(url, env) {
  const platform = url.pathname.split("/")[3];
  if (!["emag", "trendyol", "shopify", "csv"].includes(platform)) {
    return json({ error: `Unknown platform. Use: emag, trendyol, shopify, csv` }, 400);
  }
  const brandsParam = url.searchParams.get("brands") || "";
  const brandSlugs = brandsParam ? brandsParam.split(",").map(b => slugify(b.trim())).filter(Boolean) : [];
  const limit = Math.min(parseInt(url.searchParams.get("limit") || "500"), 2000);
  const offset = parseInt(url.searchParams.get("offset") || "0");
  const gender = url.searchParams.get("gender") || "";

  const conditions = ["category = 'fragrances'", "has_image = 1"];
  const params = [];
  if (brandSlugs.length > 0) { conditions.push(`brand_slug IN (${brandSlugs.map(() => "?").join(",")})`); params.push(...brandSlugs); }
  if (gender) { conditions.push("gender = ?"); params.push(gender); }

  const sql = `SELECT * FROM products WHERE ${conditions.join(" AND ")} ORDER BY popularity DESC, rating DESC LIMIT ? OFFSET ?`;
  params.push(limit, offset);

  let products;
  try {
    const result = await env.CATALOG_DB.prepare(sql).bind(...params).all();
    products = result.results.map(r => parseProductRow(r));
  } catch (err) { return json({ error: `D1 query failed: ${err.message}` }, 500); }

  if (platform === "emag") return formatEMag(products, url);
  if (platform === "trendyol") return formatTrendyol(products, url);
  if (platform === "shopify") return formatShopify(products, url);
  return formatCSV(products, url);
}

// ─── Shopify sync ─────────────────────────────────────────────────────────────

async function handleShopifySync(request, env, ctx) {
  if (!env.SHOPIFY_STORE) return json({ error: "SHOPIFY_STORE secret not set" }, 500);
  if (!env.SHOPIFY_TOKEN) return json({ error: "SHOPIFY_TOKEN secret not set" }, 500);
  if (!env.CATALOG_DB) return json({ error: "CATALOG_DB not configured" }, 500);

  let body = {};
  try { body = await request.json(); } catch {}

  const brandSlug = body.brandSlug || "";
  const limit     = Math.min(body.limit || 20, 50);
  const offset    = body.offset || 0;
  const status    = body.status || "draft";
  const vendor    = body.vendor || "";
  const markup    = body.markup || 1.4;

  const conditions = ["category = 'fragrances'", "has_image = 1"];
  const params = [];
  if (brandSlug) { conditions.push("brand_slug = ?"); params.push(brandSlug); }

  const sql = `SELECT * FROM products WHERE ${conditions.join(" AND ")} ORDER BY popularity DESC, rating DESC LIMIT ? OFFSET ?`;
  params.push(limit, offset);

  let products;
  try {
    const result = await env.CATALOG_DB.prepare(sql).bind(...params).all();
    products = result.results.map(r => parseProductRow(r));
  } catch (err) { return json({ error: `D1 query failed: ${err.message}` }, 500); }

  if (products.length === 0) return json({ done: true, message: "No products found", offset, limit });

  const shopifyBase = `https://${env.SHOPIFY_STORE}/admin/api/2024-01`;
  const shopifyHeaders = { "Content-Type": "application/json", "X-Shopify-Access-Token": env.SHOPIFY_TOKEN };
  const results = { created: 0, skipped: 0, failed: 0, errors: [] };

  for (const p of products) {
    let existingId = null;
    try {
      const checkRes = await fetch(`${shopifyBase}/products.json?handle=${p.slug}&fields=id,handle&limit=1`, { headers: shopifyHeaders });
      if (checkRes.ok) { const cd = await checkRes.json(); if (cd.products?.length > 0) existingId = cd.products[0].id; }
    } catch {}

    if (existingId) { results.skipped++; continue; }

    const base = p.popularity === "Very high" ? 89.99 : p.popularity === "High" ? 69.99 : p.popularity === "Moderate" ? 49.99 : 34.99;
    const price = (base * markup).toFixed(2);
    const comparePrice = (base * markup * 1.2).toFixed(2);
    const tags = [p.gender, p.country, ...p.main_accords.slice(0, 5), p.longevity, p.sillage, p.oil_type, "fragrance", "perfume", p.brand].filter(Boolean).join(", ");
    const bodyHtml = [`<p>${p.name} by ${p.brand}.</p>`, p.main_accords.length ? `<p>Main accords: ${p.main_accords.slice(0, 5).join(", ")}.</p>` : "", p.notes_top?.length ? `<p>Top notes: ${p.notes_top.join(", ")}.</p>` : "", p.notes_middle?.length ? `<p>Heart notes: ${p.notes_middle.join(", ")}.</p>` : "", p.notes_base?.length ? `<p>Base notes: ${p.notes_base.join(", ")}.</p>` : "", p.longevity ? `<p>Longevity: ${p.longevity}.</p>` : "", p.sillage ? `<p>Sillage: ${p.sillage}.</p>` : "", p.year ? `<p>Year: ${p.year}.</p>` : ""].filter(Boolean).join("\n");

    const product = {
      title: `${p.brand} ${p.name}`, body_html: bodyHtml, vendor: vendor || p.brand,
      product_type: p.oil_type || "Parfum", handle: p.slug, status, tags,
      variants: [{ price, compare_at_price: comparePrice, sku: p.slug, barcode: p.ean || "", inventory_management: "shopify", inventory_quantity: 10, requires_shipping: true, taxable: true, weight: 500, weight_unit: "g", option1: "100ml" }],
      options: [{ name: "Volume", values: ["100ml"] }],
      images: p.image_url ? [{ src: p.image_url, alt: `${p.brand} ${p.name}` }] : [],
      metafields: [
        { namespace: "catalog", key: "fragrance_id", value: String(p.id), type: "single_line_text_field" },
        { namespace: "catalog", key: "accords", value: p.main_accords.join("|"), type: "single_line_text_field" },
        p.perfumers?.length ? { namespace: "catalog", key: "perfumers", value: p.perfumers.join("|"), type: "single_line_text_field" } : null,
      ].filter(Boolean),
    };

    try {
      const createRes = await fetch(`${shopifyBase}/products.json`, { method: "POST", headers: shopifyHeaders, body: JSON.stringify({ product }) });
      if (createRes.status === 429) {
        await new Promise(r => setTimeout(r, 2000));
        const retryRes = await fetch(`${shopifyBase}/products.json`, { method: "POST", headers: shopifyHeaders, body: JSON.stringify({ product }) });
        if (retryRes.ok) results.created++; else { results.failed++; results.errors.push({ id: p.id, slug: p.slug, status: retryRes.status }); }
      } else if (createRes.ok) {
        results.created++;
      } else {
        results.failed++;
        const errText = await createRes.text().catch(() => "");
        results.errors.push({ id: p.id, slug: p.slug, status: createRes.status, detail: errText.slice(0, 200) });
      }
    } catch (err) { results.failed++; results.errors.push({ id: p.id, slug: p.slug, error: err.message }); }
    await new Promise(r => setTimeout(r, 700));
  }

  const nextOffset = offset + products.length;
  const done = products.length < limit;
  return json({ done, offset, limit, processed: products.length, nextOffset: done ? null : nextOffset, results, callNext: done ? null : `POST /api/shopify/sync with { offset: ${nextOffset}${brandSlug ? `, brandSlug: "${brandSlug}"` : ""} }` });
}

// ─── Export formatters ────────────────────────────────────────────────────────

function csvField(v) {
  if (v === null || v === undefined) return "";
  const s = String(v);
  if (s.includes(",") || s.includes('"') || s.includes("\n")) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function estimatePrice(p, markup = 1.4) {
  const base = p.popularity === "Very high" ? 89.99 : p.popularity === "High" ? 69.99 : p.popularity === "Moderate" ? 49.99 : 34.99;
  return (base * markup).toFixed(2);
}

function formatEMag(products, url) {
  const markup = parseFloat(url.searchParams.get("markup") || "1.4");
  const stock = url.searchParams.get("stock") || "10";
  const market = url.searchParams.get("market") || "ro";
  const marketConfig = {
    ro: { currency: "RON", vatRate: 21, apiBase: "marketplace.emag.ro", gender: { men: "Parfum masculin", women: "Parfum feminin", unisex: "Parfum unisex" } },
    bg: { currency: "BGN", vatRate: 20, apiBase: "marketplace.emag.bg", gender: { men: "Мъжки парфюм", women: "Дамски парфюм", unisex: "Унисекс парфюм" } },
    hu: { currency: "HUF", vatRate: 27, apiBase: "marketplace.emag.hu", gender: { men: "Férfi parfüm", women: "Női parfüm", unisex: "Uniszex parfüm" } }
  };
  const cfg = marketConfig[market] || marketConfig.ro;
  const headers = ["seller_id","ean","name","brand","part_number","description","image_url","gender","oil_type","longevity","sillage","main_accords","notes_all","year","country","sale_price","min_sale_price","max_sale_price","currency","vat_rate","stock","market","api_base","has_fragplace","has_fragella"];
  const rows = products.map(p => {
    const price = estimatePrice(p, markup);
    const genderKey = p.gender === "men" ? "men" : p.gender === "women" ? "women" : "unisex";
    const desc = `${p.name} ${cfg.gender[genderKey]}. ${p.main_accords.slice(0, 3).join(", ")}.${p.longevity ? ` Longevity: ${p.longevity}.` : ""}`;
    return [p.id, p.ean || "", `${p.brand} ${p.name}`, p.brand, p.slug, desc, p.image_url || "", p.gender || "", p.oil_type || "", p.longevity || "", p.sillage || "", p.main_accords.join("|"), (p.notes_all || []).join("|"), p.year || "", p.country || "", price, (parseFloat(price)*0.85).toFixed(2), (parseFloat(price)*1.15).toFixed(2), cfg.currency, cfg.vatRate, stock, market, cfg.apiBase, p.has_fragplace, p.has_fragella].map(csvField).join(",");
  });
  return new Response([headers.join(","), ...rows].join("\n"), { headers: { "Content-Type": "text/csv; charset=utf-8", "Content-Disposition": `attachment; filename="emag-${market}-export-${Date.now()}.csv"`, "Access-Control-Allow-Origin": "*" } });
}

function formatTrendyol(products, url) {
  const markup = parseFloat(url.searchParams.get("markup") || "1.4");
  const stock = parseInt(url.searchParams.get("stock") || "10");
  const market = url.searchParams.get("market") || "ro";
  const marketConfig = {
    ro: { currency: "RON", vatRate: 21, categories: { men: "Parfum Bărbați", women: "Parfum Femei", unisex: "Parfum Unisex" }, attrs: { gender: "Gen", year: "An", country: "Țara de origine", longevity: "Longevitate", sillage: "Sillage" } },
    tr: { currency: "TRY", vatRate: 20, categories: { men: "Erkek Parfüm", women: "Kadın Parfüm", unisex: "Unisex Parfüm" }, attrs: { gender: "Cinsiyet", year: "Yıl", country: "Menşei", longevity: "Kalıcılık", sillage: "Sillage" } },
    de: { currency: "EUR", vatRate: 19, categories: { men: "Herrenparfüm", women: "Damenparfüm", unisex: "Unisex Parfüm" }, attrs: { gender: "Geschlecht", year: "Jahr", country: "Herkunftsland", longevity: "Haltbarkeit", sillage: "Sillage" } },
    nl: { currency: "EUR", vatRate: 21, categories: { men: "Herengeur", women: "Damegeur", unisex: "Unisex Geur" }, attrs: { gender: "Geslacht", year: "Jaar", country: "Land van herkomst", longevity: "Houdbaarheid", sillage: "Sillage" } }
  };
  const cfg = marketConfig[market] || marketConfig.ro;
  const items = products.map(p => {
    const price = parseFloat(estimatePrice(p, markup));
    const genderKey = p.gender === "men" ? "men" : p.gender === "women" ? "women" : "unisex";
    const desc = `${p.name} by ${p.brand}. Main accords: ${p.main_accords.slice(0, 5).join(", ")}.${p.longevity ? ` Longevity: ${p.longevity}.` : ""}${p.sillage ? ` Sillage: ${p.sillage}.` : ""}`;
    return {
      barcode: p.ean || String(p.id), title: `${p.brand} ${p.name}`, productMainId: String(p.id),
      brandName: p.brand, categoryName: cfg.categories[genderKey], quantity: stock,
      stockCode: p.slug, dimensionalWeight: 0.5, description: desc,
      currencyType: cfg.currency, listPrice: parseFloat((price * 1.1).toFixed(2)), salePrice: price, vatRate: cfg.vatRate,
      images: p.image_url ? [{ url: p.image_url }] : [],
      attributes: [
        p.gender ? { attributeName: cfg.attrs.gender, attributeValue: p.gender } : null,
        p.year ? { attributeName: cfg.attrs.year, attributeValue: String(p.year) } : null,
        p.country ? { attributeName: cfg.attrs.country, attributeValue: p.country } : null,
        p.longevity ? { attributeName: cfg.attrs.longevity, attributeValue: p.longevity } : null,
        p.sillage ? { attributeName: cfg.attrs.sillage, attributeValue: p.sillage } : null,
        p.oil_type ? { attributeName: "Tip", attributeValue: p.oil_type } : null,
      ].filter(Boolean),
      _meta: { market, has_ean: !!p.ean, has_fragella: !!p.has_fragella, rating: p.rating, perfumers: p.perfumers }
    };
  });
  return new Response(JSON.stringify({ market, items, total: items.length, exportedAt: new Date().toISOString() }, null, 2), { headers: { "Content-Type": "application/json", "Content-Disposition": `attachment; filename="trendyol-${market}-export-${Date.now()}.json"`, "Access-Control-Allow-Origin": "*" } });
}

function formatShopify(products, url) {
  const markup = parseFloat(url.searchParams.get("markup") || "1.4");
  const vendor = url.searchParams.get("vendor") || "";
  const published = url.searchParams.get("published") || "TRUE";
  const headers = ["Handle","Title","Body (HTML)","Vendor","Product Category","Type","Tags","Published","Option1 Name","Option1 Value","Variant SKU","Variant Grams","Variant Inventory Tracker","Variant Inventory Qty","Variant Inventory Policy","Variant Fulfillment Service","Variant Price","Variant Compare At Price","Variant Requires Shipping","Variant Taxable","Variant Barcode","Image Src","Image Position","Image Alt Text","Gift Card","SEO Title","SEO Description","Google Shopping / Gender","Status"];
  const rows = products.map(p => {
    const price = estimatePrice(p, markup);
    const comparePrice = (parseFloat(price) * 1.2).toFixed(2);
    const tags = [p.gender, p.country, ...p.main_accords.slice(0, 5), p.longevity, p.sillage, p.oil_type, "fragrance", "perfume", p.brand].filter(Boolean).join(", ");
    const description = [`<p>${p.name} by ${p.brand}.</p>`, `<p>Main accords: ${p.main_accords.slice(0, 5).join(", ")}.</p>`, p.notes_top?.length ? `<p>Top notes: ${p.notes_top.join(", ")}.</p>` : "", p.notes_middle?.length ? `<p>Heart notes: ${p.notes_middle.join(", ")}.</p>` : "", p.notes_base?.length ? `<p>Base notes: ${p.notes_base.join(", ")}.</p>` : "", p.longevity ? `<p>Longevity: ${p.longevity}.</p>` : "", p.sillage ? `<p>Sillage: ${p.sillage}.</p>` : "", p.year ? `<p>Year: ${p.year}.</p>` : ""].filter(Boolean).join("\n");
    return [p.slug, `${p.brand} ${p.name}`, description, vendor || p.brand, "Health & Beauty > Personal Care > Cosmetics > Perfume & Cologne", p.oil_type || "Parfum", tags, published, "Volume", "100ml", p.slug, "500", "shopify", "10", "deny", "manual", price, comparePrice, "TRUE", "TRUE", p.ean || "", p.image_url || "", "1", `${p.brand} ${p.name}`, "FALSE", `${p.brand} ${p.name} - Perfume`, `${p.name} by ${p.brand}. ${p.main_accords.slice(0, 3).join(", ")}.`, p.gender === "men" ? "Male" : p.gender === "women" ? "Female" : "Unisex", "active"].map(csvField).join(",");
  });
  return new Response([headers.join(","), ...rows].join("\n"), { headers: { "Content-Type": "text/csv; charset=utf-8", "Content-Disposition": `attachment; filename="shopify-export-${Date.now()}.csv"`, "Access-Control-Allow-Origin": "*" } });
}

function formatCSV(products, url) {
  const headers = ["id","brand","brand_slug","name","slug","gender","year","country","longevity","sillage","popularity","rating","oil_type","main_accords","notes_top","notes_middle","notes_base","notes_all","accord_weights","occasion_scores","season_scores","perfumers","fp_rating","fp_reviews_count","image_url","ean","has_fragplace","has_fragella","has_image","synced_at"];
  const rows = products.map(p => [p.id, p.brand, p.brand_slug, p.name, p.slug, p.gender || "", p.year || "", p.country || "", p.longevity || "", p.sillage || "", p.popularity || "", p.rating || "", p.oil_type || "", (p.main_accords||[]).join("|"), (p.notes_top||[]).join("|"), (p.notes_middle||[]).join("|"), (p.notes_base||[]).join("|"), (p.notes_all||[]).join("|"), JSON.stringify(p.accord_weights||{}), JSON.stringify(p.occasion_scores||[]), JSON.stringify(p.season_scores||[]), (p.perfumers||[]).join("|"), p.fp_rating || "", p.fp_reviews_count || "", p.image_url || "", p.ean || "", p.has_fragplace, p.has_fragella, p.has_image, p.synced_at].map(csvField).join(","));
  return new Response([headers.join(","), ...rows].join("\n"), { headers: { "Content-Type": "text/csv; charset=utf-8", "Content-Disposition": `attachment; filename="catalog-export-${Date.now()}.csv"`, "Access-Control-Allow-Origin": "*" } });
}

// ─── Utils ────────────────────────────────────────────────────────────────────

function normalize(str) {
  return (str || "").toLowerCase().replace(/[`\u2018\u2019''']/g, "").replace(/&/g, "and").replace(/[^\w\s]/g, " ").replace(/\s+/g, " ").trim();
}

function slugify(s) {
  return (s || "").toLowerCase().replace(/'/g, "").replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");
}

function json(obj, status = 200) {
  return new Response(JSON.stringify(obj, null, 2), { status, headers: corsHeaders });
}
