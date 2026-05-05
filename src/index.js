// src/index.js
// Fragrances Catalog Worker + Workflow + Search

import { WorkflowEntrypoint } from 'cloudflare:workers';

const FRAGELLA_BASE = "https://api.fragella.com/api/v1";

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

    if (url.pathname === "/" || url.pathname === "/health") {
      return json({ worker: "fragrances-catalog-worker", status: "ok", endpoints: [
        "POST /api/workflow/start            — trigger all brands",
        "POST /api/workflow/test             — test one brand {brandSlug?}",
        "GET  /api/workflow/status?id=<id>   — instance status",
        "GET  /api/search                    — search products (see params below)",
        "GET  /api/product/{category}/{id}   — single product",
      ]});
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

    if (url.pathname === "/api/search" && request.method === "GET") {
      return handleSearch(url, env);
    }

    if (url.pathname.startsWith("/api/product/") && request.method === "GET") {
      return handleProduct(url, env);
    }

    return json({ error: "Not found" }, 404);
  },

  async scheduled(event, env, ctx) {
    console.log("Cron fired:", event.cron);
    ctx.waitUntil(startAllWorkflows(env));
  },
};

// ---------------------------------------------------------------------------
// Search endpoint
// GET /api/search?q=oud&brand=lattafa&gender=male&category=fragrances
//                &minRating=7&minPopularity=5&limit=20&offset=0
// ---------------------------------------------------------------------------
async function handleSearch(url, env) {
  const q = url.searchParams.get("q") || "";
  const brand = url.searchParams.get("brand") || "";
  const gender = url.searchParams.get("gender") || "";
  const category = url.searchParams.get("category") || "fragrances";
  const minRating = parseFloat(url.searchParams.get("minRating") || "0");
  const minPopularity = parseFloat(url.searchParams.get("minPopularity") || "0");
  const limit = Math.min(parseInt(url.searchParams.get("limit") || "20"), 100);
  const offset = parseInt(url.searchParams.get("offset") || "0");
  const sortBy = url.searchParams.get("sortBy") || "popularity"; // popularity | rating | name

  let conditions = ["category = ?"];
  let params = [category];

  if (q) {
    conditions.push("(name LIKE ? OR brand LIKE ? OR main_accords LIKE ?)");
    params.push(`%${q}%`, `%${q}%`, `%${q}%`);
  }
  if (brand) {
    conditions.push("brand_slug LIKE ?");
    params.push(`%${slugify(brand)}%`);
  }
  if (gender) {
    conditions.push("gender = ?");
    params.push(gender);
  }
  if (minRating > 0) {
    conditions.push("rating >= ?");
    params.push(minRating);
  }
  if (minPopularity > 0) {
    conditions.push("popularity >= ?");
    params.push(minPopularity);
  }

  const orderCol = sortBy === "rating" ? "rating" : sortBy === "name" ? "name" : "popularity";
  const where = conditions.join(" AND ");
  const sql = `SELECT * FROM products WHERE ${where} ORDER BY ${orderCol} DESC LIMIT ? OFFSET ?`;
  params.push(limit, offset);

  const countSql = `SELECT COUNT(*) as total FROM products WHERE ${where}`;

  try {
    const [results, countResult] = await Promise.all([
      env.CATALOG_DB.prepare(sql).bind(...params).all(),
      env.CATALOG_DB.prepare(countSql).bind(...params.slice(0, -2)).first(),
    ]);

    return json({
      total: countResult?.total || 0,
      limit,
      offset,
      results: results.results.map(r => ({
        ...r,
        main_accords: r.main_accords ? JSON.parse(r.main_accords) : [],
      })),
    });
  } catch (err) {
    return json({ error: err.message }, 500);
  }
}

// ---------------------------------------------------------------------------
// Single product
// GET /api/product/fragrances/{id}
// ---------------------------------------------------------------------------
async function handleProduct(url, env) {
  const parts = url.pathname.split("/").filter(Boolean);
  // /api/product/{category}/{id}
  const category = parts[2] || "fragrances";
  const id = parts[3];
  if (!id) return json({ error: "id required" }, 400);

  try {
    const row = await env.CATALOG_DB.prepare(
      "SELECT * FROM products WHERE id = ? AND category = ?"
    ).bind(id, category).first();

    if (!row) return json({ error: "Not found" }, 404);

    // Also fetch full catalog record from R2 for complete data
    const fullRecord = await env.MASTER_DB.get(`catalog/${row.brand_slug}/${id}.json`)
      .then(o => o ? JSON.parse(o.text()) : null)
      .catch(() => null);

    return json({
      ...row,
      main_accords: row.main_accords ? JSON.parse(row.main_accords) : [],
      full: fullRecord,
    });
  } catch (err) {
    return json({ error: err.message }, 500);
  }
}

// ---------------------------------------------------------------------------
// Workflow management
// ---------------------------------------------------------------------------
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
      if (["complete","running","queued"].includes(status.status)) return status.status;
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
    instance = await env.CATALOG_WORKFLOW.create({
      id: testInstanceId,
      params: { brandName, brandSlug: testSlug },
    });
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

  // Check D1 index
  const d1Count = await env.CATALOG_DB.prepare(
    "SELECT COUNT(*) as cnt FROM products WHERE brand_slug = ? AND category = 'fragrances'"
  ).bind(testSlug).first().catch(() => null);

  return json({
    test: testSlug,
    brandName,
    instanceId: testInstanceId,
    workflowStatus: finalStatus.status,
    workflowOutput: finalStatus.output,
    workflowError: finalStatus.error || null,
    d1IndexedRecords: d1Count?.cnt || 0,
  });
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

// ---------------------------------------------------------------------------
// CatalogWorkflow — merge + images + D1 index
// ---------------------------------------------------------------------------
export class CatalogWorkflow extends WorkflowEntrypoint {
  async run(event, step) {
    const { brandName, brandSlug } = event.payload;

    // Step 1: Merge Fragella into catalog records
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
          const fe = fragellaIndex[normName] ||
            Object.entries(fragellaIndex).find(([k]) => k.includes(normName) && normName.length > 4)?.[1] ||
            Object.entries(fragellaIndex).find(([k]) => normName.includes(k) && k.length > 4)?.[1] ||
            null;

          if (!fe) { noMatch++; return; }

          record.fragella = {
            matchType: "workflow-merge",
            year: fe.Year || null, country: fe.Country || null,
            gender: fe.Gender || null, oilType: fe.OilType || null,
            longevity: fe.Longevity || null, sillage: fe.Sillage || null,
            popularity: fe.Popularity || null, rating: fe.rating || null,
            priceValue: fe["Price Value"] || null, price: fe.Price || null,
            imageUrl: fe["Image URL"] || null, imageFallbacks: fe["Image Fallbacks"] || [],
            purchaseUrl: fe["Purchase URL"] || null,
            mainAccords: fe["Main Accords"] || [],
            mainAccordsPercentage: fe["Main Accords Percentage"] || {},
            generalNotes: fe["General Notes"] || [],
            notes: fe.Notes || {},
            seasonRanking: fe["Season Ranking"] || [],
            occasionRanking: fe["Occasion Ranking"] || [],
          };
          record.syncedAt = new Date().toISOString();

          await Promise.all([
            this.env.MASTER_DB.put(key, JSON.stringify(record), { httpMetadata: { contentType: "application/json" } }),
            this.env.MASTER_DB.put(`sources/fragella/${record.id}.json`, JSON.stringify(fe), { httpMetadata: { contentType: "application/json" } }),
          ]);
          enriched++;
        } catch {}
      }));

      return { catalogRecords: catalogKeys.length, enriched, noMatch };
    });

    // Step 2: Mirror images
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

    // Step 3: Index into D1
    const indexResult = await step.do(`index-${brandSlug}`, async () => {
      let catalogKeys = [];
      let cursor;
      do {
        const listRes = await this.env.MASTER_DB.list({ prefix: `catalog/${brandSlug}/`, limit: 1000, cursor });
        catalogKeys = catalogKeys.concat(listRes.objects.map(o => o.key));
        cursor = listRes.truncated ? listRes.cursor : null;
      } while (cursor);

      let indexed = 0, errors = 0;

      // Process in batches of 50 for D1
      for (let i = 0; i < catalogKeys.length; i += 50) {
        const batch = catalogKeys.slice(i, i + 50);
        const records = await Promise.all(batch.map(async key => {
          try {
            const obj = await this.env.MASTER_DB.get(key);
            if (!obj) return null;
            return JSON.parse(await obj.text());
          } catch { return null; }
        }));

        const stmt = this.env.CATALOG_DB.prepare(`
          INSERT OR REPLACE INTO products
            (id, category, brand, brand_slug, name, slug, gender, year, country,
             longevity, sillage, popularity, rating, main_accords, image_url,
             has_fragplace, has_fragella, has_image, synced_at)
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        `);

        const d1batch = records
          .filter(r => r && r.id)
          .map(r => stmt.bind(
            String(r.id),
            "fragrances",
            r.brand || "",
            r.brandSlug || "",
            r.name || "",
            r.slug || "",
            r.fragella?.gender || null,
            r.fragella?.year || null,
            r.fragella?.country || null,
            r.fragella?.longevity || null,
            r.fragella?.sillage || null,
            r.fragella?.popularity || null,
            r.fragella?.rating || null,
            r.fragella?.mainAccords?.length ? JSON.stringify(r.fragella.mainAccords) : null,
            r.fragella?.imageUrl || r.fragplace?.imageUrl || null,
            r.fragplace ? 1 : 0,
            r.fragella ? 1 : 0,
            (r.fragella?.imageUrl || r.fragplace?.imageUrl) ? 1 : 0,
            r.syncedAt || new Date().toISOString(),
          ));

        if (d1batch.length > 0) {
          try {
            await this.env.CATALOG_DB.batch(d1batch);
            indexed += d1batch.length;
          } catch { errors += d1batch.length; }
        }
      }

      return { catalogRecords: catalogKeys.length, indexed, errors };
    });

    return {
      brandName, brandSlug,
      merge: mergeResult,
      images: imageResult,
      index: indexResult,
      completedAt: new Date().toISOString(),
    };
  }
}

function normalize(str) {
  return (str || "").toLowerCase().replace(/[`\u2018\u2019''']/g, "").replace(/&/g, "and").replace(/[^\w\s]/g, " ").replace(/\s+/g, " ").trim();
}

function slugify(s) {
  return (s || "").toLowerCase().replace(/'/g, "").replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");
}

function json(obj, status = 200) {
  return new Response(JSON.stringify(obj, null, 2), { status, headers: corsHeaders });
}
