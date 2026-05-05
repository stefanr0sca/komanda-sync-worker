// src/index.js
// Fragrances Catalog Worker + Workflow

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
        "POST /api/workflow/start         — trigger all brands (returns immediately)",
        "POST /api/workflow/test          — test with one brand {brandSlug?} and wait for result",
        "GET  /api/workflow/status?id=<id> — get instance status",
      ]});
    }

    // Start all — returns immediately, creates instances in background
    if (url.pathname === "/api/workflow/start" && request.method === "POST") {
      ctx.waitUntil(startAllWorkflows(env));
      return json({ status: "triggering", message: "Creating/restarting workflow instances in background." });
    }

    // Test — runs ONE brand synchronously and returns full result
    if (url.pathname === "/api/workflow/test" && request.method === "POST") {
      return handleTest(request, env);
    }

    // Status check
    if (url.pathname === "/api/workflow/status" && request.method === "GET") {
      return handleStatus(url, env);
    }

    return json({ error: "Not found" }, 404);
  },

  async scheduled(event, env, ctx) {
    console.log("Cron fired:", event.cron);
    ctx.waitUntil(startAllWorkflows(env));
  },
};

// ---------------------------------------------------------------------------
// Core: start or restart workflow instances for all brands with Fragella data
// ---------------------------------------------------------------------------
async function startAllWorkflows(env) {
  let allBrands = [];
  try {
    const indexObj = await env.MASTER_DB.get("brands/index.json");
    if (!indexObj) return;
    allBrands = JSON.parse(await indexObj.text());
  } catch { return; }

  // Fast list of fragella brands in one call
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

    // Check if instance exists and if it errored — restart if so
    try {
      const existing = await env.CATALOG_WORKFLOW.get(instanceId);
      const status = await existing.status();
      if (status.status === "errored") {
        await existing.restart();
        return "restarted";
      }
      if (status.status === "complete" || status.status === "running" || status.status === "queued") {
        return status.status; // skip
      }
    } catch {}

    // Create new instance
    try {
      await env.CATALOG_WORKFLOW.create({
        id: instanceId,
        params: { brandName: brand.name, brandSlug: slug },
      });
      return "started";
    } catch { return "skipped"; }
  }));

  const counts = { started: 0, restarted: 0, running: 0, complete: 0, skipped: 0 };
  for (const r of results) {
    if (r.status === "fulfilled") counts[r.value] = (counts[r.value] || 0) + 1;
  }
  console.log(`Workflows: ${JSON.stringify(counts)} / ${brandsToStart.length} total`);
}

// ---------------------------------------------------------------------------
// /api/workflow/test — run ONE brand and wait for completion
// Returns full result so you can verify merge + images worked correctly
// POST body: { brandSlug?: string } — defaults to "dior" if not provided
// ---------------------------------------------------------------------------
async function handleTest(request, env) {
  let body = {};
  try { body = await request.json(); } catch {}
  const testSlug = body.brandSlug || "dior";

  // Load brand name from index
  let brandName = testSlug;
  try {
    const indexObj = await env.MASTER_DB.get("brands/index.json");
    if (indexObj) {
      const brands = JSON.parse(await indexObj.text());
      const match = brands.find(b => slugify(b.name) === testSlug);
      if (match) brandName = match.name;
    }
  } catch {}

  // Check fragella data exists
  const feObj = await env.MASTER_DB.get(`fragella-brands/${testSlug}.json`).catch(() => null);
  if (!feObj) return json({ error: `No fragella data for "${testSlug}". Run Phase 2a first.` }, 400);
  const feData = JSON.parse(await feObj.text());

  // Check catalog records exist
  const catalogList = await env.MASTER_DB.list({ prefix: `catalog/${testSlug}/`, limit: 3 });
  if (catalogList.objects.length === 0) return json({ error: `No catalog records for "${testSlug}"` }, 400);

  // Read first catalog record before merge
  const sampleKey = catalogList.objects[0].key;
  const sampleBefore = await env.MASTER_DB.get(sampleKey).then(o => JSON.parse(o.text())).catch(() => null);

  // Create/restart a test instance with a unique ID so it doesn't conflict
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

  // Poll for completion (max 60s)
  const start = Date.now();
  let finalStatus;
  while (Date.now() - start < 60000) {
    await new Promise(r => setTimeout(r, 2000));
    try {
      const s = await instance.status();
      if (s.status === "complete" || s.status === "errored") {
        finalStatus = s;
        break;
      }
    } catch {}
  }

  if (!finalStatus) {
    return json({ status: "timeout", instanceId: testInstanceId, message: "Workflow still running after 60s — check status endpoint." });
  }

  // Read sample record after merge to verify
  const sampleAfter = await env.MASTER_DB.get(sampleKey).then(o => JSON.parse(o.text())).catch(() => null);

  return json({
    test: testSlug,
    brandName,
    instanceId: testInstanceId,
    workflowStatus: finalStatus.status,
    workflowOutput: finalStatus.output,
    workflowError: finalStatus.error || null,
    fragellaBrandScents: feData.count,
    catalogRecordsSampled: catalogList.objects.length,
    sampleScent: sampleBefore?.name,
    fragellaBeforeMerge: sampleBefore?.fragella,
    fragellaAfterMerge: sampleAfter?.fragella ? {
      matchType: sampleAfter.fragella.matchType,
      gender: sampleAfter.fragella.gender,
      longevity: sampleAfter.fragella.longevity,
      mainAccords: sampleAfter.fragella.mainAccords?.slice(0, 3),
      imageUrl: sampleAfter.fragella.imageUrl,
    } : null,
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
// CatalogWorkflow — one instance per brand: merge + image mirror
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

    // Step 2: Mirror images to fragrances-images R2
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

    return { brandName, brandSlug, merge: mergeResult, images: imageResult, completedAt: new Date().toISOString() };
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
