// src/index.js
// Fragrances Catalog Worker + Workflow
//
// Endpoints:
//   GET  /                              → health + status
//   POST /api/workflow/start            → start full catalog pipeline for all brands
//   POST /api/workflow/start-brand      → start pipeline for one brand
//   GET  /api/workflow/status?id=<id>   → get workflow instance status
//   POST /api/workflow/pause?id=<id>    → pause a running instance
//   GET  /api/brands/progress           → summary of fragella-brands fetch progress

import { WorkflowEntrypoint } from 'cloudflare:workers';

const FRAGELLA_BASE = "https://api.fragella.com/api/v1";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Content-Type": "application/json",
};

// ---------------------------------------------------------------------------
// Main fetch handler — HTTP API
// ---------------------------------------------------------------------------
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders });
    }

    // Health / status
    if (url.pathname === "/" || url.pathname === "/health") {
      return json({
        worker: "fragrances-catalog-worker",
        status: "ok",
        endpoints: [
          "POST /api/workflow/start           — start full pipeline (all brands)",
          "POST /api/workflow/start-brand     — start pipeline for one brand {brandSlug}",
          "GET  /api/workflow/status?id=<id>  — get workflow instance status",
          "GET  /api/brands/progress          — fragella fetch progress summary",
        ],
      });
    }

    // Start workflow for ALL brands
    if (url.pathname === "/api/workflow/start" && request.method === "POST") {
      return handleStartAll(request, env);
    }

    // Start workflow for ONE brand
    if (url.pathname === "/api/workflow/start-brand" && request.method === "POST") {
      return handleStartBrand(request, env);
    }

    // Get workflow instance status
    if (url.pathname === "/api/workflow/status" && request.method === "GET") {
      return handleStatus(url, env);
    }

    // Progress summary
    if (url.pathname === "/api/brands/progress" && request.method === "GET") {
      return handleProgress(env);
    }

    return json({ error: "Not found" }, 404);
  },

  // Cron: trigger workflow for any brands not yet processed
  async scheduled(event, env, ctx) {
    console.log("Cron fired:", event.cron);
    ctx.waitUntil(cronTrigger(env));
  },
};

// ---------------------------------------------------------------------------
// Start workflow for ALL brands — creates one instance per brand
// ---------------------------------------------------------------------------
async function handleStartAll(request, env) {
  let body = {};
  try { body = await request.json(); } catch {}
  const { batchSize = 50, reset = false } = body;

  let allBrands = [];
  try {
    const indexObj = await env.MASTER_DB.get("brands/index.json");
    if (!indexObj) return json({ error: "brands/index.json not found" }, 500);
    allBrands = JSON.parse(await indexObj.text());
  } catch (err) {
    return json({ error: `Brand index load failed: ${err.message}` }, 500);
  }

  // Only start workflows for brands that have Fragella data
  const brandsWithFragella = [];
  for (const brand of allBrands) {
    const slug = slugify(brand.name);
    try {
      const exists = await env.MASTER_DB.get(`fragella-brands/${slug}.json`);
      if (exists) brandsWithFragella.push(brand);
    } catch {}
  }

  const batch = brandsWithFragella.slice(0, batchSize);
  const started = [];

  for (const brand of batch) {
    const slug = slugify(brand.name);
    const instanceId = `brand-${slug}`;
    try {
      // Check if already running
      const existing = await env.CATALOG_WORKFLOW.get(instanceId).catch(() => null);
      if (existing) {
        const status = await existing.status();
        if (status.status === "running" || status.status === "queued") {
          started.push({ brandName: brand.name, instanceId, status: "already-running" });
          continue;
        }
      }
    } catch {}

    try {
      const instance = await env.CATALOG_WORKFLOW.create({
        id: instanceId,
        params: { brandName: brand.name, brandSlug: slug },
      });
      started.push({ brandName: brand.name, instanceId: instance.id, status: "started" });
    } catch (err) {
      started.push({ brandName: brand.name, instanceId, status: "error", error: err.message });
    }
  }

  return json({
    totalBrandsWithFragella: brandsWithFragella.length,
    batchSize,
    started: started.length,
    results: started,
  });
}

// ---------------------------------------------------------------------------
// Start workflow for ONE brand
// ---------------------------------------------------------------------------
async function handleStartBrand(request, env) {
  let body = {};
  try { body = await request.json(); } catch {}
  const { brandName, brandSlug } = body;

  if (!brandName && !brandSlug) {
    return json({ error: "brandName or brandSlug required" }, 400);
  }

  const slug = brandSlug || slugify(brandName);
  const name = brandName || slug;
  const instanceId = `brand-${slug}`;

  try {
    const instance = await env.CATALOG_WORKFLOW.create({
      id: instanceId,
      params: { brandName: name, brandSlug: slug },
    });
    return json({ instanceId: instance.id, status: "started", brandName: name });
  } catch (err) {
    return json({ error: err.message }, 500);
  }
}

// ---------------------------------------------------------------------------
// Get workflow instance status
// ---------------------------------------------------------------------------
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
// Progress summary — reads state from R2
// ---------------------------------------------------------------------------
async function handleProgress(env) {
  const [fragellaBrandsState, mergeState, imagesState] = await Promise.all([
    env.MASTER_DB.get("state/sync-fragella-brands.json").then(o => o ? JSON.parse(o.text()) : null).catch(() => null),
    env.MASTER_DB.get("state/fragella-merge.json").then(o => o ? JSON.parse(o.text()) : null).catch(() => null),
    env.MASTER_DB.get("state/images-mirror.json").then(o => o ? JSON.parse(o.text()) : null).catch(() => null),
  ]);

  return json({
    fragellaBrandsFetch: fragellaBrandsState,
    fragellaMerge: mergeState,
    imagesMirror: imagesState,
  });
}

// ---------------------------------------------------------------------------
// Cron: automatically trigger workflow batches
// ---------------------------------------------------------------------------
async function cronTrigger(env) {
  let allBrands = [];
  try {
    const indexObj = await env.MASTER_DB.get("brands/index.json");
    if (!indexObj) return;
    allBrands = JSON.parse(await indexObj.text());
  } catch { return; }

  let triggered = 0;
  for (const brand of allBrands) {
    const slug = slugify(brand.name);
    // Only trigger for brands with Fragella data
    try {
      const hasFragella = await env.MASTER_DB.get(`fragella-brands/${slug}.json`);
      if (!hasFragella) continue;
    } catch { continue; }

    const instanceId = `brand-${slug}`;
    try {
      const existing = await env.CATALOG_WORKFLOW.get(instanceId).catch(() => null);
      if (existing) {
        const status = await existing.status();
        if (status.status === "running" || status.status === "queued" || status.status === "complete") continue;
      }
      await env.CATALOG_WORKFLOW.create({
        id: instanceId,
        params: { brandName: brand.name, brandSlug: slug },
      });
      triggered++;
      if (triggered >= 20) break; // max 20 per cron run
    } catch {}
  }
  console.log(`Cron: triggered ${triggered} workflow instances`);
}

// ---------------------------------------------------------------------------
// CatalogWorkflow — one instance per brand, three steps
// ---------------------------------------------------------------------------
export class CatalogWorkflow extends WorkflowEntrypoint {
  async run(event, step) {
    const { brandName, brandSlug } = event.params;

    // ── Step 1: Fragella merge ───────────────────────────────────────────────
    // Read fragella-brands/{slug}.json and match scents into catalog records.
    // Step result is persisted — if worker restarts, this step is skipped.
    const mergeResult = await step.do(`merge-fragella-${brandSlug}`, async () => {
      const feObj = await this.env.MASTER_DB.get(`fragella-brands/${brandSlug}.json`);
      if (!feObj) return { skipped: true, reason: "no fragella data" };

      const fragellaData = JSON.parse(await feObj.text());
      if (!fragellaData.scents?.length) return { skipped: true, reason: "empty scents" };

      // Build name index
      const fragellaIndex = {};
      for (const h of fragellaData.scents) {
        const normName = normalize(h.Name || h.name || "");
        if (normName) fragellaIndex[normName] = h;
      }

      // List catalog records
      let catalogKeys = [];
      let cursor;
      do {
        const listRes = await this.env.MASTER_DB.list({ prefix: `catalog/${brandSlug}/`, limit: 1000, cursor });
        catalogKeys = catalogKeys.concat(listRes.objects.map(o => o.key));
        cursor = listRes.truncated ? listRes.cursor : null;
      } while (cursor);

      if (catalogKeys.length === 0) return { skipped: true, reason: "no catalog records" };

      let enriched = 0;
      let noMatch = 0;

      await Promise.all(catalogKeys.map(async (key) => {
        try {
          const obj = await this.env.MASTER_DB.get(key);
          if (!obj) return;
          const record = JSON.parse(await obj.text());
          if (record.fragella !== null && record.fragella !== undefined) return;

          const normName = normalize(record.name || "");
          const fe =
            fragellaIndex[normName] ||
            Object.entries(fragellaIndex).find(([k]) => k.includes(normName) && normName.length > 4)?.[1] ||
            Object.entries(fragellaIndex).find(([k]) => normName.includes(k) && k.length > 4)?.[1] ||
            null;

          if (!fe) { noMatch++; return; }

          record.fragella = {
            matchType: "workflow-merge",
            year: fe.Year || null,
            country: fe.Country || null,
            gender: fe.Gender || null,
            oilType: fe.OilType || null,
            longevity: fe.Longevity || null,
            sillage: fe.Sillage || null,
            popularity: fe.Popularity || null,
            rating: fe.rating || null,
            priceValue: fe["Price Value"] || null,
            price: fe.Price || null,
            imageUrl: fe["Image URL"] || null,
            imageFallbacks: fe["Image Fallbacks"] || [],
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

    // ── Step 2: Mirror images ────────────────────────────────────────────────
    // Download Fragplace + Fragella images to fragrances-images R2 bucket.
    // 20 scents at a time to stay within CPU limits.
    const imageResult = await step.do(`mirror-images-${brandSlug}`, async () => {
      let catalogKeys = [];
      let cursor;
      do {
        const listRes = await this.env.MASTER_DB.list({ prefix: `catalog/${brandSlug}/`, limit: 1000, cursor });
        catalogKeys = catalogKeys.concat(listRes.objects.map(o => o.key));
        cursor = listRes.truncated ? listRes.cursor : null;
      } while (cursor);

      let mirrored = 0;
      let skipped = 0;
      let failed = 0;

      // Process in chunks of 20
      for (let i = 0; i < catalogKeys.length; i += 20) {
        const chunk = catalogKeys.slice(i, i + 20);
        await Promise.all(chunk.map(async (key) => {
          try {
            const obj = await this.env.MASTER_DB.get(key);
            if (!obj) return;
            const record = JSON.parse(await obj.text());
            const id = record.id;
            if (!id) return;

            const downloads = [];

            // Fragplace image
            const fpUrl = record.fragplace?.imageUrl;
            if (fpUrl) {
              const fpKey = `fragplace/${id}.webp`;
              const exists = await this.env.IMAGES_DB.get(fpKey).catch(() => null);
              if (!exists) {
                downloads.push(
                  fetch(fpUrl).then(async r => {
                    if (r.ok) {
                      const buf = await r.arrayBuffer();
                      await this.env.IMAGES_DB.put(fpKey, buf, { httpMetadata: { contentType: r.headers.get("content-type") || "image/webp" } });
                      mirrored++;
                    } else { failed++; }
                  }).catch(() => { failed++; })
                );
              } else { skipped++; }
            }

            // Fragella image
            const feUrl = record.fragella?.imageUrl;
            if (feUrl && feUrl !== fpUrl) {
              const feKey = `fragella/${id}.webp`;
              const exists = await this.env.IMAGES_DB.get(feKey).catch(() => null);
              if (!exists) {
                downloads.push(
                  fetch(feUrl).then(async r => {
                    if (r.ok) {
                      const buf = await r.arrayBuffer();
                      await this.env.IMAGES_DB.put(feKey, buf, { httpMetadata: { contentType: r.headers.get("content-type") || "image/webp" } });
                      mirrored++;
                    } else { failed++; }
                  }).catch(() => { failed++; })
                );
              } else { skipped++; }
            }

            await Promise.all(downloads);
          } catch { failed++; }
        }));
      }

      return { catalogRecords: catalogKeys.length, mirrored, skipped, failed };
    });

    return {
      brandName,
      brandSlug,
      merge: mergeResult,
      images: imageResult,
      completedAt: new Date().toISOString(),
    };
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function normalize(str) {
  return (str || "")
    .toLowerCase()
    .replace(/[`\u2018\u2019''']/g, "")
    .replace(/&/g, "and")
    .replace(/[^\w\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
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
