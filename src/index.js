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
      return json({ worker: "fragrances-catalog-worker", status: "ok" });
    }

    if (url.pathname === "/api/workflow/start" && request.method === "POST") {
      // Return immediately, create instances in background
      ctx.waitUntil(startAllWorkflows(env));
      return json({ status: "triggering", message: "Workflow instances are being created in background. Check Cloudflare Workflows dashboard for progress." });
    }

    if (url.pathname === "/api/workflow/start-brand" && request.method === "POST") {
      return handleStartBrand(request, env);
    }

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

// Core: start workflow instances for all brands with Fragella data
async function startAllWorkflows(env) {
  let allBrands = [];
  try {
    const indexObj = await env.MASTER_DB.get("brands/index.json");
    if (!indexObj) return;
    allBrands = JSON.parse(await indexObj.text());
  } catch { return; }

  // Fast list of fragella brands
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

  // Create all instances in parallel — fire and forget errors
  const results = await Promise.allSettled(brandsToStart.map(async (brand) => {
    const slug = slugify(brand.name);
    const instanceId = `brand-${slug}`;
    await env.CATALOG_WORKFLOW.create({
      id: instanceId,
      params: { brandName: brand.name, brandSlug: slug },
    });
    return instanceId;
  }));

  const started = results.filter(r => r.status === "fulfilled").length;
  const skipped = results.filter(r => r.status === "rejected").length;
  console.log(`Workflows: started=${started} skipped/existing=${skipped} total=${brandsToStart.length}`);
}

async function handleStartBrand(request, env) {
  let body = {};
  try { body = await request.json(); } catch {}
  const { brandName, brandSlug } = body;
  if (!brandName && !brandSlug) return json({ error: "brandName or brandSlug required" }, 400);
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
// CatalogWorkflow — one instance per brand
// ---------------------------------------------------------------------------
export class CatalogWorkflow extends WorkflowEntrypoint {
  async run(event, step) {
    const { brandName, brandSlug } = event.payload;

    // Step 1: Merge Fragella data into catalog records
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

            const downloads = [];
            const fpUrl = record.fragplace?.imageUrl;
            if (fpUrl) {
              const fpKey = `fragplace/${id}.webp`;
              const exists = await this.env.IMAGES_DB.get(fpKey).catch(() => null);
              if (!exists) {
                downloads.push(fetch(fpUrl).then(async r => {
                  if (r.ok) { await this.env.IMAGES_DB.put(fpKey, await r.arrayBuffer(), { httpMetadata: { contentType: r.headers.get("content-type") || "image/jpeg" } }); mirrored++; }
                  else { failed++; }
                }).catch(() => { failed++; }));
              } else { skipped++; }
            }

            const feUrl = record.fragella?.imageUrl;
            if (feUrl && feUrl !== fpUrl) {
              const feKey = `fragella/${id}.webp`;
              const exists = await this.env.IMAGES_DB.get(feKey).catch(() => null);
              if (!exists) {
                downloads.push(fetch(feUrl).then(async r => {
                  if (r.ok) { await this.env.IMAGES_DB.put(feKey, await r.arrayBuffer(), { httpMetadata: { contentType: r.headers.get("content-type") || "image/jpeg" } }); mirrored++; }
                  else { failed++; }
                }).catch(() => { failed++; }));
              } else { skipped++; }
            }

            await Promise.all(downloads);
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
