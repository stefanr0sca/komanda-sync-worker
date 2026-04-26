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

    if (url.pathname === "/api/sync-products" && request.method === "POST") {
      return handleSyncProducts(request, env);
    }

    if (url.pathname === "/api/sync-all-brands" && request.method === "POST") {
      return handleSyncAllBrands(request, env);
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
          "POST /api/sync-products     (enrich + merge scents for one brand; writes to R2)",
          "POST /api/sync-all-brands   (processes one brand per call; loop until done: true)",
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
// /api/sync-all-brands — full catalog sync across all brands in Vivantis pricelist.
//
// Processes ONE brand per call. The tester loops until done: true.
// Progress is persisted in R2 so it survives dropped connections.
//
// POST body: {
//   offset?: number,      // brand index to start at (0 = first brand)
//   reset?: boolean,      // if true, restart from brand 0
//   dryRun?: boolean,     // if true, no writes
//   scentLimit?: number   // max scents per brand (default 50)
// }
// ---------------------------------------------------------------------------
async function handleSyncAllBrands(request, env) {
  if (!env.RAPIDAPI_KEY) return json({ error: "RAPIDAPI_KEY not configured" }, 500);
  if (!env.FRAGELLA_API_KEY) return json({ error: "FRAGELLA_API_KEY not configured" }, 500);
  if (!env.MASTER_DB) return json({ error: "MASTER_DB R2 binding not configured" }, 500);

  let body = {};
  try { body = await request.json(); } catch {}

  const reset = body.reset === true;
  const dryRun = body.dryRun === true;
  const scentLimit = body.scentLimit || 50;

  // Load Vivantis pricelist and extract unique brand names
  let allBrands = [];
  try {
    const csvObj = await env.MASTER_DB.get("suppliers/vivantis/pricelist.csv");
    if (!csvObj) return json({ error: "suppliers/vivantis/pricelist.csv not found in R2" }, 500);
    const csvText = await csvObj.text();
    const rows = parseCSV(csvText, ";");
    const brandSet = new Set();
    for (const row of rows) {
      const b = (row["Brand"] || "").trim();
      if (b) brandSet.add(b);
    }
    allBrands = Array.from(brandSet).sort();
  } catch (err) {
    return json({ error: `Pricelist load failed: ${err.message}` }, 500);
  }

  // Load or reset state
  let state = {
    startedAt: new Date().toISOString(),
    status: "running",
    totalBrands: allBrands.length,
    currentOffset: 0,
    processed: 0,
    totalScentsWritten: 0,
    totalErrors: 0,
    skippedNoBrands: 0,
    brandResults: [],
  };

  if (!reset) {
    try {
      const stateObj = await env.MASTER_DB.get("state/sync-all-brands.json");
      if (stateObj) {
        const saved = JSON.parse(await stateObj.text());
        if (saved.status !== "done") state = saved;
      }
    } catch {}
  }

  const offset = typeof body.offset === "number" ? body.offset : state.currentOffset;

  if (offset >= allBrands.length) {
    return json({
      done: true,
      totalBrands: allBrands.length,
      processed: state.processed,
      totalScentsWritten: state.totalScentsWritten,
      totalErrors: state.totalErrors,
      skippedNoBrands: state.skippedNoBrands,
      note: "All brands processed.",
    });
  }

  const brandName = allBrands[offset];

  // Run sync for this brand
  const syncResult = await syncOneBrand(brandName, scentLimit, dryRun, env);

  // Update state
  state.currentOffset = offset + 1;
  state.processed++;
  state.totalScentsWritten += syncResult.written || 0;
  state.totalErrors += syncResult.errors || 0;
  if (syncResult.brandScentsFound === 0) state.skippedNoBrands++;

  state.brandResults.push({
    brand: brandName,
    scentsFound: syncResult.brandScentsFound,
    written: syncResult.written,
    fragellaExact: syncResult.fragellaExactMatch,
    fragellaNo: syncResult.fragellaNoMatch,
    vivantisNo: syncResult.vivantisNoMatch,
    errors: syncResult.errors,
  });

  const done = state.currentOffset >= allBrands.length;
  state.status = done ? "done" : "running";
  if (done) state.finishedAt = new Date().toISOString();

  if (!dryRun) {
    await env.MASTER_DB.put(
      "state/sync-all-brands.json",
      JSON.stringify(state),
      { httpMetadata: { contentType: "application/json" } }
    );
  }

  return json({
    done,
    brandName,
    offset,
    nextOffset: done ? null : state.currentOffset,
    totalBrands: allBrands.length,
    processed: state.processed,
    totalScentsWritten: state.totalScentsWritten,
    totalErrors: state.totalErrors,
    skippedNoBrands: state.skippedNoBrands,
    thisBrand: syncResult,
    finishedAt: done ? state.finishedAt : null,
  });
}

// Internal: sync one brand, return summary object
async function syncOneBrand(brandName, scentLimit, dryRun, env) {
  function normalize(str) {
    return (str || "")
      .toLowerCase()
      .replace(/[`\u2018\u2019']/g, "")
      .replace(/&/g, "and")
      .replace(/[^\w\s]/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  // Fetch Fragplace scents
  let fragplaceScents = [];
  let fragplaceTotal = 0;
  try {
    const fpRes = await fetch(FRAGPLACE_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-rapidapi-host": RAPIDAPI_HOST,
        "x-rapidapi-key": env.RAPIDAPI_KEY,
      },
      body: JSON.stringify({
        queries: [{ indexUid: "fragrances", q: "", limit: 1000, offset: 0 }],
      }),
    });
    if (fpRes.ok) {
      const fpData = await fpRes.json();
      const allHits = fpData?.results?.[0]?.hits || [];
      fragplaceTotal = fpData?.results?.[0]?.estimatedTotalHits ?? allHits.length;
      const normBrand = normalize(brandName);
      fragplaceScents = allHits.filter(h => {
        const hBrand = normalize(h.brand?.name || h.brand || "");
        return hBrand === normBrand || hBrand.includes(normBrand);
      });
    }
  } catch (_) {}

  const page = fragplaceScents.slice(0, scentLimit);

  if (page.length === 0) {
    return { brandScentsFound: 0, written: 0, fragellaExactMatch: 0, fragellaPartialMatch: 0, fragellaNoMatch: 0, vivantisMatched: 0, vivantisNoMatch: 0, errors: 0 };
  }

  // Load Vivantis pricelist
  let vivantisRows = [];
  try {
    const csvObj = await env.MASTER_DB.get("suppliers/vivantis/pricelist.csv");
    if (csvObj) vivantisRows = parseCSV(await csvObj.text(), ";");
  } catch (_) {}

  function extractScentName(rawName) {
    return rawName
      .replace(/\s*-\s*(EDP|EDT|EDC|Parfum|Extrait|Cologne|EDP Refill|Hair Mist).*$/i, "")
      .replace(/^[^-]+-\s*/, "")
      .trim();
  }

  const vivantisIndex = {};
  for (const row of vivantisRows) {
    const rawName = (row["Name"] || "").trim();
    if (!rawName) continue;
    const normKey = normalize(extractScentName(rawName));
    if (!vivantisIndex[normKey]) vivantisIndex[normKey] = [];
    vivantisIndex[normKey].push(row);
  }

  const summary = {
    brandScentsFound: fragplaceScents.length,
    written: 0, fragellaExactMatch: 0, fragellaPartialMatch: 0, fragellaNoMatch: 0,
    vivantisMatched: 0, vivantisNoMatch: 0, errors: 0,
  };

  const brandSlug = slugify(brandName);

  for (const scent of page) {
    const fragplaceId = scent.id;
    const scentName = scent.name || "";
    const scentSlug = slugify(scentName);

    try {
      // Fragella enrichment
      let fragellaData = null;
      let fragellaMatch = "none";
      try {
        const searchQuery = normalize(brandName + " " + scentName);
        const feUrl = `${FRAGELLA_BASE}/fragrances?search=${encodeURIComponent(searchQuery)}&limit=5`;
        const feRes = await fetch(feUrl, { headers: { "x-api-key": env.FRAGELLA_API_KEY } });
        if (feRes.ok) {
          const feRaw = await feRes.json();
          const feHits = Array.isArray(feRaw) ? feRaw : (feRaw.data || feRaw.results || []);
          const families = BRAND_FAMILIES[brandName] || [normalize(brandName)];
          const filtered = feHits.filter(h => {
            const b = normalize(h.Brand || h.brand || "");
            return families.some(f => b.includes(f));
          });
          const normFull = normalize(brandName + " " + scentName);
          const normScent = normalize(scentName);
          const best =
            filtered.find(h => normalize(h.Name || h.name) === normFull) ||
            filtered.find(h => normalize(h.Name || h.name) === normScent) ||
            filtered.find(h => normalize(h.Name || h.name).includes(normScent)) ||
            filtered[0] || null;
          if (best) {
            const mn = normalize(best.Name || best.name);
            fragellaMatch = (mn === normFull || mn === normScent) ? "exact" : "partial";
            fragellaData = best;
          }
        }
      } catch (_) {}

      if (fragellaMatch === "exact") summary.fragellaExactMatch++;
      else if (fragellaMatch === "partial") summary.fragellaPartialMatch++;
      else summary.fragellaNoMatch++;

      // Vivantis matching
      const normScentKey = normalize(scentName);
      let skuRows = vivantisIndex[normScentKey] || [];
      if (skuRows.length === 0) {
        for (const [key, rows] of Object.entries(vivantisIndex)) {
          if (key.includes(normScentKey) || normScentKey.includes(key)) { skuRows = rows; break; }
        }
      }
      if (skuRows.length > 0) summary.vivantisMatched++;
      else summary.vivantisNoMatch++;

      const variants = skuRows.map(row => {
        const rawName = (row["Name"] || "").trim();
        const volMatch = rawName.match(/Volume:\s*(\d+(?:\.\d+)?)\s*ml/i);
        const concMatch = rawName.match(/\b(EDP|EDT|EDC|Parfum|Extrait|Cologne|EDP Refill|Hair Mist)\b/i);
        return {
          ean: (row["EAN"] || "").trim() || null,
          ean1: (row["EAN 1"] || "").trim() || null,
          code: (row["Code"] || "").trim() || null,
          size_ml: volMatch ? parseFloat(volMatch[1]) : null,
          concentration: concMatch ? concMatch[1].toUpperCase() : null,
          wp_eur: parseFloat((row["WP w/o VAT"] || "0").replace(",", ".")) || null,
          rp_eur: parseFloat((row["RP"] || "0").replace(",", ".")) || null,
          in_stock: parseInt(row["In stock"] || "0", 10) || 0,
          in_action: (row["In Action"] || "").trim() === "YES",
          supplier: "vivantis",
          rawName,
        };
      });

      const catalogRecord = {
        id: fragplaceId, slug: scentSlug, brandSlug, brand: brandName, name: scentName,
        syncedAt: new Date().toISOString(),
        fragplace: {
          id: fragplaceId, popularityScore: scent.popularityScore ?? null,
          reviewsScoreAvg: scent.reviewsScoreAvg ?? null, reviewsCount: scent.reviewsCount ?? null,
          releasedAt: scent.releasedAt ?? null, status: scent.status ?? null,
          notes: scent.notes || [], perfumers: scent.perfumers || [],
          imageUrl: scent.image?.url || null,
        },
        fragella: fragellaData ? {
          matchType: fragellaMatch, confidence: fragellaData.Confidence || null,
          year: fragellaData.Year || null, country: fragellaData.Country || null,
          gender: fragellaData.Gender || null, oilType: fragellaData.OilType || null,
          longevity: fragellaData.Longevity || null, sillage: fragellaData.Sillage || null,
          popularity: fragellaData.Popularity || null, rating: fragellaData.rating || null,
          priceValue: fragellaData["Price Value"] || null, imageUrl: fragellaData["Image URL"] || null,
          mainAccords: fragellaData["Main Accords"] || [],
          mainAccordsPercentage: fragellaData["Main Accords Percentage"] || {},
          generalNotes: fragellaData["General Notes"] || [], notes: fragellaData.Notes || {},
          seasonRanking: fragellaData["Season Ranking"] || [],
          occasionRanking: fragellaData["Occasion Ranking"] || [],
          purchaseUrl: fragellaData["Purchase URL"] || null,
        } : null,
        variants, variantCount: variants.length, hasSupplierData: variants.length > 0,
      };

      if (!dryRun) {
        await Promise.all([
          env.MASTER_DB.put(`sources/fragplace/${fragplaceId}.json`, JSON.stringify(scent), { httpMetadata: { contentType: "application/json" } }),
          ...(fragellaData ? [env.MASTER_DB.put(`sources/fragella/${fragplaceId}.json`, JSON.stringify(fragellaData), { httpMetadata: { contentType: "application/json" } })] : []),
          env.MASTER_DB.put(`catalog/${brandSlug}/${fragplaceId}.json`, JSON.stringify(catalogRecord), { httpMetadata: { contentType: "application/json" } }),
        ]);
        summary.written++;
      }
    } catch (_) {
      summary.errors++;
    }
  }

  return summary;
}

// ---------------------------------------------------------------------------
// /api/sync-products — enrich and merge scents for one brand, write to R2.
//
// Flow per scent:
//   1. Fetch Fragplace scents (unfiltered, client-side filter by brand name)
//   2. Load Vivantis pricelist from R2 (suppliers/vivantis/pricelist.csv)
//   3. For each scent:
//      a. Search Fragella → enrichment + confidence
//      b. Find matching Vivantis SKUs by normalized scent name
//      c. Build merged catalog record with variants array
//      d. Write layered records to R2
//   4. Return summary
//
// POST body: {
//   brandName: string,    // required — e.g. "Lattafa"
//   offset?: number,      // Fragplace pagination offset (default 0)
//   limit?: number,       // max scents to process per call (default 20)
//   dryRun?: boolean      // if true, return merged records without writing
// }
// ---------------------------------------------------------------------------
async function handleSyncProducts(request, env) {
  if (!env.RAPIDAPI_KEY) return json({ error: "RAPIDAPI_KEY not configured" }, 500);
  if (!env.FRAGELLA_API_KEY) return json({ error: "FRAGELLA_API_KEY not configured" }, 500);
  if (!env.MASTER_DB) return json({ error: "MASTER_DB R2 binding not configured" }, 500);

  let body = {};
  try { body = await request.json(); } catch {}

  const brandName = (body.brandName || "").trim();
  const offset = body.offset || 0;
  const limit = body.limit || 20;
  const dryRun = body.dryRun === true;

  if (!brandName) return json({ error: "brandName is required" }, 400);

  // ── Shared normalizer ────────────────────────────────────────────────────
  function normalize(str) {
    return (str || "")
      .toLowerCase()
      .replace(/[`''']/g, "")
      .replace(/&/g, "and")
      .replace(/[^\w\s]/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  // ── Step 1: Fetch Fragplace scents (unfiltered, filter client-side) ──────
  let fragplaceScents = [];
  let fragplaceTotal = 0;
  try {
    const fpRes = await fetch(FRAGPLACE_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-rapidapi-host": RAPIDAPI_HOST,
        "x-rapidapi-key": env.RAPIDAPI_KEY,
      },
      body: JSON.stringify({
        queries: [{ indexUid: "fragrances", q: "", limit: 1000, offset: 0 }],
      }),
    });
    if (!fpRes.ok) {
      return json({ error: `Fragplace error ${fpRes.status}` }, fpRes.status);
    }
    const fpData = await fpRes.json();
    const allHits = fpData?.results?.[0]?.hits || [];
    fragplaceTotal = fpData?.results?.[0]?.estimatedTotalHits ?? allHits.length;

    // Client-side filter by brand name (normalized)
    const normBrand = normalize(brandName);
    fragplaceScents = allHits.filter(h => {
      const hBrand = normalize(h.brand?.name || h.brand || "");
      return hBrand === normBrand || hBrand.includes(normBrand);
    });
  } catch (err) {
    return json({ error: `Fragplace fetch failed: ${err.message}` }, 500);
  }

  // Apply offset + limit
  const page = fragplaceScents.slice(offset, offset + limit);
  const hasMore = offset + limit < fragplaceScents.length;

  if (page.length === 0) {
    return json({
      brandName,
      fragplaceTotal,
      brandScentsFound: fragplaceScents.length,
      offset,
      limit,
      processed: 0,
      hasMore: false,
      note: "No scents found for this brand at this offset.",
    });
  }

  // ── Step 2: Load Vivantis pricelist from R2 ──────────────────────────────
  let vivantisRows = [];
  try {
    const csvObj = await env.MASTER_DB.get("suppliers/vivantis/pricelist.csv");
    if (!csvObj) return json({ error: "suppliers/vivantis/pricelist.csv not found in R2" }, 500);
    const csvText = await csvObj.text();
    vivantisRows = parseCSV(csvText, ";");
  } catch (err) {
    return json({ error: `Pricelist load failed: ${err.message}` }, 500);
  }

  // Pre-build Vivantis scent name index: normalized scent name → [rows]
  function extractScentName(rawName) {
    return rawName
      .replace(/\s*-\s*(EDP|EDT|EDC|Parfum|Extrait|Cologne|EDP Refill|Hair Mist).*$/i, "")
      .replace(/^[^-]+-\s*/, "") // strip "BrandName - " prefix if present
      .trim();
  }

  const vivantisIndex = {};
  for (const row of vivantisRows) {
    const rawName = (row["Name"] || "").trim();
    if (!rawName) continue;
    const scentRaw = extractScentName(rawName);
    const normKey = normalize(scentRaw);
    if (!vivantisIndex[normKey]) vivantisIndex[normKey] = [];
    vivantisIndex[normKey].push(row);
  }

  // ── Step 3: Process each scent ───────────────────────────────────────────
  const results = [];
  const summary = {
    processed: 0,
    fragellaExactMatch: 0,
    fragellaPartialMatch: 0,
    fragellaNoMatch: 0,
    vivantisMatched: 0,
    vivantisNoMatch: 0,
    written: 0,
    errors: 0,
  };

  for (const scent of page) {
    summary.processed++;
    const fragplaceId = scent.id;
    const scentName = scent.name || "";
    const brandSlug = slugify(brandName);
    const scentSlug = slugify(scentName);

    try {
      // ── 3a: Fragella enrichment ──────────────────────────────────────────
      let fragellaData = null;
      let fragellaMatch = "none";
      try {
        const searchQuery = normalize(brandName + " " + scentName);
        const feUrl = `${FRAGELLA_BASE}/fragrances?search=${encodeURIComponent(searchQuery)}&limit=5`;
        const feRes = await fetch(feUrl, {
          headers: { "x-api-key": env.FRAGELLA_API_KEY },
        });
        if (feRes.ok) {
          const feRaw = await feRes.json();
          const feHits = Array.isArray(feRaw) ? feRaw : (feRaw.data || feRaw.results || []);

          // Filter by brand family
          const families = BRAND_FAMILIES[brandName] || [normalize(brandName)];
          const filtered = feHits.filter(h => {
            const b = normalize(h.Brand || h.brand || "");
            return families.some(f => b.includes(f));
          });

          const normFull = normalize(brandName + " " + scentName);
          const normScent = normalize(scentName);
          const best =
            filtered.find(h => normalize(h.Name || h.name) === normFull) ||
            filtered.find(h => normalize(h.Name || h.name) === normScent) ||
            filtered.find(h => normalize(h.Name || h.name).includes(normScent)) ||
            filtered[0] || null;

          if (best) {
            const matchedNorm = normalize(best.Name || best.name);
            fragellaMatch = (matchedNorm === normFull || matchedNorm === normScent) ? "exact" : "partial";
            fragellaData = best;
          }
        }
      } catch (_) {}

      if (fragellaMatch === "exact") summary.fragellaExactMatch++;
      else if (fragellaMatch === "partial") summary.fragellaPartialMatch++;
      else summary.fragellaNoMatch++;

      // ── 3b: Vivantis SKU matching ────────────────────────────────────────
      const normScentKey = normalize(scentName);
      const matchedRows = vivantisIndex[normScentKey] || [];

      // Fallback: partial match if exact fails
      let skuRows = matchedRows;
      if (skuRows.length === 0) {
        // Try partial — scent name contains or is contained by vivantis key
        for (const [key, rows] of Object.entries(vivantisIndex)) {
          if (key.includes(normScentKey) || normScentKey.includes(key)) {
            skuRows = rows;
            break;
          }
        }
      }

      if (skuRows.length > 0) summary.vivantisMatched++;
      else summary.vivantisNoMatch++;

      // Build variants array from matched Vivantis rows
      const variants = skuRows.map(row => {
        const rawName = (row["Name"] || "").trim();
        // Extract size_ml from "Volume: 100 ml"
        const volMatch = rawName.match(/Volume:\s*(\d+(?:\.\d+)?)\s*ml/i);
        const sizeMl = volMatch ? parseFloat(volMatch[1]) : null;
        // Extract concentration
        const concMatch = rawName.match(/\b(EDP|EDT|EDC|Parfum|Extrait|Cologne|EDP Refill|Hair Mist)\b/i);
        const concentration = concMatch ? concMatch[1].toUpperCase() : null;

        return {
          ean: (row["EAN"] || "").trim() || null,
          ean1: (row["EAN 1"] || "").trim() || null,
          code: (row["Code"] || "").trim() || null,
          size_ml: sizeMl,
          concentration,
          wp_eur: parseFloat((row["WP w/o VAT"] || "0").replace(",", ".")) || null,
          rp_eur: parseFloat((row["RP"] || "0").replace(",", ".")) || null,
          in_stock: parseInt(row["In stock"] || "0", 10) || 0,
          in_action: (row["In Action"] || "").trim() === "YES",
          supplier: "vivantis",
          rawName,
        };
      });

      // ── 3c: Build merged catalog record ─────────────────────────────────
      const catalogRecord = {
        id: fragplaceId,
        slug: scentSlug,
        brandSlug,
        brand: brandName,
        name: scentName,
        syncedAt: new Date().toISOString(),

        // Fragplace fields
        fragplace: {
          id: fragplaceId,
          popularityScore: scent.popularityScore ?? null,
          reviewsScoreAvg: scent.reviewsScoreAvg ?? null,
          reviewsCount: scent.reviewsCount ?? null,
          releasedAt: scent.releasedAt ?? null,
          status: scent.status ?? null,
          notes: scent.notes || [],
          perfumers: scent.perfumers || [],
          imageUrl: scent.image?.url || null,
        },

        // Fragella enrichment (null if not found)
        fragella: fragellaData ? {
          matchType: fragellaMatch,
          confidence: fragellaData.Confidence || null,
          year: fragellaData.Year || null,
          country: fragellaData.Country || null,
          gender: fragellaData.Gender || null,
          oilType: fragellaData.OilType || null,
          longevity: fragellaData.Longevity || null,
          sillage: fragellaData.Sillage || null,
          popularity: fragellaData.Popularity || null,
          rating: fragellaData.rating || null,
          priceValue: fragellaData["Price Value"] || null,
          imageUrl: fragellaData["Image URL"] || null,
          mainAccords: fragellaData["Main Accords"] || [],
          mainAccordsPercentage: fragellaData["Main Accords Percentage"] || {},
          generalNotes: fragellaData["General Notes"] || [],
          notes: fragellaData.Notes || {},
          seasonRanking: fragellaData["Season Ranking"] || [],
          occasionRanking: fragellaData["Occasion Ranking"] || [],
          purchaseUrl: fragellaData["Purchase URL"] || null,
        } : null,

        // Commerce layer
        variants,
        variantCount: variants.length,
        hasSupplierData: variants.length > 0,
      };

      const result = {
        fragplaceId,
        scentName,
        fragellaMatch,
        variantCount: variants.length,
      };

      if (!dryRun) {
        // Write layered records to R2
        const writes = [
          // Raw Fragplace source
          env.MASTER_DB.put(
            `sources/fragplace/${fragplaceId}.json`,
            JSON.stringify(scent),
            { httpMetadata: { contentType: "application/json" } }
          ),
          // Raw Fragella source (if found)
          ...(fragellaData ? [env.MASTER_DB.put(
            `sources/fragella/${fragplaceId}.json`,
            JSON.stringify(fragellaData),
            { httpMetadata: { contentType: "application/json" } }
          )] : []),
          // Merged catalog record
          env.MASTER_DB.put(
            `catalog/${brandSlug}/${fragplaceId}.json`,
            JSON.stringify(catalogRecord),
            { httpMetadata: { contentType: "application/json" } }
          ),
        ];
        await Promise.all(writes);
        summary.written++;
        result.written = true;
      } else {
        result.dryRun = true;
        result.catalogRecord = catalogRecord;
      }

      results.push(result);
    } catch (err) {
      summary.errors++;
      results.push({ fragplaceId, scentName, error: err.message });
    }
  }

  return json({
    brandName,
    dryRun,
    fragplaceTotal,
    brandScentsFound: fragplaceScents.length,
    offset,
    limit,
    hasMore,
    nextOffset: hasMore ? offset + limit : null,
    summary,
    results,
  });
}

// ── Simple CSV parser (handles semicolon delimiter + quoted fields) ──────────
function parseCSV(text, delimiter = ";") {
  const lines = text.split(/\r?\n/).filter(l => l.trim());
  if (lines.length < 2) return [];
  const headers = splitCSVLine(lines[0], delimiter);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const values = splitCSVLine(lines[i], delimiter);
    if (values.length === 0) continue;
    const row = {};
    headers.forEach((h, idx) => { row[h] = values[idx] || ""; });
    rows.push(row);
  }
  return rows;
}

function splitCSVLine(line, delimiter) {
  const result = [];
  let current = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      inQuotes = !inQuotes;
    } else if (ch === delimiter && !inQuotes) {
      result.push(current.trim());
      current = "";
    } else {
      current += ch;
    }
  }
  result.push(current.trim());
  return result;
}

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
    // Match against full "brand scent" string when brandName provided, scent alone otherwise
    const normScent = normalize(scentName);
    const normFull = brandName ? normalize(brandName + " " + scentName) : normScent;
    const bestMatch =
      filtered.find(h => normalize(h.Name || h.name) === normFull) ||
      filtered.find(h => normalize(h.Name || h.name) === normScent) ||
      filtered.find(h => normalize(h.Name || h.name).includes(normScent)) ||
      filtered[0] ||
      null;

    // Collect all field names seen across hits
    const fieldsSeen = new Set();
    for (const h of hits) Object.keys(h).forEach(k => fieldsSeen.add(k));

    const matchedNorm = bestMatch ? normalize(bestMatch.Name || bestMatch.name) : null;
    return json({
      scentName,
      brandName: brandName || null,
      searchQuery,
      totalHits: hits.length,
      filteredHits: filtered.length,
      exactMatch: matchedNorm === normFull || matchedNorm === normScent,
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
