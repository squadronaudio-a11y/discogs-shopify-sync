// sync.js — Discogs → Shopify (Easy Mode)
// - Creates/updates products as DRAFT (set in workflow)
// - Skips inventory updates (no location needed)
// - Writes only non-empty metafields
// - Finds existing products by SKU to avoid duplicates
// - Retries once if a product handle is taken

import 'dotenv/config';
import fetch from 'node-fetch';

const SHOPIFY_DOMAIN   = process.env.SHOPIFY_STORE_DOMAIN;
const SHOPIFY_TOKEN    = process.env.SHOPIFY_ADMIN_TOKEN;
const API_VERSION      = process.env.SHOPIFY_API_VERSION || '2025-01';
const DISCOGS_USERNAME = process.env.DISCOGS_USERNAME;
const DISCOGS_TOKEN    = process.env.DISCOGS_TOKEN;

const FOLDERS          = (process.env.SYNC_FOLDERS || '0').split(',').map(s => s.trim());
const PAGE_SIZE        = parseInt(process.env.SYNC_PAGE_SIZE || '25', 10);
const DEFAULT_STATUS   = process.env.DEFAULT_PRODUCT_STATUS || 'draft'; // draft or active
const COLLECTION_HANDLE= process.env.COLLECTION_HANDLE || null;

if (!SHOPIFY_DOMAIN || !SHOPIFY_TOKEN || !DISCOGS_USERNAME || !DISCOGS_TOKEN) {
  console.error('Missing required env vars. Need SHOPIFY_STORE_DOMAIN, SHOPIFY_ADMIN_TOKEN, DISCOGS_USERNAME, DISCOGS_TOKEN.');
  process.exit(1);
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function slugify(s) {
  return (s || '')
    .toString()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/(^-|-$)/g, '');
}

async function shopifyRest(path, method='GET', body=null) {
  const url = `https://${SHOPIFY_DOMAIN}/admin/api/${API_VERSION}${path}`;
  const res = await fetch(url, {
    method,
    headers: {
      'X-Shopify-Access-Token': SHOPIFY_TOKEN,
      'Content-Type': 'application/json',
    },
    body: body ? JSON.stringify(body) : null,
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Shopify ${method} ${path} → ${res.status}: ${text}`);
  }
  return res.json();
}

async function discogsGet(path, params={}) {
  const url = new URL(`https://api.discogs.com${path}`);
  Object.entries(params).forEach(([k,v]) => url.searchParams.set(k, v));
  const res = await fetch(url.toString(), {
    headers: { 'User-Agent': 'Discogs-Shopify-Sync/1.0', 'Authorization': `Discogs token=${DISCOGS_TOKEN}` }
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Discogs GET ${url} → ${res.status}: ${text}`);
  }
  return res.json();
}

function mapGradeTag(grade) {
  if (!grade) return null;
  return `grade:${grade.replace(/\s+/g,'')}`;
}

function buildTitle(artistList, title) {
  const artist = (artistList && artistList.length) ? artistList.map(a => a.name).join(', ') : '';
  return artist ? `${artist} – ${title}` : title;
}

function collectImageUrls(basic) {
  if (!basic) return [];
  const urls = new Set();
  if (basic.cover_image) urls.add(basic.cover_image);
  if (basic.thumb) urls.add(basic.thumb);
  if (Array.isArray(basic.images)) basic.images.forEach(img => img?.resource_url && urls.add(img.resource_url));
  return Array.from(urls);
}

// Easy Mode: no inventory updates (avoids location_id errors)
async function setInventory(/* inventoryItemId, quantity */) {
  return;
}

// Find existing product by SKU so we update instead of duplicating
async function findProductBySkuOrMetafield(sku) {
  const variants = await shopifyRest(`/variants.json?sku=${encodeURIComponent(sku)}`);
  const v = variants.variants?.[0];
  if (v?.product_id) {
    const prod = await shopifyRest(`/products/${v.product_id}.json`);
    return prod.product;
  }
  return null;
}

// Upsert only non-empty metafields (Shopify rejects blank values)
async function upsertProductMetafields(productId, kv) {
  const existing = await shopifyRest(`/products/${productId}/metafields.json`);
  const index = new Map(existing.metafields.map(m => [`${m.namespace}.${m.key}`, m]));

  for (const [full, raw] of Object.entries(kv)) {
    const value = (raw ?? '').toString().trim();
    if (!value) continue; // skip blanks

    const [namespace, key] = full.split('.');
    const current = index.get(full);
    const type = key === 'notes' ? 'multi_line_text_field' : 'single_line_text_field';

    if (current) {
      await shopifyRest(`/metafields/${current.id}.json`, 'PUT', { metafield: { id: current.id, value } });
    } else {
      await shopifyRest(`/products/${productId}/metafields.json`, 'POST', { metafield: { namespace, key, type, value } });
    }
    await sleep(200);
  }
}

async function addToCollectionByHandle(productId, handle) {
  if (!handle) return;
  try {
    const collections = await shopifyRest(`/custom_collections.json?handle=${encodeURIComponent(handle)}`);
    const coll = collections.custom_collections?.[0];
    if (!coll) return;
    await shopifyRest('/collects.json', 'POST', { collect: { product_id: productId, collection_id: coll.id } });
  } catch {
    // collection optional; ignore errors
  }
}

async function ensureProductFromDiscogsItem(item) {
  const basic = item.basic_information;
  const instanceId = item.instance_id?.toString();
  const sku = `DCG-${instanceId}`;

  const title = buildTitle(basic?.artists, basic?.title);
  const handle = slugify(`${title}-${instanceId}`);

  const body_html = [
    (basic?.formats || []).map(f => f.name).filter(Boolean).join(', '),
    basic?.labels?.[0]?.name,
    basic?.year ? `Year: ${basic.year}` : null,
  ].filter(Boolean).join(' • ');

  const mediaGrade  = item.media_condition || null;
  const sleeveGrade = item.sleeve_condition || null;
  const gradeTag    = mapGradeTag(mediaGrade);
  const sleeveTag   = sleeveGrade ? `sleeve:${sleeveGrade.replace(/\s+/g,'')}` : null;
  const tags        = [gradeTag, sleeveTag, 'source:discogs'].filter(Boolean).join(', ');

  const images = collectImageUrls(basic).slice(0, 8).map(u => ({ src: u }));

  // Find existing by SKU
  let existing = await findProductBySkuOrMetafield(sku);

  if (!existing) {
    const createPayload = {
      product: {
        title,
        body_html,
        handle,
        status: DEFAULT_STATUS,     // draft or active
        tags,
        vendor: basic?.labels?.[0]?.name || 'Unknown Label',
        product_type: 'Vinyl',
        variants: [{
          price: '0.00',           // set later in Shopify if needed
          sku,
          inventory_management: null, // Easy Mode: no inventory
          inventory_policy: 'deny',
          barcode: basic?.barcode || undefined,
        }],
        images,
      }
    };

    try {
      const created = await shopifyRest('/products.json', 'POST', createPayload);
      existing = created.product;
    } catch (e) {
      // If handle collision, make it unique and retry once
      if (String(e.message).includes('"handle":["has already been taken"]')) {
        createPayload.product.handle = `${handle}-${sku.toLowerCase()}`;
        const created2 = await shopifyRest('/products.json', 'POST', createPayload);
        existing = created2.product;
      } else {
        throw e;
      }
    }

    // Easy Mode: no inventory call
    // await setInventory(existing.variants[0].inventory_item_id, 1);

  } else {
    // Minimal updates if exists
    await shopifyRest(`/products/${existing.id}.json`, 'PUT', {
      product: { id: existing.id, tags }
    });
  }

  // Upsert metafields (skip blanks)
  await upsertProductMetafields(existing.id, {
    'discogs.release_id': basic?.id?.toString(),
    'discogs.master_id': (basic?.master_id || '').toString(),
    'discogs.media_condition': mediaGrade || '',
    'discogs.sleeve_condition': sleeveGrade || '',
    'discogs.notes': item.notes || '',
    'discogs.catalog_number': basic?.catno || '',
    'discogs.label': basic?.labels?.[0]?.name || '',
    'discogs.format': (basic?.formats || [])
      .map(f => [f.name, ...(f.descriptions || [])].filter(Boolean).join(' '))
      .join(', '),
    'discogs.year': basic?.year?.toString() || '',
    'discogs.barcode': (basic?.barcode || '').toString(),
    'discogs.instance_id': instanceId,
  });

  // Optional: add to collection
  await addToCollectionByHandle(existing.id, COLLECTION_HANDLE);

  return existing;
}

let failCount = 0;
const failures = [];

async function syncFolder(folderId) {
  let page = 1;
  while (true) {
    const data = await discogsGet(`/users/${DISCOGS_USERNAME}/collection/folders/${folderId}/releases`, {
      per_page: PAGE_SIZE,
      page,
      sort: 'added',
      sort_order: 'desc',
    });

    for (const item of data.releases) {
      try {
        await ensureProductFromDiscogsItem(item);
      } catch (e) {
        failCount++;
        failures.push({ instance: item?.instance_id, msg: e.message });
        console.error(`Failed item ${item?.instance_id} ${e.message}`);
      }
      await sleep(400);
    }

    if (!data.pagination || page >= data.pagination.pages) break;
    page++;
  }
}

(async function main() {
  console.log('Starting Discogs → Shopify sync…');
  for (const f of FOLDERS) {
    console.log('Syncing Discogs folder', f);
    await syncFolder(f);
  }
  if (failCount > 0) {
    console.log(`Sync finished with ${failCount} failed item(s).`);
    failures.slice(0, 20).forEach(f => console.log(`- ${f.instance}: ${f.msg}`));
    // process.exitCode = 1; // keep success green; uncomment to mark run failed
  } else {
    console.log('Sync finished with 0 failures.');
  }
  console.log('Sync complete.');
})();
