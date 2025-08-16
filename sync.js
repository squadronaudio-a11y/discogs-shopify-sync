// sync.js
// -------
import 'dotenv/config';
import fetch from 'node-fetch';

const SHOPIFY_DOMAIN = process.env.SHOPIFY_STORE_DOMAIN;
const SHOPIFY_TOKEN = process.env.SHOPIFY_ADMIN_TOKEN;
const API_VERSION = process.env.SHOPIFY_API_VERSION || '2025-01';
const LOCATION_ID = process.env.SHOPIFY_LOCATION_ID; // gid://shopify/Location/...
const DISCOGS_USERNAME = process.env.DISCOGS_USERNAME;
const DISCOGS_TOKEN = process.env.DISCOGS_TOKEN;
const FOLDERS = (process.env.SYNC_FOLDERS || '0').split(',').map(s => s.trim());
const PAGE_SIZE = parseInt(process.env.SYNC_PAGE_SIZE || '50', 10);
const DEFAULT_STATUS = process.env.DEFAULT_PRODUCT_STATUS || 'active';
const COLLECTION_HANDLE = process.env.COLLECTION_HANDLE || null;

if (!SHOPIFY_DOMAIN || !SHOPIFY_TOKEN || !LOCATION_ID || !DISCOGS_USERNAME || !DISCOGS_TOKEN) {
  console.error('Missing required env vars. Please check .env.');
  process.exit(1);
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function slugify(s) {
  return s.toLowerCase().replace(/[^a-z0-9]+/g,'-').replace(/(^-|-$)/g,'');
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
  Object.entries(params).forEach(([k,v])=> url.searchParams.set(k, v));
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
  return `grade:${grade.replace(/\s+/g,'')}`; // e.g., NM or VG+
}

function buildTitle(artistList, title) {
  const artist = (artistList && artistList.length) ? artistList.map(a=>a.name).join(', ') : '';
  return artist ? `${artist} – ${title}` : title;
}

function collectImageUrls(basic) {
  if (!basic || !basic.cover_image) return [];
  const urls = new Set();
  if (basic.cover_image) urls.add(basic.cover_image);
  if (basic.thumb) urls.add(basic.thumb);
  if (basic.images && Array.isArray(basic.images)) basic.images.forEach(img=> urls.add(img.resource_url));
  return Array.from(urls);
}

async function findProductBySkuOrMetafield(sku, instanceId) {
  // NOTE: In a production app you would use GraphQL to search variants by SKU.
  // Here we list recent products and check their variants.
  const bySku = await shopifyRest(`/products.json?limit=50&presentment_currencies=GBP&fields=id,title,handle,variants`);
  for (const p of bySku.products) {
    if (p.variants?.some(v=> v.sku === sku)) return p;
  }
  return null;
}

async function ensureProductFromDiscogsItem(item) {
  const basic = item.basic_information;
  const instanceId = item.instance_id?.toString();
  const sku = `DCG-${instanceId}`;
  const title = buildTitle(basic.artists, basic.title);
  const handle = slugify(`${title}-${instanceId}`);
  const body_html = [
    basic.formats?.map(f=>f.name).filter(Boolean).join(', '),
    basic.labels?.[0]?.name,
    basic.year ? `Year: ${basic.year}` : null,
  ].filter(Boolean).join(' • ');

  const mediaGrade = item.media_condition || null;
  const sleeveGrade = item.sleeve_condition || null;
  const gradeTag = mapGradeTag(mediaGrade);
  const sleeveTag = sleeveGrade ? `sleeve:${sleeveGrade.replace(/\s+/g,'')}` : null;

  const tags = [gradeTag, sleeveTag, 'source:discogs'].filter(Boolean).join(', ');

  const images = collectImageUrls(basic).slice(0, 8).map(u=> ({src:u}));

  // Try to find existing product
  let existing = await findProductBySkuOrMetafield(sku, instanceId);

  if (!existing) {
    // Create product
    const createPayload = {
      product: {
        title,
        body_html,
        handle,
        status: DEFAULT_STATUS,
        tags,
        vendor: basic.labels?.[0]?.name || 'Unknown Label',
        product_type: 'Vinyl',
        variants: [{
          price: '0.00', // set later or via pricing logic
          sku,
          inventory_management: 'shopify',
          inventory_policy: 'deny',
          barcode: basic.barcode || undefined,
        }],
        images,
      }
    };

    const created = await shopifyRest('/products.json','POST',createPayload);
    existing = created.product;

    // Inventory: set to 1
    await setInventory(existing.variants[0].inventory_item_id, 1);

  } else {
    // Update tags/images minimally
    await shopifyRest(`/products/${existing.id}.json`, 'PUT', { product: { id: existing.id, tags } });
  }

  // Update metafields (upsert)
  await upsertProductMetafields(existing.id, {
    'discogs.release_id': basic.id?.toString(),
    'discogs.master_id': (basic.master_id || '').toString(),
    'discogs.media_condition': mediaGrade || '',
    'discogs.sleeve_condition': sleeveGrade || '',
    'discogs.notes': item.notes || '',
    'discogs.catalog_number': basic.catno || '',
    'discogs.label': basic.labels?.[0]?.name || '',
    'discogs.format': (basic.formats||[]).map(f=> [f.name, ...(f.descriptions||[])] .filter(Boolean).join(' ')).join(', '),
    'discogs.year': basic.year?.toString() || '',
    'discogs.barcode': (basic.barcode||'').toString(),
    'discogs.instance_id': instanceId,
  });

  // Ensure in manual collection if requested
  if (COLLECTION_HANDLE) {
    try { await addToCollectionByHandle(existing.id, COLLECTION_HANDLE); } catch (e) { /* non-fatal */ }
  }

  return existing;
}

async function setInventory(inventoryItemId, quantity) {
  const payload = { inventory_item_id: inventoryItemId, location_id: LOCATION_ID.split('/').pop(), available: quantity };
  await shopifyRest('/inventory_levels/set.json', 'POST', payload);
}

async function upsertProductMetafields(productId, kv) {
  const existing = await shopifyRest(`/products/${productId}/metafields.json`);
  const index = new Map(existing.metafields.map(m=> [`${m.namespace}.${m.key}`, m]));
  for (const [full, value] of Object.entries(kv)) {
    const [namespace, key] = full.split('.');
    const current = index.get(full);
    const body = { metafield: { namespace, key, type: 'single_line_text_field', value, } };
    if (key === 'notes') body.metafield.type = 'multi_line_text_field';
    if (current) {
      await shopifyRest(`/metafields/${current.id}.json`, 'PUT', { metafield: { id: current.id, value } });
    } else {
      await shopifyRest(`/products/${productId}/metafields.json`, 'POST', body);
    }
    await sleep(200);
  }
}

async function addToCollectionByHandle(productId, handle) {
  const collections = await shopifyRest(`/custom_collections.json?handle=${handle}`);
  const coll = collections.custom_collections?.[0];
  if (!coll) return;
  await shopifyRest('/collects.json', 'POST', { collect: { product_id: productId, collection_id: coll.id } });
}

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
        console.error('Failed item', item?.instance_id, e.message);
      }
      await sleep(500);
    }
    if (!data.pagination || page >= data.pagination.pages) break;
    page++;
  }
}

(async function main(){
  console.log('Starting Discogs → Shopify sync…');
  for (const f of FOLDERS) {
    console.log('Syncing Discogs folder', f);
    await syncFolder(f);
  }
  console.log('Sync complete.');
})();