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
    met
