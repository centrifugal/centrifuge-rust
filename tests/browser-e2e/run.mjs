// Browser behavior wasm-bindgen-test can't drive (docs/wasm-plan.md, step 9):
// the back/forward cache and worker.terminate(), checked from outside, through
// Centrifugo's presence API. Needs `docker compose up`, Chrome, and the app
// built into www/pkg (see README.md here).
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright-core';

const www = path.join(path.dirname(fileURLToPath(import.meta.url)), 'www');
const types = { '.html': 'text/html', '.js': 'text/javascript', '.wasm': 'application/wasm' };

const server = http.createServer((req, res) => {
  const file = path.join(www, new URL(req.url, 'http://x').pathname);
  if (!file.startsWith(www) || !fs.existsSync(file) || fs.statSync(file).isDirectory()) {
    res.writeHead(404).end();
    return;
  }
  res.writeHead(200, { 'Content-Type': types[path.extname(file)] ?? 'application/octet-stream' });
  fs.createReadStream(file).pipe(res);
});
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const base = `http://127.0.0.1:${server.address().port}`;

async function presence(channel) {
  const r = await fetch('http://localhost:8010/api/presence', {
    method: 'POST',
    headers: { 'X-API-Key': 'test-api-key', 'Content-Type': 'application/json' },
    body: JSON.stringify({ channel }),
  });
  const j = await r.json();
  return Object.keys(j.result?.presence ?? {}).length;
}

// Polls until `want` clients are present, within `ms`; returns how long it took.
async function waitPresence(channel, want, ms) {
  const t0 = Date.now();
  while (Date.now() - t0 < ms) {
    if ((await presence(channel)) === want) return Date.now() - t0;
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(`${channel}: not ${want} present within ${ms} ms (now ${await presence(channel)})`);
}

// Never hang: a stuck browser call fails the run instead.
const watchdog = setTimeout(() => {
  console.log('FAIL the run took over 120 s');
  process.exit(1);
}, 120_000);

const browser = await chromium.launch({
  channel: 'chrome',
  headless: true,
  // Playwright turns the back/forward cache off by default.
  ignoreDefaultArgs: ['--disable-back-forward-cache'],
});
let failed = false;
const within = (ms, what, p) =>
  Promise.race([p, new Promise((_, rej) => setTimeout(() => rej(new Error(`${what}: over ${ms} ms`)), ms))]);

async function test(name, f) {
  const page = await browser.newPage();
  page.setDefaultTimeout(10_000);
  page.on('pageerror', (e) => console.log(`  page error: ${e}`));
  try {
    await page.goto(`${base}/index.html`);
    await page.waitForFunction(() => window.ready);
    await within(30_000, name, f(page, `e2e_${name}_${Date.now()}`));
    console.log(`ok   ${name}`);
  } catch (e) {
    failed = true;
    console.log(`FAIL ${name}: ${e.message}`);
  } finally {
    await within(5_000, 'closing the page', page.close()).catch((e) => console.log(`  ${e.message}`));
  }
}

await test('bfcache', async (page, ch) => {
  await page.evaluate((c) => window.startPage(c), ch);
  await waitPresence(ch, 1, 5000);
  // Into the cache: the client closes its transport, so the server sees it
  // go at once, not after its ping timeout (about 25 s).
  await page.goto(`${base}/other.html`);
  console.log('     navigated away');
  const gone = await waitPresence(ch, 0, 2000);
  // A page restored from the cache fires no `load`: wait for the commit.
  await page.goBack({ waitUntil: 'commit' });
  await page.waitForFunction(() => window.restored, null, { timeout: 5000 }).catch(() => {});
  if (!(await page.evaluate(() => window.restored))) {
    throw new Error('the page was not restored from the back/forward cache');
  }
  const back = await waitPresence(ch, 1, 5000);
  console.log(`     left ${gone} ms after navigating away, back ${back} ms after returning`);
});

await test('worker_terminate', async (page, ch) => {
  await page.evaluate((c) => window.startWorker(c), ch);
  await waitPresence(ch, 1, 5000);
  // No Rust code runs on terminate(): the browser closes the worker's socket.
  await page.evaluate(() => window.worker.terminate());
  const gone = await waitPresence(ch, 0, 10000);
  console.log(`     left ${gone} ms after terminate()`);
});

await within(10_000, 'closing the browser', browser.close()).catch((e) => console.log(`  ${e.message}`));
server.close();
clearTimeout(watchdog);
process.exit(failed ? 1 : 0);
