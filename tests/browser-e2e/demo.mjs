// Checks the browser example (examples/browser) the way a user would: two
// tabs on http://localhost:8080, a message typed in one shows up in both.
// Run after `docker compose -f examples/browser/compose.yml up -d`.
import { chromium } from 'playwright-core';

const url = process.env.DEMO_URL ?? 'http://localhost:8080';
const watchdog = setTimeout(() => {
  console.log('FAIL the demo check took over 60 s');
  process.exit(1);
}, 60_000);

const browser = await chromium.launch({ channel: 'chrome', headless: true });
try {
  const [a, b] = [await browser.newPage(), await browser.newPage()];
  for (const p of [a, b]) {
    p.on('pageerror', (e) => console.log(`  page error: ${e}`));
    await p.goto(url);
    await p.waitForFunction(() => document.getElementById('status').textContent === 'connected', null, {
      timeout: 15_000,
    });
  }
  const text = `hello ${Date.now()}`;
  await a.fill('#text', text);
  await a.press('#text', 'Enter');
  for (const p of [a, b]) {
    await p.waitForFunction((t) => document.getElementById('messages').textContent.includes(t), text, {
      timeout: 10_000,
    });
  }
  console.log('ok   the message typed in one tab showed up in both');
} catch (e) {
  console.log(`FAIL ${e.message}`);
  process.exitCode = 1;
} finally {
  await browser.close();
  clearTimeout(watchdog);
}
