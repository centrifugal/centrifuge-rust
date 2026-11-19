# Browser e2e

Browser behavior `wasm-bindgen-test` can't drive: the back/forward cache and
`worker.terminate()`, checked through Centrifugo's presence API, a hanging
emulation POST, and redirects (a 307 to another origin must not be followed),
checked by the test's own server.

```sh
docker compose up -d --wait           # from the repository root
cargo build --target wasm32-unknown-unknown
wasm-bindgen --target web --out-dir www/pkg target/wasm32-unknown-unknown/debug/browser_e2e.wasm
npm ci && node run.mjs                 # uses the installed Chrome
```
