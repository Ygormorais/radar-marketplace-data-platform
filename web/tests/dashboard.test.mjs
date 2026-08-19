import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const root = new URL("../", import.meta.url);

async function render() {
  const workerUrl = new URL("../dist/server/index.js", import.meta.url);
  workerUrl.searchParams.set("test", `${process.pid}-${Date.now()}`);
  const { default: worker } = await import(workerUrl.href);
  return worker.fetch(
    new Request("http://localhost/", { headers: { accept: "text/html" } }),
    { ASSETS: { fetch: async () => new Response("Not found", { status: 404 }) } },
    { waitUntil() {}, passThroughOnException() {} },
  );
}

test("renderiza a identidade do Radar sem metadados do starter", async () => {
  const response = await render();
  assert.equal(response.status, 200);
  assert.match(response.headers.get("content-type") ?? "", /^text\/html\b/i);
  const html = await response.text();
  assert.match(html, /<title>Radar — Marketplace Data Platform<\/title>/i);
  assert.match(html, /Preparando indicadores/);
  assert.doesNotMatch(html, /codex-preview|Your site is taking shape/i);
});

test("snapshot público respeita o contrato analítico", async () => {
  const raw = await readFile(new URL("public/data/dashboard.json", root), "utf8");
  const data = JSON.parse(raw);
  assert.equal(data.metadata.mode, "demo");
  assert.deepEqual(data.periods.map((item) => item.year), [2018, 2017]);
  assert.ok(data.metadata.sourceModels.length >= 4);
  for (const period of data.periods) {
    assert.equal(period.monthly.length, 12);
    assert.ok(period.regions.length >= 5);
    for (const channel of period.channels) {
      assert.ok(channel.sessions >= channel.views);
      assert.ok(channel.views >= channel.carts);
      assert.ok(channel.carts >= channel.checkouts);
      assert.ok(channel.checkouts >= channel.purchases);
    }
  }
});
