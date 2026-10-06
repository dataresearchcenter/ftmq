import assert from "node:assert/strict";
import { test } from "node:test";

import Api from "../api/index.js";
import { Query } from "../query/index.js";

// a fake `/entities` endpoint serving the requested page of `total` entities
function fakeEntities(total: number): typeof fetch {
  return (async (input: string | URL | Request) => {
    const url = new URL(String(input));
    const offset = Number(url.searchParams.get("offset") ?? 0);
    const limit = Number(url.searchParams.get("limit"));
    const size = Math.max(0, Math.min(limit, total - offset));
    const results = Array.from({ length: size }, (_, i) => ({
      id: `e${offset + i}`,
      schema: "Thing",
      properties: {},
    }));
    const next = offset + size < total ? "next" : null;
    return new Response(JSON.stringify({ results, next }));
  }) as typeof fetch;
}

test("getEntitiesAll advances by the results returned", async () => {
  const realFetch = globalThis.fetch;
  globalThis.fetch = fakeEntities(250);
  try {
    // unauthenticated, the requested 500 is clamped to a page of 100
    const api = new Api("http://api.test");
    const entities = await api.getEntitiesAll(new Query().slice(0, 500));
    assert.equal(entities.length, 250);
    assert.equal(new Set(entities.map((e) => e.id)).size, 250);
  } finally {
    globalThis.fetch = realFetch;
  }
});
