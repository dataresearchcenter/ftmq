import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";

import { A, G, M, not, P, Query, QueryError } from "../query/index.js";

interface Case {
  name: string;
  dict: Record<string, any>;
  params: Record<string, string[]> | null;
  string: string | null;
  rql: string | null;
  params_dict: Record<string, any> | null;
  rql_dict: Record<string, any> | null;
}

const cases: Case[] = JSON.parse(
  readFileSync("js/tests/fixtures/query_cases.json", "utf-8"),
);

// key-sorted JSON, for order-independent structural comparison
const canon = (value: unknown): string =>
  JSON.stringify(value, (_key, v) =>
    v && typeof v === "object" && !Array.isArray(v)
      ? Object.fromEntries(
          Object.keys(v as Record<string, unknown>)
            .sort()
            .map((k) => [k, (v as Record<string, unknown>)[k]]),
        )
      : v,
  );

// recursively sort `and`/`or` child arrays so tree ordering does not matter
// (TS orders by JSON, Python by banal.hash_data)
function norm(value: any): any {
  if (Array.isArray(value)) return value.map(norm);
  if (value && typeof value === "object") {
    const out: Record<string, any> = {};
    for (const key of Object.keys(value)) {
      let v = norm(value[key]);
      if ((key === "and" || key === "or") && Array.isArray(v)) {
        v = [...v].sort((a, b) => (canon(a) < canon(b) ? -1 : canon(a) > canon(b) ? 1 : 0));
      }
      out[key] = v;
    }
    return out;
  }
  return value;
}

// --- cross-language parity: every Python fixture round-trips through the TS Query
for (const c of cases) {
  test(`parity: ${c.name}`, () => {
    const fromDict = Query.fromDict(c.dict);

    // dict round-trips losslessly (order-independent)
    assert.deepEqual(norm(fromDict.toDict()), norm(c.dict), "dict");

    if (c.params !== null) {
      // serialize byte-parity (sorted keys / values)
      assert.deepEqual(fromDict.toParams(), c.params, "toParams");
      assert.equal(fromDict.toString(), c.string, "toString");
      // parse parity against Python's re-parsed dict (params is lossy for
      // per-metric aggregation grouping, identically in both languages)
      assert.deepEqual(
        norm(Query.fromParams(c.params).toDict()),
        norm(c.params_dict),
        "fromParams",
      );
      assert.deepEqual(
        norm(Query.fromString(c.string as string).toDict()),
        norm(c.params_dict),
        "fromString",
      );
    }

    if (c.rql !== null) {
      const expected = norm(c.rql_dict);
      // rql child order follows tree order (not canonicalized), so parity is
      // functional: our rql output and Python's both parse to the same tree.
      assert.deepEqual(
        norm(Query.fromRql(fromDict.toRql()).toDict()),
        expected,
        "toRql roundtrip",
      );
      assert.deepEqual(norm(Query.fromRql(c.rql).toDict()), expected, "fromRql");
    }
  });
}

// --- TS-internal behaviour
test("builder composes a nested tree", () => {
  const q = new Query()
    .where(M({ schema: "Person" }))
    .where(G({ countries: "de" }).or(G({ countries: "at" })))
    .where(not(P({ name__ilike: "jane" })))
    .orderBy("-name")
    .slice(0, 25);
  assert.equal(q.limit, 25);
  assert.equal(q.offset, 0);
  assert.deepEqual([...q.schemata], ["Person"]);
  assert.deepEqual([...q.countries].sort(), ["at", "de"]);
  // nested OR is not flat-expressible as Aleph params
  assert.throws(() => q.toParams(), QueryError);
});

test("toRequestParams falls back to rql for a nested tree", () => {
  const q = new Query().where(G({ countries: "de" }).or(G({ countries: "at" })));
  const params = q.toRequestParams();
  assert.equal(
    params.get("rql"),
    "or(eq(countries,de),eq(countries,at))",
  );
});

test("toRequestParams uses flat aleph params for a flat tree", () => {
  const q = new Query()
    .where(M({ schema: "Person" }), P({ name__ilike: "jane" }))
    .slice(0, 10);
  const params = q.toRequestParams();
  assert.equal(params.get("filter:schema"), "Person");
  assert.equal(params.get("filter:ilike:properties.name"), "jane");
  assert.equal(params.get("limit"), "10");
  assert.equal(params.get("rql"), null);
});

test("aggregate node builds specs and round-trips via dict", () => {
  const q = new Query()
    .where(M({ schema: "Payment" }))
    .aggregate(A({ sum: "amountEur", by: "beneficiary" }), A({ count: "id" }));
  assert.deepEqual(norm(Query.fromDict(q.toDict()).toDict()), norm(q.toDict()));
  assert.equal(q.aggregations.length, 2);
});

test("invalid comparator throws QueryError", () => {
  assert.throws(() => M({ schema__bogus: "x" }), QueryError);
});
