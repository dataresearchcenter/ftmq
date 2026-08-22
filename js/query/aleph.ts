import { Agg } from "./aggregations.js";
import { QueryError } from "./exceptions.js";
import { type Family, type Leaf } from "./leaves.js";
import { AND, combine, Expr, FAMILIES } from "./nodes.js";
import { refFromWire, type Ref } from "./refs.js";
import { byString } from "./util.js";

// Aleph meta filter keys -> ftmq meta field (some upstream keys are aliased)
const ALEPH_META: Record<string, string> = {
  id: "id",
  _id: "id",
  entity_id: "entity_id",
  canonical_id: "canonical_id",
  dataset: "dataset",
  datasets: "dataset",
  collection_id: "dataset",
  collections: "dataset",
  schema: "schema",
  schemata: "schemata",
};
// comparators expressible as a `filter:<op>:<field>` prefix
const PREFIX_OPS = [
  "gte",
  "gt",
  "lte",
  "lt",
  "like",
  "ilike",
  "startswith",
  "endswith",
];

export type Params = Record<string, string[]>;

const sortedStrings = (value: unknown): string[] =>
  (value as string[]).map((v) => String(v)).sort(byString);

/**
 * Map a filter field to a `(family, ftmq-key)` pair. Field names are the same on
 * both halves of the grammar, so this is `refFromWire` plus the upstream key
 * aliases and `schemata`, which is an is-a predicate rather than a field a
 * projection could read. Field validity is not checked here (server-side).
 */
export function resolveField(rest: string): [Family, string] {
  if (rest in ALEPH_META) return ["M", ALEPH_META[rest]];
  const ref = refFromWire(rest);
  return [ref.family as Family, ref.key];
}

function collectTerms(expr: Expr): [Leaf, boolean][] {
  if (expr.isEmpty) return [];
  if (expr.negated) {
    const leaves = [...expr.iterLeaves()];
    if (leaves.length === 1) return [[leaves[0], true]];
    throw new QueryError("Negated group is not expressible as Aleph params");
  }
  if (expr.connector === "OR") {
    throw new QueryError("OR queries are not expressible as Aleph params");
  }
  const terms: [Leaf, boolean][] = [];
  for (const child of expr.children) {
    if (child instanceof Expr) terms.push(...collectTerms(child));
    else terms.push([child, false]);
  }
  return terms;
}

function leafToParam(
  leaf: Leaf,
  inverted: boolean,
): [string, string, string[]] {
  const op = leaf.comparator;
  const field = leaf.wire;
  const value = leaf.value;
  if (inverted) {
    if (op === "eq") return ["exclude:", field, [String(value)]];
    if (op === "in") return ["exclude:", field, sortedStrings(value)];
    throw new QueryError(`Cannot invert comparator \`${op}\` for Aleph params`);
  }
  if (op === "eq") return ["filter:", field, [String(value)]];
  if (op === "in") return ["filter:", field, sortedStrings(value)];
  if (PREFIX_OPS.includes(op))
    return ["filter:", `${op}:${field}`, [String(value)]];
  if (op === "not") return ["exclude:", field, [String(value)]];
  if (op === "not_in") return ["exclude:", field, sortedStrings(value)];
  if (op === "null") {
    if (value) return ["empty:", field, [""]];
    throw new QueryError("null=False is not expressible as Aleph params");
  }
  throw new QueryError(
    `Comparator \`${op}\` is not expressible as Aleph params`,
  );
}

/** Project a filter tree to Aleph `filter:` / `exclude:` / `empty:` params. */
export function exprToParams(expr: Expr | null): Params {
  const params: Params = {};
  if (expr && !expr.isEmpty) {
    for (const [leaf, inverted] of collectTerms(expr)) {
      const [prefix, key, values] = leafToParam(leaf, inverted);
      (params[`${prefix}${key}`] ??= []).push(...values);
    }
  }
  return params;
}

function paramToNode(prefix: string, restIn: string, values: string[]): Expr {
  let op: string | null = null;
  let rest = restIn;
  for (const candidate of PREFIX_OPS) {
    if (rest.startsWith(`${candidate}:`)) {
      op = candidate;
      rest = rest.slice(candidate.length + 1);
      break;
    }
  }
  const [family, field] = resolveField(rest);
  let key: string;
  let value: unknown;
  if (prefix === "empty:") {
    key = `${field}__null`;
    value = true;
  } else if (prefix === "exclude:") {
    if (values.length > 1) {
      key = `${field}__not_in`;
      value = values;
    } else {
      key = `${field}__not`;
      value = values[0];
    }
  } else if (op !== null) {
    key = `${field}__${op}`;
    value = values[0];
  } else if (values.length > 1) {
    key = `${field}__in`;
    value = values;
  } else {
    key = field;
    value = values[0];
  }
  return FAMILIES[family]({ [key]: value });
}

/** Build a filter tree from Aleph params (non-filter keys are ignored). */
export function paramsToExpr(items: Params): Expr | null {
  const nodes: Expr[] = [];
  for (const [key, values] of Object.entries(items)) {
    for (const prefix of ["filter:", "exclude:", "empty:"]) {
      if (key.startsWith(prefix)) {
        nodes.push(paramToNode(prefix, key.slice(prefix.length), values));
        break;
      }
    }
  }
  return nodes.length ? combine(nodes, AND) : null;
}

/** Project aggregation specs to openaleph `metric:<func>` / `facet` params. */
export function aggregationsToParams(aggs: Agg[]): Params {
  const params: Params = {};
  const facets = new Set<string>();
  const sorted = [...aggs].sort(
    (a, b) => byString(a.func, b.func) || byString(a.field, b.field),
  );
  for (const agg of sorted) {
    const key = `metric:${agg.func}`;
    params[key] ??= [];
    if (!params[key].includes(agg.field)) params[key].push(agg.field);
    for (const group of agg.groups) facets.add(group.wire);
  }
  if (facets.size) params.facet = [...facets].sort(byString);
  return params;
}

/**
 * Rebuild aggregation specs from `metric:` / `facet` params.
 *
 * A `facet` with no `metric:` alongside it groups an entity count - the
 * idiomatic Aleph facet. Dropping it instead would discard the param in
 * silence and answer with empty facets.
 */
export function paramsToAggregations(items: Params): Agg[] {
  const groups: Ref[] = [...new Set(items.facet ?? [])]
    .sort(byString)
    .map(refFromWire);
  const aggs: Agg[] = [];
  for (const [key, values] of Object.entries(items)) {
    if (key.startsWith("metric:")) {
      const func = key.slice("metric:".length);
      for (const field of values) {
        aggs.push(new Agg(func as any, refFromWire(field), groups));
      }
    }
  }
  if (groups.length && !aggs.length) {
    aggs.push(new Agg("count", refFromWire("id"), groups));
  }
  return aggs;
}

/**
 * Project a `Query.select` projection to `select=` params. Fields are spelled
 * exactly as the filter keys are (`properties.<name>`, `group.<name>`).
 */
export function selectionToParams(refs: Ref[]): Params {
  const fields = refs.map((ref) => ref.wire).sort(byString);
  return fields.length ? { select: fields } : {};
}

/** Rebuild a projection from `select=` params. */
export function paramsToSelection(items: Params): Ref[] {
  const fields = [...new Set(items.select ?? [])].sort(byString);
  return fields.map(refFromWire);
}

// match Python `urllib.parse.quote(value, safe="/")`
function quote(value: string): string {
  return encodeURIComponent(value)
    .replace(
      /[!'()*]/g,
      (c) => "%" + c.charCodeAt(0).toString(16).toUpperCase(),
    )
    .replace(/%2F/gi, "/");
}

/** Render an Aleph param mapping as a URL query string (sorted keys). */
export function paramsToString(params: Params): string {
  const parts: string[] = [];
  for (const key of Object.keys(params).sort(byString)) {
    for (const value of params[key]) parts.push(`${key}=${quote(value)}`);
  }
  return parts.join("&");
}

/** Parse an Aleph URL query string into a param mapping. */
export function stringToParams(value: string): Params {
  const items: Params = {};
  for (const part of value.split("&")) {
    if (!part) continue;
    const eq = part.indexOf("=");
    const key = eq < 0 ? part : part.slice(0, eq);
    const val = eq < 0 ? "" : decodeURIComponent(part.slice(eq + 1));
    (items[key] ??= []).push(val);
  }
  return items;
}
