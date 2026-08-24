import {
  Agg,
  aggregationsFromDict,
  aggregationsToDict,
  uniqueAggs,
  type ANode,
} from "./aggregations.js";
import {
  aggregationsToParams,
  exprToParams,
  type Params,
  paramsToAggregations,
  paramsToExpr,
  paramsToSelection,
  paramsToString,
  selectionToParams,
  stringToParams,
} from "./aleph.js";
import { QueryError } from "./exceptions.js";
import { AND, combine, Expr } from "./nodes.js";
import { refFromWire, type Ref } from "./refs.js";
import { parseRqlQuery, toRql } from "./rql.js";
import { byString } from "./util.js";

interface Slice {
  start: number;
  stop: number | null;
}

function makeSlice(limit: number | null, offset: number | null): Slice | null {
  if (limit === null && !offset) return null;
  const start = offset || 0;
  return { start, stop: limit !== null ? start + limit : null };
}

/** An ordering over a single entity property (mirroring the Python `Sort`). */
export class Sort {
  readonly value: string;
  readonly ascending: boolean;

  constructor(value: string, ascending = true) {
    this.value = value;
    this.ascending = ascending;
  }

  serialize(): string {
    return this.ascending ? this.value : `-${this.value}`;
  }

  static deserialize(value: string): Sort {
    const ascending = !value.startsWith("-");
    return new Sort(ascending ? value : value.slice(1), ascending);
  }
}

export type ParamsInput = URLSearchParams | Record<string, string | string[]>;

function normalizeParams(args: ParamsInput): Params {
  const items: Params = {};
  if (
    typeof URLSearchParams !== "undefined" &&
    args instanceof URLSearchParams
  ) {
    for (const key of new Set(args.keys())) items[key] = args.getAll(key);
  } else {
    for (const [key, value] of Object.entries(args)) {
      items[key] = Array.isArray(value) ? value.map(String) : [String(value)];
    }
  }
  return items;
}

interface QueryInit {
  q?: Expr | null;
  aggregations?: Agg[];
  sort?: Sort | null;
  slice?: Slice | null;
  selection?: Ref[];
}

/** Dedupe and order refs by their wire spelling, as the Python side does. */
function uniqueRefs(refs: Ref[]): Ref[] {
  const seen = new Map<string, Ref>();
  for (const ref of refs) seen.set(ref.wire, ref);
  return [...seen.values()].sort((a, b) => byString(a.wire, b.wire));
}

/** A filter over FtM entities, mirroring the Python `ftmq.Query` serialization. */
export class Query {
  q: Expr | null;
  aggregations: Agg[];
  sort: Sort | null;
  sliceRange: Slice | null;
  selection: Ref[];

  constructor(init: QueryInit = {}) {
    this.q = init.q ?? null;
    this.aggregations = uniqueAggs(init.aggregations ?? []);
    this.sort = init.sort ?? null;
    this.sliceRange = init.slice ?? null;
    this.selection = uniqueRefs(init.selection ?? []);
  }

  private chain(patch: QueryInit): Query {
    return new Query({
      q: patch.q !== undefined ? patch.q : this.q,
      aggregations:
        patch.aggregations !== undefined
          ? patch.aggregations
          : this.aggregations,
      sort: patch.sort !== undefined ? patch.sort : this.sort,
      slice: patch.slice !== undefined ? patch.slice : this.sliceRange,
      selection:
        patch.selection !== undefined ? patch.selection : this.selection,
    });
  }

  // --- building ------------------------------------------------------------

  /** AND another set of `M` / `P` / `G` / `C` nodes into the query. */
  where(...nodes: Expr[]): Query {
    const next = combine(nodes, AND);
    if (next === null) return this.chain({});
    const q = this.q === null ? next : this.q.and(next);
    return this.chain({ q });
  }

  /** Order by a single field; a leading `-` marks descending. */
  orderBy(value: string): Query {
    return this.chain({ sort: Sort.deserialize(value) });
  }

  /** Slice the result set (`q.slice(offset, offset + limit)`). */
  slice(start = 0, stop: number | null = null): Query {
    return this.chain({ slice: { start, stop } });
  }

  /** Add aggregation projections to the query. */
  aggregate(...nodes: ANode[]): Query {
    const aggs = [...this.aggregations];
    for (const node of nodes) aggs.push(...node.aggs);
    return this.chain({ aggregations: uniqueAggs(aggs) });
  }

  /**
   * Restrict the properties the matching entities are read with.
   *
   * A projection, not a filter: it never changes *which* entities match, only
   * which of their properties come back. Takes `P` / `G` refs; the server
   * rejects anything else (as everywhere in this package, field validation is
   * server-side).
   */
  select(...refs: Ref[]): Query {
    return this.chain({ selection: uniqueRefs([...this.selection, ...refs]) });
  }

  // --- slice accessors -----------------------------------------------------

  get limit(): number | null {
    if (this.sliceRange === null) return null;
    const { start, stop } = this.sliceRange;
    if (start && stop) return stop - start;
    return stop === null ? null : stop;
  }

  get offset(): number | null {
    if (this.sliceRange === null) return null;
    return this.sliceRange.start || 0;
  }

  // --- leaf collectors -----------------------------------------------------

  private leafValues(
    predicate: (leaf: { family: string; field: string }) => boolean,
  ): Set<string> {
    const names = new Set<string>();
    if (this.q) {
      for (const leaf of this.q.iterLeaves()) {
        if (predicate(leaf)) {
          const value = leaf.value;
          if (Array.isArray(value)) value.forEach((v) => names.add(v));
          else if (typeof value === "string") names.add(value);
        }
      }
    }
    return names;
  }

  get datasets(): Set<string> {
    return this.leafValues((l) => l.family === "M" && l.field === "dataset");
  }

  get schemata(): Set<string> {
    return this.leafValues(
      (l) =>
        l.family === "M" && (l.field === "schema" || l.field === "schemata"),
    );
  }

  get countries(): Set<string> {
    return this.leafValues((l) => l.family === "G" && l.field === "countries");
  }

  // --- serialization -------------------------------------------------------

  toDict(): Record<string, any> {
    const data: Record<string, any> = {};
    if (this.q && !this.q.isEmpty) data.q = this.q.toDict();
    if (this.sort) data.order_by = this.sort.serialize();
    if (this.sliceRange) {
      data.limit = this.limit;
      data.offset = this.offset;
    }
    if (this.aggregations.length) {
      data.aggregations = aggregationsToDict(this.aggregations);
    }
    if (this.selection.length) {
      data.select = this.selection.map((ref) => ref.wire);
    }
    return data;
  }

  static fromDict(data: Record<string, any>): Query {
    const q = data.q ? Expr.fromDict(data.q) : null;
    let sort: Sort | null = null;
    if (data.order_by) {
      sort = Sort.deserialize(String(data.order_by));
    }
    const slice = makeSlice(data.limit ?? null, data.offset ?? null);
    const aggregations = data.aggregations
      ? aggregationsFromDict(data.aggregations)
      : [];
    const selection = (data.select ?? []).map((f: string) =>
      refFromWire(String(f)),
    );
    return new Query({ q, sort, slice, aggregations, selection });
  }

  toParams(): Params {
    const params: Params = { ...exprToParams(this.q) };
    if (this.aggregations.length) {
      Object.assign(params, aggregationsToParams(this.aggregations));
    }
    Object.assign(params, selectionToParams(this.selection));
    if (this.sort) {
      const direction = this.sort.ascending ? "asc" : "desc";
      params.sort = [`${this.sort.value}:${direction}`];
    }
    if (this.sliceRange) {
      if (this.offset) params.offset = [String(this.offset)];
      if (this.limit !== null) params.limit = [String(this.limit)];
    }
    return params;
  }

  static fromParams(args: ParamsInput): Query {
    const items = normalizeParams(args);
    const q = paramsToExpr(items);
    const aggs = paramsToAggregations(items);
    let sort: Sort | null = null;
    if (items.sort) {
      if (items.sort.length > 1) {
        throw new QueryError("Multi-field sort is not supported");
      }
      const value = items.sort[0];
      const idx = value.indexOf(":");
      const field = idx < 0 ? value : value.slice(0, idx);
      const direction = idx < 0 ? "" : value.slice(idx + 1);
      sort = new Sort(field, direction !== "desc");
    }
    let slice: Slice | null = null;
    if ("limit" in items || "offset" in items) {
      const offset = parseInt((items.offset ?? ["0"])[0] || "0", 10) || 0;
      const limit = items.limit ? parseInt(items.limit[0], 10) : null;
      slice = makeSlice(limit, offset);
    }
    return new Query({
      q,
      sort,
      slice,
      aggregations: aggs,
      selection: paramsToSelection(items),
    });
  }

  toString(): string {
    return paramsToString(this.toParams());
  }

  static fromString(value: string): Query {
    const s = value.startsWith("?") ? value.slice(1) : value;
    return Query.fromParams(stringToParams(s));
  }

  toRql(): string {
    return toRql(this.q, this.aggregations, this.selection);
  }

  static fromRql(value: string): Query {
    const [q, aggregations, selection] = parseRqlQuery(value);
    return new Query({ q, aggregations, selection });
  }

  /**
   * URL params for an api request: the flat Aleph grammar when the filter is
   * flat-expressible, otherwise an `rql=` filter tree. `sort` / `limit` /
   * `offset` and aggregation params are always appended.
   */
  toRequestParams(): URLSearchParams {
    let params: Params;
    try {
      params = { ...exprToParams(this.q) };
    } catch (error) {
      if (!(error instanceof QueryError)) throw error;
      params = {};
      if (this.q && !this.q.isEmpty) params.rql = [toRql(this.q, [])];
    }
    if (this.aggregations.length) {
      Object.assign(params, aggregationsToParams(this.aggregations));
    }
    Object.assign(params, selectionToParams(this.selection));
    if (this.sort) {
      const direction = this.sort.ascending ? "asc" : "desc";
      params.sort = [`${this.sort.value}:${direction}`];
    }
    if (this.sliceRange) {
      if (this.offset) params.offset = [String(this.offset)];
      if (this.limit !== null) params.limit = [String(this.limit)];
    }
    const usp = new URLSearchParams();
    for (const key of Object.keys(params).sort(byString)) {
      for (const value of params[key]) usp.append(key, value);
    }
    return usp;
  }
}
