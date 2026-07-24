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
  paramsToString,
  stringToParams,
} from "./aleph.js";
import { QueryError } from "./exceptions.js";
import { AND, combine, Expr } from "./nodes.js";
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

/** An ordering over one or more entity properties. */
export class Sort {
  readonly values: string[];
  readonly ascending: boolean;

  constructor(values: string[], ascending = true) {
    this.values = values;
    this.ascending = ascending;
  }

  serialize(): string[] {
    return this.ascending ? [...this.values] : this.values.map((v) => `-${v}`);
  }
}

export type ParamsInput = URLSearchParams | Record<string, string | string[]>;

function normalizeParams(args: ParamsInput): Params {
  const items: Params = {};
  if (typeof URLSearchParams !== "undefined" && args instanceof URLSearchParams) {
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
}

/** A filter over FtM entities, mirroring the Python `ftmq.Query` serialization. */
export class Query {
  q: Expr | null;
  aggregations: Agg[];
  sort: Sort | null;
  sliceRange: Slice | null;

  constructor(init: QueryInit = {}) {
    this.q = init.q ?? null;
    this.aggregations = uniqueAggs(init.aggregations ?? []);
    this.sort = init.sort ?? null;
    this.sliceRange = init.slice ?? null;
  }

  private chain(patch: QueryInit): Query {
    return new Query({
      q: patch.q !== undefined ? patch.q : this.q,
      aggregations:
        patch.aggregations !== undefined ? patch.aggregations : this.aggregations,
      sort: patch.sort !== undefined ? patch.sort : this.sort,
      slice: patch.slice !== undefined ? patch.slice : this.sliceRange,
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

  /** Order by one or more fields; a leading `-` marks descending. */
  orderBy(...values: string[]): Query {
    if (values.length === 0) return this.chain({ sort: null });
    const ascending = !values[0].startsWith("-");
    const sort = new Sort(
      values.map((v) => (v.startsWith("-") ? v.slice(1) : v)),
      ascending,
    );
    return this.chain({ sort });
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

  private leafValues(predicate: (leaf: { family: string; field: string }) => boolean): Set<string> {
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
      (l) => l.family === "M" && (l.field === "schema" || l.field === "schemata"),
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
    return data;
  }

  static fromDict(data: Record<string, any>): Query {
    const q = data.q ? Expr.fromDict(data.q) : null;
    let sort: Sort | null = null;
    if (data.order_by && data.order_by.length) {
      const values: string[] = data.order_by.map(String);
      const ascending = !String(values[0]).startsWith("-");
      sort = new Sort(
        values.map((v) => (v.startsWith("-") ? v.slice(1) : v)),
        ascending,
      );
    }
    const slice = makeSlice(data.limit ?? null, data.offset ?? null);
    const aggregations = data.aggregations
      ? aggregationsFromDict(data.aggregations)
      : [];
    return new Query({ q, sort, slice, aggregations });
  }

  toParams(): Params {
    const params: Params = { ...exprToParams(this.q) };
    if (this.aggregations.length) {
      Object.assign(params, aggregationsToParams(this.aggregations));
    }
    if (this.sort) {
      params.sort = this.sort
        .serialize()
        .map((v) => (v.startsWith("-") ? `${v.slice(1)}:desc` : `${v}:asc`));
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
      const svalues: string[] = [];
      let ascending = true;
      for (const value of items.sort) {
        const idx = value.indexOf(":");
        const field = idx < 0 ? value : value.slice(0, idx);
        const direction = idx < 0 ? "" : value.slice(idx + 1);
        svalues.push(field);
        ascending = direction !== "desc";
      }
      sort = new Sort(svalues, ascending);
    }
    let slice: Slice | null = null;
    if ("limit" in items || "offset" in items) {
      const offset = parseInt((items.offset ?? ["0"])[0] || "0", 10) || 0;
      const limit = items.limit ? parseInt(items.limit[0], 10) : null;
      slice = makeSlice(limit, offset);
    }
    return new Query({ q, sort, slice, aggregations: aggs });
  }

  toString(): string {
    return paramsToString(this.toParams());
  }

  static fromString(value: string): Query {
    const s = value.startsWith("?") ? value.slice(1) : value;
    return Query.fromParams(stringToParams(s));
  }

  toRql(): string {
    return toRql(this.q, this.aggregations);
  }

  static fromRql(value: string): Query {
    const [q, aggregations] = parseRqlQuery(value);
    return new Query({ q, aggregations });
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
    if (this.sort) {
      params.sort = this.sort
        .serialize()
        .map((v) => (v.startsWith("-") ? `${v.slice(1)}:desc` : `${v}:asc`));
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
