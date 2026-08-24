import { QueryError } from "./exceptions.js";
import { refFromWire, type Ref } from "./refs.js";
import { byString, ensureList } from "./util.js";

export type AggFunc = "min" | "max" | "sum" | "avg" | "count";
export const AGG_FUNCS = new Set<AggFunc>([
  "min",
  "max",
  "sum",
  "avg",
  "count",
]);

/**
 * An immutable aggregation spec: a function over a field reference, optionally
 * grouped by others.
 */
export class Agg {
  readonly func: AggFunc;
  readonly ref: Ref;
  readonly groups: Ref[];

  constructor(func: AggFunc, ref: Ref, groups: Ref[] = []) {
    if (!AGG_FUNCS.has(func)) {
      throw new QueryError(`Invalid aggregation function: \`${func}\``);
    }
    this.func = func;
    this.ref = ref;
    this.groups = [...groups].sort((a, b) => byString(a.wire, b.wire));
  }

  /** The wire spelling of the aggregated field. */
  get field(): string {
    return this.ref.wire;
  }

  /** A stable identity key for de-duplication. */
  key(): string {
    return `${this.func}:${this.field}:${this.groups.map((g) => g.wire).join(",")}`;
  }
}

/**
 * An aggregation projection node:
 * `A({ sum: P("amountEur"), by: G("countries") })`.
 *
 * Fields are addressed with the same `M` / `P` / `G` / `C` markers the filter
 * families use, called with a bare field name (plus `Year()`), so an
 * aggregation says which family it means instead of leaving it to be guessed
 * from the name.
 */
export interface ANode {
  aggs: Agg[];
}

export type ASpec = { by?: Ref | Ref[] } & Partial<
  Record<AggFunc, Ref | Ref[]>
>;

export function A(spec: ASpec): ANode {
  const groups = ensureList(spec.by) as Ref[];
  const aggs: Agg[] = [];
  for (const func of ["min", "max", "sum", "avg", "count"] as AggFunc[]) {
    const refs = spec[func];
    if (refs === undefined) continue;
    for (const ref of ensureList(refs) as Ref[]) {
      aggs.push(new Agg(func, ref, groups));
    }
  }
  if (aggs.length === 0) {
    throw new QueryError("Empty aggregation: pass at least one `func=<ref>`");
  }
  return { aggs };
}

/** De-duplicate aggregation specs by identity. */
export function uniqueAggs(aggs: Agg[]): Agg[] {
  const seen = new Map<string, Agg>();
  for (const agg of aggs) seen.set(agg.key(), agg);
  return [...seen.values()];
}

/**
 * Serialize specs to the query `toDict` shape: one `{func, field, by?}`
 * mapping per spec (fields spelled as on the wire, `by` omitted when
 * ungrouped), deterministically ordered.
 */
export function aggregationsToDict(aggs: Agg[]): Record<string, any>[] {
  const sorted = [...aggs].sort(
    (a, b) =>
      byString(a.func, b.func) ||
      byString(a.field, b.field) ||
      byString(
        a.groups.map((g) => g.wire).join(","),
        b.groups.map((g) => g.wire).join(","),
      ),
  );
  return sorted.map((agg) => {
    const spec: Record<string, any> = { func: agg.func, field: agg.field };
    if (agg.groups.length) spec.by = agg.groups.map((g) => g.wire);
    return spec;
  });
}

/** Rebuild specs from `aggregationsToDict` output. */
export function aggregationsFromDict(data: Record<string, any>[]): Agg[] {
  return data.map(
    (spec) =>
      new Agg(
        spec.func as AggFunc,
        refFromWire(String(spec.field)),
        ensureList(spec.by).map((g) => refFromWire(String(g))),
      ),
  );
}
