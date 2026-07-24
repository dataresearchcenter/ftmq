import { QueryError } from "./exceptions.js";
import { byString, ensureList } from "./util.js";

export type AggFunc = "min" | "max" | "sum" | "avg" | "count";
export const AGG_FUNCS = new Set<AggFunc>(["min", "max", "sum", "avg", "count"]);

/** An immutable aggregation spec: a function over a property, optionally grouped. */
export class Agg {
  readonly func: AggFunc;
  readonly prop: string;
  readonly groups: string[];

  constructor(func: AggFunc, prop: string, groups: string[] = []) {
    if (!AGG_FUNCS.has(func)) {
      throw new QueryError(`Invalid aggregation function: \`${func}\``);
    }
    this.func = func;
    this.prop = prop;
    this.groups = [...groups].sort(byString);
  }

  /** A stable identity key for de-duplication. */
  key(): string {
    return `${this.func}:${this.prop}:${this.groups.join(",")}`;
  }
}

/** An aggregation projection node: `A({ sum: "amountEur", by: "beneficiary" })`. */
export interface ANode {
  aggs: Agg[];
}

export type ASpec = { by?: string | string[] } & Partial<
  Record<AggFunc, string | string[]>
>;

export function A(spec: ASpec): ANode {
  const groups = ensureList(spec.by).map((g) => String(g));
  const aggs: Agg[] = [];
  for (const func of ["min", "max", "sum", "avg", "count"] as AggFunc[]) {
    const props = spec[func];
    if (props === undefined) continue;
    for (const prop of ensureList(props)) {
      aggs.push(new Agg(func, String(prop), groups));
    }
  }
  if (aggs.length === 0) {
    throw new QueryError("Empty aggregation: pass at least one `func=prop`");
  }
  return { aggs };
}

/** De-duplicate aggregation specs by identity. */
export function uniqueAggs(aggs: Agg[]): Agg[] {
  const seen = new Map<string, Agg>();
  for (const agg of aggs) seen.set(agg.key(), agg);
  return [...seen.values()];
}

/** Serialize specs to `{func: [props], groups: {group: {func: [props]}}}`. */
export function aggregationsToDict(aggs: Agg[]): Record<string, any> {
  const funcs: Record<string, Set<string>> = {};
  const groups: Record<string, Record<string, Set<string>>> = {};
  for (const agg of aggs) {
    (funcs[agg.func] ??= new Set()).add(agg.prop);
    for (const group of agg.groups) {
      ((groups[group] ??= {})[agg.func] ??= new Set()).add(agg.prop);
    }
  }
  const data: Record<string, any> = {};
  for (const [func, props] of Object.entries(funcs)) {
    data[func] = [...props].sort(byString);
  }
  const groupNames = Object.keys(groups);
  if (groupNames.length) {
    data.groups = {};
    for (const group of groupNames) {
      data.groups[group] = {};
      for (const [func, props] of Object.entries(groups[group])) {
        data.groups[group][func] = [...props].sort(byString);
      }
    }
  }
  return data;
}

/** Rebuild specs from `aggregationsToDict` output. */
export function aggregationsFromDict(data: Record<string, any>): Agg[] {
  const rest: Record<string, any> = { ...data };
  const nested: Record<string, any> = rest.groups ?? {};
  delete rest.groups;

  const groupsByAgg = new Map<string, Set<string>>();
  for (const [group, funcs] of Object.entries(nested)) {
    for (const [func, props] of Object.entries(funcs as Record<string, any>)) {
      for (const prop of ensureList(props)) {
        const k = `${func}:${prop}`;
        if (!groupsByAgg.has(k)) groupsByAgg.set(k, new Set());
        groupsByAgg.get(k)!.add(group);
      }
    }
  }

  const aggs: Agg[] = [];
  for (const [func, props] of Object.entries(rest)) {
    for (const prop of ensureList(props)) {
      const groups = [...(groupsByAgg.get(`${func}:${prop}`) ?? [])].sort(byString);
      aggs.push(new Agg(func as AggFunc, String(prop), groups));
    }
  }
  return aggs;
}
