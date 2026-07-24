import { QueryError } from "./exceptions.js";
import { asBool, ensureList } from "./util.js";

// query node family: M (meta), P (property), G (property-type group), C (context)
export type Family = "M" | "P" | "G" | "C";

// the value comparators (mirrors ftmq.enums.Comparators). `eq` is the default.
export type Comparator =
  | "eq"
  | "not"
  | "in"
  | "not_in"
  | "null"
  | "gt"
  | "gte"
  | "lt"
  | "lte"
  | "like"
  | "ilike"
  | "startswith"
  | "endswith"
  | "notlike"
  | "notilike"
  | "between";

const COMPARATORS = new Set<Comparator>([
  "eq",
  "not",
  "in",
  "not_in",
  "null",
  "gt",
  "gte",
  "lt",
  "lte",
  "like",
  "ilike",
  "startswith",
  "endswith",
  "notlike",
  "notilike",
  "between",
]);

export type LeafValue = string | boolean | string[];

// serialized form of a single leaf condition
export interface LeafDict {
  t: Family;
  f: string;
  op: Comparator;
  v: LeafValue;
}

/** Split a `field__comparator` lookup key into its parts (default `eq`). */
export function parseLookup(key: string): [string, Comparator] {
  const idx = key.indexOf("__");
  if (idx < 0) return [key, "eq"];
  const field = key.slice(0, idx);
  const op = key.slice(idx + 2);
  if (!COMPARATORS.has(op as Comparator)) {
    throw new QueryError(`Invalid comparator in lookup: \`${key}\``);
  }
  return [field, op as Comparator];
}

function castValue(comparator: Comparator, value: unknown): LeafValue {
  if (comparator === "in" || comparator === "not_in") {
    return Array.from(new Set(ensureList(value).map((v) => String(v))));
  }
  if (comparator === "null") {
    return asBool(value);
  }
  if (Array.isArray(value)) {
    throw new QueryError(
      `Invalid value for \`${comparator}\`: ${JSON.stringify(value)}`,
    );
  }
  return value === undefined || value === null ? "" : String(value);
}

/** A single query condition: a family, a field, a comparator and a value. */
export class Leaf {
  readonly family: Family;
  readonly field: string;
  readonly comparator: Comparator;
  readonly value: LeafValue;

  constructor(
    family: Family,
    field: string,
    value: unknown,
    comparator: Comparator = "eq",
  ) {
    this.family = family;
    this.field = field;
    this.comparator = comparator;
    this.value = castValue(comparator, value);
  }

  fieldDict(): LeafDict {
    let v = this.value;
    if (Array.isArray(v)) v = [...v].sort();
    return { t: this.family, f: this.field, op: this.comparator, v };
  }
}

/** Build a leaf of a family from a `field[__op]=value` lookup (no validation). */
export function makeLeaf(family: Family, key: string, value: unknown): Leaf {
  const [field, comparator] = parseLookup(key);
  return new Leaf(family, field, value, comparator);
}

/** Reconstruct a leaf from its serialized `{t,f,op,v}` form. */
export function leafFromDict(data: LeafDict): Leaf {
  const key = data.op === "eq" ? data.f : `${data.f}__${data.op}`;
  return makeLeaf(data.t, key, data.v);
}
