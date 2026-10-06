import { QueryError } from "./exceptions.js";
import { type Family } from "./leaves.js";

export const PROPERTIES_PREFIX = "properties.";
export const GROUP_PREFIX = "group.";
export const CONTEXT_PREFIX = "context.";

// meta fields a ref can address (`schemata` is an is-a predicate, not a field)
export const META_FIELDS = new Set([
  "id",
  "entity_id",
  "canonical_id",
  "dataset",
  "schema",
]);

// the year dimension is derived from date-typed values, not a column
export type RefFamily = Family | "Y";

/** A field reference (a leaf without a value), mirroring `ftmq.query.refs`. */
export class Ref {
  readonly family: RefFamily;
  readonly key: string;

  constructor(family: RefFamily, key: string) {
    this.family = family;
    this.key = key;
  }

  /** How this ref is spelled on a string surface (params, rql, dict keys). */
  get wire(): string {
    if (this.family === "P") return `${PROPERTIES_PREFIX}${this.key}`;
    if (this.family === "G") return `${GROUP_PREFIX}${this.key}`;
    if (this.family === "C") return `${CONTEXT_PREFIX}${this.key}`;
    return this.key;
  }

  toString(): string {
    return this.wire;
  }
}

/** The year dimension: `A({ count: M("id"), by: Year() })`. */
export const Year = (): Ref => new Ref("Y", "year");

/** Parse a wire spelling (see `Ref.wire`) into a ref. */
export function refFromWire(value: string): Ref {
  if (value.startsWith(PROPERTIES_PREFIX)) {
    return new Ref("P", value.slice(PROPERTIES_PREFIX.length));
  }
  if (value.startsWith(GROUP_PREFIX)) {
    return new Ref("G", value.slice(GROUP_PREFIX.length));
  }
  if (value.startsWith(CONTEXT_PREFIX)) {
    return new Ref("C", value.slice(CONTEXT_PREFIX.length));
  }
  if (META_FIELDS.has(value)) return new Ref("M", value);
  if (value === "year") return Year();
  throw new QueryError(`Unknown field: \`${value}\``);
}

/** Build a ref of a family, rejecting a meta field that is not addressable. */
export function makeRef(family: Family, key: string): Ref {
  if (family === "M" && !META_FIELDS.has(key)) {
    throw new QueryError(
      `Unknown meta field: \`${key}\` - one of (${[...META_FIELDS].join(", ")})`,
    );
  }
  return new Ref(family, key);
}
