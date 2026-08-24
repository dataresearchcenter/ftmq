import { QueryError } from "./exceptions.js";
import { type Family } from "./leaves.js";

export const PROPERTIES_PREFIX = "properties.";
export const GROUP_PREFIX = "group.";
export const CONTEXT_PREFIX = "context.";

// meta fields addressable as a reference (`schemata` is an is-a predicate, not
// a field a projection could read)
export const META_FIELDS = new Set([
  "id",
  "entity_id",
  "canonical_id",
  "dataset",
  "schema",
]);

// the year dimension is derived from date-typed values, not a column
export type RefFamily = Family | "Y";

/**
 * A field reference: a leaf without a value.
 *
 * Names *where* to read - a followthemoney property, a property-type group, a
 * meta column, a context column - without saying what to match. Built by the
 * same `M` / `P` / `G` / `C` constructors as a filter leaf, called with a bare
 * field name, and projected over by an aggregation (`A`).
 *
 * Mirrors `ftmq.query.refs`. As everywhere in this package, field names are not
 * validated here - the server does that.
 */
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

/**
 * Resolve a wire spelling back into a ref - the single place a string becomes a
 * field reference (URL params, RQL, `toDict` keys).
 *
 * The family is encoded in the spelling: `properties.<name>`, `group.<name>`,
 * `context.<name>`; meta fields and `year` are bare. Field names themselves
 * are not validated here (server-side).
 */
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
