import { Leaf, leafFromDict, makeLeaf, type Family } from "./leaves.js";
import { makeRef, type Ref } from "./refs.js";
import { byString, canon } from "./util.js";

export const AND = "AND";
export const OR = "OR";
export type Connector = "AND" | "OR";

export type Child = Expr | Leaf;

// a child's structural identity: its canonical serialization
const childKey = (child: Child): string =>
  child instanceof Expr
    ? canon(child.toDict())
    : canon({ leaf: child.fieldDict() });

/** Splice non-negated same-connector sub-groups and drop duplicate children. */
function normalize(children: Child[], connector: Connector): Child[] {
  const result: Child[] = [];
  const seen = new Set<string>();
  for (const child of children) {
    // a sub-group is normalized already, so one level of splicing is enough
    const items =
      child instanceof Expr && !child.negated && child.connector === connector
        ? child.children
        : [child];
    for (const item of items) {
      const key = childKey(item);
      if (seen.has(key)) continue;
      seen.add(key);
      result.push(item);
    }
  }
  return result;
}

/** A boolean node; children are canonicalized on construction. */
export class Expr {
  connector: Connector;
  negated: boolean;
  children: Child[];

  constructor(
    children: Child[] = [],
    connector: Connector = AND,
    negated = false,
  ) {
    this.connector = connector;
    this.negated = negated;
    this.children = normalize(children, connector);
  }

  /** Whether this node carries no condition (empty and not negated). */
  get isEmpty(): boolean {
    return this.children.length === 0 && !this.negated;
  }

  private combineWith(other: Expr, connector: Connector): Expr {
    // nodes are never modified after construction, so they can be shared
    if (this.isEmpty) return other;
    if (other.isEmpty) return this;
    return new Expr([this, other], connector);
  }

  and(other: Expr): Expr {
    return this.combineWith(other, AND);
  }

  or(other: Expr): Expr {
    return this.combineWith(other, OR);
  }

  not(): Expr {
    return new Expr([...this.children], this.connector, !this.negated);
  }

  *iterLeaves(): Generator<Leaf> {
    for (const child of this.children) {
      if (child instanceof Expr) yield* child.iterLeaves();
      else yield child;
    }
  }

  toDict(): Record<string, any> {
    const key = this.connector.toLowerCase();
    const children: any[] = [];
    for (const child of this.children) {
      // normalized already; sorting makes equivalent trees serialize alike
      if (child instanceof Expr) children.push(child.toDict());
      else children.push({ leaf: child.fieldDict() });
    }
    children.sort((a, b) => byString(canon(a), canon(b)));
    const data: Record<string, any> = { [key]: children };
    if (this.negated) data.not = true;
    return data;
  }

  static fromDict(data: Record<string, any>): Expr {
    const connector: Connector = "or" in data ? OR : AND;
    const children: Child[] = [];
    for (const child of data[connector.toLowerCase()] ?? []) {
      if ("leaf" in child) children.push(leafFromDict(child.leaf));
      else children.push(Expr.fromDict(child));
    }
    return new Expr(children, connector, Boolean(data.not));
  }
}

/**
 * A family constructor: lookups build a condition (`Expr`), a bare field name a
 * `Ref`.
 *
 * ```ts
 * P({ amountEur__gte: 1000 })   // a condition
 * P("amountEur")                // a field reference
 * ```
 */
export interface FamilyNode {
  (field: string): Ref;
  (lookups: Record<string, unknown>): Expr;
}

function makeFamily(fam: Family): FamilyNode {
  return ((arg: string | Record<string, unknown>) => {
    if (typeof arg === "string") return makeRef(fam, arg);
    const children = Object.entries(arg).map(([k, v]) => makeLeaf(fam, k, v));
    return new Expr(children, AND);
  }) as FamilyNode;
}

/** Meta fields: `dataset`, `schema`, `schemata`, `id`, ... */
export const M: FamilyNode = makeFamily("M");
/** A specific FtM property, e.g. `P({ name__ilike: "jane" })` / `P("amountEur")`. */
export const P: FamilyNode = makeFamily("P");
/** A property-type group, e.g. `G({ countries: "de" })` / `G("countries")`. */
export const G: FamilyNode = makeFamily("G");
/** A context / storage column, e.g. `C({ origin: "crawl" })` / `C("origin")`. */
export const C: FamilyNode = makeFamily("C");

export const FAMILIES: Record<Family, (l: Record<string, unknown>) => Expr> = {
  M: M as (l: Record<string, unknown>) => Expr,
  P: P as (l: Record<string, unknown>) => Expr,
  G: G as (l: Record<string, unknown>) => Expr,
  C: C as (l: Record<string, unknown>) => Expr,
};

/** Combine nodes with a single connector, skipping empties (`null` if none). */
export function combine(
  nodes: Expr[],
  connector: Connector = AND,
): Expr | null {
  let result: Expr | null = null;
  for (const node of nodes) {
    if (node.isEmpty) continue;
    if (result === null) result = node;
    else result = connector === OR ? result.or(node) : result.and(node);
  }
  return result;
}

export const and = (...nodes: Expr[]): Expr =>
  combine(nodes, AND) ?? new Expr();
export const or = (...nodes: Expr[]): Expr => combine(nodes, OR) ?? new Expr();
export const not = (node: Expr): Expr => node.not();
