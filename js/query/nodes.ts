import { Leaf, leafFromDict, makeLeaf, type Family } from "./leaves.js";
import { byString, canon } from "./util.js";

export const AND = "AND";
export const OR = "OR";
export type Connector = "AND" | "OR";

export type Child = Expr | Leaf;

/** A boolean node: a connector, an optional negation, and a list of children. */
export class Expr {
  connector: Connector;
  negated: boolean;
  children: Child[];

  constructor(children: Child[] = [], connector: Connector = AND, negated = false) {
    this.connector = connector;
    this.negated = negated;
    this.children = children;
  }

  /** Whether this node carries no condition (empty and not negated). */
  get isEmpty(): boolean {
    return this.children.length === 0 && !this.negated;
  }

  private copy(): Expr {
    return new Expr([...this.children], this.connector, this.negated);
  }

  private combineWith(other: Expr, connector: Connector): Expr {
    if (this.isEmpty) return other.copy();
    if (other.isEmpty) return this.copy();
    return new Expr([this.copy(), other.copy()], connector);
  }

  and(other: Expr): Expr {
    return this.combineWith(other, AND);
  }

  or(other: Expr): Expr {
    return this.combineWith(other, OR);
  }

  not(): Expr {
    const clone = this.copy();
    clone.negated = !this.negated;
    return clone;
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
      if (child instanceof Expr) {
        const childDict = child.toDict();
        if (!child.negated && child.connector === this.connector) {
          children.push(...childDict[key]);
        } else {
          children.push(childDict);
        }
      } else {
        children.push({ leaf: child.fieldDict() });
      }
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

function family(fam: Family, lookups: Record<string, unknown>): Expr {
  const children = Object.entries(lookups).map(([k, v]) => makeLeaf(fam, k, v));
  return new Expr(children, AND);
}

/** Meta-field conditions: `dataset`, `schema`, `schemata`, `id`, ... */
export const M = (lookups: Record<string, unknown>): Expr => family("M", lookups);
/** Specific-property conditions, e.g. `P({ name__ilike: "jane" })`. */
export const P = (lookups: Record<string, unknown>): Expr => family("P", lookups);
/** Property-type group conditions, e.g. `G({ countries: "de" })`. */
export const G = (lookups: Record<string, unknown>): Expr => family("G", lookups);
/** Context / storage-column conditions, e.g. `C({ origin: "crawl" })`. */
export const C = (lookups: Record<string, unknown>): Expr => family("C", lookups);

export const FAMILIES: Record<Family, (l: Record<string, unknown>) => Expr> = {
  M,
  P,
  G,
  C,
};

/** Combine nodes with a single connector, skipping empties (`null` if none). */
export function combine(nodes: Expr[], connector: Connector = AND): Expr | null {
  let result: Expr | null = null;
  for (const node of nodes) {
    if (node.isEmpty) continue;
    if (result === null) result = node;
    else result = connector === OR ? result.or(node) : result.and(node);
  }
  return result;
}

export const and = (...nodes: Expr[]): Expr => combine(nodes, AND) ?? new Expr();
export const or = (...nodes: Expr[]): Expr => combine(nodes, OR) ?? new Expr();
export const not = (node: Expr): Expr => node.not();
