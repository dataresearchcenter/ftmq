import { Agg, type AggFunc } from "./aggregations.js";
import { resolveField } from "./aleph.js";
import { QueryError } from "./exceptions.js";
import { type Leaf } from "./leaves.js";
import { AND, combine, Expr, FAMILIES, OR } from "./nodes.js";
import { byString } from "./util.js";

// --- the self-contained RQL codec (pyrql-compatible `{name, args}` AST) ------

export type RqlArg = string | RqlNode | RqlArg[];
export interface RqlNode {
  name: string;
  args: RqlArg[];
}

const isNode = (arg: RqlArg): arg is RqlNode =>
  typeof arg === "object" && !Array.isArray(arg);

/**
 * Parse an RQL string into a `{name, args}` AST. Values are taken verbatim
 * (already url-decoded by the URL layer), matching pyrql's unencoded output; a
 * tuple `(a,b)` is represented as an array.
 */
export function parseRql(input: string): RqlNode | null {
  const text = input.trim();
  if (!text) return null;
  let i = 0;

  const parseArgs = (): RqlArg[] => {
    i++; // consume "("
    const args: RqlArg[] = [];
    if (text[i] === ")") {
      i++;
      return args;
    }
    for (;;) {
      args.push(parseTerm());
      if (text[i] === ",") {
        i++;
        continue;
      }
      if (text[i] === ")") {
        i++;
        break;
      }
      throw new QueryError(`Invalid RQL near: \`${text.slice(i)}\``);
    }
    return args;
  };

  const parseTerm = (): RqlArg => {
    if (text[i] === "(") return parseArgs(); // tuple
    const start = i;
    while (i < text.length && !"(),".includes(text[i])) i++;
    const token = text.slice(start, i);
    if (text[i] === "(") return { name: token, args: parseArgs() };
    return token; // bare value
  };

  const node = parseTerm();
  if (!isNode(node)) {
    throw new QueryError("Invalid RQL: expected an operator");
  }
  return node;
}

/** Serialize a `{name, args}` AST to an RQL string (no encoding, like pyrql). */
export function unparseRql(node: RqlNode): string {
  const arg = (a: RqlArg): string =>
    typeof a === "string"
      ? a
      : Array.isArray(a)
        ? `(${a.map(arg).join(",")})`
        : unparseRql(a);
  return `${node.name}(${node.args.map(arg).join(",")})`;
}

// --- the tree <-> RQL bridge (mirrors ftmq.query.rql) ------------------------

const RQL_COMPARATORS: Record<string, string> = {
  eq: "eq",
  ne: "not",
  lt: "lt",
  le: "lte",
  gt: "gt",
  ge: "gte",
  in: "in",
  out: "not_in",
  like: "like",
  ilike: "ilike",
  contains: "like",
};
const TO_RQL_OPERATORS: Record<string, string> = {
  eq: "eq",
  not: "ne",
  lt: "lt",
  lte: "le",
  gt: "gt",
  gte: "ge",
  in: "in",
  not_in: "out",
  like: "like",
  ilike: "ilike",
};
const RQL_FUNCTIONS: Record<string, AggFunc> = {
  sum: "sum",
  min: "min",
  max: "max",
  mean: "avg",
  count: "count",
};
const TO_RQL_FUNCTIONS: Record<string, string> = {
  sum: "sum",
  min: "min",
  max: "max",
  avg: "mean",
  count: "count",
};
const AGG_OPERATORS = new Set([...Object.keys(RQL_FUNCTIONS), "aggregate"]);

function rqlLeaf(op: string, args: RqlArg[]): Expr {
  const comparator = RQL_COMPARATORS[op];
  if (comparator === undefined) {
    throw new QueryError(`Unsupported RQL operator: \`${op}\``);
  }
  const field = args[0] as string;
  let value = args[1] as unknown;
  const [family, key] = resolveField(field);
  if (comparator === "in" || comparator === "not_in") {
    value = Array.isArray(value) ? value : [value];
  } else if (
    (comparator === "like" || comparator === "ilike") &&
    typeof value === "string"
  ) {
    value = value.replace(/\*/g, "");
  }
  const lookup = comparator === "eq" ? key : `${key}__${comparator}`;
  return FAMILIES[family]({ [lookup]: value });
}

export function rqlToExpr(node: RqlNode): Expr {
  const { name, args } = node;
  if (name === "and" || name === "or") {
    const result = combine(
      args.map((a) => rqlToExpr(a as RqlNode)),
      name === "and" ? AND : OR,
    );
    if (result === null) throw new QueryError(`Empty RQL group: \`${name}\``);
    return result;
  }
  if (name === "not") {
    return rqlToExpr(args[0] as RqlNode).not();
  }
  return rqlLeaf(name, args);
}

function alephField(leaf: Leaf): string {
  return leaf.family === "P" ? `properties.${leaf.field}` : leaf.field;
}

function leafToRql(leaf: Leaf): RqlNode {
  const op = TO_RQL_OPERATORS[leaf.comparator];
  if (op === undefined) {
    throw new QueryError(
      `Comparator \`${leaf.comparator}\` is not expressible as RQL`,
    );
  }
  let value: RqlArg = leaf.value as string;
  if (op === "in" || op === "out") {
    value = (leaf.value as string[]).map((v) => String(v)).sort(byString);
  }
  return { name: op, args: [alephField(leaf), value] };
}

export function exprToRql(expr: Expr): RqlNode {
  const group = expr.connector === OR ? "or" : "and";
  const parts: RqlNode[] = [];
  for (const child of expr.children) {
    if (child instanceof Expr) {
      const childAst = exprToRql(child);
      if (!child.negated && childAst.name === group) {
        parts.push(...(childAst.args as RqlNode[]));
      } else {
        parts.push(childAst);
      }
    } else {
      parts.push(leafToRql(child));
    }
  }
  if (parts.length === 0) {
    throw new QueryError("Cannot serialize an empty query to RQL");
  }
  const body = parts.length === 1 ? parts[0] : { name: group, args: parts };
  if (expr.negated) return { name: "not", args: [body] };
  return body;
}

// --- aggregations <-> RQL ----------------------------------------------------

function metricAggs(node: RqlNode, groups: string[]): Agg[] {
  const func = RQL_FUNCTIONS[node.name];
  if (func === undefined) {
    throw new QueryError(`Unsupported RQL aggregate operator: \`${node.name}\``);
  }
  return node.args.map((prop) => new Agg(func, String(prop), groups));
}

function nodeAggs(node: RqlNode): Agg[] {
  if (node.name === "aggregate") {
    const groups = node.args.filter((a) => typeof a === "string") as string[];
    const aggs: Agg[] = [];
    for (const arg of node.args) {
      if (isNode(arg)) aggs.push(...metricAggs(arg, groups));
    }
    return aggs;
  }
  return metricAggs(node, []);
}

function aggsToRql(aggs: Agg[]): RqlNode[] {
  const ungrouped: RqlNode[] = [];
  const grouped = new Map<string, { groups: string[]; nodes: RqlNode[] }>();
  const sorted = [...aggs].sort(
    (a, b) =>
      byString(a.groups.join(","), b.groups.join(",")) ||
      byString(a.func, b.func) ||
      byString(a.prop, b.prop),
  );
  for (const agg of sorted) {
    const node: RqlNode = { name: TO_RQL_FUNCTIONS[agg.func], args: [agg.prop] };
    if (agg.groups.length) {
      const gk = agg.groups.join(",");
      if (!grouped.has(gk)) grouped.set(gk, { groups: agg.groups, nodes: [] });
      grouped.get(gk)!.nodes.push(node);
    } else {
      ungrouped.push(node);
    }
  }
  const nodes: RqlNode[] = [...ungrouped];
  for (const { groups, nodes: metrics } of grouped.values()) {
    nodes.push({ name: "aggregate", args: [...groups, ...metrics] });
  }
  return nodes;
}

/** Parse an RQL query string into a filter `Expr` and aggregation specs. */
export function parseRqlQuery(value: string): [Expr | null, Agg[]] {
  const data = parseRql(value);
  if (!data) return [null, []];
  const aggs: Agg[] = [];
  if (AGG_OPERATORS.has(data.name)) {
    return [null, nodeAggs(data)];
  }
  if (data.name === "and") {
    const filters: RqlNode[] = [];
    for (const child of data.args) {
      if (isNode(child) && AGG_OPERATORS.has(child.name)) {
        aggs.push(...nodeAggs(child));
      } else {
        filters.push(child as RqlNode);
      }
    }
    const expr = combine(
      filters.map((f) => rqlToExpr(f)),
      AND,
    );
    return [expr, aggs];
  }
  return [rqlToExpr(data), aggs];
}

/** Serialize a filter tree and aggregation specs to an RQL query string. */
export function toRql(expr: Expr | null, aggs: Agg[] = []): string {
  const nodes: RqlNode[] = [];
  if (expr && !expr.isEmpty) {
    const filterAst = exprToRql(expr);
    if (!expr.negated && filterAst.name === "and") {
      nodes.push(...(filterAst.args as RqlNode[]));
    } else {
      nodes.push(filterAst);
    }
  }
  nodes.push(...aggsToRql(aggs));
  if (nodes.length === 0) return "";
  if (nodes.length === 1) return unparseRql(nodes[0]);
  return unparseRql({ name: "and", args: nodes });
}
