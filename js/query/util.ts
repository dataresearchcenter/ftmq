// codepoint (not locale-aware) string comparison, matching Python's `sorted`
export const byString = (a: string, b: string): number =>
  a < b ? -1 : a > b ? 1 : 0;

// a stable, key-sorted JSON serialization used to canonically order tree
// children (so structurally equal trees serialize identically within TS)
export const canon = (value: unknown): string =>
  JSON.stringify(value, (_key, v) =>
    v && typeof v === "object" && !Array.isArray(v)
      ? Object.fromEntries(
          Object.keys(v as Record<string, unknown>)
            .sort(byString)
            .map((k) => [k, (v as Record<string, unknown>)[k]]),
        )
      : v,
  );

export const ensureList = <T>(value: T | T[] | undefined | null): T[] =>
  value === undefined || value === null
    ? []
    : Array.isArray(value)
      ? value
      : [value];

// match Python `banal.as_bool`
export const asBool = (value: unknown): boolean => {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value !== 0;
  return ["1", "true", "yes", "on", "enabled", "y", "t"].includes(
    String(value).trim().toLowerCase(),
  );
};
