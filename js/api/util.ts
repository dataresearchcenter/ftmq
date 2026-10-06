export const DEFAULT_LIMIT = 10;
export const PER_PAGE = [10, 25, 50, 100]; // page-size options for a UI
export const MAX_LIMIT = Math.max(...PER_PAGE); // public upper cap (mirrors the api)

// default a missing limit, cap an unauthenticated one; keeps `0` (aggs only)
export const clampLimit = (
  limit: number | undefined | null,
  authenticated = false,
): number => {
  if (limit == null) return DEFAULT_LIMIT;
  if (authenticated) return limit;
  return Math.min(limit, MAX_LIMIT);
};
