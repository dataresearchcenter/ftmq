export const DEFAULT_LIMIT = 10;
export const PER_PAGE = [10, 25, 50, 100]; // page-size options for a UI
export const MAX_LIMIT = Math.max(...PER_PAGE); // public upper cap (mirrors the api)

// cap an unauthenticated limit to the public maximum; a smaller limit is kept
// as-is (mirrors the api's `min(limit, default_limit)`). `limit=0` is a valid
// explicit value (aggregations only, no entities) and is preserved.
export const clampLimit = (
  limit: number | undefined | null,
  authenticated = false,
): number => {
  if (limit == null) return DEFAULT_LIMIT;
  if (authenticated) return limit;
  return Math.min(limit, MAX_LIMIT);
};
