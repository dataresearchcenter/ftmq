import type { IApiQuery, IPublicQuery } from "./types";

const DEFAULT_LIMIT = 10;
const PER_PAGE = [10, 25, 50, 100];
const PUBLIC_PARAMS = [
  "q",
  "page",
  "limit",
  "order_by",
  "schema",
  "country",
  "dataset",
]; // allowed user facing url params

export const cleanQuery = (
  query: IApiQuery,
  keys: string[] = [],
): IApiQuery => {
  const patch: IApiQuery = {
    // ensure limit is within PER_PAGE
    limit: query.limit
      ? PER_PAGE.indexOf(query.limit) < 0 && !query.api_key
        ? DEFAULT_LIMIT
        : query.limit
      : DEFAULT_LIMIT,
    page: query.page || 1,
  };
  // filter out empty params and optional filter for specific keys
  return Object.fromEntries(
    Object.entries({ ...query, ...patch }).filter(
      ([k, v]) =>
        (keys.length ? keys.indexOf(k) > -1 : true) &&
        !(
          v === undefined ||
          v === "" ||
          v === null ||
          (Array.isArray(v) && !v.length)
        ),
    ),
  );
};

export const getPublicQuery = (query: IApiQuery): IPublicQuery =>
  cleanQuery(query, PUBLIC_PARAMS);
