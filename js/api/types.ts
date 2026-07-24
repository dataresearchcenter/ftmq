import type { IEntityDatum } from "@opensanctions/followthemoney";

import type { IDatasetStats } from "./model.js";

// --- request options -------------------------------------------------------

/** Non-query request params: response-shaping flags plus the optional `q`
 * full-text search term (read by the api directly, not part of the `Query`). */
export interface IRetrieveParams {
  readonly nested?: boolean;
  readonly featured?: boolean;
  readonly dehydrate?: boolean;
  readonly dehydrate_nested?: boolean;
  readonly stats?: boolean;
  // a `q` term routes the /entities query to full-text search via ftmq.search
  readonly q?: string;
}

// --- aggregations ----------------------------------------------------------

// ungrouped aggregations, Aleph `metrics`: `{prop: {func: value}}`
export type IMetrics = {
  readonly [prop: string]: { readonly [func: string]: number };
};

// a grouped-aggregation bucket, e.g. `{value: "2011", label: "2011", count: 3}`
export interface IFacetValue {
  readonly value: string;
  readonly label: string;
  readonly count?: number;
  readonly [func: string]: string | number | undefined;
}

// grouped aggregations, Aleph `facets`: `{field: {values: [...], total}}`
export type IFacets = {
  readonly [field: string]: {
    readonly values: IFacetValue[];
    readonly total: number;
  };
};

// --- responses -------------------------------------------------------------

// the api echoes back the canonical query serialization (`Query.toDict`)
export type IQueryDict = Record<string, any>;

/** The list / search response, matching the OpenAleph api v2 envelope. */
export interface IEntitiesResult {
  readonly status: string;
  readonly results: IEntityDatum[];
  readonly total: number;
  readonly total_type: string;
  readonly page: number;
  readonly pages: number;
  readonly limit: number;
  readonly offset: number;
  readonly next: string | null;
  readonly previous: string | null;
  readonly facets: IFacets;
  readonly metrics: IMetrics;
  readonly filters: Record<string, string[]>;
  readonly query_q: string | null;
  // ftmq extensions (additive)
  readonly query: IQueryDict;
  readonly stats: IDatasetStats | null;
  readonly links: Record<string, string>;
}

export interface IAutocompleteItem {
  readonly id: string;
  readonly name: string;
}

export interface IAutocompleteResult {
  readonly candidates: IAutocompleteItem[];
}
