import type { IEntityDatum } from "@opensanctions/followthemoney";

import type { IDatasetStats } from "./model.js";

// --- request options -------------------------------------------------------

/** Response-shaping flags (read by the api directly, not part of the query). */
export interface IRetrieveParams {
  readonly nested?: boolean;
  readonly featured?: boolean;
  readonly dehydrate?: boolean;
  readonly dehydrate_nested?: boolean;
  readonly stats?: boolean;
}

// --- responses -------------------------------------------------------------

// the api echoes back the canonical query serialization (`Query.toDict`)
export type IQueryDict = Record<string, any>;

export interface IEntitiesResult {
  readonly total: number;
  readonly items: number;
  readonly query: IQueryDict;
  readonly url: string;
  readonly next_url: string | null;
  readonly prev_url: string | null;
  readonly stats: IDatasetStats | null;
  readonly entities: IEntityDatum[];
}

type Aggregation = {
  readonly min?: string | number;
  readonly max?: string | number;
  readonly sum?: number;
  readonly avg?: number;
  readonly count?: number;
};

type AggregationGroupValues = {
  readonly [key: string]: string | number;
};

type AggregationGrouper = {
  readonly [key: string]: AggregationGroupValues;
};

type AggregationGroup = {
  readonly groups?: {
    readonly min?: AggregationGrouper;
    readonly max?: AggregationGrouper;
    readonly sum?: AggregationGrouper;
    readonly avg?: AggregationGrouper;
    readonly count?: AggregationGrouper;
  };
};

type Aggregations = {
  readonly [key: string]: Aggregation | AggregationGroup | undefined;
};

export interface IAggregationResult {
  readonly total: number;
  readonly query: IQueryDict;
  readonly url: string;
  readonly stats: IDatasetStats;
  readonly aggregations: Aggregations;
}

export interface IAutocompleteItem {
  readonly id: string;
  readonly name: string;
}

export interface IAutocompleteResult {
  readonly candidates: IAutocompleteItem[];
}
