import type { IEntityDatum, ICoverage } from "../model";

export interface IPublicQuery {
  // visible api params in the browser
  readonly q?: string;
  readonly page?: number;
  readonly order_by?: string;
  readonly schema?: string;
  readonly country?: string;
  readonly limit?: number;
}

export interface IApiQuery extends IPublicQuery {
  api_key?: string;
  readonly nested?: boolean;
  readonly featured?: boolean;
  readonly dehydrate?: boolean;
  readonly dehydrate_nested?: boolean;
  readonly reverse?: string;
  readonly dataset?: string;
  readonly order_by?: string;
  readonly [key: string]: any; // actual filter props
}

export interface IEntitiesResult {
  readonly total: number;
  readonly items: number;
  readonly query: IApiQuery;
  readonly url: string;
  readonly next_url: string | null;
  readonly prev_url: string | null;
  readonly coverage: ICoverage;
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
  readonly min?: AggregationGrouper;
  readonly max?: AggregationGrouper;
  readonly sum?: AggregationGrouper;
  readonly avg?: AggregationGrouper;
  readonly count?: AggregationGrouper;
};

type Aggregations = {
  readonly [key: string]: Aggregation | AggregationGroup | undefined; // FIXME
};

export interface IAggregationResult {
  readonly total: number;
  readonly query: IApiQuery;
  readonly url: string;
  readonly coverage: ICoverage;
  readonly aggregations: Aggregations;
}

export interface IAutocompleteItem {
  readonly id: string;
  readonly name: string;
}

export interface IAutocompleteResult {
  readonly candidates: IAutocompleteItem[];
}
