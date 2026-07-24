import type { IEntityDatum } from "@opensanctions/followthemoney";

import { Query } from "../query/index.js";
import type { ICatalog, IDataset } from "./model.js";
import type {
  IAggregationResult,
  IAutocompleteResult,
  IEntitiesResult,
  IRetrieveParams,
} from "./types.js";
import { clampLimit } from "./util.js";

type ApiError = {
  detail: string[];
};

export * from "./model.js";
export * from "./types.js";
export * from "./util.js";

export default class Api {
  private endpoint: string;
  private api_key?: string;

  constructor(endpoint: string, api_key?: string) {
    this.endpoint = endpoint;
    this.api_key = api_key;
  }

  async getCatalog(opts: RequestInit = {}): Promise<ICatalog> {
    return await this.get("catalog", opts);
  }

  async getDataset(dataset: string, opts: RequestInit = {}): Promise<IDataset> {
    return await this.get(`catalog/${dataset}`, opts);
  }

  async getEntity(
    id: string,
    retrieve: IRetrieveParams = { nested: true },
    opts: RequestInit = {},
  ): Promise<IEntityDatum> {
    return await this.get(`entities/${id}`, opts, undefined, retrieve);
  }

  async getEntities(
    query: Query = new Query(),
    retrieve: IRetrieveParams = {},
    opts: RequestInit = {},
  ): Promise<IEntitiesResult> {
    return await this.get("entities", opts, query, retrieve);
  }

  async getEntitiesAll(
    query: Query = new Query(),
    retrieve: IRetrieveParams = {},
  ): Promise<IEntityDatum[]> {
    // chain requests via `offset` to paginate through all results
    const limit = query.limit ?? 100;
    let offset = query.offset ?? 0;
    let entities: IEntityDatum[] = [];
    for (;;) {
      const res = await this.getEntities(query.slice(offset, offset + limit), retrieve);
      entities = [...entities, ...res.entities];
      if (!res.next_url || res.entities.length === 0) return entities;
      offset += limit;
    }
  }

  async getAggregations(
    query: Query = new Query(),
    opts: RequestInit = {},
  ): Promise<IAggregationResult> {
    return await this.get("aggregate", opts, query);
  }

  async search(
    q: string,
    query: Query = new Query(),
    retrieve: IRetrieveParams = {},
    opts: RequestInit = {},
  ): Promise<IEntitiesResult> {
    return await this.get("search", opts, query, retrieve, q);
  }

  async autocomplete(q: string, opts: RequestInit = {}): Promise<IAutocompleteResult> {
    return await this.get("autocomplete", opts, undefined, undefined, q);
  }

  onNotFound(error: ApiError): any {
    const errorMsg = error.detail.join("; ");
    console.log("404 NOT FOUND", errorMsg);
    throw new Error(errorMsg);
  }

  onError(status: number, error: ApiError): any {
    const errorMsg = error.detail.join("; ");
    console.log(status, errorMsg);
    throw new Error(errorMsg);
  }

  // build the request url params from a Query (flat aleph, or `rql=` for a
  // nested tree) plus retrieve flags, the search term and the api key
  private params(
    query?: Query,
    retrieve: IRetrieveParams = {},
    q?: string,
  ): URLSearchParams {
    let params = new URLSearchParams();
    if (query) {
      const authenticated = !!this.api_key;
      const limit = clampLimit(query.limit, authenticated);
      const offset = query.offset ?? 0;
      params = query.slice(offset, offset + limit).toRequestParams();
    }
    for (const [key, value] of Object.entries(retrieve)) {
      if (value !== undefined) params.set(key, String(value));
    }
    if (q !== undefined) params.set("q", q);
    // the api key is only accessible on the server and bumps the page limit
    if (this.api_key) params.set("api_key", this.api_key);
    return params;
  }

  async get(
    path: string,
    opts: RequestInit = {},
    query?: Query,
    retrieve: IRetrieveParams = {},
    q?: string,
  ): Promise<any> {
    const url = `${this.endpoint}/${path}?${this.params(query, retrieve, q)}`;
    const res = await fetch(url, opts);
    if (res.ok) {
      return await res.json();
    }
    if (res.status >= 400 && res.status < 600) {
      const error = await res.json();
      if (res.status === 404) {
        this.onNotFound(error);
      } else {
        this.onError(res.status, error);
      }
    }
  }
}

// arbitrary typed fetchers that just take a full url
async function fetcher(url: string, opts: RequestInit = {}): Promise<any> {
  const res = await fetch(url, opts);
  if (res.ok) {
    return await res.json();
  }
  throw new Error(`Fetch error: ${res.status} ${res.statusText}`);
}

export async function getCatalog(url: string, opts: RequestInit = {}): Promise<ICatalog> {
  return fetcher(url, opts);
}

export async function getDataset(url: string, opts: RequestInit = {}): Promise<IDataset> {
  return fetcher(url, opts);
}

export async function getEntity(
  url: string,
  opts: RequestInit = {},
): Promise<IEntityDatum> {
  return fetcher(url, opts);
}
