import type { IEntityDatum, ICatalog, IDataset } from "../model";
import type {
  IAggregationResult,
  IApiQuery,
  IEntitiesResult,
  IAutocompleteResult,
} from "./types";

type ApiError = {
  detail: string[];
};

export * from "./types";
export * from "./util";

export default class Api {
  private endpoint: string;
  private api_key?: string;

  constructor(endpoint: string, api_key?: string) {
    this.endpoint = endpoint;
    this.api_key = api_key;
  }

  async getCatalog(opts: RequestInit = {}): Promise<ICatalog> {
    return await this.api("catalog", {}, opts);
  }

  async getDataset(dataset: string, opts: RequestInit = {}): Promise<IDataset> {
    return await this.api(`catalog/${dataset}`, {}, opts);
  }

  async getEntity(id: string, opts: RequestInit = {}): Promise<IEntityDatum> {
    return await this.api(`entities/${id}`, { nested: true }, opts);
  }

  async getEntities(
    query: IApiQuery = {},
    opts: RequestInit = {},
  ): Promise<IEntitiesResult> {
    return await this.api(`entities`, query, opts);
  }

  async getEntitiesAll(q: IApiQuery = {}): Promise<IEntityDatum[]> {
    // chain requests to paginate and get all results
    let { entities } = await this.getEntities(q);
    for (let page = 2; true; page++) {
      const res = await this.getEntities({ ...q, page });
      entities = [...entities, ...res.entities];
      if (!res.next_url) {
        return entities;
      }
    }
  }

  async getAggregations(
    query: IApiQuery = {},
    opts: RequestInit,
  ): Promise<IAggregationResult> {
    return await this.api("aggregate", query, opts);
  }

  async search(q: string, query: IApiQuery = {}): Promise<IEntitiesResult> {
    return await this.api("search", { ...query, q });
  }

  async autocomplete(q: string): Promise<IAutocompleteResult> {
    return await this.api("autocomplete", { q });
  }

  async similar(id: string, query: IApiQuery = {}): Promise<IEntitiesResult> {
    return await this.api("similar", { ...query, id });
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

  async api(
    path: string,
    query: IApiQuery = {},
    opts: RequestInit = {},
  ): Promise<any> {
    query.api_key = this.api_key; // this var is only accessible on server
    const cleanedQuery = Object.fromEntries(
      Object.entries(query).filter(([_, v]) => v.length),
    );
    const qs = new URLSearchParams(cleanedQuery);
    const url = `${this.endpoint}/${path}?${qs.toString()}`;
    const res = await fetch(url, opts);
    if (res.ok) {
      const data = await res.json();
      return data;
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
    const data = await res.json();
    return data;
  }
  throw new Error(`Fetch error: ${res.status} ${res.statusText}`);
}

export async function getCatalog(
  url: string,
  opts: RequestInit = {},
): Promise<ICatalog> {
  return fetcher(url, opts);
}

export async function getDataset(
  url: string,
  opts: RequestInit = {},
): Promise<ICatalog> {
  return fetcher(url, opts);
}

export async function getEntity(
  url: string,
  opts: RequestInit = {},
): Promise<IEntityDatum> {
  return fetcher(url, opts);
}
