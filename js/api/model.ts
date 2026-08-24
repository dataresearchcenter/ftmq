// ftmq-specific dataset & catalog metadata models, adapted from the Python
// `ftmq.model` (Catalog / Dataset / DatasetStats).
//
// The followthemoney data model itself (Entity, Model, Schema, Property,
// PropertyType, ...) lives in `@opensanctions/followthemoney`; these
// dataset / catalog / statistics concepts are NOT part of that package and are
// kept here.

export interface IPublisher {
  readonly name: string;
  readonly name_en?: string | null;
  readonly acronym?: string | null;
  readonly url?: string | null;
  readonly description?: string | null;
  readonly country?: string | null;
  readonly official?: boolean | null;
  readonly logo_url?: string | null;
}

export interface IResource {
  readonly name: string;
  readonly url: string;
  readonly title?: string | null;
  readonly checksum?: string | null;
  readonly timestamp?: string | null;
  readonly mime_type?: string | null;
  readonly size?: number | null;
}

export interface ICoverage {
  readonly start?: string | null;
  readonly end?: string | null;
  readonly frequency?: string | null;
  readonly countries?: string[] | null;
  readonly schedule?: string | null;
}

// --- statistics (ftmq.model.stats.DatasetStats) ---

export interface ICountry {
  readonly code: string;
  readonly count: number;
  readonly label?: string | null;
}

export interface ISchema {
  readonly name: string;
  readonly count: number;
  readonly label: string;
  readonly plural: string;
}

export interface ISchemata {
  readonly total: number;
  readonly countries: ICountry[];
  readonly schemata: ISchema[];
}

export interface IDatasetStats {
  readonly things: ISchemata;
  readonly intervals: ISchemata;
  readonly entity_count: number;
  readonly start?: string | null;
  readonly end?: string | null;
  readonly countries: string[];
}

// --- dataset / catalog (ftmq.model.dataset) ---

export type TContentType = "documents" | "structured" | "mixed";

export interface IDataset {
  readonly name: string;
  readonly title?: string | null;
  readonly prefix?: string | null;
  readonly license?: string | null;
  readonly summary?: string | null;
  readonly description?: string | null;
  readonly url?: string | null;
  readonly updated_at?: string | null;
  readonly version?: string | null;
  readonly category?: string | null;
  readonly publisher?: IPublisher | null;
  readonly maintainer?: IPublisher | null;
  readonly coverage?: ICoverage | null;
  readonly stats?: IDatasetStats | null;
  readonly resources?: IResource[];
  readonly entity_count?: number | null;
  readonly thing_count?: number | null;
  readonly content_type?: TContentType | null;
  readonly git_repo?: string | null;
  readonly uri?: string | null;
  readonly tags?: string[];
  readonly children?: string[];
}

export interface ICatalog {
  readonly name: string;
  readonly title?: string | null;
  readonly datasets: IDataset[];
  readonly description?: string | null;
  readonly url?: string | null;
  readonly updated_at?: string | null;
  readonly publisher?: IPublisher | null;
  readonly maintainer?: IPublisher | null;
  readonly logo_url?: string | null;
  readonly git_repo?: string | null;
  readonly uri?: string | null;
}
