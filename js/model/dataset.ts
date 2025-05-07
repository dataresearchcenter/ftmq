export type TDatasetCategory =
  | "news"
  | "leak"
  | "land"
  | "gazette"
  | "court"
  | "company"
  | "sanctions"
  | "procurement"
  | "finance"
  | "grey"
  | "library"
  | "license"
  | "regulatory"
  | "poi"
  | "customs"
  | "census"
  | "transport"
  | "casefile"
  | "other"
  | null;

export type TDatasetFrequency =
  | "unknown"
  | "never"
  | "hourly"
  | "daily"
  | "weekly"
  | "monthly"
  | "annually"
  | null;

export type TContentType = "documents" | "structured" | "mixed";

export interface IPublisher {
  readonly name: string;
  readonly url?: string | null;
  readonly description?: string | null;
  readonly country?: string | null;
  readonly country_label?: string | null;
  readonly official?: boolean | null;
  readonly logo_url?: string | null;
}

export interface ICoverage {
  readonly start?: string | null;
  readonly end?: string | null;
  readonly frequency?: TDatasetFrequency;
  readonly countries?: string[] | null;
  readonly schedule?: string[] | null;
}

export interface ISchemataStats {
  readonly total?: number | null;
  readonly countries?: ICountry[] | null;
  readonly schemata?: ISchema[] | null;
}

export interface ICountry {
  readonly code: string;
  readonly count: number;
  readonly label?: string | null;
}

export interface ISchema {
  readonly name: string;
  readonly count: number;
  readonly label: string | null;
  readonly plural: string | null;
}

export interface IResource {
  readonly name: string;
  readonly url: string;
  readonly title?: string | null;
  readonly checksum?: string | null;
  readonly timestamp?: string | null;
  readonly mime_type?: string | null;
  readonly mime_type_label?: string | null;
  readonly size?: number | null;
}

export interface IMaintainer {
  readonly name: string;
  readonly description?: string | null;
  readonly url?: string | null;
  readonly country?: string | null;
  readonly country_label?: string | null;
  readonly logo_url?: string | null;
}

export interface IDataset {
  readonly name: string;
  readonly prefix?: string | null;
  readonly title?: string | null;
  readonly license?: string | null;
  readonly summary?: string | null;
  readonly description?: string | null;
  readonly url?: string | null;
  readonly updated_at?: string | null;
  readonly version?: string | null;
  readonly category?: TDatasetCategory;
  readonly publisher?: IPublisher | null;
  readonly coverage?: ICoverage | null;
  readonly things?: ISchemataStats | null;
  readonly intervals?: ISchemataStats | null;
  readonly entity_count?: number | null;
  readonly resources?: IResource[] | null;
  readonly index_url?: string | null;
  readonly catalog?: string | null;
  readonly countries?: string[] | null;
  readonly info_url?: string | null;
  readonly data_url?: string | null;
  readonly git_repo?: string | null;
  readonly uri?: string | null;
  readonly maintainer?: IMaintainer | null;
  readonly content_type?: TContentType | null;
  readonly aleph_url?: string | null;
}

export interface ICatalog {
  readonly name: string;
  readonly title?: string | null;
  readonly datasets: IDataset[];
  readonly description?: string | null;
  readonly url?: string | null;
  readonly updated_at?: string | null;
  readonly publisher?: IPublisher | null;
  readonly maintainer?: IMaintainer | null;
  readonly git_repo?: string | null;
  readonly uri?: string | null;
}
