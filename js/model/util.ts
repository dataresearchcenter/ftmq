import { Model } from "./model";
import defaultModel from "../data/defaultModel.json";
import type {
  Entity,
  TEntity,
  TSchema,
  IEntityDatum,
  TProperty,
  Values,
} from "../model";

const model = new Model(defaultModel);

export function getProxy(raw: TEntity): Entity {
  return model.getEntity(raw);
}

export function getSchema(
  schemaName: string | null | undefined | TSchema,
): TSchema {
  return model.getSchema(schemaName);
}

export function getProperty(name: string): TProperty | undefined {
  return model.getProperty(name);
}

type EntityDict = {
  [id: string]: Entity;
};

export function createEntityDict(
  entities: IEntityDatum[],
  lookups?: string[],
): EntityDict {
  const data: EntityDict = {};
  for (const entity of entities.map(getProxy)) {
    if (lookups) {
      const keys = lookups.map((l) => entity.getFirst(l)).filter((k) => !!k);
      if (keys.length > 0) {
        data[keys.join("-")] = entity;
      }
    } else {
      data[entity.id] = entity;
    }
  }
  return data;
}

export function pickStringValues(values: Values): string[] {
  return values.filter((v): v is string => typeof v === "string");
}

export function pickLongestString(values: Values): string {
  return pickStringValues(values).reduce(
    (res, value) => (value.length > res.length ? value : res),
    "",
  );
}

export const countryNames: { [key: string]: string } =
  defaultModel["types"]["country"]["values"];
