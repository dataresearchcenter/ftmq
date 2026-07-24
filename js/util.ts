import slugify from "slugify";

import type { TEntity } from "./model";

export interface IEntityUrlParams {
  readonly slug: string[];
}

export function getEntityUrlParams(entity: TEntity): IEntityUrlParams {
  return {
    slug: [entity.id, slugify(entity.caption).slice(0, 255)],
  };
}

export function getEntityUrl(entity: TEntity, prefix?: string): string {
  const { slug } = getEntityUrlParams(entity);
  return `${prefix || "/entities"}/${slug.join("/")}`;
}
