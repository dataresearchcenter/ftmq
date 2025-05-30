import type { Schema } from "./schema";
import type { PropertyType } from "./propertyType";

export interface IPropertyDatum {
  name: string;
  qname: string;
  label: string;
  type: string;
  description?: string;
  stub?: boolean;
  hidden?: boolean;
  matchable?: boolean;
  range?: string | null;
  reverse?: string;
}

/**
 * Definition of a property, with relevant metadata for type,
 * labels and other useful criteria.
 */
export class Property {
  public readonly schema: Schema;
  public readonly name: string;
  public readonly qname: string;
  public readonly label: string;
  public readonly type: PropertyType;
  public readonly hidden: boolean;
  public readonly matchable: boolean;
  public readonly description: string | null;
  public readonly stub: boolean;
  private readonly range: string | null;
  private readonly reverse: string | null;

  constructor(schema: Schema, property: IPropertyDatum) {
    this.schema = schema;
    this.name = property.name;
    this.qname = property.qname;
    this.label = property.label || property.name;
    this.hidden = !!property.hidden;
    this.description = property.description || null;
    this.stub = !!property.stub;
    this.matchable = !!property.matchable;
    this.range = property.range || null;
    this.reverse = property.reverse || null;
    this.type = schema.model.getType(property.type);
  }

  getRange(): Schema | undefined {
    if (!this.range) {
      return undefined;
    }
    return this.schema.model.getSchema(this.range);
  }

  getReverse(): Property | undefined {
    const range = this.getRange();
    if (range === undefined || this.reverse === null) {
      return undefined;
    }
    return range.getProperty(this.reverse);
  }

  static isProperty = (
    item: Property | string | undefined,
  ): item is Property => {
    if (typeof item === "string") {
      return false;
    }
    return !!item;
  };

  toString(): string {
    return this.qname;
  }
}
