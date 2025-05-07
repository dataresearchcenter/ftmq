import type { Model } from "./model";
import type { IPropertyDatum, Property as TProperty } from "./property";
import { Property } from "./property";

interface IEdgeSpecification {
  source: string;
  target: string;
  directed: boolean;
  label?: string;
  caption: string[];
  required?: string[];
}

export type SchemaSpec = string | null | undefined | Schema;
export type { TProperty };

export interface ISchemaDatum {
  label: string;
  plural: string;
  schemata: string[];
  extends: string[];
  abstract?: boolean;
  hidden?: boolean;
  matchable?: boolean;
  generated?: boolean;
  description?: string;
  edge?: IEdgeSpecification;
  featured?: string[];
  caption?: string[];
  required?: string[];
  properties: {
    [x: string]: IPropertyDatum;
  };
}

export class Schema {
  static readonly THING = "Thing";

  public readonly model: Model;
  public readonly name: string;
  public readonly label: string;
  public readonly plural: string;
  public readonly abstract: boolean;
  public readonly hidden: boolean;
  public readonly matchable: boolean;
  public readonly generated: boolean;
  public readonly description: string | null;
  public readonly featured: string[];
  public readonly schemata: string[];
  public readonly extends: string[];
  public readonly caption: string[];
  public readonly required: string[];
  public readonly edge?: IEdgeSpecification;
  public readonly isEdge: boolean;
  private properties: Map<string, TProperty> = new Map();

  constructor(model: Model, schemaName: string, config: ISchemaDatum) {
    this.model = model;
    this.name = schemaName;
    this.label = config.label || this.name;
    this.plural = config.plural || this.label;
    this.schemata = config.schemata;
    this.extends = config.extends;
    this.featured = config.featured || [];
    this.caption = config.caption || [];
    this.required = config.required || [];
    this.abstract = !!config.abstract;
    this.hidden = !!config.hidden;
    this.matchable = !!config.matchable;
    this.generated = !!config.generated;
    this.description = config.description || null;
    this.isEdge = !!config.edge;
    this.edge = config.edge;

    Object.entries(config.properties).forEach(([propertyName, property]) => {
      this.properties.set(propertyName, new Property(this, property));
    });
  }

  isThing(): boolean {
    return this.isA(Schema.THING);
  }

  getExtends(): Array<Schema> {
    return this.extends.map((name) => this.model.getSchema(name));
  }

  getParents(): Array<Schema> {
    const parents = new Map<string, Schema>();
    for (const ext of this.getExtends()) {
      parents.set(ext.name, ext);
      for (const parent of ext.getParents()) {
        parents.set(parent.name, parent);
      }
    }
    return Array.from(parents.values());
  }

  getChildren(): Array<Schema> {
    const children = new Array<Schema>();
    for (const schema of this.model.getSchemata()) {
      const parents = schema.getParents().map((s) => s.name);
      if (parents.indexOf(this.name) !== -1) {
        children.push(schema);
      }
    }
    return children;
  }

  getProperties(qualified = false): Map<string, TProperty> {
    const properties = new Map<string, TProperty>();
    this.getExtends().forEach((schema) => {
      schema.getProperties(qualified).forEach((prop, name) => {
        properties.set(name, prop);
      });
    });
    this.properties.forEach((prop, name) => {
      properties.set(qualified ? prop.qname : name, prop);
    });
    return properties;
  }

  getFeaturedProperties(): Array<TProperty> {
    return this.featured
      .map((name) => this.getProperty(name))
      .filter(Property.isProperty);
  }

  hasProperty(prop: string | TProperty): boolean {
    if (Property.isProperty(prop)) {
      return this.getProperties(true).has(prop.qname);
    }
    return this.getProperties().has(prop);
  }

  /**
   * Get the value of a property. If it's not defined, return an
   * empty array. If it's not a valid property, raise an error.
   *
   * @param prop name or Property
   */
  getProperty(prop: string | TProperty): TProperty | undefined {
    if (Property.isProperty(prop)) {
      return prop;
    }
    return this.getProperties().get(prop);
  }

  isA(schema: SchemaSpec): boolean {
    try {
      schema = this.model.getSchema(schema);
      return !!~this.schemata.indexOf(schema.name);
    } catch (error) {
      return false;
    }
  }

  isAny(schemata: Array<SchemaSpec>): boolean {
    for (const schema of schemata) {
      if (this.isA(schema)) {
        return true;
      }
    }
    return false;
  }

  static isSchema = (item: Schema | undefined): item is Schema => {
    return !!item;
  };

  static getAllParents(schemata: Array<Schema>): Array<Schema> {
    const parents = Array.from(schemata);
    for (const schema of schemata) {
      for (const parent of schema.getParents()) {
        if (parents.indexOf(parent) === -1) {
          parents.push(parent);
        }
      }
    }
    return parents;
  }

  toString(): string {
    return this.name;
  }
}
