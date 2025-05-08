import type { Entity as EntityType, IEntityDatum } from "./entity";
import { Entity } from "./entity";
import { Property } from "./property";
import type { ISchemaDatum, Schema as TSchema } from "./schema";
import { Schema } from "./schema";
import type {
  IPropertyTypeDatum,
  PropertyType as TPropertyType,
} from "./propertyType";
import { PropertyType } from "./propertyType";

interface IModelDatum {
  schemata: { [name: string]: ISchemaDatum };
  types: { [name: string]: IPropertyTypeDatum };
}

export * from "./value";
export { default as IconRegistry } from "./icons";
export type { TSchema };

export class Model {
  public readonly schemata: { [x: string]: TSchema | undefined } = {};
  public readonly types: { [x: string]: TPropertyType } = {};

  constructor(config: IModelDatum) {
    this.types = {};
    Object.entries(config.types).forEach(([typeName, typeData]) => {
      this.types[typeName] = new PropertyType(typeName, typeData);
    });

    this.schemata = {};
    Object.entries(config.schemata).forEach(([schemaName, schema]) => {
      this.schemata[schemaName] = new Schema(this, schemaName, schema);
    });
  }

  getSchema(schemaName: string | null | undefined | TSchema): TSchema {
    if (schemaName === null || schemaName === undefined) {
      throw new Error("Invalid schema.");
    }
    if (schemaName instanceof Schema) {
      return schemaName;
    }
    const schema = this.schemata[schemaName];
    if (schema === undefined) {
      throw new Error("No such schema: " + schemaName);
    }
    return schema;
  }

  /**
   * Get a list of all schemata.
   */
  getSchemata(): TSchema[] {
    return Object.keys(this.schemata)
      .map((name) => this.schemata[name])
      .filter(Schema.isSchema);
  }

  /**
   * Get a list of all unique properties.
   */
  getProperties(): Property[] {
    const qnames = new Map<string, Property>();
    this.getSchemata().forEach((schema) => {
      schema.getProperties().forEach((prop) => {
        qnames.set(prop.qname, prop);
      });
    });
    return Array.from(qnames.values());
  }

  /**
   * Get an arbitrary property
   */
  getProperty(name: string): Property | undefined {
    return this.getProperties().filter((p) => p.name === name)[0];
  }

  /**
   * Get a particular property type.
   *
   * @param type name of the type
   */
  getType(type: string | TPropertyType): TPropertyType {
    if (type instanceof PropertyType) {
      return type;
    }
    return this.types[type];
  }

  /**
   * Convert a raw JSON object to an entity proxy.
   *
   * @param raw entity source data
   */
  getEntity(raw: IEntityDatum | EntityType): EntityType {
    if (raw instanceof Entity) {
      return raw;
    } else {
      return new Entity(this, raw);
    }
  }
}
