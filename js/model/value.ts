import type { Value } from "./entity";
import type { Property } from "./property";

const castPropertyValue = (
  prop: Property,
  value: Value,
): Value | Date | number => {
  if (typeof value !== "string") return value; // Entity
  if (prop.type.name == "number") return parseFloat(value);
  if (prop.type.name == "date") {
    if (value.length == 4) return value.toString();
    return new Date(value);
  }
  return value;
};

export const getPrimitiveValue = (
  prop: Property,
  value: Value | null,
): number | string => {
  if (!value) return "";
  const casted = castPropertyValue(prop, value);
  if (typeof casted == "string" || typeof casted == "number") return casted;
  if (casted instanceof Date) return casted.toISOString();
  // we are an entity!
  return casted.getCaption();
};
