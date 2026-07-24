export class QueryError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "QueryError";
    Object.setPrototypeOf(this, QueryError.prototype);
  }
}
