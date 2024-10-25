/* eslint-disable @typescript-eslint/no-require-imports */
/* eslint-disable @typescript-eslint/no-explicit-any */
import { Trino, ConnectionOptions } from "trino-client";
import { Knex } from "knex";
const BaseClient = require("knex/lib/dialects/postgres/index.js");
const QueryCompiler = require("knex/lib/dialects/postgres/query/pg-querycompiler.js");

export type KnexTrinoConfig = Knex.Config & {
  trino: ConnectionOptions;
};

// move offset to front of limit
QueryCompiler.prototype.offset = function () {
  const noLimit = !this.single.limit && this.single.limit !== 0;
  const noOffset = !this.single.offset;
  if (noOffset) return "";
  let offset = `offset ${
    noOffset ? "0" : this._getValueOrParameterFromAttribute("offset")
  }`;
  if (!noLimit) {
    offset += ` limit ${this._getValueOrParameterFromAttribute("limit")}`;
  }
  return offset;
};
QueryCompiler.prototype.limit = function () {
  const noLimit = !this.single.limit && this.single.limit !== 0;
  const noOffset = !this.single.offset;
  if (!noOffset || noLimit) return "";
  this.single.limit = noLimit ? -1 : this.single.limit;
  return `limit ${this._getValueOrParameterFromAttribute("limit")}`;
};

class ClientAtlasSqlOdbcImpl extends BaseClient {
  private trino;

  constructor(config: KnexTrinoConfig) {
    super({
      ...config,
      // Enforce a single connection:
      pool: { min: 1, max: 1 },
      connection: {},
    } satisfies Knex.Config);
  }

  _driver() {
    this.trino = Trino.create(this.config.trino);
  }

  // Acquire a connection from the pool.
  async acquireConnection() {
    return this.trino;
  }

  async destroyRawConnection(connection: any) {
    // There is only one connection, if this one goes shut down the database
    await connection.close();
  }

  // Releases a connection back to the connection pool,
  // returning a promise resolved when the connection is released.
  releaseConnection() {}

  // Destroy the current connection pool for the client.
  async destroy(callback) {
    callback();
  }

  async _query(connection: any, obj: any) {
    if (!obj.sql) throw new Error("The query is empty");

    const query = obj.bindings.reduce((memo, binding, index) => {
      let value;
      switch (typeof binding) {
        case "string":
          value = `'${binding}'`;
          break;
        case "boolean":
          value = binding ? "TRUE" : "FALSE";
          break;
        default:
          value = binding;
      }
      return memo.replace(`$${index + 1}`, value);
    }, obj.sql.replaceAll('"', ""));

    const iter = await connection.query(query);
    const result = await iter.next();

    if (!result.value.data?.length) {
      obj.response = [];
      return obj;
    }

    const formatedResults = result.value.data.map((resultArr) => {
      return resultArr.reduce((memo, resultArrVal, index) => {
        memo[result.value.columns[index].name] = resultArrVal;
        return memo;
      }, {});
    });
    obj.response = formatedResults;
    return obj;
  }

  // Process the response as returned from the query.
  processResponse(obj) {
    const resp = obj.response;
    if (obj.method === "raw") return resp;
    if (obj.method === "first") return resp[0];
    if (obj.method === "pluck") return resp.map(obj.pluck);
    return resp;
  }
}

const ClientAtlasSqlOdbc =
  ClientAtlasSqlOdbcImpl as unknown as typeof Knex.Client;
export default ClientAtlasSqlOdbc;
