/* eslint-disable @typescript-eslint/no-require-imports */
/* eslint-disable @typescript-eslint/no-explicit-any */
import { Trino, ConnectionOptions } from "trino-client";
import { Knex } from "knex";
const BaseClient = require("knex/lib/dialects/postgres/index.js");

export type KnexTrinoConfig = Knex.Config & {
  trino: ConnectionOptions;
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
    let query = obj.sql;
    console.log(query);
    obj.bindings.forEach((binding, index) => {
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
      query = query.replace(`$${index + 1}`, value);
    });
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

  processResponse(obj, runner) {
    const resp = obj.response;
    if (obj.output) return obj.output.call(runner, resp);
    if (obj.method === "raw") return resp;
    if (obj.method === "first") return resp[0];
    if (obj.method === "pluck") return resp.map(obj.pluck);
    return resp;
  }
}

const ClientAtlasSqlOdbc =
  ClientAtlasSqlOdbcImpl as unknown as typeof Knex.Client;
export default ClientAtlasSqlOdbc;
