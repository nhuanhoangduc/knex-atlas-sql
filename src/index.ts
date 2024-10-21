/* eslint-disable @typescript-eslint/no-require-imports */
/* eslint-disable @typescript-eslint/no-explicit-any */
import odbc from "odbc";
import { Knex } from "knex";
const ClientPg = require("knex/lib/dialects/mysql/index.js");
const {
  formatQuery,
} = require("knex/lib/execution/internal/query-executioner.js");

class ClientAtlasSqlOdbcImpl extends ClientPg {
  private pool;
  private poolConnection;

  constructor(config: Knex.Config) {
    super(config);
  }

  _driver() {
    this.poolConnection = odbc
      .pool(this.config.connection.filename)
      .then((pool) => {
        this.pool = pool;
      })
      .catch((err) => {
        console.log(err);
        throw err;
      });
  }

  // async _acquireOnlyConnection() {
  //   await this.poolConnection;
  //   const connection = await this.pool.connect();
  //   connection.__knexUid = Date.now();
  //   return connection;
  // }

  // Acquire a connection from the pool.
  async acquireConnection() {
    await this.poolConnection;
    const connection = await this.pool.connect();
    connection.__knexUid = Date.now();
    return connection;
  }

  async destroyRawConnection(connection: any) {
    // There is only one connection, if this one goes shut down the database
    await connection.close();
  }

  // async acquireConnection() {}

  // Releases a connection back to the connection pool,
  // returning a promise resolved when the connection is released.
  releaseConnection() {}

  // Destroy the current connection pool for the client.
  async destroy(callback) {
    try {
      if (this.pool) {
        await this.pool.close();
      }
      this.pool = undefined;

      if (typeof callback === "function") {
        callback();
      }
    } catch (err) {
      if (typeof callback === "function") {
        return callback(err);
      }
      throw err;
    }
  }

  async _query(connection: any, obj: any) {
    if (!obj.sql) throw new Error("The query is empty");
    const query = formatQuery(obj.sql, obj.bindings, "UTC", this);
    const response = await connection.query(query);
    obj.response = [response.slice(0, response.length), response.columns];
    return obj;
  }
}

const ClientAtlasSqlOdbc =
  ClientAtlasSqlOdbcImpl as unknown as typeof Knex.Client;
export default ClientAtlasSqlOdbc;
