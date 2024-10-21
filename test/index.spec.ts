import Knex from "knex";
import ClientAtlasSqlOdbc from "../src/index";
import { describe, it, expect, beforeAll } from "vitest";

describe("Basic tests", () => {
  let db: Knex.Knex;

  beforeAll(() => {
    db = Knex({
      client: ClientAtlasSqlOdbc,
      connection: "DSN=MongoDB_Atlas_SQL",
    });
  });

  it("should run sql operations in the engine", async () => {
    const result = await db.raw(
      `select "_id", "full_name" from "users" where "id" = 3006`
    );
    expect(result).toBeTruthy();
    expect(result[0]).toHaveLength(1);
    expect(result[1]).toHaveLength(2);
  });

  it("should be able to use query builder", async () => {
    const results = await db("users")
      .from("users")
      .select("id", "full_name")
      .where({ id: 3006 });
    expect(results).toBeTruthy();
    expect(results[0].id).toBe(3006);
  });
});
