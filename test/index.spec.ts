import Knex from "knex";
import ClientAtlasSqlOdbc from "../src/index";
import { describe, it, expect, beforeAll } from "vitest";

describe("Basic tests", () => {
  let db: Knex.Knex;

  beforeAll(() => {
    db = Knex({
      client: ClientAtlasSqlOdbc,
      trino: {
        server: "http://localhost:8080",
        schema: "cohart_dev",
        catalog: "mongodb",
      },
    });
  });

  it("should run sql operations in the engine", async () => {
    const result = await db.raw(
      `select "_id", "full_name" from "users" where "id" = 3006`
    );
    expect(result).toBeTruthy();
    expect(result).toHaveLength(1);
  });

  it("should be able to use query builder", async () => {
    const results = await db("users")
      .select("id", "full_name")
      .where({ id: 3006 });
    expect(results).toBeTruthy();
    expect(results[0].id).toBe(3006);
  });

  it("should be able to join tables", async () => {
    const results = await db("artworks as a")
      .leftJoin("artwork_users as au", (join) =>
        join.on("au.artwork_id", "a.id").andOnVal("au.type", "creator")
      )
      .where({
        "a.is_sold": true,
        "au.is_hidden_on_profile": false,
      })
      .whereIn("a.artwork_creator_id", 1)
      .whereNotIn("a.id", 2)
      .groupBy("a.artwork_creator_id")
      .select("a.artwork_creator_id as userId")
      .count("* as count");
    expect(results).toHaveLength(0);
  });

  it("Should able to count", async () => {
    const result = await db("testimonials as t")
      .count("* as count")
      .where("t.receiver_id", 3006)
      .first();
    expect(result[0].count).toBe(0);
  });
});
