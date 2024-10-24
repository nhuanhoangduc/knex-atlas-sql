const { createServer } = require("node:http");
const Knex = require("knex");
const ClientAtlasSqlOdbc = require("./dist");

const hostname = "127.0.0.1";
const port = 3000;
const db = Knex({
  client: ClientAtlasSqlOdbc.default,
  trino: {
    server: "http://localhost:8181",
    schema: "dev",
    catalog: "mongodb",
  },
});

const server = createServer(async (req, res) => {
  if (req.url.includes("favicon.ico")) {
    res.statusCode = 200;
    res.setHeader("Content-Type", "text/plain");
    res.end();
    return;
  }

  try {
    await Promise.all([
      db.raw(`select "_id", "full_name" from "users" where "id" = 3006`),
      db("users").select("id", "full_name").where({ id: 3006 }),
      db("artworks as a")
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
        .count("* as count"),
      db("testimonials as t").count("* as count").first(),
      db("users")
        .select("id", "full_name")
        .where({ full_name: "Khan" })
        .first(),
      db("users")
        .select("id", "full_name")
        .where({ full_name: "Khan", is_deleted: false })
        .first(),
      db("users").select("id", "full_name").limit(6),
      db("users")
        .select("id", "full_name")
        .orderBy([{ column: "id", order: "desc" }])
        .offset(3)
        .limit(6),
      db("users")
        .select("id", "full_name")
        .orderBy([{ column: "id", order: "desc" }])
        .offset(3),
    ]);
    res.statusCode = 200;
    res.setHeader("Content-Type", "text/plain");
    res.end("Hello World");
  } catch (error) {
    res.statusCode = 500;
    res.setHeader("Content-Type", "text/plain");
    res.end(error.message);
  }
});

server.listen(port, hostname, () => {
  console.log(`Server running at http://${hostname}:${port}/`);
});
