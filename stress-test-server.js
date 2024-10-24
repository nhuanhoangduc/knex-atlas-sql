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
    const results = await Promise.all([
      db.raw(
        `with "collector_artwork" as (select "u"."id", "u"."full_name" as "fullName", "u"."email", "u"."username", "u"."subtitle", "u"."is_verified" as "isVerified", "u"."cover_image" as "coverImage", "u"."cover_image_medium" as "coverImageMedium", "u"."location", "a"."id" as "artworkId", "a"."title", "a"."user_id" as "userId", "ai"."image_original" as "imageOriginal", "ai"."image_original_width" as "imageOriginalWidth", "ai"."image_original_height" as "imageOriginalHeight", "ai"."image_small" as "imageSmall", "ai"."image_small_width" as "imageSmallWidth", "ai"."image_small_height" as "imageSmallHeight", row_number() over(partition by u.id  order by a.id desc) as "rowNumber" from "link_bio_collectors" as "lbc" inner join "users" as "u" on "lbc"."collector_id" = "u"."id" inner join "artwork_users" as "au" on "lbc"."collector_id" = "au"."user_id" inner join "artworks" as "a" on "au"."artwork_id" = "a"."id" left join "artwork_images" as "ai" on "a"."id" = "ai"."artwork_id" and "ai"."is_main" = TRUE where "lbc"."user_id" = 938 and "a"."is_deleted" = FALSE and "a"."id" in (select "id" from "artworks" as "a2" where "a2"."artwork_creator_id" = 938) order by "lbc"."id" asc) select * from "collector_artwork" where "rowNumber" = 1 limit 5`
      ),
      db.raw(
        `select "artworks"."id" from "artworks" where "artwork_creator_id" in (1462, 2045, 822, 2725, 1223, 1249, 1603, 2386, 1699, 1341, 2003, 1500, 2116, 2345, 2560, 3645, 3641, 529, 3809, 3775, 3920, 3966, 1486, 2744, 1530, 4011, 1488, 1492, 2005, 960, 567, 4104) order by "artworks"."created_at" DESC limit 10`
      ),
      db.raw(
        `select "a"."artwork_creator_id" as "userId", count(*) as count from "artworks" as "a" inner join "marketplace_order_items" as "i" on "i"."artwork_id" = "a"."id" inner join "marketplace_orders" as "o" on "o"."id" = "i"."order_id" inner join "transactions" as "t" on "t"."order_id" = "o"."id" where "t"."payment_status" = 'succeeded' and "a"."price" > 0 and "a"."id" in (select "a"."id" as "artworkId" from "artworks" as "a" inner join "artwork_users" as "au" on "au"."artwork_id" = "a"."id" and "au"."type" = 'creator' where "au"."is_hidden_on_profile" = FALSE and "a"."artwork_creator_id" in (938)) group by "a"."artwork_creator_id"`
      ),

      db.raw(
        `select * from ((select "mo"."id" as "id", 'marketplace' as type, "a"."id" as "artworkId", "am"."image_original" as "imageOriginal", "am"."image_original_width" as "imageOriginalWidth", "am"."image_original_height" as "imageOriginalHeight", "am"."image_small" as "imageSmall", "am"."image_small_width" as "imageSmallWidth", "am"."image_small_height" as "imageSmallHeight", "am"."image_medium" as "imageMedium", "am"."image_medium_width" as "imageMediumWidth", "am"."image_medium_height" as "imageMediumHeight", "am"."image_large" as "imageLarge", "am"."image_large_width" as "imageLargeWidth", "am"."image_large_height" as "imageLargeHeight", "am"."image_full" as "imageFull", "am"."image_full_width" as "imageFullWidth", "am"."image_full_height" as "imageFullHeight", "a"."title" as "artworkTitle", "a"."user_id" as "createdById", CONCAT(mo.first_name, ' ', mo.last_name) as "artworkCollectorName", "mo"."created_at" as "saleDate", "mo"."delivery_method" as "deliveryMethod", "u"."full_name" as "collector", "u"."id" as "collector_id", "u"."cover_image" as "collector_cover_image", "u"."username" as "collector_username", "a"."updated_at" as "artworkUpdatedAt", "mo"."code" as "orderCode", "s"."tracking_url" as "orderShippingTrackingUrl", "s"."shipping_status" as "orderShippingStatus", "s"."total_amount" as "orderShippingPrice", "a"."price" as "artworkPrice", "t"."payment_status" as "paymentStatus" from "marketplace_order_items" as "mot" left join "marketplace_orders" as "mo" on "mo"."id" = "mot"."order_id" left join "transactions" as "t" on "t"."order_id" = "mot"."order_id" left join "artworks" as "a" on "a"."id" = "mot"."artwork_id" and "a"."is_nft" = FALSE left join "artwork_images" as "am" on "am"."artwork_id" = "a"."id" and "am"."is_main" = TRUE left join "shippings" as "s" on "s"."order_id" = "mot"."order_id" left join "users" as "u" on "u"."id" = "mo"."user_id" where "mot"."type" = 'artwork' and "mot"."artwork_id" in (select "a1"."id" from "artworks" as "a1" where "a1"."user_id" = 938) and "mot"."artwork_id" not in (select "artwork_id" from "artwork_users" where "user_id" = 938 and "is_hidden_on_profile" = TRUE) and "t"."payment_status" = 'succeeded' order by "mo"."created_at" desc) union (select "s1"."id" as "id", 'manual' as type, "a"."id" as "artworkId", "am"."image_original" as "imageOriginal", "am"."image_original_width" as "imageOriginalWidth", "am"."image_original_height" as "imageOriginalHeight", "am"."image_small" as "imageSmall", "am"."image_small_width" as "imageSmallWidth", "am"."image_small_height" as "imageSmallHeight", "am"."image_medium" as "imageMedium", "am"."image_medium_width" as "imageMediumWidth", "am"."image_medium_height" as "imageMediumHeight", "am"."image_large" as "imageLarge", "am"."image_large_width" as "imageLargeWidth", "am"."image_large_height" as "imageLargeHeight", "am"."image_full" as "imageFull", "am"."image_full_width" as "imageFullWidth", "am"."image_full_height" as "imageFullHeight", "a"."title" as "artworkTitle", "a"."user_id" as "createdById", "a"."artwork_collector_name" as "artworkCollectorName", "s1"."sale_date" as "saleDate", null as deliveryMethod, "u"."full_name" as "collector", "u"."id" as "collector_id", "u"."cover_image" as "collector_cover_image", "u"."username" as "collector_username", "a"."updated_at" as "artworkUpdatedAt", null as orderCode, null as orderShippingTrackingUrl, null as orderShippingStatus, null as orderShippingPrice, "a"."price" as "artworkPrice", null as paymentStatus from "sales" as "s1" left join "users" as "u" on "u"."id" = "s1"."collector_id" left join "artworks" as "a" on "a"."id" = "s1"."artwork_id" left join "artwork_images" as "am" on "am"."artwork_id" = "a"."id" and "am"."is_main" = TRUE where "s1"."user_id" = 938 and "a"."price" > 0 and "s1"."artwork_id" not in (select "artwork_id" from "artwork_users" where "user_id" = 938 and "is_hidden_on_profile" = TRUE) order by "s1"."sale_date" desc, "a"."updated_at" desc)) as "result" order by "result"."saleDate" desc, "result"."artworkUpdatedAt" desc limit 5`
      ),
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

// echo 'GET http://127.0.0.1:3000' | vegeta attack -rate=10 -duration=30s | tee results.bin | vegeta report
