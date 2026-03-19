/**
 * Dual-insertion benchmark: PostgreSQL (pg) vs MongoDB (mongodb)
 * - Streams CSV (no full-file load)
 * - Batches rows into chunks of 1000
 * - Times Postgres insert then Mongo insert per chunk
 * - Appends per-chunk metrics to benchmark_results.csv
 *
 * Usage:
 *   node index.js /path/to/dataset.csv
 *
 * Env (optional):
 *   PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD
 *   MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_COLLECTION
 */

const fs = require("fs");
const path = require("path");
const csv = require("csv-parser");
const { Pool } = require("pg");
const { MongoClient } = require("mongodb");
const { performance } = require("perf_hooks");

const CONFIG = {
  chunkSize: Number(process.env.CHUNK_SIZE || 1000),
  progressEveryRecords: Number(process.env.PROGRESS_EVERY || 10000),
  resultsPath: process.env.RESULTS_PATH || path.resolve(process.cwd(), "benchmark_results.csv"),
  postgres: {
    host: process.env.PG_HOST || "13.53.135.220",
    port: Number(process.env.PG_PORT || 5432),
    database: process.env.PG_DATABASE || "benchmark_db",
    user: process.env.PG_USER || "srinath",
    password: process.env.PG_PASSWORD || undefined,
    max: Number(process.env.PG_POOL_MAX || 10),
    idleTimeoutMillis: Number(process.env.PG_IDLE_TIMEOUT_MS || 30_000),
    connectionTimeoutMillis: Number(process.env.PG_CONN_TIMEOUT_MS || 10_000),
  },
  mongo: {
    host: process.env.MONGO_HOST || "18.234.225.241",
    port: Number(process.env.MONGO_PORT || 27017),
    db: process.env.MONGO_DB || "benchmark_db",
    collection: process.env.MONGO_COLLECTION || "orders",
  },
};

const CSV_HEADER_MAP = {
  index: "index",
  "Order ID": "orderId",
  Date: "date",
  Status: "status",
  Fulfilment: "fulfilment",
  "Sales Channel": "salesChannel",
  "ship-service-level": "shipServiceLevel",
  Style: "style",
  SKU: "sku",
  Category: "category",
  Size: "size",
  ASIN: "asin",
  "Courier Status": "courierStatus",
  Qty: "qty",
  currency: "currency",
  Amount: "amount",
  "ship-city": "shipCity",
  "ship-state": "shipState",
  "ship-postal-code": "shipPostalCode",
  "ship-country": "shipCountry",
  "promotion-ids": "promotionIds",
  B2B: "b2b",
  "fulfilled-by": "fulfilledBy",
  "Unnamed: 22": "unnamed22",
};

function parseDate(value) {
  if (!value) return null;
  // Try ISO first, then fall back to Date parsing. If invalid, store null and keep raw in Mongo.
  const d = new Date(value);
  // eslint-disable-next-line no-restricted-globals
  if (isNaN(d.getTime())) return null;
  return d;
}

function parseIntOrNull(value) {
  if (value === undefined || value === null || value === "") return null;
  const n = Number.parseInt(String(value), 10);
  return Number.isFinite(n) ? n : null;
}

function parseFloatOrNull(value) {
  if (value === undefined || value === null || value === "") return null;
  const cleaned = String(value).replace(/,/g, "");
  const n = Number.parseFloat(cleaned);
  return Number.isFinite(n) ? n : null;
}

function parseBoolOrNull(value) {
  if (value === undefined || value === null || value === "") return null;
  const v = String(value).trim().toLowerCase();
  if (["true", "t", "1", "yes", "y"].includes(v)) return true;
  if (["false", "f", "0", "no", "n"].includes(v)) return false;
  return null;
}

function normalizeRow(raw) {
  const out = {};
  for (const [csvKey, outKey] of Object.entries(CSV_HEADER_MAP)) {
    out[outKey] = raw[csvKey];
  }

  // Type-normalized fields (kept in both DBs)
  return {
    index: parseIntOrNull(out.index),
    orderId: out.orderId || null,
    date: parseDate(out.date),
    status: out.status || null,
    fulfilment: out.fulfilment || null,
    salesChannel: out.salesChannel || null,
    shipServiceLevel: out.shipServiceLevel || null,
    style: out.style || null,
    sku: out.sku || null,
    category: out.category || null,
    size: out.size || null,
    asin: out.asin || null,
    courierStatus: out.courierStatus || null,
    qty: parseIntOrNull(out.qty),
    currency: out.currency || null,
    amount: parseFloatOrNull(out.amount),
    shipCity: out.shipCity || null,
    shipState: out.shipState || null,
    shipPostalCode: out.shipPostalCode || null,
    shipCountry: out.shipCountry || null,
    promotionIds: out.promotionIds || null,
    b2b: parseBoolOrNull(out.b2b),
    fulfilledBy: out.fulfilledBy || null,
    unnamed22: out.unnamed22 || null,
    _raw: raw, // preserve exact raw CSV row for Mongo analysis/auditing
  };
}

function toPostgresRow(doc) {
  // Relational schema mapping (flat, typed)
  return {
    index: doc.index,
    order_id: doc.orderId,
    order_date: doc.date ? doc.date.toISOString().slice(0, 10) : null, // date-only
    status: doc.status,
    fulfilment: doc.fulfilment,
    sales_channel: doc.salesChannel,
    ship_service_level: doc.shipServiceLevel,
    style: doc.style,
    sku: doc.sku,
    category: doc.category,
    size: doc.size,
    asin: doc.asin,
    courier_status: doc.courierStatus,
    qty: doc.qty,
    currency: doc.currency,
    amount: doc.amount,
    ship_city: doc.shipCity,
    ship_state: doc.shipState,
    ship_postal_code: doc.shipPostalCode,
    ship_country: doc.shipCountry,
    promotion_ids: doc.promotionIds,
    b2b: doc.b2b,
    fulfilled_by: doc.fulfilledBy,
    unnamed_22: doc.unnamed22,
  };
}

function escapeCsv(value) {
  const s = value === null || value === undefined ? "" : String(value);
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function ensureResultsHeader(resultsPath) {
  if (fs.existsSync(resultsPath)) return;
  const header =
    [
      "timestamp",
      "chunk_index",
      "chunk_records",
      "total_processed",
      "pg_ms",
      "mongo_ms",
      "pg_errors",
      "mongo_errors",
      "pg_avg_ms_per_record_chunk",
      "mongo_avg_ms_per_record_chunk",
    ].join(",") + "\n";
  fs.writeFileSync(resultsPath, header, "utf8");
}

async function ensurePostgresSchema(pool) {
  const ddl = `
    CREATE TABLE IF NOT EXISTS orders (
      id BIGSERIAL PRIMARY KEY,
      "index" INTEGER NULL,
      order_id TEXT NULL,
      order_date DATE NULL,
      status TEXT NULL,
      fulfilment TEXT NULL,
      sales_channel TEXT NULL,
      ship_service_level TEXT NULL,
      style TEXT NULL,
      sku TEXT NULL,
      category TEXT NULL,
      size TEXT NULL,
      asin TEXT NULL,
      courier_status TEXT NULL,
      qty INTEGER NULL,
      currency TEXT NULL,
      amount NUMERIC NULL,
      ship_city TEXT NULL,
      ship_state TEXT NULL,
      ship_postal_code TEXT NULL,
      ship_country TEXT NULL,
      promotion_ids TEXT NULL,
      b2b BOOLEAN NULL,
      fulfilled_by TEXT NULL,
      unnamed_22 TEXT NULL
    );
  `;
  await pool.query(ddl);
}

async function insertChunkPostgres(pool, pgRows) {
  if (pgRows.length === 0) return;

  const cols = [
    "index",
    "order_id",
    "order_date",
    "status",
    "fulfilment",
    "sales_channel",
    "ship_service_level",
    "style",
    "sku",
    "category",
    "size",
    "asin",
    "courier_status",
    "qty",
    "currency",
    "amount",
    "ship_city",
    "ship_state",
    "ship_postal_code",
    "ship_country",
    "promotion_ids",
    "b2b",
    "fulfilled_by",
    "unnamed_22",
  ];

  const values = [];
  const placeholders = [];
  let p = 1;
  for (const r of pgRows) {
    const rowPlaceholders = [];
    for (const c of cols) {
      values.push(r[c]);
      rowPlaceholders.push(`$${p++}`);
    }
    placeholders.push(`(${rowPlaceholders.join(",")})`);
  }

  const sql = `INSERT INTO orders (${cols.map((c) => `"${c}"`).join(",")}) VALUES ${placeholders.join(",")};`;
  await pool.query(sql, values);
}

async function ensureMongoSchema(mongoCollection) {
  // Ensure an index helpful for typical analyses (optional, harmless if exists)
  // Not required for correctness; keep minimal.
  try {
    await mongoCollection.createIndex({ orderId: 1 });
  } catch {
    // ignore index errors (permissions, etc.)
  }
}

async function insertChunkMongo(mongoCollection, docs) {
  if (docs.length === 0) return;
  await mongoCollection.insertMany(docs, { ordered: false });
}

async function main() {
  const inputCsvPath = process.argv[2];
  if (!inputCsvPath) {
    console.error("Usage: node index.js /path/to/dataset.csv");
    process.exitCode = 2;
    return;
  }

  ensureResultsHeader(CONFIG.resultsPath);

  const pool = new Pool(CONFIG.postgres);

  const mongoUri = `mongodb://${CONFIG.mongo.host}:${CONFIG.mongo.port}`;
  const mongoClient = new MongoClient(mongoUri, {
    // keep defaults; you can tune later for research
    maxPoolSize: Number(process.env.MONGO_POOL_MAX || 20),
    serverSelectionTimeoutMS: Number(process.env.MONGO_SERVER_SELECTION_TIMEOUT_MS || 10_000),
  });

  let shuttingDown = false;
  const shutdown = async (reason) => {
    if (shuttingDown) return;
    shuttingDown = true;
    if (reason) console.error(reason);
    try {
      await pool.end();
    } catch {}
    try {
      await mongoClient.close();
    } catch {}
  };

  process.on("SIGINT", () => {
    shutdown("Received SIGINT, closing connections...").finally(() => process.exit(130));
  });
  process.on("SIGTERM", () => {
    shutdown("Received SIGTERM, closing connections...").finally(() => process.exit(143));
  });

  await ensurePostgresSchema(pool);
  await mongoClient.connect();
  const mongoDb = mongoClient.db(CONFIG.mongo.db);
  const mongoCollection = mongoDb.collection(CONFIG.mongo.collection);
  await ensureMongoSchema(mongoCollection);

  const overallStart = performance.now();

  let buffer = [];
  let chunkIndex = 0;
  let totalProcessed = 0;
  let pgErrors = 0;
  let mongoErrors = 0;
  let pgTotalMs = 0;
  let mongoTotalMs = 0;

  const inputStream = fs.createReadStream(inputCsvPath);
  const parser = csv({
    // csv-parser uses the first line as headers automatically; we keep them as-is.
    skipLines: 0,
    mapHeaders: ({ header }) => (header ? header.trim() : header),
  });

  const csvStream = inputStream.pipe(parser);

  const appendResultLine = (lineObj) => {
    const line =
      [
        lineObj.timestamp,
        lineObj.chunk_index,
        lineObj.chunk_records,
        lineObj.total_processed,
        lineObj.pg_ms,
        lineObj.mongo_ms,
        lineObj.pg_errors,
        lineObj.mongo_errors,
        lineObj.pg_avg_ms_per_record_chunk,
        lineObj.mongo_avg_ms_per_record_chunk,
      ]
        .map(escapeCsv)
        .join(",") + "\n";
    fs.appendFileSync(CONFIG.resultsPath, line, "utf8");
  };

  const processChunk = async (docsChunk) => {
    const ts = new Date().toISOString();
    const thisChunkIndex = ++chunkIndex;
    const chunkRecords = docsChunk.length;

    const pgRows = docsChunk.map(toPostgresRow);

    let pgMs = 0;
    let mongoMs = 0;

    // Postgres timing
    try {
      const t0 = performance.now();
      await insertChunkPostgres(pool, pgRows);
      const t1 = performance.now();
      pgMs = t1 - t0;
      pgTotalMs += pgMs;
    } catch (err) {
      pgErrors += 1;
      console.error(`[Chunk ${thisChunkIndex}] Postgres insert error:`, err?.message || err);
    }

    // Mongo timing
    try {
      const t0 = performance.now();
      await insertChunkMongo(mongoCollection, docsChunk);
      const t1 = performance.now();
      mongoMs = t1 - t0;
      mongoTotalMs += mongoMs;
    } catch (err) {
      mongoErrors += 1;
      console.error(`[Chunk ${thisChunkIndex}] Mongo insert error:`, err?.message || err);
    }

    totalProcessed += chunkRecords;

    appendResultLine({
      timestamp: ts,
      chunk_index: thisChunkIndex,
      chunk_records: chunkRecords,
      total_processed: totalProcessed,
      pg_ms: pgMs.toFixed(3),
      mongo_ms: mongoMs.toFixed(3),
      pg_errors: pgErrors,
      mongo_errors: mongoErrors,
      pg_avg_ms_per_record_chunk: chunkRecords ? (pgMs / chunkRecords).toFixed(6) : "",
      mongo_avg_ms_per_record_chunk: chunkRecords ? (mongoMs / chunkRecords).toFixed(6) : "",
    });

    if (totalProcessed % CONFIG.progressEveryRecords === 0) {
      console.log(`Processed ${totalProcessed.toLocaleString()} records...`);
    }
  };

  return new Promise((resolve, reject) => {
    csvStream.on("data", async (rawRow) => {
      csvStream.pause();
      try {
        buffer.push(normalizeRow(rawRow));
        if (buffer.length >= CONFIG.chunkSize) {
          const chunk = buffer;
          buffer = [];
          await processChunk(chunk);
        }
        csvStream.resume();
      } catch (err) {
        csvStream.destroy(err);
      }
    });

    csvStream.on("end", async () => {
      try {
        if (buffer.length > 0) {
          await processChunk(buffer);
          buffer = [];
        }

        const overallEnd = performance.now();
        const totalMs = overallEnd - overallStart;

        const pgAvg = totalProcessed ? pgTotalMs / totalProcessed : 0;
        const mongoAvg = totalProcessed ? mongoTotalMs / totalProcessed : 0;

        console.log("Benchmark complete.");
        console.log(`Total records: ${totalProcessed.toLocaleString()}`);
        console.log(`Total wall time: ${totalMs.toFixed(2)} ms`);
        console.log(`Postgres total insert time: ${pgTotalMs.toFixed(2)} ms (avg ${pgAvg.toFixed(6)} ms/record), errors: ${pgErrors}`);
        console.log(`Mongo total insert time: ${mongoTotalMs.toFixed(2)} ms (avg ${mongoAvg.toFixed(6)} ms/record), errors: ${mongoErrors}`);

        await shutdown();
        resolve();
      } catch (err) {
        await shutdown(err);
        reject(err);
      }
    });

    csvStream.on("error", async (err) => {
      await shutdown(err);
      reject(err);
    });
  });
}

main().catch((err) => {
  console.error("Fatal error:", err?.stack || err);
  process.exitCode = 1;
});

