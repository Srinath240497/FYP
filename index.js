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
    password: process.env.PG_PASSWORD || "Chill@123",
    max: Number(process.env.PG_POOL_MAX || 10),
    idleTimeoutMillis: Number(process.env.PG_IDLE_TIMEOUT_MS || 30_000),
    connectionTimeoutMillis: Number(process.env.PG_CONN_TIMEOUT_MS || 10_000),
  },
  mongo: {
    host: process.env.MONGO_HOST || "13.50.245.233",
    port: Number(process.env.MONGO_PORT || 27017),
    db: process.env.MONGO_DB || "benchmark_db",
  },
};

function quotePgIdent(identifier) {
  return `"${String(identifier).replace(/"/g, '""')}"`;
}

function sanitizeIdentifier(value, fallbackPrefix) {
  const lower = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
  const withFallback = lower || fallbackPrefix;
  if (/^[0-9]/.test(withFallback)) return `${fallbackPrefix}_${withFallback}`;
  return withFallback;
}

function sanitizePostgresColumnName(header, index) {
  return sanitizeIdentifier(header, `col_${index + 1}`);
}

function sanitizeResourceName(name, fallbackPrefix) {
  return sanitizeIdentifier(name, fallbackPrefix);
}

function buildUniquePostgresColumns(csvHeaders) {
  const used = new Set();
  return csvHeaders.map((header, idx) => {
    const base = sanitizePostgresColumnName(header, idx);
    let candidate = base;
    let n = 2;
    while (used.has(candidate)) {
      candidate = `${base}_${n}`;
      n += 1;
    }
    used.add(candidate);
    return candidate;
  });
}

async function detectCsvHeaders(inputCsvPath) {
  return new Promise((resolve, reject) => {
    const input = fs.createReadStream(inputCsvPath);
    const parser = csv({
      skipLines: 0,
      mapHeaders: ({ header }) => (header ? header.trim() : header),
    });

    let settled = false;
    const finish = (fn) => (arg) => {
      if (settled) return;
      settled = true;
      input.destroy();
      parser.destroy();
      fn(arg);
    };

    parser.on("headers", finish(resolve));
    parser.on("error", finish(reject));
    input.on("error", finish(reject));
    parser.on(
      "end",
      finish(() => reject(new Error("CSV file has no header row."))),
    );

    input.pipe(parser);
  });
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

async function ensurePostgresSchema(pool, tableName, pgColumns) {
  const quotedTable = quotePgIdent(tableName);
  const colsDDL = pgColumns.map((c) => `${quotePgIdent(c)} TEXT`).join(", ");
  await pool.query(`DROP TABLE IF EXISTS ${quotedTable};`);
  await pool.query(`CREATE TABLE IF NOT EXISTS ${quotedTable} (${colsDDL});`);
}

async function insertChunkPostgres(pool, tableName, pgColumns, csvHeaders, docsChunk) {
  if (docsChunk.length === 0) return;
  const values = [];
  const placeholders = [];
  let p = 1;
  for (const doc of docsChunk) {
    const rowPlaceholders = [];
    for (const header of csvHeaders) {
      values.push(doc[header] ?? null);
      rowPlaceholders.push(`$${p++}`);
    }
    placeholders.push(`(${rowPlaceholders.join(",")})`);
  }

  const sql = `INSERT INTO ${quotePgIdent(tableName)} (${pgColumns.map((c) => quotePgIdent(c)).join(",")}) VALUES ${placeholders.join(",")};`;
  await pool.query(sql, values);
}

async function ensureMongoSchema(mongoCollection) {
  try {
    await mongoCollection.drop();
  } catch (err) {
    if (err?.codeName !== "NamespaceNotFound") throw err;
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

  const csvHeaders = await detectCsvHeaders(inputCsvPath);
  if (!Array.isArray(csvHeaders) || csvHeaders.length === 0) {
    throw new Error("CSV file has no headers; cannot build dynamic schema.");
  }

  const csvBaseName = path.basename(inputCsvPath, path.extname(inputCsvPath));
  const tableName = sanitizeResourceName(csvBaseName, "dataset");
  const collectionName = sanitizeResourceName(csvBaseName, "dataset");
  const pgColumns = buildUniquePostgresColumns(csvHeaders);

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

  await ensurePostgresSchema(pool, tableName, pgColumns);
  await mongoClient.connect();
  const mongoDb = mongoClient.db(CONFIG.mongo.db);
  const mongoCollection = mongoDb.collection(collectionName);
  await ensureMongoSchema(mongoCollection);

  // Start benchmark timing only after DB objects are reset/created.
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

    let pgMs = 0;
    let mongoMs = 0;

    // Postgres timing
    try {
      const t0 = performance.now();
      await insertChunkPostgres(pool, tableName, pgColumns, csvHeaders, docsChunk);
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
        buffer.push(rawRow);
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

// Allow other scripts to import CONFIG without accidentally running this benchmark.
if (require.main === module) {
  main().catch((err) => {
    console.error("Fatal error:", err?.stack || err);
    process.exitCode = 1;
  });
}

module.exports = {
  CONFIG,
};

