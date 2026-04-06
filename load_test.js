/**
 * Concurrency Load Test: PostgreSQL vs MongoDB
 *
 * Research goal:
 * - Measure how each database behaves under increasing concurrent read load.
 *
 * Metrics reported for each userCount:
 * - Total Wall Time (how long all virtual users take together)
 * - Throughput (operations per second)
 * - Average Latency (mean of individual query times)
 *
 * Output:
 * - Console table: Users | DB | Throughput (ops/s) | Avg Latency (ms)
 * - Appended CSV: concurrency_results.csv
 */

const fs = require("fs");
const path = require("path");
const { performance } = require("perf_hooks");
const { Pool } = require("pg");
const { MongoClient } = require("mongodb");
const { CONFIG } = require("./index.js");

function getArgValue(flagName, defaultValue) {
  const argv = process.argv.slice(2);
  const asEq = argv.find((a) => a.startsWith(`${flagName}=`));
  if (asEq) return asEq.slice(flagName.length + 1);
  const idx = argv.indexOf(flagName);
  if (idx !== -1 && idx + 1 < argv.length) return argv[idx + 1];
  return defaultValue;
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

function sanitizeResourceName(name, fallbackPrefix) {
  return sanitizeIdentifier(name, fallbackPrefix);
}

function quotePgIdent(identifier) {
  return `"${String(identifier).replace(/"/g, '""')}"`;
}

function escapeCsv(value) {
  const s = value === null || value === undefined ? "" : String(value);
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function ensureCsvHeader(csvPath) {
  if (fs.existsSync(csvPath)) return;
  const header = [
    "Timestamp",
    "Users",
    "DB",
    "TotalWallTimeMS",
    "ThroughputOpsPerSec",
    "AvgLatencyMS",
  ].join(",") + "\n";
  fs.writeFileSync(csvPath, header, "utf8");
}

function appendCsvRow(csvPath, row) {
  const line =
    [
      row.Timestamp,
      row.Users,
      row.DB,
      row.TotalWallTimeMS,
      row.ThroughputOpsPerSec,
      row.AvgLatencyMS,
    ]
      .map(escapeCsv)
      .join(",") + "\n";
  fs.appendFileSync(csvPath, line, "utf8");
}

/**
 * Simulate `userCount` concurrent virtual users with Promise.all.
 * Why this matters: Promise.all launches all tasks immediately from the app layer,
 * creating a consistent and reproducible contention pattern for concurrency analysis.
 */
async function runParallelTest(userCount, queryFn) {
  const wallStart = performance.now();

  const userTasks = Array.from({ length: userCount }, async () => {
    const t0 = performance.now();
    await queryFn();
    const t1 = performance.now();
    return t1 - t0;
  });

  const individualLatencies = await Promise.all(userTasks);
  const wallEnd = performance.now();

  const totalWallTimeMs = wallEnd - wallStart;
  const totalWallTimeSec = totalWallTimeMs / 1000;
  const sumLatencyMs = individualLatencies.reduce((a, b) => a + b, 0);
  const avgLatencyMs = sumLatencyMs / userCount;
  const throughputOpsPerSec = userCount / totalWallTimeSec;

  return {
    totalWallTimeMs,
    throughputOpsPerSec,
    avgLatencyMs,
  };
}

async function main() {
  const datasetName = getArgValue("--dataset", "");
  const pgTable = getArgValue("--pg-table", datasetName ? sanitizeResourceName(datasetName, "dataset") : "");
  const mongoCollectionName = getArgValue(
    "--mongo-collection",
    datasetName ? sanitizeResourceName(datasetName, "dataset") : "",
  );

  if (!pgTable || !mongoCollectionName) {
    console.error(
      "Missing dataset mapping. Provide either:\n" +
        "  --dataset \"<CSV base name used in index.js>\"\n" +
        "or:\n" +
        "  --pg-table \"<postgres table name>\" --mongo-collection \"<mongo collection name>\"",
    );
    process.exitCode = 2;
    return;
  }

  const csvPath = getArgValue(
    "--report",
    path.resolve(process.cwd(), "concurrency_results.csv"),
  );
  ensureCsvHeader(csvPath);

  // Fixed test loop as requested.
  const userCounts = [100, 250, 500, 750, 1000];

  // Reuse existing config to create long-lived clients for the full test.
  const pgPool = new Pool(CONFIG.postgres);
  const mongoUri = `mongodb://${CONFIG.mongo.host}:${CONFIG.mongo.port}`;
  const mongoClient = new MongoClient(mongoUri, {
    maxPoolSize: Number(process.env.MONGO_POOL_MAX || 20),
    serverSelectionTimeoutMS: Number(process.env.MONGO_SERVER_SELECTION_TIMEOUT_MS || 10_000),
  });

  await mongoClient.connect();
  const mongoDb = mongoClient.db(CONFIG.mongo.db);
  const mongoCollection = mongoDb.collection(mongoCollectionName);

  const rowsForConsole = [];

  async function runAndRecord({ userCount, dbName, queryFn }) {
    const metrics = await runParallelTest(userCount, queryFn);
    const row = {
      Timestamp: new Date().toISOString(),
      Users: userCount,
      DB: dbName,
      TotalWallTimeMS: metrics.totalWallTimeMs.toFixed(3),
      ThroughputOpsPerSec: metrics.throughputOpsPerSec.toFixed(3),
      AvgLatencyMS: metrics.avgLatencyMs.toFixed(3),
    };
    appendCsvRow(csvPath, row);
    rowsForConsole.push({
      Users: row.Users,
      DB: row.DB,
      "Throughput (ops/s)": row.ThroughputOpsPerSec,
      "Avg Latency (ms)": row.AvgLatencyMS,
    });
  }

  try {
    for (const userCount of userCounts) {
      // Standard read query for PostgreSQL.
      await runAndRecord({
        userCount,
        dbName: "Postgres",
        queryFn: async () => {
          const sql = `SELECT * FROM ${quotePgIdent(pgTable)};`;
          await pgPool.query(sql);
        },
      });

      // Standard read query for MongoDB.
      await runAndRecord({
        userCount,
        dbName: "MongoDB",
        queryFn: async () => {
          await mongoCollection.find({}).toArray();
        },
      });
    }
  } finally {
    await pgPool.end();
    await mongoClient.close();
  }

  console.table(rowsForConsole);
  console.log(`Saved concurrency results to: ${csvPath}`);
}

main().catch((err) => {
  console.error("Fatal error:", err?.stack || err);
  process.exitCode = 1;
});

