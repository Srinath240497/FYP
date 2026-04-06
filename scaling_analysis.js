/**
 * Scaling / predictability analysis: Single-Node (sequential) vs High-Demand (concurrent at ceiling).
 *
 * For each database, collects 5,000 per-operation latencies under:
 *   - Single-Node: one client, strictly sequential ops.
 *   - High-Demand: many concurrent in-flight ops (default 300 virtual users; env/CLI 100–500).
 *
 * Metrics: Mean, sample standard deviation, P99 latency, PredictabilityScore = Mean / P99.
 *
 * Usage:
 *   node scaling_analysis.js --dataset "<name>"
 *   node scaling_analysis.js --pg-table t --mongo-collection c
 *   SCALING_CONCURRENT_USERS=400 node scaling_analysis.js --dataset foo
 */

const fs = require("fs");
const path = require("path");
const { performance } = require("perf_hooks");
const { Pool } = require("pg");
const { MongoClient } = require("mongodb");

require("dotenv").config({ path: path.resolve(process.cwd(), ".env") });
const { CONFIG } = require("./index.js");

const BATCH_OPS = 5000;
const MIN_CONCURRENT = 100;
const MAX_CONCURRENT = 500;
const DEFAULT_CONCURRENT = 300;
const OUT_DEFAULT = path.resolve(process.cwd(), "scaling_predictability.csv");

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

function clampConcurrent(n) {
  if (Number.isNaN(n) || n < MIN_CONCURRENT) return MIN_CONCURRENT;
  if (n > MAX_CONCURRENT) return MAX_CONCURRENT;
  return n;
}

function mean(arr) {
  if (!arr.length) return NaN;
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}

/** Sample standard deviation (Bessel's correction). */
function stdDevSample(arr) {
  if (arr.length < 2) return 0;
  const m = mean(arr);
  const ss = arr.reduce((s, x) => s + (x - m) ** 2, 0);
  return Math.sqrt(ss / (arr.length - 1));
}

/** P99: nearest-rank percentile on sorted ascending latencies (ms). */
function p99LatencySortedAsc(sorted) {
  const n = sorted.length;
  if (n === 0) return NaN;
  const k = Math.ceil(0.99 * n) - 1;
  return sorted[Math.min(n - 1, Math.max(0, k))];
}

function summarizeLatencies(latenciesMs) {
  const sorted = [...latenciesMs].sort((a, b) => a - b);
  const m = mean(latenciesMs);
  const sd = stdDevSample(latenciesMs);
  const p99 = p99LatencySortedAsc(sorted);
  const predictability = p99 > 0 ? m / p99 : NaN;
  return { mean: m, stdDev: sd, p99, predictability };
}

/**
 * Strictly sequential: one op after another.
 * @param {number} count
 * @param {() => Promise<void>} opFn
 */
async function runSequentialBatch(count, opFn) {
  const latencies = [];
  for (let i = 0; i < count; i += 1) {
    const t0 = performance.now();
    await opFn();
    latencies.push(performance.now() - t0);
  }
  return latencies;
}

/**
 * High-demand: `concurrency` workers each run ops until `count` total complete (interleaved I/O).
 * @param {number} count
 * @param {number} concurrency
 * @param {() => Promise<void>} opFn
 */
async function runConcurrentCeilingBatch(count, concurrency, opFn) {
  /** @type {number[]} */
  const latencies = [];
  let next = 0;

  async function worker() {
    while (true) {
      const idx = next;
      next += 1;
      if (idx >= count) return;
      const t0 = performance.now();
      await opFn();
      latencies.push(performance.now() - t0);
    }
  }

  await Promise.all(Array.from({ length: concurrency }, () => worker()));
  return latencies;
}

function writeCsv(outPath, rows) {
  const header =
    ["DB_Type", "Mode", "Mean", "StdDev", "P99", "PredictabilityScore"].join(",") + "\n";
  const lines = rows.map((r) =>
    [
      r.DB_Type,
      r.Mode,
      r.Mean,
      r.StdDev,
      r.P99,
      r.PredictabilityScore,
    ]
      .map(escapeCsv)
      .join(","),
  );
  fs.writeFileSync(outPath, header + lines.join("\n") + "\n", "utf8");
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
        '  --dataset "<CSV base name used in index.js>"\n' +
        "or:\n" +
        '  --pg-table "<postgres table>" --mongo-collection "<mongo collection>"',
    );
    process.exitCode = 2;
    return;
  }

  const concurrentRaw = Number(
    getArgValue("--concurrent-users", process.env.SCALING_CONCURRENT_USERS || String(DEFAULT_CONCURRENT)),
  );
  const concurrentUsers = clampConcurrent(concurrentRaw);

  if (concurrentRaw !== concurrentUsers) {
    console.warn(
      `Concurrent users ${concurrentRaw} clamped to ceiling band [${MIN_CONCURRENT}, ${MAX_CONCURRENT}] -> ${concurrentUsers}`,
    );
  }

  const outPath = path.resolve(process.cwd(), getArgValue("--out", OUT_DEFAULT));

  const pgPool = new Pool({
    ...CONFIG.postgres,
    max: Math.max(Number(CONFIG.postgres.max || 10), concurrentUsers),
  });
  const mongoUri = `mongodb://${CONFIG.mongo.host}:${CONFIG.mongo.port}`;
  const mongoClient = new MongoClient(mongoUri, {
    maxPoolSize: Math.max(Number(process.env.MONGO_POOL_MAX || 20), concurrentUsers),
    serverSelectionTimeoutMS: Number(process.env.MONGO_SERVER_SELECTION_TIMEOUT_MS || 10_000),
  });

  await mongoClient.connect();
  const mongoDb = mongoClient.db(CONFIG.mongo.db);
  const mongoCollection = mongoDb.collection(mongoCollectionName);

  const pgQuery = async () => {
    const sql = `SELECT * FROM ${quotePgIdent(pgTable)};`;
    await pgPool.query(sql);
  };

  const mongoQuery = async () => {
    await mongoCollection.find({}).toArray();
  };

  /** @type {Array<{DB_Type: string, Mode: string, Mean: string, StdDev: string, P99: string, PredictabilityScore: string}>} */
  const rows = [];

  try {
    console.log(
      `Single-Node (sequential): ${BATCH_OPS} ops each | High-Demand: ${BATCH_OPS} ops, concurrency=${concurrentUsers}`,
    );

    for (const { label, dbType, opFn } of [
      { label: "Postgres", dbType: "Postgres", opFn: pgQuery },
      { label: "MongoDB", dbType: "MongoDB", opFn: mongoQuery },
    ]) {
      console.log(`[${label}] Single-Node batch...`);
      const seqLat = await runSequentialBatch(BATCH_OPS, opFn);
      const seq = summarizeLatencies(seqLat);
      rows.push({
        DB_Type: dbType,
        Mode: "Single-Node",
        Mean: seq.mean.toFixed(6),
        StdDev: seq.stdDev.toFixed(6),
        P99: seq.p99.toFixed(6),
        PredictabilityScore: seq.predictability.toFixed(6),
      });

      console.log(`[${label}] High-Demand batch...`);
      const concLat = await runConcurrentCeilingBatch(BATCH_OPS, concurrentUsers, opFn);
      const conc = summarizeLatencies(concLat);
      rows.push({
        DB_Type: dbType,
        Mode: "High-Demand",
        Mean: conc.mean.toFixed(6),
        StdDev: conc.stdDev.toFixed(6),
        P99: conc.p99.toFixed(6),
        PredictabilityScore: conc.predictability.toFixed(6),
      });
    }
  } finally {
    await pgPool.end().catch(() => {});
    await mongoClient.close().catch(() => {});
  }

  writeCsv(outPath, rows);
  console.table(
    rows.map((r) => ({
      DB_Type: r.DB_Type,
      Mode: r.Mode,
      Mean_ms: r.Mean,
      StdDev_ms: r.StdDev,
      P99_ms: r.P99,
      Predictability: r.PredictabilityScore,
    })),
  );
  console.log(`Wrote ${outPath}`);
}

main().catch((err) => {
  console.error("Fatal error:", err?.stack || err);
  process.exitCode = 1;
});
