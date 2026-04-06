/**
 * Reliability threshold test: concurrent reads with error categorization.
 *
 * Loads DB endpoints from .env via dotenv (PG_HOST, MONGO_HOST, etc.).
 * Stress ladder: fixed concurrency levels per batch.
 * Uses Promise.allSettled + per-query try/catch so one failure does not abort the batch.
 */

const fs = require("fs");
const path = require("path");
const { performance } = require("perf_hooks");
const { Pool } = require("pg");
const { MongoClient } = require("mongodb");

require("dotenv").config({ path: path.resolve(process.cwd(), ".env") });
// CONFIG must be required after dotenv so process.env is populated.
const { CONFIG } = require("./index.js");

const STRESS_LADDER = [100, 200, 400, 600, 800, 1000];
const DEGRADATION_THRESHOLD_PERCENT = 1;
const REPORT_PATH = path.resolve(process.cwd(), "reliability_report.json");

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

/**
 * @param {unknown} err
 * @returns {'Timeout' | 'Deadlock' | 'Connection Refused' | 'Other'}
 */
function categorizeError(err) {
  const msg = String(
    err && typeof err === "object" && "message" in err ? err.message : err,
  ).toLowerCase();
  const code = err && typeof err === "object" && "code" in err ? err.code : undefined;
  const codeStr = code !== undefined && code !== null ? String(code) : "";

  if (
    code === "ECONNREFUSED" ||
    msg.includes("connection refused") ||
    msg.includes("connect econnrefused")
  ) {
    return "Connection Refused";
  }

  if (
    code === "ETIMEDOUT" ||
    code === "ESOCKETTIMEDOUT" ||
    codeStr === "57014" ||
    msg.includes("timeout") ||
    msg.includes("timed out") ||
    msg.includes("server selection timed out")
  ) {
    return "Timeout";
  }

  if (codeStr === "40P01" || msg.includes("deadlock")) {
    return "Deadlock";
  }

  return "Other";
}

/**
 * One virtual user: try/catch around the query, return outcome (never throws).
 * @param {() => Promise<void>} queryFn
 */
async function runOneUserQuery(queryFn) {
  try {
    await queryFn();
    return { ok: true, category: null };
  } catch (err) {
    return { ok: false, category: categorizeError(err), error: String(err?.message || err) };
  }
}

/**
 * @param {number} concurrency
 * @param {() => Promise<void>} queryFn
 */
async function runBatchWithAllSettled(concurrency, queryFn) {
  const wallStart = performance.now();
  const userPromises = Array.from({ length: concurrency }, () => runOneUserQuery(queryFn));
  const settled = await Promise.allSettled(userPromises);
  const wallEnd = performance.now();

  const outcomes = settled.map((s) => {
    if (s.status === "fulfilled") return s.value;
    return { ok: false, category: categorizeError(s.reason), error: String(s.reason?.message || s.reason) };
  });

  const failures = outcomes.filter((o) => !o.ok);
  const errorCount = failures.length;
  const errorRatePercent = concurrency === 0 ? 0 : (errorCount / concurrency) * 100;

  /** @type {Record<string, number>} */
  const categoryCounts = { Timeout: 0, Deadlock: 0, "Connection Refused": 0, Other: 0 };
  for (const f of failures) {
    if (f.category && categoryCounts[f.category] !== undefined) {
      categoryCounts[f.category] += 1;
    } else {
      categoryCounts.Other += 1;
    }
  }

  return {
    concurrency,
    wallTimeMs: wallEnd - wallStart,
    successCount: concurrency - errorCount,
    errorCount,
    errorRatePercent: Number(errorRatePercent.toFixed(4)),
    categoryCounts,
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
        '  --dataset "<CSV base name used in index.js>"\n' +
        "or:\n" +
        '  --pg-table "<postgres table>" --mongo-collection "<mongo collection>"',
    );
    process.exitCode = 2;
    return;
  }

  const pgPool = new Pool(CONFIG.postgres);
  const mongoUri = `mongodb://${CONFIG.mongo.host}:${CONFIG.mongo.port}`;
  const mongoClient = new MongoClient(mongoUri, {
    maxPoolSize: Number(process.env.MONGO_POOL_MAX || 20),
    serverSelectionTimeoutMS: Number(process.env.MONGO_SERVER_SELECTION_TIMEOUT_MS || 10_000),
  });

  await mongoClient.connect();
  const mongoDb = mongoClient.db(CONFIG.mongo.db);
  const mongoCollection = mongoDb.collection(mongoCollectionName);

  /** @type {typeof STRESS_LADDER[number] | null} */
  let pgFirstErrorConcurrency = null;
  /** @type {typeof STRESS_LADDER[number] | null} */
  let mongoFirstErrorConcurrency = null;
  /** @type {typeof STRESS_LADDER[number] | null} */
  let pgFirstDegradationConcurrency = null;
  /** @type {typeof STRESS_LADDER[number] | null} */
  let mongoFirstDegradationConcurrency = null;

  const pgBatches = [];
  const mongoBatches = [];

  try {
    for (const concurrency of STRESS_LADDER) {
      const pgBatch = await runBatchWithAllSettled(concurrency, async () => {
        const sql = `SELECT * FROM ${quotePgIdent(pgTable)};`;
        await pgPool.query(sql);
      });
      pgBatches.push(pgBatch);
      if (pgBatch.errorCount > 0 && pgFirstErrorConcurrency === null) {
        pgFirstErrorConcurrency = concurrency;
      }
      if (pgBatch.errorRatePercent > DEGRADATION_THRESHOLD_PERCENT && pgFirstDegradationConcurrency === null) {
        pgFirstDegradationConcurrency = concurrency;
      }

      const mongoBatch = await runBatchWithAllSettled(concurrency, async () => {
        await mongoCollection.find({}).toArray();
      });
      mongoBatches.push(mongoBatch);
      if (mongoBatch.errorCount > 0 && mongoFirstErrorConcurrency === null) {
        mongoFirstErrorConcurrency = concurrency;
      }
      if (
        mongoBatch.errorRatePercent > DEGRADATION_THRESHOLD_PERCENT &&
        mongoFirstDegradationConcurrency === null
      ) {
        mongoFirstDegradationConcurrency = concurrency;
      }

      console.log(
        `[Concurrency ${concurrency}] Postgres error rate: ${pgBatch.errorRatePercent}% | Mongo error rate: ${mongoBatch.errorRatePercent}%`,
      );
    }
  } finally {
    await pgPool.end().catch(() => {});
    await mongoClient.close().catch(() => {});
  }

  const report = {
    generatedAt: new Date().toISOString(),
    researchQuestion:
      "Reliability thresholds: concurrency at first error and at >1% error rate",
    env: {
      pgHost: CONFIG.postgres.host,
      mongoHost: CONFIG.mongo.host,
    },
    stressLadder: [...STRESS_LADDER],
    degradationThresholdPercent: DEGRADATION_THRESHOLD_PERCENT,
    postgres: {
      table: pgTable,
      firstErrorAtConcurrency: pgFirstErrorConcurrency,
      firstDegradationAtConcurrency: pgFirstDegradationConcurrency,
      batches: pgBatches,
    },
    mongodb: {
      collection: mongoCollectionName,
      firstErrorAtConcurrency: mongoFirstErrorConcurrency,
      firstDegradationAtConcurrency: mongoFirstDegradationConcurrency,
      batches: mongoBatches,
    },
  };

  fs.writeFileSync(REPORT_PATH, JSON.stringify(report, null, 2), "utf8");
  console.log(`Wrote ${REPORT_PATH}`);
}

main().catch((err) => {
  console.error("Fatal error:", err?.stack || err);
  process.exitCode = 1;
});
