/**
 * Query Benchmark (PostgreSQL vs MongoDB) for AWS `benchmark_db`
 *
 * Purpose (for MSc Dissertation):
 * - Compare cold vs warm cache behavior by contrasting the first execution
 *   round latency vs later rounds (2-10). We cannot fully flush DB/OS
 *   caches from a user script without privileged access, so this is an
 *   operationally practical approximation of "first-run" vs "steady-state".
 * - Measure scalability under complex filtering (range + categorical filter).
 * - Benchmark aggregation performance to highlight relational GROUP BY +
 *   SUM vs MongoDB Aggregation Framework.
 *
 * Outputs:
 * - `query_performance_report.csv` with columns:
 *   QueryType,Database,ExecutionTimeMS,Timestamp
 *
 * Usage examples:
 * - node query_benchmark.js --pg-table "international_sales_report"
 *                          --mongo-collection "international_sales_report"
 *                          --dataset "international sales report"
 *
 * - Or, derive table/collection from `--dataset` (CSV base name used by index.js):
 *   node query_benchmark.js --dataset "International sale Report"
 *
 * Notes:
 * - index.js stores all Postgres values as TEXT, so numeric comparisons and SUM
 *   require conversion from TEXT to numeric at query time.
 * - SELECT * queries can be expensive for large datasets. Adjust limits via
 *   optional flags if your dataset is very large.
 */

const fs = require("fs");
const path = require("path");
const { performance } = require("perf_hooks");
const { Pool } = require("pg");
const { MongoClient } = require("mongodb");

// Reuse the CONFIG object from your existing insertion benchmark.
const { CONFIG } = require("./index.js");

function getArgValue(flagName, defaultValue) {
  // Supports:
  //   --flag=value
  //   --flag value
  const argv = process.argv.slice(2);
  const asEq = argv.find((a) => a.startsWith(`${flagName}=`));
  if (asEq) return asEq.slice(flagName.length + 1);
  const idx = argv.indexOf(flagName);
  if (idx !== -1 && idx + 1 < argv.length) return argv[idx + 1];
  return defaultValue;
}

function escapeCsv(value) {
  const s = value === null || value === undefined ? "" : String(value);
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

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

function sanitizeResourceName(name, fallbackPrefix) {
  return sanitizeIdentifier(name, fallbackPrefix);
}

function sanitizePostgresColumnNameFromHeader(header) {
  // index.js uses sanitizePostgresColumnName(header, index), which falls back to
  // col_{index+1} only if the sanitized header becomes empty.
  // In practice, for well-formed column headers (like "Total Profit"),
  // the sanitized form is stable (e.g., total_profit).
  return sanitizeIdentifier(header, "col");
}

function numericTextToPgDoubleExpr(pgColumnIdent) {
  // Why: index.js stores numeric-like values as TEXT. For fair filtering/SUM,
  // we convert TEXT to numeric inside the SQL query.
  // - regexp_replace strips currency symbols and thousands separators.
  // - NULLIF(...,'') prevents errors on empty strings.
  const cleaned = `regexp_replace(${pgColumnIdent}, '[^0-9\\.-]', '', 'g')`;
  return `NULLIF(${cleaned}, '')::numeric`;
}

function buildMongoConvertToDoubleExpr(fieldPath) {
  // Why: MongoDB comparisons/SUM need numeric coercion when source values
  // are stored as strings (which matches index.js behavior).
  return {
    $convert: {
      input: fieldPath,
      to: "double",
      onError: null,
      onNull: null,
    },
  };
}

async function withTimeout(promise, timeoutMs) {
  let timer;
  const timeoutPromise = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`Timed out after ${timeoutMs}ms`)), timeoutMs);
  });
  try {
    return await Promise.race([promise, timeoutPromise]);
  } finally {
    clearTimeout(timer);
  }
}

async function runConcurrentUsers(concurrency, taskFactory) {
  const tasks = [];
  for (let userId = 1; userId <= concurrency; userId++) {
    tasks.push(
      (async () => {
        const execStart = performance.now();
        await taskFactory(userId);
        const execEnd = performance.now();
        // Why: timestamp should reflect when the measured work completes,
        // so it aligns better with ExecutionTimeMS in logs and CSV.
        const finished = new Date().toISOString();
        return { execMs: execEnd - execStart, timestamp: finished };
      })(),
    );
  }
  return Promise.all(tasks);
}

function appendCsvRows(reportPath, rows) {
  // rows: [{ QueryType, Database, ExecutionTimeMS, Timestamp }]
  const lines = rows
    .map(
      (r) =>
        [r.QueryType, r.Database, r.ExecutionTimeMS, r.Timestamp].map(escapeCsv).join(",") + "\n",
    )
    .join("");
  fs.appendFileSync(reportPath, lines, "utf8");
}

function initReport(reportPath) {
  const header = ["QueryType", "Database", "ExecutionTimeMS", "Timestamp"].join(",") + "\n";
  fs.writeFileSync(reportPath, header, "utf8");
}

async function createPostgresPool(pgConfig) {
  return new Pool(pgConfig);
}

async function createMongoClientAndCollection({ mongoConfig, dbName, collectionName }) {
  const mongoUri = `mongodb://${mongoConfig.host}:${mongoConfig.port}`;
  const mongoClient = new MongoClient(mongoUri, {
    maxPoolSize: Number(process.env.MONGO_POOL_MAX || 20),
    serverSelectionTimeoutMS: Number(process.env.MONGO_SERVER_SELECTION_TIMEOUT_MS || 10_000),
  });
  await mongoClient.connect();
  const mongoDb = mongoClient.db(dbName);
  const mongoCollection = mongoDb.collection(collectionName);
  return { mongoClient, mongoCollection };
}

async function runSelectAllPostgres({ pool, pgTable, queryTimeoutMs }) {
  const sql = `SELECT * FROM ${quotePgIdent(pgTable)};`;
  await withTimeout(pool.query(sql), queryTimeoutMs);
}

async function runSelectAllMongo({ mongoCollection, queryTimeoutMs }) {
  // Why: SELECT * / find() are intentionally included to reflect end-to-end
  // read + result transfer costs, not just index lookup time.
  await withTimeout(mongoCollection.find({}).toArray(), queryTimeoutMs);
}

async function runComplexFilterPostgres({
  pool,
  pgTable,
  pgTotalProfitColumn,
  pgRegionColumn,
  profitThreshold,
  regionValue,
  queryTimeoutMs,
}) {
  const totalProfitIdent = quotePgIdent(pgTotalProfitColumn);
  const regionIdent = quotePgIdent(pgRegionColumn);
  const profitNumExpr = numericTextToPgDoubleExpr(totalProfitIdent);

  const sql = `SELECT * FROM ${quotePgIdent(pgTable)}
WHERE ${profitNumExpr} > $1 AND ${regionIdent} = $2;`;

  await withTimeout(pool.query(sql, [profitThreshold, regionValue]), queryTimeoutMs);
}

async function runComplexFilterMongo({
  mongoCollection,
  profitField,
  regionField,
  profitThreshold,
  regionValue,
  queryTimeoutMs,
}) {
  const profitPath = `$${profitField}`;
  const regionPath = `$${regionField}`;

  const filter = {
    // Why: $expr lets MongoDB evaluate an expression-based numeric comparison
    // at query time (required because index.js stores values as strings).
    $expr: {
      $and: [
        { $gt: [buildMongoConvertToDoubleExpr(profitPath), profitThreshold] },
        { $eq: [regionPath, regionValue] },
      ],
    },
  };

  await withTimeout(mongoCollection.find(filter).toArray(), queryTimeoutMs);
}

async function runAggregationPostgres({
  pool,
  pgTable,
  pgCategoryColumn,
  pgSumColumn,
  queryTimeoutMs,
}) {
  const categoryIdent = quotePgIdent(pgCategoryColumn);
  const sumIdent = quotePgIdent(pgSumColumn);
  const sumNumExpr = numericTextToPgDoubleExpr(sumIdent);

  const sql = `SELECT ${categoryIdent} AS category, SUM(${sumNumExpr}) AS total_sum
FROM ${quotePgIdent(pgTable)}
GROUP BY ${categoryIdent};`;

  await withTimeout(pool.query(sql), queryTimeoutMs);
}

async function runAggregationMongo({
  mongoCollection,
  categoryField,
  sumField,
  queryTimeoutMs,
}) {
  // Why: GROUP BY + SUM is a classic comparison point for relational optimizers
  // vs Mongo's aggregation execution engine.
  const pipeline = [
    {
      $group: {
        _id: `$${categoryField}`,
        total_sum: {
          $sum: {
            $convert: {
              input: `$${sumField}`,
              to: "double",
              onError: 0,
              onNull: 0,
            },
          },
        },
      },
    },
  ];

  await withTimeout(mongoCollection.aggregate(pipeline, { allowDiskUse: true }).toArray(), queryTimeoutMs);
}

async function main() {
  const concurrency = Number(getArgValue("--concurrency", 5));
  const roundsColdWarm = Number(getArgValue("--coldwarm-rounds", 10));
  const selectAllQueryTimeoutMs = Number(getArgValue("--selectall-timeout-ms", 60_000));
  const complexFilterQueryTimeoutMs = Number(getArgValue("--complex-timeout-ms", 60_000));
  const aggregationQueryTimeoutMs = Number(getArgValue("--agg-timeout-ms", 60_000));

  const datasetName = getArgValue("--dataset", "");
  const pgTable = getArgValue("--pg-table", datasetName ? sanitizeResourceName(datasetName, "dataset") : "");
  const mongoCollection = getArgValue(
    "--mongo-collection",
    datasetName ? sanitizeResourceName(datasetName, "dataset") : "",
  );

  if (!pgTable || !mongoCollection) {
    console.error(
      "Missing dataset mapping. Provide either:\n" +
        "  --dataset \"<CSV base name used in index.js>\" \n" +
        "or:\n" +
        "  --pg-table \"<postgres table name>\" --mongo-collection \"<mongo collection name>\"",
    );
    process.exitCode = 2;
    return;
  }

  // Column headers as they appeared in the original CSV.
  const totalProfitColumnHeader = getArgValue("--total-profit-column", "Total Profit");
  const regionColumnHeader = getArgValue("--region-column", "Region");
  const totalRevenueColumnHeader = getArgValue("--total-revenue-column", "Total Revenue");
  const itemTypeColumnHeader = getArgValue("--item-type-column", "Item Type");

  const profitThreshold = Number(getArgValue("--profit-threshold", 5000));
  const regionValue = getArgValue("--region-value", "Europe");

  const reportPath = getArgValue(
    "--report",
    path.resolve(process.cwd(), "query_performance_report.csv"),
  );
  initReport(reportPath);

  const pgTotalProfitColumn = sanitizePostgresColumnNameFromHeader(totalProfitColumnHeader);
  const pgRegionColumn = sanitizePostgresColumnNameFromHeader(regionColumnHeader);
  const pgSumRevenueColumn = sanitizePostgresColumnNameFromHeader(totalRevenueColumnHeader);
  const pgCategoryColumn = sanitizePostgresColumnNameFromHeader(itemTypeColumnHeader);

  const mongoProfitField = totalProfitColumnHeader; // Mongo uses original header keys
  const mongoRegionField = regionColumnHeader;
  const mongoSumField = totalRevenueColumnHeader;
  const mongoCategoryField = itemTypeColumnHeader;

  console.log(`Starting query benchmark with concurrency=${concurrency}`);
  console.log(`PostgreSQL table: ${pgTable}`);
  console.log(`MongoDB collection: ${mongoCollection}`);
  console.log(`Report: ${reportPath}`);

  // Shared clients for warm-state rounds.
  const poolWarm = await createPostgresPool(CONFIG.postgres);
  const { mongoClient: mongoWarmClient, mongoCollection: mongoWarmCollection } =
    await createMongoClientAndCollection({
      mongoConfig: CONFIG.mongo,
      dbName: CONFIG.mongo.db,
      collectionName: mongoCollection,
    });

  const databaseRows = [];

  async function recordConcurrentExecution({
    QueryType,
    Database,
    concurrency,
    taskFactory,
    queryTimeoutMs,
  }) {
    const results = await runConcurrentUsers(concurrency, async (userId) => {
      // Note: userId is currently unused in the queries; it exists to preserve
      // the "simultaneous users" model for contention experiments.
      await taskFactory(userId);
    });

    for (const r of results) {
      databaseRows.push({
        QueryType,
        Database,
        ExecutionTimeMS: r.execMs.toFixed(3),
        Timestamp: r.timestamp,
      });
    }
    // Ensure all queued lines are persisted after each test stage.
    appendCsvRows(reportPath, databaseRows.splice(0, databaseRows.length));
  }

  // ----------------------------
  // 1) Cold vs Warm Cache
  // ----------------------------
  const coldWarmQueryType = "ColdVsWarm_SelectAll";
  console.log(`Running Cold vs Warm Cache test (${roundsColdWarm} rounds)...`);

  const coldWarmTimes = {
    Postgres: [],
    MongoDB: [],
  };

  for (let round = 1; round <= roundsColdWarm; round++) {
    const isColdRound = round === 1;
    const QueryType = coldWarmQueryType; // database column disambiguates

    if (isColdRound) {
      // Why: create fresh clients so that per-connection state is not reused.
      // This is a best-effort approximation of "cold" conditions.
      const coldPool = await createPostgresPool(CONFIG.postgres);
      const { mongoClient: coldMongoClient, mongoCollection: coldMongoCollection } =
        await createMongoClientAndCollection({
          mongoConfig: CONFIG.mongo,
          dbName: CONFIG.mongo.db,
          collectionName: mongoCollection,
        });

      await recordConcurrentExecution({
        QueryType,
        Database: "Postgres",
        concurrency,
        queryTimeoutMs: selectAllQueryTimeoutMs,
        taskFactory: () => runSelectAllPostgres({ pool: coldPool, pgTable, queryTimeoutMs: selectAllQueryTimeoutMs }),
      });

      await recordConcurrentExecution({
        QueryType,
        Database: "MongoDB",
        concurrency,
        queryTimeoutMs: selectAllQueryTimeoutMs,
        taskFactory: () => runSelectAllMongo({ mongoCollection: coldMongoCollection, queryTimeoutMs: selectAllQueryTimeoutMs }),
      });

      await coldPool.end();
      await coldMongoClient.close();
    } else {
      await recordConcurrentExecution({
        QueryType,
        Database: "Postgres",
        concurrency,
        queryTimeoutMs: selectAllQueryTimeoutMs,
        taskFactory: () => runSelectAllPostgres({ pool: poolWarm, pgTable, queryTimeoutMs: selectAllQueryTimeoutMs }),
      });

      await recordConcurrentExecution({
        QueryType,
        Database: "MongoDB",
        concurrency,
        queryTimeoutMs: selectAllQueryTimeoutMs,
        taskFactory: () => runSelectAllMongo({ mongoCollection: mongoWarmCollection, queryTimeoutMs: selectAllQueryTimeoutMs }),
      });
    }
  }

  // Compute FirstRun vs WarmAvg (rounds 2..N).
  // We reconstruct times from the CSV we just wrote by re-running queries here would be redundant.
  // Instead, compute from the recorded rows in-memory:
  // However, recordConcurrentExecution currently flushes rows to disk and doesn't keep them.
  // For correctness, read back from the file for summarization.
  //
  // Why read-back: keeps the "output requirement" simple and ensures summary uses the
  // exact persisted values.
  const reportLines = fs.readFileSync(reportPath, "utf8").trim().split("\n");
  // header at 0
  const coldWarmRows = reportLines
    .slice(1)
    .map((l) => {
      const parts = l.split(",");
      return {
        QueryType: parts[0],
        Database: parts[1],
        ExecutionTimeMS: Number(parts[2]),
      };
    })
    .filter((r) => r.QueryType === coldWarmQueryType);

  const byDb = { Postgres: [], MongoDB: [] };
  for (const r of coldWarmRows) {
    if (!Number.isFinite(r.ExecutionTimeMS)) continue;
    byDb[r.Database].push(r.ExecutionTimeMS);
  }

  // For dissertation reporting: interpret the earliest round as cold, and remaining as warm.
  // Since we appended round-by-round, the first `concurrency` entries per DB correspond to round 1.
  const firstChunkSize = concurrency; // per round, 5 users
  const summaryNow = new Date().toISOString();

  for (const dbName of Object.keys(byDb)) {
    const times = byDb[dbName];
    const firstTimes = times.slice(0, firstChunkSize);
    const warmTimes = times.slice(firstChunkSize); // rounds 2..N across all users
    const firstAvg = firstTimes.reduce((a, b) => a + b, 0) / Math.max(1, firstTimes.length);
    const warmAvg = warmTimes.reduce((a, b) => a + b, 0) / Math.max(1, warmTimes.length);

    appendCsvRows(reportPath, [
      {
        QueryType: "ColdVsWarm_FirstRunAvg",
        Database: dbName,
        ExecutionTimeMS: firstAvg.toFixed(3),
        Timestamp: summaryNow,
      },
      {
        QueryType: "ColdVsWarm_WarmAvg_2toN",
        Database: dbName,
        ExecutionTimeMS: warmAvg.toFixed(3),
        Timestamp: summaryNow,
      },
    ]);

    console.log(
      `${dbName}: FirstRunAvg=${firstAvg.toFixed(3)} ms, WarmAvg=${warmAvg.toFixed(3)} ms (rounds 2..${roundsColdWarm})`,
    );
  }

  // ----------------------------
  // 2) Complex Filtering
  // ----------------------------
  console.log("Running complex filtering scalability test...");
  await recordConcurrentExecution({
    QueryType: "ComplexFiltering_TotalProfit_Region",
    Database: "Postgres",
    concurrency,
    queryTimeoutMs: complexFilterQueryTimeoutMs,
    taskFactory: () =>
      runComplexFilterPostgres({
        pool: poolWarm,
        pgTable,
        pgTotalProfitColumn,
        pgRegionColumn,
        profitThreshold,
        regionValue,
        queryTimeoutMs: complexFilterQueryTimeoutMs,
      }),
  });
  await recordConcurrentExecution({
    QueryType: "ComplexFiltering_TotalProfit_Region",
    Database: "MongoDB",
    concurrency,
    queryTimeoutMs: complexFilterQueryTimeoutMs,
    taskFactory: () =>
      runComplexFilterMongo({
        mongoCollection: mongoWarmCollection,
        profitField: mongoProfitField,
        regionField: mongoRegionField,
        profitThreshold,
        regionValue,
        queryTimeoutMs: complexFilterQueryTimeoutMs,
      }),
  });

  // ----------------------------
  // 3) Aggregation Benchmark
  // ----------------------------
  console.log("Running aggregation benchmark (SUM grouped by category)...");
  await recordConcurrentExecution({
    QueryType: "Aggregation_SumRevenue_GroupByCategory",
    Database: "Postgres",
    concurrency,
    queryTimeoutMs: aggregationQueryTimeoutMs,
    taskFactory: () =>
      runAggregationPostgres({
        pool: poolWarm,
        pgTable,
        pgCategoryColumn,
        pgSumColumn: pgSumRevenueColumn,
        queryTimeoutMs: aggregationQueryTimeoutMs,
      }),
  });
  await recordConcurrentExecution({
    QueryType: "Aggregation_SumRevenue_GroupByCategory",
    Database: "MongoDB",
    concurrency,
    queryTimeoutMs: aggregationQueryTimeoutMs,
    taskFactory: () =>
      runAggregationMongo({
        mongoCollection: mongoWarmCollection,
        categoryField: mongoCategoryField,
        sumField: mongoSumField,
        queryTimeoutMs: aggregationQueryTimeoutMs,
      }),
  });

  // Clean up warm clients.
  await poolWarm.end();
  await mongoWarmClient.close();

  console.log("Benchmark complete. CSV report generated.");
  console.log(`Final report: ${reportPath}`);
}

main().catch((err) => {
  console.error("Fatal error:", err?.stack || err);
  process.exitCode = 1;
});

