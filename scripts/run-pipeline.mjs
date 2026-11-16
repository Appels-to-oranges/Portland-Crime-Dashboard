import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { getPgConfig } from "./lib/db-config.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const node = process.execPath;

function runNode(script) {
  return new Promise((resolve, reject) => {
    const child = spawn(node, [script], { stdio: "inherit" });
    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`${script} exited with ${code}`));
    });
  });
}

async function main() {
  const client = new pg.Client(getPgConfig());
  await client.connect();
  try {
    const { rows: [lock] } = await client.query(
      "SELECT pg_try_advisory_lock(hashtext('pdxblockbrief:data-refresh')::bigint) AS acquired",
    );
    if (!lock.acquired) throw new Error("A crime data refresh is already running.");

    await runNode(path.join(__dirname, "ingest.mjs"));
    await runNode(path.join(__dirname, "ingest-reference.mjs"));
    await runNode(path.join(__dirname, "run-dbt.mjs"));

    const { rows: [freshness] } = await client.query(`
      SELECT
        (SELECT max(_loaded_at)::text FROM raw.offenses) AS loaded_at,
        (SELECT max(to_date(nullif(trim(report_month_year), ''), 'Month YYYY'))::text
         FROM raw.offenses) AS raw_month,
        (SELECT max(to_date(report_month_year, 'Month YYYY'))::text
         FROM marts.mart_offense_monthly_neighborhood) AS dashboard_month
    `);
    console.log("Latest load:", freshness.loaded_at);
    console.log("Latest raw month:", freshness.raw_month);
    console.log("Latest dashboard month:", freshness.dashboard_month);
    if (!freshness.dashboard_month || freshness.dashboard_month !== freshness.raw_month) {
      throw new Error("The dashboard tables are behind the raw offense data.");
    }
    console.error("Pipeline finished: ingest + reference data + dbt run.");
  } finally {
    await client.end();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
