import { pipeline } from "node:stream/promises";
import { createReadStream, createWriteStream } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import pg from "pg";
import copyFrom from "pg-copy-streams";
import { getPgConfig } from "./lib/db-config.mjs";

const BASE_URL =
  "https://public.tableau.com/views/PPBOpenDataDownloads/New_Offense_Data_";

function defaultYears() {
  const cur = new Date().getFullYear();
  return Array.from({ length: cur - 2021 }, (_, i) => String(2022 + i)).join(",");
}

function buildUrl(year) {
  if (process.env.OFFENSE_CSV_URL) return process.env.OFFENSE_CSV_URL;
  return `${BASE_URL}${year}.csv?:showVizHome=no`;
}

async function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

async function downloadToFile(url, destPath, retries = 3) {
  const userAgents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "portland-crime-dashboard/1.0 (data refresh)",
  ];
  for (let attempt = 1; attempt <= retries; attempt++) {
    const ua = userAgents[(attempt - 1) % userAgents.length];
    const res = await fetch(url, {
      headers: { "User-Agent": ua, "Accept": "text/csv,*/*" },
      redirect: "follow",
    });
    if (res.ok) {
      await pipeline(res.body, createWriteStream(destPath));
      return;
    }
    console.error(`Attempt ${attempt}/${retries} failed: ${res.status} ${res.statusText} for ${url}`);
    if (attempt < retries) await sleep(5000 * attempt);
  }
  throw new Error(`Download failed after ${retries} attempts for ${url}`);
}

const COPY_SQL = `
  COPY raw.offenses (
    address, case_number, council_district, crime_against, custom_crime_against,
    custom_crime_category, neighborhood, occur_date, occur_time, offense_category,
    offense_count, offense_type, open_data_lat, open_data_lon, open_data_x, open_data_y,
    report_date, report_month_year
  )
  FROM STDIN WITH (FORMAT csv, HEADER true, NULL '')
`;

async function main() {
  const fullRefresh = process.env.FULL_REFRESH === "1";
  const years = (process.env.OFFENSE_YEARS || defaultYears())
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  const cfg = getPgConfig();
  const client = new pg.Client(cfg);
  await client.connect();

  await client.query(`CREATE SCHEMA IF NOT EXISTS raw`);
  await client.query(`
    CREATE TABLE IF NOT EXISTS raw.offenses (
      address text,
      case_number text,
      council_district text,
      crime_against text,
      custom_crime_against text,
      custom_crime_category text,
      neighborhood text,
      occur_date text,
      occur_time text,
      offense_category text,
      offense_count text,
      offense_type text,
      open_data_lat text,
      open_data_lon text,
      open_data_x text,
      open_data_y text,
      report_date text,
      report_month_year text,
      _loaded_at timestamptz NOT NULL DEFAULT now()
    )
  `);

  const tmpDir = await mkdtemp(path.join(tmpdir(), "ppb-csv-"));
  try {
    // Download all CSVs before touching the database
    const csvPaths = [];
    for (const year of years) {
      const url = buildUrl(year);
      const csvPath = path.join(tmpDir, `offenses_${year}.csv`);
      console.error(`Downloading ${year}: ${url}`);
      await downloadToFile(url, csvPath);
      csvPaths.push({ year, csvPath });
      console.error(`Downloaded ${year}.`);
    }

    await client.query("BEGIN");

    if (fullRefresh) {
      console.error("FULL_REFRESH=1 — truncating all rows.");
      await client.query("TRUNCATE TABLE raw.offenses");
    } else {
      for (const year of years) {
        await client.query(
          `DELETE FROM raw.offenses WHERE report_month_year LIKE '%' || $1`,
          [year],
        );
        console.error(`Cleared existing rows for ${year}.`);
      }
    }

    for (const { year, csvPath } of csvPaths) {
      const stream = client.query(copyFrom.from(COPY_SQL));
      await pipeline(createReadStream(csvPath), stream);
      console.error(`Loaded year ${year}.`);
    }

    await client.query("COMMIT");
    const mode = fullRefresh ? "full refresh" : "incremental";
    console.error(`Ingest complete (${mode}): raw.offenses loaded (${years.join(", ")}).`);
  } catch (e) {
    await client.query("ROLLBACK").catch(() => {});
    throw e;
  } finally {
    await client.end().catch(() => {});
    await rm(tmpDir, { recursive: true, force: true });
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
