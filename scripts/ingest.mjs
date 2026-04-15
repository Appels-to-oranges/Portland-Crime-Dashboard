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
const DEFAULT_YEARS = "2022,2023,2024,2025,2026";

function buildUrl(year) {
  if (process.env.OFFENSE_CSV_URL) return process.env.OFFENSE_CSV_URL;
  return `${BASE_URL}${year}.csv?:showVizHome=no`;
}

async function downloadToFile(url, destPath) {
  const res = await fetch(url, {
    headers: { "User-Agent": "portland-crime-dashboard/1.0 (data refresh)" },
  });
  if (!res.ok) {
    throw new Error(`Download failed ${res.status} ${res.statusText} for ${url}`);
  }
  await pipeline(res.body, createWriteStream(destPath));
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
  const years = (process.env.OFFENSE_YEARS || DEFAULT_YEARS)
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
    await client.query("BEGIN");
    await client.query("TRUNCATE TABLE raw.offenses");

    for (const year of years) {
      const url = buildUrl(year);
      const csvPath = path.join(tmpDir, `offenses_${year}.csv`);
      console.error(`Downloading ${year}: ${url}`);
      await downloadToFile(url, csvPath);

      const stream = client.query(copyFrom.from(COPY_SQL));
      await pipeline(createReadStream(csvPath), stream);
      console.error(`Loaded year ${year}.`);
    }

    await client.query("COMMIT");
    console.error(`Ingest complete: raw.offenses loaded (${years.join(", ")}).`);
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
