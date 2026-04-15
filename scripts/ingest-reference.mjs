import AdmZip from "adm-zip";
import pg from "pg";
import shp from "shpjs";
import { getPgConfig } from "./lib/db-config.mjs";

const REL_URL =
  "https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/tab20_zcta520_county20_natl.txt";
const GAZ_URL =
  "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2024_Gazetteer/2024_Gaz_zcta_national.zip";
/** 2020 cartographic ZCTA polygons (~67MB); used for point-in-polygon (OR/WA subset kept in DB). */
const ZCTA_CB_ZIP_URL =
  process.env.ZCTA_CB_ZIP_URL ||
  "https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_zcta520_500k.zip";
const ACS_YEAR = process.env.CENSUS_ACS_YEAR || "2022";
const WEATHER_LAT = Number(process.env.WEATHER_LAT || "45.5152");
const WEATHER_LON = Number(process.env.WEATHER_LON || "-122.6784");
const WEATHER_START = process.env.WEATHER_START || "2022-01-01";
const WEATHER_END = process.env.WEATHER_END || new Date().toISOString().slice(0, 10);

const UA = { "User-Agent": "portland-crime-dashboard/1.0 (reference data)" };

async function fetchText(url) {
  const res = await fetch(url, { headers: UA });
  if (!res.ok) {
    throw new Error(`GET ${url} failed ${res.status} ${res.statusText}`);
  }
  return res.text();
}

async function fetchBuffer(url) {
  const res = await fetch(url, { headers: UA });
  if (!res.ok) {
    throw new Error(`GET ${url} failed ${res.status} ${res.statusText}`);
  }
  return Buffer.from(await res.arrayBuffer());
}

/** ZCTA5 GEOIDs that touch an Oregon (41) or Washington (53) county. */
function loadOrWaZctaSet(relText) {
  const zctas = new Set();
  for (const line of relText.split(/\r?\n/)) {
    if (!line || line.startsWith("OID_ZCTA5_20")) continue;
    const p = line.split("|");
    const zcta = p[1]?.trim();
    const countyGeoid = p[9]?.trim();
    if (!zcta || !/^\d{5}$/.test(zcta) || !countyGeoid || countyGeoid.length < 2) continue;
    const st = countyGeoid.slice(0, 2);
    if (st === "41" || st === "53") zctas.add(zcta);
  }
  return zctas;
}

function parseGazetteerZctaRows(zipBuffer, orWaZctas) {
  const zip = new AdmZip(zipBuffer);
  const entry = zip.getEntries().find((e) => e.entryName.endsWith(".txt"));
  if (!entry) throw new Error("Gazetteer zip missing .txt entry");
  const text = zip.readAsText(entry);
  const rows = [];
  for (const line of text.split(/\r?\n/)) {
    if (!line || line.startsWith("GEOID")) continue;
    const p = line.split("\t").map((s) => s.trim());
    const geoid = p[0];
    if (!geoid || !orWaZctas.has(geoid)) continue;
    const lat = Number(p[5]);
    const lon = Number(p[6]);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
    rows.push({ zcta: geoid, intpt_lat: lat, intpt_lon: lon });
  }
  return rows;
}

function parseAcsInt(s) {
  if (s == null || s === "") return null;
  const n = Number(s);
  if (!Number.isFinite(n) || n < 0) return null;
  return n;
}

async function loadAcsForZctas(orWaZctas) {
  const url = new URL(`https://api.census.gov/data/${ACS_YEAR}/acs/acs5`);
  url.searchParams.set(
    "get",
    "NAME,B01003_001E,B17001_001E,B17001_002E,B19013_001E",
  );
  url.searchParams.set("for", "zip code tabulation area:*");
  const res = await fetch(url.toString(), { headers: UA });
  if (!res.ok) {
    throw new Error(`Census ACS failed ${res.status} ${res.statusText}`);
  }
  const data = await res.json();
  if (!Array.isArray(data) || data.length < 2) {
    throw new Error("Census ACS returned unexpected JSON");
  }
  const header = data[0];
  const zi = header.indexOf("zip code tabulation area");
  const rows = [];
  for (let i = 1; i < data.length; i++) {
    const row = data[i];
    const zcta = row[zi];
    if (!orWaZctas.has(zcta)) continue;
    const pop = parseAcsInt(row[1]);
    const povUniv = parseAcsInt(row[2]);
    const povCt = parseAcsInt(row[3]);
    let medInc = parseAcsInt(row[4]);
    if (row[4] === "-666666666") medInc = null;
    rows.push({
      zcta,
      population: pop,
      poverty_universe: povUniv,
      poverty_count: povCt,
      median_household_income: medInc,
    });
  }
  return rows;
}

async function loadZctaPolygons(orWaZctas) {
  console.error("Downloading Census 2020 ZCTA cartographic boundaries (zip)…");
  const zipBuf = await fetchBuffer(ZCTA_CB_ZIP_URL);
  const geojson = await shp(zipBuf);
  if (!geojson?.features?.length) {
    throw new Error("ZCTA shapefile parse returned no features");
  }
  const rows = [];
  for (const f of geojson.features) {
    const zcta = f.properties?.ZCTA5CE20?.trim();
    if (!zcta || !orWaZctas.has(zcta) || !f.geometry) continue;
    rows.push({
      zcta,
      geojson: JSON.stringify(f.geometry),
    });
  }
  console.error(`ZCTA polygons kept (OR/WA): ${rows.length}`);
  return rows;
}

async function loadWeatherDaily() {
  const u = new URL("https://archive-api.open-meteo.com/v1/archive");
  u.searchParams.set("latitude", String(WEATHER_LAT));
  u.searchParams.set("longitude", String(WEATHER_LON));
  u.searchParams.set("start_date", WEATHER_START);
  u.searchParams.set("end_date", WEATHER_END);
  u.searchParams.set("daily", "temperature_2m_max,precipitation_sum");
  const res = await fetch(u.toString(), { headers: UA });
  if (!res.ok) {
    throw new Error(`Open-Meteo failed ${res.status} ${res.statusText}`);
  }
  const j = await res.json();
  const times = j.daily?.time ?? [];
  const tmax = j.daily?.temperature_2m_max ?? [];
  const precip = j.daily?.precipitation_sum ?? [];
  const rows = [];
  for (let i = 0; i < times.length; i++) {
    rows.push({
      obs_date: times[i],
      temp_max_c: tmax[i] != null ? Number(tmax[i]) : null,
      precip_mm: precip[i] != null ? Number(precip[i]) : null,
    });
  }
  return rows;
}

async function main() {
  const cfg = getPgConfig();
  const client = new pg.Client(cfg);
  await client.connect();

  try {
    await client.query("CREATE EXTENSION IF NOT EXISTS postgis");
  } catch (e) {
    console.error(
      "PostGIS is required for ZCTA point-in-polygon joins. Enable it on your Postgres instance (e.g. CREATE EXTENSION postgis).",
    );
    throw e;
  }

  await client.query(`CREATE SCHEMA IF NOT EXISTS raw`);

  await client.query(`
    CREATE TABLE IF NOT EXISTS raw.zcta_geometry (
      zcta text NOT NULL PRIMARY KEY,
      geom geometry(MultiPolygon, 4326) NOT NULL,
      _loaded_at timestamptz NOT NULL DEFAULT now()
    )
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS zcta_geometry_geom_gix
    ON raw.zcta_geometry USING GIST (geom)
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS raw.zcta_reference (
      zcta text NOT NULL PRIMARY KEY,
      intpt_lat double precision NOT NULL,
      intpt_lon double precision NOT NULL,
      _loaded_at timestamptz NOT NULL DEFAULT now()
    )
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS raw.acs_zcta (
      zcta text NOT NULL PRIMARY KEY,
      acs_year text NOT NULL,
      population bigint,
      poverty_universe bigint,
      poverty_count bigint,
      median_household_income integer,
      _loaded_at timestamptz NOT NULL DEFAULT now()
    )
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS raw.weather_daily (
      obs_date date NOT NULL PRIMARY KEY,
      temp_max_c double precision,
      precip_mm double precision,
      _loaded_at timestamptz NOT NULL DEFAULT now()
    )
  `);

  console.error("Downloading ZCTA–county relationship file…");
  const relText = await fetchText(REL_URL);
  const orWaZctas = loadOrWaZctaSet(relText);
  console.error(`OR/WA ZCTAs (unique): ${orWaZctas.size}`);

  const zctaPolyRows = await loadZctaPolygons(orWaZctas);

  console.error("Downloading ZCTA gazetteer (zip)…");
  const gazBuf = await fetchBuffer(GAZ_URL);
  const zctaRows = parseGazetteerZctaRows(gazBuf, orWaZctas);
  console.error(`ZCTA centroid rows: ${zctaRows.length}`);

  console.error(`Fetching ACS ${ACS_YEAR} 5-year (all ZCTAs; filter local)…`);
  const acsRows = await loadAcsForZctas(orWaZctas);
  console.error(`ACS rows kept: ${acsRows.length}`);

  console.error("Fetching Open-Meteo daily weather…");
  const wxRows = await loadWeatherDaily();
  console.error(`Weather days: ${wxRows.length}`);

  await client.query("BEGIN");
  try {
    await client.query(
      "TRUNCATE raw.zcta_geometry, raw.zcta_reference, raw.acs_zcta, raw.weather_daily",
    );

    for (const r of zctaPolyRows) {
      await client.query(
        `INSERT INTO raw.zcta_geometry (zcta, geom)
         VALUES (
           $1,
           ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON($2::json), 4326))
         )`,
        [r.zcta, r.geojson],
      );
    }
    await client.query("ANALYZE raw.zcta_geometry");

    for (const r of zctaRows) {
      await client.query(
        `INSERT INTO raw.zcta_reference (zcta, intpt_lat, intpt_lon) VALUES ($1, $2, $3)`,
        [r.zcta, r.intpt_lat, r.intpt_lon],
      );
    }

    for (const r of acsRows) {
      await client.query(
        `INSERT INTO raw.acs_zcta (zcta, acs_year, population, poverty_universe, poverty_count, median_household_income)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [
          r.zcta,
          ACS_YEAR,
          r.population,
          r.poverty_universe,
          r.poverty_count,
          r.median_household_income,
        ],
      );
    }

    for (const r of wxRows) {
      await client.query(
        `INSERT INTO raw.weather_daily (obs_date, temp_max_c, precip_mm) VALUES ($1::date, $2, $3)`,
        [r.obs_date, r.temp_max_c, r.precip_mm],
      );
    }

    await client.query("COMMIT");
    console.error(
      "Reference ingest complete: zcta_geometry (PostGIS), zcta_reference, acs_zcta, weather_daily.",
    );
  } catch (e) {
    await client.query("ROLLBACK").catch(() => {});
    throw e;
  } finally {
    await client.end().catch(() => {});
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
