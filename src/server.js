import express from "express";
import path from "node:path";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { getPgConfig } from "../scripts/lib/db-config.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const publicDir = path.join(__dirname, "..", "public");

const app = express();
const port = Number(process.env.PORT || 3000);
const pool = new pg.Pool({ ...getPgConfig(), max: 10 });

app.use(express.static(publicDir));

app.get("/health", (_req, res) => res.json({ ok: true }));

function parseFilters(query) {
  const where = [];
  const params = [];
  let idx = 1;
  if (query.start) {
    where.push(`to_date(report_month_year, 'Month YYYY') >= $${idx++}::date`);
    params.push(query.start + "-01");
  }
  if (query.end) {
    where.push(`to_date(report_month_year, 'Month YYYY') <= ($${idx++}::date + interval '1 month - 1 day')`);
    params.push(query.end + "-01");
  }
  if (query.neighborhood) {
    where.push(`neighborhood = $${idx++}`);
    params.push(query.neighborhood);
  }
  if (query.category) {
    where.push(`offense_category = $${idx++}`);
    params.push(query.category);
  }
  return { clause: where.length ? "AND " + where.join(" AND ") : "", params };
}

function parseZctaFilters(query) {
  const where = ["month_start >= '2022-01-01'::date"];
  const params = [];
  let idx = 1;
  if (query.start) {
    where.push(`month_start >= $${idx++}::date`);
    params.push(query.start + "-01");
  }
  if (query.end) {
    where.push(`month_start <= ($${idx++}::date + interval '1 month - 1 day')`);
    params.push(query.end + "-01");
  }
  if (query.neighborhood) {
    where.push(`neighborhood = $${idx++}`);
    params.push(query.neighborhood);
  }
  if (query.category) {
    where.push(`offense_category = $${idx++}`);
    params.push(query.category);
  }
  return { clause: "AND " + where.join(" AND "), params };
}

// Distinct filter values + date range
app.get("/api/filters", async (_req, res) => {
  try {
    const [neighborhoods, categories, dateRange] = await Promise.all([
      pool.query(`SELECT DISTINCT neighborhood FROM marts.mart_offense_monthly_neighborhood
                  WHERE neighborhood IS NOT NULL ORDER BY 1`),
      pool.query(`SELECT DISTINCT offense_category FROM marts.mart_offense_monthly_neighborhood
                  WHERE offense_category IS NOT NULL ORDER BY 1`),
      pool.query(`SELECT to_char(min(to_date(report_month_year, 'Month YYYY')), 'YYYY-MM') AS min_month,
                         to_char(max(to_date(report_month_year, 'Month YYYY')), 'YYYY-MM') AS max_month
                  FROM marts.mart_offense_monthly_neighborhood`),
    ]);
    res.json({
      neighborhoods: neighborhoods.rows.map((r) => r.neighborhood),
      categories: categories.rows.map((r) => r.offense_category),
      ...dateRange.rows[0],
    });
  } catch (e) {
    console.error(e);
    res.status(503).json({ error: "database_unavailable" });
  }
});

// Monthly crime trend
app.get("/api/trend", async (req, res) => {
  const { clause, params } = parseFilters(req.query);
  try {
    const { rows } = await pool.query(
      `SELECT to_char(to_date(report_month_year, 'Month YYYY'), 'YYYY-MM') AS month,
              report_month_year AS month_label,
              sum(offense_count)::int AS total
       FROM marts.mart_offense_monthly_neighborhood
       WHERE report_month_year IS NOT NULL ${clause}
       GROUP BY 1, 2 ORDER BY 1`,
      params,
    );
    res.json(rows);
  } catch (e) {
    console.error(e);
    res.status(503).json({ error: "database_unavailable" });
  }
});

// Offense counts by category
app.get("/api/by-category", async (req, res) => {
  const { clause, params } = parseFilters(req.query);
  try {
    const { rows } = await pool.query(
      `SELECT offense_category AS category,
              sum(offense_count)::int AS total
       FROM marts.mart_offense_monthly_neighborhood
       WHERE offense_category IS NOT NULL ${clause}
       GROUP BY 1 ORDER BY total DESC`,
      params,
    );
    res.json(rows);
  } catch (e) {
    console.error(e);
    res.status(503).json({ error: "database_unavailable" });
  }
});

// Top neighborhoods
app.get("/api/by-neighborhood", async (req, res) => {
  const { clause, params } = parseFilters(req.query);
  try {
    const { rows } = await pool.query(
      `SELECT neighborhood,
              sum(offense_count)::int AS total
       FROM marts.mart_offense_monthly_neighborhood
       WHERE neighborhood IS NOT NULL ${clause}
       GROUP BY 1 ORDER BY total DESC LIMIT 15`,
      params,
    );
    res.json(rows);
  } catch (e) {
    console.error(e);
    res.status(503).json({ error: "database_unavailable" });
  }
});

// Weather vs crime (monthly, from ZCTA mart)
app.get("/api/weather-crime", async (req, res) => {
  const { clause, params } = parseZctaFilters(req.query);
  try {
    const { rows } = await pool.query(
      `SELECT month_start::text AS month,
              sum(offense_count)::int AS total,
              round(avg(month_avg_high_temp_c)::numeric, 1) AS avg_high_c,
              round(sum(month_total_precip_mm)::numeric / nullif(count(DISTINCT zcta), 0), 1) AS avg_precip_mm
       FROM marts.mart_offense_monthly_zcta
       WHERE month_start IS NOT NULL ${clause}
       GROUP BY month_start ORDER BY month_start`,
      params,
    );
    res.json(rows);
  } catch (e) {
    console.error(e);
    res.status(503).json({ error: "database_unavailable" });
  }
});

// Demographics by ZCTA
app.get("/api/demographics", async (req, res) => {
  const { clause, params } = parseZctaFilters(req.query);
  try {
    const { rows } = await pool.query(
      `SELECT zcta,
              sum(offense_count)::int AS total_offenses,
              max(population)::int AS population,
              round(max(poverty_rate_pct)::numeric, 1) AS poverty_rate_pct,
              max(median_household_income)::int AS median_income
       FROM marts.mart_offense_monthly_zcta
       WHERE zcta IS NOT NULL AND population > 0 ${clause}
       GROUP BY zcta
       ORDER BY total_offenses DESC`,
      params,
    );
    res.json(rows);
  } catch (e) {
    console.error(e);
    res.status(503).json({ error: "database_unavailable" });
  }
});

// Heatmap: month-of-year x offense category (averaged across years)
app.get("/api/heatmap", async (req, res) => {
  const { clause, params } = parseZctaFilters(req.query);
  try {
    const { rows } = await pool.query(
      `WITH monthly AS (
         SELECT extract(month FROM month_start)::int AS month_num,
                extract(year FROM month_start)::int AS yr,
                offense_category AS category,
                sum(offense_count)::int AS total
         FROM marts.mart_offense_monthly_zcta
         WHERE month_start IS NOT NULL AND offense_category IS NOT NULL ${clause}
         GROUP BY 1, 2, 3
       )
       SELECT month_num,
              category,
              round(avg(total)::numeric, 1)::float AS avg_total
       FROM monthly
       GROUP BY 1, 2
       ORDER BY 1, 2`,
      params,
    );
    res.json(rows);
  } catch (e) {
    console.error(e);
    res.status(503).json({ error: "database_unavailable" });
  }
});

// Seasonality: average offenses + weather by month-of-year across all years
app.get("/api/seasonality", async (req, res) => {
  const { clause, params } = parseZctaFilters(req.query);
  try {
    const { rows } = await pool.query(
      `WITH monthly AS (
         SELECT month_start,
                extract(month FROM month_start)::int AS month_num,
                sum(offense_count)::int AS total,
                avg(month_avg_high_temp_c) AS avg_high_c,
                sum(month_total_precip_mm) / nullif(count(DISTINCT zcta), 0) AS avg_precip_mm
         FROM marts.mart_offense_monthly_zcta
         WHERE month_start IS NOT NULL ${clause}
         GROUP BY month_start
       )
       SELECT month_num,
              to_char(to_date(month_num::text, 'MM'), 'Mon') AS month_name,
              round(avg(total)::numeric, 0)::int AS avg_offenses,
              round(avg(avg_high_c)::numeric, 1) AS avg_high_c,
              round(avg(avg_precip_mm)::numeric, 1) AS avg_precip_mm
       FROM monthly
       GROUP BY month_num
       ORDER BY month_num`,
      params,
    );
    res.json(rows);
  } catch (e) {
    console.error(e);
    res.status(503).json({ error: "database_unavailable" });
  }
});

// Correlation: scatter data + Pearson r for temp-crime and precip-crime
app.get("/api/correlation", async (req, res) => {
  const { clause, params } = parseZctaFilters(req.query);
  try {
    const [scatter, corr] = await Promise.all([
      pool.query(
        `SELECT month_start::text AS month,
                sum(offense_count)::int AS total,
                round(avg(month_avg_high_temp_c)::numeric, 1) AS avg_high_c,
                round(sum(month_total_precip_mm)::numeric / nullif(count(DISTINCT zcta), 0), 1) AS avg_precip_mm
         FROM marts.mart_offense_monthly_zcta
         WHERE month_start IS NOT NULL AND month_avg_high_temp_c IS NOT NULL ${clause}
         GROUP BY month_start ORDER BY month_start`,
        params,
      ),
      pool.query(
        `WITH monthly AS (
           SELECT month_start,
                  sum(offense_count)::float AS total,
                  avg(month_avg_high_temp_c)::float AS avg_high_c,
                  sum(month_total_precip_mm)::float / nullif(count(DISTINCT zcta), 0) AS avg_precip_mm
           FROM marts.mart_offense_monthly_zcta
           WHERE month_start IS NOT NULL AND month_avg_high_temp_c IS NOT NULL ${clause}
           GROUP BY month_start
         )
         SELECT round(corr(total, avg_high_c)::numeric, 3) AS r_temp,
                round(corr(total, avg_precip_mm)::numeric, 3) AS r_precip
         FROM monthly`,
        params,
      ),
    ]);
    res.json({
      points: scatter.rows,
      r_temp: corr.rows[0]?.r_temp ?? null,
      r_precip: corr.rows[0]?.r_precip ?? null,
    });
  } catch (e) {
    console.error(e);
    res.status(503).json({ error: "database_unavailable" });
  }
});

// GeoJSON choropleth: ZCTA polygons with crime stats
app.get("/api/geo", async (req, res) => {
  const { clause, params } = parseZctaFilters(req.query);
  try {
    const { rows } = await pool.query(
      `WITH stats AS (
         SELECT zcta,
                sum(offense_count)::int AS total_offenses,
                max(population)::int AS population
         FROM marts.mart_offense_monthly_zcta
         WHERE zcta IS NOT NULL ${clause}
         GROUP BY zcta
       )
       SELECT json_build_object(
         'type', 'FeatureCollection',
         'features', coalesce(json_agg(json_build_object(
           'type', 'Feature',
           'properties', json_build_object(
             'zcta', g.zcta,
             'total', coalesce(s.total_offenses, 0),
             'population', coalesce(s.population, 0),
             'rate', CASE WHEN coalesce(s.population, 0) > 0
                         THEN round(s.total_offenses * 1000.0 / s.population, 1)
                         ELSE 0 END
           ),
           'geometry', ST_AsGeoJSON(g.geom)::json
         )), '[]'::json)
       ) AS geojson
       FROM raw.zcta_geometry g
       INNER JOIN stats s ON g.zcta = s.zcta`,
      params,
    );
    res.json(rows[0]?.geojson ?? { type: "FeatureCollection", features: [] });
  } catch (e) {
    console.error(e);
    res.status(503).json({ error: "database_unavailable" });
  }
});

// Precipitation deep-dive: buckets + scatter + Pearson r
app.get("/api/precip-analysis", async (req, res) => {
  const { clause, params } = parseZctaFilters(req.query);
  try {
    const [scatter, buckets, corr] = await Promise.all([
      pool.query(
        `SELECT month_start::text AS month,
                sum(offense_count)::int AS total,
                round(sum(month_total_precip_mm)::numeric / nullif(count(DISTINCT zcta), 0), 1) AS precip_mm
         FROM marts.mart_offense_monthly_zcta
         WHERE month_start IS NOT NULL AND month_total_precip_mm IS NOT NULL ${clause}
         GROUP BY month_start ORDER BY month_start`,
        params,
      ),
      pool.query(
        `WITH monthly AS (
           SELECT month_start,
                  sum(offense_count)::int AS total,
                  sum(month_total_precip_mm)::float / nullif(count(DISTINCT zcta), 0) AS precip_mm
           FROM marts.mart_offense_monthly_zcta
           WHERE month_start IS NOT NULL AND month_total_precip_mm IS NOT NULL ${clause}
           GROUP BY month_start
         )
         SELECT bucket,
                count(*)::int AS month_count,
                round(avg(total)::numeric, 0)::int AS avg_offenses
         FROM (
           SELECT total, precip_mm,
                  CASE
                    WHEN precip_mm < 50 THEN 'Dry (0-50mm)'
                    WHEN precip_mm < 100 THEN 'Light (50-100mm)'
                    WHEN precip_mm < 200 THEN 'Moderate (100-200mm)'
                    ELSE 'Heavy (200+mm)'
                  END AS bucket
           FROM monthly
         ) sub
         GROUP BY bucket
         ORDER BY CASE bucket
           WHEN 'Dry (0-50mm)' THEN 1
           WHEN 'Light (50-100mm)' THEN 2
           WHEN 'Moderate (100-200mm)' THEN 3
           ELSE 4 END`,
        params,
      ),
      pool.query(
        `WITH monthly AS (
           SELECT month_start,
                  sum(offense_count)::float AS total,
                  sum(month_total_precip_mm)::float / nullif(count(DISTINCT zcta), 0) AS precip_mm
           FROM marts.mart_offense_monthly_zcta
           WHERE month_start IS NOT NULL AND month_total_precip_mm IS NOT NULL ${clause}
           GROUP BY month_start
         )
         SELECT round(corr(total, precip_mm)::numeric, 3) AS r_precip
         FROM monthly`,
        params,
      ),
    ]);
    res.json({
      points: scatter.rows,
      buckets: buckets.rows,
      r_precip: corr.rows[0]?.r_precip ?? null,
    });
  } catch (e) {
    console.error(e);
    res.status(503).json({ error: "database_unavailable" });
  }
});

// Per-category precipitation correlation (Pearson r for each offense category)
app.get("/api/precip-by-category", async (req, res) => {
  const { clause, params } = parseZctaFilters(req.query);
  try {
    const { rows } = await pool.query(
      `WITH monthly AS (
         SELECT month_start,
                offense_category AS category,
                sum(offense_count)::float AS total,
                sum(month_total_precip_mm)::float / nullif(count(DISTINCT zcta), 0) AS precip_mm
         FROM marts.mart_offense_monthly_zcta
         WHERE month_start IS NOT NULL
           AND month_total_precip_mm IS NOT NULL
           AND offense_category IS NOT NULL ${clause}
         GROUP BY month_start, offense_category
       )
       SELECT category,
              round(corr(total, precip_mm)::numeric, 3) AS r_precip,
              count(*)::int AS months,
              round(avg(total)::numeric, 0)::int AS avg_offenses
       FROM monthly
       GROUP BY category
       HAVING count(*) >= 6
       ORDER BY corr(total, precip_mm) DESC NULLS LAST`,
      params,
    );
    res.json(rows);
  } catch (e) {
    console.error(e);
    res.status(503).json({ error: "database_unavailable" });
  }
});

// Crime by hour of day
app.get("/api/by-hour", async (req, res) => {
  const where = [];
  const params = [];
  let idx = 1;
  if (req.query.neighborhood) {
    where.push(`neighborhood = $${idx++}`);
    params.push(req.query.neighborhood);
  }
  if (req.query.category) {
    where.push(`offense_category = $${idx++}`);
    params.push(req.query.category);
  }
  const clause = where.length ? "WHERE " + where.join(" AND ") : "";
  try {
    const { rows } = await pool.query(
      `SELECT occur_hour AS hour,
              sum(offense_count)::int AS total
       FROM intermediate.int_offense_by_hour
       ${clause}
       GROUP BY occur_hour
       ORDER BY occur_hour`,
      params,
    );
    res.json(rows);
  } catch (e) {
    console.error(e);
    res.status(503).json({ error: "database_unavailable" });
  }
});

// Data freshness metadata
app.get("/api/meta", async (_req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT max(_loaded_at)::text AS last_refreshed,
             max(to_date(report_month_year, 'Month YYYY'))::text AS data_through,
             count(*)::int AS total_rows
      FROM raw.offenses
    `);
    res.json(rows[0] ?? {});
  } catch (e) {
    console.error(e);
    res.status(503).json({ error: "database_unavailable" });
  }
});

// Legacy summary (kept for backward compat)
app.get("/api/summary", async (_req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT count(*)::int AS mart_rows,
             max(report_month_year) AS latest_report_month_year
      FROM marts.mart_offense_monthly_neighborhood
    `);
    res.json(rows[0] ?? {});
  } catch (e) {
    console.error(e);
    res.status(503).json({ error: "database_unavailable" });
  }
});

app.listen(port, () => {
  console.error(`Open http://localhost:${port}`);
});
