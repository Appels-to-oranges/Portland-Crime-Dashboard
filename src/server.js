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
  const where = [];
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
  if (query.category) {
    where.push(`offense_category = $${idx++}`);
    params.push(query.category);
  }
  return { clause: where.length ? "AND " + where.join(" AND ") : "", params };
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
