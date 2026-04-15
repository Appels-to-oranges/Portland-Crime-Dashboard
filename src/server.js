import express from "express";
import path from "node:path";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { getPgConfig } from "../scripts/lib/db-config.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const publicDir = path.join(__dirname, "..", "public");

const app = express();
const port = Number(process.env.PORT || 3000);

app.use(express.static(publicDir));

app.get("/health", (_req, res) => {
  res.json({ ok: true });
});

app.get("/api/summary", async (_req, res) => {
  const cfg = getPgConfig();
  const client = new pg.Client(cfg);
  try {
    await client.connect();
    const { rows } = await client.query(`
      select
        count(*)::int as mart_rows,
        max(report_month_year) as latest_report_month_year
      from marts.mart_offense_monthly_neighborhood
    `);
    res.json(rows[0] ?? {});
  } catch (e) {
    console.error(e);
    res.status(503).json({ error: "database_unavailable", detail: String(e.message) });
  } finally {
    await client.end().catch(() => {});
  }
});

app.listen(port, () => {
  console.error(`Open http://localhost:${port}`);
});
