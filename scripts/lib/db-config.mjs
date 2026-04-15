import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import parse from "pg-connection-string";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.join(__dirname, "..", "..");

/**
 * Railway provides DATABASE_URL. dbt-postgres wants discrete fields — we parse once.
 */
export function getPgConfig() {
  const url = process.env.DATABASE_URL;
  if (!url) {
    throw new Error("DATABASE_URL is required (set by Railway Postgres or locally).");
  }
  const c = parse(url);
  if (!c.host || !c.database) {
    throw new Error("DATABASE_URL could not be parsed (missing host or database).");
  }
  return {
    host: c.host,
    port: c.port ?? 5432,
    user: c.user,
    password: c.password ?? "",
    database: c.database,
    ssl:
      process.env.PGSSLMODE === "disable"
        ? false
        : {
            rejectUnauthorized: process.env.PGSSL_REJECT_UNAUTHORIZED === "true",
          },
  };
}

/**
 * Writes dbt/profiles/generated_profiles.yml (gitignored) from DATABASE_URL.
 */
export function writeDbtProfiles() {
  const c = getPgConfig();
  const dir = path.join(repoRoot, "dbt", "profiles");
  fs.mkdirSync(dir, { recursive: true });
  const out = path.join(dir, "generated_profiles.yml");
  const threads = Number(process.env.DBT_THREADS || 4);
  const body = `portland_crime:
  target: prod
  outputs:
    prod:
      type: postgres
      host: ${JSON.stringify(c.host)}
      user: ${JSON.stringify(c.user)}
      password: ${JSON.stringify(c.password)}
      port: ${c.port}
      dbname: ${JSON.stringify(c.database)}
      schema: ${JSON.stringify(process.env.DBT_SCHEMA || "dbt")}
      threads: ${threads}
      sslmode: ${process.env.PGSSLMODE === "disable" ? "disable" : "require"}
`;
  fs.writeFileSync(out, body, "utf8");
  return out;
}
