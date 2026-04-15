import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { writeDbtProfiles } from "./lib/db-config.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.join(__dirname, "..");
const dbtDir = path.join(repoRoot, "dbt");
const profilesDir = path.join(dbtDir, "profiles");

function run(cmd, args, extraEnv) {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, {
      stdio: "inherit",
      env: { ...process.env, ...extraEnv },
      shell: process.platform === "win32",
    });
    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`${cmd} exited with ${code}`));
    });
  });
}

async function main() {
  writeDbtProfiles();
  const args = [
    "run",
    "--project-dir",
    dbtDir,
    "--profiles-dir",
    profilesDir,
    ...process.argv.slice(2),
  ];
  try {
    await run("dbt", args);
  } catch {
    await run("dbt.cmd", args);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
