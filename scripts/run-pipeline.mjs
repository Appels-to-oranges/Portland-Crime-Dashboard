import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

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
  await runNode(path.join(__dirname, "ingest.mjs"));
  await runNode(path.join(__dirname, "ingest-reference.mjs"));
  await runNode(path.join(__dirname, "run-dbt.mjs"));
  console.error("Pipeline finished: ingest + reference data + dbt run.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
