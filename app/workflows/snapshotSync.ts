import { Task, Workflow } from "@514labs/moose-lib";
import { execSync } from "node:child_process";
import path from "node:path";

const BACKFILL_SCRIPT = path.resolve(
  __dirname,
  "../../scripts/backfill.py"
);
const INGEST_BASE = "http://localhost:4000";

// Runs the backfill script for the latest snapshot only — picks up any new weekly data
const syncLatestSnapshot = new Task<null, string>("syncLatestSnapshot", {
  run: async (): Promise<string> => {
    try {
      // Run the Python backfill script in --latest-only mode
      const output = execSync(
        `python3 ${BACKFILL_SCRIPT} --latest-only --ingest-url ${INGEST_BASE}`,
        { encoding: "utf-8", timeout: 120_000 }
      );
      console.log("[snapshotSync] backfill output:", output);
      return output.trim();
    } catch (err: any) {
      console.error("[snapshotSync] backfill failed:", err.message);
      throw err;
    }
  },
  retries: 2,
  timeout: "3m",
});

const logSyncComplete = new Task<string, void>("logSyncComplete", {
  run: async ({ input }) => {
    console.log(`[snapshotSync] Completed: ${input}`);
  },
  retries: 1,
  timeout: "10s",
});

export const snapshotSyncWorkflow = new Workflow("snapshotSync", {
  startingTask: syncLatestSnapshot,
  retries: 2,
  timeout: "5m",
  // Runs every Sunday at midnight UTC — picks up the weekly SEO report
  schedule: "0 0 * * 0",
});
