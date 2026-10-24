import { defineConfig } from "@playwright/test";

const real = Boolean(process.env.REHEARSAL_BASE_URL);
export default defineConfig({
  testDir: "./e2e",
  workers: 1,
  timeout: 60000,
  reporter: [["list"], ["json", { outputFile: "test-results/rehearsal.json" }]],
  use: { baseURL: process.env.REHEARSAL_BASE_URL || "http://127.0.0.1:38173", trace: real ? "off" : "retain-on-failure", screenshot: "only-on-failure" },
  webServer: real ? undefined : [
    { command: ".venv/bin/python -m scripts.ops.frontend_fixture", cwd: "../..", url: "http://127.0.0.1:38180/v1/health/ready", timeout: 180000, reuseExistingServer: false },
    { command: "npm run dev -- --host 127.0.0.1 --port 38173 --strictPort", env: { REHEARSAL_API_URL: "http://127.0.0.1:38180", VITE_API_BASE_URL: "http://127.0.0.1:38173" }, url: "http://127.0.0.1:38173", reuseExistingServer: false },
  ],
});
