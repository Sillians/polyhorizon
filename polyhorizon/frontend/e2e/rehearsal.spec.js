import { test, expect } from "@playwright/test";

test("actual HTTP forecast renders chart, quantiles, history, and lineage", async ({ page }, testInfo) => {
  const errors = [];
  page.on("pageerror", error => errors.push(error.message));
  await page.goto("/");
  if (!process.env.REHEARSAL_BASE_URL) {
    await page.evaluate(() => {
      const label = document.createElement("p");
      label.textContent = "REHEARSAL ONLY — synthetic bars, trained fixture model, freshness bypassed. Not production readiness.";
      label.style.cssText = "padding:16px;background:#ffebad;color:#302500;font-weight:bold;margin:0";
      document.body.prepend(label);
    });
  }
  await expect(page.locator("#health")).toHaveText("Service ready", { timeout: 30000 });
  const key = process.env.REHEARSAL_BASE_URL ? process.env.REHEARSAL_FORECAST_KEY : "fixture-client";
  if (key) {
    await page.locator("#settings-open").click();
    await page.locator("#api-key").fill(key);
    await page.locator("#connection-form").getByRole("button", { name: "Apply and connect" }).click();
    await expect(page.locator("#health")).toHaveText("Service ready");
  }
  await page.locator("#symbol").selectOption("NVDA");
  await page.locator("#horizon").selectOption("1");
  const response = page.waitForResponse(r => r.url().endsWith("/v1/forecast") && r.request().method() === "POST");
  await page.locator("#run").click();
  const forecast = await response;
  expect(forecast.status()).toBe(200);
  const payload = await forecast.json();
  expect(payload.symbol).toBe("NVDA");
  expect(payload.model_version).toBeTruthy();
  for (const row of payload.predictions) {
    expect(row.p10).toBeLessThanOrEqual(row.p50);
    expect(row.p50).toBeLessThanOrEqual(row.p90);
  }
  await expect(page.locator("#notice")).toContainText("Forecast ready.");
  await expect(page.locator("#notice")).not.toContainText("Historical series unavailable");
  await expect(page.locator("#chart")).toBeVisible();
  await expect(page.locator("#rows tr")).toHaveCount(payload.predictions.length);
  await expect(page.locator("#provenance")).toContainText(payload.model_version);
  await page.locator("#rows tr").first().click();
  await expect(page.locator("#inspect")).toContainText("P50");
  await page.screenshot({ path: testInfo.outputPath("forecast.png"), fullPage: true });
  await testInfo.attach("forecast-evidence", { body: JSON.stringify({ mode: process.env.REHEARSAL_BASE_URL ? "real-stack" : "synthetic-trained-fixture", model_version: payload.model_version, features_timestamp: payload.features_timestamp, rows: payload.predictions.length }), contentType: "application/json" });
  await page.locator("#horizon").selectOption("2");
  await expect(page.locator("#chart")).toBeHidden();
  expect(errors).toEqual([]);
});

test("operations are separate and locked without operator credentials", async ({ page }) => {
  await page.goto("/ops");
  await expect(page.locator("#ops-lock")).toBeVisible();
  await expect(page.locator("#ops-content")).toBeHidden();
  const denial = await page.request.get("/v1/ops/session");
  expect(denial.status()).toBe(403);
  if (!process.env.REHEARSAL_BASE_URL) {
    const clientDenial = await page.request.get("/v1/ops/session", { headers: { "X-API-Key": "fixture-client" } });
    expect(clientDenial.status()).toBe(403);
    const forecastDenial = await page.request.post("/v1/forecast", { data: { symbol: "NVDA", horizon: 1 } });
    expect(forecastDenial.status()).toBe(401);
  }
});

test("operator can inspect evidence and relock without enabling disabled actions", async ({ page }) => {
  test.skip(Boolean(process.env.REHEARSAL_BASE_URL) && !process.env.REHEARSAL_OPERATOR_KEY, "Supply an operator credential for the real-stack admin rehearsal");
  await page.goto("/ops");
  await expect(page.locator("#health")).toHaveText("Service ready");
  await page.locator("#operator-key").fill(process.env.REHEARSAL_BASE_URL ? process.env.REHEARSAL_OPERATOR_KEY : "fixture-operator");
  await page.locator("#ops-login").getByRole("button", { name: "Unlock console" }).click();
  await expect(page.locator("#ops-content")).toBeVisible();
  await expect(page.locator("#operator-key")).toHaveValue("");
  await expect(page.locator("#ops-checked")).toContainText("Checked", { timeout: 30000 });
  if (!process.env.REHEARSAL_BASE_URL) {
    await expect(page.locator("#ops-gate-title")).toHaveText("Forecast may be unavailable");
    await expect(page.locator("#reload")).toBeDisabled();
    await expect(page.locator("#features")).toBeDisabled();
  }
  await page.locator("#lock").click();
  await expect(page.locator("#ops-content")).toBeHidden();
  await expect(page.locator("#ops-lock")).toBeVisible();
});
