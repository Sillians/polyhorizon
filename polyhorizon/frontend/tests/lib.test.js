import test from "node:test";
import assert from "node:assert/strict";

import {
  aggregateDaily,
  computeUncertainty,
  daysToBars,
  formatPercent,
} from "../src/lib.js";


test("daysToBars uses metadata and caps the model horizon", () => {
  assert.equal(daysToBars(2, 13, 39), 26);
  assert.equal(daysToBars(4, 13, 39), 39);
});

test("formatPercent treats the API value as a fraction", () => {
  assert.match(formatPercent(0.0125), /1[.,]25/);
});

test("uncertainty is normalized by base price", () => {
  const result = computeUncertainty([{ p10: 99, p90: 101 }], 100);
  assert.equal(result.label, "Narrow");
  assert.equal(result.relativeWidth, 0.02);
});

test("daily aggregation keeps the final market bar per day", () => {
  const predictions = [
    { step: 1, timestamp: "2026-07-20T14:00:00Z", p50: 1 },
    { step: 2, timestamp: "2026-07-20T20:00:00Z", p50: 2 },
    { step: 3, timestamp: "2026-07-21T20:00:00Z", p50: 3 },
  ];
  const rows = aggregateDaily(predictions, "America/New_York");
  assert.equal(rows.length, 2);
  assert.equal(rows[0].p50, 2);
  assert.equal(rows[0].step, "Day 1");
});
