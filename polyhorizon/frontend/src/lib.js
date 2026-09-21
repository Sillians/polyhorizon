export function daysToBars(days, barsPerDay, maxPredictionLength) {
  const parsed = Number(days);
  const bars = Number.isFinite(parsed) && parsed >= 1 ? parsed * barsPerDay : barsPerDay;
  return Math.min(bars, maxPredictionLength);
}

export function formatCurrency(value, currency = "USD") {
  if (value == null || value === "" || !Number.isFinite(Number(value))) return "—";
  return new Intl.NumberFormat(undefined, {
    style: "currency",
    currency,
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(Number(value));
}

export function formatPercent(value) {
  if (value == null || value === "" || !Number.isFinite(Number(value))) return "—";
  return new Intl.NumberFormat(undefined, {
    style: "percent",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(Number(value));
}

export function formatTimestamp(value, timeZone = "America/New_York") {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "—";
  return new Intl.DateTimeFormat(undefined, {
    timeZone,
    dateStyle: "medium",
    timeStyle: "short",
  }).format(date);
}

export function computeUncertainty(predictions, basePrice) {
  if (!predictions?.length || !Number.isFinite(Number(basePrice)) || Number(basePrice) <= 0) {
    return { label: "—", relativeWidth: null };
  }
  const widths = predictions.map((row) => Math.abs(Number(row.p90) - Number(row.p10)));
  const relativeWidth = widths.reduce((sum, width) => sum + width, 0) / widths.length / Number(basePrice);
  let label = "Moderate";
  if (relativeWidth <= 0.02) label = "Narrow";
  if (relativeWidth >= 0.05) label = "Wide";
  return { label, relativeWidth };
}

function marketDateKey(timestamp, timeZone) {
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  return formatter.format(new Date(timestamp));
}

export function aggregateDaily(predictions, timeZone = "America/New_York") {
  const byDate = new Map();
  for (const row of predictions || []) {
    byDate.set(marketDateKey(row.timestamp, timeZone), row);
  }
  return Array.from(byDate.values()).map((row, index) => ({
    ...row,
    step: `Day ${index + 1}`,
  }));
}
