import { formatCurrency, formatTimestamp } from "./lib.js";

const NS = "http://www.w3.org/2000/svg";
function element(name, attributes, text) {
  const node = document.createElementNS(NS, name);
  Object.entries(attributes).forEach(([key, value]) => node.setAttribute(key, value));
  if (text !== undefined) node.textContent = text;
  return node;
}

export function drawChart(svg, result, history, metadata, onSelect) {
  svg.replaceChildren();
  const width = Math.max(300, svg.clientWidth);
  svg.setAttribute("viewBox", "0 0 " + width + " 360");
  const observed = history.length ? [...history] : [{ timestamp: result.features_timestamp, price: result.base_price }];
  if (observed.at(-1).timestamp !== result.features_timestamp) observed.push({ timestamp: result.features_timestamp, price: result.base_price });
  const predictions = result.predictions;
  const values = [...observed.map(r => r.price), ...predictions.flatMap(r => [r.p10, r.p90])];
  const minimum = Math.min(...values), maximum = Math.max(...values);
  const pad = Math.max((maximum - minimum) * .15, .1);
  const low = minimum - pad, high = maximum + pad;
  const left = 76, right = width - 16, top = 35, bottom = 307;
  const x = index => left + index / Math.max(1, observed.length + predictions.length - 1) * (right - left);
  const y = price => bottom - (price - low) / (high - low) * (bottom - top);
  for (let i = 0; i <= 4; i++) {
    const value = low + (high - low) * i / 4;
    svg.append(element("line", { x1: left, y1: y(value), x2: right, y2: y(value), class: "chart-grid" }));
    svg.append(element("text", { x: left - 10, y: y(value) + 4, "text-anchor": "end" }, formatCurrency(value, metadata.currency)));
  }
  const points = rows => rows.map(([px, py]) => px + "," + py).join(" ");
  const boundary = x(observed.length - 1);
  svg.append(element("polygon", { class: "forecast-band", points: points([
    [boundary, y(result.base_price)],
    ...predictions.map((r, i) => [x(observed.length + i), y(r.p90)]),
    ...predictions.map((r, i) => [x(observed.length + i), y(r.p10)]).reverse(),
  ]) }));
  svg.append(element("polyline", { class: "history-line", points: points(observed.map((r, i) => [x(i), y(r.price)])) }));
  svg.append(element("polyline", { class: "forecast-line", points: points([[boundary, y(result.base_price)], ...predictions.map((r, i) => [x(observed.length + i), y(r.p50)])]) }));
  svg.append(element("line", { x1: boundary, x2: boundary, y1: top, y2: bottom, class: "boundary" }));
  svg.append(element("text", { x: Math.min(boundary + 10, right - 120), y: 22 }, "FORECAST START"));
  const labels = [[left, observed[0].timestamp, "start"], [right, predictions.at(-1).timestamp, "end"]];
  labels.forEach(([px, timestamp, anchor], index) => svg.append(element("text", { x: px, y: width < 500 ? 330 + index * 20 : 340, "text-anchor": anchor }, formatTimestamp(timestamp, metadata.market_timezone))));
  const cursor = element("line", { class: "crosshair", y1: top, y2: bottom });
  const point = element("circle", { class: "chart-point", r: 5 });
  svg.append(cursor, point);
  let selected = 0;
  function select(index) {
    selected = Math.max(0, Math.min(predictions.length - 1, index));
    const row = predictions[selected], px = x(observed.length + selected);
    cursor.setAttribute("x1", px); cursor.setAttribute("x2", px);
    point.setAttribute("cx", px); point.setAttribute("cy", y(row.p50));
    onSelect(row);
  }
  svg.onpointermove = event => {
    const matrix = svg.getScreenCTM();
    if (!matrix) return;
    const p = new DOMPoint(event.clientX, event.clientY).matrixTransform(matrix.inverse());
    select(Math.round((p.x - left) / (right - left) * (observed.length + predictions.length - 1)) - observed.length);
  };
  svg.onkeydown = event => {
    if (["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) {
      event.preventDefault();
      select(event.key === "Home" ? 0 : event.key === "End" ? predictions.length - 1 : selected + (event.key === "ArrowRight" ? 1 : -1));
    }
  };
  select(0);
  return select;
}
