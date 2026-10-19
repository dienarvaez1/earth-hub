// The charts, drawn with Observable Plot from the query results.
import * as Plot from "@observablehq/plot";
import type { Row } from "./db";

const ACCENT = "#c2410c";

function draw(figure: HTMLElement, rows: Row[], chart: (width: number) => Element, label: string) {
  figure.replaceChildren();
  if (rows.length === 0) {
    const empty = document.createElement("p");
    empty.className = "empty";
    empty.textContent = "No events match the filters.";
    figure.append(empty);
    return;
  }
  const svg = chart(Math.max(280, figure.clientWidth || 600));
  svg.setAttribute("role", "img");
  svg.setAttribute("aria-label", label);
  figure.append(svg);
}

// Plot's own interval names; ISO weeks start on Monday, like the SQL's.
function interval(bucket: string): "day" | "monday" | "month" {
  return bucket === "week" ? "monday" : bucket === "day" ? "day" : "month";
}

export function overTime(figure: HTMLElement, rows: Row[], bucket: string): void {
  const data = rows.map((r) => ({ ...r, period: new Date(r.period as number) }));
  draw(
    figure,
    rows,
    (width) =>
      Plot.plot({
        width,
        height: 240,
        marginLeft: 50,
        x: { type: "utc", label: null },
        y: { label: `Events per ${bucket}`, grid: true },
        marks: [
          Plot.rectY(data, { x: "period", y: "events", interval: interval(bucket), fill: ACCENT }),
          Plot.ruleY([0]),
          Plot.tip(data, Plot.pointerX({ x: "period", y: "events", interval: interval(bucket) })),
        ],
      }),
    `Events per ${bucket}`,
  );
}

export function magnitudes(figure: HTMLElement, rows: Row[]): void {
  draw(
    figure,
    rows,
    (width) =>
      Plot.plot({
        width,
        height: 220,
        marginLeft: 50,
        x: { label: "Magnitude" },
        y: { label: "Events", grid: true },
        marks: [
          Plot.rectY(rows, { x1: "bin_start", x2: "bin_end", y: "events", fill: ACCENT, inset: 0.5 }),
          Plot.ruleY([0]),
          Plot.tip(rows, Plot.pointerX({ x: (r: Row) => ((r.bin_start as number) + (r.bin_end as number)) / 2, y: "events" })),
        ],
      }),
    "Events per magnitude bin of 0.5",
  );
}

export function depthAgainstMagnitude(figure: HTMLElement, points: Row[]): void {
  const withMagnitude = points.filter((p) => p.magnitude !== null);
  draw(
    figure,
    withMagnitude,
    (width) =>
      Plot.plot({
        width,
        height: 220,
        marginLeft: 50,
        x: { label: "Magnitude" },
        y: { label: "Depth (km)", reverse: true, grid: true },
        color: { scheme: "oranges", type: "log", legend: false },
        marks: [Plot.dot(withMagnitude, Plot.hexbin({ fill: "count" }, { x: "magnitude", y: "depth_km", binWidth: 8 }))],
      }),
    "Depth against magnitude, as hexagons shaded by number of events",
  );
}

export function topBars(figure: HTMLElement, rows: Row[], name: (r: Row) => string, label: string): void {
  const data = rows.map((r) => ({ name: name(r), events: r.events }));
  draw(
    figure,
    rows,
    (width) =>
      Plot.plot({
        width,
        height: Math.max(80, data.length * 24 + 30),
        marginLeft: 150,
        marginRight: 60, // room for the count after the longest bar
        x: { label: "Events", grid: true },
        y: { label: null, domain: data.map((d) => d.name) },
        marks: [
          Plot.barX(data, { x: "events", y: "name", fill: ACCENT }),
          Plot.text(data, { x: "events", y: "name", text: (d: { events: number }) => d.events.toLocaleString("en-US"), dx: 4, textAnchor: "start" }),
        ],
      }),
    label,
  );
}
