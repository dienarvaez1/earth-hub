// The charts, drawn with Observable Plot from the query results.
import * as Plot from "@observablehq/plot";
import { LEVELS, colorAt } from "./colors";
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
  const plot = chart(Math.max(280, figure.clientWidth || 600));
  // With a color legend Plot returns a <figure> around the legend and the chart's <svg>.
  const svg = plot.tagName.toLowerCase() === "svg" ? plot : (plot.querySelector(":scope > svg") ?? plot);
  svg.setAttribute("role", "img");
  svg.setAttribute("aria-label", label);
  figure.append(plot);
}

// Plot's own interval names; ISO weeks start on Monday, like the SQL's.
function interval(bucket: string): "day" | "monday" | "month" {
  return bucket === "week" ? "monday" : bucket === "day" ? "day" : "month";
}

export function overTime(figure: HTMLElement, rows: Row[], bucket: string): void {
  // One segment per period and level with events, stacked bottom to top in LEVELS' order.
  const order = LEVELS.map((l) => l.label);
  const data = rows.flatMap((r) =>
    LEVELS.filter((l) => (r[l.key] as number) > 0).map((l) => ({
      period: new Date(r.period as number),
      level: l.label,
      events: r[l.key] as number,
    })),
  );
  draw(
    figure,
    rows,
    (width) =>
      Plot.plot({
        width,
        height: 260,
        marginLeft: 50,
        x: { type: "utc", label: null },
        y: { label: `Events per ${bucket}`, grid: true },
        color: {
          domain: order,
          range: LEVELS.map((l) => l.color),
          legend: true,
          label: "Magnitude",
        },
        marks: [
          Plot.rectY(data, {
            x: "period",
            y: "events",
            fill: "level",
            order,
            interval: interval(bucket),
            tip: true,
          }),
          Plot.ruleY([0]),
        ],
      }),
    `Events per ${bucket}, split by magnitude level`,
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
          // Each bar in the map's color for the middle of its bin, as in the map's legend.
          Plot.rectY(rows, {
            x1: "bin_start",
            x2: "bin_end",
            y: "events",
            fill: (r: Row) => colorAt(((r.bin_start as number) + (r.bin_end as number)) / 2),
            inset: 0.5,
          }),
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
