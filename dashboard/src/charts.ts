// The charts, drawn with Observable Plot from the query results.
import * as Plot from "@observablehq/plot";
import { LEVELS, colorAt } from "./colors";
import type { Row } from "./db";

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
function interval(bucket: string): "hour" | "day" | "monday" | "month" {
  return bucket === "week" ? "monday" : bucket === "month" ? "month" : (bucket as "hour" | "day");
}

// Hours in UTC on the 24-hour clock, like the rest of the page; midnight shows the day.
function hourTick(moment: Date): string {
  const hour = moment.getUTCHours();
  if (hour !== 0) return `${String(hour).padStart(2, "0")}:00`;
  return moment.toLocaleDateString("en-US", { month: "short", day: "numeric", timeZone: "UTC" });
}

// The magnitude levels' stacking order, from the baseline (or the left) on: the strongest level
// in front, Below 2.5 the farthest, and No magnitude (only with All) last.
const STACK = [...LEVELS.slice(0, 4).reverse(), ...LEVELS.slice(4)].map((l) => l.label);

// The color scale of the magnitude levels, with the chart's own legend.
const LEVEL_COLORS = {
  domain: LEVELS.map((l) => l.label),
  range: LEVELS.map((l) => l.color),
  legend: true,
  label: "Magnitude",
};

export type TimeStyle = "areas" | "stacked" | "lines" | "trend";

const STYLE_LABELS: Record<TimeStyle, string> = {
  areas: "stacked by magnitude level",
  stacked: "split by magnitude level",
  lines: "one line per magnitude level",
  trend: "per magnitude level, with each level's linear trend",
};

export function overTime(figure: HTMLElement, rows: Row[], bucket: string, style: TimeStyle): void {
  const stack = STACK;
  const point = (r: Row, l: (typeof LEVELS)[number]) => ({
    period: new Date(r.period as number),
    level: l.label,
    events: r[l.key] as number,
  });
  // Columns: one segment per period and level with events. Areas, lines and trends: every
  // period of each level that has events at all, zeros included.
  const shown = LEVELS.filter((l) => rows.some((r) => (r[l.key] as number) > 0));
  const data =
    style === "stacked"
      ? rows.flatMap((r) => LEVELS.filter((l) => (r[l.key] as number) > 0).map((l) => point(r, l)))
      : rows.flatMap((r) => shown.map((l) => point(r, l)));
  const xy = { x: "period", y: "events" };
  const marks: Record<TimeStyle, () => Plot.Markish[]> = {
    areas: () => [
      // The tip names the level once, as Magnitude (the series channel would repeat it).
      Plot.areaY(data, {
        ...xy,
        fill: "level",
        order: stack,
        curve: "linear",
        tip: { format: { z: false } },
      }),
      // A single period has no area to draw: a column stands for it.
      Plot.rectY(rows.length === 1 ? data : [], {
        ...xy,
        fill: "level",
        order: stack,
        interval: interval(bucket),
      }),
    ],
    stacked: () => [
      Plot.rectY(data, { ...xy, fill: "level", order: stack, interval: interval(bucket), tip: true }),
    ],
    lines: () => [
      // The crosshair, in the text color so it follows the theme.
      Plot.ruleX(data, Plot.pointerX({ x: "period", stroke: "currentColor", strokeOpacity: 0.35 })),
      Plot.lineY(data, { ...xy, z: "level", stroke: "level", strokeWidth: 2, curve: "linear" }),
      // A single period has no line to draw: a dot stands for it.
      Plot.dot(rows.length === 1 ? data : [], { ...xy, fill: "level", r: 4 }),
      Plot.tip(data, Plot.pointerX({ ...xy, stroke: "level" })),
    ],
    trend: () => [
      Plot.dot(data, { ...xy, fill: "level", r: 3, fillOpacity: 0.6, tip: true }),
      // Least squares per level; a single period gives no line.
      Plot.linearRegressionY(data, { ...xy, stroke: "level", strokeWidth: 2, ci: 0 }),
    ],
  };
  draw(
    figure,
    rows,
    (width) =>
      Plot.plot({
        width,
        height: 260,
        marginLeft: 50,
        x: { type: "utc", label: null, ...(bucket === "hour" ? { tickFormat: hourTick } : {}) },
        y: { label: `Events per ${bucket}`, grid: true },
        color: LEVEL_COLORS,
        marks: [...marks[style](), Plot.ruleY([0])],
      }),
    `Events per ${bucket}, ${STYLE_LABELS[style]}`,
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

/** The same chart from depth_bins.sql, when there are too many events to send one by one:
 * each bin at its centre, weighing as many events as it holds. */
export function depthBins(figure: HTMLElement, bins: Row[]): void {
  const centres = bins.map((b) => ({
    magnitude: (b.magnitude_from as number) + 0.05,
    depth_km: (b.depth_from_km as number) + 2.5,
    events: b.events as number,
  }));
  draw(
    figure,
    centres,
    (width) =>
      Plot.plot({
        width,
        height: 220,
        marginLeft: 50,
        x: { label: "Magnitude" },
        y: { label: "Depth (km)", reverse: true, grid: true },
        color: { scheme: "oranges", type: "log", legend: false },
        marks: [
          Plot.dot(
            centres,
            Plot.hexbin({ fill: "sum" }, { x: "magnitude", y: "depth_km", fill: "events", binWidth: 8 }),
          ),
        ],
      }),
    "Depth against magnitude, as hexagons shaded by number of events",
  );
}

/** The top countries or regions: each bar's length is its share of all the events in the
 * bars shown (they add up to 100%), split by magnitude level in the legend's colors, with its
 * number of events at its end. */
export function topBars(
  figure: HTMLElement,
  rows: Row[],
  name: (r: Row) => string,
  label: string,
  place: "country" | "region",
): void {
  const total = rows.reduce((sum, r) => sum + (r.events as number), 0);
  const percent = (events: number) => (100 * events) / total;
  const bars = rows.map((r) => ({
    name: name(r),
    events: r.events as number,
    share: percent(r.events as number),
  }));
  const segments = rows.flatMap((r) =>
    LEVELS.filter((l) => (r[l.key] as number) > 0).map((l) => ({
      name: name(r),
      level: l.label,
      events: r[l.key] as number,
      share: percent(r[l.key] as number),
    })),
  );
  const asPercent = (share: number) => `${share.toFixed(1)}%`;
  const Place = place === "country" ? "Country" : "Region";
  const places = place === "country" ? "countries" : "regions";
  draw(
    figure,
    rows,
    (width) =>
      Plot.plot({
        width,
        height: Math.max(80, bars.length * 24 + 30),
        marginLeft: 150,
        marginRight: 60, // room for the count after the longest bar
        x: { label: `% of the events in these ${places}`, grid: true, tickFormat: (s: number) => `${s}%` },
        y: { label: null, domain: bars.map((d) => d.name) },
        color: LEVEL_COLORS,
        marks: [
          Plot.barX(segments, {
            x: "share",
            y: "name",
            fill: "level",
            order: STACK,
            channels: { [Place]: "name", Events: "events" },
            tip: {
              format: {
                [Place]: true,
                fill: true,
                x: asPercent,
                Events: (n: number) => n.toLocaleString("en-US"),
                y: false,
              },
            },
          }),
          Plot.text(bars, {
            x: "share",
            y: "name",
            text: (d: { events: number }) => d.events.toLocaleString("en-US"),
            dx: 4,
            textAnchor: "start",
          }),
        ],
      }),
    label,
  );
}
