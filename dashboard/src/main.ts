// The dashboard: loads the snapshot, runs the queries for the current filters and draws
// the summary, the map, the charts and the table.
import "./style.css";
import * as charts from "./charts";
import { DEFAULT_FILTER, Db, type Filter, type Row } from "./db";
import { EventMap } from "./map";

interface Meta {
  run_id: string;
  written_at: string;
  rows: number;
  bytes: number;
}

// What the browser tests and the performance measurements read.
interface Status {
  ready: boolean;
  error: string | null;
  updates: number;
  timings: Record<string, number>;
  mapFeatures: number;
  summary: Row | null;
  // Runs every report for a filter, then restores the form's: lets the browser tests compare
  // DuckDB-WASM's answers with DuckDB's in Python on the same file. Read-only.
  queryAll?: (filter: Partial<Filter>) => Promise<Record<string, Row[]>>;
}
declare global {
  interface Window {
    __earthHub: Status;
  }
}
const status: Status = { ready: false, error: null, updates: 0, timings: {}, mapFeatures: 0, summary: null };
window.__earthHub = status;

const REPORTS = [
  "summary",
  "over_time",
  "magnitude_bins",
  "top_countries",
  "top_regions",
  "significant",
  "map_points",
  "countries",
  "regions",
  "event_types",
];

const $ = <T extends HTMLElement>(selector: string) => document.querySelector(selector) as T;
const form = $<HTMLFormElement>("#filters");
const numbers = new Intl.NumberFormat("en-US");
const select = (name: string) => form.elements.namedItem(name) as unknown as HTMLSelectElement;

async function fetchOk(path: string): Promise<Response> {
  const response = await fetch(path);
  if (response.status === 404) throw new Error("No data has been exported yet.");
  if (!response.ok) throw new Error(`${path} returned ${response.status}.`);
  return response;
}

async function start(): Promise<void> {
  const t0 = performance.now();
  const map = new EventMap($("#map"));
  const meta = fetchOk("/data/meta.json").then((r) => r.json() as Promise<Meta>);
  const parquet = fetchOk("/data/earthquakes.parquet").then(async (r) => {
    const bytes = new Uint8Array(await r.arrayBuffer());
    status.timings.download_ms = performance.now() - t0;
    return bytes;
  });
  // A missing snapshot is reported as such, not as whatever DuckDB makes of it.
  const [db] = await Promise.all([Db.open(parquet, status.timings), meta, parquet]);
  status.timings.database_ms = performance.now() - t0;
  $("#as-of").textContent =
    `${numbers.format((await meta).rows)} events, exported ${(await meta).written_at.slice(0, 16).replace("T", " ")} UTC`;
  await fillOptions(db);

  // One query sequence at a time: the filter table is shared.
  let running: Promise<unknown> = Promise.resolve();
  const queue = <T>(task: () => Promise<T>): Promise<T> => {
    const next = running.then(task);
    running = next.catch(() => undefined);
    return next;
  };
  const update = () => queue(() => render(db, map)).catch(fail);
  status.queryAll = (filter) =>
    queue(async () => {
      await db.setFilter({ ...DEFAULT_FILTER, ...filter });
      const results: Record<string, Row[]> = {};
      for (const name of REPORTS) results[name] = await db.query(name);
      await db.setFilter(readFilter());
      return results;
    });

  let pending: number | undefined;
  // On the document: the time bucket select sits outside the form (form="filters").
  document.addEventListener("input", (event) => {
    const target = event.target as HTMLInputElement;
    if (target.form !== form) return;
    if (target.name === "country") {
      select("region").value = "";
    }
    window.clearTimeout(pending);
    pending = window.setTimeout(update, 150);
  });
  form.addEventListener("reset", () => window.setTimeout(update, 0));
  let width = window.innerWidth;
  window.addEventListener("resize", () => {
    if (Math.abs(window.innerWidth - width) < 40) return;
    width = window.innerWidth;
    window.clearTimeout(pending);
    pending = window.setTimeout(update, 250);
  });

  await update();
  await map.idle();
  status.timings.first_map_ms = performance.now() - t0;
  status.ready = true;
}

async function fillOptions(db: Db): Promise<void> {
  await db.setFilter(DEFAULT_FILTER);
  fill("country", await db.query("countries"), (r) => String(r.country));
  fill("event_type", await db.query("event_types"), (r) => String(r.event_type));
  fill("region", await db.query("regions"), (r) => String(r.region));
}

function fill(name: string, rows: Row[], value: (r: Row) => string): void {
  const element = select(name);
  const current = element.value;
  element.replaceChildren(new Option("All", ""));
  for (const row of rows) {
    element.append(new Option(`${value(row)} (${numbers.format(row.events as number)})`, value(row)));
  }
  element.value = rows.some((r) => value(r) === current) ? current : "";
}

function readFilter(): Filter {
  const data = new FormData(form);
  const text = (name: string) => (data.get(name) as string | null)?.trim() || null;
  const number = (name: string) => {
    const value = text(name);
    return value === null || Number.isNaN(Number(value)) ? null : Number(value);
  };
  const end = text("end");
  return {
    ...DEFAULT_FILTER,
    start_time: text("start") && `${text("start")} 00:00:00`,
    // The "To" day is included: the filter's end is the next midnight, exclusive.
    end_time: end && `${nextDay(end)} 00:00:00`,
    min_magnitude: number("min_magnitude"),
    max_magnitude: number("max_magnitude"),
    country: text("country"),
    region: text("region"),
    event_type: text("event_type"),
    include_deleted: data.get("include_deleted") === "on",
    time_bucket: (text("time_bucket") ?? "month") as Filter["time_bucket"],
  };
}

function nextDay(day: string): string {
  const date = new Date(`${day}T00:00:00Z`);
  date.setUTCDate(date.getUTCDate() + 1);
  return date.toISOString().slice(0, 10);
}

async function render(db: Db, map: EventMap): Promise<void> {
  const t0 = performance.now();
  const filter = readFilter();
  await db.setFilter(filter);
  const [summary] = await db.query("summary");
  const points = await db.query("map_points");
  const time = await db.query("over_time");
  const bins = await db.query("magnitude_bins");
  const countries = await db.query("top_countries");
  const regions = await db.query("top_regions");
  const significant = await db.query("significant");
  const regionOptions = await db.query("regions");
  status.timings.last_query_ms = performance.now() - t0;

  fill("region", regionOptions, (r) => String(r.region));
  showSummary(summary);
  await map.show(points);
  charts.overTime($("#chart-time"), time, filter.time_bucket);
  charts.magnitudes($("#chart-magnitude"), bins);
  charts.depthAgainstMagnitude($("#chart-depth"), points);
  charts.topBars($("#chart-countries"), countries, (r) => String(r.country), "Top countries by number of events");
  charts.topBars(
    $("#chart-regions"),
    regions,
    (r) => (r.country === "Unknown" ? String(r.region) : `${r.region}, ${r.country}`),
    "Top regions by number of events",
  );
  showSignificant(significant);

  status.timings.last_update_ms = performance.now() - t0;
  status.mapFeatures = map.featureCount;
  status.summary = summary;
  status.updates += 1;
}

function showSummary(s: Row): void {
  const day = (ms: unknown) => (ms === null ? "–" : new Date(ms as number).toISOString().slice(0, 10));
  const cards: [string, string, string, string][] = [
    ["events", "Events", numbers.format(s.events as number), `${numbers.format(s.with_magnitude as number)} with a magnitude`],
    ["max", "Largest", s.max_magnitude === null ? "–" : `M ${s.max_magnitude}`, String(s.max_magnitude_place ?? "")],
    ["range", "First and last", s.first_event === null ? "–" : `${day(s.first_event)}`, s.last_event === null ? "" : `to ${day(s.last_event)}`],
    ["countries", "Countries", numbers.format(s.countries as number), `${numbers.format(s.tsunami_flagged as number)} flagged for tsunami`],
  ];
  $("#summary").replaceChildren(
    ...cards.map(([id, label, value, detail]) => {
      const card = document.createElement("div");
      card.className = "card";
      card.dataset.testid = `summary-${id}`;
      for (const [cls, text] of [["label", label], ["value", value], ["detail", detail]]) {
        const part = document.createElement("div");
        part.className = cls;
        part.textContent = text;
        card.append(part);
      }
      return card;
    }),
  );
}

function showSignificant(rows: Row[]): void {
  const table = $<HTMLTableElement>("#significant");
  table.replaceChildren();
  table.createCaption().textContent = rows.length ? "" : "No significant events match the filters.";
  if (rows.length === 0) return;
  const head = table.createTHead().insertRow();
  for (const [text, cls] of [["Time (UTC)", ""], ["Magnitude", "num"], ["Place", ""], ["Depth (km)", "num"], ["", ""]]) {
    const th = document.createElement("th");
    th.textContent = text;
    th.className = cls;
    th.scope = "col";
    head.append(th);
  }
  const body = table.createTBody();
  for (const r of rows) {
    const row = body.insertRow();
    const cell = (text: string, cls = "") => {
      const td = row.insertCell();
      td.textContent = text;
      td.className = cls;
      return td;
    };
    cell(new Date(r.event_ts as number).toISOString().slice(0, 16).replace("T", " "));
    cell(String(r.magnitude), "num");
    cell(String(r.place ?? ""));
    cell((r.depth_km as number).toFixed(1), "num");
    const link = cell("");
    if (typeof r.url === "string" && r.url.startsWith("https://")) {
      const a = document.createElement("a");
      a.href = r.url;
      a.rel = "noopener";
      a.textContent = "USGS";
      link.append(a);
    }
  }
}

function fail(error: unknown): void {
  const message = error instanceof Error ? error.message : String(error);
  status.error = message;
  const line = $("#as-of");
  line.textContent = message;
  line.classList.add("error");
  console.error(error);
}

start().catch(fail);
