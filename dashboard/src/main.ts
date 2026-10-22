// The dashboard: loads the snapshot, runs the queries for the current filters and draws
// the summary, the map, the charts and the table.
import "./style.css";
import * as charts from "./charts";
import { DEFAULT_FILTER, Db, type Filter, type Row } from "./db";
import { EventMap, legend } from "./map";
import { BUCKET, DEFAULT_PERIOD, type Period, day, firstDay, lastDay } from "./periods";
import { countryLabel, countryOrder, regionLabel, regionOrder } from "./places";

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
  mapClustered: boolean;
  mapSinglePoints: number;
  summary: Row | null;
  // Runs every report for a filter, then restores the form's: lets the browser tests compare
  // DuckDB-WASM's answers with DuckDB's in Python on the same file. Read-only.
  queryAll?: (filter: Partial<Filter>) => Promise<Record<string, Row[]>>;
  // Resolves once the map has drawn what it was last given (its clustering runs in a worker).
  mapIdle?: () => Promise<void>;
  // Opens an event's popup, as a click on its point does; false if the map does not show it.
  openPopup?: (key: string) => boolean;
}
declare global {
  interface Window {
    __earthHub: Status;
  }
}
const status: Status = { ready: false, error: null, updates: 0, timings: {}, mapFeatures: 0, mapClustered: false, mapSinglePoints: 0, summary: null };
window.__earthHub = status;

const REPORTS = [
  "summary",
  "over_time",
  "magnitude_bins",
  "top_countries",
  "top_regions",
  "significant",
  "largest",
  "map_points",
  "date_range",
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
  $("#legend").replaceWith(legend());
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

  status.mapIdle = () => map.idle();
  status.openPopup = (key) => map.openPopup(key);

  let pending: number | undefined;
  // On the document: the time bucket select sits outside the form (form="filters").
  document.addEventListener("input", (event) => {
    const target = event.target as HTMLInputElement;
    if (target.form !== form) return;
    if (target.name === "country") {
      select("region").value = "";
    }
    if (target.name === "start" || target.name === "end") {
      select("period").value = "custom"; // picking a day is a custom period
      clampDay(target);
    }
    if (target.name === "period") {
      periodChanged();
    }
    window.clearTimeout(pending);
    pending = window.setTimeout(update, 150);
  });
  form.addEventListener("reset", () =>
    window.setTimeout(() => {
      periodChanged();
      update();
    }, 0),
  );
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
  // Custom days run from the first day with data to today; the calendar greys out the rest.
  const [range] = await db.query("date_range");
  for (const name of ["start", "end"]) {
    const input = form.elements.namedItem(name) as HTMLInputElement;
    if (range.first_event !== null) input.min = day(new Date(range.first_event as number));
    input.max = day(new Date());
  }
  fillCountries(await db.query("countries"));
  fill("event_type", await db.query("event_types"), (r) => String(r.event_type), DEFAULT_EVENT_TYPE);
  fillRegions(await db.query("regions"));
}

// The place menus show US states and territories as "US-<name>", grouped (places.ts).
const fillCountries = (rows: Row[]) =>
  fill("country", rows, (r) => String(r.country), "", countryLabel, countryOrder);
const fillRegions = (rows: Row[]) =>
  fill("region", rows, (r) => String(r.region), "", regionLabel, regionOrder);

// The type shown at first and after Reset: other types (quarry blasts, explosions...) only
// when chosen. "All" when the data has no earthquakes.
const DEFAULT_EVENT_TYPE = "earthquake";

/** Fills a select's options, keeping its choice; `initial` is chosen on the first fill and
 * is the form's default (what Reset returns to); `label` is what an option shows for its
 * value, and `order` puts the options in the menu's order (as given by default). */
function fill(
  name: string,
  rows: Row[],
  value: (r: Row) => string,
  initial = "",
  label: (v: string) => string = (v) => v,
  order: (rows: Row[], value: (r: Row) => string) => Row[] = (rows) => rows,
): void {
  const element = select(name);
  const first = element.dataset.filled === undefined;
  const current = first ? initial : element.value;
  const all = new Option("All", "", initial === "");
  element.replaceChildren(all);
  for (const row of order(rows, value)) {
    const v = value(row);
    const text = `${label(v)} (${numbers.format(row.events as number)})`;
    element.append(new Option(text, v, v === initial));
  }
  element.value = rows.some((r) => value(r) === current) ? current : "";
  if (element.value === "") all.defaultSelected = true; // the initial type is not in the data
  element.dataset.filled = "true";
}

// The period last chosen from the presets: Custom starts from its days.
let lastPreset: Period = DEFAULT_PERIOD;

function periodChanged(): void {
  const period = select("period").value as Period;
  const custom = period === "custom";
  for (const label of form.querySelectorAll<HTMLElement>(".custom-date")) label.hidden = !custom;
  const start = form.elements.namedItem("start") as HTMLInputElement;
  const end = form.elements.namedItem("end") as HTMLInputElement;
  if (custom && !start.value && !end.value) {
    start.value = firstDay(lastPreset, new Date()) ?? "";
    end.value = lastDay(lastPreset, new Date()) ?? "";
    clampDay(start);
    clampDay(end);
  }
  if (!custom) {
    lastPreset = period;
    start.value = end.value = "";
    (document.querySelector('select[name="time_bucket"]') as HTMLSelectElement).value = BUCKET[period];
  }
}

/** Keeps a typed day within the calendar's limits (picked days already are). */
function clampDay(input: HTMLInputElement): void {
  if (!input.value) return;
  if (input.min && input.value < input.min) input.value = input.min;
  if (input.max && input.value > input.max) input.value = input.max;
}

/** The magnitude band's bounds, from its "min:max" value (an empty side: no limit). */
function magnitudeBounds(value: string | null): [number | null, number | null] {
  const bound = (s: string | undefined) => (s ? Number(s) : null);
  const [min, max] = (value ?? "").split(":");
  return [bound(min), bound(max)];
}

/** The filter's time bounds: a preset as of now, or the custom days (either may be empty). */
function timeBounds(data: FormData): [string | null, string | null] {
  const period = ((data.get("period") as string | null) || DEFAULT_PERIOD) as Period;
  const midnight = (d: string | null) => (d ? `${d} 00:00:00` : null);
  const custom = period === "custom";
  const start = custom ? (data.get("start") as string | null) || null : firstDay(period, new Date());
  const end = custom ? (data.get("end") as string | null) || null : lastDay(period, new Date());
  // The last day is included: the filter's end is the next midnight, exclusive.
  return [midnight(start), midnight(end && nextDay(end))];
}

function readFilter(): Filter {
  const data = new FormData(form);
  const text = (name: string) => (data.get(name) as string | null)?.trim() || null;
  const [start_time, end_time] = timeBounds(data);
  const [min_magnitude, max_magnitude] = magnitudeBounds(text("magnitude"));
  return {
    ...DEFAULT_FILTER,
    start_time,
    end_time,
    min_magnitude,
    max_magnitude,
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
  const order = new FormData(form).get("order") === "largest" ? "largest" : "significant";
  if (order !== shownOrder) tableSort = null; // other events: back to their own order
  shownOrder = order;
  const significant = await db.query(order);
  const regionOptions = await db.query("regions");
  status.timings.last_query_ms = performance.now() - t0;

  fillRegions(regionOptions);
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
  status.mapClustered = map.clustered;
  status.mapSinglePoints = map.singleCount;
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

// The table's columns that sort, and how. The query picks the events (latest or largest);
// a header only reorders those shown: ascending, then descending, then the query's order.
type SortKey = "event_ts" | "magnitude" | "place" | "depth_km";
type TableSort = { key: SortKey; direction: "ascending" | "descending" };
const COLUMNS: [string, SortKey | null, string][] = [
  ["Time (UTC)", "event_ts", ""],
  ["Magnitude", "magnitude", "num"],
  ["Place", "place", ""],
  ["Depth (km)", "depth_km", "num"],
  ["", null, ""],
];
let tableSort: TableSort | null = null;
let shownOrder = "significant";
let shownRows: Row[] = [];

function sorted(rows: Row[], sort: TableSort | null): Row[] {
  if (sort === null) return rows;
  const sign = sort.direction === "ascending" ? 1 : -1;
  const compare = (a: Row, b: Row): number => {
    const x = a[sort.key];
    const y = b[sort.key];
    if (sort.key === "place") {
      const p = String(x ?? "");
      const q = String(y ?? "");
      if (!p || !q) return p ? -1 : q ? 1 : 0; // no place: last either way
      return sign * p.localeCompare(q);
    }
    if (x === null || y === null) return x === null ? (y === null ? 0 : 1) : -1;
    return sign * ((x as number) - (y as number));
  };
  return rows.map((row, i) => ({ row, i })).sort((a, b) => compare(a.row, b.row) || a.i - b.i).map((e) => e.row);
}

function nextSort(key: SortKey): TableSort | null {
  if (tableSort?.key !== key) return { key, direction: "ascending" };
  return tableSort.direction === "ascending" ? { key, direction: "descending" } : null;
}

function showSignificant(rows: Row[]): void {
  shownRows = rows;
  const table = $<HTMLTableElement>("#significant");
  table.replaceChildren();
  table.createCaption().textContent = rows.length ? "" : "No significant events match the filters.";
  if (rows.length === 0) return;
  const head = table.createTHead().insertRow();
  for (const [text, key, cls] of COLUMNS) {
    const th = document.createElement("th");
    th.className = cls;
    th.scope = "col";
    if (key === null) {
      th.textContent = text;
    } else {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "sort";
      button.dataset.sort = key;
      const active = tableSort?.key === key ? tableSort.direction : null;
      th.setAttribute("aria-sort", active ?? "none");
      button.textContent = text;
      const arrow = document.createElement("span");
      arrow.className = "arrow";
      arrow.setAttribute("aria-hidden", "true");
      arrow.textContent = active === "ascending" ? "▲" : active === "descending" ? "▼" : "↕";
      button.append(arrow);
      button.title = `Sort by ${text.toLowerCase()}`;
      button.addEventListener("click", () => {
        tableSort = nextSort(key);
        showSignificant(shownRows);
        table.querySelector<HTMLButtonElement>(`button[data-sort="${key}"]`)?.focus();
      });
      th.append(button);
    }
    head.append(th);
  }
  const body = table.createTBody();
  for (const r of sorted(rows, tableSort)) {
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
