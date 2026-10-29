// The map: MapLibre GL on OpenFreeMap tiles. Small result sets show every event as its own
// point; large ones are clustered when zoomed out, except strong events, which are always
// single points drawn on top, so a cluster can never hide them. Very large ones come as cells
// counted in SQL (map_grid.sql), clustered the same way, and once zoomed in, the visible
// area's events replace the cells when there are few enough (map_area.sql).
import * as maplibregl from "maplibre-gl";
import type { GeoJSONSource, MapGeoJSONFeature, MapLayerMouseEvent } from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import workerUrl from "maplibre-gl/dist/maplibre-gl-worker.mjs?url";
import { MAGNITUDE_STOPS } from "./colors";
import type { MapView, Row } from "./db";
import { localTime, utcTime } from "./times";

// MapLibre 6 runs its tile work in a separate worker file, served with the page.
maplibregl.setWorkerUrl(workerUrl);

export const STYLE_URL = "https://tiles.openfreemap.org/styles/positron";

/** Up to this many events, none is clustered. */
export const CLUSTER_ABOVE = 5000;
/** Events at or above this magnitude are never clustered. */
export const STRONG_MAGNITUDE = 6;
/** Above this many events, the map gets cells (map_grid.sql), not every event. */
export const GRID_ABOVE = 25000;
/** The cells' size, in degrees of longitude and latitude. */
export const CELL_DEGREES = 0.25;
/** From this zoom, the visible area's events (map_area.sql) replace the cells, when it holds at
 * most AREA_LIMIT of them. */
export const AREA_ZOOM = 4;
export const AREA_LIMIT = 25000;
/** The zoom the page opens at, the closest it goes, and the zoom a significant event chosen in
 * the table is shown at. */
export const START_ZOOM = 1;
export const MAX_ZOOM = 15;
export const FOCUS_ZOOM = 5;
/** setup.sql's map_view for the whole world: what map_grid.sql and map_strong.sql read. */
export const WORLD: MapView = {
  strong_magnitude: STRONG_MAGNITUDE,
  cell_degrees: CELL_DEGREES,
  west: null,
  south: null,
  east: null,
  north: null,
  area_limit: AREA_LIMIT,
};

// Each feature's number of events: 1 for an event, its count for a cell; clusters add them up.
const COUNT: maplibregl.ExpressionSpecification = ["get", "n"];
// Clusters and cells are drawn as counts, events as points.
const IS_COUNT: maplibregl.ExpressionSpecification = ["any", ["has", "point_count"], ["has", "cell"]];

type Feature = {
  type: "Feature";
  geometry: { type: "Point"; coordinates: number[] };
  properties: Record<string, unknown>;
};

function eventFeature(p: Row): Feature {
  return {
    type: "Feature",
    geometry: { type: "Point", coordinates: [p.longitude as number, p.latitude as number] },
    properties: {
      key: p.key,
      magnitude: p.magnitude,
      depth_km: p.depth_km,
      time: p.event_time_ms,
      place: p.place,
      title: p.title,
      url: p.url,
      deleted: p.review_status === "deleted",
      tsunami: p.tsunami === true,
      n: 1,
    },
  };
}

// Points take their magnitude's color, clusters their strongest event's (colors.ts).
function color(magnitude: maplibregl.ExpressionSpecification): maplibregl.ExpressionSpecification {
  return ["interpolate", ["linear"], magnitude, ...MAGNITUDE_STOPS.flat()] as maplibregl.ExpressionSpecification;
}

/** The color legend: a bar blending the stops, labelled at each; below the first stop and
 * above the last, colors stay at the end ones. */
export function legend(): HTMLElement {
  const low = MAGNITUDE_STOPS[0][0];
  const high = MAGNITUDE_STOPS[MAGNITUDE_STOPS.length - 1][0];
  const at = (m: number) => ((m - low) / (high - low)) * 100;
  const box = document.createElement("div");
  box.className = "legend";
  box.dataset.stops = JSON.stringify(MAGNITUDE_STOPS);
  const title = document.createElement("span");
  title.className = "legend-title";
  title.textContent = "Magnitude";
  const scale = document.createElement("span");
  scale.className = "legend-scale";
  const bar = document.createElement("span");
  bar.className = "legend-bar";
  bar.style.background = `linear-gradient(to right, ${MAGNITUDE_STOPS.map(([m, c]) => `${c} ${at(m)}%`).join(", ")})`;
  scale.append(bar);
  const labels: string[] = [];
  MAGNITUDE_STOPS.forEach(([m], i) => {
    const text = i === 0 ? `≤${m}` : i === MAGNITUDE_STOPS.length - 1 ? `${m}+` : String(m);
    labels.push(text);
    const tick = document.createElement("span");
    tick.className = `legend-tick${i === 0 ? " start" : i === MAGNITUDE_STOPS.length - 1 ? " end" : ""}`;
    tick.style.left = `${at(m)}%`;
    tick.textContent = text;
    scale.append(tick);
  });
  box.append(title, scale);
  box.setAttribute("role", "img");
  box.setAttribute("aria-label", `Dot color by magnitude, light to dark: ${labels.join(", ")}`);
  return box;
}
const MAGNITUDE: maplibregl.ExpressionSpecification = ["coalesce", ["get", "magnitude"], 0];
const POINT_PAINT = {
  "circle-color": color(MAGNITUDE),
  "circle-radius": ["interpolate", ["linear"], MAGNITUDE, 0, 3, 7, 14] as maplibregl.ExpressionSpecification,
  "circle-stroke-width": 0.5,
  "circle-stroke-color": "#44403c",
};

export class EventMap {
  private readonly map: maplibregl.Map;
  private readonly ready: Promise<void>;
  featureCount = 0;
  clustered = false; // whether the last result set was clustered
  singleCount = 0; // events drawn as single points whatever the zoom
  grid = false; // whether the last result set came as cells
  cellCount = 0;
  areaCount: number | null = null; // the visible area's events shown instead of the cells
  private shown: Feature[] = [];
  private strong: Feature[] = [];
  private cells: Feature[] = [];
  private popup: maplibregl.Popup | null = null; // one at a time

  constructor(container: HTMLElement) {
    this.map = new maplibregl.Map({
      container,
      style: STYLE_URL,
      center: [-120, 40],
      zoom: START_ZOOM,
      maxZoom: MAX_ZOOM,
      attributionControl: { compact: true },
      cooperativeGestures: true, // scrolling the page does not zoom the map
    });
    this.map.addControl(new maplibregl.NavigationControl({ showCompass: false }));
    // A resized page closes the popup. Only a change of width does: on phones the address bar
    // changes the height while scrolling.
    let width = window.innerWidth;
    window.addEventListener("resize", () => {
      if (window.innerWidth === width) return;
      width = window.innerWidth;
      this.closePopup();
    });
    this.ready = new Promise((resolve) => this.map.on("load", () => this.addLayers(resolve)));
  }

  private addLayers(done: () => void): void {
    const empty = { type: "FeatureCollection" as const, features: [] };
    this.map.addSource("clustered", {
      type: "geojson",
      data: empty,
      cluster: true,
      clusterMaxZoom: 6,
      clusterRadius: 40,
      clusterProperties: {
        max_magnitude: ["max", ["coalesce", ["get", "max_magnitude"], MAGNITUDE]],
        n: ["+", COUNT],
      },
    });
    this.map.addSource("single", { type: "geojson", data: empty });
    this.map.addLayer({
      id: "clusters",
      type: "circle",
      source: "clustered",
      filter: IS_COUNT,
      paint: {
        "circle-color": color(["get", "max_magnitude"]),
        "circle-opacity": 0.8,
        "circle-radius": ["step", COUNT, 12, 100, 18, 1000, 26, 10000, 34],
        "circle-stroke-width": 1,
        "circle-stroke-color": "#ffffff",
      },
    });
    this.map.addLayer({
      id: "cluster-count",
      type: "symbol",
      source: "clustered",
      filter: IS_COUNT,
      layout: {
        // As MapLibre abbreviates point counts: 1.2k, 15.5k, 156k.
        "text-field": [
          "case",
          [">=", COUNT, 10000],
          ["concat", ["to-string", ["round", ["/", COUNT, 1000]]], "k"],
          [">=", COUNT, 1000],
          ["concat", ["to-string", ["/", ["round", ["/", COUNT, 100]], 10]], "k"],
          ["to-string", COUNT],
        ],
        "text-size": 11,
        "text-font": ["Noto Sans Regular"],
      },
      paint: { "text-color": ["case", [">=", ["get", "max_magnitude"], 4.5], "#ffffff", "#1c1917"] },
    });
    this.map.addLayer({
      id: "points",
      type: "circle",
      source: "clustered",
      filter: ["!", IS_COUNT],
      paint: POINT_PAINT,
    });
    this.map.addLayer({
      id: "single-points",
      type: "circle",
      source: "single",
      layout: { "circle-sort-key": MAGNITUDE }, // the strongest on top
      paint: POINT_PAINT,
    });
    this.map.on("click", "clusters", (event: MapLayerMouseEvent) => this.zoomInto(event.features?.[0]));
    for (const layer of ["points", "single-points"]) {
      this.map.on("click", layer, (event: MapLayerMouseEvent) => this.showPopup(event.features?.[0]));
    }
    for (const layer of ["clusters", "points", "single-points"]) {
      this.map.on("mouseenter", layer, () => (this.map.getCanvas().style.cursor = "pointer"));
      this.map.on("mouseleave", layer, () => (this.map.getCanvas().style.cursor = ""));
    }
    done();
  }

  async show(points: Row[]): Promise<void> {
    await this.ready;
    const features = points.map(eventFeature);
    this.clustered = features.length > CLUSTER_ABOVE;
    const strong = (f: Feature) => (f.properties.magnitude as number | null) !== null && (f.properties.magnitude as number) >= STRONG_MAGNITUDE;
    const single = this.clustered ? features.filter(strong) : features;
    const rest = this.clustered ? features.filter((f) => !strong(f)) : [];
    this.setData("single", single);
    this.setData("clustered", rest);
    this.featureCount = features.length;
    this.singleCount = single.length;
    this.shown = features;
    this.grid = false;
    this.cellCount = 0;
    this.areaCount = null;
  }

  /** Too many events to draw one by one: `cells` (map_grid.sql), clustered as events are, and
   * `strong` (map_strong.sql) as single points on top. */
  async showGrid(cells: Row[], strong: Row[]): Promise<void> {
    await this.ready;
    this.strong = strong.map(eventFeature);
    this.cells = cells.map((c) => ({
      type: "Feature",
      geometry: { type: "Point", coordinates: [c.longitude as number, c.latitude as number] },
      properties: { cell: true, n: c.events, max_magnitude: c.max_magnitude },
    }));
    this.setData("single", this.strong);
    this.grid = true;
    this.clustered = true;
    this.singleCount = this.strong.length;
    this.cellCount = this.cells.length;
    this.showArea(null);
  }

  /** In grid mode: the visible area's events (map_area.sql) in place of the cells, or the
   * cells again (null). */
  showArea(points: Row[] | null): void {
    const area = points === null ? null : points.map(eventFeature);
    this.setData("clustered", area ?? this.cells);
    this.shown = [...this.strong, ...(area ?? [])];
    this.areaCount = area === null ? null : area.length;
    this.featureCount = this.strong.length + (area ?? this.cells).length;
  }

  /** The area map_area.sql should read, as setup.sql's map_view wants it: the visible area and
   * half its size again on every side (so a short pan stays covered), or null when zoomed out
   * too far for it. */
  area(): Pick<MapView, "west" | "south" | "east" | "north"> | null {
    if (this.map.getZoom() < AREA_ZOOM) return null;
    const bounds = this.map.getBounds();
    const dx = (bounds.getEast() - bounds.getWest()) / 2;
    const dy = (bounds.getNorth() - bounds.getSouth()) / 2;
    const south = Math.max(-90, bounds.getSouth() - dy);
    const north = Math.min(90, bounds.getNorth() + dy);
    const span = bounds.getEast() - bounds.getWest() + 2 * dx;
    if (span >= 360) return { west: -180, south, east: 180, north };
    // MapLibre's longitudes continue past ±180 when the view crosses the antimeridian.
    const west = ((((bounds.getWest() - dx + 180) % 360) + 360) % 360) - 180;
    return { west, south, east: west + span, north };
  }

  /** `callback` after every move or zoom, once the map has stopped. */
  onMoveEnd(callback: () => void): void {
    this.map.on("moveend", callback);
  }

  /** A significant event from the table: flies to it at FOCUS_ZOOM and opens its popup,
   * whatever the map shows (its point, a cluster or a cell). */
  focusEvent(row: Row): void {
    const feature = eventFeature(row);
    this.map.flyTo({ center: feature.geometry.coordinates as [number, number], zoom: FOCUS_ZOOM });
    this.showPopup(feature as unknown as MapGeoJSONFeature);
  }

  /** Where the map looks (for the tests). */
  view(): { longitude: number; latitude: number; zoom: number; maxZoom: number } {
    const center = this.map.getCenter();
    return { longitude: center.lng, latitude: center.lat, zoom: this.map.getZoom(), maxZoom: this.map.getMaxZoom() };
  }

  closePopup(): void {
    this.popup?.remove();
    this.popup = null;
  }

  /** Moves the map at once (for the tests). */
  jumpTo(longitude: number, latitude: number, zoom: number): void {
    this.map.jumpTo({ center: [longitude, latitude], zoom });
  }

  private setData(source: "single" | "clustered", features: Feature[]): void {
    (this.map.getSource(source) as GeoJSONSource).setData({ type: "FeatureCollection", features } as Parameters<GeoJSONSource["setData"]>[0]);
  }

  /** Resolves once the map has drawn what it was last given. */
  idle(): Promise<void> {
    return new Promise((resolve) => {
      if (this.map.loaded() && !this.map.isMoving()) {
        this.map.once("idle", () => resolve());
        this.map.triggerRepaint();
      } else {
        this.map.once("idle", () => resolve());
      }
    });
  }

  private async zoomInto(feature: MapGeoJSONFeature | undefined): Promise<void> {
    if (feature === undefined) return;
    const [lon, lat] = (feature.geometry as { coordinates: number[] }).coordinates;
    if (feature.properties.cell) {
      // A cell: its events come with the area once zoomed in far enough.
      this.map.easeTo({ center: [lon, lat], zoom: Math.max(AREA_ZOOM, this.map.getZoom() + 2) });
      return;
    }
    const source = this.map.getSource("clustered") as GeoJSONSource;
    const zoom = await source.getClusterExpansionZoom(feature.properties.cluster_id as number);
    this.map.easeTo({ center: [lon, lat], zoom });
  }

  /** The popup of the event with `key`, as a click on its point opens it (for the tests). */
  openPopup(key: string): boolean {
    const feature = this.shown.find((f) => f.properties.key === key);
    if (feature === undefined) return false;
    this.showPopup(feature as unknown as MapGeoJSONFeature);
    return true;
  }

  private showPopup(feature: MapGeoJSONFeature | undefined): void {
    if (feature === undefined) return;
    const p = feature.properties;
    const [lon, lat] = (feature.geometry as { coordinates: number[] }).coordinates;
    const body = document.createElement("div");
    const title = document.createElement("strong");
    title.textContent = String(p.title ?? p.place ?? `Event ${p.key}`);
    const line = (text: string, testid: string) => {
      const div = document.createElement("div");
      div.textContent = text;
      div.dataset.testid = testid;
      body.append(div);
      return div;
    };
    const magnitude = p.magnitude === null || p.magnitude === undefined ? "no magnitude" : `M ${p.magnitude}`;
    body.append(title);
    line(
      `${magnitude} · ${p.depth_km} km deep` + (p.tsunami ? " · tsunami flagged" : "") + (p.deleted ? " · deleted by USGS" : ""),
      "popup-details",
    );
    // The time in UTC, then where it happened (the zone's name on hover).
    line(utcTime(p.time as number), "popup-utc");
    const local = localTime(p.time as number, lat, lon);
    if (local !== null) line(local.text, "popup-local").title = local.zone;
    if (typeof p.url === "string" && p.url.startsWith("https://")) {
      const link = document.createElement("a");
      link.href = p.url;
      link.rel = "noopener";
      link.textContent = "USGS event page";
      body.append(link);
    }
    this.closePopup();
    // closeOnClick: a click elsewhere on the map closes it (a click on a point opens that one's).
    const popup = new maplibregl.Popup({ maxWidth: "280px", closeOnClick: true });
    popup.on("close", () => {
      if (this.popup === popup) this.popup = null;
    });
    this.popup = popup.setLngLat([lon, lat]).setDOMContent(body).addTo(this.map);
  }
}
