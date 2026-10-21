// The map: MapLibre GL on OpenFreeMap tiles. Small result sets show every event as its own
// point; large ones are clustered when zoomed out, except strong events, which are always
// single points drawn on top, so a cluster can never hide them.
import * as maplibregl from "maplibre-gl";
import type { GeoJSONSource, MapGeoJSONFeature, MapLayerMouseEvent } from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import workerUrl from "maplibre-gl/dist/maplibre-gl-worker.mjs?url";
import { MAGNITUDE_STOPS } from "./colors";
import type { Row } from "./db";
import { localTime, utcTime } from "./times";

// MapLibre 6 runs its tile work in a separate worker file, served with the page.
maplibregl.setWorkerUrl(workerUrl);

export const STYLE_URL = "https://tiles.openfreemap.org/styles/positron";

/** Up to this many events, none is clustered. */
export const CLUSTER_ABOVE = 5000;
/** Events at or above this magnitude are never clustered. */
export const STRONG_MAGNITUDE = 6;

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
  private shown: { properties: Record<string, unknown>; geometry: { coordinates: number[] } }[] = [];

  constructor(container: HTMLElement) {
    this.map = new maplibregl.Map({
      container,
      style: STYLE_URL,
      center: [-120, 40],
      zoom: 1.4,
      attributionControl: { compact: true },
      cooperativeGestures: true, // scrolling the page does not zoom the map
    });
    this.map.addControl(new maplibregl.NavigationControl({ showCompass: false }));
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
      clusterProperties: { max_magnitude: ["max", MAGNITUDE] },
    });
    this.map.addSource("single", { type: "geojson", data: empty });
    this.map.addLayer({
      id: "clusters",
      type: "circle",
      source: "clustered",
      filter: ["has", "point_count"],
      paint: {
        "circle-color": color(["get", "max_magnitude"]),
        "circle-opacity": 0.8,
        "circle-radius": ["step", ["get", "point_count"], 12, 100, 18, 1000, 26, 10000, 34],
        "circle-stroke-width": 1,
        "circle-stroke-color": "#ffffff",
      },
    });
    this.map.addLayer({
      id: "cluster-count",
      type: "symbol",
      source: "clustered",
      filter: ["has", "point_count"],
      layout: {
        "text-field": ["get", "point_count_abbreviated"],
        "text-size": 11,
        "text-font": ["Noto Sans Regular"],
      },
      paint: { "text-color": ["case", [">=", ["get", "max_magnitude"], 4.5], "#ffffff", "#1c1917"] },
    });
    this.map.addLayer({
      id: "points",
      type: "circle",
      source: "clustered",
      filter: ["!", ["has", "point_count"]],
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
    const features = points.map((p) => ({
      type: "Feature" as const,
      geometry: { type: "Point" as const, coordinates: [p.longitude as number, p.latitude as number] },
      properties: {
        key: p.key,
        magnitude: p.magnitude,
        depth_km: p.depth_km,
        time: p.event_time_ms,
        place: p.place,
        title: p.title,
        url: p.url,
        deleted: p.review_status === "deleted",
      },
    }));
    this.clustered = features.length > CLUSTER_ABOVE;
    const strong = (f: (typeof features)[number]) => (f.properties.magnitude as number | null) !== null && (f.properties.magnitude as number) >= STRONG_MAGNITUDE;
    const single = this.clustered ? features.filter(strong) : features;
    const rest = this.clustered ? features.filter((f) => !strong(f)) : [];
    (this.map.getSource("single") as GeoJSONSource).setData({ type: "FeatureCollection", features: single });
    (this.map.getSource("clustered") as GeoJSONSource).setData({ type: "FeatureCollection", features: rest });
    this.featureCount = features.length;
    this.singleCount = single.length;
    this.shown = features;
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
    const source = this.map.getSource("clustered") as GeoJSONSource;
    const zoom = await source.getClusterExpansionZoom(feature.properties.cluster_id as number);
    const [lon, lat] = (feature.geometry as { coordinates: number[] }).coordinates;
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
    line(`${magnitude} · ${p.depth_km} km deep` + (p.deleted ? " · deleted by USGS" : ""), "popup-details");
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
    new maplibregl.Popup({ maxWidth: "280px" }).setLngLat([lon, lat]).setDOMContent(body).addTo(this.map);
  }
}
