// The map: MapLibre GL on OpenFreeMap tiles. Small result sets show every event as its own
// point; large ones are clustered when zoomed out, except strong events, which are always
// single points drawn on top, so a cluster can never hide them.
import * as maplibregl from "maplibre-gl";
import type { GeoJSONSource, MapGeoJSONFeature, MapLayerMouseEvent } from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import workerUrl from "maplibre-gl/dist/maplibre-gl-worker.mjs?url";
import type { Row } from "./db";

// MapLibre 6 runs its tile work in a separate worker file, served with the page.
maplibregl.setWorkerUrl(workerUrl);

export const STYLE_URL = "https://tiles.openfreemap.org/styles/positron";

/** Up to this many events, none is clustered. */
export const CLUSTER_ABOVE = 5000;
/** Events at or above this magnitude are never clustered. */
export const STRONG_MAGNITUDE = 6;

// Magnitude colors, light to dark, for points (their magnitude) and clusters (their
// strongest event's).
function color(magnitude: maplibregl.ExpressionSpecification): maplibregl.ExpressionSpecification {
  return ["interpolate", ["linear"], magnitude, 0, "#fed976", 2.5, "#fd8d3c", 4.5, "#e31a1c", 6, "#800026"];
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
        id: p.id,
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

  private showPopup(feature: MapGeoJSONFeature | undefined): void {
    if (feature === undefined) return;
    const p = feature.properties;
    const [lon, lat] = (feature.geometry as { coordinates: number[] }).coordinates;
    const body = document.createElement("div");
    const title = document.createElement("strong");
    title.textContent = String(p.title ?? p.place ?? `Event ${p.id}`);
    const details = document.createElement("div");
    const magnitude = p.magnitude === null || p.magnitude === undefined ? "no magnitude" : `M ${p.magnitude}`;
    details.textContent =
      `${magnitude} · ${p.depth_km} km deep · ${new Date(p.time as number).toISOString().slice(0, 16).replace("T", " ")} UTC` +
      (p.deleted ? " · deleted by USGS" : "");
    body.append(title, details);
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
