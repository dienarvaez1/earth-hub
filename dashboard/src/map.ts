// The map: MapLibre GL on OpenFreeMap tiles, one GeoJSON source of the filtered events,
// clustered when zoomed out.
import * as maplibregl from "maplibre-gl";
import type { GeoJSONSource, MapGeoJSONFeature, MapLayerMouseEvent } from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import workerUrl from "maplibre-gl/dist/maplibre-gl-worker.mjs?url";
import type { Row } from "./db";

// MapLibre 6 runs its tile work in a separate worker file, served with the page.
maplibregl.setWorkerUrl(workerUrl);

export const STYLE_URL = "https://tiles.openfreemap.org/styles/positron";

// Magnitude colors, light to dark; also used in the legend-less popup text.
const MAGNITUDE_COLOR: maplibregl.ExpressionSpecification = [
  "interpolate",
  ["linear"],
  ["coalesce", ["get", "magnitude"], 0],
  0, "#fed976",
  2.5, "#fd8d3c",
  4.5, "#e31a1c",
  6, "#800026",
];

export class EventMap {
  private readonly map: maplibregl.Map;
  private readonly ready: Promise<void>;
  featureCount = 0;

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
    this.map.addSource("events", {
      type: "geojson",
      data: { type: "FeatureCollection", features: [] },
      cluster: true,
      clusterMaxZoom: 6,
      clusterRadius: 40,
    });
    this.map.addLayer({
      id: "clusters",
      type: "circle",
      source: "events",
      filter: ["has", "point_count"],
      paint: {
        "circle-color": "#fd8d3c",
        "circle-opacity": 0.75,
        "circle-radius": ["step", ["get", "point_count"], 12, 100, 18, 1000, 26, 10000, 34],
        "circle-stroke-width": 1,
        "circle-stroke-color": "#ffffff",
      },
    });
    this.map.addLayer({
      id: "cluster-count",
      type: "symbol",
      source: "events",
      filter: ["has", "point_count"],
      layout: {
        "text-field": ["get", "point_count_abbreviated"],
        "text-size": 11,
        "text-font": ["Noto Sans Regular"],
      },
    });
    this.map.addLayer({
      id: "points",
      type: "circle",
      source: "events",
      filter: ["!", ["has", "point_count"]],
      paint: {
        "circle-color": MAGNITUDE_COLOR,
        "circle-radius": ["interpolate", ["linear"], ["coalesce", ["get", "magnitude"], 0], 0, 3, 7, 14],
        "circle-stroke-width": 0.5,
        "circle-stroke-color": "#44403c",
      },
    });
    this.map.on("click", "clusters", (event: MapLayerMouseEvent) => this.zoomInto(event.features?.[0]));
    this.map.on("click", "points", (event: MapLayerMouseEvent) => this.showPopup(event.features?.[0]));
    for (const layer of ["clusters", "points"]) {
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
    (this.map.getSource("events") as GeoJSONSource).setData({ type: "FeatureCollection", features });
    this.featureCount = features.length;
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
    const source = this.map.getSource("events") as GeoJSONSource;
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
