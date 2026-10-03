import { useEffect, useRef } from "react";
import type { Map as MapInstance, GeoJSONSource } from "maplibre-gl";
const vendorPath = "/vendor/maplibre-gl.mjs";
const maplibregl: typeof import("maplibre-gl") = await import(
  /* @vite-ignore */ vendorPath
);
import "maplibre-gl/dist/maplibre-gl.css";
import type { Depot, Position, Route } from "./types";
export function FleetMap({
  positions,
  routes,
  depots,
  selected,
  onSelect,
  onPoint,
}: {
  positions: Position[];
  routes: Route[];
  depots: Depot[];
  selected: string;
  onSelect: (id: string) => void;
  onPoint?: (lat: number, lon: number) => void;
}) {
  const element = useRef<HTMLDivElement>(null),
    map = useRef<MapInstance | null>(null),
    select = useRef(onSelect),
    point = useRef(onPoint);
  select.current = onSelect;
  point.current = onPoint;
  const latest = useRef({ positions, routes, depots, selected });
  latest.current = { positions, routes, depots, selected };
  function update() {
    const m = map.current;
    if (!m?.getSource("fleet")) return;
    const d = latest.current;
    (m.getSource("fleet") as GeoJSONSource).setData({
      type: "FeatureCollection",
      features: d.positions.map((p) => ({
        type: "Feature",
        geometry: { type: "Point", coordinates: [p.lon, p.lat] },
        properties: {
          id: p.vehicle_id,
          speed: p.speed,
          selected: p.vehicle_id === d.selected,
          scenario: p.scenario,
        },
      })),
    });
    (m.getSource("routes") as GeoJSONSource).setData({
      type: "FeatureCollection",
      features: d.routes
        .filter(
          (r) =>
            r.vehicle_id === d.selected ||
            (!d.selected && ["active", "disrupted"].includes(r.status)),
        )
        .map((r) => ({
          type: "Feature",
          geometry: { type: "LineString", coordinates: r.geometry },
          properties: { id: r.id, status: r.status },
        })),
    });
    (m.getSource("depots") as GeoJSONSource).setData({
      type: "FeatureCollection",
      features: d.depots.map((p) => ({
        type: "Feature",
        geometry: { type: "Point", coordinates: [p.lon, p.lat] },
        properties: { name: p.name },
      })),
    });
  }
  useEffect(() => {
    if (!element.current) return;
    const m = new maplibregl.Map({
      container: element.current,
      center: [28.12, -26.04],
      zoom: 9.5,
      style: {
        version: 8,
        sources: {
          osm: {
            type: "raster",
            tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
            tileSize: 256,
            attribution: "© OpenStreetMap contributors",
          },
        },
        layers: [{ id: "osm", type: "raster", source: "osm" }],
      },
    });
    map.current = m;
    m.addControl(new maplibregl.NavigationControl(), "top-right");
    m.on("load", () => {
      for (const id of ["routes", "depots", "fleet"])
        m.addSource(id, {
          type: "geojson",
          data: { type: "FeatureCollection", features: [] },
        });
      m.addLayer({
        id: "route-lines",
        type: "line",
        source: "routes",
        paint: {
          "line-color": [
            "case",
            ["==", ["get", "status"], "disrupted"],
            "#dc2626",
            "#2563eb",
          ],
          "line-width": 3,
          "line-opacity": 0.7,
        },
      });
      m.addLayer({
        id: "depot-dots",
        type: "circle",
        source: "depots",
        paint: {
          "circle-radius": 9,
          "circle-color": "#334155",
          "circle-stroke-width": 2,
          "circle-stroke-color": "white",
        },
      });
      m.addLayer({
        id: "fleet-dots",
        type: "circle",
        source: "fleet",
        paint: {
          "circle-radius": ["case", ["get", "selected"], 10, 6],
          "circle-color": [
            "case",
            ["!=", ["get", "scenario"], ""],
            "#dc2626",
            "#059669",
          ],
          "circle-stroke-width": 2,
          "circle-stroke-color": "white",
        },
      });
      m.on("click", "fleet-dots", (e) => {
        const id = e.features?.[0]?.properties?.id;
        if (id) select.current(id);
      });
      m.on("mouseenter", "fleet-dots", () => {
        m.getCanvas().style.cursor = "pointer";
      });
      m.on("mouseleave", "fleet-dots", () => {
        m.getCanvas().style.cursor = "";
      });
      update();
    });
    m.on("click", (e) => {
      if (
        !m.getLayer("fleet-dots") ||
        !m.queryRenderedFeatures(e.point, { layers: ["fleet-dots"] }).length
      )
        point.current?.(e.lngLat.lat, e.lngLat.lng);
    });
    return () => {
      m.remove();
      map.current = null;
    };
  }, []);
  useEffect(update, [positions, routes, depots, selected]);
  return (
    <div ref={element} className="map" aria-label="Fleet operations map" />
  );
}
