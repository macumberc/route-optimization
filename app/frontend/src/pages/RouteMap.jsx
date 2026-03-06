import React, { useEffect, useState } from "react";
import { Select, DatePicker, Spin } from "antd";
import { MapContainer, TileLayer, CircleMarker, Marker, Popup, useMap } from "react-leaflet";
import L from "leaflet";
import dayjs from "dayjs";
import { fetchMapData, fetchDepots } from "../api";

const DEPOT_ICON = new L.Icon({
  iconUrl: "https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-blue.png",
  shadowUrl: "https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/images/marker-shadow.png",
  iconSize: [25, 41],
  iconAnchor: [12, 41],
  popupAnchor: [1, -34],
  shadowSize: [41, 41],
});

const COLOR_MAP = { green: "#52c41a", gold: "#faad14", red: "#f5222d" };

function FitBounds({ bounds }) {
  const map = useMap();
  useEffect(() => {
    if (bounds?.length) map.fitBounds(bounds, { padding: [40, 40] });
  }, [bounds, map]);
  return null;
}

export default function RouteMap() {
  const [mapData, setMapData] = useState({ depots: [], stops: [] });
  const [depots, setDepots] = useState([]);
  const [loading, setLoading] = useState(true);
  const [filters, setFilters] = useState({ date: undefined, depot_id: undefined });

  useEffect(() => {
    fetchDepots().then(setDepots);
  }, []);

  useEffect(() => {
    setLoading(true);
    fetchMapData(filters)
      .then(setMapData)
      .finally(() => setLoading(false));
  }, [filters]);

  const bounds = React.useMemo(() => {
    const pts = [
      ...mapData.depots.map((d) => [d.lat, d.lon]),
      ...mapData.stops.filter((s) => s.lat && s.lon).map((s) => [s.lat, s.lon]),
    ];
    return pts.length >= 2 ? pts : [[25, -125], [50, -65]];
  }, [mapData]);

  return (
    <Spin spinning={loading}>
      <div className="filter-bar">
        <DatePicker
          placeholder="Date"
          onChange={(d) => setFilters((f) => ({ ...f, date: d?.format("YYYY-MM-DD") || undefined }))}
        />
        <Select
          placeholder="Depot" allowClear style={{ width: 180 }}
          onChange={(v) => setFilters((f) => ({ ...f, depot_id: v || undefined }))}
          options={depots.map((d) => ({ value: d.depot_id, label: d.name }))}
        />
      </div>

      <div className="map-container">
        <MapContainer center={[39.5, -98.35]} zoom={4} scrollWheelZoom>
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />
          <FitBounds bounds={bounds} />

          {mapData.depots.map((d) => (
            <Marker key={d.depot_id} position={[d.lat, d.lon]} icon={DEPOT_ICON}>
              <Popup>
                <strong>{d.name}</strong><br />
                {d.depot_id}<br />
                {mapData.stops.filter((s) => s.depot_id === d.depot_id).length} stops
              </Popup>
            </Marker>
          ))}

          {mapData.stops
            .filter((s) => s.lat && s.lon)
            .map((s, i) => (
              <CircleMarker
                key={`${s.route_id}-${s.stop_sequence}-${i}`}
                center={[s.lat, s.lon]}
                radius={5}
                pathOptions={{
                  fillColor: COLOR_MAP[s.color] || "#1890ff",
                  color: COLOR_MAP[s.color] || "#1890ff",
                  fillOpacity: 0.85,
                  weight: 1,
                }}
              >
                <Popup>
                  <strong>Stop #{s.stop_sequence}</strong><br />
                  Route: {s.route_id}<br />
                  Order: {s.order_id}<br />
                  Delay: {s.delay_minutes} min<br />
                  {s.address}
                </Popup>
              </CircleMarker>
            ))}
        </MapContainer>
      </div>
    </Spin>
  );
}
