const BASE = "/api";

async function get(path, params = {}) {
  const qs = new URLSearchParams();
  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== null && v !== "") qs.append(k, v);
  });
  const url = `${BASE}${path}${qs.toString() ? "?" + qs : ""}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`API error ${res.status}: ${url}`);
  return res.json();
}

export const fetchKpis = (p) => get("/kpis", p);
export const fetchRoutes = (p) => get("/routes", p);
export const fetchStops = (routeId) => get(`/stops/${routeId}`);
export const fetchVehicles = (p) => get("/vehicles", p);
export const fetchDelays = (p) => get("/delays", p);
export const fetchMapData = (p) => get("/map-data", p);
export const fetchDepots = () => get("/depots");
export const fetchClients = () => get("/clients");
export const fetchRegions = () => get("/regions");
