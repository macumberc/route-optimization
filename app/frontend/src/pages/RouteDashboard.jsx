import React, { useEffect, useState } from "react";
import { Select, DatePicker, Table, Spin, Tag } from "antd";
import {
  BarChart, Bar, LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer,
} from "recharts";
import dayjs from "dayjs";
import KPICard from "../components/KPICard";
import { fetchKpis, fetchRoutes, fetchDepots, fetchClients, fetchRegions } from "../api";

const { RangePicker } = DatePicker;
const NAVY = "#1B2A4A";
const ORANGE = "#E87722";

export default function RouteDashboard() {
  const [kpis, setKpis] = useState(null);
  const [routes, setRoutes] = useState([]);
  const [depots, setDepots] = useState([]);
  const [clients, setClients] = useState([]);
  const [regions, setRegions] = useState([]);
  const [loading, setLoading] = useState(true);

  const [filters, setFilters] = useState({
    start_date: undefined,
    end_date: undefined,
    depot_id: undefined,
    client_id: undefined,
    service_region: undefined,
  });

  useEffect(() => {
    Promise.all([fetchDepots(), fetchClients(), fetchRegions()]).then(
      ([d, c, r]) => { setDepots(d); setClients(c); setRegions(r); }
    );
  }, []);

  useEffect(() => {
    setLoading(true);
    Promise.all([fetchKpis(filters), fetchRoutes(filters)])
      .then(([k, r]) => { setKpis(k); setRoutes(r); })
      .finally(() => setLoading(false));
  }, [filters]);

  const updateFilter = (key, val) =>
    setFilters((f) => ({ ...f, [key]: val || undefined }));

  const costByDepot = React.useMemo(() => {
    const map = {};
    routes.forEach((r) => {
      if (!map[r.depot_id]) map[r.depot_id] = { depot: r.depot_id, total: 0, count: 0 };
      map[r.depot_id].total += r.actual_cost_usd || 0;
      map[r.depot_id].count += r.planned_stops || 0;
    });
    return Object.values(map).map((d) => ({
      depot: d.depot.replace("DEPOT_", ""),
      cost_per_delivery: d.count ? +(d.total / d.count).toFixed(2) : 0,
    }));
  }, [routes]);

  const onTimeTrend = React.useMemo(() => {
    const map = {};
    routes.forEach((r) => {
      const week = r.route_date?.substring(0, 10);
      if (!week) return;
      if (!map[week]) map[week] = { week, planned: 0, actual: 0 };
      map[week].planned += r.planned_stops || 0;
      const efficiency = r.actual_miles && r.planned_miles
        ? Math.min(100, (r.planned_miles / r.actual_miles) * 100) : 90;
      map[week].actual += efficiency;
    });
    return Object.values(map)
      .sort((a, b) => a.week.localeCompare(b.week))
      .slice(-12)
      .map((d) => ({ ...d, rate: d.planned ? +(d.actual / Object.keys(map).length).toFixed(1) : 0 }));
  }, [routes]);

  const columns = [
    { title: "Route", dataIndex: "route_id", width: 140, ellipsis: true },
    { title: "Depot", dataIndex: "depot_id", width: 110, render: (v) => v?.replace("DEPOT_", "") },
    { title: "Vehicle", dataIndex: "vehicle_id", width: 120, ellipsis: true },
    { title: "Stops", dataIndex: "planned_stops", width: 70, sorter: (a, b) => a.planned_stops - b.planned_stops },
    {
      title: "Miles (Plan / Actual)", width: 160,
      render: (_, r) => `${r.planned_miles?.toFixed(0) ?? "—"} / ${r.actual_miles?.toFixed(0) ?? "—"}`,
    },
    {
      title: "Method", dataIndex: "optimization_method", width: 140,
      render: (v) => {
        const colors = { or_tools_cvrp: "blue", greedy_nearest: "green", manual_dispatch: "default" };
        return <Tag color={colors[v] || "default"}>{v}</Tag>;
      },
    },
    {
      title: "Cost", dataIndex: "actual_cost_usd", width: 100,
      render: (v) => v != null ? `$${v.toFixed(0)}` : "—",
      sorter: (a, b) => (a.actual_cost_usd || 0) - (b.actual_cost_usd || 0),
    },
  ];

  return (
    <Spin spinning={loading}>
      <div className="kpi-row">
        <KPICard value={kpis?.on_time_rate} label="On-Time Rate" suffix="%" />
        <KPICard value={kpis?.avg_cost_per_delivery != null ? `$${kpis.avg_cost_per_delivery}` : null} label="Cost / Delivery" />
        <KPICard value={kpis?.avg_miles_per_stop} label="Miles / Stop" />
        <KPICard value={kpis?.fleet_utilization_pct} label="Fleet Utilization" suffix="%" />
        <KPICard value={kpis?.total_deliveries?.toLocaleString()} label="Total Deliveries" />
        <KPICard value={kpis?.late_delivery_rate} label="Late Rate" suffix="%" />
      </div>

      <div className="filter-bar">
        <RangePicker
          onChange={(dates) => {
            updateFilter("start_date", dates?.[0]?.format("YYYY-MM-DD"));
            updateFilter("end_date", dates?.[1]?.format("YYYY-MM-DD"));
          }}
        />
        <Select
          placeholder="Depot" allowClear style={{ width: 160 }}
          onChange={(v) => updateFilter("depot_id", v)}
          options={depots.map((d) => ({ value: d.depot_id, label: d.name }))}
        />
        <Select
          placeholder="Client" allowClear style={{ width: 140 }}
          onChange={(v) => updateFilter("client_id", v)}
          options={clients.map((c) => ({ value: c.client_id, label: c.client_id }))}
        />
        <Select
          placeholder="Region" allowClear style={{ width: 160 }}
          onChange={(v) => updateFilter("service_region", v)}
          options={regions.map((r) => ({ value: r.service_region, label: r.service_region }))}
        />
      </div>

      <div className="charts-grid">
        <div className="chart-card">
          <h3>Cost per Delivery by Depot</h3>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={costByDepot}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="depot" />
              <YAxis />
              <Tooltip formatter={(v) => `$${v}`} />
              <Bar dataKey="cost_per_delivery" fill={ORANGE} radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
        <div className="chart-card">
          <h3>Route Efficiency Trend</h3>
          <ResponsiveContainer width="100%" height={280}>
            <LineChart data={onTimeTrend}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="week" />
              <YAxis />
              <Tooltip />
              <Line type="monotone" dataKey="rate" stroke={NAVY} strokeWidth={2} dot={{ r: 3 }} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="chart-card">
        <h3>Active Routes</h3>
        <Table
          dataSource={routes}
          columns={columns}
          rowKey="route_id"
          size="small"
          pagination={{ pageSize: 15, showSizeChanger: true }}
          scroll={{ x: 900 }}
        />
      </div>
    </Spin>
  );
}
