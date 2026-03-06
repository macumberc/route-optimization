import React, { useEffect, useState } from "react";
import { Select, Table, Spin, Tag } from "antd";
import {
  PieChart, Pie, Cell, BarChart, Bar, XAxis, YAxis,
  CartesianGrid, Tooltip, Legend, ResponsiveContainer,
} from "recharts";
import { fetchVehicles, fetchDepots } from "../api";

const NAVY = "#1B2A4A";
const ORANGE = "#E87722";
const STATUS_COLORS = { active: "#52c41a", maintenance: "#faad14", retired: "#8c8c8c" };
const PIE_COLORS = ["#52c41a", "#faad14", "#8c8c8c", "#1890ff"];

export default function Fleet() {
  const [vehicles, setVehicles] = useState([]);
  const [depots, setDepots] = useState([]);
  const [loading, setLoading] = useState(true);
  const [depotFilter, setDepotFilter] = useState(undefined);
  const [typeFilter, setTypeFilter] = useState(undefined);

  useEffect(() => { fetchDepots().then(setDepots); }, []);

  useEffect(() => {
    setLoading(true);
    fetchVehicles({ depot_id: depotFilter, vehicle_type: typeFilter })
      .then(setVehicles)
      .finally(() => setLoading(false));
  }, [depotFilter, typeFilter]);

  const statusData = React.useMemo(() => {
    const map = {};
    vehicles.forEach((v) => {
      map[v.status] = (map[v.status] || 0) + 1;
    });
    return Object.entries(map).map(([name, value]) => ({ name, value }));
  }, [vehicles]);

  const costByType = React.useMemo(() => {
    const map = {};
    vehicles.forEach((v) => {
      if (!map[v.vehicle_type]) map[v.vehicle_type] = { type: v.vehicle_type, total: 0, count: 0, routes: 0 };
      map[v.vehicle_type].total += v.cost_per_mile_usd || 0;
      map[v.vehicle_type].count += 1;
      map[v.vehicle_type].routes += v.route_count || 0;
    });
    return Object.values(map).map((d) => ({
      type: d.type,
      avg_cost_per_mile: +(d.total / d.count).toFixed(2),
      total_routes: d.routes,
    }));
  }, [vehicles]);

  const vehicleTypes = React.useMemo(
    () => [...new Set(vehicles.map((v) => v.vehicle_type))],
    [vehicles]
  );

  const columns = [
    { title: "Vehicle", dataIndex: "vehicle_id", width: 140, ellipsis: true },
    { title: "Type", dataIndex: "vehicle_type", width: 100 },
    { title: "Depot", dataIndex: "depot_id", width: 110, render: (v) => v?.replace("DEPOT_", "") },
    { title: "Capacity (lbs)", dataIndex: "capacity_weight_lbs", width: 120, sorter: (a, b) => a.capacity_weight_lbs - b.capacity_weight_lbs },
    { title: "Max Stops", dataIndex: "max_stops_per_route", width: 100 },
    { title: "$/mile", dataIndex: "cost_per_mile_usd", width: 90, render: (v) => `$${v?.toFixed(2) ?? "—"}` },
    { title: "Routes", dataIndex: "route_count", width: 80, sorter: (a, b) => a.route_count - b.route_count },
    { title: "Avg Miles", dataIndex: "avg_miles_per_route", width: 100 },
    {
      title: "Status", dataIndex: "status", width: 110,
      render: (v) => <Tag color={STATUS_COLORS[v] || "default"}>{v}</Tag>,
    },
  ];

  return (
    <Spin spinning={loading}>
      <div className="filter-bar">
        <Select
          placeholder="Depot" allowClear style={{ width: 180 }}
          onChange={setDepotFilter}
          options={depots.map((d) => ({ value: d.depot_id, label: d.name }))}
        />
        <Select
          placeholder="Vehicle Type" allowClear style={{ width: 160 }}
          onChange={setTypeFilter}
          options={vehicleTypes.map((t) => ({ value: t, label: t }))}
        />
      </div>

      <div className="charts-grid">
        <div className="chart-card">
          <h3>Vehicle Status Breakdown</h3>
          <ResponsiveContainer width="100%" height={280}>
            <PieChart>
              <Pie
                data={statusData} dataKey="value" nameKey="name"
                cx="50%" cy="50%" outerRadius={100} label={({ name, value }) => `${name}: ${value}`}
              >
                {statusData.map((_, i) => (
                  <Cell key={i} fill={PIE_COLORS[i % PIE_COLORS.length]} />
                ))}
              </Pie>
              <Tooltip />
              <Legend />
            </PieChart>
          </ResponsiveContainer>
        </div>
        <div className="chart-card">
          <h3>Avg Cost per Mile by Vehicle Type</h3>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={costByType}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="type" />
              <YAxis />
              <Tooltip formatter={(v) => `$${v}`} />
              <Bar dataKey="avg_cost_per_mile" fill={ORANGE} radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="charts-grid">
        <div className="chart-card">
          <h3>Routes per Vehicle Type</h3>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={costByType}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="type" />
              <YAxis />
              <Tooltip />
              <Bar dataKey="total_routes" fill={NAVY} radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="chart-card">
        <h3>Vehicle Fleet</h3>
        <Table
          dataSource={vehicles}
          columns={columns}
          rowKey="vehicle_id"
          size="small"
          pagination={{ pageSize: 15, showSizeChanger: true }}
          scroll={{ x: 1000 }}
        />
      </div>
    </Spin>
  );
}
