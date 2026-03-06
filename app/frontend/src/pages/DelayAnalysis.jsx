import React, { useEffect, useState } from "react";
import { Select, DatePicker, Table, Spin } from "antd";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer,
} from "recharts";
import { fetchDelays, fetchRegions } from "../api";

const { RangePicker } = DatePicker;
const NAVY = "#1B2A4A";
const ORANGE = "#E87722";
const REASON_COLORS = ["#E87722", "#1B2A4A", "#52c41a", "#1890ff", "#722ed1", "#eb2f96", "#faad14"];

export default function DelayAnalysis() {
  const [data, setData] = useState({ by_reason: [], by_month: [], by_region: [], worst_routes: [] });
  const [regions, setRegions] = useState([]);
  const [loading, setLoading] = useState(true);
  const [filters, setFilters] = useState({
    start_date: undefined,
    end_date: undefined,
    service_region: undefined,
  });

  useEffect(() => { fetchRegions().then(setRegions); }, []);

  useEffect(() => {
    setLoading(true);
    fetchDelays(filters)
      .then(setData)
      .finally(() => setLoading(false));
  }, [filters]);

  const monthlyStacked = React.useMemo(() => {
    const map = {};
    const allReasons = new Set();
    data.by_month.forEach((r) => {
      const m = r.month?.substring(0, 7) || "Unknown";
      if (!map[m]) map[m] = { month: m };
      map[m][r.label] = r.count;
      allReasons.add(r.label);
    });
    return { chart: Object.values(map).sort((a, b) => a.month.localeCompare(b.month)), reasons: [...allReasons] };
  }, [data.by_month]);

  const regionData = React.useMemo(
    () => data.by_region.map((r) => ({ region: r.label, late_rate: r.count, avg_delay: r.avg_delay })),
    [data.by_region]
  );

  const reasonData = React.useMemo(
    () => data.by_reason.map((r) => ({ reason: r.label, count: r.count, avg_delay: r.avg_delay })),
    [data.by_reason]
  );

  const worstColumns = [
    { title: "Route", dataIndex: "route_id", width: 150, ellipsis: true },
    { title: "Depot", dataIndex: "depot_id", width: 110, render: (v) => v?.replace("DEPOT_", "") },
    { title: "Date", dataIndex: "route_date", width: 110 },
    { title: "Vehicle", dataIndex: "vehicle_id", width: 130, ellipsis: true },
    {
      title: "Total Delay (min)", dataIndex: "total_delay_minutes", width: 140,
      sorter: (a, b) => a.total_delay_minutes - b.total_delay_minutes,
      defaultSortOrder: "descend",
      render: (v) => <span style={{ color: v > 60 ? "#f5222d" : "#faad14", fontWeight: 600 }}>{v}</span>,
    },
    { title: "Delayed Stops", dataIndex: "delayed_stops", width: 120 },
  ];

  return (
    <Spin spinning={loading}>
      <div className="filter-bar">
        <RangePicker
          onChange={(dates) => {
            setFilters((f) => ({
              ...f,
              start_date: dates?.[0]?.format("YYYY-MM-DD") || undefined,
              end_date: dates?.[1]?.format("YYYY-MM-DD") || undefined,
            }));
          }}
        />
        <Select
          placeholder="Region" allowClear style={{ width: 180 }}
          onChange={(v) => setFilters((f) => ({ ...f, service_region: v || undefined }))}
          options={regions.map((r) => ({ value: r.service_region, label: r.service_region }))}
        />
      </div>

      <div className="charts-grid">
        <div className="chart-card">
          <h3>Delay Reasons by Month</h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={monthlyStacked.chart}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="month" />
              <YAxis />
              <Tooltip />
              <Legend />
              {monthlyStacked.reasons.map((reason, i) => (
                <Bar
                  key={reason}
                  dataKey={reason}
                  stackId="delays"
                  fill={REASON_COLORS[i % REASON_COLORS.length]}
                />
              ))}
            </BarChart>
          </ResponsiveContainer>
        </div>
        <div className="chart-card">
          <h3>Late Deliveries by Region</h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={regionData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="region" />
              <YAxis />
              <Tooltip />
              <Bar dataKey="late_rate" name="Delayed Stops" fill={NAVY} radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="charts-grid">
        <div className="chart-card">
          <h3>Delay Reason Distribution</h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={reasonData} layout="vertical">
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis type="number" />
              <YAxis dataKey="reason" type="category" width={120} />
              <Tooltip />
              <Bar dataKey="count" name="Occurrences" fill={ORANGE} radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
        <div className="chart-card">
          <h3>Top 10 Worst Routes by Delay</h3>
          <Table
            dataSource={data.worst_routes}
            columns={worstColumns}
            rowKey="route_id"
            size="small"
            pagination={false}
          />
        </div>
      </div>
    </Spin>
  );
}
