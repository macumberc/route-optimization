import React from "react";

export default function KPICard({ value, label, suffix = "" }) {
  return (
    <div className="kpi-card">
      <div className="kpi-value">
        {value ?? "—"}
        {suffix && <span style={{ fontSize: 16, marginLeft: 2 }}>{suffix}</span>}
      </div>
      <div className="kpi-label">{label}</div>
    </div>
  );
}
