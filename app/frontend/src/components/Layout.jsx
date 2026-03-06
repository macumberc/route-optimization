import React from "react";
import { useNavigate, useLocation } from "react-router-dom";
import { Menu } from "antd";
import {
  DashboardOutlined,
  EnvironmentOutlined,
  CarOutlined,
  WarningOutlined,
} from "@ant-design/icons";

const NAV_ITEMS = [
  { key: "/", icon: <DashboardOutlined />, label: "Dashboard" },
  { key: "/map", icon: <EnvironmentOutlined />, label: "Route Map" },
  { key: "/fleet", icon: <CarOutlined />, label: "Fleet" },
  { key: "/delays", icon: <WarningOutlined />, label: "Delays" },
];

export default function AppLayout({ children }) {
  const navigate = useNavigate();
  const location = useLocation();

  return (
    <>
      <header className="app-header">
        <span className="app-logo">
          <span className="app-logo-accent">NorthStar</span> Logistics
        </span>
        <nav className="app-nav">
          <Menu
            mode="horizontal"
            selectedKeys={[location.pathname]}
            items={NAV_ITEMS}
            onClick={({ key }) => navigate(key)}
          />
        </nav>
      </header>
      <main className="page-content">{children}</main>
    </>
  );
}
