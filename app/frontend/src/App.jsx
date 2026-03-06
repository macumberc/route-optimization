import React from "react";
import { Routes, Route } from "react-router-dom";
import AppLayout from "./components/Layout";
import RouteDashboard from "./pages/RouteDashboard";
import RouteMap from "./pages/RouteMap";
import Fleet from "./pages/Fleet";
import DelayAnalysis from "./pages/DelayAnalysis";

export default function App() {
  return (
    <AppLayout>
      <Routes>
        <Route path="/" element={<RouteDashboard />} />
        <Route path="/map" element={<RouteMap />} />
        <Route path="/fleet" element={<Fleet />} />
        <Route path="/delays" element={<DelayAnalysis />} />
      </Routes>
    </AppLayout>
  );
}
