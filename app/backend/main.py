import os
import time
import logging
from contextlib import contextmanager
from datetime import date, datetime
from functools import wraps
from typing import Any, Optional

from fastapi import FastAPI, Query, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from databricks.sdk import WorkspaceClient
from databricks import sql as databricks_sql

logger = logging.getLogger("route-optimization")

CATALOG = os.environ.get("CATALOG_NAME", "northstar_route_optimization")
SCHEMA = os.environ.get("SCHEMA_NAME", "demo")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "148ccb90800933a1")

w = WorkspaceClient()

app = FastAPI(title="NorthStar Route Optimization API")

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


def _table(name: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{name}`"


def get_connection():
    return databricks_sql.connect(
        server_hostname=w.config.host.replace("https://", ""),
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        credentials_provider=lambda: w.config._header_factory(),
    )


@contextmanager
def get_cursor():
    conn = get_connection()
    try:
        cursor = conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Simple TTL cache for expensive aggregations
# ---------------------------------------------------------------------------
_cache: dict[str, tuple[float, Any]] = {}
CACHE_TTL = 30


def ttl_cache(key_fn):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            key = key_fn(*args, **kwargs)
            now = time.time()
            if key in _cache:
                ts, val = _cache[key]
                if now - ts < CACHE_TTL:
                    return val
            result = fn(*args, **kwargs)
            _cache[key] = (now, result)
            return result
        return wrapper
    return decorator


def _rows_to_dicts(cursor) -> list[dict]:
    cols = [desc[0] for desc in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def _where_clauses(filters: dict[str, Any]) -> tuple[str, str]:
    """Build WHERE fragment from non-None filter values."""
    parts = []
    for col, val in filters.items():
        if val is not None:
            parts.append(f"{col} = '{val}'")
    clause = " AND ".join(parts)
    return f"WHERE {clause}" if clause else "", clause


def _date_filter(start_date: Optional[str], end_date: Optional[str], col: str = "route_date") -> str:
    parts = []
    if start_date:
        parts.append(f"{col} >= '{start_date}'")
    if end_date:
        parts.append(f"{col} <= '{end_date}'")
    return " AND ".join(parts)


# ---------------------------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/kpis")
def get_kpis(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    depot_id: Optional[str] = Query(None),
    client_id: Optional[str] = Query(None),
    service_region: Optional[str] = Query(None),
):
    filters = []
    if start_date:
        filters.append(f"rp.route_date >= '{start_date}'")
    if end_date:
        filters.append(f"rp.route_date <= '{end_date}'")
    if depot_id:
        filters.append(f"rp.depot_id = '{depot_id}'")
    if client_id:
        filters.append(f"rp.client_mix LIKE '%{client_id}%'")
    if service_region:
        filters.append(f"do.service_region = '{service_region}'")

    where = f"WHERE {' AND '.join(filters)}" if filters else ""

    sql = f"""
    WITH route_stop_agg AS (
        SELECT
            rs.route_id,
            COUNT(*) AS total_stops,
            SUM(CASE WHEN rs.delay_minutes = 0 THEN 1 ELSE 0 END) AS on_time_stops,
            SUM(CASE WHEN rs.delay_minutes > 15 THEN 1 ELSE 0 END) AS late_stops
        FROM {_table('route_stops')} rs
        GROUP BY rs.route_id
    ),
    joined AS (
        SELECT
            rp.route_id,
            rp.actual_cost_usd,
            rp.actual_miles,
            rp.planned_stops,
            rp.vehicle_id,
            rsa.total_stops,
            rsa.on_time_stops,
            rsa.late_stops
        FROM {_table('route_plans')} rp
        LEFT JOIN route_stop_agg rsa ON rp.route_id = rsa.route_id
        LEFT JOIN {_table('delivery_orders')} do ON rp.depot_id = do.depot_id
            AND rp.route_date = do.requested_date
        {where}
    )
    SELECT
        ROUND(COALESCE(SUM(on_time_stops) * 100.0 / NULLIF(SUM(total_stops), 0), 0), 1) AS on_time_rate,
        ROUND(COALESCE(AVG(actual_cost_usd), 0), 2) AS avg_cost_per_delivery,
        ROUND(COALESCE(SUM(actual_miles) / NULLIF(SUM(total_stops), 0), 0), 2) AS avg_miles_per_stop,
        ROUND(COALESCE(COUNT(DISTINCT vehicle_id) * 100.0 / NULLIF(
            (SELECT COUNT(*) FROM {_table('vehicles')} WHERE status = 'active'), 0
        ), 0), 1) AS fleet_utilization_pct,
        COALESCE(SUM(total_stops), 0) AS total_deliveries,
        ROUND(COALESCE(SUM(late_stops) * 100.0 / NULLIF(SUM(total_stops), 0), 0), 1) AS late_delivery_rate
    FROM joined
    """
    with get_cursor() as cursor:
        cursor.execute(sql)
        rows = _rows_to_dicts(cursor)
        return rows[0] if rows else {}


@app.get("/api/routes")
def get_routes(
    date: Optional[str] = Query(None),
    depot_id: Optional[str] = Query(None),
    client_id: Optional[str] = Query(None),
):
    filters = []
    if date:
        filters.append(f"route_date = '{date}'")
    if depot_id:
        filters.append(f"depot_id = '{depot_id}'")
    if client_id:
        filters.append(f"client_mix LIKE '%{client_id}%'")
    where = f"WHERE {' AND '.join(filters)}" if filters else ""

    sql = f"""
    SELECT
        route_id, route_date, vehicle_id, driver_id, depot_id,
        client_mix, planned_stops, planned_miles, planned_duration_min,
        planned_cost_usd, actual_miles, actual_duration_min, actual_cost_usd,
        optimization_method
    FROM {_table('route_plans')}
    {where}
    ORDER BY route_date DESC, depot_id
    LIMIT 200
    """
    with get_cursor() as cursor:
        cursor.execute(sql)
        return _rows_to_dicts(cursor)


@app.get("/api/stops/{route_id}")
def get_stops(route_id: str):
    sql = f"""
    SELECT
        route_stop_id, route_id, stop_sequence, order_id,
        planned_arrival, actual_arrival, planned_departure, actual_departure,
        status, delay_minutes, delay_reason, service_time_min
    FROM {_table('route_stops')}
    WHERE route_id = '{route_id}'
    ORDER BY stop_sequence
    """
    with get_cursor() as cursor:
        cursor.execute(sql)
        return _rows_to_dicts(cursor)


@app.get("/api/vehicles")
def get_vehicles(
    depot_id: Optional[str] = Query(None),
    vehicle_type: Optional[str] = Query(None),
):
    filters = []
    if depot_id:
        filters.append(f"v.depot_id = '{depot_id}'")
    if vehicle_type:
        filters.append(f"v.vehicle_type = '{vehicle_type}'")
    where = f"WHERE {' AND '.join(filters)}" if filters else ""

    sql = f"""
    WITH vehicle_routes AS (
        SELECT vehicle_id, COUNT(*) as route_count,
               AVG(actual_miles) as avg_miles, AVG(actual_cost_usd) as avg_cost
        FROM {_table('route_plans')}
        GROUP BY vehicle_id
    )
    SELECT
        v.vehicle_id, v.vehicle_type, v.capacity_weight_lbs,
        v.capacity_volume_cuft, v.max_stops_per_route, v.depot_id,
        v.fuel_type, v.cost_per_mile_usd, v.daily_fixed_cost_usd, v.status,
        COALESCE(vr.route_count, 0) AS route_count,
        ROUND(COALESCE(vr.avg_miles, 0), 1) AS avg_miles_per_route,
        ROUND(COALESCE(vr.avg_cost, 0), 2) AS avg_cost_per_route
    FROM {_table('vehicles')} v
    LEFT JOIN vehicle_routes vr ON v.vehicle_id = vr.vehicle_id
    {where}
    ORDER BY v.depot_id, v.vehicle_id
    """
    with get_cursor() as cursor:
        cursor.execute(sql)
        return _rows_to_dicts(cursor)


@app.get("/api/delays")
def get_delays(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    service_region: Optional[str] = Query(None),
):
    date_filters = []
    if start_date:
        date_filters.append(f"rp.route_date >= '{start_date}'")
    if end_date:
        date_filters.append(f"rp.route_date <= '{end_date}'")
    if service_region:
        date_filters.append(f"do.service_region = '{service_region}'")
    where = f"WHERE rs.delay_minutes > 0 AND {' AND '.join(date_filters)}" if date_filters else "WHERE rs.delay_minutes > 0"

    sql = f"""
    WITH delay_data AS (
        SELECT
            rs.delay_reason,
            do.service_region,
            DATE_TRUNC('MONTH', rp.route_date) AS month,
            rs.delay_minutes,
            rs.route_id
        FROM {_table('route_stops')} rs
        JOIN {_table('route_plans')} rp ON rs.route_id = rp.route_id
        JOIN {_table('delivery_orders')} do ON rs.order_id = do.order_id
        {where}
    )
    SELECT 'by_reason' AS category, delay_reason AS label, NULL AS month,
           COUNT(*) AS count, ROUND(AVG(delay_minutes), 1) AS avg_delay
    FROM delay_data GROUP BY delay_reason

    UNION ALL

    SELECT 'by_month' AS category, delay_reason AS label,
           CAST(month AS STRING) AS month,
           COUNT(*) AS count, ROUND(AVG(delay_minutes), 1) AS avg_delay
    FROM delay_data GROUP BY delay_reason, month

    UNION ALL

    SELECT 'by_region' AS category, service_region AS label, NULL AS month,
           COUNT(*) AS count, ROUND(AVG(delay_minutes), 1) AS avg_delay
    FROM delay_data GROUP BY service_region

    ORDER BY category, month, count DESC
    """
    with get_cursor() as cursor:
        cursor.execute(sql)
        rows = _rows_to_dicts(cursor)

    by_reason = [r for r in rows if r["category"] == "by_reason"]
    by_month = [r for r in rows if r["category"] == "by_month"]
    by_region = [r for r in rows if r["category"] == "by_region"]

    # Top 10 worst routes
    worst_sql = f"""
    SELECT rs.route_id, rp.depot_id, rp.route_date, rp.vehicle_id,
           SUM(rs.delay_minutes) AS total_delay_minutes,
           COUNT(*) AS delayed_stops
    FROM {_table('route_stops')} rs
    JOIN {_table('route_plans')} rp ON rs.route_id = rp.route_id
    WHERE rs.delay_minutes > 0
    GROUP BY rs.route_id, rp.depot_id, rp.route_date, rp.vehicle_id
    ORDER BY total_delay_minutes DESC
    LIMIT 10
    """
    with get_cursor() as cursor:
        cursor.execute(worst_sql)
        worst_routes = _rows_to_dicts(cursor)

    return {
        "by_reason": by_reason,
        "by_month": by_month,
        "by_region": by_region,
        "worst_routes": worst_routes,
    }


DEPOT_COORDS = {
    "DEPOT_ATL": {"lat": 33.749, "lon": -84.388, "name": "Atlanta"},
    "DEPOT_CHI": {"lat": 41.878, "lon": -87.630, "name": "Chicago"},
    "DEPOT_DAL": {"lat": 32.777, "lon": -96.797, "name": "Dallas"},
    "DEPOT_DEN": {"lat": 39.739, "lon": -104.990, "name": "Denver"},
    "DEPOT_LAX": {"lat": 34.052, "lon": -118.244, "name": "Los Angeles"},
    "DEPOT_NYC": {"lat": 40.713, "lon": -74.006, "name": "New York"},
    "DEPOT_PHX": {"lat": 33.449, "lon": -112.074, "name": "Phoenix"},
    "DEPOT_SEA": {"lat": 47.606, "lon": -122.332, "name": "Seattle"},
}


@app.get("/api/map-data")
def get_map_data(
    date: Optional[str] = Query(None),
    depot_id: Optional[str] = Query(None),
):
    filters = []
    if date:
        filters.append(f"rp.route_date = '{date}'")
    if depot_id:
        filters.append(f"rp.depot_id = '{depot_id}'")
    where = f"WHERE {' AND '.join(filters)}" if filters else ""

    sql = f"""
    SELECT
        rp.route_id, rp.depot_id, rp.vehicle_id,
        rs.stop_sequence, rs.order_id, rs.delay_minutes, rs.status AS stop_status,
        do.destination_lat, do.destination_lon, do.destination_address
    FROM {_table('route_plans')} rp
    JOIN {_table('route_stops')} rs ON rp.route_id = rs.route_id
    JOIN {_table('delivery_orders')} do ON rs.order_id = do.order_id
    {where}
    ORDER BY rp.route_id, rs.stop_sequence
    LIMIT 2000
    """
    with get_cursor() as cursor:
        cursor.execute(sql)
        rows = _rows_to_dicts(cursor)

    depots = [
        {"depot_id": k, **v}
        for k, v in DEPOT_COORDS.items()
        if not depot_id or k == depot_id
    ]

    stops = []
    for r in rows:
        delay = r.get("delay_minutes") or 0
        if delay == 0:
            color = "green"
        elif delay <= 15:
            color = "gold"
        else:
            color = "red"
        stops.append({
            "route_id": r["route_id"],
            "depot_id": r["depot_id"],
            "vehicle_id": r["vehicle_id"],
            "stop_sequence": r["stop_sequence"],
            "order_id": r["order_id"],
            "lat": float(r["destination_lat"]) if r["destination_lat"] else None,
            "lon": float(r["destination_lon"]) if r["destination_lon"] else None,
            "address": r["destination_address"],
            "delay_minutes": delay,
            "status": r["stop_status"],
            "color": color,
        })

    return {"depots": depots, "stops": stops}


@app.get("/api/depots")
def get_depots():
    return [{"depot_id": k, **v} for k, v in DEPOT_COORDS.items()]


@app.get("/api/clients")
def get_clients():
    return [{"client_id": f"CLIENT_{c}"} for c in ["A", "B", "C", "D", "E"]]


@app.get("/api/regions")
def get_regions():
    sql = f"SELECT DISTINCT service_region FROM {_table('delivery_orders')} ORDER BY service_region"
    with get_cursor() as cursor:
        cursor.execute(sql)
        return [{"service_region": row[0]} for row in cursor.fetchall()]


# ---------------------------------------------------------------------------
# Static file serving & SPA fallback
# ---------------------------------------------------------------------------
if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static-assets")


@app.get("/{full_path:path}")
def serve_spa(full_path: str):
    file_path = os.path.join(STATIC_DIR, full_path)
    if full_path and os.path.isfile(file_path):
        return FileResponse(file_path)
    index = os.path.join(STATIC_DIR, "index.html")
    if os.path.isfile(index):
        return FileResponse(index)
    return {"detail": "Frontend not built. Run: cd frontend && npm run build"}
