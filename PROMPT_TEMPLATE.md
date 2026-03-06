# Genie Data Room + Databricks App — 3PL Route Optimization Prompt

A **filled-out** prompt template for building a Databricks Genie data room **and** an interactive Databricks App focused on **route optimization for a third-party logistics (3PL) company**. Copy the Phase prompts below into your AI coding assistant (Cursor Agent, Claude Code, etc.) to build the room end-to-end.

---

## Scenario Summary (Pre-filled for 3PL Route Optimization)

| # | Question | Answer |
|---|----------|--------|
| 1 | **Industry/domain** | Third-party logistics (3PL) / Last-mile & regional delivery |
| 2 | **Business problem** | Route optimization to minimize cost per delivery, reduce late deliveries, and maximize vehicle utilization across multi-client fleets |
| 3 | **Persona** | Route planner / logistics operations manager at a 3PL coordinating deliveries for multiple clients |
| 4 | **Fictional company** | NorthStar Logistics |
| 5 | **Tables (2–5)** | 4 tables |
| 6 | **Table names** | `delivery_orders`, `route_plans`, `route_stops`, `vehicles` |
| 7 | **Dimensions** | 8 depots, 5 service regions, 40 vehicles, 6 vehicle types, 5 client programs, 12 months of data |
| 8 | **Time range** | Jan 2025 — Dec 2025 |
| 9 | **Key metrics** | On-Time Delivery Rate, Cost Per Delivery, Miles Per Stop, Vehicle Utilization, Route Efficiency, Late Delivery Rate |
| 10 | **Sample questions** | See list in Phase 1 below |
| 11 | **Target catalog** | User's choice or default to username |
| 12 | **Distributable** | Yes — Git repo + pip install |

---

## How to Use This Prompt

1. **Option A:** Run **Phase 1** first (Google Sheets template), then **Phase 2** (Databricks deploy), then **Phase 3–6** as needed.
2. **Option B:** Paste **Phase 1 + Phase 2** together to plan and deploy in one go; follow up with Phase 3–6 when you want to scale, add the app, and package.
3. All placeholders are already filled for the NorthStar Logistics 3PL route optimization scenario; change company name, table names, or dimensions in the prompts if you want a variant.

---

## The Prompt

Copy the phase(s) below and paste into your AI assistant.

---

### Phase 1 — Plan the Genie Data Room (Google Sheets)

```
Based on the following template for creating a Genie data room:
https://docs.google.com/spreadsheets/d/1w4FIx3IqhJjfsN4-mfVNEJR49_LE30bzY0XdXgh21So/edit?usp=sharing

Create a copy and populate it with this 3PL route optimization scenario:

- Industry: Third-party logistics (3PL) — last-mile and regional delivery operations for multiple clients
- Business problem: Route optimization to minimize cost per delivery, reduce late deliveries, and maximize vehicle utilization across a multi-client fleet
- Persona: Route planner / logistics operations manager at a 3PL coordinating deliveries for multiple clients out of multiple depots
- Company name: NorthStar Logistics
- Tables (4):
  1. delivery_orders — individual delivery requests (order_id, client_id, depot_id, destination_address, destination_lat, destination_lon, requested_date, time_window_start, time_window_end, package_weight_lbs, package_volume_cuft, service_region, priority, status)
  2. route_plans — planned daily routes (route_id, route_date, vehicle_id, driver_id, depot_id, client_mix, planned_stops, planned_miles, planned_duration_min, planned_cost_usd, actual_miles, actual_duration_min, actual_cost_usd, optimization_method)
  3. route_stops — individual stops within a route (route_stop_id, route_id, stop_sequence, order_id, planned_arrival, actual_arrival, planned_departure, actual_departure, status, delay_minutes, delay_reason, service_time_min)
  4. vehicles — fleet inventory (vehicle_id, vehicle_type, capacity_weight_lbs, capacity_volume_cuft, max_stops_per_route, depot_id, fuel_type, cost_per_mile_usd, daily_fixed_cost_usd, status)
- Dimensions: 8 depots, 5 service regions, 40 vehicles, 6 vehicle types (sprinter van, box truck, 26ft truck, cargo van, refrigerated van, flatbed), 5 client programs, 12 months of data
- Time range: Jan 2025 — Dec 2025
- Key metrics Genie should know:
  - On-Time Delivery Rate = (deliveries arriving within the customer time window / total deliveries) × 100; target 95%+
  - Cost Per Delivery = total route cost (fuel + driver + fixed) / number of deliveries on route; lower is better
  - Miles Per Stop = total route miles / number of stops; lower = denser, more efficient routing
  - Vehicle Utilization = (total load weight or volume on route / vehicle capacity) × 100; target 75%+
  - Route Efficiency = (sum of straight-line distances between stops / actual route miles) × 100; closer to 100% = less deadhead
  - Late Delivery Rate = (deliveries arriving after time_window_end / total deliveries) × 100; inverse of on-time
- Sample questions users would ask:
  1. Which routes had the highest cost per delivery last month?
  2. What is our on-time delivery rate by depot and week?
  3. Which service regions have the most late deliveries, and what are the top delay reasons?
  4. What is our average vehicle utilization by vehicle type?
  5. How does miles-per-stop compare across depots?
  6. Which clients have the worst on-time performance this quarter?
  7. What are the busiest depots by stop count, and are they hitting vehicle capacity limits?

Populate every tab in the template: Instructions, Scenario, Dataset 1-4, and Document URLs.
```

### Phase 2 — Deploy to Databricks

```
Deploy the 3PL route optimization datasets and a Genie room to my Databricks workspace.

1. Create a Unity Catalog catalog and schema for the data (e.g., northstar_route_optimization or user default).
2. Generate these four tables with realistic synthetic data using SQL (CTAS statements):
   - delivery_orders: individual delivery requests (order_id, client_id, depot_id, destination_address, destination_lat, destination_lon, requested_date, time_window_start, time_window_end, package_weight_lbs, package_volume_cuft, service_region, priority, status). 5 clients, 8 depots, 5 service regions. Include seasonal peaks (Q4 holiday, back-to-school in Aug) and realistic package weight/volume distributions by client.
   - route_plans: daily route records (route_id, route_date, vehicle_id, driver_id, depot_id, client_mix, planned_stops, planned_miles, planned_duration_min, planned_cost_usd, actual_miles, actual_duration_min, actual_cost_usd, optimization_method). Link to vehicles table. Include planned vs actual variance — actuals should sometimes exceed planned (traffic, weather, access issues). optimization_method in ('greedy_nearest', 'or_tools_cvrp', 'manual_dispatch').
   - route_stops: stop-level detail (route_stop_id, route_id, stop_sequence, order_id, planned_arrival, actual_arrival, planned_departure, actual_departure, status, delay_minutes, delay_reason, service_time_min). Status in ('completed', 'attempted', 'skipped', 'rescheduled'). delay_reason in ('traffic', 'customer_not_available', 'access_issue', 'weather', 'vehicle_breakdown', NULL). Most stops should be on-time; ~8-12% late with realistic delay distributions.
   - vehicles: fleet info (vehicle_id, vehicle_type, capacity_weight_lbs, capacity_volume_cuft, max_stops_per_route, depot_id, fuel_type, cost_per_mile_usd, daily_fixed_cost_usd, status). 40 vehicles across 6 types and 8 depots. fuel_type in ('diesel', 'gasoline', 'electric', 'hybrid'). status in ('active', 'maintenance', 'retired').
   The data should have:
   - Seasonal patterns (Q4 holiday surge, summer lull, back-to-school Aug spike) and day-of-week effects (heavier Mon-Fri, lighter weekends)
   - Proper relationships: same order_id links delivery_orders to route_stops; same vehicle_id links route_plans to vehicles; same depot_id across all tables
   - Realistic distributions: Pareto (80/20) for delivery volume by client and depot, heavier packages for certain clients, urban routes with more stops but fewer miles vs rural routes with fewer stops but more miles
   - Geographic realism: destination_lat/lon clustered around depot locations (e.g., 8 US metro areas), not random
   - At least 12,000+ rows total (e.g., ~6k delivery orders, ~1.5k route plans, ~6k route stops, 40 vehicles)
3. Add column comments to every column describing what it contains and valid values (e.g., optimization_method: 'greedy_nearest' = nearest-neighbor heuristic, 'or_tools_cvrp' = Google OR-Tools CVRP solver, 'manual_dispatch' = dispatcher-assigned).
4. Create a fully-configured Genie space on top of these tables with:
   - General instructions: NorthStar Logistics 3PL route optimization; explain On-Time Delivery Rate, Cost Per Delivery, Miles Per Stop, Vehicle Utilization, Route Efficiency, Late Delivery Rate with formulas
   - 5–7 sample questions on the landing page (cost per delivery by route, on-time rate by depot/week, delay reasons by region, vehicle utilization by type, miles per stop by depot, client performance, capacity analysis)
   - 5 example SQL queries: cost per delivery by depot, on-time rate trend by week, delay reasons breakdown, vehicle utilization by type, planned vs actual miles comparison
   - SQL snippets: filters (e.g., by client_id, depot_id, service_region, date range), expressions for the six metrics, measures that map business terms to SQL
   - 5 benchmark questions with expected SQL answers for validation
   - Column configs with entity matching and format assistance on vehicle_id, depot_id, client_id, service_region, and date columns
```

### Phase 3 — Scale the Data

```
Enhance the 3PL route optimization datasets for larger scale and more realistic patterns.
Increase row counts significantly (e.g., more delivery orders, longer history, more drivers, more depots) while maintaining data quality:
- Seasonal trends (Q4 holiday surge, back-to-school Aug, post-holiday dip in Jan), day-of-week effects (Mon-Fri heavy, Sat light, Sun minimal)
- Proper distributions (Pareto by client/depot), realistic variance in delivery times, route durations, and delays
- Geographic clustering: stops near depot metro areas with realistic lat/lon scatter; urban vs suburban vs rural route profiles
- Coherent relationships: every route_stop links to a valid delivery_order and route_plan; vehicle utilization stays within capacity bounds; planned vs actual variance is realistic (not random noise)
- More delay diversity: add peak-season congestion, weather events (Jan ice storms, Jul heat), and Monday surge effects
Don't just multiply rows; make the data generation logic richer (e.g., more depots, more drivers, multiple optimization methods with different performance profiles).
```

### Phase 4 — Build the Databricks App

```
Build a Databricks App (React frontend + Python FastAPI backend) that provides an interactive route optimization dashboard for NorthStar Logistics. The app reads from the same Unity Catalog tables used by the Genie room (delivery_orders, route_plans, route_stops, vehicles).

The app should have these pages/views:

1. **Route Dashboard (home page)**
   - KPI cards at the top: On-Time Delivery Rate, Avg Cost Per Delivery, Avg Miles Per Stop, Fleet Utilization, total deliveries today
   - Filters: date range picker, depot selector, client selector, service region
   - Bar chart: cost per delivery by depot
   - Line chart: on-time delivery rate trend by week
   - Table: today's active routes with planned vs actual miles, stops completed, current status

2. **Route Map View**
   - Interactive map (use Mapbox GL JS or Leaflet) showing:
     - Depot locations as warehouse icons
     - Planned route paths as polylines color-coded by vehicle type
     - Stop markers color-coded by status (green = on-time, yellow = delayed, red = missed)
   - Click a route to see its stop sequence, timing, and delay details in a side panel
   - Filter by depot, date, client, vehicle type

3. **Fleet & Vehicle View**
   - Vehicle utilization heatmap (vehicles × dates, colored by utilization %)
   - Vehicle status breakdown (active / maintenance / retired)
   - Average cost per mile by vehicle type and fuel type
   - Capacity analysis: which vehicle types are consistently over/under capacity

4. **Delay Analysis View**
   - Delay reasons breakdown (stacked bar by month)
   - Late delivery rate by service region (choropleth or bar chart)
   - Top 10 worst-performing routes with delay details
   - Drill-down: click a delay reason to see affected orders

Technical requirements:
- Backend: FastAPI, connects to the Unity Catalog tables via Databricks SQL connector (databricks-sql-connector)
- Frontend: React with a modern component library (Ant Design or Material UI), Recharts or similar for charts, Mapbox GL JS or React-Leaflet for maps
- Use the Databricks Apps SDK for authentication (no hardcoded credentials)
- App config in app.yaml with a SQL warehouse resource for querying
- Responsive layout, clean typography, NorthStar Logistics branding (dark navy + orange accent)
- Deploy using `databricks apps deploy`
```

### Phase 5 — Package as a Distributable Repo

```
Wrap the 3PL route optimization Genie room, datasets, and Databricks App into a GitHub repo so it can be distributed.
DO NOT do any more work inside the Databricks workspace.

I want to give someone the repo link, have them clone it into a Databricks
Git folder, then run a single notebook that creates:
- The Unity Catalog catalog/schema and all four tables (delivery_orders, route_plans, route_stops, vehicles)
- The Genie room with instructions, sample questions, example SQL, and benchmarks
- Optionally deploys the Databricks App (if app deployment flag is set)

The notebook should:
- Use dbutils widgets for catalog_name, schema_name, warehouse_id, and deploy_app (boolean)
- Default catalog_name to the current user's name
- Skip Genie space creation if no warehouse_id is provided
- Skip app deployment if deploy_app is not set to "true"
- Print clickable links to the Genie space and the app at the end
- Have zero external dependencies beyond what's in a Databricks runtime

The repo structure should include:
- deploy_notebook.py — the main deployment notebook
- app/ — the Databricks App source (React frontend + FastAPI backend)
- app/app.yaml — Databricks App config
- README.md — setup instructions, screenshots, and scenario description
```

### Phase 6 — Package as pip-installable Library (optional)

```
Package the 3PL route optimization Genie room as a pip-installable Python library so someone can do:

  %pip install northstar-route-optimization

  from northstar_route_optimization import deploy
  result = deploy(spark, catalog="my_catalog", warehouse_id="abc123")

Requirements:
- Rename the notebook to avoid conflicts with setup.py (e.g., setup_notebook.py or deploy_notebook.py)
- Create a pyproject.toml with setuptools backend, package name northstar-route-optimization
- Extract all logic into a clean Python module with a single deploy() function that creates the four tables and the Genie space
- The deploy() function should accept spark, catalog, schema, warehouse_id, and deploy_app (bool)
- Handle permission errors gracefully (not all users can CREATE CATALOG)
- Build the wheel and sdist, validate with twine check
- Update README with both pip install and Git folder instructions, and a short description of the 3PL route optimization scenario
```

---

## Tips from Experience

These are lessons learned from building this repo that will save you debugging time:

### Genie API Gotchas
- The `serialized_space` field is required and must be a JSON string (not an object)
- Tables in `data_sources.tables` **must be sorted alphabetically** by their `identifier`
- All IDs in the payload must be **32-character lowercase hex strings** (no letters past 'f')
- Join specs can be tricky — if the API rejects them, remove them and add joins via the UI later
- Use `requests.post` with the notebook's API token rather than the Databricks CLI for the Genie API

### Data Generation
- Use `EXPLODE(SEQUENCE(...))` to generate date ranges
- Use `CROSS JOIN` to create cartesian products of dimensions, then filter with `WHERE RAND() < probability` to get realistic sparsity
- Use `CASE WHEN` on `MONTH()` to inject seasonal patterns (e.g., Q4 holiday surge for 3PL peak season)
- Use `SIN()` with `DAYOFYEAR()` for smooth cyclical patterns in delivery volumes
- Use `HASH(CONCAT(key1, key2)) % 100` for deterministic-looking pseudo-random assignment
- **Route optimization-specific:** Keep delivery_orders, route_plans, route_stops, and vehicles aligned on depot_id/client_id/vehicle_id; generate destination_lat/lon clustered around depot metro areas (not random points); introduce realistic planned-vs-actual variance so route efficiency and on-time metrics are non-trivial
- **Geographic data:** Pick 8 real US metro areas for depot locations (e.g., Atlanta, Chicago, Dallas, Denver, LA, NYC, Phoenix, Seattle); scatter delivery destinations within a 50-mile radius using normal distribution on lat/lon

### Databricks App
- Use the Databricks Apps SDK (`from databricks.sdk import WorkspaceClient`) for auth — never hardcode tokens
- The app.yaml `resources` block should reference a SQL warehouse for backend queries
- For maps, Mapbox GL JS gives the best performance with large route datasets; React-Leaflet is simpler but slower with many polylines
- Batch SQL queries in the backend — don't make one query per KPI card; use CTEs to combine metrics in fewer round trips
- Cache expensive queries (e.g., route history aggregations) with a short TTL (30-60s) to keep the app responsive

### Packaging
- `setup.py` conflicts with Python packaging — rename your notebook to `setup_notebook.py`
- Wrap `CREATE CATALOG` in try/except for permission errors — most shared workspaces restrict this
- The only non-stdlib dependency needed is `requests` (pyspark is already in Databricks runtimes)
- Use `spark.conf.get("spark.databricks.workspaceUrl")` for the API base URL
- Use `dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()` for auth

### Distribution
- Always provide two paths: Git folder (notebook) and pip install (library)
- Default the catalog name to the current user's username for zero-config deploys
- Make the Genie space optional (skip if no warehouse_id) so users can test just the tables first
- Make the app deployment optional — not everyone has Databricks Apps enabled on their workspace
