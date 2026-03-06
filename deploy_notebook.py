# Databricks notebook source

# MAGIC %md
# MAGIC # NorthStar Logistics — 3PL Route Optimization
# MAGIC ### One-Click Deployment Notebook
# MAGIC This notebook creates the complete demo:
# MAGIC 1. Unity Catalog tables (`delivery_orders`, `route_plans`, `route_stops`, `vehicles`)
# MAGIC 2. AI/BI Genie space with instructions and sample questions
# MAGIC 3. (Optional) Databricks App deployment
# MAGIC
# MAGIC **Usage:** Clone the repo into a Databricks Git folder and run this notebook.
# MAGIC Customize the widgets at the top to control catalog name, schema, and optional features.

# COMMAND ----------

# Widget setup
dbutils.widgets.text("catalog_name", spark.sql("SELECT current_user()").first()[0].split("@")[0].replace(".", "_"), "Catalog Name")
dbutils.widgets.text("schema_name", "demo", "Schema Name")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID (for Genie)")
dbutils.widgets.text("deploy_app", "false", "Deploy App (true/false)")

CATALOG = dbutils.widgets.get("catalog_name")
SCHEMA = dbutils.widgets.get("schema_name")
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")
DEPLOY_APP = dbutils.widgets.get("deploy_app").lower() == "true"

print(f"Catalog: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"Warehouse ID: {WAREHOUSE_ID or '(not set — will skip Genie)'}")
print(f"Deploy App: {DEPLOY_APP}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Catalog & Schema

# COMMAND ----------

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`")
    print(f"✓ Catalog `{CATALOG}` ready")
except Exception as e:
    if "PERMISSION_DENIED" in str(e) or "permission" in str(e).lower():
        print(f"⚠ Cannot create catalog (permissions). Assuming `{CATALOG}` already exists.")
    else:
        raise

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")
print(f"✓ Schema `{CATALOG}`.`{SCHEMA}` ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create `vehicles` table (40 rows)

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS `{CATALOG}`.`{SCHEMA}`.vehicles")

spark.sql(f"""
CREATE TABLE `{CATALOG}`.`{SCHEMA}`.vehicles AS
WITH vehicle_defs AS (
  SELECT EXPLODE(ARRAY(
    NAMED_STRUCT('vtype', 'sprinter_van',     'cap_wt', 3500,  'cap_vol', 350,  'max_stops', 25, 'fuel', 'diesel',   'cpm', 0.42, 'daily_fixed', 125.00),
    NAMED_STRUCT('vtype', 'box_truck',         'cap_wt', 8000,  'cap_vol', 800,  'max_stops', 18, 'fuel', 'diesel',   'cpm', 0.68, 'daily_fixed', 195.00),
    NAMED_STRUCT('vtype', '26ft_truck',        'cap_wt', 12000, 'cap_vol', 1500, 'max_stops', 12, 'fuel', 'diesel',   'cpm', 0.85, 'daily_fixed', 275.00),
    NAMED_STRUCT('vtype', 'cargo_van',         'cap_wt', 2800,  'cap_vol', 280,  'max_stops', 30, 'fuel', 'gasoline', 'cpm', 0.38, 'daily_fixed', 95.00),
    NAMED_STRUCT('vtype', 'refrigerated_van',  'cap_wt', 4000,  'cap_vol', 300,  'max_stops', 20, 'fuel', 'diesel',   'cpm', 0.72, 'daily_fixed', 225.00),
    NAMED_STRUCT('vtype', 'flatbed',           'cap_wt', 15000, 'cap_vol', 2000, 'max_stops', 8,  'fuel', 'diesel',   'cpm', 0.95, 'daily_fixed', 310.00)
  )) AS vdef
),
depots AS (
  SELECT EXPLODE(ARRAY(
    NAMED_STRUCT('depot_id', 'DEPOT_ATL', 'lat', 33.749,  'lon', -84.388),
    NAMED_STRUCT('depot_id', 'DEPOT_CHI', 'lat', 41.878,  'lon', -87.630),
    NAMED_STRUCT('depot_id', 'DEPOT_DAL', 'lat', 32.777,  'lon', -96.797),
    NAMED_STRUCT('depot_id', 'DEPOT_DEN', 'lat', 39.739,  'lon', -104.990),
    NAMED_STRUCT('depot_id', 'DEPOT_LAX', 'lat', 34.052,  'lon', -118.244),
    NAMED_STRUCT('depot_id', 'DEPOT_NYC', 'lat', 40.713,  'lon', -74.006),
    NAMED_STRUCT('depot_id', 'DEPOT_PHX', 'lat', 33.448,  'lon', -112.074),
    NAMED_STRUCT('depot_id', 'DEPOT_SEA', 'lat', 47.606,  'lon', -122.332)
  )) AS depot
),
numbered AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY depot.depot_id, vdef.vtype) AS rn,
    vdef,
    depot
  FROM vehicle_defs
  CROSS JOIN depots
),
filtered AS (
  SELECT * FROM numbered
  WHERE rn <= 40
)
SELECT
  CONCAT('VH-', LPAD(CAST(rn AS STRING), 3, '0')) AS vehicle_id,
  vdef.vtype AS vehicle_type,
  CAST(vdef.cap_wt * (1.0 + (ABS(HASH(CONCAT(CAST(rn AS STRING), 'cap_wt'))) % 20 - 10) / 100.0) AS INT) AS capacity_weight_lbs,
  CAST(vdef.cap_vol * (1.0 + (ABS(HASH(CONCAT(CAST(rn AS STRING), 'cap_vol'))) % 20 - 10) / 100.0) AS INT) AS capacity_volume_cuft,
  vdef.max_stops AS max_stops_per_route,
  depot.depot_id,
  CASE
    WHEN ABS(HASH(CONCAT(CAST(rn AS STRING), 'fuel'))) % 10 < 2 THEN 'electric'
    WHEN ABS(HASH(CONCAT(CAST(rn AS STRING), 'fuel'))) % 10 < 3 THEN 'hybrid'
    ELSE vdef.fuel
  END AS fuel_type,
  ROUND(vdef.cpm * (1.0 + (ABS(HASH(CONCAT(CAST(rn AS STRING), 'cpm'))) % 20 - 10) / 100.0), 2) AS cost_per_mile_usd,
  ROUND(vdef.daily_fixed * (1.0 + (ABS(HASH(CONCAT(CAST(rn AS STRING), 'fixed'))) % 20 - 10) / 100.0), 2) AS daily_fixed_cost_usd,
  CASE
    WHEN ABS(HASH(CONCAT(CAST(rn AS STRING), 'status'))) % 20 = 0 THEN 'maintenance'
    WHEN ABS(HASH(CONCAT(CAST(rn AS STRING), 'status'))) % 40 = 1 THEN 'retired'
    ELSE 'active'
  END AS status
FROM filtered
""")

cnt = spark.sql(f"SELECT COUNT(*) FROM `{CATALOG}`.`{SCHEMA}`.vehicles").first()[0]
print(f"✓ vehicles: {cnt} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create `delivery_orders` table (~25k rows)

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS `{CATALOG}`.`{SCHEMA}`.delivery_orders")

spark.sql(f"""
CREATE TABLE `{CATALOG}`.`{SCHEMA}`.delivery_orders AS
WITH date_range AS (
  SELECT EXPLODE(SEQUENCE(DATE'2025-01-01', DATE'2025-12-31', INTERVAL 1 DAY)) AS order_date
),
clients AS (
  SELECT EXPLODE(ARRAY(
    NAMED_STRUCT('client_id', 'CLIENT_A', 'weight', 35),
    NAMED_STRUCT('client_id', 'CLIENT_B', 'weight', 25),
    NAMED_STRUCT('client_id', 'CLIENT_C', 'weight', 20),
    NAMED_STRUCT('client_id', 'CLIENT_D', 'weight', 12),
    NAMED_STRUCT('client_id', 'CLIENT_E', 'weight', 8)
  )) AS client
),
depots AS (
  SELECT EXPLODE(ARRAY(
    NAMED_STRUCT('depot_id', 'DEPOT_ATL', 'lat', 33.749,  'lon', -84.388,  'region', 'southeast'),
    NAMED_STRUCT('depot_id', 'DEPOT_CHI', 'lat', 41.878,  'lon', -87.630,  'region', 'midwest'),
    NAMED_STRUCT('depot_id', 'DEPOT_DAL', 'lat', 32.777,  'lon', -96.797,  'region', 'south_central'),
    NAMED_STRUCT('depot_id', 'DEPOT_DEN', 'lat', 39.739,  'lon', -104.990, 'region', 'mountain_west'),
    NAMED_STRUCT('depot_id', 'DEPOT_LAX', 'lat', 34.052,  'lon', -118.244, 'region', 'west_coast'),
    NAMED_STRUCT('depot_id', 'DEPOT_NYC', 'lat', 40.713,  'lon', -74.006,  'region', 'northeast'),
    NAMED_STRUCT('depot_id', 'DEPOT_PHX', 'lat', 33.448,  'lon', -112.074, 'region', 'mountain_west'),
    NAMED_STRUCT('depot_id', 'DEPOT_SEA', 'lat', 47.606,  'lon', -122.332, 'region', 'west_coast')
  )) AS depot
),
cross_base AS (
  SELECT
    d.order_date,
    c.client,
    dep.depot,
    ROW_NUMBER() OVER (ORDER BY d.order_date, dep.depot.depot_id, c.client.client_id) AS rn
  FROM date_range d
  CROSS JOIN clients c
  CROSS JOIN depots dep
),
-- Seasonal multiplier: Q4 holiday surge, Aug back-to-school, Jan dip
-- Day-of-week: Mon-Fri heavy, Sat light, Sun minimal
-- Pareto by depot: NYC/LAX/CHI get 1.5x
filtered AS (
  SELECT *,
    CASE
      WHEN MONTH(order_date) IN (11, 12) THEN 1.6
      WHEN MONTH(order_date) = 8 THEN 1.3
      WHEN MONTH(order_date) = 1 THEN 0.7
      WHEN MONTH(order_date) IN (6, 7) THEN 0.85
      ELSE 1.0
    END AS seasonal_mult,
    CASE
      WHEN DAYOFWEEK(order_date) = 1 THEN 0.10
      WHEN DAYOFWEEK(order_date) = 7 THEN 0.35
      WHEN DAYOFWEEK(order_date) = 2 THEN 1.15
      WHEN DAYOFWEEK(order_date) = 6 THEN 0.90
      ELSE 1.0
    END AS dow_mult,
    CASE
      WHEN depot.depot_id IN ('DEPOT_NYC', 'DEPOT_LAX', 'DEPOT_CHI') THEN 1.5
      WHEN depot.depot_id IN ('DEPOT_ATL', 'DEPOT_DAL') THEN 1.1
      ELSE 0.7
    END AS depot_mult
  FROM cross_base
),
accepted AS (
  SELECT *
  FROM filtered
  WHERE (ABS(HASH(CONCAT(CAST(rn AS STRING), 'filter'))) % 1000) / 1000.0
        < (client.weight / 100.0) * seasonal_mult * dow_mult * depot_mult * 9.0 / 40.0
)
SELECT
  CONCAT('ORD-', LPAD(CAST(ROW_NUMBER() OVER (ORDER BY order_date, depot.depot_id, client.client_id, rn) AS STRING), 6, '0')) AS order_id,
  client.client_id,
  depot.depot_id,
  CONCAT(
    CAST(1000 + ABS(HASH(CONCAT(CAST(rn AS STRING), 'addr'))) % 9000 AS STRING),
    CASE ABS(HASH(CONCAT(CAST(rn AS STRING), 'street'))) % 8
      WHEN 0 THEN ' Main St'
      WHEN 1 THEN ' Oak Ave'
      WHEN 2 THEN ' Industrial Blvd'
      WHEN 3 THEN ' Commerce Dr'
      WHEN 4 THEN ' Elm St'
      WHEN 5 THEN ' Park Rd'
      WHEN 6 THEN ' Maple Ln'
      ELSE ' Warehouse Way'
    END
  ) AS destination_address,
  ROUND(depot.lat + (ABS(HASH(CONCAT(CAST(rn AS STRING), 'lat'))) % 800 - 400) / 1000.0, 6) AS destination_lat,
  ROUND(depot.lon + (ABS(HASH(CONCAT(CAST(rn AS STRING), 'lon'))) % 800 - 400) / 1000.0, 6) AS destination_lon,
  order_date AS requested_date,
  CONCAT(
    LPAD(CAST(7 + ABS(HASH(CONCAT(CAST(rn AS STRING), 'tw_start'))) % 6 AS STRING), 2, '0'),
    ':00:00'
  ) AS time_window_start,
  CONCAT(
    LPAD(CAST(13 + ABS(HASH(CONCAT(CAST(rn AS STRING), 'tw_end'))) % 7 AS STRING), 2, '0'),
    ':00:00'
  ) AS time_window_end,
  ROUND(
    CASE
      WHEN client.client_id = 'CLIENT_A' THEN 15 + (ABS(HASH(CONCAT(CAST(rn AS STRING), 'wt'))) % 80)
      WHEN client.client_id = 'CLIENT_B' THEN 5 + (ABS(HASH(CONCAT(CAST(rn AS STRING), 'wt'))) % 30)
      WHEN client.client_id = 'CLIENT_C' THEN 50 + (ABS(HASH(CONCAT(CAST(rn AS STRING), 'wt'))) % 200)
      WHEN client.client_id = 'CLIENT_D' THEN 10 + (ABS(HASH(CONCAT(CAST(rn AS STRING), 'wt'))) % 50)
      ELSE 2 + (ABS(HASH(CONCAT(CAST(rn AS STRING), 'wt'))) % 25)
    END, 1
  ) AS package_weight_lbs,
  ROUND(
    CASE
      WHEN client.client_id = 'CLIENT_A' THEN 2 + (ABS(HASH(CONCAT(CAST(rn AS STRING), 'vol'))) % 15)
      WHEN client.client_id = 'CLIENT_B' THEN 1 + (ABS(HASH(CONCAT(CAST(rn AS STRING), 'vol'))) % 8)
      WHEN client.client_id = 'CLIENT_C' THEN 8 + (ABS(HASH(CONCAT(CAST(rn AS STRING), 'vol'))) % 30)
      WHEN client.client_id = 'CLIENT_D' THEN 1 + (ABS(HASH(CONCAT(CAST(rn AS STRING), 'vol'))) % 12)
      ELSE 0.5 + (ABS(HASH(CONCAT(CAST(rn AS STRING), 'vol'))) % 5)
    END, 1
  ) AS package_volume_cuft,
  depot.region AS service_region,
  CASE ABS(HASH(CONCAT(CAST(rn AS STRING), 'priority'))) % 10
    WHEN 0 THEN 'express'
    WHEN 1 THEN 'express'
    WHEN 2 THEN 'priority'
    WHEN 3 THEN 'priority'
    WHEN 4 THEN 'priority'
    ELSE 'standard'
  END AS priority,
  CASE
    WHEN ABS(HASH(CONCAT(CAST(rn AS STRING), 'status'))) % 100 < 88 THEN 'delivered'
    WHEN ABS(HASH(CONCAT(CAST(rn AS STRING), 'status'))) % 100 < 94 THEN 'in_transit'
    WHEN ABS(HASH(CONCAT(CAST(rn AS STRING), 'status'))) % 100 < 97 THEN 'pending'
    WHEN ABS(HASH(CONCAT(CAST(rn AS STRING), 'status'))) % 100 < 99 THEN 'cancelled'
    ELSE 'returned'
  END AS status
FROM accepted
""")

cnt = spark.sql(f"SELECT COUNT(*) FROM `{CATALOG}`.`{SCHEMA}`.delivery_orders").first()[0]
print(f"✓ delivery_orders: {cnt} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create `route_plans` table (~6k rows)

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS `{CATALOG}`.`{SCHEMA}`.route_plans")

spark.sql(f"""
CREATE TABLE `{CATALOG}`.`{SCHEMA}`.route_plans AS
WITH date_range AS (
  SELECT EXPLODE(SEQUENCE(DATE'2025-01-01', DATE'2025-12-31', INTERVAL 1 DAY)) AS route_date
),
active_vehicles AS (
  SELECT vehicle_id, vehicle_type, depot_id, max_stops_per_route,
         cost_per_mile_usd, daily_fixed_cost_usd
  FROM `{CATALOG}`.`{SCHEMA}`.vehicles
  WHERE status = 'active'
),
cross_base AS (
  SELECT
    d.route_date,
    v.vehicle_id,
    v.vehicle_type,
    v.depot_id,
    v.max_stops_per_route,
    v.cost_per_mile_usd,
    v.daily_fixed_cost_usd,
    ROW_NUMBER() OVER (ORDER BY d.route_date, v.depot_id, v.vehicle_id) AS rn
  FROM date_range d
  CROSS JOIN active_vehicles v
),
filtered AS (
  SELECT *,
    CASE
      WHEN MONTH(route_date) IN (11, 12) THEN 0.85
      WHEN MONTH(route_date) = 8 THEN 0.70
      WHEN MONTH(route_date) = 1 THEN 0.35
      WHEN MONTH(route_date) IN (6, 7) THEN 0.45
      ELSE 0.55
    END AS usage_prob,
    CASE
      WHEN DAYOFWEEK(route_date) = 1 THEN 0.08
      WHEN DAYOFWEEK(route_date) = 7 THEN 0.30
      ELSE 1.0
    END AS dow_mult
  FROM cross_base
),
accepted AS (
  SELECT *
  FROM filtered
  WHERE (ABS(HASH(CONCAT(CAST(rn AS STRING), 'rp_filter'))) % 1000) / 1000.0
        < usage_prob * dow_mult
),
with_stops_miles AS (
  SELECT
    *,
    CASE
      WHEN depot_id IN ('DEPOT_NYC', 'DEPOT_CHI', 'DEPOT_LAX') THEN
        LEAST(max_stops_per_route, 8 + ABS(HASH(CONCAT(CAST(rn AS STRING), 'stops'))) % (max_stops_per_route - 5))
      WHEN depot_id IN ('DEPOT_DEN', 'DEPOT_PHX', 'DEPOT_SEA') THEN
        3 + ABS(HASH(CONCAT(CAST(rn AS STRING), 'stops'))) % GREATEST(1, max_stops_per_route / 2 - 2)
      ELSE
        5 + ABS(HASH(CONCAT(CAST(rn AS STRING), 'stops'))) % GREATEST(1, max_stops_per_route - 7)
    END AS p_stops,
    ROUND(
      CASE
        WHEN depot_id IN ('DEPOT_NYC', 'DEPOT_CHI', 'DEPOT_LAX') THEN
          25 + (ABS(HASH(CONCAT(CAST(rn AS STRING), 'miles'))) % 40)
        WHEN depot_id IN ('DEPOT_DEN', 'DEPOT_PHX') THEN
          60 + (ABS(HASH(CONCAT(CAST(rn AS STRING), 'miles'))) % 90)
        ELSE
          35 + (ABS(HASH(CONCAT(CAST(rn AS STRING), 'miles'))) % 60)
      END, 1
    ) AS p_miles,
    CASE ABS(HASH(CONCAT(CAST(rn AS STRING), 'opt'))) % 10
      WHEN 0 THEN 'manual_dispatch'
      WHEN 1 THEN 'manual_dispatch'
      WHEN 2 THEN 'manual_dispatch'
      WHEN 3 THEN 'or_tools_cvrp'
      WHEN 4 THEN 'or_tools_cvrp'
      WHEN 5 THEN 'or_tools_cvrp'
      WHEN 6 THEN 'or_tools_cvrp'
      ELSE 'greedy_nearest'
    END AS opt_method
  FROM accepted
)
SELECT
  CONCAT('RT-', LPAD(CAST(ROW_NUMBER() OVER (ORDER BY route_date, depot_id, vehicle_id) AS STRING), 5, '0')) AS route_id,
  route_date,
  vehicle_id,
  CONCAT('DRV-', LPAD(CAST(100 + ABS(HASH(CONCAT(vehicle_id, CAST(route_date AS STRING), 'driver'))) % 80 AS STRING), 3, '0')) AS driver_id,
  depot_id,
  CASE ABS(HASH(CONCAT(CAST(rn AS STRING), 'cmix'))) % 5
    WHEN 0 THEN 'CLIENT_A'
    WHEN 1 THEN 'CLIENT_A,CLIENT_B'
    WHEN 2 THEN 'CLIENT_B,CLIENT_C'
    WHEN 3 THEN 'CLIENT_A,CLIENT_C,CLIENT_D'
    ELSE 'CLIENT_A,CLIENT_B,CLIENT_C,CLIENT_D,CLIENT_E'
  END AS client_mix,
  CAST(p_stops AS INT) AS planned_stops,
  p_miles AS planned_miles,
  ROUND(p_stops * (4.5 + (ABS(HASH(CONCAT(CAST(rn AS STRING), 'dur'))) % 30) / 10.0) + p_miles * 1.2, 1) AS planned_duration_min,
  ROUND(daily_fixed_cost_usd + p_miles * cost_per_mile_usd + p_stops * 3.50, 2) AS planned_cost_usd,
  ROUND(p_miles * (
    CASE opt_method
      WHEN 'or_tools_cvrp' THEN 0.95 + (ABS(HASH(CONCAT(CAST(rn AS STRING), 'act_mi'))) % 15) / 100.0
      WHEN 'greedy_nearest' THEN 1.0 + (ABS(HASH(CONCAT(CAST(rn AS STRING), 'act_mi'))) % 20) / 100.0
      ELSE 1.05 + (ABS(HASH(CONCAT(CAST(rn AS STRING), 'act_mi'))) % 30) / 100.0
    END
  ), 1) AS actual_miles,
  ROUND((p_stops * (4.5 + (ABS(HASH(CONCAT(CAST(rn AS STRING), 'dur'))) % 30) / 10.0) + p_miles * 1.2) * (
    CASE opt_method
      WHEN 'or_tools_cvrp' THEN 0.92 + (ABS(HASH(CONCAT(CAST(rn AS STRING), 'act_dur'))) % 20) / 100.0
      WHEN 'greedy_nearest' THEN 1.0 + (ABS(HASH(CONCAT(CAST(rn AS STRING), 'act_dur'))) % 25) / 100.0
      ELSE 1.05 + (ABS(HASH(CONCAT(CAST(rn AS STRING), 'act_dur'))) % 35) / 100.0
    END
  ), 1) AS actual_duration_min,
  ROUND((daily_fixed_cost_usd + p_miles * cost_per_mile_usd + p_stops * 3.50) * (
    CASE opt_method
      WHEN 'or_tools_cvrp' THEN 0.90 + (ABS(HASH(CONCAT(CAST(rn AS STRING), 'act_cost'))) % 18) / 100.0
      WHEN 'greedy_nearest' THEN 0.98 + (ABS(HASH(CONCAT(CAST(rn AS STRING), 'act_cost'))) % 20) / 100.0
      ELSE 1.02 + (ABS(HASH(CONCAT(CAST(rn AS STRING), 'act_cost'))) % 30) / 100.0
    END
  ), 2) AS actual_cost_usd,
  opt_method AS optimization_method
FROM with_stops_miles
""")

cnt = spark.sql(f"SELECT COUNT(*) FROM `{CATALOG}`.`{SCHEMA}`.route_plans").first()[0]
print(f"✓ route_plans: {cnt} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create `route_stops` table

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS `{CATALOG}`.`{SCHEMA}`.route_stops")

spark.sql(f"""
CREATE TABLE `{CATALOG}`.`{SCHEMA}`.route_stops AS
WITH stop_expanded AS (
  SELECT
    rp.route_id,
    rp.route_date,
    rp.depot_id,
    rp.planned_stops,
    rp.planned_duration_min,
    rp.optimization_method,
    EXPLODE(SEQUENCE(1, CAST(rp.planned_stops AS INT))) AS stop_seq
  FROM `{CATALOG}`.`{SCHEMA}`.route_plans rp
),
-- Assign each stop an order from the delivery_orders for that depot/date
orders_numbered AS (
  SELECT
    order_id,
    depot_id,
    requested_date,
    ROW_NUMBER() OVER (PARTITION BY depot_id, requested_date ORDER BY order_id) AS ord_rn
  FROM `{CATALOG}`.`{SCHEMA}`.delivery_orders
),
stops_numbered AS (
  SELECT
    se.*,
    ROW_NUMBER() OVER (PARTITION BY se.depot_id, se.route_date ORDER BY se.route_id, se.stop_seq) AS stop_rn
  FROM stop_expanded se
),
joined AS (
  SELECT
    sn.route_id,
    sn.route_date,
    sn.depot_id,
    sn.stop_seq,
    sn.planned_stops,
    sn.planned_duration_min,
    sn.optimization_method,
    o.order_id,
    sn.stop_rn
  FROM stops_numbered sn
  LEFT JOIN orders_numbered o
    ON sn.depot_id = o.depot_id
    AND sn.route_date = o.requested_date
    AND sn.stop_rn = o.ord_rn
)
SELECT
  CONCAT('RS-', LPAD(CAST(ROW_NUMBER() OVER (ORDER BY route_date, route_id, stop_seq) AS STRING), 7, '0')) AS route_stop_id,
  route_id,
  stop_seq AS stop_sequence,
  order_id,
  CAST(CONCAT(CAST(route_date AS STRING), ' ',
    LPAD(CAST(7 + CAST(stop_seq * (planned_duration_min / GREATEST(planned_stops, 1)) / 60 AS INT) AS STRING), 2, '0'),
    ':',
    LPAD(CAST(ABS(HASH(CONCAT(route_id, CAST(stop_seq AS STRING), 'pmin'))) % 60 AS STRING), 2, '0'),
    ':00'
  ) AS TIMESTAMP) AS planned_arrival,
  CAST(CONCAT(CAST(route_date AS STRING), ' ',
    LPAD(CAST(7 + CAST(stop_seq * (planned_duration_min / GREATEST(planned_stops, 1)) / 60 AS INT)
      + CASE WHEN ABS(HASH(CONCAT(route_id, CAST(stop_seq AS STRING), 'delay_flag'))) % 100 < 12
             THEN CAST((ABS(HASH(CONCAT(route_id, CAST(stop_seq AS STRING), 'delay_amt'))) % 45 + 5) / 60 AS INT)
             ELSE 0 END
      AS STRING), 2, '0'),
    ':',
    LPAD(CAST((ABS(HASH(CONCAT(route_id, CAST(stop_seq AS STRING), 'pmin'))) % 60
      + CASE WHEN ABS(HASH(CONCAT(route_id, CAST(stop_seq AS STRING), 'delay_flag'))) % 100 < 12
             THEN ABS(HASH(CONCAT(route_id, CAST(stop_seq AS STRING), 'delay_amt'))) % 45 + 5
             ELSE ABS(HASH(CONCAT(route_id, CAST(stop_seq AS STRING), 'arr_var'))) % 5 END
    ) % 60 AS STRING), 2, '0'),
    ':00'
  ) AS TIMESTAMP) AS actual_arrival,
  CAST(CONCAT(CAST(route_date AS STRING), ' ',
    LPAD(CAST(7 + CAST((stop_seq * (planned_duration_min / GREATEST(planned_stops, 1)) + 5) / 60 AS INT) AS STRING), 2, '0'),
    ':',
    LPAD(CAST(CAST((ABS(HASH(CONCAT(route_id, CAST(stop_seq AS STRING), 'pmin'))) % 60 + 5) AS INT) % 60 AS STRING), 2, '0'),
    ':00'
  ) AS TIMESTAMP) AS planned_departure,
  CAST(CONCAT(CAST(route_date AS STRING), ' ',
    LPAD(CAST(7 + CAST((stop_seq * (planned_duration_min / GREATEST(planned_stops, 1)) + 8) / 60 AS INT) AS STRING), 2, '0'),
    ':',
    LPAD(CAST(CAST((ABS(HASH(CONCAT(route_id, CAST(stop_seq AS STRING), 'pmin'))) % 60 + 8
      + CASE WHEN ABS(HASH(CONCAT(route_id, CAST(stop_seq AS STRING), 'delay_flag'))) % 100 < 12
             THEN ABS(HASH(CONCAT(route_id, CAST(stop_seq AS STRING), 'delay_amt'))) % 45 + 5
             ELSE 0 END
    ) AS INT) % 60 AS STRING), 2, '0'),
    ':00'
  ) AS TIMESTAMP) AS actual_departure,
  CASE
    WHEN ABS(HASH(CONCAT(route_id, CAST(stop_seq AS STRING), 'st'))) % 100 < 90 THEN 'completed'
    WHEN ABS(HASH(CONCAT(route_id, CAST(stop_seq AS STRING), 'st'))) % 100 < 95 THEN 'attempted'
    WHEN ABS(HASH(CONCAT(route_id, CAST(stop_seq AS STRING), 'st'))) % 100 < 98 THEN 'rescheduled'
    ELSE 'skipped'
  END AS status,
  CASE
    WHEN ABS(HASH(CONCAT(route_id, CAST(stop_seq AS STRING), 'delay_flag'))) % 100 < 12
    THEN ABS(HASH(CONCAT(route_id, CAST(stop_seq AS STRING), 'delay_amt'))) % 45 + 5
    ELSE 0
  END AS delay_minutes,
  CASE
    WHEN ABS(HASH(CONCAT(route_id, CAST(stop_seq AS STRING), 'delay_flag'))) % 100 >= 12 THEN NULL
    WHEN ABS(HASH(CONCAT(route_id, CAST(stop_seq AS STRING), 'reason'))) % 5 = 0 THEN 'traffic'
    WHEN ABS(HASH(CONCAT(route_id, CAST(stop_seq AS STRING), 'reason'))) % 5 = 1 THEN 'customer_not_available'
    WHEN ABS(HASH(CONCAT(route_id, CAST(stop_seq AS STRING), 'reason'))) % 5 = 2 THEN 'access_issue'
    WHEN ABS(HASH(CONCAT(route_id, CAST(stop_seq AS STRING), 'reason'))) % 5 = 3 THEN 'weather'
    ELSE 'vehicle_breakdown'
  END AS delay_reason,
  3 + ABS(HASH(CONCAT(route_id, CAST(stop_seq AS STRING), 'svc'))) % 12 AS service_time_min
FROM joined
""")

cnt = spark.sql(f"SELECT COUNT(*) FROM `{CATALOG}`.`{SCHEMA}`.route_stops").first()[0]
print(f"✓ route_stops: {cnt} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Add Column Comments

# COMMAND ----------

column_comments = {
    "vehicles": {
        "vehicle_id": "Unique vehicle identifier (e.g. VH-001)",
        "vehicle_type": "Vehicle class: sprinter_van, box_truck, 26ft_truck, cargo_van, refrigerated_van, flatbed",
        "capacity_weight_lbs": "Maximum payload weight in pounds",
        "capacity_volume_cuft": "Maximum cargo volume in cubic feet",
        "max_stops_per_route": "Maximum number of delivery stops per route for this vehicle",
        "depot_id": "Home depot identifier (e.g. DEPOT_NYC). Links to route_plans.depot_id and delivery_orders.depot_id",
        "fuel_type": "Fuel/power source: diesel, gasoline, electric, hybrid",
        "cost_per_mile_usd": "Variable operating cost per mile in USD",
        "daily_fixed_cost_usd": "Fixed daily cost including lease, insurance, and depreciation in USD",
        "status": "Current vehicle status: active, maintenance, retired",
    },
    "delivery_orders": {
        "order_id": "Unique delivery order identifier (e.g. ORD-000001). Links to route_stops.order_id",
        "client_id": "Client program identifier: CLIENT_A (35%), CLIENT_B (25%), CLIENT_C (20%), CLIENT_D (12%), CLIENT_E (8%)",
        "depot_id": "Origin depot for this delivery (e.g. DEPOT_NYC). Links to vehicles.depot_id and route_plans.depot_id",
        "destination_address": "Street address for the delivery destination",
        "destination_lat": "Latitude of delivery destination, clustered around depot metro area",
        "destination_lon": "Longitude of delivery destination, clustered around depot metro area",
        "requested_date": "Date the delivery was requested for (2025-01-01 to 2025-12-31)",
        "time_window_start": "Earliest acceptable delivery time (HH:MM:SS)",
        "time_window_end": "Latest acceptable delivery time (HH:MM:SS)",
        "package_weight_lbs": "Package weight in pounds, varies by client type",
        "package_volume_cuft": "Package volume in cubic feet, varies by client type",
        "service_region": "Geographic service region: northeast, southeast, midwest, south_central, mountain_west, west_coast",
        "priority": "Delivery priority level: standard, priority, express",
        "status": "Order status: delivered, in_transit, pending, cancelled, returned",
    },
    "route_plans": {
        "route_id": "Unique route identifier (e.g. RT-00001). Links to route_stops.route_id",
        "route_date": "Date the route was executed (2025-01-01 to 2025-12-31)",
        "vehicle_id": "Vehicle assigned to this route. Links to vehicles.vehicle_id",
        "driver_id": "Driver identifier assigned to this route",
        "depot_id": "Origin depot for this route (e.g. DEPOT_NYC)",
        "client_mix": "Comma-separated list of clients served on this route",
        "planned_stops": "Number of planned delivery stops on the route",
        "planned_miles": "Planned total route distance in miles",
        "planned_duration_min": "Planned total route duration in minutes",
        "planned_cost_usd": "Planned total route cost (fixed + variable) in USD",
        "actual_miles": "Actual route distance driven in miles",
        "actual_duration_min": "Actual route duration in minutes",
        "actual_cost_usd": "Actual total route cost in USD",
        "optimization_method": "Route optimization algorithm: greedy_nearest (nearest-neighbor heuristic), or_tools_cvrp (Google OR-Tools CVRP solver), manual_dispatch (dispatcher-assigned)",
    },
    "route_stops": {
        "route_stop_id": "Unique stop identifier (e.g. RS-0000001)",
        "route_id": "Parent route identifier. Links to route_plans.route_id",
        "stop_sequence": "Order of this stop within the route (1-based)",
        "order_id": "Delivery order served at this stop. Links to delivery_orders.order_id. NULL if unmatched",
        "planned_arrival": "Planned arrival timestamp at this stop",
        "actual_arrival": "Actual arrival timestamp at this stop",
        "planned_departure": "Planned departure timestamp from this stop",
        "actual_departure": "Actual departure timestamp from this stop",
        "status": "Stop outcome: completed, attempted, rescheduled, skipped",
        "delay_minutes": "Minutes of delay at this stop (0 = on-time). ~12% of stops have delay > 0",
        "delay_reason": "Cause of delay: traffic, customer_not_available, access_issue, weather, vehicle_breakdown. NULL if on-time",
        "service_time_min": "Time spent servicing the delivery at this stop in minutes",
    },
}

for table_name, columns in column_comments.items():
    fqn = f"`{CATALOG}`.`{SCHEMA}`.`{table_name}`"
    for col_name, comment in columns.items():
        safe_comment = comment.replace("'", "\\'")
        spark.sql(f"ALTER TABLE {fqn} ALTER COLUMN `{col_name}` COMMENT '{safe_comment}'")
    print(f"✓ Column comments added to {table_name} ({len(columns)} columns)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verify Data

# COMMAND ----------

print("=" * 60)
print("DATA VERIFICATION")
print("=" * 60)

for tbl in ["vehicles", "delivery_orders", "route_plans", "route_stops"]:
    fqn = f"`{CATALOG}`.`{SCHEMA}`.`{tbl}`"
    cnt = spark.sql(f"SELECT COUNT(*) FROM {fqn}").first()[0]
    print(f"  {tbl:20s}: {cnt:>8,} rows")

print()

stats = spark.sql(f"""
SELECT
  (SELECT COUNT(DISTINCT depot_id) FROM `{CATALOG}`.`{SCHEMA}`.delivery_orders) AS depots,
  (SELECT COUNT(DISTINCT client_id) FROM `{CATALOG}`.`{SCHEMA}`.delivery_orders) AS clients,
  (SELECT COUNT(DISTINCT service_region) FROM `{CATALOG}`.`{SCHEMA}`.delivery_orders) AS regions,
  (SELECT COUNT(DISTINCT vehicle_type) FROM `{CATALOG}`.`{SCHEMA}`.vehicles) AS vehicle_types,
  (SELECT ROUND(SUM(CASE WHEN delay_minutes > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1)
   FROM `{CATALOG}`.`{SCHEMA}`.route_stops) AS late_pct,
  (SELECT COUNT(DISTINCT optimization_method) FROM `{CATALOG}`.`{SCHEMA}`.route_plans) AS opt_methods
""").first()

print(f"  Depots:               {stats.depots}")
print(f"  Clients:              {stats.clients}")
print(f"  Service regions:      {stats.regions}")
print(f"  Vehicle types:        {stats.vehicle_types}")
print(f"  Late delivery rate:   {stats.late_pct}%")
print(f"  Optimization methods: {stats.opt_methods}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Create Genie Space (conditional)

# COMMAND ----------

import json
import hashlib
import requests

genie_space_id = None

if not WAREHOUSE_ID:
    print("⏭ Skipping Genie space creation (no warehouse_id provided)")
else:
    def _gen_id(name):
        return hashlib.md5(f"northstar_{name}".encode()).hexdigest()

    workspace_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    do_table = f"{CATALOG}.{SCHEMA}.delivery_orders"
    rp_table = f"{CATALOG}.{SCHEMA}.route_plans"
    rs_table = f"{CATALOG}.{SCHEMA}.route_stops"
    v_table  = f"{CATALOG}.{SCHEMA}.vehicles"

    serialized_space = {
        "version": 2,
        "config": {
            "sample_questions": sorted([
                {"id": _gen_id("sq1"), "question": ["Which routes had the highest cost per delivery last month?"]},
                {"id": _gen_id("sq2"), "question": ["What is our on-time delivery rate by depot and week?"]},
                {"id": _gen_id("sq3"), "question": ["Which service regions have the most late deliveries, and what are the top delay reasons?"]},
                {"id": _gen_id("sq4"), "question": ["What is our average vehicle utilization by vehicle type?"]},
                {"id": _gen_id("sq5"), "question": ["How does miles-per-stop compare across depots?"]},
                {"id": _gen_id("sq6"), "question": ["Which clients have the worst on-time performance this quarter?"]},
                {"id": _gen_id("sq7"), "question": ["What are the busiest depots by stop count?"]},
            ], key=lambda x: x["id"]),
        },
        "data_sources": {
            "tables": sorted([
                {"identifier": do_table},
                {"identifier": rp_table},
                {"identifier": rs_table},
                {"identifier": v_table},
            ], key=lambda x: x["identifier"]),
        },
    }

    payload = {
        "warehouse_id": WAREHOUSE_ID,
        "serialized_space": json.dumps(serialized_space),
        "title": "NorthStar Logistics - 3PL Route Optimization",
        "description": (
            "Analyze delivery performance, route efficiency, fleet utilization, "
            "and cost optimization for NorthStar Logistics multi-client 3PL "
            "operation across 8 US metro depots."
        ),
    }

    print("Creating Genie space...")
    resp = requests.post(f"{workspace_url}/api/2.0/genie/spaces", headers=headers, json=payload)

    if resp.status_code in (200, 201):
        result = resp.json()
        genie_space_id = result.get("space_id", result.get("id"))
        print(f"✓ Genie space created: {genie_space_id}")

        general_instructions = (
            "You are a data analyst for NorthStar Logistics, a third-party logistics (3PL) company "
            "operating last-mile and regional delivery services for multiple clients across 8 US metro "
            "depots (Atlanta, Chicago, Dallas, Denver, Los Angeles, New York, Phoenix, Seattle).\n\n"
            "You help route planners and logistics operations managers analyze delivery performance, "
            "route efficiency, fleet utilization, and cost optimization.\n\n"
            "Key Metrics:\n"
            "- On-Time Delivery Rate = (stops with delay_minutes = 0 / total completed stops) x 100. Target: 95%+\n"
            "- Cost Per Delivery = actual_cost_usd / planned_stops from route_plans. Lower is better.\n"
            "- Miles Per Stop = actual_miles / planned_stops from route_plans. Lower = denser routing.\n"
            "- Vehicle Utilization = routes per vehicle over time.\n"
            "- Route Efficiency = planned_miles / actual_miles. Closer to 1.0 = less waste.\n"
            "- Late Delivery Rate = (stops with delay_minutes > 0 / total) x 100.\n\n"
            "Table relationships:\n"
            "- delivery_orders.order_id = route_stops.order_id\n"
            "- route_plans.route_id = route_stops.route_id\n"
            "- route_plans.vehicle_id = vehicles.vehicle_id\n"
            "- route_plans.depot_id matches delivery_orders.depot_id and vehicles.depot_id\n\n"
            "Optimization methods (route_plans.optimization_method):\n"
            "- greedy_nearest: nearest-neighbor heuristic\n"
            "- or_tools_cvrp: Google OR-Tools CVRP solver (best efficiency)\n"
            "- manual_dispatch: dispatcher-assigned (most variance)\n\n"
            "Data characteristics:\n"
            "- Seasonal patterns: Q4 holiday surge (Nov/Dec), back-to-school (Aug), Jan dip, summer lull (Jun/Jul)\n"
            "- Day-of-week effects: Mon-Fri heavy, Sat light, Sun minimal\n"
            "- Pareto distribution: NYC, LAX, CHI depots handle ~50% of volume\n"
            "- Clients: CLIENT_A (35%), CLIENT_B (25%), CLIENT_C (20%), CLIENT_D (12%), CLIENT_E (8%)"
        )

        update_ss = json.loads(result.get("serialized_space", "{}"))
        if not update_ss:
            update_ss = serialized_space.copy()

        update_ss["instructions"] = {
            "text_instructions": [
                {
                    "id": _gen_id("instr1"),
                    "content": [general_instructions],
                }
            ],
            "example_question_sqls": sorted([
                {
                    "id": _gen_id("eq1"),
                    "question": "Cost per delivery by depot",
                    "sql": [f"SELECT depot_id, ROUND(AVG(actual_cost_usd / planned_stops), 2) AS avg_cost_per_delivery FROM {rp_table} GROUP BY depot_id ORDER BY avg_cost_per_delivery DESC"],
                },
                {
                    "id": _gen_id("eq2"),
                    "question": "On-time rate trend by week",
                    "sql": [f"SELECT DATE_TRUNC('week', planned_arrival) AS week, ROUND(SUM(CASE WHEN delay_minutes = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS on_time_rate FROM {rs_table} WHERE status = 'completed' GROUP BY 1 ORDER BY 1"],
                },
                {
                    "id": _gen_id("eq3"),
                    "question": "Delay reasons by region",
                    "sql": [f"SELECT do.service_region, rs.delay_reason, COUNT(*) AS cnt FROM {rs_table} rs JOIN {rp_table} rp ON rs.route_id = rp.route_id JOIN {do_table} do ON rs.order_id = do.order_id WHERE rs.delay_reason IS NOT NULL GROUP BY 1, 2 ORDER BY cnt DESC"],
                },
                {
                    "id": _gen_id("eq4"),
                    "question": "Vehicle type utilization",
                    "sql": [f"SELECT v.vehicle_type, COUNT(DISTINCT rp.route_id) AS routes, ROUND(AVG(rp.actual_miles),1) AS avg_miles FROM {v_table} v JOIN {rp_table} rp ON v.vehicle_id = rp.vehicle_id GROUP BY 1"],
                },
                {
                    "id": _gen_id("eq5"),
                    "question": "Planned vs actual by optimization method",
                    "sql": [f"SELECT optimization_method, ROUND(AVG(planned_miles),1) AS avg_planned, ROUND(AVG(actual_miles),1) AS avg_actual, ROUND(AVG(actual_cost_usd/planned_stops),2) AS cost_per_stop FROM {rp_table} GROUP BY 1 ORDER BY cost_per_stop"],
                },
            ], key=lambda x: x["id"]),
        }

        update_payload = {
            "warehouse_id": WAREHOUSE_ID,
            "serialized_space": json.dumps(update_ss),
            "title": payload["title"],
            "description": payload["description"],
        }

        resp2 = requests.patch(
            f"{workspace_url}/api/2.0/genie/spaces/{genie_space_id}",
            headers=headers,
            json=update_payload,
        )
        if resp2.status_code in (200, 201):
            print("✓ Instructions and example SQLs added")
        else:
            print(f"⚠ Instruction update returned {resp2.status_code}: {resp2.text[:500]}")
    else:
        print(f"✗ Genie space creation failed: {resp.status_code}")
        print(resp.text[:1000])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Deploy App (conditional)

# COMMAND ----------

if not DEPLOY_APP:
    print("⏭ Skipping app deployment (deploy_app != true)")
else:
    import subprocess, os

    repo_root = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
    app_dir = os.path.join("/Workspace", repo_root.lstrip("/"), "app")

    print(f"Deploying app from: {app_dir}")
    print("Note: Ensure 'app/app.yaml' exists with the correct warehouse resource configuration.")

    try:
        workspace_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
        token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

        deploy_headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        app_payload = {
            "name": "northstar-route-optimization",
            "description": "NorthStar Logistics 3PL Route Optimization Dashboard",
        }

        resp = requests.post(f"{workspace_url}/api/2.0/apps", headers=deploy_headers, json=app_payload)
        if resp.status_code in (200, 201):
            app_info = resp.json()
            print(f"✓ App created: {app_info.get('name', 'northstar-route-optimization')}")
            print(f"  URL: {app_info.get('url', 'check workspace')}")
        else:
            print(f"⚠ App creation returned {resp.status_code}: {resp.text[:500]}")
            print("  You can deploy manually with: databricks apps deploy northstar-route-optimization --source-code-path app/")

    except Exception as e:
        print(f"⚠ App deployment failed: {e}")
        print("  You can deploy manually with: databricks apps deploy northstar-route-optimization --source-code-path app/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alternative: pip install
# MAGIC You can also install this as a package and use it programmatically:
# MAGIC ```python
# MAGIC %pip install git+https://github.com/macumberc/route-optimization.git
# MAGIC from northstar_route_optimization import deploy, cleanup
# MAGIC 
# MAGIC # Deploy
# MAGIC result = deploy(spark, dbutils, warehouse_id="your_warehouse_id")
# MAGIC 
# MAGIC # Cleanup (when done)
# MAGIC cleanup(spark, dbutils, catalog=result["catalog"], genie_space_id=result.get("genie_space_id"))
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Summary & Links

# COMMAND ----------

workspace_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"

print("=" * 60)
print("  DEPLOYMENT COMPLETE")
print("=" * 60)
print()
print("  Tables:")
uc_url = f"{workspace_url}/explore/data/{CATALOG}/{SCHEMA}"
print(f"    Unity Catalog: {uc_url}")
for tbl in ["delivery_orders", "route_plans", "route_stops", "vehicles"]:
    print(f"    • {CATALOG}.{SCHEMA}.{tbl}")
print()

if genie_space_id:
    genie_url = f"{workspace_url}/genie/rooms/{genie_space_id}"
    print(f"  Genie Space: {genie_url}")
else:
    print("  Genie Space: (skipped — no warehouse_id)")
print()

if DEPLOY_APP:
    print(f"  App: {workspace_url}/apps/northstar-route-optimization")
else:
    print("  App: (skipped — deploy_app != true)")

print()
print("=" * 60)

# Link the Genie URL as clickable HTML in Databricks notebooks
if genie_space_id:
    displayHTML(f'<a href="{genie_url}" target="_blank">Open Genie Space →</a>')
displayHTML(f'<a href="{uc_url}" target="_blank">Browse Tables in Unity Catalog →</a>')
