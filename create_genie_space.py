#!/usr/bin/env python3
"""Create a Genie space for NorthStar Logistics 3PL Route Optimization."""

import json
import hashlib
import requests
import subprocess


def gen_id(name):
    return hashlib.md5(f"northstar_{name}".encode()).hexdigest()


def get_token(profile):
    result = subprocess.run(
        ["databricks", "auth", "token", f"--profile={profile}"],
        capture_output=True, text=True
    )
    return json.loads(result.stdout)["access_token"]


def main():
    profile = "field-eng-east"
    workspace_url = "https://adb-984752964297111.11.azuredatabricks.net"
    warehouse_id = "148ccb90800933a1"
    catalog = "northstar_route_optimization"
    schema = "demo"

    token = get_token(profile)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    do_table = f"{catalog}.{schema}.delivery_orders"
    rp_table = f"{catalog}.{schema}.route_plans"
    rs_table = f"{catalog}.{schema}.route_stops"
    v_table = f"{catalog}.{schema}.vehicles"

    serialized_space = {
        "version": 2,
        "config": {
            "sample_questions": [
                {"id": gen_id("sq1"), "question": ["Which routes had the highest cost per delivery last month?"]},
                {"id": gen_id("sq2"), "question": ["What is our on-time delivery rate by depot and week?"]},
                {"id": gen_id("sq3"), "question": ["Which service regions have the most late deliveries, and what are the top delay reasons?"]},
                {"id": gen_id("sq4"), "question": ["What is our average vehicle utilization by vehicle type?"]},
                {"id": gen_id("sq5"), "question": ["How does miles-per-stop compare across depots?"]},
                {"id": gen_id("sq6"), "question": ["Which clients have the worst on-time performance this quarter?"]},
                {"id": gen_id("sq7"), "question": ["What are the busiest depots by stop count?"]},
            ],
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
        "- manual_dispatch: dispatcher-assigned (most variance)"
    )

    payload = {
        "warehouse_id": warehouse_id,
        "serialized_space": json.dumps(serialized_space),
        "title": "NorthStar Logistics - 3PL Route Optimization",
        "description": (
            "Analyze delivery performance, route efficiency, fleet utilization, and cost optimization "
            "for NorthStar Logistics multi-client 3PL operation across 8 US metro depots."
        ),
    }

    print("Creating Genie space...")
    resp = requests.post(
        f"{workspace_url}/api/2.0/genie/spaces",
        headers=headers,
        json=payload,
    )

    if resp.status_code in (200, 201):
        result = resp.json()
        space_id = result.get("space_id", result.get("id", "unknown"))
        print(f"Genie space created: {space_id}")
        print(f"URL: {workspace_url}/genie/rooms/{space_id}")

        print("\nUpdating with general instructions...")
        update_ss = json.loads(result.get("serialized_space", "{}"))
        if not update_ss:
            update_ss = serialized_space.copy()

        update_ss["instructions"] = {
            "general_instructions": [general_instructions],
            "example_question_sqls": [
                {
                    "id": gen_id("eq1"),
                    "question": "Cost per delivery by depot",
                    "sql": [f"SELECT depot_id, ROUND(AVG(actual_cost_usd / planned_stops), 2) AS avg_cost_per_delivery FROM {rp_table} GROUP BY depot_id ORDER BY avg_cost_per_delivery DESC"],
                },
                {
                    "id": gen_id("eq2"),
                    "question": "On-time rate trend by week",
                    "sql": [f"SELECT DATE_TRUNC('week', planned_arrival) AS week, ROUND(SUM(CASE WHEN delay_minutes = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS on_time_rate FROM {rs_table} WHERE status = 'completed' GROUP BY 1 ORDER BY 1"],
                },
                {
                    "id": gen_id("eq3"),
                    "question": "Delay reasons by region",
                    "sql": [f"SELECT do.service_region, rs.delay_reason, COUNT(*) AS cnt FROM {rs_table} rs JOIN {rp_table} rp ON rs.route_id = rp.route_id JOIN {do_table} do ON rs.order_id = do.order_id WHERE rs.delay_reason IS NOT NULL GROUP BY 1, 2 ORDER BY cnt DESC"],
                },
                {
                    "id": gen_id("eq4"),
                    "question": "Vehicle type utilization",
                    "sql": [f"SELECT v.vehicle_type, COUNT(DISTINCT rp.route_id) AS routes, ROUND(AVG(rp.actual_miles),1) AS avg_miles FROM {v_table} v JOIN {rp_table} rp ON v.vehicle_id = rp.vehicle_id GROUP BY 1"],
                },
                {
                    "id": gen_id("eq5"),
                    "question": "Planned vs actual by optimization method",
                    "sql": [f"SELECT optimization_method, ROUND(AVG(planned_miles),1) AS avg_planned, ROUND(AVG(actual_miles),1) AS avg_actual, ROUND(AVG(actual_cost_usd/planned_stops),2) AS cost_per_stop FROM {rp_table} GROUP BY 1 ORDER BY cost_per_stop"],
                },
            ],
        }

        update_payload = {
            "warehouse_id": warehouse_id,
            "serialized_space": json.dumps(update_ss),
            "title": payload["title"],
            "description": payload["description"],
        }

        resp2 = requests.put(
            f"{workspace_url}/api/2.0/genie/spaces/{space_id}",
            headers=headers,
            json=update_payload,
        )
        if resp2.status_code in (200, 201):
            print("Instructions and example SQLs added successfully!")
        else:
            print(f"Update failed: {resp2.status_code}")
            print(resp2.text[:1000])

        return space_id
    else:
        print(f"Failed: {resp.status_code}")
        print(resp.text[:3000])
        return None


if __name__ == "__main__":
    main()
