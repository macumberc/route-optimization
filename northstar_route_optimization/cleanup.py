"""Remove all NorthStar Route Optimization demo assets from Databricks."""

import requests


def cleanup(spark, dbutils=None, catalog=None, schema="demo", genie_space_id=None, app_name=None):
    """
    Remove all NorthStar Route Optimization demo assets.

    Args:
        spark: SparkSession
        dbutils: DBUtils (optional, needed for Genie/App cleanup)
        catalog: Catalog name to clean up. Defaults to current username.
        schema: Schema name. Defaults to "demo".
        genie_space_id: Genie space ID to delete. If None, skips.
        app_name: App name to delete. If None, skips.

    Returns:
        dict with cleanup results
    """
    results = {
        "tables_dropped": [],
        "schema_dropped": False,
        "catalog_dropped": False,
        "genie_deleted": False,
        "app_deleted": False,
    }

    # ------------------------------------------------------------------ #
    # 1. Derive catalog name from current user if not provided
    # ------------------------------------------------------------------ #
    if catalog is None:
        catalog = (
            spark.sql("SELECT current_user()")
            .first()[0]
            .split("@")[0]
            .replace(".", "_")
        )

    CATALOG = catalog
    SCHEMA = schema

    print(f"Cleaning up: `{CATALOG}`.`{SCHEMA}`")
    print()

    # ------------------------------------------------------------------ #
    # 2. Drop all 4 tables
    # ------------------------------------------------------------------ #
    tables = ["route_stops", "route_plans", "delivery_orders", "vehicles"]
    for tbl in tables:
        fqn = f"`{CATALOG}`.`{SCHEMA}`.`{tbl}`"
        try:
            spark.sql(f"DROP TABLE IF EXISTS {fqn}")
            print(f"✓ Dropped table {fqn}")
            results["tables_dropped"].append(tbl)
        except Exception as e:
            print(f"⚠ Could not drop {fqn}: {e}")

    # ------------------------------------------------------------------ #
    # 3. Drop the schema
    # ------------------------------------------------------------------ #
    try:
        spark.sql(f"DROP SCHEMA IF EXISTS `{CATALOG}`.`{SCHEMA}` CASCADE")
        print(f"✓ Dropped schema `{CATALOG}`.`{SCHEMA}`")
        results["schema_dropped"] = True
    except Exception as e:
        print(f"⚠ Could not drop schema: {e}")

    # ------------------------------------------------------------------ #
    # 4. Try to drop the catalog
    # ------------------------------------------------------------------ #
    try:
        spark.sql(f"DROP CATALOG IF EXISTS `{CATALOG}` CASCADE")
        print(f"✓ Dropped catalog `{CATALOG}`")
        results["catalog_dropped"] = True
    except Exception as e:
        if "PERMISSION_DENIED" in str(e) or "permission" in str(e).lower():
            print(f"⚠ Cannot drop catalog `{CATALOG}` (permissions). Skipping.")
        else:
            print(f"⚠ Could not drop catalog: {e}")

    # ------------------------------------------------------------------ #
    # 5. Delete Genie space via REST API
    # ------------------------------------------------------------------ #
    if genie_space_id and dbutils:
        try:
            workspace_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
            token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }

            resp = requests.delete(
                f"{workspace_url}/api/2.0/genie/spaces/{genie_space_id}",
                headers=headers,
            )
            if resp.status_code in (200, 204):
                print(f"✓ Deleted Genie space: {genie_space_id}")
                results["genie_deleted"] = True
            else:
                print(f"⚠ Genie space deletion returned {resp.status_code}: {resp.text[:500]}")
        except Exception as e:
            print(f"⚠ Could not delete Genie space: {e}")
    elif genie_space_id and not dbutils:
        print("⚠ genie_space_id provided but dbutils is None — cannot delete Genie space")
    else:
        print("⏭ Skipping Genie space deletion (no genie_space_id)")

    # ------------------------------------------------------------------ #
    # 6. Delete App via REST API
    # ------------------------------------------------------------------ #
    if app_name and dbutils:
        try:
            workspace_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
            token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }

            resp = requests.delete(
                f"{workspace_url}/api/2.0/apps/{app_name}",
                headers=headers,
            )
            if resp.status_code in (200, 204):
                print(f"✓ Deleted app: {app_name}")
                results["app_deleted"] = True
            else:
                print(f"⚠ App deletion returned {resp.status_code}: {resp.text[:500]}")
        except Exception as e:
            print(f"⚠ Could not delete app: {e}")
    elif app_name and not dbutils:
        print("⚠ app_name provided but dbutils is None — cannot delete app")
    else:
        print("⏭ Skipping app deletion (no app_name)")

    # ------------------------------------------------------------------ #
    # 7. Summary
    # ------------------------------------------------------------------ #
    print()
    print("=" * 60)
    print("  CLEANUP COMPLETE")
    print("=" * 60)
    print(f"  Tables dropped:  {len(results['tables_dropped'])}")
    print(f"  Schema dropped:  {results['schema_dropped']}")
    print(f"  Catalog dropped: {results['catalog_dropped']}")
    print(f"  Genie deleted:   {results['genie_deleted']}")
    print(f"  App deleted:     {results['app_deleted']}")
    print("=" * 60)

    return results
