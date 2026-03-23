"""
demo.py
=======
Demonstrates pg_export_import package usage.

Run with env vars set:

    export SRC_PG_HOST=192.168.10.13  SRC_PG_PORT=5439
    export SRC_PG_DB=bayvrio_db       SRC_PG_PASSWORD=secret
    export TGT_PG_HOST=192.168.10.13  TGT_PG_PORT=5439
    export TGT_PG_DB=bayvrio_db       TGT_PG_PASSWORD=secret
    export CSV_DIR=/tmp/pg_demo

    python demo.py
"""

import logging
import os

from pg_export_import import ConnectionConfig
from pg_export_import import ExportImportResult
from pg_export_import import export_and_import
from pg_export_import import run_pipeline

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Demo 1: Single-table export_and_import
# ---------------------------------------------------------------------------


def demo_single_table(
    src: ConnectionConfig, tgt: ConnectionConfig, csv_dir: str
) -> None:
    """Export one table and import it into a target table."""
    print("\n" + "=" * 60)
    print("DEMO 1: Single-table export_and_import")
    print("=" * 60)

    result: ExportImportResult = export_and_import(
        source_config=src,
        target_config=tgt,
        source_table="drug_master",
        target_table="drug_master1",
        where_clause="",  # all rows
        csv_path=os.path.join(csv_dir, "drug_master_single.csv"),
        fetch_size=5000,  # explicit batch size
    )

    print(f"  Status   : {result.status}")
    print(f"  Exported : {result.exported_count:,} rows")
    print(f"  Imported : {result.imported_count:,} rows")
    print(f"  CSV      : {result.csv_path}")
    print(f"  Duration : {result.duration_seconds:.2f}s")

    if result.error:
        print(f"  Error    : {result.error}")

    # Clean up CSV on success
    if result.status == "success" and os.path.exists(result.csv_path):
        os.remove(result.csv_path)
        logger.info("Removed CSV: %s", result.csv_path)


# ---------------------------------------------------------------------------
# Demo 2: delete_target_rows before a filtered export
# ---------------------------------------------------------------------------


def demo_delete_then_export(
    src: ConnectionConfig, tgt: ConnectionConfig, csv_dir: str
) -> None:
    """Delete matching rows from target then export/import using the built-in delete_before_import param."""
    print("\n" + "=" * 60)
    print("DEMO 2: export_and_import with delete_before_import=True")
    print("=" * 60)

    result = export_and_import(
        source_config=src,
        target_config=tgt,
        source_table="drug_master",
        target_table="drug_master1",
        where_clause="",
        where_params=None,
        csv_path=os.path.join(csv_dir, "drug_master_filtered.csv"),
        delete_before_import=True,  # delete target rows before importing
        # delete_where_clause="...",     # optional: different WHERE for DELETE vs SELECT
        # delete_where_params=(...),     # optional: bind values for delete_where_clause
    )

    print(f"  Status   : {result.status}")
    print(f"  Deleted  : {result.deleted_count:,} rows from target")
    print(f"  Exported : {result.exported_count:,} rows")
    print(f"  Imported : {result.imported_count:,} rows")
    print(f"  Duration : {result.duration_seconds:.2f}s")

    if result.error:
        print(f"  Error    : {result.error}")

    if result.status == "success" and os.path.exists(result.csv_path):
        os.remove(result.csv_path)
        logger.info("Removed CSV: %s", result.csv_path)


# ---------------------------------------------------------------------------
# Demo 3: Multi-table pipeline with shared timestamp
# ---------------------------------------------------------------------------


def demo_pipeline(src: ConnectionConfig, tgt: ConnectionConfig, csv_dir: str) -> None:
    """Run a multi-table pipeline — all CSVs share the same timestamp."""
    print("\n" + "=" * 60)
    print("DEMO 3: run_pipeline (multi-table, FK-aware)")
    print("=" * 60)

    tables = [
        # Table 1 — inherits pipeline-level delete_before_import=True → DELETE runs
        {
            "source_table": "drug_master",
            "target_table": "drug_master1",
            "where_clause": "",  # all rows
            "where_params": None,
            # "fetch_size": 2_000,     # uncomment to override batch size for this table
            # "delete_before_import": False,  # uncomment to skip DELETE for this table only
        },
        # Add child tables below, e.g.:
        # {
        #     "source_table": "drug_detail",
        #     "target_table": "drug_detail1",
        #     "where_clause": "drug_id IN (SELECT id FROM drug_master)",
        #     "where_params": None,
        #     "delete_before_import": False,  # append-only — no DELETE even though pipeline default is True
        # },
    ]

    results = run_pipeline(
        source_config=src,
        target_config=tgt,
        tables=tables,
        stop_on_failure=True,
        csv_dir=csv_dir,
        fetch_size=10_000,  # pipeline-level default; override per-table via "fetch_size" key
        delete_before_import=True,  # pipeline-level default; override per-table via "delete_before_import" key
    )

    # Summary table
    print()
    print(f"  {'TABLE':<35} {'STATUS':<14} {'EXP':>7} {'IMP':>7} {'DEL':>7}")
    print("  " + "-" * 66)
    for r in results:
        print(
            f"  {r['table']:<35} {r['status']:<14} "
            f"{r['exported']:>7,} {r['imported']:>7,} {r['deleted']:>7,}"
        )
        if r["error"]:
            print(f"    ERROR: {r['error']}")
        if r["csv_path"]:
            print(f"    CSV  : {r['csv_path']}")

    failed = [r for r in results if r["status"] != "success"]
    print()
    if failed:
        print(f"  FAILED: {len(failed)}/{len(results)} table(s) failed.")
    else:
        print(f"  OK: all {len(results)} table(s) succeeded.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    csv_dir = os.environ.get("CSV_DIR", "/tmp/pg_demo")
    src_config = ConnectionConfig(
        host="localhost",
        port=5434,
        dbname="db_name",
        user="db_user",
        password="db_password",
    )
    tgt_config = ConnectionConfig(
        host="localhost",
        port=5434,
        dbname="db_name",
        user="db_user",
        password="db_password",
    )
    os.makedirs(csv_dir, exist_ok=True)
    logger.info("CSV output directory: %s", csv_dir)

    demo_single_table(src_config, tgt_config, csv_dir)
    demo_delete_then_export(src_config, tgt_config, csv_dir)
    demo_pipeline(src_config, tgt_config, csv_dir)

    print("\nDemo complete.")
