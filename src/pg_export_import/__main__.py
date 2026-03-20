"""
pg_export_import.__main__
=========================
CLI entry point.  Invoked via:

    python -m pg_export_import
    pg-export-import          (after pip install)

Configuration is entirely via environment variables — see README for the full
list.  The table pipeline is defined in TABLE_PIPELINE below; edit it to match
your schema's FK dependency order.
"""

from __future__ import annotations

import logging
import os
import sys

from pg_export_import.core import ConnectionConfig
from pg_export_import.pipeline import run_pipeline

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Table pipeline configuration
# ---------------------------------------------------------------------------
# Each entry is a dict with:
#   source_table          str   source table reference (e.g. "public.orders")
#   target_table          str   target table reference
#   where_clause          str   SQL WHERE fragment (empty = all rows)
#   where_params          tuple | dict | None  bind values for where_clause
#
# Optional overrides for the DELETE step:
#   delete_where_clause   str   WHERE fragment for DELETE on target
#   delete_where_params   tuple | dict | None  bind values for delete clause
#
# List order = execution order. Parent tables must come before child tables.
# ---------------------------------------------------------------------------

TABLE_PIPELINE: list[dict] = [
    {
        "source_table": "drug_master",
        "target_table": "drug_master1",
        "where_clause": "",        # empty = all rows
        "where_params": None,
    },
    # Add more tables here in FK dependency order, e.g.:
    # {
    #     "source_table": "public.order_items",
    #     "target_table": "staging.order_items",
    #     "where_clause": "created_at >= %s",
    #     "where_params": ("2024-01-01",),
    # },
]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Run the pipeline and print a summary table.

    Connection parameters are read from environment variables at call time so
    that the module can be imported without them being set.
    """
    # Required env vars — KeyError gives a clear message if any are missing.
    source_config = ConnectionConfig(
        host=os.environ["SRC_PG_HOST"],
        port=int(os.environ.get("SRC_PG_PORT", "5432")),
        dbname=os.environ["SRC_PG_DB"],
        user=os.environ.get("SRC_PG_USER", "postgres"),
        password=os.environ["SRC_PG_PASSWORD"],
    )
    target_config = ConnectionConfig(
        host=os.environ["TGT_PG_HOST"],
        port=int(os.environ.get("TGT_PG_PORT", "5432")),
        dbname=os.environ["TGT_PG_DB"],
        user=os.environ.get("TGT_PG_USER", "postgres"),
        password=os.environ["TGT_PG_PASSWORD"],
    )
    csv_dir = os.environ.get("CSV_DIR")  # None → run_pipeline falls back to tempdir

    results = run_pipeline(
        source_config=source_config,
        target_config=target_config,
        tables=TABLE_PIPELINE,
        stop_on_failure=True,
        csv_dir=csv_dir,
    )

    # Print summary table
    print("\n" + "=" * 70)
    print(f"{'TABLE':<35} {'STATUS':<14} {'EXP':>7} {'IMP':>7} {'DEL':>7}")
    print("-" * 70)
    for r in results:
        print(
            f"{r['table']:<35} {r['status']:<14} "
            f"{r['exported']:>7} {r['imported']:>7} {r['deleted']:>7}"
        )
        if r["error"]:
            print(f"  ERROR: {r['error']}")
    print("=" * 70)

    failed = [r for r in results if r["status"] != "success"]
    if failed:
        logger.error("%d/%d table(s) failed.", len(failed), len(results))
        sys.exit(1)
    else:
        logger.info(
            "Pipeline complete. %d/%d table(s) succeeded.",
            len(results), len(TABLE_PIPELINE),
        )


if __name__ == "__main__":
    main()
