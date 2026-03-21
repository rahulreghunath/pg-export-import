"""
pg_export_import.pipeline
=========================
Orchestrates a sequential, FK-aware export/import pipeline across multiple
PostgreSQL tables using :func:`pg_export_import.core.export_and_import`.

Pipeline flow per table
-----------------------
1. DELETE matching rows from the target table (idempotent re-runs)
2. Call export_and_import  (export source → CSV → import target)
3. Delete the CSV file on success

FK ordering
-----------
The ``tables`` list is processed top-to-bottom.  Put parent tables first so
that:
  - DELETE on the target does not violate FK constraints from child rows
    (delete children before parents if the target already has child rows)
  - IMPORT respects FK constraints (parents exist before children are inserted)

CSV file naming
---------------
Each pipeline run captures a single timestamp at start.  All CSV files for
that run share the same timestamp in their names::

    {csv_dir}/{source_table}_{YYYYMMDD_HHMMSS}.csv

Dots in schema-qualified names are replaced with underscores, e.g.
``public.orders`` → ``public_orders_20250320_143022.csv``.
"""

from __future__ import annotations

import logging
import os
import tempfile
import time
from datetime import datetime
from typing import Any

from pg_export_import.core import (
    ConnectionConfig,
    ExportImportResult,
    delete_target_rows,
    export_and_import,
)

__all__ = ["run_pipeline", "delete_target_rows"]

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal: single-table processor
# ---------------------------------------------------------------------------


def _process_table(
    source_config: ConnectionConfig,
    target_config: ConnectionConfig,
    entry: dict,
    idx: int,
    total: int,
    csv_dir: str,
    run_ts: str,
    fetch_size: int,
    delete_before_import: bool,
) -> tuple[dict, bool]:
    """Process a single table entry: optionally delete target rows, export/import, clean CSV.

    Args:
        source_config: Source database connection parameters.
        target_config: Target database connection parameters.
        entry: Table config dict (see :func:`run_pipeline` for the schema).
        idx: 1-based position in the pipeline (for logging).
        total: Total number of tables in the pipeline (for logging).
        csv_dir: Directory where CSV files are written.
        run_ts: Timestamp string shared by all tables in this pipeline run
            (``YYYYMMDD_HHMMSS`` format).
        fetch_size: Default rows fetched per round-trip from the source cursor.
            Overridden per-table via ``entry["fetch_size"]``.
        delete_before_import: Whether to delete matching rows from the target
            before importing.  Overridden per-table via
            ``entry["delete_before_import"]``.

    Returns:
        ``(row, failed)`` where *row* is the result dict and *failed* is
        ``True`` when the step errored and the caller should halt the pipeline.
    """
    source_table = entry["source_table"]
    target_table = entry["target_table"]
    where_clause = entry.get("where_clause", "")
    where_params = entry.get("where_params", None)
    delete_where_clause = entry.get("delete_where_clause", where_clause)
    delete_where_params = entry.get("delete_where_params", where_params)
    effective_fetch_size = entry.get("fetch_size", fetch_size)
    should_delete = entry.get("delete_before_import", delete_before_import)

    logger.info(
        "--- Table %d/%d: %s → %s  where=%r  delete=%s",
        idx, total, source_table, target_table, where_clause or "(all rows)", should_delete,
    )

    # Build deterministic CSV path: dots in table name replaced with underscores.
    csv_filename = f"{source_table.replace('.', '_')}_{run_ts}.csv"
    csv_path = os.path.join(csv_dir, csv_filename)

    row: dict = {
        "table": f"{source_table} → {target_table}",
        "status": "pending",
        "exported": 0,
        "imported": 0,
        "deleted": 0,
        "duration": 0.0,
        "csv_path": csv_path,
        "error": None,
    }

    # Step 1 — DELETE matching rows from target (skipped when should_delete is False)
    if should_delete:
        try:
            row["deleted"] = delete_target_rows(
                target_config, target_table, delete_where_clause, delete_where_params
            )
        except Exception as exc:
            row["status"] = "delete_failed"
            row["error"] = str(exc)
            logger.error("DELETE failed for %s: %s", target_table, exc, exc_info=True)
            return row, True
    else:
        logger.debug("Skipping DELETE for %s (delete_before_import=False)", target_table)

    # Step 2 — export_and_import
    try:
        result: ExportImportResult = export_and_import(
            source_config=source_config,
            target_config=target_config,
            source_table=source_table,
            target_table=target_table,
            where_clause=where_clause,
            where_params=where_params,
            csv_path=csv_path,
            fetch_size=effective_fetch_size,
        )
    except Exception as exc:
        row["status"] = "error"
        row["error"] = str(exc)
        logger.error("export_and_import raised for %s: %s", source_table, exc, exc_info=True)
        return row, True

    row["exported"] = result.exported_count
    row["imported"] = result.imported_count
    row["duration"] = result.duration_seconds
    row["csv_path"] = result.csv_path
    row["status"] = result.status
    row["error"] = result.error

    if result.status != "success":
        logger.error(
            "Table %s failed (status=%s): %s  CSV preserved at %s",
            source_table, result.status, result.error, result.csv_path,
        )
        return row, True

    # Step 3 — clean up CSV on success
    try:
        if result.csv_path and os.path.exists(result.csv_path):
            os.remove(result.csv_path)
            logger.debug("Removed CSV: %s", result.csv_path)
    except OSError as exc:
        logger.warning("Could not remove CSV %s: %s", result.csv_path, exc)

    return row, False


# ---------------------------------------------------------------------------
# Public: pipeline orchestrator
# ---------------------------------------------------------------------------


def run_pipeline(
    source_config: ConnectionConfig,
    target_config: ConnectionConfig,
    tables: list[dict],
    stop_on_failure: bool = True,
    csv_dir: str | None = None,
    fetch_size: int = 5000,
    delete_before_import: bool = True,
) -> list[dict]:
    """Run the export/import pipeline sequentially for each table in *tables*.

    For each entry the pipeline:

    1. DELETEs matching rows from the target table.
    2. Calls :func:`~pg_export_import.core.export_and_import`
       (export → CSV → import).
    3. Removes the CSV file on success (preserved on failure for re-import).

    CSV files are named ``{source_table}_{YYYYMMDD_HHMMSS}.csv`` where dots
    in schema-qualified table names are replaced with underscores.  All tables
    in a single run share the same timestamp.

    Tables are processed in list order; put FK parents before children.

    Args:
        source_config: Source database connection parameters.
        target_config: Target database connection parameters.
        tables: Ordered list of table config dicts.  Each dict must have:

            * ``source_table`` (str) — source table reference
            * ``target_table`` (str) — target table reference
            * ``where_clause`` (str, optional) — SQL WHERE fragment;
              defaults to ``""`` (all rows)
            * ``where_params`` (tuple | dict | None, optional) — bind values
            * ``delete_where_clause`` (str, optional) — override WHERE for the
              DELETE step; falls back to *where_clause*
            * ``delete_where_params`` (tuple | dict | None, optional) — bind
              values for *delete_where_clause*
            * ``fetch_size`` (int, optional) — per-table override for the
              number of rows fetched per round-trip; takes precedence over the
              pipeline-level *fetch_size* argument
            * ``delete_before_import`` (bool, optional) — per-table override
              for whether to delete matching rows from the target before
              importing; takes precedence over the pipeline-level argument

        stop_on_failure: If ``True`` (default), halt on the first error.
            If ``False``, continue with remaining tables.
        csv_dir: Directory where intermediate CSV files are written.  Created
            automatically if it does not exist.  Defaults to the system temp
            directory when ``None``.
        fetch_size: Rows fetched per round-trip from the source cursor for all
            tables.  Individual tables can override this via ``"fetch_size"``
            in their config dict.  Defaults to ``5000``.
        delete_before_import: Whether to delete matching rows from the target
            before importing, for all tables.  Set to ``False`` to append
            without deleting (e.g. for insert-only / audit tables).  Individual
            tables can override this via ``"delete_before_import"`` in their
            config dict.  Defaults to ``True``.

    Returns:
        List of result dicts, one per table attempted:
        ``{table, status, exported, imported, deleted, duration, csv_path, error}``
    """
    if not tables:
        logger.info("Pipeline is empty — nothing to do.")
        return []

    resolved_csv_dir = csv_dir if csv_dir is not None else tempfile.gettempdir()
    os.makedirs(resolved_csv_dir, exist_ok=True)
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    pipeline_start = time.monotonic()

    logger.info(
        "Pipeline start: %d table(s), csv_dir=%s, run_ts=%s, fetch_size=%d, delete_before_import=%s",
        len(tables), resolved_csv_dir, run_ts, fetch_size, delete_before_import,
    )

    total = len(tables)
    summary: list[dict] = []

    for idx, entry in enumerate(tables, start=1):
        row, failed = _process_table(
            source_config, target_config, entry, idx, total,
            resolved_csv_dir, run_ts, fetch_size, delete_before_import,
        )
        summary.append(row)
        if failed and stop_on_failure:
            logger.error("Stopping pipeline after failure on table %d/%d.", idx, total)
            break

    attempted = len(summary)
    succeeded = sum(1 for r in summary if r["status"] == "success")
    skipped = total - attempted
    logger.info(
        "Pipeline complete: %d/%d table(s) succeeded%s  total=%.2fs",
        succeeded, total,
        f"  ({skipped} not attempted)" if skipped else "",
        time.monotonic() - pipeline_start,
    )
    return summary
