"""
pg_export_import
================
Stream-export filtered PostgreSQL rows to CSV and bulk-import via COPY.

Public API
----------
Library:
    ConnectionConfig     — database connection parameters
    ExportImportResult   — result of a single export/import call
    export_and_import    — export a table from source and import into target

Pipeline orchestrator:
    run_pipeline         — sequential multi-table FK-aware pipeline
    delete_target_rows   — delete matching rows from a target table
"""

from pg_export_import.core import (
    ConnectionConfig,
    ExportImportResult,
    delete_target_rows,
    export_and_import,
)
from pg_export_import.pipeline import run_pipeline

__version__ = "0.1.0"

__all__ = [
    "ConnectionConfig",
    "ExportImportResult",
    "export_and_import",
    "run_pipeline",
    "delete_target_rows",
]
