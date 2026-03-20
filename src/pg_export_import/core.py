"""
pg_export_import.core
=====================
Production-ready PostgreSQL export/import utility.

Streams filtered rows from a source table to CSV, then bulk-loads them into a
target table using PostgreSQL COPY.  Both ends can be on different servers.

Data flow
---------
    Source PG
        └─[server-side cursor, batched fetchmany]──► Python process
                                                          └─[csv.writer]──► CSV file
    CSV file
        └─[COPY ... FROM STDIN WITH CSV HEADER]──► Target PG

Security model
--------------
* All schema/table/column identifiers are validated with a strict regex
  (``^[A-Za-z_][A-Za-z0-9_$]*$``, max 63 bytes) then wrapped in
  ``psycopg.sql.Identifier``.  String interpolation is never used for
  identifiers.

* ``where_clause`` is the primary injection surface.  It is embedded into
  the SELECT as a raw SQL fragment – **the text itself is not parameterised**.
  Rules:
    - The clause MUST come from trusted application code, never verbatim from
      user input.
    - Runtime values MUST be supplied via ``where_params`` (positional ``%s``
      or named ``%(key)s``), which psycopg parameterises safely.
    - A warning is logged when the clause is non-empty but ``where_params`` is
      ``None``, because that is a common mistake.

PostgreSQL notes / limitations
--------------------------------
Foreign keys
    Import parent tables first.  This utility does not resolve FK dependency
    order; callers must sequence multiple ``export_and_import`` calls correctly.
    Example: import ``customers`` before ``orders``.

Triggers
    ``COPY`` fires row-level triggers.  For large imports you may want to
    disable triggers on the target beforehand::

        ALTER TABLE target DISABLE TRIGGER ALL;
        -- run import --
        ALTER TABLE target ENABLE TRIGGER ALL;

    ``DISABLE TRIGGER ALL`` requires superuser or table owner privileges.

Indexes
    Large imports are faster if non-unique indexes are dropped first and
    recreated after.  This utility does not manage indexes.

Sequences / IDENTITY columns
    ``COPY`` does not advance sequences.  If the target has a serial/identity
    column whose values were populated from the source, run
    ``SELECT setval(...)`` after import to avoid future conflicts.

NULL handling
    Python's ``csv`` module writes ``None`` as an empty string.  PostgreSQL
    COPY interprets an unquoted empty field as ``NULL`` for typed columns
    (integer, date, …) but as an empty string ``''`` for text columns.
    If your source has ``NULL`` in a text column, the target will receive
    ``''``, not ``NULL``.  Callers that need exact NULL preservation for text
    columns should post-process or use a different approach.

CSV temp files
    Files are NOT automatically deleted.  The caller is responsible for
    cleanup after a successful import.  On failure the file is preserved
    intentionally to allow retry or debugging.

Character encoding
    The CSV file is written and read in UTF-8.  Both source and target
    PostgreSQL databases should use UTF-8 encoding.

Large tables
    For very large tables (hundreds of millions of rows) consider:
    * Adding a LIMIT clause inside ``where_clause`` and batching by range.
    * Ensuring sufficient disk space: CSV is text-encoded and typically
      1.5–2× the size of the raw table data.
    * Running ``VACUUM ANALYZE`` on the target after import.
"""

from __future__ import annotations

import csv
import dataclasses
import json
import logging
import os
import re
import tempfile
import time
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

__all__ = [
    "ConnectionConfig",
    "ExportImportResult",
    "export_and_import",
]

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Identifier validation
# ---------------------------------------------------------------------------

# Matches unquoted PostgreSQL identifiers.  PostgreSQL NAMEDATALEN is 64, so
# names longer than 63 bytes are silently truncated — we reject them instead.
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*\Z")
_MAX_IDENT_LEN = 63


def _validate_identifier(name: str) -> None:
    """Raise ``ValueError`` if *name* is not a safe PostgreSQL identifier.

    Only unquoted identifiers are accepted.  The identifier may later be
    double-quoted by ``psycopg.sql.Identifier``; that is fine and expected.

    Args:
        name: A single identifier fragment (no dots, no quotes).

    Raises:
        ValueError: If *name* is empty, too long, or contains unsafe characters.
    """
    if not name:
        raise ValueError("Identifier must not be empty.")
    if len(name.encode()) > _MAX_IDENT_LEN:
        raise ValueError(
            f"Identifier {name!r} exceeds PostgreSQL's {_MAX_IDENT_LEN}-byte limit."
        )
    if not _IDENT_RE.match(name):
        raise ValueError(
            f"Identifier {name!r} contains invalid characters.  "
            f"Only letters, digits, underscores, and dollar signs are allowed, "
            f"and the first character must be a letter or underscore."
        )


def _parse_table_ref(table_ref: str) -> tuple[str | None, str]:
    """Split a table reference into ``(schema, table)`` parts.

    Accepts bare table names (``"orders"``) or schema-qualified names
    (``"public.orders"``).  Each part is validated by
    :func:`_validate_identifier`.

    Args:
        table_ref: A table reference string, optionally schema-qualified.

    Returns:
        A ``(schema, table)`` tuple where *schema* may be ``None``.

    Raises:
        ValueError: If any part is invalid or the string has more than one dot.
    """
    parts = table_ref.split(".")
    if len(parts) == 1:
        _validate_identifier(parts[0])
        return None, parts[0]
    if len(parts) == 2:
        _validate_identifier(parts[0])
        _validate_identifier(parts[1])
        return parts[0], parts[1]
    raise ValueError(
        f"Table reference {table_ref!r} has too many dots.  "
        f"Expected 'table' or 'schema.table'."
    )


def _build_table_sql(table_ref: str) -> sql.Composable:
    """Return a ``psycopg.sql.Composable`` for a validated table reference.

    Args:
        table_ref: A validated table reference (bare or schema-qualified).

    Returns:
        A ``sql.Composable`` safe for use inside a ``psycopg.sql.SQL`` query.
    """
    schema, table = _parse_table_ref(table_ref)
    if schema is not None:
        return sql.Identifier(schema) + sql.SQL(".") + sql.Identifier(table)
    return sql.Identifier(table)


# ---------------------------------------------------------------------------
# Type-aware CSV serialization
# ---------------------------------------------------------------------------


def _fetch_column_type_info(
    conn: psycopg.Connection[Any],
    oids: list[int],
) -> dict[int, tuple[str, str]]:
    """Return a mapping of OID → (typname, typcategory) from pg_catalog.pg_type.

    Used to classify columns as arrays (typcategory='A') or JSON/JSONB
    (typname in ('json','jsonb')) so that values are serialized correctly
    before being written to CSV for COPY.

    Args:
        conn: An open database connection (used for a one-off query).
        oids: List of PostgreSQL type OIDs from cursor.description.

    Returns:
        Dict mapping each OID to a (typname, typcategory) pair.
        OIDs not found in pg_type are absent from the result.
    """
    if not oids:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT oid, typname, typcategory FROM pg_catalog.pg_type WHERE oid = ANY(%s)",
            (oids,),
        )
        return {row[0]: (row[1], row[2]) for row in cur.fetchall()}


def _pg_array_literal(value: list[Any]) -> str:
    """Convert a Python list to a PostgreSQL array literal for COPY.

    PostgreSQL COPY expects array literals in curly-brace notation, not
    Python's bracket notation.

    Examples:
        ['S4995']          → '{"S4995"}'
        [1, 2, 3]          → '{1,2,3}'
        [None, 'a']        → '{NULL,"a"}'
        [['a', 'b'], ['c']]→ '{{"a","b"},{"c"}}'
    """

    def _fmt(elem: Any) -> str:
        if elem is None:
            return "NULL"
        if isinstance(elem, list):
            return _pg_array_literal(elem)
        if isinstance(elem, bool):
            return "true" if elem else "false"
        if isinstance(elem, (int, float)):
            return str(elem)
        escaped = str(elem).replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'

    return "{" + ",".join(_fmt(e) for e in value) + "}"


def _serialize_cell(value: Any, typname: str, typcategory: str) -> Any:
    """Serialize a single cell value for CSV/COPY based on its PostgreSQL type.

    Returns:
        ``None`` unchanged — csv.writer writes an empty field, which COPY
        reads as NULL.
        A properly formatted string for array and JSON/JSONB columns.
        The original value unchanged for all scalar types.
    """
    if value is None:
        return None
    if typcategory == "A" and isinstance(value, list):
        return _pg_array_literal(value)
    if typname in ("json", "jsonb"):
        return json.dumps(value, ensure_ascii=False)
    return value


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class ConnectionConfig:
    """Connection parameters for a PostgreSQL database.

    Attributes:
        host: Hostname or IP address of the PostgreSQL server.
        port: Port number (default: 5432).
        dbname: Name of the database.
        user: Database user name.
        password: Database password.  In production use environment variables
            or a secrets manager — never hard-code.
        sslmode: SSL mode passed to libpq (e.g. ``"require"``, ``"prefer"``).
        connect_timeout: Connection timeout in seconds.
    """

    host: str
    dbname: str
    user: str
    password: str
    port: int = 5432
    sslmode: str = "prefer"
    connect_timeout: int = 10


@dataclasses.dataclass
class ExportImportResult:
    """Result of an :func:`export_and_import` call.

    Attributes:
        exported_count: Number of rows written to the CSV file.
        imported_count: Number of rows loaded into the target table.
        csv_path: Absolute path to the CSV file on disk.
        source_table: Table reference used as the export source.
        target_table: Table reference used as the import target.
        status: ``"success"``, ``"export_failed"``, or ``"import_failed"``.
        error: Human-readable error message when *status* is not ``"success"``.
        duration_seconds: Wall-clock time of the entire operation.
    """

    exported_count: int
    imported_count: int
    csv_path: str
    source_table: str
    target_table: str
    status: str
    error: str | None = None
    duration_seconds: float = 0.0


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _connect(config: ConnectionConfig) -> psycopg.Connection[Any]:
    """Open a psycopg v3 connection from a :class:`ConnectionConfig`.

    ``autocommit`` is left at its default (``False``); callers manage
    transactions explicitly.

    Args:
        config: Connection parameters.

    Returns:
        An open ``psycopg.Connection``.

    Raises:
        psycopg.OperationalError: If the connection cannot be established.
    """
    conninfo = (
        f"host={config.host} "
        f"port={config.port} "
        f"dbname={config.dbname} "
        f"user={config.user} "
        f"password={config.password} "
        f"sslmode={config.sslmode} "
        f"connect_timeout={config.connect_timeout}"
    )
    logger.debug(
        "Connecting to PostgreSQL: host=%s port=%s dbname=%s user=%s",
        config.host,
        config.port,
        config.dbname,
        config.user,
    )
    return psycopg.connect(conninfo)


def _export_to_csv(
    conn: psycopg.Connection[Any],
    table_ref: str,
    where_clause: str,
    csv_path: str,
    where_params: tuple[Any, ...] | dict[str, Any] | None,
    fetch_size: int,
) -> tuple[int, list[str]]:
    """Stream rows from *table_ref* to a CSV file.

    Uses a server-side (named) cursor so the full result set is never held
    in memory.  Column names and type info are resolved via a lightweight
    ``LIMIT 0`` probe before the named cursor is declared, keeping the two
    operations cleanly separated on the same connection.

    Args:
        conn: An open database connection to the source.
        table_ref: Validated source table reference.
        where_clause: SQL fragment for the WHERE clause (no ``WHERE`` keyword).
            May be an empty string to export all rows.
        csv_path: Absolute path to write the CSV file.
        where_params: Positional tuple or named dict of bind values for
            *where_clause*.  Pass ``None`` only when *where_clause* is empty.
        fetch_size: Number of rows to fetch per round-trip.

    Returns:
        A ``(row_count, column_names)`` tuple where *column_names* preserves
        the server-reported column order.

    Raises:
        psycopg.Error: On any database error during export.
        OSError: If the CSV file cannot be written.
    """
    # Warn when where_clause has text but no params (likely injection risk).
    if where_clause.strip() and where_params is None:
        logger.warning(
            "where_clause is non-empty but where_params is None.  "
            "Ensure where_clause does not contain user-supplied values; "
            "pass runtime values via where_params instead."
        )

    table_sql = _build_table_sql(table_ref)

    if where_clause.strip():
        query = (
            sql.SQL("SELECT * FROM ")
            + table_sql
            + sql.SQL(" WHERE ")
            + sql.SQL(where_clause)
        )
    else:
        query = sql.SQL("SELECT * FROM ") + table_sql

    logger.info("Starting export: table=%s where=%r", table_ref, where_clause or "(none)")

    # Resolve column names and type info via a LIMIT 0 probe BEFORE declaring
    # the named server-side cursor.
    #
    # Safety note — same connection, sequential cursors:
    # Both the probe cursor and _fetch_column_type_info run on *conn* before
    # the named cursor is declared.  psycopg v3 processes one command at a time
    # on a connection; as long as each client-side cursor is fully closed (the
    # `with` block exits) before the next command is issued, the wire-protocol
    # state is clean.  The probe cursor fetches 0 rows and the type-info query
    # calls fetchall(), so both are fully consumed before DECLARE is sent.
    # This is safe; the concern about truncation only arises when a second
    # cursor sends a command WHILE a named cursor's FETCH is in flight.
    with conn.cursor() as probe_cur:
        probe_cur.execute(
            sql.SQL("SELECT * FROM ") + _build_table_sql(table_ref) + sql.SQL(" LIMIT 0")
        )
        if probe_cur.description is None:
            raise RuntimeError("cursor.description is None after LIMIT 0 probe.")
        column_names = [col.name for col in probe_cur.description]
        oids = [col.type_code for col in probe_cur.description]

    logger.debug("Source columns (%d): %s", len(column_names), column_names)
    type_info = _fetch_column_type_info(conn, oids)
    col_types = [type_info.get(oid, ("unknown", "")) for oid in oids]

    row_count = 0

    with conn.cursor(name="pg_export_cursor") as cur:
        cur.execute(query, where_params)

        with open(csv_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(column_names)  # header

            while True:
                rows = cur.fetchmany(fetch_size)
                if not rows:
                    break
                for row in rows:
                    writer.writerow(
                        [_serialize_cell(v, *col_types[i]) for i, v in enumerate(row)]
                    )
                row_count += len(rows)
                if row_count % 50_000 == 0:
                    logger.info("  … exported %d rows so far", row_count)

    logger.info("Export complete: %d rows → %s", row_count, csv_path)
    return row_count, column_names


def _import_from_csv(
    conn: psycopg.Connection[Any],
    table_ref: str,
    csv_path: str,
    columns: list[str],
) -> int:
    """Load a CSV file into *table_ref* using ``COPY ... FROM STDIN``.

    The COPY command includes an explicit column list (derived from the CSV
    header) so the target table's column order need not match the source's.
    Extra columns in the target will receive their default values.

    The operation runs inside the caller-managed transaction.  On success the
    caller should commit; on failure the caller should roll back.

    Args:
        conn: An open database connection to the target (autocommit=False).
        table_ref: Validated target table reference.
        csv_path: Absolute path to the CSV file to load.
        columns: Ordered list of column names matching the CSV header.

    Returns:
        Number of rows imported (``cursor.rowcount`` after COPY).

    Raises:
        psycopg.Error: On any database error during import.
        FileNotFoundError: If *csv_path* does not exist.
    """
    table_sql = _build_table_sql(table_ref)
    col_list = sql.SQL(", ").join(sql.Identifier(c) for c in columns)

    copy_sql = (
        sql.SQL("COPY ")
        + table_sql
        + sql.SQL(" (")
        + col_list
        + sql.SQL(") FROM STDIN WITH (FORMAT CSV, HEADER TRUE)")
    )

    logger.info(
        "Starting import: table=%s columns=%s file=%s",
        table_ref,
        columns,
        csv_path,
    )

    with conn.cursor() as cur:
        with cur.copy(copy_sql) as copy:
            with open(csv_path, "rb") as fh:
                while True:
                    block = fh.read(65_536)  # 64 KB chunks
                    if not block:
                        break
                    copy.write(block)

        imported = cur.rowcount

    logger.info("Import complete: %d rows ← %s", imported, csv_path)
    return imported


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def export_and_import(
    source_config: ConnectionConfig,
    target_config: ConnectionConfig,
    source_table: str,
    target_table: str,
    where_clause: str,
    csv_path: str | None = None,
    where_params: tuple[Any, ...] | dict[str, Any] | None = None,
    fetch_size: int = 5000,
) -> ExportImportResult:
    """Export filtered rows from a source table and import them into a target table.

    **Two-phase process**:

    1. *Export* – Connects to *source_config*, opens a server-side cursor,
       streams rows matching *where_clause* to a CSV file on local disk.
    2. *Import* – Connects to *target_config*, issues
       ``COPY target_table (...) FROM STDIN WITH CSV HEADER``, loading the CSV
       file written in phase 1.  The import is atomic: any failure triggers a
       full rollback.

    The CSV file acts as a durable checkpoint.  If the import fails, the
    exported data remains on disk and can be re-imported after fixing the
    issue.  **The caller is responsible for deleting the file after a
    successful import.**

    Security warning — ``where_clause``
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ``where_clause`` is embedded into the SELECT statement as a raw SQL
    fragment.  The text itself is **not** parameterised.  Rules:

    * The clause text MUST originate from trusted application code.
    * Runtime values MUST be passed via ``where_params``, never interpolated
      into the clause string itself.
    * Example safe usage::

          export_and_import(
              ...,
              where_clause="status = %s AND created_at > %s",
              where_params=("active", datetime(2024, 1, 1)),
          )

    * Unsafe usage (do not do this)::

          # WRONG – user_input could be "1=1; DROP TABLE orders; --"
          export_and_import(..., where_clause=f"id = {user_input}")

    Args:
        source_config: Connection parameters for the source database.
        target_config: Connection parameters for the target database.
        source_table: Source table reference, e.g. ``"orders"`` or
            ``"public.orders"``.
        target_table: Target table reference, e.g. ``"orders_archive"`` or
            ``"staging.orders"``.
        where_clause: SQL fragment appended after ``WHERE`` (no ``WHERE``
            keyword).  Pass an empty string ``""`` to export all rows.
        csv_path: Path for the intermediate CSV file.  If ``None``, a
            temporary file is created in the system temp directory.
        where_params: Bind values for ``where_clause`` placeholders.
        fetch_size: Rows fetched per round-trip from the source cursor.

    Returns:
        An :class:`ExportImportResult` with counts, paths, and status.

    Raises:
        ValueError: If any table identifier is invalid (fast-fail, before any
            connections are opened).
    """
    # --- Pre-flight: validate identifiers (fail fast, no I/O yet) ---
    _parse_table_ref(source_table)
    _parse_table_ref(target_table)

    # --- Resolve csv_path ---
    if csv_path is None:
        fd, csv_path = tempfile.mkstemp(suffix=".csv", prefix="pg_export_")
        os.close(fd)
        logger.debug("Using temp CSV file: %s", csv_path)
    csv_path = str(Path(csv_path).resolve())

    start = time.monotonic()
    exported_count = 0
    imported_count = 0

    # ------------------------------------------------------------------ #
    # Phase 1 — Export                                                     #
    # ------------------------------------------------------------------ #
    column_names: list[str] = []
    try:
        with _connect(source_config) as src_conn:
            exported_count, column_names = _export_to_csv(
                conn=src_conn,
                table_ref=source_table,
                where_clause=where_clause,
                csv_path=csv_path,
                where_params=where_params,
                fetch_size=fetch_size,
            )
    except Exception as exc:
        logger.error("Export failed: %s", exc, exc_info=True)
        return ExportImportResult(
            exported_count=0,
            imported_count=0,
            csv_path=csv_path,
            source_table=source_table,
            target_table=target_table,
            status="export_failed",
            error=str(exc),
            duration_seconds=time.monotonic() - start,
        )

    # Zero-row export is valid — skip import to avoid an empty COPY.
    if exported_count == 0:
        logger.info("Zero rows exported; skipping import.")
        return ExportImportResult(
            exported_count=0,
            imported_count=0,
            csv_path=csv_path,
            source_table=source_table,
            target_table=target_table,
            status="success",
            duration_seconds=time.monotonic() - start,
        )

    # ------------------------------------------------------------------ #
    # Phase 2 — Import                                                     #
    # ------------------------------------------------------------------ #
    try:
        with _connect(target_config) as tgt_conn:
            try:
                imported_count = _import_from_csv(
                    conn=tgt_conn,
                    table_ref=target_table,
                    csv_path=csv_path,
                    columns=column_names,
                )
                tgt_conn.commit()
            except Exception:
                tgt_conn.rollback()
                raise
    except Exception as exc:
        logger.error("Import failed: %s", exc, exc_info=True)
        return ExportImportResult(
            exported_count=exported_count,
            imported_count=0,
            csv_path=csv_path,
            source_table=source_table,
            target_table=target_table,
            status="import_failed",
            error=str(exc),
            duration_seconds=time.monotonic() - start,
        )

    return ExportImportResult(
        exported_count=exported_count,
        imported_count=imported_count,
        csv_path=csv_path,
        source_table=source_table,
        target_table=target_table,
        status="success",
        duration_seconds=time.monotonic() - start,
    )
