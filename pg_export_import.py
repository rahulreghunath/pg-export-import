"""
pg_export_import.py
===================
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

Example usage
-------------
See the ``if __name__ == "__main__"`` block at the bottom of this file.
"""

from __future__ import annotations

import csv
import dataclasses
import json
import logging
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
    column_names: list[str],
    col_types: list[tuple[str, str]],
) -> int:
    """Stream rows from *table_ref* to a CSV file using a dedicated connection.

    This function issues ONLY the server-side cursor DECLARE + FETCH commands
    on *conn*.  No other queries are run on this connection before or during
    the fetch loop.  Callers must resolve column metadata (names, types) on a
    separate connection and pass them in — mixing metadata queries on the same
    connection corrupts psycopg v3's wire-protocol state and causes fetchmany()
    to return empty prematurely, silently truncating the export.

    Args:
        conn: A DEDICATED connection used exclusively for the export cursor.
            No other queries should be executed on this connection.
        table_ref: Validated source table reference.
        where_clause: SQL fragment for the WHERE clause (no ``WHERE`` keyword).
            May be an empty string to export all rows.
        csv_path: Absolute path to write the CSV file.
        where_params: Positional tuple or named dict of bind values for
            *where_clause*.  Pass ``None`` only when *where_clause* is empty.
        fetch_size: Number of rows to fetch per round-trip.
        column_names: Ordered column names resolved by the caller on a separate
            connection (used as the CSV header).
        col_types: Per-column (typname, typcategory) pairs, parallel to
            *column_names*, used by :func:`_serialize_cell`.

    Returns:
        Number of rows written to the CSV file.

    Raises:
        psycopg.Error: On any database error during export.
        OSError: If the CSV file cannot be written.
    """
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

    row_count = 0
    batch_num = 0

    print(f"[DEBUG] Opening named server-side cursor 'pg_export_cursor' (dedicated connection), fetch_size={fetch_size}")
    with conn.cursor(name="pg_export_cursor") as cur:
        cur.execute(query, where_params)
        print(f"[DEBUG] Query executed on server-side cursor — starting fetch loop")

        with open(csv_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(column_names)  # header
            print(f"[DEBUG] CSV header written to {csv_path!r}")

            while True:
                rows = cur.fetchmany(fetch_size)
                batch_num += 1
                print(f"[DEBUG] Batch {batch_num}: fetched {len(rows)} rows (cumulative: {row_count + len(rows)})")
                if not rows:
                    print(f"[DEBUG] Empty batch — fetch loop complete")
                    break
                for row in rows:
                    writer.writerow(
                        [_serialize_cell(v, *col_types[i]) for i, v in enumerate(row)]
                    )
                row_count += len(rows)
                if row_count % 50_000 == 0:
                    logger.info("  … exported %d rows so far", row_count)

    csv_size = Path(csv_path).stat().st_size
    print(f"[DEBUG] Export done: {row_count} rows, {batch_num - 1} batches, CSV size={csv_size:,} bytes ({csv_path!r})")
    logger.info("Export complete: %d rows → %s", row_count, csv_path)
    return row_count


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
    csv_size = Path(csv_path).stat().st_size
    print(f"[DEBUG] Import starting: table={table_ref!r}, {len(columns)} columns, CSV size={csv_size:,} bytes")
    print(f"[DEBUG] Import columns: {columns}")

    blocks_written = 0
    bytes_written = 0
    with conn.cursor() as cur:
        with cur.copy(copy_sql) as copy:
            with open(csv_path, "rb") as fh:
                while True:
                    block = fh.read(65_536)  # 64 KB chunks
                    if not block:
                        break
                    copy.write(block)
                    blocks_written += 1
                    bytes_written += len(block)
                    print(f"[DEBUG]   COPY block {blocks_written}: {len(block):,} bytes sent (total: {bytes_written:,})")

        imported = cur.rowcount

    print(f"[DEBUG] Import done: {imported} rows imported via {blocks_written} COPY blocks")
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
        import os
        os.close(fd)
        logger.debug("Using temp CSV file: %s", csv_path)
    csv_path = str(Path(csv_path).resolve())

    start = time.monotonic()
    exported_count = 0
    imported_count = 0

    print(f"[DEBUG] export_and_import start")
    print(f"[DEBUG]   source: {source_config.host}:{source_config.port}/{source_config.dbname} user={source_config.user}")
    print(f"[DEBUG]   target: {target_config.host}:{target_config.port}/{target_config.dbname} user={target_config.user}")
    print(f"[DEBUG]   source_table={source_table!r}  target_table={target_table!r}")
    print(f"[DEBUG]   where_clause={where_clause!r}  where_params={where_params!r}")
    print(f"[DEBUG]   fetch_size={fetch_size}  csv_path={csv_path!r}")

    # ------------------------------------------------------------------ #
    # Phase 1a — Metadata (separate connection, closed before export)     #
    # ------------------------------------------------------------------ #
    # IMPORTANT: All schema queries (COUNT, column names, type OIDs) run on
    # a dedicated metadata connection that is fully closed before the export
    # connection is opened.  Running any query on the same connection that
    # will later hold the named server-side cursor corrupts psycopg v3's
    # wire-protocol state and causes fetchmany() to return empty prematurely,
    # silently truncating the export.
    column_names: list[str] = []
    col_types: list[tuple[str, str]] = []
    expected_count: int = 0
    print(f"\n[DEBUG] === Phase 1a: Metadata (separate connection) ===")
    try:
        with _connect(source_config) as meta_conn:
            table_sql_meta = _build_table_sql(source_table)

            # COUNT(*) with the same WHERE so we know the expected total.
            if where_clause.strip():
                count_query = (
                    sql.SQL("SELECT COUNT(*) FROM ")
                    + table_sql_meta
                    + sql.SQL(" WHERE ")
                    + sql.SQL(where_clause)
                )
            else:
                count_query = sql.SQL("SELECT COUNT(*) FROM ") + table_sql_meta

            with meta_conn.cursor() as mcur:
                mcur.execute(count_query, where_params)
                row = mcur.fetchone()
                expected_count = row[0] if row else 0
            print(f"[DEBUG] COUNT(*) on source: expected_count={expected_count}")
            logger.info("Source row count: %d", expected_count)

            # Schema probe — LIMIT 0 to get column names and OIDs.
            with meta_conn.cursor() as mcur:
                mcur.execute(
                    sql.SQL("SELECT * FROM ") + table_sql_meta + sql.SQL(" LIMIT 0")
                )
                if mcur.description is None:
                    raise RuntimeError("cursor.description is None after LIMIT 0 probe.")
                column_names = [col.name for col in mcur.description]
                oids = [col.type_code for col in mcur.description]
            print(f"[DEBUG] Schema probe: {len(column_names)} columns: {column_names}")
            print(f"[DEBUG] Column OIDs: {oids}")

            # Type info lookup.
            type_info = _fetch_column_type_info(meta_conn, oids)
            col_types = [type_info.get(oid, ("unknown", "")) for oid in oids]
            print(f"[DEBUG] Resolved column types: { {col: ct for col, ct in zip(column_names, col_types)} }")
        # meta_conn is now fully closed — the export connection is clean.
        print(f"[DEBUG] Metadata connection closed")
    except Exception as exc:
        logger.error("Metadata fetch failed: %s", exc, exc_info=True)
        print(f"[DEBUG] Metadata fetch FAILED: {exc}")
        return ExportImportResult(
            exported_count=0,
            imported_count=0,
            csv_path=csv_path,
            source_table=source_table,
            target_table=target_table,
            status="export_failed",
            error=f"Metadata fetch failed: {exc}",
            duration_seconds=time.monotonic() - start,
        )

    # ------------------------------------------------------------------ #
    # Phase 1b — Export (dedicated connection, no other queries)          #
    # ------------------------------------------------------------------ #
    print(f"\n[DEBUG] === Phase 1b: Export (dedicated connection) ===")
    try:
        with _connect(source_config) as src_conn:
            exported_count = _export_to_csv(
                conn=src_conn,
                table_ref=source_table,
                where_clause=where_clause,
                csv_path=csv_path,
                where_params=where_params,
                fetch_size=fetch_size,
                column_names=column_names,
                col_types=col_types,
            )
    except Exception as exc:
        logger.error("Export failed: %s", exc, exc_info=True)
        print(f"[DEBUG] Export FAILED after {time.monotonic() - start:.2f}s: {exc}")
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

    export_elapsed = time.monotonic() - start
    print(f"[DEBUG] Phase 1 complete: exported_count={exported_count}  expected={expected_count}  elapsed={export_elapsed:.2f}s")
    if exported_count != expected_count:
        msg = (
            f"Row count mismatch: exported {exported_count} rows but "
            f"COUNT(*) returned {expected_count} before export started. "
            f"The export is incomplete."
        )
        logger.warning(msg)
        print(f"[DEBUG] WARNING: {msg}")

    # Zero-row export is valid — skip import to avoid an empty COPY.
    if exported_count == 0:
        logger.info("Zero rows exported; skipping import.")
        print(f"[DEBUG] Zero rows exported — skipping import phase")
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
    print(f"\n[DEBUG] === Phase 2: Import ===")
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
        print(f"[DEBUG] Import FAILED after {time.monotonic() - start:.2f}s total: {exc}")
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

    total_elapsed = time.monotonic() - start
    print(f"[DEBUG] Phase 2 complete: imported_count={imported_count}  elapsed={total_elapsed:.2f}s total")
    print(f"[DEBUG] Final result: status=success  exported={exported_count}  imported={imported_count}  duration={total_elapsed:.2f}s")

    return ExportImportResult(
        exported_count=exported_count,
        imported_count=imported_count,
        csv_path=csv_path,
        source_table=source_table,
        target_table=target_table,
        status="success",
        duration_seconds=total_elapsed,
    )


# ---------------------------------------------------------------------------
# Example usage
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import os

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )

    # ------------------------------------------------------------------
    # Example 1: Export active users from production → staging
    # ------------------------------------------------------------------
    # Production and staging share the same schema; only active users are
    # migrated.  Passwords come from environment variables — never hard-code.

    prod_cfg = ConnectionConfig(
        host=os.environ.get("PROD_PG_HOST", "prod-db.example.com"),
        dbname=os.environ.get("PROD_PG_DB", "app_production"),
        user=os.environ.get("PROD_PG_USER", "readonly_user"),
        password=os.environ.get("PROD_PG_PASSWORD", ""),
    )
    staging_cfg = ConnectionConfig(
        host=os.environ.get("STAGING_PG_HOST", "staging-db.example.com"),
        dbname=os.environ.get("STAGING_PG_DB", "app_staging"),
        user=os.environ.get("STAGING_PG_USER", "rw_user"),
        password=os.environ.get("STAGING_PG_PASSWORD", ""),
    )

    print("\n--- Example 1: active users ---")
    result1 = export_and_import(
        source_config=prod_cfg,
        target_config=staging_cfg,
        source_table="public.users",
        target_table="public.users",
        where_clause="status = %s",
        where_params=("active",),
        csv_path="/tmp/active_users.csv",
    )
    print(result1)
    if result1.status == "success":
        os.remove(result1.csv_path)  # caller cleans up after success

    # ------------------------------------------------------------------
    # Example 2: Export recent orders with a date range filter
    # ------------------------------------------------------------------
    # Note: orders references customers — import customers first if the
    # target table has a foreign key constraint on customer_id.
    from datetime import datetime

    print("\n--- Example 2: recent orders ---")
    result2 = export_and_import(
        source_config=prod_cfg,
        target_config=staging_cfg,
        source_table="sales.orders",
        target_table="sales.orders",
        where_clause="created_at >= %s AND created_at < %s AND status != %s",
        where_params=(datetime(2024, 1, 1), datetime(2024, 4, 1), "cancelled"),
        # csv_path omitted → auto-generated temp file
    )
    print(result2)
    if result2.status == "success":
        os.remove(result2.csv_path)
