# pg-export-import

Stream-export filtered PostgreSQL rows to CSV and bulk-import via `COPY`.  Supports multi-table, FK-aware pipelines with idempotent re-runs.

## Installation

```bash
pip install -e .          # editable / development
pip install -e ".[dev]"   # include pytest
```

## Requirements

- Python 3.11+
- PostgreSQL (source and target can be on different servers)

---

## Library usage

### Single table

```python
from pg_export_import import ConnectionConfig, export_and_import

src = ConnectionConfig(host="db1.example.com", dbname="prod", user="app", password="...")
tgt = ConnectionConfig(host="db2.example.com", dbname="staging", user="app", password="...")

result = export_and_import(
    source_config=src,
    target_config=tgt,
    source_table="public.orders",
    target_table="public.orders",
    where_clause="created_at >= %s",
    where_params=("2024-01-01",),
    csv_path="/data/exports/orders.csv",   # optional; auto-generated if omitted
    delete_before_import=True,             # optional; delete target rows before importing (default False)
    # delete_where_clause="created_at >= %s",  # optional; defaults to where_clause
    # delete_where_params=("2024-01-01",),     # optional; defaults to where_params
)
print(result.status, result.exported_count, result.imported_count, result.deleted_count)
```

`result.status` is one of `"success"`, `"export_failed"`, `"delete_failed"`, or `"import_failed"`.

### Multi-table pipeline

```python
from pg_export_import import ConnectionConfig, run_pipeline

src = ConnectionConfig(...)
tgt = ConnectionConfig(...)

tables = [
    # parents before children (FK order)
    {"source_table": "customers",   "target_table": "customers",   "where_clause": ""},
    {"source_table": "orders",      "target_table": "orders",      "where_clause": "created_at >= %s", "where_params": ("2024-01-01",)},
    {"source_table": "order_items", "target_table": "order_items", "where_clause": "created_at >= %s", "where_params": ("2024-01-01",)},
]

results = run_pipeline(
    source_config=src,
    target_config=tgt,
    tables=tables,
    csv_dir="/data/exports",        # optional; defaults to tempdir
    fetch_size=5000,                # optional; rows per round-trip (default 5000)
    delete_before_import=True,      # optional; set False to append without deleting (default True)
)
for r in results:
    print(r["table"], r["status"], r["exported"], r["imported"])
```

#### CSV file naming

Each pipeline run captures a single timestamp once at start.  All tables in that run share the same timestamp:

```
{csv_dir}/{source_table}_{YYYYMMDD_HHMMSS}.csv
```

Dots in schema-qualified names become underscores:

| source_table     | csv_dir         | filename                                   |
|------------------|-----------------|--------------------------------------------|
| `orders`         | `/data/exports` | `orders_20250320_143022.csv`               |
| `public.orders`  | `/data/exports` | `public_orders_20250320_143022.csv`        |

- The `csv_dir` is created automatically if it does not exist.
- On success, CSV files are deleted.
- On failure, CSV files are preserved for debugging or re-import.

---

## Table config schema

Each entry in the `tables` argument to `run_pipeline` supports:

| Key                    | Type                      | Required | Description                                        |
|------------------------|---------------------------|----------|----------------------------------------------------|
| `source_table`         | str                       | yes      | Source table reference (`"table"` or `"schema.table"`) |
| `target_table`         | str                       | yes      | Target table reference                             |
| `where_clause`         | str                       | yes      | SQL WHERE fragment (empty string = all rows)       |
| `where_params`         | tuple \| dict \| None     | no       | Bind values for `where_clause` placeholders        |
| `delete_where_clause`  | str                       | no       | WHERE fragment for DELETE on target (defaults to `where_clause`) |
| `delete_where_params`  | tuple \| dict \| None     | no       | Bind values for `delete_where_clause`              |
| `fetch_size`           | int                       | no       | Per-table override for rows fetched per round-trip; takes precedence over `run_pipeline`'s `fetch_size` argument |
| `delete_before_import` | bool                      | no       | Per-table override; when `False`, skips DELETE for this table regardless of the pipeline-level argument |

---

## Performance notes

### TRUNCATE for full-table deletes

When `delete_before_import=True` and the `where_clause` is empty (i.e. exporting all rows), `delete_target_rows()` uses `TRUNCATE` instead of `DELETE FROM`. This is significantly faster for large tables because TRUNCATE:

- Does not scan rows or write per-row WAL entries
- Reclaims storage immediately
- Completes in near-constant time regardless of table size

This applies automatically to Category A (master/reference) tables where no WHERE filter is needed. When a `where_clause` is provided, a standard `DELETE ... WHERE` is used as before.

**Note:** `TRUNCATE` does not return a row count, so `deleted_count` will be `0` for truncated tables. The `deleted_count` field only reflects rows removed via filtered `DELETE`.

---

## Security notes

- `where_clause` is embedded as a raw SQL fragment — it **must** come from trusted application code, never from user input.
- Runtime filter values **must** be passed via `where_params`, not interpolated into the clause string.
- All table and column identifiers are validated and safely quoted via `psycopg.sql.Identifier`.
