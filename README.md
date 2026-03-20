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
)
print(result.status, result.exported_count, result.imported_count)
```

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
    csv_dir="/data/exports",   # optional; defaults to tempdir
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

## CLI usage

Edit `TABLE_PIPELINE` in `src/pg_export_import/__main__.py` to define your tables, then run:

```bash
# Required env vars
export SRC_PG_HOST=db1.example.com
export SRC_PG_DB=prod
export SRC_PG_PASSWORD=secret

export TGT_PG_HOST=db2.example.com
export TGT_PG_DB=staging
export TGT_PG_PASSWORD=secret

# Optional
export CSV_DIR=/data/exports   # default: system temp directory

pg-export-import
# or
python -m pg_export_import
```

### Environment variable reference

| Variable          | Required | Default    | Description                              |
|-------------------|----------|------------|------------------------------------------|
| `SRC_PG_HOST`     | yes      | —          | Source PostgreSQL host                   |
| `SRC_PG_DB`       | yes      | —          | Source database name                     |
| `SRC_PG_PASSWORD` | yes      | —          | Source database password                 |
| `SRC_PG_PORT`     | no       | `5432`     | Source PostgreSQL port                   |
| `SRC_PG_USER`     | no       | `postgres` | Source database user                     |
| `TGT_PG_HOST`     | yes      | —          | Target PostgreSQL host                   |
| `TGT_PG_DB`       | yes      | —          | Target database name                     |
| `TGT_PG_PASSWORD` | yes      | —          | Target database password                 |
| `TGT_PG_PORT`     | no       | `5432`     | Target PostgreSQL port                   |
| `TGT_PG_USER`     | no       | `postgres` | Target database user                     |
| `CSV_DIR`         | no       | tempdir    | Output directory for intermediate CSVs   |

---

## Table config schema

Each entry in `TABLE_PIPELINE` (or the `tables` argument to `run_pipeline`) supports:

| Key                    | Type                      | Required | Description                                        |
|------------------------|---------------------------|----------|----------------------------------------------------|
| `source_table`         | str                       | yes      | Source table reference (`"table"` or `"schema.table"`) |
| `target_table`         | str                       | yes      | Target table reference                             |
| `where_clause`         | str                       | yes      | SQL WHERE fragment (empty string = all rows)       |
| `where_params`         | tuple \| dict \| None     | no       | Bind values for `where_clause` placeholders        |
| `delete_where_clause`  | str                       | no       | WHERE fragment for DELETE on target (defaults to `where_clause`) |
| `delete_where_params`  | tuple \| dict \| None     | no       | Bind values for `delete_where_clause`              |

---

## Security notes

- `where_clause` is embedded as a raw SQL fragment — it **must** come from trusted application code, never from user input.
- Runtime filter values **must** be passed via `where_params`, not interpolated into the clause string.
- All table and column identifiers are validated and safely quoted via `psycopg.sql.Identifier`.
