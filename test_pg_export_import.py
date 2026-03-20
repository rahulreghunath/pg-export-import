"""
Tests for pg_export_import.py

Unit tests run with no database.
Integration tests (marked @pytest.mark.integration) require a real PostgreSQL
instance.  Set the following environment variables before running them:

    PG_TEST_HOST     (default: localhost)
    PG_TEST_PORT     (default: 5432)
    PG_TEST_DB       (default: pg_export_test)
    PG_TEST_USER     (default: postgres)
    PG_TEST_PASSWORD (default: postgres)

Run only unit tests:
    pytest test_pg_export_import.py -m "not integration"

Run all tests (requires PostgreSQL):
    pytest test_pg_export_import.py
"""

from __future__ import annotations

import csv
import os
import textwrap
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, patch, call
import pytest

from pg_export_import import (
    ConnectionConfig,
    ExportImportResult,
    _build_table_sql,
    _fetch_column_type_info,
    _parse_table_ref,
    _pg_array_literal,
    _serialize_cell,
    _validate_identifier,
    export_and_import,
)
from psycopg import sql


# ==========================================================================
# Unit tests — identifier validation
# ==========================================================================


class TestValidateIdentifier:
    def test_valid_simple_name(self):
        _validate_identifier("users")  # no exception

    def test_valid_underscore_start(self):
        _validate_identifier("_private")

    def test_valid_mixed_case(self):
        _validate_identifier("MyTable")

    def test_valid_with_digits(self):
        _validate_identifier("table2")

    def test_valid_with_dollar(self):
        _validate_identifier("col$1")

    def test_valid_exactly_63_bytes(self):
        _validate_identifier("a" * 63)

    def test_rejects_empty(self):
        with pytest.raises(ValueError, match="empty"):
            _validate_identifier("")

    def test_rejects_too_long(self):
        with pytest.raises(ValueError, match="63-byte"):
            _validate_identifier("a" * 64)

    def test_rejects_digit_start(self):
        with pytest.raises(ValueError, match="invalid characters"):
            _validate_identifier("1table")

    def test_rejects_hyphen(self):
        with pytest.raises(ValueError, match="invalid characters"):
            _validate_identifier("my-table")

    def test_rejects_space(self):
        with pytest.raises(ValueError, match="invalid characters"):
            _validate_identifier("my table")

    def test_rejects_semicolon(self):
        with pytest.raises(ValueError, match="invalid characters"):
            _validate_identifier("t; DROP TABLE users; --")

    def test_rejects_single_quote(self):
        with pytest.raises(ValueError, match="invalid characters"):
            _validate_identifier("o'brien")

    def test_rejects_double_quote(self):
        with pytest.raises(ValueError, match="invalid characters"):
            _validate_identifier('"quoted"')

    def test_rejects_asterisk(self):
        with pytest.raises(ValueError, match="invalid characters"):
            _validate_identifier("table*")

    def test_rejects_dot(self):
        # Dots belong in parse_table_ref, not here.
        with pytest.raises(ValueError, match="invalid characters"):
            _validate_identifier("schema.table")


class TestParseTableRef:
    def test_bare_table(self):
        schema, table = _parse_table_ref("orders")
        assert schema is None
        assert table == "orders"

    def test_schema_qualified(self):
        schema, table = _parse_table_ref("public.orders")
        assert schema == "public"
        assert table == "orders"

    def test_rejects_too_many_dots(self):
        with pytest.raises(ValueError, match="too many dots"):
            _parse_table_ref("a.b.c")

    def test_rejects_invalid_schema(self):
        with pytest.raises(ValueError, match="invalid characters"):
            _validate_identifier("bad-schema")  # surfaced via parse internally

    def test_rejects_invalid_table_part(self):
        with pytest.raises(ValueError, match="invalid characters"):
            _parse_table_ref("public.1bad")


class TestBuildTableSql:
    def test_bare_table_produces_identifier(self):
        result = _build_table_sql("users")
        composed = result.as_string(None)  # type: ignore[arg-type]
        assert '"users"' in composed

    def test_schema_qualified_produces_dotted(self):
        result = _build_table_sql("public.users")
        composed = result.as_string(None)  # type: ignore[arg-type]
        assert '"public"."users"' in composed


# ==========================================================================
# Unit tests — export_and_import with mocked psycopg
# ==========================================================================


def _make_fake_description(col_names: list[str]):
    """Return a list of mock column descriptors (each with a .name attr)."""
    return [MagicMock(name=col) for col in col_names]


class TestExportAndImportUnit:
    """Unit tests using mocked psycopg connections."""

    def _make_cursor_mock(self, rows: list[tuple], columns: list[str]):
        cur = MagicMock()
        cur.__enter__ = lambda s: s
        cur.__exit__ = MagicMock(return_value=False)
        # description uses real attribute, not constructor name
        desc = []
        for col in columns:
            d = MagicMock()
            d.name = col
            d.type_code = 25  # text OID — no special serialization
            desc.append(d)
        cur.description = desc

        # fetchmany returns all rows on first call, then empty list
        fetch_calls = [rows, []]
        cur.fetchmany.side_effect = fetch_calls
        cur.rowcount = len(rows)
        return cur

    def _make_copy_mock(self):
        copy_cm = MagicMock()
        copy_cm.__enter__ = lambda s: s
        copy_cm.__exit__ = MagicMock(return_value=False)
        copy_cm.write = MagicMock()
        return copy_cm

    @patch("pg_export_import._connect")
    @patch("pg_export_import._fetch_column_type_info", return_value={})
    def test_happy_path(self, _mock_type_info, mock_connect, tmp_path):
        columns = ["id", "name", "status"]
        rows = [(1, "Alice", "active"), (2, "Bob", "active")]
        csv_file = str(tmp_path / "out.csv")

        # Source connection mock
        src_cur = self._make_cursor_mock(rows, columns)
        src_conn = MagicMock()
        src_conn.__enter__ = lambda s: s
        src_conn.__exit__ = MagicMock(return_value=False)
        src_conn.cursor.return_value = src_cur

        # Target connection mock
        tgt_cur = MagicMock()
        tgt_cur.__enter__ = lambda s: s
        tgt_cur.__exit__ = MagicMock(return_value=False)
        copy_mock = self._make_copy_mock()
        tgt_cur.copy.return_value = copy_mock
        tgt_cur.rowcount = 2
        tgt_conn = MagicMock()
        tgt_conn.__enter__ = lambda s: s
        tgt_conn.__exit__ = MagicMock(return_value=False)
        tgt_conn.cursor.return_value = tgt_cur

        mock_connect.side_effect = [src_conn, tgt_conn]

        src_cfg = ConnectionConfig("src-host", "src_db", "user", "pass")
        tgt_cfg = ConnectionConfig("tgt-host", "tgt_db", "user", "pass")

        result = export_and_import(
            source_config=src_cfg,
            target_config=tgt_cfg,
            source_table="users",
            target_table="users",
            where_clause="status = %s",
            where_params=("active",),
            csv_path=csv_file,
        )

        assert result.status == "success"
        assert result.exported_count == 2
        assert result.source_table == "users"
        assert result.target_table == "users"
        assert result.error is None

        # Verify CSV was written with header
        written = Path(csv_file).read_text()
        assert "id,name,status" in written
        assert "Alice" in written

    @patch("pg_export_import._connect")
    @patch("pg_export_import._fetch_column_type_info", return_value={})
    def test_zero_rows_skips_import(self, _mock_type_info, mock_connect, tmp_path):
        columns = ["id", "email"]
        rows: list = []
        csv_file = str(tmp_path / "empty.csv")

        src_cur = self._make_cursor_mock(rows, columns)
        src_conn = MagicMock()
        src_conn.__enter__ = lambda s: s
        src_conn.__exit__ = MagicMock(return_value=False)
        src_conn.cursor.return_value = src_cur

        mock_connect.return_value = src_conn

        src_cfg = ConnectionConfig("h", "db", "u", "p")
        tgt_cfg = ConnectionConfig("h", "db", "u", "p")

        result = export_and_import(
            source_config=src_cfg,
            target_config=tgt_cfg,
            source_table="users",
            target_table="users",
            where_clause="id = %s",
            where_params=(9999,),
            csv_path=csv_file,
        )

        assert result.status == "success"
        assert result.exported_count == 0
        assert result.imported_count == 0
        # import connection should not have been opened
        assert mock_connect.call_count == 1

    @patch("pg_export_import._connect")
    def test_export_failure_returns_export_failed(self, mock_connect, tmp_path):
        csv_file = str(tmp_path / "fail.csv")
        src_conn = MagicMock()
        src_conn.__enter__ = lambda s: s
        src_conn.__exit__ = MagicMock(return_value=False)
        src_conn.cursor.side_effect = Exception("connection refused")
        mock_connect.return_value = src_conn

        result = export_and_import(
            source_config=ConnectionConfig("h", "db", "u", "p"),
            target_config=ConnectionConfig("h", "db", "u", "p"),
            source_table="tbl",
            target_table="tbl",
            where_clause="",
            csv_path=csv_file,
        )

        assert result.status == "export_failed"
        assert "connection refused" in result.error

    @patch("pg_export_import._connect")
    @patch("pg_export_import._fetch_column_type_info", return_value={})
    def test_import_failure_returns_import_failed_and_rollback(
        self, _mock_type_info, mock_connect, tmp_path
    ):
        columns = ["id"]
        rows = [(1,), (2,)]
        csv_file = str(tmp_path / "import_fail.csv")

        src_cur = self._make_cursor_mock(rows, columns)
        src_conn = MagicMock()
        src_conn.__enter__ = lambda s: s
        src_conn.__exit__ = MagicMock(return_value=False)
        src_conn.cursor.return_value = src_cur

        tgt_cur = MagicMock()
        tgt_cur.__enter__ = lambda s: s
        tgt_cur.__exit__ = MagicMock(return_value=False)
        tgt_cur.copy.side_effect = Exception("FK violation")
        tgt_conn = MagicMock()
        tgt_conn.__enter__ = lambda s: s
        tgt_conn.__exit__ = MagicMock(return_value=False)
        tgt_conn.cursor.return_value = tgt_cur

        mock_connect.side_effect = [src_conn, tgt_conn]

        result = export_and_import(
            source_config=ConnectionConfig("h", "db", "u", "p"),
            target_config=ConnectionConfig("h", "db", "u", "p"),
            source_table="orders",
            target_table="orders",
            where_clause="",
            csv_path=csv_file,
        )

        assert result.status == "import_failed"
        assert result.exported_count == 2
        assert result.imported_count == 0
        tgt_conn.rollback.assert_called_once()

    def test_invalid_source_table_raises_value_error(self, tmp_path):
        with pytest.raises(ValueError):
            export_and_import(
                source_config=ConnectionConfig("h", "db", "u", "p"),
                target_config=ConnectionConfig("h", "db", "u", "p"),
                source_table="bad-table!",
                target_table="ok_table",
                where_clause="",
                csv_path=str(tmp_path / "x.csv"),
            )

    def test_invalid_target_table_raises_value_error(self, tmp_path):
        with pytest.raises(ValueError):
            export_and_import(
                source_config=ConnectionConfig("h", "db", "u", "p"),
                target_config=ConnectionConfig("h", "db", "u", "p"),
                source_table="ok_table",
                target_table="DROP TABLE users --",
                where_clause="",
                csv_path=str(tmp_path / "x.csv"),
            )

    @patch("pg_export_import._connect")
    @patch("pg_export_import._fetch_column_type_info", return_value={})
    def test_where_clause_without_params_logs_warning(
        self, _mock_type_info, mock_connect, tmp_path, caplog
    ):
        columns = ["id"]
        rows: list = []
        csv_file = str(tmp_path / "w.csv")

        src_cur = self._make_cursor_mock(rows, columns)
        src_conn = MagicMock()
        src_conn.__enter__ = lambda s: s
        src_conn.__exit__ = MagicMock(return_value=False)
        src_conn.cursor.return_value = src_cur
        mock_connect.return_value = src_conn

        import logging

        with caplog.at_level(logging.WARNING, logger="pg_export_import"):
            export_and_import(
                source_config=ConnectionConfig("h", "db", "u", "p"),
                target_config=ConnectionConfig("h", "db", "u", "p"),
                source_table="tbl",
                target_table="tbl",
                where_clause="status = 'active'",
                where_params=None,
                csv_path=csv_file,
            )

        assert any("where_params is None" in r.message for r in caplog.records)

    @patch("pg_export_import._connect")
    @patch("pg_export_import._fetch_column_type_info", return_value={})
    def test_auto_generated_csv_path(self, _mock_type_info, mock_connect):
        columns = ["id"]
        rows = [(42,)]

        src_cur = self._make_cursor_mock(rows, columns)
        src_conn = MagicMock()
        src_conn.__enter__ = lambda s: s
        src_conn.__exit__ = MagicMock(return_value=False)
        src_conn.cursor.return_value = src_cur

        tgt_cur = MagicMock()
        tgt_cur.__enter__ = lambda s: s
        tgt_cur.__exit__ = MagicMock(return_value=False)
        copy_mock = self._make_copy_mock()
        tgt_cur.copy.return_value = copy_mock
        tgt_cur.rowcount = 1
        tgt_conn = MagicMock()
        tgt_conn.__enter__ = lambda s: s
        tgt_conn.__exit__ = MagicMock(return_value=False)
        tgt_conn.cursor.return_value = tgt_cur

        mock_connect.side_effect = [src_conn, tgt_conn]

        result = export_and_import(
            source_config=ConnectionConfig("h", "db", "u", "p"),
            target_config=ConnectionConfig("h", "db", "u", "p"),
            source_table="tbl",
            target_table="tbl",
            where_clause="",
            csv_path=None,
        )

        assert result.status == "success"
        assert result.csv_path.endswith(".csv")
        # Clean up temp file
        if Path(result.csv_path).exists():
            os.remove(result.csv_path)

    @patch("pg_export_import._connect")
    @patch("pg_export_import._fetch_column_type_info", return_value={})
    def test_schema_qualified_tables(self, _mock_type_info, mock_connect, tmp_path):
        columns = ["order_id", "amount"]
        rows = [(100, "99.99")]
        csv_file = str(tmp_path / "sq.csv")

        src_cur = self._make_cursor_mock(rows, columns)
        src_conn = MagicMock()
        src_conn.__enter__ = lambda s: s
        src_conn.__exit__ = MagicMock(return_value=False)
        src_conn.cursor.return_value = src_cur

        tgt_cur = MagicMock()
        tgt_cur.__enter__ = lambda s: s
        tgt_cur.__exit__ = MagicMock(return_value=False)
        copy_mock = self._make_copy_mock()
        tgt_cur.copy.return_value = copy_mock
        tgt_cur.rowcount = 1
        tgt_conn = MagicMock()
        tgt_conn.__enter__ = lambda s: s
        tgt_conn.__exit__ = MagicMock(return_value=False)
        tgt_conn.cursor.return_value = tgt_cur

        mock_connect.side_effect = [src_conn, tgt_conn]

        result = export_and_import(
            source_config=ConnectionConfig("h", "db", "u", "p"),
            target_config=ConnectionConfig("h", "db", "u", "p"),
            source_table="sales.orders",
            target_table="archive.orders",
            where_clause="",
            csv_path=csv_file,
        )

        assert result.status == "success"
        assert result.source_table == "sales.orders"
        assert result.target_table == "archive.orders"

    def test_result_includes_duration(self, tmp_path):
        # Even a failed result should include duration
        with pytest.raises(ValueError):
            export_and_import(
                source_config=ConnectionConfig("h", "db", "u", "p"),
                target_config=ConnectionConfig("h", "db", "u", "p"),
                source_table="bad!",
                target_table="tbl",
                where_clause="",
                csv_path=str(tmp_path / "x.csv"),
            )

    @patch("pg_export_import._connect")
    def test_export_failed_result_has_duration(self, mock_connect, tmp_path):
        src_conn = MagicMock()
        src_conn.__enter__ = lambda s: s
        src_conn.__exit__ = MagicMock(return_value=False)
        src_conn.cursor.side_effect = Exception("boom")
        mock_connect.return_value = src_conn

        result = export_and_import(
            source_config=ConnectionConfig("h", "db", "u", "p"),
            target_config=ConnectionConfig("h", "db", "u", "p"),
            source_table="tbl",
            target_table="tbl",
            where_clause="",
            csv_path=str(tmp_path / "x.csv"),
        )

        assert result.duration_seconds >= 0.0


# ==========================================================================
# Unit tests — type-aware serialization helpers
# ==========================================================================


class TestPgArrayLiteral:
    def test_single_string_element(self):
        assert _pg_array_literal(["S4995"]) == '{"S4995"}'

    def test_multiple_string_elements(self):
        assert _pg_array_literal(["a", "b"]) == '{"a","b"}'

    def test_integer_elements(self):
        assert _pg_array_literal([1, 2, 3]) == "{1,2,3}"

    def test_none_element_becomes_null(self):
        assert _pg_array_literal([None, "x"]) == '{NULL,"x"}'

    def test_all_none(self):
        assert _pg_array_literal([None, None]) == "{NULL,NULL}"

    def test_empty_list(self):
        assert _pg_array_literal([]) == "{}"

    def test_nested_list(self):
        assert _pg_array_literal([["a", "b"], ["c"]]) == '{{"a","b"},{"c"}}'

    def test_string_with_double_quote_escaped(self):
        assert _pg_array_literal(['say "hi"']) == '{"say \\"hi\\""}'

    def test_string_with_backslash_escaped(self):
        assert _pg_array_literal(["a\\b"]) == '{"a\\\\b"}'

    def test_boolean_true(self):
        assert _pg_array_literal([True]) == "{true}"

    def test_boolean_false(self):
        assert _pg_array_literal([False]) == "{false}"

    def test_float_element(self):
        assert _pg_array_literal([1.5]) == "{1.5}"


class TestSerializeCell:
    # --- NULL passthrough ---
    def test_none_returns_none_for_any_type(self):
        assert _serialize_cell(None, "text", "S") is None
        assert _serialize_cell(None, "text", "A") is None
        assert _serialize_cell(None, "jsonb", "U") is None

    # --- Array columns (typcategory='A') ---
    def test_array_list_produces_pg_literal(self):
        result = _serialize_cell(["S4995"], "_text", "A")
        assert result == '{"S4995"}'

    def test_array_with_multiple_elements(self):
        result = _serialize_cell(["a", "b", "c"], "_text", "A")
        assert result == '{"a","b","c"}'

    def test_array_non_list_passthrough(self):
        # Already-serialized string should pass through unchanged
        result = _serialize_cell('{"S4995"}', "_text", "A")
        assert result == '{"S4995"}'

    # --- JSON/JSONB columns ---
    def test_jsonb_list_produces_json_string(self):
        result = _serialize_cell(["S4995"], "jsonb", "U")
        assert result == '["S4995"]'

    def test_json_dict_produces_json_string(self):
        result = _serialize_cell({"key": "val"}, "json", "U")
        assert result == '{"key": "val"}'

    def test_jsonb_nested_structure(self):
        result = _serialize_cell({"a": [1, 2]}, "jsonb", "U")
        import json as _json
        assert _json.loads(result) == {"a": [1, 2]}

    def test_json_non_ascii_preserved(self):
        result = _serialize_cell({"msg": "héllo"}, "jsonb", "U")
        assert "héllo" in result

    # --- Scalar passthrough ---
    def test_int_unchanged(self):
        assert _serialize_cell(42, "int4", "N") == 42

    def test_str_unchanged(self):
        assert _serialize_cell("hello", "text", "S") == "hello"

    def test_bool_unchanged(self):
        assert _serialize_cell(True, "bool", "B") is True


class TestSerializationEndToEnd:
    """Verify CSV output contains correct PostgreSQL literals for complex types."""

    @patch("pg_export_import._connect")
    @patch("pg_export_import._fetch_column_type_info")
    def test_array_column_written_as_pg_literal(
        self, mock_type_info, mock_connect, tmp_path
    ):
        # Simulate a text[] column (OID 1009) and a plain text column (OID 25)
        mock_type_info.return_value = {1009: ("_text", "A"), 25: ("text", "S")}

        columns = ["tags", "name"]
        rows = [(["S4995", "X1"], "Alice")]
        csv_file = str(tmp_path / "array_out.csv")

        src_cur = MagicMock()
        src_cur.__enter__ = lambda s: s
        src_cur.__exit__ = MagicMock(return_value=False)
        desc = []
        for col, oid in zip(columns, [1009, 25]):
            d = MagicMock()
            d.name = col
            d.type_code = oid
            desc.append(d)
        src_cur.description = desc
        src_cur.fetchmany.side_effect = [[rows[0]], []]
        src_cur.rowcount = 1

        src_conn = MagicMock()
        src_conn.__enter__ = lambda s: s
        src_conn.__exit__ = MagicMock(return_value=False)
        src_conn.cursor.return_value = src_cur

        tgt_cur = MagicMock()
        tgt_cur.__enter__ = lambda s: s
        tgt_cur.__exit__ = MagicMock(return_value=False)
        copy_mock = MagicMock()
        copy_mock.__enter__ = lambda s: s
        copy_mock.__exit__ = MagicMock(return_value=False)
        tgt_cur.copy.return_value = copy_mock
        tgt_cur.rowcount = 1
        tgt_conn = MagicMock()
        tgt_conn.__enter__ = lambda s: s
        tgt_conn.__exit__ = MagicMock(return_value=False)
        tgt_conn.cursor.return_value = tgt_cur

        mock_connect.side_effect = [src_conn, tgt_conn]

        result = export_and_import(
            source_config=ConnectionConfig("h", "db", "u", "p"),
            target_config=ConnectionConfig("h", "db", "u", "p"),
            source_table="tbl",
            target_table="tbl",
            where_clause="",
            csv_path=csv_file,
        )

        assert result.status == "success"
        with open(csv_file, newline="") as fh:
            reader = csv.reader(fh)
            _header = next(reader)
            data_row = next(reader)
        tags_cell, _name_cell = data_row
        # PostgreSQL array literal, not Python repr
        assert tags_cell == '{"S4995","X1"}'
        assert tags_cell != "['S4995', 'X1']"

    @patch("pg_export_import._connect")
    @patch("pg_export_import._fetch_column_type_info")
    def test_jsonb_column_written_as_json_string(
        self, mock_type_info, mock_connect, tmp_path
    ):
        mock_type_info.return_value = {3802: ("jsonb", "U")}

        columns = ["data"]
        rows = [({"id": 1, "vals": ["S4995"]},)]
        csv_file = str(tmp_path / "json_out.csv")

        src_cur = MagicMock()
        src_cur.__enter__ = lambda s: s
        src_cur.__exit__ = MagicMock(return_value=False)
        d = MagicMock()
        d.name = "data"
        d.type_code = 3802
        src_cur.description = [d]
        src_cur.fetchmany.side_effect = [[rows[0]], []]
        src_cur.rowcount = 1

        src_conn = MagicMock()
        src_conn.__enter__ = lambda s: s
        src_conn.__exit__ = MagicMock(return_value=False)
        src_conn.cursor.return_value = src_cur

        tgt_cur = MagicMock()
        tgt_cur.__enter__ = lambda s: s
        tgt_cur.__exit__ = MagicMock(return_value=False)
        copy_mock = MagicMock()
        copy_mock.__enter__ = lambda s: s
        copy_mock.__exit__ = MagicMock(return_value=False)
        tgt_cur.copy.return_value = copy_mock
        tgt_cur.rowcount = 1
        tgt_conn = MagicMock()
        tgt_conn.__enter__ = lambda s: s
        tgt_conn.__exit__ = MagicMock(return_value=False)
        tgt_conn.cursor.return_value = tgt_cur

        mock_connect.side_effect = [src_conn, tgt_conn]

        result = export_and_import(
            source_config=ConnectionConfig("h", "db", "u", "p"),
            target_config=ConnectionConfig("h", "db", "u", "p"),
            source_table="tbl",
            target_table="tbl",
            where_clause="",
            csv_path=csv_file,
        )

        assert result.status == "success"
        import json as _json
        with open(csv_file, newline="") as fh:
            reader = csv.reader(fh)
            _header = next(reader)
            data_row = next(reader)
        cell = data_row[0]
        # Valid JSON, not Python dict repr
        parsed = _json.loads(cell)
        assert parsed == {"id": 1, "vals": ["S4995"]}

    @patch("pg_export_import._connect")
    @patch("pg_export_import._fetch_column_type_info", return_value={})
    def test_null_written_as_empty_field(self, _mock_type_info, mock_connect, tmp_path):
        columns = ["id", "optional"]
        rows = [(1, None)]
        csv_file = str(tmp_path / "null_out.csv")

        src_cur = MagicMock()
        src_cur.__enter__ = lambda s: s
        src_cur.__exit__ = MagicMock(return_value=False)
        desc = []
        for col in columns:
            d = MagicMock()
            d.name = col
            d.type_code = 25
            desc.append(d)
        src_cur.description = desc
        src_cur.fetchmany.side_effect = [[rows[0]], []]
        src_cur.rowcount = 1

        src_conn = MagicMock()
        src_conn.__enter__ = lambda s: s
        src_conn.__exit__ = MagicMock(return_value=False)
        src_conn.cursor.return_value = src_cur

        tgt_cur = MagicMock()
        tgt_cur.__enter__ = lambda s: s
        tgt_cur.__exit__ = MagicMock(return_value=False)
        copy_mock = MagicMock()
        copy_mock.__enter__ = lambda s: s
        copy_mock.__exit__ = MagicMock(return_value=False)
        tgt_cur.copy.return_value = copy_mock
        tgt_cur.rowcount = 1
        tgt_conn = MagicMock()
        tgt_conn.__enter__ = lambda s: s
        tgt_conn.__exit__ = MagicMock(return_value=False)
        tgt_conn.cursor.return_value = tgt_cur

        mock_connect.side_effect = [src_conn, tgt_conn]

        result = export_and_import(
            source_config=ConnectionConfig("h", "db", "u", "p"),
            target_config=ConnectionConfig("h", "db", "u", "p"),
            source_table="tbl",
            target_table="tbl",
            where_clause="",
            csv_path=csv_file,
        )

        assert result.status == "success"
        content = Path(csv_file).read_text()
        # NULL should be an empty CSV field, not the string "None"
        assert "None" not in content
        assert "1," in content  # id=1, optional=empty


# ==========================================================================
# Security tests
# ==========================================================================


class TestSecurityInjectionAttempts:
    """Ensure common SQL injection payloads are rejected by identifier validation."""

    @pytest.mark.parametrize(
        "payload",
        [
            "'; DROP TABLE users; --",
            "users UNION SELECT * FROM passwords",
            "users; SELECT 1",
            "1=1",
            "users/**/WHERE/**/1=1",
            "users\x00",
            "users\n",
            "users\t",
            "",
            "a" * 64,
        ],
    )
    def test_injection_via_table_name_rejected(self, payload, tmp_path):
        with pytest.raises((ValueError, Exception)):
            export_and_import(
                source_config=ConnectionConfig("h", "db", "u", "p"),
                target_config=ConnectionConfig("h", "db", "u", "p"),
                source_table=payload,
                target_table="safe_table",
                where_clause="",
                csv_path=str(tmp_path / "x.csv"),
            )

    @pytest.mark.parametrize(
        "payload",
        [
            "safe_source",
            "public.orders",
            "_private_table",
            "MyTable123",
        ],
    )
    def test_valid_identifiers_pass_validation(self, payload):
        schema, table = _parse_table_ref(payload)
        # If we get here without exception, validation passed
        assert table


# ==========================================================================
# Integration tests (require real PostgreSQL)
# ==========================================================================

PG_TEST_HOST = os.environ.get("PG_TEST_HOST", "localhost")
PG_TEST_PORT = int(os.environ.get("PG_TEST_PORT", "5432"))
PG_TEST_DB = os.environ.get("PG_TEST_DB", "pg_export_test")
PG_TEST_USER = os.environ.get("PG_TEST_USER", "postgres")
PG_TEST_PASSWORD = os.environ.get("PG_TEST_PASSWORD", "postgres")


def _test_cfg() -> ConnectionConfig:
    return ConnectionConfig(
        host=PG_TEST_HOST,
        port=PG_TEST_PORT,
        dbname=PG_TEST_DB,
        user=PG_TEST_USER,
        password=PG_TEST_PASSWORD,
    )


@pytest.fixture(scope="module")
def pg_conn():
    """Module-scoped connection for integration test setup/teardown."""
    import psycopg

    pytest.importorskip("psycopg")
    try:
        conn = psycopg.connect(
            host=PG_TEST_HOST,
            port=PG_TEST_PORT,
            dbname=PG_TEST_DB,
            user=PG_TEST_USER,
            password=PG_TEST_PASSWORD,
        )
        conn.autocommit = True
        yield conn
        conn.close()
    except Exception as e:
        pytest.skip(f"PostgreSQL not available: {e}")


@pytest.fixture(autouse=True)
def _cleanup_tables(request):
    """Drop test tables after each integration test."""
    if "integration" not in request.keywords:
        yield
        return
    # Only request pg_conn for integration tests (avoids skipping unit tests).
    pg_conn = request.getfixturevalue("pg_conn")
    yield
    pg_conn.execute("DROP TABLE IF EXISTS export_src, import_dst, schema_src, schema_dst")


@pytest.mark.integration
class TestIntegration:
    def test_full_round_trip(self, pg_conn, tmp_path):
        pg_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS export_src (
                id     SERIAL PRIMARY KEY,
                name   TEXT NOT NULL,
                active BOOLEAN NOT NULL DEFAULT TRUE
            )
            """
        )
        pg_conn.execute("TRUNCATE export_src RESTART IDENTITY")
        pg_conn.execute(
            "INSERT INTO export_src (name, active) VALUES "
            "('Alice', TRUE), ('Bob', FALSE), ('Carol', TRUE)"
        )

        pg_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS import_dst (
                id     INTEGER,
                name   TEXT,
                active BOOLEAN
            )
            """
        )
        pg_conn.execute("TRUNCATE import_dst")

        cfg = _test_cfg()
        result = export_and_import(
            source_config=cfg,
            target_config=cfg,
            source_table="export_src",
            target_table="import_dst",
            where_clause="active = %s",
            where_params=(True,),
            csv_path=str(tmp_path / "rt.csv"),
        )

        assert result.status == "success"
        assert result.exported_count == 2  # Alice and Carol
        assert result.imported_count == 2

        rows = pg_conn.execute("SELECT name FROM import_dst ORDER BY name").fetchall()
        names = [r[0] for r in rows]
        assert names == ["Alice", "Carol"]

        os.remove(result.csv_path)

    def test_zero_rows(self, pg_conn, tmp_path):
        pg_conn.execute(
            "CREATE TABLE IF NOT EXISTS export_src (id INTEGER, val TEXT)"
        )
        pg_conn.execute("TRUNCATE export_src")

        cfg = _test_cfg()
        result = export_and_import(
            source_config=cfg,
            target_config=cfg,
            source_table="export_src",
            target_table="export_src",  # same table, zero-row export is a no-op
            where_clause="id = %s",
            where_params=(9999,),
            csv_path=str(tmp_path / "zero.csv"),
        )

        assert result.status == "success"
        assert result.exported_count == 0
        assert result.imported_count == 0

    def test_export_all_rows_no_where(self, pg_conn, tmp_path):
        pg_conn.execute(
            "CREATE TABLE IF NOT EXISTS export_src (id INTEGER)"
        )
        pg_conn.execute("TRUNCATE export_src")
        pg_conn.execute(
            "INSERT INTO export_src VALUES (1), (2), (3)"
        )
        pg_conn.execute(
            "CREATE TABLE IF NOT EXISTS import_dst (id INTEGER)"
        )
        pg_conn.execute("TRUNCATE import_dst")

        cfg = _test_cfg()
        result = export_and_import(
            source_config=cfg,
            target_config=cfg,
            source_table="export_src",
            target_table="import_dst",
            where_clause="",
            csv_path=str(tmp_path / "all.csv"),
        )

        assert result.status == "success"
        assert result.exported_count == 3

        os.remove(result.csv_path)

    def test_large_batch(self, pg_conn, tmp_path):
        pg_conn.execute(
            "CREATE TABLE IF NOT EXISTS export_src (id INTEGER)"
        )
        pg_conn.execute("TRUNCATE export_src")
        pg_conn.execute(
            "INSERT INTO export_src SELECT g FROM generate_series(1, 12000) g"
        )
        pg_conn.execute(
            "CREATE TABLE IF NOT EXISTS import_dst (id INTEGER)"
        )
        pg_conn.execute("TRUNCATE import_dst")

        cfg = _test_cfg()
        result = export_and_import(
            source_config=cfg,
            target_config=cfg,
            source_table="export_src",
            target_table="import_dst",
            where_clause="",
            fetch_size=1000,
            csv_path=str(tmp_path / "large.csv"),
        )

        assert result.status == "success"
        assert result.exported_count == 12000
        assert result.imported_count == 12000

        os.remove(result.csv_path)

    def test_type_mismatch_returns_import_failed(self, pg_conn, tmp_path):
        pg_conn.execute(
            "CREATE TABLE IF NOT EXISTS export_src (id INTEGER, val TEXT)"
        )
        pg_conn.execute("TRUNCATE export_src")
        pg_conn.execute("INSERT INTO export_src VALUES (1, 'not_a_date')")

        pg_conn.execute(
            "CREATE TABLE IF NOT EXISTS import_dst (id INTEGER, val DATE)"
        )
        pg_conn.execute("TRUNCATE import_dst")

        cfg = _test_cfg()
        result = export_and_import(
            source_config=cfg,
            target_config=cfg,
            source_table="export_src",
            target_table="import_dst",
            where_clause="",
            csv_path=str(tmp_path / "type_err.csv"),
        )

        assert result.status == "import_failed"
        assert result.error is not None
        # CSV was preserved
        assert Path(result.csv_path).exists()

    def test_schema_qualified_tables(self, pg_conn, tmp_path):
        pg_conn.execute("CREATE SCHEMA IF NOT EXISTS test_src_schema")
        pg_conn.execute("CREATE SCHEMA IF NOT EXISTS test_dst_schema")
        pg_conn.execute(
            "CREATE TABLE IF NOT EXISTS test_src_schema.schema_src (id INTEGER, label TEXT)"
        )
        pg_conn.execute("TRUNCATE test_src_schema.schema_src")
        pg_conn.execute(
            "INSERT INTO test_src_schema.schema_src VALUES (1, 'hello'), (2, 'world')"
        )
        pg_conn.execute(
            "CREATE TABLE IF NOT EXISTS test_dst_schema.schema_dst (id INTEGER, label TEXT)"
        )
        pg_conn.execute("TRUNCATE test_dst_schema.schema_dst")

        cfg = _test_cfg()
        result = export_and_import(
            source_config=cfg,
            target_config=cfg,
            source_table="test_src_schema.schema_src",
            target_table="test_dst_schema.schema_dst",
            where_clause="",
            csv_path=str(tmp_path / "schema.csv"),
        )

        assert result.status == "success"
        assert result.exported_count == 2

        rows = pg_conn.execute(
            "SELECT label FROM test_dst_schema.schema_dst ORDER BY label"
        ).fetchall()
        assert [r[0] for r in rows] == ["hello", "world"]

        os.remove(result.csv_path)
        pg_conn.execute("DROP TABLE IF EXISTS test_src_schema.schema_src, test_dst_schema.schema_dst")
        pg_conn.execute("DROP SCHEMA IF EXISTS test_src_schema, test_dst_schema")

    def test_array_and_jsonb_round_trip(self, pg_conn, tmp_path):
        """text[] and jsonb columns must survive export→CSV→COPY without corruption."""
        pg_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS export_src (
                id    SERIAL PRIMARY KEY,
                tags  TEXT[],
                meta  JSONB
            )
            """
        )
        pg_conn.execute("TRUNCATE export_src RESTART IDENTITY")
        pg_conn.execute(
            "INSERT INTO export_src (tags, meta) VALUES "
            "(%s, %s), (%s, %s), (%s, %s)",
            (
                ["S4995", "X1"], '{"score": 1, "labels": ["a"]}',
                ["alpha"], '{"score": 2}',
                None, None,
            ),
        )

        pg_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS import_dst (
                id    INTEGER,
                tags  TEXT[],
                meta  JSONB
            )
            """
        )
        pg_conn.execute("TRUNCATE import_dst")

        cfg = _test_cfg()
        result = export_and_import(
            source_config=cfg,
            target_config=cfg,
            source_table="export_src",
            target_table="import_dst",
            where_clause="",
            csv_path=str(tmp_path / "arr_json.csv"),
        )

        assert result.status == "success", result.error
        assert result.exported_count == 3
        assert result.imported_count == 3

        rows = pg_conn.execute(
            "SELECT tags, meta FROM import_dst ORDER BY id"
        ).fetchall()

        # Row 0: array and jsonb intact
        assert rows[0][0] == ["S4995", "X1"]
        assert rows[0][1] == {"score": 1, "labels": ["a"]}

        # Row 1: single-element array and simple jsonb intact
        assert rows[1][0] == ["alpha"]
        assert rows[1][1] == {"score": 2}

        # Row 2: both NULL → NULL (not empty string)
        assert rows[2][0] is None
        assert rows[2][1] is None

        os.remove(result.csv_path)
