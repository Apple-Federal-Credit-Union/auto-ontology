# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES.
# All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Fabric connector, against a fake pyodbc.

The ``fabric`` extra is optional and CI does not install it, so every unit
test swaps in a fake ``pyodbc`` module. The live test at the bottom runs only
when ``AUTO_ONTOLOGY_FABRIC_TEST_URL`` names a real endpoint.
"""

from __future__ import annotations

import logging
import os
import struct
import sys
from types import SimpleNamespace
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

import pandas as pd
import pytest
from pytest import LogCaptureFixture, MonkeyPatch
from sqlglot.dialects import DIALECTS

from auto_ontology.connectors.base import StatementTimeout
from auto_ontology.connectors.connection_string_factory import build_connection_string
from auto_ontology.connectors.db_errors import is_infrastructure_error
from auto_ontology.connectors.fabric import (
    SQL_COPT_SS_ACCESS_TOKEN,
    TOKEN_SCOPE,
    FabricDatabase,
    ReadOnlyViolation,
    _odbc_connection_string,
    _parse_connection_string,
    _resolve_dialect,
    _token_struct,
    prepare_read_only,
)
from auto_ontology.connectors.registry import CONNECTOR_REGISTRY, create_connector

HOST = "workspace.datawarehouse.fabric.microsoft.com"
URL = f"fabric://client-id:s3cret@{HOST}/sales_wh?tenant_id=tenant-id"


# ----------------------------------------------------------------------
# Fake driver
# ----------------------------------------------------------------------


class FakeError(Exception):
    """Stands in for ``pyodbc.Error``: SQLSTATE first, then the message."""


Result = tuple[list[str], list[tuple[Any, ...]]]
Responder = Callable[[str], Result]


class FakeCursor:
    def __init__(self, connection: "FakeConnection", timeout: int) -> None:
        self.connection = connection
        self.timeout = timeout
        self.description: list[tuple[str]] | None = None
        self._rows: list[tuple[Any, ...]] = []
        self.closed = False

    def execute(self, sql: str, parameters: Any = None) -> None:
        self.connection.driver.executed.append((sql, parameters, self.timeout))
        columns, rows = self.connection.driver.respond(sql)
        self.description = [(name,) for name in columns] if columns else None
        self._rows = rows

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows

    def close(self) -> None:
        self.closed = True


class FakeConnection:
    def __init__(self, driver: "FakeDriver") -> None:
        self.driver = driver
        self.timeout = 0
        self.closed = False

    def cursor(self) -> FakeCursor:
        return FakeCursor(self, self.timeout)

    def close(self) -> None:
        self.closed = True


class FakeDriver:
    """Records every connect and statement; answers via *respond*."""

    def __init__(self) -> None:
        self.connects: list[tuple[str, dict[str, Any]]] = []
        self.connections: list[FakeConnection] = []
        self.executed: list[tuple[str, Any, int]] = []
        self.respond: Responder = lambda _sql: (["value"], [(1,)])
        self.module = SimpleNamespace(Error=FakeError, connect=self.connect)

    def connect(self, connection_string: str, **kwargs: Any) -> FakeConnection:
        self.connects.append((connection_string, kwargs))
        connection = FakeConnection(self)
        self.connections.append(connection)
        return connection


@pytest.fixture
def driver(monkeypatch: MonkeyPatch) -> FakeDriver:
    fake = FakeDriver()
    monkeypatch.setitem(sys.modules, "pyodbc", fake.module)
    return fake


def _answer(mapping: dict[str, Result | Exception]) -> Responder:
    """Answer by the first mapping key found in the statement."""

    def respond(sql: str) -> Result:
        for needle, result in mapping.items():
            if needle in sql:
                if isinstance(result, Exception):
                    raise result
                return result
        raise AssertionError(f"unexpected statement: {sql}")

    return respond


# ----------------------------------------------------------------------
# Connection string
# ----------------------------------------------------------------------


def test_parse_connection_string() -> None:
    settings = _parse_connection_string(
        f"fabric://client%40id:se%2Fcr%3Bet@{HOST}/sales%20wh?tenant_id=tenant-id"
    )

    assert settings == {
        "host": HOST,
        "port": 1433,
        "database": "sales wh",
        "client_id": "client@id",
        "secret": "se/cr;et",
        "tenant_id": "tenant-id",
        "auth": "service_principal",
        "driver": "ODBC Driver 18 for SQL Server",
        "login_timeout": None,
    }


def test_parse_connection_string_overrides() -> None:
    settings = _parse_connection_string(
        f"fabric://c:s@{HOST}:1444/db?tenant_id=t&auth=TOKEN"
        "&driver=ODBC%20Driver%2017%20for%20SQL%20Server&login_timeout=5"
    )

    assert settings["port"] == 1444
    assert settings["auth"] == "token"
    assert settings["driver"] == "ODBC Driver 17 for SQL Server"
    assert settings["login_timeout"] == 5


@pytest.mark.parametrize(
    ("url", "message"),
    [
        (f"mssql://c:s@{HOST}/db", "Not a Fabric"),
        ("fabric://c:s@/db", "missing the SQL endpoint host"),
        (f"fabric://c@{HOST}/db", "requires a service principal"),
        (f"fabric://{HOST}/db", "requires a service principal"),
        (f"fabric://c:s@{HOST}/", "warehouse or lakehouse name"),
        (f"fabric://c:s@{HOST}/db?auth=password", "Unsupported Fabric auth"),
        (f"fabric://c:s@{HOST}/db?auth=token", "requires tenant_id"),
    ],
)
def test_parse_connection_string_rejects(url: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        _parse_connection_string(url)


def test_odbc_string_for_service_principal() -> None:
    odbc = _odbc_connection_string(
        _parse_connection_string(f"fabric://client:se%7Dcr%3Bet@{HOST}/db")
    )

    assert odbc == (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server=tcp:{HOST},1433;"
        "Database={db};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Authentication=ActiveDirectoryServicePrincipal;"
        "UID={client};"
        # ``}`` doubled and the value braced, so ``;`` cannot end it early.
        "PWD={se}}cr;et};"
    )


def test_odbc_string_for_token_carries_no_credentials() -> None:
    odbc = _odbc_connection_string(
        _parse_connection_string(
            f"fabric://client:secret@{HOST}/db?auth=token&tenant_id=t"
        )
    )

    assert "Encrypt=yes" in odbc
    assert "TrustServerCertificate=no" in odbc
    assert "Authentication" not in odbc
    assert "UID" not in odbc
    assert "secret" not in odbc


def test_build_connection_string() -> None:
    url = build_connection_string(
        {
            "type": "fabric",
            "host": f"tcp:{HOST},1433",
            "user": "client@id",
            "password": "se/cret",
            "database": "sales wh",
            "tenant_id": "tenant-id",
        }
    )

    assert url == (
        f"fabric://client%40id:se%2Fcret@{HOST}:1433/sales%20wh?tenant_id=tenant-id"
    )
    settings = _parse_connection_string(url)
    assert (settings["client_id"], settings["secret"]) == ("client@id", "se/cret")
    assert settings["auth"] == "service_principal"


def test_build_connection_string_strips_a_pasted_url() -> None:
    url = build_connection_string(
        {
            "type": "fabric",
            "host": f"https://{HOST}/",
            "user": "c",
            "password": "s",
            "database": "db",
        }
    )

    assert url == f"fabric://c:s@{HOST}:1433/db"


def test_build_connection_string_strips_a_pasted_url_with_port() -> None:
    """A pasted endpoint with an embedded port must not double up with ours.

    ``https://<host>:1433/`` -- the literal shape in the portal's SQL
    connection string -- previously left the port glued to the host, so
    appending the default produced an unparseable ``host:1433:1433``.
    """
    url = build_connection_string(
        {
            "type": "fabric",
            "host": f"https://{HOST}:1433/",
            "user": "c",
            "password": "s",
            "database": "db",
        }
    )

    assert url == f"fabric://c:s@{HOST}:1433/db"
    settings = _parse_connection_string(url)
    assert settings["host"] == HOST
    assert settings["port"] == 1433


def test_build_connection_string_strips_a_pasted_url_with_port_and_path() -> None:
    """A non-default embedded port is kept, and a trailing path is dropped."""
    url = build_connection_string(
        {
            "type": "fabric",
            "host": f"https://{HOST}:1444/some/path",
            "user": "c",
            "password": "s",
            "database": "db",
        }
    )

    assert url == f"fabric://c:s@{HOST}:1444/db"


@pytest.mark.parametrize("flag", [True, "true"])
def test_build_connection_string_token_auth(flag: object) -> None:
    url = build_connection_string(
        {
            "type": "fabric",
            "host": HOST,
            "user": "c",
            "password": "s",
            "database": "db",
            "tenant_id": "t",
            "token_auth": flag,
        }
    )

    assert parse_qs(urlparse(url).query) == {"auth": ["token"], "tenant_id": ["t"]}


def test_build_connection_string_token_auth_needs_a_tenant() -> None:
    with pytest.raises(ValueError, match="tenant_id"):
        build_connection_string(
            {
                "type": "fabric",
                "host": HOST,
                "user": "c",
                "password": "s",
                "database": "db",
                "token_auth": True,
            }
        )


# ----------------------------------------------------------------------
# Registry and dialect
# ----------------------------------------------------------------------


def test_registry_entry() -> None:
    assert CONNECTOR_REGISTRY["fabric"] is FabricDatabase


def test_create_connector_is_lazy_and_takes_schemas(driver: FakeDriver) -> None:
    database = create_connector(URL, schemas=["Sales"])

    assert isinstance(database, FabricDatabase)
    assert database.database_name == "sales_wh"
    # Building every configured connector at startup must not dial Fabric.
    assert driver.connects == []


def test_dialect_is_fabric_when_sqlglot_has_it() -> None:
    assert _resolve_dialect(DIALECTS) == "fabric"
    assert FabricDatabase.dialect.fget(None) == "fabric"  # type: ignore[attr-defined]


def test_dialect_falls_back_to_tsql() -> None:
    assert _resolve_dialect(["TSQL", "Postgres"]) == "tsql"


def test_qualify_quotes_for_tsql(driver: FakeDriver) -> None:
    assert FabricDatabase(URL).qualify("dbo", "order") == "[dbo].[order]"


# ----------------------------------------------------------------------
# Execution
# ----------------------------------------------------------------------


def test_execute_returns_a_dataframe(driver: FakeDriver) -> None:
    database = FabricDatabase(URL)

    frame = database.execute("SELECT ? AS value", [1])

    assert frame.to_dict(orient="records") == [{"value": 1}]
    assert driver.executed == [("SELECT ? AS value", [1], 0)]
    odbc, kwargs = driver.connects[0]
    assert "Authentication=ActiveDirectoryServicePrincipal" in odbc
    assert kwargs == {"autocommit": True, "timeout": 30}


def test_execute_reuses_one_connection(driver: FakeDriver) -> None:
    database = FabricDatabase(URL)

    database.execute("SELECT 1 AS value")
    database.execute("SELECT 2 AS value")
    database.close()

    assert len(driver.connects) == 1
    assert driver.connections[0].closed


def test_timeout_applies_to_one_statement_only(driver: FakeDriver) -> None:
    database = FabricDatabase(URL)

    database.execute("SELECT 1 AS value", timeout_s=2.5)
    database.execute("SELECT 1 AS value")

    assert [timeout for _, _, timeout in driver.executed] == [3, 0]
    assert driver.connections[0].timeout == 0


def test_driver_timeout_maps_to_statement_timeout(driver: FakeDriver) -> None:
    driver.respond = _answer(
        {
            "SELECT": FakeError(
                "HYT00", "[Microsoft][ODBC Driver 18] Query timeout expired"
            )
        }
    )
    database = FabricDatabase(URL)

    with pytest.raises(StatementTimeout) as caught:
        database.execute("SELECT 1 AS value", timeout_s=5)

    assert caught.value.timeout_s == 5
    assert isinstance(caught.value.__cause__, FakeError)


def test_driver_timeout_without_a_cap_is_not_relabelled(driver: FakeDriver) -> None:
    driver.respond = _answer({"SELECT": FakeError("HYT00", "Query timeout expired")})

    with pytest.raises(FakeError):
        FabricDatabase(URL).execute("SELECT 1 AS value")


def test_login_timeout_is_not_a_statement_timeout(
    driver: FakeDriver, monkeypatch: MonkeyPatch
) -> None:
    # The real driver reports a login that timed out with the same HYT00.
    def connect(_connection_string: str, **_kwargs: Any) -> FakeConnection:
        raise FakeError("HYT00", "[Microsoft] Login timeout expired")

    monkeypatch.setattr(driver.module, "connect", connect)

    with pytest.raises(FakeError, match="Login timeout"):
        FabricDatabase(URL).execute("SELECT 1 AS value", timeout_s=5)


def test_other_driver_errors_propagate(driver: FakeDriver) -> None:
    driver.respond = _answer({"SELECT": FakeError("42S02", "Invalid object name 'x'")})

    with pytest.raises(FakeError, match="Invalid object name"):
        FabricDatabase(URL).execute("SELECT * FROM x", timeout_s=5)


def test_dropped_connection_is_reopened_once(driver: FakeDriver) -> None:
    failures = [FakeError("08S01", "Communication link failure")]

    def respond(_sql: str) -> Result:
        if failures:
            raise failures.pop()
        return ["value"], [(1,)]

    driver.respond = respond
    database = FabricDatabase(URL)

    assert database.execute("SELECT 1 AS value").iloc[0]["value"] == 1
    assert len(driver.connects) == 2
    assert driver.connections[0].closed


def test_microsoft_driver_connectivity_errors_are_infrastructure() -> None:
    assert is_infrastructure_error(
        "('08S01', '[Microsoft][ODBC Driver 18 for SQL Server]Communication link "
        "failure (0) (SQLExecDirectW)')"
    )
    assert is_infrastructure_error("('HYT00', '[Microsoft] Login timeout expired')")
    assert not is_infrastructure_error("('HYT00', '[Microsoft] Query timeout expired')")


def test_missing_driver_names_the_extra(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "pyodbc", None)

    with pytest.raises(ImportError, match="uv sync --extra fabric"):
        FabricDatabase(URL).execute("SELECT 1")


# ----------------------------------------------------------------------
# Read-only guard
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO dbo.t VALUES (1)",
        "UPDATE dbo.t SET a = 1",
        "DELETE FROM dbo.t",
        "MERGE INTO dbo.t USING dbo.s ON t.a = s.a WHEN MATCHED THEN DELETE;",
        "DROP TABLE dbo.t",
        "CREATE TABLE dbo.t (a INT)",
        "ALTER TABLE dbo.t ADD b INT",
        "TRUNCATE TABLE dbo.t",
        "SELECT a INTO dbo.copy FROM dbo.t",
        "WITH x AS (SELECT 1 AS a) INSERT INTO dbo.t SELECT a FROM x",
        "EXEC sp_who",
        "SELECT 1; DROP TABLE dbo.t",
        "SET NOCOUNT ON",
        "DECLARE @x INT",
        "SELECT * FROM OPENROWSET(BULK 'x') AS r",
        "",
        "   ",
    ],
)
def test_non_read_statements_are_refused_before_sending(
    driver: FakeDriver, sql: str
) -> None:
    with pytest.raises(ReadOnlyViolation):
        FabricDatabase(URL).execute(sql)

    assert driver.connects == []
    assert driver.executed == []


def test_read_only_violation_is_not_infrastructure() -> None:
    with pytest.raises(ReadOnlyViolation) as caught:
        prepare_read_only("DROP TABLE dbo.t")

    assert not is_infrastructure_error(caught.value, "DROP TABLE dbo.t")


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT TOP 5 [a] FROM [dbo].[t]",
        "WITH x AS (SELECT 1 AS a) SELECT a FROM x",
        "SELECT 1 AS a UNION ALL SELECT 2",
        "SELECT a FROM t ORDER BY a OFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY",
        "SELECT 'insert into t' AS note",
        "SELECT 1;",
    ],
)
def test_read_queries_are_sent_unchanged(sql: str) -> None:
    assert prepare_read_only(sql) == sql


def test_limit_is_rewritten_to_top(driver: FakeDriver) -> None:
    FabricDatabase(URL).execute('SELECT * FROM "dbo"."t" LIMIT 1000')

    assert driver.executed[0][0] == "SELECT TOP 1000 * FROM [dbo].[t]"


def test_limit_rewrite_keeps_placeholders() -> None:
    assert (
        prepare_read_only("SELECT a FROM t WHERE b = ? LIMIT 1")
        == "SELECT TOP 1 a FROM t WHERE b = ?"
    )


def test_parameterized_limit_is_refused_rather_than_misbound(
    driver: FakeDriver,
) -> None:
    """A ``?`` LIMIT would move ahead of other placeholders once rewritten to TOP.

    ``execute()`` passes the caller's parameter list positionally against
    whatever SQL is actually sent, so silently rewriting ``... WHERE b = ?
    LIMIT ?`` to ``SELECT TOP (?) ... WHERE b = ?`` would bind the LIMIT value
    to ``b`` and vice versa. Refusing it outright is safer than running a
    statement with swapped parameters.
    """
    with pytest.raises(ReadOnlyViolation, match="parameterized LIMIT"):
        prepare_read_only("SELECT a FROM t WHERE b = ? LIMIT ?")

    with pytest.raises(ReadOnlyViolation, match="parameterized LIMIT"):
        FabricDatabase(URL).execute(
            "SELECT a FROM t WHERE b = ? LIMIT ?", parameters=["x", 10]
        )
    assert driver.executed == []


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM (SELECT a FROM t LIMIT ?) x LIMIT 5",
        "SELECT * FROM (SELECT a FROM t WHERE id = ? LIMIT ?) AS s LIMIT 10",
        "WITH c AS (SELECT a FROM t WHERE b = ? LIMIT ?) SELECT a FROM c LIMIT 1",
    ],
    ids=["nested", "nested-after-predicate", "cte"],
)
def test_every_parameterized_limit_is_refused_not_just_the_first(sql: str) -> None:
    """Rendering rewrites every LIMIT in the tree, so every one is checked."""
    with pytest.raises(ReadOnlyViolation, match="parameterized LIMIT"):
        prepare_read_only(sql)


def test_nested_literal_limits_are_all_rewritten() -> None:
    assert (
        prepare_read_only(
            "SELECT * FROM (SELECT a FROM t WHERE b = ? LIMIT 3) x LIMIT 5"
        )
        == "SELECT TOP 5 * FROM (SELECT TOP 3 a AS a FROM t WHERE b = ?) AS x"
    )


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT TOP (?) a FROM dbo.t WHERE b = ?",
        "SELECT * FROM (SELECT TOP (?) a FROM t WHERE b = ?) AS x",
    ],
)
def test_native_parameterized_top_is_sent_unchanged(
    driver: FakeDriver, sql: str
) -> None:
    """``TOP (?)`` needs no rewrite, so its placeholders are already in order."""
    FabricDatabase(URL).execute(sql, parameters=[5, "x"])

    assert driver.executed == [(sql, [5, "x"], 0)]


def test_native_parameterized_top_beside_a_limit_is_refused() -> None:
    """Once a LIMIT forces a re-render, a ``TOP (?)`` can't be told apart from it."""
    with pytest.raises(ReadOnlyViolation, match="parameterized LIMIT"):
        prepare_read_only("SELECT TOP (?) a FROM (SELECT a FROM t LIMIT 5) AS x")


# ----------------------------------------------------------------------
# Metadata
# ----------------------------------------------------------------------


def test_get_tables_shape_and_system_schemas(driver: FakeDriver) -> None:
    driver.respond = _answer(
        {
            "INFORMATION_SCHEMA.TABLES": (
                ["table_schema", "table_name", "table_type"],
                [("dbo", "orders", "base table"), ("sales", "v_orders", "view")],
            )
        }
    )

    frame = FabricDatabase(URL).get_tables()

    assert list(frame.columns) == ["table_schema", "table_name", "table_type"]
    assert frame.to_dict(orient="records")[1] == {
        "table_schema": "sales",
        "table_name": "v_orders",
        "table_type": "view",
    }
    sql = driver.executed[0][0]
    for schema in ("'sys'", "'INFORMATION_SCHEMA'", "'queryinsights'"):
        assert schema in sql


SCHEMA_LIST = "SELECT name FROM sys.schemas"


def _tables_in(*schemas: str) -> Responder:
    """Answer the schema list and one table per schema, named after its schema."""
    return _answer(
        {
            SCHEMA_LIST: (["name"], [(schema,) for schema in schemas]),
            "INFORMATION_SCHEMA.TABLES": (
                ["table_schema", "table_name", "table_type"],
                [(schema, f"t_{schema}", "base table") for schema in schemas],
            ),
        }
    )


def test_schema_allowlist_falls_back_to_case_insensitive(driver: FakeDriver) -> None:
    driver.respond = _tables_in("dbo", "Sales")

    frame = FabricDatabase(URL, schemas=["sales"]).get_tables()

    assert frame["table_schema"].tolist() == ["Sales"]
    assert frame["table_name"].tolist() == ["t_Sales"]


def test_schema_allowlist_exact_name_excludes_case_variants(
    driver: FakeDriver,
) -> None:
    """``Sales`` and ``sales`` are distinct schemas under a case-sensitive collation."""
    driver.respond = _tables_in("dbo", "Sales", "sales")

    assert FabricDatabase(URL, schemas=["Sales"]).get_tables()[
        "table_schema"
    ].tolist() == ["Sales"]
    assert FabricDatabase(URL, schemas=["sales"]).get_tables()[
        "table_schema"
    ].tolist() == ["sales"]


def test_schema_allowlist_resolves_against_every_schema_not_one_read(
    driver: FakeDriver,
) -> None:
    """A read holding only ``sales`` rows must not satisfy an allowlist of ``Sales``."""
    driver.respond = _answer(
        {
            SCHEMA_LIST: (["name"], [("Sales",), ("sales",)]),
            "sys.views": (
                ["table_schema", "table_name", "view_definition"],
                [("sales", "v", "CREATE VIEW sales.v AS SELECT 1 AS a")],
            ),
        }
    )

    assert FabricDatabase(URL, schemas=["Sales"]).get_views().empty


def test_get_columns_reports_boolean_nullability(driver: FakeDriver) -> None:
    driver.respond = _answer(
        {
            "INFORMATION_SCHEMA.COLUMNS": (
                [
                    "table_schema",
                    "table_name",
                    "column_name",
                    "data_type",
                    "is_nullable",
                    "ordinal_position",
                ],
                [
                    ("dbo", "orders", "id", "int", False, 1),
                    ("dbo", "orders", "note", "varchar", True, 2),
                ],
            )
        }
    )

    frame = FabricDatabase(URL).get_columns()

    assert list(frame.columns) == [
        "table_schema",
        "table_name",
        "column_name",
        "data_type",
        "is_nullable",
        "ordinal_position",
    ]
    assert frame["is_nullable"].tolist() == [False, True]
    assert all(isinstance(v, bool) for v in frame["is_nullable"])


def test_get_views_shape(driver: FakeDriver) -> None:
    driver.respond = _answer(
        {
            "sys.views": (
                ["table_schema", "table_name", "view_definition"],
                [("dbo", "v", "CREATE VIEW dbo.v AS SELECT 1 AS a")],
            )
        }
    )

    frame = FabricDatabase(URL).get_views()

    assert list(frame.columns) == ["table_schema", "table_name", "view_definition"]
    assert len(frame) == 1


def test_get_pks_and_fks_shapes(driver: FakeDriver) -> None:
    driver.respond = _answer(
        {
            "sys.key_constraints": (
                ["table_schema", "table_name", "column_name", "ordinal_position"],
                [("dbo", "orders", "id", 1)],
            ),
            "sys.foreign_keys": (
                [
                    "table_schema",
                    "table_name",
                    "column_name",
                    "referenced_schema",
                    "referenced_table",
                    "referenced_column",
                ],
                [("dbo", "lines", "order_id", "dbo", "orders", "id")],
            ),
        }
    )
    database = FabricDatabase(URL)

    pks = database.get_pks()
    fks = database.get_fks()

    assert pks.to_dict(orient="records") == [
        {
            "table_schema": "dbo",
            "table_name": "orders",
            "column_name": "id",
            "ordinal_position": 1,
        }
    ]
    assert fks.iloc[0]["referenced_table"] == "orders"
    assert list(fks.columns) == [
        "table_schema",
        "table_name",
        "column_name",
        "referenced_schema",
        "referenced_table",
        "referenced_column",
    ]


@pytest.mark.parametrize(
    "outcome",
    [([], []), FakeError("42S02", "Invalid object name 'sys.key_constraints'")],
    ids=["no-constraints", "unsupported"],
)
def test_pks_and_fks_are_empty_frames_where_unsupported(
    driver: FakeDriver, outcome: Result | Exception
) -> None:
    driver.respond = _answer(
        {"sys.key_constraints": outcome, "sys.foreign_keys": outcome}
    )
    database = FabricDatabase(URL)

    pks = database.get_pks()
    fks = database.get_fks()

    assert pks.empty and list(pks.columns) == [
        "table_schema",
        "table_name",
        "column_name",
        "ordinal_position",
    ]
    assert fks.empty and len(fks.columns) == 6


@pytest.mark.parametrize(
    "error",
    [
        FakeError("08S01", "[Microsoft][ODBC Driver 18]Communication link failure"),
        FakeError("HYT00", "[Microsoft] Login timeout expired"),
    ],
    ids=["transport-failure", "login-timeout"],
)
def test_pks_and_fks_raise_on_transport_failures(
    driver: FakeDriver, error: FakeError
) -> None:
    """A transport failure or login timeout is not "unsupported"; it must raise.

    Swallowing it into an empty frame would make ingestion report a catalog
    with no keys instead of surfacing that the endpoint could not be reached.
    """
    driver.respond = _answer({"sys.key_constraints": error, "sys.foreign_keys": error})
    database = FabricDatabase(URL)

    with pytest.raises(FakeError):
        database.get_pks()
    with pytest.raises(FakeError):
        database.get_fks()


def test_pks_raises_when_the_reconnect_retry_still_fails(driver: FakeDriver) -> None:
    """A dropped connection that still fails after :meth:`execute`'s one retry.

    ``_optional_read`` must not add a second layer of swallowing on top of
    ``execute``'s own reconnect-and-retry; once that has already given up, the
    error is exactly as real as any other read's.
    """

    def respond(sql: str) -> Result:
        if "sys.key_constraints" in sql:
            raise FakeError("08S01", "Communication link failure")
        raise AssertionError(f"unexpected statement: {sql}")

    driver.respond = respond
    database = FabricDatabase(URL)

    with pytest.raises(FakeError, match="Communication link failure"):
        database.get_pks()
    # Reconnected once (per execute()'s own retry) and gave up, not swallowed.
    assert len(driver.connects) == 2


def test_get_queries_reads_query_insights(driver: FakeDriver) -> None:
    driver.respond = _answer(
        {
            "queryinsights.exec_requests_history": (
                ["end_time", "query_text"],
                [(pd.Timestamp("2026-01-01T00:00:00"), "SELECT 1")],
            )
        }
    )

    frame = FabricDatabase(URL).get_queries(hours=6)

    assert list(frame.columns) == ["end_time", "query_text"]
    assert "DATEADD(hour, -6," in driver.executed[0][0]


def test_get_queries_is_empty_without_query_insights(
    driver: FakeDriver, caplog: LogCaptureFixture
) -> None:
    driver.respond = _answer(
        {"queryinsights": FakeError("42S02", "Invalid object name 'queryinsights'")}
    )

    with caplog.at_level(logging.INFO, logger="auto_ontology.connectors.fabric"):
        frame = FabricDatabase(URL).get_queries()

    assert frame.empty and list(frame.columns) == ["end_time", "query_text"]
    assert "queryinsights.exec_requests_history" in caplog.text


QUERY_INSIGHTS_DENIED = FakeError(
    "42000",
    "[42000] [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]The SELECT "
    "permission was denied on the object 'exec_requests_history', database "
    "'sales_wh', schema 'queryinsights'. (229) (SQLExecDirectW)",
)


@pytest.mark.parametrize(
    "error",
    [
        QUERY_INSIGHTS_DENIED,
        FakeError(
            "42000",
            "[SQL Server]The SELECT permission was denied on the column "
            "'command' of the object 'exec_requests_history'. (230)",
        ),
        FakeError("42501", "permission denied"),
    ],
    ids=["msg-229", "msg-230", "sqlstate-42501"],
)
def test_get_queries_skips_an_unreadable_query_insights(
    driver: FakeDriver, caplog: LogCaptureFixture, error: FakeError
) -> None:
    """A Viewer cannot read Query Insights; that must not fail the whole ingest."""
    driver.respond = _answer({"queryinsights": error})
    database = FabricDatabase(URL)

    with caplog.at_level(logging.DEBUG, logger="auto_ontology.connectors.fabric"):
        first = database.get_queries()
        second = database.get_queries()

    assert first.empty and list(first.columns) == ["end_time", "query_text"]
    assert second.empty
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "no permission to read" in warnings[0].getMessage()


@pytest.mark.parametrize(
    "error",
    [
        FakeError("28000", "[SQL Server]Login failed for user '<token-identified>'"),
        FakeError("08S01", "[Microsoft][ODBC Driver 18]Communication link failure"),
        FakeError("HYT00", "[Microsoft] Login timeout expired"),
        FakeError("42000", "[SQL Server]Incorrect syntax near 'FROM'. (102)"),
    ],
    ids=["login-failed", "transport-failure", "login-timeout", "other-42000"],
)
def test_get_queries_still_raises_auth_and_transport_failures(
    driver: FakeDriver, error: FakeError
) -> None:
    driver.respond = _answer({"queryinsights": error})

    with pytest.raises(FakeError):
        FabricDatabase(URL).get_queries()


def test_permission_denied_on_keys_is_not_swallowed(driver: FakeDriver) -> None:
    """An empty PK/FK frame erases stored keys, so a denial there must raise."""
    driver.respond = _answer(
        {
            "sys.key_constraints": QUERY_INSIGHTS_DENIED,
            "sys.foreign_keys": QUERY_INSIGHTS_DENIED,
        }
    )
    database = FabricDatabase(URL)

    with pytest.raises(FakeError):
        database.get_pks()
    with pytest.raises(FakeError):
        database.get_fks()


def test_metadata_errors_other_than_constraints_propagate(driver: FakeDriver) -> None:
    driver.respond = _answer({"INFORMATION_SCHEMA": FakeError("28000", "Login failed")})

    with pytest.raises(FakeError):
        FabricDatabase(URL).get_tables()


def test_get_schemas(driver: FakeDriver) -> None:
    driver.respond = _answer({"sys.schemas": (["name"], [("dbo",), ("sales",)])})

    assert FabricDatabase(URL).get_schemas() == ["dbo", "sales"]


# ----------------------------------------------------------------------
# Token authentication
# ----------------------------------------------------------------------


def test_token_struct_layout() -> None:
    packed = _token_struct("abc")

    assert struct.unpack("<I", packed[:4])[0] == 6
    assert packed[4:].decode("utf-16-le") == "abc"


def test_token_auth_passes_the_token_before_connecting(
    driver: FakeDriver, monkeypatch: MonkeyPatch
) -> None:
    credentials: list[dict[str, str]] = []
    scopes: list[str] = []

    class Credential:
        def __init__(self, **kwargs: str) -> None:
            credentials.append(kwargs)

        def get_token(self, scope: str) -> SimpleNamespace:
            scopes.append(scope)
            return SimpleNamespace(token="tok")

    identity = SimpleNamespace(ClientSecretCredential=Credential)
    monkeypatch.setitem(sys.modules, "azure", SimpleNamespace(identity=identity))
    monkeypatch.setitem(sys.modules, "azure.identity", identity)

    FabricDatabase(
        f"fabric://client:secret@{HOST}/db?auth=token&tenant_id=tenant"
    ).execute("SELECT 1 AS value")

    odbc, kwargs = driver.connects[0]
    assert "UID" not in odbc and "secret" not in odbc
    assert kwargs["attrs_before"] == {SQL_COPT_SS_ACCESS_TOKEN: _token_struct("tok")}
    assert credentials == [
        {"tenant_id": "tenant", "client_id": "client", "client_secret": "secret"}
    ]
    assert scopes == [TOKEN_SCOPE]


# ----------------------------------------------------------------------
# Live endpoint
# ----------------------------------------------------------------------


def test_live_fabric_connector_end_to_end() -> None:
    url = os.environ.get("AUTO_ONTOLOGY_FABRIC_TEST_URL")
    if not url:
        pytest.skip("Set AUTO_ONTOLOGY_FABRIC_TEST_URL to run the live Fabric test")
    pytest.importorskip("pyodbc")

    database = create_connector(url)
    try:
        assert isinstance(database, FabricDatabase)
        database.ping()
        assert database.execute("SELECT 1 AS value").iloc[0]["value"] == 1
        assert database.execute("SELECT ? AS value", ["x"]).iloc[0]["value"] == "x"

        tables = database.get_tables()
        assert set(tables.columns) == {"table_schema", "table_name", "table_type"}
        columns = database.get_columns()
        assert set(columns["table_name"]) <= set(tables["table_name"])
        assert all(isinstance(v, bool) for v in columns["is_nullable"].dropna())

        for frame in (
            database.get_views(),
            database.get_pks(),
            database.get_fks(),
            database.get_queries(hours=1),
        ):
            assert isinstance(frame, pd.DataFrame)

        if not tables.empty:
            row = tables.iloc[0]
            qualified = database.qualify(row["table_schema"], row["table_name"])
            assert len(database.execute(f"SELECT * FROM {qualified} LIMIT 1")) <= 1

        with pytest.raises(ReadOnlyViolation):
            database.execute("CREATE TABLE dbo.auto_ontology_probe (a INT)")
    finally:
        database.close()
