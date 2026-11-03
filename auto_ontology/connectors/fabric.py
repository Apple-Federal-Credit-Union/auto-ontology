# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES.
# All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Microsoft Fabric connector implementing Auto Ontology's SQLDatabase ABC.

Covers both SQL surfaces Fabric exposes over TDS: a **Warehouse** and the
read-only **SQL analytics endpoint** of a Lakehouse. Both are reached at the
workspace's ``<workspace>.datawarehouse.fabric.microsoft.com`` endpoint on
port 1433, one database per warehouse or lakehouse, through pyodbc and
Microsoft's "ODBC Driver 18 for SQL Server".

The driver and its Python bindings are optional: ``uv sync --extra fabric``
installs ``pyodbc`` and ``azure-identity``, and the ``msodbcsql18`` system
package provides the driver itself. Both are imported lazily, so a deployment
that never configures a Fabric connection needs neither.

Properties of the engine that shape this connector:

* **Entra ID only.** Fabric accepts no SQL logins. By default the ODBC driver
  signs in as a service principal itself
  (``Authentication=ActiveDirectoryServicePrincipal``). With ``auth=token`` the
  connector instead fetches the token through azure-identity and hands it to
  the driver as a pre-connect attribute, which needs ``tenant_id``.
* **Constraints are metadata only.** A Warehouse accepts primary and foreign
  keys solely as ``NOT ENFORCED``; they are still recorded in
  ``sys.key_constraints`` and ``sys.foreign_keys``, so they are read from
  there. A Lakehouse SQL endpoint has none, and those reads come back empty.
* **T-SQL.** The pipeline emits ``LIMIT n`` in its profiling and probe
  queries, which T-SQL rejects; such statements are rewritten to ``TOP n``
  through sqlglot before they are sent.
* **Read-only by design.** :meth:`FabricDatabase.execute` refuses anything but
  a single ``SELECT`` / ``WITH`` query before it reaches the endpoint. The
  grant on the endpoint remains the real boundary; this keeps a generated
  statement from ever testing it.

Example
-------
::

    CONNECTION_STRINGS=fabric://<client_id>:<secret>@<workspace>.datawarehouse.fabric.microsoft.com/<database>?tenant_id=<tenant>
"""

from __future__ import annotations

import logging
import math
import struct
import threading
import time
from typing import Any, Iterable, Optional
from urllib.parse import parse_qs, unquote, urlparse

import pandas as pd
import sqlglot
from sqlglot import exp
from sqlglot.dialects import DIALECTS
from sqlglot.errors import SqlglotError

from auto_ontology.catalog.constants import TableTypes
from auto_ontology.connectors.base import SQLDatabase, StatementTimeout

logger = logging.getLogger(__name__)

DEFAULT_PORT = 1433
DEFAULT_DRIVER = "ODBC Driver 18 for SQL Server"
DEFAULT_LOGIN_TIMEOUT_S = 30

AUTH_SERVICE_PRINCIPAL = "service_principal"
AUTH_TOKEN = "token"
_AUTH_MODES = frozenset({AUTH_SERVICE_PRINCIPAL, AUTH_TOKEN})

# Entra scope for Azure SQL-family endpoints, Fabric's included.
TOKEN_SCOPE = "https://database.windows.net/.default"
# msodbcsql's SQL_COPT_SS_ACCESS_TOKEN pre-connect attribute.
SQL_COPT_SS_ACCESS_TOKEN = 1256


def _resolve_dialect(known: Iterable[str]) -> str:
    """Fabric's own sqlglot dialect (sqlglot 30.17+), else T-SQL, its closest kin."""
    return "fabric" if "fabric" in {name.lower() for name in known} else "tsql"


# Resolved once, off the class, because the dialect contract test reads
# ``dialect`` without an instance.
_DIALECT = _resolve_dialect(DIALECTS)

# The driver reports a query that outlived SQL_ATTR_QUERY_TIMEOUT as HYT00.
_TIMEOUT_SQLSTATES = frozenset({"HYT00"})
# The connection died under the statement; reopen and run it once more, which
# is safe only because execute() runs nothing but read-only queries.
_CONNECTION_LOST_SQLSTATES = frozenset({"08S01", "08003"})

# Schemas every Fabric database carries that hold no user data: the catalog
# views, Query Insights, and the fixed database-role schemas. Compared exactly,
# since Fabric's default collation is case-sensitive.
_SYSTEM_SCHEMAS = (
    "INFORMATION_SCHEMA",
    "sys",
    "queryinsights",
    "guest",
    "db_owner",
    "db_accessadmin",
    "db_securityadmin",
    "db_ddladmin",
    "db_backupoperator",
    "db_datareader",
    "db_datawriter",
    "db_denydatareader",
    "db_denydatawriter",
)

_TABLE_SCHEMA = ["table_schema", "table_name", "table_type"]
_COLUMN_SCHEMA = [
    "table_schema",
    "table_name",
    "column_name",
    "data_type",
    "is_nullable",
    "ordinal_position",
]
_VIEW_SCHEMA = ["table_schema", "table_name", "view_definition"]
_PK_SCHEMA = ["table_schema", "table_name", "column_name", "ordinal_position"]
_FK_SCHEMA = [
    "table_schema",
    "table_name",
    "column_name",
    "referenced_schema",
    "referenced_table",
    "referenced_column",
]
_QUERY_SCHEMA = ["end_time", "query_text"]

# Nodes that make a statement more than a read, wherever they sit in the tree.
# ``Into`` is T-SQL's ``SELECT ... INTO new_table``, which parses as a SELECT.
_WRITE_NODES: tuple[type[exp.Expression], ...] = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Merge,
    exp.Create,
    exp.Drop,
    exp.Alter,
    exp.TruncateTable,
    exp.Command,
    exp.Into,
    exp.Execute,
)


class ReadOnlyViolation(ValueError):
    """A statement was refused because it is not a single read-only query."""


def _first(query: dict[str, list[str]], key: str) -> str | None:
    value = query.get(key, [None])[0]
    return unquote(value).strip() if value else None


def _parse_connection_string(connection_string: str) -> dict[str, Any]:
    """Parse a Fabric URL into the settings this connector needs.

    Expected format::

        fabric://CLIENT_ID:SECRET@HOST[:1433]/DATABASE[?tenant_id=...&auth=...]

    ``auth`` is ``service_principal`` (the default; the ODBC driver signs in)
    or ``token`` (the connector fetches the token; needs ``tenant_id``).
    ``driver`` and ``login_timeout`` override the ODBC driver name and the
    seconds allowed to establish a connection.
    """
    parsed = urlparse(connection_string)
    if parsed.scheme.split("+", 1)[0].lower() != "fabric":
        raise ValueError("Not a Fabric connection string (expected fabric://...)")
    if not parsed.hostname:
        raise ValueError("Fabric connection string is missing the SQL endpoint host")

    client_id = unquote(parsed.username or "")
    secret = unquote(parsed.password or "")
    if not client_id or not secret:
        raise ValueError(
            "Fabric connection string requires a service principal, e.g. "
            "fabric://<client_id>:<secret>@<workspace>.datawarehouse.fabric"
            ".microsoft.com/<database>"
        )

    database = unquote(parsed.path.lstrip("/"))
    if not database:
        raise ValueError(
            "Fabric connection string requires the warehouse or lakehouse name "
            "as its path"
        )

    query = parse_qs(parsed.query)
    auth = (_first(query, "auth") or AUTH_SERVICE_PRINCIPAL).lower()
    if auth not in _AUTH_MODES:
        raise ValueError(
            f"Unsupported Fabric auth {auth!r} (use service_principal or token)"
        )
    tenant_id = _first(query, "tenant_id")
    if auth == AUTH_TOKEN and not tenant_id:
        raise ValueError("Fabric auth=token requires tenant_id")

    login_timeout = _first(query, "login_timeout")
    return {
        "host": parsed.hostname,
        "port": parsed.port or DEFAULT_PORT,
        "database": database,
        "client_id": client_id,
        "secret": secret,
        "tenant_id": tenant_id,
        "auth": auth,
        "driver": _first(query, "driver") or DEFAULT_DRIVER,
        "login_timeout": int(login_timeout) if login_timeout else None,
    }


def _odbc_value(value: str) -> str:
    """Brace-quote an ODBC attribute value so ``;`` or ``}`` cannot end it early."""
    return "{" + value.replace("}", "}}") + "}"


def _odbc_connection_string(settings: dict[str, Any]) -> str:
    """Build the driver connection string; credentials only on the driver path."""
    parts = [
        f"Driver={_odbc_value(settings['driver'])}",
        f"Server=tcp:{settings['host']},{settings['port']}",
        f"Database={_odbc_value(settings['database'])}",
        "Encrypt=yes",
        "TrustServerCertificate=no",
    ]
    if settings["auth"] == AUTH_SERVICE_PRINCIPAL:
        parts += [
            "Authentication=ActiveDirectoryServicePrincipal",
            f"UID={_odbc_value(settings['client_id'])}",
            f"PWD={_odbc_value(settings['secret'])}",
        ]
    return ";".join(parts) + ";"


def _token_struct(token: str) -> bytes:
    """Pack an access token the way msodbcsql expects it: length-prefixed UTF-16-LE."""
    raw = token.encode("utf-16-le")
    return struct.pack(f"<I{len(raw)}s", len(raw), raw)


def _import_pyodbc() -> Any:
    try:
        import pyodbc
    except ImportError as exc:
        raise ImportError(
            "The Fabric connector needs pyodbc and Microsoft's ODBC Driver 18 for "
            "SQL Server: install the 'fabric' extra (uv sync --extra fabric) and "
            "the msodbcsql18 system package"
        ) from exc
    return pyodbc


def _sqlstate(error: BaseException) -> str | None:
    """pyodbc puts the SQLSTATE first in ``args``."""
    args = getattr(error, "args", ())
    return args[0] if args and isinstance(args[0], str) else None


def prepare_read_only(sql: str, dialect: str = _DIALECT) -> str:
    """Return the text to send for *sql*, refusing anything but one read query.

    A statement sqlglot cannot parse is refused rather than sent unchecked:
    without a tree there is no telling a read from a write.

    A query carrying ``LIMIT`` -- which T-SQL lacks, but which the profiling and
    probe queries emit -- is re-rendered so it becomes ``TOP``. Everything else
    is sent exactly as written.

    Raises:
        ReadOnlyViolation: *sql* is empty, unparseable, more than one
            statement, or anything other than a side-effect-free query.
    """
    try:
        trees = [tree for tree in sqlglot.parse(sql, read=dialect) if tree is not None]
    except SqlglotError as exc:
        raise ReadOnlyViolation(
            f"Fabric connector refused a statement it could not parse: {exc}"
        ) from exc
    if len(trees) != 1:
        raise ReadOnlyViolation(
            f"Fabric connector runs exactly one statement per call; got {len(trees)}"
        )
    tree = trees[0]
    if not isinstance(tree, exp.Query) or tree.find(*_WRITE_NODES) is not None:
        raise ReadOnlyViolation(
            "Fabric connector is read-only: only SELECT and WITH queries are "
            f"allowed, not {tree.key.upper()}"
        )
    if tree.find(exp.Limit) is not None:
        return tree.sql(dialect=dialect)
    return sql


def _literal_list(values: tuple[str, ...]) -> str:
    return ", ".join("'" + value.replace("'", "''") + "'" for value in values)


_NOT_SYSTEM = f"NOT IN ({_literal_list(_SYSTEM_SCHEMAS)})"


class FabricDatabase(SQLDatabase):
    """Concrete :class:`SQLDatabase` for a Fabric Warehouse or Lakehouse SQL endpoint.

    Parameters
    ----------
    connection_string:
        ``fabric://client_id:secret@<workspace>.datawarehouse.fabric.microsoft.com/<database>``
    schemas:
        Optional ingestion allowlist, matched case-insensitively. Empty or
        ``None`` means every schema except the system ones.
    """

    supports_statement_timeout = True

    def __init__(
        self,
        connection_string: str,
        schemas: list[str] | None = None,
    ) -> None:
        self._settings = _parse_connection_string(connection_string)
        self._database_name = str(self._settings["database"])
        self._schema_filter: set[str] | None = {
            s.strip().casefold() for s in (schemas or []) if s and s.strip()
        } or None
        if self._schema_filter:
            logger.info(
                "Fabric ingestion restricted to schemas: %s",
                sorted(self._schema_filter),
            )
        self._lock = threading.RLock()
        self._connection: Any = None
        self._credential: Any = None

    # ------------------------------------------------------------------
    # Transport
    # ------------------------------------------------------------------

    def _access_token(self) -> bytes:
        """Fetch an Entra token for the endpoint; azure-identity caches and renews it."""
        if self._credential is None:
            try:
                from azure.identity import ClientSecretCredential
            except ImportError as exc:
                raise ImportError(
                    "Fabric auth=token needs azure-identity: install the 'fabric' "
                    "extra (uv sync --extra fabric)"
                ) from exc
            self._credential = ClientSecretCredential(
                tenant_id=str(self._settings["tenant_id"]),
                client_id=str(self._settings["client_id"]),
                client_secret=str(self._settings["secret"]),
            )
        return _token_struct(self._credential.get_token(TOKEN_SCOPE).token)

    def _open(self) -> Any:
        pyodbc = _import_pyodbc()
        kwargs: dict[str, Any] = {
            "autocommit": True,
            "timeout": self._settings["login_timeout"] or DEFAULT_LOGIN_TIMEOUT_S,
        }
        if self._settings["auth"] == AUTH_TOKEN:
            kwargs["attrs_before"] = {SQL_COPT_SS_ACCESS_TOKEN: self._access_token()}
        connection = pyodbc.connect(_odbc_connection_string(self._settings), **kwargs)
        logger.info(
            "Opened Fabric connection to %s/%s (auth=%s)",
            self._settings["host"],
            self._database_name,
            self._settings["auth"],
        )
        return connection

    def _reset_connection(self) -> None:
        if self._connection is not None:
            try:
                self._connection.close()
            except Exception:
                logger.debug("Failed to close Fabric connection", exc_info=True)
            self._connection = None

    def _run(
        self, sql: str, parameters: Optional[list], timeout_s: float | None
    ) -> pd.DataFrame:
        """Run *sql* on the shared connection, capped at *timeout_s* if given.

        pyodbc applies ``Connection.timeout`` as SQL_ATTR_QUERY_TIMEOUT to each
        cursor it creates, so setting it just before the cursor and clearing it
        after scopes the cap to this one statement.
        """
        connection = self._connection
        connection.timeout = 0 if timeout_s is None else max(1, math.ceil(timeout_s))
        try:
            cursor = connection.cursor()
        finally:
            connection.timeout = 0
        try:
            if parameters:
                cursor.execute(sql, parameters)
            else:
                cursor.execute(sql)
            if cursor.description is None:
                return pd.DataFrame()
            columns = [str(description[0]) for description in cursor.description]
            rows = [tuple(row) for row in cursor.fetchall()]
            return pd.DataFrame(rows, columns=columns)
        finally:
            try:
                cursor.close()
            except Exception:
                logger.debug("Failed to close Fabric cursor", exc_info=True)

    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------

    @property
    def dialect(self) -> str:
        """Return this engine's sqlglot dialect name.

        Must be a member of ``sqlglot.dialects.DIALECTS`` — callers pass it
        straight to sqlglot without translation. See ``CONNECTOR_REGISTRY``.
        """
        return _DIALECT

    @property
    def database_name(self) -> str:
        return self._database_name

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def execute(
        self,
        sql: str,
        parameters: Optional[list] = None,
        *,
        timeout_s: float | None = None,
    ) -> pd.DataFrame:
        """Run one read-only query and return the result as a DataFrame.

        Parameters use pyodbc's ``?`` placeholders. With *timeout_s*, the driver
        cancels the statement once it has run that long. A connection that
        dropped underneath the statement is reopened and the query retried once.

        Raises:
            ReadOnlyViolation: *sql* is not a single ``SELECT`` / ``WITH`` query;
                nothing was sent.
            StatementTimeout: the statement outlived *timeout_s*.
        """
        statement = prepare_read_only(sql, self.dialect)
        pyodbc = _import_pyodbc()
        with self._lock:
            try:
                return self._run_capped(pyodbc, statement, parameters, timeout_s)
            except pyodbc.Error as exc:
                if _sqlstate(exc) not in _CONNECTION_LOST_SQLSTATES:
                    raise
                logger.warning(
                    "Fabric connection to %s dropped (%s); reconnecting once",
                    self._database_name,
                    _sqlstate(exc),
                )
                self._reset_connection()
            return self._run_capped(pyodbc, statement, parameters, timeout_s)

    def _run_capped(
        self,
        pyodbc: Any,
        sql: str,
        parameters: Optional[list],
        timeout_s: float | None,
    ) -> pd.DataFrame:
        """:meth:`_run`, with the driver's query timeout raised as StatementTimeout.

        The connection is opened first and outside that mapping: a login that
        times out reports the same HYT00, and it is not the statement's cap.
        """
        if self._connection is None:
            self._connection = self._open()
        started = time.monotonic()
        try:
            return self._run(sql, parameters, timeout_s)
        except pyodbc.Error as exc:
            if timeout_s is not None and _sqlstate(exc) in _TIMEOUT_SQLSTATES:
                raise StatementTimeout.since(timeout_s, started) from exc
            raise

    # ------------------------------------------------------------------
    # Schema introspection
    # ------------------------------------------------------------------

    def _keep_schemas(self, frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        """Shape *frame* to *columns* and drop rows outside the schema allowlist.

        Filtered here rather than in SQL: the default collation is
        case-sensitive, and the allowlist is matched case-insensitively.
        """
        if frame.empty:
            return pd.DataFrame(columns=columns)
        if self._schema_filter is not None:
            keep = (
                frame["table_schema"]
                .astype(str)
                .str.casefold()
                .isin(self._schema_filter)
            )
            frame = frame.loc[keep].reset_index(drop=True)
        return frame[columns]

    def _optional_read(
        self, what: str, sql: str, columns: list[str], *, level: int = logging.DEBUG
    ) -> pd.DataFrame:
        """Run a metadata read that some Fabric items do not support.

        A Lakehouse SQL endpoint, for one, may refuse a catalog view a Warehouse
        serves. That is not an ingestion failure -- the item simply has none of
        *what* -- so it yields an empty, correctly shaped frame.
        """
        pyodbc = _import_pyodbc()
        try:
            frame = self.execute(sql)
        except pyodbc.Error:
            logger.log(
                level,
                "Fabric database %s does not expose %s; returning none",
                self._database_name,
                what,
                exc_info=level <= logging.DEBUG,
            )
            return pd.DataFrame(columns=columns)
        if "table_schema" not in columns:
            return frame if not frame.empty else pd.DataFrame(columns=columns)
        return self._keep_schemas(frame, columns)

    def get_schemas(self) -> list[str]:
        """List the database's user schemas.

        Used by the connection UI to offer a schema selection, and by the
        connection test -- it runs a real query, so it proves the endpoint,
        the credentials and the database all work.
        """
        frame = self.execute(
            f"SELECT name FROM sys.schemas WHERE name {_NOT_SYSTEM} ORDER BY name"
        )
        if frame.empty:
            return []
        return [str(name) for name in frame.iloc[:, 0].tolist()]

    def get_tables(self) -> pd.DataFrame:
        frame = self.execute(f"""
            SELECT
                TABLE_SCHEMA AS table_schema,
                TABLE_NAME AS table_name,
                CASE TABLE_TYPE
                    WHEN 'VIEW' THEN '{TableTypes.VIEW}'
                    ELSE '{TableTypes.BASE_TABLE}'
                END AS table_type
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA {_NOT_SYSTEM}
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """)
        return self._keep_schemas(frame, _TABLE_SCHEMA)

    def get_columns(self) -> pd.DataFrame:
        frame = self.execute(f"""
            SELECT
                TABLE_SCHEMA AS table_schema,
                TABLE_NAME AS table_name,
                COLUMN_NAME AS column_name,
                DATA_TYPE AS data_type,
                CAST(CASE WHEN IS_NULLABLE = 'YES' THEN 1 ELSE 0 END AS BIT)
                    AS is_nullable,
                ORDINAL_POSITION AS ordinal_position
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA {_NOT_SYSTEM}
            ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
        """)
        frame = self._keep_schemas(frame, _COLUMN_SCHEMA)
        if not frame.empty:
            frame["is_nullable"] = frame["is_nullable"].map(
                lambda value: None if value is None else bool(value)
            )
        return frame

    def get_views(self) -> pd.DataFrame:
        """Return view definitions from ``sys.sql_modules``.

        Not ``INFORMATION_SCHEMA.VIEWS``, which truncates a definition at 4000
        characters.
        """
        frame = self.execute(f"""
            SELECT
                s.name AS table_schema,
                v.name AS table_name,
                m.definition AS view_definition
            FROM sys.views AS v
            JOIN sys.schemas AS s ON s.schema_id = v.schema_id
            LEFT JOIN sys.sql_modules AS m ON m.object_id = v.object_id
            WHERE s.name {_NOT_SYSTEM}
            ORDER BY s.name, v.name
        """)
        return self._keep_schemas(frame, _VIEW_SCHEMA)

    def get_pks(self) -> pd.DataFrame:
        """Return ``NOT ENFORCED`` primary keys; empty where the item has none."""
        return self._optional_read(
            "primary keys",
            f"""
            SELECT
                s.name AS table_schema,
                t.name AS table_name,
                c.name AS column_name,
                ic.key_ordinal AS ordinal_position
            FROM sys.key_constraints AS kc
            JOIN sys.tables AS t ON t.object_id = kc.parent_object_id
            JOIN sys.schemas AS s ON s.schema_id = t.schema_id
            JOIN sys.index_columns AS ic
              ON ic.object_id = kc.parent_object_id
             AND ic.index_id = kc.unique_index_id
            JOIN sys.columns AS c
              ON c.object_id = ic.object_id
             AND c.column_id = ic.column_id
            WHERE kc.type = 'PK'
              AND s.name {_NOT_SYSTEM}
            ORDER BY s.name, t.name, ic.key_ordinal
            """,
            _PK_SCHEMA,
        )

    def get_fks(self) -> pd.DataFrame:
        """Return ``NOT ENFORCED`` foreign keys; empty where the item has none."""
        return self._optional_read(
            "foreign keys",
            f"""
            SELECT
                ps.name AS table_schema,
                pt.name AS table_name,
                pc.name AS column_name,
                rs.name AS referenced_schema,
                rt.name AS referenced_table,
                rc.name AS referenced_column
            FROM sys.foreign_keys AS fk
            JOIN sys.foreign_key_columns AS fkc
              ON fkc.constraint_object_id = fk.object_id
            JOIN sys.tables AS pt ON pt.object_id = fkc.parent_object_id
            JOIN sys.schemas AS ps ON ps.schema_id = pt.schema_id
            JOIN sys.columns AS pc
              ON pc.object_id = fkc.parent_object_id
             AND pc.column_id = fkc.parent_column_id
            JOIN sys.tables AS rt ON rt.object_id = fkc.referenced_object_id
            JOIN sys.schemas AS rs ON rs.schema_id = rt.schema_id
            JOIN sys.columns AS rc
              ON rc.object_id = fkc.referenced_object_id
             AND rc.column_id = fkc.referenced_column_id
            WHERE ps.name {_NOT_SYSTEM}
            ORDER BY ps.name, pt.name, fk.name, fkc.constraint_column_id
            """,
            _FK_SCHEMA,
        )

    def get_queries(self, hours: int = 24) -> pd.DataFrame:
        """Return recent successful statements from Query Insights.

        ``queryinsights.exec_requests_history`` is populated asynchronously and
        is missing on items or capacities without Query Insights; either way
        the result is simply empty.
        """
        return self._optional_read(
            "queryinsights.exec_requests_history",
            f"""
            SELECT
                end_time,
                command AS query_text
            FROM queryinsights.exec_requests_history
            WHERE end_time >= DATEADD(hour, -{int(hours)}, SYSUTCDATETIME())
              AND status = 'Succeeded'
              AND command IS NOT NULL
            ORDER BY end_time DESC
            """,
            _QUERY_SCHEMA,
            level=logging.INFO,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def ping(self) -> None:
        """Verify the endpoint, the credentials, and that the database is readable."""
        self.execute("SELECT 1 AS ok")

    def close(self) -> None:
        with self._lock:
            self._reset_connection()
