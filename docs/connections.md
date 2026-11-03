<!--
SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES.
All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Connections and credential storage

Auto-Ontology reads source databases from two places: connections added in the
UI, and the `CONNECTION_STRINGS` environment variable. Use one per deployment,
because the two code paths order them differently:

- **Answering questions** uses UI-managed connections when any exist, and falls
  back to `CONNECTION_STRINGS`.
- **Ingestion** uses `CONNECTION_STRINGS` whenever it's set, and reads
  UI-managed connections only when it's empty.

## Connections added in the UI

Add connections under **Settings → Connections**, or through the API
(`POST /api/connections/test`, then `POST /api/connections`; permission
`connection:manage`).

Each connection can be scoped with a schema allowlist plus
`table_allow_regex` / `table_deny_regex`, applied at catalog extraction. Deny
wins, and names are unqualified and case-sensitive.

Where the credentials live depends on whether Vault is configured:

- **Without Vault.** The full connection object, password included, is
  serialized to JSON and stored unencrypted on the catalog's database row in
  Postgres.
- **With Vault.** Credentials are written to Vault, keyed by database name, and
  the catalog row stores none. The database name is the lookup key.

## `CONNECTION_STRINGS`

A comma- or newline-separated list of connection URLs supplied at deploy time
(`.env` for Docker Compose, `--set connectionStrings=...` for Helm). It carries
no schema or table filters. `GET /api/connections/source` reports whether it is set.

The URL scheme selects the connector:

| Engine        | Scheme                      | Add in the UI |
| ------------- | --------------------------- | :-----------: |
| PostgreSQL    | `postgresql://`, `postgres://` | yes        |
| MySQL         | `mysql://`                  | yes           |
| Databricks    | `databricks://`             | yes           |
| Snowflake     | `snowflake://`              | yes           |
| ClickHouse    | `clickhouse://`             | yes           |
| Trino         | `trino://`                  | yes           |
| Apache Kyuubi | `kyuubi://`                 | yes           |
| HeavyDB       | `heavydb://`                | yes           |
| Microsoft Fabric | `fabric://`              | yes           |
| DuckDB        | `duckdb://`                 | no            |
| SQLite        | `sqlite://`                 | no            |

The registry lives in
[`auto_ontology/connectors/registry.py`](../auto_ontology/connectors/registry.py).

## Microsoft Fabric

The `fabric` connector reads a Fabric **Warehouse** or the read-only **SQL
analytics endpoint** of a **Lakehouse**, one warehouse or lakehouse per
connection. It only runs `SELECT` / `WITH` queries.

### Install

The connector is optional and needs two things the default install does not
include:

- the `fabric` Python extra (`pyodbc`, `azure-identity`): `uv sync --extra fabric`;
- Microsoft's **ODBC Driver 18 for SQL Server**, a system package
  (`msodbcsql18`, from Microsoft's apt/yum repository, installed with
  `ACCEPT_EULA=Y`). The driver is proprietary; see Microsoft's
  [install guide](https://learn.microsoft.com/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server).

The backend image installs both only when built with
`docker build --build-arg WITH_FABRIC=true .`; by default it contains neither.

### Connection string

Use the host from the warehouse's or lakehouse's *SQL connection string*
setting:

```text
fabric://<client_id>:<client_secret>@<workspace>.datawarehouse.fabric.microsoft.com[:1433]/<warehouse_or_lakehouse>[?tenant_id=<tenant_id>&auth=token]
```

URL-encode the client secret. The connection always uses TLS
(`Encrypt=yes`) on port 1433 unless another port is given. Optional query
parameters: `driver` (ODBC driver name, default `ODBC Driver 18 for SQL
Server`) and `login_timeout` (seconds). In the UI, pick **Microsoft Fabric**
and fill in the same fields; a schema can be named or picked from a list.

### Authentication

Fabric accepts Microsoft Entra ID identities only, never SQL logins. Both
modes use a service principal (client ID and secret):

- **Driver sign-in** (default, `auth=service_principal`): the ODBC driver signs
  in itself with `Authentication=ActiveDirectoryServicePrincipal`. `tenant_id`
  is not needed.
- **In-process token** (`auth=token`; the *Fetch Entra Token in Process* box in
  the UI): the connector obtains the access token through `azure-identity` and
  hands it to the driver. Requires `tenant_id`.

### Permissions

The service principal needs:

- the **Viewer** role (or higher) on the Fabric workspace, or the warehouse or
  lakehouse shared with it directly; and
- **read access through the SQL endpoint** (`ReadData`, "Read all data using
  SQL"), which the Viewer role includes, plus `SELECT` on any objects
  restricted by T-SQL permissions.

A Fabric administrator must also allow service principals to use Fabric APIs
in the tenant settings. Write access is never needed.

## Vault

Vault is off by default. It turns on only when all four of these are set:

- `VAULT_ADDR`
- `VAULT_NAMESPACE`
- `VAULT_ROLE_ID`
- `VAULT_SECRET_ID`

`VAULT_AUTH_MOUNT` and `VAULT_KV_MOUNT` are optional overrides. A partial
configuration is ignored with a warning, and credentials fall back to plaintext
storage in Postgres.
