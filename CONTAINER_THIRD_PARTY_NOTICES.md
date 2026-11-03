<!--
SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES.
All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Container Third-Party Notices

This file covers what the Auto-Ontology container images add on top of their
base image. The application's own open-source dependencies (Python and npm
packages) are listed separately in [`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md).

Every image ships these files under `/licenses/`: `LICENSE`,
`THIRD_PARTY_NOTICES.md`, this file, and `OPEN_SOURCE_NOTICE`.

## Source code

Source code for the open-source components added on top of the base image is
archived by NVIDIA at https://opensource.nvidia.com/oss/teams/nvidia, as stated
in `OPEN_SOURCE_NOTICE`. Auto-Ontology's own source code is at
https://github.com/NVIDIA/auto-ontology.

## Base image

All three images are built on the NVIDIA-approved Ubuntu 22.04 base container
(OSRB Bug 3840915), pulled from NVIDIA's registry:

| Image | Base |
| --- | --- |
| `auto-ontology` (backend and ingestion) | `nvcr.io/nvidia/base/ubuntu:jammy-20250619` |
| `auto-ontology-frontend` | `nvcr.io/nvidia/base/ubuntu:jammy-20250619` |
| `auto-ontology-mcp` | `nvcr.io/nvidia/base/ubuntu:jammy-20250619` |

Source code for the base container is provided by NVIDIA with that image.

## Components added on top of the base image

### `auto-ontology` (backend and ingestion) — `Dockerfile`

| Component | Version | License | Project URL |
| --- | --- | --- | --- |
| CPython (python-build-standalone, installed by uv) | 3.12 | PSF-2.0, plus the licenses of its bundled libraries | https://github.com/astral-sh/python-build-standalone |
| Python packages in `/opt/venv` | per `uv.lock` | see `THIRD_PARTY_NOTICES.md` (Backend) | — |
| `ca-certificates` (Ubuntu package) | jammy | MPL-2.0 (certificate data), GPL-2.0-or-later (scripts) | https://packages.ubuntu.com/jammy/ca-certificates |
| `libpq5` (Ubuntu package) | jammy | PostgreSQL License | https://www.postgresql.org |
| `tini` (Ubuntu package) | jammy | MIT | https://github.com/krallin/tini |

#### Optional: `--build-arg WITH_FABRIC=true`

Off by default; the default image contains none of these. When enabled, the
build adds Microsoft's apt repository and installs the ODBC driver used by the
Microsoft Fabric connector, accepting Microsoft's EULA (`ACCEPT_EULA=Y`).
`msodbcsql18` is **proprietary, not open source**: whoever builds and
distributes such an image is responsible for complying with its license terms.

| Component | Version | License | Project URL |
| --- | --- | --- | --- |
| `msodbcsql18` (Microsoft ODBC Driver 18 for SQL Server, packages.microsoft.com) | 18.x | Microsoft Software License Terms (proprietary) | https://learn.microsoft.com/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server |
| `packages-microsoft-prod` (Microsoft apt repository configuration and signing key) | — | Microsoft | https://packages.microsoft.com |
| `unixodbc`, `odbcinst`, `libodbc2`, `libodbcinst2`, `unixodbc-common` (Ubuntu packages) | jammy | LGPL-2.1-or-later (libraries), GPL-2.0-or-later (tools) | https://packages.ubuntu.com/jammy/unixodbc |
| `libkrb5-3`, `libgssapi-krb5-2` (Ubuntu packages) | jammy | MIT | https://packages.ubuntu.com/jammy/libkrb5-3 |
| `openssl` (Ubuntu package) | jammy | Apache-2.0 | https://packages.ubuntu.com/jammy/openssl |

### `auto-ontology-frontend` — `frontend/Dockerfile`

| Component | Version | License | Project URL |
| --- | --- | --- | --- |
| Node.js, including its bundled npm and Corepack (NodeSource package) | 22.x | MIT, plus the licenses of its bundled dependencies | https://github.com/nodejs/node |
| npm packages in the Next.js standalone bundle | per `frontend/pnpm-lock.yaml` | see `THIRD_PARTY_NOTICES.md` (Frontend runtime) | — |
| Prisma CLI and engines (`/app/migrate`) | 7.7.0 | Apache-2.0 | https://github.com/prisma/prisma |
| dotenv (`/app/migrate`) | 17.4.2 | BSD-2-Clause | https://github.com/motdotla/dotenv |
| Geist and Geist Mono fonts (bundled by `next/font`) | — | SIL Open Font License 1.1 | https://github.com/vercel/geist-font |
| `ca-certificates` (Ubuntu package) | jammy | MPL-2.0 (certificate data), GPL-2.0-or-later (scripts) | https://packages.ubuntu.com/jammy/ca-certificates |
| `tini` (Ubuntu package) | jammy | MIT | https://github.com/krallin/tini |

### `auto-ontology-mcp` — `mcp/Dockerfile`

| Component | Version | License | Project URL |
| --- | --- | --- | --- |
| CPython (python-build-standalone, installed by uv) | 3.12 | PSF-2.0, plus the licenses of its bundled libraries | https://github.com/astral-sh/python-build-standalone |
| Python packages in `/opt/venv` | per `mcp/uv.lock` | see `THIRD_PARTY_NOTICES.md` (MCP server) | — |
| `ca-certificates` (Ubuntu package) | jammy | MPL-2.0 (certificate data), GPL-2.0-or-later (scripts) | https://packages.ubuntu.com/jammy/ca-certificates |
| `tini` (Ubuntu package) | jammy | MIT | https://github.com/krallin/tini |

Build-only tools (uv, compilers, `curl`, `gnupg`, pnpm) are used in earlier
build stages and are not present in the final images. The frontend image keeps
the npm and Corepack that ship with Node.js.

## Updating this file

Update this file whenever a Dockerfile changes its base image, its apt
packages, or the runtimes it installs. `THIRD_PARTY_NOTICES.md` tracks
dependency changes.
