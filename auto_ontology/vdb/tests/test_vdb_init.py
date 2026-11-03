# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES.
# All rights reserved.
# SPDX-License-Identifier: Apache-2.0

""":func:`auto_ontology.vdb.get_vdb` wires its ``vector_size`` from settings.

No Postgres connection is made here -- ``PostgresVDB.__init__`` only connects
when ``reset=True`` and a ``database_name`` is given, and this never passes
either, so the assertion is purely on how the two settings are threaded
through without needing a live database.
"""

from __future__ import annotations

import pytest

from auto_ontology.vdb import get_vdb


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    # Enough for get_postgres_connection_string() to build a (never-dialed)
    # URL. EMBED_DIMENSIONS / DEFAULT_MODELS_DIMENSIONS are cleared so a value
    # exported in the ambient shell can't leak into these assertions.
    monkeypatch.setenv("POSTGRES_USER", "u")
    monkeypatch.setenv("POSTGRES_PASSWORD", "p")
    monkeypatch.delenv("EMBED_DIMENSIONS", raising=False)
    monkeypatch.delenv("DEFAULT_MODELS_DIMENSIONS", raising=False)


def test_vector_size_defaults_to_2048_when_embed_dimensions_is_unset() -> None:
    assert get_vdb().vector_size == 2048


def test_vector_size_follows_embed_dimensions(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EMBED_DIMENSIONS", "1024")
    assert get_vdb().vector_size == 1024
