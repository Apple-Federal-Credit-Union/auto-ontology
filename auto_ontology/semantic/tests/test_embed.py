# SPDX-FileCopyrightText: Copyright (c) 2024-2026, NVIDIA CORPORATION & AFFILIATES.
# All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the pure helpers in auto_ontology.semantic.embed."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from auto_ontology.semantic import embed
from auto_ontology.semantic.embed import _format_sample_values


def test_format_sample_values_handles_legacy_json_string_and_native_list() -> None:
    # Legacy Column nodes still store sample_values as a JSON string.
    assert _format_sample_values('["a", "b"]') == " Sample values: a, b."
    # Current writers store a native list.
    assert _format_sample_values(["a", "b"]) == " Sample values: a, b."


def test_format_sample_values_filters_long_values_and_handles_empty() -> None:
    long_value = "x" * 31
    assert _format_sample_values([long_value, "ok"]) == " Sample values: ok."
    assert _format_sample_values(None) == ""
    assert _format_sample_values([]) == ""
    assert _format_sample_values(["a", None, "b"]) == " Sample values: a, b."
    assert _format_sample_values('["a", null, "b"]') == " Sample values: a, b."


# ---------------------------------------------------------------------------
# build_semantic_embedder's EmbedParams -- EMBED_DIMENSIONS
# ---------------------------------------------------------------------------


def test_build_semantic_embedder_sends_configured_dimensions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The semantic VDB embedder is a direct ``EmbedParams(...)`` construction --
    it must pick up EMBED_DIMENSIONS like every other one, not silently omit it.
    """
    monkeypatch.setenv("EMBED_API_KEY", "key")
    monkeypatch.setenv("EMBED_DIMENSIONS", "2048")
    monkeypatch.setattr(embed, "get_semantic_vdb", lambda **_: MagicMock())

    embedder = embed.build_semantic_embedder("shop", reset=False)

    assert embedder is not None
    assert embedder.embed_params.dimensions == 2048


def test_build_semantic_embedder_unset_dimensions_sends_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EMBED_API_KEY", "key")
    monkeypatch.delenv("EMBED_DIMENSIONS", raising=False)
    monkeypatch.delenv("DEFAULT_MODELS_DIMENSIONS", raising=False)
    monkeypatch.setattr(embed, "get_semantic_vdb", lambda **_: MagicMock())

    embedder = embed.build_semantic_embedder("shop", reset=False)

    assert embedder is not None
    assert embedder.embed_params.dimensions is None
