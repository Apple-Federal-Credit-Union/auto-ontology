# SPDX-FileCopyrightText: Copyright (c) 2024-2026, NVIDIA CORPORATION & AFFILIATES.
# All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""VDB initialization — data layer and semantic layer collections."""

from auto_ontology.infra.postgres import get_postgres_connection_string
from auto_ontology.vdb.postgres import PostgresVDB

DATA_VDB_COLLECTION: str = "data_objects_layer"
SEMANTIC_VDB_COLLECTION: str = "semantic_layer"
VDB_SCHEMA: str = "vdb"


def get_vdb(
    *,
    database_name: str | None = None,
    collection_name: str = DATA_VDB_COLLECTION,
    reset: bool = False,
) -> PostgresVDB:
    """Build a PostgresVDB pointed at the local pgvector-enabled Postgres.

    When database_name is provided, the VDB will use it to reset old embeddings for the given database.

    ``vector_size`` follows ``EMBED_DIMENSIONS`` (default
    ``EMBED_DIMENSIONS_DEFAULT``), so the column type and HNSW index match
    whatever the configured embedding model emits. Changing it after a
    collection has been created requires re-ingesting -- the column's width
    is fixed at creation and isn't migrated by switching the setting.
    """
    # Imported lazily: auto_ontology.utils (embed_dimensions' package) pulls in
    # the retriever, which imports this module -- a module-level import here
    # would be circular.
    from auto_ontology.utils.model_config import (
        EMBED_DIMENSIONS_DEFAULT,
        embed_dimensions,
    )

    connection_string = get_postgres_connection_string()
    kwargs: dict = {
        "connection_string": connection_string,
        "collection_name": collection_name,
        "schema_name": VDB_SCHEMA,
        "vector_size": embed_dimensions() or EMBED_DIMENSIONS_DEFAULT,
    }
    if database_name:
        kwargs["database_name"] = database_name
    return PostgresVDB(reset=reset, **kwargs)


def get_data_vdb(
    *, database_name: str | None = None, reset: bool = False
) -> PostgresVDB:
    """Backward-compatible alias for :func:`get_vdb` (tabular / data layer)."""
    return get_vdb(database_name=database_name, reset=reset)


def get_semantic_vdb(
    *, database_name: str | None = None, reset: bool = False
) -> PostgresVDB:
    """Build a PostgresVDB for the semantic layer."""
    return get_vdb(
        database_name=database_name,
        collection_name=SEMANTIC_VDB_COLLECTION,
        reset=reset,
    )
