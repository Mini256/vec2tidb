"""Common utilities and functions for Qdrant subcommands."""

import json
from typing import Optional
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session
import click


def get_snapshot_uri(
    dataset: Optional[str] = None, snapshot_uri: Optional[str] = None
) -> Optional[str]:
    """
    Get snapshot URI from dataset name or return custom snapshot URI.

    Args:
        dataset: Dataset name to get predefined snapshot URI
        snapshot_uri: Custom snapshot URI (takes precedence over dataset)

    Returns:
        Resolved snapshot URI or None if neither provided

    Raises:
        click.UsageError: If dataset is provided but invalid
    """
    if snapshot_uri:
        return snapshot_uri
    elif dataset:
        dataset_snapshots = {
            "midlib": "https://snapshots.qdrant.io/midlib.snapshot",
            "qdrant-docs": "https://snapshots.qdrant.io/qdrant-docs-04-05.snapshot",
            "prefix-cache": "https://snapshots.qdrant.io/prefix-cache.snapshot",
        }
        resolved_uri = dataset_snapshots.get(dataset)
        if not resolved_uri:
            raise click.UsageError(f"Invalid dataset: {dataset}")
        return resolved_uri
    else:
        return None


def drop_vector_table(
    db_engine: Engine,
    table_name: str,
):
    """Drop a vector table if it exists."""
    preparer = db_engine.dialect.identifier_preparer
    table_name = preparer.quote_identifier(table_name)
    with Session(db_engine) as session:
        session.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        session.commit()


def create_vector_table(
    db_engine: Engine,
    table_name: str,
    id_column: str,
    vector_column: str,
    payload_column: str,
    distance_metric: str = "cosine",
    dimensions: int = 1536,
    id_column_type: str = "BIGINT",
):
    """Create a vector table with the specified configuration."""
    if distance_metric == "l2":
        distance_fn = "VEC_L2_DISTANCE"
    elif distance_metric == "cosine":
        distance_fn = "VEC_COSINE_DISTANCE"
    else:
        raise click.UsageError(f"Invalid distance metric: {distance_metric}")

    preparer = db_engine.dialect.identifier_preparer
    index_name = preparer.quote_identifier(f"vec_idx_{table_name}_on_{vector_column}")
    table_name = preparer.quote_identifier(table_name)
    id_column = preparer.quote_identifier(id_column)
    vector_column = preparer.quote_identifier(vector_column)
    payload_column = preparer.quote_identifier(payload_column)

    # Create table with direct SQL to avoid pytidb vector index issues
    with Session(db_engine) as session:
        create_sql = f"""
        CREATE TABLE {table_name} (
            {id_column} {id_column_type} PRIMARY KEY,
            {vector_column} VECTOR({dimensions}),
            {payload_column} JSON,
            `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            VECTOR INDEX {index_name} (({distance_fn}({vector_column})))
        )
        """
        session.execute(text(create_sql))
        session.commit()


def check_vector_table(
    db_engine: Engine,
    table_name: str,
    id_column: str,
    vector_column: str,
    payload_column: Optional[str],
):
    """Check if a vector table exists and has the required columns."""
    preparer = db_engine.dialect.identifier_preparer
    table_name = preparer.quote_identifier(table_name)

    with Session(db_engine) as session:
        try:
            session.execute(text(f"SELECT 1 FROM {table_name} LIMIT 1"))
        except Exception as e:
            raise Exception(f"Table {table_name} does not exist: {e}")

        columns = session.execute(text(f"SHOW COLUMNS FROM {table_name};")).fetchall()
        column_names = [col[0] for col in columns]
        if id_column not in column_names:
            raise Exception(
                f"Column `{id_column}` does not exist in table {table_name}"
            )
        if vector_column not in column_names:
            raise Exception(
                f"Column `{vector_column}` does not exist in table {table_name}"
            )
        if payload_column and payload_column not in column_names:
            raise Exception(
                f"Column `{payload_column}` does not exist in table {table_name}"
            )


def insert_points(
    db_engine: Engine,
    points: list[PointStruct],
    table_name: str,
    id_column: str,
    vector_column: str,
    payload_column: str,
):
    """Insert points into a vector table."""
    preparer = db_engine.dialect.identifier_preparer
    table_name = preparer.quote_identifier(table_name)
    id_column = preparer.quote_identifier(id_column)
    vector_column = preparer.quote_identifier(vector_column)
    payload_column = preparer.quote_identifier(payload_column)

    with Session(db_engine) as session:
        insert_sql = f"""
        INSERT INTO {table_name}
        ({id_column}, {vector_column}, {payload_column})
        VALUES (:id, :vector, :payload)
        """

        insert_records = []
        for point in points:
            id_value = point.id
            vector_str = json.dumps(point.vector)
            payload_str = json.dumps(point.payload)
            insert_records.append(
                {
                    "id": id_value,
                    "vector": vector_str,
                    "payload": payload_str,
                }
            )

        session.execute(text(insert_sql), insert_records)
        session.commit()


def update_points(
    db_engine: Engine,
    points: list[PointStruct],
    table_name: str,
    id_column: str,
    vector_column: str,
    payload_column: Optional[str],
):
    """Update points in a vector table."""
    preparer = db_engine.dialect.identifier_preparer
    table_name = preparer.quote_identifier(table_name)
    id_column = preparer.quote_identifier(id_column)
    vector_column = preparer.quote_identifier(vector_column)
    payload_column = (
        preparer.quote_identifier(payload_column) if payload_column else None
    )

    with Session(db_engine) as session:
        if payload_column:
            set_clause = f"{vector_column} = :vector, {payload_column} = :payload"
        else:
            set_clause = f"{vector_column} = :vector"

        # Prepare update SQL
        update_sql = f"""
        UPDATE {table_name}
        SET {set_clause}
        WHERE {id_column} = :id
        """

        # Prepare data for batch update
        update_records = []
        for point in points:
            id_value = point.id
            vector_str = json.dumps(point.vector)
            if payload_column:
                payload_str = json.dumps(point.payload)
                update_records.append(
                    {
                        "id": id_value,
                        "vector": vector_str,
                        "payload": payload_str,
                    }
                )
            else:
                update_records.append(
                    {
                        "id": id_value,
                        "vector": vector_str,
                    }
                )

        session.execute(text(update_sql), update_records)
        session.commit() 