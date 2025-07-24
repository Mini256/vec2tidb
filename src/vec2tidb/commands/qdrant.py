from typing import Optional

import click
import json
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session

from vec2tidb.common import process_with_tqdm
from vec2tidb.tidb import create_tidb_engine



def migrate(
    mode: str,
    qdrant_api_url: str,
    qdrant_api_key: Optional[str],
    qdrant_collection_name: str,
    tidb_database_url: str,
    table_name: str,
    id_column: str,
    id_column_type: str,
    vector_column: str,
    payload_column: str,
    batch_size: int = 100,
    drop_table: bool = False,
):
    """Migrate vector data from a Qdrant collection to a TiDB table."""

    # Initialize Qdrant client
    qdrant_client = QdrantClient(url=qdrant_api_url, api_key=qdrant_api_key)
    if not qdrant_client.collection_exists(collection_name=qdrant_collection_name):
        raise click.UsageError(
            f"Requested Qdrant collection '{qdrant_collection_name}' does not exist"
        )

    # Validate Qdrant collection has data
    vector_total = qdrant_client.count(collection_name=qdrant_collection_name).count
    if vector_total == 0:
        raise click.UsageError(
            f"No records present in requested Qdrant collection '{qdrant_collection_name}'"
        )
    
    # Determine the type of point IDs in the Qdrant collection by fetching the first point
    id_column_type = "BIGINT"
    sample_points = qdrant_client.scroll(collection_name=qdrant_collection_name, limit=1)
    if sample_points and sample_points[0]:
        sample_point = sample_points[0][0]
        if isinstance(sample_point.id, int):
            id_column_type = "BIGINT"
        elif isinstance(sample_point.id, str):
            id_length = len(sample_point.id)
            id_column_type = f"VARCHAR({id_length})"
        else:
            raise click.BadParameter(f"Unsupported Qdrant point ID type: {type(sample_point.id)}")

    # Get collection info to determine vector dimension
    collection_info = qdrant_client.get_collection(
        collection_name=qdrant_collection_name
    )
    vector_dimension = collection_info.config.params.vectors.size
    vector_distance_metric = collection_info.config.params.vectors.distance.lower()

    migration_summary = [
        "=" * 64,
        "Source database: Qdrant",
        "Source collection:",
        f"  - Name           : {qdrant_collection_name}",
        f"  - Vector Count   : {vector_total}",
        f"  - Dimension      : {vector_dimension}",
        f"  - Distance Metric: {vector_distance_metric}",
        "",
        "Target database: TiDB",
        "Target table:",
        f"  - Name           : {table_name}",
        f"  - ID Column      : {id_column} ({id_column_type})",
        f"  - Vector Column  : {vector_column}",
        f"  - Payload Column : {payload_column}",
        "=" * 64,
        "",
    ]
    for line in migration_summary:
        click.echo(line)

    # Initialize TiDB client
    db_engine = create_tidb_engine(tidb_database_url)
    click.echo(f"🔌 Connected to TiDB database.")

    # Setup TiDB table
    if mode == "create":
        try:
            if drop_table:
                drop_vector_table(db_engine, table_name)
                click.echo(f"✅ Dropped existing TiDB table: {table_name}")

            create_vector_table(
                db_engine,
                table_name,
                id_column,
                vector_column,
                payload_column,
                distance_metric=vector_distance_metric,
                dimensions=vector_dimension,
                id_column_type=id_column_type,
            )
            click.echo(f"✅ Created new TiDB table: {table_name}")
        except Exception as e:
            raise click.ClickException(f"Failed to create table: {e}")
    elif mode == "update":
        try:
            check_vector_table(db_engine, table_name, id_column, vector_column, payload_column)
            click.echo(f"✅ Verified the existing TiDB table: {table_name}")
        except Exception as e:
            raise click.ClickException(f"Failed to check table: {e}")

    # Migrate data with progress tracking
    click.echo("⏳ Starting data migration...\n")

    # Track pagination state
    current_offset = None

    def batch_processor(batch_size=batch_size, **kwargs):
        """Process a batch of vectors from Qdrant."""
        nonlocal current_offset

        points, next_page_offset = qdrant_client.scroll(
            collection_name=qdrant_collection_name,
            limit=batch_size,
            offset=current_offset,
            with_payload=True,
            with_vectors=True,
        )

        if not points:
            return 0, False

        # Update offset for next iteration
        current_offset = next_page_offset

        # Insert/update records in TiDB
        if mode == "create":
            insert_points(db_engine, points, table_name, id_column, vector_column, payload_column)
        elif mode == "update":
            update_points(db_engine, points, table_name, id_column, vector_column, payload_column)

        has_more = next_page_offset is not None
        return len(points), has_more

    # Use the progress tracking utility
    processed_total = process_with_tqdm(
        tasks_total=vector_total, batch_processor=batch_processor, batch_size=batch_size
    )

    click.echo()
    click.echo(f"🎉 Migration completed successfully! Migrated {processed_total} points from Qdrant to TiDB.")


def drop_vector_table(
    db_engine: Engine,
    table_name: str,
):
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
            raise Exception(f"Column `{id_column}` does not exist in table {table_name}")
        if vector_column not in column_names:
            raise Exception(f"Column `{vector_column}` does not exist in table {table_name}")
        if payload_column and payload_column not in column_names:
            raise Exception(f"Column `{payload_column}` does not exist in table {table_name}")


def insert_points(
    db_engine: Engine,
    points: list[PointStruct],
    table_name: str,
    id_column: str,
    vector_column: str,
    payload_column: str,
):
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
            insert_records.append({
                "id": id_value,
                "vector": vector_str,
                "payload": payload_str,
            })

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
    preparer = db_engine.dialect.identifier_preparer
    table_name = preparer.quote_identifier(table_name)
    id_column = preparer.quote_identifier(id_column)
    vector_column = preparer.quote_identifier(vector_column)
    payload_column = preparer.quote_identifier(payload_column) if payload_column else None

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
                update_records.append({
                    "id": id_value,
                    "vector": vector_str,
                    "payload": payload_str,
                })
            else:
                update_records.append({
                    "id": id_value,
                    "vector": vector_str,
                })

        session.execute(text(update_sql), update_records)
        session.commit()


def load_sample(
    qdrant_api_url: str,
    qdrant_api_key: Optional[str],
    qdrant_collection_name: str,
    snapshot_uri: str,
):
    qdrant_client = QdrantClient(url=qdrant_api_url, api_key=qdrant_api_key)

    click.echo(f"⏳ Loading sample collection from {snapshot_uri}...")
    qdrant_client.recover_snapshot(
        collection_name=qdrant_collection_name,
        location=snapshot_uri
    )
    click.echo(f"✅ Loaded sample collection from {snapshot_uri}")