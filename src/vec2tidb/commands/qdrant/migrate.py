"""Migration subcommand for transferring data from Qdrant to TiDB."""

import time
from typing import Optional
from qdrant_client import QdrantClient
import click

from vec2tidb.processing import process_batches_concurrent
from vec2tidb.tidb import create_tidb_engine
from .common import (
    drop_vector_table,
    create_vector_table,
    check_vector_table,
    insert_points,
    update_points,
)


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
    workers: int = 1,
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
    sample_points = qdrant_client.scroll(
        collection_name=qdrant_collection_name, limit=1
    )
    if sample_points and sample_points[0]:
        sample_point = sample_points[0][0]
        if isinstance(sample_point.id, int):
            id_column_type = "BIGINT"
        elif isinstance(sample_point.id, str):
            id_length = len(sample_point.id)
            id_column_type = f"VARCHAR({id_length})"
        else:
            raise click.BadParameter(
                f"Unsupported Qdrant point ID type: {type(sample_point.id)}"
            )

    # Get collection info to determine vector dimension
    collection_info = qdrant_client.get_collection(
        collection_name=qdrant_collection_name
    )
    vector_dimension = collection_info.config.params.vectors.size
    vector_distance_metric = collection_info.config.params.vectors.distance.lower()

    migration_summary = [
        "=" * 80,
        "🚚 MIGRATION SUMMARY",
        "=" * 80,
        f"{'Property':<20} {'Value':<30} {'Details':<25}",
        "-" * 80,
        f"{'Source Database':<20} {'Qdrant':<30} {'vector database':<25}",
        f"{'Source Collection':<20} {qdrant_collection_name:<30} {'source data':<25}",
        f"{'Vector Count':<20} {str(vector_total):<30} {'records':<25}",
        f"{'Dimension':<20} {str(vector_dimension):<30} {'features':<25}",
        f"{'Distance Metric':<20} {vector_distance_metric:<30} {'similarity function':<25}",
        "",
        f"{'Target Database':<20} {'TiDB':<30} {'relational database':<25}",
        f"{'Target Table':<20} {table_name:<30} {'destination table':<25}",
        f"{'ID Column':<20} {id_column:<30} {f'({id_column_type})':<25}",
        f"{'Vector Column':<20} {vector_column:<30} {'VECTOR type':<25}",
        f"{'Payload Column':<20} {payload_column or 'None':<30} {'JSON type':<25}",
        "",
        f"{'Mode':<20} {mode:<30} {'operation type':<25}",
        f"{'Batch Size':<20} {str(batch_size):<30} {'records per batch':<25}",
        f"{'Workers':<20} {str(workers):<30} {'concurrent threads':<25}",
        "=" * 80,
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

            click.echo(f"⏳ Creating new TiDB table: {table_name}")
            start_time = time.time()
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
            click.echo(f"✅ Created new TiDB table: {table_name} (cost time: {time.time() - start_time:.2f}s)")
        except Exception as e:
            raise click.ClickException(f"Failed to create table: {e}")
    elif mode == "update":
        try:
            check_vector_table(
                db_engine, table_name, id_column, vector_column, payload_column
            )
            click.echo(f"✅ Verified the existing TiDB table: {table_name}")
        except Exception as e:
            raise click.ClickException(f"Failed to check table: {e}")

    # Migrate data with progress tracking
    click.echo("⏳ Starting data migration...\n")

    def batch_generator(batch_size):
        """Generate batches of vectors from Qdrant."""
        current_offset = None
        while True:
            points, next_page_offset = qdrant_client.scroll(
                collection_name=qdrant_collection_name,
                limit=batch_size,
                offset=current_offset,
                with_payload=True,
                with_vectors=True,
            )

            if not points:
                break

            yield points
            current_offset = next_page_offset

            if next_page_offset is None:
                break

    def batch_processor(points):
        """Process a batch of vectors and insert into TiDB."""
        if not points:
            return 0

        # For single worker, reuse the main engine; for multiple workers, create new engine for thread safety
        if workers == 1:
            worker_db_engine = db_engine
            cleanup_engine = False
        else:
            worker_db_engine = create_tidb_engine(tidb_database_url)
            cleanup_engine = True

        try:
            # Insert/update records in TiDB
            if mode == "create":
                insert_points(
                    worker_db_engine,
                    points,
                    table_name,
                    id_column,
                    vector_column,
                    payload_column,
                )
            elif mode == "update":
                update_points(
                    worker_db_engine,
                    points,
                    table_name,
                    id_column,
                    vector_column,
                    payload_column,
                )

            return len(points)
        finally:
            # Clean up the worker engine only if it was created for this worker
            if cleanup_engine:
                worker_db_engine.dispose()

    # Use unified concurrent processing (handles both single and multiple workers)
    processed_total = process_batches_concurrent(
        tasks_total=vector_total,
        batch_generator=batch_generator,
        batch_processor=batch_processor,
        workers=workers,
        batch_size=batch_size,
    )

    click.echo()
    click.echo(
        f"🎉 Migration completed successfully! Migrated {processed_total} points from Qdrant to TiDB."
    ) 