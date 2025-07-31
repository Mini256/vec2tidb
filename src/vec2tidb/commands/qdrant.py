import asyncio
from typing import Optional

import click
import json
import subprocess
import sys
import time
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session

from vec2tidb.processing import process_batches_concurrent
from vec2tidb.tidb import create_tidb_engine


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


def load_sample(
    qdrant_api_url: str,
    qdrant_api_key: Optional[str],
    qdrant_collection_name: str,
    snapshot_uri: str,
):
    """Load a sample collection from a Qdrant snapshot."""
    qdrant_client = QdrantClient(url=qdrant_api_url, api_key=qdrant_api_key)
    click.echo(f"⏳ Loading sample collection from {snapshot_uri}...")
    qdrant_client.recover_snapshot(
        collection_name=qdrant_collection_name,
        location=snapshot_uri,
        wait=False,
    )
    click.echo(f"✅ Loaded sample collection in the background")


def benchmark(
    qdrant_api_url: str,
    qdrant_api_key: Optional[str],
    qdrant_collection_name: str,
    tidb_database_url: str,
    worker_list: list[int],
    batch_size_list: list[int],
    table_prefix: str = "benchmark_test",
    cleanup_tables: bool = False,
):
    """Run performance benchmarks with different worker and batch size configurations."""

    # Initialize Qdrant client
    qdrant_client = QdrantClient(url=qdrant_api_url, api_key=qdrant_api_key)
    if not qdrant_client.collection_exists(collection_name=qdrant_collection_name):
        raise click.UsageError(
            f"Qdrant collection '{qdrant_collection_name}' does not exist. "
            f"Use `vec2tidb qdrant load-sample` to load sample data."
        )

    vector_count = qdrant_client.count(collection_name=qdrant_collection_name).count
    if vector_count == 0:
        raise click.UsageError(
            f"Qdrant collection '{qdrant_collection_name}' is empty. "
            f"Use `vec2tidb qdrant load-sample` to load sample data."
        )

    # Get collection info
    collection_info = qdrant_client.get_collection(
        collection_name=qdrant_collection_name
    )
    vector_dimension = collection_info.config.params.vectors.size
    distance_metric = collection_info.config.params.vectors.distance.lower()

    click.echo("🚀 Starting vec2tidb concurrent migration benchmark")
    click.echo("=" * 60)
    click.echo(f"Collection: {qdrant_collection_name}")
    click.echo(f"Vectors: {vector_count}")
    click.echo(f"Dimensions: {vector_dimension}")
    click.echo(f"Distance: {distance_metric}")
    click.echo("=" * 60)

    # Generate test configurations
    test_configs = []
    created_tables = []  # Track tables for potential cleanup
    for workers in worker_list:
        for batch_size in batch_size_list:
            test_configs.append((workers, batch_size))

    results = []

    # Run benchmark tests
    for i, (workers, batch_size) in enumerate(test_configs):
        table_suffix = f"{workers}w_{batch_size}b"
        table_name = f"{table_prefix}_{table_suffix}"
        created_tables.append(table_name)  # Track this table

        click.echo(f"⏳ Testing with workers={workers}, batch_size={batch_size}...")

        # Build command
        cmd = [
            sys.executable,
            "-m",
            "vec2tidb.cli",
            "qdrant",
            "migrate",
            "--qdrant-api-url",
            qdrant_api_url,
            "--qdrant-collection-name",
            qdrant_collection_name,
            "--tidb-database-url",
            tidb_database_url,
            "--table-name",
            table_name,
            "--workers",
            str(workers),
            "--batch-size",
            str(batch_size),
            "--drop-table",
        ]

        if qdrant_api_key:
            cmd.extend(["--qdrant-api-key", qdrant_api_key])

        # Run test
        start_time = time.time()
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            end_time = time.time()
            execution_time = end_time - start_time
            click.echo(f"✅ Completed in {execution_time:.2f}s")
            results.append((workers, batch_size, execution_time))
        except subprocess.CalledProcessError as e:
            click.echo(f"❌ Error: {e}")
            click.echo(f"stderr: {e.stderr}")
            results.append((workers, batch_size, float("inf")))

        # Wait between tests (except for the last one)
        if i < len(test_configs) - 1:
            time.sleep(2)

    # Print results summary
    click.echo("\n" + "=" * 80)
    click.echo("📊 BENCHMARK RESULTS")
    click.echo("=" * 80)
    click.echo(
        f"{'Workers':<8} {'Batch Size':<12} {'Time (s)':<10} {'Records/s':<12} {'Performance':<12}"
    )
    click.echo("-" * 80)

    valid_times = [result[2] for result in results if result[2] != float("inf")]
    best_time = min(valid_times) if valid_times else float("inf")

    for workers, batch_size, execution_time in results:
        if execution_time == float("inf"):
            performance = "FAILED"
            time_str = "FAILED"
            records_per_sec = "FAILED"
        else:
            speedup = best_time / execution_time
            if speedup >= 1.0:
                performance = f"{speedup:.2f}x"
            else:
                performance = f"{1/speedup:.2f}x slower"
            time_str = f"{execution_time:.2f}"
            # Calculate records per second (throughput)
            records_per_sec = f"{vector_count / execution_time:.0f}"

        click.echo(
            f"{workers:<8} {batch_size:<12} {time_str:<10} {records_per_sec:<12} {performance:<12}"
        )

    # Clean up benchmark tables if requested
    if cleanup_tables:
        click.echo("\n🧹 Cleaning up benchmark tables...")
        db_engine = create_tidb_engine(tidb_database_url)
        cleaned_count = 0
        for table_name in created_tables:
            try:
                drop_vector_table(db_engine, table_name)
                cleaned_count += 1
            except Exception as e:
                click.echo(f"❌ Failed to drop table {table_name}: {e}")

        db_engine.dispose()

    click.echo("🎉 Benchmark execution completed!")

    click.echo("\n💡 Recommendations:")
    if results:
        # Find best configuration
        valid_results = [(w, b, t) for w, b, t in results if t != float("inf")]
        if valid_results:
            best_workers, best_batch, best_time = min(valid_results, key=lambda x: x[2])
            best_throughput = vector_count / best_time
            click.echo(
                f"   • Best performance: {best_workers} workers, batch size {best_batch}"
            )
            click.echo(
                f"   • Completed in {best_time:.2f} seconds ({best_throughput:.0f} records/s)"
            )
        else:
            click.echo("   • All tests failed. Check your database connections.")

    click.echo(f"   • For production use, consider your system's CPU cores and memory")
    click.echo(f"   • Monitor database connection limits when using many workers")


async def dump(
    qdrant_api_url: str,
    qdrant_api_key: Optional[str],
    qdrant_collection_name: str,
    output_file: str,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    include_vectors: bool = True,
    include_payload: bool = True,
    batch_size: int = 500,
    max_concurrent_batches: int = 5,
    buffer_size: int = 10000,
):
    """Export Qdrant collection data to CSV format using optimized batch processing."""
    
    import csv
    import os
    import asyncio
    from qdrant_client import AsyncQdrantClient
    from tqdm import tqdm
    from collections import deque

    qdrant_client = AsyncQdrantClient(
        url=qdrant_api_url, 
        api_key=qdrant_api_key,
        timeout=60.0,
    )

    try:
        # Check if collection exists
        collection_exists = await qdrant_client.collection_exists(collection_name=qdrant_collection_name)
        if not collection_exists:
            raise click.UsageError(
                f"Requested Qdrant collection '{qdrant_collection_name}' does not exist"
            )

        # Get collection info
        collection_info = await qdrant_client.get_collection(
            collection_name=qdrant_collection_name
        )
        vector_dimension = collection_info.config.params.vectors.size
        vector_distance_metric = collection_info.config.params.vectors.distance.lower()
        
        # Get total count
        count_result = await qdrant_client.count(collection_name=qdrant_collection_name)
        total_count = count_result.count
        if total_count == 0:
            raise click.UsageError(
                f"No records present in requested Qdrant collection '{qdrant_collection_name}'"
            )
        
        # Set limit to total count if not specified
        if limit is None:
            limit = total_count
        
        actual_limit = min(limit, total_count)
        
        click.echo(f"🚀 Optimized export of {actual_limit} records from collection '{qdrant_collection_name}'")
        click.echo(f"📁 Output file: {output_file}")
        click.echo(f"🔢 Vector dimension: {vector_dimension}")
        click.echo(f"📏 Distance metric: {vector_distance_metric}")
        click.echo(f"📋 Include vectors: {include_vectors}")
        click.echo(f"📄 Include payload: {include_payload}")
        click.echo(f"📦 Batch size: {batch_size}")
        click.echo(f"⚡ Max concurrent batches: {max_concurrent_batches}")
        click.echo(f"💾 Buffer size: {buffer_size}")
        click.echo()
        
        # Create output directory if it doesn't exist
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            click.echo(f"📁 Created output directory: {output_dir}")
        
        # Prepare CSV headers
        headers = ['id']
        if include_vectors:
            headers.extend([f'vector_{i}' for i in range(vector_dimension)])
        if include_payload:
            headers.append('payload')
        
        # Pre-compile JSON serialization for payload
        json_dumps = json.dumps

        # Add elapsed time for the dump process
        start_time = time.time()
        
        # Use buffered writing for better performance
        with open(output_file, 'w', newline='', encoding='utf-8', buffering=buffer_size) as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)
            
            # Calculate total batches
            total_batches = (actual_limit + batch_size - 1) // batch_size
            
            # Create progress bar
            with tqdm(total=actual_limit, desc="Exporting", unit=" records") as pbar:
                current_offset = offset or 0
                records_exported = 0
                
                # Use semaphore to limit concurrent batches
                semaphore = asyncio.Semaphore(max_concurrent_batches)
                
                async def fetch_batch(batch_offset, batch_size_limit):
                    """Fetch a single batch with semaphore control."""
                    async with semaphore:
                        max_retries = 3
                        for attempt in range(max_retries):
                            try:
                                points, next_offset = await qdrant_client.scroll(
                                    collection_name=qdrant_collection_name,
                                    limit=batch_size_limit,
                                    offset=batch_offset,
                                    with_payload=include_payload,
                                    with_vectors=include_vectors,
                                )
                                return points, next_offset
                            except Exception as e:
                                if "Message too long" in str(e) and batch_size_limit > 100:
                                    # Reduce batch size for gRPC message size issues
                                    new_batch_size = batch_size_limit // 2
                                    click.echo(f"⚠️ gRPC message too long, reducing batch size from {batch_size_limit} to {new_batch_size}")
                                    if new_batch_size >= 100:
                                        return await fetch_batch(batch_offset, new_batch_size)
                                elif attempt < max_retries - 1:
                                    click.echo(f"⚠️ Error fetching batch at offset {batch_offset} (attempt {attempt + 1}/{max_retries}): {e}")
                                    await asyncio.sleep(1)  # Wait before retry
                                    continue
                                else:
                                    click.echo(f"❌ Failed to fetch batch at offset {batch_offset} after {max_retries} attempts: {e}")
                                    return [], None
                        return [], None
                
                # Process batches with controlled concurrency
                pending_batches = deque()
                completed_batches = deque()
                
                # Start initial batch requests
                for i in range(min(max_concurrent_batches, total_batches)):
                    if records_exported >= actual_limit:
                        break
                    
                    batch_offset = current_offset + (i * batch_size)
                    current_batch_size = min(batch_size, actual_limit - records_exported - (i * batch_size))
                    
                    if current_batch_size > 0:
                        task = asyncio.create_task(fetch_batch(batch_offset, current_batch_size))
                        pending_batches.append((task, batch_offset))
                
                # Process batches as they complete
                while pending_batches and records_exported < actual_limit:
                    # Wait for next batch to complete
                    done, pending = await asyncio.wait(
                        [task for task, _ in pending_batches],
                        return_when=asyncio.FIRST_COMPLETED
                    )
                    
                    # Process completed batches
                    for task in done:
                        points, next_offset = await task
                        
                        if points:
                            # Optimize CSV writing with batch processing
                            rows = []
                            for point in points:
                                row = [point.id]
                                
                                if include_vectors:
                                    row.extend(point.vector)
                                
                                if include_payload:
                                    row.append(json_dumps(point.payload) if point.payload else '')
                                
                                rows.append(row)
                            
                            # Write all rows at once
                            writer.writerows(rows)
                            
                            # Update progress
                            records_exported += len(points)
                            pbar.update(len(points))
                        
                        # Remove completed task from pending
                        pending_batches = deque((t, offset) for t, offset in pending_batches if t != task)
                        
                        # Start new batch if needed
                        if records_exported < actual_limit and len(pending_batches) < max_concurrent_batches:
                            next_batch_offset = current_offset + (len(pending_batches) + len(completed_batches)) * batch_size
                            remaining_records = actual_limit - records_exported
                            current_batch_size = min(batch_size, remaining_records)
                            
                            if current_batch_size > 0:
                                new_task = asyncio.create_task(fetch_batch(next_batch_offset, current_batch_size))
                                pending_batches.append((new_task, next_batch_offset))
                
                # Wait for remaining batches
                if pending_batches:
                    remaining_tasks = [task for task, _ in pending_batches]
                    remaining_results = await asyncio.gather(*remaining_tasks, return_exceptions=True)
                    
                    for result in remaining_results:
                        if isinstance(result, Exception):
                            click.echo(f"⚠️ Error in remaining batch: {result}")
                        elif result[0]:  # points exist
                            points, _ = result
                            rows = []
                            for point in points:
                                row = [point.id]
                                
                                if include_vectors:
                                    row.extend(point.vector)
                                
                                if include_payload:
                                    row.append(json_dumps(point.payload) if point.payload else '')
                                
                                rows.append(row)
                            
                            writer.writerows(rows)
                            records_exported += len(points)
                            pbar.update(len(points))
        
        # Get final file size
        file_size = os.path.getsize(output_file)
        file_size_mb = file_size / (1024 * 1024)
        elapsed_time = time.time() - start_time
        
        click.echo()
        click.echo(f"🚀 Optimized export completed successfully!")
        click.echo(f"📊 Records exported: {records_exported}")
        click.echo(f"📁 File size: {file_size_mb:.2f} MB")
        click.echo(f"📄 Output file: {output_file}")
        click.echo(f"⏱️ Elapsed time: {elapsed_time:.2f} seconds")
        
    finally:
        # Close the async client
        await qdrant_client.close()


def dump_sync(
    qdrant_api_url: str,
    qdrant_api_key: Optional[str],
    qdrant_collection_name: str,
    output_file: str,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    include_vectors: bool = True,
    include_payload: bool = True,
    batch_size: int = 500,
    max_concurrent_batches: int = 5,
    buffer_size: int = 10000,
):
    """Synchronous wrapper for the async dump function."""
    return asyncio.run(dump(
        qdrant_api_url=qdrant_api_url,
        qdrant_api_key=qdrant_api_key,
        qdrant_collection_name=qdrant_collection_name,
        output_file=output_file,
        limit=limit,
        offset=offset,
        include_vectors=include_vectors,
        include_payload=include_payload,
        batch_size=batch_size,
        max_concurrent_batches=max_concurrent_batches,
        buffer_size=buffer_size,
    ))

