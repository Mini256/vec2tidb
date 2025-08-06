"""Benchmark subcommand for performance testing."""

import subprocess
import sys
import time
from typing import Optional
from qdrant_client import QdrantClient
import click

from vec2tidb.tidb import create_tidb_engine
from .common import drop_vector_table


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