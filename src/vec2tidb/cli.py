"""CLI interface for vec2tidb."""

import click
import logging
from typing import Optional
from dotenv import load_dotenv

from vec2tidb.commands.qdrant.migrate import migrate as qdrant_migrate_impl
from vec2tidb.commands.qdrant.load_sample import load_sample as qdrant_load_sample_impl
from vec2tidb.commands.qdrant.benchmark import benchmark as qdrant_benchmark_impl
from vec2tidb.commands.qdrant.dump import dump_sync as qdrant_dump_impl
from vec2tidb.commands.qdrant.common import get_snapshot_uri
from vec2tidb.commands.tidb.batch_update import batch_update_impl


load_dotenv()


def setup_logging(verbose: bool = False):
    """Setup logging configuration for CLI."""
    # Configure logging to output to console
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.StreamHandler()
        ]
    )


@click.group()
def cli():
    """vec2tidb - A CLI tool for migrating data from third-party vector databases to TiDB."""
    # Setup logging when CLI is invoked
    setup_logging()
    pass


# Common options for reuse across commands
def tidb_connection_options(f):
    """Common TiDB connection options."""
    f = click.option(
        "--tidb-database-url",
        envvar="TIDB_DATABASE_URL",
        default="mysql+pymysql://root:@localhost:4000/test",
        help="TiDB database URL (default: mysql+pymysql://root:@localhost:4000/test)",
    )(f)
    return f


# Subcommands


# Subcommand: tidb
@cli.group(name="tidb")
def tidb_group():
    """TiDB database operations commands."""
    pass


# Subcommand: qdrant
@cli.group(name="qdrant")
def qdrant_group():
    """Qdrant database migration commands."""
    pass


# Common options for qdrant


def qdrant_connection_options(f):
    """Common Qdrant connection options."""
    f = click.option(
        "--qdrant-api-url",
        envvar="QDRANT_API_URL",
        default="http://localhost:6333",
        help="Qdrant API URL (default: http://localhost:6333)",
    )(f)
    f = click.option(
        "--qdrant-api-key",
        envvar="QDRANT_API_KEY",
        help="Qdrant API key (if authentication is enabled)",
    )(f)
    f = click.option(
        "--qdrant-collection-name",
        envvar="QDRANT_COLLECTION_NAME",
        required=True,
        help="Qdrant collection name",
    )(f)
    return f


# Subcommand: qdrant migrate
@qdrant_group.command(name="migrate")
@qdrant_connection_options
@tidb_connection_options
@click.option(
    "--mode",
    type=click.Choice(["create", "update"]),
    default="create",
    help=(
        "Migration mode. "
        "create: Create a new TiDB table and migrate all data from Qdrant collection. "
        "update: Update an existing TiDB table by matching IDs and updating specified columns."
    ),
)
@click.option(
    "--table-name",
    help=(
        "Target table name is required in update mode. "
        "Default to collection name in create mode."
    ),
)
@click.option(
    "--id-column",
    help=(
        "ID column name is required in update mode. " "Default to 'id' in create mode."
    ),
)
@click.option(
    "--id-column-type",
    default="BIGINT",
    help=("ID column type, default to 'BIGINT'."),
)
@click.option(
    "--vector-column",
    help=(
        "Vector column name is required in update mode. "
        "Default to 'vector' in create mode."
    ),
)
@click.option(
    "--payload-column",
    help=(
        "Payload column name is optional in update mode. "
        "Default to 'payload' in create mode."
    ),
)
@click.option(
    "--batch-size", default=100, help="Batch size for migration (default: 100)"
)
@click.option(
    "--workers",
    default=1,
    help="Number of concurrent workers for migration (default: 1)",
)
@click.option(
    "--drop-table",
    is_flag=True,
    help="Drop the target table if it exists.",
)
def qdrant_migrate(
    mode: str,
    qdrant_api_url: str,
    qdrant_api_key: str,
    qdrant_collection_name: str,
    tidb_database_url: str,
    table_name: Optional[str],
    id_column: Optional[str],
    id_column_type: Optional[str],
    vector_column: Optional[str],
    payload_column: Optional[str],
    batch_size: Optional[int] = 100,
    workers: Optional[int] = 1,
    drop_table: bool = False,
):
    """Migrate vector data from a Qdrant collection to a TiDB table."""

    # Validate mode
    if mode not in ["create", "update"]:
        raise ValueError(f"Invalid mode: {mode}")

    # Validate and set default values for parameters.
    if mode == "create":
        if not table_name:
            table_name = qdrant_collection_name
        if not id_column:
            id_column = "id"
        if not vector_column:
            vector_column = "vector"
        if not payload_column:
            payload_column = "payload"
    elif mode == "update":
        if not table_name:
            raise click.UsageError("Option --table-name is required for update mode")
        if not id_column:
            raise click.UsageError("Option --id-column is required for update mode")
        if not vector_column:
            raise click.UsageError("Option --vector-column is required for update mode")

    qdrant_migrate_impl(
        mode=mode,
        qdrant_api_url=qdrant_api_url,
        qdrant_api_key=qdrant_api_key,
        qdrant_collection_name=qdrant_collection_name,
        tidb_database_url=tidb_database_url,
        table_name=table_name,
        id_column=id_column,
        id_column_type=id_column_type,
        vector_column=vector_column,
        payload_column=payload_column,
        batch_size=batch_size,
        workers=workers,
        drop_table=drop_table,
    )


# Subcommand: qdrant load-sample
@qdrant_group.command(name="load-sample")
@qdrant_connection_options
@click.option(
    "--dataset",
    required=True,
    default="midlib",
    type=click.Choice(["midlib", "qdrant-docs", "prefix-cache"]),
    help="Sample dataset to load (default: midlib)",
)
@click.option(
    "--snapshot-uri",
    envvar="SNAPSHOT_URI",
    help="Custom snapshot URI (auto-determined from dataset if not provided)",
)
def qdrant_load_sample(
    qdrant_api_url: str,
    qdrant_api_key: str,
    qdrant_collection_name: str,
    dataset: str,
    snapshot_uri: Optional[str],
):
    """Load a sample collection from a Qdrant collection."""

    resolved_snapshot_uri = get_snapshot_uri(dataset=dataset, snapshot_uri=snapshot_uri)

    qdrant_load_sample_impl(
        qdrant_api_url=qdrant_api_url,
        qdrant_api_key=qdrant_api_key,
        qdrant_collection_name=qdrant_collection_name,
        snapshot_uri=resolved_snapshot_uri,
    )


# Subcommand: qdrant benchmark
@qdrant_group.command(name="benchmark")
@qdrant_connection_options
@tidb_connection_options
@click.option(
    "--workers",
    default="1,2,4,8",
    help="Comma-separated list of worker counts to test (default: 1,2,4,8)",
)
@click.option(
    "--batch-sizes",
    default="100,500,1000",
    help="Comma-separated list of batch sizes to test (default: 100,500,1000)",
)
@click.option(
    "--table-prefix",
    default="benchmark_test",
    help="Prefix for benchmark table names (default: benchmark_test)",
)
@click.option(
    "--cleanup-tables",
    is_flag=True,
    help="Drop all benchmark tables after tests are completed",
)
def qdrant_benchmark(
    qdrant_api_url: str,
    qdrant_api_key: str,
    qdrant_collection_name: str,
    tidb_database_url: str,
    workers: str,
    batch_sizes: str,
    table_prefix: str,
    cleanup_tables: bool,
):
    """Run performance benchmarks with different worker and batch size configurations."""

    worker_list = [int(w.strip()) for w in workers.split(",")]
    batch_size_list = [int(b.strip()) for b in batch_sizes.split(",")]

    qdrant_benchmark_impl(
        qdrant_api_url=qdrant_api_url,
        qdrant_api_key=qdrant_api_key,
        qdrant_collection_name=qdrant_collection_name,
        tidb_database_url=tidb_database_url,
        worker_list=worker_list,
        batch_size_list=batch_size_list,
        table_prefix=table_prefix,
        cleanup_tables=cleanup_tables,
    )


# Subcommand: qdrant dump
@qdrant_group.command(name="dump")
@qdrant_connection_options
@click.option(
    "--output-file",
    required=True,
    help="Output CSV file path",
)
@click.option(
    "--limit",
    type=int,
    help="Maximum number of records to export (default: all records)",
)
@click.option(
    "--offset",
    type=int,
    help="Number of records to skip before starting export",
)
@click.option(
    "--no-vectors",
    is_flag=True,
    help="Exclude vector data from export",
)
@click.option(
    "--no-payload",
    is_flag=True,
    help="Exclude payload data from export",
)
@click.option(
    "--batch-size",
    default=500,
    help="Batch size for processing (default: 500)",
)
@click.option(
    "--buffer-size",
    default=10000,
    help="File buffer size in bytes (default: 10000)",
)
def qdrant_dump(
    qdrant_api_url: str,
    qdrant_api_key: str,
    qdrant_collection_name: str,
    output_file: str,
    limit: Optional[int],
    offset: Optional[int],
    no_vectors: bool,
    no_payload: bool,
    batch_size: int,
    buffer_size: int,
):
    """Export Qdrant collection data to CSV format with optimized performance."""

    qdrant_dump_impl(
        qdrant_api_url=qdrant_api_url,
        qdrant_api_key=qdrant_api_key,
        qdrant_collection_name=qdrant_collection_name,
        output_file=output_file,
        limit=limit,
        offset=offset,
        include_vectors=not no_vectors,
        include_payload=not no_payload,
        batch_size=batch_size,
        buffer_size=buffer_size,
    )


# Subcommand: tidb batch-update
@tidb_group.command(name="batch-update")
@tidb_connection_options
@click.option(
    "--source-table",
    required=True,
    help="Source table name",
)
@click.option(
    "--source-id-column",
    required=True,
    help="ID column name in source table",
)
@click.option(
    "--target-table",
    required=True,
    help="Target table name",
)
@click.option(
    "--target-id-column",
    required=True,
    help="ID column name in target table",
)
@click.option(
    "--column-mapping",
    required=True,
    help="Column mapping in format 'target_col1:source_col1,target_col2:source_col2'",
)
@click.option(
    "--batch-size",
    default=5000,
    help="Batch size for processing (default: 5000)",
)
@click.option(
    "--compact",
    is_flag=True,
    help="Execute ALTER TABLE COMPACT on target table before updating",
)
@click.option(
    "--verbose",
    is_flag=True,
    help="Enable verbose logging",
)
def tidb_batch_update(
    tidb_database_url: str,
    source_table: str,
    source_id_column: str,
    target_table: str,
    target_id_column: str,
    column_mapping: str,
    batch_size: int,
    compact: bool,
    verbose: bool,
):
    """Batch update target table with data from source table based on ID matching."""
    
    # Parse column mapping
    try:
        mapping_dict = {}
        for mapping in column_mapping.split(","):
            if ":" not in mapping:
                raise ValueError(f"Invalid column mapping format: {mapping}")
            source_col, target_col = mapping.split(":", 1)
            mapping_dict[source_col.strip()] = target_col.strip()
        
        if not mapping_dict:
            raise ValueError("No valid column mappings provided")
            
    except Exception as e:
        raise click.UsageError(f"Invalid column mapping format: {e}")
    
    # Setup logging based on verbose flag
    setup_logging(verbose=verbose)
    
    batch_update_impl(
        tidb_database_url=tidb_database_url,
        source_table=source_table,
        source_id_column=source_id_column,
        target_table=target_table,
        target_id_column=target_id_column,
        column_mapping=mapping_dict,
        batch_size=batch_size,
        compact=compact,
    )


if __name__ == "__main__":
    cli()
