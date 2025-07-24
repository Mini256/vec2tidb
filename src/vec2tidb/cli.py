"""CLI interface for vec2tidb."""

import click
from typing import Optional

from vec2tidb.commands.qdrant import (
    migrate as qdrant_migrate_impl,
    load_sample as qdrant_load_sample_impl,
)


@click.group()
def cli():
    """vec2tidb - A CLI tool for migrating data from third-party vector databases to TiDB."""
    pass


# subcommands

# subcommand: qdrant


@cli.group(name="qdrant")
def qdrant_group():
    """Qdrant database migration commands."""
    pass


# subcommand: qdrant migrate


@qdrant_group.command(name="migrate")
@click.option(
    "--qdrant-api-url",
    envvar="QDRANT_API_URL",
    required=True,
    default="http://localhost:6333",
)
@click.option("--qdrant-api-key", envvar="QDRANT_API_KEY")
@click.option(
    "--qdrant-collection-name", envvar="QDRANT_COLLECTION_NAME", required=True
)
@click.option(
    "--tidb-database-url",
    envvar="TIDB_DATABASE_URL",
    required=True,
    default="mysql+pymysql://root:@localhost:4000/test",
)
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
        "ID column name is required in update mode. "
        "Default to 'id' in create mode."
    ),
)
@click.option(
    "--id-column-type",
    default="BIGINT",
    help=(
        "ID column type, default to 'BIGINT'."
    ),
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
    "--batch-size",
    default=100,
    help="Batch size for migration (default: 100)"
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
            raise ValueError("Option --table-name is required for update mode")
        if not id_column:
            raise ValueError("Option --id-column is required for update mode")
        if not vector_column:
            raise ValueError("Option --vector-column is required for update mode")


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
        drop_table=drop_table,
    )


# subcommand: qdrant load-sample-collection


@qdrant_group.command(name="load-sample")
@click.option(
    "--qdrant-api-url",
    envvar="QDRANT_API_URL",
    required=True,
    default="http://localhost:6333",
)
@click.option("--qdrant-api-key", envvar="QDRANT_API_KEY")
@click.option(
    "--qdrant-collection-name", envvar="QDRANT_COLLECTION_NAME", required=True
)
@click.option(
    "--dataset",
    required=True,
    default="midlib",
    type=click.Choice(["midlib"]),
)
@click.option(
    "--snapshot-uri",
    envvar="SNAPSHOT_URI",
    required=True,
    default="https://snapshots.qdrant.io/midlib.snapshot",
)
def qdrant_load_sample_collection(
    qdrant_api_url: str,
    qdrant_api_key: str,
    qdrant_collection_name: str,
    dataset: str,
    snapshot_uri: str,
):
    """Load a sample collection from a Qdrant collection."""

    if not snapshot_uri:
        if dataset == "midlib":
            snapshot_uri = "https://snapshots.qdrant.io/midlib.snapshot"
        else:
            raise click.UsageError(f"Invalid dataset: {dataset}")

    qdrant_load_sample_impl(
        qdrant_api_url=qdrant_api_url,
        qdrant_api_key=qdrant_api_key,
        qdrant_collection_name=qdrant_collection_name,
        snapshot_uri=snapshot_uri,
    )

if __name__ == "__main__":
    cli()
