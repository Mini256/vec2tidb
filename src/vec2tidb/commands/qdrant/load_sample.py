"""Load sample subcommand for loading sample data from Qdrant snapshots."""

from typing import Optional
from qdrant_client import QdrantClient
import click


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