"""Pytest configuration and shared fixtures."""

import pytest
import os
import logging
from dotenv import load_dotenv
from qdrant_client import QdrantClient
from sqlalchemy import create_engine, Engine, text


@pytest.fixture(scope="session", autouse=True)
def env():
    """Load environment variables from .env file."""
    print("Loading environment variables")
    load_dotenv("tests/.env")


@pytest.fixture(scope="session")
def qdrant_api_url():
    """Get Qdrant API URL from environment."""
    return os.getenv("QDRANT_API_URL", "http://localhost:6333")


@pytest.fixture(scope="session")
def qdrant_api_key():
    """Get Qdrant API key from environment."""
    return os.getenv("QDRANT_API_KEY")


@pytest.fixture(scope="session")
def tidb_database_url():
    """Get TiDB database URL from environment."""
    return os.getenv("TIDB_DATABASE_URL", "mysql+pymysql://root@localhost:4000/test")


@pytest.fixture(scope="session")
def qdrant_client(qdrant_api_url, qdrant_api_key):
    """Create Qdrant client with proper authentication."""
    if qdrant_api_key:
        return QdrantClient(url=qdrant_api_url, api_key=qdrant_api_key)
    else:
        return QdrantClient(url=qdrant_api_url)


@pytest.fixture(scope="session")
def tidb_engine(tidb_database_url):
    """Create TiDB SQLAlchemy engine."""
    return create_engine(tidb_database_url)


@pytest.fixture(scope="session")
def qdrant_available(qdrant_client):
    """Check if Qdrant is available and skip tests if not."""
    try:
        qdrant_client.get_collections()
        return True
    except Exception as e:
        logging.error(f"Qdrant is not available: {e}")
        pytest.skip(f"Qdrant not available: {e}")


@pytest.fixture(scope="session")
def tidb_available(tidb_engine):
    """Check if TiDB is available and skip tests if not."""
    try:
        with tidb_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as e:
        logging.error(f"TiDB is not available: {e}")
        pytest.skip(f"TiDB not available: {e}")

