import uuid
import logging

from qdrant_client import QdrantClient
from sqlalchemy import create_engine, text


TIDB_DATABASE_URL = "mysql+pymysql://root@localhost:4000/test"
QDRANT_API_URL = "http://localhost:6333"



def generate_unique_name(prefix: str = "test_vec2tidb") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def check_qdrant_available():
    """Check if Qdrant is available."""
    try:
        client = QdrantClient(url=QDRANT_API_URL)
        client.get_collections()
        return True
    except:
        return False


def check_tidb_available():
    """Check if TiDB is available."""
    try:
        engine = create_engine(TIDB_DATABASE_URL)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as e:
        logging.error(f"TiDB is not available: {e}")
        return False