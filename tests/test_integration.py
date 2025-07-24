"""Integration tests with real local databases."""

from typing import Generator
import pytest
import json
import time
from qdrant_client import QdrantClient
from qdrant_client.http.models.models import CollectionInfo
from sqlalchemy import create_engine, Engine, text
from sqlalchemy.orm import Session


from click.testing import CliRunner, Result
from vec2tidb.cli import cli
from vec2tidb.commands.qdrant import get_snapshot_uri
from tests.utils import generate_unique_name, check_qdrant_available, check_tidb_available, QDRANT_API_URL, TIDB_DATABASE_URL


def run_cli_command(args: list[str]) -> Result:
    """Run CLI command with given arguments and return the result."""
    runner = CliRunner()
    return runner.invoke(cli, args, catch_exceptions=False)


@pytest.fixture(scope="session")
def qdrant_client():
    return QdrantClient(url=QDRANT_API_URL)


@pytest.fixture(scope="session")
def sample_collection_name():
    return generate_unique_name("midlib")


@pytest.fixture(scope="session")
def sample_collection(qdrant_client, sample_collection_name) -> Generator[CollectionInfo, None, None]:
    collection_name = sample_collection_name

    try:
        # Recover from snapshot - this will automatically create the collection
        print(f"📝 Recovering collection '{collection_name}' from snapshot...")
        midlib_snapshot_uri = get_snapshot_uri(dataset="midlib")
        qdrant_client.recover_snapshot(
            collection_name=collection_name,
            location=midlib_snapshot_uri
        )
        print(f"✅ Created collection '{collection_name}' and recovered sample data from snapshot")

        # Get collection info.
        time.sleep(5)
        collection = qdrant_client.get_collection(collection_name=collection_name)
        
        # Verify collection exists after creation
        if not qdrant_client.collection_exists(collection_name=collection_name):
            pytest.fail(f"Collection '{collection_name}' does not exist after creation")
        print(f"✅ Collection '{collection_name}' exists after creation")

        yield collection

        # Clean up
        qdrant_client.delete_collection(collection_name=collection_name)
    except Exception as e:
        print(f"❌ Failed to create sample collection: {e}")
        pytest.skip(f"Qdrant sample collection setup failed: {e}")


@pytest.fixture(scope="session")
def tidb_engine():
    return create_engine(TIDB_DATABASE_URL)


@pytest.mark.skipif(not check_qdrant_available(), reason="Qdrant not available")
@pytest.mark.skipif(not check_tidb_available(), reason="TiDB not available")
def test_qdrant_migration_create_mode(
    qdrant_client: QdrantClient,
    sample_collection_name: str,
    sample_collection: CollectionInfo,
    tidb_engine: Engine,
):
    """Test CLI migration in create mode."""

    collection_name = sample_collection_name
    table_name = collection_name  # vec2tidb uses collection name as table name by default.
    vectors_count = qdrant_client.count(collection_name=collection_name).count
    vector_dimension = sample_collection.config.params.vectors.size
    distance_metric = sample_collection.config.params.vectors.distance
    
    print(f"🚀 Testing CLI migration in create mode...")
    print(f"   Source: Qdrant collection '{collection_name}'")
    print(f"   Target: TiDB table '{table_name}'")
    print(f"   Vector count: {vectors_count}")
    print(f"   Vector dimension: {vector_dimension}")
    print(f"   Distance metric: {distance_metric}")
    
    # Verify collection exists before running CLI
    if not qdrant_client.collection_exists(collection_name=collection_name):
        pytest.fail(f"Collection '{collection_name}' does not exist before CLI execution")
    print(f"✅ Verified collection '{collection_name}' exists before CLI execution")

    run_cli_command([
        "qdrant",
        "migrate",
        "--qdrant-api-url", QDRANT_API_URL,
        "--qdrant-collection-name", collection_name,
        "--tidb-database-url", TIDB_DATABASE_URL,
        "--mode", "create"
    ])
    
    # Check table exists and has VECTOR column.
    with Session(tidb_engine) as session:
        columns = session.execute(text(f"SHOW COLUMNS FROM {table_name}")).fetchall()
        column_names = [col[0] for col in columns]
        print(f"📋 Table columns: {column_names}")
        
        # Verify VECTOR column exists
        assert "vector" in column_names, "VECTOR column should exist"
        
        # Check record count
        records_count = session.execute(text(f"SELECT COUNT(*) FROM `{table_name}`")).scalar()
        print(f"✅ Table {table_name} exists with {records_count} records")
        assert records_count == vectors_count, (
            f"The records count in the target table is not equal "
            f"to the vectors count in the source collection: "
            f"{records_count} != {vectors_count}."
        )
        
        # Check vector dimension
        sample_vector_str = session.execute(text(f"SELECT vector FROM {table_name} LIMIT 1")).scalar()
        sample_vector = json.loads(sample_vector_str)
        assert len(sample_vector) == vector_dimension

        # Test vector similarity search
        search_sql = f"""
        SELECT id, VEC_COSINE_DISTANCE(vector, :vector) as distance 
        FROM {table_name} 
        ORDER BY distance ASC 
        LIMIT 3
        """
        results = session.execute(text(search_sql), {
            "vector": sample_vector_str,
        }).fetchall()
        print(f"🔍 Vector similarity search: {len(results)} results")
        for row in results:
            print(f"   - ID: {row[0]}, Distance: {row[1]:.4f}")
    
    print("✅ CLI migration test completed!")
    
    # Clean up
    with Session(tidb_engine) as session:
        session.execute(text(f"DROP TABLE IF EXISTS `{table_name}`"))
        session.commit()


@pytest.fixture(scope="session")
def sample_table(
    tidb_engine: Engine,
    qdrant_client: QdrantClient,
    sample_collection_name: str,
    sample_collection: CollectionInfo,
) -> Generator[str, None, None]:
    table_name = generate_unique_name("midlib_update")

    # Get 20 points from the sample collection.
    points, _ = qdrant_client.scroll(
        collection_name=sample_collection_name,
        limit=20,
    )

    # Create the vector table need to be updated.
    with Session(tidb_engine) as session:
        session.execute(text(f"""
            CREATE TABLE {table_name} (
                id VARCHAR(255) PRIMARY KEY,
                vector VECTOR({sample_collection.config.params.vectors.size}),
                payload JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
        """))
        # Insert records one by one to avoid SQLAlchemy parameter binding issues.
        insert_records = [{"id": point.id} for point in points]
        # Use executemany-style batch insert with SQLAlchemy's parameter style for PyMySQL
        session.execute(
            text(f"INSERT INTO {table_name} (id) VALUES (:id)"),
            insert_records
        )
        session.commit()
    
    yield table_name

    with Session(tidb_engine) as session:
        session.execute(text(f"DROP TABLE IF EXISTS `{table_name}`"))
        session.commit()


@pytest.mark.skipif(not check_qdrant_available(), reason="Qdrant not available")
@pytest.mark.skipif(not check_tidb_available(), reason="TiDB not available")
def test_qdrant_migration_update_mode(
    qdrant_client: QdrantClient,
    sample_collection_name: str,
    sample_collection: CollectionInfo,
    tidb_engine: Engine,
    sample_table: str,
):
    """Test CLI migration in update mode."""

    collection_name = sample_collection_name
    table_name = sample_table
    vectors_count = qdrant_client.count(collection_name=collection_name).count
    vector_dimension = sample_collection.config.params.vectors.size
    distance_metric = sample_collection.config.params.vectors.distance.lower()

    print(f"🚀 Testing CLI migration in update mode...")
    print(f"   Source: Qdrant collection '{collection_name}'")
    print(f"   Target: TiDB table '{table_name}'")
    print(f"   Vector count: {vectors_count}")
    print(f"   Vector dimension: {vector_dimension}")
    print(f"   Distance metric: {distance_metric}")

    # Now run CLI migration in update mode
    print(f"🚀 Testing CLI migration in update mode...")
    run_cli_command([
        "qdrant",
        "migrate",
        "--qdrant-api-url", QDRANT_API_URL,
        "--qdrant-collection-name", collection_name,
        "--tidb-database-url", TIDB_DATABASE_URL,
        "--table-name", table_name,
        "--id-column", "id",
        "--vector-column", "vector",
        "--payload-column", "payload",
        "--mode", "update",
    ])

    # Check table exists and has all records
    with Session(tidb_engine) as session:
        # Check table structure
        columns = session.execute(text(f"SHOW COLUMNS FROM {table_name}")).fetchall()
        column_names = [col[0] for col in columns]
        print(f"📋 Table columns: {column_names}")

        # Verify VECTOR column exists
        assert "vector" in column_names, "VECTOR column should exist"

        # Check that there are no records no vector and no payload.
        null_both_count = session.execute(
            text(f"SELECT COUNT(*) FROM {table_name} WHERE vector IS NULL AND payload IS NULL")
        ).scalar()
        print(f"🧪 Records with both NULL vector and NULL payload: {null_both_count}")
        assert null_both_count == 0, (
            f"There are records with both NULL vector and NULL payload in the target table: {null_both_count}."
        )

        # Check vector dimension
        sample_vector_str = session.execute(text(f"SELECT vector FROM {table_name} LIMIT 1")).scalar()
        sample_vector = json.loads(sample_vector_str)
        assert len(sample_vector) == vector_dimension

        # Test vector similarity search
        search_sql = f"""
        SELECT id, VEC_COSINE_DISTANCE(vector, :vector) as distance 
        FROM {table_name} 
        ORDER BY distance ASC 
        LIMIT 3
        """
        results = session.execute(text(search_sql), {
            "vector": sample_vector_str,
        }).fetchall()
        print(f"🔍 Vector similarity search after update: {len(results)} results")
        for row in results:
            print(f"   - ID: {row[0]}, Distance: {row[1]:.4f}")

    print("✅ CLI migration update mode test completed!")

    # Clean up
    with Session(tidb_engine) as session:
        session.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        session.commit()


@pytest.mark.skipif(not check_qdrant_available(), reason="Qdrant not available")
def test_qdrant_load_sample_command(qdrant_client: QdrantClient):
    """Test CLI load-sample command."""
    collection_name = generate_unique_name("sample_test")
    
    print(f"🚀 Testing CLI load-sample command...")
    print(f"   Collection: {collection_name}")
    
    try:
        # Run load-sample command
        result = run_cli_command([
            "qdrant",
            "load-sample",
            "--qdrant-api-url", QDRANT_API_URL,
            "--qdrant-collection-name", collection_name,
            "--dataset", "midlib"
        ])
        
        assert result.exit_code == 0, f"Load-sample command should succeed: {result.stdout}"
        
        # Wait a bit for the snapshot recovery to complete
        time.sleep(10)
        
        # Verify collection was created
        assert qdrant_client.collection_exists(collection_name=collection_name), "Collection should exist after load-sample"
        
        # Verify collection has data
        count = qdrant_client.count(collection_name=collection_name).count
        assert count > 0, "Collection should have data after load-sample"
        
        print(f"✅ Load-sample test completed! Created collection with {count} points")
        
    finally:
        # Clean up
        if qdrant_client.collection_exists(collection_name=collection_name):
            qdrant_client.delete_collection(collection_name=collection_name)


@pytest.mark.skipif(not check_qdrant_available(), reason="Qdrant not available")
@pytest.mark.skipif(not check_tidb_available(), reason="TiDB not available")
def test_qdrant_migration_with_drop_table(
    sample_collection_name: str,
    sample_collection: CollectionInfo,
    tidb_engine: Engine,
):
    """Test CLI migration with drop-table flag."""
    collection_name = sample_collection_name
    table_name = generate_unique_name("drop_test")
    
    print(f"🚀 Testing CLI migration with drop-table flag...")
    print(f"   Collection: {collection_name}")
    print(f"   Table: {table_name}")
    
    # Create a table first
    with Session(tidb_engine) as session:
        session.execute(text(f"""
            CREATE TABLE {table_name} (
                old_column VARCHAR(255)
            )
        """))
        session.commit()
    
    # Run migration with drop-table flag
    result = run_cli_command([
        "qdrant",
        "migrate",
        "--qdrant-api-url", QDRANT_API_URL,
        "--qdrant-collection-name", collection_name,
        "--tidb-database-url", TIDB_DATABASE_URL,
        "--table-name", table_name,
        "--mode", "create",
        "--drop-table"
    ])
    
    assert result.exit_code == 0, f"Migration with drop-table should succeed: {result.stdout}"
    
    # Verify table was recreated with correct schema
    with Session(tidb_engine) as session:
        columns = session.execute(text(f"SHOW COLUMNS FROM {table_name}")).fetchall()
        column_names = [col[0] for col in columns]
        print(f"📋 New table columns: {column_names}")
        
        # Should have vector columns, not the old_column
        assert "vector" in column_names, "New table should have vector column"
        assert "old_column" not in column_names, "Old column should be gone"
    
    print("✅ Drop-table test completed!")
    
    # Clean up
    with Session(tidb_engine) as session:
        session.execute(text(f"DROP TABLE IF EXISTS `{table_name}`"))
        session.commit()


@pytest.mark.skipif(not check_qdrant_available(), reason="Qdrant not available")
@pytest.mark.skipif(not check_tidb_available(), reason="TiDB not available")
def test_qdrant_benchmark_command(
    sample_collection_name: str,
    sample_collection: CollectionInfo,
    tidb_engine: Engine,
):
    """Test CLI benchmark command."""
    collection_name = sample_collection_name
    table_prefix = generate_unique_name("bench")
    
    print(f"🚀 Testing CLI benchmark command...")
    print(f"   Collection: {collection_name}")
    print(f"   Table prefix: {table_prefix}")
    
    # Run benchmark with small worker and batch size list for testing
    result = run_cli_command([
        "qdrant",
        "benchmark",
        "--qdrant-api-url", QDRANT_API_URL,
        "--qdrant-collection-name", collection_name,
        "--tidb-database-url", TIDB_DATABASE_URL,
        "--workers", "1,2",
        "--batch-sizes", "50,100",
        "--table-prefix", table_prefix
    ])
    
    assert result.exit_code == 0, f"Benchmark command should succeed: {result.stdout}"
    assert "BENCHMARK RESULTS" in result.stdout, "Output should contain benchmark results"
    assert "Workers" in result.stdout, "Output should contain worker information"
    assert "Records/s" in result.stdout, "Output should contain throughput information"
    
    print("✅ Benchmark test completed!")
    
    # Clean up benchmark tables
    with Session(tidb_engine) as session:
        # Get all tables that start with the prefix
        tables_result = session.execute(text("SHOW TABLES")).fetchall()
        for (table,) in tables_result:
            if table.startswith(table_prefix):
                session.execute(text(f"DROP TABLE IF EXISTS `{table}`"))
        session.commit()


def test_cli_help_commands():
    """Test CLI help commands."""
    
    print(f"🚀 Testing CLI help commands...")
    
    # Test main help
    result = run_cli_command(["--help"])
    assert result.exit_code == 0, "Main help should succeed"
    assert "vec2tidb" in result.stdout, "Help should contain vec2tidb"
    
    # Test qdrant help
    result = run_cli_command(["qdrant", "--help"])
    assert result.exit_code == 0, "Qdrant help should succeed"
    assert "qdrant" in result.stdout, "Help should contain qdrant"
    
    # Test migrate help
    result = run_cli_command(["qdrant", "migrate", "--help"])
    assert result.exit_code == 0, "Migrate help should succeed"
    assert "migrate" in result.stdout, "Help should contain migrate"
    
    # Test load-sample help
    result = run_cli_command(["qdrant", "load-sample", "--help"])
    assert result.exit_code == 0, "Load-sample help should succeed"
    assert "load-sample" in result.stdout, "Help should contain load-sample"
    
    # Test benchmark help
    result = run_cli_command(["qdrant", "benchmark", "--help"])
    assert result.exit_code == 0, "Benchmark help should succeed"
    assert "benchmark" in result.stdout, "Help should contain benchmark"
    
    print("✅ CLI help commands test completed!")


def test_cli_error_handling():
    """Test CLI error handling for invalid inputs."""
    
    print(f"🚀 Testing CLI error handling...")
    
    # Test migration with non-existent collection
    result = run_cli_command([
        "qdrant",
        "migrate",
        "--qdrant-api-url", QDRANT_API_URL,
        "--qdrant-collection-name", "nonexistent_collection_12345",
        "--tidb-database-url", TIDB_DATABASE_URL,
        "--mode", "create"
    ])
    
    assert result.exit_code != 0, "Migration with non-existent collection should fail"
    assert "does not exist" in result.stdout, "Error message should mention collection doesn't exist"
    
    # Test load-sample with invalid dataset
    result = run_cli_command([
        "qdrant",
        "load-sample",
        "--qdrant-api-url", QDRANT_API_URL,
        "--qdrant-collection-name", "test_collection",
        "--dataset", "invalid_dataset_name"
    ])
    
    assert result.exit_code != 0, "Load-sample with invalid dataset should fail"
    assert "Invalid value for '--dataset'" in result.stdout, "Error message should mention invalid dataset"
    
    print("✅ CLI error handling test completed!")

