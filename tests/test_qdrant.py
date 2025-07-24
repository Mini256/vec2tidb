"""Tests for Qdrant plugin."""

from unittest.mock import Mock, patch

import pytest
from qdrant_client.models import PointStruct

from vec2tidb.commands.qdrant import (
    migrate, create_vector_table, check_vector_table, insert_points, update_points,
    get_snapshot_uri, load_sample, benchmark, drop_vector_table
)


@patch("vec2tidb.commands.qdrant.QdrantClient")
@patch("vec2tidb.commands.qdrant.create_tidb_engine")
@patch("vec2tidb.commands.qdrant.create_vector_table")
@patch("vec2tidb.commands.qdrant.process_batches_concurrent")
@patch("vec2tidb.commands.qdrant.click")
def test_migrate_create_mode(
    mock_click,
    mock_process_concurrent,
    mock_create_table,
    mock_create_engine,
    mock_qdrant_client,
):
    """Test migrate function in create mode."""
    # Setup mocks
    mock_client_instance = Mock()
    mock_qdrant_client.return_value = mock_client_instance
    mock_client_instance.collection_exists.return_value = True
    mock_client_instance.count.return_value = Mock(count=100)
    
    # Mock the collection info with distance metric
    mock_vectors = Mock()
    mock_vectors.size = 768
    mock_vectors.distance = Mock()
    mock_vectors.distance.lower.return_value = "cosine"
    
    mock_params = Mock()
    mock_params.vectors = mock_vectors
    
    mock_config = Mock()
    mock_config.params = mock_params
    
    mock_client_instance.get_collection.return_value = Mock(config=mock_config)
    mock_client_instance.scroll.return_value = ([], None)  # Empty result for scroll

    mock_engine = Mock()
    mock_create_engine.return_value = mock_engine
    mock_engine.dialect.identifier_preparer.format_table.return_value = "test_table"
    mock_engine.dialect.identifier_preparer.format_column.side_effect = lambda x: x

    mock_create_table.return_value = "test_table"
    mock_process_concurrent.return_value = 100

    # Call migrate function
    migrate(
        mode="create",
        qdrant_api_url="http://localhost:6333",
        qdrant_api_key=None,
        qdrant_collection_name="test",
        tidb_database_url="mysql+pymysql://root@localhost:4000/test",
        table_name="test_table",
        id_column="id",
        id_column_type="BIGINT",
        vector_column="vector",
        payload_column="payload",
    )

    # Verify calls
    mock_qdrant_client.assert_called_once_with(
        url="http://localhost:6333", api_key=None
    )
    mock_client_instance.collection_exists.assert_called_once_with(
        collection_name="test"
    )
    mock_client_instance.count.assert_called_once_with(collection_name="test")
    mock_create_engine.assert_called_once_with(
        "mysql+pymysql://root@localhost:4000/test"
    )
    mock_create_table.assert_called_once_with(
        mock_engine, "test_table", "id", "vector", "payload", distance_metric="cosine", dimensions=768, id_column_type="BIGINT"
    )
    mock_process_concurrent.assert_called_once()


@patch("vec2tidb.commands.qdrant.QdrantClient")
@patch("vec2tidb.commands.qdrant.create_tidb_engine")
def test_migrate_collection_not_exists(mock_create_engine, mock_qdrant_client):
    """Test migrate function when collection doesn't exist."""
    # Setup mocks
    mock_client_instance = Mock()
    mock_qdrant_client.return_value = mock_client_instance
    mock_client_instance.collection_exists.return_value = False

    # Call migrate function and expect exception
    with pytest.raises(
        Exception, match="Requested Qdrant collection 'test' does not exist"
    ):
        migrate(
            mode="create",
            qdrant_api_url="http://localhost:6333",
            qdrant_api_key=None,
            qdrant_collection_name="test",
            tidb_database_url="mysql+pymysql://root@localhost:4000/test",
            table_name="test_table",
            id_column="id",
            id_column_type="BIGINT",
            vector_column="vector",
            payload_column="payload",
        )


@patch("vec2tidb.commands.qdrant.QdrantClient")
@patch("vec2tidb.commands.qdrant.create_tidb_engine")
def test_migrate_empty_collection(mock_create_engine, mock_qdrant_client):
    """Test migrate function when collection is empty."""
    # Setup mocks
    mock_client_instance = Mock()
    mock_qdrant_client.return_value = mock_client_instance
    mock_client_instance.collection_exists.return_value = True
    mock_client_instance.count.return_value = Mock(count=0)

    # Call migrate function and expect exception
    with pytest.raises(
        Exception, match="No records present in requested Qdrant collection 'test'"
    ):
        migrate(
            mode="create",
            qdrant_api_url="http://localhost:6333",
            qdrant_api_key=None,
            qdrant_collection_name="test",
            tidb_database_url="mysql+pymysql://root@localhost:4000/test",
            table_name="test_table",
            id_column="id",
            id_column_type="BIGINT",
            vector_column="vector",
            payload_column="payload",
        )


@patch("vec2tidb.commands.qdrant.QdrantClient")
@patch("vec2tidb.commands.qdrant.create_tidb_engine")
@patch("vec2tidb.commands.qdrant.check_vector_table")
@patch("vec2tidb.commands.qdrant.process_batches_concurrent")
@patch("vec2tidb.commands.qdrant.click")
def test_migrate_update_mode(
    mock_click,
    mock_process_concurrent,
    mock_check_table,
    mock_create_engine,
    mock_qdrant_client,
):
    """Test migrate function in update mode."""
    # Setup mocks
    mock_client_instance = Mock()
    mock_qdrant_client.return_value = mock_client_instance
    mock_client_instance.collection_exists.return_value = True
    mock_client_instance.count.return_value = Mock(count=50)
    
    # Mock the collection info with distance metric
    mock_vectors = Mock()
    mock_vectors.size = 1536
    mock_vectors.distance = Mock()
    mock_vectors.distance.lower.return_value = "l2"
    
    mock_params = Mock()
    mock_params.vectors = mock_vectors
    
    mock_config = Mock()
    mock_config.params = mock_params
    
    mock_client_instance.get_collection.return_value = Mock(config=mock_config)
    mock_client_instance.scroll.return_value = ([], None)  # Empty result for scroll

    mock_engine = Mock()
    mock_create_engine.return_value = mock_engine
    mock_engine.dialect.identifier_preparer.format_table.return_value = "test_table"
    mock_engine.dialect.identifier_preparer.format_column.side_effect = lambda x: x

    mock_process_concurrent.return_value = 50

    # Call migrate function
    migrate(
        mode="update",
        qdrant_api_url="http://localhost:6333",
        qdrant_api_key="test-key",
        qdrant_collection_name="test",
        tidb_database_url="mysql+pymysql://root@localhost:4000/test",
        table_name="test_table",
        id_column="id",
        id_column_type="BIGINT",
        vector_column="vector",
        payload_column="payload",
    )

    # Verify calls
    mock_qdrant_client.assert_called_once_with(
        url="http://localhost:6333", api_key="test-key"
    )
    mock_check_table.assert_called_once_with(
        mock_engine, "test_table", "id", "vector", "payload"
    )
    mock_process_concurrent.assert_called_once()


@patch("vec2tidb.commands.qdrant.QdrantClient")
@patch("vec2tidb.commands.qdrant.create_tidb_engine")
@patch("vec2tidb.commands.qdrant.check_vector_table")
@patch("vec2tidb.commands.qdrant.process_batches_concurrent")
@patch("vec2tidb.commands.qdrant.click")
def test_migrate_update_mode_no_payload(
    mock_click,
    mock_process_concurrent,
    mock_check_table,
    mock_create_engine,
    mock_qdrant_client,
):
    """Test migrate function in update mode without payload column."""
    # Setup mocks
    mock_client_instance = Mock()
    mock_qdrant_client.return_value = mock_client_instance
    mock_client_instance.collection_exists.return_value = True
    mock_client_instance.count.return_value = Mock(count=25)
    
    # Mock the collection info with distance metric
    mock_vectors = Mock()
    mock_vectors.size = 512
    mock_vectors.distance = Mock()
    mock_vectors.distance.lower.return_value = "cosine"
    
    mock_params = Mock()
    mock_params.vectors = mock_vectors
    
    mock_config = Mock()
    mock_config.params = mock_params
    
    mock_client_instance.get_collection.return_value = Mock(config=mock_config)
    mock_client_instance.scroll.return_value = ([], None)  # Empty result for scroll

    mock_engine = Mock()
    mock_create_engine.return_value = mock_engine
    mock_engine.dialect.identifier_preparer.format_table.return_value = "test_table"
    mock_engine.dialect.identifier_preparer.format_column.side_effect = lambda x: x

    mock_process_concurrent.return_value = 25

    # Call migrate function
    migrate(
        mode="update",
        qdrant_api_url="http://localhost:6333",
        qdrant_api_key=None,
        qdrant_collection_name="test",
        tidb_database_url="mysql+pymysql://root@localhost:4000/test",
        table_name="test_table",
        id_column="id",
        id_column_type="BIGINT",
        vector_column="vector",
        payload_column=None,
    )

    # Verify calls
    mock_check_table.assert_called_once_with(
        mock_engine, "test_table", "id", "vector", None
    )
    mock_process_concurrent.assert_called_once()


def test_create_vector_table():
    """Test create_vector_table function."""
    from sqlalchemy import create_engine
    from unittest.mock import patch, Mock

    # Mock engine and session
    mock_engine = Mock()
    mock_session = Mock()
    mock_session.__enter__ = Mock(return_value=mock_session)
    mock_session.__exit__ = Mock(return_value=None)

    # Mock the identifier preparer to return actual identifiers
    mock_preparer = Mock()
    mock_preparer.quote_identifier.side_effect = lambda x: x
    mock_engine.dialect.identifier_preparer = mock_preparer

    with patch('vec2tidb.commands.qdrant.Session', return_value=mock_session):
        create_vector_table(
            mock_engine,
            "test_table",
            "id",
            "vector",
            "payload",
            "cosine",
            768,
            "BIGINT"
        )

        mock_session.execute.assert_called_once()
        mock_session.commit.assert_called_once()

        # Verify the SQL contains expected elements
        call_args = mock_session.execute.call_args[0][0]
        assert "CREATE TABLE test_table" in str(call_args)
        assert "id BIGINT PRIMARY KEY" in str(call_args)
        assert "vector VECTOR(768)" in str(call_args)
        assert "payload JSON" in str(call_args)
        assert "VEC_COSINE_DISTANCE" in str(call_args)
        assert "`created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP" in str(call_args)
        assert "`updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP" in str(call_args)


def test_create_vector_table_l2_distance():
    """Test create_vector_table function with L2 distance."""
    from sqlalchemy import create_engine
    from unittest.mock import patch, Mock

    # Mock engine and session
    mock_engine = Mock()
    mock_session = Mock()
    mock_session.__enter__ = Mock(return_value=mock_session)
    mock_session.__exit__ = Mock(return_value=None)

    # Mock the identifier preparer to return actual identifiers
    mock_preparer = Mock()
    mock_preparer.quote_identifier.side_effect = lambda x: x
    mock_engine.dialect.identifier_preparer = mock_preparer

    with patch('vec2tidb.commands.qdrant.Session', return_value=mock_session):
        create_vector_table(
            mock_engine,
            "test_table",
            "id",
            "vector",
            "payload",
            "l2",
            1536,
            "BIGINT"
        )

        mock_session.execute.assert_called_once()
        mock_session.commit.assert_called_once()

        # Verify the SQL contains expected elements
        call_args = mock_session.execute.call_args[0][0]
        assert "CREATE TABLE test_table" in str(call_args)
        assert "id BIGINT PRIMARY KEY" in str(call_args)
        assert "vector VECTOR(1536)" in str(call_args)
        assert "payload JSON" in str(call_args)
        assert "VEC_L2_DISTANCE" in str(call_args)


def test_create_vector_table_invalid_distance():
    """Test create_vector_table function with invalid distance metric."""
    from sqlalchemy import create_engine
    from unittest.mock import patch, Mock

    # Mock engine and session
    mock_engine = Mock()
    mock_session = Mock()
    mock_session.__enter__ = Mock(return_value=mock_session)
    mock_session.__exit__ = Mock(return_value=None)

    with patch('vec2tidb.commands.qdrant.Session', return_value=mock_session):
        with pytest.raises(Exception, match="Invalid distance metric: euclidean"):
            create_vector_table(
                mock_engine,
                "test_table",
                "id",
                "vector",
                "payload",
                "euclidean",
                768,
                "BIGINT"
            )


def test_check_vector_table_success():
    """Test check_vector_table function when table and columns exist."""
    from unittest.mock import patch, Mock

    # Mock engine and session
    mock_engine = Mock()
    mock_session = Mock()
    mock_session.__enter__ = Mock(return_value=mock_session)
    mock_session.__exit__ = Mock(return_value=None)

    # Mock the identifier preparer to return actual identifiers
    mock_preparer = Mock()
    mock_preparer.quote_identifier.side_effect = lambda x: x
    mock_engine.dialect.identifier_preparer = mock_preparer

    # Mock column results as tuples (first element is column name)
    mock_columns = [("id",), ("vector",), ("payload",)]

    with patch('vec2tidb.commands.qdrant.Session', return_value=mock_session):
        # Mock the execute method to return different results for different calls
        call_count = 0
        def execute_side_effect(sql):
            nonlocal call_count
            call_count += 1
            if call_count == 1:  # First call for SELECT 1
                return Mock()
            elif call_count == 2:  # Second call for SHOW COLUMNS
                mock_result = Mock()
                # Ensure fetchall returns the same list each time
                mock_result.fetchall = Mock(return_value=mock_columns)
                return mock_result
            return Mock()
        
        mock_session.execute.side_effect = execute_side_effect

        # Should not raise any exception
        check_vector_table(mock_engine, "test_table", "id", "vector", "payload")


def test_check_vector_table_not_exists():
    """Test check_vector_table function when table doesn't exist."""
    from unittest.mock import patch, Mock

    # Mock engine and session
    mock_engine = Mock()
    mock_session = Mock()
    mock_session.__enter__ = Mock(return_value=mock_session)
    mock_session.__exit__ = Mock(return_value=None)

    # Mock the identifier preparer to return actual identifiers
    mock_preparer = Mock()
    mock_preparer.quote_identifier.side_effect = lambda x: x
    mock_engine.dialect.identifier_preparer = mock_preparer

    with patch('vec2tidb.commands.qdrant.Session', return_value=mock_session):
        mock_session.execute.side_effect = Exception("Table doesn't exist")

        with pytest.raises(Exception, match="Table test_table does not exist"):
            check_vector_table(mock_engine, "test_table", "id", "vector", "payload")


def test_check_vector_table_missing_column():
    """Test check_vector_table function when required column is missing."""
    from unittest.mock import patch, Mock

    # Mock engine and session
    mock_engine = Mock()
    mock_session = Mock()
    mock_session.__enter__ = Mock(return_value=mock_session)
    mock_session.__exit__ = Mock(return_value=None)

    # Mock the identifier preparer to return actual identifiers
    mock_preparer = Mock()
    mock_preparer.quote_identifier.side_effect = lambda x: x
    mock_engine.dialect.identifier_preparer = mock_preparer

    # Mock column results as tuples (missing vector column)
    mock_columns = [("id",), ("payload",)]

    with patch('vec2tidb.commands.qdrant.Session', return_value=mock_session):
        # Mock the execute method to return different results for different calls
        call_count = 0
        def execute_side_effect(sql):
            nonlocal call_count
            call_count += 1
            if call_count == 1:  # First call for SELECT 1
                return Mock()
            elif call_count == 2:  # Second call for SHOW COLUMNS
                mock_result = Mock()
                # Ensure fetchall returns the same list each time
                mock_result.fetchall = Mock(return_value=mock_columns)
                return mock_result
            return Mock()
        
        mock_session.execute.side_effect = execute_side_effect

        with pytest.raises(Exception, match="Column `vector` does not exist in table test_table"):
            check_vector_table(mock_engine, "test_table", "id", "vector", "payload")


def test_insert_points():
    """Test insert_points function."""
    from unittest.mock import patch, Mock

    # Mock engine and session
    mock_engine = Mock()
    mock_session = Mock()
    mock_session.__enter__ = Mock(return_value=mock_session)
    mock_session.__exit__ = Mock(return_value=None)

    # Mock the identifier preparer to return actual identifiers
    mock_preparer = Mock()
    mock_preparer.quote_identifier.side_effect = lambda x: x
    mock_engine.dialect.identifier_preparer = mock_preparer

    # Create test points
    points = [
        PointStruct(id="1", vector=[1.0, 2.0, 3.0], payload={"key": "value1"}),
        PointStruct(id="2", vector=[4.0, 5.0, 6.0], payload={"key": "value2"}),
    ]

    with patch('vec2tidb.commands.qdrant.Session', return_value=mock_session):
        insert_points(mock_engine, points, "test_table", "id", "vector", "payload")

        # Verify execute was called with the correct SQL
        mock_session.execute.assert_called_once()
        call_args = mock_session.execute.call_args[0][0]
        assert "INSERT INTO test_table" in str(call_args)
        assert "(id, vector, payload)" in str(call_args)
        assert "VALUES (:id, :vector, :payload)" in str(call_args)

        # Verify commit was called
        mock_session.commit.assert_called_once()


def test_insert_points_no_payload():
    """Test insert_points function without payload column."""
    from unittest.mock import patch, Mock

    # Mock engine and session
    mock_engine = Mock()
    mock_session = Mock()
    mock_session.__enter__ = Mock(return_value=mock_session)
    mock_session.__exit__ = Mock(return_value=None)

    # Mock the identifier preparer to return actual identifiers
    mock_preparer = Mock()
    mock_preparer.quote_identifier.side_effect = lambda x: x
    mock_engine.dialect.identifier_preparer = mock_preparer

    # Create test points
    points = [
        PointStruct(id="1", vector=[1.0, 2.0, 3.0], payload={"key": "value1"}),
        PointStruct(id="2", vector=[4.0, 5.0, 6.0], payload={"key": "value2"}),
    ]

    with patch('vec2tidb.commands.qdrant.Session', return_value=mock_session):
        # The insert_points function actually accepts None for payload_column
        # but it will still include it in the SQL since it's quoted as None
        insert_points(mock_engine, points, "test_table", "id", "vector", None)

        # Verify execute was called with the correct SQL
        mock_session.execute.assert_called_once()
        call_args = mock_session.execute.call_args[0][0]
        assert "INSERT INTO test_table" in str(call_args)
        assert "(id, vector, None)" in str(call_args)
        assert "VALUES (:id, :vector, :payload)" in str(call_args)

        # Verify commit was called
        mock_session.commit.assert_called_once()


def test_update_points():
    """Test update_points function."""
    from unittest.mock import patch, Mock

    # Mock engine and session
    mock_engine = Mock()
    mock_session = Mock()
    mock_session.__enter__ = Mock(return_value=mock_session)
    mock_session.__exit__ = Mock(return_value=None)

    # Mock the identifier preparer to return actual identifiers
    mock_preparer = Mock()
    mock_preparer.quote_identifier.side_effect = lambda x: x
    mock_engine.dialect.identifier_preparer = mock_preparer

    # Create test points
    points = [
        PointStruct(id="1", vector=[1.0, 2.0, 3.0], payload={"key": "value1"}),
        PointStruct(id="2", vector=[4.0, 5.0, 6.0], payload={"key": "value2"}),
    ]

    with patch('vec2tidb.commands.qdrant.Session', return_value=mock_session):
        update_points(mock_engine, points, "test_table", "id", "vector", "payload")

        # Verify execute was called with the correct SQL
        mock_session.execute.assert_called_once()
        call_args = mock_session.execute.call_args[0][0]
        assert "UPDATE test_table" in str(call_args)
        assert "SET vector = :vector, payload = :payload" in str(call_args)
        assert "WHERE id = :id" in str(call_args)

        # Verify commit was called
        mock_session.commit.assert_called_once()


def test_update_points_no_payload():
    """Test update_points function without payload column."""
    from unittest.mock import patch, Mock

    # Mock engine and session
    mock_engine = Mock()
    mock_session = Mock()
    mock_session.__enter__ = Mock(return_value=mock_session)
    mock_session.__exit__ = Mock(return_value=None)

    # Mock the identifier preparer to return actual identifiers
    mock_preparer = Mock()
    mock_preparer.quote_identifier.side_effect = lambda x: x
    mock_engine.dialect.identifier_preparer = mock_preparer

    # Create test points
    points = [
        PointStruct(id="1", vector=[1.0, 2.0, 3.0], payload={"key": "value1"}),
        PointStruct(id="2", vector=[4.0, 5.0, 6.0], payload={"key": "value2"}),
    ]

    with patch('vec2tidb.commands.qdrant.Session', return_value=mock_session):
        update_points(mock_engine, points, "test_table", "id", "vector", None)

        # Verify execute was called with the correct SQL
        mock_session.execute.assert_called_once()
        call_args = mock_session.execute.call_args[0][0]
        assert "UPDATE test_table" in str(call_args)
        assert "SET vector = :vector" in str(call_args)
        assert "WHERE id = :id" in str(call_args)

        # Verify commit was called
        mock_session.commit.assert_called_once()


@patch("vec2tidb.commands.qdrant.QdrantClient")
@patch("vec2tidb.commands.qdrant.create_tidb_engine")
@patch("vec2tidb.commands.qdrant.create_vector_table")
@patch("vec2tidb.commands.qdrant.process_batches_concurrent")
@patch("vec2tidb.commands.qdrant.click")
def test_migrate_single_worker(
    mock_click,
    mock_process_concurrent,
    mock_create_table,
    mock_create_engine,
    mock_qdrant_client,
):
    """Test migrate function with single worker (should use sequential processing)."""
    # Setup mocks
    mock_client_instance = Mock()
    mock_qdrant_client.return_value = mock_client_instance
    mock_client_instance.collection_exists.return_value = True
    mock_client_instance.count.return_value = Mock(count=100)
    
    # Mock the sample point for ID type detection
    sample_point = PointStruct(id=1, vector=[1.0, 2.0], payload={})
    mock_client_instance.scroll.return_value = ([sample_point], None)
    
    # Mock the collection info with distance metric
    mock_vectors = Mock()
    mock_vectors.size = 768
    mock_vectors.distance = Mock()
    mock_vectors.distance.lower.return_value = "cosine"
    
    mock_params = Mock()
    mock_params.vectors = mock_vectors
    
    mock_config = Mock()
    mock_config.params = mock_params
    
    mock_client_instance.get_collection.return_value = Mock(config=mock_config)

    mock_engine = Mock()
    mock_create_engine.return_value = mock_engine
    mock_engine.dialect.identifier_preparer.quote_identifier.side_effect = lambda x: x

    mock_process_concurrent.return_value = 100

    # Call migrate function with workers=1
    migrate(
        mode="create",
        qdrant_api_url="http://localhost:6333",
        qdrant_api_key=None,
        qdrant_collection_name="test",
        tidb_database_url="mysql+pymysql://root@localhost:4000/test",
        table_name="test_table",
        id_column="id",
        id_column_type="BIGINT",
        vector_column="vector",
        payload_column="payload",
        workers=1,
    )

    # Verify concurrent processing was used (handles single worker internally)
    mock_process_concurrent.assert_called_once()
    
    # Verify the correct parameters were passed
    call_args = mock_process_concurrent.call_args
    assert call_args[1]['workers'] == 1


@patch("vec2tidb.commands.qdrant.QdrantClient")
@patch("vec2tidb.commands.qdrant.create_tidb_engine")
@patch("vec2tidb.commands.qdrant.create_vector_table")
@patch("vec2tidb.commands.qdrant.process_batches_concurrent")
@patch("vec2tidb.commands.qdrant.click")
def test_migrate_multiple_workers(
    mock_click,
    mock_process_concurrent,
    mock_create_table,
    mock_create_engine,
    mock_qdrant_client,
):
    """Test migrate function with multiple workers (should use concurrent processing)."""
    # Setup mocks
    mock_client_instance = Mock()
    mock_qdrant_client.return_value = mock_client_instance
    mock_client_instance.collection_exists.return_value = True
    mock_client_instance.count.return_value = Mock(count=100)
    
    # Mock the sample point for ID type detection
    sample_point = PointStruct(id=1, vector=[1.0, 2.0], payload={})
    mock_client_instance.scroll.return_value = ([sample_point], None)
    
    # Mock the collection info with distance metric
    mock_vectors = Mock()
    mock_vectors.size = 768
    mock_vectors.distance = Mock()
    mock_vectors.distance.lower.return_value = "cosine"
    
    mock_params = Mock()
    mock_params.vectors = mock_vectors
    
    mock_config = Mock()
    mock_config.params = mock_params
    
    mock_client_instance.get_collection.return_value = Mock(config=mock_config)

    mock_engine = Mock()
    mock_create_engine.return_value = mock_engine
    mock_engine.dialect.identifier_preparer.quote_identifier.side_effect = lambda x: x

    mock_process_concurrent.return_value = 100

    # Call migrate function with workers=4
    migrate(
        mode="create",
        qdrant_api_url="http://localhost:6333",
        qdrant_api_key=None,
        qdrant_collection_name="test",
        tidb_database_url="mysql+pymysql://root@localhost:4000/test",
        table_name="test_table",
        id_column="id",
        id_column_type="BIGINT",
        vector_column="vector",
        payload_column="payload",
        workers=4,
    )

    # Verify concurrent processing was used
    mock_process_concurrent.assert_called_once()
    
    # Verify the correct parameters were passed to concurrent processor
    call_args = mock_process_concurrent.call_args
    assert call_args[1]['workers'] == 4


def test_get_snapshot_uri_with_dataset():
    """Test get_snapshot_uri function with valid dataset."""
    
    # Test with valid datasets
    assert get_snapshot_uri(dataset="midlib") == "https://snapshots.qdrant.io/midlib.snapshot"
    assert get_snapshot_uri(dataset="qdrant-docs") == "https://snapshots.qdrant.io/qdrant-docs-04-05.snapshot"
    assert get_snapshot_uri(dataset="prefix-cache") == "https://snapshots.qdrant.io/prefix-cache.snapshot"


def test_get_snapshot_uri_with_custom_uri():
    """Test get_snapshot_uri function with custom snapshot URI."""
    
    custom_uri = "https://example.com/custom.snapshot"
    assert get_snapshot_uri(snapshot_uri=custom_uri) == custom_uri
    
    # Custom URI takes precedence over dataset
    assert get_snapshot_uri(dataset="midlib", snapshot_uri=custom_uri) == custom_uri


def test_get_snapshot_uri_invalid_dataset():
    """Test get_snapshot_uri function with invalid dataset."""
    
    import click
    
    with pytest.raises(click.UsageError, match="Invalid dataset: invalid_dataset"):
        get_snapshot_uri(dataset="invalid_dataset")


def test_get_snapshot_uri_no_params():
    """Test get_snapshot_uri function with no parameters."""
    
    assert get_snapshot_uri() is None
    assert get_snapshot_uri(dataset=None, snapshot_uri=None) is None


@patch("vec2tidb.commands.qdrant.QdrantClient")
@patch("vec2tidb.commands.qdrant.click")
def test_load_sample(mock_click, mock_qdrant_client):
    """Test load_sample function."""
    
    # Setup mocks
    mock_client_instance = Mock()
    mock_qdrant_client.return_value = mock_client_instance
    
    # Call load_sample function
    load_sample(
        qdrant_api_url="http://localhost:6333",
        qdrant_api_key="test-key",
        qdrant_collection_name="test_collection",
        snapshot_uri="https://example.com/test.snapshot"
    )
    
    # Verify calls
    mock_qdrant_client.assert_called_once_with(
        url="http://localhost:6333", api_key="test-key"
    )
    mock_client_instance.recover_snapshot.assert_called_once_with(
        collection_name="test_collection",
        location="https://example.com/test.snapshot",
        wait=False
    )


@patch("vec2tidb.commands.qdrant.QdrantClient")
@patch("vec2tidb.commands.qdrant.subprocess")
@patch("vec2tidb.commands.qdrant.time")
@patch("vec2tidb.commands.qdrant.sys")
@patch("vec2tidb.commands.qdrant.click")
def test_benchmark(mock_click, mock_sys, mock_time, mock_subprocess, mock_qdrant_client):
    """Test benchmark function."""
    
    # Setup mocks
    mock_client_instance = Mock()
    mock_qdrant_client.return_value = mock_client_instance
    mock_client_instance.collection_exists.return_value = True
    mock_client_instance.count.return_value = Mock(count=1000)
    
    # Mock collection info
    mock_vectors = Mock()
    mock_vectors.size = 768
    mock_vectors.distance = Mock()
    mock_vectors.distance.lower.return_value = "cosine"
    
    mock_params = Mock()
    mock_params.vectors = mock_vectors
    
    mock_config = Mock()
    mock_config.params = mock_params
    
    mock_client_instance.get_collection.return_value = Mock(config=mock_config)
    
    # Mock subprocess results
    mock_result = Mock()
    mock_result.stdout = "Migration completed"
    mock_result.stderr = ""
    mock_subprocess.run.return_value = mock_result
    
    # Mock sys.executable
    mock_sys.executable = "/usr/bin/python"
    
    # Mock time - need more time calls for 4 benchmark runs
    mock_time.time.side_effect = [0.0, 10.0, 0.0, 15.0, 0.0, 20.0, 0.0, 25.0]  # Start/end times for each test
    
    # Call benchmark function
    benchmark(
        qdrant_api_url="http://localhost:6333",
        qdrant_api_key="test-key",
        qdrant_collection_name="test_collection",
        tidb_database_url="mysql+pymysql://root@localhost:4000/test",
        worker_list=[1, 2],
        batch_size_list=[100, 200],
        table_prefix="benchmark_test"
    )
    
    # Verify Qdrant client calls
    mock_qdrant_client.assert_called_once_with(
        url="http://localhost:6333", api_key="test-key"
    )
    mock_client_instance.collection_exists.assert_called_once_with(
        collection_name="test_collection"
    )
    mock_client_instance.count.assert_called_once_with(
        collection_name="test_collection"
    )
    
    # Verify subprocess was called for each configuration
    assert mock_subprocess.run.call_count == 4  # 2 workers * 2 batch sizes


@patch("vec2tidb.commands.qdrant.QdrantClient")
def test_benchmark_collection_not_exists(mock_qdrant_client):
    """Test benchmark function when collection doesn't exist."""
    
    # Setup mocks
    mock_client_instance = Mock()
    mock_qdrant_client.return_value = mock_client_instance
    mock_client_instance.collection_exists.return_value = False
    
    # Import click to use real UsageError
    import click
    
    # Call benchmark function and expect exception
    with pytest.raises(click.UsageError, match="does not exist"):
        benchmark(
            qdrant_api_url="http://localhost:6333",
            qdrant_api_key=None,
            qdrant_collection_name="test_collection",
            tidb_database_url="mysql+pymysql://root@localhost:4000/test",
            worker_list=[1],
            batch_size_list=[100],
            table_prefix="benchmark_test"
        )


@patch("vec2tidb.commands.qdrant.QdrantClient")
def test_benchmark_empty_collection(mock_qdrant_client):
    """Test benchmark function when collection is empty."""
    
    # Setup mocks
    mock_client_instance = Mock()
    mock_qdrant_client.return_value = mock_client_instance
    mock_client_instance.collection_exists.return_value = True
    mock_client_instance.count.return_value = Mock(count=0)
    
    # Import click to use real UsageError
    import click
    
    # Call benchmark function and expect exception
    with pytest.raises(click.UsageError, match="is empty"):
        benchmark(
            qdrant_api_url="http://localhost:6333",
            qdrant_api_key=None,
            qdrant_collection_name="test_collection",
            tidb_database_url="mysql+pymysql://root@localhost:4000/test",
            worker_list=[1],
            batch_size_list=[100],
            table_prefix="benchmark_test"
        )


def test_drop_vector_table():
    """Test drop_vector_table function."""
    from unittest.mock import patch, Mock
    
    # Mock engine and session
    mock_engine = Mock()
    mock_session = Mock()
    mock_session.__enter__ = Mock(return_value=mock_session)
    mock_session.__exit__ = Mock(return_value=None)
    
    # Mock the identifier preparer
    mock_preparer = Mock()
    mock_preparer.quote_identifier.side_effect = lambda x: f"`{x}`"
    mock_engine.dialect.identifier_preparer = mock_preparer
    
    with patch('vec2tidb.commands.qdrant.Session', return_value=mock_session):
        drop_vector_table(mock_engine, "test_table")
        
        # Verify execute was called with DROP TABLE command
        mock_session.execute.assert_called_once()
        call_args = mock_session.execute.call_args[0][0]
        assert "DROP TABLE IF EXISTS `test_table`" in str(call_args)
        
        # Verify commit was called
        mock_session.commit.assert_called_once()


@patch("vec2tidb.commands.qdrant.QdrantClient")
@patch("vec2tidb.commands.qdrant.create_tidb_engine")
@patch("vec2tidb.commands.qdrant.drop_vector_table")
@patch("vec2tidb.commands.qdrant.create_vector_table")
@patch("vec2tidb.commands.qdrant.process_batches_concurrent")
@patch("vec2tidb.commands.qdrant.click")
def test_migrate_with_drop_table(
    mock_click,
    mock_process_concurrent,
    mock_create_table,
    mock_drop_table,
    mock_create_engine,
    mock_qdrant_client,
):
    """Test migrate function with drop_table=True."""
    
    # Setup mocks
    mock_client_instance = Mock()
    mock_qdrant_client.return_value = mock_client_instance
    mock_client_instance.collection_exists.return_value = True
    mock_client_instance.count.return_value = Mock(count=100)
    
    # Mock the sample point for ID type detection
    sample_point = PointStruct(id="test_id", vector=[1.0, 2.0], payload={})
    mock_client_instance.scroll.return_value = ([sample_point], None)
    
    # Mock the collection info
    mock_vectors = Mock()
    mock_vectors.size = 768
    mock_vectors.distance = Mock()
    mock_vectors.distance.lower.return_value = "cosine"
    
    mock_params = Mock()
    mock_params.vectors = mock_vectors
    
    mock_config = Mock()
    mock_config.params = mock_params
    
    mock_client_instance.get_collection.return_value = Mock(config=mock_config)
    
    mock_engine = Mock()
    mock_create_engine.return_value = mock_engine
    mock_engine.dialect.identifier_preparer.quote_identifier.side_effect = lambda x: x
    
    mock_process_concurrent.return_value = 100
    
    # Call migrate function with drop_table=True
    migrate(
        mode="create",
        qdrant_api_url="http://localhost:6333",
        qdrant_api_key=None,
        qdrant_collection_name="test",
        tidb_database_url="mysql+pymysql://root@localhost:4000/test",
        table_name="test_table",
        id_column="id",
        id_column_type="BIGINT",
        vector_column="vector",
        payload_column="payload",
        drop_table=True,
    )
    
    # Verify drop_vector_table was called
    mock_drop_table.assert_called_once_with(mock_engine, "test_table")
    
    # Verify create_vector_table was called with VARCHAR type (auto-detected from string ID)
    mock_create_table.assert_called_once_with(
        mock_engine, "test_table", "id", "vector", "payload", 
        distance_metric="cosine", dimensions=768, id_column_type="VARCHAR(7)"
    )


@patch("vec2tidb.commands.qdrant.QdrantClient")
@patch("vec2tidb.commands.qdrant.create_tidb_engine")
@patch("vec2tidb.commands.qdrant.create_vector_table")
@patch("vec2tidb.commands.qdrant.process_batches_concurrent")
@patch("vec2tidb.commands.qdrant.click")
def test_migrate_id_type_detection_integer(
    mock_click,
    mock_process_concurrent,
    mock_create_table,
    mock_create_engine,
    mock_qdrant_client,
):
    """Test migrate function with integer ID type detection."""
    
    # Setup mocks
    mock_client_instance = Mock()
    mock_qdrant_client.return_value = mock_client_instance
    mock_client_instance.collection_exists.return_value = True
    mock_client_instance.count.return_value = Mock(count=100)
    
    # Mock the sample point with integer ID
    sample_point = PointStruct(id=123, vector=[1.0, 2.0], payload={})
    mock_client_instance.scroll.return_value = ([sample_point], None)
    
    # Mock the collection info
    mock_vectors = Mock()
    mock_vectors.size = 768
    mock_vectors.distance = Mock()
    mock_vectors.distance.lower.return_value = "cosine"
    
    mock_params = Mock()
    mock_params.vectors = mock_vectors
    
    mock_config = Mock()
    mock_config.params = mock_params
    
    mock_client_instance.get_collection.return_value = Mock(config=mock_config)
    
    mock_engine = Mock()
    mock_create_engine.return_value = mock_engine
    mock_engine.dialect.identifier_preparer.quote_identifier.side_effect = lambda x: x
    
    mock_process_concurrent.return_value = 100
    
    # Call migrate function
    migrate(
        mode="create",
        qdrant_api_url="http://localhost:6333",
        qdrant_api_key=None,
        qdrant_collection_name="test",
        tidb_database_url="mysql+pymysql://root@localhost:4000/test",
        table_name="test_table",
        id_column="id",
        id_column_type="BIGINT",
        vector_column="vector",
        payload_column="payload",
    )
    
    # Verify create_vector_table was called with BIGINT type (auto-detected from integer ID)
    mock_create_table.assert_called_once_with(
        mock_engine, "test_table", "id", "vector", "payload", 
        distance_metric="cosine", dimensions=768, id_column_type="BIGINT"
    )


@patch("vec2tidb.commands.qdrant.QdrantClient")
@patch("vec2tidb.commands.qdrant.create_tidb_engine")
def test_migrate_unsupported_id_type(mock_create_engine, mock_qdrant_client):
    """Test migrate function with unsupported ID type."""
    
    # Setup mocks
    mock_client_instance = Mock()
    mock_qdrant_client.return_value = mock_client_instance
    mock_client_instance.collection_exists.return_value = True
    mock_client_instance.count.return_value = Mock(count=100)
    
    # Mock the sample point with unsupported ID type (using a dict to bypass validation)
    sample_point = Mock()
    sample_point.id = 123.45  # float ID (unsupported)
    sample_point.vector = [1.0, 2.0]
    sample_point.payload = {}
    mock_client_instance.scroll.return_value = ([sample_point], None)
    
    # Call migrate function and expect exception
    with pytest.raises(Exception, match="Unsupported Qdrant point ID type"):
        migrate(
            mode="create",
            qdrant_api_url="http://localhost:6333",
            qdrant_api_key=None,
            qdrant_collection_name="test",
            tidb_database_url="mysql+pymysql://root@localhost:4000/test",
            table_name="test_table",
            id_column="id",
            id_column_type="BIGINT",
            vector_column="vector",
            payload_column="payload",
        )
