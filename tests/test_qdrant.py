"""Tests for Qdrant plugin."""

from unittest.mock import Mock, patch
import json

import pytest
from qdrant_client.models import PointStruct

from vec2tidb.commands.qdrant import migrate, create_vector_table, check_vector_table, insert_points, update_points


@patch("vec2tidb.commands.qdrant.QdrantClient")
@patch("vec2tidb.commands.qdrant.create_tidb_engine")
@patch("vec2tidb.commands.qdrant.create_vector_table")
@patch("vec2tidb.commands.qdrant.process_with_tqdm")
@patch("vec2tidb.commands.qdrant.click")
def test_migrate_create_mode(
    mock_click,
    mock_process_with_tqdm,
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
    mock_process_with_tqdm.return_value = 100

    # Call migrate function
    migrate(
        mode="create",
        qdrant_api_url="http://localhost:6333",
        qdrant_api_key=None,
        qdrant_collection_name="test",
        tidb_database_url="mysql+pymysql://root@localhost:4000/test",
        table_name="test_table",
        id_column="id",
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
        mock_engine, "test_table", "id", "vector", "payload", distance_metric="cosine", dimensions=768
    )
    mock_process_with_tqdm.assert_called_once()


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
            vector_column="vector",
            payload_column="payload",
        )


@patch("vec2tidb.commands.qdrant.QdrantClient")
@patch("vec2tidb.commands.qdrant.create_tidb_engine")
@patch("vec2tidb.commands.qdrant.check_vector_table")
@patch("vec2tidb.commands.qdrant.process_with_tqdm")
@patch("vec2tidb.commands.qdrant.click")
def test_migrate_update_mode(
    mock_click,
    mock_process_with_tqdm,
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

    mock_process_with_tqdm.return_value = 50

    # Call migrate function
    migrate(
        mode="update",
        qdrant_api_url="http://localhost:6333",
        qdrant_api_key="test-key",
        qdrant_collection_name="test",
        tidb_database_url="mysql+pymysql://root@localhost:4000/test",
        table_name="test_table",
        id_column="id",
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
    mock_process_with_tqdm.assert_called_once()


@patch("vec2tidb.commands.qdrant.QdrantClient")
@patch("vec2tidb.commands.qdrant.create_tidb_engine")
@patch("vec2tidb.commands.qdrant.check_vector_table")
@patch("vec2tidb.commands.qdrant.process_with_tqdm")
@patch("vec2tidb.commands.qdrant.click")
def test_migrate_update_mode_no_payload(
    mock_click,
    mock_process_with_tqdm,
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

    mock_process_with_tqdm.return_value = 25

    # Call migrate function
    migrate(
        mode="update",
        qdrant_api_url="http://localhost:6333",
        qdrant_api_key=None,
        qdrant_collection_name="test",
        tidb_database_url="mysql+pymysql://root@localhost:4000/test",
        table_name="test_table",
        id_column="id",
        vector_column="vector",
        payload_column=None,
    )

    # Verify calls
    mock_check_table.assert_called_once_with(
        mock_engine, "test_table", "id", "vector", None
    )
    mock_process_with_tqdm.assert_called_once()


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
            768
        )

        mock_session.execute.assert_called_once()
        mock_session.commit.assert_called_once()

        # Verify the SQL contains expected elements
        call_args = mock_session.execute.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS test_table" in str(call_args)
        assert "id VARCHAR(255) PRIMARY KEY" in str(call_args)
        assert "vector VECTOR(768)" in str(call_args)
        assert "payload JSON" in str(call_args)
        assert "VEC_COSINE_DISTANCE" in str(call_args)
        assert "`created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP" in str(call_args)
        assert "`updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP" in str(call_args)


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
            1536
        )

        mock_session.execute.assert_called_once()
        mock_session.commit.assert_called_once()

        # Verify the SQL contains expected elements
        call_args = mock_session.execute.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS test_table" in str(call_args)
        assert "id VARCHAR(255) PRIMARY KEY" in str(call_args)
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
        with pytest.raises(ValueError, match="Invalid distance metric: euclidean"):
            create_vector_table(
                mock_engine,
                "test_table",
                "id",
                "vector",
                "payload",
                "euclidean",
                768
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
