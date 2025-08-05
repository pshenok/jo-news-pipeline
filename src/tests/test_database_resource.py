import pytest
from unittest.mock import Mock, patch, MagicMock
import psycopg2
from src.resources.database import PostgresResource


class TestPostgresResource:
    """Tests for PostgreSQL database resource."""
    
    def test_connection_string(self):
        """Test that connection string is properly formatted."""
        # Arrange
        resource = PostgresResource(
            host="testhost",
            port=5432,
            database="testdb",
            user="testuser",
            password="testpass"
        )
        
        # Act
        conn_string = resource.connection_string
        
        # Assert
        assert conn_string == "postgresql://testuser:testpass@testhost:5432/testdb"
    
    @patch('psycopg2.connect')
    def test_get_connection_success(self, mock_connect):
        """Sunshine test: Successful database connection."""
        # Arrange
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        resource = PostgresResource()
        
        # Act
        with resource.get_connection() as conn:
            assert conn == mock_conn
        
        # Assert
        mock_connect.assert_called_once()
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()
    
    @patch('psycopg2.connect')
    def test_get_connection_error(self, mock_connect):
        """Rainy test: Database connection error handling."""
        # Arrange
        mock_connect.side_effect = psycopg2.OperationalError("Connection failed")
        resource = PostgresResource()
        
        # Act & Assert
        with pytest.raises(psycopg2.OperationalError):
            with resource.get_connection():
                pass
    
    @patch('psycopg2.connect')
    def test_execute_query(self, mock_connect):
        """Test query execution returns results."""
        # Arrange
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            {'id': 1, 'title': 'Test Release'}
        ]
        mock_cursor.description = [('id',), ('title',)]
        
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        resource = PostgresResource()
        
        # Act
        results = resource.execute_query("SELECT * FROM test")
        
        # Assert
        assert len(results) == 1
        assert results[0]['id'] == 1
        assert results[0]['title'] == 'Test Release'
