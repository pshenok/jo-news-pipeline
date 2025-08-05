import pytest
from unittest.mock import Mock, MagicMock, patch
import hashlib
from dagster import build_asset_context, materialize_to_memory
from src.assets.scraper import raw_press_releases
from src.assets.summarizer import press_release_summary


class TestRawPressReleasesAsset:
    """Tests for raw_press_releases asset."""
    
    def test_raw_press_releases_no_new_urls(self):
        """Test when all URLs are already scraped."""
        # Arrange
        mock_postgres = Mock()
        mock_scraper = Mock()
        
        # Mock scraper to return URLs
        mock_scraper.get_sec_urls.return_value = ['https://test.com/1', 'https://test.com/2']
        
        # Mock database to show all URLs already exist
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (hashlib.sha256('https://test.com/1'.encode()).hexdigest(),),
            (hashlib.sha256('https://test.com/2'.encode()).hexdigest(),)
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_postgres.get_connection.return_value.__enter__.return_value = mock_conn
        
        context = build_asset_context(
            resources={"postgres": mock_postgres, "scraper": mock_scraper}
        )
        
        # Act
        result = raw_press_releases(context)
        
        # Assert
        assert result.metadata["new_urls"] == 0
        assert result.metadata["scraped"] == 0
    
    def test_raw_press_releases_scraping_error(self):
        """Rainy test: Handle scraping errors gracefully."""
        # Arrange
        mock_postgres = Mock()
        mock_scraper = Mock()
        
        # Mock scraper to return URLs
        mock_scraper.get_sec_urls.return_value = ['https://test.com/1']
        
        # Mock database to show URL doesn't exist
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_postgres.get_connection.return_value.__enter__.return_value = mock_conn
        
        # Mock scraper to fail
        mock_scraper.scrape_url.return_value = {
            'success': False,
            'url': 'https://test.com/1',
            'error': 'Network timeout'
        }
        
        context = build_asset_context(
            resources={"postgres": mock_postgres, "scraper": mock_scraper}
        )
        
        # Act
        result = raw_press_releases(context)
        
        # Assert
        assert result.metadata["errors"] == 1
        assert result.metadata["scraped"] == 0


class TestPressReleaseSummaryAsset:
    """Tests for press_release_summary asset."""
    
    def test_summary_no_unsummarized(self):
        """Test when all releases are already summarized."""
        # Arrange
        mock_postgres = Mock()
        mock_llm = Mock()
        
        # Mock database to return no unsummarized releases
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_postgres.get_connection.return_value.__enter__.return_value = mock_conn
        
        context = build_asset_context(
            resources={"postgres": mock_postgres, "llm": mock_llm}
        )
        
        # Act
        result = press_release_summary(context)
        
        # Assert
        assert result.metadata["processed"] == 0
        assert result.metadata["message"] == "All press releases already summarized"
    
    def test_summary_llm_unavailable(self):
        """Rainy test: Handle LLM service unavailability."""
        # Arrange
        mock_postgres = Mock()
        mock_llm = Mock()
        
        # Mock database to return unsummarized releases
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(1, 'Test Title', 'Test Content')]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_postgres.get_connection.return_value.__enter__.return_value = mock_conn
        
        # Mock LLM to be unavailable
        mock_llm.test_connection.return_value = False
        
        context = build_asset_context(
            resources={"postgres": mock_postgres, "llm": mock_llm}
        )
        
        # Act
        result = press_release_summary(context)
        
        # Assert
        assert result.metadata["error"] == "LLM service not available"
        assert result.metadata["processed"] == 0
