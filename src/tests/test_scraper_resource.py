import pytest
from unittest.mock import Mock, patch
import hashlib
from datetime import datetime
from src.resources.scraper import ScraperResource


class TestScraperResource:
    """Tests for web scraper resource."""
    
    @patch('requests.get')
    def test_scrape_url_success(self, mock_get):
        """Sunshine test: Successful URL scraping."""
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '<html><body>Test content</body></html>'
        mock_get.return_value = mock_response
        
        scraper = ScraperResource()
        
        # Act
        result = scraper.scrape_url('https://test.com')
        
        # Assert
        assert result['success'] is True
        assert result['url'] == 'https://test.com'
        assert result['content'] == '<html><body>Test content</body></html>'
        assert 'url_hash' in result
        assert result['url_hash'] == hashlib.sha256('https://test.com'.encode()).hexdigest()
    
    @patch('requests.get')
    def test_scrape_url_error(self, mock_get):
        """Rainy test: Handle scraping errors gracefully."""
        # Arrange
        mock_get.side_effect = Exception("Network error")
        scraper = ScraperResource()
        
        # Act
        result = scraper.scrape_url('https://test.com')
        
        # Assert
        assert result['success'] is False
        assert result['url'] == 'https://test.com'
        assert 'error' in result
        assert "Network error" in result['error']
    
    def test_parse_content_with_title(self):
        """Test parsing HTML content with title."""
        # Arrange
        html = """
        <html>
            <body>
                <h1 class="article-headline">Test Press Release</h1>
                <div class="article-content">
                    <p>This is test content.</p>
                    <p>Another paragraph.</p>
                </div>
            </body>
        </html>
        """
        scraper = ScraperResource()
        
        # Act
        result = scraper.parse_content(html, 'https://test.com')
        
        # Assert
        assert result['title'] == 'Test Press Release'
        assert 'This is test content' in result['content']
        assert 'Another paragraph' in result['content']
        assert result['url'] == 'https://test.com'
    
    def test_parse_content_no_title(self):
        """Test parsing HTML content without title."""
        # Arrange
        html = "<html><body><p>Content only</p></body></html>"
        scraper = ScraperResource()
        
        # Act
        result = scraper.parse_content(html, 'https://test.com')
        
        # Assert
        assert result['title'] == 'No title found'
        assert 'Content only' in result['content']
    
    @patch.dict('os.environ', {'SCRAPER_API_KEY': ''})
    def test_scrape_without_api_key(self):
        """Test scraping without API key."""
        # Arrange
        scraper = ScraperResource()
        
        # Act
        result = scraper.scrape_url('https://test.com')
        
        # Assert
        assert result['success'] is False
        assert 'No API key' in result['error']
