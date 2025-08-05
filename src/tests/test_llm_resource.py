import pytest
from unittest.mock import Mock, patch
from src.resources.llm import LLMResource


class TestLLMResource:
    """Tests for Ollama LLM resource."""
    
    @patch('requests.get')
    def test_connection_success(self, mock_get):
        """Sunshine test: Successful Ollama connection."""
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'models': [{'name': 'qwen2.5:0.5b'}]}
        mock_get.return_value = mock_response
        
        llm = LLMResource()
        
        # Act
        result = llm.test_connection()
        
        # Assert
        assert result is True
        mock_get.assert_called_with('http://ollama:11434/api/tags', timeout=5)
    
    @patch('requests.get')
    def test_connection_failure(self, mock_get):
        """Rainy test: Handle Ollama connection failure."""
        # Arrange
        mock_get.side_effect = Exception("Connection refused")
        llm = LLMResource()
        
        # Act
        result = llm.test_connection()
        
        # Assert
        assert result is False
    
    @patch('requests.post')
    def test_summarize_success(self, mock_post):
        """Test successful summarization."""
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'response': '• First point\n• Second point\n• Third point'
        }
        mock_post.return_value = mock_response
        
        llm = LLMResource()
        
        # Act
        result = llm.summarize("Test content", "Test title")
        
        # Assert
        assert len(result['bullet_points']) == 3
        assert result['bullet_points'][0] == 'First point'
        assert result['word_count'] > 0
        assert result['model_used'] == 'qwen2.5:0.5b'
    
    @patch('requests.post')
    def test_summarize_api_error(self, mock_post):
        """Rainy test: Handle API errors during summarization."""
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal server error"
        mock_post.return_value = mock_response
        
        llm = LLMResource()
        
        # Act
        result = llm.summarize("Test content", "Test title")
        
        # Assert
        assert 'Summary generation failed' in result['summary']
        assert result['model_used'] == 'failed'
        assert len(result['bullet_points']) == 3
