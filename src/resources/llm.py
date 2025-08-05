import os
import requests
from dagster import ConfigurableResource, get_dagster_logger
from typing import Dict, Any

class LLMResource(ConfigurableResource):
    def test_connection(self) -> bool:
        logger = get_dagster_logger()
        try:
            ollama_host = os.getenv("OLLAMA_HOST", "ollama")
            ollama_port = os.getenv("OLLAMA_PORT", "11434")
            
            response = requests.get(f"http://{ollama_host}:{ollama_port}/api/tags", timeout=5)
            if response.status_code == 200:
                logger.info("Ollama is accessible")
                return True
            else:
                logger.error(f"Ollama returned status {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"Failed to connect to Ollama: {str(e)}")
            return False
    
    def summarize(self, content: str, title: str = "") -> Dict[str, Any]:
        logger = get_dagster_logger()
        
        try:
            ollama_host = os.getenv("OLLAMA_HOST", "ollama")
            ollama_port = os.getenv("OLLAMA_PORT", "11434")
            model = os.getenv("LLM_MODEL", "qwen2.5:0.5b")
            
            content = content[:2000] if content else "No content available"
            
            prompt = f"""Summarize this SEC press release into EXACTLY 3 bullet points.
Each bullet point should be concise and factual.
Total word count for all 3 bullets must be ≤50 words.

Title: {title or 'No title'}

Content:
{content}

Format your response as:
• First key point
• Second key point  
• Third key point"""
            
            response = requests.post(
                f"http://{ollama_host}:{ollama_port}/api/generate",
                json={
                    "model": model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": 0.3,
                        "num_predict": 150
                    }
                },
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                summary_text = result.get('response', '').strip()
                
                bullet_points = []
                for line in summary_text.split('\n'):
                    line = line.strip()
                    if line and (line.startswith('•') or line.startswith('-') or line.startswith('*')):
                        bullet_points.append(line.lstrip('•-* ').strip())
                
                if len(bullet_points) < 3:
                    sentences = summary_text.split('.')
                    bullet_points = [s.strip() for s in sentences if s.strip()][:3]
                
                while len(bullet_points) < 3:
                    bullet_points.append("Detail unavailable")
                bullet_points = bullet_points[:3]
                
                word_count = sum(len(point.split()) for point in bullet_points)
                
                return {
                    'summary': '\n'.join([f'• {point}' for point in bullet_points]),
                    'bullet_points': bullet_points,
                    'word_count': word_count,
                    'model_used': model
                }
            else:
                raise Exception(f"Ollama API returned status {response.status_code}: {response.text}")
                
        except Exception as e:
            logger.error(f"Summarization error: {str(e)}")
            return {
                'summary': "• Summary generation failed\n• Error in processing\n• Please retry",
                'bullet_points': ["Summary generation failed", "Error in processing", "Please retry"],
                'word_count': 8,
                'model_used': "failed"
            }
