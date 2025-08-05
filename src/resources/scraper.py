import os
import hashlib
import requests
from datetime import datetime
from dagster import ConfigurableResource, get_dagster_logger
from bs4 import BeautifulSoup

class ScraperResource(ConfigurableResource):
    def scrape_url(self, url: str, render_js: bool = False):
        api_key = os.getenv("SCRAPER_API_KEY", "")
        
        if not api_key:
            return {'success': False, 'error': 'No API key'}
        
        response = requests.get(
            "https://app.scrapingbee.com/api/v1/",
            params={
                'api_key': api_key,
                'url': url,
                'render_js': str(render_js).lower()
            },
            timeout=30
        )
        
        if response.status_code == 200:
            return {
                'success': True,
                'url': url,
                'url_hash': hashlib.sha256(url.encode()).hexdigest(),
                'content': response.text,
                'scraped_at': datetime.utcnow().isoformat()
            }
        else:
            return {
                'success': False,
                'url': url,
                'error': f"Status code: {response.status_code}"
            }
    
    def get_sec_urls(self, limit=50):
        return [f"https://www.sec.gov/news/press-release/2024-{i}" for i in range(1, limit+1)]
    
    def parse_content(self, html, url):
        soup = BeautifulSoup(html, 'html.parser')
        
        for element in soup(['script', 'style']):
            element.decompose()
        
        title = soup.find('h1')
        title_text = title.text.strip() if title else 'No title'
        
        content = soup.get_text(separator='\n', strip=True)[:5000]
        
        return {
            'title': title_text,
            'content': content,
            'url': url
        }
