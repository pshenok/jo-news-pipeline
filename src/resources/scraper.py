import os
import hashlib
import requests
from datetime import datetime
from dagster import ConfigurableResource, get_dagster_logger
from bs4 import BeautifulSoup

class ScraperResource(ConfigurableResource):
    def scrape_url(self, url: str, render_js: bool = False):
        logger = get_dagster_logger()
        api_key = os.getenv("SCRAPER_API_KEY", "")
        
        if not api_key:
            logger.error("SCRAPER_API_KEY not set")
            return {'success': False, 'error': 'No API key'}
        
        logger.info(f"Scraping: {url}")
        
        params = {
            'api_key': api_key,
            'url': url,
            'render_js': str(render_js).lower(),
            'premium_proxy': 'false',
            'country_code': 'us'
        }
        
        try:
            response = requests.get(
                "https://app.scrapingbee.com/api/v1/",
                params=params,
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
        except Exception as e:
            return {
                'success': False,
                'url': url,
                'error': str(e)
            }
    
    def get_sec_urls(self, limit=50):
        """Get actual SEC press release URLs from the listing page."""
        logger = get_dagster_logger()
        urls = []
        page = 0
        
        while len(urls) < limit and page < 5:  # Max 5 pages
            listing_url = f"https://www.sec.gov/newsroom/press-releases?page={page}"
            logger.info(f"Fetching listing page: {listing_url}")
            
            result = self.scrape_url(listing_url, render_js=True)
            
            if result['success']:
                soup = BeautifulSoup(result['content'], 'html.parser')
                
                # Find press release links
                selectors = [
                    'a[href*="/news/press-release/"]',
                    'a[href*="/newsroom/press-release/"]',
                    'article a[href*="press-release"]',
                    '.views-row a[href*="press-release"]',
                    'td.views-field-field-display-title a',
                    '.view-content a[href*="press-release"]'
                ]
                
                found_on_page = False
                for selector in selectors:
                    links = soup.select(selector)
                    if links:
                        logger.info(f"Found {len(links)} links with selector: {selector}")
                        for link in links:
                            href = link.get('href', '')
                            if href:
                                if not href.startswith('http'):
                                    href = f"https://www.sec.gov{href}"
                                # Only add if it's a press release URL and not already in list
                                if 'press-release' in href and href not in urls:
                                    urls.append(href)
                                    found_on_page = True
                                    if len(urls) >= limit:
                                        break
                        if len(urls) >= limit:
                            break
                
                if not found_on_page:
                    logger.warning(f"No links found on page {page}")
                    # Try to find any links that might be press releases
                    all_links = soup.find_all('a', href=True)
                    for link in all_links:
                        href = link.get('href', '')
                        if '/2025-' in href or '/2024-' in href:  # Look for year patterns
                            if not href.startswith('http'):
                                href = f"https://www.sec.gov{href}"
                            if href not in urls:
                                urls.append(href)
                                found_on_page = True
                                if len(urls) >= limit:
                                    break
                    
                    if not found_on_page:
                        break
                    
                page += 1
            else:
                logger.error(f"Failed to fetch listing page: {result.get('error')}")
                break
        
        # If no URLs found from listing, use a fallback with current year
        if not urls:
            logger.warning("No URLs found from listing pages, using fallback URLs")
            current_year = datetime.now().year
            # Try to get recent press releases based on current year
            for year in [current_year, current_year - 1]:
                for i in range(200, 150, -1):  # Start from higher numbers (more recent)
                    url = f"https://www.sec.gov/news/press-release/{year}-{i}"
                    urls.append(url)
                    if len(urls) >= limit:
                        break
                if len(urls) >= limit:
                    break
        
        logger.info(f"Returning {len(urls[:limit])} URLs")
        return urls[:limit]
    
    def parse_content(self, html, url):
        soup = BeautifulSoup(html, 'html.parser')
        
        # Remove scripts and styles
        for element in soup(['script', 'style']):
            element.decompose()
        
        # Find title
        title = None
        title_selectors = [
            'h1.article__headline',
            'h1.page-title',
            'h1',
            '.article__headline',
            'meta[property="og:title"]'
        ]
        
        for selector in title_selectors:
            if selector.startswith('meta'):
                elem = soup.find('meta', property='og:title')
                if elem:
                    title = elem.get('content', '')
                    break
            else:
                elem = soup.select_one(selector)
                if elem:
                    title = elem.get_text(strip=True)
                    break
        
        if not title:
            title = 'No title found'
        
        # Get content
        content = ""
        content_selectors = [
            '.article__content',
            '.article__body',
            '.field--name-body',
            'article .content',
            'main .content',
            '.region-content'
        ]
        
        for selector in content_selectors:
            elem = soup.select_one(selector)
            if elem:
                paragraphs = elem.find_all(['p', 'li'])
                if paragraphs:
                    content = '\n\n'.join([p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)])
                    break
        
        if not content:
            # Fallback: get all text
            content = soup.get_text(separator='\n', strip=True)
            lines = [line.strip() for line in content.split('\n') if line.strip()]
            content = '\n'.join(lines[:100])  # Limit to first 100 lines
        
        # Try to extract publication date
        published_at = None
        date_selectors = [
            'time[datetime]',
            '.date-display-single',
            '.field--name-field-display-date',
            'meta[property="article:published_time"]'
        ]
        
        for selector in date_selectors:
            if selector.startswith('meta'):
                elem = soup.find('meta', property='article:published_time')
                if elem:
                    try:
                        published_at = datetime.fromisoformat(elem.get('content', '').replace('Z', '+00:00'))
                    except:
                        pass
            else:
                elem = soup.select_one(selector)
                if elem:
                    if elem.get('datetime'):
                        try:
                            published_at = datetime.fromisoformat(elem.get('datetime').replace('Z', '+00:00'))
                        except:
                            pass
                    else:
                        # Try to parse text date
                        date_text = elem.get_text(strip=True)
                        # Add date parsing logic here if needed
        
        return {
            'title': title[:500],
            'content': content[:5000],
            'url': url,
            'published_at': published_at
        }
