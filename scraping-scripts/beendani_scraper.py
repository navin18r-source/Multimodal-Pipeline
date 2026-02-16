import requests
from bs4 import BeautifulSoup
import json
import time
import logging
from typing import List, Dict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BeendaniScraper:
    BASE_URL = "https://www.beendani.in"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
    
    def scrape_products(self) -> List[Dict]:
        """Scrape all products from Rajasthani Jewellery category"""
        all_products = []
        
        # Main jewelry category - 22 pages with 421 items total
        base_url = f"{self.BASE_URL}/rajasthani-jewellery-18.html"
        
        for page in range(1, 23):  # 22 pages total
            if page == 1:
                url = base_url
            else:
                url = f"{base_url}?page={page}"
            
            logger.info(f"Fetching page {page}/22: {url}")
            
            try:
                response = self.session.get(url, timeout=15)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find product links
                product_links = soup.find_all('a', href=lambda x: x and '/product/' in x)
                
                # Deduplicate product URLs
                seen_urls = set()
                for link in product_links:
                    product_url = link.get('href', '')
                    if product_url and product_url not in seen_urls:
                        seen_urls.add(product_url)
                        
                        # Extract product name from link text or image alt
                        product_name = link.get_text(strip=True)
                        if not product_name:
                            img = link.find('img')
                            product_name = img.get('alt', '') if img else ''
                        
                        # Extract product ID from URL
                        product_id = product_url.split('/')[-1].replace('.html', '')
                        
                        product_data = {
                            "id": product_id,
                            "product_name": product_name,
                            "product_url": product_url if product_url.startswith('http') else f"{self.BASE_URL}{product_url}",
                            "category": "Rajasthani Poshak & Rajputi Jewelry",
                            "vendor": "Beendani",
                            "region": "Rajasthan",
                            "cultural_type": "Marwadi Choket, Bajuband, Aad, Pochi, Meti Haar, Rani Har, Rakhadi, Borla"
                        }
                        
                        all_products.append(product_data)
                
                logger.info(f"Page {page}: Found {len(seen_urls)} unique products")
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error fetching page {page}: {e}")
                continue
        
        # Deduplicate final list
        unique_products = []
        seen_ids = set()
        for product in all_products:
            if product['id'] not in seen_ids:
                seen_ids.add(product['id'])
                unique_products.append(product)
        
        return unique_products
    
    def save_data(self, products: List[Dict], filename: str = 'data/beendani_products.json'):
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(products, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved {len(products)} products to {filename}")

if __name__ == "__main__":
    scraper = BeendaniScraper()
    products = scraper.scrape_products()
    scraper.save_data(products)
    logger.info(f"Total unique products scraped: {len(products)}")
