import requests
import json
import time
import logging
from typing import List, Dict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GyawunScraper:
    BASE_URL = "https://gyawun.com"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'})
    
    def scrape_products(self) -> List[Dict]:
        all_products = []
        page = 1
        
        while True:
            url = f"{self.BASE_URL}/products.json?page={page}&limit=250"
            logger.info(f"Fetching page {page}: {url}")
            
            try:
                response = self.session.get(url, timeout=15)
                response.raise_for_status()
                data = response.json()
                products = data.get('products', [])
                
                if not products:
                    logger.info(f"No more products found on page {page}")
                    break
                
                for product in products:
                    product_data = self.parse_product(product)
                    if product_data:
                        all_products.append(product_data)
                
                logger.info(f"Page {page}: Found {len(products)} products")
                page += 1
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error fetching page {page}: {e}")
                break
        
        return all_products
    
    def parse_product(self, product: Dict) -> Dict:
        try:
            variant = product.get('variants', [{}])[0]
            images = [img['src'] for img in product.get('images', [])]
            tags = product.get('tags', [])
            if isinstance(tags, str):
                tags = [t.strip() for t in tags.split(',')]
            
            return {
                "id": str(product.get('id')),
                "product_name": product.get('title', ''),
                "product_url": f"{self.BASE_URL}/products/{product.get('handle', '')}",
                "price": variant.get('price', ''),
                "currency": "INR",
                "category": product.get('product_type', ''),
                "vendor": product.get('vendor', 'Gyawun'),
                "tags": tags,
                "description": product.get('body_html', ''),
                "images": images,
                "availability": "in_stock" if variant.get('available', False) else "out_of_stock",
                "sku": variant.get('sku', ''),
                "region": "Jammu & Kashmir",
                "cultural_type": "Vintage Kashmiri Jewelry & Crafts",
                "created_at": product.get('created_at', ''),
                "updated_at": product.get('updated_at', '')
            }
        except Exception as e:
            logger.error(f"Error parsing product {product.get('id')}: {e}")
            return None
    
    def save_data(self, products: List[Dict], filename: str = 'data/gyawun_products.json'):
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(products, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved {len(products)} products to {filename}")

if __name__ == "__main__":
    scraper = GyawunScraper()
    products = scraper.scrape_products()
    scraper.save_data(products)
    logger.info(f"Total products scraped: {len(products)}")
