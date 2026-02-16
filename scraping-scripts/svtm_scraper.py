import requests
import json
import time
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('svtm.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SVTMScraper:
    """
    Scraper for SVTM Jewels - Tamil Nadu Antique Collection
    Platform: Shopify (Server-rendered HTML)
    Focus: /collections/antique-collection
    """
    
    SITE_BASE = "https://svtmjewels.com"
    COLLECTION_URL = f"{SITE_BASE}/collections/antique-collection"
    
    def __init__(self, output_dir="svtm_data"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
    
    def get_all_products_from_collection(self):
        """
        Scrape all products from the antique-collection with pagination.
        Returns list of product cards with basic metadata.
        """
        logger.info("Fetching products from Antique Collection...")
        all_products = []
        page = 1
        
        while True:
            url = f"{self.COLLECTION_URL}?page={page}"
            logger.info(f"Fetching page {page}: {url}")
            
            try:
                response = self.session.get(url, timeout=15)
                if response.status_code != 200:
                    logger.error(f"Failed to fetch page {page}: {response.status_code}")
                    break
                
                soup = BeautifulSoup(response.text, 'lxml')
                
                # Find all product cards - try multiple Shopify selectors
                product_cards = soup.select('[data-product-id]')
                
                if not product_cards:
                    product_cards = soup.select('.product-card, .product-item, .grid-product, .product')
                
                if not product_cards:
                    logger.info(f"No products found on page {page}. Stopping.")
                    break
                
                logger.info(f"Found {len(product_cards)} products on page {page}")
                
                for card in product_cards:
                    product_data = self.parse_product_card(card)
                    if product_data:
                        all_products.append(product_data)
                
                page += 1
                time.sleep(1)  # Polite delay
                
            except Exception as e:
                logger.error(f"Error fetching page {page}: {e}")
                break
        
        logger.info(f"Total products found: {len(all_products)}")
        return all_products
    
    def parse_product_card(self, card):
        """
        Extract metadata from a single product card.
        """
        try:
            # Product ID
            product_id = card.get('data-product-id', '')
            
            # Product Handle
            product_handle = card.get('data-product-handle', '')
            
            # Product URL
            link = card.find('a', href=True)
            product_url = link['href'] if link else ''
            if product_url and not product_url.startswith('http'):
                product_url = f"{self.SITE_BASE}{product_url}"
            
            # Product Name
            title_elem = card.select_one('.product-card__title, .grid-product__title, .product-title, h3, h2')
            product_name = title_elem.get_text(strip=True) if title_elem else ''
            
            # Price
            price_elem = card.select_one('.product__price, .price, .money, .grid-product__price')
            price_text = price_elem.get_text(strip=True) if price_elem else ''
            price = price_text.replace('₹', '').replace('Rs.', '').replace(',', '').strip()
            
            # Availability
            sold_out = card.select_one('.sold-out, .badge--sold-out')
            availability = 'Sold Out' if sold_out else 'Available'
            
            # Images
            img = card.find('img')
            primary_image = img.get('src', '') if img else ''
            if primary_image and not primary_image.startswith('http'):
                primary_image = f"https:{primary_image}"
            
            return {
                'product_id': product_id,
                'product_handle': product_handle,
                'product_name': product_name,
                'price': price,
                'currency': 'INR',
                'availability': availability,
                'product_url': product_url,
                'primary_image': primary_image,
                'scraped_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error parsing product card: {e}")
            return None
    
    def get_product_details(self, product_url):
        """
        Visit product detail page and extract additional metadata.
        """
        try:
            response = self.session.get(product_url, timeout=15)
            if response.status_code != 200:
                logger.warning(f"Failed to fetch {product_url}: {response.status_code}")
                return {}
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # Product Name (from detail page)
            title_elem = soup.select_one('h1.product-single__title, h1')
            product_name = title_elem.get_text(strip=True) if title_elem else ''
            
            # Price (from detail page)
            price_elem = soup.select_one('.product__price, span.product__price')
            price_text = price_elem.get_text(strip=True) if price_elem else ''
            price = price_text.replace('₹', '').replace('Rs.', '').replace(',', '').strip()
            
            # Description
            description = ''
            rte_blocks = soup.select('.rte')
            for rte in rte_blocks:
                if 'Description:' in rte.get_text() or len(rte.get_text(strip=True)) > 50:
                    description = rte.get_text(strip=True)
                    break
            
            if not description:
                desc_elem = soup.select_one('.product-single__description, .product__description')
                description = desc_elem.get_text(strip=True) if desc_elem else ''
            
            # All images
            images = []
            img_elems = soup.select('.product__thumb img, .product__photo img, .photoswipe__image, .product-single__photo img')
            for img in img_elems:
                src = img.get('src', '') or img.get('data-src', '') or img.get('data-photoswipe-src', '')
                if src and not src.startswith('http'):
                    src = f"https:{src}"
                if src and '/cdn/shop/files/' in src:
                    images.append(src)
            
            # SKU
            sku_elem = soup.select_one('[itemprop="sku"], .product-sku')
            sku = sku_elem.get_text(strip=True) if sku_elem else ''
            
            return {
                'product_name': product_name,
                'price': price,
                'description': description,
                'all_images': ' | '.join(images) if images else '',
                'sku': sku
            }
            
        except Exception as e:
            logger.error(f"Error fetching details from {product_url}: {e}")
            return {}
    
    def run(self, fetch_details=True):
        """
        Main scraping workflow.
        
        Args:
            fetch_details: If True, visit each product page for full details
        """
        logger.info("="*50)
        logger.info("Starting SVTM Jewels Scraper")
        logger.info("Category: Antique Collection (Tamil Nadu)")
        logger.info("="*50)
        
        # Step 1: Get all products from collection page
        products = self.get_all_products_from_collection()
        
        if not products:
            logger.error("No products found. Exiting.")
            return
        
        # Step 2: Optionally fetch detail pages
        if fetch_details:
            logger.info("Fetching product detail pages...")
            for idx, product in enumerate(products, 1):
                logger.info(f"Processing {idx}/{len(products)}: {product['product_name']}")
                
                details = self.get_product_details(product['product_url'])
                product.update(details)
                
                if idx % 20 == 0:
                    self.save_data(products, "svtm_products_partial.json")
                
                time.sleep(0.5)
        
        # Step 3: Final save
        self.save_data(products, "svtm_products_final.json")
        self.save_csv(products, "svtm_products_final.csv")
        
        logger.info("="*50)
        logger.info(f"DONE. Total products scraped: {len(products)}")
        logger.info("="*50)
    
    def save_data(self, data, filename):
        filepath = self.output_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved checkpoint: {filepath}")
    
    def save_csv(self, data, filename):
        df = pd.DataFrame(data)
        df.to_csv(self.output_dir / filename, index=False)
        logger.info(f"Saved CSV: {filename}")

if __name__ == "__main__":
    scraper = SVTMScraper()
    scraper.run(fetch_details=True)
