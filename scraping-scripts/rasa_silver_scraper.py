import requests
import json
import time
import logging
import re
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup
from html import unescape

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('rasa_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class RasaSilverScraper:
    """
    High-performance scraper for Rasa Silver jewelry products.
    Uses Shopify's JSON endpoint for efficient metadata extraction.
    """
    
    SITE_BASE = "https://rasasilver.com"
    COLLECTION_URL = f"{SITE_BASE}/collections/newest-products"
    
    def __init__(self, output_dir="rasa_data"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
    
    def get_all_product_handles(self):
        """
        Phase 1: Crawl collection pages to extract all product handles.
        Returns list of product handles.
        """
        logger.info("="*70)
        logger.info("RASA SILVER - Newest Products Collection Scraper")
        logger.info("Target: 3,459 products with complete metadata")
        logger.info("="*70)
        logger.info("PHASE 1: Extracting product handles from collection pages...")
        
        all_handles = []
        page = 1
        
        while True:
            url = f"{self.COLLECTION_URL}?page={page}"
            logger.info(f"Fetching collection page {page}...")
            
            try:
                response = self.session.get(url, timeout=15)
                if response.status_code != 200:
                    logger.error(f"Failed to fetch page {page}: {response.status_code}")
                    break
                
                soup = BeautifulSoup(response.text, 'lxml')
                
                # Extract product links using the identified selector
                product_links = soup.select('a.grid-product__link')
                
                if not product_links:
                    logger.info(f"No products found on page {page}. Collection complete.")
                    break
                
                # Extract handles from URLs
                for link in product_links:
                    href = link.get('href', '')
                    if '/products/' in href:
                        # Extract handle from URL: /products/{handle}
                        handle = href.split('/products/')[-1].split('?')[0]
                        if handle and handle not in all_handles:
                            all_handles.append(handle)
                
                logger.info(f"Page {page}: Found {len(product_links)} products. Total: {len(all_handles)}")
                
                page += 1
                time.sleep(1)  # Polite delay
                
            except Exception as e:
                logger.error(f"Error fetching page {page}: {e}")
                break
        
        logger.info(f"✅ Phase 1 Complete: Collected {len(all_handles)} product handles")
        return all_handles
    
    def fetch_product_json(self, handle):
        """
        Fetch product data from Shopify's JSON endpoint.
        URL pattern: /products/{handle}.js
        """
        url = f"{self.SITE_BASE}/products/{handle}.js"
        
        try:
            response = self.session.get(url, timeout=15)
            if response.status_code != 200:
                logger.warning(f"Failed to fetch JSON for {handle}: {response.status_code}")
                return None
            
            return response.json()
            
        except Exception as e:
            logger.error(f"Error fetching JSON for {handle}: {e}")
            return None
    
    def parse_product_json(self, product_json, handle):
        """
        Parse Shopify product JSON into structured metadata.
        """
        if not product_json:
            return None
        
        try:
            # Basic Info
            product_id = product_json.get('id', '')
            title = product_json.get('title', '')
            product_url = f"{self.SITE_BASE}/products/{handle}"
            
            # Pricing (in paise, convert to rupees)
            price_paise = product_json.get('price', 0)
            price = price_paise / 100 if price_paise else 0
            
            compare_at_price_paise = product_json.get('compare_at_price', 0)
            compare_at_price = compare_at_price_paise / 100 if compare_at_price_paise else 0
            
            # Catalog Data
            vendor = product_json.get('vendor', '')
            product_type = product_json.get('type', '')
            tags = product_json.get('tags', [])
            
            # Description (HTML and clean text)
            description_html = product_json.get('description', '')
            description_text = self.clean_html(description_html)
            
            # Images - extract all image URLs
            images = []
            for img in product_json.get('images', []):
                img_url = img if isinstance(img, str) else img.get('src', '')
                if img_url:
                    # Ensure full URL
                    if img_url.startswith('//'):
                        img_url = f"https:{img_url}"
                    elif not img_url.startswith('http'):
                        img_url = f"{self.SITE_BASE}{img_url}"
                    images.append(img_url)
            
            # Variants (different sizes, colors, etc.)
            variants = []
            for variant in product_json.get('variants', []):
                variant_price = variant.get('price', 0) / 100 if variant.get('price') else 0
                variants.append({
                    'id': variant.get('id', ''),
                    'title': variant.get('title', ''),
                    'sku': variant.get('sku', ''),
                    'price': variant_price,
                    'available': variant.get('available', False),
                    'weight': variant.get('weight', 0),
                    'weight_unit': variant.get('weight_unit', '')
                })
            
            # Availability
            available = product_json.get('available', False)
            
            # Timestamps
            created_at = product_json.get('created_at', '')
            updated_at = product_json.get('updated_at', '')
            
            return {
                'product_id': product_id,
                'handle': handle,
                'product_name': title,
                'product_url': product_url,
                'price': price,
                'compare_at_price': compare_at_price,
                'currency': 'INR',
                'vendor': vendor,
                'product_type': product_type,
                'tags': tags,
                'description_html': description_html,
                'description_text': description_text,
                'images': images,
                'image_count': len(images),
                'variants': variants,
                'variant_count': len(variants),
                'available': available,
                'created_at': created_at,
                'updated_at': updated_at,
                'scraped_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error parsing JSON for {handle}: {e}")
            return None
    
    def clean_html(self, html_text):
        """Remove HTML tags and clean text."""
        if not html_text:
            return ''
        
        # Remove HTML tags
        soup = BeautifulSoup(html_text, 'lxml')
        text = soup.get_text(separator=' ', strip=True)
        
        # Unescape HTML entities
        text = unescape(text)
        
        # Clean up whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def run(self):
        """Main scraping workflow"""
        # Phase 1: Get all product handles
        handles = self.get_all_product_handles()
        
        if not handles:
            logger.error("No product handles found. Exiting.")
            return
        
        # Phase 2: Fetch and parse product data
        logger.info("="*70)
        logger.info("PHASE 2: Fetching product metadata from JSON endpoints...")
        logger.info("="*70)
        
        full_catalog = []
        
        for idx, handle in enumerate(handles, 1):
            logger.info(f"Processing {idx}/{len(handles)}: {handle}")
            
            # Fetch JSON data
            product_json = self.fetch_product_json(handle)
            
            # Parse and structure data
            product_data = self.parse_product_json(product_json, handle)
            
            if product_data:
                full_catalog.append(product_data)
            
            # Save checkpoint every 100 products
            if idx % 100 == 0:
                self.save_data(full_catalog, "rasa_silver_partial.json")
                logger.info(f"💾 Checkpoint saved: {len(full_catalog)} products")
            
            time.sleep(0.5)  # Polite delay
        
        # Final save
        self.save_data(full_catalog, "rasa_silver_products.json")
        
        # Summary
        total_images = sum(p['image_count'] for p in full_catalog)
        total_variants = sum(p['variant_count'] for p in full_catalog)
        
        logger.info("="*70)
        logger.info("✅ SCRAPING COMPLETE!")
        logger.info(f"Total products: {len(full_catalog)}")
        logger.info(f"Total images: {total_images}")
        logger.info(f"Total variants: {total_variants}")
        logger.info(f"Average images per product: {total_images/len(full_catalog):.1f}")
        logger.info(f"Output: rasa_data/rasa_silver_products.json")
        logger.info("="*70)
    
    def save_data(self, data, filename):
        """Save data to JSON file"""
        filepath = self.output_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    scraper = RasaSilverScraper()
    scraper.run()
