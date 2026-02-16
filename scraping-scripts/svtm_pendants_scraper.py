import requests
import json
import time
import logging
import re
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('svtm_pendants.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SVTMPendantsScraper:
    """
    Complete scraper for SVTM Jewels Pendants Collection
    Extracts images + full metadata (description, price, product details)
    """
    
    SITE_BASE = "https://svtmjewels.com"
    COLLECTION_URL = f"{SITE_BASE}/collections/pendants"
    
    def __init__(self, output_dir="pendants_data"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
    
    def scrape_collection(self):
        """Scrape all products from the pendants collection."""
        logger.info("="*60)
        logger.info("SVTM Jewels - Pendants Collection Scraper")
        logger.info("Target: 87 products with complete metadata")
        logger.info("="*60)
        
        all_products = []
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
                product_items = soup.select('#product-grid .grid__item')
                
                if not product_items:
                    logger.info(f"No products found on page {page}. Stopping.")
                    break
                
                logger.info(f"Found {len(product_items)} products on page {page}")
                
                for item in product_items:
                    title_elem = item.select_one('.full-unstyled-link')
                    product_name = title_elem.get_text(strip=True) if title_elem else 'Unknown'
                    
                    product_url = ''
                    if title_elem and title_elem.get('href'):
                        product_url = title_elem['href']
                        if not product_url.startswith('http'):
                            product_url = f"{self.SITE_BASE}{product_url}"
                    
                    if product_url:
                        all_products.append({
                            'product_name': product_name,
                            'product_url': product_url
                        })
                
                page += 1
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error fetching page {page}: {e}")
                break
        
        logger.info(f"Total products found: {len(all_products)}")
        return all_products
    
    def extract_product_details(self, product_url):
        """Visit product detail page and extract complete metadata."""
        try:
            response = self.session.get(product_url, timeout=15)
            if response.status_code != 200:
                logger.warning(f"Failed to fetch {product_url}: {response.status_code}")
                return None
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # Product Title
            title_elem = soup.select_one('.product__title h1')
            product_name = title_elem.get_text(strip=True) if title_elem else ''
            
            # Price
            price_elem = soup.select_one('.price__regular .price-item--regular')
            price_text = price_elem.get_text(strip=True) if price_elem else ''
            price = price_text.replace('Rs.', '').replace(',', '').strip()
            
            # Description
            desc_elem = soup.select_one('.product__description.rte')
            description = desc_elem.get_text(strip=True) if desc_elem else ''
            
            # Extract dimensions from description
            dimensions = ''
            if description:
                dim_match = re.search(r'Height-\s*[\d.]+\s*mm,\s*Width-\s*[\d.]+\s*mm', description)
                if dim_match:
                    dimensions = dim_match.group(0)
            
            # Product Details Table
            metal = purity = net_weight = gross_weight = ''
            table_rows = soup.select('#product-details-table tr')
            for idx, row in enumerate(table_rows, 1):
                cells = row.select('td')
                if len(cells) >= 2:
                    value = cells[-1].get_text(strip=True)
                    if idx == 1:
                        metal = value
                    elif idx == 2:
                        purity = value
                    elif idx == 3:
                        net_weight = value
                    elif idx == 4:
                        gross_weight = value
            
            # All Product Images
            images = []
            img_elements = soup.select('.product__media-list img')
            
            for img in img_elements:
                src = img.get('src', '') or img.get('data-src', '')
                if src:
                    if src.startswith('//'):
                        src = f"https:{src}"
                    elif not src.startswith('http'):
                        src = f"{self.SITE_BASE}{src}"
                    
                    if '/cdn/shop/files/' in src:
                        base_url = src.split('?')[0]
                        images.append(base_url)
            
            images = list(set(images))
            
            return {
                'product_name': product_name,
                'product_url': product_url,
                'price': price,
                'currency': 'INR',
                'description': description,
                'dimensions': dimensions,
                'metal': metal,
                'purity': purity,
                'net_gold_weight': net_weight,
                'gross_weight': gross_weight,
                'images': images,
                'image_count': len(images),
                'scraped_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error extracting details from {product_url}: {e}")
            return None
    
    def run(self):
        """Main workflow"""
        products = self.scrape_collection()
        
        if not products:
            logger.error("No products found. Exiting.")
            return
        
        logger.info("="*60)
        logger.info("Extracting full metadata from product pages...")
        logger.info("="*60)
        
        full_catalog = []
        
        for idx, product in enumerate(products, 1):
            logger.info(f"Processing {idx}/{len(products)}: {product['product_name']}")
            
            details = self.extract_product_details(product['product_url'])
            if details:
                full_catalog.append(details)
            
            # Save checkpoint every 25 products
            if idx % 25 == 0:
                self.save_data(full_catalog, "svtm_pendants_partial.json")
            
            time.sleep(0.5)
        
        self.save_data(full_catalog, "svtm_pendants_collection.json")
        
        total_images = sum(p['image_count'] for p in full_catalog)
        logger.info("="*60)
        logger.info(f"✅ COMPLETE!")
        logger.info(f"Total products: {len(full_catalog)}")
        logger.info(f"Total images: {total_images}")
        logger.info(f"Output: pendants_data/svtm_pendants_collection.json")
        logger.info("="*60)
    
    def save_data(self, data, filename):
        """Save data to JSON file"""
        filepath = self.output_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"💾 Saved: {filepath}")

if __name__ == "__main__":
    scraper = SVTMPendantsScraper()
    scraper.run()
