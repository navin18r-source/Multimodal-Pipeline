import requests
import json
import time
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('gahane_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class GahaneScraper:
    BASE_URL = "https://gahanejewellry.com/wp-json/wc/store/products"
    
    def __init__(self, base_output_dir="data"):
        self.base_output_dir = Path(base_output_dir)
        self.base_output_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        })

    def fetch_products(self, page=1, per_page=50):
        """Fetch products from WooCommerce Store API"""
        params = {
            "page": page,
            "per_page": per_page
        }
        try:
            response = self.session.get(self.BASE_URL, params=params)
            response.raise_for_status()
            
            # Check headers for total pages (optional, but good for progress)
            total_pages = response.headers.get("X-WP-TotalPages")
            total_items = response.headers.get("X-WP-Total")
            
            return response.json(), total_pages, total_items
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching page {page}: {e}")
            return None, 0, 0

    def process_product(self, item):
        """Extract relevant details from a raw product item"""
        try:
            # Basic Details
            name = item.get("name")
            product_url = item.get("permalink")
            description = item.get("description", "")
            short_description = item.get("short_description", "")
            sku = item.get("sku", "")
            
            # Categories
            categories = [cat.get("name") for cat in item.get("categories", [])]
            
            # Price Calculation (minor units)
            prices = item.get("prices", {})
            price_minor = prices.get("price", "0")
            minor_unit = prices.get("currency_minor_unit", 2)
            currency_symbol = prices.get("currency_symbol", "₹")
            
            try:
                price_val = float(price_minor) / (10 ** minor_unit)
                price_text = f"{currency_symbol}{price_val}"
            except (ValueError, TypeError):
                price_val = 0
                price_text = ""

            # Images
            images = [img.get("src") for img in item.get("images", []) if img.get("src")]
            
            # Attributes
            attributes = {}
            for attr in item.get("attributes", []):
                attr_name = attr.get("name")
                attr_terms = [t.get("name") for t in attr.get("terms", [])]
                if attr_name:
                    attributes[attr_name] = attr_terms

            return {
                "product_name": name,
                "product_url": product_url,
                "description": description,
                "short_description": short_description,
                "sku": sku,
                "categories": categories,
                "price": price_val,
                "price_text": price_text,
                "images": images,
                "attributes": attributes,
                "api_id": item.get("id")
            }
            
        except Exception as e:
            logger.error(f"Error processing product {item.get('id', 'unknown')}: {e}")
            return None

    def run(self):
        """Main execution loop"""
        page = 1
        all_products = []
        per_page = 50
        
        logger.info("Starting scrape for Gahane Jewellery...")
        
        while True:
            logger.info(f"Fetching page {page}...")
            products, total_pages, total_items = self.fetch_products(page=page, per_page=per_page)
            
            if not products:
                break
            
            logger.info(f"Found {len(products)} products on page {page} (Total items available: {total_items})")
            
            for item in products:
                product = self.process_product(item)
                if product:
                    all_products.append(product)
            
            # Check if we reached the last page or empty results
            if len(products) < per_page:
                logger.info("Reached last page.")
                break
                
            page += 1
            time.sleep(1) # Polite delay
            
        self.save_data(all_products)
        logger.info(f"Scraping completed. Total products: {len(all_products)}")

    def save_data(self, products):
        """Save extracted data to JSON"""
        output_file = self.base_output_dir / "gahane_products.json"
        
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(products, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved {len(products)} products to {output_file}")
        except Exception as e:
            logger.error(f"Error saving data: {e}")

if __name__ == "__main__":
    # Absolute path for output as requested
    output_dir = "/Volumes/Macintosh HD/tanishq_scraper/bengal/gahane/data"
    
    scraper = GahaneScraper(base_output_dir=output_dir)
    scraper.run()
