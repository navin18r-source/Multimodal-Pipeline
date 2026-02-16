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
        logging.FileHandler('shristi_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ShristiScraper:
    BASE_URL = "https://www.shristijewellery.com/collections/bridal-sets/products.json"
    
    def __init__(self, base_output_dir="data"):
        self.base_output_dir = Path(base_output_dir)
        self.base_output_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        })

    def fetch_products(self, page=1, limit=250):
        """Fetch products from Shopify JSON endpoint"""
        params = {
            "page": page,
            "limit": limit
        }
        try:
            response = self.session.get(self.BASE_URL, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching page {page}: {e}")
            return None

    def process_product(self, item):
        """Extract relevant details from a raw product item"""
        try:
            # Basic Details
            title = item.get("title")
            handle = item.get("handle")
            product_url = f"https://www.shristijewellery.com/products/{handle}" if handle else ""
            description = item.get("body_html", "")
            vendor = item.get("vendor")
            product_type = item.get("product_type")
            published_at = item.get("published_at")
            
            # Images
            images = [img.get("src") for img in item.get("images", []) if img.get("src")]
            
            # Variants (Price, SKU, etc.)
            variants = []
            min_price = float('inf')
            max_price = 0
            
            for v in item.get("variants", []):
                price = float(v.get("price", 0))
                compare_at = float(v.get("compare_at_price", 0)) if v.get("compare_at_price") else None
                
                min_price = min(min_price, price)
                max_price = max(max_price, price)
                
                variants.append({
                    "id": v.get("id"),
                    "title": v.get("title"),
                    "sku": v.get("sku"),
                    "price": price,
                    "compare_at_price": compare_at,
                    "available": v.get("available")
                })
            
            price_text = f"₹{min_price}" if min_price != float('inf') else ""
            if min_price != max_price:
                price_text += f" - ₹{max_price}"

            return {
                "product_name": title,
                "product_url": product_url,
                "description": description,
                "brand": vendor,
                "product_type": product_type,
                "price_text": price_text,
                "min_price": min_price if min_price != float('inf') else 0,
                "images": images,
                "variants": variants,
                "published_at": published_at,
                "api_id": item.get("id")
            }
            
        except Exception as e:
            logger.error(f"Error processing product {item.get('id', 'unknown')}: {e}")
            return None

    def run(self):
        """Main execution loop"""
        page = 1
        all_products = []
        
        logger.info("Starting scrape for Shristi Jewellery (Bridal Sets)...")
        
        while True:
            logger.info(f"Fetching page {page}...")
            data = self.fetch_products(page=page)
            
            if not data:
                break
                
            products = data.get("products", [])
            if not products:
                logger.info("No more products found.")
                break
            
            logger.info(f"Found {len(products)} products on page {page}")
            
            for item in products:
                product = self.process_product(item)
                if product:
                    all_products.append(product)
            
            page += 1
            time.sleep(1) # Polite delay
            
        self.save_data(all_products)
        logger.info(f"Scraping completed. Total products: {len(all_products)}")

    def save_data(self, products):
        """Save extracted data to JSON"""
        output_file = self.base_output_dir / "shristi_products.json"
        
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(products, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved {len(products)} products to {output_file}")
        except Exception as e:
            logger.error(f"Error saving data: {e}")

if __name__ == "__main__":
    # Absolute path for output as requested
    output_dir = "/Volumes/Macintosh HD/tanishq_scraper/tribal/shristijewellery/data"
    
    scraper = ShristiScraper(base_output_dir=output_dir)
    scraper.run()
