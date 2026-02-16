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
        logging.FileHandler('bhima_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BhimaScraper:
    BASE_URL = "https://prod-apis.bhimagold.com/api/app/product/products"
    
    def __init__(self, base_output_dir="data"):
        self.base_output_dir = Path(base_output_dir)
        self.base_output_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        
        # Headers mimicking a real browser to bypass basic Cloudflare checks
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Referer": "https://www.bhimagold.com/",
            "Origin": "https://www.bhimagold.com",
            "Accept-Language": "en-US,en;q=0.9",
            "sec-ch-ua": '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site"
        })

    def fetch_page(self, page_number):
        """Fetch a single page of products"""
        params = {
            "country": "En-in",
            "pageNumber": page_number,
            "collections": "Alljewellery"
        }
        
        try:
            response = self.session.get(self.BASE_URL, params=params, timeout=30)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 403:
                logger.error(f"Page {page_number}: Access Forbidden (403). Cloudflare might be blocking requests.")
                return None
            else:
                logger.error(f"Page {page_number}: Request failed with status {response.status_code}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Page {page_number}: Request error: {e}")
            return None

    def process_product(self, item):
        """Extract relevant details from a raw product item"""
        try:
            slug = item.get("slug")
            product_url = f"https://www.bhimagold.com/products/{slug}" if slug else ""
            
            # Price Extraction (Handling the x100 format usually seen in APIs)
            # Example: 5903100 -> 59031.00
            price_raw = 0
            price_discounted_raw = 0
            
            variant_items = item.get("variantItems", [])
            if variant_items:
                variant = variant_items[0]
                price_raw = variant.get("price", 0)
                price_discounted_raw = variant.get("priceDiscounted", 0)
            else:
                # Fallback if variantItems is empty but these fields exist at top level (sometimes happens)
                price_discounted_raw = item.get("converted_special_price", 0)

            # Convert to float
            price_mrp = price_raw / 100.0 if price_raw else 0
            price_selling = price_discounted_raw / 100.0 if price_discounted_raw else 0
            
            # Images
            images = []
            if item.get("image"):
                images.append(item.get("image"))
            if variant_items:
                v_img = variant_items[0].get("image")
                if v_img and v_img not in images:
                    images.append(v_img)

            # Metadata
            category = item.get("CategoryName")
            
            return {
                "product_name": item.get("title"),
                "product_url": product_url,
                "price_mrp": price_mrp,
                "price_selling": price_selling,
                "currency": "INR",
                "images": images,
                "category": category,
                "id": item.get("id"),
                "slug": slug,
                "has_variants": len(variant_items) > 1,
                "variant_count": len(variant_items)
            }
            
        except Exception as e:
            logger.error(f"Error processing product {item.get('id', 'unknown')}: {e}")
            return None

    def run(self):
        """Main execution loop"""
        page = 1
        all_products = []
        total_count = None
        
        logger.info("Starting scrape for Bhima Gold (All Jewellery)...")
        
        while True:
            logger.info(f"Fetching page {page}...")
            data = self.fetch_page(page)
            
            if not data:
                logger.warning(f"Stopping at page {page} due to error or empty response.")
                break
            
            # Check structure
            resp_data = data.get("data", {})
            products_list = resp_data.get("productList", [])
            
            if not products_list:
                logger.info(f"No products found on page {page}. Scraping finished.")
                break
                
            if total_count is None:
                total_count = resp_data.get("count")
                logger.info(f"Total products to fetch: {total_count}")

            logger.info(f"Found {len(products_list)} products on page {page}")
            
            for item in products_list:
                product = self.process_product(item)
                if product:
                    all_products.append(product)
            
            # Save progress every 10 pages
            if page % 10 == 0:
                self.save_data(all_products, partial=True)
            
            page += 1
            # Polite delay to avoid tripping Cloudflare rate limits more aggressively
            time.sleep(1.5) 
            
        self.save_data(all_products)
        logger.info(f"Scraping completed. Total products collected: {len(all_products)}")

    def save_data(self, products, partial=False):
        """Save extracted data to JSON"""
        filename = "bhima_products_partial.json" if partial else "bhima_products.json"
        output_file = self.base_output_dir / filename
        
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(products, f, ensure_ascii=False, indent=2)
            if not partial:
                logger.info(f"Saved {len(products)} products to {output_file}")
        except Exception as e:
            logger.error(f"Error saving data: {e}")

if __name__ == "__main__":
    # Absolute path for output as requested
    output_dir = "/Volumes/Macintosh HD/tanishq_scraper/karnataka/bhima/data"
    
    scraper = BhimaScraper(base_output_dir=output_dir)
    scraper.run()
