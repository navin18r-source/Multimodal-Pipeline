import requests
import json
import time
import logging
from pathlib import Path
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('swadesh_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SwadeshScraper:
    BASE_URL = "https://www.swadeshonline.com"
    API_ENDPOINT = "https://www.swadeshonline.com/ext/search/application/api/v1.0/collections/artisanal/items"
    COLLECTION_SLUG = "artisanal"
    
    def __init__(self, base_output_dir="data"):
        self.base_output_dir = Path(base_output_dir)
        self.base_output_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.swadeshonline.com/collection/artisanal",
            "Origin": "https://www.swadeshonline.com"
        })

    def fetch_page(self, page_id=1, page_size=12):
        """Fetch a single page of products from the API"""
        params = {
            "page_id": page_id,
            "page_size": page_size,
            "sort": "latest" 
        }
        
        try:
            response = self.session.get(self.API_ENDPOINT, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching page_id {page_id}: {e}")
            return None

    def run(self, max_pages=None):
        """Main execution loop"""
        current_page_id = 1
        all_products = []
        total_items_expected = 0
        pages_fetched = 0
        
        logger.info(f"Starting scrape for collection: {self.COLLECTION_SLUG}")
        
        while True:
            if max_pages and pages_fetched >= max_pages:
                logger.info("Reached maximum page limit.")
                break
                
            logger.info(f"Fetching page_id {current_page_id}...")
            data = self.fetch_page(current_page_id)
            
            if not data:
                logger.warning("No data received, stopping.")
                break
            
            # Pagination info
            page_info = data.get("page", {})
            has_next = page_info.get("has_next", False)
            next_id = page_info.get("next_id")
            
            if pages_fetched == 0:
                total_items_expected = page_info.get("item_total", 0)
                logger.info(f"Total items expected: {total_items_expected}")
            
            items = data.get("items", [])
            item_count = len(items)
            logger.info(f"Found {item_count} items on page_id {current_page_id}")
            
            for item in items:
                product = self.process_product(item)
                if product:
                    all_products.append(product)
            
            pages_fetched += 1
            
            if not has_next or not next_id:
                logger.info("No more pages available.")
                break
            
            # Prevent infinite loop if next_id doesn't change
            if str(next_id) == str(current_page_id):
                 logger.warning(f"Next ID {next_id} is same as current {current_page_id}. Stopping loop.")
                 break
                 
            current_page_id = next_id
            time.sleep(1) # Polite delay
            
        # Save results
        self.save_data(all_products)
        logger.info(f"Scraping completed. Extracted {len(all_products)} products. Expected ~{total_items_expected}.")

    def process_product(self, item):
        """Extract relevant details from a raw product item"""
        try:
            # Basic Info
            name = item.get("name", "Unknown Product")
            slug = item.get("slug", "")
            product_url = f"{self.BASE_URL}/product/{slug}" if slug else ""
            
            # Price
            price_info = item.get("price", {}).get("effective", {})
            price = price_info.get("min", 0)
            currency = price_info.get("currency_symbol", "₹")
            price_text = f"{currency}{price}"
            
            # Description
            description = item.get("description", "")
            
            # Images
            images = []
            medias = item.get("medias", [])
            for media in medias:
                if media.get("type") == "image":
                    url = media.get("url")
                    if url:
                        images.append(url)
            
            # Attributes / Specifications
            attributes = item.get("attributes", {})
            # Merge top-level extracted attributes with the 'attributes' dict for completeness
            extracted_attributes = {
                "sku": attributes.get("identifier", {}).get("sku_code", [""])[0],
                "ean": attributes.get("identifier", {}).get("ean", [""])[0],
                "material": attributes.get("material-type"),
                "craft": attributes.get("craft-name"),
                "technique": attributes.get("craft-technique"),
                "dimensions": attributes.get("product-dimension-cms-l-x-w-x-h"),
                "care_instructions": attributes.get("care-instructions"),
                "origin": attributes.get("state-of-origin") or attributes.get("country_of_origin"),
                "brand": item.get("brand", {}).get("name"),
                "net_quantity": attributes.get("net_quantity", {}).get("value"),
                "net_quantity_unit": attributes.get("net_quantity", {}).get("unit"),
            }
            
            # Categories
            categories = []
            for cat in item.get("categories", []):
                categories.append(cat.get("name"))

            return {
                "product_name": name,
                "product_url": product_url,
                "price": price,
                "price_text": price_text,
                "description": description,
                "images": images,
                "specifications": {k: v for k, v in extracted_attributes.items() if v}, # Clean empty values
                "categories": categories,
                "api_id": item.get("uid") 
            }
            
        except Exception as e:
            logger.error(f"Error processing product {item.get('uid', 'unknown')}: {e}")
            return None

    def run(self, max_pages=None):
        """Main execution loop"""
        page_no = 1
        all_products = []
        total_items_expected = 0
        calculated_max_pages = float('inf')
        page_size = 20
        
        logger.info(f"Starting scrape for collection: {self.COLLECTION_SLUG}")
        
        while True:
            # Check explicit max_pages limit
            if max_pages and page_no > max_pages:
                logger.info("Reached user-defined maximum page limit.")
                break
            
            # Check calculated max_pages based on item_total
            if page_no > calculated_max_pages:
                logger.info(f"Reached calculated last page {calculated_max_pages}.")
                break
                
            logger.info(f"Fetching page {page_no}...")
            # Ensure we pass page_size to the fetch_page method if needed or use default
            data = self.fetch_page(page_no, page_size=page_size)
            
            if not data:
                logger.warning("No data received, stopping.")
                break
            
            # Update total pages from API response on first page
            page_info = data.get("page", {})
            if page_no == 1:
                total_items_expected = page_info.get("item_total", 0)
                if total_items_expected > 0:
                    import math
                    calculated_max_pages = math.ceil(total_items_expected / page_size)
                    logger.info(f"Total items: {total_items_expected}. Calculated max pages: {calculated_max_pages}")
            
            items = data.get("items", [])
            if not items:
                logger.info("No items found on this page, stopping.")
                break
                
            logger.info(f"Found {len(items)} items on page {page_no}")
            
            for item in items:
                product = self.process_product(item)
                if product:
                    all_products.append(product)
            
            # Safety break if we simply run too long (e.g. 50 pages for ~500 items is excessive)
            if page_no > 50 and not max_pages: 
                logger.warning("Safety limit reached (50 pages). Stopping.")
                break

            page_no += 1
            time.sleep(1) # Polite delay
            
        # Save results
        self.save_data(all_products)
        logger.info(f"Scraping completed. Extracted {len(all_products)} products. Expected ~{total_items_expected}.")

    def save_data(self, products):
        """Save extracted data to JSON"""
        output_file = self.base_output_dir / "swadesh_products.json"
        
        # Deduplicate based on product URL or ID
        seen_ids = set()
        unique_products = []
        for p in products:
            if p['api_id'] not in seen_ids:
                seen_ids.add(p['api_id'])
                unique_products.append(p)
        
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(unique_products, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved {len(unique_products)} unique products to {output_file}")
        except Exception as e:
            logger.error(f"Error saving data: {e}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--test', action='store_true', help='Test mode: scrape only first page')
    args = parser.parse_args()
    
    # Absolute path for output as requested
    output_dir = "/Volumes/Macintosh HD/tanishq_scraper/tribal/swadesh/data"
    
    scraper = SwadeshScraper(base_output_dir=output_dir)
    scraper.run(max_pages=1 if args.test else None)
