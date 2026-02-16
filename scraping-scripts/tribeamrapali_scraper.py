import requests
import json
import time
import logging
from datetime import datetime
from pathlib import Path

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tribeamrapali_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TribeAmrapaliScraper:
    """
    Scraper for Tribe Amrapali (www.tribeamrapali.com/tribal).
    Uses the internal API /CatalogSEO/getItemsFromDB to extract all products directly.
    """
    
    API_URL = "https://www.tribeamrapali.com/CatalogSEO/getItemsFromDB"
    SITE_BASE = "https://www.tribeamrapali.com"
    
    def __init__(self, base_output_dir="tribal-data"):
        self.base_output_dir = Path(base_output_dir)
        self.base_output_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://www.tribeamrapali.com/tribal?pg=1"
        })
    
    def fetch_products(self, page=1):
        """Fetch products for a specific page from the API"""
        payload = {
            "SEOID": 42,
            "SearchText": "",
            "Page": page,
            "Material": "",
            "Look": "",
            "Finish": "",
            "ByType": "",
            "CollectionName": "",
            "PriceFrom": "",
            "PriceTill": "",
            "Colors": "",
            "Genre": "",
            "NoOfPrduct": 28,  # Default from site
            "Sort": "",
            "PriceSearchRangeID": 1
        }
        
        try:
            response = self.session.post(self.API_URL, data=payload)
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to fetch page {page}: Status {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error fetching page {page}: {e}")
            return None

    def process_product(self, item):
        """Process a single product item from the API JSON response"""
        try:
            # Basic info
            product_url = f"{self.SITE_BASE}/{item.get('SEOURLKey', '')}"
            name = item.get('ProductName', '')
            price = item.get('PriceToShow', 0)
            symbol = item.get('PriceToShowSymbol', '₹')
            price_text = f"{symbol}{int(price)}" if price else ""
            
            # Description
            # The API gives full description and short description.
            # We can combine them or pick the most relevant.
            desc = item.get('ProductDescription', '')
            short_desc = item.get('ProductDescriptionShort', '')
            full_description = f"{desc}\n\n{short_desc}".strip()
            
            # Images
            # API provides Image1FileName through Image10FileName
            images = []
            for i in range(1, 11):
                key = f"Image{i}FileName"
                img_url = item.get(key)
                if img_url:
                    if not img_url.startswith('http'):
                        img_url = f"{self.SITE_BASE}{img_url}" if img_url.startswith('/') else f"{self.SITE_BASE}/{img_url}"
                    images.append(img_url)
            
            # Thumbnail
            thumbnail = item.get('ImageThumbnail1', '')
            
            # SKU/Style Number
            style_number = item.get('ProductSKU', '')
            
            # Categories/Specs
            material = item.get('ProductMaterial', '')
            look = item.get('ProductLook', '')
            finish = item.get('ProductFinish', '')
            dimensions = item.get('ProductDimensionsText', '')
            
            if dimensions:
                full_description += f"\n\nDimensions: {dimensions}"

            return {
                'product_url': product_url,
                'product_name': name,
                'price_text': price_text,
                'price': f"{symbol}{price}",
                'description': full_description,
                'style_number': style_number,
                'material': material,
                'look': look,
                'finish': finish,
                'images': images,
                'primary_image': images[0] if images else thumbnail,
                'thumbnail': thumbnail,
                'scraped_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error processing item {item.get('ProductSKU', 'Unknown')}: {e}")
            return None

    def run(self, test_mode=False):
        """Main execution method"""
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        logger.info("="*70)
        logger.info("TRIBE AMRAPALI API SCRAPER")
        logger.info("="*70)
        
        all_products = []
        page = 1
        total_pages = 1 # Will be updated after first request
        
        while page <= total_pages:
            logger.info(f"📄 Fetching page {page}/{total_pages}...")
            
            data = self.fetch_products(page)
            if not data:
                break
            
            # Update total pages from API response
            if page == 1:
                total_pages = data.get('PagesTotal', 1)
                logger.info(f"📊 Total pages to scrape: {total_pages}")
            
            items = data.get('ListOfItems', [])
            if not items:
                logger.info("   No items found in this page.")
                break
            
            logger.info(f"   Found {len(items)} items.")
            
            for item in items:
                product_data = self.process_product(item)
                if product_data:
                    all_products.append(product_data)
            
            # Save intermediate results every 5 pages
            if page % 5 == 0:
                self.save_data(all_products)
                
            if test_mode and page >= 1:
                logger.info("🧪 TEST MODE: Stopping after 1 page")
                break
                
            page += 1
            time.sleep(1) # Be polite
            
        # Final save
        self.save_data(all_products)
        logger.info(f"🎉 All tasks completed! Total products: {len(all_products)}")

    def save_data(self, products):
        """Save data to JSON files"""
        if not products:
            return
            
        # Deduplicate by URL just in case
        unique_products = {p['product_url']: p for p in products}.values()
        products_list = list(unique_products)
        
        tribal_file = self.base_output_dir / "tribal_products.json"
        with open(tribal_file, 'w', encoding='utf-8') as f:
            json.dump(products_list, f, indent=2, ensure_ascii=False)
        
        all_file = self.base_output_dir / "all_products.json"
        with open(all_file, 'w', encoding='utf-8') as f:
            json.dump({"tribal": products_list}, f, indent=2, ensure_ascii=False)
            
        logger.info(f"💾 Saved {len(products_list)} products to {tribal_file}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--test', action='store_true', help='Run in test mode (only 1 page)')
    args = parser.parse_args()
    
    # Use the absolute path for output
    output_dir = "/Volumes/Macintosh HD/tanishq_scraper/tribal/tribeamrapali/data"
    
    scraper = TribeAmrapaliScraper(base_output_dir=output_dir)
    scraper.run(test_mode=args.test)
