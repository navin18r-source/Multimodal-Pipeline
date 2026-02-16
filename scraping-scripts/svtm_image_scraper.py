import requests
import json
import time
import logging
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('svtm_images.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SVTMImageScraper:
    """
    Image-only scraper for SVTM Jewels Antique Collection
    Extracts only product images from the collection pages
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
    
    def scrape_collection_images(self):
        """
        Scrape all product images from the antique-collection.
        Returns list of image URLs.
        """
        logger.info("="*60)
        logger.info("SVTM Jewels - Antique Collection Image Scraper")
        logger.info("Target: 394 products")
        logger.info("="*60)
        
        all_images = []
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
                
                # Use the correct selector: #product-grid .grid__item
                product_items = soup.select('#product-grid .grid__item')
                
                if not product_items:
                    logger.info(f"No products found on page {page}. Stopping.")
                    break
                
                logger.info(f"Found {len(product_items)} products on page {page}")
                
                # Extract images from each product card
                for item in product_items:
                    product_data = self.extract_images_from_card(item)
                    if product_data:
                        all_images.append(product_data)
                
                logger.info(f"Total images collected so far: {len(all_images)}")
                
                page += 1
                time.sleep(1)  # Polite delay
                
            except Exception as e:
                logger.error(f"Error fetching page {page}: {e}")
                break
        
        logger.info("="*60)
        logger.info(f"COMPLETE: Scraped {len(all_images)} products")
        logger.info("="*60)
        
        return all_images
    
    def extract_images_from_card(self, item):
        """
        Extract all image URLs from a single product card.
        """
        try:
            # Product title (for reference)
            title_elem = item.select_one('.full-unstyled-link')
            product_name = title_elem.get_text(strip=True) if title_elem else 'Unknown'
            
            # Product URL
            product_url = ''
            if title_elem and title_elem.get('href'):
                product_url = title_elem['href']
                if not product_url.startswith('http'):
                    product_url = f"{self.SITE_BASE}{product_url}"
            
            # Extract all images from the card
            images = []
            img_elements = item.select('.card__media img, img')
            
            for img in img_elements:
                # Try multiple attributes
                src = img.get('src', '') or img.get('data-src', '') or img.get('srcset', '')
                
                if src:
                    # Handle srcset (take the first URL)
                    if 'srcset' in src or ',' in src:
                        src = src.split(',')[0].split()[0]
                    
                    # Ensure it's a full URL
                    if src.startswith('//'):
                        src = f"https:{src}"
                    elif not src.startswith('http'):
                        src = f"{self.SITE_BASE}{src}"
                    
                    # Only include CDN images
                    if '/cdn/shop/files/' in src:
                        # Remove size parameters to get full resolution
                        base_url = src.split('?')[0]
                        images.append(base_url)
            
            # Remove duplicates
            images = list(set(images))
            
            return {
                'product_name': product_name,
                'product_url': product_url,
                'images': images,
                'image_count': len(images),
                'scraped_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error extracting images from card: {e}")
            return None
    
    def save_data(self, data, filename):
        """Save data to JSON file"""
        filepath = self.output_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved: {filepath}")
    
    def run(self):
        """Main workflow"""
        # Scrape all images
        products_with_images = self.scrape_collection_images()
        
        if not products_with_images:
            logger.error("No images found. Exiting.")
            return
        
        # Save results
        self.save_data(products_with_images, "svtm_antique_images.json")
        
        # Create a flat list of all image URLs
        all_image_urls = []
        for product in products_with_images:
            all_image_urls.extend(product['images'])
        
        # Save flat list
        self.save_data(all_image_urls, "svtm_image_urls.json")
        
        # Summary
        logger.info("="*60)
        logger.info(f"Total products: {len(products_with_images)}")
        logger.info(f"Total images: {len(all_image_urls)}")
        logger.info(f"Average images per product: {len(all_image_urls)/len(products_with_images):.1f}")
        logger.info("="*60)

if __name__ == "__main__":
    scraper = SVTMImageScraper()
    scraper.run()
