import requests
import json
import time
import logging
import re
import os
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup
from html import unescape
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('surana_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SuranaJewellersScraper:
    """
    Scraper for Surana Jewellers of Jaipur (Rajasthan).
    Extracts product data from the frontpage collection (455 products across 22 pages).
    """
    
    SITE_BASE = "https://www.suranajewellersofjaipur.com"
    COLLECTION_URL = "https://www.suranajewellersofjaipur.com/collections/frontpage"
    
    # Based on analysis: 455 products, 21 per page = 22 pages
    TOTAL_PAGES = 22
    PRODUCTS_PER_PAGE = 21
    
    def __init__(self, base_output_dir="surana-data"):
        self.base_output_dir = Path(base_output_dir)
        self.base_output_dir.mkdir(parents=True, exist_ok=True)
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': self.SITE_BASE,
        })
        self.session.verify = False
        self.driver = None
    
    def setup_selenium(self):
        """Setup Selenium WebDriver"""
        if self.driver:
            return
        
        logger.info("🚀 Initializing Selenium WebDriver...")
        
        options = Options()
        options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        self.driver = webdriver.Chrome(options=options)
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        logger.info("✅ Selenium WebDriver ready")
    
    def extract_sku_from_text(self, text):
        """
        Extract SKU from product title or description.
        SKU pattern: Usually alphanumeric code at the end (e.g., KMNE3496, GDNE0507)
        """
        if not text:
            return ''
        
        # Try to find SKU pattern at the end of text
        # Pattern: Capital letters followed by numbers (e.g., KMNE3496)
        match = re.search(r'\b([A-Z]{2,}[0-9]{3,})\b', text)
        if match:
            return match.group(1)
        
        # Alternative: Look for code in parentheses
        match = re.search(r'\(([A-Z0-9]+)\)', text)
        if match:
            return match.group(1)
        
        return ''
    
    def extract_products_from_listing(self, html):
        """Extract product URLs from collection listing page"""
        soup = BeautifulSoup(html, 'lxml')
        products = []
        
        # Find all product items
        product_items = soup.find_all('div', class_='product-item')
        
        for item in product_items:
            try:
                # Find product link
                link_tag = item.find('a', href=re.compile(r'/products/'))
                if not link_tag:
                    continue
                
                product_url = link_tag.get('href', '')
                if product_url.startswith('/'):
                    product_url = f"{self.SITE_BASE}{product_url}"
                
                # Get product title from image alt attribute (link text is empty)
                img_tag = item.find('img', class_='pri-img')
                if not img_tag:
                    img_tag = item.find('img')
                
                title_text = ''
                image_url = ''
                
                if img_tag:
                    # Get title from alt attribute
                    title_text = img_tag.get('alt', '')
                    
                    # Get image URL from srcset (already loaded) or data-srcset
                    srcset = img_tag.get('srcset') or img_tag.get('data-srcset', '')
                    if srcset:
                        # Extract first URL from srcset (format: "url 180w, url 360w, ...")
                        urls = srcset.split(',')
                        if urls:
                            first_url = urls[0].strip().split(' ')[0]
                            if first_url.startswith('//'):
                                image_url = f"https:{first_url}"
                            elif first_url.startswith('/'):
                                image_url = f"{self.SITE_BASE}{first_url}"
                            else:
                                image_url = first_url
                    
                    # Fallback to src if srcset not available
                    if not image_url:
                        src = img_tag.get('src', '')
                        if src:
                            if src.startswith('//'):
                                image_url = f"https:{src}"
                            elif src.startswith('/'):
                                image_url = f"{self.SITE_BASE}{src}"
                            else:
                                image_url = src
                
                products.append({
                    'product_url': product_url,
                    'product_name': title_text,
                    'listing_image': image_url
                })
                
            except Exception as e:
                logger.warning(f"Error parsing product item: {e}")
                continue
        
        return products

    
    def fetch_product_details(self, product_url):
        """Fetch detailed product information from product page"""
        try:
            self.setup_selenium()
            self.driver.get(product_url)
            
            # Wait for page load
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(2)
            
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'lxml')
            
            details = {}
            
            # Try to extract from Shopify JSON
            # The product data is in format: product: {...}
            product_json = None
            scripts = soup.find_all('script')
            for script in scripts:
                if script.string and 'product:' in script.string and '"title"' in script.string:
                    # Try to extract product: {...} pattern
                    match = re.search(r'product:\s*({[^}]+?"title".*?"content".*?}),', script.string, re.DOTALL)
                    if match:
                        try:
                            product_json = json.loads(match.group(1))
                            break
                        except json.JSONDecodeError:
                            continue

            
            if product_json:
                # Extract from JSON
                details['product_name'] = product_json.get('title', '')
                
                # SKU from variants (often empty, so we'll also try text extraction)
                sku_from_json = ''
                if 'variants' in product_json and product_json['variants']:
                    sku_from_json = product_json['variants'][0].get('sku', '')
                
                # Extract description
                desc_html = product_json.get('description', '')
                if desc_html:
                    desc_soup = BeautifulSoup(desc_html, 'lxml')
                    desc_text = desc_soup.get_text(separator='\n', strip=True)
                    details['description'] = desc_text
                else:
                    details['description'] = ''
                
                # Try to extract SKU from title or description
                sku_from_text = self.extract_sku_from_text(details['product_name'])
                if not sku_from_text:
                    sku_from_text = self.extract_sku_from_text(details['description'])
                
                # Prefer text-extracted SKU if JSON SKU is empty
                details['product_code'] = sku_from_json if sku_from_json else sku_from_text
                
                # Price (usually "Request For Price")
                price_cents = product_json.get('price', 0)
                if price_cents > 0:
                    details['price'] = f"₹ {price_cents / 100:,.0f}"
                else:
                    details['price'] = "Request For Price"
                
                # Images from JSON
                images = []
                if 'images' in product_json:
                    for img_path in product_json['images']:
                        if img_path.startswith('//'):
                            img_url = f"https:{img_path}"
                        elif img_path.startswith('/'):
                            img_url = f"{self.SITE_BASE}{img_path}"
                        else:
                            img_url = img_path
                        images.append(img_url)
                
                details['images'] = images
                details['primary_image'] = images[0] if images else ''
                
            else:
                # Fallback to CSS selectors
                logger.warning(f"Could not find product JSON for {product_url}, using CSS fallback")
                
                # Title
                title_tag = soup.find('h1')
                if not title_tag:
                    title_tag = soup.find('h1', class_=re.compile(r'product'))
                details['product_name'] = title_tag.get_text(strip=True) if title_tag else ''
                
                # Description
                desc_tag = soup.find('div', class_=re.compile(r'product.*description|rte'))
                desc_text = desc_tag.get_text(separator='\n', strip=True) if desc_tag else ''
                details['description'] = desc_text
                
                # Extract SKU from title or description
                sku = self.extract_sku_from_text(details['product_name'])
                if not sku:
                    sku = self.extract_sku_from_text(desc_text)
                details['product_code'] = sku
                
                # Price
                price_tag = soup.find('span', class_=re.compile(r'price'))
                if price_tag:
                    price_text = price_tag.get_text(strip=True)
                    details['price'] = price_text if price_text else "Request For Price"
                else:
                    details['price'] = "Request For Price"
                
                # Images
                images = []
                img_tags = soup.find_all('img', src=re.compile(r'cdn\.shopify\.com'))
                for img in img_tags[:10]:  # Limit to first 10 images
                    img_url = img.get('src', '')
                    if img_url:
                        if img_url.startswith('//'):
                            img_url = f"https:{img_url}"
                        # Get high-res version (remove size parameters)
                        img_url = re.sub(r'_\d+x\d+\.', '.', img_url)
                        if img_url not in images:
                            images.append(img_url)
                
                details['images'] = images
                details['primary_image'] = images[0] if images else ''
            
            return details
            
        except Exception as e:
            logger.error(f"Error fetching details from {product_url}: {e}")
            return None
    
    def scrape_all_pages(self):
        """Scrape all product listings from all pages"""
        logger.info(f"\n📦 SCRAPING SURANA JEWELLERS COLLECTION")
        logger.info(f"   Total Pages: {self.TOTAL_PAGES}")
        logger.info(f"   Expected Products: ~{self.TOTAL_PAGES * self.PRODUCTS_PER_PAGE}")
        
        all_products = []
        
        for page in range(1, self.TOTAL_PAGES + 1):
            # Build paginated URL
            if page == 1:
                url = self.COLLECTION_URL
            else:
                url = f"{self.COLLECTION_URL}?page={page}"
            
            logger.info(f"\n   📄 Fetching page {page}/{self.TOTAL_PAGES}...")
            
            self.setup_selenium()
            self.driver.get(url)
            
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                time.sleep(2)
            except Exception as e:
                logger.warning(f"   ⚠️  Page {page} failed to load: {e}")
                continue
            
            # Extract products from this page
            html = self.driver.page_source
            products = self.extract_products_from_listing(html)
            
            if not products:
                logger.warning(f"   ⚠️  No products found on page {page}")
            else:
                logger.info(f"   Found {len(products)} products on page {page}")
                all_products.extend(products)
        
        logger.info(f"\n   📊 Total products collected: {len(all_products)}")
        return all_products
    
    def run(self, fetch_details=True):
        """Main execution method"""
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        logger.info("="*70)
        logger.info("SURANA JEWELLERS OF JAIPUR SCRAPER")
        logger.info("="*70)
        
        # Step 1: Scrape all product listings
        all_products = self.scrape_all_pages()
        
        # Save raw listing data
        listing_file = self.base_output_dir / "product_listings.json"
        with open(listing_file, 'w', encoding='utf-8') as f:
            json.dump(all_products, f, indent=2, ensure_ascii=False)
        logger.info(f"\n💾 Saved product listings to {listing_file}")
        
        # Step 2: Fetch details for each product
        if fetch_details and all_products:
            logger.info(f"\n🔍 Fetching details for {len(all_products)} products...")
            detailed_products = []
            
            for idx, product in enumerate(all_products, 1):
                logger.info(f"   [{idx}/{len(all_products)}] {product['product_name'][:60]}...")
                details = self.fetch_product_details(product['product_url'])
                
                if details:
                    # Merge listing data with details
                    product.update(details)
                    product['scraped_at'] = datetime.now().isoformat()
                    detailed_products.append(product)
                    
                    # Save progress every 50 products
                    if idx % 50 == 0:
                        progress_file = self.base_output_dir / f"progress_{idx}.json"
                        with open(progress_file, 'w', encoding='utf-8') as f:
                            json.dump(detailed_products, f, indent=2, ensure_ascii=False)
                        logger.info(f"      💾 Progress saved to {progress_file}")
                    
                    time.sleep(0.5)  # Rate limiting
                else:
                    # Keep the product with listing data only
                    product['scraped_at'] = datetime.now().isoformat()
                    product['fetch_error'] = True
                    detailed_products.append(product)
            
            # Save final complete data
            complete_file = self.base_output_dir / "surana_complete.json"
            with open(complete_file, 'w', encoding='utf-8') as f:
                json.dump(detailed_products, f, indent=2, ensure_ascii=False)
            
            logger.info(f"\n💾 Saved complete data to {complete_file}")
            
            # Generate summary
            successful = len([p for p in detailed_products if not p.get('fetch_error')])
            with_sku = len([p for p in detailed_products if p.get('product_code')])
            
            logger.info(f"\n📊 SUMMARY:")
            logger.info(f"   Total products: {len(detailed_products)}")
            logger.info(f"   Successfully fetched: {successful}")
            logger.info(f"   With SKU: {with_sku}")
        
        # Cleanup
        if self.driver:
            self.driver.quit()
        
        logger.info("\n🎉 All tasks completed!")

if __name__ == "__main__":
    scraper = SuranaJewellersScraper(base_output_dir="surana-data")
    scraper.run(fetch_details=True)
