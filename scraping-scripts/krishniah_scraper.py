import requests
import json
import time
import logging
import re
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
        logging.FileHandler('krishniah_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class KrishniahChettyCompleteScraper:
    """
    Advanced scraper for Krishniah Chetty Karnataka jewelry.
    Automatically detects the best scraping method:
    1. API endpoint detection (fastest)
    2. Selenium infinite scroll (most reliable)
    """
    
    SITE_BASE = "https://krishniahchetty.co"
    CATEGORY_URL = f"{SITE_BASE}/productslisting/wedding"
    
    def __init__(self, output_dir="krishniah_data"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
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
        """Setup Selenium WebDriver with optimal settings"""
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
    
    def detect_api_endpoint(self):
        """
        Attempt to detect if there's a JSON API endpoint for pagination.
        Returns the API pattern if found, None otherwise.
        """
        logger.info("🔍 Attempting to detect API endpoint...")
        
        # Common API patterns
        api_patterns = [
            # REST API patterns
            f"{self.SITE_BASE}/api/products?category=wedding&page={{page}}&limit={{limit}}",
            f"{self.SITE_BASE}/api/products/wedding?page={{page}}&limit={{limit}}",
            f"{self.SITE_BASE}/api/v1/products?category=wedding&offset={{offset}}&limit={{limit}}",
            
            # AJAX patterns
            f"{self.SITE_BASE}/productslisting/wedding?page={{page}}",
            f"{self.SITE_BASE}/productslisting/wedding/page/{{page}}",
            f"{self.SITE_BASE}/productslisting/wedding?offset={{offset}}&limit={{limit}}",
            
            # Load more patterns
            f"{self.SITE_BASE}/loadmore?category=wedding&page={{page}}",
            f"{self.SITE_BASE}/products/ajax/load?category=wedding&offset={{offset}}",
        ]
        
        for pattern in api_patterns:
            try:
                # Test with page 2 or offset 36
                test_url = pattern.format(page=2, limit=36, offset=36)
                response = self.session.get(test_url, timeout=10)
                
                if response.status_code == 200:
                    content_type = response.headers.get('content-type', '').lower()
                    
                    # Check if it's JSON
                    if 'json' in content_type or response.text.strip().startswith('{') or response.text.strip().startswith('['):
                        try:
                            data = response.json()
                            if data and (isinstance(data, list) or isinstance(data, dict)):
                                logger.info(f"✅ Found API endpoint: {test_url}")
                                return pattern
                        except:
                            pass
                    
                    # Check if HTML contains new products
                    if 'html' in content_type:
                        products = self.extract_json_ld(response.text)
                        if products:
                            logger.info(f"✅ Found HTML pagination: {test_url}")
                            return pattern
                            
            except Exception as e:
                continue
        
        logger.info("❌ No API endpoint detected. Will use Selenium method.")
        return None
    
    def scrape_via_api(self, api_pattern):
        """
        Scrape exactly 1327 products using detected API endpoint.
        Simple approach: fetch pages until we have 1327+ products, then trim.
        """
        logger.info("="*70)
        logger.info("METHOD: API PAGINATION (Fast)")
        logger.info("="*70)
        
        all_products = []
        page = 1
        TARGET_PRODUCTS = 1327
        MAX_PAGES = 40  # 40 pages * 36 products = 1440, enough for 1327
        
        logger.info(f"🎯 Target: {TARGET_PRODUCTS} products from wedding collection")
        logger.info(f"📄 Fetching up to {MAX_PAGES} pages (36 products each)")
        
        while page <= MAX_PAGES:
            try:
                url = api_pattern.format(page=page, limit=36, offset=(page-1)*36)
                
                # Progress tracking
                progress_pct = (len(all_products) / TARGET_PRODUCTS) * 100
                if len(all_products) < TARGET_PRODUCTS:
                    logger.info(f"📄 Page {page}/{MAX_PAGES}... {len(all_products)}/{TARGET_PRODUCTS} ({progress_pct:.1f}%)")
                else:
                    logger.info(f"📄 Page {page}/{MAX_PAGES}... {len(all_products)} products (target reached, finishing...)")
                
                response = self.session.get(url, timeout=20)
                
                if response.status_code != 200:
                    logger.warning(f"⚠️  Page {page} returned status {response.status_code}")
                    break
                
                # Extract products from HTML
                products = self.extract_json_ld(response.text)
                
                if not products:
                    logger.info(f"   ℹ️  No more products found on page {page}")
                    break
                
                logger.info(f"   ✅ Found {len(products)} products")
                all_products.extend(products)
                
                # Stop if we have enough
                if len(all_products) >= TARGET_PRODUCTS:
                    logger.info(f"🎯 Target reached! Got {len(all_products)} products")
                    break
                
                page += 1
                time.sleep(0.5)  # Polite delay
                
            except Exception as e:
                logger.error(f"❌ Error on page {page}: {e}")
                break
        
        # Trim to exactly 1327 products if we have more
        if len(all_products) > TARGET_PRODUCTS:
            logger.info(f"✂️  Trimming from {len(all_products)} to exactly {TARGET_PRODUCTS} products")
            all_products = all_products[:TARGET_PRODUCTS]
        
        logger.info(f"✅ Scraping complete: {len(all_products)} products collected")
        return all_products
    
    def scrape_via_selenium(self):
        """
        Scrape all 1327 products using Selenium infinite scroll.
        Optimized for Krishniah Chetty's dynamic loading.
        """
        logger.info("="*70)
        logger.info("METHOD: SELENIUM INFINITE SCROLL (Optimized for 1327 products)")
        logger.info("="*70)
        
        self.setup_selenium()
        
        logger.info(f"🌐 Loading page: {self.CATEGORY_URL}")
        self.driver.get(self.CATEGORY_URL)
        
        # Wait for initial content
        try:
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(5)  # Extra time for JS to initialize
            logger.info("✅ Page loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load page: {e}")
            return []
        
        logger.info("📜 Starting infinite scroll to load all 1327 products...")
        logger.info("   Target: 1327 products")
        
        products_count = 0
        scroll_pause = 1.5  # Faster initial scroll
        scroll_attempts = 0
        max_scroll_attempts = 300  # Enough for 1327 products
        no_change_count = 0
        last_count_check = 0
        
        while scroll_attempts < max_scroll_attempts:
            # Get current product count
            current_html = self.driver.page_source
            current_products = self.extract_json_ld(current_html)
            current_count = len(current_products)
            
            # Log progress every 50 products or when count changes
            if current_count > products_count:
                if current_count - last_count_check >= 50 or current_count < 100:
                    progress_pct = (current_count / 1327) * 100
                    logger.info(f"   📦 {current_count}/1327 products ({progress_pct:.1f}%) - scroll #{scroll_attempts + 1}")
                    last_count_check = current_count
                
                products_count = current_count
                no_change_count = 0
                
                # Check if we've reached the target
                if products_count >= 1327:
                    logger.info(f"🎯 Target reached! Loaded {products_count} products")
                    # Do a few more scrolls to be sure
                    for _ in range(3):
                        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        time.sleep(2)
                    break
            else:
                no_change_count += 1
            
            # Stop if no new products after 8 scrolls (more patient)
            if no_change_count >= 8:
                logger.info(f"✅ No new products after {no_change_count} scrolls. Total: {products_count}")
                break
            
            # Multiple scroll strategies for better loading
            try:
                # Strategy 1: Scroll to bottom
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(scroll_pause)
                
                # Strategy 2: Scroll to last product element
                product_elements = self.driver.find_elements(By.CSS_SELECTOR, '[itemtype*="Product"], .product-item, .product-card')
                if product_elements and len(product_elements) > 0:
                    last_product = product_elements[-1]
                    self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", last_product)
                    time.sleep(0.5)
                
                # Strategy 3: Small scroll back up to trigger lazy load
                if scroll_attempts % 3 == 0:
                    self.driver.execute_script("window.scrollBy(0, -300);")
                    time.sleep(0.3)
                    self.driver.execute_script("window.scrollBy(0, 300);")
                    time.sleep(0.5)
                
            except Exception as e:
                logger.warning(f"Scroll error: {e}")
                # Fallback to simple scroll
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(scroll_pause)
            
            scroll_attempts += 1
            
            # Adaptive timing based on progress
            if products_count > 1000:
                scroll_pause = 2.5  # Slower near the end
            elif products_count > 500:
                scroll_pause = 2.0
            elif products_count > 200:
                scroll_pause = 1.8
        
        # Final extraction
        logger.info("🔄 Extracting final product list...")
        final_html = self.driver.page_source
        all_products = self.extract_json_ld(final_html)
        
        if len(all_products) >= 1327:
            logger.info(f"✅ SUCCESS! Scraped all {len(all_products)} products")
        elif len(all_products) >= 1300:
            logger.info(f"✅ Nearly complete! Scraped {len(all_products)}/1327 products ({(len(all_products)/1327)*100:.1f}%)")
        else:
            logger.warning(f"⚠️  Partial scrape: {len(all_products)}/1327 products ({(len(all_products)/1327)*100:.1f}%)")
        
        return all_products
    
    def extract_json_ld(self, html):
        """
        Extract JSON-LD structured data from HTML.
        Returns list of product data dictionaries.
        """
        soup = BeautifulSoup(html, 'lxml')
        json_ld_scripts = soup.find_all('script', type='application/ld+json')
        
        all_products = []
        
        for script in json_ld_scripts:
            try:
                data = json.loads(script.string)
                
                # ItemList contains multiple products
                if data.get('@type') == 'ItemList':
                    items = data.get('itemListElement', [])
                    all_products.extend(items)
                
                # Single Product
                elif data.get('@type') == 'Product':
                    all_products.append(data)
                    
            except (json.JSONDecodeError, AttributeError) as e:
                continue
        
        return all_products
    
    def fetch_product_details(self, product_url):
        """
        Fetch detailed product page for additional metadata.
        """
        try:
            response = self.session.get(product_url, timeout=20)
            if response.status_code != 200:
                return None
            
            soup = BeautifulSoup(response.text, 'lxml')
            details = {}
            
            # Extract product specifications
            spec_sections = [
                soup.find('div', class_=re.compile(r'product.*details|specifications', re.I)),
                soup.find('div', class_='product-info'),
                soup.find('table', class_=re.compile(r'details', re.I)),
                soup.find('div', id=re.compile(r'specifications|details', re.I))
            ]
            
            for section in spec_sections:
                if section:
                    text = section.get_text()
                    
                    # Extract jewelry specifications
                    details['metal'] = self.extract_pattern(text, r'Metal[:\s]+([^\n]+)', '')
                    details['purity'] = self.extract_pattern(text, r'Purity[:\s]+([^\n]+)', '')
                    details['gross_weight'] = self.extract_pattern(text, r'Gross Weight[:\s]+([\d.]+)', '')
                    details['net_weight'] = self.extract_pattern(text, r'Net Weight[:\s]+([\d.]+)', '')
                    details['stone_weight'] = self.extract_pattern(text, r'Stone Weight[:\s]+([\d.]+)', '')
                    details['making_charge'] = self.extract_pattern(text, r'Making Charge[:\s]+([\d.]+)', '')
                    
                    if any(details.values()):
                        break
            
            # Full description
            desc_tag = soup.find('div', class_=re.compile(r'description', re.I))
            if desc_tag:
                details['full_description'] = self.clean_html(str(desc_tag))
            
            # All images
            images = []
            img_tags = soup.find_all('img', src=re.compile(r'files\.krishniahchetty', re.I))
            for img in img_tags:
                img_src = img.get('src', '')
                if 'files.krishniahchetty' in img_src:
                    if img_src.startswith('//'):
                        img_src = f"https:{img_src}"
                    if img_src not in images:
                        images.append(img_src)
            
            details['all_images'] = images
            
            return details
            
        except Exception as e:
            logger.error(f"Error fetching details from {product_url}: {e}")
            return None
    
    def extract_pattern(self, text, pattern, default=''):
        """Extract first match of regex pattern from text."""
        match = re.search(pattern, text, re.I)
        return match.group(1).strip() if match else default
    
    def clean_html(self, html_text):
        """Remove HTML tags and clean text."""
        if not html_text:
            return ''
        soup = BeautifulSoup(html_text, 'lxml')
        text = soup.get_text(separator=' ', strip=True)
        text = unescape(text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text
    
    def parse_product(self, product_data, additional_details=None):
        """
        Parse product data from JSON-LD and merge with additional details.
        """
        try:
            product_url = product_data.get('url', '')
            product_name = product_data.get('name', '')
            description = product_data.get('description', '')
            sku = product_data.get('sku', '')
            image = product_data.get('image', '')
            
            # Extract price and availability
            price = 0
            available = False
            offers = product_data.get('offers', {})
            
            if isinstance(offers, dict):
                price = offers.get('price', 0)
                availability = offers.get('availability', '')
                available = 'InStock' in availability
            
            product_id = sku.split('-')[0] if '-' in sku else sku
            
            structured_data = {
                'product_id': product_id,
                'sku': sku,
                'product_name': product_name,
                'product_url': product_url,
                'category': 'wedding',
                'description': description,
                'price': float(price) if price else 0,
                'currency': 'INR',
                'available': available,
                'images': [image] if image else [],
                'primary_image': image,
                'scraped_at': datetime.now().isoformat()
            }
            
            # Merge additional details
            if additional_details:
                structured_data['full_description'] = additional_details.get('full_description', description)
                structured_data['images'] = additional_details.get('all_images', [image])
                
                structured_data['technical_specs'] = {
                    'metal': additional_details.get('metal', ''),
                    'purity': additional_details.get('purity', ''),
                    'gross_weight': additional_details.get('gross_weight', ''),
                    'net_weight': additional_details.get('net_weight', ''),
                    'stone_weight': additional_details.get('stone_weight', ''),
                    'making_charge': additional_details.get('making_charge', '')
                }
            
            return structured_data
            
        except Exception as e:
            logger.error(f"Error parsing product: {e}")
            return None
    
    def run(self, fetch_detailed_pages=True, retry_if_incomplete=True):
        """
        Main scraping workflow with intelligent method selection.
        Optimized to ensure all 1327 products are scraped.
        
        Args:
            fetch_detailed_pages: If True, visits each product page for additional details.
            retry_if_incomplete: If True, retries scraping if less than 1300 products found.
        """
        # Disable SSL warnings
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        logger.info("="*70)
        logger.info("KRISHNIAH CHETTY & SONS - Complete Jewelry Scraper")
        logger.info("Target: ALL 1327 products from wedding collection")
        logger.info("="*70)
        
        # Step 1: Try to detect API endpoint
        api_pattern = self.detect_api_endpoint()
        
        # Step 2: Scrape using best available method
        attempt = 1
        max_attempts = 2
        products_raw = []
        
        while attempt <= max_attempts:
            if attempt > 1:
                logger.info(f"\n{'='*70}")
                logger.info(f"🔄 RETRY ATTEMPT {attempt}/{max_attempts}")
                logger.info(f"{'='*70}\n")
                time.sleep(5)  # Wait before retry
            
            if api_pattern:
                products_raw = self.scrape_via_api(api_pattern)
            else:
                products_raw = self.scrape_via_selenium()
            
            # Check if we got all products
            if len(products_raw) >= 1300:  # Allow small margin
                logger.info(f"✅ SUCCESS! Got {len(products_raw)}/1327 products")
                break
            elif len(products_raw) > 0 and not retry_if_incomplete:
                logger.warning(f"⚠️  Partial scrape: {len(products_raw)}/1327 products (retry disabled)")
                break
            elif len(products_raw) > 0:
                logger.warning(f"⚠️  Incomplete: {len(products_raw)}/1327 products. Will retry...")
                attempt += 1
            else:
                logger.error(f"❌ No products found on attempt {attempt}")
                attempt += 1
        
        if not products_raw:
            logger.error("❌ No products found after all attempts. Exiting.")
            if self.driver:
                self.driver.quit()
            return
        
        # Report final count
        completion_rate = (len(products_raw) / 1327) * 100
        logger.info(f"\n{'='*70}")
        logger.info(f"📊 Scraping Result: {len(products_raw)}/1327 products ({completion_rate:.1f}%)")
        logger.info(f"{'='*70}\n")
        
        # Step 3: Process and enrich product data
        logger.info("🔄 Processing product data...")
        if fetch_detailed_pages:
            logger.info("   Mode: DETAILED (fetching individual pages for specs)")
            estimated_time = len(products_raw) * 0.5 / 60  # 0.5 sec per product
            logger.info(f"   Estimated time: {estimated_time:.0f} minutes")
        else:
            logger.info("   Mode: FAST (using JSON-LD data only)")
        
        all_products = []
        failed_products = []
        
        for idx, product_data in enumerate(products_raw, 1):
            product_name = product_data.get('name', 'Unknown')
            product_url = product_data.get('url', '')
            
            if idx % 100 == 0:
                progress_pct = (idx / len(products_raw)) * 100
                logger.info(f"   [{idx}/{len(products_raw)}] ({progress_pct:.1f}%) Processing...")
            elif idx <= 5 or idx == len(products_raw):
                logger.info(f"   [{idx}/{len(products_raw)}] {product_name[:50]}...")
            
            # Fetch additional details if requested
            additional_details = None
            if fetch_detailed_pages and product_url:
                additional_details = self.fetch_product_details(product_url)
                if not additional_details and idx <= 10:
                    logger.warning(f"      ⚠️  Could not fetch details for: {product_name}")
                time.sleep(0.3)  # Polite delay
            
            # Parse product
            structured_product = self.parse_product(product_data, additional_details)
            
            if structured_product:
                all_products.append(structured_product)
            else:
                failed_products.append(product_name)
            
            # Checkpoint save every 100 products
            if len(all_products) % 100 == 0:
                self.save_data(all_products, "krishniah_checkpoint.json")
                logger.info(f"      💾 Checkpoint: {len(all_products)} products saved")
        
        # Final save
        self.save_data(all_products, "krishniah_complete_1327.json")
        
        # Generate detailed summary
        total_images = sum(len(p.get('images', [])) for p in all_products)
        products_with_price = sum(1 for p in all_products if p.get('price', 0) > 0)
        products_available = sum(1 for p in all_products if p.get('available'))
        products_with_specs = sum(1 for p in all_products if p.get('technical_specs', {}).get('metal'))
        
        logger.info("\n" + "="*70)
        logger.info("🎉 SCRAPING COMPLETE!")
        logger.info("="*70)
        logger.info(f"Target products:           1327")
        logger.info(f"Products scraped:          {len(all_products)}")
        logger.info(f"Success rate:              {(len(all_products)/1327)*100:.1f}%")
        logger.info(f"Products with pricing:     {products_with_price} ({(products_with_price/len(all_products))*100:.1f}%)")
        logger.info(f"Products available:        {products_available} ({(products_available/len(all_products))*100:.1f}%)")
        if fetch_detailed_pages:
            logger.info(f"Products with specs:       {products_with_specs} ({(products_with_specs/len(all_products))*100:.1f}%)")
        logger.info(f"Total images collected:    {total_images}")
        logger.info(f"Avg images per product:    {total_images/len(all_products):.1f}")
        
        if failed_products:
            logger.info(f"\n⚠️  Failed to process:      {len(failed_products)} products")
            if len(failed_products) <= 10:
                for name in failed_products:
                    logger.info(f"   - {name}")
        
        logger.info(f"\n📁 Output file: {self.output_dir}/krishniah_complete_1327.json")
        logger.info("="*70)
        
        # Final verification
        if len(all_products) >= 1327:
            logger.info("\n🎯 VERIFICATION: ✅ ALL 1327 PRODUCTS SUCCESSFULLY SCRAPED!")
        elif len(all_products) >= 1300:
            logger.info(f"\n🎯 VERIFICATION: ⚠️  Nearly complete ({len(all_products)}/1327 products)")
        else:
            logger.warning(f"\n🎯 VERIFICATION: ❌ Incomplete ({len(all_products)}/1327 products)")
            logger.warning("   Consider running again or checking the website manually")
        
        # Cleanup
        if self.driver:
            self.driver.quit()
            logger.info("\n✅ Browser closed. Scraping session ended.")
    
    def save_data(self, data, filename):
        """Save data to JSON file with proper formatting"""
        filepath = self.output_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    scraper = KrishniahChettyCompleteScraper()
    
    # Run with full details and retry logic (RECOMMENDED)
    # This will ensure all 1327 products are scraped
    scraper.run(fetch_detailed_pages=True, retry_if_incomplete=True)
    
    # For faster scraping without individual page visits:
    # scraper.run(fetch_detailed_pages=False, retry_if_incomplete=True)
    
    # For quick test (no retry, no details):
    # scraper.run(fetch_detailed_pages=False, retry_if_incomplete=False)