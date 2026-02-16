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
        logging.FileHandler('krishana_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class KrishnaJewellersScraper:
    """
    Advanced scraper for Krishna Jewellers (Andhra).
    Based on the architecture of KrishniahChettyCompleteScraper.
    Supports fetching multiple categories and organizing output into specific folders.
    """
    
    SITE_BASE = "https://krishnajewellers.com"
    
    # Configuration
    CONFIG = {
        "polki": [
            "https://krishnajewellers.com/collections/polki-bangles",
            "https://krishnajewellers.com/collections/polki-long-necklaces",
            "https://krishnajewellers.com/collections/gold-polki-short-necklaces",
            "https://krishnajewellers.com/collections/polki-bracelets",
            "https://krishnajewellers.com/collections/polki-earrings",
            "https://krishnajewellers.com/collections/polki-pendants",
            "https://krishnajewellers.com/collections/polki-vaddanam",
            "https://krishnajewellers.com/collections/polki-mangtika",
            "https://krishnajewellers.com/collections/polki-rings"
        ],
        "kundan": [
            "https://krishnajewellers.com/collections/kundan-short-necklace",
            "https://krishnajewellers.com/collections/kundan-long-necklace",
            "https://krishnajewellers.com/collections/kundan-bangles",
            "https://krishnajewellers.com/collections/kundan-earrings",
            "https://krishnajewellers.com/collections/kundan-vaddanam",
            "https://krishnajewellers.com/collections/kundan-pendants"
        ]
    }
    
    def __init__(self, base_output_dir="krishana-j"):
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
    
    def extract_from_dom(self, html):
        """
        Extract product data directly from DOM elements.
        Only extracts products from the actual collection grid, excluding cross-sell items.
        """
        soup = BeautifulSoup(html, 'lxml')
        products = []
        
        # IMPORTANT: Only select product cards that are within grid__item containers
        # This excludes the cross-sell products in drawer-crossell__item containers
        # Note: grid__item elements are <li> tags, not <div> tags
        grid_items = soup.find_all('li', class_=re.compile(r'grid__item'))
        
        for grid_item in grid_items:
            # Find the product card within this grid item
            card = grid_item.find('div', class_=re.compile(r'product-card'))
            if not card:
                continue
                
            try:
                # Name & URL
                name_tag = card.find('a', class_='product-card__name')
                if not name_tag:
                    continue
                    
                product_name = name_tag.get_text(strip=True)
                product_url = name_tag.get('href', '')
                if product_url.startswith('/'):
                    product_url = f"{self.SITE_BASE}{product_url}"
                
                # Price
                # Try data-price attribute first (usually in paise/cents)
                price_cents = card.get('data-price')
                if price_cents:
                    price = float(price_cents) / 100
                else:
                    # Fallback to text
                    price_tag = card.find(class_='product-card__price')
                    price_text = price_tag.get_text(strip=True) if price_tag else '0'
                    price = float(re.sub(r'[^\d.]', '', price_text)) if re.search(r'\d', price_text) else 0
                
                # Image
                image = ''
                img_tag = card.find('img')
                if img_tag:
                    # Try src or data-src
                    src = img_tag.get('src') or img_tag.get('data-src')
                    if src:
                        if src.startswith('//'):
                            src = f"https:{src}"
                        # Clean URL parameters to get high res
                        if '?' in src:
                            src = src.split('?')[0] + '?v=' + src.split('v=')[1].split('&')[0] if 'v=' in src else src.split('?')[0]
                        image = src

                # SKU - approximate from URL handle if not visible
                sku = product_url.split('/')[-1]

                products.append({
                    'product_id': sku,
                    'sku': sku,
                    'product_name': product_name,
                    'product_url': product_url,
                    'description': '', # Will be fetched in detail page
                    'price': price,
                    'currency': 'INR',
                    'available': True, # Assume in stock if listed, or check class
                    'images': [image] if image else [],
                    'primary_image': image,
                    'scraped_at': datetime.now().isoformat()
                })
                
            except Exception as e:
                logger.warning(f"Error parsing product card: {e}")
                continue
                
        return products

    def scrape_category_url(self, url, category_name):
        """
        Scrape all products from a single category URL using Selenium/Infinite Scroll.
        """
        self.setup_selenium()
        logger.info(f"🌐 Loading page: {url}")
        self.driver.get(url)
        
        try:
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(5) 
            logger.info(f"✅ Page loaded: {url}")
        except Exception as e:
            logger.error(f"Failed to load page {url}: {e}")
            return []

        # Scroll logic
        logger.info("📜 Starting scroll to load all products...")
        
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        products_seen = set()
        all_products_raw = []
        
        scroll_attempts = 0
        max_scroll_attempts = 30 # Adjust based on expected catalog size
        
        while scroll_attempts < max_scroll_attempts:
            # Extract current products from DOM
            current_html = self.driver.page_source
            current_batch = self.extract_from_dom(current_html)
            
            new_count = 0
            for p in current_batch:
                p_url = p.get('product_url')
                if p_url and p_url not in products_seen:
                    products_seen.add(p_url)
                    all_products_raw.append(p)
                    new_count += 1
            
            if new_count > 0:
                logger.info(f"   Found {new_count} new products (Total: {len(all_products_raw)})")
            
            # Scroll down
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3) # Wait for load
            
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                # Try a few more times to be sure
                time.sleep(2)
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    logger.info("✅ Reached bottom of page.")
                    break
            last_height = new_height
            scroll_attempts += 1

        return all_products_raw
    
    def fetch_product_details(self, product_url):
        """
        Fetch detailed product page for additional metadata.
        """
        try:
            # Ensure URL is absolute
            if product_url.startswith('//'):
                product_url = f"https:{product_url}"
            elif product_url.startswith('/'):
                product_url = f"{self.SITE_BASE}{product_url}"
                
            response = self.session.get(product_url, timeout=20)
            if response.status_code != 200:
                return None
            
            soup = BeautifulSoup(response.text, 'lxml')
            details = {}
            
            # Extract common generic specs if available
            # Note: Krishna Jewellers implementation might differ, so we look for generic patterns
            text_content = soup.get_text()
            
            details['metal'] = self.extract_pattern(text_content, r'(?:Metal|Material)[:\s]+([^\n]+)', '')
            details['purity'] = self.extract_pattern(text_content, r'(?:Purity|Gold Purity)[:\s]+([^\n]+)', '')
            details['gross_weight'] = self.extract_pattern(text_content, r'(?:Gross Weight)[:\s]+([\d.]+\s*\w*)', '')
            details['net_weight'] = self.extract_pattern(text_content, r'(?:Net Weight)[:\s]+([\d.]+\s*\w*)', '')
            details['stone_weight'] = self.extract_pattern(text_content, r'(?:Stone Weight)[:\s]+([\d.]+\s*\w*)', '')
            
            # Full description
            desc_tag = soup.find('div', class_=re.compile(r'description|rte|product-description', re.I))
            if desc_tag:
                details['full_description'] = self.clean_html(str(desc_tag))
            
            # All images
            images = []
            # Common patterns for product images
            for img in soup.find_all('img'):
                src = img.get('src', '')
                if 'product' in src.lower() or 'collection' in src.lower() or 'cdn.shopify.com' in src:
                    if src.startswith('//'):
                        src = f"https:{src}"
                    # Filter out small thumbnails or irrelevant icons if possible
                    if 'icon' not in src.lower() and src not in images:
                        images.append(src)
            
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
        Parse product data from JSON-LD/Scraped data and merge with additional details.
        """
        try:
            # Handle different JSON-LD structures (Shopify sometimes nests things differently)
            # Check for direct keys first (from DOM extraction)
            product_url = product_data.get('product_url') or product_data.get('url', '')
            
            if not product_url and 'item' in product_data: # Sometimes inside 'item' wrapper
                 product_url = product_data.get('item', {}).get('url', '')

            product_name = product_data.get('product_name') or product_data.get('name', '')
            
            if not product_name and 'item' in product_data:
                product_name = product_data.get('item', {}).get('name', '')

            description = product_data.get('description', '')
            sku = product_data.get('sku', '')
            image = product_data.get('primary_image') or product_data.get('image', '')
            
            # Extract price and availability
            price = product_data.get('price', 0)
            available = product_data.get('available', False)
            
            if not price:
                offers = product_data.get('offers', {})
                if isinstance(offers, dict):
                    price = offers.get('price', 0)
                    availability = offers.get('availability', '')
                    available = 'InStock' in availability
                elif isinstance(offers, list) and offers: # Sometimes offers is a list
                    price = offers[0].get('price', 0)
                    availability = offers[0].get('availability', '')
                    available = 'InStock' in availability
            
            product_id = sku # Default to SKU
            
            structured_data = {
                'product_id': product_id,
                'sku': sku,
                'product_name': product_name,
                'product_url': product_url,
                'description': description,
                'price': float(price),
                'currency': 'INR', # Assuming INR for Indian site
                'available': available,
                'images': product_data.get('images', [image] if image else []),
                'primary_image': image,
                'scraped_at': datetime.now().isoformat()
            }
            
            # Merge additional details
            if additional_details:
                if 'full_description' in additional_details:
                    structured_data['full_description'] = additional_details['full_description']
                
                if additional_details.get('all_images'):
                    structured_data['images'] = additional_details['all_images']
                
                structured_data['technical_specs'] = {
                    'metal': additional_details.get('metal', ''),
                    'purity': additional_details.get('purity', ''),
                    'gross_weight': additional_details.get('gross_weight', ''),
                    'net_weight': additional_details.get('net_weight', ''),
                    'stone_weight': additional_details.get('stone_weight', ''),
                }
            
            return structured_data
            
        except Exception as e:
            logger.error(f"Error parsing product: {e}")
            return None

    def run(self, fetch_detailed_pages=True):
        """
        Main execution method.
        Iterates over configured categories and scrapes data.
        """
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        logger.info("="*70)
        logger.info("KRISHNA JEWELLERS SCRAPER")
        logger.info("="*70)
        
        for category_type, urls in self.CONFIG.items():
            logger.info(f"\n📦 PROCESSING CATEGORY GROUP: {category_type.upper()}")
            
            category_output_dir = self.base_output_dir / category_type
            category_output_dir.mkdir(parents=True, exist_ok=True)
            
            # Combine all products for this category type (polki/kundan)
            combined_products = []
            
            for url in urls:
                logger.info(f"   🔗 Scraping URL: {url}")
                products_raw = self.scrape_category_url(url, category_type)
                logger.info(f"      Found {len(products_raw)} raw products from this URL")
                
                # Process these products
                for idx, p_raw in enumerate(products_raw):
                    # Basic Parse
                    structured = self.parse_product(p_raw)
                    if structured and structured.get('product_url'):
                        # Check duplication in combined list
                        if not any(cp['product_url'] == structured['product_url'] for cp in combined_products):
                            
                            # Fetch details if needed
                            if fetch_detailed_pages:
                                logger.info(f"      Fetching details for: {structured['product_name'][:30]}...")
                                details = self.fetch_product_details(structured['product_url'])
                                structured = self.parse_product(p_raw, details)
                                time.sleep(0.5) 
                                
                            combined_products.append(structured)

            # Save results for this category type
            filename = f"{category_type}_complete.json"
            filepath = category_output_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(combined_products, f, indent=2, ensure_ascii=False)
            
            logger.info(f"✅ Saved {len(combined_products)} products to {filepath}")

        # Cleanup
        if self.driver:
            self.driver.quit()
        logger.info("\n🎉 All tasks completed.")

if __name__ == "__main__":
    # Create scraper instance targeting the specified output folder
    scraper = KrishnaJewellersScraper(base_output_dir="krishana-j")
    
    # Run
    scraper.run(fetch_detailed_pages=True)
