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
        logging.FileHandler('amethyst_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AmethystStoreScraper:
    """
    Scraper for The Amethyst Store (Andhra Pradesh).
    Extracts product data from 9 categories: Oddiyanam, Jada, Vanki, Idols, 
    Maangtikka, Hair Maatal, Bracelets, Chain, and Rings.
    """
    
    SITE_BASE = "https://www.theamethyststore.com"
    
    # Configuration - 9 categories
    CATEGORIES = {
        "oddiyanam": "https://www.theamethyststore.com/collections/silver-oddiyanam-vandanam-hip-belts",
        "jada": "https://www.theamethyststore.com/collections/oxidised-silver-jada-hair-accessories",
        "vanki": "https://www.theamethyststore.com/collections/gold-plated-silver-vanki-arm-belt",
        "idols": "https://www.theamethyststore.com/collections/silver-idols",
        "maangtikka": "https://www.theamethyststore.com/collections/gold-plated-silver-maangtikka",
        "hair_maatal": "https://www.theamethyststore.com/collections/hair-maatal",
        "bracelets": "https://www.theamethyststore.com/collections/gold-plated-silver-bracelets",
        "chain": "https://www.theamethyststore.com/collections/gold-plated-silver-chain",
        "rings": "https://www.theamethyststore.com/collections/gold-plated-silver-rings"
    }
    
    def __init__(self, base_output_dir="amethyst-data"):
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
    
    def extract_products_from_listing(self, html):
        """Extract product URLs from collection listing page"""
        soup = BeautifulSoup(html, 'lxml')
        products = []
        
        # Find all product cards
        product_cards = soup.find_all('div', class_='grid-product__content')
        
        for card in product_cards:
            try:
                link_tag = card.find('a', class_='grid-product__link')
                if not link_tag:
                    continue
                
                product_url = link_tag.get('href', '')
                if product_url.startswith('/'):
                    product_url = f"{self.SITE_BASE}{product_url}"
                
                title_tag = card.find('div', class_='grid-product__title')
                title = title_tag.get_text(strip=True) if title_tag else ''
                
                price_tag = card.find('div', class_='grid-product__price')
                price_text = price_tag.get_text(strip=True) if price_tag else ''
                
                products.append({
                    'product_url': product_url,
                    'product_name': title,
                    'price_text': price_text
                })
                
            except Exception as e:
                logger.warning(f"Error parsing product card: {e}")
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
            
            # Extract product data from JavaScript var product = {...}
            product_json = None
            scripts = soup.find_all('script')
            for script in scripts:
                if script.string and 'var product = {' in script.string:
                    # Extract JSON from var product = {...};
                    match = re.search(r'var product = ({.*?});', script.string, re.DOTALL)
                    if match:
                        try:
                            product_json = json.loads(match.group(1))
                            break
                        except json.JSONDecodeError:
                            continue
            
            if product_json:
                # Extract SKU from variants
                if 'variants' in product_json and product_json['variants']:
                    details['product_code'] = product_json['variants'][0].get('sku', '')
                else:
                    details['product_code'] = ''
                
                # Extract title
                details['product_name'] = product_json.get('title', '')
                
                # Extract price (convert from cents)
                price_cents = product_json.get('price', 0)
                details['price'] = f"₹ {price_cents / 100:,.0f}"
                
                # Extract description (HTML encoded)
                desc_html = product_json.get('description', '')
                if desc_html:
                    # Parse HTML description
                    desc_soup = BeautifulSoup(desc_html, 'lxml')
                    desc_text = desc_soup.get_text(separator='\n', strip=True)
                    details['description'] = desc_text
                    
                    # Extract dimensions
                    height_match = re.search(r'Height[:\-\s]+(\d+[\s]*mm)', desc_text, re.I)
                    width_match = re.search(r'Width[:\-\s]+(\d+[\s]*mm)', desc_text, re.I)
                    
                    details['height'] = height_match.group(1).strip() if height_match else ''
                    details['width'] = width_match.group(1).strip() if width_match else ''
                else:
                    details['description'] = ''
                    details['height'] = ''
                    details['width'] = ''
                
                # Extract images from JSON
                images = []
                if 'images' in product_json:
                    for img_path in product_json['images']:
                        if img_path.startswith('//'):
                            img_url = f"https:{img_path}"
                        elif img_path.startswith('/'):
                            img_url = f"{self.SITE_BASE}{img_path}"
                        else:
                            img_url = img_path
                        
                        # Get high-res version
                        if '?' in img_url:
                            img_url = img_url.split('?')[0] + '?v=' + img_url.split('v=')[1] if 'v=' in img_url else img_url
                        
                        images.append(img_url)
                
                details['images'] = images
                details['primary_image'] = images[0] if images else ''
            else:
                # Fallback to CSS selectors if JSON not found
                logger.warning(f"Could not find product JSON for {product_url}, using CSS fallback")
                
                sku_tag = soup.find('span', class_='product-single__sku')
                details['product_code'] = sku_tag.get_text(strip=True) if sku_tag else ''
                
                title_tag = soup.find('h1', class_='product-single__title')
                details['product_name'] = title_tag.get_text(strip=True) if title_tag else ''
                
                price_tag = soup.find('span', class_='product__price')
                details['price'] = price_tag.get_text(strip=True) if price_tag else ''
                
                desc_tag = soup.find('div', class_='product-single__description')
                desc_text = desc_tag.get_text(separator='\n', strip=True) if desc_tag else ''
                details['description'] = desc_text
                
                height_match = re.search(r'Height[:\-\s]+(\d+[\s]*mm)', desc_text, re.I)
                width_match = re.search(r'Width[:\-\s]+(\d+[\s]*mm)', desc_text, re.I)
                details['height'] = height_match.group(1).strip() if height_match else ''
                details['width'] = width_match.group(1).strip() if width_match else ''
                
                images = []
                thumb_links = soup.find_all('a', class_='product__thumb')
                for thumb in thumb_links:
                    img_url = thumb.get('href', '')
                    if img_url:
                        if img_url.startswith('//'):
                            img_url = f"https:{img_url}"
                        images.append(img_url)
                
                details['images'] = images
                details['primary_image'] = images[0] if images else ''
            
            return details
            
        except Exception as e:
            logger.error(f"Error fetching details from {product_url}: {e}")
            return None
    
    def scrape_category(self, category_name, category_url):
        """Scrape all products from a category with pagination"""
        logger.info(f"\n📦 SCRAPING CATEGORY: {category_name.upper()}")
        logger.info(f"   URL: {category_url}")
        
        all_products = []
        page = 1
        
        while True:
            # Build paginated URL
            if page == 1:
                url = category_url
            else:
                url = f"{category_url}?page={page}"
            
            logger.info(f"   📄 Fetching page {page}...")
            
            self.setup_selenium()
            self.driver.get(url)
            
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                time.sleep(3)
            except Exception as e:
                logger.warning(f"   ⚠️  Page {page} failed to load or doesn't exist")
                break
            
            # Extract products from this page
            html = self.driver.page_source
            products = self.extract_products_from_listing(html)
            
            if not products:
                logger.info(f"   ✅ No more products found. Finished at page {page-1}")
                break
            
            logger.info(f"   Found {len(products)} products on page {page}")
            all_products.extend(products)
            page += 1
            
            # Safety limit
            if page > 50:
                logger.warning("   ⚠️  Reached page limit of 50")
                break
        
        logger.info(f"   📊 Total products found: {len(all_products)}")
        return all_products
    
    def run(self, fetch_details=True):
        """Main execution method"""
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        logger.info("="*70)
        logger.info("THE AMETHYST STORE SCRAPER")
        logger.info("="*70)
        
        all_category_data = {}
        
        for category_name, category_url in self.CATEGORIES.items():
            # Scrape product listings
            products = self.scrape_category(category_name, category_url)
            
            # Fetch details for each product
            if fetch_details and products:
                logger.info(f"   🔍 Fetching details for {len(products)} products...")
                detailed_products = []
                
                for idx, product in enumerate(products, 1):
                    logger.info(f"      [{idx}/{len(products)}] {product['product_name'][:40]}...")
                    details = self.fetch_product_details(product['product_url'])
                    
                    if details:
                        # Merge listing data with details
                        product.update(details)
                        product['scraped_at'] = datetime.now().isoformat()
                        detailed_products.append(product)
                        time.sleep(0.5)  # Rate limiting
                
                all_category_data[category_name] = detailed_products
            else:
                all_category_data[category_name] = products
            
            # Save category data
            category_file = self.base_output_dir / f"{category_name}.json"
            with open(category_file, 'w', encoding='utf-8') as f:
                json.dump(all_category_data[category_name], f, indent=2, ensure_ascii=False)
            
            logger.info(f"   ✅ Saved {len(all_category_data[category_name])} products to {category_file}")
        
        # Save combined data
        combined_file = self.base_output_dir / "all_categories.json"
        with open(combined_file, 'w', encoding='utf-8') as f:
            json.dump(all_category_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"\n💾 Saved combined data to {combined_file}")
        
        # Cleanup
        if self.driver:
            self.driver.quit()
        
        logger.info("\n🎉 All tasks completed!")
        
        # Print summary
        logger.info("\n📊 SUMMARY:")
        total = 0
        for cat, prods in all_category_data.items():
            count = len(prods)
            total += count
            logger.info(f"   {cat}: {count} products")
        logger.info(f"   TOTAL: {total} products")

if __name__ == "__main__":
    scraper = AmethystStoreScraper(base_output_dir="amethyst-data")
    scraper.run(fetch_details=True)
