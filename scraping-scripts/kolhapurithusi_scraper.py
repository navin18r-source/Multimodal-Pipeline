import requests
import json
import time
import logging
import re
import os
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup
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
        logging.FileHandler('kolhapurithusi_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class KolhapurithusiScraper:
    """
    Scraper for Kolhapurithusi.in (Maharastra).
    Extracts product data from 59 collections.
    """
    
    SITE_BASE = "https://kolhapurithusi.in"
    
    # Configuration - 59 categories
    CATEGORIES = {
        "1 Gram Bangles Design": "https://kolhapurithusi.in/collections/1-gram-bangles-design",
        "1 Gram Mangalsutra Design": "https://kolhapurithusi.in/collections/1-gram-mangalsutra-design",
        "1 One Gram Geru Finish": "https://kolhapurithusi.in/collections/1-one-gram-geru-finish",
        "Bajuband": "https://kolhapurithusi.in/collections/bajuband",
        "Bakuli Collection": "https://kolhapurithusi.in/collections/bakuli-collection",
        "Bangles": "https://kolhapurithusi.in/collections/kolhapuri-bangles",
        "Black+Golden Thushi": "https://kolhapurithusi.in/collections/black-golden-thushi",
        "Broad 10 Line Manchali": "https://kolhapurithusi.in/collections/broad-10-line-manchali",
        "Broad Long Haar": "https://kolhapurithusi.in/collections/broad-long-haar",
        "Broad Thushi": "https://kolhapurithusi.in/collections/broad-thushi",
        "Bugadi": "https://kolhapurithusi.in/collections/bugadi",
        "Chain Mangalstura": "https://kolhapurithusi.in/collections/chain-mangalstura",
        "Choker Haar": "https://kolhapurithusi.in/collections/choker-haar",
        "Choker Thushi": "https://kolhapurithusi.in/collections/choker-thushi",
        "Combos": "https://kolhapurithusi.in/collections/combos",
        "Complete Mangalsutra Collection": "https://kolhapurithusi.in/collections/complete-mangalsutra-collection",
        "Cyrstal Manchali": "https://kolhapurithusi.in/collections/cyrstal-manchali",
        "Earcuffs & Kaan-Vel": "https://kolhapurithusi.in/collections/earcuffs-kaan-vel",
        "Earrings": "https://kolhapurithusi.in/collections/kolhapuri-earrings",
        "Forming Bangles Collection": "https://kolhapurithusi.in/collections/forming-bangles",
        "Forming Collection": "https://kolhapurithusi.in/collections/forming-collection",
        "Forming Mangalsutra Collection": "https://kolhapurithusi.in/collections/forming-mangalsutra-collection",
        "Forming Rani Haar Collection": "https://kolhapurithusi.in/collections/forming-rani-haar-collection-online",
        "Forming Short Mangalsutra": "https://kolhapurithusi.in/collections/forming-short-mangalsutra",
        "Gadi Thushi": "https://kolhapurithusi.in/collections/kolhapuri-gadi-thushi",
        "Gatta Thushi": "https://kolhapurithusi.in/collections/gatta-thushi",
        "Geru Finish Choker": "https://kolhapurithusi.in/collections/geru-finish-choker",
        "Geru Finish Haar": "https://kolhapurithusi.in/collections/geru-finish-haar",
        "Geru Finish Mangalsutra": "https://kolhapurithusi.in/collections/geru-finish-mangalsutra",
        "Geru Finish bangles": "https://kolhapurithusi.in/collections/geru-finish-bangles",
        "Haar And Necklace": "https://kolhapurithusi.in/collections/kolhpuri-haar-and-necklace",
        "Javmala": "https://kolhapurithusi.in/collections/javmala",
        "Jhumkas Collections": "https://kolhapurithusi.in/collections/jhumkas-collections",
        "Kolhapuri Saaj": "https://kolhapurithusi.in/collections/kolhapuri-saaj",
        "Kolhapuri Thushi": "https://kolhapurithusi.in/collections/kolhapuri-thushi",
        "Long Manchali": "https://kolhapurithusi.in/collections/long-manchali",
        "Loose/ Jondhali Pot Thushi": "https://kolhapurithusi.in/collections/loose-jondhali-pot-thushi",
        "Manchali": "https://kolhapurithusi.in/collections/manchali",
        "Moti Jhumkis": "https://kolhapurithusi.in/collections/moti-jhumkis",
        "Moti Mala & Haar": "https://kolhapurithusi.in/collections/moti-mala-and-haar",
        "Moti Thushi": "https://kolhapurithusi.in/collections/moti-thushi",
        "Nath": "https://kolhapurithusi.in/collections/nath",
        "One Gram Jhumkis Collection": "https://kolhapurithusi.in/collections/one-gram-jhumkis-collection",
        "One Gram Necklace": "https://kolhapurithusi.in/collections/one-gram-necklace-design",
        "One Gram Short Necklace Design": "https://kolhapurithusi.in/collections/one-gram-short-necklace-design",
        "Oxidised Collection": "https://kolhapurithusi.in/collections/oxidized-jewellery",
        "Oxidized Bangles": "https://kolhapurithusi.in/collections/oxidized-bangles",
        "Oxidized Mala and Haar": "https://kolhapurithusi.in/collections/oxidized-mala-and-haar",
        "Oxidized Nath": "https://kolhapurithusi.in/collections/oxidized-nath",
        "Oxidized Necklace Set": "https://kolhapurithusi.in/collections/oxidized-necklace-set",
        "Oxidized long manchali": "https://kolhapurithusi.in/collections/oxidized-long-manchali",
        "Oxidized manchali": "https://kolhapurithusi.in/collections/oxidized-manchali",
        "Oxidized thushi": "https://kolhapurithusi.in/collections/oxidized-thushi",
        "Pendants": "https://kolhapurithusi.in/collections/pendants",
        "Premium Karwari Collection": "https://kolhapurithusi.in/collections/premium-karwari-collection",
        "Premium South Indian Necklace": "https://kolhapurithusi.in/collections/premiuim-south-indian-necklace-collection",
        "Putli Chapla Haar": "https://kolhapurithusi.in/collections/putli-chapla-haar",
        "Regal Peshwai Collection": "https://kolhapurithusi.in/collections/regal-peshwai-collection",
        "Rukmini haar": "https://kolhapurithusi.in/collections/rukmini-haar",
        "Shivai Haar": "https://kolhapurithusi.in/collections/shivai-haar",
        "Temple Jewellery": "https://kolhapurithusi.in/collections/temple-jewellery",
        "Three Line haar": "https://kolhapurithusi.in/collections/three-line-haar",
        "Traditional Mala": "https://kolhapurithusi.in/collections/traditional-mala",
        "Vajratika": "https://kolhapurithusi.in/collections/vajratika",
        "Zhalar Thushi": "https://kolhapurithusi.in/collections/zhalar-thushi"
    }
    
    def __init__(self, base_output_dir="kolhapurithusi-data"):
        self.base_output_dir = Path(base_output_dir)
        self.base_output_dir.mkdir(parents=True, exist_ok=True)
        
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
    
    def clean_price(self, price_text):
        """Clean price text to extract readable format"""
        if not price_text:
            return ""
        # Remove "Unit price/per" or other clutter
        # Regex to find pattern like Rs. 2,800 or 2,800
        # Often text is '280000Rs. 2,800.00' where 280000 is hidden
        
        # Try to find standard currency patterns
        matches = re.findall(r'(?:Rs\.?|INR|₹)\s*[\d,]+(?:\.\d{2})?', price_text, re.IGNORECASE)
        if matches:
            return matches[-1] # Usually the last one is the visible one if there are duplicates
        
        # If just numbers
        matches = re.findall(r'[\d,]+(?:\.\d{2})?', price_text)
        if matches:
            # Filter out likely long hidden numbers (e.g. 280000 vs 2,800)
            # This is heuristic.
            for m in reversed(matches):
                if len(m) < 7: # likely real price
                    return f"₹ {m}"
            return f"₹ {matches[0]}"
            
        return price_text.strip()

    def extract_products_from_listing(self, html):
        """Extract product URLs from collection listing page"""
        soup = BeautifulSoup(html, 'lxml')
        products = []
        
        # Selectors for Shopify grid items
        # Updated based on debugging: uses .grid-item (single dash), .x-card-title, .x-card-price
        product_cards = soup.select('.grid-item, .grid__item, .product-item, .card-wrapper')
        
        for card in product_cards:
            try:
                # Try multiple selectors for link
                link_tag = card.select_one('a[href*="/products/"]')
                if not link_tag:
                    continue
                
                product_url = link_tag.get('href', '')
                if not product_url.startswith('http'):
                    if product_url.startswith('/'):
                        product_url = f"{self.SITE_BASE}{product_url}"
                    else:
                        product_url = f"{self.SITE_BASE}/{product_url}"
                
                # Title
                title_tag = card.select_one('.x-card-title, h3, .card__heading, .product-title, .title')
                title = title_tag.get_text(strip=True) if title_tag else ''
                
                # Price
                price_tag = card.select_one('.x-card-price, .price, .price-item, .card-information__text')
                raw_price = price_tag.get_text(strip=True) if price_tag else ''
                price_text = self.clean_price(raw_price)
                
                products.append({
                    'product_url': product_url,
                    'product_name': title,
                    'price_text': price_text
                })
                
            except Exception as e:
                # logger.warning(f"Error parsing product card: {e}") 
                continue
        
        # Deduplicate by URL
        unique_products = {p['product_url']: p for p in products}.values()
        return list(unique_products)
    
    def fetch_product_details(self, product_url):
        """Fetch detailed product information from product page"""
        try:
            if not self.driver:
                self.setup_selenium()
                
            self.driver.get(product_url)
            
            # Wait for page load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(1) # Small delay for JS to settle
            
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'lxml')
            
            details = {}
            found_json = False
            
            # Strategy A: application/ld+json (Preferred)
            ld_json_scripts = soup.find_all('script', type='application/ld+json')
            for script in ld_json_scripts:
                if not script.string:
                    continue
                try:
                    data = json.loads(script.string)
                    # Handle if it's a list of objects
                    if isinstance(data, list):
                        for item in data:
                            if item.get('@type') in ['Product', 'ProductGroup']:
                                data = item
                                break
                    
                    if data.get('@type') in ['Product', 'ProductGroup']:
                        details['product_name'] = data.get('name', '')
                        details['description'] = data.get('description', '')
                        
                        # Images
                        imgs = data.get('image', [])
                        if isinstance(imgs, str): imgs = [imgs]
                        details['images'] = imgs
                        
                        # Price from offers
                        offers = data.get('offers', {})
                        if isinstance(offers, list): offers = offers[0] if offers else {}
                        
                        price = offers.get('price', '')
                        currency = offers.get('priceCurrency', 'INR')
                        if price:
                            details['price'] = f"{currency} {price}"
                        
                        found_json = True
                        break
                except Exception as e:
                    pass
            
            if not found_json:
                # Fallback to previous JSON methods or CSS
                pass

            # CSS Fallback / Augmentation (if JSON missing or incomplete)
            if not details.get('product_name'):
                title_tag = soup.select_one('h1.product-title, h1.product__title, h1')
                details['product_name'] = title_tag.get_text(strip=True) if title_tag else ''
            
            if not details.get('price'):
                price_tag = soup.select_one('.main-product-price.price, .price .price-item--regular, .product__price, .price')
                raw_price = price_tag.get_text(strip=True) if price_tag else ''
                details['price'] = self.clean_price(raw_price)
                
            if not details.get('description'):
                desc_tag = soup.select_one('.product-description, .rte, .product__description')
                # Clean html from description
                if desc_tag:
                    details['description_text'] = desc_tag.get_text(separator='\n', strip=True)
                    # Use the HTML as description if needed, or just text
                    details['description'] = str(desc_tag)
                else:
                    details['description_text'] = ''
            else:
                 # valid description from json, create text version
                 soup_desc = BeautifulSoup(details['description'], 'lxml')
                 details['description_text'] = soup_desc.get_text(separator='\n', strip=True)

            # Image Extraction (CSS fallback is often better for all gallery images)
            css_images = []
            img_elements = soup.select('.splide__slide img, .product__media img, .product-single__photo img, .featured-image img')
            for img in img_elements:
                src = img.get('src') or img.get('data-src') or img.get('srcset', '').split(' ')[0]
                if src:
                    if src.startswith('//'): src = f"https:{src}"
                    # High res replacement
                    if '_small' in src: src = src.replace('_small', '_1024x1024')
                    elif '_medium' in src: src = src.replace('_medium', '_1024x1024')
                    elif '_large' in src: src = src.replace('_large', '_1024x1024')
                    elif '_x' in src: # generic width pattern like _100x
                         src = re.sub(r'_\d+x', '_1024x1024', src)
                    
                    css_images.append(src)
            
            # Combine images (JSON might only have one, CSS has gallery)
            current_images = details.get('images', [])
            all_images = list(dict.fromkeys(current_images + css_images)) # Deduplicate
            details['images'] = [img for img in all_images if 'http' in img] # Basic filter
            
            # SKU/Code
            # Often hard to find if not in JSON
            
            # Post-processing
            if 'images' in details and details['images']:
                details['primary_image'] = details['images'][0]
            else:
                details['primary_image'] = ''
                
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
            url = category_url if page == 1 else f"{category_url}?page={page}"
            
            logger.info(f"   📄 Fetching page {page}...")
            
            self.setup_selenium()
            self.driver.get(url)
            
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                # Check for "no products found" text or similar 404 behavior if redirect happens
                # Shopify usually stays on same URL with empty grid or redirects to page 1 if overflow
                # We check extracted products count
            except Exception as e:
                logger.warning(f"   ⚠️  Page {page} failed to load")
                break
            
            # Extract products from this page
            html = self.driver.page_source
            products = self.extract_products_from_listing(html)
            
            if not products:
                logger.info(f"   ✅ No more products found. Finished at page {page}")
                break
                
            # Check for potential infinite loop (if redirected to page 1)
            # compare first product of this page with first product of saved list
            if all_products and products[0]['product_url'] == all_products[0]['product_url']:
                 logger.info(f"   ⚠️  Redirected to start. Finished.")
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
    
    def run(self, test_mode=False):
        """Main execution method"""
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        logger.info("="*70)
        logger.info("KOLHAPURITHUSI SCRAPER")
        logger.info("="*70)
        
        all_category_data = {}
        
        # If test mode, only do 2 categories
        categories_to_scrape = list(self.CATEGORIES.items())
        if test_mode:
            logger.info("🧪 TEST MODE: Scraping only first 2 categories")
            categories_to_scrape = categories_to_scrape[:2]
        
        for category_name, category_url in categories_to_scrape:
            # Scrape product listings
            products = self.scrape_category(category_name, category_url)
            
            # Fetch details for each product
            if products:
                logger.info(f"   🔍 Fetching details for {len(products)} products...")
                detailed_products = []
                
                # Test mode: only 3 products per category
                products_list = products[:3] if test_mode else products
                
                for idx, product in enumerate(products_list, 1):
                    logger.info(f"      [{idx}/{len(products_list)}] {product['product_name'][:40]}...")
                    details = self.fetch_product_details(product['product_url'])
                    
                    if details:
                        product.update(details)
                        product['scraped_at'] = datetime.now().isoformat()
                        detailed_products.append(product)
                        time.sleep(0.5)  # Rate limiting
                
                all_category_data[category_name] = detailed_products
            else:
                all_category_data[category_name] = []
            
            # Save category data immediately
            safe_name = re.sub(r'[^\w\-]', '_', category_name)
            category_file = self.base_output_dir / f"{safe_name}.json"
            with open(category_file, 'w', encoding='utf-8') as f:
                json.dump(all_category_data[category_name], f, indent=2, ensure_ascii=False)
            
            logger.info(f"   ✅ Saved {len(all_category_data[category_name])} products to {category_file}")
        
        # Save combined data
        combined_file = self.base_output_dir / "all_collections.json"
        with open(combined_file, 'w', encoding='utf-8') as f:
            json.dump(all_category_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"\n💾 Saved combined data to {combined_file}")
        
        # Cleanup
        if self.driver:
            self.driver.quit()
        
        logger.info("\n🎉 All tasks completed!")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--test', action='store_true', help='Run in test mode (fewer categories/products)')
    args = parser.parse_args()
    
    # Use the absolute path for output to ensure it goes to the right place
    output_dir = "/Volumes/Macintosh HD/tanishq_scraper/maharastra/kolhapurithusi/data"
    
    scraper = KolhapurithusiScraper(base_output_dir=output_dir)
    scraper.run(test_mode=args.test)
