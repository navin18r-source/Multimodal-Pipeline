import requests
import json
import time
import logging
import re
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup
from html import unescape

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mychungath_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MyChungathScraper:
    """
    High-performance scraper for MyChungath Kerala jewelry products.
    Extracts complete metadata from all 6 categories.
    """
    
    SITE_BASE = "https://mychungath.com"
    
    # All 6 categories as per requirement
    CATEGORIES = {
        'necklace': f"{SITE_BASE}/collections/necklace",
        'rings': f"{SITE_BASE}/collections/rings",
        'bracelet': f"{SITE_BASE}/collections/bracelet",
        'stud': f"{SITE_BASE}/collections/stud",
        'bangles': f"{SITE_BASE}/collections/bangles",
        'diamond': f"{SITE_BASE}/collections/diamond"
    }
    
    def __init__(self, output_dir="mychungath_data"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
    
    def get_category_product_handles(self, category_name, category_url):
        """
        Phase 1: Crawl category pages to extract all product handles.
        Returns list of product handles for a specific category.
        """
        logger.info(f"\n{'='*70}")
        logger.info(f"Extracting products from category: {category_name.upper()}")
        logger.info(f"{'='*70}")
        
        all_handles = []
        page = 1
        
        while True:
            url = f"{category_url}?page={page}"
            logger.info(f"Fetching {category_name} page {page}...")
            
            try:
                response = self.session.get(url, timeout=15)
                if response.status_code != 200:
                    logger.error(f"Failed to fetch page {page}: {response.status_code}")
                    break
                
                soup = BeautifulSoup(response.text, 'lxml')
                
                # Try multiple selectors for product links
                product_links = (
                    soup.select('a.grid-product__link') or 
                    soup.select('a.product-card__link') or
                    soup.select('a[href*="/products/"]')
                )
                
                if not product_links:
                    logger.info(f"No products found on page {page}. Category complete.")
                    break
                
                # Extract handles from URLs
                for link in product_links:
                    href = link.get('href', '')
                    if '/products/' in href:
                        handle = href.split('/products/')[-1].split('?')[0]
                        if handle and handle not in all_handles:
                            all_handles.append(handle)
                
                logger.info(f"Page {page}: Found {len(product_links)} products. Total: {len(all_handles)}")
                
                page += 1
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error fetching page {page}: {e}")
                break
        
        logger.info(f"✅ Category '{category_name}' complete: {len(all_handles)} products")
        return all_handles
    
    def fetch_product_json(self, handle):
        """
        Fetch product data from Shopify's JSON endpoint.
        URL pattern: /products/{handle}.js
        """
        url = f"{self.SITE_BASE}/products/{handle}.js"
        
        try:
            response = self.session.get(url, timeout=15)
            if response.status_code != 200:
                logger.warning(f"Failed to fetch JSON for {handle}: {response.status_code}")
                return None
            
            return response.json()
            
        except Exception as e:
            logger.error(f"Error fetching JSON for {handle}: {e}")
            return None
    
    def fetch_product_page(self, handle):
        """
        Fetch product HTML page to extract additional metadata not in JSON.
        This gets the Product Details section.
        """
        url = f"{self.SITE_BASE}/products/{handle}"
        
        try:
            response = self.session.get(url, timeout=15)
            if response.status_code != 200:
                return None
            
            return BeautifulSoup(response.text, 'lxml')
            
        except Exception as e:
            logger.error(f"Error fetching page for {handle}: {e}")
            return None
    
    def extract_product_details(self, soup):
        """
        Extract Product Details metadata from the HTML page.
        Looks for fields like:
        - Category, Item Category, Barcode, Purity, Item Style
        - Gross Weight, Stone Weight, Stone Charge, Net Weight
        """
        details = {}
        
        if not soup:
            return details
        
        try:
            # Find product details section (common patterns)
            details_section = (
                soup.find('div', class_=re.compile(r'product.*details', re.I)) or
                soup.find('div', class_=re.compile(r'product.*info', re.I)) or
                soup.find('div', class_='product-single__description')
            )
            
            if details_section:
                # Extract all text and parse key-value pairs
                text = details_section.get_text()
                
                # Common patterns for Kerala jewelry metadata
                patterns = {
                    'category': r'Category[:\s]+([^\n]+)',
                    'item_category': r'Item Category[:\s]+([^\n]+)',
                    'barcode': r'Barcode[:\s]+([^\n]+)',
                    'purity': r'Purity[:\s]+([^\n]+)',
                    'item_style': r'Item Style[:\s]+([^\n]+)',
                    'gross_weight': r'Gross Weight[:\s]+([\d.]+)',
                    'stone_weight': r'Stone Weight[:\s]+([\d.]+)',
                    'stone_charge': r'Stone Charge[:\s]+([\d.]+)',
                    'net_weight': r'Net Weight[:\s]+([\d.]+)',
                    'making_charge': r'Making Charge[:\s]+([\d.]+)',
                    'wastage': r'Wastage[:\s]+([\d.]+)'
                }
                
                for key, pattern in patterns.items():
                    match = re.search(pattern, text, re.I)
                    if match:
                        value = match.group(1).strip()
                        # Convert numeric values
                        if key in ['gross_weight', 'stone_weight', 'stone_charge', 
                                   'net_weight', 'making_charge', 'wastage']:
                            try:
                                details[key] = float(value)
                            except:
                                details[key] = value
                        else:
                            details[key] = value
            
            # Also check for table/list format
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cols = row.find_all(['td', 'th'])
                    if len(cols) >= 2:
                        key = cols[0].get_text().strip().lower().replace(' ', '_').replace(':', '')
                        value = cols[1].get_text().strip()
                        if key and value:
                            details[key] = value
            
        except Exception as e:
            logger.error(f"Error extracting product details: {e}")
        
        return details
    
    def parse_product_json(self, product_json, handle, category, product_page_soup):
        """
        Parse Shopify product JSON + HTML page into complete structured metadata.
        """
        if not product_json:
            return None
        
        try:
            # Basic Info
            product_id = product_json.get('id', '')
            title = product_json.get('title', '')
            product_url = f"{self.SITE_BASE}/products/{handle}"
            
            # Pricing (in paise, convert to rupees)
            price_paise = product_json.get('price', 0)
            price = price_paise / 100 if price_paise else 0
            
            compare_at_price_paise = product_json.get('compare_at_price', 0)
            compare_at_price = compare_at_price_paise / 100 if compare_at_price_paise else 0
            
            # Catalog Data
            vendor = product_json.get('vendor', '')
            product_type = product_json.get('type', '')
            tags = product_json.get('tags', [])
            
            # Description
            description_html = product_json.get('description', '')
            description_text = self.clean_html(description_html)
            
            # Images - extract all image URLs
            images = []
            for img in product_json.get('images', []):
                img_url = img if isinstance(img, str) else img.get('src', '')
                if img_url:
                    if img_url.startswith('//'):
                        img_url = f"https:{img_url}"
                    elif not img_url.startswith('http'):
                        img_url = f"{self.SITE_BASE}{img_url}"
                    images.append(img_url)
            
            # Variants
            variants = []
            for variant in product_json.get('variants', []):
                variant_price = variant.get('price', 0) / 100 if variant.get('price') else 0
                variants.append({
                    'id': variant.get('id', ''),
                    'title': variant.get('title', ''),
                    'sku': variant.get('sku', ''),
                    'price': variant_price,
                    'available': variant.get('available', False),
                    'weight': variant.get('weight', 0),
                    'weight_unit': variant.get('weight_unit', '')
                })
            
            # Extract Product Details from HTML page
            product_details = self.extract_product_details(product_page_soup)
            
            # Availability
            available = product_json.get('available', False)
            
            # Timestamps
            created_at = product_json.get('created_at', '')
            updated_at = product_json.get('updated_at', '')
            
            return {
                'product_id': product_id,
                'handle': handle,
                'product_name': title,
                'product_url': product_url,
                'category': category,
                'price': price,
                'compare_at_price': compare_at_price,
                'currency': 'INR',
                'vendor': vendor,
                'product_type': product_type,
                'tags': tags,
                'description_html': description_html,
                'description_text': description_text,
                'images': images,
                'image_count': len(images),
                'variants': variants,
                'variant_count': len(variants),
                'available': available,
                'created_at': created_at,
                'updated_at': updated_at,
                
                # Kerala-specific Product Details
                'product_details': {
                    'category': product_details.get('category', ''),
                    'item_category': product_details.get('item_category', ''),
                    'barcode': product_details.get('barcode', ''),
                    'purity': product_details.get('purity', ''),
                    'item_style': product_details.get('item_style', ''),
                    'gross_weight': product_details.get('gross_weight', 0),
                    'stone_weight': product_details.get('stone_weight', 0),
                    'stone_charge': product_details.get('stone_charge', 0),
                    'net_weight': product_details.get('net_weight', 0),
                    'making_charge': product_details.get('making_charge', 0),
                    'wastage': product_details.get('wastage', 0)
                },
                
                'scraped_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error parsing data for {handle}: {e}")
            return None
    
    def clean_html(self, html_text):
        """Remove HTML tags and clean text."""
        if not html_text:
            return ''
        
        soup = BeautifulSoup(html_text, 'lxml')
        text = soup.get_text(separator=' ', strip=True)
        text = unescape(text)
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def run(self):
        """Main scraping workflow"""
        logger.info("="*70)
        logger.info("MYCHUNGATH.COM - Kerala Jewelry Scraper")
        logger.info("Target: All 6 categories (Necklace, Rings, Bracelet, Stud, Bangles, Diamond)")
        logger.info("="*70)
        
        all_products = []
        category_stats = {}
        
        # Process each category
        for category_name, category_url in self.CATEGORIES.items():
            # Phase 1: Get product handles for this category
            handles = self.get_category_product_handles(category_name, category_url)
            
            if not handles:
                logger.warning(f"No products found for category: {category_name}")
                continue
            
            category_stats[category_name] = len(handles)
            
            # Phase 2: Fetch detailed metadata for each product
            logger.info(f"\n{'='*70}")
            logger.info(f"Fetching metadata for {len(handles)} products in '{category_name}'...")
            logger.info(f"{'='*70}")
            
            for idx, handle in enumerate(handles, 1):
                logger.info(f"[{category_name}] Processing {idx}/{len(handles)}: {handle}")
                
                # Fetch JSON data
                product_json = self.fetch_product_json(handle)
                
                # Fetch HTML page for additional details
                product_page = self.fetch_product_page(handle)
                
                # Parse and structure data
                product_data = self.parse_product_json(
                    product_json, 
                    handle, 
                    category_name,
                    product_page
                )
                
                if product_data:
                    all_products.append(product_data)
                
                # Save checkpoint every 50 products
                if len(all_products) % 50 == 0:
                    self.save_data(all_products, "mychungath_partial.json")
                    logger.info(f"💾 Checkpoint saved: {len(all_products)} products")
                
                time.sleep(0.5)  # Polite delay
        
        # Final save
        self.save_data(all_products, "mychungath_complete.json")
        
        # Summary
        total_images = sum(p['image_count'] for p in all_products)
        total_variants = sum(p['variant_count'] for p in all_products)
        
        logger.info("\n" + "="*70)
        logger.info("✅ SCRAPING COMPLETE!")
        logger.info("="*70)
        logger.info(f"Total products: {len(all_products)}")
        logger.info(f"Total images: {total_images}")
        logger.info(f"Total variants: {total_variants}")
        logger.info(f"Average images per product: {total_images/len(all_products):.1f}")
        logger.info("\nProducts by category:")
        for cat, count in category_stats.items():
            logger.info(f"  • {cat}: {count} products")
        logger.info(f"\nOutput: mychungath_data/mychungath_complete.json")
        logger.info("="*70)
    
    def save_data(self, data, filename):
        """Save data to JSON file"""
        filepath = self.output_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    scraper = MyChungathScraper()
    scraper.run()
