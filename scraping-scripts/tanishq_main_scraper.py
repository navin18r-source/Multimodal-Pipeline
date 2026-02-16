"""
Tanishq Scraper - Extracts from H4 tags (actual structure)
"""

import requests
from bs4 import BeautifulSoup
import json
import time
import random
from typing import Dict, List, Set, Optional
from urllib.parse import urljoin
import re
from datetime import datetime
import logging
from pathlib import Path
import pandas as pd
from collections import defaultdict
import concurrent.futures
import threading

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraping.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class TanishqScraper:
    """Scraper that extracts data from H4 tags"""
    
    BASE_URL = "https://www.tanishq.co.in"
    
    PRICE_RANGES = {
        "Under 25K": "pmin=0&pmax=25000",
        "25K - 50K": "pmin=25000&pmax=50000",
        "50K - 100K": "pmin=50000&pmax=100000",
        "100K and Above": "pmin=100000&pmax=3000000"
    }
    
    CATEGORIES = {
        "jewellery": {
            "all": "/shop/jewellery?lang=en_IN"
        }
    }
    
    def __init__(self, output_dir: str = "scarp-data-1"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        self.session = requests.Session()
        # Optimize connection pool for high concurrency
        adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)
        
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
        })
        
        self.seen_product_ids: Set[str] = set()
        self.stats = defaultdict(int)
        self.lock = threading.Lock()
        self.seen_product_ids: Set[str] = set()
        self.stats = defaultdict(int)
        self.lock = threading.Lock()
        self.max_workers = 10 # Extremely safe concurrency
    
    def is_gift_card(self, product_id: str) -> bool:
        pid_lower = str(product_id).lower()
        return ('gift' in pid_lower and 'card' in pid_lower) or 'gctanishq' in pid_lower
    
    def get_with_retry(self, url: str, max_retries: int = 5) -> Optional[requests.Response]:
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    # Exponential backoff: 2s, 4s, 8s, 16s...
                    sleep_time = 2 * (2 ** attempt) + random.uniform(0, 1)
                    time.sleep(sleep_time)
                
                # Increased timeout to 60s to handle slow server responses
                response = self.session.get(url, timeout=60)
                
                # Special handling for 429 Too Many Requests
                if response.status_code == 429:
                    logger.warning(f"Got 429 for {url}. Sleeping longer...")
                    time.sleep(10 + 5 * attempt)
                    continue
                    
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    return None
                time.sleep(2 * (attempt + 1))
        return None
    
    def extract_product_ids_from_listing(self, soup: BeautifulSoup) -> List[Dict[str, str]]:
        products = []
        # Find all data-pid attributes which are very reliable
        tiles = soup.select('[data-pid]')
        
        for tile in tiles:
            product_id = str(tile.get('data-pid')).lower()
            if product_id and not self.is_gift_card(product_id):
                # Avoid duplicates within the same page
                if product_id not in [p['product_id'] for p in products]:
                    products.append({
                        'product_id': product_id, 
                        'sku': product_id, 
                        'url': f"/product/{product_id}.html"
                    })
        
        return products
    
    def scrape_category_page(self, url: str, category_name: str, price_filter: str = "") -> List[Dict[str, str]]:
        """Scrape a category page with bulk fetch (sz=1000) and robust pagination"""
        display_name = f"{category_name} ({price_filter})" if price_filter else category_name
        logger.info(f"Scraping Listing: {display_name}")
        
        all_products = []
        category_seen_pids = set()
        page = 1
        page_size = 240 # Reduced from 1000 to 240 for stability (server timeouts)
        
        base_filter_url = url
        if price_filter:
            connector = "&" if "?" in base_filter_url else "?"
            base_filter_url += f"{connector}{self.PRICE_RANGES.get(price_filter, price_filter)}"
            
        while page <= 200: # Covers 48k products with sz=240
            page_url = f"{base_filter_url}&sz={page_size}&start={(page-1)*page_size}"
            logger.info(f"Fetching: {page_url}")
            
            response = self.get_with_retry(page_url)
            if not response:
                break
            
            soup = BeautifulSoup(response.content, 'html.parser')
            products = self.extract_product_ids_from_listing(soup)
            
            if not products:
                logger.info(f"No more products found on page {page}")
                break
            
            new_found = 0
            for product in products:
                pid = product['product_id']
                if pid not in category_seen_pids:
                    product['category'] = category_name
                    product['price_bracket'] = price_filter
                    product['source_url'] = page_url
                    all_products.append(product)
                    category_seen_pids.add(pid)
                    new_found += 1
            
            logger.info(f"Page {page}: Found {len(products)} products ({new_found} new)")
            
            # If no new products found on this page, we've likely hit the end or are looping
            if new_found == 0:
                logger.info("No new products found on this page. Stopping.")
                break
            
            # If we found fewer products than the page size, we might be at the end
            if len(products) < page_size:
                logger.info(f"Found {len(products)} which is less than sz={page_size}. Reached end.")
                break
                
            page += 1
        
        return all_products
    
    def scrape_all_categories(self) -> Dict[str, List[Dict]]:
        """Scrape all categories using price-based ranges for discovery"""
        all_products = {}
        
        for category_group, subcategories in self.CATEGORIES.items():
            logger.info(f"Processing Group: {category_group}")
            
            for subcategory, path in subcategories.items():
                full_url = urljoin(self.BASE_URL, path)
                
                # Use price ranges for discovery to ensure all data is captured
                for range_name in self.PRICE_RANGES.keys():
                    products = self.scrape_category_page(full_url, f"{subcategory}_{range_name}", range_name)
                    all_products[f"{subcategory}_{range_name}"] = products
                    self.stats['total_listings'] += len(products)
        
        return all_products
    
    def deduplicate_products(self, all_products: Dict[str, List[Dict]]) -> List[Dict]:
        logger.info("Deduplicating...")
        unique_products = {}
        duplicate_map = defaultdict(list)
        
        for category, products in all_products.items():
            for product in products:
                key = product['product_id']
                if not key:
                    continue
                
                if key in unique_products:
                    duplicate_map[key].append(category)
                else:
                    unique_products[key] = product
                    duplicate_map[key] = [category]
        
        for key, product in unique_products.items():
            product['appears_in_categories'] = duplicate_map[key]
        
        logger.info(f"Unique: {len(unique_products)}, Duplicates: {self.stats['total_listings'] - len(unique_products)}")
        return list(unique_products.values())
    
    def extract_product_details(self, product_meta: Dict) -> Optional[Dict]:
        """Extract product details from H4 tags"""
        product_id = str(product_meta['product_id']).lower()
        
        with self.lock:
            if product_id in self.seen_product_ids:
                return None
            self.seen_product_ids.add(product_id)
        
        # Always use lowercase product_id for the URL to ensure reliability
        url = f"{self.BASE_URL}/product/{product_id}.html"
        logger.info(f"Scraping: {product_id}")
        
        response = self.get_with_retry(url)
        if not response:
            self.stats['failed_products'] += 1
            return None
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        details = {
            'product_id': product_id,
            'sku': '',
            'product_url': url,
            'scraped_at': datetime.now().isoformat(),
            'categories': product_meta.get('appears_in_categories', [])
        }
        
        # Extract SKU from scripts
        script_data = self.extract_from_scripts(soup)
        if script_data:
            details['sku'] = script_data.get('sku', '')
        
        # Extract product name (clean it)
        raw_name = self.extract_product_name(soup)
        details['product_name'] = raw_name.replace(' | Tanishq Online Store', '').strip()
        
        # Extract price
        details['price'] = self.extract_price(soup)
        details['currency'] = 'INR' if details['price'] else ''
        
        # Extract ALL accordion sections (this is where the data is!)
        accordion_data = self.extract_accordion_details(soup)
        details.update(accordion_data)
        
        # Images
        details['images'] = self.extract_images(soup)
        
        with self.lock:
            self.stats['successful_products'] += 1
        
        return details
    
    def extract_product_name(self, soup: BeautifulSoup) -> str:
        """Extract product name"""
        # Try product-name div first
        name_elem = soup.select_one('.product-name, .style-product-name')
        if name_elem:
            return name_elem.get_text(strip=True)
        
        # Try meta tag
        meta = soup.find('meta', property='og:title')
        if meta and meta.get('content'):
            return meta['content']
        
        # Try title
        title = soup.find('title')
        if title:
            return title.get_text(strip=True)
        
        return ""
    
    def extract_price(self, soup: BeautifulSoup) -> Optional[float]:
        """Extract price with high reliability using GTM data or sales value"""
        # 1. Try GTM data (most reliable)
        gtm_data = soup.select_one('.gtm-data')
        if gtm_data and gtm_data.get('data-prices'):
            try:
                return float(gtm_data['data-prices'])
            except (ValueError, TypeError):
                pass

        # 2. Fallback to sales price element
        price_elem = soup.select_one('.product-price .sales .value, .price-sales, .pdp-product-main-sale-price')
        if price_elem:
            text = price_elem.get_text()
            price_match = re.search(r'₹?\s*([\d,]+)', text)
            if price_match:
                try:
                    return float(price_match.group(1).replace(',', ''))
                except ValueError:
                    pass
        return None
    
    def extract_accordion_details(self, soup: BeautifulSoup) -> Dict:
        """Extract data from accordion sections, excluding Description"""
        
        details = {
            'product_type': '',
            'metal': '',
            'karat': '',
            'occasion': '',
            'community': '',
            'collection': '',
            'gender': '',
            'jewellery_category': '',
            'gross_weight': '',
            'dimensions': '',
            'stone_type': '',
            'stone_weight': '',
            'diamond_clarity': '',
            'diamond_color': '',
            'diamond_count': '',
            'diamond_shape': '',
            'setting_type': '',
            'raw_attributes': {}
        }
        
        accordion_containers = soup.select('.product-details-acordian-container')
        logger.debug(f"Found {len(accordion_containers)} accordion containers")
        
        for container in accordion_containers:
            header = container.select_one('.accordian-header')
            if not header:
                continue
                
            header_text = header.get_text(strip=True).upper()
            
            # Skip Description section as requested
            # Extract Description
            if 'DESCRIPTION' in header_text:
                content = container.select_one('.accordian-content')
                if content:
                    details['description'] = content.get_text(strip=True)
                continue
                
            content = container.select_one('.accordian-content')
            if not content:
                continue
                
            # Extract key-value pairs
            # Tanishq uses <h4> for value and <p> for key in these blocks
            blocks = content.select('.col-lg-4, .col-6')
            for block in blocks:
                val_elem = block.select_one('h4')
                key_elem = block.select_one('p')
                
                if val_elem and key_elem:
                    key = key_elem.get_text(strip=True)
                    val = val_elem.get_text(strip=True)
                    
                    details['raw_attributes'][key] = val
                    
                    # Map to specific fields for backward compatibility
                    key_lower = key.lower()
                    if 'karat' in key_lower:
                        details['karat'] = val
                    elif 'material colour' in key_lower:
                        details['metal_color'] = val # Keep distinct
                    elif 'metal' == key_lower:
                        details['metal'] = val # Keep distinct
                    elif 'gross weight' in key_lower:
                        details['gross_weight'] = val
                    elif 'jewellery type' in key_lower:
                        details['jewellery_category'] = val
                    elif 'product type' in key_lower:
                        details['product_type'] = val
                    elif 'brand' in key_lower:
                        details['raw_attributes']['brand'] = val
                    elif 'collection' in key_lower:
                        details['collection'] = val
                    elif 'gender' in key_lower:
                        details['gender'] = val
                    elif 'occasion' in key_lower:
                        details['occasion'] = val
                    elif 'diamond clarity' in key_lower:
                        details['diamond_clarity'] = val
                    elif 'diamond color' in key_lower:
                        details['diamond_color'] = val
                    elif 'no of diamonds' in key_lower:
                        # Sometimes it's "No of Diamonds", sometimes "Diamond Count"
                        details['diamond_count'] = val
                    elif 'diamond setting' in key_lower:
                        details['setting_type'] = val
                    elif 'diamond shape' in key_lower:
                        details['diamond_shape'] = val
                    elif 'stone type' in key_lower:
                        details['stone_type'] = val
                    elif 'stone weight' in key_lower:
                        details['stone_weight'] = val
        
        return details
    
    def extract_from_scripts(self, soup: BeautifulSoup) -> Optional[Dict]:
        """Extract SKU from JavaScript"""
        scripts = soup.find_all('script')
        
        for script in scripts:
            if not script.string:
                continue
            
            if 'CQuotient' in script.string or 'dw.ac' in script.string:
                sku_match = re.search(r'sku:\s*["\']([^"\']+)["\']', script.string)
                if sku_match:
                    return {'sku': sku_match.group(1)}
        
        return None
    
    def extract_images(self, soup: BeautifulSoup) -> List[str]:
        images = []
        img_elements = soup.select('img[src*="hi-res"], .product-primary-image img')
        
        for img in img_elements:
            src = img.get('src') or img.get('data-src')
            if src:
                full_url = urljoin(self.BASE_URL, src)
                if full_url not in images:
                    images.append(full_url)
        
        return images[:10]
    
    def save_checkpoint(self, data, filename: str):
        filepath = self.output_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved: {filepath}")
    
    def run_complete_pipeline(self):
        logger.info("=" * 80)
        logger.info("TANISHQ SCRAPING PIPELINE")
        logger.info("=" * 80)
        
        logger.info("\n[STEP 1] Scraping category listings...")
        all_category_products = self.scrape_all_categories()
        self.save_checkpoint(all_category_products, 'raw_listings.json')
        
        logger.info("\n[STEP 2] Deduplicating...")
        unique_products = self.deduplicate_products(all_category_products)
        self.save_checkpoint(unique_products, 'unique_products_index.json')
        
        logger.info(f"\n[STEP 3] Scraping {len(unique_products)} product details using {self.max_workers} threads...")
        detailed_products = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_product = {executor.submit(self.extract_product_details, p): p for p in unique_products}
            
            completed_count = 0
            for future in concurrent.futures.as_completed(future_to_product):
                completed_count += 1
                try:
                    details = future.result()
                    if details:
                        detailed_products.append(details)
                except Exception as exc:
                    product_id = future_to_product[future].get('product_id', 'unknown')
                    logger.error(f'Product {product_id} generated an exception: {exc}')
                    with self.lock:
                        self.stats['failed_products'] += 1
                
                if completed_count % 100 == 0:
                    logger.info(f"Progress: {completed_count}/{len(unique_products)}")
                    self.save_checkpoint(detailed_products, f'products_checkpoint_{completed_count}.json')
        
        logger.info(f"\n[STEP 4] Saving final results...")
        self.save_checkpoint(detailed_products, 'final_products.json')
        
        df = pd.DataFrame(detailed_products)
        df.to_csv(self.output_dir / 'final_products.csv', index=False, encoding='utf-8')
        df.to_excel(self.output_dir / 'final_products.xlsx', index=False, engine='openpyxl')
        
        self.generate_report(detailed_products)
        
        logger.info("\n" + "=" * 80)
        logger.info("SCRAPING COMPLETED")
        logger.info("=" * 80)
        
        return detailed_products
    
    def generate_report(self, products: List[Dict]):
        report = {
            'timestamp': datetime.now().isoformat(),
            'total_products': len(products),
            'total_listings': self.stats['total_listings'],
            'duplicates_removed': self.stats['total_listings'] - len(products),
            'successful': self.stats['successful_products'],
            'failed': self.stats['failed_products']
        }
        
        fields = ['product_name', 'price', 'sku', 'metal', 'karat', 'product_type', 
                  'collection', 'gender', 'occasion', 'stone_type']
        for field in fields:
            filled = sum(1 for p in products if p.get(field))
            report[f'{field}_completeness'] = f"{filled}/{len(products)} ({filled/len(products)*100:.1f}%)"
        
        self.save_checkpoint(report, 'scraping_report.json')
        logger.info(f"\n{json.dumps(report, indent=2)}")


    def run_batch_mode(self, input_file: str, final_filename: str):
        """
        Run scraping in batch mode.
        Args:
            input_file: Path to the JSON file containing list of products to scrape
            final_filename: Name of the final output file (e.g. batch1_product_checkpoint.json)
        """
        logger.info("=" * 80)
        logger.info(f"TANISHQ BATCH SCRAPER - Input: {input_file}")
        logger.info("=" * 80)
        
        # Load input products
        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                unique_products = json.load(f)
        except Exception as e:
            logger.error(f"Failed to load input file {input_file}: {e}")
            return []
            
        logger.info(f"\n[START] Scraping {len(unique_products)} products using {self.max_workers} threads...")
        detailed_products = []
        
        # Extract batch name from final filename (e.g., "batch1" from "batch1_product_checkpoint.json")
        batch_prefix = final_filename.split('_')[0] if '_' in final_filename else "batch"
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_product = {executor.submit(self.extract_product_details, p): p for p in unique_products}
            
            completed_count = 0
            for future in concurrent.futures.as_completed(future_to_product):
                completed_count += 1
                try:
                    details = future.result()
                    if details:
                        detailed_products.append(details)
                except Exception as exc:
                    product_id = future_to_product[future].get('product_id', 'unknown')
                    logger.error(f'Product {product_id} generated an exception: {exc}')
                    with self.lock:
                        self.stats['failed_products'] += 1
                
                # Save intermediate checkpoints
                if completed_count % 100 == 0:
                    logger.info(f"Progress: {completed_count}/{len(unique_products)}")
                    self.save_checkpoint(detailed_products, f'{batch_prefix}_checkpoint_{completed_count}.json')
        
        logger.info(f"\n[FINISH] Saving final batch result to {final_filename}...")
        self.save_checkpoint(detailed_products, final_filename)
        
        # Also save CSV/Excel for convenience
        try:
            df = pd.DataFrame(detailed_products)
            base_name = Path(final_filename).stem
            df.to_csv(self.output_dir / f'{base_name}.csv', index=False, encoding='utf-8')
            df.to_excel(self.output_dir / f'{base_name}.xlsx', index=False, engine='openpyxl')
        except Exception as e:
            logger.error(f"Failed to save CSV/Excel: {e}")
        
        self.generate_report(detailed_products)
        
        logger.info("\n" + "=" * 80)
        logger.info("BATCH SCRAPING COMPLETED")
        logger.info("=" * 80)
        
        return detailed_products


if __name__ == "__main__":
    scraper = TanishqScraper(output_dir="scarp-data-1")
    products = scraper.run_complete_pipeline()
    print(f"\n✅ Scraped {len(products)} products")