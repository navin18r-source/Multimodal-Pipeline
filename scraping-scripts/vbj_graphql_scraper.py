import requests
import json
import time
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('vbj_graphql.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class VBJGraphQLScraper:
    # Extracted Credentials
    API_URL = "https://shop.vummidi.com/api/graphql"
    ACCESS_TOKEN = "eb6949f9c39586bad5c98e0240ab5096"
    
    HEADERS = {
        "X-Shopify-Storefront-Access-Token": ACCESS_TOKEN,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    # Removed hardcoded handles in favor of dynamic discovery

    def __init__(self, output_dir="vbj_data"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.session = requests.Session()

    def run_query(self, query, variables=None):
        """Execute GraphQL query with retry logic"""
        payload = {"query": query, "variables": variables}
        for attempt in range(3):
            try:
                response = self.session.post(self.API_URL, headers=self.HEADERS, json=payload, timeout=30)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    logger.warning(f"Rate limited (429). Sleeping 10s...")
                    time.sleep(10)
                else:
                    logger.error(f"API Error {response.status_code}: {response.text}")
                    return None
            except Exception as e:
                logger.error(f"Request failed: {e}")
                time.sleep(5)
    def fetch_all_collections(self):
        """Dynamically fetch ALL collection handles from the store"""
        logger.info("Discovering all collections from API...")
        handles = []
        has_next = True
        cursor = None
        
        query = """
        query getCollections($cursor: String) {
          collections(first: 250, after: $cursor) {
            pageInfo {
              hasNextPage
              endCursor
            }
            edges {
              node {
                handle
                title
              }
            }
          }
        }
        """
        
        while has_next:
            variables = {"cursor": cursor}
            response = self.run_query(query, variables)
            
            if not response or 'collections' not in response.get('data', {}):
                break
                
            data = response['data']['collections']
            for edge in data['edges']:
                node = edge['node']
                handle = node['handle']
                # Filter out irrelevant functional collections if needed, but keeping all for now
                handles.append(handle)
                logger.info(f" -> Found collection: {node['title']} ({handle})")
                
            has_next = data['pageInfo']['hasNextPage']
            cursor = data['pageInfo']['endCursor']
            
        logger.info(f"Total collections found: {len(handles)}")
        return handles

    def get_products_from_collection(self, handle):
        """Fetch all products in a specific collection using cursor pagination"""
        logger.info(f"Fetching products for collection: {handle}")
        
        all_products = []
        has_next_page = True
        cursor = None
        
        query = """
        query getCollectionProducts($handle: String!, $cursor: String) {
          collectionByHandle(handle: $handle) {
            products(first: 250, after: $cursor) {
              pageInfo {
                hasNextPage
                endCursor
              }
              edges {
                node {
                  id
                  title
                  handle
                  description
                  productType
                  vendor
                  availableForSale
                  
                  priceRange {
                    minVariantPrice {
                      amount
                      currencyCode
                    }
                  }
                  
                  images(first: 10) {
                    edges {
                      node {
                        url
                        altText
                      }
                    }
                  }
                  
                  tags
                  variants(first: 5) {
                    edges {
                      node {
                        id
                        title
                        sku
                        weight
                        weightUnit
                        price {
                          amount
                          currencyCode
                        }
                        selectedOptions {
                          name
                          value
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
        """
        
        while has_next_page:
            variables = {"handle": handle, "cursor": cursor}
            response = self.run_query(query, variables)
            
            if not response or 'errors' in response:
                logger.error(f"Query error: {response}")
                break
                
            data = response.get('data', {}).get('collectionByHandle', {})
            if not data:
                logger.warning(f"Collection '{handle}' not found or empty.")
                break
                
            products_data = data.get('products', {})
            edges = products_data.get('edges', [])
            
            if not edges:
                break
                
            for edge in edges:
                node = edge['node']
                all_products.append(self.parse_product(node, handle))
                
            page_info = products_data.get('pageInfo', {})
            has_next_page = page_info.get('hasNextPage', False)
            cursor = page_info.get('endCursor')
            
            logger.info(f"  Fetched {len(edges)} products. Total so far: {len(all_products)}")
            time.sleep(1) # Polite delay
            
        return all_products

    def parse_product(self, node, category):
        """Transform raw GraphQL node into flat dictionary with metadata"""
        # Images
        images = [img['node']['url'] for img in node.get('images', {}).get('edges', [])]
        image_url = images[0] if images else ""
        
        # Price
        price_range = node.get('priceRange', {}).get('minVariantPrice', {})
        price = f"{price_range.get('amount', 0)}"
        
        # Variants logic
        variants_edges = node.get('variants', {}).get('edges', [])
        first_variant = variants_edges[0]['node'] if variants_edges else {}
        sku = first_variant.get('sku', '')
        
        # Extract Weight (often available in variant details)
        weight = first_variant.get('weight', '')
        weight_unit = first_variant.get('weightUnit', '')
        
        # Metadata from Tags
        tags = node.get('tags', [])
        meta = {
            "purity": "",
            "gender": "",
            "metal_type": "",
            "metal_color": "",
            "stone_type": "",
            "net_weight": f"{weight} {weight_unit}".strip() if weight else "",
            "gross_weight": "",
            "collection": "",
            "diamond_quality": "" # Corresponds to "Quality" in screenshot (IF-VVS1/DEF)
        }
        
        for tag in tags:
            try:
                if ';' in tag:
                    key, val = tag.split(';', 1)
                    key_lower = key.lower().strip()
                    val = val.strip()
                    
                    if key_lower == 'purity':
                        meta['purity'] = f"{val} kt"
                    elif key_lower == 'gender':
                        meta['gender'] = val
                    elif key_lower == 'metal-type' or key_lower == 'metal':
                        meta['metal_type'] = val
                    elif key_lower == 'stone-type':
                        meta['stone_type'] = val
                    elif key_lower == 'gross-weight':
                       meta['gross_weight'] = f"{val} g"
                    elif key_lower == 'net-weight':
                       meta['net_weight'] = f"{val} g"
                    elif key_lower == 'collection':
                        meta['collection'] = val
                    elif key_lower == 'diamond-clarity' or key_lower == 'quality':
                        meta['diamond_quality'] = val
                    elif key_lower == 'metal-color':
                        meta['metal_color'] = val
            except:
                pass

        return {
            "product_id": node.get('id'),
            "sku": sku,
            "title": node.get('title'),
            "category": category,
            "product_type": node.get('productType'),
            "price": price,
            "currency": price_range.get('currencyCode', 'INR'),
            "purity": meta['purity'],
            "gender": meta['gender'],
            "net_weight": meta['net_weight'],
            "gross_weight": meta['gross_weight'],
            "metal_type": meta['metal_type'],
            "metal_color": meta['metal_color'],
            "collection": meta['collection'],
            "diamond_quality": meta['diamond_quality'],
            "description": node.get('description', '').replace('\n', ' '),
            "product_url": f"https://www.vummidi.com/us/product/{node.get('handle')}",
            "image_url": image_url,
            "all_images": " | ".join(images),
            "all_tags": ", ".join(tags),
            "scraped_at": datetime.now().isoformat()
        }

    def run(self):
        full_catalog = []
        self.output_dir.mkdir(exist_ok=True)
        
        # dynamic discovery
        all_handles = self.fetch_all_collections()
        
        # Deduplication set
        seen_ids = set()
        
        for handle in all_handles:
            products = self.get_products_from_collection(handle)
            
            for p in products:
                if p['product_id'] not in seen_ids:
                    full_catalog.append(p)
                    seen_ids.add(p['product_id'])
            
            # Partial Save
            self.save_data(full_catalog, "vbj_products_partial.json")
        
        # Final Save
        self.save_data(full_catalog, "vbj_products_final.json")
        self.save_csv(full_catalog, "vbj_products_final.csv")
        logger.info("="*50)
        logger.info(f"DONE. Total products scraped: {len(full_catalog)}")
        logger.info("="*50)

    def save_data(self, data, filename):
        filepath = self.output_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved checkpoint: {filepath}")

    def save_csv(self, data, filename):
        df = pd.DataFrame(data)
        df.to_csv(self.output_dir / filename, index=False)
        logger.info(f"Saved CSV: {filename}")

if __name__ == "__main__":
    scraper = VBJGraphQLScraper()
    scraper.run()
