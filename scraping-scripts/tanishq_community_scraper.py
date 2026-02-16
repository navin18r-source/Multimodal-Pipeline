import json
import time
import random
import logging
from typing import Dict, List, Set
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CommunityScraper:
    BASE_URL = "https://www.tanishq.co.in"
    
    COMMUNITIES = {
        "Bihari Bride": "maithili?lang=en_IN",
        "Tamil Bride": "tamil-jewellery?lang=en_IN",
        "Telugu Bride": "rivaah-telugu-jewellery?lang=en_IN",
        "Kannadiga Bride": "rivaah-telugu-jewellery?lang=en_IN",
        "Gujarati Bride": "rivaah-gujarati-jewellery?lang=en_IN",
        "Marathi Bride": "rivaah-maharashtrian-jewellery?lang=en_IN",
        "Bengali Bride": "rivaah-bengali-jewellery?lang=en_IN",
        "Punjabi Bride": "rivaah?lang=en_IN&prefn1=community&prefv1=Punjabi",
        "UP Bride": "rivaah?lang=en_IN&prefn1=community&prefv1=North%20Indian",
        "Marwari Bride": "rivaah?lang=en_IN",
        "Odia Bride": "rivaah?lang=en_IN",
        "Muslim Bride": "rivaah?lang=en_IN&prefn1=community&prefv1=North%20Indian"
    }

    def __init__(self):
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--window-size=1920,1080')
        options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
        
        logger.info("Initializing Chrome Driver...")
        self.driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)

    def scrape_community_skus(self) -> List[Dict]:
        sku_community_map = {} # SKU -> Set of communities

        try:
            for community_name, path in self.COMMUNITIES.items():
                base_url = f"{self.BASE_URL}/shop/{path}"
                logger.info(f"Scraping Community: {community_name}")
                
                page = 1
                while page <= 20: 
                    if page == 1:
                        page_url = base_url
                    else:
                        connector = "&" if "?" in base_url else "?"
                        page_url = f"{base_url}{connector}sz=48&start={(page-1)*48}"
                    
                    try:
                        logger.info(f"Navigating to {page_url}")
                        self.driver.get(page_url)
                        
                        # Handle Modal
                        try:
                            WebDriverWait(self.driver, 5).until(
                                EC.element_to_be_clickable((By.CSS_SELECTOR, '.close, .close-button, .modal-close, [aria-label="Close"]'))
                            ).click()
                            logger.info("Closed modal overlay")
                        except Exception:
                            pass
                        
                        # Wait for products
                        try:
                            WebDriverWait(self.driver, 30).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, '[data-pid]'))
                            )
                        except Exception:
                            logger.info(f"No more products found (timeout) on page {page}")
                            break 
                        
                        # Scroll
                        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
                        time.sleep(1)
                        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        time.sleep(2) 
                        
                        # Find tiles
                        product_tiles = self.driver.find_elements(By.CSS_SELECTOR, '.product-tile[data-pid], [data-pid]')
                        
                        if not product_tiles:
                            logger.info(f"No products found on page {page}")
                            break
                        
                        count_on_page = 0
                        for tile in product_tiles:
                            sku = tile.get_attribute('data-pid')
                            if sku and len(sku) > 5:
                                if sku not in sku_community_map:
                                    sku_community_map[sku] = set()
                                sku_community_map[sku].add(community_name)
                                count_on_page += 1
                        
                        logger.info(f"Page {page}: Found {count_on_page} products")
                        
                        if count_on_page < 10:
                            logger.info("Reached end of results (low count).")
                            break
                            
                        page += 1
                        time.sleep(random.uniform(2, 4))
                        
                    except Exception as e:
                        logger.error(f"Error scraping {page_url}: {e}")
                        break
                        
        finally:
            self.driver.quit()

        # Results
        results = []
        for sku, communities in sku_community_map.items():
            results.append({
                "sku": sku,
                "community": sorted(list(communities))
            })
            
        return results

    def save_data(self, data: List[Dict]):
        with open('communtiy-data.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved {len(data)} SKU mappings to communtiy-data.json")

if __name__ == "__main__":
    scraper = CommunityScraper()
    data = scraper.scrape_community_skus()
    scraper.save_data(data)
