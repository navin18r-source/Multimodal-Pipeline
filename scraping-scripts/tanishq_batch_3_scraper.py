from scraper import TanishqScraper
from pathlib import Path
import sys

def run_batch_3():
    print("Initializing Batch 3 Scraper...")
    scraper = TanishqScraper(output_dir="batch_three")
    
    input_file = "scarp-data/batch_inputs/batch_3.json"
    final_filename = "batch3_product_checkpoint.json"
    
    if not Path(input_file).exists():
        print(f"Error: Input file {input_file} not found. Run prepare_batches.py first.")
        sys.exit(1)
        
    print(f"Starting Batch 3 processing with input: {input_file}")
    scraper.run_batch_mode(input_file, final_filename)

if __name__ == "__main__":
    run_batch_3()
