from scraper import TanishqScraper
from pathlib import Path
import sys

def run_batch_1():
    print("Initializing Batch 1 Scraper...")
    # Output directory strictly as requested: "batch_one"
    scraper = TanishqScraper(output_dir="batch_one")
    
    input_file = "scarp-data/batch_inputs/batch_1.json"
    final_filename = "batch1_product_checkpoint.json"
    
    if not Path(input_file).exists():
        print(f"Error: Input file {input_file} not found. Run prepare_batches.py first.")
        sys.exit(1)
        
    print(f"Starting Batch 1 processing with input: {input_file}")
    print(f"Final output will be: batch_one/{final_filename}")
    
    # Run the batch method
    scraper.run_batch_mode(input_file, final_filename)

if __name__ == "__main__":
    run_batch_1()
