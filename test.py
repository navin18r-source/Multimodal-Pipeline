import torch
from transformers import AutoTokenizer, AutoProcessor, AutoModel
from peft import PeftModel
from qdrant_client import QdrantClient
from PIL import Image
import os
import argparse
import shutil
import json
import re
import time
import numpy as np

class Config:
    MODEL_NAME = "google/siglip-so400m-patch14-384"
    LORA_WEIGHTS_PATH = "/workspace/models/siglip2-jewelry-lora/best"
    COLLECTION_NAME = "jewelry_collection"
    QDRANT_PATH = "/workspace/qdrant_db"
    OUTPUT_DIR = "/workspace/search_results"
    DEVICE = "cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu"

class JewelrySearcher:
    def __init__(self):
        self.processor = AutoProcessor.from_pretrained(Config.MODEL_NAME)
        self.tokenizer = AutoTokenizer.from_pretrained(Config.MODEL_NAME)
        base_model = AutoModel.from_pretrained(Config.MODEL_NAME).to(Config.DEVICE)
        if os.path.exists(Config.LORA_WEIGHTS_PATH):
            self.model = PeftModel.from_pretrained(base_model, Config.LORA_WEIGHTS_PATH).to(Config.DEVICE)
        else:
            self.model = base_model
        self.model.eval()
        self.client = QdrantClient(path=Config.QDRANT_PATH)
        os.makedirs(Config.OUTPUT_DIR, exist_ok=True)

    def get_text_embedding(self, query):
        inputs = self.tokenizer(text=query, padding="max_length", truncation=True, max_length=64, return_tensors="pt").to(Config.DEVICE)
        with torch.no_grad():
            if hasattr(self.model, 'get_text_features'):
                outputs = self.model.get_text_features(**inputs)
            else:
                outputs = self.model.base_model.get_text_features(**inputs)
            if hasattr(outputs, 'pooler_output'):
                outputs = outputs.pooler_output
            emb = torch.nn.functional.normalize(outputs, dim=-1)
        return emb.cpu().numpy().squeeze()

    def get_image_embedding(self, image_path):
        img = Image.open(image_path).convert('RGB')
        inputs = self.processor(images=img, return_tensors="pt").to(Config.DEVICE)
        with torch.no_grad():
            if hasattr(self.model, 'get_image_features'):
                outputs = self.model.get_image_features(**inputs)
            else:
                outputs = self.model.base_model.get_image_features(**inputs)
            if hasattr(outputs, 'pooler_output'):
                outputs = outputs.pooler_output
            emb = torch.nn.functional.normalize(outputs, dim=-1)
        return emb.cpu().numpy().squeeze()

    def search(self, text_query=None, image_path=None, top_k=10):
        embs = []
        if text_query: embs.append(self.get_text_embedding(text_query))
        if image_path: embs.append(self.get_image_embedding(image_path))
        if not embs: return

        final_emb = np.mean(embs, axis=0)
        final_emb = final_emb / np.linalg.norm(final_emb)
        
        res_obj = self.client.query_points(collection_name=Config.COLLECTION_NAME, query=final_emb.tolist(), limit=top_k)
        results = res_obj.points
        
        label = text_query if text_query else os.path.basename(image_path)
        safe_query = re.sub(r'[^\w\s-]', '', label).strip().replace(' ', '_').lower()
        query_dir = os.path.join(Config.OUTPUT_DIR, safe_query)
        os.makedirs(query_dir, exist_ok=True)
        
        search_metadata = []
        print(f"\nResults for: {label}")
        for i, res in enumerate(results):
            rank = i + 1
            score = f"{res.score:.4f}"
            prod_id = res.payload['product_id']
            src_path = res.payload['path']
            dest_filename = f"{rank:02d}_{score}_{prod_id}{os.path.splitext(src_path)[1]}"
            dest_path = os.path.join(query_dir, dest_filename)
            
            if os.path.exists(src_path):
                try: shutil.copy2(src_path, dest_path)
                except Exception: pass
            
            search_metadata.append({"rank": rank, "score": float(res.score), "product_id": prod_id, "path": src_path})
            print(f"{rank}. [{score}] ID: {prod_id}")

        self.update_json_results(label, search_metadata)

    def update_json_results(self, query, metadata):
        json_path = os.path.join(Config.OUTPUT_DIR, "search_results.json")
        all_results = {}
        if os.path.exists(json_path):
            try:
                with open(json_path, 'r') as f: all_results = json.load(f)
            except Exception: pass
        all_results[query] = metadata
        with open(json_path, 'w') as f: json.dump(all_results, f, indent=4)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", type=str)
    parser.add_argument("--image", type=str)
    args = parser.parse_args()
    searcher = JewelrySearcher()
    
    if args.query or args.image:
        searcher.search(text_query=args.query, image_path=args.image)
    else:
        while True:
            cmd = input("\nSearch: ").strip()
            if not cmd or cmd.lower() == 'exit': break
            
            if "|" in cmd:
                parts = cmd.split("|")
                img_p, txt_q = parts[0].strip(), parts[1].strip()
                searcher.search(text_query=txt_q, image_path=img_p)
            elif os.path.exists(cmd) and not os.path.isdir(cmd):
                searcher.search(image_path=cmd)
            else:
                searcher.search(text_query=cmd)

if __name__ == "__main__":
    main()
