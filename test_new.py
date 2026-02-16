import torch
from transformers import AutoTokenizer, AutoProcessor, AutoModel
from peft import PeftModel
from qdrant_client import QdrantClient
from PIL import Image
import os
import json
import re
import numpy as np
import google.generativeai as genai
import requests
from bs4 import BeautifulSoup
import shutil
import argparse
from pathlib import Path
import io

class Config:
    MODEL_NAME = "google/siglip-so400m-patch14-384"
    LORA_WEIGHTS_PATH = "/workspace/models/siglip2-jewelry-lora-new/best"
    COLLECTION_NAME = "jewelry_collection"
    QDRANT_PATH = "/workspace/qdrant_db_new"
    DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
    SARVAM_API_KEY = os.getenv("SARVAM_API_KEY")
    IMAGE_BASE_DIR = "/workspace/multimodal-dataset/"
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
    GEMINI_MODEL = "gemini-2.0-flash-exp"
    OUTPUT_DIR = "/workspace/search_results_new"

class UnifiedJewelrySearcher:
    def __init__(self):
        print("🚀 Loading Models...")
        self.processor = AutoProcessor.from_pretrained(Config.MODEL_NAME)
        self.tokenizer = AutoTokenizer.from_pretrained(Config.MODEL_NAME)
        base_model = AutoModel.from_pretrained(Config.MODEL_NAME).to(Config.DEVICE)
        if os.path.exists(Config.LORA_WEIGHTS_PATH):
            self.model = PeftModel.from_pretrained(base_model, Config.LORA_WEIGHTS_PATH).to(Config.DEVICE)
        else:
            self.model = base_model
        self.model.eval()
        
        self.client = QdrantClient(path=Config.QDRANT_PATH)
        
        if Config.SARVAM_API_KEY: print("✨ Sarvam AI: ACTIVE")
        if Config.GEMINI_API_KEY:
            genai.configure(api_key=Config.GEMINI_API_KEY)
            self.gemini = genai.GenerativeModel(Config.GEMINI_MODEL)
            print("✨ Gemini AI: ACTIVE")
        else:
            self.gemini = None
            
        os.makedirs(Config.OUTPUT_DIR, exist_ok=True)
        print("✅ Ready!")

    # --- AI Utility Methods ---
    def sarvam_translate(self, text):
        url = "https://api.sarvam.ai/translate"
        headers = {"api-subscription-key": Config.SARVAM_API_KEY, "Content-Type": "application/json"}
        payload = {"input": text, "source_language_code": "auto", "target_language_code": "en-IN", "speaker_gender": "Female", "mode": "formal"}
        try:
            response = requests.post(url, headers=headers, json=payload)
            return response.json().get("translated_text", text)
        except: return text

    def sarvam_stt(self, audio_path):
        url = "https://api.sarvam.ai/speech-to-text-translate"
        headers = {"api-subscription-key": Config.SARVAM_API_KEY}
        try:
            with open(audio_path, 'rb') as f:
                response = requests.post(url, headers=headers, files={'file': f})
                return response.json().get("transcript", "")
        except: return None

    def detect_and_describe(self, image):
        if not self.gemini: return image, None
        prompt = "Locate jewelry. Return ONLY JSON: {\"bbox\": [y_min, x_min, y_max, x_max], \"description\": \"...\"}"
        try:
            response = self.gemini.generate_content([prompt, image])
            res = json.loads(re.search(r'\{.*\}', response.text, re.DOTALL).group())
            bbox, desc = res.get("bbox"), res.get("description")
            if bbox:
                w, h = image.size
                image = image.crop((bbox[1]*w/1000, bbox[0]*h/1000, bbox[3]*w/1000, bbox[2]*h/1000))
                print(f"🎯 AI Description: {desc}")
            return image, desc
        except: return image, None

    # --- Embedding Methods ---
    def get_text_embedding(self, query):
        inputs = self.tokenizer(text=query, padding="max_length", truncation=True, max_length=64, return_tensors="pt").to(Config.DEVICE)
        with torch.no_grad():
            if hasattr(self.model, 'get_text_features'):
                out = self.model.get_text_features(**inputs)
            else:
                out = self.model.base_model.get_text_features(**inputs)
            if hasattr(out, 'pooler_output'): out = out.pooler_output
            return torch.nn.functional.normalize(out, dim=-1).cpu().numpy().squeeze()

    def get_image_embedding(self, image):
        inputs = self.processor(images=image, return_tensors="pt").to(Config.DEVICE)
        with torch.no_grad():
            if hasattr(self.model, 'get_image_features'):
                out = self.model.get_image_features(**inputs)
            else:
                out = self.model.base_model.get_image_features(**inputs)
            if hasattr(out, 'pooler_output'): out = out.pooler_output
            return torch.nn.functional.normalize(out, dim=-1).cpu().numpy().squeeze()

    # --- Core Search & Logic ---
    def search(self, text_query=None, image_input=None, top_k=10):
        weighted_embs = []
        label = text_query
        
        # 1. Process Image
        if image_input:
            if isinstance(image_input, str): 
                label = os.path.basename(image_input) if not label else label
                
                # Resolve path
                img_path = image_input
                if not os.path.exists(img_path) and not img_path.startswith("http"):
                    img_path = os.path.join(Config.IMAGE_BASE_DIR, img_path)
                
                image = Image.open(img_path).convert('RGB')
            else:
                image = image_input

            cropped_img, ai_desc = self.detect_and_describe(image)
            # Image Signal (Weight: 1.0)
            img_emb = self.get_image_embedding(cropped_img)
            weighted_embs.append((img_emb, 1.0))
            
            if ai_desc:
                # AI Description Signal (Weight: 0.5 - purely supportive)
                desc_emb = self.get_text_embedding(ai_desc)
                weighted_embs.append((desc_emb, 0.5))
        
        # 2. Process Text (User Intent - High Weight)
        if text_query:
            eng_text = self.sarvam_translate(text_query)
            text_emb = self.get_text_embedding(eng_text)
            # Text Intent Signal (Weight: 2.0 - Dominant for "in gold" accuracy)
            weighted_embs.append((text_emb, 2.0))
            if not label: label = text_query

        if not weighted_embs: return

        # 3. Weighted Fusion
        final_emb = np.zeros_like(weighted_embs[0][0])
        total_weight = 0
        for emb, weight in weighted_embs:
            final_emb += emb * weight
            total_weight += weight
            
        final_emb = final_emb / total_weight
        final_emb = final_emb / np.linalg.norm(final_emb)
        
        # 4. Search
        res_obj = self.client.query_points(collection_name=Config.COLLECTION_NAME, query=final_emb.tolist(), limit=top_k)
        results = res_obj.points
        
        # 5. Output Management (Matches your test.py structure)
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
            ext = os.path.splitext(src_path)[1]
            dest_filename = f"{rank:02d}_{score}_{prod_id}{ext}"
            dest_path = os.path.join(query_dir, dest_filename)
            
            if os.path.exists(src_path):
                try: shutil.copy2(src_path, dest_path)
                except: pass
            
            search_metadata.append({"rank": rank, "score": float(res.score), "product_id": prod_id, "path": src_path})
            print(f"{rank}. [{score}] ID: {prod_id}")

        self.update_json_results(label, search_metadata)
        print(f"📂 Results saved to: {query_dir}")

    def update_json_results(self, query, metadata):
        json_path = os.path.join(Config.OUTPUT_DIR, "search_results.json")
        all_results = {}
        if os.path.exists(json_path):
            try:
                with open(json_path, 'r') as f: all_results = json.load(f)
            except: pass
        all_results[query] = metadata
        with open(json_path, 'w') as f: json.dump(all_results, f, indent=4)

    def process_url(self, url):
        try:
            resp = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
            soup = BeautifulSoup(resp.content, 'html.parser')
            img_tag = soup.find('meta', property='og:image') or soup.find('img')
            img_url = img_tag.get('content') or img_tag.get('src')
            img_data = requests.get(img_url, stream=True).raw
            return Image.open(img_data).convert('RGB')
        except: return None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", type=str, help="Text search query")
    parser.add_argument("--image", type=str, help="Path to local image")
    args = parser.parse_args()
    
    searcher = UnifiedJewelrySearcher()
    
    if args.query or args.image:
        searcher.search(text_query=args.query, image_input=args.image)
    else:
        print("\n✨ UNIFIED JEWELRY SEARCH ✨")
        while True:
            cmd = input("\nsearch: ").strip()
            if not cmd or cmd.lower() == 'exit': break
            
            # Hybrid
            if "|" in cmd:
                parts = cmd.split("|")
                img_p, txt_q = parts[0].strip(), parts[1].strip()
                searcher.search(text_query=txt_q, image_input=img_p if os.path.exists(img_p) else None)
            # URL
            elif cmd.startswith("http"):
                img = searcher.process_url(cmd)
                if img: searcher.search(image_input=img)
                else: print("❌ URL Error.")
            # Image Path
            elif os.path.exists(cmd) and not os.path.isdir(cmd):
                ext = Path(cmd).suffix.lower()
                if ext in ['.mp3', '.wav', '.m4a']:
                    print("� Transcribing...")
                    text = searcher.sarvam_stt(cmd)
                    if text: searcher.search(text_query=text)
                else:
                    searcher.search(image_input=cmd)
            # Text
            else:
                searcher.search(text_query=cmd)

if __name__ == "__main__":
    main()
