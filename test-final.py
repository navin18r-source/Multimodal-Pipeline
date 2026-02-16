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
import sys
from langdetect import detect, LangDetectException
from difflib import get_close_matches
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- ENVIRONMENT FIX: Monkey-patch for older transformers versions ---
try:
    import transformers.utils.import_utils
    if not hasattr(transformers.utils.import_utils, "is_torch_fx_available"):
        transformers.utils.import_utils.is_torch_fx_available = lambda: False
    
    # Fix for XLMRobertaTokenizer (common in BGE-M3)
    from transformers.models.xlm_roberta.tokenization_xlm_roberta import XLMRobertaTokenizer
    if not hasattr(XLMRobertaTokenizer, "prepare_for_model"):
        # Alias to the older method name or providing a pass-through
        def prepare_for_model_shim(self, *args, **kwargs):
            return self.prepare_for_tokenization(*args, **kwargs)
        XLMRobertaTokenizer.prepare_for_model = prepare_for_model_shim
except ImportError:
    pass

# Ensure the script's directory is in sys.path so it can find reranking.py
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from reranking import Reranker
except ImportError as e:
    print(f"\n⚠️  CRITICAL: Could not import 'reranking.py' or 'FlagEmbedding' library.")
    print(f"👉 ERROR: {e}")
    print(f"👉 FIX 1: Ensure 'reranking.py' is in the SAME folder as this script ({os.path.basename(__file__)}).")
    print(f"👉 FIX 2: Run 'python3 -m pip install FlagEmbedding'.")
    print("🔻 RERANKING IS CURRENTLY DISABLED (Using Dummy fallback)\n")
    
    class Reranker: 
        def __init__(self, model_name=None): pass
        def rerank(self, query, candidates, top_k=None): return candidates[:top_k] if top_k else candidates

class Config:
    MODEL_NAME = "google/siglip-so400m-patch14-384"
    LORA_WEIGHTS_PATH = "/workspace/models/siglip2-jewelry-lora/best"
    COLLECTION_NAME = "jewelry_collection"
    QDRANT_PATH = "/workspace/qdrant_db_rerank"
    DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
    SARVAM_API_KEY = os.getenv("SARVAM_API_KEY")
    IMAGE_BASE_DIR = "/workspace/multimodal-dataset/"
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
    GEMINI_MODEL = "gemini-2.0-flash-exp"
    OUTPUT_DIR = "/workspace/search_results"
    
    # Vocabulary for English Typos (Add more as needed)
    JEWELRY_VOCAB = [
        "kundan", "polki", "meenakari", "jhumka", "temple", "antique", "gold", "silver",
        "diamond", "ruby", "emerald", "necklace", "earrings", "bangles", "bracelet",
        "pendant", "choker", "mangalsutra", "maangtikka", "nosepin", "ring", "studs"
    ]

class LanguageHandler:
    def __init__(self):
        pass

    def correct_english_typos(self, text):
        """Simple heuristic spell checker for known jewelry terms."""
        words = text.split()
        corrected_words = []
        for word in words:
            # Check if word is close to any key term (allow 1-2 char diff)
            matches = get_close_matches(word.lower(), Config.JEWELRY_VOCAB, n=1, cutoff=0.8)
            if matches:
                # Preservation Logic: If it matched 'kundan', use 'Kundan' (title case for safety)
                # But keep original casing if it wasn't a typo match
                corrected_words.append(matches[0].title() if word.lower() != matches[0] else word)
            else:
                corrected_words.append(word)
        return " ".join(corrected_words)

    def process_query(self, text, sarvam_fn):
        # 1. Check for known jewelry terms first (Force English path for things like 'dimond')
        words = text.lower().split()
        force_english = any(get_close_matches(w, Config.JEWELRY_VOCAB, n=1, cutoff=0.7) for w in words)
        
        if force_english or len(text) < 5:
            lang = "en"
        else:
            try:
                lang = detect(text)
            except LangDetectException:
                lang = "en"

        if lang == "en":
            # Path B: English -> Normalize
            normalized = self.correct_english_typos(text)
            print(f"✨ English Normalizer: '{text}' -> '{normalized}'")
            return normalized
        else:
            # Path A: Non-English (Hindi/Tamil/etc) -> Sarvam Translate
            print(f"🌍 Detected Language: {lang}")
            print(f"🔄 Routing to Sarvam AI (Target: en-IN)...")
            return sarvam_fn(text)

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
        self.lang_handler = LanguageHandler()
        
        # Initialize Reranker
        self.reranker = Reranker() # Uses default BGE model
        
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
        # Target en-IN preserves "Kundan", "Jhumka" etc.
        payload = {"input": text, "source_language_code": "auto", "target_language_code": "en-IN", "speaker_gender": "Female", "mode": "formal"}
        try:
            response = requests.post(url, headers=headers, json=payload)
            return response.json().get("translated_text", text)
        except: return text

    def sarvam_stt(self, audio_path):
        url = "https://api.sarvam.ai/speech-to-text-translate"
        headers = {"api-subscription-key": Config.SARVAM_API_KEY}
        
        # Explicit MIME types to fix "Invalid file type: None" error
        ext = os.path.splitext(audio_path)[1].lower()
        mime_map = {
            '.mp3': 'audio/mpeg',
            '.wav': 'audio/wav',
            '.m4a': 'audio/x-m4a',
            '.ogg': 'audio/ogg'
        }
        mime_type = mime_map.get(ext, 'application/octet-stream')

        try:
            with open(audio_path, 'rb') as f:
                print(f"📤 Sending audio to Sarvam: {os.path.basename(audio_path)} ({mime_type})...")
                # Requests needs (filename, file_object, content_type) for explicit mime
                files = {'file': (os.path.basename(audio_path), f, mime_type)}
                response = requests.post(url, headers=headers, files=files)
                
                if response.status_code != 200:
                    print(f"❌ Sarvam API Error ({response.status_code}): {response.text}")
                    return None
                    
                transcript = response.json().get("transcript", "")
                if not transcript:
                    print(f"⚠️ Sarvam returned empty transcript. Raw: {response.text}")
                else:
                    print(f"✅ Transcript: {transcript}")
                return transcript
        except Exception as e:
            print(f"❌ STT Exception: {e}")
            return None

    def detect_and_describe(self, image):
        if not self.gemini: return image, None
        prompt = "Locate jewelry. Return ONLY JSON: {\"bbox\": [y_min, x_min, y_max, x_max], \"description\": \"...\"}"
        try:
            response = self.gemini.generate_content([prompt, image])
            res = json.loads(re.search(r'\{.*\}', response.text, re.DOTEXT).group())
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
            if hasattr(self.model, 'base_model'):
                out = self.model.base_model.text_model(**inputs)
            else:
                out = self.model.text_model(**inputs)
            return torch.nn.functional.normalize(out.pooler_output, dim=-1).cpu().numpy().squeeze()

    def get_image_embedding(self, image):
        inputs = self.processor(images=image, return_tensors="pt").to(Config.DEVICE)
        with torch.no_grad():
            if hasattr(self.model, 'base_model'):
                out = self.model.base_model.vision_model(**inputs)
            else:
                out = self.model.vision_model(**inputs)
            return torch.nn.functional.normalize(out.pooler_output, dim=-1).cpu().numpy().squeeze()

    # --- Helper for Saving Images ---
    def save_results_to_folder(self, results, folder_path, prefix="score", force_initial=False):
        if os.path.exists(folder_path): shutil.rmtree(folder_path)
        os.makedirs(folder_path, exist_ok=True)
        
        saved_items = []
        for i, res in enumerate(results):
            rank = i + 1
            # Fix: Ensure embedding_only folder uses the real vector distance
            if force_initial:
                score = res.get('initial_score', 0)
            else:
                score = res.get('rerank_score', res.get('initial_score', 0))
                
            prod_id = res['product_id']
            src_path = res['path']
            
            # Create filename: rank_score_id.ext
            ext = os.path.splitext(src_path)[1]
            dest_filename = f"{rank:02d}_{prefix}_{score:.4f}_{prod_id}{ext}"
            dest_path = os.path.join(folder_path, dest_filename)
            
            if os.path.exists(src_path):
                try: shutil.copy2(src_path, dest_path)
                except: pass
            
            saved_items.append({
                "rank": rank,
                "score": float(score),
                "product_id": prod_id,
                "path": src_path
            })
        return saved_items

    # --- Core Search & Logic ---
    def search(self, text_query=None, image_input=None, embedding_top_k=50, rerank_top_k=20):
        weighted_embs = []
        label = text_query if text_query else "image_search"
        final_query_text = ""
        
        # 1. Process Image
        ai_materials = []
        user_materials = []
        
        if image_input:
            if isinstance(image_input, str): 
                label = os.path.basename(image_input) if not text_query else text_query
                img_path = image_input
                if not os.path.exists(img_path) and not img_path.startswith("http"):
                    img_path = os.path.join(Config.IMAGE_BASE_DIR, img_path)
                image = Image.open(img_path).convert('RGB')
            else:
                image = image_input

            cropped_img, ai_desc = self.detect_and_describe(image)
            img_emb = self.get_image_embedding(cropped_img)
            weighted_embs.append((img_emb, 1.0)) # Visual Basis
            
            if ai_desc:
                norm_desc = self.lang_handler.correct_english_typos(ai_desc)
                # Extract materials Gemini found
                ai_materials = [m for m in ["gold", "silver", "platinum", "diamond"] if m in norm_desc.lower()]
                desc_emb = self.get_text_embedding(norm_desc)
                # We'll decide the weight later based on conflict
                ai_desc_emb_data = (desc_emb, 0.5) 
        
        # 2. Process Text
        if text_query:
            processed_text = self.lang_handler.process_query(text_query, self.sarvam_translate)
            final_query_text = processed_text 
            text_emb = self.get_text_embedding(processed_text)
            
            # Extract materials user explicitly wants
            user_materials = [m for m in ["gold", "silver", "platinum", "diamond"] if m in processed_text.lower()]
            
            # Boost Text Weight (3.0) to ensure it overrides visuals
            text_weight = 3.0 if image_input else 2.0
            weighted_embs.append((text_emb, text_weight))

        # 3. Handle Material Conflict (Gold vs Silver bypass)
        if image_input and ai_desc and user_materials:
            # Check if user material contradicts AI material (e.g. User says 'Silver', AI said 'Gold')
            has_conflict = any(um in ["silver", "gold", "platinum"] and any(am != um for am in ai_materials if am in ["silver", "gold", "platinum"]) for um in user_materials)
            
            if has_conflict:
                print(f"⚠️  Conflict Detected: User wants {user_materials} but Image shows {ai_materials}. Suppressing AI description.")
                # We don't add ai_desc_emb_data or we add it with 0 weight
            else:
                weighted_embs.append(ai_desc_emb_data)
        elif image_input and ai_desc:
            weighted_embs.append(ai_desc_emb_data)

        if not weighted_embs: return

        # 4. Weighted Fusion
        final_emb = np.zeros_like(weighted_embs[0][0])
        total_weight = 0
        for emb, weight in weighted_embs:
            final_emb += emb * weight
            total_weight += weight
        final_emb = (final_emb / total_weight) / np.linalg.norm(final_emb / total_weight)
        
        # 5. Search (Retrieval)
        print(f"🔍 Vector Search: Fetching {embedding_top_k} candidates...")
        try:
            res_obj = self.client.query_points(collection_name=Config.COLLECTION_NAME, query=final_emb.tolist(), limit=embedding_top_k)
        except Exception as e:
            print(f"\n❌ Error: Qdrant Collection '{Config.COLLECTION_NAME}' not found. Run index.py first.")
            return

        embedding_results = []
        for res in res_obj.points:
            embedding_results.append({
                "product_id": res.payload['product_id'],
                "path": res.payload['path'],
                "semantic_description": res.payload.get('semantic_description', ''),
                "initial_score": res.score
            })
            
        # 5. Reranking
        rerank_query = final_query_text if final_query_text else (ai_desc if image_input and ai_desc else "")
        reranked_results = []
        
        if embedding_results and rerank_query:
            print(f"🧠 Reranking all {len(embedding_results)} candidates using BGE-M3...")
            reranked_results = self.reranker.rerank(rerank_query, embedding_results, top_k=rerank_top_k)
        else:
            reranked_results = embedding_results[:rerank_top_k]

        # 6. Output Management (Save to folders)
        safe_query = re.sub(r'[^\w\s-]', '', label).strip().replace(' ', '_').lower()
        root_dir = os.path.join(Config.OUTPUT_DIR, safe_query)
        
        # Folder Paths
        emb_folder = os.path.join(root_dir, "embedding_only")
        rank_folder = os.path.join(root_dir, "reranked")
        
        # Save Results
        # Flag: force_initial=True ensures embedding_only folder shows the actual Vector similarity
        emb_metadata = self.save_results_to_folder(embedding_results, emb_folder, prefix="dist", force_initial=True)
        # Reranked folder shows the high-precision BGE scores
        rank_metadata = self.save_results_to_folder(reranked_results, rank_folder, prefix="score", force_initial=False)
        
        # Save Unified Metadata JSON
        self.update_json_results(label, {
            "embedding_only": emb_metadata,
            "reranked": rank_metadata
        })
        
        print(f"\n✅ Search complete. View results in: {root_dir}")
        print(f"\n🏆 TOP 10 RERANKED RESULTS:")
        for res in rank_metadata[:10]:
            print(f"   {res['rank']}. [Score: {res['score']:.4f}] ID: {res['product_id']}")
        
        print(f"\n📂 Saved: {len(emb_metadata)} in /embedding_only, {len(rank_metadata)} in /reranked")

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
                    print("🎤 Transcribing...")
                    text = searcher.sarvam_stt(cmd)
                    if text: searcher.search(text_query=text)
                else:
                    searcher.search(image_input=cmd)
            # Text
            else:
                searcher.search(text_query=cmd)

if __name__ == "__main__":
    main()
