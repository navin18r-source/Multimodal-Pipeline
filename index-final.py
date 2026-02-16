import torch
from transformers import AutoModel, AutoProcessor
from peft import PeftModel
from PIL import Image
import pandas as pd
import numpy as np
from tqdm import tqdm
import os
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from torch.utils.data import Dataset, DataLoader

class Config:
    MODEL_NAME = "google/siglip-so400m-patch14-384"
    LORA_WEIGHTS_PATH = "/workspace/models/siglip2-jewelry-lora/best"
    CATALOG_CSV = "/workspace/datasets/jewelry/siglip_training_metadata.csv"
    IMAGE_BASE_DIR = "/workspace/multimodal-dataset/"
    
    COLLECTION_NAME = "jewelry_collection"
    QDRANT_PATH = "/workspace/qdrant_db_rerank"
    VECTOR_SIZE = 1152
    
    BATCH_SIZE = 16
    DEVICE = "cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu"

class ImageDataset(Dataset):
    def __init__(self, df, processor):
        self.df = df
        self.processor = processor
        
    def __len__(self):
        return len(self.df)
        
    def __getitem__(self, idx):
        row = self.df.iloc[idx]
        try:
            image_path = str(row['image_path'])
            if not image_path.startswith("/") and not image_path.startswith("http"):
                image_path = os.path.join(Config.IMAGE_BASE_DIR, image_path)
            
            img = Image.open(image_path).convert('RGB')
            inputs = self.processor(images=img, return_tensors="pt")
            return {
                'pixel_values': inputs['pixel_values'].squeeze(0),
                'product_id': str(row.get('product_id', idx)),
                'path': image_path,
                'description': str(row.get('description', row.get('caption', '')))
            }
        except Exception:
            return None

def main():
    processor = AutoProcessor.from_pretrained(Config.MODEL_NAME)
    base_model = AutoModel.from_pretrained(Config.MODEL_NAME).to(Config.DEVICE)
    if os.path.exists(Config.LORA_WEIGHTS_PATH):
        model = PeftModel.from_pretrained(base_model, Config.LORA_WEIGHTS_PATH).to(Config.DEVICE)
    else:
        model = base_model
    model.eval()
    df = pd.read_csv(Config.CATALOG_CSV)
    dataset = ImageDataset(df, processor)
    dataloader = DataLoader(dataset, batch_size=Config.BATCH_SIZE, shuffle=False, num_workers=4, collate_fn=lambda x: [y for y in x if y is not None])
    
    client = QdrantClient(path=Config.QDRANT_PATH)
    client.recreate_collection(collection_name=Config.COLLECTION_NAME, vectors_config=VectorParams(size=Config.VECTOR_SIZE, distance=Distance.COSINE))
    
    point_id = 0
    for batch in tqdm(dataloader):
        if not batch: continue
        pixel_values = torch.stack([b['pixel_values'] for b in batch]).to(Config.DEVICE)
        
        with torch.no_grad():
            if hasattr(model, 'base_model'): 
                v_out = model.base_model.vision_model(pixel_values=pixel_values)
            else: 
                v_out = model.vision_model(pixel_values=pixel_values)
                
            embs = torch.nn.functional.normalize(v_out.pooler_output, dim=-1).detach().cpu().numpy()
            
        batch_points = []
        for i, b in enumerate(batch):
            batch_points.append(PointStruct(
                id=point_id, 
                vector=embs[i].tolist(), 
                payload={
                    "product_id": b['product_id'], 
                    "path": b['path'],
                    "semantic_description": b['description']
                }
            ))
            point_id += 1
        client.upsert(collection_name=Config.COLLECTION_NAME, points=batch_points)

if __name__ == "__main__":
    main()
