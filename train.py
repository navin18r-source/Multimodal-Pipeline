import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import Dataset, DataLoader
from transformers import AutoModel, AutoProcessor, AutoTokenizer
from peft import LoraConfig, get_peft_model, TaskType
from PIL import Image
import pandas as pd
from tqdm import tqdm
import os
import math
import gc
from sklearn.model_selection import train_test_split

class Config:
    MODEL_NAME = "google/siglip-so400m-patch14-384"
    TRAINING_DATA_CSV = "/workspace/datasets/jewelry/train.csv"
    IMAGE_BASE_DIR = "/workspace/multimodal-dataset/"
    OUTPUT_DIR = "/workspace/models/siglip2-jewelry-lora"
    
    LORA_R = 16
    LORA_ALPHA = 32
    LORA_DROPOUT = 0.1
    LORA_TARGET_MODULES = ["q_proj", "k_proj", "v_proj", "out_proj"]
    
    EPOCHS = 5
    BATCH_SIZE = 32
    GRADIENT_ACCUMULATION_STEPS = 4
    
    LEARNING_RATE = 1e-4
    LR_TEMPERATURE = 2e-5
    WEIGHT_DECAY = 0.01
    WARMUP_RATIO = 0.1
    MAX_TEXT_LENGTH = 64
    MIXED_PRECISION = False
    VAL_SPLIT = 0.05

class JewelryDataset(Dataset):
    def __init__(self, df, processor, tokenizer):
        self.df = df
        self.processor = processor
        self.tokenizer = tokenizer
        
    def __len__(self):
        return len(self.df)
    
    def _get_path(self, path):
        clean_path = path
        prefixes = ["/workspace/multimodal-dataset/", "multimodal-dataset/", "/workspace/", "/multimodal-dataset/"]
        for pfx in prefixes:
            if clean_path.startswith(pfx):
                clean_path = clean_path[len(pfx):]
                break
        return os.path.join(Config.IMAGE_BASE_DIR, clean_path)

    def __getitem__(self, idx):
        row = self.df.iloc[idx]
        try:
            full_path = self._get_path(row['image_path'])
            image = Image.open(full_path).convert('RGB')
            caption = str(row['description'])
            if not caption or caption == 'nan': return None
            image_inputs = self.processor(images=image, return_tensors="pt")
            text_inputs = self.tokenizer(text=caption, padding="max_length", truncation=True, max_length=Config.MAX_TEXT_LENGTH, return_tensors="pt")
            return {
                'pixel_values': image_inputs['pixel_values'].squeeze(0),
                'input_ids': text_inputs['input_ids'].squeeze(0),
                'attention_mask': text_inputs['attention_mask'].squeeze(0),
            }
        except Exception:
            return None
    
    @staticmethod
    def collate_fn(batch):
        batch = [b for b in batch if b is not None]
        if len(batch) == 0: return None
        return {
            'pixel_values': torch.stack([b['pixel_values'] for b in batch]),
            'input_ids': torch.stack([b['input_ids'] for b in batch]),
            'attention_mask': torch.stack([b['attention_mask'] for b in batch]),
        }

class SigLIPLoss(nn.Module):
    def __init__(self):
        super().__init__()
        self.temperature = nn.Parameter(torch.tensor(3.0))
        self.bias = nn.Parameter(torch.tensor(-10.0))
        
    def forward(self, img_emb, txt_emb):
        img_emb = F.normalize(img_emb, dim=-1)
        txt_emb = F.normalize(txt_emb, dim=-1)
        logits = (img_emb @ txt_emb.T) * self.temperature.exp() + self.bias
        labels = 2 * torch.eye(img_emb.shape[0], device=logits.device) - 1
        return -F.logsigmoid(labels * logits).mean()

class Trainer:
    def __init__(self):
        self.device = "cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu"
        self.processor = AutoProcessor.from_pretrained(Config.MODEL_NAME)
        self.tokenizer = AutoTokenizer.from_pretrained(Config.MODEL_NAME)
        base = AutoModel.from_pretrained(Config.MODEL_NAME)
        lora_config = LoraConfig(r=Config.LORA_R, lora_alpha=Config.LORA_ALPHA, target_modules=Config.LORA_TARGET_MODULES, lora_dropout=Config.LORA_DROPOUT, task_type=TaskType.FEATURE_EXTRACTION)
        self.model = get_peft_model(base, lora_config).to(self.device)
        self.loss_fn = SigLIPLoss().to(self.device)
        self.optimizer = torch.optim.AdamW([
            {'params': [p for p in self.model.parameters() if p.requires_grad], 'lr': Config.LEARNING_RATE},
            {'params': list(self.loss_fn.parameters()), 'lr': Config.LR_TEMPERATURE},
        ], weight_decay=Config.WEIGHT_DECAY)
        self.scaler = torch.amp.GradScaler(self.device) if Config.MIXED_PRECISION and self.device == "cuda" else None

    def train(self, train_loader, val_loader):
        os.makedirs(Config.OUTPUT_DIR, exist_ok=True)
        total_steps = (len(train_loader) // Config.GRADIENT_ACCUMULATION_STEPS) * Config.EPOCHS
        scheduler = torch.optim.lr_scheduler.OneCycleLR(self.optimizer, max_lr=[Config.LEARNING_RATE, Config.LR_TEMPERATURE], total_steps=total_steps, pct_start=Config.WARMUP_RATIO, anneal_strategy='cos')
        best_loss = float('inf')
        for epoch in range(Config.EPOCHS):
            self.model.train()
            pbar = tqdm(train_loader, desc=f"Epoch {epoch+1}")
            for idx, batch in enumerate(pbar):
                if batch is None: continue
                pixel_values = batch['pixel_values'].to(self.device)
                input_ids = batch['input_ids'].to(self.device)
                attention_mask = batch['attention_mask'].to(self.device)
                with torch.amp.autocast(self.device) if self.scaler else torch.inference_mode(False):
                    v_out = self.model.base_model.vision_model(pixel_values=pixel_values)
                    t_out = self.model.base_model.text_model(input_ids=input_ids, attention_mask=attention_mask)
                    loss = self.loss_fn(v_out.pooler_output, t_out.pooler_output)
                scaled_loss = loss / Config.GRADIENT_ACCUMULATION_STEPS
                if self.scaler: self.scaler.scale(scaled_loss).backward()
                else: scaled_loss.backward()
                if (idx + 1) % Config.GRADIENT_ACCUMULATION_STEPS == 0:
                    if self.scaler:
                        self.scaler.unscale_(self.optimizer)
                        torch.nn.utils.clip_grad_norm_(self.model.parameters(), 1.0)
                        self.scaler.step(self.optimizer)
                        self.scaler.update()
                    else:
                        torch.nn.utils.clip_grad_norm_(self.model.parameters(), 1.0)
                        self.optimizer.step()
                    scheduler.step()
                    self.optimizer.zero_grad()
                pbar.set_postfix({'loss': f"{loss.item():.4f}"})
            val_loss = self.validate(val_loader)
            if val_loss < best_loss:
                best_loss = val_loss
                self.save(os.path.join(Config.OUTPUT_DIR, "best"))
            self.save(os.path.join(Config.OUTPUT_DIR, f"epoch_{epoch+1}"))

    @torch.no_grad()
    def validate(self, loader):
        self.model.eval()
        total = 0
        for batch in loader:
            if batch is None: continue
            v_out = self.model.base_model.vision_model(pixel_values=batch['pixel_values'].to(self.device))
            t_out = self.model.base_model.text_model(input_ids=batch['input_ids'].to(self.device), attention_mask=batch['attention_mask'].to(self.device))
            total += self.loss_fn(v_out.pooler_output, t_out.pooler_output).item()
        return total / len(loader)

    def save(self, path):
        os.makedirs(path, exist_ok=True)
        self.model.save_pretrained(path)
        self.processor.save_pretrained(path)
        torch.save({'temp': self.loss_fn.temperature, 'bias': self.loss_fn.bias}, os.path.join(path, "loss.pt"))

def main():
    df = pd.read_csv(Config.TRAINING_DATA_CSV)
    train_df, val_df = train_test_split(df, test_size=Config.VAL_SPLIT, random_state=42)
    trainer = Trainer()
    train_ds = JewelryDataset(train_df, trainer.processor, trainer.tokenizer)
    val_ds = JewelryDataset(val_df, trainer.processor, trainer.tokenizer)
    train_loader = DataLoader(train_ds, batch_size=Config.BATCH_SIZE, shuffle=True, collate_fn=JewelryDataset.collate_fn, num_workers=4, pin_memory=True)
    val_loader = DataLoader(val_ds, batch_size=Config.BATCH_SIZE, shuffle=False, collate_fn=JewelryDataset.collate_fn, num_workers=4)
    trainer.train(train_loader, val_loader)

if __name__ == "__main__":
    main()
