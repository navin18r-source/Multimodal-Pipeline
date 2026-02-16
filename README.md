# 💎 Multimodal Jewelry Retrieval System

This project is a high-precision search engine for jewelry that understands both images and text to find exactly what you are looking for.

Powered by **Fine-tuned Google SigLIP (LoRA)**, **Gemini Vision**, **Sarvam AI**, and **Qdrant Vector Database**.

## 🚀 Key Features

*   **Multimodal Search**: Query using **Text**, **Image**, **Audio**, or **Hybrid** (Image + Text) inputs.
*   **Intelligent Vision**: Fine-tuned `siglip-so400m` model understands intricate jewelry details (e.g., "Polki", "Temple", "Meenakari").
*   **Two-Stage Reranking**: Retrieves candidates via **Qdrant** and re-ranks top results using **BGE-M3** for maximum relevance.
*   **Smart Material Resolution**: Automatically detects and resolves conflicts between visual cues (e.g., gold appearance) and user intent (e.g., "I want silver").
*   **Multilingual & Voice Support**: Real-time **Hindi/Tamil to English** translation and **Speech-to-Text** via **Sarvam AI**.

## 🛠️ Architecture

1.  **Training (`train-final.py`)**: Fine-tunes SigLIP on jewelry datasets using LoRA.
2.  **Indexing (`index-final.py`)**: Generates embeddings and builds a Qdrant vector index.
3.  **Inference (`test-final.py`)**: The unified search engine handling query processing, retrieval, and reranking.

## 📦 Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Run Inference
```bash
python test-final.py
```
*   **Text Search**: Enter queries like *"Antique gold choker with rubies"*
*   **Image Search**: Provide path to an image file.
*   **Hybrid**: `path/to/image.jpg | "find matching earrings"`

## 🔧 Tech Stack
*   **Core**: PyTorch, Transformers, PEFT (LoRA)
*   **Vector DB**: Qdrant
*   **Models**: SigLIP (Vision), Gemini (V-L), Sarvam (Translation/Audio), BGE-M3 (Reranking)
