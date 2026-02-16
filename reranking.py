import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer

class Reranker:
    def __init__(self, model_name='BAAI/bge-reranker-v2-m3', use_fp16=True):
        """
        Initializes the Reranker using standard Hugging Face Transformers.
        This bypasses the 'FlagEmbedding' wrapper to avoid version conflicts.
        """
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.use_fp16 = use_fp16 and self.device == "cuda"
        print(f"🔄 Loading SOTA Reranker: {model_name} on {self.device} (Native HF)...")
        
        try:
            self.tokenizer = AutoTokenizer.from_pretrained(model_name)
            self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
            self.model.eval()
            self.model.to(self.device)
            
            if self.use_fp16:
                self.model.half()
                print("⚡ FP16 Optimization Enabled")
                
            print("✅ Reranker Ready (Transformers Native).")
        except Exception as e:
            print(f"❌ Error loading Reranker: {e}")
            self.model = None

    def compute_score(self, pairs):
        """
        Computes relevance scores for a list of (query, document) pairs.
        """
        if not self.model: return []
        
        # Batch processing to avoid OOM
        batch_size = 16 
        all_scores = []
        
        with torch.no_grad():
            for i in range(0, len(pairs), batch_size):
                batch = pairs[i:i+batch_size]
                # Unzip pairs
                queries, docs = zip(*batch)
                
                inputs = self.tokenizer(
                    list(queries), 
                    list(docs), 
                    padding=True, 
                    truncation=True, 
                    return_tensors='pt', 
                    max_length=512
                ).to(self.device)
                
                output = self.model(**inputs, return_dict=True)
                logits = output.logits.view(-1).float()
                
                # BGE-M3 uses raw logits for ranking, but sigmoid gives 0-1 probability
                # We'll use the raw logits for ranking as it's standard for BGE
                all_scores.extend(logits.cpu().numpy().tolist())
                
        return all_scores

    def rerank(self, query, candidates, top_k=None):
        """
        Reranks candidates.
        """
        if not self.model or not candidates:
            return candidates[:top_k] if top_k else candidates

        # Prepare pairs
        pairs = []
        for c in candidates:
            text = c.get('semantic_description', c.get('text_content', ''))
            pairs.append((query, text))

        # Compute
        scores = self.compute_score(pairs)
        
        # Attach and Sort
        for i, c in enumerate(candidates):
            c['rerank_score'] = scores[i]

        ranked = sorted(candidates, key=lambda x: x['rerank_score'], reverse=True)

        if top_k:
            return ranked[:top_k]
        return ranked
