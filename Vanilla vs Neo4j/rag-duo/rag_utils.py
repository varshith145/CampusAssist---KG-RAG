from __future__ import annotations

import json
import requests
from typing import List
from sentence_transformers import SentenceTransformer

# Embeddings (CPU)
_ST_MODEL = SentenceTransformer("all-MiniLM-L6-v2")  # 384-dim


def embed_texts(texts: List[str]) -> List[List[float]]:
    if isinstance(texts, str):
        texts = [texts]
    vecs = _ST_MODEL.encode(list(texts), convert_to_numpy=True, normalize_embeddings=False)
    return [v.tolist() for v in vecs]


# Generation via Ollama
OLLAMA_HOST = "http://localhost:11434"


def generate(prompt: str, model: str = "llama3.2:3b", temperature: float = 0.2, timeout: int = 600) -> str:
    url = f"{OLLAMA_HOST}/api/generate"
    payload = {
        "model": model,
        "prompt": prompt,
        "options": {"temperature": temperature},
    }
    with requests.post(url, json=payload, stream=True, timeout=timeout) as resp:
        resp.raise_for_status()
        out_chunks: List[str] = []
        for line in resp.iter_lines(decode_unicode=True):
            if not line:
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue
            chunk = msg.get("response")
            if chunk:
                out_chunks.append(chunk)
            if msg.get("done"):
                break
        return "".join(out_chunks)
