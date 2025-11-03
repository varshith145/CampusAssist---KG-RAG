from __future__ import annotations

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))


import pandas as pd
import chromadb
from pathlib import Path
from tqdm import tqdm
from rag_utils import embed_texts

DB_DIR = Path(__file__).parent / "store"
COLL_NAME = "events"
CSV_PATH = Path(__file__).resolve().parents[1] / "data" / "events_normalized.csv"


def _rows_to_docs(df: pd.DataFrame) -> list[str]:
    if "canon_text" in df.columns:
        return df["canon_text"].fillna("").astype(str).tolist()

    def row_to_line(r):
        term = str(r.get("Term", "")).strip()
        ev = str(r.get("event", "")).strip()
        sd = str(r.get("start_date", "")).strip()
        ed = str(r.get("end_date", "")).strip()
        src = str(r.get("source", "")).strip()
        rng = f"{sd} to {ed}" if sd and ed and sd != ed else sd
        return f"{term} — {ev} — {rng} — source: {src}".strip()

    return [row_to_line(r) for _, r in df.iterrows()]


def main():
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV not found: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH)
    docs = _rows_to_docs(df)
    ids = [f"row-{i}" for i in range(len(docs))]
    metas = [{"row": i} for i in range(len(docs))]

    client = chromadb.PersistentClient(path=str(DB_DIR))
    try:
        client.delete_collection(name=COLL_NAME)
    except Exception:
        pass
    coll = client.create_collection(name=COLL_NAME)

    BATCH = 64
    for i in tqdm(range(0, len(docs), BATCH), desc="Embedding"):
        batch_docs = docs[i : i + BATCH]
        batch_ids = ids[i : i + BATCH]
        batch_meta = metas[i : i + BATCH]
        vecs = embed_texts(batch_docs)
        coll.add(ids=batch_ids, documents=batch_docs, metadatas=batch_meta, embeddings=vecs)

    print(f"✅ Ingested {len(docs)} rows into Chroma at {DB_DIR}")


if __name__ == "__main__":
    main()
