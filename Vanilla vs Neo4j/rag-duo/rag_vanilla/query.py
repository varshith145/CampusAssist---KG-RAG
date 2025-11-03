# rag_vanilla/query.py
from __future__ import annotations

# --- make sure we can import rag_utils from project root ---
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))

import re
from datetime import datetime

import chromadb
from rag_utils import embed_texts, generate

DB_DIR = Path(__file__).parent / "store"
COLL_NAME = "events"

TEMPLATE = """You are a precise assistant. Answer ONLY using the context.
If the answer is not in the context, say "Not found in context."

Task: Answer the user's question as a single, direct sentence. If the question asks for a date, return just the date in YYYY-MM-DD.

Question:
{q}

Context:
{ctx}

Answer:"""


# -------- Utility: handle weekday/date questions deterministically --------
def utility_answer(q: str) -> str | None:
    """
    Handles small 'tool' questions outside the CSV, like:
      - 'which day is 2025-12-04?'
      - 'is 2025-12-04 monday?'
      - 'what day is 2025-12-04'
    Returns a string if handled, else None.
    """
    s = q.strip().lower()

    # Match YYYY-MM-DD (strict ISO)
    m = re.search(r'(\d{4})-(\d{2})-(\d{2})', s)
    if not m:
        return None

    y, mm, dd = map(int, m.groups())
    try:
        d = datetime(y, mm, dd)
    except ValueError:
        return "That date is invalid."

    weekday = d.strftime('%A')  # e.g., 'Thursday'

    # If user asked a yes/no like "is it monday?"
    yn = re.search(
        r'\bis it\s+(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b',
        s
    )
    if yn:
        asked = yn.group(1).title()
        verdict = "Yes" if asked == weekday else "No"
        return f"{verdict}. {d.date().isoformat()} is a {weekday}."

    # If user asked open-ended (which/what day)
    if re.search(r'\b(which|what)\s+day\b', s) or s.endswith('?') or True:
        # Default concise answer
        return f"{d.date().isoformat()} is a {weekday}."

    return None


# ---------------- Retrieval + Generation (Vanilla RAG) ----------------
def retrieve(q: str, k: int = 5):
    client = chromadb.PersistentClient(path=str(DB_DIR))
    coll = client.get_collection(name=COLL_NAME)
    qv = embed_texts([q])[0]
    res = coll.query(
        query_embeddings=[qv],
        n_results=k,
        include=["documents", "metadatas", "distances"],
    )
    docs = res["documents"][0]
    metas = res["metadatas"][0]
    dists = res["distances"][0]
    return list(zip(docs, metas, dists))


def main():
    # Read question from CLI
    q = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else input("Ask: ").strip()
    if not q:
        print("Please provide a question.")
        return

    # 1) Utility short-circuit (handles weekday/date questions)
    u = utility_answer(q)
    if u:
        print(u)
        return

    # 2) Vanilla RAG flow
    hits = retrieve(q, k=5)
    ctx = "\n\n---\n\n".join(h[0] for h in hits)
    prompt = TEMPLATE.format(q=q, ctx=ctx)

    answer = generate(prompt, model="llama3.2:3b", temperature=0.2)
    print(answer)


if __name__ == "__main__":
    main()
