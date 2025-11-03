# RAG Duo (Vanilla vs Neo4j)

Two tiny command-line RAG pipelines to compare:
- **Vanilla RAG**: Chroma vector store (local)
- **Neo4j RAG**: vector index in Neo4j + light graph context

## Data
- Use `data/events_normalized.csv` (already normalized) as the initial corpus.
- Optional: put extra `.txt/.md` files under `data/corpus/`.

## Setup (quick)
```bash
python -m venv .venv
# Windows: .\.venv\Scripts\Activate
# macOS/Linux: source .venv/bin/activate

pip install --upgrade pip
pip install -r requirements.txt
