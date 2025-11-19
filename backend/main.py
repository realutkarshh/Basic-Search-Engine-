import os
import math
import re
from collections import defaultdict

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from pymongo import MongoClient
from bson import ObjectId

# ------------------ Config & setup ------------------ #

load_dotenv()  # load .env from project root

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "basic_search_engine")

if not MONGO_URI:
    raise RuntimeError("MONGO_URI is not set in .env")

client = MongoClient(MONGO_URI)
db = client[MONGO_DB_NAME]

DOCS_COLL = db["documents"]
INDEX_COLL = db["index_terms"]

app = FastAPI(
    title="Mini Search Engine API",
    description="Simple TF-IDF based search API",
    version="0.1.0",
)

# Allow your frontend (Vercel) to call the API.
# For now, we allow all origins; later you can lock it to your Vercel URL.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # TODO: change to ["https://your-vercel-domain.vercel.app"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------ Tokenization (same as indexer) ------------------ #

STOPWORDS = {
    "the", "is", "in", "at", "of", "a", "an", "and", "or", "to", "for",
    "on", "with", "by", "this", "that", "it", "as", "are", "was", "were",
    "be", "from", "which", "into", "about", "can", "will", "has", "have",
    "had", "you", "your", "we", "they", "their", "our", "not"
}

TOKEN_RE = re.compile(r"[a-zA-Z0-9]+")


def tokenize(text: str):
    text = text.lower()
    tokens = TOKEN_RE.findall(text)
    tokens = [t for t in tokens if len(t) > 2 and t not in STOPWORDS]
    return tokens


# ------------------ Search logic ------------------ #

def search_query(q: str, limit: int = 20):
    terms = tokenize(q)
    if not terms:
        return []

    index_docs = list(INDEX_COLL.find({"term": {"$in": terms}}))
    if not index_docs:
        return []

    scores = defaultdict(float)

    for term_doc in index_docs:
        term = term_doc["term"]
        idf = term_doc.get("idf", 0.0)
        for posting in term_doc.get("docs", []):
            doc_id = posting["doc_id"]
            tf = posting["tf"]
            score = (1.0 + math.log(tf)) * idf if tf > 0 else 0.0
            scores[doc_id] += score

    if not scores:
        return []

    sorted_docs = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    top_docs = sorted_docs[:limit]

    doc_ids = [doc_id for doc_id, _ in top_docs]
    normalized_ids = [ObjectId(d) if not isinstance(d, ObjectId) else d for d in doc_ids]

    # IMPORTANT: fetch new metadata fields
    docs_cursor = DOCS_COLL.find(
        {"_id": {"$in": normalized_ids}},
        {
            "url": 1,
            "title": 1,
            "snippet": 1,
            "favicon": 1,       # NEW
            "image": 1,         # NEW
            "site_name": 1      # NEW
        }
    )

    docs_by_id = {doc["_id"]: doc for doc in docs_cursor}

    results = []
    for doc_id, score in top_docs:
        meta = docs_by_id.get(doc_id)
        if not meta:
            continue

        results.append({
            "id": str(doc_id),
            "url": meta.get("url", ""),
            "title": meta.get("title", "") or meta.get("url", ""),
            "snippet": meta.get("snippet", ""),

            # NEW fields
            "favicon": meta.get("favicon", ""),
            "image": meta.get("image", ""),
            "site_name": meta.get("site_name", ""),

            "score": score,
        })

    return results



# ------------------ API endpoints ------------------ #

@app.get("/search")
def search(q: str = Query(..., description="Search query"), limit: int = 20):
    """
    Search endpoint.
    Example: GET /search?q=python
    """
    results = search_query(q, limit=limit)
    return {
        "query": q,
        "count": len(results),
        "results": results,
    }


@app.get("/")
def root():
    return {"message": "Mini Search Engine API. Use /search?q=your+query"}
