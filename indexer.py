import os
import re
import math
from collections import defaultdict, Counter

from dotenv import load_dotenv
from pymongo import MongoClient


# ------------------ Config & setup ------------------ #

load_dotenv()  # loads .env from current directory

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "basic_search_engine")

if not MONGO_URI:
    raise RuntimeError("MONGO_URI is not set in .env")

client = MongoClient(MONGO_URI)
db = client[MONGO_DB_NAME]

PAGES_COLL = db["pages"]
DOCS_COLL = db["documents"]
INDEX_COLL = db["index_terms"]


# ------------------ Tokenization ------------------ #

# Simple English stopword list (can tweak later)
STOPWORDS = {
    "the", "is", "in", "at", "of", "a", "an", "and", "or", "to", "for",
    "on", "with", "by", "this", "that", "it", "as", "are", "was", "were",
    "be", "from", "which", "into", "about", "can", "will", "has", "have",
    "had", "you", "your", "we", "they", "their", "our", "not", "how"
}

TOKEN_RE = re.compile(r"[a-zA-Z0-9]+")

def tokenize(text: str):
    """
    Convert text -> list of normalized tokens.
    Lowercase, keep only alphanumeric sequences, remove stopwords & very short tokens.
    """
    text = text.lower()
    tokens = TOKEN_RE.findall(text)
    tokens = [t for t in tokens if len(t) > 2 and t not in STOPWORDS]
    return tokens


# ------------------ Index building ------------------ #

def build_index():
    print("Fetching pages from MongoDB...")
    pages_cursor = PAGES_COLL.find({}, {"url": 1, "title": 1, "text": 1})
    pages = list(pages_cursor)

    if not pages:
        print("No pages found in 'pages' collection. Run the crawler first.")
        return

    print(f"Found {len(pages)} pages. Building index...")

    # In-memory data structures
    # inverted_index: term -> dict(doc_id -> tf)
    inverted_index = defaultdict(lambda: defaultdict(int))
    # doc_lengths: doc_id -> doc length (number of tokens)
    doc_lengths = {}
    # doc_metadata: doc_id -> dict(url, title, snippet)
    doc_metadata = {}

    for page in pages:
        doc_id = page["_id"]
        url = page.get("url", "")
        title = page.get("title", "") or url
        text = page.get("text", "") or ""

        tokens = tokenize(text)
        if not tokens:
            continue

        doc_lengths[doc_id] = len(tokens)
        doc_metadata[doc_id] = {
            "url": url,
            "title": title,
            "snippet": text[:300]  # first 300 chars as snippet
        }

        # term frequencies for this doc
        tf_counter = Counter(tokens)
        for term, tf in tf_counter.items():
            inverted_index[term][doc_id] += tf

    num_docs = len(doc_lengths)
    print(f"Indexed {num_docs} documents with tokens.")

    if num_docs == 0:
        print("No documents contained tokens. Aborting index build.")
        return

    # Compute IDF and prepare Mongo documents
    index_docs = []
    for term, postings in inverted_index.items():
        df = len(postings)  # document frequency
        idf = math.log(num_docs / (1 + df))  # simple IDF (natural log)

        term_entry = {
            "term": term,
            "idf": float(idf),
            "docs": [
                {"doc_id": doc_id, "tf": int(tf)}
                for doc_id, tf in postings.items()
            ],
        }
        index_docs.append(term_entry)

    print(f"Built index for {len(index_docs)} unique terms.")

    # ------------------ Store in MongoDB ------------------ #

    print("Dropping old 'documents' and 'index_terms' collections (if they exist)...")
    DOCS_COLL.drop()
    INDEX_COLL.drop()

    print("Inserting documents metadata...")
    docs_bulk = []
    for doc_id, meta in doc_metadata.items():
        docs_bulk.append({
            "_id": doc_id,  # reuse same ObjectId as in pages
            "url": meta["url"],
            "title": meta["title"],
            "length": doc_lengths[doc_id],
            "snippet": meta["snippet"],
        })

    if docs_bulk:
        DOCS_COLL.insert_many(docs_bulk)
    print(f"Inserted {len(docs_bulk)} documents into 'documents' collection.")

    print("Inserting index terms (this may take a moment)...")
    # Insert in batches to avoid huge single insert
    batch_size = 1000
    for i in range(0, len(index_docs), batch_size):
        batch = index_docs[i:i + batch_size]
        INDEX_COLL.insert_many(batch)
        print(f"Inserted {i + len(batch)} / {len(index_docs)} index terms...")

    print("Index build complete ✅")


if __name__ == "__main__":
    build_index()
