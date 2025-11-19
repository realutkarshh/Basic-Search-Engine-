import os
import re
import math
from collections import defaultdict, Counter

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError

# ------------------ Config & setup ------------------ #

load_dotenv()

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

STOPWORDS = {
    "the", "is", "in", "at", "of", "a", "an", "and", "or", "to", "for",
    "on", "with", "by", "this", "that", "it", "as", "are", "was", "were",
    "be", "from", "which", "into", "about", "can", "will", "has", "have",
    "had", "you", "your", "we", "they", "their", "our", "not", "how"
}

TOKEN_RE = re.compile(r"[a-zA-Z0-9]+")

def tokenize(text: str):
    text = (text or "").lower()
    tokens = TOKEN_RE.findall(text)
    tokens = [t for t in tokens if len(t) > 2 and t not in STOPWORDS]
    return tokens

# ------------------ Index building ------------------ #

def build_index():
    print("Fetching pages from MongoDB...")

    # Request snippet + text + title + url (and optional content_type if you stored it)
    projection = {"url": 1, "title": 1, "text": 1, "snippet": 1}
    try:
        cursor = PAGES_COLL.find({}, projection)
    except PyMongoError as e:
        print("Failed to query pages collection:", e)
        return

    # iterate defensively so a single bad document won't abort the whole run
    pages = []
    for doc in cursor:
        try:
            pages.append(doc)
        except Exception as e:
            # If decoding a particular document fails, skip it and continue
            print("Skipping a problematic document while reading cursor:", e)
            continue

    if not pages:
        print("No pages found in 'pages' collection. Run the crawler first.")
        return

    print(f"Found {len(pages)} pages. Building index...")

    inverted_index = defaultdict(lambda: defaultdict(int))
    doc_lengths = {}
    doc_metadata = {}

    for page in pages:
        try:
            doc_id = page["_id"]
            url = page.get("url", "")
            # Prefer stored title; fallback to url if empty
            title = page.get("title") or url

            # Prefer full text for indexing. If missing, try snippet.
            # Ensure we have a string to tokenize.
            text = page.get("text")
            if text is None:
                text = page.get("snippet", "")
            if text is None:
                text = ""

            # Defensive: skip tiny or empty documents (avoids noise)
            if not isinstance(text, str) or len(text.strip()) < 50:
                # store docs metadata but don't index tokens for tiny docs
                continue

            tokens = tokenize(text)
            if not tokens:
                continue

            doc_lengths[doc_id] = len(tokens)
            # store snippet preferentially: page.snippet else first 300 chars of text
            snippet = page.get("snippet")
            if not snippet:
                snippet = text[:300]

            doc_metadata[doc_id] = {
                "url": url,
                "title": title,
                "snippet": snippet
            }

            tf_counter = Counter(tokens)
            for term, tf in tf_counter.items():
                inverted_index[term][doc_id] += tf

        except KeyError as e:
            print("Skipping doc due to missing key:", e)
            continue
        except Exception as e:
            print("Unexpected error while processing a page, skipping:", e)
            continue

    num_docs = len(doc_lengths)
    print(f"Indexed {num_docs} documents with tokens.")

    if num_docs == 0:
        print("No documents contained tokens. Aborting index build.")
        return

    index_docs = []
    for term, postings in inverted_index.items():
        df = len(postings)
        idf = math.log(num_docs / (1 + df))
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

    print("Dropping old 'documents' and 'index_terms' collections (if they exist)...")
    DOCS_COLL.drop()
    INDEX_COLL.drop()

    print("Inserting documents metadata...")
    docs_bulk = []
    for doc_id, meta in doc_metadata.items():
        docs_bulk.append({
            "_id": doc_id,
            "url": meta["url"],
            "title": meta["title"],
            "length": doc_lengths[doc_id],
            "snippet": meta["snippet"],
        })

    if docs_bulk:
        DOCS_COLL.insert_many(docs_bulk)
    print(f"Inserted {len(docs_bulk)} documents into 'documents' collection.")

    print("Inserting index terms (this may take a moment)...")
    batch_size = 1000
    for i in range(0, len(index_docs), batch_size):
        batch = index_docs[i:i + batch_size]
        INDEX_COLL.insert_many(batch)
        print(f"Inserted {i + len(batch)} / {len(index_docs)} index terms...")

    print("Index build complete ✅")


if __name__ == "__main__":
    build_index()
