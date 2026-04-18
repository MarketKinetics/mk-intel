"""
segment_store.py
================
MK Shared Infrastructure — Baseline Segment Store (RAG layer)
 
Loads the 7 MK baseline audience segments (BTAs) into ChromaDB and exposes
a clean query interface used by all MK platforms.
 
This module is shared infrastructure. It lives at the project root
under store/ and is NOT specific to any single platform.
 
ChromaDB persists to vector_db/ at the project root.
 
──────────────────────────────────────────────────────────────────
Public API
──────────────────────────────────────────────────────────────────
 
    load_segments(jsonl_path, force_reload)
        Ingest JSONL into ChromaDB. Idempotent.
 
    list_all_segments()
        Return all 7 BTAs with full profiles.
        Primary input to the LLM pre-filter shortlist call.
 
    get_segment_by_id(segment_id)
        Direct lookup of one segment by ID e.g. "BTA_00".
 
    get_segments_by_ids(segment_ids)
        Batch lookup of multiple segments by ID list.
        Used after the pre-filter returns a shortlist.
 
    query_segments(query_text, n_results, where)
        Semantic similarity search.
        IMPORTANT: query_text must be an LLM-inferred trait description,
        NOT a raw SOBJ statement. See docstring for details.
 
    store_info()
        Health check — returns current state of the store.
 
    delete_store()
        Wipes the collection. For testing and forced reloads only.
 
──────────────────────────────────────────────────────────────────
Pipeline position
──────────────────────────────────────────────────────────────────
 
    OBJ + SOBJs approved
        ↓
    Ingest proprietary data
        ↓
    Augment all baseline segments          ← list_all_segments() used here
        ↓
    LLM pre-filter per SOBJ                ← query_segments() used here
    → shortlist 2-3 segment IDs per SOBJ
        ↓
    Fetch shortlisted profiles             ← get_segments_by_ids() used here
        ↓
    Build full TAAW per (segment, SOBJ)
        ↓
    Score + rank
"""
 
from __future__ import annotations
 
import json
from pathlib import Path
from typing import Optional
 
import chromadb
from chromadb.utils import embedding_functions
 
 
# ── Path resolution ───────────────────────────────────────────────────────────
#
# This file lives at:
#   Market_Kinetics/store/segment_store.py
#
# parents[0] = Market_Kinetics/store/
# parents[1] = Market_Kinetics/            ← project root
#
# All paths are derived from the project root so this module works
# on any machine regardless of where the repo is cloned.
# No hardcoded absolute paths anywhere in this file.
 
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
 
_DEFAULT_JSONL = (
    _PROJECT_ROOT
    / "data"
    / "societal_processed"
    / "bta_cards"
    / "mk_bta_rag_corpus.jsonl"
)
 
_CHROMA_PATH = _PROJECT_ROOT / "data" / "vector_db"
 
COLLECTION_NAME = "mk_baseline_segments"
 
 
# ── Embedding model ───────────────────────────────────────────────────────────
#
# all-MiniLM-L6-v2:
#   - Small (80MB), fast, fully local, zero API cost
#   - Downloaded once by sentence-transformers, cached on disk
#   - Sufficient for 7 documents of this length
#   - To swap models in the future, change this constant only
 
_EMBED_FN = embedding_functions.SentenceTransformerEmbeddingFunction(
    model_name="all-MiniLM-L6-v2"
)
 
 
# ── Internal helpers ──────────────────────────────────────────────────────────
 
def _get_collection() -> chromadb.Collection:
    """
    Returns the ChromaDB collection, creating it if it does not exist.
    PersistentClient writes to disk — data survives between Python sessions.
    """
    client = chromadb.PersistentClient(path=str(_CHROMA_PATH))
    collection = client.get_or_create_collection(
        name=COLLECTION_NAME,
        embedding_function=_EMBED_FN,
        metadata={"hnsw:space": "cosine"},
    )
    return collection
 
 
def _tags_to_metadata(tags: list[str]) -> dict:
    """
    Parse the flat tags list into named, filterable metadata fields.
 
    ChromaDB metadata values must be str, int, or float — no lists.
    Tags are stored both as a comma-separated string (full inspection)
    and as individual named fields (precise filtering).
 
    Tag conventions in mk_bta_rag_corpus.jsonl:
        age bins    : "35-44", "55-64", "65+" etc.
        sex         : "male", "female"
        race/eth    : "white", "hispanic", "black" etc.
        education   : "hs_or_less", "some_college", "bachelors_plus"
        income      : "100-199k", "50-99k", "200k_plus" etc.
        tenure      : "owner", "renter"
        employment  : "employed", "not_employed"
        marital     : "married", "not_married"
        psych       : "party_alignment_republican" etc.
        media       : "media_usage_instagram", "media_usage_youtube" etc.
    """
    metadata: dict = {"tags": ",".join(tags)}
 
    for tag in tags:
 
        # Income: contains "k" e.g. "100-199k", "50-99k", "200k_plus"
        if "k" in tag:
            metadata["income_tier"] = tag
 
        # Age bin: digits + hyphen or plus, no "k"
        elif any(c.isdigit() for c in tag) and ("-" in tag or "+" in tag):
            metadata["age_bin"] = tag
 
        # Sex
        elif tag in ("male", "female"):
            metadata["sex"] = tag
 
        # Tenure
        elif tag in ("owner", "renter"):
            metadata["tenure"] = tag
 
        # Employment
        elif tag in ("employed", "not_employed"):
            metadata["employment"] = tag
 
        # Marital
        elif tag in ("married", "not_married"):
            metadata["marital"] = tag
 
        # Education
        elif tag in ("hs_or_less", "some_college", "bachelors_plus"):
            metadata["education"] = tag
 
        # Media channels: prefix "media_usage_"
        elif tag.startswith("media_usage_"):
            existing = metadata.get("media_channels", "")
            channel  = tag.replace("media_usage_", "")
            metadata["media_channels"] = (
                f"{existing},{channel}" if existing else channel
            )
 
        # Psych signals: underscore-separated, no digits
        elif "_" in tag and not any(c.isdigit() for c in tag):
            existing = metadata.get("psych_signals", "")
            metadata["psych_signals"] = (
                f"{existing},{tag}" if existing else tag
            )
 
        # Race/ethnicity: single words not caught above
        elif tag.isalpha() and tag not in (
            "male", "female", "owner", "renter",
            "employed", "married"
        ):
            metadata["race_eth"] = tag
 
    return metadata
 
 
def _parse_record(raw: dict) -> dict:
    """
    Transform one raw JSONL record into a ChromaDB-ready dict.
 
    Input fields expected: segment_id, archetype_name, tags, rag_text
    Returns:
        doc_id   : ChromaDB document ID  (= segment_id)
        document : text to embed          (= rag_text)
        metadata : structured fields for filtering
    """
    segment_id = str(raw["segment_id"])
    archetype  = str(raw.get("archetype_name", ""))
    rag_text   = str(raw["rag_text"])
    tags       = raw.get("tags", [])
 
    # Defensive: handle stringified lists
    if isinstance(tags, str):
        tags = [t.strip() for t in tags.strip("[]").replace("'", "").split(",")]
 
    tag_metadata = _tags_to_metadata(tags)
 
    metadata = {
        "segment_id":     segment_id,
        "archetype_name": archetype,
        **tag_metadata,
    }
 
    return {
        "doc_id":   segment_id,
        "document": rag_text,
        "metadata": metadata,
    }
 
 
def _format_result(doc_id: str, document: str, metadata: dict) -> dict:
    """
    Consistent output format for all public functions that return segments.
    """
    return {
        "segment_id":     metadata.get("segment_id", doc_id),
        "archetype_name": metadata.get("archetype_name", ""),
        "metadata":       metadata,
        "rag_text":       document,
    }
 
 
# ── Public API ────────────────────────────────────────────────────────────────
 
def load_segments(
    jsonl_path: Optional[Path] = None,
    force_reload: bool = False,
) -> int:
    """
    Ingest baseline segments from JSONL into ChromaDB.
 
    Idempotent by default — if segments are already loaded and
    force_reload=False, skips ingestion and returns the existing count.
    Use force_reload=True to wipe and re-ingest after updating the JSONL.
 
    Args:
        jsonl_path   : path to the JSONL file.
                       Defaults to the standard project location.
                       Pass a custom path for testing or alternative datasets.
        force_reload : wipe existing collection and reload from scratch.
 
    Returns:
        int — number of segments in the collection after loading.
 
    Raises:
        FileNotFoundError : JSONL file not found at the given path.
        ValueError        : JSONL file contains no valid records.
    """
    path = Path(jsonl_path) if jsonl_path else _DEFAULT_JSONL
 
    if not path.exists():
        raise FileNotFoundError(
            f"Segment JSONL not found at: {path}\n"
            f"Default expected location:  {_DEFAULT_JSONL}\n"
            f"Pass jsonl_path= explicitly if your file is elsewhere."
        )
 
    collection = _get_collection()
 
    existing_count = collection.count()
    if existing_count > 0 and not force_reload:
        print(f"[segment_store] Already loaded ({existing_count} segments). "
              f"Pass force_reload=True to re-ingest.")
        return existing_count
 
    if force_reload and existing_count > 0:
        print(f"[segment_store] force_reload=True — wiping {existing_count} segments...")
        client = chromadb.PersistentClient(path=str(_CHROMA_PATH))
        client.delete_collection(COLLECTION_NAME)
        collection = _get_collection()
 
    records = []
    with open(path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"[segment_store] Warning: skipping malformed line {line_num}: {e}")
 
    if not records:
        raise ValueError(f"No valid records found in {path}")
 
    parsed = [_parse_record(r) for r in records]
 
    collection.upsert(
        ids=       [p["doc_id"]   for p in parsed],
        documents= [p["document"] for p in parsed],
        metadatas= [p["metadata"] for p in parsed],
    )
 
    count = collection.count()
    print(f"[segment_store] Loaded {count} segments into '{COLLECTION_NAME}'.")
    print(f"[segment_store] Persisted to: {_CHROMA_PATH}")
    return count
 
 
def list_all_segments() -> list[dict]:
    """
    Returns all segments in the store with full profiles.
 
    PRIMARY USE: feeds the LLM pre-filter shortlist call.
    The pre-filter receives the full list of augmented segment profiles
    and reasons about which 2-3 are plausible candidates per SOBJ.
 
    Results are sorted by segment_id for consistent ordering.
 
    Returns:
        List of dicts — one per segment:
        [
            {
                "segment_id":     "BTA_00",
                "archetype_name": "Young Diverse Working Households",
                "metadata":       { age_bin, income_tier, tenure, ... },
                "rag_text":       "Segment ID: BTA_00 ...",
            },
            ...
        ]
 
    Raises:
        RuntimeError : collection is empty — call load_segments() first.
    """
    collection = _get_collection()
 
    if collection.count() == 0:
        raise RuntimeError(
            "Segment store is empty. Call load_segments() first."
        )
 
    results = collection.get(include=["documents", "metadatas"])
 
    output = [
        _format_result(
            results["ids"][i],
            results["documents"][i],
            results["metadatas"][i],
        )
        for i in range(len(results["ids"]))
    ]
 
    output.sort(key=lambda x: x["segment_id"])
    return output
 
 
def get_segment_by_id(segment_id: str) -> Optional[dict]:
    """
    Direct lookup of a single segment by its ID.
 
    Args:
        segment_id : segment identifier string e.g. "BTA_00", "BTA_03".
 
    Returns:
        Dict with full segment data, or None if the ID is not found:
        {
            "segment_id":     "BTA_00",
            "archetype_name": "Young Diverse Working Households",
            "metadata":       { age_bin, income_tier, tenure, ... },
            "rag_text":       "Segment ID: BTA_00 ...",
        }
    """
    collection = _get_collection()
 
    results = collection.get(
        ids=[segment_id],
        include=["documents", "metadatas"],
    )
 
    if not results["ids"]:
        return None
 
    return _format_result(
        results["ids"][0],
        results["documents"][0],
        results["metadatas"][0],
    )
 
 
def get_segments_by_ids(segment_ids: list[str]) -> list[dict]:
    """
    Batch lookup of multiple segments by ID list.
 
    PRIMARY USE: fetches the shortlisted segments after the LLM pre-filter
    returns a list of candidate segment IDs per SOBJ. These profiles are
    then passed to TAAW generation.
 
    Segments are returned in the same order as segment_ids.
    If an ID is not found, it is silently skipped with a warning —
    one missing ID does not break the batch.
 
    Args:
        segment_ids : list of segment ID strings
                      e.g. ["BTA_01", "BTA_03"]
 
    Returns:
        List of dicts — one per found segment, in input order:
        [
            {
                "segment_id":     "BTA_01",
                "archetype_name": "Older White Renters",
                "metadata":       { ... },
                "rag_text":       "...",
            },
            ...
        ]
 
    Raises:
        ValueError   : segment_ids list is empty.
        RuntimeError : collection is empty — call load_segments() first.
    """
    if not segment_ids:
        raise ValueError("segment_ids list cannot be empty.")
 
    collection = _get_collection()
 
    if collection.count() == 0:
        raise RuntimeError(
            "Segment store is empty. Call load_segments() first."
        )
 
    results = collection.get(
        ids=segment_ids,
        include=["documents", "metadatas"],
    )
 
    found_ids = set(results["ids"])
    for sid in segment_ids:
        if sid not in found_ids:
            print(f"[segment_store] Warning: segment_id '{sid}' not found — skipping.")
 
    found: dict[str, dict] = {}
    for i in range(len(results["ids"])):
        doc_id = results["ids"][i]
        found[doc_id] = _format_result(
            doc_id,
            results["documents"][i],
            results["metadatas"][i],
        )
 
    return [found[sid] for sid in segment_ids if sid in found]
 
 
def query_segments(
    query_text: str,
    n_results: int = 3,
    where: Optional[dict] = None,
) -> list[dict]:
    """
    Semantic similarity search across all baseline segments.
 
    ──────────────────────────────────────────────────────────────
    IMPORTANT — what to pass as query_text:
    ──────────────────────────────────────────────────────────────
    Do NOT pass a raw SOBJ statement.
 
    "TA will renew subscription at next billing cycle" is a poor
    query — none of the segment documents contain subscription
    language and similarity scores will be meaningless.
 
    INSTEAD, pass an LLM-inferred trait description:
    The LLM first interprets what kind of person could accomplish
    the SOBJ, then produces a trait description that maps to
    segment vocabulary. That description is what you query with.
 
    Example:
        SOBJ      : "TA renews subscription at next billing cycle"
        LLM infers: "financially stable adult with digital engagement,
                     existing brand loyalty, responsive to value messaging,
                     moderate-to-high household income, homeowner"
        query_text: the inferred description above ✓
    ──────────────────────────────────────────────────────────────
 
    Args:
        query_text : LLM-inferred trait description (not raw SOBJ text).
        n_results  : number of segments to return (default 3, max 7).
        where      : optional ChromaDB metadata filter.
                     Examples:
                       {"tenure": "owner"}
                       {"income_tier": "100-199k"}
                       {"sex": "female"}
 
    Returns:
        List of dicts sorted by similarity descending:
        [
            {
                "segment_id":     "BTA_01",
                "archetype_name": "Older White Renters",
                "similarity":     0.87,
                "metadata":       { ... },
                "rag_text":       "...",
            },
            ...
        ]
 
    Raises:
        RuntimeError : collection is empty — call load_segments() first.
    """
    collection = _get_collection()
 
    if collection.count() == 0:
        raise RuntimeError(
            "Segment store is empty. Call load_segments() first."
        )
 
    n = min(n_results, collection.count())
 
    kwargs: dict = {
        "query_texts": [query_text],
        "n_results":   n,
        "include":     ["documents", "metadatas", "distances"],
    }
    if where:
        kwargs["where"] = where
 
    results = collection.query(**kwargs)
 
    output = []
    for i in range(len(results["ids"][0])):
        distance   = results["distances"][0][i]
        similarity = round(1 - (distance / 2), 4)
 
        segment = _format_result(
            results["ids"][0][i],
            results["documents"][0][i],
            results["metadatas"][0][i],
        )
        segment["similarity"] = similarity
        output.append(segment)
 
    return output
 
 
def store_info() -> dict:
    """
    Returns the current state of the segment store.
    Use for health checks, debugging, and pipeline pre-flight checks.
 
    Returns:
        {
            "collection_name" : str,
            "segment_count"   : int,
            "chroma_path"     : str,
            "jsonl_default"   : str,
            "embed_model"     : str,
            "is_loaded"       : bool,
        }
    """
    collection = _get_collection()
    count = collection.count()
    return {
        "collection_name": COLLECTION_NAME,
        "segment_count":   count,
        "chroma_path":     str(_CHROMA_PATH),
        "jsonl_default":   str(_DEFAULT_JSONL),
        "embed_model":     "all-MiniLM-L6-v2",
        "is_loaded":       count > 0,
    }
 
 
def delete_store() -> None:
    """
    Wipes the entire segment collection from ChromaDB.
 
    Use during testing or when forcing a clean reload.
    After calling this, load_segments() must be called again
    before any query function will work.
    """
    client = chromadb.PersistentClient(path=str(_CHROMA_PATH))
    try:
        client.delete_collection(COLLECTION_NAME)
        print(f"[segment_store] Collection '{COLLECTION_NAME}' deleted.")
    except Exception:
        print(f"[segment_store] Collection '{COLLECTION_NAME}' did not exist — nothing to delete.")
