"""
test_segment_store.py
=====================
Pytest test suite for store/segment_store.py

Run from the project root:
    pytest tests/test_segment_store.py -v

These tests use the real JSONL file and real ChromaDB instance.
They are integration tests, not unit tests — they verify the full
load → query → retrieve cycle works end to end.

Test isolation: the store is loaded once per session via a session-scoped
fixture. Individual tests do not modify the store.
The delete_store() and force_reload tests run last to avoid
interfering with other tests.
"""

import pytest
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from store.segment_store import (
    load_segments,
    list_all_segments,
    get_segment_by_id,
    get_segments_by_ids,
    query_segments,
    store_info,
    delete_store,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session", autouse=True)
def loaded_store():
    """
    Load segments once for the entire test session.
    All tests in this file share this loaded state.
    """
    count = load_segments()
    assert count > 0, "load_segments() returned 0 — check JSONL path"
    yield count


# ── store_info ────────────────────────────────────────────────────────────────

class TestStoreInfo:

    def test_returns_dict_with_required_keys(self):
        info = store_info()
        required = {"collection_name", "segment_count", "chroma_path",
                    "jsonl_default", "embed_model", "is_loaded"}
        assert required.issubset(info.keys())

    def test_is_loaded_true_after_load(self):
        info = store_info()
        assert info["is_loaded"] is True

    def test_segment_count_positive(self):
        info = store_info()
        assert info["segment_count"] > 0

    def test_embed_model_correct(self):
        info = store_info()
        assert info["embed_model"] == "all-MiniLM-L6-v2"


# ── load_segments ─────────────────────────────────────────────────────────────

class TestLoadSegments:

    def test_idempotent_returns_same_count(self):
        count1 = load_segments()
        count2 = load_segments()
        assert count1 == count2

    def test_raises_if_path_not_found(self):
        with pytest.raises(FileNotFoundError):
            load_segments(jsonl_path=Path("/nonexistent/path/file.jsonl"))

    def test_force_reload_returns_same_count(self):
        count_before = store_info()["segment_count"]
        count_after  = load_segments(force_reload=True)
        assert count_after == count_before


# ── list_all_segments ─────────────────────────────────────────────────────────

class TestListAllSegments:

    def test_returns_list(self):
        result = list_all_segments()
        assert isinstance(result, list)

    def test_returns_all_segments(self):
        result = list_all_segments()
        assert len(result) == store_info()["segment_count"]

    def test_each_item_has_required_keys(self):
        result = list_all_segments()
        required = {"segment_id", "archetype_name", "metadata", "rag_text"}
        for item in result:
            assert required.issubset(item.keys()), f"Missing keys in {item['segment_id']}"

    def test_sorted_by_segment_id(self):
        result = list_all_segments()
        ids = [r["segment_id"] for r in result]
        assert ids == sorted(ids)

    def test_segment_ids_are_strings(self):
        result = list_all_segments()
        for item in result:
            assert isinstance(item["segment_id"], str)

    def test_rag_text_is_non_empty_string(self):
        result = list_all_segments()
        for item in result:
            assert isinstance(item["rag_text"], str)
            assert len(item["rag_text"]) > 0

    def test_metadata_contains_archetype_name(self):
        result = list_all_segments()
        for item in result:
            assert "archetype_name" in item["metadata"]


# ── get_segment_by_id ─────────────────────────────────────────────────────────

class TestGetSegmentById:

    def test_returns_correct_segment(self):
        all_segs = list_all_segments()
        first_id = all_segs[0]["segment_id"]
        result = get_segment_by_id(first_id)
        assert result is not None
        assert result["segment_id"] == first_id

    def test_returns_none_for_unknown_id(self):
        result = get_segment_by_id("DOES_NOT_EXIST_999")
        assert result is None

    def test_returned_dict_has_required_keys(self):
        all_segs  = list_all_segments()
        first_id  = all_segs[0]["segment_id"]
        result    = get_segment_by_id(first_id)
        required  = {"segment_id", "archetype_name", "metadata", "rag_text"}
        assert required.issubset(result.keys())

    def test_metadata_has_tags(self):
        all_segs = list_all_segments()
        first_id = all_segs[0]["segment_id"]
        result   = get_segment_by_id(first_id)
        assert "tags" in result["metadata"]


# ── get_segments_by_ids ───────────────────────────────────────────────────────

class TestGetSegmentsByIds:

    def test_returns_all_requested_segments(self):
        all_segs = list_all_segments()
        ids      = [s["segment_id"] for s in all_segs[:2]]
        result   = get_segments_by_ids(ids)
        assert len(result) == 2

    def test_preserves_input_order(self):
        all_segs    = list_all_segments()
        ids         = [s["segment_id"] for s in all_segs[:3]]
        ids_reversed = list(reversed(ids))
        result      = get_segments_by_ids(ids_reversed)
        returned_ids = [r["segment_id"] for r in result]
        assert returned_ids == ids_reversed

    def test_skips_unknown_ids_with_warning(self, capsys):
        all_segs = list_all_segments()
        valid_id = all_segs[0]["segment_id"]
        result   = get_segments_by_ids([valid_id, "FAKE_ID_999"])
        assert len(result) == 1
        assert result[0]["segment_id"] == valid_id
        captured = capsys.readouterr()
        assert "FAKE_ID_999" in captured.out
        assert "not found" in captured.out

    def test_raises_on_empty_list(self):
        with pytest.raises(ValueError):
            get_segments_by_ids([])

    def test_each_result_has_required_keys(self):
        all_segs = list_all_segments()
        ids      = [s["segment_id"] for s in all_segs[:2]]
        result   = get_segments_by_ids(ids)
        required = {"segment_id", "archetype_name", "metadata", "rag_text"}
        for item in result:
            assert required.issubset(item.keys())


# ── query_segments ────────────────────────────────────────────────────────────

class TestQuerySegments:

    def test_returns_list(self):
        result = query_segments("financially stable homeowner digital media user")
        assert isinstance(result, list)

    def test_returns_correct_number_of_results(self):
        result = query_segments("young urban renter low income", n_results=2)
        assert len(result) == 2

    def test_each_result_has_required_keys(self):
        result   = query_segments("conservative values homeowner employed")
        required = {"segment_id", "archetype_name", "similarity", "metadata", "rag_text"}
        for item in result:
            assert required.issubset(item.keys())

    def test_similarity_scores_between_0_and_1(self):
        result = query_segments("digital media consumer high income")
        for item in result:
            assert 0.0 <= item["similarity"] <= 1.0

    def test_results_sorted_by_similarity_descending(self):
        result = query_segments("married employed suburban owner")
        scores = [r["similarity"] for r in result]
        assert scores == sorted(scores, reverse=True)

    def test_n_results_default_is_3(self):
        result = query_segments("any query text here")
        assert len(result) <= 3

    def test_where_filter_respects_metadata(self):
        result = query_segments(
            "homeowner stable income digital user",
            n_results=7,
            where={"tenure": "owner"},
        )
        for item in result:
            assert item["metadata"].get("tenure") == "owner"

    def test_returns_results_for_trait_description(self):
        # This is the correct usage pattern — LLM-inferred traits, not raw SOBJ
        trait_description = (
            "financially stable adult, homeowner, digitally active, "
            "conservative leaning, moderate to high household income"
        )
        result = query_segments(trait_description, n_results=3)
        assert len(result) > 0
        assert result[0]["similarity"] > 0.0


# ── delete_store (runs last — modifies store state) ───────────────────────────

class TestDeleteStore:
    """
    These tests modify the store. They run last.
    After delete, force_reload restores the store for any subsequent runs.
    """

    def test_delete_and_reload(self):
        # Delete
        delete_store()
        info = store_info()
        assert info["segment_count"] == 0
        assert info["is_loaded"] is False

        # Reload — restores store for any tests that run after this file
        count = load_segments()
        assert count > 0
        assert store_info()["is_loaded"] is True
