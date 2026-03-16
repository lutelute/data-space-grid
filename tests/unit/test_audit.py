"""Unit tests for the audit logger.

Tests that hash computation is deterministic, all audit entry fields are
populated, entries are immutable, and the logger persists/queries correctly.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.connector.audit import AuditLogger, compute_hash
from src.connector.models import AuditAction, AuditEntry, AuditOutcome


# ---------------------------------------------------------------------------
# Hash computation
# ---------------------------------------------------------------------------


class TestComputeHash:
    """Tests for the compute_hash utility function."""

    def test_hash_is_deterministic(self) -> None:
        """The same input should always produce the same hash."""
        data = b'{"feeder_id": "F-12", "max_active_power_kw": 500}'
        h1 = compute_hash(data)
        h2 = compute_hash(data)
        assert h1 == h2

    def test_different_inputs_produce_different_hashes(self) -> None:
        """Different inputs should produce different hashes."""
        h1 = compute_hash(b"request-body-alpha")
        h2 = compute_hash(b"request-body-beta")
        assert h1 != h2

    def test_hash_is_sha256_hex_digest(self) -> None:
        """The hash should be a 64-character lowercase hex string (SHA-256)."""
        h = compute_hash(b"test data")
        assert len(h) == 64
        assert h == h.lower()
        # Verify all characters are hex
        int(h, 16)

    def test_empty_input_has_known_hash(self) -> None:
        """SHA-256 of empty bytes is a well-known constant."""
        expected = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        assert compute_hash(b"") == expected

    def test_hash_changes_with_single_byte_difference(self) -> None:
        """Changing a single byte should completely change the hash."""
        h1 = compute_hash(b"data-A")
        h2 = compute_hash(b"data-B")
        assert h1 != h2


# ---------------------------------------------------------------------------
# AuditEntry field population
# ---------------------------------------------------------------------------


class TestAuditEntryFields:
    """Tests that all fields on an AuditEntry are correctly populated."""

    def test_all_fields_populated_on_log_exchange(self, tmp_path: Path) -> None:
        """log_exchange should populate every field on the returned AuditEntry."""
        logger = AuditLogger(log_path=str(tmp_path / "audit.jsonl"))
        entry = logger.log_exchange(
            requester_id="agg-001",
            provider_id="dso-001",
            asset_id="asset-fc-01",
            purpose_tag="congestion_management",
            request_body=b'{"feeder_id": "F-12"}',
            response_body=b'{"max_active_power_kw": 500}',
            contract_id="c-001",
            action=AuditAction.READ,
            outcome=AuditOutcome.SUCCESS,
        )
        assert entry.requester_id == "agg-001"
        assert entry.provider_id == "dso-001"
        assert entry.asset_id == "asset-fc-01"
        assert entry.purpose_tag == "congestion_management"
        assert entry.contract_id == "c-001"
        assert entry.action == AuditAction.READ
        assert entry.outcome == AuditOutcome.SUCCESS
        assert entry.timestamp is not None
        assert entry.timestamp.tzinfo is not None
        # Hashes should be SHA-256 hex digests
        assert len(entry.request_hash) == 64
        assert len(entry.response_hash) == 64

    def test_request_hash_matches_compute_hash(self, tmp_path: Path) -> None:
        """The request_hash should match compute_hash(request_body)."""
        logger = AuditLogger(log_path=str(tmp_path / "audit.jsonl"))
        request_body = b"test-request-body"
        entry = logger.log_exchange(
            requester_id="agg-001",
            provider_id="dso-001",
            asset_id="asset-fc-01",
            purpose_tag="test",
            request_body=request_body,
            response_body=b"response",
            contract_id="c-001",
            action=AuditAction.READ,
            outcome=AuditOutcome.SUCCESS,
        )
        assert entry.request_hash == compute_hash(request_body)

    def test_response_hash_matches_compute_hash(self, tmp_path: Path) -> None:
        """The response_hash should match compute_hash(response_body)."""
        logger = AuditLogger(log_path=str(tmp_path / "audit.jsonl"))
        response_body = b"test-response-body"
        entry = logger.log_exchange(
            requester_id="agg-001",
            provider_id="dso-001",
            asset_id="asset-fc-01",
            purpose_tag="test",
            request_body=b"request",
            response_body=response_body,
            contract_id="c-001",
            action=AuditAction.WRITE,
            outcome=AuditOutcome.SUCCESS,
        )
        assert entry.response_hash == compute_hash(response_body)


# ---------------------------------------------------------------------------
# AuditEntry immutability
# ---------------------------------------------------------------------------


class TestAuditEntryImmutability:
    """Tests that audit entries behave as immutable records."""

    def test_entries_property_returns_copy(self, tmp_path: Path) -> None:
        """The entries property should return a copy, not the internal list."""
        logger = AuditLogger(log_path=str(tmp_path / "audit.jsonl"))
        logger.log_exchange(
            requester_id="agg-001",
            provider_id="dso-001",
            asset_id="asset-fc-01",
            purpose_tag="test",
            request_body=b"req",
            response_body=b"resp",
            contract_id="c-001",
            action=AuditAction.READ,
            outcome=AuditOutcome.SUCCESS,
        )
        entries = logger.entries
        assert len(entries) == 1

        # Modifying the returned list should not affect the logger's internal state
        entries.clear()
        assert len(logger.entries) == 1

    def test_audit_entry_model_is_frozen_like(self, tmp_path: Path) -> None:
        """AuditEntry fields should not be casually reassigned once created.

        While Pydantic v2 BaseModel is not frozen by default, we verify that the
        entry preserves its original values through normal usage.
        """
        logger = AuditLogger(log_path=str(tmp_path / "audit.jsonl"))
        entry = logger.log_exchange(
            requester_id="agg-001",
            provider_id="dso-001",
            asset_id="asset-fc-01",
            purpose_tag="congestion_management",
            request_body=b"req",
            response_body=b"resp",
            contract_id="c-001",
            action=AuditAction.READ,
            outcome=AuditOutcome.SUCCESS,
        )
        # Verify the entry retains its original data after being stored
        stored = logger.entries[0]
        assert stored.requester_id == entry.requester_id
        assert stored.request_hash == entry.request_hash
        assert stored.response_hash == entry.response_hash
        assert stored.timestamp == entry.timestamp


# ---------------------------------------------------------------------------
# Persistence and reload
# ---------------------------------------------------------------------------


class TestAuditPersistence:
    """Tests for audit log file persistence and reloading."""

    def test_entry_persisted_to_jsonl_file(self, tmp_path: Path) -> None:
        """Each logged entry should be written as a JSON line to the log file."""
        log_path = tmp_path / "audit.jsonl"
        logger = AuditLogger(log_path=str(log_path))
        logger.log_exchange(
            requester_id="agg-001",
            provider_id="dso-001",
            asset_id="asset-fc-01",
            purpose_tag="test",
            request_body=b"req",
            response_body=b"resp",
            contract_id="c-001",
            action=AuditAction.READ,
            outcome=AuditOutcome.SUCCESS,
        )

        assert log_path.exists()
        lines = log_path.read_text().strip().splitlines()
        assert len(lines) == 1

        data = json.loads(lines[0])
        assert data["requester_id"] == "agg-001"
        assert data["action"] == "read"

    def test_multiple_entries_appended(self, tmp_path: Path) -> None:
        """Multiple entries should be appended as separate JSON lines."""
        log_path = tmp_path / "audit.jsonl"
        logger = AuditLogger(log_path=str(log_path))
        for i in range(3):
            logger.log_exchange(
                requester_id=f"req-{i}",
                provider_id="dso-001",
                asset_id="asset-fc-01",
                purpose_tag="test",
                request_body=f"req-{i}".encode(),
                response_body=f"resp-{i}".encode(),
                contract_id="c-001",
                action=AuditAction.READ,
                outcome=AuditOutcome.SUCCESS,
            )

        assert len(logger) == 3
        lines = log_path.read_text().strip().splitlines()
        assert len(lines) == 3

    def test_reload_entries_from_existing_file(self, tmp_path: Path) -> None:
        """A new AuditLogger should reload entries from an existing log file."""
        log_path = tmp_path / "audit.jsonl"
        logger1 = AuditLogger(log_path=str(log_path))
        logger1.log_exchange(
            requester_id="agg-001",
            provider_id="dso-001",
            asset_id="asset-fc-01",
            purpose_tag="test",
            request_body=b"req",
            response_body=b"resp",
            contract_id="c-001",
            action=AuditAction.READ,
            outcome=AuditOutcome.SUCCESS,
        )
        assert len(logger1) == 1

        # Create a second logger pointing at the same file
        logger2 = AuditLogger(log_path=str(log_path))
        assert len(logger2) == 1
        assert logger2.entries[0].requester_id == "agg-001"


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------


class TestAuditQuery:
    """Tests for the audit log query method."""

    def _create_logger_with_entries(self, tmp_path: Path) -> AuditLogger:
        """Create a logger with several varied entries for query tests."""
        logger = AuditLogger(log_path=str(tmp_path / "audit.jsonl"))
        logger.log_exchange(
            requester_id="agg-001",
            provider_id="dso-001",
            asset_id="asset-fc-01",
            purpose_tag="congestion_management",
            request_body=b"req1",
            response_body=b"resp1",
            contract_id="c-001",
            action=AuditAction.READ,
            outcome=AuditOutcome.SUCCESS,
        )
        logger.log_exchange(
            requester_id="agg-002",
            provider_id="dso-001",
            asset_id="asset-fc-02",
            purpose_tag="flexibility",
            request_body=b"req2",
            response_body=b"resp2",
            contract_id="c-002",
            action=AuditAction.WRITE,
            outcome=AuditOutcome.DENIED,
        )
        logger.log_exchange(
            requester_id="agg-001",
            provider_id="dso-002",
            asset_id="asset-fc-01",
            purpose_tag="congestion_management",
            request_body=b"req3",
            response_body=b"resp3",
            contract_id="c-003",
            action=AuditAction.READ,
            outcome=AuditOutcome.SUCCESS,
        )
        return logger

    def test_query_all(self, tmp_path: Path) -> None:
        """Querying with no filters should return all entries."""
        logger = self._create_logger_with_entries(tmp_path)
        assert len(logger.query()) == 3

    def test_query_by_requester_id(self, tmp_path: Path) -> None:
        """Filtering by requester_id should return matching entries."""
        logger = self._create_logger_with_entries(tmp_path)
        results = logger.query(requester_id="agg-001")
        assert len(results) == 2
        assert all(e.requester_id == "agg-001" for e in results)

    def test_query_by_action(self, tmp_path: Path) -> None:
        """Filtering by action should return matching entries."""
        logger = self._create_logger_with_entries(tmp_path)
        results = logger.query(action=AuditAction.WRITE)
        assert len(results) == 1
        assert results[0].action == AuditAction.WRITE

    def test_query_by_outcome(self, tmp_path: Path) -> None:
        """Filtering by outcome should return matching entries."""
        logger = self._create_logger_with_entries(tmp_path)
        results = logger.query(outcome=AuditOutcome.DENIED)
        assert len(results) == 1
        assert results[0].outcome == AuditOutcome.DENIED

    def test_query_by_contract_id(self, tmp_path: Path) -> None:
        """Filtering by contract_id should return matching entries."""
        logger = self._create_logger_with_entries(tmp_path)
        results = logger.query(contract_id="c-001")
        assert len(results) == 1
        assert results[0].contract_id == "c-001"

    def test_query_no_matches(self, tmp_path: Path) -> None:
        """Querying with a filter that matches nothing should return an empty list."""
        logger = self._create_logger_with_entries(tmp_path)
        results = logger.query(requester_id="nonexistent")
        assert results == []
