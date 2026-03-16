"""Audit logger for the Federated Data Space Connector.

Every data exchange in the federated data space generates an immutable audit
entry.  This is non-negotiable – failing to audit means failing the request
(spec Pattern 4 – Audit Trail on Every Exchange).

The ``AuditLogger`` produces :class:`~src.connector.models.AuditEntry` records
containing SHA-256 hashes of both request and response content for tamper
evidence, together with the purpose tag, requester identity, contract ID,
action classification, and outcome.

Storage is append-only: each entry is serialised as a single JSON line and
appended to a file.  This guarantees that previously written records cannot be
silently modified.

Key design decisions:
  - ``compute_hash()`` is a standalone utility so that callers (e.g. the
    ConnectorMiddleware) can hash request / response bodies independently
    before calling ``log_exchange()``.
  - The ``AuditLogger`` keeps an in-memory list *and* an on-disk JSON-lines
    file.  In-memory entries enable fast queries; the file provides durable,
    append-only persistence.
  - Query methods accept optional filters and return copies of matching
    entries, preserving immutability of the internal log.
  - All timestamps are timezone-aware UTC.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from src.connector.models import AuditAction, AuditEntry, AuditOutcome


def _utc_now() -> datetime:
    """Return the current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------


def compute_hash(data: bytes) -> str:
    """Compute the SHA-256 hex digest of *data*.

    This is used to produce tamper-evidence hashes of request and response
    bodies.  The hash is deterministic: the same input always yields the same
    output.

    Args:
        data: Raw bytes to hash.

    Returns:
        The lowercase hex-encoded SHA-256 digest (64 characters).
    """
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class AuditError(Exception):
    """Base exception for audit logging errors."""


class AuditWriteError(AuditError):
    """Raised when an audit entry cannot be persisted to the log file."""

    def __init__(self, path: str, detail: str) -> None:
        self.path = path
        self.detail = detail
        super().__init__(f"Failed to write audit entry to '{path}': {detail}")


# ---------------------------------------------------------------------------
# AuditLogger
# ---------------------------------------------------------------------------


class AuditLogger:
    """Append-only audit logger for federated data space exchanges.

    The logger writes :class:`~src.connector.models.AuditEntry` records as
    JSON-lines to a file and maintains an in-memory copy for querying.

    Usage::

        logger = AuditLogger(log_path="./audit/connector.jsonl")
        entry = logger.log_exchange(
            requester_id="agg-001",
            provider_id="dso-001",
            asset_id="asset-fc-01",
            purpose_tag="congestion_management",
            request_body=b'{"feeder_id": "F-12"}',
            response_body=b'{"max_active_power_kw": 500}',
            contract_id="contract-001",
            action=AuditAction.READ,
            outcome=AuditOutcome.SUCCESS,
        )

    Args:
        log_path: Path to the JSON-lines audit file.  Defaults to
            ``"./audit/audit.jsonl"``.  Parent directories are created
            automatically if they do not exist.
    """

    def __init__(self, log_path: str = "./audit/audit.jsonl") -> None:
        self._log_path = Path(log_path)
        self._entries: list[AuditEntry] = []

        # Ensure the parent directory exists so the first write does not fail.
        self._log_path.parent.mkdir(parents=True, exist_ok=True)

        # Load any existing entries from the file so in-memory state is
        # consistent with on-disk state after a restart.
        self._load_existing_entries()

    # -- public API ----------------------------------------------------------

    def log_exchange(
        self,
        *,
        requester_id: str,
        provider_id: str,
        asset_id: str,
        purpose_tag: str,
        request_body: bytes,
        response_body: bytes,
        contract_id: str,
        action: AuditAction,
        outcome: AuditOutcome,
    ) -> AuditEntry:
        """Record a data exchange in the audit trail.

        The method computes SHA-256 hashes of the request and response bodies,
        creates an :class:`~src.connector.models.AuditEntry`, persists it to
        the JSON-lines file, and appends it to the in-memory log.

        Args:
            requester_id: Participant ID of the data requester.
            provider_id: Participant ID of the data provider.
            asset_id: ID of the data asset that was accessed.
            purpose_tag: Purpose tag from the contract (must match allowed
                purpose).
            request_body: Raw bytes of the request body.
            response_body: Raw bytes of the response body.
            contract_id: ID of the contract authorising this exchange.
            action: Type of action performed.
            outcome: Outcome of the exchange.

        Returns:
            The newly created :class:`~src.connector.models.AuditEntry`.

        Raises:
            AuditWriteError: If the entry cannot be written to the log file.
        """
        entry = AuditEntry(
            timestamp=_utc_now(),
            requester_id=requester_id,
            provider_id=provider_id,
            asset_id=asset_id,
            purpose_tag=purpose_tag,
            request_hash=compute_hash(request_body),
            response_hash=compute_hash(response_body),
            contract_id=contract_id,
            action=action,
            outcome=outcome,
        )

        self._persist_entry(entry)
        self._entries.append(entry)
        return entry

    def query(
        self,
        *,
        requester_id: Optional[str] = None,
        provider_id: Optional[str] = None,
        asset_id: Optional[str] = None,
        contract_id: Optional[str] = None,
        action: Optional[AuditAction] = None,
        outcome: Optional[AuditOutcome] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> list[AuditEntry]:
        """Retrieve audit entries matching the given filters.

        All filter parameters are optional.  When multiple filters are
        provided they are combined with logical AND.  Omitting all filters
        returns every entry in the log.

        Args:
            requester_id: Filter by requester participant ID.
            provider_id: Filter by provider participant ID.
            asset_id: Filter by data asset ID.
            contract_id: Filter by contract ID.
            action: Filter by audit action type.
            outcome: Filter by exchange outcome.
            start_time: Return entries at or after this timestamp (inclusive).
            end_time: Return entries at or before this timestamp (inclusive).

        Returns:
            A list of matching :class:`~src.connector.models.AuditEntry`
            instances, ordered by timestamp ascending.
        """
        results: list[AuditEntry] = []
        for entry in self._entries:
            if requester_id is not None and entry.requester_id != requester_id:
                continue
            if provider_id is not None and entry.provider_id != provider_id:
                continue
            if asset_id is not None and entry.asset_id != asset_id:
                continue
            if contract_id is not None and entry.contract_id != contract_id:
                continue
            if action is not None and entry.action != action:
                continue
            if outcome is not None and entry.outcome != outcome:
                continue
            if start_time is not None and entry.timestamp < start_time:
                continue
            if end_time is not None and entry.timestamp > end_time:
                continue
            results.append(entry)
        return results

    @property
    def entries(self) -> list[AuditEntry]:
        """Return all audit entries (read-only copy)."""
        return list(self._entries)

    @property
    def log_path(self) -> Path:
        """Return the path to the audit log file."""
        return self._log_path

    def __len__(self) -> int:
        """Return the number of audit entries in the log."""
        return len(self._entries)

    # -- private helpers -----------------------------------------------------

    def _persist_entry(self, entry: AuditEntry) -> None:
        """Append a single audit entry as a JSON line to the log file.

        Raises:
            AuditWriteError: If writing fails for any reason.
        """
        try:
            line = entry.model_dump_json() + "\n"
            with open(self._log_path, "a", encoding="utf-8") as fh:
                fh.write(line)
        except OSError as exc:
            raise AuditWriteError(str(self._log_path), str(exc)) from exc

    def _load_existing_entries(self) -> None:
        """Load previously persisted entries from the JSON-lines file.

        Invalid lines are silently skipped so that a partially corrupted
        file does not prevent the logger from starting.
        """
        if not self._log_path.exists():
            return

        try:
            with open(self._log_path, "r", encoding="utf-8") as fh:
                for line in fh:
                    stripped = line.strip()
                    if not stripped:
                        continue
                    try:
                        data = json.loads(stripped)
                        self._entries.append(AuditEntry.model_validate(data))
                    except (json.JSONDecodeError, ValueError):
                        # Skip corrupted lines – the append-only contract
                        # means we never modify or delete existing lines.
                        continue
        except OSError:
            # File may have been deleted between the exists() check and open().
            pass
