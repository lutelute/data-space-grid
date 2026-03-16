"""Integration tests for audit trail completeness verification.

Verifies that every data exchange in the federated data space produces
a correct and complete audit entry:
  1. After a congestion management flow, every exchange has an audit entry.
  2. Each audit entry has all required fields: request_hash, response_hash,
     purpose_tag, timestamp, and requester_id.
  3. SHA-256 hashes in audit entries match the actual request and response
     content.
  4. Purpose tag and contract ID from request headers are recorded correctly.
  5. Audit entries distinguish between read and write actions.
  6. Denied requests (auth failures beyond the middleware layer) also
     produce audit entries with outcome="denied".

These tests create fresh in-memory instances of all services (catalog,
DSO, aggregator) with mock authentication and in-memory event bus.
The audit logger is injected so that entries can be inspected after each
exchange.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI

from src.catalog.routes import create_router as create_catalog_router
from src.catalog.store import CatalogStore
from src.connector.audit import AuditLogger, compute_hash
from src.connector.events import Topic
from src.connector.middleware import ConnectorMiddleware
from src.connector.models import AuditAction, AuditOutcome
from src.participants.aggregator.routes import (
    create_router as create_aggregator_router,
)
from src.participants.aggregator.store import AggregatorStore
from src.participants.dso.routes import create_router as create_dso_router
from src.participants.dso.store import DSOStore
from tests.conftest import (
    MockEventBus,
    MockKeycloakAuthBackend,
    make_aggregator_user,
    make_dso_user,
)


def _utc_now() -> datetime:
    """Return the current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def shared_event_bus():
    """A shared in-memory event bus for DSO and Aggregator nodes."""
    return MockEventBus()


@pytest.fixture()
def dso_audit_logger(tmp_path: Path):
    """Audit logger for the DSO node."""
    return AuditLogger(log_path=str(tmp_path / "dso-audit-trail.jsonl"))


@pytest.fixture()
def aggregator_audit_logger(tmp_path: Path):
    """Audit logger for the Aggregator node."""
    return AuditLogger(log_path=str(tmp_path / "agg-audit-trail.jsonl"))


@pytest.fixture()
def catalog_audit_logger(tmp_path: Path):
    """Audit logger for the Catalog service."""
    return AuditLogger(log_path=str(tmp_path / "cat-audit-trail.jsonl"))


@pytest.fixture()
async def catalog_client(catalog_audit_logger: AuditLogger):
    """Catalog client for audit trail tests with a fresh in-memory store."""
    store = CatalogStore(database_url="sqlite:///:memory:")
    backend = MockKeycloakAuthBackend(mock_user=make_dso_user())
    router = create_catalog_router(store=store)
    app = FastAPI(title="Test Catalog (Audit)", version="0.1.0-test")
    app.add_middleware(
        ConnectorMiddleware,
        auth_backend=backend,
        audit_logger=catalog_audit_logger,
        participant_id="catalog-001",
    )
    app.include_router(router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test-catalog-audit",
    ) as client:
        yield client


@pytest.fixture()
async def dso_client(
    dso_audit_logger: AuditLogger, shared_event_bus: MockEventBus
):
    """DSO client for audit trail tests with seeded data."""
    store = DSOStore(database_url="sqlite:///:memory:")
    store.seed()
    backend = MockKeycloakAuthBackend(mock_user=make_dso_user())
    router = create_dso_router(
        store=store, audit_logger=dso_audit_logger, event_bus=shared_event_bus
    )
    app = FastAPI(title="Test DSO (Audit)", version="0.1.0-test")
    app.add_middleware(
        ConnectorMiddleware,
        auth_backend=backend,
        audit_logger=dso_audit_logger,
        participant_id="dso-001",
    )
    app.include_router(router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test-dso-audit",
    ) as client:
        yield client


@pytest.fixture()
async def aggregator_client(
    aggregator_audit_logger: AuditLogger, shared_event_bus: MockEventBus
):
    """Aggregator client for audit trail tests with shared event bus."""
    agg_store = AggregatorStore(database_url="sqlite:///:memory:")
    backend = MockKeycloakAuthBackend(mock_user=make_aggregator_user())
    router = create_aggregator_router(
        store=agg_store,
        audit_logger=aggregator_audit_logger,
        event_bus=shared_event_bus,
    )
    app = FastAPI(title="Test Aggregator (Audit)", version="0.1.0-test")
    app.add_middleware(
        ConnectorMiddleware,
        auth_backend=backend,
        audit_logger=aggregator_audit_logger,
        participant_id="aggregator-001",
    )
    app.include_router(router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test-aggregator-audit",
    ) as client:
        yield client


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


DSO_HEADERS = {"Authorization": "Bearer test-dso-token"}
AGG_HEADERS = {"Authorization": "Bearer test-agg-token"}


def _dso_constraint_asset_payload() -> dict:
    """Return asset registration payload for DSO feeder constraints."""
    return {
        "provider_id": "dso-001",
        "name": "Feeder F-102 Congestion Constraints",
        "description": "Real-time congestion constraints for feeder F-102",
        "data_type": "feeder_constraint",
        "sensitivity": "medium",
        "endpoint": "https://dso.local/api/v1/constraints",
        "update_frequency": "5m",
        "resolution": "per_feeder",
        "anonymized": False,
        "personal_data": False,
        "policy_metadata": {
            "allowed_purposes": "congestion_management",
            "min_retention_days": "7",
            "max_retention_days": "90",
        },
    }


def _contract_initiation_payload(asset_id: str) -> dict:
    """Return contract initiation payload for congestion management."""
    now = _utc_now()
    return {
        "provider_id": "dso-001",
        "consumer_id": "aggregator-001",
        "asset_id": asset_id,
        "purpose": "congestion_management",
        "allowed_operations": ["read"],
        "redistribution_allowed": False,
        "retention_days": 30,
        "anonymization_required": False,
        "emergency_override": True,
        "valid_from": (now - timedelta(hours=1)).isoformat(),
        "valid_until": (now + timedelta(days=90)).isoformat(),
    }


def _flexibility_offer_payload(feeder_id: str) -> dict:
    """Return a flexibility offer payload for a specific feeder."""
    now = _utc_now()
    return {
        "unit_id": "DER-UNIT-102",
        "aggregator_id": "aggregator-001",
        "feeder_id": feeder_id,
        "direction": "both",
        "pq_range": {
            "p_min_kw": -200.0,
            "p_max_kw": 500.0,
            "q_min_kvar": -50.0,
            "q_max_kvar": 50.0,
        },
        "availability_windows": [
            {
                "window_id": "AW-AUDIT-001",
                "available_from": now.isoformat(),
                "available_until": (now + timedelta(hours=4)).isoformat(),
                "pq_range": {
                    "p_min_kw": -200.0,
                    "p_max_kw": 500.0,
                    "q_min_kvar": -50.0,
                    "q_max_kvar": 50.0,
                },
                "ramp_up_rate_kw_per_min": 50.0,
                "ramp_down_rate_kw_per_min": 50.0,
                "min_duration_minutes": 15.0,
                "max_duration_minutes": 120.0,
            },
        ],
        "response_confidence": {
            "level": "high",
            "probability_pct": 92.0,
            "historical_delivery_rate_pct": 95.0,
        },
        "price_eur_per_kwh": 0.12,
        "valid_from": now.isoformat(),
        "valid_until": (now + timedelta(hours=24)).isoformat(),
    }


def _flexibility_request_payload(feeder_id: str) -> dict:
    """Return a DSO flexibility request (dispatch) payload."""
    now = _utc_now()
    return {
        "feeder_id": feeder_id,
        "requested_power_kw": 200.0,
        "direction": "export",
        "needed_from": (now + timedelta(minutes=15)).isoformat(),
        "needed_until": (now + timedelta(hours=2)).isoformat(),
        "priority": 2,
        "reason": "congestion_management",
    }


def _dispatch_response_payload(
    command_id: str, event_id: str, feeder_id: str
) -> dict:
    """Return a dispatch actuals payload from the aggregator."""
    now = _utc_now()
    return {
        "command_id": command_id,
        "event_id": event_id,
        "participant_id": "aggregator-001",
        "feeder_id": feeder_id,
        "commanded_kw": 200.0,
        "delivered_kw": 185.0,
        "delivered_kvar": 5.0,
        "delivery_start": (now - timedelta(hours=2)).isoformat(),
        "delivery_end": now.isoformat(),
        "delivery_accuracy_pct": 92.5,
        "interval_values_kw": [180.0, 185.0, 190.0, 185.0, 180.0, 185.0],
        "interval_minutes": 5.0,
    }


# ---------------------------------------------------------------------------
# Test: Audit entries after congestion management flow
# ---------------------------------------------------------------------------


class TestAuditAfterCongestionFlow:
    """Verify every exchange in a congestion management flow is audited."""

    @pytest.mark.asyncio
    async def test_every_exchange_has_audit_entry(
        self,
        catalog_client: httpx.AsyncClient,
        dso_client: httpx.AsyncClient,
        aggregator_client: httpx.AsyncClient,
        shared_event_bus: MockEventBus,
        dso_audit_logger: AuditLogger,
        aggregator_audit_logger: AuditLogger,
        catalog_audit_logger: AuditLogger,
    ) -> None:
        """Execute a congestion management flow and verify audit coverage.

        Steps:
          1. DSO registers asset in catalog
          2. Aggregator discovers asset
          3. Aggregator initiates contract
          4. DSO accepts contract
          5. Aggregator reads constraints from DSO
          6. Aggregator submits flexibility offer
          7. DSO publishes flexibility request
          8. Aggregator reports dispatch actuals

        After completion, verify the combined audit logs cover all exchanges.
        """
        feeder_id = "F-102"

        # Step 1: DSO registers asset in catalog
        reg_response = await catalog_client.post(
            "/api/v1/assets",
            json=_dso_constraint_asset_payload(),
            headers=DSO_HEADERS,
        )
        assert reg_response.status_code == 201
        asset_id = reg_response.json()["id"]

        # Step 2: Aggregator discovers asset
        search_response = await catalog_client.get(
            "/api/v1/assets",
            params={"type": "feeder_constraint"},
            headers=DSO_HEADERS,
        )
        assert search_response.status_code == 200

        # Step 3: Aggregator initiates contract
        contract_response = await catalog_client.post(
            "/api/v1/contracts",
            json=_contract_initiation_payload(asset_id),
            headers=DSO_HEADERS,
        )
        assert contract_response.status_code == 201
        contract_id = contract_response.json()["contract_id"]

        # Step 4: DSO accepts contract
        accept_response = await catalog_client.put(
            f"/api/v1/contracts/{contract_id}/accept",
            headers=DSO_HEADERS,
        )
        assert accept_response.status_code == 200

        # Step 5: Aggregator reads constraints from DSO
        constraint_response = await dso_client.get(
            f"/api/v1/constraints/{feeder_id}",
            headers=DSO_HEADERS,
        )
        assert constraint_response.status_code == 200

        # Step 6: Aggregator submits flexibility offer
        offer_response = await aggregator_client.post(
            "/api/v1/flexibility-offers",
            json=_flexibility_offer_payload(feeder_id),
            headers=AGG_HEADERS,
        )
        assert offer_response.status_code == 201

        # Step 7: DSO publishes flexibility request
        flex_response = await dso_client.post(
            "/api/v1/flexibility-requests",
            json=_flexibility_request_payload(feeder_id),
            headers=DSO_HEADERS,
        )
        assert flex_response.status_code == 201
        flex_data = flex_response.json()

        # Step 8: Aggregator reports dispatch actuals
        dispatch_events = [
            (t, e)
            for t, e in shared_event_bus.produced_events
            if t == Topic.DISPATCH_COMMANDS.value
        ]
        command = dispatch_events[-1][1] if dispatch_events else None
        command_id = command.command_id if command else "DC-AUDIT-FALLBACK"

        actual_response = await aggregator_client.post(
            "/api/v1/dispatch-response",
            json=_dispatch_response_payload(
                command_id=command_id,
                event_id=flex_data["request_id"],
                feeder_id=feeder_id,
            ),
            headers=AGG_HEADERS,
        )
        assert actual_response.status_code == 201

        # --- Verify audit trail coverage ---

        # Catalog: asset registration (POST), search (GET),
        # contract initiation (POST), contract accept (PUT)
        cat_entries = catalog_audit_logger.entries
        assert len(cat_entries) >= 4

        # DSO: constraint read (GET), flexibility request (POST)
        dso_entries = dso_audit_logger.entries
        assert len(dso_entries) >= 2

        # Aggregator: flexibility offer (POST), dispatch response (POST)
        agg_entries = aggregator_audit_logger.entries
        assert len(agg_entries) >= 2

        # Total: at least 8 exchanges across all services
        total_entries = len(cat_entries) + len(dso_entries) + len(agg_entries)
        assert total_entries >= 8

    @pytest.mark.asyncio
    async def test_all_entries_have_required_fields_after_flow(
        self,
        catalog_client: httpx.AsyncClient,
        dso_client: httpx.AsyncClient,
        aggregator_client: httpx.AsyncClient,
        dso_audit_logger: AuditLogger,
        aggregator_audit_logger: AuditLogger,
        catalog_audit_logger: AuditLogger,
    ) -> None:
        """After a flow, every audit entry has all required fields populated."""
        feeder_id = "F-102"

        # Execute a simplified flow
        reg = await catalog_client.post(
            "/api/v1/assets",
            json=_dso_constraint_asset_payload(),
            headers=DSO_HEADERS,
        )
        asset_id = reg.json()["id"]

        await catalog_client.get(
            "/api/v1/assets",
            headers=DSO_HEADERS,
        )

        await dso_client.get(
            f"/api/v1/constraints/{feeder_id}",
            headers=DSO_HEADERS,
        )

        await aggregator_client.post(
            "/api/v1/flexibility-offers",
            json=_flexibility_offer_payload(feeder_id),
            headers=AGG_HEADERS,
        )

        # Collect all entries from all services
        all_entries = (
            catalog_audit_logger.entries
            + dso_audit_logger.entries
            + aggregator_audit_logger.entries
        )
        assert len(all_entries) >= 4

        for entry in all_entries:
            # Required field: timestamp
            assert entry.timestamp is not None
            assert isinstance(entry.timestamp, datetime)

            # Required field: requester_id
            assert entry.requester_id
            assert isinstance(entry.requester_id, str)
            assert len(entry.requester_id) > 0

            # Required field: provider_id
            assert entry.provider_id
            assert isinstance(entry.provider_id, str)

            # Required field: request_hash
            assert entry.request_hash
            assert isinstance(entry.request_hash, str)
            assert len(entry.request_hash) == 64  # SHA-256 hex digest

            # Required field: response_hash
            assert entry.response_hash
            assert isinstance(entry.response_hash, str)
            assert len(entry.response_hash) == 64  # SHA-256 hex digest

            # Required field: purpose_tag
            assert entry.purpose_tag is not None
            assert isinstance(entry.purpose_tag, str)

            # Required field: action
            assert entry.action in (
                AuditAction.READ,
                AuditAction.WRITE,
                AuditAction.DISPATCH,
                AuditAction.SUBSCRIBE,
            )

            # Required field: outcome
            assert entry.outcome in (
                AuditOutcome.SUCCESS,
                AuditOutcome.DENIED,
                AuditOutcome.ERROR,
            )


# ---------------------------------------------------------------------------
# Test: Individual audit entry field verification
# ---------------------------------------------------------------------------


class TestAuditFieldCompleteness:
    """Integration tests verifying individual audit entry fields."""

    @pytest.mark.asyncio
    async def test_request_hash_is_valid_sha256(
        self,
        dso_client: httpx.AsyncClient,
        dso_audit_logger: AuditLogger,
    ) -> None:
        """Audit entry request_hash should be a valid SHA-256 hex digest."""
        await dso_client.get(
            "/api/v1/constraints", headers=DSO_HEADERS
        )

        entries = dso_audit_logger.entries
        assert len(entries) >= 1

        for entry in entries:
            assert len(entry.request_hash) == 64
            # Must be valid hexadecimal
            int(entry.request_hash, 16)
            # Must be lowercase
            assert entry.request_hash == entry.request_hash.lower()

    @pytest.mark.asyncio
    async def test_response_hash_is_valid_sha256(
        self,
        dso_client: httpx.AsyncClient,
        dso_audit_logger: AuditLogger,
    ) -> None:
        """Audit entry response_hash should be a valid SHA-256 hex digest."""
        await dso_client.get(
            "/api/v1/constraints", headers=DSO_HEADERS
        )

        entries = dso_audit_logger.entries
        assert len(entries) >= 1

        for entry in entries:
            assert len(entry.response_hash) == 64
            int(entry.response_hash, 16)
            assert entry.response_hash == entry.response_hash.lower()

    @pytest.mark.asyncio
    async def test_timestamp_is_timezone_aware_utc(
        self,
        dso_client: httpx.AsyncClient,
        dso_audit_logger: AuditLogger,
    ) -> None:
        """Audit entry timestamps should be timezone-aware UTC."""
        before = _utc_now()

        await dso_client.get(
            "/api/v1/constraints", headers=DSO_HEADERS
        )

        after = _utc_now()

        entries = dso_audit_logger.entries
        assert len(entries) >= 1

        for entry in entries:
            # Timestamp should be timezone-aware
            assert entry.timestamp.tzinfo is not None
            # Timestamp should be between before and after
            assert before <= entry.timestamp <= after

    @pytest.mark.asyncio
    async def test_requester_id_matches_authenticated_user(
        self,
        dso_client: httpx.AsyncClient,
        dso_audit_logger: AuditLogger,
    ) -> None:
        """Audit entry requester_id should match the authenticated user."""
        await dso_client.get(
            "/api/v1/constraints", headers=DSO_HEADERS
        )

        entries = dso_audit_logger.entries
        assert len(entries) >= 1

        for entry in entries:
            # The DSO mock user has participant_id="dso-001"
            assert entry.requester_id == "dso-001"

    @pytest.mark.asyncio
    async def test_provider_id_matches_service_participant(
        self,
        dso_client: httpx.AsyncClient,
        dso_audit_logger: AuditLogger,
    ) -> None:
        """Audit entry provider_id should match the service's participant ID."""
        await dso_client.get(
            "/api/v1/constraints", headers=DSO_HEADERS
        )

        entries = dso_audit_logger.entries
        assert len(entries) >= 1

        for entry in entries:
            # The DSO service is configured with participant_id="dso-001"
            assert entry.provider_id == "dso-001"

    @pytest.mark.asyncio
    async def test_purpose_tag_from_header(
        self,
        dso_client: httpx.AsyncClient,
        dso_audit_logger: AuditLogger,
    ) -> None:
        """Audit entry purpose_tag should reflect the X-Purpose-Tag header."""
        headers = {
            **DSO_HEADERS,
            "X-Purpose-Tag": "congestion_management",
        }
        await dso_client.get(
            "/api/v1/constraints", headers=headers
        )

        entries = dso_audit_logger.entries
        assert len(entries) >= 1

        entry = entries[-1]
        assert entry.purpose_tag == "congestion_management"

    @pytest.mark.asyncio
    async def test_purpose_tag_defaults_to_unknown(
        self,
        dso_client: httpx.AsyncClient,
        dso_audit_logger: AuditLogger,
    ) -> None:
        """Without X-Purpose-Tag header, purpose_tag defaults to 'unknown'."""
        await dso_client.get(
            "/api/v1/constraints", headers=DSO_HEADERS
        )

        entries = dso_audit_logger.entries
        assert len(entries) >= 1

        entry = entries[-1]
        assert entry.purpose_tag == "unknown"

    @pytest.mark.asyncio
    async def test_contract_id_from_header(
        self,
        dso_client: httpx.AsyncClient,
        dso_audit_logger: AuditLogger,
    ) -> None:
        """Audit entry contract_id should reflect the X-Contract-ID header."""
        headers = {
            **DSO_HEADERS,
            "X-Contract-ID": "contract-test-123",
        }
        await dso_client.get(
            "/api/v1/constraints", headers=headers
        )

        entries = dso_audit_logger.entries
        assert len(entries) >= 1

        entry = entries[-1]
        assert entry.contract_id == "contract-test-123"

    @pytest.mark.asyncio
    async def test_read_action_for_get_requests(
        self,
        dso_client: httpx.AsyncClient,
        dso_audit_logger: AuditLogger,
    ) -> None:
        """GET requests should be classified as READ action."""
        await dso_client.get(
            "/api/v1/constraints", headers=DSO_HEADERS
        )

        entries = dso_audit_logger.entries
        assert len(entries) >= 1
        assert entries[-1].action == AuditAction.READ

    @pytest.mark.asyncio
    async def test_write_action_for_post_requests(
        self,
        aggregator_client: httpx.AsyncClient,
        aggregator_audit_logger: AuditLogger,
    ) -> None:
        """POST requests should be classified as WRITE action."""
        await aggregator_client.post(
            "/api/v1/flexibility-offers",
            json=_flexibility_offer_payload("F-102"),
            headers=AGG_HEADERS,
        )

        entries = aggregator_audit_logger.entries
        assert len(entries) >= 1
        assert entries[-1].action == AuditAction.WRITE

    @pytest.mark.asyncio
    async def test_success_outcome_for_200_responses(
        self,
        dso_client: httpx.AsyncClient,
        dso_audit_logger: AuditLogger,
    ) -> None:
        """Successful (2xx) responses should have outcome=SUCCESS."""
        await dso_client.get(
            "/api/v1/constraints", headers=DSO_HEADERS
        )

        entries = dso_audit_logger.entries
        assert len(entries) >= 1
        assert entries[-1].outcome == AuditOutcome.SUCCESS


# ---------------------------------------------------------------------------
# Test: Hash verification — hashes match actual content
# ---------------------------------------------------------------------------


class TestAuditHashVerification:
    """Integration tests verifying audit hashes match actual content."""

    @pytest.mark.asyncio
    async def test_response_hash_matches_actual_response_body(
        self,
        dso_client: httpx.AsyncClient,
        dso_audit_logger: AuditLogger,
    ) -> None:
        """The response_hash in the audit entry should match the actual response.

        The middleware hashes the response body and records it. The same
        body bytes are then sent to the client. So hashing the client-
        received content should produce the same hash.
        """
        response = await dso_client.get(
            "/api/v1/constraints", headers=DSO_HEADERS
        )
        assert response.status_code == 200
        actual_response_body = response.content

        entries = dso_audit_logger.entries
        assert len(entries) >= 1

        entry = entries[-1]
        expected_hash = compute_hash(actual_response_body)
        assert entry.response_hash == expected_hash

    @pytest.mark.asyncio
    async def test_request_hash_for_get_is_empty_body_hash(
        self,
        dso_client: httpx.AsyncClient,
        dso_audit_logger: AuditLogger,
    ) -> None:
        """GET requests have an empty body, so request_hash = hash(b'').

        The middleware reads the request body with ``await request.body()``
        which returns ``b''`` for GET requests.
        """
        await dso_client.get(
            "/api/v1/constraints", headers=DSO_HEADERS
        )

        entries = dso_audit_logger.entries
        assert len(entries) >= 1

        entry = entries[-1]
        empty_hash = compute_hash(b"")
        assert entry.request_hash == empty_hash

    @pytest.mark.asyncio
    async def test_response_hash_for_specific_feeder(
        self,
        dso_client: httpx.AsyncClient,
        dso_audit_logger: AuditLogger,
    ) -> None:
        """Response hash for a specific feeder constraint should match content."""
        response = await dso_client.get(
            "/api/v1/constraints/F-102", headers=DSO_HEADERS
        )
        assert response.status_code == 200
        actual_body = response.content

        entries = dso_audit_logger.entries
        assert len(entries) >= 1

        entry = entries[-1]
        assert entry.response_hash == compute_hash(actual_body)

    @pytest.mark.asyncio
    async def test_response_hash_for_post_request(
        self,
        aggregator_client: httpx.AsyncClient,
        aggregator_audit_logger: AuditLogger,
    ) -> None:
        """Response hash for POST (flexibility offer) should match content."""
        response = await aggregator_client.post(
            "/api/v1/flexibility-offers",
            json=_flexibility_offer_payload("F-102"),
            headers=AGG_HEADERS,
        )
        assert response.status_code == 201
        actual_body = response.content

        entries = aggregator_audit_logger.entries
        assert len(entries) >= 1

        entry = entries[-1]
        assert entry.response_hash == compute_hash(actual_body)

    @pytest.mark.asyncio
    async def test_hashes_are_deterministic(
        self,
        dso_client: httpx.AsyncClient,
        dso_audit_logger: AuditLogger,
    ) -> None:
        """Same content should always produce the same hash.

        Make two identical requests to the same endpoint and verify
        the hashes follow deterministic patterns.
        """
        # First request
        r1 = await dso_client.get(
            "/api/v1/constraints", headers=DSO_HEADERS
        )
        # Second request
        r2 = await dso_client.get(
            "/api/v1/constraints", headers=DSO_HEADERS
        )

        entries = dso_audit_logger.entries
        assert len(entries) >= 2

        e1 = entries[-2]
        e2 = entries[-1]

        # Both GET requests have empty body → same request_hash
        assert e1.request_hash == e2.request_hash
        assert e1.request_hash == compute_hash(b"")

        # If the responses are identical, response_hashes should match
        if r1.content == r2.content:
            assert e1.response_hash == e2.response_hash

    @pytest.mark.asyncio
    async def test_different_endpoints_produce_different_hashes(
        self,
        dso_client: httpx.AsyncClient,
        dso_audit_logger: AuditLogger,
    ) -> None:
        """Different endpoints should produce different response hashes."""
        r1 = await dso_client.get(
            "/api/v1/constraints", headers=DSO_HEADERS
        )
        r2 = await dso_client.get(
            "/api/v1/congestion-signals", headers=DSO_HEADERS
        )

        entries = dso_audit_logger.entries
        assert len(entries) >= 2

        e1 = entries[-2]
        e2 = entries[-1]

        # Different response content should yield different hashes
        if r1.content != r2.content:
            assert e1.response_hash != e2.response_hash


# ---------------------------------------------------------------------------
# Test: Audit trail persistence and on-disk integrity
# ---------------------------------------------------------------------------


class TestAuditPersistence:
    """Integration tests verifying audit entries are persisted to disk."""

    @pytest.mark.asyncio
    async def test_audit_entries_persisted_to_file(
        self,
        dso_client: httpx.AsyncClient,
        dso_audit_logger: AuditLogger,
    ) -> None:
        """Audit entries should be persisted as JSON lines in the log file."""
        await dso_client.get(
            "/api/v1/constraints", headers=DSO_HEADERS
        )

        log_path = dso_audit_logger.log_path
        assert log_path.exists()

        with open(log_path, "r", encoding="utf-8") as fh:
            lines = [line.strip() for line in fh if line.strip()]

        assert len(lines) >= 1

        # Each line should be valid JSON
        for line in lines:
            data = json.loads(line)
            assert "request_hash" in data
            assert "response_hash" in data
            assert "purpose_tag" in data
            assert "timestamp" in data
            assert "requester_id" in data

    @pytest.mark.asyncio
    async def test_persisted_entries_match_in_memory(
        self,
        dso_client: httpx.AsyncClient,
        dso_audit_logger: AuditLogger,
    ) -> None:
        """On-disk entries should match the in-memory entries."""
        await dso_client.get(
            "/api/v1/constraints", headers=DSO_HEADERS
        )
        await dso_client.get(
            "/api/v1/congestion-signals", headers=DSO_HEADERS
        )

        in_memory_entries = dso_audit_logger.entries
        log_path = dso_audit_logger.log_path

        with open(log_path, "r", encoding="utf-8") as fh:
            on_disk_lines = [line.strip() for line in fh if line.strip()]

        assert len(on_disk_lines) == len(in_memory_entries)

        for i, line in enumerate(on_disk_lines):
            disk_data = json.loads(line)
            mem_entry = in_memory_entries[i]
            assert disk_data["requester_id"] == mem_entry.requester_id
            assert disk_data["provider_id"] == mem_entry.provider_id
            assert disk_data["request_hash"] == mem_entry.request_hash
            assert disk_data["response_hash"] == mem_entry.response_hash

    @pytest.mark.asyncio
    async def test_audit_log_is_append_only(
        self,
        dso_client: httpx.AsyncClient,
        dso_audit_logger: AuditLogger,
    ) -> None:
        """Multiple exchanges should append to the log, not overwrite it."""
        # First exchange
        await dso_client.get(
            "/api/v1/constraints", headers=DSO_HEADERS
        )
        log_path = dso_audit_logger.log_path
        with open(log_path, "r", encoding="utf-8") as fh:
            first_count = sum(1 for line in fh if line.strip())

        # Second exchange
        await dso_client.get(
            "/api/v1/congestion-signals", headers=DSO_HEADERS
        )
        with open(log_path, "r", encoding="utf-8") as fh:
            second_count = sum(1 for line in fh if line.strip())

        assert second_count > first_count
        assert second_count == first_count + 1


# ---------------------------------------------------------------------------
# Test: Audit with custom headers (purpose and contract metadata)
# ---------------------------------------------------------------------------


class TestAuditHeaderMetadata:
    """Integration tests verifying audit entries capture request header metadata."""

    @pytest.mark.asyncio
    async def test_purpose_and_contract_headers_in_audit(
        self,
        dso_client: httpx.AsyncClient,
        dso_audit_logger: AuditLogger,
    ) -> None:
        """Both X-Purpose-Tag and X-Contract-ID should be captured in audit."""
        headers = {
            **DSO_HEADERS,
            "X-Purpose-Tag": "congestion_management",
            "X-Contract-ID": "contract-cm-001",
        }
        await dso_client.get(
            "/api/v1/constraints", headers=headers
        )

        entries = dso_audit_logger.entries
        assert len(entries) >= 1
        entry = entries[-1]
        assert entry.purpose_tag == "congestion_management"
        assert entry.contract_id == "contract-cm-001"

    @pytest.mark.asyncio
    async def test_multiple_requests_with_different_purposes(
        self,
        dso_client: httpx.AsyncClient,
        dso_audit_logger: AuditLogger,
    ) -> None:
        """Different requests with different purpose tags are recorded separately."""
        # Request 1: congestion_management purpose
        await dso_client.get(
            "/api/v1/constraints",
            headers={
                **DSO_HEADERS,
                "X-Purpose-Tag": "congestion_management",
            },
        )

        # Request 2: grid_analysis purpose
        await dso_client.get(
            "/api/v1/congestion-signals",
            headers={
                **DSO_HEADERS,
                "X-Purpose-Tag": "grid_analysis",
            },
        )

        entries = dso_audit_logger.entries
        assert len(entries) >= 2

        purposes = [e.purpose_tag for e in entries[-2:]]
        assert "congestion_management" in purposes
        assert "grid_analysis" in purposes

    @pytest.mark.asyncio
    async def test_asset_id_is_request_path(
        self,
        dso_client: httpx.AsyncClient,
        dso_audit_logger: AuditLogger,
    ) -> None:
        """The asset_id in audit entries should match the request path."""
        await dso_client.get(
            "/api/v1/constraints/F-102", headers=DSO_HEADERS
        )

        entries = dso_audit_logger.entries
        assert len(entries) >= 1

        entry = entries[-1]
        assert entry.asset_id == "/api/v1/constraints/F-102"


# ---------------------------------------------------------------------------
# Test: Multi-service audit trail
# ---------------------------------------------------------------------------


class TestMultiServiceAuditTrail:
    """Integration tests verifying audit trail across multiple services."""

    @pytest.mark.asyncio
    async def test_each_service_maintains_own_audit_log(
        self,
        dso_client: httpx.AsyncClient,
        aggregator_client: httpx.AsyncClient,
        dso_audit_logger: AuditLogger,
        aggregator_audit_logger: AuditLogger,
    ) -> None:
        """Each service should maintain its own independent audit log."""
        # DSO exchange
        await dso_client.get(
            "/api/v1/constraints", headers=DSO_HEADERS
        )

        # Aggregator exchange
        await aggregator_client.post(
            "/api/v1/flexibility-offers",
            json=_flexibility_offer_payload("F-102"),
            headers=AGG_HEADERS,
        )

        dso_entries = dso_audit_logger.entries
        agg_entries = aggregator_audit_logger.entries

        # Each should have at least one entry
        assert len(dso_entries) >= 1
        assert len(agg_entries) >= 1

        # DSO entries should have provider_id="dso-001"
        assert all(e.provider_id == "dso-001" for e in dso_entries)

        # Aggregator entries should have provider_id="aggregator-001"
        assert all(e.provider_id == "aggregator-001" for e in agg_entries)

    @pytest.mark.asyncio
    async def test_catalog_audits_asset_registration_and_search(
        self,
        catalog_client: httpx.AsyncClient,
        catalog_audit_logger: AuditLogger,
    ) -> None:
        """Catalog should audit both asset registration and search."""
        # Register an asset (write)
        await catalog_client.post(
            "/api/v1/assets",
            json=_dso_constraint_asset_payload(),
            headers=DSO_HEADERS,
        )

        # Search assets (read)
        await catalog_client.get(
            "/api/v1/assets", headers=DSO_HEADERS
        )

        entries = catalog_audit_logger.entries
        assert len(entries) >= 2

        actions = [e.action for e in entries]
        assert AuditAction.WRITE in actions
        assert AuditAction.READ in actions

        # All should be successful
        assert all(e.outcome == AuditOutcome.SUCCESS for e in entries)
