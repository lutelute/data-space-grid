"""Integration tests for the full congestion management use case (end-to-end).

Verifies the complete workflow:
  1. DSO publishes feeder constraint to the federated catalog.
  2. Aggregator discovers the constraint asset.
  3. Aggregator negotiates and activates a contract with the DSO.
  4. Aggregator reads the feeder constraint data.
  5. Aggregator submits a flexibility offer.
  6. DSO publishes a flexibility request (dispatch via events).
  7. Aggregator reports dispatch actuals.
  8. All steps are audited.

These tests create fresh in-memory instances of all services (catalog,
DSO, aggregator) with mock authentication and in-memory event bus so
that the full flow can be exercised without external infrastructure.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI

from src.catalog.routes import create_router as create_catalog_router
from src.catalog.store import CatalogStore
from src.connector.audit import AuditLogger
from src.connector.events import Topic
from src.connector.middleware import ConnectorMiddleware
from src.connector.models import AuditAction, AuditOutcome, ContractStatus
from src.participants.aggregator.routes import (
    create_router as create_aggregator_router,
)
from src.participants.aggregator.store import AggregatorStore
from src.participants.dso.routes import create_router as create_dso_router
from src.participants.dso.store import DSOStore
from src.semantic.cim import SensitivityTier
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
    return AuditLogger(log_path=str(tmp_path / "dso-e2e-audit.jsonl"))


@pytest.fixture()
def aggregator_audit_logger(tmp_path: Path):
    """Audit logger for the Aggregator node."""
    return AuditLogger(log_path=str(tmp_path / "agg-e2e-audit.jsonl"))


@pytest.fixture()
def catalog_audit_logger(tmp_path: Path):
    """Audit logger for the Catalog service."""
    return AuditLogger(log_path=str(tmp_path / "catalog-e2e-audit.jsonl"))


@pytest.fixture()
async def catalog_client_e2e(catalog_audit_logger: AuditLogger):
    """Catalog client for E2E tests with a fresh in-memory store."""
    store = CatalogStore(database_url="sqlite:///:memory:")
    backend = MockKeycloakAuthBackend(mock_user=make_dso_user())
    router = create_catalog_router(store=store)
    app = FastAPI(title="Test Catalog E2E", version="0.1.0-test")
    app.add_middleware(
        ConnectorMiddleware,
        auth_backend=backend,
        audit_logger=catalog_audit_logger,
        participant_id="catalog-001",
    )
    app.include_router(router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test-catalog"
    ) as client:
        yield client


@pytest.fixture()
async def dso_client_e2e(
    dso_audit_logger: AuditLogger, shared_event_bus: MockEventBus
):
    """DSO client for E2E tests with seeded data and shared event bus."""
    store = DSOStore(database_url="sqlite:///:memory:")
    store.seed()
    backend = MockKeycloakAuthBackend(mock_user=make_dso_user())
    router = create_dso_router(
        store=store, audit_logger=dso_audit_logger, event_bus=shared_event_bus
    )
    app = FastAPI(title="Test DSO E2E", version="0.1.0-test")
    app.add_middleware(
        ConnectorMiddleware,
        auth_backend=backend,
        audit_logger=dso_audit_logger,
        participant_id="dso-001",
    )
    app.include_router(router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test-dso"
    ) as client:
        yield client


@pytest.fixture()
async def aggregator_client_e2e(
    aggregator_audit_logger: AuditLogger, shared_event_bus: MockEventBus
):
    """Aggregator client for E2E tests with shared event bus."""
    agg_store = AggregatorStore(database_url="sqlite:///:memory:")
    backend = MockKeycloakAuthBackend(mock_user=make_aggregator_user())
    router = create_aggregator_router(
        store=agg_store,
        audit_logger=aggregator_audit_logger,
        event_bus=shared_event_bus,
    )
    app = FastAPI(title="Test Aggregator E2E", version="0.1.0-test")
    app.add_middleware(
        ConnectorMiddleware,
        auth_backend=backend,
        audit_logger=aggregator_audit_logger,
        participant_id="aggregator-001",
    )
    app.include_router(router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test-aggregator",
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
                "window_id": "AW-E2E-001",
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
# Test: Full E2E congestion management flow
# ---------------------------------------------------------------------------


class TestCongestionManagementE2E:
    """Full end-to-end integration test for the congestion management use case."""

    @pytest.mark.asyncio
    async def test_full_congestion_management_flow(
        self,
        catalog_client_e2e: httpx.AsyncClient,
        dso_client_e2e: httpx.AsyncClient,
        aggregator_client_e2e: httpx.AsyncClient,
        shared_event_bus: MockEventBus,
        dso_audit_logger: AuditLogger,
        aggregator_audit_logger: AuditLogger,
    ) -> None:
        """Execute the complete congestion management flow end-to-end.

        Steps:
          1. DSO publishes constraint asset to catalog
          2. Aggregator discovers the asset
          3. Aggregator negotiates contract
          4. DSO accepts contract
          5. Aggregator reads constraint data from DSO
          6. Aggregator submits flexibility offer
          7. DSO publishes flexibility request (dispatch via events)
          8. Aggregator reports dispatch actuals
          9. Verify audit trail completeness
        """
        feeder_id = "F-102"

        # ----- Step 1: DSO publishes constraint asset to catalog -----
        reg_response = await catalog_client_e2e.post(
            "/api/v1/assets",
            json=_dso_constraint_asset_payload(),
            headers=DSO_HEADERS,
        )
        assert reg_response.status_code == 201
        asset_id = reg_response.json()["id"]
        assert asset_id

        # ----- Step 2: Aggregator discovers the asset -----
        search_response = await catalog_client_e2e.get(
            "/api/v1/assets",
            params={"type": "feeder_constraint", "provider": "dso-001"},
            headers=DSO_HEADERS,
        )
        assert search_response.status_code == 200
        discovered_assets = search_response.json()
        assert len(discovered_assets) >= 1
        discovered_asset = discovered_assets[0]
        assert discovered_asset["id"] == asset_id
        assert discovered_asset["data_type"] == "feeder_constraint"
        assert discovered_asset["policy_metadata"]["allowed_purposes"] == "congestion_management"

        # ----- Step 3: Aggregator initiates contract negotiation -----
        contract_payload = _contract_initiation_payload(asset_id)
        contract_response = await catalog_client_e2e.post(
            "/api/v1/contracts",
            json=contract_payload,
            headers=DSO_HEADERS,
        )
        assert contract_response.status_code == 201
        contract = contract_response.json()
        contract_id = contract["contract_id"]
        assert contract["status"] == "offered"
        assert contract["purpose"] == "congestion_management"

        # ----- Step 4: DSO accepts contract -----
        accept_response = await catalog_client_e2e.put(
            f"/api/v1/contracts/{contract_id}/accept",
            headers=DSO_HEADERS,
        )
        assert accept_response.status_code == 200
        active_contract = accept_response.json()
        assert active_contract["status"] == "active"

        # ----- Step 5: Aggregator reads constraint data from DSO -----
        constraints_response = await dso_client_e2e.get(
            f"/api/v1/constraints/{feeder_id}",
            headers=DSO_HEADERS,
        )
        assert constraints_response.status_code == 200
        constraint_data = constraints_response.json()
        assert constraint_data["feeder_id"] == feeder_id
        assert constraint_data["congestion_level"] > 0
        assert "max_active_power_kw" in constraint_data
        assert "sensitivity" in constraint_data

        # ----- Step 6: Aggregator submits flexibility offer -----
        offer_response = await aggregator_client_e2e.post(
            "/api/v1/flexibility-offers",
            json=_flexibility_offer_payload(feeder_id),
            headers=AGG_HEADERS,
        )
        assert offer_response.status_code == 201
        offer = offer_response.json()
        assert offer["feeder_id"] == feeder_id
        assert "envelope_id" in offer
        assert offer["direction"] == "both"
        assert offer["pq_range"]["p_max_kw"] == 500.0

        # ----- Step 7: DSO publishes flexibility request (dispatch) -----
        flex_request_response = await dso_client_e2e.post(
            "/api/v1/flexibility-requests",
            json=_flexibility_request_payload(feeder_id),
            headers=DSO_HEADERS,
        )
        assert flex_request_response.status_code == 201
        flex_request = flex_request_response.json()
        assert flex_request["feeder_id"] == feeder_id
        assert flex_request["requested_power_kw"] == 200.0
        request_id = flex_request["request_id"]

        # Verify event was published to event bus
        dispatch_events = [
            (t, e)
            for t, e in shared_event_bus.produced_events
            if t == Topic.DISPATCH_COMMANDS.value
        ]
        assert len(dispatch_events) >= 1
        dispatch_command = dispatch_events[-1][1]
        assert dispatch_command.feeder_id == feeder_id
        assert dispatch_command.target_power_kw == 200.0

        # ----- Step 8: Aggregator reports dispatch actuals -----
        dispatch_payload = _dispatch_response_payload(
            command_id=dispatch_command.command_id,
            event_id=request_id,
            feeder_id=feeder_id,
        )
        actuals_response = await aggregator_client_e2e.post(
            "/api/v1/dispatch-response",
            json=dispatch_payload,
            headers=AGG_HEADERS,
        )
        assert actuals_response.status_code == 201
        actuals = actuals_response.json()
        assert actuals["feeder_id"] == feeder_id
        assert actuals["commanded_kw"] == 200.0
        assert actuals["delivered_kw"] == 185.0
        assert actuals["delivery_accuracy_pct"] == 92.5
        assert "actual_id" in actuals

        # Verify dispatch actuals event was published
        actual_events = [
            (t, e)
            for t, e in shared_event_bus.produced_events
            if t == Topic.DISPATCH_ACTUALS.value
        ]
        assert len(actual_events) >= 1
        actual_event = actual_events[-1][1]
        assert actual_event.delivered_kw == 185.0

        # ----- Step 9: Verify audit trail completeness -----
        # Both DSO and Aggregator should have audit entries
        dso_entries = dso_audit_logger.entries
        agg_entries = aggregator_audit_logger.entries

        # DSO should have audit entries for constraint read and
        # flexibility request write
        assert len(dso_entries) >= 2

        # Aggregator should have audit entries for flexibility offer
        # and dispatch response
        assert len(agg_entries) >= 2

        # Verify all audit entries have required fields
        all_entries = dso_entries + agg_entries
        for entry in all_entries:
            assert entry.requester_id
            assert entry.provider_id
            assert entry.request_hash
            assert entry.response_hash
            assert entry.action
            assert entry.outcome
            assert entry.timestamp


# ---------------------------------------------------------------------------
# Test: Individual steps of the congestion management flow
# ---------------------------------------------------------------------------


class TestCongestionStepByStep:
    """Test individual steps of the congestion management flow independently."""

    @pytest.mark.asyncio
    async def test_dso_publishes_constraint_to_catalog(
        self, catalog_client_e2e: httpx.AsyncClient
    ) -> None:
        """DSO can register a feeder constraint asset in the catalog."""
        response = await catalog_client_e2e.post(
            "/api/v1/assets",
            json=_dso_constraint_asset_payload(),
            headers=DSO_HEADERS,
        )
        assert response.status_code == 201
        data = response.json()
        assert data["provider_id"] == "dso-001"
        assert data["data_type"] == "feeder_constraint"
        assert data["sensitivity"] == "medium"

    @pytest.mark.asyncio
    async def test_aggregator_reads_feeder_constraints(
        self, dso_client_e2e: httpx.AsyncClient
    ) -> None:
        """Aggregator can read feeder constraints from the DSO node."""
        response = await dso_client_e2e.get(
            "/api/v1/constraints",
            headers=DSO_HEADERS,
        )
        assert response.status_code == 200
        constraints = response.json()
        assert len(constraints) >= 1

        # Verify congested feeder F-102 is present
        feeder_ids = [c["feeder_id"] for c in constraints]
        assert "F-102" in feeder_ids

        # Check F-102 has high congestion
        f102 = [c for c in constraints if c["feeder_id"] == "F-102"][0]
        assert f102["congestion_level"] == 0.85

    @pytest.mark.asyncio
    async def test_aggregator_submits_flexibility_offer(
        self, aggregator_client_e2e: httpx.AsyncClient
    ) -> None:
        """Aggregator can submit a flexibility offer for a congested feeder."""
        response = await aggregator_client_e2e.post(
            "/api/v1/flexibility-offers",
            json=_flexibility_offer_payload("F-102"),
            headers=AGG_HEADERS,
        )
        assert response.status_code == 201
        offer = response.json()
        assert offer["feeder_id"] == "F-102"
        assert offer["direction"] == "both"
        assert offer["envelope_id"].startswith("FE-AGG-")
        assert offer["sensitivity"] == "medium"

    @pytest.mark.asyncio
    async def test_dso_dispatches_via_events(
        self,
        dso_client_e2e: httpx.AsyncClient,
        shared_event_bus: MockEventBus,
    ) -> None:
        """DSO flexibility request publishes a dispatch command to event bus."""
        response = await dso_client_e2e.post(
            "/api/v1/flexibility-requests",
            json=_flexibility_request_payload("F-102"),
            headers=DSO_HEADERS,
        )
        assert response.status_code == 201

        # Verify DispatchCommand was published
        dispatch_events = [
            (t, e)
            for t, e in shared_event_bus.produced_events
            if t == Topic.DISPATCH_COMMANDS.value
        ]
        assert len(dispatch_events) >= 1
        command = dispatch_events[-1][1]
        assert command.feeder_id == "F-102"
        assert command.target_power_kw == 200.0
        assert command.issuer_id == "dso-001"
        assert command.target_participant_id == "aggregator-001"

    @pytest.mark.asyncio
    async def test_aggregator_reports_actuals(
        self,
        aggregator_client_e2e: httpx.AsyncClient,
        shared_event_bus: MockEventBus,
    ) -> None:
        """Aggregator can report dispatch actuals back to DSO via API and events."""
        payload = _dispatch_response_payload(
            command_id="DC-DSO-TEST-001",
            event_id="EVT-TEST-001",
            feeder_id="F-102",
        )
        response = await aggregator_client_e2e.post(
            "/api/v1/dispatch-response",
            json=payload,
            headers=AGG_HEADERS,
        )
        assert response.status_code == 201
        actual = response.json()
        assert actual["feeder_id"] == "F-102"
        assert actual["commanded_kw"] == 200.0
        assert actual["delivered_kw"] == 185.0
        assert actual["actual_id"].startswith("DA-AGG-")

        # Verify DispatchActual was published to event bus
        actual_events = [
            (t, e)
            for t, e in shared_event_bus.produced_events
            if t == Topic.DISPATCH_ACTUALS.value
        ]
        assert len(actual_events) >= 1
        actual_event = actual_events[-1][1]
        assert actual_event.delivered_kw == 185.0
        assert actual_event.feeder_id == "F-102"


# ---------------------------------------------------------------------------
# Test: Audit trail completeness
# ---------------------------------------------------------------------------


class TestAuditTrailCompleteness:
    """Integration tests verifying audit entries are generated for all exchanges."""

    @pytest.mark.asyncio
    async def test_dso_constraint_read_generates_audit_entry(
        self,
        dso_client_e2e: httpx.AsyncClient,
        dso_audit_logger: AuditLogger,
    ) -> None:
        """Reading feeder constraints from DSO should generate an audit entry."""
        # Read constraints
        await dso_client_e2e.get(
            "/api/v1/constraints", headers=DSO_HEADERS
        )

        # Verify audit entry was created
        entries = dso_audit_logger.entries
        assert len(entries) >= 1
        read_entries = [
            e for e in entries if e.action == AuditAction.READ
        ]
        assert len(read_entries) >= 1

        entry = read_entries[-1]
        assert entry.request_hash
        assert entry.response_hash
        assert entry.outcome == AuditOutcome.SUCCESS

    @pytest.mark.asyncio
    async def test_flexibility_request_generates_audit_entry(
        self,
        dso_client_e2e: httpx.AsyncClient,
        dso_audit_logger: AuditLogger,
    ) -> None:
        """DSO flexibility request (write) should generate an audit entry."""
        await dso_client_e2e.post(
            "/api/v1/flexibility-requests",
            json=_flexibility_request_payload("F-102"),
            headers=DSO_HEADERS,
        )

        entries = dso_audit_logger.entries
        write_entries = [
            e for e in entries if e.action == AuditAction.WRITE
        ]
        assert len(write_entries) >= 1

        entry = write_entries[-1]
        assert entry.request_hash
        assert entry.response_hash
        assert entry.outcome == AuditOutcome.SUCCESS

    @pytest.mark.asyncio
    async def test_dispatch_response_generates_audit_entry(
        self,
        aggregator_client_e2e: httpx.AsyncClient,
        aggregator_audit_logger: AuditLogger,
    ) -> None:
        """Aggregator dispatch response (write) should generate an audit entry."""
        payload = _dispatch_response_payload(
            command_id="DC-AUDIT-001",
            event_id="EVT-AUDIT-001",
            feeder_id="F-102",
        )
        await aggregator_client_e2e.post(
            "/api/v1/dispatch-response",
            json=payload,
            headers=AGG_HEADERS,
        )

        entries = aggregator_audit_logger.entries
        write_entries = [
            e for e in entries if e.action == AuditAction.WRITE
        ]
        assert len(write_entries) >= 1

        entry = write_entries[-1]
        assert entry.request_hash
        assert entry.response_hash
        assert entry.outcome == AuditOutcome.SUCCESS

    @pytest.mark.asyncio
    async def test_audit_entries_have_all_required_fields(
        self,
        dso_client_e2e: httpx.AsyncClient,
        dso_audit_logger: AuditLogger,
    ) -> None:
        """Every audit entry should have all required fields populated."""
        # Generate some exchanges
        await dso_client_e2e.get(
            "/api/v1/constraints", headers=DSO_HEADERS
        )
        await dso_client_e2e.get(
            "/api/v1/congestion-signals", headers=DSO_HEADERS
        )

        entries = dso_audit_logger.entries
        assert len(entries) >= 2

        for entry in entries:
            # All required fields must be present and non-empty
            assert entry.timestamp is not None
            assert entry.requester_id
            assert entry.provider_id
            assert entry.request_hash
            assert len(entry.request_hash) == 64  # SHA-256 hex
            assert entry.response_hash
            assert len(entry.response_hash) == 64  # SHA-256 hex
            assert entry.action in (
                AuditAction.READ,
                AuditAction.WRITE,
                AuditAction.DISPATCH,
                AuditAction.SUBSCRIBE,
            )
            assert entry.outcome in (
                AuditOutcome.SUCCESS,
                AuditOutcome.DENIED,
                AuditOutcome.ERROR,
            )

    @pytest.mark.asyncio
    async def test_audit_hashes_are_valid_sha256(
        self,
        dso_client_e2e: httpx.AsyncClient,
        dso_audit_logger: AuditLogger,
    ) -> None:
        """Audit entry hashes should be valid SHA-256 hex digests."""
        await dso_client_e2e.get(
            "/api/v1/constraints", headers=DSO_HEADERS
        )

        entries = dso_audit_logger.entries
        assert len(entries) >= 1

        for entry in entries:
            # SHA-256 produces 64 hex characters
            assert len(entry.request_hash) == 64
            assert len(entry.response_hash) == 64
            # Must be valid hex
            int(entry.request_hash, 16)
            int(entry.response_hash, 16)
            # Must be lowercase
            assert entry.request_hash == entry.request_hash.lower()
            assert entry.response_hash == entry.response_hash.lower()


# ---------------------------------------------------------------------------
# Test: Event bus integration
# ---------------------------------------------------------------------------


class TestEventBusIntegration:
    """Integration tests for event bus message flow between services."""

    @pytest.mark.asyncio
    async def test_dispatch_command_includes_correct_metadata(
        self,
        dso_client_e2e: httpx.AsyncClient,
        shared_event_bus: MockEventBus,
    ) -> None:
        """Dispatch commands should include all required metadata fields."""
        await dso_client_e2e.post(
            "/api/v1/flexibility-requests",
            json=_flexibility_request_payload("F-102"),
            headers=DSO_HEADERS,
        )

        dispatch_events = [
            (t, e)
            for t, e in shared_event_bus.produced_events
            if t == Topic.DISPATCH_COMMANDS.value
        ]
        assert len(dispatch_events) >= 1
        command = dispatch_events[-1][1]

        # All required fields
        assert command.command_id
        assert command.event_id
        assert command.issuer_id == "dso-001"
        assert command.target_participant_id == "aggregator-001"
        assert command.feeder_id == "F-102"
        assert command.target_power_kw == 200.0
        assert command.activation_time is not None
        assert command.duration_minutes > 0
        assert command.sensitivity == SensitivityTier.MEDIUM

    @pytest.mark.asyncio
    async def test_dispatch_actual_includes_correct_metadata(
        self,
        aggregator_client_e2e: httpx.AsyncClient,
        shared_event_bus: MockEventBus,
    ) -> None:
        """Dispatch actuals should include all required metadata fields."""
        payload = _dispatch_response_payload(
            command_id="DC-META-001",
            event_id="EVT-META-001",
            feeder_id="F-102",
        )
        await aggregator_client_e2e.post(
            "/api/v1/dispatch-response",
            json=payload,
            headers=AGG_HEADERS,
        )

        actual_events = [
            (t, e)
            for t, e in shared_event_bus.produced_events
            if t == Topic.DISPATCH_ACTUALS.value
        ]
        assert len(actual_events) >= 1
        actual = actual_events[-1][1]

        # All required fields
        assert actual.actual_id
        assert actual.command_id == "DC-META-001"
        assert actual.event_id == "EVT-META-001"
        assert actual.participant_id == "aggregator-001"
        assert actual.feeder_id == "F-102"
        assert actual.commanded_kw == 200.0
        assert actual.delivered_kw == 185.0
        assert actual.delivery_accuracy_pct == 92.5
        assert actual.sensitivity == SensitivityTier.MEDIUM

    @pytest.mark.asyncio
    async def test_multiple_dispatches_produce_multiple_events(
        self,
        dso_client_e2e: httpx.AsyncClient,
        shared_event_bus: MockEventBus,
    ) -> None:
        """Multiple flexibility requests produce multiple dispatch events."""
        # Send two flexibility requests
        await dso_client_e2e.post(
            "/api/v1/flexibility-requests",
            json=_flexibility_request_payload("F-101"),
            headers=DSO_HEADERS,
        )
        await dso_client_e2e.post(
            "/api/v1/flexibility-requests",
            json=_flexibility_request_payload("F-103"),
            headers=DSO_HEADERS,
        )

        dispatch_events = [
            (t, e)
            for t, e in shared_event_bus.produced_events
            if t == Topic.DISPATCH_COMMANDS.value
        ]
        assert len(dispatch_events) >= 2

        feeder_ids = {e.feeder_id for _, e in dispatch_events}
        assert "F-101" in feeder_ids
        assert "F-103" in feeder_ids


# ---------------------------------------------------------------------------
# Test: Health endpoints for all services
# ---------------------------------------------------------------------------


class TestServiceHealth:
    """Verify all service health endpoints are accessible."""

    @pytest.mark.asyncio
    async def test_catalog_health(
        self, catalog_client_e2e: httpx.AsyncClient
    ) -> None:
        """Catalog /health returns 200 with correct service name."""
        response = await catalog_client_e2e.get("/health")
        assert response.status_code == 200
        assert response.json()["service"] == "federated-catalog"

    @pytest.mark.asyncio
    async def test_dso_health(
        self, dso_client_e2e: httpx.AsyncClient
    ) -> None:
        """DSO /health returns 200 with correct service name."""
        response = await dso_client_e2e.get("/health")
        assert response.status_code == 200
        assert response.json()["service"] == "dso-node"

    @pytest.mark.asyncio
    async def test_aggregator_health(
        self, aggregator_client_e2e: httpx.AsyncClient
    ) -> None:
        """Aggregator /health returns 200 with correct service name."""
        response = await aggregator_client_e2e.get("/health")
        assert response.status_code == 200
        assert response.json()["service"] == "aggregator-node"
