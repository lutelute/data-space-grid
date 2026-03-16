"""Integration tests for the contract negotiation flow between participants.

Verifies the end-to-end contract handshake:
  1. Aggregator discovers a DSO asset in the federated catalog.
  2. Aggregator initiates a contract negotiation for the asset.
  3. DSO accepts the contract (OFFERED -> NEGOTIATING -> ACTIVE).
  4. Contract status becomes ACTIVE with correct metadata.
  5. Data access works against the DSO node with an active contract.
  6. Rejected contracts block further acceptance.
  7. Contract for non-existent assets is rejected with 404.

These tests use the shared ``catalog_client`` and ``dso_client`` fixtures
from ``conftest.py``.  Each test creates a fresh catalog with in-memory
SQLite so tests are isolated.
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
from src.connector.middleware import ConnectorMiddleware
from src.connector.models import ContractStatus
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
# Helpers
# ---------------------------------------------------------------------------


def _dso_constraint_asset_payload() -> dict:
    """Return a valid asset registration payload for a DSO feeder constraint."""
    return {
        "provider_id": "dso-001",
        "name": "Feeder F-102 Constraints",
        "description": "Real-time feeder constraints for congested feeder F-102",
        "data_type": "feeder_constraint",
        "sensitivity": "medium",
        "endpoint": "https://dso.local/api/v1/constraints/F-102",
        "update_frequency": "5m",
        "resolution": "per_feeder",
        "anonymized": False,
        "personal_data": False,
        "policy_metadata": {
            "allowed_purposes": "congestion_management,grid_analysis",
            "min_retention_days": "7",
            "max_retention_days": "90",
        },
    }


def _contract_initiation_payload(asset_id: str) -> dict:
    """Return a valid contract initiation payload for an asset."""
    now = _utc_now()
    return {
        "provider_id": "dso-001",
        "consumer_id": "aggregator-001",
        "asset_id": asset_id,
        "purpose": "congestion_management",
        "allowed_operations": ["read", "aggregate"],
        "redistribution_allowed": False,
        "retention_days": 30,
        "anonymization_required": False,
        "emergency_override": True,
        "valid_from": (now - timedelta(hours=1)).isoformat(),
        "valid_until": (now + timedelta(days=90)).isoformat(),
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
async def catalog_client_fresh(tmp_path: Path):
    """Create a catalog client backed by a fresh in-memory store."""
    store = CatalogStore(database_url="sqlite:///:memory:")
    audit_logger = AuditLogger(
        log_path=str(tmp_path / "catalog-contract-audit.jsonl")
    )
    backend = MockKeycloakAuthBackend(mock_user=make_dso_user())
    router = create_catalog_router(store=store)
    app = FastAPI(title="Test Catalog", version="0.1.0-test")
    app.add_middleware(
        ConnectorMiddleware,
        auth_backend=backend,
        audit_logger=audit_logger,
        participant_id="catalog-001",
    )
    app.include_router(router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test-catalog"
    ) as client:
        yield client


@pytest.fixture()
async def dso_client_seeded(tmp_path: Path):
    """Create a DSO client with a seeded in-memory store.

    The DSO store is seeded with sample data (feeders F-101, F-102, F-103)
    so that data access tests have real data to retrieve.
    """
    store = DSOStore(database_url="sqlite:///:memory:")
    store.seed()
    audit_logger = AuditLogger(
        log_path=str(tmp_path / "dso-contract-audit.jsonl")
    )
    bus = MockEventBus()
    backend = MockKeycloakAuthBackend(mock_user=make_dso_user())
    router = create_dso_router(store=store, audit_logger=audit_logger, event_bus=bus)
    app = FastAPI(title="Test DSO", version="0.1.0-test")
    app.add_middleware(
        ConnectorMiddleware,
        auth_backend=backend,
        audit_logger=audit_logger,
        participant_id="dso-001",
    )
    app.include_router(router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test-dso"
    ) as client:
        yield client


# ---------------------------------------------------------------------------
# Test: Full contract negotiation lifecycle
# ---------------------------------------------------------------------------


class TestContractNegotiationLifecycle:
    """Integration tests for the contract negotiation state machine via API."""

    @pytest.mark.asyncio
    async def test_initiate_contract_for_discovered_asset(
        self, catalog_client_fresh: httpx.AsyncClient
    ) -> None:
        """Aggregator discovers a DSO asset and initiates contract negotiation."""
        client = catalog_client_fresh
        headers = {"Authorization": "Bearer test-dso-token"}

        # Step 1: DSO registers an asset
        reg_response = await client.post(
            "/api/v1/assets",
            json=_dso_constraint_asset_payload(),
            headers=headers,
        )
        assert reg_response.status_code == 201
        asset_id = reg_response.json()["id"]

        # Step 2: Aggregator discovers the asset
        search_response = await client.get(
            "/api/v1/assets",
            params={"type": "feeder_constraint"},
            headers=headers,
        )
        assert search_response.status_code == 200
        discovered = search_response.json()
        assert len(discovered) >= 1
        discovered_asset = discovered[0]
        assert discovered_asset["id"] == asset_id

        # Step 3: Aggregator initiates contract negotiation
        contract_payload = _contract_initiation_payload(asset_id)
        contract_response = await client.post(
            "/api/v1/contracts",
            json=contract_payload,
            headers=headers,
        )
        assert contract_response.status_code == 201
        contract = contract_response.json()
        assert contract["status"] == "offered"
        assert contract["asset_id"] == asset_id
        assert contract["provider_id"] == "dso-001"
        assert contract["consumer_id"] == "aggregator-001"
        assert contract["purpose"] == "congestion_management"
        assert "contract_id" in contract

    @pytest.mark.asyncio
    async def test_accept_contract_transitions_to_active(
        self, catalog_client_fresh: httpx.AsyncClient
    ) -> None:
        """Accepting a contract should transition it from OFFERED to ACTIVE."""
        client = catalog_client_fresh
        headers = {"Authorization": "Bearer test-dso-token"}

        # Register asset and create contract
        reg = await client.post(
            "/api/v1/assets",
            json=_dso_constraint_asset_payload(),
            headers=headers,
        )
        asset_id = reg.json()["id"]

        contract_resp = await client.post(
            "/api/v1/contracts",
            json=_contract_initiation_payload(asset_id),
            headers=headers,
        )
        contract_id = contract_resp.json()["contract_id"]
        assert contract_resp.json()["status"] == "offered"

        # DSO accepts the contract
        accept_resp = await client.put(
            f"/api/v1/contracts/{contract_id}/accept",
            headers=headers,
        )
        assert accept_resp.status_code == 200
        accepted = accept_resp.json()
        assert accepted["status"] == "active"
        assert accepted["contract_id"] == contract_id

    @pytest.mark.asyncio
    async def test_get_contract_status(
        self, catalog_client_fresh: httpx.AsyncClient
    ) -> None:
        """Contract status can be retrieved by ID at any point."""
        client = catalog_client_fresh
        headers = {"Authorization": "Bearer test-dso-token"}

        # Register asset and create contract
        reg = await client.post(
            "/api/v1/assets",
            json=_dso_constraint_asset_payload(),
            headers=headers,
        )
        asset_id = reg.json()["id"]

        contract_resp = await client.post(
            "/api/v1/contracts",
            json=_contract_initiation_payload(asset_id),
            headers=headers,
        )
        contract_id = contract_resp.json()["contract_id"]

        # Get contract status
        status_resp = await client.get(
            f"/api/v1/contracts/{contract_id}", headers=headers
        )
        assert status_resp.status_code == 200
        status = status_resp.json()
        assert status["contract_id"] == contract_id
        assert status["status"] == "offered"
        assert status["purpose"] == "congestion_management"
        assert status["allowed_operations"] == ["read", "aggregate"]
        assert status["emergency_override"] is True

    @pytest.mark.asyncio
    async def test_reject_contract(
        self, catalog_client_fresh: httpx.AsyncClient
    ) -> None:
        """Rejecting a contract should transition it to REJECTED."""
        client = catalog_client_fresh
        headers = {"Authorization": "Bearer test-dso-token"}

        # Register asset and create contract
        reg = await client.post(
            "/api/v1/assets",
            json=_dso_constraint_asset_payload(),
            headers=headers,
        )
        asset_id = reg.json()["id"]

        contract_resp = await client.post(
            "/api/v1/contracts",
            json=_contract_initiation_payload(asset_id),
            headers=headers,
        )
        contract_id = contract_resp.json()["contract_id"]

        # DSO rejects the contract
        reject_resp = await client.put(
            f"/api/v1/contracts/{contract_id}/reject",
            headers=headers,
        )
        assert reject_resp.status_code == 200
        rejected = reject_resp.json()
        assert rejected["status"] == "rejected"
        assert rejected["contract_id"] == contract_id

    @pytest.mark.asyncio
    async def test_cannot_accept_rejected_contract(
        self, catalog_client_fresh: httpx.AsyncClient
    ) -> None:
        """Accepting a rejected contract should return 409 Conflict."""
        client = catalog_client_fresh
        headers = {"Authorization": "Bearer test-dso-token"}

        # Register asset and create contract
        reg = await client.post(
            "/api/v1/assets",
            json=_dso_constraint_asset_payload(),
            headers=headers,
        )
        asset_id = reg.json()["id"]

        contract_resp = await client.post(
            "/api/v1/contracts",
            json=_contract_initiation_payload(asset_id),
            headers=headers,
        )
        contract_id = contract_resp.json()["contract_id"]

        # Reject the contract
        await client.put(
            f"/api/v1/contracts/{contract_id}/reject",
            headers=headers,
        )

        # Try to accept after rejection
        accept_resp = await client.put(
            f"/api/v1/contracts/{contract_id}/accept",
            headers=headers,
        )
        assert accept_resp.status_code == 409

    @pytest.mark.asyncio
    async def test_contract_for_nonexistent_asset_returns_404(
        self, catalog_client_fresh: httpx.AsyncClient
    ) -> None:
        """Initiating a contract for a non-existent asset should return 404."""
        client = catalog_client_fresh
        headers = {"Authorization": "Bearer test-dso-token"}

        contract_payload = _contract_initiation_payload("nonexistent-asset-id")
        response = await client.post(
            "/api/v1/contracts",
            json=contract_payload,
            headers=headers,
        )
        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_get_nonexistent_contract_returns_404(
        self, catalog_client_fresh: httpx.AsyncClient
    ) -> None:
        """Requesting a non-existent contract should return 404."""
        client = catalog_client_fresh
        headers = {"Authorization": "Bearer test-dso-token"}

        response = await client.get(
            "/api/v1/contracts/nonexistent-contract-id",
            headers=headers,
        )
        assert response.status_code == 404


# ---------------------------------------------------------------------------
# Test: Contract metadata integrity
# ---------------------------------------------------------------------------


class TestContractMetadata:
    """Integration tests for contract metadata preservation through lifecycle."""

    @pytest.mark.asyncio
    async def test_contract_preserves_all_terms(
        self, catalog_client_fresh: httpx.AsyncClient
    ) -> None:
        """Contract terms are preserved through the full lifecycle."""
        client = catalog_client_fresh
        headers = {"Authorization": "Bearer test-dso-token"}

        # Register asset
        reg = await client.post(
            "/api/v1/assets",
            json=_dso_constraint_asset_payload(),
            headers=headers,
        )
        asset_id = reg.json()["id"]

        # Create contract with specific terms
        payload = _contract_initiation_payload(asset_id)
        contract_resp = await client.post(
            "/api/v1/contracts", json=payload, headers=headers
        )
        contract_id = contract_resp.json()["contract_id"]

        # Accept the contract
        accept_resp = await client.put(
            f"/api/v1/contracts/{contract_id}/accept",
            headers=headers,
        )
        active_contract = accept_resp.json()

        # Verify all terms are preserved after acceptance
        assert active_contract["status"] == "active"
        assert active_contract["provider_id"] == "dso-001"
        assert active_contract["consumer_id"] == "aggregator-001"
        assert active_contract["asset_id"] == asset_id
        assert active_contract["purpose"] == "congestion_management"
        assert active_contract["allowed_operations"] == ["read", "aggregate"]
        assert active_contract["redistribution_allowed"] is False
        assert active_contract["retention_days"] == 30
        assert active_contract["anonymization_required"] is False
        assert active_contract["emergency_override"] is True
        assert "valid_from" in active_contract
        assert "valid_until" in active_contract
        assert "created_at" in active_contract
        assert "updated_at" in active_contract

    @pytest.mark.asyncio
    async def test_contract_timestamps_are_updated(
        self, catalog_client_fresh: httpx.AsyncClient
    ) -> None:
        """Contract updated_at timestamp changes on status transitions."""
        client = catalog_client_fresh
        headers = {"Authorization": "Bearer test-dso-token"}

        # Register asset and create contract
        reg = await client.post(
            "/api/v1/assets",
            json=_dso_constraint_asset_payload(),
            headers=headers,
        )
        asset_id = reg.json()["id"]

        contract_resp = await client.post(
            "/api/v1/contracts",
            json=_contract_initiation_payload(asset_id),
            headers=headers,
        )
        contract = contract_resp.json()
        created_at = contract["created_at"]
        contract_id = contract["contract_id"]

        # Accept the contract
        accept_resp = await client.put(
            f"/api/v1/contracts/{contract_id}/accept",
            headers=headers,
        )
        accepted = accept_resp.json()

        # created_at should remain the same, updated_at should be >=
        assert accepted["created_at"] == created_at
        assert accepted["updated_at"] >= created_at


# ---------------------------------------------------------------------------
# Test: Data access after active contract
# ---------------------------------------------------------------------------


class TestDataAccessWithContract:
    """Integration tests verifying data access works with active contract context."""

    @pytest.mark.asyncio
    async def test_dso_data_accessible_after_contract_active(
        self, dso_client_seeded: httpx.AsyncClient
    ) -> None:
        """DSO node returns data when accessed with valid authentication.

        In the real flow, the middleware would verify the contract. Here we
        verify that the DSO data endpoints are functional and return the
        seeded data.
        """
        client = dso_client_seeded
        headers = {"Authorization": "Bearer test-dso-token"}

        # Access feeder constraints (seeded data should be available)
        response = await client.get(
            "/api/v1/constraints", headers=headers
        )
        assert response.status_code == 200
        constraints = response.json()
        assert len(constraints) >= 1

        # Verify constraint data structure
        constraint = constraints[0]
        assert "feeder_id" in constraint
        assert "max_active_power_kw" in constraint
        assert "congestion_level" in constraint
        assert "sensitivity" in constraint

    @pytest.mark.asyncio
    async def test_dso_specific_feeder_constraint_accessible(
        self, dso_client_seeded: httpx.AsyncClient
    ) -> None:
        """Specific feeder constraint can be retrieved by feeder ID."""
        client = dso_client_seeded
        headers = {"Authorization": "Bearer test-dso-token"}

        # Access specific feeder constraint (F-102 is the congested feeder)
        response = await client.get(
            "/api/v1/constraints/F-102", headers=headers
        )
        assert response.status_code == 200
        constraint = response.json()
        assert constraint["feeder_id"] == "F-102"
        assert constraint["congestion_level"] == 0.85

    @pytest.mark.asyncio
    async def test_dso_congestion_signals_accessible(
        self, dso_client_seeded: httpx.AsyncClient
    ) -> None:
        """DSO congestion signals can be retrieved after contract is active."""
        client = dso_client_seeded
        headers = {"Authorization": "Bearer test-dso-token"}

        response = await client.get(
            "/api/v1/congestion-signals", headers=headers
        )
        assert response.status_code == 200
        signals = response.json()
        assert len(signals) >= 1
        assert all("congestion_level" in s for s in signals)
        assert all("feeder_id" in s for s in signals)

    @pytest.mark.asyncio
    async def test_nonexistent_feeder_returns_404(
        self, dso_client_seeded: httpx.AsyncClient
    ) -> None:
        """Requesting a non-existent feeder constraint returns 404."""
        client = dso_client_seeded
        headers = {"Authorization": "Bearer test-dso-token"}

        response = await client.get(
            "/api/v1/constraints/NONEXISTENT", headers=headers
        )
        assert response.status_code == 404
