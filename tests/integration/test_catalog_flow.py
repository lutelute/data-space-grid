"""Integration tests for the federated catalog asset registration and discovery flow.

Verifies the end-to-end catalog workflow:
  1. DSO registers a data asset in the federated catalog.
  2. Catalog returns the asset in search results filtered by provider, type,
     and sensitivity.
  3. Asset metadata includes policy information.
  4. Asset can be retrieved by ID with full details.
  5. Asset can be deregistered and disappears from search results.
  6. Multiple assets from different providers can coexist and be filtered.

These tests use the shared ``catalog_client`` fixture from ``conftest.py``
which provides an ``httpx.AsyncClient`` backed by a real FastAPI catalog app
with mock authentication (no real Keycloak).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import httpx
import pytest
from fastapi import FastAPI

from src.catalog.routes import create_router as create_catalog_router
from src.catalog.store import CatalogStore
from src.connector.audit import AuditLogger
from src.connector.middleware import ConnectorMiddleware
from tests.conftest import (
    MockKeycloakAuthBackend,
    make_dso_user,
)


def _utc_now() -> datetime:
    """Return the current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
async def catalog_with_store(tmp_path):
    """Create a catalog client with a fresh in-memory store for isolation."""
    store = CatalogStore(database_url="sqlite:///:memory:")
    audit_logger = AuditLogger(
        log_path=str(tmp_path / "catalog-integ-audit.jsonl")
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


def _aggregator_flexibility_asset_payload() -> dict:
    """Return a valid asset registration payload for aggregator flexibility."""
    return {
        "provider_id": "aggregator-001",
        "name": "Aggregate Flexibility Envelope F-102",
        "description": "Aggregate flexibility from DER fleet on feeder F-102",
        "data_type": "flexibility_envelope",
        "sensitivity": "medium",
        "endpoint": "https://aggregator.local/api/v1/flexibility-offers",
        "update_frequency": "15m",
        "resolution": "per_feeder",
        "anonymized": False,
        "personal_data": False,
        "policy_metadata": {
            "allowed_purposes": "congestion_management",
            "min_retention_days": "7",
        },
    }


def _prosumer_demand_asset_payload() -> dict:
    """Return a valid asset registration payload for prosumer demand profile."""
    return {
        "provider_id": "prosumer-001",
        "name": "Anonymized Campus Demand Profile",
        "description": "k-anonymized demand profile from campus prosumer",
        "data_type": "demand_profile",
        "sensitivity": "high_privacy",
        "endpoint": "https://prosumer.local/api/v1/demand-profile",
        "update_frequency": "1h",
        "resolution": "15min",
        "anonymized": True,
        "personal_data": True,
        "policy_metadata": {
            "allowed_purposes": "research,forecasting",
            "consent_required": "true",
            "k_anonymity_level": "5",
        },
    }


# ---------------------------------------------------------------------------
# Test: DSO registers asset and catalog returns it in search
# ---------------------------------------------------------------------------


class TestAssetRegistrationAndDiscovery:
    """Integration tests for asset registration and catalog discovery."""

    @pytest.mark.asyncio
    async def test_register_asset_returns_201_with_catalog_assigned_id(
        self, catalog_with_store: httpx.AsyncClient
    ) -> None:
        """Registering an asset should return 201 with a catalog-assigned ID."""
        client = catalog_with_store
        payload = _dso_constraint_asset_payload()

        response = await client.post(
            "/api/v1/assets",
            json=payload,
            headers={"Authorization": "Bearer test-dso-token"},
        )

        assert response.status_code == 201
        data = response.json()
        assert "id" in data
        assert data["id"]  # non-empty
        assert data["provider_id"] == "dso-001"
        assert data["data_type"] == "feeder_constraint"
        assert data["sensitivity"] == "medium"
        assert data["name"] == "Feeder F-102 Constraints"
        assert "created_at" in data
        assert "updated_at" in data

    @pytest.mark.asyncio
    async def test_registered_asset_appears_in_search(
        self, catalog_with_store: httpx.AsyncClient
    ) -> None:
        """A registered asset should be discoverable via the search endpoint."""
        client = catalog_with_store
        payload = _dso_constraint_asset_payload()

        # Register asset
        reg_response = await client.post(
            "/api/v1/assets",
            json=payload,
            headers={"Authorization": "Bearer test-dso-token"},
        )
        assert reg_response.status_code == 201
        asset_id = reg_response.json()["id"]

        # Search for the asset
        search_response = await client.get(
            "/api/v1/assets",
            headers={"Authorization": "Bearer test-dso-token"},
        )
        assert search_response.status_code == 200
        assets = search_response.json()
        assert len(assets) >= 1
        asset_ids = [a["id"] for a in assets]
        assert asset_id in asset_ids

    @pytest.mark.asyncio
    async def test_search_by_provider_filters_correctly(
        self, catalog_with_store: httpx.AsyncClient
    ) -> None:
        """Searching by provider should only return that provider's assets."""
        client = catalog_with_store
        headers = {"Authorization": "Bearer test-dso-token"}

        # Register DSO and Aggregator assets
        await client.post(
            "/api/v1/assets",
            json=_dso_constraint_asset_payload(),
            headers=headers,
        )
        await client.post(
            "/api/v1/assets",
            json=_aggregator_flexibility_asset_payload(),
            headers=headers,
        )

        # Search by DSO provider
        response = await client.get(
            "/api/v1/assets",
            params={"provider": "dso-001"},
            headers=headers,
        )
        assert response.status_code == 200
        assets = response.json()
        assert len(assets) >= 1
        assert all(a["provider_id"] == "dso-001" for a in assets)

        # Search by Aggregator provider
        response = await client.get(
            "/api/v1/assets",
            params={"provider": "aggregator-001"},
            headers=headers,
        )
        assets = response.json()
        assert len(assets) >= 1
        assert all(a["provider_id"] == "aggregator-001" for a in assets)

    @pytest.mark.asyncio
    async def test_search_by_data_type_filters_correctly(
        self, catalog_with_store: httpx.AsyncClient
    ) -> None:
        """Searching by data type should only return matching assets."""
        client = catalog_with_store
        headers = {"Authorization": "Bearer test-dso-token"}

        # Register two different data types
        await client.post(
            "/api/v1/assets",
            json=_dso_constraint_asset_payload(),
            headers=headers,
        )
        await client.post(
            "/api/v1/assets",
            json=_aggregator_flexibility_asset_payload(),
            headers=headers,
        )

        # Search by feeder_constraint type
        response = await client.get(
            "/api/v1/assets",
            params={"type": "feeder_constraint"},
            headers=headers,
        )
        assert response.status_code == 200
        assets = response.json()
        assert len(assets) >= 1
        assert all(a["data_type"] == "feeder_constraint" for a in assets)

    @pytest.mark.asyncio
    async def test_search_by_sensitivity_filters_correctly(
        self, catalog_with_store: httpx.AsyncClient
    ) -> None:
        """Searching by sensitivity tier should only return matching assets."""
        client = catalog_with_store
        headers = {"Authorization": "Bearer test-dso-token"}

        # Register medium and high_privacy assets
        await client.post(
            "/api/v1/assets",
            json=_dso_constraint_asset_payload(),
            headers=headers,
        )
        await client.post(
            "/api/v1/assets",
            json=_prosumer_demand_asset_payload(),
            headers=headers,
        )

        # Search by high_privacy sensitivity
        response = await client.get(
            "/api/v1/assets",
            params={"sensitivity": "high_privacy"},
            headers=headers,
        )
        assert response.status_code == 200
        assets = response.json()
        assert len(assets) >= 1
        assert all(a["sensitivity"] == "high_privacy" for a in assets)

    @pytest.mark.asyncio
    async def test_asset_metadata_includes_policy_info(
        self, catalog_with_store: httpx.AsyncClient
    ) -> None:
        """Registered assets should include policy metadata in responses."""
        client = catalog_with_store
        headers = {"Authorization": "Bearer test-dso-token"}
        payload = _dso_constraint_asset_payload()

        # Register and retrieve
        reg_response = await client.post(
            "/api/v1/assets", json=payload, headers=headers
        )
        asset_id = reg_response.json()["id"]

        # Get asset by ID
        detail_response = await client.get(
            f"/api/v1/assets/{asset_id}", headers=headers
        )
        assert detail_response.status_code == 200
        data = detail_response.json()

        # Verify policy metadata is present and correct
        assert "policy_metadata" in data
        policy = data["policy_metadata"]
        assert policy["allowed_purposes"] == "congestion_management,grid_analysis"
        assert policy["min_retention_days"] == "7"
        assert policy["max_retention_days"] == "90"

    @pytest.mark.asyncio
    async def test_get_asset_by_id(
        self, catalog_with_store: httpx.AsyncClient
    ) -> None:
        """Retrieving an asset by ID returns full details."""
        client = catalog_with_store
        headers = {"Authorization": "Bearer test-dso-token"}
        payload = _dso_constraint_asset_payload()

        # Register
        reg_response = await client.post(
            "/api/v1/assets", json=payload, headers=headers
        )
        asset_id = reg_response.json()["id"]

        # Retrieve by ID
        response = await client.get(
            f"/api/v1/assets/{asset_id}", headers=headers
        )
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == asset_id
        assert data["provider_id"] == payload["provider_id"]
        assert data["name"] == payload["name"]
        assert data["description"] == payload["description"]
        assert data["data_type"] == payload["data_type"]
        assert data["endpoint"] == payload["endpoint"]

    @pytest.mark.asyncio
    async def test_get_nonexistent_asset_returns_404(
        self, catalog_with_store: httpx.AsyncClient
    ) -> None:
        """Requesting a non-existent asset should return 404."""
        client = catalog_with_store
        headers = {"Authorization": "Bearer test-dso-token"}

        response = await client.get(
            "/api/v1/assets/nonexistent-id", headers=headers
        )
        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_delete_asset_removes_from_catalog(
        self, catalog_with_store: httpx.AsyncClient
    ) -> None:
        """Deleting an asset should remove it from search results."""
        client = catalog_with_store
        headers = {"Authorization": "Bearer test-dso-token"}

        # Register an asset
        reg_response = await client.post(
            "/api/v1/assets",
            json=_dso_constraint_asset_payload(),
            headers=headers,
        )
        asset_id = reg_response.json()["id"]

        # Verify it exists
        response = await client.get(
            f"/api/v1/assets/{asset_id}", headers=headers
        )
        assert response.status_code == 200

        # Delete the asset
        del_response = await client.delete(
            f"/api/v1/assets/{asset_id}", headers=headers
        )
        assert del_response.status_code == 204

        # Verify it is gone
        response = await client.get(
            f"/api/v1/assets/{asset_id}", headers=headers
        )
        assert response.status_code == 404

        # Verify it is gone from search
        search_response = await client.get(
            "/api/v1/assets",
            params={"provider": "dso-001"},
            headers=headers,
        )
        asset_ids = [a["id"] for a in search_response.json()]
        assert asset_id not in asset_ids


# ---------------------------------------------------------------------------
# Test: Multiple providers coexist in catalog
# ---------------------------------------------------------------------------


class TestMultiProviderCatalog:
    """Integration tests for multi-provider asset coexistence."""

    @pytest.mark.asyncio
    async def test_multiple_providers_register_assets(
        self, catalog_with_store: httpx.AsyncClient
    ) -> None:
        """Multiple providers can register assets in the same catalog."""
        client = catalog_with_store
        headers = {"Authorization": "Bearer test-dso-token"}

        # Register assets from three providers
        r1 = await client.post(
            "/api/v1/assets",
            json=_dso_constraint_asset_payload(),
            headers=headers,
        )
        r2 = await client.post(
            "/api/v1/assets",
            json=_aggregator_flexibility_asset_payload(),
            headers=headers,
        )
        r3 = await client.post(
            "/api/v1/assets",
            json=_prosumer_demand_asset_payload(),
            headers=headers,
        )
        assert r1.status_code == 201
        assert r2.status_code == 201
        assert r3.status_code == 201

        # Unfiltered search returns all three
        response = await client.get(
            "/api/v1/assets", headers=headers
        )
        assert response.status_code == 200
        assets = response.json()
        assert len(assets) == 3
        provider_ids = {a["provider_id"] for a in assets}
        assert provider_ids == {"dso-001", "aggregator-001", "prosumer-001"}

    @pytest.mark.asyncio
    async def test_combined_filters_narrow_results(
        self, catalog_with_store: httpx.AsyncClient
    ) -> None:
        """Combining provider and type filters narrows search results."""
        client = catalog_with_store
        headers = {"Authorization": "Bearer test-dso-token"}

        # Register assets from multiple providers
        await client.post(
            "/api/v1/assets",
            json=_dso_constraint_asset_payload(),
            headers=headers,
        )
        await client.post(
            "/api/v1/assets",
            json=_aggregator_flexibility_asset_payload(),
            headers=headers,
        )
        await client.post(
            "/api/v1/assets",
            json=_prosumer_demand_asset_payload(),
            headers=headers,
        )

        # Filter by provider=dso-001 AND type=feeder_constraint
        response = await client.get(
            "/api/v1/assets",
            params={"provider": "dso-001", "type": "feeder_constraint"},
            headers=headers,
        )
        assert response.status_code == 200
        assets = response.json()
        assert len(assets) == 1
        assert assets[0]["provider_id"] == "dso-001"
        assert assets[0]["data_type"] == "feeder_constraint"


# ---------------------------------------------------------------------------
# Test: Catalog health endpoint
# ---------------------------------------------------------------------------


class TestCatalogHealth:
    """Integration tests for the catalog health endpoint."""

    @pytest.mark.asyncio
    async def test_health_endpoint_returns_200(
        self, catalog_with_store: httpx.AsyncClient
    ) -> None:
        """The /health endpoint should return 200 without authentication."""
        client = catalog_with_store
        response = await client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "federated-catalog"
