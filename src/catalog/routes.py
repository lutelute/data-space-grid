"""API routes for the Federated Catalog service.

The catalog is the discovery backbone of the Federated Data Space.  Participants
register data assets so that others can search, discover, and negotiate access
contracts.  The catalog stores only metadata; actual data remains under the
provider's local control.

Routes:

  **Health**
    ``GET /health`` -- infrastructure health probe (exempt from auth).

  **Asset management**
    ``POST /api/v1/assets``        -- register a new data asset.
    ``GET  /api/v1/assets``        -- search / discover assets (query params:
                                      ``provider``, ``type``, ``sensitivity``).
    ``GET  /api/v1/assets/{id}``   -- get asset details + policy metadata.
    ``DELETE /api/v1/assets/{id}`` -- deregister an asset.

  **Contract negotiation**
    ``POST /api/v1/contracts``             -- initiate contract negotiation.
    ``GET  /api/v1/contracts/{id}``        -- get contract status.
    ``PUT  /api/v1/contracts/{id}/accept`` -- accept a contract (transition
                                              OFFERED -> ACTIVE via NEGOTIATING).
    ``PUT  /api/v1/contracts/{id}/reject`` -- reject a contract (transition
                                              OFFERED/NEGOTIATING -> REJECTED).

Key design decisions:
  - The router delegates all persistence to :class:`~src.catalog.store.CatalogStore`.
  - Contract accept performs a two-step transition (OFFERED -> NEGOTIATING ->
    ACTIVE) to match the spec state machine while keeping the API simple.
  - Query parameters for asset search use ``provider``, ``type``, and
    ``sensitivity`` (not ``provider_id`` / ``data_type``) for a cleaner
    external API; they are mapped to ``AssetSearchQuery`` internally.
  - All error responses use ``{"detail": ...}`` format consistent with FastAPI
    conventions and the ConnectorMiddleware.
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from starlette.responses import JSONResponse

from src.catalog.schemas import (
    AssetRegistration,
    AssetResponse,
    AssetSearchQuery,
    ContractInitiation,
    ContractResponse,
)
from src.catalog.store import CatalogStore
from src.connector.models import ContractStatus
from src.semantic.cim import SensitivityTier

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Module-level store instance
# ---------------------------------------------------------------------------

# The store is initialised at module import and shared across all route
# handlers.  The ``create_router()`` factory accepts an optional store so
# tests can inject a custom (e.g. in-memory) instance.

_default_store = CatalogStore()


# ---------------------------------------------------------------------------
# Router factory
# ---------------------------------------------------------------------------


def create_router(store: Optional[CatalogStore] = None) -> APIRouter:
    """Create the catalog API router with the given data store.

    Args:
        store: The catalog data store to use.  When ``None``, the module-level
            default store (SQLite file-backed) is used.

    Returns:
        A :class:`~fastapi.APIRouter` with all catalog endpoints registered.
    """
    catalog_store = store or _default_store

    router = APIRouter()

    # -- Health endpoint -----------------------------------------------------

    @router.get(
        "/health",
        summary="Health check",
        response_class=JSONResponse,
    )
    async def health() -> dict[str, str]:
        """Return a simple health status for infrastructure probes."""
        return {"status": "healthy", "service": "federated-catalog"}

    # -- Asset routes --------------------------------------------------------

    @router.post(
        "/api/v1/assets",
        response_model=AssetResponse,
        status_code=201,
        summary="Register a data asset",
    )
    async def register_asset(registration: AssetRegistration) -> AssetResponse:
        """Register a new data asset in the federated catalog.

        The provider supplies metadata about the asset; the catalog assigns
        a unique ``id`` and records the registration timestamp.
        """
        asset = catalog_store.register_asset(registration)
        logger.info(
            "Asset registered: id=%s provider=%s type=%s",
            asset.id,
            asset.provider_id,
            asset.data_type,
        )
        return asset

    @router.get(
        "/api/v1/assets",
        response_model=list[AssetResponse],
        summary="Search/discover assets",
    )
    async def search_assets(
        provider: Optional[str] = Query(
            default=None,
            description="Filter by provider participant ID",
        ),
        type: Optional[str] = Query(
            default=None,
            description="Filter by semantic data type (e.g. 'feeder_constraint')",
        ),
        sensitivity: Optional[SensitivityTier] = Query(
            default=None,
            description="Filter by sensitivity tier (high, medium, high_privacy)",
        ),
    ) -> list[AssetResponse]:
        """Search and discover data assets in the federated catalog.

        All query parameters are optional.  When omitted, all registered
        assets are returned.  Filters are combined with AND logic.
        """
        query = AssetSearchQuery(
            provider_id=provider,
            data_type=type,
            sensitivity=sensitivity,
        )
        return catalog_store.search_assets(query)

    @router.get(
        "/api/v1/assets/{asset_id}",
        response_model=AssetResponse,
        summary="Get asset details",
    )
    async def get_asset(asset_id: str) -> AssetResponse:
        """Retrieve a single asset by its ID, including policy metadata."""
        asset = catalog_store.get_asset(asset_id)
        if asset is None:
            raise HTTPException(status_code=404, detail=f"Asset '{asset_id}' not found")
        return asset

    @router.delete(
        "/api/v1/assets/{asset_id}",
        status_code=204,
        summary="Deregister an asset",
    )
    async def delete_asset(asset_id: str) -> None:
        """Deregister a data asset from the federated catalog.

        Returns 204 on success, 404 if the asset does not exist.
        """
        deleted = catalog_store.delete_asset(asset_id)
        if not deleted:
            raise HTTPException(status_code=404, detail=f"Asset '{asset_id}' not found")

    # -- Contract routes -----------------------------------------------------

    @router.post(
        "/api/v1/contracts",
        response_model=ContractResponse,
        status_code=201,
        summary="Initiate contract negotiation",
    )
    async def create_contract(initiation: ContractInitiation) -> ContractResponse:
        """Initiate a contract negotiation for a data asset.

        The consumer proposes contract terms; the catalog creates the contract
        in ``OFFERED`` state and assigns a ``contract_id``.
        """
        # Validate the target asset exists.
        asset = catalog_store.get_asset(initiation.asset_id)
        if asset is None:
            raise HTTPException(
                status_code=404,
                detail=f"Asset '{initiation.asset_id}' not found",
            )

        contract = catalog_store.create_contract(initiation)
        logger.info(
            "Contract created: id=%s provider=%s consumer=%s asset=%s",
            contract.contract_id,
            contract.provider_id,
            contract.consumer_id,
            contract.asset_id,
        )
        return contract

    @router.get(
        "/api/v1/contracts/{contract_id}",
        response_model=ContractResponse,
        summary="Get contract status",
    )
    async def get_contract(contract_id: str) -> ContractResponse:
        """Retrieve a single contract by its ID, including its current status."""
        contract = catalog_store.get_contract(contract_id)
        if contract is None:
            raise HTTPException(
                status_code=404,
                detail=f"Contract '{contract_id}' not found",
            )
        return contract

    @router.put(
        "/api/v1/contracts/{contract_id}/accept",
        response_model=ContractResponse,
        summary="Accept a contract",
    )
    async def accept_contract(contract_id: str) -> ContractResponse:
        """Accept a contract, transitioning it to ACTIVE state.

        Performs a two-step transition through the state machine:
          OFFERED -> NEGOTIATING -> ACTIVE

        If the contract is already in NEGOTIATING state, only the final
        transition to ACTIVE is applied.
        """
        contract = catalog_store.get_contract(contract_id)
        if contract is None:
            raise HTTPException(
                status_code=404,
                detail=f"Contract '{contract_id}' not found",
            )

        # Perform state transitions based on current status.
        if contract.status == ContractStatus.OFFERED:
            # Two-step: OFFERED -> NEGOTIATING -> ACTIVE
            result = catalog_store.update_contract_status(
                contract_id, ContractStatus.NEGOTIATING
            )
            if result is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Contract '{contract_id}' not found",
                )
            result = catalog_store.update_contract_status(
                contract_id, ContractStatus.ACTIVE
            )
            if result is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Contract '{contract_id}' not found",
                )
        elif contract.status == ContractStatus.NEGOTIATING:
            # Single step: NEGOTIATING -> ACTIVE
            result = catalog_store.update_contract_status(
                contract_id, ContractStatus.ACTIVE
            )
            if result is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Contract '{contract_id}' not found",
                )
        else:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Cannot accept contract '{contract_id}': "
                    f"current status is '{contract.status.value}' "
                    f"(must be 'offered' or 'negotiating')"
                ),
            )

        logger.info("Contract accepted: id=%s", contract_id)
        return result

    @router.put(
        "/api/v1/contracts/{contract_id}/reject",
        response_model=ContractResponse,
        summary="Reject a contract",
    )
    async def reject_contract(contract_id: str) -> ContractResponse:
        """Reject a contract, transitioning it to REJECTED state.

        A contract can be rejected from the ``OFFERED`` or ``NEGOTIATING``
        states.  Once rejected, it cannot be reactivated.
        """
        contract = catalog_store.get_contract(contract_id)
        if contract is None:
            raise HTTPException(
                status_code=404,
                detail=f"Contract '{contract_id}' not found",
            )

        if contract.status not in (ContractStatus.OFFERED, ContractStatus.NEGOTIATING):
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Cannot reject contract '{contract_id}': "
                    f"current status is '{contract.status.value}' "
                    f"(must be 'offered' or 'negotiating')"
                ),
            )

        result = catalog_store.update_contract_status(
            contract_id, ContractStatus.REJECTED
        )
        if result is None:
            raise HTTPException(
                status_code=404,
                detail=f"Contract '{contract_id}' not found",
            )

        logger.info("Contract rejected: id=%s", contract_id)
        return result

    return router
