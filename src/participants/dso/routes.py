"""API routes for the DSO (Distribution System Operator) participant node.

The DSO publishes grid operational data to the federated data space: feeder
constraints, congestion signals, hosting capacity, and flexibility requests.
Other participants (aggregators, prosumers) discover these assets via the
federated catalog and negotiate contracts for access.

Routes:

  **Health**
    ``GET /health`` -- infrastructure health probe (exempt from auth).

  **Feeder constraints** (contract-gated)
    ``GET  /api/v1/constraints``              -- list all feeder constraints
                                                 (optional ``feeder_id`` filter).
    ``GET  /api/v1/constraints/{feeder_id}``  -- specific feeder constraint.

  **Congestion signals**
    ``GET  /api/v1/congestion-signals``       -- current congestion levels
                                                 (optional ``feeder_id`` filter).

  **Hosting capacity**
    ``GET  /api/v1/hosting-capacity``         -- available capacity per node
                                                 (optional ``feeder_id`` filter).

  **Flexibility requests**
    ``POST /api/v1/flexibility-requests``     -- publish a flexibility need.

  **Audit**
    ``GET  /api/v1/audit``                    -- query audit log (admin only).

Key design decisions:
  - The router delegates all persistence to
    :class:`~src.participants.dso.store.DSOStore`.
  - Flexibility requests are stored in-memory within the router closure;
    the DSOStore focuses on grid operational data (constraints, signals,
    capacity).
  - The ``create_router()`` factory accepts an optional store and audit
    logger so that tests can inject custom instances.
  - Query parameters for congestion signals and hosting capacity use
    ``feeder_id`` for consistent filtering across endpoints.
  - All error responses use ``{"detail": ...}`` format consistent with
    FastAPI conventions and the ConnectorMiddleware.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from starlette.responses import JSONResponse

from src.connector.audit import AuditLogger
from src.connector.models import AuditAction, AuditEntry, AuditOutcome
from src.participants.dso.store import DSOStore
from src.semantic.cim import (
    CongestionSignal,
    FeederConstraint,
    HostingCapacity,
    SensitivityTier,
)

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    """Return the current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Request / response models for flexibility requests
# ---------------------------------------------------------------------------


class FlexibilityRequestCreate(BaseModel):
    """Input schema for publishing a flexibility need.

    The DSO submits this when it requires flexibility from aggregators
    on a specific feeder (e.g., to manage congestion).
    """

    feeder_id: str = Field(..., description="Feeder needing flexibility")
    requested_power_kw: float = Field(
        ..., ge=0, description="Requested flexibility amount in kW"
    )
    direction: str = Field(
        default="both",
        description="Flexibility direction: 'import', 'export', or 'both'",
    )
    needed_from: datetime = Field(
        ..., description="When flexibility is needed from"
    )
    needed_until: datetime = Field(
        ..., description="When flexibility is needed until"
    )
    priority: int = Field(
        default=1,
        ge=0,
        le=3,
        description="Request priority (0=lowest, 3=emergency)",
    )
    reason: str = Field(
        default="congestion_management",
        description="Reason for the flexibility need",
    )


class FlexibilityRequestResponse(BaseModel):
    """Response schema for a published flexibility request.

    Includes auto-generated fields (``request_id``, ``created_at``,
    ``sensitivity``) in addition to the submitted data.
    """

    request_id: str = Field(..., description="Unique request identifier")
    feeder_id: str = Field(..., description="Feeder needing flexibility")
    requested_power_kw: float = Field(
        ..., ge=0, description="Requested flexibility amount in kW"
    )
    direction: str = Field(
        ..., description="Flexibility direction: 'import', 'export', or 'both'"
    )
    needed_from: datetime = Field(
        ..., description="When flexibility is needed from"
    )
    needed_until: datetime = Field(
        ..., description="When flexibility is needed until"
    )
    priority: int = Field(
        ..., ge=0, le=3, description="Request priority (0=lowest, 3=emergency)"
    )
    reason: str = Field(..., description="Reason for the flexibility need")
    sensitivity: SensitivityTier = Field(
        default=SensitivityTier.MEDIUM,
        description="Data sensitivity classification",
    )
    created_at: datetime = Field(
        ..., description="When this request was created"
    )


# ---------------------------------------------------------------------------
# Module-level store instance
# ---------------------------------------------------------------------------

# The store is initialised at module import and shared across all route
# handlers.  The ``create_router()`` factory accepts an optional store so
# tests can inject a custom (e.g. in-memory) instance.

_default_store = DSOStore()


# ---------------------------------------------------------------------------
# Router factory
# ---------------------------------------------------------------------------


def create_router(
    store: Optional[DSOStore] = None,
    audit_logger: Optional[AuditLogger] = None,
) -> APIRouter:
    """Create the DSO API router with the given data store and audit logger.

    Args:
        store: The DSO data store to use.  When ``None``, the module-level
            default store (SQLite file-backed) is used.
        audit_logger: The audit logger for the ``/api/v1/audit`` endpoint.
            When ``None``, the audit endpoint returns an empty list.

    Returns:
        A :class:`~fastapi.APIRouter` with all DSO endpoints registered.
    """
    dso_store = store or _default_store
    _audit = audit_logger

    # In-memory storage for flexibility requests.  In production these
    # would be persisted to the database; for the prototype, in-memory
    # storage is sufficient.
    _flexibility_requests: list[FlexibilityRequestResponse] = []

    router = APIRouter()

    # -- Health endpoint -----------------------------------------------------

    @router.get(
        "/health",
        summary="Health check",
        response_class=JSONResponse,
    )
    async def health() -> dict[str, str]:
        """Return a simple health status for infrastructure probes."""
        return {"status": "healthy", "service": "dso-node"}

    # -- Feeder constraint routes --------------------------------------------

    @router.get(
        "/api/v1/constraints",
        response_model=list[FeederConstraint],
        summary="List feeder constraints",
    )
    async def list_constraints(
        feeder_id: Optional[str] = Query(
            default=None,
            description="Filter by feeder identifier",
        ),
    ) -> list[FeederConstraint]:
        """List feeder constraints, optionally filtered by feeder ID.

        Feeder constraints are contract-gated: the ConnectorMiddleware
        verifies that the requester has an active contract with the DSO
        before the request reaches this handler.
        """
        return dso_store.list_feeder_constraints(feeder_id=feeder_id)

    @router.get(
        "/api/v1/constraints/{feeder_id}",
        response_model=FeederConstraint,
        summary="Get specific feeder constraint",
    )
    async def get_constraint(feeder_id: str) -> FeederConstraint:
        """Retrieve the most recent constraint for a specific feeder.

        Returns 404 if no constraint exists for the given feeder ID.
        """
        constraint = dso_store.get_feeder_constraint(feeder_id)
        if constraint is None:
            raise HTTPException(
                status_code=404,
                detail=f"Feeder '{feeder_id}' not found",
            )
        return constraint

    # -- Congestion signal routes --------------------------------------------

    @router.get(
        "/api/v1/congestion-signals",
        response_model=list[CongestionSignal],
        summary="Current congestion levels",
    )
    async def list_congestion_signals(
        feeder_id: Optional[str] = Query(
            default=None,
            description="Filter by feeder identifier",
        ),
    ) -> list[CongestionSignal]:
        """List current congestion signals across all feeders.

        Congestion signals are published when congestion levels change.
        Aggregators subscribe to these signals to adjust flexibility offers.
        """
        return dso_store.list_congestion_signals(feeder_id=feeder_id)

    # -- Hosting capacity routes ---------------------------------------------

    @router.get(
        "/api/v1/hosting-capacity",
        response_model=list[HostingCapacity],
        summary="Available hosting capacity per node",
    )
    async def list_hosting_capacity(
        feeder_id: Optional[str] = Query(
            default=None,
            description="Filter by feeder identifier",
        ),
    ) -> list[HostingCapacity]:
        """List available hosting capacity at grid nodes.

        Indicates how much additional generation or load can be connected.
        Data is aggregated per feeder for authorized participants.
        """
        return dso_store.list_hosting_capacity(feeder_id=feeder_id)

    # -- Flexibility request routes ------------------------------------------

    @router.post(
        "/api/v1/flexibility-requests",
        response_model=FlexibilityRequestResponse,
        status_code=201,
        summary="Publish flexibility need",
    )
    async def create_flexibility_request(
        body: FlexibilityRequestCreate,
    ) -> FlexibilityRequestResponse:
        """Publish a flexibility need for a specific feeder.

        The DSO creates a flexibility request when it needs aggregators
        to provide demand response or generation adjustment on a feeder
        (e.g., to manage congestion or voltage issues).
        """
        flex_request = FlexibilityRequestResponse(
            request_id=str(uuid.uuid4()),
            feeder_id=body.feeder_id,
            requested_power_kw=body.requested_power_kw,
            direction=body.direction,
            needed_from=body.needed_from,
            needed_until=body.needed_until,
            priority=body.priority,
            reason=body.reason,
            sensitivity=SensitivityTier.MEDIUM,
            created_at=_utc_now(),
        )
        _flexibility_requests.append(flex_request)
        logger.info(
            "Flexibility request created: id=%s feeder=%s power=%s kW",
            flex_request.request_id,
            flex_request.feeder_id,
            flex_request.requested_power_kw,
        )
        return flex_request

    # -- Audit routes --------------------------------------------------------

    @router.get(
        "/api/v1/audit",
        response_model=list[AuditEntry],
        summary="Query audit log (admin only)",
    )
    async def query_audit(
        requester_id: Optional[str] = Query(
            default=None,
            description="Filter by requester participant ID",
        ),
        action: Optional[AuditAction] = Query(
            default=None,
            description="Filter by audit action (read, write, dispatch, subscribe)",
        ),
        outcome: Optional[AuditOutcome] = Query(
            default=None,
            description="Filter by exchange outcome (success, denied, error)",
        ),
    ) -> list[AuditEntry]:
        """Query the DSO node's audit log.

        Returns audit entries matching the given filters.  This endpoint
        is restricted to admin users (enforced by the ConnectorMiddleware
        and policy engine).
        """
        if _audit is None:
            return []
        return _audit.query(
            requester_id=requester_id,
            action=action,
            outcome=outcome,
        )

    return router
