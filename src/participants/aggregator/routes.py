"""API routes for the Aggregator participant node.

The Aggregator publishes aggregate flexibility envelopes F(t) to the federated
data space without exposing individual device states x_i.  Other participants
(DSO, prosumers) discover these assets via the federated catalog and negotiate
contracts for access.

Routes:

  **Health**
    ``GET /health`` -- infrastructure health probe (exempt from auth).

  **Flexibility offers** (contract-gated)
    ``GET  /api/v1/flexibility-offers``  -- list aggregate flexibility envelopes
                                            (optional ``feeder_id`` filter).
    ``POST /api/v1/flexibility-offers``  -- submit a new flexibility offer.

  **Availability windows**
    ``GET  /api/v1/availability``        -- list availability windows
                                            (optional ``envelope_id`` filter).

  **Baseline consumption**
    ``GET  /api/v1/baseline``            -- list baseline profiles
                                            (optional ``event_id`` filter).

  **Dispatch response**
    ``POST /api/v1/dispatch-response``   -- report dispatch actuals to DSO.
        Also publishes a ``DispatchActual`` to the ``dispatch-actuals``
        Kafka topic via the event bus.

  **Audit**
    ``GET  /api/v1/audit``               -- query audit log (admin only).

Key design decisions:
  - The router delegates all persistence to
    :class:`~src.participants.aggregator.store.AggregatorStore`.
  - The ``create_router()`` factory accepts an optional store, audit
    logger, and event bus so that tests can inject custom instances.
  - When an ``EventBus`` is provided, the dispatch-response endpoint
    publishes a ``DispatchActual`` to the ``dispatch-actuals`` topic
    so that the DSO receives aggregator performance data.
  - Query parameters for flexibility offers and availability windows use
    ``feeder_id`` and ``envelope_id`` for consistent filtering across
    endpoints.
  - The flexibility envelope computation for GET requests uses
    :func:`~src.participants.aggregator.flexibility.compute_aggregate_flexibility`
    to produce a consolidated view of the DER fleet.
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
from src.connector.events import EventBus, EventBusError, Topic
from src.connector.models import AuditAction, AuditEntry, AuditOutcome
from src.participants.aggregator.flexibility import (
    compute_aggregate_flexibility,
)
from src.participants.aggregator.store import AggregatorStore
from src.semantic.cim import SensitivityTier
from src.semantic.iec61850 import (
    AvailabilityWindow,
    FlexibilityDirection,
    FlexibilityEnvelope,
    PQRange,
    ResponseConfidence,
)
from src.semantic.openadr import Baseline, DispatchActual

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    """Return the current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Request models for flexibility offer submission
# ---------------------------------------------------------------------------


class FlexibilityOfferCreate(BaseModel):
    """Input schema for submitting a new flexibility offer.

    The Aggregator submits this when it wants to advertise available
    flexibility from its DER fleet on a specific feeder.
    """

    unit_id: str = Field(..., description="DER unit this offer applies to")
    aggregator_id: str = Field(
        default="aggregator-001",
        description="Identifier of the offering aggregator",
    )
    feeder_id: str = Field(
        ..., description="Distribution feeder for grid location context"
    )
    direction: FlexibilityDirection = Field(
        ..., description="Direction of offered flexibility"
    )
    pq_range: PQRange = Field(
        ..., description="Active and reactive power operating range"
    )
    availability_windows: list[AvailabilityWindow] = Field(
        default_factory=list,
        description="Time windows when this flexibility is available",
    )
    response_confidence: ResponseConfidence = Field(
        ..., description="Confidence in delivering the offered flexibility"
    )
    price_eur_per_kwh: Optional[float] = Field(
        default=None,
        ge=0,
        description="Indicative price for flexibility activation in EUR/kWh",
    )
    valid_from: datetime = Field(
        ..., description="Start of validity window"
    )
    valid_until: datetime = Field(
        ..., description="End of validity window"
    )


# ---------------------------------------------------------------------------
# Request models for dispatch response submission
# ---------------------------------------------------------------------------


class DispatchResponseCreate(BaseModel):
    """Input schema for reporting dispatch actuals back to the DSO.

    The Aggregator submits this after executing a dispatch command,
    reporting what was actually delivered versus what was commanded.
    """

    command_id: str = Field(
        ..., description="Dispatch command this actual responds to"
    )
    event_id: str = Field(
        ..., description="DR event this actual is part of"
    )
    participant_id: str = Field(
        default="aggregator-001",
        description="Aggregator that executed the dispatch",
    )
    feeder_id: str = Field(
        ..., description="Feeder where the dispatch was executed"
    )
    commanded_kw: float = Field(
        ..., description="Power adjustment that was commanded in kW"
    )
    delivered_kw: float = Field(
        ..., description="Power adjustment actually delivered in kW"
    )
    delivered_kvar: Optional[float] = Field(
        default=None,
        description="Reactive power adjustment actually delivered in kVAr",
    )
    delivery_start: datetime = Field(
        ..., description="When the actual delivery started"
    )
    delivery_end: datetime = Field(
        ..., description="When the actual delivery ended"
    )
    delivery_accuracy_pct: float = Field(
        ...,
        ge=0.0,
        le=100.0,
        description="Percentage of commanded power that was delivered",
    )
    interval_values_kw: list[float] = Field(
        default_factory=list,
        description="Time-series of actual power values per interval in kW",
    )
    interval_minutes: float = Field(
        default=5.0,
        ge=1.0,
        description="Time resolution of interval values in minutes",
    )


# ---------------------------------------------------------------------------
# Module-level store instance
# ---------------------------------------------------------------------------

# The store is initialised at module import and shared across all route
# handlers.  The ``create_router()`` factory accepts an optional store so
# tests can inject a custom (e.g. in-memory) instance.

_default_store = AggregatorStore()


# ---------------------------------------------------------------------------
# Router factory
# ---------------------------------------------------------------------------


def create_router(
    store: Optional[AggregatorStore] = None,
    audit_logger: Optional[AuditLogger] = None,
    event_bus: Optional[EventBus] = None,
) -> APIRouter:
    """Create the Aggregator API router with the given data store and audit logger.

    Args:
        store: The Aggregator data store to use.  When ``None``, the
            module-level default store (SQLite file-backed) is used.
        audit_logger: The audit logger for the ``/api/v1/audit`` endpoint.
            When ``None``, the audit endpoint returns an empty list.
        event_bus: The event bus for publishing dispatch actuals to Kafka.
            When ``None``, events are not published (HTTP-only mode).

    Returns:
        A :class:`~fastapi.APIRouter` with all Aggregator endpoints registered.
    """
    agg_store = store or _default_store
    _audit = audit_logger
    _event_bus = event_bus

    router = APIRouter()

    # -- Health endpoint -----------------------------------------------------

    @router.get(
        "/health",
        summary="Health check",
        response_class=JSONResponse,
    )
    async def health() -> dict[str, str]:
        """Return a simple health status for infrastructure probes."""
        return {"status": "healthy", "service": "aggregator-node"}

    # -- Flexibility offer routes --------------------------------------------

    @router.get(
        "/api/v1/flexibility-offers",
        response_model=list[FlexibilityEnvelope],
        summary="List aggregate flexibility envelopes",
    )
    async def list_flexibility_offers(
        feeder_id: Optional[str] = Query(
            default=None,
            description="Filter by feeder identifier",
        ),
    ) -> list[FlexibilityEnvelope]:
        """List aggregate flexibility envelopes from the DER fleet.

        Returns stored flexibility offers, optionally filtered by feeder ID.
        Flexibility envelopes are contract-gated: the ConnectorMiddleware
        verifies that the requester has an active contract with the
        Aggregator before the request reaches this handler.
        """
        return agg_store.list_flexibility_offers(feeder_id=feeder_id)

    @router.post(
        "/api/v1/flexibility-offers",
        response_model=FlexibilityEnvelope,
        status_code=201,
        summary="Submit flexibility offer",
    )
    async def create_flexibility_offer(
        body: FlexibilityOfferCreate,
    ) -> FlexibilityEnvelope:
        """Submit a new flexibility offer for a DER fleet on a feeder.

        The Aggregator creates a flexibility envelope representing the
        aggregate feasibility region {(P, Q) | feasible} of its DER
        portfolio without exposing individual device states.
        """
        envelope = FlexibilityEnvelope(
            envelope_id=f"FE-AGG-{uuid.uuid4().hex[:8]}",
            unit_id=body.unit_id,
            aggregator_id=body.aggregator_id,
            feeder_id=body.feeder_id,
            direction=body.direction,
            pq_range=body.pq_range,
            availability_windows=body.availability_windows,
            response_confidence=body.response_confidence,
            price_eur_per_kwh=body.price_eur_per_kwh,
            valid_from=body.valid_from,
            valid_until=body.valid_until,
            sensitivity=SensitivityTier.MEDIUM,
            updated_at=_utc_now(),
        )
        agg_store.add_flexibility_offer(envelope)
        logger.info(
            "Flexibility offer created: id=%s feeder=%s direction=%s",
            envelope.envelope_id,
            envelope.feeder_id,
            envelope.direction.value,
        )
        return envelope

    # -- Availability window routes ------------------------------------------

    @router.get(
        "/api/v1/availability",
        response_model=list[AvailabilityWindow],
        summary="Availability windows",
    )
    async def list_availability(
        envelope_id: Optional[str] = Query(
            default=None,
            description="Filter by parent flexibility envelope ID",
        ),
    ) -> list[AvailabilityWindow]:
        """List availability windows when DER flexibility is available.

        Returns time windows with power range and ramp capability details.
        When ``envelope_id`` is provided, only windows for that specific
        flexibility offer are returned.
        """
        return agg_store.list_availability_windows(envelope_id=envelope_id)

    # -- Baseline routes -----------------------------------------------------

    @router.get(
        "/api/v1/baseline",
        response_model=list[Baseline],
        summary="Baseline consumption",
    )
    async def list_baselines(
        event_id: Optional[str] = Query(
            default=None,
            description="Filter by DR event identifier",
        ),
    ) -> list[Baseline]:
        """List baseline consumption profiles for DR settlement.

        Baselines represent expected power consumption or generation in the
        absence of a DR event.  Used to calculate actual demand response
        delivered by comparing actuals against the baseline.
        """
        return agg_store.list_baselines(event_id=event_id)

    # -- Dispatch response routes --------------------------------------------

    @router.post(
        "/api/v1/dispatch-response",
        response_model=DispatchActual,
        status_code=201,
        summary="Report dispatch actuals",
    )
    async def create_dispatch_response(
        body: DispatchResponseCreate,
    ) -> DispatchActual:
        """Report dispatch actuals back to the DSO.

        The Aggregator submits this after executing a dispatch command.
        Reports what was actually delivered versus what was commanded,
        enabling settlement and performance tracking.

        When an event bus is configured, the ``DispatchActual`` is also
        published to the ``dispatch-actuals`` Kafka topic so that the DSO
        can receive the response asynchronously.
        """
        actual = DispatchActual(
            actual_id=f"DA-AGG-{uuid.uuid4().hex[:8]}",
            command_id=body.command_id,
            event_id=body.event_id,
            participant_id=body.participant_id,
            feeder_id=body.feeder_id,
            commanded_kw=body.commanded_kw,
            delivered_kw=body.delivered_kw,
            delivered_kvar=body.delivered_kvar,
            delivery_start=body.delivery_start,
            delivery_end=body.delivery_end,
            delivery_accuracy_pct=body.delivery_accuracy_pct,
            interval_values_kw=body.interval_values_kw,
            interval_minutes=body.interval_minutes,
            reported_at=_utc_now(),
            sensitivity=SensitivityTier.MEDIUM,
        )
        agg_store.add_dispatch_response(actual)
        logger.info(
            "Dispatch response reported: id=%s command=%s delivered=%s kW",
            actual.actual_id,
            actual.command_id,
            actual.delivered_kw,
        )

        # Publish the DispatchActual to the event bus so that the DSO
        # receives the aggregator's response asynchronously.
        if _event_bus is not None:
            try:
                _event_bus.produce(
                    Topic.DISPATCH_ACTUALS, actual, key=body.feeder_id
                )
                logger.info(
                    "DispatchActual published: actual_id=%s feeder=%s",
                    actual.actual_id,
                    actual.feeder_id,
                )
            except EventBusError as exc:
                logger.warning(
                    "Failed to publish DispatchActual %s: %s",
                    actual.actual_id,
                    exc,
                )

        return actual

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
        """Query the Aggregator node's audit log.

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
