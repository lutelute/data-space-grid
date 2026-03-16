"""API routes for the Prosumer (Campus) participant node.

The Prosumer node manages consent-gated consumer data within the federated
data space: smart meter readings, demand profiles, DR eligibility,
controllable margins, and consent records.  All data sharing is governed by
purpose-based consent and minimum-disclosure anonymization — consumer data
is never shared raw.  Other participants (DSO, aggregators) discover these
assets via the federated catalog and negotiate contracts for access.

Routes:

  **Health**
    ``GET /health`` -- infrastructure health probe (exempt from auth).

  **Meter data** (consent-required, prosumer-only or identified-consented)
    ``GET  /api/v1/meter-data``           -- list meter readings
                                             (optional ``meter_id`` filter).

  **Demand profile** (consent + purpose-gated, anonymized per disclosure level)
    ``GET  /api/v1/demand-profile``       -- list demand profiles, anonymized
                                             according to purpose-based
                                             disclosure level.

  **DR eligibility**
    ``GET  /api/v1/dr-eligibility``       -- list DR eligibility records
                                             (optional ``building_id`` filter).

  **Controllable margin**
    ``GET  /api/v1/controllable-margin``  -- list controllable margin records
                                             (optional ``building_id`` filter).

  **Consent management**
    ``GET    /api/v1/consents``           -- list active consent records.
    ``POST   /api/v1/consents``           -- grant a new consent.
    ``DELETE /api/v1/consents/{id}``      -- revoke an existing consent.

  **Audit**
    ``GET  /api/v1/audit``               -- query audit log (admin only).

Key design decisions:
  - The router delegates all persistence to
    :class:`~src.participants.prosumer.store.ProsumerStore`.
  - The ``ConsentManager`` controls purpose-based access to data endpoints.
  - The ``DataAnonymizer`` applies minimum-disclosure transformations before
    any consumer data leaves the node.
  - Meter data is restricted to prosumer-only (``RAW``) or
    identified-consented (``billing``) access.
  - Demand profiles are anonymized according to the requesting purpose's
    disclosure level (spec Pattern 5).
  - The ``create_router()`` factory accepts optional store, consent manager,
    anonymizer, and audit logger so that tests can inject custom instances.
  - All error responses use ``{"detail": ...}`` format consistent with
    FastAPI conventions and the ConnectorMiddleware.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional, Union

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from starlette.responses import JSONResponse

from src.connector.audit import AuditLogger
from src.connector.models import AuditAction, AuditEntry, AuditOutcome
from src.participants.prosumer.anonymizer import (
    ControllableMarginResult,
    DataAnonymizer,
    UnknownPurposeError,
)
from src.participants.prosumer.consent import (
    ConsentAlreadyRevokedError,
    ConsentManager,
    ConsentNotFoundError,
    InvalidConsentPurposeError,
)
from src.participants.prosumer.store import ProsumerStore
from src.semantic.consumer import (
    AnonymizedLoadSeries,
    ConsentRecord,
    DemandProfile,
    DisclosureLevel,
    MeterReading,
    PURPOSE_DISCLOSURE_MAP,
)

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    """Return the current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Request / response models for consent management
# ---------------------------------------------------------------------------


class ConsentGrantRequest(BaseModel):
    """Input schema for granting a new consent record.

    The prosumer submits this to allow a specific requester to access
    their data for a stated purpose.  The disclosure level is automatically
    determined from the purpose via :data:`PURPOSE_DISCLOSURE_MAP`.
    """

    purpose: str = Field(
        ...,
        description=(
            "Data usage purpose (must be a key in PURPOSE_DISCLOSURE_MAP: "
            "research, dr_dispatch, billing, grid_analysis, forecasting)"
        ),
    )
    requester_id: str = Field(
        ..., description="Identifier of the party being granted access"
    )
    expiry: datetime = Field(
        ..., description="When this consent expires (UTC)"
    )
    allowed_data_types: list[str] = Field(
        default_factory=lambda: ["demand_profile"],
        description="Data types covered by this consent",
    )
    valid_from: Optional[datetime] = Field(
        default=None,
        description="When the consent becomes effective (defaults to now)",
    )


class ControllableMarginResponse(BaseModel):
    """Response schema for controllability-only disclosure.

    Contains only the controllable margin in kW — no time-series data,
    no prosumer identity, no consumption patterns.
    """

    controllable_margin_kw: float = Field(
        ..., description="Available controllable margin in kW"
    )


# ---------------------------------------------------------------------------
# Module-level store instance
# ---------------------------------------------------------------------------

# The store is initialised at module import and shared across all route
# handlers.  The ``create_router()`` factory accepts an optional store so
# tests can inject a custom (e.g. in-memory) instance.

_default_store = ProsumerStore()


# ---------------------------------------------------------------------------
# Router factory
# ---------------------------------------------------------------------------


def create_router(
    store: Optional[ProsumerStore] = None,
    consent_manager: Optional[ConsentManager] = None,
    anonymizer: Optional[DataAnonymizer] = None,
    audit_logger: Optional[AuditLogger] = None,
) -> APIRouter:
    """Create the Prosumer API router with the given dependencies.

    Args:
        store: The Prosumer data store to use.  When ``None``, the
            module-level default store (SQLite file-backed) is used.
        consent_manager: The consent manager for controlling data access.
            When ``None``, a default manager for ``prosumer-001`` is created.
        anonymizer: The data anonymizer for purpose-based disclosure.
            When ``None``, a default anonymizer for ``prosumer-001`` is created.
        audit_logger: The audit logger for the ``/api/v1/audit`` endpoint.
            When ``None``, the audit endpoint returns an empty list.

    Returns:
        A :class:`~fastapi.APIRouter` with all Prosumer endpoints registered.
    """
    prosumer_store = store or _default_store
    _consent = consent_manager or ConsentManager(prosumer_id="prosumer-001")
    _anonymizer = anonymizer or DataAnonymizer(prosumer_id="prosumer-001")
    _audit = audit_logger

    router = APIRouter()

    # -- Health endpoint -----------------------------------------------------

    @router.get(
        "/health",
        summary="Health check",
        response_class=JSONResponse,
    )
    async def health() -> dict[str, str]:
        """Return a simple health status for infrastructure probes."""
        return {"status": "healthy", "service": "prosumer-node"}

    # -- Meter data routes ---------------------------------------------------

    @router.get(
        "/api/v1/meter-data",
        response_model=list[MeterReading],
        summary="Smart meter readings (consent-required)",
    )
    async def list_meter_data(
        meter_id: Optional[str] = Query(
            default=None,
            description="Filter by smart meter identifier",
        ),
        prosumer_id: Optional[str] = Query(
            default=None,
            description="Filter by prosumer identifier",
        ),
        purpose: str = Query(
            default="billing",
            description="Data usage purpose (determines disclosure level)",
        ),
        requester_id: str = Query(
            default="self",
            description="Identifier of the requesting party",
        ),
    ) -> list[MeterReading]:
        """List smart meter readings (consent-required).

        Meter data is HIGH_PRIVACY and restricted to prosumer-only (self
        access with ``RAW`` disclosure) or identified-consented access
        (``billing`` purpose with explicit consent).  All other purposes
        are denied — meter data is too granular for anonymized sharing.

        The ConnectorMiddleware verifies contract-level authorization;
        this handler additionally enforces consent-level access control.
        """
        # Self-access: prosumer viewing their own data
        if requester_id == "self":
            return prosumer_store.list_meter_readings(
                meter_id=meter_id,
                prosumer_id=prosumer_id,
            )

        # External access: consent check required
        disclosure = _consent.get_disclosure_level(requester_id, purpose)
        if disclosure is None:
            raise HTTPException(
                status_code=403,
                detail=(
                    f"No active consent for requester '{requester_id}' "
                    f"with purpose '{purpose}'"
                ),
            )

        # Meter data only available at RAW or IDENTIFIED_CONSENTED level
        if disclosure not in (
            DisclosureLevel.RAW,
            DisclosureLevel.IDENTIFIED_CONSENTED,
        ):
            raise HTTPException(
                status_code=403,
                detail=(
                    f"Meter data not available at disclosure level "
                    f"'{disclosure.value}' (purpose: '{purpose}'). "
                    f"Meter data requires 'billing' purpose or self-access."
                ),
            )

        return prosumer_store.list_meter_readings(
            meter_id=meter_id,
            prosumer_id=prosumer_id,
        )

    # -- Demand profile routes -----------------------------------------------

    @router.get(
        "/api/v1/demand-profile",
        summary="Demand profiles (consent + purpose-gated, anonymized)",
    )
    async def list_demand_profiles(
        prosumer_id: Optional[str] = Query(
            default=None,
            description="Filter by prosumer identifier",
        ),
        profile_type: Optional[str] = Query(
            default=None,
            description="Filter by profile type (e.g., 'typical_day')",
        ),
        purpose: str = Query(
            ...,
            description=(
                "Data usage purpose (determines anonymization level): "
                "research, dr_dispatch, billing, grid_analysis, forecasting"
            ),
        ),
        requester_id: str = Query(
            ...,
            description="Identifier of the requesting party",
        ),
    ) -> Union[
        list[DemandProfile],
        list[AnonymizedLoadSeries],
        ControllableMarginResponse,
    ]:
        """List demand profiles, anonymized according to purpose.

        The anonymization level is determined by the purpose via
        :data:`PURPOSE_DISCLOSURE_MAP` (spec Pattern 5):

        - ``billing``: Identified-consented (identity intact).
        - ``forecasting``: Anonymized (identity stripped).
        - ``research`` / ``grid_analysis``: Aggregated (statistical only).
        - ``dr_dispatch``: Controllability-only (single scalar).

        Requires active consent matching the requester and purpose.
        """
        # Consent check
        disclosure = _consent.get_disclosure_level(requester_id, purpose)
        if disclosure is None:
            raise HTTPException(
                status_code=403,
                detail=(
                    f"No active consent for requester '{requester_id}' "
                    f"with purpose '{purpose}'"
                ),
            )

        # Retrieve raw profiles from store
        profiles = prosumer_store.list_demand_profiles(
            prosumer_id=prosumer_id,
            profile_type=profile_type,
        )

        if not profiles:
            raise HTTPException(
                status_code=404,
                detail="No demand profiles found matching the given filters",
            )

        # Apply anonymization based on purpose
        try:
            # For CONTROLLABILITY_ONLY: return a single scalar
            if disclosure == DisclosureLevel.CONTROLLABILITY_ONLY:
                margin = _anonymizer.compute_controllable_margin(profiles)
                return ControllableMarginResponse(
                    controllable_margin_kw=margin.controllable_margin_kw,
                )

            # For AGGREGATED: aggregate all profiles into one series
            if disclosure == DisclosureLevel.AGGREGATED:
                series = _anonymizer.aggregate_load_series(profiles)
                return [series]

            # For RAW, IDENTIFIED_CONSENTED, ANONYMIZED: transform each profile
            anonymized = []
            for profile in profiles:
                result = _anonymizer.anonymize_demand_profile(profile, purpose)
                if isinstance(result, DemandProfile):
                    anonymized.append(result)
            return anonymized

        except UnknownPurposeError as exc:
            raise HTTPException(
                status_code=400,
                detail=str(exc),
            ) from exc

    # -- DR eligibility routes -----------------------------------------------

    @router.get(
        "/api/v1/dr-eligibility",
        summary="DR participation eligibility",
    )
    async def list_dr_eligibility(
        building_id: Optional[str] = Query(
            default=None,
            description="Filter by building identifier",
        ),
        feeder_id: Optional[str] = Query(
            default=None,
            description="Filter by feeder identifier",
        ),
    ) -> list[dict]:
        """List demand response program eligibility records.

        Returns information about which DR programs each building can
        participate in and the maximum demand reduction available.
        Contract-gated via the ConnectorMiddleware.
        """
        return prosumer_store.list_dr_eligibility(
            building_id=building_id,
            feeder_id=feeder_id,
        )

    # -- Controllable margin routes ------------------------------------------

    @router.get(
        "/api/v1/controllable-margin",
        summary="Available controllable margin",
    )
    async def list_controllable_margins(
        building_id: Optional[str] = Query(
            default=None,
            description="Filter by building identifier",
        ),
        feeder_id: Optional[str] = Query(
            default=None,
            description="Filter by feeder identifier",
        ),
    ) -> list[dict]:
        """List pre-computed controllable margin data per building.

        Controllable margins represent the available load flexibility
        for DR dispatch purposes.  Contract-gated via the
        ConnectorMiddleware.
        """
        return prosumer_store.list_controllable_margins(
            building_id=building_id,
            feeder_id=feeder_id,
        )

    # -- Consent management routes -------------------------------------------

    @router.get(
        "/api/v1/consents",
        response_model=list[ConsentRecord],
        summary="List active consents",
    )
    async def list_consents() -> list[ConsentRecord]:
        """List all currently active consent records.

        Returns consent records with ``ACTIVE`` status.  Automatically
        expires consents whose validity window has passed.  This endpoint
        is accessible only by the prosumer themselves.
        """
        return _consent.list_active_consents()

    @router.post(
        "/api/v1/consents",
        response_model=ConsentRecord,
        status_code=201,
        summary="Grant consent",
    )
    async def grant_consent(
        body: ConsentGrantRequest,
    ) -> ConsentRecord:
        """Grant consent for a specific purpose and requester.

        The prosumer grants permission for a named requester to access
        their data for a stated purpose.  The disclosure level is
        automatically determined from the purpose via
        :data:`PURPOSE_DISCLOSURE_MAP`.

        Unknown purposes are rejected (fail-closed).
        """
        try:
            consent = _consent.grant_consent(
                purpose=body.purpose,
                requester_id=body.requester_id,
                expiry=body.expiry,
                allowed_data_types=body.allowed_data_types,
                valid_from=body.valid_from,
            )
        except InvalidConsentPurposeError as exc:
            raise HTTPException(
                status_code=400,
                detail=str(exc),
            ) from exc

        logger.info(
            "Consent granted: id=%s requester=%s purpose=%s disclosure=%s",
            consent.consent_id,
            consent.requester_id,
            consent.purpose,
            consent.disclosure_level.value,
        )
        return consent

    @router.delete(
        "/api/v1/consents/{consent_id}",
        response_model=ConsentRecord,
        summary="Revoke consent",
    )
    async def revoke_consent(consent_id: str) -> ConsentRecord:
        """Revoke an active consent immediately.

        Revocation takes effect for all subsequent data requests.
        In-flight exchanges that started before revocation are not
        affected (they complete with the data already released).
        """
        try:
            consent = _consent.revoke_consent(consent_id)
        except ConsentNotFoundError as exc:
            raise HTTPException(
                status_code=404,
                detail=str(exc),
            ) from exc
        except ConsentAlreadyRevokedError as exc:
            raise HTTPException(
                status_code=409,
                detail=str(exc),
            ) from exc

        logger.info(
            "Consent revoked: id=%s requester=%s purpose=%s",
            consent.consent_id,
            consent.requester_id,
            consent.purpose,
        )
        return consent

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
        """Query the Prosumer node's audit log.

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
