"""FastAPI application for the Prosumer (Campus) participant node.

The Prosumer node manages consent-gated consumer data within the federated
data space: smart meter readings, demand profiles, DR eligibility,
controllable margins, and consent records.  All data sharing is governed by
purpose-based consent and minimum-disclosure anonymization.

The application follows spec Pattern 1 (FastAPI Service with Connector
Middleware): the ``ConnectorMiddleware`` wraps every request with
authentication, policy check, and audit logging.  Health endpoints are
exempt from authentication to allow infrastructure probes.

Usage::

    uvicorn src.participants.prosumer.main:app --host 0.0.0.0 --port 8003 \
        --ssl-keyfile certs/prosumer.key --ssl-certfile certs/prosumer.crt \
        --ssl-ca-certs certs/ca.crt

Key design decisions:
  - The Prosumer node runs on port 8003 with mTLS for service-to-service
    trust.
  - ``ConnectorMiddleware`` is configured with the ``prosumer-001``
    participant ID and default backends.  In production these would be
    configured via environment variables.
  - The audit logger is shared between the middleware and the router so
    that the ``GET /api/v1/audit`` endpoint can query the same audit
    entries recorded by the middleware.
  - The router is created via
    :func:`~src.participants.prosumer.routes.create_router` so that
    tests can inject a custom data store, consent manager, anonymizer,
    and audit logger.
"""

from __future__ import annotations

from fastapi import FastAPI

from src.connector.audit import AuditLogger
from src.connector.auth import KeycloakAuthBackend
from src.connector.middleware import ConnectorMiddleware
from src.participants.prosumer.routes import create_router

# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------


def create_app() -> FastAPI:
    """Create and configure the Prosumer participant node FastAPI application.

    Returns:
        A fully configured :class:`~fastapi.FastAPI` application with the
        ``ConnectorMiddleware`` and all Prosumer routes registered.
    """
    application = FastAPI(
        title="Prosumer Node - Federated Data Space",
        description=(
            "Campus prosumer participant node for the power sector federated "
            "data space. Manages consent-gated anonymized demand profiles, "
            "smart meter data, DR eligibility, and controllable margins."
        ),
        version="0.1.0",
    )

    # Shared audit logger: used by both the middleware (to record every
    # exchange) and the /api/v1/audit route (to query entries).
    audit_logger = AuditLogger(log_path="./audit/prosumer.jsonl")

    # Connector middleware: auth + policy + audit on every request.
    # Health endpoints are exempt from auth by default.
    application.add_middleware(
        ConnectorMiddleware,
        auth_backend=KeycloakAuthBackend(),
        audit_logger=audit_logger,
        participant_id="prosumer-001",
    )

    # Register all Prosumer routes (health, meter data, demand profiles,
    # DR eligibility, controllable margins, consents, audit).
    router = create_router(audit_logger=audit_logger)
    application.include_router(router)

    return application


# Module-level app instance used by uvicorn.
app: FastAPI = create_app()
