"""FastAPI application for the Aggregator participant node.

The Aggregator node publishes aggregate flexibility envelopes F(t) to the
federated data space without exposing individual device states x_i.  Other
participants (DSO, prosumers) discover these assets via the federated catalog
and negotiate contracts for access.

The application follows spec Pattern 1 (FastAPI Service with Connector
Middleware): the ``ConnectorMiddleware`` wraps every request with
authentication, policy check, and audit logging.  Health endpoints are
exempt from authentication to allow infrastructure probes.

Usage::

    uvicorn src.participants.aggregator.main:app --host 0.0.0.0 --port 8002 \
        --ssl-keyfile certs/aggregator.key --ssl-certfile certs/aggregator.crt \
        --ssl-ca-certs certs/ca.crt

Key design decisions:
  - The Aggregator node runs on port 8002 with mTLS for service-to-service
    trust.
  - ``ConnectorMiddleware`` is configured with the ``aggregator-001``
    participant ID and default backends.  In production these would be
    configured via environment variables.
  - The audit logger is shared between the middleware and the router so
    that the ``GET /api/v1/audit`` endpoint can query the same audit
    entries recorded by the middleware.
  - The router is created via
    :func:`~src.participants.aggregator.routes.create_router` so that
    tests can inject a custom data store and audit logger.
"""

from __future__ import annotations

from fastapi import FastAPI

from src.connector.audit import AuditLogger
from src.connector.auth import KeycloakAuthBackend
from src.connector.middleware import ConnectorMiddleware
from src.participants.aggregator.routes import create_router

# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------


def create_app() -> FastAPI:
    """Create and configure the Aggregator participant node FastAPI application.

    Returns:
        A fully configured :class:`~fastapi.FastAPI` application with the
        ``ConnectorMiddleware`` and all Aggregator routes registered.
    """
    application = FastAPI(
        title="Aggregator Node - Federated Data Space",
        description=(
            "DER Aggregator participant node for the power sector federated "
            "data space. Publishes aggregate flexibility envelopes, "
            "availability windows, baselines, and dispatch responses."
        ),
        version="0.1.0",
    )

    # Shared audit logger: used by both the middleware (to record every
    # exchange) and the /api/v1/audit route (to query entries).
    audit_logger = AuditLogger(log_path="./audit/aggregator.jsonl")

    # Connector middleware: auth + policy + audit on every request.
    # Health endpoints are exempt from auth by default.
    application.add_middleware(
        ConnectorMiddleware,
        auth_backend=KeycloakAuthBackend(),
        audit_logger=audit_logger,
        participant_id="aggregator-001",
    )

    # Register all Aggregator routes (health, flexibility offers,
    # availability, baseline, dispatch response, audit).
    router = create_router(audit_logger=audit_logger)
    application.include_router(router)

    return application


# Module-level app instance used by uvicorn.
app: FastAPI = create_app()
