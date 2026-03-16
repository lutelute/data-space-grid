"""FastAPI application for the DSO (Distribution System Operator) participant node.

The DSO node publishes grid operational data to the federated data space:
feeder constraints, congestion signals, hosting capacity, and flexibility
requests.  Other participants (aggregators, prosumers) discover these assets
via the federated catalog and negotiate contracts for access.

The application follows spec Pattern 1 (FastAPI Service with Connector
Middleware): the ``ConnectorMiddleware`` wraps every request with
authentication, policy check, and audit logging.  Health endpoints are
exempt from authentication to allow infrastructure probes.

Usage::

    uvicorn src.participants.dso.main:app --host 0.0.0.0 --port 8001 \
        --ssl-keyfile certs/dso.key --ssl-certfile certs/dso.crt \
        --ssl-ca-certs certs/ca.crt

Key design decisions:
  - The DSO node runs on port 8001 with mTLS for service-to-service trust.
  - ``ConnectorMiddleware`` is configured with the ``dso-001`` participant
    ID and default backends.  In production these would be configured via
    environment variables.
  - The audit logger is shared between the middleware and the router so
    that the ``GET /api/v1/audit`` endpoint can query the same audit
    entries recorded by the middleware.
  - The router is created via :func:`~src.participants.dso.routes.create_router`
    so that tests can inject a custom data store and audit logger.
"""

from __future__ import annotations

from fastapi import FastAPI

from src.connector.audit import AuditLogger
from src.connector.auth import KeycloakAuthBackend
from src.connector.middleware import ConnectorMiddleware
from src.participants.dso.routes import create_router

# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------


def create_app() -> FastAPI:
    """Create and configure the DSO participant node FastAPI application.

    Returns:
        A fully configured :class:`~fastapi.FastAPI` application with the
        ``ConnectorMiddleware`` and all DSO routes registered.
    """
    application = FastAPI(
        title="DSO Node - Federated Data Space",
        description=(
            "Distribution System Operator participant node for the power "
            "sector federated data space. Publishes feeder constraints, "
            "congestion signals, hosting capacity, and flexibility requests."
        ),
        version="0.1.0",
    )

    # Shared audit logger: used by both the middleware (to record every
    # exchange) and the /api/v1/audit route (to query entries).
    audit_logger = AuditLogger(log_path="./audit/dso.jsonl")

    # Connector middleware: auth + policy + audit on every request.
    # Health endpoints are exempt from auth by default.
    application.add_middleware(
        ConnectorMiddleware,
        auth_backend=KeycloakAuthBackend(),
        audit_logger=audit_logger,
        participant_id="dso-001",
    )

    # Register all DSO routes (health, constraints, congestion signals,
    # hosting capacity, flexibility requests, audit).
    router = create_router(audit_logger=audit_logger)
    application.include_router(router)

    return application


# Module-level app instance used by uvicorn.
app: FastAPI = create_app()
