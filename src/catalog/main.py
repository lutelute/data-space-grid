"""FastAPI application for the Federated Catalog service.

The catalog is shared infrastructure that enables participants to register
data assets with metadata (provider, update frequency, resolution, sensitivity,
policy hints, endpoint URL) and discover assets registered by others.  It also
tracks contract negotiation lifecycle (offer / accept / reject).

The application follows spec Pattern 1 (FastAPI Service with Connector
Middleware): the ``ConnectorMiddleware`` wraps every request with
authentication, policy check, and audit logging.  Health endpoints are
exempt from authentication to allow infrastructure probes.

Usage::

    uvicorn src.catalog.main:app --host 0.0.0.0 --port 8000

Key design decisions:
  - The catalog runs on port 8000 (no TLS) as shared infrastructure.
    Participant nodes use mTLS on their own ports.
  - ``ConnectorMiddleware`` is configured with the ``catalog-001`` participant
    ID and default backends.  In production these would be configured via
    environment variables.
  - The router is created via :func:`~src.catalog.routes.create_router` so
    that tests can inject a custom data store.
  - CORS, versioned API prefix, and other FastAPI niceties can be added as
    the prototype evolves.
"""

from __future__ import annotations

from fastapi import FastAPI

from src.catalog.routes import create_router
from src.connector.audit import AuditLogger
from src.connector.auth import KeycloakAuthBackend
from src.connector.middleware import ConnectorMiddleware

# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------


def create_app() -> FastAPI:
    """Create and configure the Federated Catalog FastAPI application.

    Returns:
        A fully configured :class:`~fastapi.FastAPI` application with the
        ``ConnectorMiddleware`` and all catalog routes registered.
    """
    application = FastAPI(
        title="Federated Catalog - Data Space",
        description=(
            "Federated discovery catalog for data asset registration, "
            "search, and contract negotiation in the power sector data space."
        ),
        version="0.1.0",
    )

    # Connector middleware: auth + policy + audit on every request.
    # Health endpoints are exempt from auth by default.
    application.add_middleware(
        ConnectorMiddleware,
        auth_backend=KeycloakAuthBackend(),
        audit_logger=AuditLogger(log_path="./audit/catalog.jsonl"),
        participant_id="catalog-001",
    )

    # Register all catalog routes (health, assets, contracts).
    router = create_router()
    application.include_router(router)

    return application


# Module-level app instance used by uvicorn.
app: FastAPI = create_app()
