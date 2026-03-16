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
  - An :class:`~src.connector.events.EventBus` is initialised at startup
    and closed at shutdown.  The DSO subscribes to the ``dispatch-actuals``
    topic to receive aggregator dispatch responses, and the flexibility-
    requests route publishes ``DispatchCommand`` events to the
    ``dispatch-commands`` topic.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from src.connector.audit import AuditLogger
from src.connector.auth import KeycloakAuthBackend
from src.connector.events import EventBus, Topic
from src.connector.middleware import ConnectorMiddleware
from src.participants.dso.routes import create_router
from src.semantic.openadr import DispatchActual

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Event handlers
# ---------------------------------------------------------------------------


def _handle_dispatch_actual(event: DispatchActual) -> None:
    """Handle a dispatch-actuals event received from an aggregator.

    The DSO subscribes to the ``dispatch-actuals`` topic so it can track
    how aggregators respond to dispatch commands.  This handler logs the
    received actual for observability; in production it would update a
    settlement or monitoring store.
    """
    logger.info(
        "Received dispatch actual: actual_id=%s command_id=%s "
        "delivered=%s kW accuracy=%.1f%%",
        event.actual_id,
        event.command_id,
        event.delivered_kw,
        event.delivery_accuracy_pct,
    )


# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------


def create_app() -> FastAPI:
    """Create and configure the DSO participant node FastAPI application.

    Returns:
        A fully configured :class:`~fastapi.FastAPI` application with the
        ``ConnectorMiddleware``, event bus lifecycle hooks, and all DSO
        routes registered.
    """

    # Event bus: shared across routes and lifecycle hooks.
    event_bus = EventBus()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        """Manage event bus startup and shutdown lifecycle.

        On startup the DSO registers a handler for the ``dispatch-actuals``
        topic so it can receive aggregator responses.  On shutdown the bus
        drains any queued events and closes Kafka connections.
        """
        # -- Startup --
        event_bus.register_handler(
            Topic.DISPATCH_ACTUALS, _handle_dispatch_actual
        )
        logger.info(
            "DSO event bus started: subscribed to %s",
            Topic.DISPATCH_ACTUALS.value,
        )
        yield
        # -- Shutdown --
        event_bus.drain_offline_queue()
        event_bus.close()
        logger.info("DSO event bus shut down")

    application = FastAPI(
        title="DSO Node - Federated Data Space",
        description=(
            "Distribution System Operator participant node for the power "
            "sector federated data space. Publishes feeder constraints, "
            "congestion signals, hosting capacity, and flexibility requests."
        ),
        version="0.1.0",
        lifespan=lifespan,
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
    # hosting capacity, flexibility requests, audit).  The event bus is
    # injected so that the flexibility-requests endpoint can publish
    # dispatch commands to Kafka.
    router = create_router(audit_logger=audit_logger, event_bus=event_bus)
    application.include_router(router)

    return application


# Module-level app instance used by uvicorn.
app: FastAPI = create_app()
