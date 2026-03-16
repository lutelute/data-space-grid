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
  - An :class:`~src.connector.events.EventBus` is initialised at startup
    and closed at shutdown.  The Aggregator subscribes to the
    ``dispatch-commands`` and ``congestion-alerts`` topics to receive DSO
    instructions and grid state changes, and the dispatch-response route
    publishes ``DispatchActual`` events to the ``dispatch-actuals`` topic.
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
from src.participants.aggregator.routes import create_router
from src.semantic.cim import CongestionSignal
from src.semantic.openadr import DispatchCommand

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Event handlers
# ---------------------------------------------------------------------------


def _handle_dispatch_command(event: DispatchCommand) -> None:
    """Handle a dispatch-commands event received from the DSO.

    The Aggregator subscribes to the ``dispatch-commands`` topic so it can
    receive real-time dispatch instructions from the DSO.  This handler
    logs the received command for observability; in production it would
    trigger DER fleet coordination logic.
    """
    logger.info(
        "Received dispatch command: command_id=%s feeder=%s "
        "target_power=%s kW emergency=%s",
        event.command_id,
        event.feeder_id,
        event.target_power_kw,
        event.is_emergency,
    )


def _handle_congestion_alert(event: CongestionSignal) -> None:
    """Handle a congestion-alerts event received from the DSO.

    The Aggregator subscribes to the ``congestion-alerts`` topic so it can
    adjust flexibility offers in response to real-time grid congestion
    changes.  This handler logs the received signal; in production it
    would trigger offer re-evaluation.
    """
    logger.info(
        "Received congestion alert: signal_id=%s feeder=%s "
        "congestion_level=%.2f remaining_capacity=%s kW",
        event.signal_id,
        event.feeder_id,
        event.congestion_level,
        event.max_available_capacity_kw,
    )


# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------


def create_app() -> FastAPI:
    """Create and configure the Aggregator participant node FastAPI application.

    Returns:
        A fully configured :class:`~fastapi.FastAPI` application with the
        ``ConnectorMiddleware``, event bus lifecycle hooks, and all
        Aggregator routes registered.
    """

    # Event bus: shared across routes and lifecycle hooks.
    event_bus = EventBus()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        """Manage event bus startup and shutdown lifecycle.

        On startup the Aggregator registers handlers for the
        ``dispatch-commands`` and ``congestion-alerts`` topics.  On
        shutdown the bus drains any queued events and closes Kafka
        connections.
        """
        # -- Startup --
        event_bus.register_handler(
            Topic.DISPATCH_COMMANDS, _handle_dispatch_command
        )
        event_bus.register_handler(
            Topic.CONGESTION_ALERTS, _handle_congestion_alert
        )
        logger.info(
            "Aggregator event bus started: subscribed to %s, %s",
            Topic.DISPATCH_COMMANDS.value,
            Topic.CONGESTION_ALERTS.value,
        )
        yield
        # -- Shutdown --
        event_bus.drain_offline_queue()
        event_bus.close()
        logger.info("Aggregator event bus shut down")

    application = FastAPI(
        title="Aggregator Node - Federated Data Space",
        description=(
            "DER Aggregator participant node for the power sector federated "
            "data space. Publishes aggregate flexibility envelopes, "
            "availability windows, baselines, and dispatch responses."
        ),
        version="0.1.0",
        lifespan=lifespan,
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
    # availability, baseline, dispatch response, audit).  The event bus
    # is injected so that the dispatch-response endpoint can publish
    # actuals to Kafka.
    router = create_router(audit_logger=audit_logger, event_bus=event_bus)
    application.include_router(router)

    return application


# Module-level app instance used by uvicorn.
app: FastAPI = create_app()
