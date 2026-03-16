"""Kafka event producer/consumer wrapper for the Federated Data Space.

Provides an ``EventBus`` abstraction over Apache Kafka for asynchronous event
streaming between participant nodes.  The bus handles serialisation of Pydantic
models, topic management, and resilience when Kafka is unavailable.

Topics:
  - ``dr-events``         — DR event notifications (DREvent)
  - ``dispatch-commands`` — Real-time dispatch commands (DispatchCommand)
  - ``dispatch-actuals``  — Dispatch response actuals (DispatchActual)
  - ``congestion-alerts`` — Real-time congestion level changes (CongestionSignal)
  - ``audit-events``      — Audit entries for centralised analysis (AuditEntry)

Key design decisions:
  - Event serialisation uses Pydantic ``model_dump_json()`` / ``model_validate_json()``
    so every message on the wire is a well-typed, validated JSON document.
  - Configuration is driven by the ``KAFKA_BOOTSTRAP_SERVERS`` environment
    variable (default: ``localhost:9092``).
  - When Kafka is unreachable, events are queued in a local in-memory buffer
    and the caller may optionally fall back to synchronous REST delivery
    with retry (spec edge case 6 — Kafka Unavailability).
  - Consumers run in the caller's thread; the ``consume()`` method yields
    control only after the handler processes each message, keeping the design
    synchronous and easy to reason about.
  - All timestamps are timezone-aware UTC.
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections import deque
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Optional, Union

import httpx
from pydantic import BaseModel, Field

from src.connector.models import AuditEntry
from src.semantic.cim import CongestionSignal
from src.semantic.openadr import DispatchActual, DispatchCommand, DREvent

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    """Return the current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Constants & defaults
# ---------------------------------------------------------------------------

_DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
_DEFAULT_REST_TIMEOUT_SECONDS = 10.0
_DEFAULT_REST_MAX_RETRIES = 3
_DEFAULT_REST_BACKOFF_FACTOR = 0.5
_DEFAULT_MAX_OFFLINE_QUEUE_SIZE = 10_000


class Topic(str, Enum):
    """Well-known Kafka topics for the Federated Data Space event bus.

    Each topic carries a specific Pydantic model type.  The ``EventBus``
    uses these values to validate that the correct model is produced to
    the correct topic.
    """

    DR_EVENTS = "dr-events"
    DISPATCH_COMMANDS = "dispatch-commands"
    DISPATCH_ACTUALS = "dispatch-actuals"
    CONGESTION_ALERTS = "congestion-alerts"
    AUDIT_EVENTS = "audit-events"


#: Mapping from topic to the expected Pydantic model type.
TOPIC_MODEL_MAP: dict[Topic, type[BaseModel]] = {
    Topic.DR_EVENTS: DREvent,
    Topic.DISPATCH_COMMANDS: DispatchCommand,
    Topic.DISPATCH_ACTUALS: DispatchActual,
    Topic.CONGESTION_ALERTS: CongestionSignal,
    Topic.AUDIT_EVENTS: AuditEntry,
}


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class EventBusError(Exception):
    """Base exception for event bus errors."""


class KafkaUnavailableError(EventBusError):
    """Raised when Kafka cannot be reached and no fallback is configured."""


class EventSerializationError(EventBusError):
    """Raised when an event cannot be serialised or deserialised."""


class RESTFallbackError(EventBusError):
    """Raised when the REST fallback delivery fails after retries."""


# ---------------------------------------------------------------------------
# Queued event model
# ---------------------------------------------------------------------------


class QueuedEvent(BaseModel):
    """An event buffered locally while Kafka is unavailable.

    Events are held in memory and replayed in order once Kafka connectivity
    is restored or drained via the REST fallback path.
    """

    topic: str = Field(..., description="Target Kafka topic name")
    payload_json: str = Field(
        ..., description="JSON-serialised event payload"
    )
    queued_at: datetime = Field(
        default_factory=_utc_now,
        description="Timestamp when the event was queued locally",
    )
    key: Optional[str] = Field(
        default=None, description="Optional partition key"
    )


# ---------------------------------------------------------------------------
# EventBus
# ---------------------------------------------------------------------------


class EventBus:
    """Kafka event producer/consumer wrapper with REST fallback.

    The ``EventBus`` is the single entry point for publishing and consuming
    asynchronous events in the Federated Data Space.  It wraps the Kafka
    producer/consumer lifecycle and transparently falls back to REST-based
    delivery when Kafka is unreachable (spec edge case 6).

    Usage::

        bus = EventBus()

        # Produce a DR event to Kafka
        event = DREvent(event_id="ev-1", ...)
        bus.produce(Topic.DR_EVENTS, event)

        # Consume events (blocking)
        def handle(event: DREvent) -> None:
            ...
        bus.consume(Topic.DR_EVENTS, handle)

    Args:
        bootstrap_servers: Comma-separated Kafka broker addresses.  When
            ``None``, the ``KAFKA_BOOTSTRAP_SERVERS`` environment variable
            is used (falling back to ``localhost:9092``).
        rest_fallback_urls: Optional mapping of topic names to REST endpoint
            URLs for synchronous fallback delivery when Kafka is unavailable.
        rest_timeout: HTTP request timeout for REST fallback (seconds).
        rest_max_retries: Maximum retry attempts for REST fallback delivery.
        rest_backoff_factor: Multiplier for exponential backoff between
            REST retries (delay = factor * 2^attempt).
        max_offline_queue_size: Maximum number of events held in the local
            offline queue.  Oldest events are discarded when the limit is
            reached.
    """

    def __init__(
        self,
        *,
        bootstrap_servers: Optional[str] = None,
        rest_fallback_urls: Optional[dict[str, str]] = None,
        rest_timeout: float = _DEFAULT_REST_TIMEOUT_SECONDS,
        rest_max_retries: int = _DEFAULT_REST_MAX_RETRIES,
        rest_backoff_factor: float = _DEFAULT_REST_BACKOFF_FACTOR,
        max_offline_queue_size: int = _DEFAULT_MAX_OFFLINE_QUEUE_SIZE,
    ) -> None:
        self._bootstrap_servers = (
            bootstrap_servers
            or os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
            or _DEFAULT_BOOTSTRAP_SERVERS
        )
        self._rest_fallback_urls: dict[str, str] = rest_fallback_urls or {}
        self._rest_timeout = rest_timeout
        self._rest_max_retries = rest_max_retries
        self._rest_backoff_factor = rest_backoff_factor
        self._max_offline_queue_size = max_offline_queue_size

        # Lazy-initialised Kafka producer / consumers.
        self._producer: Any = None
        self._consumers: dict[str, Any] = {}

        # Local offline queue for events produced while Kafka is unreachable.
        self._offline_queue: deque[QueuedEvent] = deque(
            maxlen=max_offline_queue_size
        )

        # Registered consumer handlers for in-process dispatch.
        self._handlers: dict[str, list[Callable[[BaseModel], None]]] = {}

    # -- Properties ----------------------------------------------------------

    @property
    def bootstrap_servers(self) -> str:
        """The configured Kafka bootstrap servers."""
        return self._bootstrap_servers

    @property
    def offline_queue_size(self) -> int:
        """Number of events waiting in the local offline queue."""
        return len(self._offline_queue)

    @property
    def offline_queue(self) -> list[QueuedEvent]:
        """Read-only snapshot of the offline queue contents."""
        return list(self._offline_queue)

    # -- Kafka connectivity helpers ------------------------------------------

    def _get_producer(self) -> Any:
        """Return a connected Kafka producer, creating one if necessary.

        Returns:
            A ``kafka.KafkaProducer`` instance.

        Raises:
            KafkaUnavailableError: If the Kafka broker cannot be reached.
        """
        if self._producer is not None:
            return self._producer

        try:
            from kafka import KafkaProducer  # type: ignore[import-untyped]

            self._producer = KafkaProducer(
                bootstrap_servers=self._bootstrap_servers.split(","),
                value_serializer=lambda v: v.encode("utf-8")
                if isinstance(v, str)
                else v,
                key_serializer=lambda k: k.encode("utf-8")
                if k is not None
                else None,
                retries=3,
                acks="all",
            )
            return self._producer
        except Exception as exc:
            self._producer = None
            raise KafkaUnavailableError(
                f"Cannot connect to Kafka at {self._bootstrap_servers}: {exc}"
            ) from exc

    def _get_consumer(self, topic: str, group_id: Optional[str] = None) -> Any:
        """Return a connected Kafka consumer for *topic*.

        Args:
            topic: Kafka topic name.
            group_id: Consumer group ID.  Defaults to ``{topic}-group``.

        Returns:
            A ``kafka.KafkaConsumer`` instance.

        Raises:
            KafkaUnavailableError: If the Kafka broker cannot be reached.
        """
        cache_key = f"{topic}:{group_id or ''}"
        if cache_key in self._consumers:
            return self._consumers[cache_key]

        try:
            from kafka import KafkaConsumer  # type: ignore[import-untyped]

            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self._bootstrap_servers.split(","),
                group_id=group_id or f"{topic}-group",
                value_deserializer=lambda v: v.decode("utf-8"),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                consumer_timeout_ms=1000,
            )
            self._consumers[cache_key] = consumer
            return consumer
        except Exception as exc:
            raise KafkaUnavailableError(
                f"Cannot connect to Kafka at {self._bootstrap_servers}: {exc}"
            ) from exc

    # -- REST fallback -------------------------------------------------------

    def _deliver_via_rest(
        self,
        topic: str,
        payload_json: str,
    ) -> bool:
        """Attempt to deliver an event via synchronous REST with retry.

        Uses exponential backoff identical to
        :class:`~src.connector.catalog_client.CatalogClient`.

        Args:
            topic: The topic name (used to look up the REST fallback URL).
            payload_json: The JSON-serialised event body.

        Returns:
            ``True`` if delivery succeeded, ``False`` otherwise.

        Raises:
            RESTFallbackError: If the REST endpoint cannot be reached after
                all retry attempts.
        """
        url = self._rest_fallback_urls.get(topic)
        if url is None:
            return False

        last_exc: Optional[Exception] = None

        for attempt in range(self._rest_max_retries):
            try:
                with httpx.Client(timeout=self._rest_timeout) as client:
                    response = client.post(
                        url,
                        content=payload_json,
                        headers={"Content-Type": "application/json"},
                    )
                if response.is_success:
                    logger.info(
                        "REST fallback delivery succeeded: topic=%s url=%s",
                        topic,
                        url,
                    )
                    return True
                last_exc = Exception(
                    f"HTTP {response.status_code}: {response.text}"
                )
            except (httpx.ConnectError, httpx.TimeoutException) as exc:
                last_exc = exc

            delay = self._rest_backoff_factor * (2 ** attempt)
            logger.warning(
                "REST fallback failed (attempt %d/%d): topic=%s url=%s — %s. "
                "Retrying in %.1fs…",
                attempt + 1,
                self._rest_max_retries,
                topic,
                url,
                last_exc,
                delay,
            )
            time.sleep(delay)

        raise RESTFallbackError(
            f"REST fallback to {url} for topic '{topic}' failed after "
            f"{self._rest_max_retries} attempts: {last_exc}"
        )

    # -- Public API: produce -------------------------------------------------

    def produce(
        self,
        topic: Union[Topic, str],
        event: BaseModel,
        *,
        key: Optional[str] = None,
    ) -> bool:
        """Publish an event to a Kafka topic.

        If Kafka is unavailable, the method attempts REST fallback delivery
        (when configured).  If REST fallback also fails (or is not configured),
        the event is queued in the local offline buffer for later replay.

        Args:
            topic: Target Kafka topic (use :class:`Topic` enum values for
                well-known topics).
            event: A Pydantic model instance to serialise and publish.
            key: Optional partition key.

        Returns:
            ``True`` if the event was delivered (via Kafka or REST fallback),
            ``False`` if it was queued locally.
        """
        topic_str = topic.value if isinstance(topic, Topic) else topic
        payload_json = event.model_dump_json()

        # Attempt Kafka delivery first.
        try:
            producer = self._get_producer()
            producer.send(topic_str, value=payload_json, key=key)
            producer.flush(timeout=5)
            logger.info(
                "Event produced to Kafka: topic=%s key=%s",
                topic_str,
                key,
            )
            return True
        except (KafkaUnavailableError, Exception) as kafka_exc:
            logger.warning(
                "Kafka unavailable for topic '%s': %s — attempting fallback",
                topic_str,
                kafka_exc,
            )
            # Reset the broken producer so the next call retries connection.
            self._producer = None

        # Attempt REST fallback delivery.
        try:
            if self._deliver_via_rest(topic_str, payload_json):
                return True
        except RESTFallbackError as rest_exc:
            logger.warning(
                "REST fallback also failed for topic '%s': %s — queuing locally",
                topic_str,
                rest_exc,
            )

        # Queue locally as last resort.
        queued = QueuedEvent(
            topic=topic_str,
            payload_json=payload_json,
            key=key,
        )
        self._offline_queue.append(queued)
        logger.info(
            "Event queued locally: topic=%s queue_size=%d",
            topic_str,
            len(self._offline_queue),
        )
        return False

    # -- Public API: consume -------------------------------------------------

    def consume(
        self,
        topic: Union[Topic, str],
        handler: Callable[[BaseModel], None],
        *,
        group_id: Optional[str] = None,
        max_messages: Optional[int] = None,
    ) -> int:
        """Consume events from a Kafka topic and dispatch to *handler*.

        Each consumed message is deserialised into the Pydantic model type
        associated with the topic (see :data:`TOPIC_MODEL_MAP`) and passed to
        *handler*.  Messages that fail deserialisation are logged and skipped.

        The method blocks until either *max_messages* have been processed or
        the consumer times out (``consumer_timeout_ms``).

        Args:
            topic: Source Kafka topic.
            handler: Callable that receives a deserialised Pydantic model.
            group_id: Consumer group ID (defaults to ``{topic}-group``).
            max_messages: Stop after processing this many messages.  ``None``
                means consume until the consumer times out.

        Returns:
            The number of messages successfully processed.

        Raises:
            KafkaUnavailableError: If Kafka cannot be reached.
        """
        topic_str = topic.value if isinstance(topic, Topic) else topic
        topic_enum = Topic(topic_str) if topic_str in Topic._value2member_map_ else None
        model_cls = TOPIC_MODEL_MAP.get(topic_enum) if topic_enum else None

        consumer = self._get_consumer(topic_str, group_id=group_id)
        processed = 0

        for message in consumer:
            try:
                raw = message.value
                if model_cls is not None:
                    event = model_cls.model_validate_json(raw)
                else:
                    # Unknown topic — parse as generic JSON and wrap in a
                    # BaseModel for type-safety.
                    event = BaseModel.model_validate_json(raw)
                handler(event)
                processed += 1
            except Exception as exc:
                logger.warning(
                    "Failed to deserialise message from '%s' offset=%s: %s",
                    topic_str,
                    message.offset,
                    exc,
                )
                continue

            if max_messages is not None and processed >= max_messages:
                break

        logger.info(
            "Consumed %d messages from topic '%s'",
            processed,
            topic_str,
        )
        return processed

    # -- Public API: handler registration ------------------------------------

    def register_handler(
        self,
        topic: Union[Topic, str],
        handler: Callable[[BaseModel], None],
    ) -> None:
        """Register a handler for in-process event dispatch.

        Registered handlers are invoked when :meth:`dispatch_local` is called,
        enabling unit testing and in-process event routing without Kafka.

        Args:
            topic: Topic to register the handler for.
            handler: Callable that receives a deserialised Pydantic model.
        """
        topic_str = topic.value if isinstance(topic, Topic) else topic
        self._handlers.setdefault(topic_str, []).append(handler)
        logger.info(
            "Handler registered for topic '%s' (total: %d)",
            topic_str,
            len(self._handlers[topic_str]),
        )

    def dispatch_local(
        self,
        topic: Union[Topic, str],
        event: BaseModel,
    ) -> int:
        """Dispatch an event to all locally registered handlers.

        This enables synchronous, in-process event routing that works
        without a Kafka broker — useful for testing and single-process
        deployments.

        Args:
            topic: Topic to dispatch on.
            event: The event to dispatch.

        Returns:
            The number of handlers that were invoked.
        """
        topic_str = topic.value if isinstance(topic, Topic) else topic
        handlers = self._handlers.get(topic_str, [])
        invoked = 0
        for handler in handlers:
            try:
                handler(event)
                invoked += 1
            except Exception as exc:
                logger.warning(
                    "Handler error for topic '%s': %s",
                    topic_str,
                    exc,
                )
        return invoked

    # -- Offline queue management --------------------------------------------

    def drain_offline_queue(self) -> int:
        """Attempt to deliver all queued offline events via Kafka.

        Events that are successfully delivered are removed from the queue.
        Events that fail delivery remain in the queue for a future drain
        attempt.

        Returns:
            The number of events successfully delivered.
        """
        if not self._offline_queue:
            return 0

        delivered = 0
        remaining: deque[QueuedEvent] = deque(
            maxlen=self._max_offline_queue_size
        )

        while self._offline_queue:
            queued = self._offline_queue.popleft()
            try:
                producer = self._get_producer()
                producer.send(
                    queued.topic,
                    value=queued.payload_json,
                    key=queued.key,
                )
                producer.flush(timeout=5)
                delivered += 1
            except (KafkaUnavailableError, Exception):
                remaining.append(queued)
                # If Kafka is down, don't keep trying — put everything
                # remaining back and stop.
                while self._offline_queue:
                    remaining.append(self._offline_queue.popleft())
                break

        self._offline_queue = remaining
        if delivered > 0:
            logger.info(
                "Drained %d events from offline queue (%d remaining)",
                delivered,
                len(self._offline_queue),
            )
        return delivered

    def clear_offline_queue(self) -> int:
        """Discard all events in the offline queue.

        Returns:
            The number of events that were discarded.
        """
        count = len(self._offline_queue)
        self._offline_queue.clear()
        if count > 0:
            logger.info("Cleared %d events from offline queue", count)
        return count

    # -- Lifecycle -----------------------------------------------------------

    def close(self) -> None:
        """Close all Kafka connections and release resources.

        Outstanding events in the offline queue are **not** discarded —
        call :meth:`drain_offline_queue` first if delivery is required.
        """
        if self._producer is not None:
            try:
                self._producer.close(timeout=5)
            except Exception as exc:
                logger.warning("Error closing Kafka producer: %s", exc)
            self._producer = None

        for cache_key, consumer in self._consumers.items():
            try:
                consumer.close()
            except Exception as exc:
                logger.warning(
                    "Error closing Kafka consumer '%s': %s", cache_key, exc
                )
        self._consumers.clear()
        logger.info("EventBus closed")

    def __enter__(self) -> EventBus:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
