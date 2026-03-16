"""Shared test fixtures for the Federated Data Space test suite.

Provides reusable pytest fixtures and factory functions for integration and
unit tests across all services (catalog, DSO, aggregator, prosumer).

Fixtures include:
  - Mock Keycloak token generation and authentication bypass
  - Mock Kafka producer/consumer via in-memory EventBus
  - Async HTTP test clients for each FastAPI service
  - Sample data factories for all semantic models
  - Test database (audit log) setup/teardown

Usage::

    def test_catalog_health(catalog_client):
        response = await catalog_client.get("/health")
        assert response.status_code == 200

    def test_with_factories(feeder_constraint_factory):
        constraint = feeder_constraint_factory(congestion_level=0.8)
        assert constraint.congestion_level == 0.8
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from unittest.mock import MagicMock, patch

import httpx
import pytest
from fastapi import FastAPI

from src.catalog.routes import create_router as create_catalog_router
from src.connector.audit import AuditLogger
from src.connector.auth import AuthenticatedUser, KeycloakAuthBackend
from src.connector.events import EventBus, Topic
from src.connector.middleware import ConnectorMiddleware
from src.connector.models import (
    AuditAction,
    AuditEntry,
    AuditOutcome,
    ContractOffer,
    ContractStatus,
    DataAsset,
    DataUsageContract,
    Participant,
    PolicyEffect,
    PolicyRule,
)
from src.connector.policy import PolicyEngine
from src.participants.aggregator.routes import (
    create_router as create_aggregator_router,
)
from src.participants.dso.routes import create_router as create_dso_router
from src.participants.prosumer.routes import (
    create_router as create_prosumer_router,
)
from src.semantic.cim import (
    CongestionSignal,
    Feeder,
    FeederConstraint,
    GridNode,
    HostingCapacity,
    NodeType,
    SensitivityTier,
    Switch,
    SwitchState,
)
from src.semantic.consumer import (
    AnonymizedLoadSeries,
    ConsentRecord,
    ConsentStatus,
    DemandProfile,
    DisclosureLevel,
    MeterReading,
)
from src.semantic.iec61850 import (
    AvailabilityWindow,
    ConfidenceLevel,
    DERType,
    DERUnit,
    DeviceClassMix,
    FlexibilityDirection,
    FlexibilityEnvelope,
    PQRange,
    ResponseConfidence,
    StateOfCharge,
)
from src.semantic.openadr import (
    Baseline,
    DispatchActual,
    DispatchCommand,
    DREvent,
    DRSignal,
    EventStatus,
    SignalType,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utc_now() -> datetime:
    """Return the current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)


def _validity_window(
    hours: int = 24,
) -> tuple[datetime, datetime]:
    """Return a (valid_from, valid_until) tuple spanning *hours* from now."""
    now = _utc_now()
    return now - timedelta(hours=1), now + timedelta(hours=hours)


def _uid(prefix: str = "") -> str:
    """Generate a short unique identifier with an optional prefix."""
    short = uuid.uuid4().hex[:8]
    return f"{prefix}{short}" if prefix else short


# ---------------------------------------------------------------------------
# Mock Keycloak token generation
# ---------------------------------------------------------------------------


class MockKeycloakAuthBackend(KeycloakAuthBackend):
    """Auth backend that bypasses real Keycloak for testing.

    Returns a pre-configured :class:`AuthenticatedUser` without making any
    HTTP requests or validating JWTs.  The user identity can be customised
    per-test by setting :attr:`mock_user`.
    """

    def __init__(self, mock_user: AuthenticatedUser | None = None) -> None:
        super().__init__(
            server_url="http://localhost:8080",
            realm="dataspace-test",
        )
        self.mock_user: AuthenticatedUser = mock_user or _make_authenticated_user()

    def authenticate_token(self, token: str) -> AuthenticatedUser:
        """Return the configured mock user without any token validation."""
        return self.mock_user


def _make_authenticated_user(
    *,
    subject: str = "test-subject-001",
    participant_id: str = "test-participant-001",
    participant_type: str = "dso",
    username: str = "test-user",
    email: str = "test@dataspace.local",
    roles: list[str] | None = None,
    organization: str = "TestOrg",
    certificate_dn: str | None = "CN=test-node,O=DataSpace,C=NL",
) -> AuthenticatedUser:
    """Create an AuthenticatedUser with sensible test defaults."""
    return AuthenticatedUser(
        subject=subject,
        participant_id=participant_id,
        participant_type=participant_type,
        username=username,
        email=email,
        roles=roles or ["dso_operator"],
        organization=organization,
        certificate_dn=certificate_dn,
        token_claims={
            "sub": subject,
            "participant_id": participant_id,
            "participant_type": participant_type,
            "preferred_username": username,
            "email": email,
            "roles": roles or ["dso_operator"],
        },
    )


def make_dso_user() -> AuthenticatedUser:
    """Create an authenticated DSO operator user for testing."""
    return _make_authenticated_user(
        subject="dso-subject-001",
        participant_id="dso-001",
        participant_type="dso",
        username="dso-operator",
        roles=["dso_operator"],
        organization="GridCo",
        certificate_dn="CN=dso-node,O=GridCo,C=NL",
    )


def make_aggregator_user() -> AuthenticatedUser:
    """Create an authenticated Aggregator user for testing."""
    return _make_authenticated_user(
        subject="agg-subject-001",
        participant_id="aggregator-001",
        participant_type="aggregator",
        username="aggregator-operator",
        roles=["aggregator"],
        organization="FlexEnergy",
        certificate_dn="CN=aggregator-node,O=FlexEnergy,C=NL",
    )


def make_prosumer_user() -> AuthenticatedUser:
    """Create an authenticated Prosumer user for testing."""
    return _make_authenticated_user(
        subject="pros-subject-001",
        participant_id="prosumer-001",
        participant_type="prosumer",
        username="campus-admin",
        roles=["prosumer"],
        organization="TechCampus",
        certificate_dn="CN=prosumer-node,O=TechCampus,C=NL",
    )


def generate_test_token(participant_type: str = "dso") -> str:
    """Generate a fake Bearer token string for testing.

    The token is not a valid JWT; it is a placeholder that the
    :class:`MockKeycloakAuthBackend` will accept without validation.
    """
    return f"test-bearer-token-{participant_type}-{_uid()}"


# ---------------------------------------------------------------------------
# Mock Keycloak fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_auth_backend() -> MockKeycloakAuthBackend:
    """A mock Keycloak auth backend that returns a default DSO user."""
    return MockKeycloakAuthBackend(mock_user=make_dso_user())


@pytest.fixture()
def dso_auth_backend() -> MockKeycloakAuthBackend:
    """A mock auth backend configured for a DSO operator."""
    return MockKeycloakAuthBackend(mock_user=make_dso_user())


@pytest.fixture()
def aggregator_auth_backend() -> MockKeycloakAuthBackend:
    """A mock auth backend configured for an Aggregator."""
    return MockKeycloakAuthBackend(mock_user=make_aggregator_user())


@pytest.fixture()
def prosumer_auth_backend() -> MockKeycloakAuthBackend:
    """A mock auth backend configured for a Prosumer."""
    return MockKeycloakAuthBackend(mock_user=make_prosumer_user())


@pytest.fixture()
def dso_token() -> str:
    """A fake Bearer token for DSO requests."""
    return generate_test_token("dso")


@pytest.fixture()
def aggregator_token() -> str:
    """A fake Bearer token for Aggregator requests."""
    return generate_test_token("aggregator")


@pytest.fixture()
def prosumer_token() -> str:
    """A fake Bearer token for Prosumer requests."""
    return generate_test_token("prosumer")


# ---------------------------------------------------------------------------
# Mock Kafka producer / consumer (in-memory EventBus)
# ---------------------------------------------------------------------------


class MockEventBus(EventBus):
    """EventBus that operates entirely in-memory without Kafka.

    All events produced are stored in :attr:`produced_events` for
    assertion.  Handlers registered via :meth:`register_handler` are
    invoked synchronously via :meth:`dispatch_local`.
    """

    def __init__(self) -> None:
        super().__init__(bootstrap_servers="mock:9092")
        self.produced_events: list[tuple[str, Any]] = []

    def produce(
        self,
        topic: Topic | str,
        event: Any,
        *,
        key: str | None = None,
    ) -> bool:
        """Store the event in memory and dispatch to local handlers."""
        topic_str = topic.value if isinstance(topic, Topic) else topic
        self.produced_events.append((topic_str, event))
        self.dispatch_local(topic, event)
        return True

    def consume(self, *args: Any, **kwargs: Any) -> int:
        """No-op consume — events are dispatched synchronously via produce."""
        return 0

    def close(self) -> None:
        """No-op close — no Kafka connections to tear down."""

    def reset(self) -> None:
        """Clear all produced events and registered handlers."""
        self.produced_events.clear()
        self._handlers.clear()


@pytest.fixture()
def mock_event_bus() -> MockEventBus:
    """An in-memory EventBus that captures produced events."""
    return MockEventBus()


# ---------------------------------------------------------------------------
# Test database (audit logger) setup/teardown
# ---------------------------------------------------------------------------


@pytest.fixture()
def test_audit_logger(tmp_path: Path) -> AuditLogger:
    """An AuditLogger backed by a temporary file, cleaned up after the test."""
    return AuditLogger(log_path=str(tmp_path / "test-audit.jsonl"))


@pytest.fixture()
def test_audit_path(tmp_path: Path) -> Path:
    """A temporary directory path for audit log files."""
    audit_dir = tmp_path / "audit"
    audit_dir.mkdir(parents=True, exist_ok=True)
    return audit_dir


# ---------------------------------------------------------------------------
# FastAPI test application factories
# ---------------------------------------------------------------------------


def _create_test_app(
    *,
    title: str,
    auth_backend: MockKeycloakAuthBackend,
    audit_logger: AuditLogger,
    participant_id: str,
    router: Any,
) -> FastAPI:
    """Create a minimal FastAPI app with mock middleware for testing."""
    app = FastAPI(title=title, version="0.1.0-test")
    app.add_middleware(
        ConnectorMiddleware,
        auth_backend=auth_backend,
        audit_logger=audit_logger,
        participant_id=participant_id,
    )
    app.include_router(router)
    return app


def create_test_catalog_app(
    *,
    auth_backend: MockKeycloakAuthBackend | None = None,
    audit_logger: AuditLogger | None = None,
    audit_path: str = "/tmp/test-catalog-audit.jsonl",
) -> FastAPI:
    """Create a Catalog FastAPI app configured for testing."""
    backend = auth_backend or MockKeycloakAuthBackend(mock_user=make_dso_user())
    logger = audit_logger or AuditLogger(log_path=audit_path)
    router = create_catalog_router()
    return _create_test_app(
        title="Test Catalog",
        auth_backend=backend,
        audit_logger=logger,
        participant_id="catalog-001",
        router=router,
    )


def create_test_dso_app(
    *,
    auth_backend: MockKeycloakAuthBackend | None = None,
    audit_logger: AuditLogger | None = None,
    event_bus: EventBus | None = None,
    audit_path: str = "/tmp/test-dso-audit.jsonl",
) -> FastAPI:
    """Create a DSO FastAPI app configured for testing."""
    backend = auth_backend or MockKeycloakAuthBackend(mock_user=make_dso_user())
    logger = audit_logger or AuditLogger(log_path=audit_path)
    bus = event_bus or MockEventBus()
    router = create_dso_router(audit_logger=logger, event_bus=bus)
    return _create_test_app(
        title="Test DSO",
        auth_backend=backend,
        audit_logger=logger,
        participant_id="dso-001",
        router=router,
    )


def create_test_aggregator_app(
    *,
    auth_backend: MockKeycloakAuthBackend | None = None,
    audit_logger: AuditLogger | None = None,
    event_bus: EventBus | None = None,
    audit_path: str = "/tmp/test-aggregator-audit.jsonl",
) -> FastAPI:
    """Create an Aggregator FastAPI app configured for testing."""
    backend = auth_backend or MockKeycloakAuthBackend(
        mock_user=make_aggregator_user()
    )
    logger = audit_logger or AuditLogger(log_path=audit_path)
    bus = event_bus or MockEventBus()
    router = create_aggregator_router(audit_logger=logger, event_bus=bus)
    return _create_test_app(
        title="Test Aggregator",
        auth_backend=backend,
        audit_logger=logger,
        participant_id="aggregator-001",
        router=router,
    )


def create_test_prosumer_app(
    *,
    auth_backend: MockKeycloakAuthBackend | None = None,
    audit_logger: AuditLogger | None = None,
    audit_path: str = "/tmp/test-prosumer-audit.jsonl",
) -> FastAPI:
    """Create a Prosumer FastAPI app configured for testing."""
    backend = auth_backend or MockKeycloakAuthBackend(
        mock_user=make_prosumer_user()
    )
    logger = audit_logger or AuditLogger(log_path=audit_path)
    router = create_prosumer_router(audit_logger=logger)
    return _create_test_app(
        title="Test Prosumer",
        auth_backend=backend,
        audit_logger=logger,
        participant_id="prosumer-001",
        router=router,
    )


# ---------------------------------------------------------------------------
# Async HTTP test client fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
async def catalog_client(
    tmp_path: Path, dso_auth_backend: MockKeycloakAuthBackend
) -> httpx.AsyncClient:
    """Async HTTP client for the Catalog service with mock auth."""
    audit_logger = AuditLogger(log_path=str(tmp_path / "catalog-audit.jsonl"))
    app = create_test_catalog_app(
        auth_backend=dso_auth_backend, audit_logger=audit_logger
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test-catalog"
    ) as client:
        yield client


@pytest.fixture()
async def dso_client(
    tmp_path: Path,
    dso_auth_backend: MockKeycloakAuthBackend,
    mock_event_bus: MockEventBus,
) -> httpx.AsyncClient:
    """Async HTTP client for the DSO service with mock auth and event bus."""
    audit_logger = AuditLogger(log_path=str(tmp_path / "dso-audit.jsonl"))
    app = create_test_dso_app(
        auth_backend=dso_auth_backend,
        audit_logger=audit_logger,
        event_bus=mock_event_bus,
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test-dso"
    ) as client:
        yield client


@pytest.fixture()
async def aggregator_client(
    tmp_path: Path,
    aggregator_auth_backend: MockKeycloakAuthBackend,
    mock_event_bus: MockEventBus,
) -> httpx.AsyncClient:
    """Async HTTP client for the Aggregator service with mock auth and event bus."""
    audit_logger = AuditLogger(
        log_path=str(tmp_path / "aggregator-audit.jsonl")
    )
    app = create_test_aggregator_app(
        auth_backend=aggregator_auth_backend,
        audit_logger=audit_logger,
        event_bus=mock_event_bus,
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test-aggregator",
    ) as client:
        yield client


@pytest.fixture()
async def prosumer_client(
    tmp_path: Path,
    prosumer_auth_backend: MockKeycloakAuthBackend,
) -> httpx.AsyncClient:
    """Async HTTP client for the Prosumer service with mock auth."""
    audit_logger = AuditLogger(
        log_path=str(tmp_path / "prosumer-audit.jsonl")
    )
    app = create_test_prosumer_app(
        auth_backend=prosumer_auth_backend, audit_logger=audit_logger
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test-prosumer"
    ) as client:
        yield client


# ---------------------------------------------------------------------------
# Sample data factories — CIM models
# ---------------------------------------------------------------------------


def make_feeder_constraint(
    *,
    feeder_id: str | None = None,
    max_active_power_kw: float = 500.0,
    min_voltage_pu: float = 0.95,
    max_voltage_pu: float = 1.05,
    congestion_level: float = 0.3,
    sensitivity: SensitivityTier = SensitivityTier.MEDIUM,
) -> FeederConstraint:
    """Create a FeederConstraint with sensible test defaults."""
    vf, vu = _validity_window()
    return FeederConstraint(
        feeder_id=feeder_id or f"feeder-{_uid()}",
        max_active_power_kw=max_active_power_kw,
        min_voltage_pu=min_voltage_pu,
        max_voltage_pu=max_voltage_pu,
        congestion_level=congestion_level,
        valid_from=vf,
        valid_until=vu,
        sensitivity=sensitivity,
    )


def make_congestion_signal(
    *,
    signal_id: str | None = None,
    feeder_id: str | None = None,
    congestion_level: float = 0.6,
    max_available_capacity_kw: float = 200.0,
    direction: str = "both",
) -> CongestionSignal:
    """Create a CongestionSignal with sensible test defaults."""
    vf, vu = _validity_window()
    return CongestionSignal(
        signal_id=signal_id or f"sig-{_uid()}",
        feeder_id=feeder_id or f"feeder-{_uid()}",
        congestion_level=congestion_level,
        max_available_capacity_kw=max_available_capacity_kw,
        direction=direction,
        valid_from=vf,
        valid_until=vu,
    )


def make_hosting_capacity(
    *,
    node_id: str | None = None,
    feeder_id: str | None = None,
    max_generation_kw: float = 1000.0,
    max_load_kw: float = 800.0,
) -> HostingCapacity:
    """Create a HostingCapacity with sensible test defaults."""
    vf, vu = _validity_window()
    return HostingCapacity(
        node_id=node_id or f"node-{_uid()}",
        feeder_id=feeder_id or f"feeder-{_uid()}",
        max_generation_kw=max_generation_kw,
        max_load_kw=max_load_kw,
        current_generation_kw=300.0,
        current_load_kw=400.0,
        voltage_headroom_pu=0.03,
        thermal_headroom_pct=45.0,
        valid_from=vf,
        valid_until=vu,
    )


def make_grid_node(
    *,
    node_id: str | None = None,
    feeder_id: str | None = None,
    node_type: NodeType = NodeType.LOAD_POINT,
    voltage_level_kv: float = 10.0,
) -> GridNode:
    """Create a GridNode with sensible test defaults."""
    vf, vu = _validity_window()
    return GridNode(
        node_id=node_id or f"node-{_uid()}",
        name=f"Test Node {_uid()}",
        node_type=node_type,
        feeder_id=feeder_id or f"feeder-{_uid()}",
        voltage_level_kv=voltage_level_kv,
        latitude=52.37,
        longitude=4.89,
        valid_from=vf,
        valid_until=vu,
    )


def make_feeder(
    *,
    feeder_id: str | None = None,
    substation_id: str | None = None,
    voltage_level_kv: float = 10.0,
    max_rated_power_kw: float = 5000.0,
) -> Feeder:
    """Create a Feeder with sensible test defaults."""
    vf, vu = _validity_window()
    return Feeder(
        feeder_id=feeder_id or f"feeder-{_uid()}",
        name=f"Test Feeder {_uid()}",
        substation_id=substation_id or f"sub-{_uid()}",
        voltage_level_kv=voltage_level_kv,
        max_rated_power_kw=max_rated_power_kw,
        node_ids=[f"node-{_uid()}" for _ in range(3)],
        switch_ids=[f"sw-{_uid()}" for _ in range(2)],
        valid_from=vf,
        valid_until=vu,
    )


def make_switch(
    *,
    switch_id: str | None = None,
    feeder_id: str | None = None,
    state: SwitchState = SwitchState.CLOSED,
    rated_current_a: float = 400.0,
) -> Switch:
    """Create a Switch with sensible test defaults."""
    vf, vu = _validity_window()
    return Switch(
        switch_id=switch_id or f"sw-{_uid()}",
        name=f"Test Switch {_uid()}",
        feeder_id=feeder_id or f"feeder-{_uid()}",
        from_node_id=f"node-{_uid()}",
        to_node_id=f"node-{_uid()}",
        state=state,
        rated_current_a=rated_current_a,
        valid_from=vf,
        valid_until=vu,
    )


# ---------------------------------------------------------------------------
# Sample data factories — IEC 61850 models
# ---------------------------------------------------------------------------


def make_pq_range(
    *,
    p_min_kw: float = -100.0,
    p_max_kw: float = 500.0,
    q_min_kvar: float = -50.0,
    q_max_kvar: float = 50.0,
) -> PQRange:
    """Create a PQRange with sensible test defaults."""
    return PQRange(
        p_min_kw=p_min_kw,
        p_max_kw=p_max_kw,
        q_min_kvar=q_min_kvar,
        q_max_kvar=q_max_kvar,
    )


def make_state_of_charge(
    *,
    aggregate_soc_pct: float = 65.0,
    total_energy_capacity_kwh: float = 2000.0,
    available_energy_kwh: float = 1300.0,
) -> StateOfCharge:
    """Create a StateOfCharge with sensible test defaults."""
    return StateOfCharge(
        aggregate_soc_pct=aggregate_soc_pct,
        total_energy_capacity_kwh=total_energy_capacity_kwh,
        available_energy_kwh=available_energy_kwh,
    )


def make_device_class_mix(
    *,
    der_type: DERType = DERType.BATTERY_STORAGE,
    share_pct: float = 40.0,
    aggregate_capacity_kw: float = 200.0,
) -> DeviceClassMix:
    """Create a DeviceClassMix with sensible test defaults."""
    return DeviceClassMix(
        der_type=der_type,
        share_pct=share_pct,
        aggregate_capacity_kw=aggregate_capacity_kw,
    )


def make_response_confidence(
    *,
    level: ConfidenceLevel = ConfidenceLevel.HIGH,
    probability_pct: float = 95.0,
) -> ResponseConfidence:
    """Create a ResponseConfidence with sensible test defaults."""
    return ResponseConfidence(
        level=level,
        probability_pct=probability_pct,
        historical_delivery_rate_pct=92.0,
    )


def make_availability_window(
    *,
    window_id: str | None = None,
    pq_range: PQRange | None = None,
    hours_from_now: int = 2,
    duration_hours: int = 4,
) -> AvailabilityWindow:
    """Create an AvailabilityWindow with sensible test defaults."""
    now = _utc_now()
    return AvailabilityWindow(
        window_id=window_id or f"win-{_uid()}",
        available_from=now + timedelta(hours=hours_from_now),
        available_until=now + timedelta(hours=hours_from_now + duration_hours),
        pq_range=pq_range or make_pq_range(),
        ramp_up_rate_kw_per_min=50.0,
        ramp_down_rate_kw_per_min=50.0,
        min_duration_minutes=15.0,
        max_duration_minutes=120.0,
    )


def make_der_unit(
    *,
    unit_id: str | None = None,
    aggregator_id: str = "aggregator-001",
    feeder_id: str | None = None,
    total_rated_capacity_kw: float = 500.0,
    current_output_kw: float = 150.0,
) -> DERUnit:
    """Create a DERUnit with sensible test defaults."""
    vf, vu = _validity_window()
    return DERUnit(
        unit_id=unit_id or f"der-{_uid()}",
        name=f"Test DER Unit {_uid()}",
        aggregator_id=aggregator_id,
        feeder_id=feeder_id or f"feeder-{_uid()}",
        device_class_mix=[
            make_device_class_mix(
                der_type=DERType.BATTERY_STORAGE,
                share_pct=60.0,
                aggregate_capacity_kw=300.0,
            ),
            make_device_class_mix(
                der_type=DERType.SOLAR_PV,
                share_pct=40.0,
                aggregate_capacity_kw=200.0,
            ),
        ],
        total_rated_capacity_kw=total_rated_capacity_kw,
        current_output_kw=current_output_kw,
        state_of_charge=make_state_of_charge(),
        valid_from=vf,
        valid_until=vu,
    )


def make_flexibility_envelope(
    *,
    envelope_id: str | None = None,
    unit_id: str | None = None,
    aggregator_id: str = "aggregator-001",
    feeder_id: str | None = None,
    direction: FlexibilityDirection = FlexibilityDirection.BOTH,
) -> FlexibilityEnvelope:
    """Create a FlexibilityEnvelope with sensible test defaults."""
    vf, vu = _validity_window()
    return FlexibilityEnvelope(
        envelope_id=envelope_id or f"env-{_uid()}",
        unit_id=unit_id or f"der-{_uid()}",
        aggregator_id=aggregator_id,
        feeder_id=feeder_id or f"feeder-{_uid()}",
        direction=direction,
        pq_range=make_pq_range(),
        availability_windows=[make_availability_window()],
        state_of_charge=make_state_of_charge(),
        response_confidence=make_response_confidence(),
        device_class_mix=[
            make_device_class_mix(der_type=DERType.BATTERY_STORAGE),
        ],
        price_eur_per_kwh=0.12,
        valid_from=vf,
        valid_until=vu,
    )


# ---------------------------------------------------------------------------
# Sample data factories — OpenADR models
# ---------------------------------------------------------------------------


def make_dr_signal(
    *,
    signal_id: str | None = None,
    signal_type: SignalType = SignalType.LOAD_DISPATCH,
    current_value: float = 100.0,
    duration_minutes: float = 60.0,
) -> DRSignal:
    """Create a DRSignal with sensible test defaults."""
    vf, vu = _validity_window(hours=2)
    return DRSignal(
        signal_id=signal_id or f"sig-{_uid()}",
        signal_type=signal_type,
        signal_name="LOAD_DISPATCH_SIGNAL",
        current_value=current_value,
        target_kw=current_value,
        duration_minutes=duration_minutes,
        valid_from=vf,
        valid_until=vu,
    )


def make_dr_event(
    *,
    event_id: str | None = None,
    issuer_id: str = "dso-001",
    status: EventStatus = EventStatus.PENDING,
    priority: int = 1,
    is_emergency: bool = False,
    feeder_id: str | None = None,
) -> DREvent:
    """Create a DREvent with sensible test defaults."""
    now = _utc_now()
    vf, vu = _validity_window(hours=4)
    return DREvent(
        event_id=event_id or f"ev-{_uid()}",
        program_id="dr-program-001",
        issuer_id=issuer_id,
        target_participant_ids=["aggregator-001"],
        status=status,
        priority=priority,
        signals=[make_dr_signal()],
        feeder_id=feeder_id or f"feeder-{_uid()}",
        event_start=now + timedelta(hours=1),
        event_end=now + timedelta(hours=3),
        ramp_up_minutes=15.0,
        recovery_minutes=10.0,
        is_emergency=is_emergency,
        valid_from=vf,
        valid_until=vu,
    )


def make_baseline(
    *,
    baseline_id: str | None = None,
    event_id: str | None = None,
    participant_id: str = "aggregator-001",
    feeder_id: str | None = None,
) -> Baseline:
    """Create a Baseline with sensible test defaults."""
    now = _utc_now()
    vf, vu = _validity_window()
    return Baseline(
        baseline_id=baseline_id or f"bl-{_uid()}",
        event_id=event_id or f"ev-{_uid()}",
        participant_id=participant_id,
        feeder_id=feeder_id,
        methodology="avg_10_of_10",
        interval_minutes=15.0,
        values_kw=[100.0, 105.0, 98.0, 102.0],
        baseline_start=now,
        baseline_end=now + timedelta(hours=1),
        valid_from=vf,
        valid_until=vu,
    )


def make_dispatch_command(
    *,
    command_id: str | None = None,
    event_id: str | None = None,
    issuer_id: str = "dso-001",
    target_participant_id: str = "aggregator-001",
    contract_id: str = "contract-001",
    feeder_id: str | None = None,
    target_power_kw: float = 200.0,
    duration_minutes: float = 30.0,
    is_emergency: bool = False,
) -> DispatchCommand:
    """Create a DispatchCommand with sensible test defaults."""
    now = _utc_now()
    return DispatchCommand(
        command_id=command_id or f"cmd-{_uid()}",
        event_id=event_id or f"ev-{_uid()}",
        issuer_id=issuer_id,
        target_participant_id=target_participant_id,
        contract_id=contract_id,
        feeder_id=feeder_id or f"feeder-{_uid()}",
        target_power_kw=target_power_kw,
        activation_time=now + timedelta(minutes=5),
        duration_minutes=duration_minutes,
        is_emergency=is_emergency,
    )


def make_dispatch_actual(
    *,
    actual_id: str | None = None,
    command_id: str | None = None,
    event_id: str | None = None,
    participant_id: str = "aggregator-001",
    feeder_id: str | None = None,
    commanded_kw: float = 200.0,
    delivered_kw: float = 185.0,
    delivery_accuracy_pct: float = 92.5,
) -> DispatchActual:
    """Create a DispatchActual with sensible test defaults."""
    now = _utc_now()
    return DispatchActual(
        actual_id=actual_id or f"act-{_uid()}",
        command_id=command_id or f"cmd-{_uid()}",
        event_id=event_id or f"ev-{_uid()}",
        participant_id=participant_id,
        feeder_id=feeder_id or f"feeder-{_uid()}",
        commanded_kw=commanded_kw,
        delivered_kw=delivered_kw,
        delivery_start=now - timedelta(minutes=30),
        delivery_end=now,
        delivery_accuracy_pct=delivery_accuracy_pct,
        interval_values_kw=[180.0, 185.0, 190.0, 185.0],
    )


# ---------------------------------------------------------------------------
# Sample data factories — Consumer models
# ---------------------------------------------------------------------------


def make_meter_reading(
    *,
    reading_id: str | None = None,
    meter_id: str | None = None,
    prosumer_id: str = "prosumer-001",
    active_power_kw: float = 3.5,
) -> MeterReading:
    """Create a MeterReading with sensible test defaults."""
    return MeterReading(
        reading_id=reading_id or f"mr-{_uid()}",
        meter_id=meter_id or f"meter-{_uid()}",
        prosumer_id=prosumer_id,
        active_power_kw=active_power_kw,
        reactive_power_kvar=0.5,
        voltage_v=230.0,
        cumulative_energy_kwh=12500.0,
        reading_timestamp=_utc_now(),
    )


def make_demand_profile(
    *,
    profile_id: str | None = None,
    prosumer_id: str = "prosumer-001",
    profile_type: str = "historical",
    disclosure_level: DisclosureLevel = DisclosureLevel.RAW,
) -> DemandProfile:
    """Create a DemandProfile with sensible test defaults."""
    now = _utc_now()
    vf, vu = _validity_window()
    values = [2.5, 3.0, 3.5, 4.0, 3.8, 3.2, 2.8, 2.5]
    return DemandProfile(
        profile_id=profile_id or f"dp-{_uid()}",
        prosumer_id=prosumer_id,
        profile_type=profile_type,
        values_kw=values,
        peak_demand_kw=max(values),
        total_energy_kwh=sum(values) * 0.25,
        profile_start=now - timedelta(hours=2),
        profile_end=now,
        disclosure_level=disclosure_level,
        valid_from=vf,
        valid_until=vu,
    )


def make_anonymized_load_series(
    *,
    series_id: str | None = None,
    source_count: int = 50,
    feeder_id: str | None = None,
    k_anonymity_level: int = 5,
) -> AnonymizedLoadSeries:
    """Create an AnonymizedLoadSeries with sensible test defaults."""
    now = _utc_now()
    vf, vu = _validity_window()
    values = [150.0, 155.0, 160.0, 158.0, 152.0, 148.0]
    return AnonymizedLoadSeries(
        series_id=series_id or f"als-{_uid()}",
        source_count=source_count,
        feeder_id=feeder_id,
        values_kw=values,
        mean_kw=153.8,
        std_dev_kw=4.2,
        peak_kw=max(values),
        min_kw=min(values),
        k_anonymity_level=k_anonymity_level,
        series_start=now - timedelta(hours=1),
        series_end=now,
        valid_from=vf,
        valid_until=vu,
    )


def make_consent_record(
    *,
    consent_id: str | None = None,
    prosumer_id: str = "prosumer-001",
    requester_id: str = "aggregator-001",
    purpose: str = "dr_dispatch",
    status: ConsentStatus = ConsentStatus.ACTIVE,
) -> ConsentRecord:
    """Create a ConsentRecord with sensible test defaults."""
    vf, vu = _validity_window(hours=720)
    return ConsentRecord(
        consent_id=consent_id or f"consent-{_uid()}",
        prosumer_id=prosumer_id,
        requester_id=requester_id,
        purpose=purpose,
        allowed_data_types=["meter_reading", "demand_profile"],
        disclosure_level=DisclosureLevel.CONTROLLABILITY_ONLY,
        status=status,
        valid_from=vf,
        valid_until=vu,
    )


# ---------------------------------------------------------------------------
# Sample data factories — Connector core models
# ---------------------------------------------------------------------------


def make_participant(
    *,
    pid: str = "test-participant-001",
    name: str | None = None,
    organization: str = "TestOrg",
    roles: list[str] | None = None,
    certificate_dn: str | None = None,
) -> Participant:
    """Create a Participant with sensible test defaults."""
    return Participant(
        id=pid,
        name=name or f"Participant {pid}",
        organization=organization,
        roles=roles or ["aggregator"],
        certificate_dn=certificate_dn,
    )


def make_data_asset(
    *,
    asset_id: str | None = None,
    provider_id: str = "dso-001",
    name: str | None = None,
    data_type: str = "feeder_constraint",
    sensitivity: SensitivityTier = SensitivityTier.MEDIUM,
    endpoint: str | None = None,
) -> DataAsset:
    """Create a DataAsset with sensible test defaults."""
    aid = asset_id or f"asset-{_uid()}"
    return DataAsset(
        id=aid,
        provider_id=provider_id,
        name=name or f"Asset {aid}",
        description=f"Test data asset {aid}",
        data_type=data_type,
        sensitivity=sensitivity,
        endpoint=endpoint or f"https://dso.local/api/{aid}",
        update_frequency="5m",
    )


def make_contract_offer(
    *,
    offer_id: str | None = None,
    provider_id: str = "dso-001",
    consumer_id: str = "aggregator-001",
    asset_id: str | None = None,
    purpose: str = "congestion_management",
    retention_days: int = 30,
) -> ContractOffer:
    """Create a ContractOffer with sensible test defaults."""
    vf, vu = _validity_window(hours=2160)
    return ContractOffer(
        offer_id=offer_id or f"offer-{_uid()}",
        provider_id=provider_id,
        consumer_id=consumer_id,
        asset_id=asset_id or f"asset-{_uid()}",
        purpose=purpose,
        allowed_operations=["read"],
        retention_days=retention_days,
        valid_from=vf,
        valid_until=vu,
    )


def make_data_usage_contract(
    *,
    contract_id: str | None = None,
    provider_id: str = "dso-001",
    consumer_id: str = "aggregator-001",
    asset_id: str | None = None,
    purpose: str = "congestion_management",
    status: ContractStatus = ContractStatus.ACTIVE,
    allowed_operations: list[str] | None = None,
    retention_days: int = 30,
    redistribution_allowed: bool = False,
    anonymization_required: bool = False,
    emergency_override: bool = False,
    valid_from: datetime | None = None,
    valid_until: datetime | None = None,
) -> DataUsageContract:
    """Create a DataUsageContract with sensible test defaults."""
    now = _utc_now()
    return DataUsageContract(
        contract_id=contract_id or f"contract-{_uid()}",
        provider_id=provider_id,
        consumer_id=consumer_id,
        asset_id=asset_id or f"asset-{_uid()}",
        purpose=purpose,
        allowed_operations=allowed_operations or ["read"],
        retention_days=retention_days,
        redistribution_allowed=redistribution_allowed,
        anonymization_required=anonymization_required,
        emergency_override=emergency_override,
        status=status,
        valid_from=valid_from or now - timedelta(days=1),
        valid_until=valid_until or now + timedelta(days=90),
    )


def make_policy_rule(
    *,
    rule_id: str | None = None,
    asset_id: str | None = None,
    sensitivity: SensitivityTier | None = None,
    allowed_roles: list[str] | None = None,
    allowed_operations: list[str] | None = None,
    allowed_purposes: list[str] | None = None,
    effect: PolicyEffect = PolicyEffect.ALLOW,
    priority: int = 50,
) -> PolicyRule:
    """Create a PolicyRule with sensible test defaults."""
    return PolicyRule(
        rule_id=rule_id or f"rule-{_uid()}",
        asset_id=asset_id,
        sensitivity=sensitivity,
        allowed_roles=allowed_roles or [],
        allowed_operations=allowed_operations or [],
        allowed_purposes=allowed_purposes or [],
        effect=effect,
        priority=priority,
    )


def make_audit_entry(
    *,
    requester_id: str = "aggregator-001",
    provider_id: str = "dso-001",
    asset_id: str | None = None,
    purpose_tag: str = "congestion_management",
    contract_id: str | None = None,
    action: AuditAction = AuditAction.READ,
    outcome: AuditOutcome = AuditOutcome.SUCCESS,
) -> AuditEntry:
    """Create an AuditEntry with sensible test defaults."""
    from src.connector.audit import compute_hash

    return AuditEntry(
        requester_id=requester_id,
        provider_id=provider_id,
        asset_id=asset_id or f"asset-{_uid()}",
        purpose_tag=purpose_tag,
        request_hash=compute_hash(b"test-request"),
        response_hash=compute_hash(b"test-response"),
        contract_id=contract_id or f"contract-{_uid()}",
        action=action,
        outcome=outcome,
    )


# ---------------------------------------------------------------------------
# Policy engine fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def policy_engine() -> PolicyEngine:
    """A PolicyEngine pre-loaded with DSO, Aggregator, and Prosumer participants."""
    engine = PolicyEngine()
    engine.register_participant(
        make_participant(
            pid="dso-001",
            roles=["dso_operator"],
            organization="GridCo",
        )
    )
    engine.register_participant(
        make_participant(
            pid="aggregator-001",
            roles=["aggregator"],
            organization="FlexEnergy",
        )
    )
    engine.register_participant(
        make_participant(
            pid="prosumer-001",
            roles=["prosumer"],
            organization="TechCampus",
        )
    )
    return engine


# ---------------------------------------------------------------------------
# Pytest-style factory fixtures (function-scoped callables)
# ---------------------------------------------------------------------------


@pytest.fixture()
def feeder_constraint_factory():
    """Callable factory fixture for creating FeederConstraint instances."""
    return make_feeder_constraint


@pytest.fixture()
def congestion_signal_factory():
    """Callable factory fixture for creating CongestionSignal instances."""
    return make_congestion_signal


@pytest.fixture()
def hosting_capacity_factory():
    """Callable factory fixture for creating HostingCapacity instances."""
    return make_hosting_capacity


@pytest.fixture()
def grid_node_factory():
    """Callable factory fixture for creating GridNode instances."""
    return make_grid_node


@pytest.fixture()
def feeder_factory():
    """Callable factory fixture for creating Feeder instances."""
    return make_feeder


@pytest.fixture()
def switch_factory():
    """Callable factory fixture for creating Switch instances."""
    return make_switch


@pytest.fixture()
def flexibility_envelope_factory():
    """Callable factory fixture for creating FlexibilityEnvelope instances."""
    return make_flexibility_envelope


@pytest.fixture()
def der_unit_factory():
    """Callable factory fixture for creating DERUnit instances."""
    return make_der_unit


@pytest.fixture()
def dr_event_factory():
    """Callable factory fixture for creating DREvent instances."""
    return make_dr_event


@pytest.fixture()
def dispatch_command_factory():
    """Callable factory fixture for creating DispatchCommand instances."""
    return make_dispatch_command


@pytest.fixture()
def dispatch_actual_factory():
    """Callable factory fixture for creating DispatchActual instances."""
    return make_dispatch_actual


@pytest.fixture()
def meter_reading_factory():
    """Callable factory fixture for creating MeterReading instances."""
    return make_meter_reading


@pytest.fixture()
def demand_profile_factory():
    """Callable factory fixture for creating DemandProfile instances."""
    return make_demand_profile


@pytest.fixture()
def consent_record_factory():
    """Callable factory fixture for creating ConsentRecord instances."""
    return make_consent_record


@pytest.fixture()
def participant_factory():
    """Callable factory fixture for creating Participant instances."""
    return make_participant


@pytest.fixture()
def data_asset_factory():
    """Callable factory fixture for creating DataAsset instances."""
    return make_data_asset


@pytest.fixture()
def contract_factory():
    """Callable factory fixture for creating DataUsageContract instances."""
    return make_data_usage_contract


@pytest.fixture()
def contract_offer_factory():
    """Callable factory fixture for creating ContractOffer instances."""
    return make_contract_offer
