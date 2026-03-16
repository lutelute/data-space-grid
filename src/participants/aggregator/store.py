"""SQLite-backed local data store for the Aggregator participant node.

Provides persistent storage for DER fleet aggregate data that the Aggregator
publishes to the federated data space: flexibility offers, availability
windows, baselines, and dispatch responses.  The store is pre-seeded with
sample data for development and integration testing.

Key design decisions:
  - Four tables: ``flexibility_offers``, ``availability_windows``,
    ``baselines``, and ``dispatch_responses``, each mapping to the
    corresponding semantic model from :mod:`src.semantic.iec61850` and
    :mod:`src.semantic.openadr`.
  - Uses a separate SQLAlchemy ``Base`` from other participant stores so
    all can coexist in the same process without table name collisions.
  - JSON serialization for list/dict fields that SQLite does not natively
    support (e.g., availability windows, device class mix, interval values).
  - All timestamps are stored as timezone-aware UTC datetimes.
  - The ``seed()`` method populates the store with realistic sample data
    for a mixed DER fleet (battery, solar PV, EV chargers) across two
    feeders (F-101, F-102) covering different flexibility profiles.
  - The store exposes synchronous methods; async wrappers can be added at
    the route layer when needed.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import Column, DateTime, Float, String, Text, create_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from src.semantic.cim import SensitivityTier
from src.semantic.iec61850 import (
    AvailabilityWindow,
    ConfidenceLevel,
    DERType,
    DeviceClassMix,
    FlexibilityDirection,
    FlexibilityEnvelope,
    PQRange,
    ResponseConfidence,
    StateOfCharge,
)
from src.semantic.openadr import Baseline, DispatchActual

AggregatorBase = declarative_base()


def _utc_now() -> datetime:
    """Return the current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# SQLAlchemy ORM models
# ---------------------------------------------------------------------------


class FlexibilityOfferRow(AggregatorBase):  # type: ignore[misc]
    """SQLAlchemy model for the ``flexibility_offers`` table.

    Stores aggregate flexibility envelopes published by the Aggregator.
    Complex nested fields (availability windows, device class mix, state
    of charge, response confidence) are serialized as JSON text.
    """

    __tablename__ = "flexibility_offers"

    id = Column(String, primary_key=True)
    envelope_id = Column(String, nullable=False, index=True)
    unit_id = Column(String, nullable=False, index=True)
    aggregator_id = Column(String, nullable=False, index=True)
    feeder_id = Column(String, nullable=False, index=True)
    direction = Column(String, nullable=False)
    p_min_kw = Column(Float, nullable=False)
    p_max_kw = Column(Float, nullable=False)
    q_min_kvar = Column(Float, nullable=False)
    q_max_kvar = Column(Float, nullable=False)
    availability_windows_json = Column(Text, nullable=False, default="[]")
    state_of_charge_json = Column(Text, nullable=True)
    response_confidence_json = Column(Text, nullable=False)
    device_class_mix_json = Column(Text, nullable=False, default="[]")
    price_eur_per_kwh = Column(Float, nullable=True)
    valid_from = Column(DateTime(timezone=True), nullable=False)
    valid_until = Column(DateTime(timezone=True), nullable=False)
    sensitivity = Column(String, nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)


class AvailabilityWindowRow(AggregatorBase):  # type: ignore[misc]
    """SQLAlchemy model for the ``availability_windows`` table.

    Stores time windows when DER flexibility is available, linked to a
    specific flexibility offer by ``envelope_id``.
    """

    __tablename__ = "availability_windows"

    id = Column(String, primary_key=True)
    window_id = Column(String, nullable=False, index=True)
    envelope_id = Column(String, nullable=False, index=True)
    available_from = Column(DateTime(timezone=True), nullable=False)
    available_until = Column(DateTime(timezone=True), nullable=False)
    p_min_kw = Column(Float, nullable=False)
    p_max_kw = Column(Float, nullable=False)
    q_min_kvar = Column(Float, nullable=False)
    q_max_kvar = Column(Float, nullable=False)
    ramp_up_rate_kw_per_min = Column(Float, nullable=False)
    ramp_down_rate_kw_per_min = Column(Float, nullable=False)
    min_duration_minutes = Column(Float, nullable=False)
    max_duration_minutes = Column(Float, nullable=False)


class BaselineRow(AggregatorBase):  # type: ignore[misc]
    """SQLAlchemy model for the ``baselines`` table.

    Stores baseline consumption or generation profiles used for DR
    settlement verification.
    """

    __tablename__ = "baselines"

    id = Column(String, primary_key=True)
    baseline_id = Column(String, nullable=False, index=True)
    event_id = Column(String, nullable=False, index=True)
    participant_id = Column(String, nullable=False, index=True)
    feeder_id = Column(String, nullable=True)
    methodology = Column(String, nullable=False)
    interval_minutes = Column(Float, nullable=False)
    values_kw_json = Column(Text, nullable=False)
    baseline_start = Column(DateTime(timezone=True), nullable=False)
    baseline_end = Column(DateTime(timezone=True), nullable=False)
    valid_from = Column(DateTime(timezone=True), nullable=False)
    valid_until = Column(DateTime(timezone=True), nullable=False)
    sensitivity = Column(String, nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)


class DispatchResponseRow(AggregatorBase):  # type: ignore[misc]
    """SQLAlchemy model for the ``dispatch_responses`` table.

    Stores actual dispatch response data reported by the Aggregator
    back to the DSO after executing a dispatch command.
    """

    __tablename__ = "dispatch_responses"

    id = Column(String, primary_key=True)
    actual_id = Column(String, nullable=False, index=True)
    command_id = Column(String, nullable=False, index=True)
    event_id = Column(String, nullable=False, index=True)
    participant_id = Column(String, nullable=False, index=True)
    feeder_id = Column(String, nullable=False, index=True)
    commanded_kw = Column(Float, nullable=False)
    delivered_kw = Column(Float, nullable=False)
    delivered_kvar = Column(Float, nullable=True)
    delivery_start = Column(DateTime(timezone=True), nullable=False)
    delivery_end = Column(DateTime(timezone=True), nullable=False)
    delivery_accuracy_pct = Column(Float, nullable=False)
    interval_values_kw_json = Column(Text, nullable=False, default="[]")
    interval_minutes = Column(Float, nullable=False)
    reported_at = Column(DateTime(timezone=True), nullable=False)
    sensitivity = Column(String, nullable=False)


# ---------------------------------------------------------------------------
# AggregatorStore
# ---------------------------------------------------------------------------


class AggregatorStore:
    """Persistent local data store for the Aggregator participant node.

    Manages flexibility offers, availability windows, baselines, and
    dispatch response data.  Backed by SQLAlchemy with SQLite (dev) or
    PostgreSQL (prod).

    Usage::

        store = AggregatorStore("sqlite:///data/aggregator.db")
        store.seed()  # populate with sample data
        offers = store.list_flexibility_offers()
        baseline = store.get_baseline(baseline_id)

    Args:
        database_url: SQLAlchemy database URL.  Defaults to a file-based
            SQLite database for development.
    """

    def __init__(
        self, database_url: str = "sqlite:///data/aggregator.db"
    ) -> None:
        self._engine = create_engine(database_url, echo=False)
        AggregatorBase.metadata.create_all(self._engine)
        self._session_factory = sessionmaker(bind=self._engine)

    def _session(self) -> Session:
        """Create a new database session."""
        return self._session_factory()

    # -- Flexibility offer operations ----------------------------------------

    def add_flexibility_offer(
        self, envelope: FlexibilityEnvelope
    ) -> FlexibilityEnvelope:
        """Add a flexibility offer (envelope) to the store.

        Args:
            envelope: The flexibility envelope data to store.

        Returns:
            The stored flexibility envelope (unchanged).
        """
        row = FlexibilityOfferRow(
            id=str(uuid.uuid4()),
            envelope_id=envelope.envelope_id,
            unit_id=envelope.unit_id,
            aggregator_id=envelope.aggregator_id,
            feeder_id=envelope.feeder_id,
            direction=envelope.direction.value,
            p_min_kw=envelope.pq_range.p_min_kw,
            p_max_kw=envelope.pq_range.p_max_kw,
            q_min_kvar=envelope.pq_range.q_min_kvar,
            q_max_kvar=envelope.pq_range.q_max_kvar,
            availability_windows_json=json.dumps(
                [w.model_dump(mode="json") for w in envelope.availability_windows]
            ),
            state_of_charge_json=(
                json.dumps(envelope.state_of_charge.model_dump(mode="json"))
                if envelope.state_of_charge is not None
                else None
            ),
            response_confidence_json=json.dumps(
                envelope.response_confidence.model_dump(mode="json")
            ),
            device_class_mix_json=json.dumps(
                [d.model_dump(mode="json") for d in envelope.device_class_mix]
            ),
            price_eur_per_kwh=envelope.price_eur_per_kwh,
            valid_from=envelope.valid_from,
            valid_until=envelope.valid_until,
            sensitivity=envelope.sensitivity.value,
            updated_at=envelope.updated_at,
        )
        with self._session() as session:
            session.add(row)
            session.commit()
        return envelope

    def list_flexibility_offers(
        self,
        *,
        feeder_id: Optional[str] = None,
        aggregator_id: Optional[str] = None,
    ) -> list[FlexibilityEnvelope]:
        """List flexibility offers, optionally filtered by feeder or aggregator.

        Args:
            feeder_id: Filter by feeder identifier.  When ``None``, all
                offers are returned.
            aggregator_id: Filter by aggregator identifier.

        Returns:
            List of matching flexibility envelopes.
        """
        with self._session() as session:
            q = session.query(FlexibilityOfferRow)
            if feeder_id is not None:
                q = q.filter(FlexibilityOfferRow.feeder_id == feeder_id)
            if aggregator_id is not None:
                q = q.filter(
                    FlexibilityOfferRow.aggregator_id == aggregator_id
                )
            return [self._row_to_flexibility_offer(row) for row in q.all()]

    def get_flexibility_offer(
        self, envelope_id: str
    ) -> Optional[FlexibilityEnvelope]:
        """Retrieve a specific flexibility offer by envelope ID.

        Args:
            envelope_id: The unique envelope identifier.

        Returns:
            The flexibility envelope, or ``None`` if not found.
        """
        with self._session() as session:
            row = (
                session.query(FlexibilityOfferRow)
                .filter(FlexibilityOfferRow.envelope_id == envelope_id)
                .first()
            )
            if row is None:
                return None
            return self._row_to_flexibility_offer(row)

    # -- Availability window operations --------------------------------------

    def add_availability_window(
        self, window: AvailabilityWindow, envelope_id: str
    ) -> AvailabilityWindow:
        """Add an availability window linked to a flexibility offer.

        Args:
            window: The availability window data to store.
            envelope_id: The parent flexibility envelope identifier.

        Returns:
            The stored availability window (unchanged).
        """
        row = AvailabilityWindowRow(
            id=str(uuid.uuid4()),
            window_id=window.window_id,
            envelope_id=envelope_id,
            available_from=window.available_from,
            available_until=window.available_until,
            p_min_kw=window.pq_range.p_min_kw,
            p_max_kw=window.pq_range.p_max_kw,
            q_min_kvar=window.pq_range.q_min_kvar,
            q_max_kvar=window.pq_range.q_max_kvar,
            ramp_up_rate_kw_per_min=window.ramp_up_rate_kw_per_min,
            ramp_down_rate_kw_per_min=window.ramp_down_rate_kw_per_min,
            min_duration_minutes=window.min_duration_minutes,
            max_duration_minutes=window.max_duration_minutes,
        )
        with self._session() as session:
            session.add(row)
            session.commit()
        return window

    def list_availability_windows(
        self, *, envelope_id: Optional[str] = None
    ) -> list[AvailabilityWindow]:
        """List availability windows, optionally filtered by envelope ID.

        Args:
            envelope_id: Filter by parent envelope identifier.  When
                ``None``, all windows are returned.

        Returns:
            List of matching availability windows.
        """
        with self._session() as session:
            q = session.query(AvailabilityWindowRow)
            if envelope_id is not None:
                q = q.filter(
                    AvailabilityWindowRow.envelope_id == envelope_id
                )
            return [self._row_to_availability_window(row) for row in q.all()]

    # -- Baseline operations -------------------------------------------------

    def add_baseline(self, baseline: Baseline) -> Baseline:
        """Add a baseline record to the store.

        Args:
            baseline: The baseline data to store.

        Returns:
            The stored baseline (unchanged).
        """
        row = BaselineRow(
            id=str(uuid.uuid4()),
            baseline_id=baseline.baseline_id,
            event_id=baseline.event_id,
            participant_id=baseline.participant_id,
            feeder_id=baseline.feeder_id,
            methodology=baseline.methodology,
            interval_minutes=baseline.interval_minutes,
            values_kw_json=json.dumps(baseline.values_kw),
            baseline_start=baseline.baseline_start,
            baseline_end=baseline.baseline_end,
            valid_from=baseline.valid_from,
            valid_until=baseline.valid_until,
            sensitivity=baseline.sensitivity.value,
            updated_at=baseline.updated_at,
        )
        with self._session() as session:
            session.add(row)
            session.commit()
        return baseline

    def list_baselines(
        self, *, event_id: Optional[str] = None
    ) -> list[Baseline]:
        """List baselines, optionally filtered by event ID.

        Args:
            event_id: Filter by DR event identifier.  When ``None``, all
                baselines are returned.

        Returns:
            List of matching baselines.
        """
        with self._session() as session:
            q = session.query(BaselineRow)
            if event_id is not None:
                q = q.filter(BaselineRow.event_id == event_id)
            return [self._row_to_baseline(row) for row in q.all()]

    def get_baseline(self, baseline_id: str) -> Optional[Baseline]:
        """Retrieve a specific baseline by its baseline ID.

        Args:
            baseline_id: The unique baseline identifier.

        Returns:
            The baseline, or ``None`` if not found.
        """
        with self._session() as session:
            row = (
                session.query(BaselineRow)
                .filter(BaselineRow.baseline_id == baseline_id)
                .first()
            )
            if row is None:
                return None
            return self._row_to_baseline(row)

    # -- Dispatch response operations ----------------------------------------

    def add_dispatch_response(
        self, actual: DispatchActual
    ) -> DispatchActual:
        """Add a dispatch response (actual) to the store.

        Args:
            actual: The dispatch actual data to store.

        Returns:
            The stored dispatch actual (unchanged).
        """
        row = DispatchResponseRow(
            id=str(uuid.uuid4()),
            actual_id=actual.actual_id,
            command_id=actual.command_id,
            event_id=actual.event_id,
            participant_id=actual.participant_id,
            feeder_id=actual.feeder_id,
            commanded_kw=actual.commanded_kw,
            delivered_kw=actual.delivered_kw,
            delivered_kvar=actual.delivered_kvar,
            delivery_start=actual.delivery_start,
            delivery_end=actual.delivery_end,
            delivery_accuracy_pct=actual.delivery_accuracy_pct,
            interval_values_kw_json=json.dumps(actual.interval_values_kw),
            interval_minutes=actual.interval_minutes,
            reported_at=actual.reported_at,
            sensitivity=actual.sensitivity.value,
        )
        with self._session() as session:
            session.add(row)
            session.commit()
        return actual

    def list_dispatch_responses(
        self, *, event_id: Optional[str] = None
    ) -> list[DispatchActual]:
        """List dispatch responses, optionally filtered by event ID.

        Args:
            event_id: Filter by DR event identifier.  When ``None``, all
                responses are returned.

        Returns:
            List of matching dispatch actuals.
        """
        with self._session() as session:
            q = session.query(DispatchResponseRow)
            if event_id is not None:
                q = q.filter(DispatchResponseRow.event_id == event_id)
            return [self._row_to_dispatch_response(row) for row in q.all()]

    def get_dispatch_response(
        self, actual_id: str
    ) -> Optional[DispatchActual]:
        """Retrieve a specific dispatch response by its actual ID.

        Args:
            actual_id: The unique actual report identifier.

        Returns:
            The dispatch actual, or ``None`` if not found.
        """
        with self._session() as session:
            row = (
                session.query(DispatchResponseRow)
                .filter(DispatchResponseRow.actual_id == actual_id)
                .first()
            )
            if row is None:
                return None
            return self._row_to_dispatch_response(row)

    # -- Seed data -----------------------------------------------------------

    def seed(self) -> None:
        """Populate the store with sample data for testing.

        Creates realistic DER fleet aggregate data for two feeders:
          - **F-101**: Mixed fleet (battery + solar PV) with bidirectional
            flexibility and high confidence.
          - **F-102**: EV charger fleet with down-only flexibility (load
            curtailment) and moderate confidence.

        Each feeder gets one flexibility offer with availability windows,
        one baseline, and one dispatch response.  Existing data is not
        cleared, so calling ``seed()`` multiple times will add duplicate
        records.
        """
        now = _utc_now()
        validity_start = now
        validity_end = now + timedelta(hours=24)

        # -- Flexibility offers -----------------------------------------------

        # Fleet 1: Mixed battery + solar on F-101
        offer_1 = FlexibilityEnvelope(
            envelope_id="FE-AGG-101-001",
            unit_id="DER-UNIT-101",
            aggregator_id="aggregator-001",
            feeder_id="F-101",
            direction=FlexibilityDirection.BOTH,
            pq_range=PQRange(
                p_min_kw=-500.0,
                p_max_kw=1200.0,
                q_min_kvar=-200.0,
                q_max_kvar=200.0,
            ),
            availability_windows=[
                AvailabilityWindow(
                    window_id="AW-101-001",
                    available_from=validity_start,
                    available_until=validity_start + timedelta(hours=6),
                    pq_range=PQRange(
                        p_min_kw=-500.0,
                        p_max_kw=1200.0,
                        q_min_kvar=-200.0,
                        q_max_kvar=200.0,
                    ),
                    ramp_up_rate_kw_per_min=50.0,
                    ramp_down_rate_kw_per_min=50.0,
                    min_duration_minutes=15.0,
                    max_duration_minutes=240.0,
                ),
                AvailabilityWindow(
                    window_id="AW-101-002",
                    available_from=validity_start + timedelta(hours=8),
                    available_until=validity_start + timedelta(hours=20),
                    pq_range=PQRange(
                        p_min_kw=-300.0,
                        p_max_kw=800.0,
                        q_min_kvar=-150.0,
                        q_max_kvar=150.0,
                    ),
                    ramp_up_rate_kw_per_min=40.0,
                    ramp_down_rate_kw_per_min=40.0,
                    min_duration_minutes=30.0,
                    max_duration_minutes=180.0,
                ),
            ],
            state_of_charge=StateOfCharge(
                aggregate_soc_pct=65.0,
                total_energy_capacity_kwh=5000.0,
                available_energy_kwh=3250.0,
                min_soc_limit_pct=10.0,
                max_soc_limit_pct=90.0,
                timestamp=now,
            ),
            response_confidence=ResponseConfidence(
                level=ConfidenceLevel.HIGH,
                probability_pct=92.0,
                historical_delivery_rate_pct=95.0,
            ),
            device_class_mix=[
                DeviceClassMix(
                    der_type=DERType.BATTERY_STORAGE,
                    share_pct=60.0,
                    aggregate_capacity_kw=720.0,
                ),
                DeviceClassMix(
                    der_type=DERType.SOLAR_PV,
                    share_pct=40.0,
                    aggregate_capacity_kw=480.0,
                ),
            ],
            price_eur_per_kwh=0.12,
            valid_from=validity_start,
            valid_until=validity_end,
            sensitivity=SensitivityTier.MEDIUM,
            updated_at=now,
        )

        # Fleet 2: EV charger fleet on F-102
        offer_2 = FlexibilityEnvelope(
            envelope_id="FE-AGG-102-001",
            unit_id="DER-UNIT-102",
            aggregator_id="aggregator-001",
            feeder_id="F-102",
            direction=FlexibilityDirection.DOWN,
            pq_range=PQRange(
                p_min_kw=-800.0,
                p_max_kw=0.0,
                q_min_kvar=-100.0,
                q_max_kvar=100.0,
            ),
            availability_windows=[
                AvailabilityWindow(
                    window_id="AW-102-001",
                    available_from=validity_start + timedelta(hours=17),
                    available_until=validity_start + timedelta(hours=23),
                    pq_range=PQRange(
                        p_min_kw=-800.0,
                        p_max_kw=0.0,
                        q_min_kvar=-100.0,
                        q_max_kvar=100.0,
                    ),
                    ramp_up_rate_kw_per_min=100.0,
                    ramp_down_rate_kw_per_min=100.0,
                    min_duration_minutes=15.0,
                    max_duration_minutes=120.0,
                ),
            ],
            state_of_charge=StateOfCharge(
                aggregate_soc_pct=35.0,
                total_energy_capacity_kwh=3200.0,
                available_energy_kwh=1120.0,
                min_soc_limit_pct=20.0,
                max_soc_limit_pct=80.0,
                timestamp=now,
            ),
            response_confidence=ResponseConfidence(
                level=ConfidenceLevel.MEDIUM,
                probability_pct=78.0,
                historical_delivery_rate_pct=82.0,
            ),
            device_class_mix=[
                DeviceClassMix(
                    der_type=DERType.EV_CHARGER,
                    share_pct=100.0,
                    aggregate_capacity_kw=800.0,
                ),
            ],
            price_eur_per_kwh=0.08,
            valid_from=validity_start,
            valid_until=validity_end,
            sensitivity=SensitivityTier.MEDIUM,
            updated_at=now,
        )

        # -- Baselines --------------------------------------------------------

        baselines = [
            Baseline(
                baseline_id="BL-AGG-101-001",
                event_id="DR-EVT-001",
                participant_id="aggregator-001",
                feeder_id="F-101",
                methodology="avg_10_of_10",
                interval_minutes=15.0,
                values_kw=[
                    450.0, 460.0, 470.0, 480.0,
                    490.0, 500.0, 510.0, 520.0,
                ],
                baseline_start=validity_start,
                baseline_end=validity_start + timedelta(hours=2),
                valid_from=validity_start,
                valid_until=validity_end,
                sensitivity=SensitivityTier.MEDIUM,
                updated_at=now,
            ),
            Baseline(
                baseline_id="BL-AGG-102-001",
                event_id="DR-EVT-002",
                participant_id="aggregator-001",
                feeder_id="F-102",
                methodology="regression",
                interval_minutes=15.0,
                values_kw=[
                    600.0, 620.0, 640.0, 660.0,
                    680.0, 700.0, 720.0, 740.0,
                ],
                baseline_start=validity_start + timedelta(hours=17),
                baseline_end=validity_start + timedelta(hours=19),
                valid_from=validity_start,
                valid_until=validity_end,
                sensitivity=SensitivityTier.MEDIUM,
                updated_at=now,
            ),
        ]

        # -- Dispatch responses -----------------------------------------------

        dispatch_responses = [
            DispatchActual(
                actual_id="DA-AGG-101-001",
                command_id="DC-DSO-101-001",
                event_id="DR-EVT-001",
                participant_id="aggregator-001",
                feeder_id="F-101",
                commanded_kw=300.0,
                delivered_kw=285.0,
                delivered_kvar=10.0,
                delivery_start=validity_start - timedelta(hours=2),
                delivery_end=validity_start - timedelta(hours=1),
                delivery_accuracy_pct=95.0,
                interval_values_kw=[
                    280.0, 285.0, 290.0, 288.0,
                    285.0, 282.0, 280.0, 285.0,
                    290.0, 288.0, 285.0, 282.0,
                ],
                interval_minutes=5.0,
                reported_at=validity_start - timedelta(minutes=30),
                sensitivity=SensitivityTier.MEDIUM,
            ),
        ]

        for offer in [offer_1, offer_2]:
            self.add_flexibility_offer(offer)
        for baseline in baselines:
            self.add_baseline(baseline)
        for response in dispatch_responses:
            self.add_dispatch_response(response)

    # -- Conversion helpers --------------------------------------------------

    @staticmethod
    def _row_to_flexibility_offer(
        row: FlexibilityOfferRow,
    ) -> FlexibilityEnvelope:
        """Convert a ``FlexibilityOfferRow`` ORM object to a ``FlexibilityEnvelope`` model."""
        availability_windows_data = json.loads(row.availability_windows_json)
        device_class_mix_data = json.loads(row.device_class_mix_json)

        state_of_charge = None
        if row.state_of_charge_json is not None:
            state_of_charge = StateOfCharge(**json.loads(row.state_of_charge_json))

        response_confidence = ResponseConfidence(
            **json.loads(row.response_confidence_json)
        )

        return FlexibilityEnvelope(
            envelope_id=row.envelope_id,
            unit_id=row.unit_id,
            aggregator_id=row.aggregator_id,
            feeder_id=row.feeder_id,
            direction=FlexibilityDirection(row.direction),
            pq_range=PQRange(
                p_min_kw=row.p_min_kw,
                p_max_kw=row.p_max_kw,
                q_min_kvar=row.q_min_kvar,
                q_max_kvar=row.q_max_kvar,
            ),
            availability_windows=[
                AvailabilityWindow(**w) for w in availability_windows_data
            ],
            state_of_charge=state_of_charge,
            response_confidence=response_confidence,
            device_class_mix=[
                DeviceClassMix(**d) for d in device_class_mix_data
            ],
            price_eur_per_kwh=row.price_eur_per_kwh,
            valid_from=row.valid_from,
            valid_until=row.valid_until,
            sensitivity=SensitivityTier(row.sensitivity),
            updated_at=row.updated_at,
        )

    @staticmethod
    def _row_to_availability_window(
        row: AvailabilityWindowRow,
    ) -> AvailabilityWindow:
        """Convert an ``AvailabilityWindowRow`` ORM object to an ``AvailabilityWindow`` model."""
        return AvailabilityWindow(
            window_id=row.window_id,
            available_from=row.available_from,
            available_until=row.available_until,
            pq_range=PQRange(
                p_min_kw=row.p_min_kw,
                p_max_kw=row.p_max_kw,
                q_min_kvar=row.q_min_kvar,
                q_max_kvar=row.q_max_kvar,
            ),
            ramp_up_rate_kw_per_min=row.ramp_up_rate_kw_per_min,
            ramp_down_rate_kw_per_min=row.ramp_down_rate_kw_per_min,
            min_duration_minutes=row.min_duration_minutes,
            max_duration_minutes=row.max_duration_minutes,
        )

    @staticmethod
    def _row_to_baseline(row: BaselineRow) -> Baseline:
        """Convert a ``BaselineRow`` ORM object to a ``Baseline`` model."""
        return Baseline(
            baseline_id=row.baseline_id,
            event_id=row.event_id,
            participant_id=row.participant_id,
            feeder_id=row.feeder_id,
            methodology=row.methodology,
            interval_minutes=row.interval_minutes,
            values_kw=json.loads(row.values_kw_json),
            baseline_start=row.baseline_start,
            baseline_end=row.baseline_end,
            valid_from=row.valid_from,
            valid_until=row.valid_until,
            sensitivity=SensitivityTier(row.sensitivity),
            updated_at=row.updated_at,
        )

    @staticmethod
    def _row_to_dispatch_response(
        row: DispatchResponseRow,
    ) -> DispatchActual:
        """Convert a ``DispatchResponseRow`` ORM object to a ``DispatchActual`` model."""
        return DispatchActual(
            actual_id=row.actual_id,
            command_id=row.command_id,
            event_id=row.event_id,
            participant_id=row.participant_id,
            feeder_id=row.feeder_id,
            commanded_kw=row.commanded_kw,
            delivered_kw=row.delivered_kw,
            delivered_kvar=row.delivered_kvar,
            delivery_start=row.delivery_start,
            delivery_end=row.delivery_end,
            delivery_accuracy_pct=row.delivery_accuracy_pct,
            interval_values_kw=json.loads(row.interval_values_kw_json),
            interval_minutes=row.interval_minutes,
            reported_at=row.reported_at,
            sensitivity=SensitivityTier(row.sensitivity),
        )
