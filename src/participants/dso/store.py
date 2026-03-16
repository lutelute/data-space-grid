"""SQLite-backed local data store for the DSO participant node.

Provides persistent storage for grid operational data that the DSO publishes
to the federated data space: feeder constraints, congestion signals, and
hosting capacity.  The store is pre-seeded with sample data for development
and integration testing.

Key design decisions:
  - Three tables: ``feeder_constraints``, ``congestion_signals``, and
    ``hosting_capacity``, each mapping directly to the corresponding CIM
    semantic model from :mod:`src.semantic.cim`.
  - Uses a separate SQLAlchemy ``Base`` from the catalog store so both can
    coexist in the same process without table name collisions.
  - JSON serialization for list/dict fields that SQLite does not natively
    support.
  - All timestamps are stored as timezone-aware UTC datetimes.
  - The ``seed()`` method populates the store with realistic sample data
    for three feeders (F-101, F-102, F-103) covering normal, congested,
    and near-limit operating conditions.
  - The store exposes synchronous methods; async wrappers can be added at
    the route layer when needed.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import Column, DateTime, Float, String, Text, create_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from src.semantic.cim import (
    CongestionSignal,
    FeederConstraint,
    HostingCapacity,
    SensitivityTier,
)

DSOBase = declarative_base()


def _utc_now() -> datetime:
    """Return the current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# SQLAlchemy ORM models
# ---------------------------------------------------------------------------


class FeederConstraintRow(DSOBase):  # type: ignore[misc]
    """SQLAlchemy model for the ``feeder_constraints`` table.

    Stores operational limits on distribution feeders published by the DSO.
    """

    __tablename__ = "feeder_constraints"

    id = Column(String, primary_key=True)
    feeder_id = Column(String, nullable=False, index=True)
    max_active_power_kw = Column(Float, nullable=False)
    min_voltage_pu = Column(Float, nullable=False)
    max_voltage_pu = Column(Float, nullable=False)
    congestion_level = Column(Float, nullable=False)
    valid_from = Column(DateTime(timezone=True), nullable=False)
    valid_until = Column(DateTime(timezone=True), nullable=False)
    sensitivity = Column(String, nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)


class CongestionSignalRow(DSOBase):  # type: ignore[misc]
    """SQLAlchemy model for the ``congestion_signals`` table.

    Stores real-time congestion signals for feeders or grid segments.
    """

    __tablename__ = "congestion_signals"

    id = Column(String, primary_key=True)
    signal_id = Column(String, nullable=False, index=True)
    feeder_id = Column(String, nullable=False, index=True)
    congestion_level = Column(Float, nullable=False)
    max_available_capacity_kw = Column(Float, nullable=False)
    direction = Column(String, nullable=False, default="both")
    timestamp = Column(DateTime(timezone=True), nullable=False)
    valid_from = Column(DateTime(timezone=True), nullable=False)
    valid_until = Column(DateTime(timezone=True), nullable=False)
    sensitivity = Column(String, nullable=False)


class HostingCapacityRow(DSOBase):  # type: ignore[misc]
    """SQLAlchemy model for the ``hosting_capacity`` table.

    Stores available hosting capacity at grid nodes or feeders.
    """

    __tablename__ = "hosting_capacity"

    id = Column(String, primary_key=True)
    node_id = Column(String, nullable=False, index=True)
    feeder_id = Column(String, nullable=False, index=True)
    max_generation_kw = Column(Float, nullable=False)
    max_load_kw = Column(Float, nullable=False)
    current_generation_kw = Column(Float, nullable=False)
    current_load_kw = Column(Float, nullable=False)
    voltage_headroom_pu = Column(Float, nullable=False)
    thermal_headroom_pct = Column(Float, nullable=False)
    valid_from = Column(DateTime(timezone=True), nullable=False)
    valid_until = Column(DateTime(timezone=True), nullable=False)
    sensitivity = Column(String, nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)


# ---------------------------------------------------------------------------
# DSOStore
# ---------------------------------------------------------------------------


class DSOStore:
    """Persistent local data store for the DSO participant node.

    Manages feeder constraints, congestion signals, and hosting capacity
    data.  Backed by SQLAlchemy with SQLite (dev) or PostgreSQL (prod).

    Usage::

        store = DSOStore("sqlite:///data/dso.db")
        store.seed()  # populate with sample data
        constraints = store.list_feeder_constraints()
        signal = store.get_congestion_signal(signal_id)

    Args:
        database_url: SQLAlchemy database URL.  Defaults to a file-based
            SQLite database for development.
    """

    def __init__(self, database_url: str = "sqlite:///data/dso.db") -> None:
        self._engine = create_engine(database_url, echo=False)
        DSOBase.metadata.create_all(self._engine)
        self._session_factory = sessionmaker(bind=self._engine)

    def _session(self) -> Session:
        """Create a new database session."""
        return self._session_factory()

    # -- Feeder constraint operations ----------------------------------------

    def add_feeder_constraint(
        self, constraint: FeederConstraint
    ) -> FeederConstraint:
        """Add a feeder constraint record to the store.

        Args:
            constraint: The feeder constraint data to store.

        Returns:
            The stored feeder constraint (unchanged).
        """
        row = FeederConstraintRow(
            id=str(uuid.uuid4()),
            feeder_id=constraint.feeder_id,
            max_active_power_kw=constraint.max_active_power_kw,
            min_voltage_pu=constraint.min_voltage_pu,
            max_voltage_pu=constraint.max_voltage_pu,
            congestion_level=constraint.congestion_level,
            valid_from=constraint.valid_from,
            valid_until=constraint.valid_until,
            sensitivity=constraint.sensitivity.value,
            updated_at=constraint.updated_at,
        )
        with self._session() as session:
            session.add(row)
            session.commit()
        return constraint

    def list_feeder_constraints(
        self, *, feeder_id: Optional[str] = None
    ) -> list[FeederConstraint]:
        """List feeder constraints, optionally filtered by feeder ID.

        Args:
            feeder_id: Filter by feeder identifier.  When ``None``, all
                constraints are returned.

        Returns:
            List of matching feeder constraints.
        """
        with self._session() as session:
            q = session.query(FeederConstraintRow)
            if feeder_id is not None:
                q = q.filter(FeederConstraintRow.feeder_id == feeder_id)
            return [self._row_to_feeder_constraint(row) for row in q.all()]

    def get_feeder_constraint(
        self, feeder_id: str
    ) -> Optional[FeederConstraint]:
        """Retrieve the most recent feeder constraint for a given feeder.

        Args:
            feeder_id: The feeder identifier.

        Returns:
            The most recent feeder constraint, or ``None`` if not found.
        """
        with self._session() as session:
            row = (
                session.query(FeederConstraintRow)
                .filter(FeederConstraintRow.feeder_id == feeder_id)
                .order_by(FeederConstraintRow.updated_at.desc())
                .first()
            )
            if row is None:
                return None
            return self._row_to_feeder_constraint(row)

    # -- Congestion signal operations ----------------------------------------

    def add_congestion_signal(
        self, signal: CongestionSignal
    ) -> CongestionSignal:
        """Add a congestion signal record to the store.

        Args:
            signal: The congestion signal data to store.

        Returns:
            The stored congestion signal (unchanged).
        """
        row = CongestionSignalRow(
            id=str(uuid.uuid4()),
            signal_id=signal.signal_id,
            feeder_id=signal.feeder_id,
            congestion_level=signal.congestion_level,
            max_available_capacity_kw=signal.max_available_capacity_kw,
            direction=signal.direction,
            timestamp=signal.timestamp,
            valid_from=signal.valid_from,
            valid_until=signal.valid_until,
            sensitivity=signal.sensitivity.value,
        )
        with self._session() as session:
            session.add(row)
            session.commit()
        return signal

    def list_congestion_signals(
        self, *, feeder_id: Optional[str] = None
    ) -> list[CongestionSignal]:
        """List congestion signals, optionally filtered by feeder ID.

        Args:
            feeder_id: Filter by feeder identifier.  When ``None``, all
                signals are returned.

        Returns:
            List of matching congestion signals.
        """
        with self._session() as session:
            q = session.query(CongestionSignalRow)
            if feeder_id is not None:
                q = q.filter(CongestionSignalRow.feeder_id == feeder_id)
            return [self._row_to_congestion_signal(row) for row in q.all()]

    def get_congestion_signal(
        self, signal_id: str
    ) -> Optional[CongestionSignal]:
        """Retrieve a specific congestion signal by its signal ID.

        Args:
            signal_id: The unique signal identifier.

        Returns:
            The congestion signal, or ``None`` if not found.
        """
        with self._session() as session:
            row = (
                session.query(CongestionSignalRow)
                .filter(CongestionSignalRow.signal_id == signal_id)
                .first()
            )
            if row is None:
                return None
            return self._row_to_congestion_signal(row)

    # -- Hosting capacity operations -----------------------------------------

    def add_hosting_capacity(
        self, capacity: HostingCapacity
    ) -> HostingCapacity:
        """Add a hosting capacity record to the store.

        Args:
            capacity: The hosting capacity data to store.

        Returns:
            The stored hosting capacity (unchanged).
        """
        row = HostingCapacityRow(
            id=str(uuid.uuid4()),
            node_id=capacity.node_id,
            feeder_id=capacity.feeder_id,
            max_generation_kw=capacity.max_generation_kw,
            max_load_kw=capacity.max_load_kw,
            current_generation_kw=capacity.current_generation_kw,
            current_load_kw=capacity.current_load_kw,
            voltage_headroom_pu=capacity.voltage_headroom_pu,
            thermal_headroom_pct=capacity.thermal_headroom_pct,
            valid_from=capacity.valid_from,
            valid_until=capacity.valid_until,
            sensitivity=capacity.sensitivity.value,
            updated_at=capacity.updated_at,
        )
        with self._session() as session:
            session.add(row)
            session.commit()
        return capacity

    def list_hosting_capacity(
        self, *, feeder_id: Optional[str] = None
    ) -> list[HostingCapacity]:
        """List hosting capacity records, optionally filtered by feeder ID.

        Args:
            feeder_id: Filter by feeder identifier.  When ``None``, all
                records are returned.

        Returns:
            List of matching hosting capacity records.
        """
        with self._session() as session:
            q = session.query(HostingCapacityRow)
            if feeder_id is not None:
                q = q.filter(HostingCapacityRow.feeder_id == feeder_id)
            return [self._row_to_hosting_capacity(row) for row in q.all()]

    def get_hosting_capacity(
        self, node_id: str
    ) -> Optional[HostingCapacity]:
        """Retrieve the most recent hosting capacity for a given node.

        Args:
            node_id: The grid node identifier.

        Returns:
            The most recent hosting capacity record, or ``None`` if not found.
        """
        with self._session() as session:
            row = (
                session.query(HostingCapacityRow)
                .filter(HostingCapacityRow.node_id == node_id)
                .order_by(HostingCapacityRow.updated_at.desc())
                .first()
            )
            if row is None:
                return None
            return self._row_to_hosting_capacity(row)

    # -- Seed data -----------------------------------------------------------

    def seed(self) -> None:
        """Populate the store with sample data for testing.

        Creates realistic grid operational data for three feeders:
          - **F-101**: Normal operations (low congestion, ample capacity)
          - **F-102**: Congested feeder (high congestion, limited capacity)
          - **F-103**: Near-limit feeder (moderate congestion, tight margins)

        Each feeder gets one feeder constraint, one congestion signal, and
        one hosting capacity record.  Existing data is not cleared, so
        calling ``seed()`` multiple times will add duplicate records.
        """
        now = _utc_now()
        validity_start = now
        validity_end = now + timedelta(hours=24)

        # -- Feeder constraints -----------------------------------------------
        constraints = [
            FeederConstraint(
                feeder_id="F-101",
                max_active_power_kw=5000.0,
                min_voltage_pu=0.95,
                max_voltage_pu=1.05,
                congestion_level=0.2,
                valid_from=validity_start,
                valid_until=validity_end,
                sensitivity=SensitivityTier.MEDIUM,
                updated_at=now,
            ),
            FeederConstraint(
                feeder_id="F-102",
                max_active_power_kw=3000.0,
                min_voltage_pu=0.94,
                max_voltage_pu=1.06,
                congestion_level=0.85,
                valid_from=validity_start,
                valid_until=validity_end,
                sensitivity=SensitivityTier.MEDIUM,
                updated_at=now,
            ),
            FeederConstraint(
                feeder_id="F-103",
                max_active_power_kw=4000.0,
                min_voltage_pu=0.95,
                max_voltage_pu=1.05,
                congestion_level=0.6,
                valid_from=validity_start,
                valid_until=validity_end,
                sensitivity=SensitivityTier.MEDIUM,
                updated_at=now,
            ),
        ]

        # -- Congestion signals -----------------------------------------------
        signals = [
            CongestionSignal(
                signal_id="CS-101-001",
                feeder_id="F-101",
                congestion_level=0.2,
                max_available_capacity_kw=4000.0,
                direction="both",
                timestamp=now,
                valid_from=validity_start,
                valid_until=validity_end,
                sensitivity=SensitivityTier.MEDIUM,
            ),
            CongestionSignal(
                signal_id="CS-102-001",
                feeder_id="F-102",
                congestion_level=0.85,
                max_available_capacity_kw=450.0,
                direction="export",
                timestamp=now,
                valid_from=validity_start,
                valid_until=validity_end,
                sensitivity=SensitivityTier.MEDIUM,
            ),
            CongestionSignal(
                signal_id="CS-103-001",
                feeder_id="F-103",
                congestion_level=0.6,
                max_available_capacity_kw=1600.0,
                direction="import",
                timestamp=now,
                valid_from=validity_start,
                valid_until=validity_end,
                sensitivity=SensitivityTier.MEDIUM,
            ),
        ]

        # -- Hosting capacity -------------------------------------------------
        capacities = [
            HostingCapacity(
                node_id="N-101-A",
                feeder_id="F-101",
                max_generation_kw=3000.0,
                max_load_kw=5000.0,
                current_generation_kw=800.0,
                current_load_kw=2500.0,
                voltage_headroom_pu=0.04,
                thermal_headroom_pct=50.0,
                valid_from=validity_start,
                valid_until=validity_end,
                sensitivity=SensitivityTier.MEDIUM,
                updated_at=now,
            ),
            HostingCapacity(
                node_id="N-102-A",
                feeder_id="F-102",
                max_generation_kw=2000.0,
                max_load_kw=3000.0,
                current_generation_kw=1800.0,
                current_load_kw=2700.0,
                voltage_headroom_pu=0.01,
                thermal_headroom_pct=10.0,
                valid_from=validity_start,
                valid_until=validity_end,
                sensitivity=SensitivityTier.MEDIUM,
                updated_at=now,
            ),
            HostingCapacity(
                node_id="N-103-A",
                feeder_id="F-103",
                max_generation_kw=2500.0,
                max_load_kw=4000.0,
                current_generation_kw=1500.0,
                current_load_kw=3200.0,
                voltage_headroom_pu=0.02,
                thermal_headroom_pct=25.0,
                valid_from=validity_start,
                valid_until=validity_end,
                sensitivity=SensitivityTier.MEDIUM,
                updated_at=now,
            ),
        ]

        for constraint in constraints:
            self.add_feeder_constraint(constraint)
        for signal in signals:
            self.add_congestion_signal(signal)
        for capacity in capacities:
            self.add_hosting_capacity(capacity)

    # -- Conversion helpers --------------------------------------------------

    @staticmethod
    def _row_to_feeder_constraint(row: FeederConstraintRow) -> FeederConstraint:
        """Convert a ``FeederConstraintRow`` ORM object to a ``FeederConstraint`` model."""
        return FeederConstraint(
            feeder_id=row.feeder_id,
            max_active_power_kw=row.max_active_power_kw,
            min_voltage_pu=row.min_voltage_pu,
            max_voltage_pu=row.max_voltage_pu,
            congestion_level=row.congestion_level,
            valid_from=row.valid_from,
            valid_until=row.valid_until,
            sensitivity=SensitivityTier(row.sensitivity),
            updated_at=row.updated_at,
        )

    @staticmethod
    def _row_to_congestion_signal(row: CongestionSignalRow) -> CongestionSignal:
        """Convert a ``CongestionSignalRow`` ORM object to a ``CongestionSignal`` model."""
        return CongestionSignal(
            signal_id=row.signal_id,
            feeder_id=row.feeder_id,
            congestion_level=row.congestion_level,
            max_available_capacity_kw=row.max_available_capacity_kw,
            direction=row.direction,
            timestamp=row.timestamp,
            valid_from=row.valid_from,
            valid_until=row.valid_until,
            sensitivity=SensitivityTier(row.sensitivity),
        )

    @staticmethod
    def _row_to_hosting_capacity(row: HostingCapacityRow) -> HostingCapacity:
        """Convert a ``HostingCapacityRow`` ORM object to a ``HostingCapacity`` model."""
        return HostingCapacity(
            node_id=row.node_id,
            feeder_id=row.feeder_id,
            max_generation_kw=row.max_generation_kw,
            max_load_kw=row.max_load_kw,
            current_generation_kw=row.current_generation_kw,
            current_load_kw=row.current_load_kw,
            voltage_headroom_pu=row.voltage_headroom_pu,
            thermal_headroom_pct=row.thermal_headroom_pct,
            valid_from=row.valid_from,
            valid_until=row.valid_until,
            sensitivity=SensitivityTier(row.sensitivity),
            updated_at=row.updated_at,
        )
