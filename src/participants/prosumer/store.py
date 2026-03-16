"""SQLite-backed local data store for the Prosumer participant node.

Provides persistent storage for campus prosumer data including smart meter
readings, demand profiles, DR eligibility, controllable margins, and consent
records.  The store is pre-seeded with sample campus data for development
and integration testing.

Key design decisions:
  - Five tables: ``meter_readings``, ``demand_profiles``,
    ``dr_eligibility``, ``controllable_margins``, and ``consent_records``,
    each mapping to the corresponding semantic model from
    :mod:`src.semantic.consumer` or local prosumer-specific data types.
  - Uses a separate SQLAlchemy ``Base`` from other participant stores so
    all can coexist in the same process without table name collisions.
  - JSON serialization for list fields that SQLite does not natively
    support (e.g., ``values_kw`` in demand profiles, ``allowed_data_types``
    in consent records).
  - All timestamps are stored as timezone-aware UTC datetimes.
  - The ``seed()`` method populates the store with realistic sample data
    for a campus prosumer with three buildings (BLDG-A office, BLDG-B lab,
    BLDG-C dormitory) on feeder F-101, covering different load profiles,
    DR eligibility, and controllable margins.
  - The store exposes synchronous methods; async wrappers can be added at
    the route layer when needed.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import Column, DateTime, Float, Integer, String, Text, create_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from src.semantic.cim import SensitivityTier
from src.semantic.consumer import (
    ConsentRecord,
    ConsentStatus,
    DemandProfile,
    DisclosureLevel,
    MeterReading,
)

ProsumerBase = declarative_base()


def _utc_now() -> datetime:
    """Return the current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# SQLAlchemy ORM models
# ---------------------------------------------------------------------------


class MeterReadingRow(ProsumerBase):  # type: ignore[misc]
    """SQLAlchemy model for the ``meter_readings`` table.

    Stores smart meter readings from prosumer installations.  HIGH_PRIVACY
    sensitivity because raw meter data can reveal occupancy patterns.
    """

    __tablename__ = "meter_readings"

    id = Column(String, primary_key=True)
    reading_id = Column(String, nullable=False, index=True)
    meter_id = Column(String, nullable=False, index=True)
    prosumer_id = Column(String, nullable=False, index=True)
    active_power_kw = Column(Float, nullable=False)
    reactive_power_kvar = Column(Float, nullable=True)
    voltage_v = Column(Float, nullable=True)
    cumulative_energy_kwh = Column(Float, nullable=True)
    reading_timestamp = Column(DateTime(timezone=True), nullable=False)
    interval_minutes = Column(Float, nullable=False, default=15.0)
    quality_flag = Column(String, nullable=False, default="valid")
    sensitivity = Column(String, nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)


class DemandProfileRow(ProsumerBase):  # type: ignore[misc]
    """SQLAlchemy model for the ``demand_profiles`` table.

    Stores consumer demand profiles representing typical consumption
    patterns.  List-valued fields are stored as JSON text.
    """

    __tablename__ = "demand_profiles"

    id = Column(String, primary_key=True)
    profile_id = Column(String, nullable=False, index=True)
    prosumer_id = Column(String, nullable=False, index=True)
    profile_type = Column(String, nullable=False)
    interval_minutes = Column(Float, nullable=False, default=15.0)
    values_kw_json = Column(Text, nullable=False)
    peak_demand_kw = Column(Float, nullable=False)
    total_energy_kwh = Column(Float, nullable=False)
    profile_start = Column(DateTime(timezone=True), nullable=False)
    profile_end = Column(DateTime(timezone=True), nullable=False)
    disclosure_level = Column(String, nullable=False)
    valid_from = Column(DateTime(timezone=True), nullable=False)
    valid_until = Column(DateTime(timezone=True), nullable=False)
    sensitivity = Column(String, nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)


class DREligibilityRow(ProsumerBase):  # type: ignore[misc]
    """SQLAlchemy model for the ``dr_eligibility`` table.

    Stores demand response program eligibility information per building.
    Tracks which DR programs each building can participate in and the
    maximum demand reduction available.
    """

    __tablename__ = "dr_eligibility"

    id = Column(String, primary_key=True)
    prosumer_id = Column(String, nullable=False, index=True)
    building_id = Column(String, nullable=False, index=True)
    program_type = Column(String, nullable=False)
    eligible = Column(Integer, nullable=False, default=1)
    max_reduction_kw = Column(Float, nullable=False)
    current_enrollment_status = Column(String, nullable=False, default="enrolled")
    feeder_id = Column(String, nullable=False, index=True)
    valid_from = Column(DateTime(timezone=True), nullable=False)
    valid_until = Column(DateTime(timezone=True), nullable=False)
    sensitivity = Column(String, nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)


class ControllableMarginRow(ProsumerBase):  # type: ignore[misc]
    """SQLAlchemy model for the ``controllable_margins`` table.

    Stores pre-computed controllable margin data per building, representing
    the available load flexibility for DR dispatch purposes.
    """

    __tablename__ = "controllable_margins"

    id = Column(String, primary_key=True)
    prosumer_id = Column(String, nullable=False, index=True)
    building_id = Column(String, nullable=False, index=True)
    margin_kw = Column(Float, nullable=False)
    base_load_kw = Column(Float, nullable=False)
    peak_load_kw = Column(Float, nullable=False)
    controllable_load_kw = Column(Float, nullable=False)
    feeder_id = Column(String, nullable=False, index=True)
    valid_from = Column(DateTime(timezone=True), nullable=False)
    valid_until = Column(DateTime(timezone=True), nullable=False)
    sensitivity = Column(String, nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)


class ConsentRecordRow(ProsumerBase):  # type: ignore[misc]
    """SQLAlchemy model for the ``consent_records`` table.

    Stores consent records that control purpose-based data sharing.
    HIGH_PRIVACY sensitivity — only visible to the consent holder.
    """

    __tablename__ = "consent_records"

    id = Column(String, primary_key=True)
    consent_id = Column(String, nullable=False, index=True)
    prosumer_id = Column(String, nullable=False, index=True)
    requester_id = Column(String, nullable=False, index=True)
    purpose = Column(String, nullable=False)
    allowed_data_types_json = Column(Text, nullable=False)
    disclosure_level = Column(String, nullable=False)
    status = Column(String, nullable=False)
    granted_at = Column(DateTime(timezone=True), nullable=False)
    revoked_at = Column(DateTime(timezone=True), nullable=True)
    valid_from = Column(DateTime(timezone=True), nullable=False)
    valid_until = Column(DateTime(timezone=True), nullable=False)
    sensitivity = Column(String, nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)


# ---------------------------------------------------------------------------
# ProsumerStore
# ---------------------------------------------------------------------------


class ProsumerStore:
    """Persistent local data store for the Prosumer participant node.

    Manages meter readings, demand profiles, DR eligibility, controllable
    margins, and consent records.  Backed by SQLAlchemy with SQLite (dev)
    or PostgreSQL (prod).

    Usage::

        store = ProsumerStore("sqlite:///data/prosumer.db")
        store.seed()  # populate with sample campus data
        readings = store.list_meter_readings(meter_id="MTR-A-001")
        profiles = store.list_demand_profiles(prosumer_id="prosumer-campus-001")

    Args:
        database_url: SQLAlchemy database URL.  Defaults to a file-based
            SQLite database for development.
    """

    def __init__(
        self, database_url: str = "sqlite:///data/prosumer.db"
    ) -> None:
        self._engine = create_engine(database_url, echo=False)
        ProsumerBase.metadata.create_all(self._engine)
        self._session_factory = sessionmaker(bind=self._engine)

    def _session(self) -> Session:
        """Create a new database session."""
        return self._session_factory()

    # -- Meter reading operations --------------------------------------------

    def add_meter_reading(self, reading: MeterReading) -> MeterReading:
        """Add a meter reading record to the store.

        Args:
            reading: The meter reading data to store.

        Returns:
            The stored meter reading (unchanged).
        """
        row = MeterReadingRow(
            id=str(uuid.uuid4()),
            reading_id=reading.reading_id,
            meter_id=reading.meter_id,
            prosumer_id=reading.prosumer_id,
            active_power_kw=reading.active_power_kw,
            reactive_power_kvar=reading.reactive_power_kvar,
            voltage_v=reading.voltage_v,
            cumulative_energy_kwh=reading.cumulative_energy_kwh,
            reading_timestamp=reading.reading_timestamp,
            interval_minutes=reading.interval_minutes,
            quality_flag=reading.quality_flag,
            sensitivity=reading.sensitivity.value,
            updated_at=reading.updated_at,
        )
        with self._session() as session:
            session.add(row)
            session.commit()
        return reading

    def list_meter_readings(
        self,
        *,
        meter_id: Optional[str] = None,
        prosumer_id: Optional[str] = None,
    ) -> list[MeterReading]:
        """List meter readings, optionally filtered by meter or prosumer ID.

        Args:
            meter_id: Filter by smart meter identifier.  When ``None``, all
                readings are returned.
            prosumer_id: Filter by prosumer identifier.

        Returns:
            List of matching meter readings.
        """
        with self._session() as session:
            q = session.query(MeterReadingRow)
            if meter_id is not None:
                q = q.filter(MeterReadingRow.meter_id == meter_id)
            if prosumer_id is not None:
                q = q.filter(MeterReadingRow.prosumer_id == prosumer_id)
            return [self._row_to_meter_reading(row) for row in q.all()]

    def get_meter_reading(
        self, reading_id: str
    ) -> Optional[MeterReading]:
        """Retrieve a specific meter reading by its reading ID.

        Args:
            reading_id: The unique reading identifier.

        Returns:
            The meter reading, or ``None`` if not found.
        """
        with self._session() as session:
            row = (
                session.query(MeterReadingRow)
                .filter(MeterReadingRow.reading_id == reading_id)
                .first()
            )
            if row is None:
                return None
            return self._row_to_meter_reading(row)

    # -- Demand profile operations -------------------------------------------

    def add_demand_profile(self, profile: DemandProfile) -> DemandProfile:
        """Add a demand profile record to the store.

        Args:
            profile: The demand profile data to store.

        Returns:
            The stored demand profile (unchanged).
        """
        row = DemandProfileRow(
            id=str(uuid.uuid4()),
            profile_id=profile.profile_id,
            prosumer_id=profile.prosumer_id,
            profile_type=profile.profile_type,
            interval_minutes=profile.interval_minutes,
            values_kw_json=json.dumps(profile.values_kw),
            peak_demand_kw=profile.peak_demand_kw,
            total_energy_kwh=profile.total_energy_kwh,
            profile_start=profile.profile_start,
            profile_end=profile.profile_end,
            disclosure_level=profile.disclosure_level.value,
            valid_from=profile.valid_from,
            valid_until=profile.valid_until,
            sensitivity=profile.sensitivity.value,
            updated_at=profile.updated_at,
        )
        with self._session() as session:
            session.add(row)
            session.commit()
        return profile

    def list_demand_profiles(
        self,
        *,
        prosumer_id: Optional[str] = None,
        profile_type: Optional[str] = None,
    ) -> list[DemandProfile]:
        """List demand profiles, optionally filtered by prosumer or type.

        Args:
            prosumer_id: Filter by prosumer identifier.  When ``None``, all
                profiles are returned.
            profile_type: Filter by profile type (e.g., ``'typical_day'``).

        Returns:
            List of matching demand profiles.
        """
        with self._session() as session:
            q = session.query(DemandProfileRow)
            if prosumer_id is not None:
                q = q.filter(DemandProfileRow.prosumer_id == prosumer_id)
            if profile_type is not None:
                q = q.filter(DemandProfileRow.profile_type == profile_type)
            return [self._row_to_demand_profile(row) for row in q.all()]

    def get_demand_profile(
        self, profile_id: str
    ) -> Optional[DemandProfile]:
        """Retrieve a specific demand profile by its profile ID.

        Args:
            profile_id: The unique profile identifier.

        Returns:
            The demand profile, or ``None`` if not found.
        """
        with self._session() as session:
            row = (
                session.query(DemandProfileRow)
                .filter(DemandProfileRow.profile_id == profile_id)
                .first()
            )
            if row is None:
                return None
            return self._row_to_demand_profile(row)

    # -- DR eligibility operations -------------------------------------------

    def add_dr_eligibility(
        self,
        *,
        prosumer_id: str,
        building_id: str,
        program_type: str,
        eligible: bool,
        max_reduction_kw: float,
        current_enrollment_status: str,
        feeder_id: str,
        valid_from: datetime,
        valid_until: datetime,
        sensitivity: SensitivityTier = SensitivityTier.MEDIUM,
    ) -> dict:
        """Add a DR eligibility record to the store.

        Args:
            prosumer_id: Prosumer who owns this building.
            building_id: Building identifier.
            program_type: DR program type (e.g., ``'hvac_curtailment'``).
            eligible: Whether the building is eligible for this program.
            max_reduction_kw: Maximum demand reduction available in kW.
            current_enrollment_status: Enrollment status (e.g., ``'enrolled'``).
            feeder_id: Feeder the building is connected to.
            valid_from: Start of validity window.
            valid_until: End of validity window.
            sensitivity: Data sensitivity classification.

        Returns:
            A dict representing the stored DR eligibility record.
        """
        now = _utc_now()
        row = DREligibilityRow(
            id=str(uuid.uuid4()),
            prosumer_id=prosumer_id,
            building_id=building_id,
            program_type=program_type,
            eligible=1 if eligible else 0,
            max_reduction_kw=max_reduction_kw,
            current_enrollment_status=current_enrollment_status,
            feeder_id=feeder_id,
            valid_from=valid_from,
            valid_until=valid_until,
            sensitivity=sensitivity.value,
            updated_at=now,
        )
        with self._session() as session:
            session.add(row)
            session.commit()
        return self._row_to_dr_eligibility_dict(row)

    def list_dr_eligibility(
        self,
        *,
        prosumer_id: Optional[str] = None,
        building_id: Optional[str] = None,
        feeder_id: Optional[str] = None,
    ) -> list[dict]:
        """List DR eligibility records, optionally filtered.

        Args:
            prosumer_id: Filter by prosumer identifier.
            building_id: Filter by building identifier.
            feeder_id: Filter by feeder identifier.

        Returns:
            List of matching DR eligibility records as dicts.
        """
        with self._session() as session:
            q = session.query(DREligibilityRow)
            if prosumer_id is not None:
                q = q.filter(DREligibilityRow.prosumer_id == prosumer_id)
            if building_id is not None:
                q = q.filter(DREligibilityRow.building_id == building_id)
            if feeder_id is not None:
                q = q.filter(DREligibilityRow.feeder_id == feeder_id)
            return [self._row_to_dr_eligibility_dict(row) for row in q.all()]

    def get_dr_eligibility(
        self, prosumer_id: str, building_id: str, program_type: str
    ) -> Optional[dict]:
        """Retrieve a specific DR eligibility record.

        Args:
            prosumer_id: The prosumer identifier.
            building_id: The building identifier.
            program_type: The DR program type.

        Returns:
            The DR eligibility record as a dict, or ``None`` if not found.
        """
        with self._session() as session:
            row = (
                session.query(DREligibilityRow)
                .filter(DREligibilityRow.prosumer_id == prosumer_id)
                .filter(DREligibilityRow.building_id == building_id)
                .filter(DREligibilityRow.program_type == program_type)
                .first()
            )
            if row is None:
                return None
            return self._row_to_dr_eligibility_dict(row)

    # -- Controllable margin operations --------------------------------------

    def add_controllable_margin(
        self,
        *,
        prosumer_id: str,
        building_id: str,
        margin_kw: float,
        base_load_kw: float,
        peak_load_kw: float,
        controllable_load_kw: float,
        feeder_id: str,
        valid_from: datetime,
        valid_until: datetime,
        sensitivity: SensitivityTier = SensitivityTier.MEDIUM,
    ) -> dict:
        """Add a controllable margin record to the store.

        Args:
            prosumer_id: Prosumer who owns this building.
            building_id: Building identifier.
            margin_kw: Available controllable margin in kW.
            base_load_kw: Base (non-controllable) load in kW.
            peak_load_kw: Peak total load in kW.
            controllable_load_kw: Total controllable load in kW.
            feeder_id: Feeder the building is connected to.
            valid_from: Start of validity window.
            valid_until: End of validity window.
            sensitivity: Data sensitivity classification.

        Returns:
            A dict representing the stored controllable margin record.
        """
        now = _utc_now()
        row = ControllableMarginRow(
            id=str(uuid.uuid4()),
            prosumer_id=prosumer_id,
            building_id=building_id,
            margin_kw=margin_kw,
            base_load_kw=base_load_kw,
            peak_load_kw=peak_load_kw,
            controllable_load_kw=controllable_load_kw,
            feeder_id=feeder_id,
            valid_from=valid_from,
            valid_until=valid_until,
            sensitivity=sensitivity.value,
            updated_at=now,
        )
        with self._session() as session:
            session.add(row)
            session.commit()
        return self._row_to_controllable_margin_dict(row)

    def list_controllable_margins(
        self,
        *,
        prosumer_id: Optional[str] = None,
        building_id: Optional[str] = None,
        feeder_id: Optional[str] = None,
    ) -> list[dict]:
        """List controllable margin records, optionally filtered.

        Args:
            prosumer_id: Filter by prosumer identifier.
            building_id: Filter by building identifier.
            feeder_id: Filter by feeder identifier.

        Returns:
            List of matching controllable margin records as dicts.
        """
        with self._session() as session:
            q = session.query(ControllableMarginRow)
            if prosumer_id is not None:
                q = q.filter(ControllableMarginRow.prosumer_id == prosumer_id)
            if building_id is not None:
                q = q.filter(
                    ControllableMarginRow.building_id == building_id
                )
            if feeder_id is not None:
                q = q.filter(ControllableMarginRow.feeder_id == feeder_id)
            return [
                self._row_to_controllable_margin_dict(row) for row in q.all()
            ]

    def get_controllable_margin(
        self, prosumer_id: str, building_id: str
    ) -> Optional[dict]:
        """Retrieve the most recent controllable margin for a building.

        Args:
            prosumer_id: The prosumer identifier.
            building_id: The building identifier.

        Returns:
            The controllable margin record as a dict, or ``None`` if not found.
        """
        with self._session() as session:
            row = (
                session.query(ControllableMarginRow)
                .filter(ControllableMarginRow.prosumer_id == prosumer_id)
                .filter(ControllableMarginRow.building_id == building_id)
                .order_by(ControllableMarginRow.updated_at.desc())
                .first()
            )
            if row is None:
                return None
            return self._row_to_controllable_margin_dict(row)

    # -- Consent record operations -------------------------------------------

    def add_consent_record(self, consent: ConsentRecord) -> ConsentRecord:
        """Add a consent record to the store.

        Args:
            consent: The consent record data to store.

        Returns:
            The stored consent record (unchanged).
        """
        row = ConsentRecordRow(
            id=str(uuid.uuid4()),
            consent_id=consent.consent_id,
            prosumer_id=consent.prosumer_id,
            requester_id=consent.requester_id,
            purpose=consent.purpose,
            allowed_data_types_json=json.dumps(consent.allowed_data_types),
            disclosure_level=consent.disclosure_level.value,
            status=consent.status.value,
            granted_at=consent.granted_at,
            revoked_at=consent.revoked_at,
            valid_from=consent.valid_from,
            valid_until=consent.valid_until,
            sensitivity=consent.sensitivity.value,
            updated_at=consent.updated_at,
        )
        with self._session() as session:
            session.add(row)
            session.commit()
        return consent

    def list_consent_records(
        self,
        *,
        prosumer_id: Optional[str] = None,
        status: Optional[str] = None,
    ) -> list[ConsentRecord]:
        """List consent records, optionally filtered by prosumer or status.

        Args:
            prosumer_id: Filter by prosumer identifier.
            status: Filter by consent status (e.g., ``'active'``).

        Returns:
            List of matching consent records.
        """
        with self._session() as session:
            q = session.query(ConsentRecordRow)
            if prosumer_id is not None:
                q = q.filter(ConsentRecordRow.prosumer_id == prosumer_id)
            if status is not None:
                q = q.filter(ConsentRecordRow.status == status)
            return [self._row_to_consent_record(row) for row in q.all()]

    def get_consent_record(
        self, consent_id: str
    ) -> Optional[ConsentRecord]:
        """Retrieve a specific consent record by its consent ID.

        Args:
            consent_id: The unique consent identifier.

        Returns:
            The consent record, or ``None`` if not found.
        """
        with self._session() as session:
            row = (
                session.query(ConsentRecordRow)
                .filter(ConsentRecordRow.consent_id == consent_id)
                .first()
            )
            if row is None:
                return None
            return self._row_to_consent_record(row)

    # -- Seed data -----------------------------------------------------------

    def seed(self) -> None:
        """Populate the store with sample campus data for testing.

        Creates realistic campus prosumer data for a university campus
        (``prosumer-campus-001``) on feeder F-101 with three buildings:

          - **BLDG-A** (Office): Moderate daytime consumption with HVAC
            curtailment DR eligibility.
          - **BLDG-B** (Laboratory): Higher baseline load with limited
            curtailment potential.
          - **BLDG-C** (Dormitory): Evening-peaked profile with EV managed
            charging and battery dispatch eligibility.

        Each building gets meter readings (4 intervals), a typical-day
        demand profile, DR eligibility records, controllable margin data,
        and sample consent records.  Existing data is not cleared, so
        calling ``seed()`` multiple times will add duplicate records.
        """
        now = _utc_now()
        validity_start = now
        validity_end = now + timedelta(hours=24)
        prosumer_id = "prosumer-campus-001"
        feeder_id = "F-101"

        # -- Meter readings ---------------------------------------------------
        # 4 readings per building (15-min intervals)
        buildings = [
            {
                "building_id": "BLDG-A",
                "meter_id": "MTR-A-001",
                "readings": [
                    (120.0, 15.0, 231.5, 5000.0),
                    (135.0, 18.0, 230.8, 5033.75),
                    (145.0, 20.0, 230.2, 5070.0),
                    (130.0, 16.0, 231.0, 5102.5),
                ],
            },
            {
                "building_id": "BLDG-B",
                "meter_id": "MTR-B-001",
                "readings": [
                    (250.0, 30.0, 230.0, 12000.0),
                    (260.0, 32.0, 229.5, 12065.0),
                    (270.0, 35.0, 229.0, 12132.5),
                    (255.0, 31.0, 229.8, 12196.25),
                ],
            },
            {
                "building_id": "BLDG-C",
                "meter_id": "MTR-C-001",
                "readings": [
                    (80.0, 10.0, 232.0, 8000.0),
                    (85.0, 12.0, 231.5, 8021.25),
                    (180.0, 25.0, 230.5, 8066.25),
                    (200.0, 28.0, 230.0, 8116.25),
                ],
            },
        ]

        for bldg in buildings:
            for i, (p_kw, q_kvar, v_v, cum_kwh) in enumerate(
                bldg["readings"]
            ):
                reading = MeterReading(
                    reading_id=f"RD-{bldg['building_id']}-{i + 1:03d}",
                    meter_id=bldg["meter_id"],
                    prosumer_id=prosumer_id,
                    active_power_kw=p_kw,
                    reactive_power_kvar=q_kvar,
                    voltage_v=v_v,
                    cumulative_energy_kwh=cum_kwh,
                    reading_timestamp=now - timedelta(minutes=15 * (3 - i)),
                    interval_minutes=15.0,
                    quality_flag="valid",
                    sensitivity=SensitivityTier.HIGH_PRIVACY,
                    updated_at=now,
                )
                self.add_meter_reading(reading)

        # -- Demand profiles --------------------------------------------------
        # Typical-day profiles (96 intervals = 24h at 15-min resolution)
        # BLDG-A: Office — peaks during business hours
        office_values = (
            [50.0] * 24         # 00:00–06:00 (low overnight)
            + [80.0, 100.0, 120.0, 140.0]  # 06:00–07:00 (ramp up)
            + [150.0] * 8       # 07:00–09:00 (morning)
            + [160.0] * 16      # 09:00–13:00 (peak office hours)
            + [150.0] * 8       # 13:00–15:00 (early afternoon)
            + [130.0] * 8       # 15:00–17:00 (late afternoon)
            + [100.0, 80.0, 70.0, 60.0]  # 17:00–18:00 (ramp down)
            + [50.0] * 24       # 18:00–24:00 (evening low)
        )

        # BLDG-B: Lab — higher constant load
        lab_values = (
            [200.0] * 24        # 00:00–06:00 (baseline equipment)
            + [220.0] * 4       # 06:00–07:00
            + [250.0] * 8       # 07:00–09:00
            + [280.0] * 16      # 09:00–13:00 (peak research hours)
            + [270.0] * 8       # 13:00–15:00
            + [260.0] * 8       # 15:00–17:00
            + [240.0] * 4       # 17:00–18:00
            + [210.0] * 24      # 18:00–24:00
        )

        # BLDG-C: Dormitory — evening peaked
        dorm_values = (
            [60.0] * 24         # 00:00–06:00 (overnight low)
            + [70.0] * 4        # 06:00–07:00 (wake up)
            + [90.0] * 8        # 07:00–09:00 (morning routine)
            + [50.0] * 16       # 09:00–13:00 (most residents out)
            + [60.0] * 8        # 13:00–15:00
            + [80.0] * 8        # 15:00–17:00 (returning)
            + [150.0, 170.0, 190.0, 200.0]  # 17:00–18:00 (evening ramp)
            + [210.0] * 24      # 18:00–24:00 (evening peak)
        )

        profiles_data = [
            ("BLDG-A", "DP-A-001", "typical_day", office_values),
            ("BLDG-B", "DP-B-001", "typical_day", lab_values),
            ("BLDG-C", "DP-C-001", "typical_day", dorm_values),
        ]

        for bldg_id, profile_id, ptype, values in profiles_data:
            peak_kw = max(values)
            total_kwh = sum(values) * 0.25  # 15-min intervals -> kWh
            profile = DemandProfile(
                profile_id=profile_id,
                prosumer_id=prosumer_id,
                profile_type=ptype,
                interval_minutes=15.0,
                values_kw=values,
                peak_demand_kw=peak_kw,
                total_energy_kwh=round(total_kwh, 2),
                profile_start=validity_start,
                profile_end=validity_end,
                disclosure_level=DisclosureLevel.RAW,
                valid_from=validity_start,
                valid_until=validity_end,
                sensitivity=SensitivityTier.HIGH_PRIVACY,
                updated_at=now,
            )
            self.add_demand_profile(profile)

        # -- DR eligibility ---------------------------------------------------
        dr_programs = [
            {
                "building_id": "BLDG-A",
                "programs": [
                    ("hvac_curtailment", True, 40.0, "enrolled"),
                    ("lighting_dimming", True, 15.0, "enrolled"),
                ],
            },
            {
                "building_id": "BLDG-B",
                "programs": [
                    ("hvac_curtailment", True, 25.0, "enrolled"),
                    ("equipment_scheduling", False, 0.0, "not_eligible"),
                ],
            },
            {
                "building_id": "BLDG-C",
                "programs": [
                    ("ev_managed_charging", True, 50.0, "enrolled"),
                    ("battery_dispatch", True, 30.0, "enrolled"),
                    ("hvac_curtailment", True, 20.0, "pending"),
                ],
            },
        ]

        for bldg in dr_programs:
            for program_type, eligible, max_kw, status in bldg["programs"]:
                self.add_dr_eligibility(
                    prosumer_id=prosumer_id,
                    building_id=bldg["building_id"],
                    program_type=program_type,
                    eligible=eligible,
                    max_reduction_kw=max_kw,
                    current_enrollment_status=status,
                    feeder_id=feeder_id,
                    valid_from=validity_start,
                    valid_until=validity_end,
                    sensitivity=SensitivityTier.MEDIUM,
                )

        # -- Controllable margins ---------------------------------------------
        margins = [
            {
                "building_id": "BLDG-A",
                "margin_kw": 55.0,
                "base_load_kw": 100.0,
                "peak_load_kw": 160.0,
                "controllable_load_kw": 60.0,
            },
            {
                "building_id": "BLDG-B",
                "margin_kw": 25.0,
                "base_load_kw": 200.0,
                "peak_load_kw": 280.0,
                "controllable_load_kw": 30.0,
            },
            {
                "building_id": "BLDG-C",
                "margin_kw": 80.0,
                "base_load_kw": 50.0,
                "peak_load_kw": 210.0,
                "controllable_load_kw": 100.0,
            },
        ]

        for m in margins:
            self.add_controllable_margin(
                prosumer_id=prosumer_id,
                building_id=m["building_id"],
                margin_kw=m["margin_kw"],
                base_load_kw=m["base_load_kw"],
                peak_load_kw=m["peak_load_kw"],
                controllable_load_kw=m["controllable_load_kw"],
                feeder_id=feeder_id,
                valid_from=validity_start,
                valid_until=validity_end,
                sensitivity=SensitivityTier.MEDIUM,
            )

        # -- Consent records --------------------------------------------------
        consents = [
            ConsentRecord(
                consent_id="CONSENT-campus-001",
                prosumer_id=prosumer_id,
                requester_id="aggregator-001",
                purpose="dr_dispatch",
                allowed_data_types=["controllable_margin", "dr_eligibility"],
                disclosure_level=DisclosureLevel.CONTROLLABILITY_ONLY,
                status=ConsentStatus.ACTIVE,
                granted_at=now - timedelta(days=7),
                revoked_at=None,
                valid_from=now - timedelta(days=7),
                valid_until=now + timedelta(days=180),
                sensitivity=SensitivityTier.HIGH_PRIVACY,
                updated_at=now,
            ),
            ConsentRecord(
                consent_id="CONSENT-campus-002",
                prosumer_id=prosumer_id,
                requester_id="dso-001",
                purpose="grid_analysis",
                allowed_data_types=["demand_profile"],
                disclosure_level=DisclosureLevel.AGGREGATED,
                status=ConsentStatus.ACTIVE,
                granted_at=now - timedelta(days=30),
                revoked_at=None,
                valid_from=now - timedelta(days=30),
                valid_until=now + timedelta(days=335),
                sensitivity=SensitivityTier.HIGH_PRIVACY,
                updated_at=now,
            ),
            ConsentRecord(
                consent_id="CONSENT-campus-003",
                prosumer_id=prosumer_id,
                requester_id="researcher-001",
                purpose="research",
                allowed_data_types=["demand_profile"],
                disclosure_level=DisclosureLevel.AGGREGATED,
                status=ConsentStatus.ACTIVE,
                granted_at=now - timedelta(days=14),
                revoked_at=None,
                valid_from=now - timedelta(days=14),
                valid_until=now + timedelta(days=90),
                sensitivity=SensitivityTier.HIGH_PRIVACY,
                updated_at=now,
            ),
        ]

        for consent in consents:
            self.add_consent_record(consent)

    # -- Conversion helpers --------------------------------------------------

    @staticmethod
    def _row_to_meter_reading(row: MeterReadingRow) -> MeterReading:
        """Convert a ``MeterReadingRow`` ORM object to a ``MeterReading`` model."""
        return MeterReading(
            reading_id=row.reading_id,
            meter_id=row.meter_id,
            prosumer_id=row.prosumer_id,
            active_power_kw=row.active_power_kw,
            reactive_power_kvar=row.reactive_power_kvar,
            voltage_v=row.voltage_v,
            cumulative_energy_kwh=row.cumulative_energy_kwh,
            reading_timestamp=row.reading_timestamp,
            interval_minutes=row.interval_minutes,
            quality_flag=row.quality_flag,
            sensitivity=SensitivityTier(row.sensitivity),
            updated_at=row.updated_at,
        )

    @staticmethod
    def _row_to_demand_profile(row: DemandProfileRow) -> DemandProfile:
        """Convert a ``DemandProfileRow`` ORM object to a ``DemandProfile`` model."""
        return DemandProfile(
            profile_id=row.profile_id,
            prosumer_id=row.prosumer_id,
            profile_type=row.profile_type,
            interval_minutes=row.interval_minutes,
            values_kw=json.loads(row.values_kw_json),
            peak_demand_kw=row.peak_demand_kw,
            total_energy_kwh=row.total_energy_kwh,
            profile_start=row.profile_start,
            profile_end=row.profile_end,
            disclosure_level=DisclosureLevel(row.disclosure_level),
            valid_from=row.valid_from,
            valid_until=row.valid_until,
            sensitivity=SensitivityTier(row.sensitivity),
            updated_at=row.updated_at,
        )

    @staticmethod
    def _row_to_dr_eligibility_dict(row: DREligibilityRow) -> dict:
        """Convert a ``DREligibilityRow`` ORM object to a plain dict."""
        return {
            "prosumer_id": row.prosumer_id,
            "building_id": row.building_id,
            "program_type": row.program_type,
            "eligible": bool(row.eligible),
            "max_reduction_kw": row.max_reduction_kw,
            "current_enrollment_status": row.current_enrollment_status,
            "feeder_id": row.feeder_id,
            "valid_from": row.valid_from,
            "valid_until": row.valid_until,
            "sensitivity": row.sensitivity,
            "updated_at": row.updated_at,
        }

    @staticmethod
    def _row_to_controllable_margin_dict(
        row: ControllableMarginRow,
    ) -> dict:
        """Convert a ``ControllableMarginRow`` ORM object to a plain dict."""
        return {
            "prosumer_id": row.prosumer_id,
            "building_id": row.building_id,
            "margin_kw": row.margin_kw,
            "base_load_kw": row.base_load_kw,
            "peak_load_kw": row.peak_load_kw,
            "controllable_load_kw": row.controllable_load_kw,
            "feeder_id": row.feeder_id,
            "valid_from": row.valid_from,
            "valid_until": row.valid_until,
            "sensitivity": row.sensitivity,
            "updated_at": row.updated_at,
        }

    @staticmethod
    def _row_to_consent_record(row: ConsentRecordRow) -> ConsentRecord:
        """Convert a ``ConsentRecordRow`` ORM object to a ``ConsentRecord`` model."""
        return ConsentRecord(
            consent_id=row.consent_id,
            prosumer_id=row.prosumer_id,
            requester_id=row.requester_id,
            purpose=row.purpose,
            allowed_data_types=json.loads(row.allowed_data_types_json),
            disclosure_level=DisclosureLevel(row.disclosure_level),
            status=ConsentStatus(row.status),
            granted_at=row.granted_at,
            revoked_at=row.revoked_at,
            valid_from=row.valid_from,
            valid_until=row.valid_until,
            sensitivity=SensitivityTier(row.sensitivity),
            updated_at=row.updated_at,
        )
