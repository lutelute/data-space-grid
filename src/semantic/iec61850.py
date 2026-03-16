"""IEC 61850-inspired DER semantic models for the Federated Data Space.

Models represent aggregate flexibility of distributed energy resources (DER)
without exposing individual device states. Aligned to IEC 61850 logical-node
concepts but focused on the data semantics (not the wire protocol). Every
exchanged data type has an explicit sensitivity tier, timestamps, and validity
windows to support contract-gated data sharing.

Data Sensitivity Classification (from spec):
  - DER flexibility envelopes: MEDIUM (contract-gated, DSO + authorized aggregators)
  - Aggregate availability windows: MEDIUM (contract-gated)
  - Device class mix: MEDIUM (aggregate composition only, no individual devices)

Key Design Principle:
  Aggregators share aggregate flexibility F(t) — never individual device states
  x_i. This preserves data sovereignty while enabling grid coordination.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field

from src.semantic.cim import SensitivityTier


def _utc_now() -> datetime:
    """Return the current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)


class DERType(str, Enum):
    """Classification of distributed energy resource types."""

    BATTERY_STORAGE = "battery_storage"
    SOLAR_PV = "solar_pv"
    WIND = "wind"
    EV_CHARGER = "ev_charger"
    HEAT_PUMP = "heat_pump"
    CHP = "chp"  # Combined heat and power
    CONTROLLABLE_LOAD = "controllable_load"
    FUEL_CELL = "fuel_cell"
    OTHER = "other"


class FlexibilityDirection(str, Enum):
    """Direction of flexibility that can be offered."""

    UP = "up"  # Can increase generation or decrease consumption
    DOWN = "down"  # Can decrease generation or increase consumption
    BOTH = "both"  # Bidirectional flexibility


class ConfidenceLevel(str, Enum):
    """Qualitative confidence in delivery of offered flexibility."""

    HIGH = "high"  # > 90% probability of delivery
    MEDIUM = "medium"  # 70-90% probability
    LOW = "low"  # 50-70% probability
    INDICATIVE = "indicative"  # Estimate only, not firm


class PQRange(BaseModel):
    """Active and reactive power operating range for DER flexibility.

    Defines the P (active) and Q (reactive) power bounds that a DER
    portfolio can operate within. Values represent aggregate capability,
    never individual device limits.
    """

    p_min_kw: float = Field(
        ..., description="Minimum active power in kW (negative = consumption)"
    )
    p_max_kw: float = Field(
        ..., description="Maximum active power in kW (positive = generation)"
    )
    q_min_kvar: float = Field(
        ..., description="Minimum reactive power in kVAr"
    )
    q_max_kvar: float = Field(
        ..., description="Maximum reactive power in kVAr"
    )


class StateOfCharge(BaseModel):
    """Aggregate state of charge for storage assets in a DER portfolio.

    Represents the combined energy state of all storage devices (batteries,
    EVs, thermal storage) without exposing individual device levels.
    """

    aggregate_soc_pct: float = Field(
        ..., ge=0.0, le=100.0,
        description="Weighted average state of charge as percentage",
    )
    total_energy_capacity_kwh: float = Field(
        ..., ge=0, description="Total energy capacity across all storage assets in kWh"
    )
    available_energy_kwh: float = Field(
        ..., ge=0, description="Currently available energy in kWh"
    )
    min_soc_limit_pct: float = Field(
        default=10.0, ge=0.0, le=100.0,
        description="Minimum aggregate SOC limit (operational floor) as percentage",
    )
    max_soc_limit_pct: float = Field(
        default=90.0, ge=0.0, le=100.0,
        description="Maximum aggregate SOC limit (operational ceiling) as percentage",
    )
    timestamp: datetime = Field(
        default_factory=_utc_now, description="When this SOC snapshot was taken"
    )


class DeviceClassMix(BaseModel):
    """Aggregate composition of device types in a DER portfolio.

    Provides the share of each device class without exposing individual
    device counts, identifiers, or locations. Enables the DSO to understand
    the nature of available flexibility without compromising device-level
    data sovereignty.
    """

    der_type: DERType = Field(..., description="Type of DER device class")
    share_pct: float = Field(
        ..., ge=0.0, le=100.0,
        description="Percentage share of total portfolio capacity",
    )
    aggregate_capacity_kw: float = Field(
        ..., ge=0, description="Combined rated capacity of this device class in kW"
    )


class ResponseConfidence(BaseModel):
    """Confidence assessment for flexibility delivery.

    Expresses the aggregator's confidence that it can deliver the offered
    flexibility, considering device availability, forecast uncertainty,
    and historical performance.
    """

    level: ConfidenceLevel = Field(
        ..., description="Qualitative confidence level"
    )
    probability_pct: float = Field(
        ..., ge=0.0, le=100.0,
        description="Estimated probability of full delivery as percentage",
    )
    historical_delivery_rate_pct: Optional[float] = Field(
        default=None, ge=0.0, le=100.0,
        description="Historical delivery rate for similar flexibility offers",
    )


class AvailabilityWindow(BaseModel):
    """Time window when DER flexibility is available.

    Defines a period during which the aggregator can provide a specific
    power range and ramp capability. Multiple windows can be combined
    to express a full availability schedule.
    """

    window_id: str = Field(..., description="Unique identifier for this availability window")
    available_from: datetime = Field(
        ..., description="Start of availability period"
    )
    available_until: datetime = Field(
        ..., description="End of availability period"
    )
    pq_range: PQRange = Field(
        ..., description="Active and reactive power range during this window"
    )
    ramp_up_rate_kw_per_min: float = Field(
        ..., ge=0, description="Maximum ramp-up rate in kW per minute"
    )
    ramp_down_rate_kw_per_min: float = Field(
        ..., ge=0, description="Maximum ramp-down rate in kW per minute"
    )
    min_duration_minutes: float = Field(
        ..., ge=0, description="Minimum activation duration in minutes"
    )
    max_duration_minutes: float = Field(
        ..., ge=0, description="Maximum activation duration in minutes"
    )


class DERUnit(BaseModel):
    """IEC 61850-inspired aggregate DER unit representation.

    Represents a logical grouping of DER devices managed by an aggregator.
    Exposes aggregate capabilities and constraints without revealing
    individual device states. Maps conceptually to IEC 61850 ZDER/ZBAT/ZGEN
    logical nodes but at the portfolio level.
    """

    unit_id: str = Field(..., description="Unique identifier for this DER unit")
    name: str = Field(..., description="Human-readable name for the unit")
    aggregator_id: str = Field(
        ..., description="Identifier of the managing aggregator"
    )
    feeder_id: str = Field(
        ..., description="Distribution feeder this unit connects to"
    )
    device_class_mix: list[DeviceClassMix] = Field(
        default_factory=list,
        description="Composition of device types in this unit",
    )
    total_rated_capacity_kw: float = Field(
        ..., ge=0, description="Total rated active power capacity in kW"
    )
    current_output_kw: float = Field(
        ..., description="Current aggregate active power output in kW"
    )
    current_output_kvar: float = Field(
        default=0.0, description="Current aggregate reactive power output in kVAr"
    )
    state_of_charge: Optional[StateOfCharge] = Field(
        default=None,
        description="Aggregate SOC for storage assets (None if no storage)",
    )
    is_available: bool = Field(
        default=True, description="Whether this unit is currently available for dispatch"
    )
    valid_from: datetime = Field(..., description="Start of validity window")
    valid_until: datetime = Field(..., description="End of validity window")
    sensitivity: SensitivityTier = Field(
        default=SensitivityTier.MEDIUM,
        description="Data sensitivity classification",
    )
    updated_at: datetime = Field(
        default_factory=_utc_now, description="Timestamp of last update"
    )


class FlexibilityEnvelope(BaseModel):
    """Aggregate flexibility envelope F(t) for a DER portfolio.

    The central model for DER-DSO coordination. Represents the total
    flexibility an aggregator can offer across its portfolio within a
    time window, without exposing individual device states. Published
    via the federated catalog and accessible only through active contracts
    with appropriate purpose tags (e.g., 'congestion_management').
    """

    envelope_id: str = Field(..., description="Unique envelope identifier")
    unit_id: str = Field(
        ..., description="DER unit this envelope applies to"
    )
    aggregator_id: str = Field(
        ..., description="Identifier of the offering aggregator"
    )
    feeder_id: str = Field(
        ..., description="Distribution feeder for grid location context"
    )
    direction: FlexibilityDirection = Field(
        ..., description="Direction of offered flexibility"
    )
    pq_range: PQRange = Field(
        ..., description="Active and reactive power operating range"
    )
    availability_windows: list[AvailabilityWindow] = Field(
        default_factory=list,
        description="Time windows when this flexibility is available",
    )
    state_of_charge: Optional[StateOfCharge] = Field(
        default=None,
        description="Aggregate SOC snapshot (for storage-backed flexibility)",
    )
    response_confidence: ResponseConfidence = Field(
        ..., description="Confidence in delivering the offered flexibility"
    )
    device_class_mix: list[DeviceClassMix] = Field(
        default_factory=list,
        description="Aggregate device composition (no individual device data)",
    )
    price_eur_per_kwh: Optional[float] = Field(
        default=None, ge=0,
        description="Indicative price for flexibility activation in EUR/kWh",
    )
    valid_from: datetime = Field(..., description="Start of validity window")
    valid_until: datetime = Field(..., description="End of validity window")
    sensitivity: SensitivityTier = Field(
        default=SensitivityTier.MEDIUM,
        description="Data sensitivity classification",
    )
    updated_at: datetime = Field(
        default_factory=_utc_now, description="Timestamp of last update"
    )
