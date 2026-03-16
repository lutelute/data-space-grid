"""Consumer data semantic models for the Federated Data Space.

Models for prosumer/consumer data management including meter readings,
demand profiles, anonymized load series, and consent records. Implements
purpose-based minimum-disclosure principles: consumer data is never shared
raw — the disclosure level is determined by purpose, not by requester.

Data Sensitivity Classification (from spec):
  - Smart meter readings: HIGH_PRIVACY (consent-required, prosumer-only or identified)
  - Building/BEMS data: HIGH_PRIVACY (consent-required, k-anonymized minimum)
  - Anonymized load profiles: MEDIUM (contract-gated, pre-anonymized)
  - Consent records: HIGH_PRIVACY (only visible to the consent holder)

Key Design Principle:
  Default is maximum restriction; only explicit consent widens access.
  Disclosure level is determined by purpose (PURPOSE_DISCLOSURE_MAP),
  and the prosumer node enforces this locally before any data leaves.
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


class DisclosureLevel(str, Enum):
    """Minimum disclosure level for consumer data sharing.

    Determines how consumer data is transformed before leaving the
    prosumer node. Each purpose maps to a specific disclosure level
    via PURPOSE_DISCLOSURE_MAP. The prosumer node enforces this
    transformation locally before any data is shared.
    """

    RAW = "raw"  # Only for consent-holder themselves
    IDENTIFIED_CONSENTED = "identified"  # Contract use with explicit consent
    ANONYMIZED = "anonymized"  # k-anonymized, no individual identification
    AGGREGATED = "aggregated"  # Statistical aggregates only
    CONTROLLABILITY_ONLY = "controllability"  # Only controllable margin, nothing else


PURPOSE_DISCLOSURE_MAP: dict[str, DisclosureLevel] = {
    "research": DisclosureLevel.AGGREGATED,
    "dr_dispatch": DisclosureLevel.CONTROLLABILITY_ONLY,
    "billing": DisclosureLevel.IDENTIFIED_CONSENTED,
    "grid_analysis": DisclosureLevel.AGGREGATED,
    "forecasting": DisclosureLevel.ANONYMIZED,
}
"""Mapping from data usage purpose to the maximum disclosure level allowed.

This map is enforced by the prosumer node's anonymizer before any consumer
data leaves the local data store. If a purpose is not in this map, the
request is denied (fail-closed).
"""


class ConsentStatus(str, Enum):
    """Status of a consumer consent record."""

    ACTIVE = "active"  # Consent is currently valid
    REVOKED = "revoked"  # Consent has been revoked by the consumer
    EXPIRED = "expired"  # Consent has passed its validity window


class MeterReading(BaseModel):
    """Smart meter reading from a prosumer installation.

    Represents a single metered value or a batch of interval readings
    from a consumer's smart meter. HIGH_PRIVACY sensitivity because
    raw meter data can reveal occupancy patterns and personal behavior.
    Never shared outside the prosumer node without anonymization.
    """

    reading_id: str = Field(..., description="Unique reading identifier")
    meter_id: str = Field(..., description="Smart meter device identifier")
    prosumer_id: str = Field(
        ..., description="Identifier of the prosumer who owns this data"
    )
    active_power_kw: float = Field(
        ..., description="Active power reading in kW (positive=consumption, negative=export)"
    )
    reactive_power_kvar: Optional[float] = Field(
        default=None, description="Reactive power reading in kVAr"
    )
    voltage_v: Optional[float] = Field(
        default=None, ge=0, description="Voltage reading in volts"
    )
    cumulative_energy_kwh: Optional[float] = Field(
        default=None, ge=0,
        description="Cumulative energy consumption in kWh",
    )
    reading_timestamp: datetime = Field(
        ..., description="When this meter reading was taken"
    )
    interval_minutes: float = Field(
        default=15.0, ge=1.0,
        description="Metering interval duration in minutes",
    )
    quality_flag: str = Field(
        default="valid",
        description="Data quality flag (e.g., 'valid', 'estimated', 'missing')",
    )
    sensitivity: SensitivityTier = Field(
        default=SensitivityTier.HIGH_PRIVACY,
        description="Data sensitivity classification",
    )
    updated_at: datetime = Field(
        default_factory=_utc_now, description="Timestamp of last update"
    )


class DemandProfile(BaseModel):
    """Consumer demand profile representing typical consumption patterns.

    A time-series profile of expected or historical demand. Can be shared
    at different disclosure levels depending on the requesting purpose:
    raw (self only), identified (billing), anonymized (forecasting),
    aggregated (research/grid analysis), or controllability-only (DR dispatch).
    """

    profile_id: str = Field(..., description="Unique profile identifier")
    prosumer_id: str = Field(
        ..., description="Identifier of the prosumer this profile belongs to"
    )
    profile_type: str = Field(
        ...,
        description="Type of demand profile (e.g., 'historical', 'forecast', 'typical_day')",
    )
    interval_minutes: float = Field(
        default=15.0, ge=1.0,
        description="Time resolution of profile values in minutes",
    )
    values_kw: list[float] = Field(
        ..., description="Ordered list of demand values in kW per interval"
    )
    peak_demand_kw: float = Field(
        ..., description="Peak demand value within this profile in kW"
    )
    total_energy_kwh: float = Field(
        ..., ge=0, description="Total energy consumption over the profile period in kWh"
    )
    profile_start: datetime = Field(
        ..., description="Start time of the profile period"
    )
    profile_end: datetime = Field(
        ..., description="End time of the profile period"
    )
    disclosure_level: DisclosureLevel = Field(
        default=DisclosureLevel.RAW,
        description="Current disclosure level of this profile data",
    )
    valid_from: datetime = Field(..., description="Start of validity window")
    valid_until: datetime = Field(..., description="End of validity window")
    sensitivity: SensitivityTier = Field(
        default=SensitivityTier.HIGH_PRIVACY,
        description="Data sensitivity classification",
    )
    updated_at: datetime = Field(
        default_factory=_utc_now, description="Timestamp of last update"
    )


class AnonymizedLoadSeries(BaseModel):
    """Anonymized load time-series for sharing outside the prosumer node.

    Pre-anonymized aggregate load data suitable for contract-gated sharing.
    Individual prosumer identity cannot be recovered from this data.
    Created by the prosumer node's anonymizer before any data leaves
    the local data store.
    """

    series_id: str = Field(..., description="Unique series identifier")
    source_count: int = Field(
        ..., ge=1,
        description="Number of individual prosumers aggregated into this series",
    )
    feeder_id: Optional[str] = Field(
        default=None, description="Feeder for geographic aggregation context"
    )
    interval_minutes: float = Field(
        default=15.0, ge=1.0,
        description="Time resolution of series values in minutes",
    )
    values_kw: list[float] = Field(
        ..., description="Ordered list of anonymized load values in kW per interval"
    )
    mean_kw: float = Field(
        ..., description="Mean load across the series in kW"
    )
    std_dev_kw: float = Field(
        ..., ge=0, description="Standard deviation of load in kW"
    )
    peak_kw: float = Field(
        ..., description="Peak load value in the series in kW"
    )
    min_kw: float = Field(
        ..., description="Minimum load value in the series in kW"
    )
    k_anonymity_level: int = Field(
        ..., ge=2,
        description="k-anonymity level (minimum group size for anonymization)",
    )
    series_start: datetime = Field(
        ..., description="Start time of the series period"
    )
    series_end: datetime = Field(
        ..., description="End time of the series period"
    )
    disclosure_level: DisclosureLevel = Field(
        default=DisclosureLevel.ANONYMIZED,
        description="Disclosure level of this series data",
    )
    valid_from: datetime = Field(..., description="Start of validity window")
    valid_until: datetime = Field(..., description="End of validity window")
    sensitivity: SensitivityTier = Field(
        default=SensitivityTier.MEDIUM,
        description="Data sensitivity classification (MEDIUM because pre-anonymized)",
    )
    updated_at: datetime = Field(
        default_factory=_utc_now, description="Timestamp of last update"
    )


class ConsentRecord(BaseModel):
    """Consumer consent record for purpose-based data sharing.

    Records explicit consent from a prosumer to share their data for a
    specific purpose with a specific requester. The prosumer node checks
    consent records before any data leaves. Consent can be revoked at
    any time, and revocation takes effect immediately for subsequent
    requests (in-flight data is not affected).
    """

    consent_id: str = Field(..., description="Unique consent record identifier")
    prosumer_id: str = Field(
        ..., description="Identifier of the consenting prosumer"
    )
    requester_id: str = Field(
        ..., description="Identifier of the party granted access"
    )
    purpose: str = Field(
        ...,
        description="Data usage purpose (must match PURPOSE_DISCLOSURE_MAP key)",
    )
    allowed_data_types: list[str] = Field(
        ...,
        description="List of data types covered by this consent (e.g., 'meter_reading', 'demand_profile')",
    )
    disclosure_level: DisclosureLevel = Field(
        ...,
        description="Maximum disclosure level permitted under this consent",
    )
    status: ConsentStatus = Field(
        default=ConsentStatus.ACTIVE,
        description="Current consent status",
    )
    granted_at: datetime = Field(
        default_factory=_utc_now,
        description="When consent was granted",
    )
    revoked_at: Optional[datetime] = Field(
        default=None,
        description="When consent was revoked (None if still active)",
    )
    valid_from: datetime = Field(..., description="Start of consent validity window")
    valid_until: datetime = Field(..., description="End of consent validity window")
    sensitivity: SensitivityTier = Field(
        default=SensitivityTier.HIGH_PRIVACY,
        description="Data sensitivity classification",
    )
    updated_at: datetime = Field(
        default_factory=_utc_now, description="Timestamp of last update"
    )
