"""CIM-based grid topology semantic models for the Federated Data Space.

Models aligned to IEC 61970 Common Information Model (CIM) for power system
topology. Every exchanged data type has an explicit sensitivity tier, timestamps,
and validity windows to support contract-gated data sharing.

Data Sensitivity Classification (from spec):
  - Grid topology, protection settings: HIGH (operators + contractors only)
  - Feeder congestion signals: MEDIUM (contract-gated, authorized aggregators)
  - Hosting capacity: MEDIUM (contract-gated, aggregated by feeder)
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Optional


def _utc_now() -> datetime:
    """Return the current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)

from pydantic import BaseModel, Field


class SensitivityTier(str, Enum):
    """Data sensitivity classification for access control and policy enforcement.

    Every data asset in the federated data space is tagged with a sensitivity tier
    that determines who can access it and under what contract terms.
    """

    HIGH = "high"  # Grid topology, protection settings
    MEDIUM = "medium"  # Flexibility envelopes, congestion signals
    HIGH_PRIVACY = "high_privacy"  # Smart meter, BEMS data


class SwitchState(str, Enum):
    """Operational state of a switch in the grid topology."""

    OPEN = "open"
    CLOSED = "closed"
    UNKNOWN = "unknown"


class NodeType(str, Enum):
    """Classification of grid topology nodes."""

    SUBSTATION = "substation"
    DISTRIBUTION_TRANSFORMER = "distribution_transformer"
    JUNCTION = "junction"
    LOAD_POINT = "load_point"
    GENERATION_POINT = "generation_point"
    MEASUREMENT_POINT = "measurement_point"


class FeederConstraint(BaseModel):
    """CIM-based feeder constraint - shared by DSO to authorized participants.

    Represents operational limits on a distribution feeder. Published via the
    federated catalog and accessible only through active contracts with
    appropriate purpose tags (e.g., 'congestion_management').
    """

    feeder_id: str = Field(..., description="Unique identifier of the feeder")
    max_active_power_kw: float = Field(
        ..., ge=0, description="Maximum allowed active power flow in kW"
    )
    min_voltage_pu: float = Field(
        ..., ge=0, le=2.0, description="Minimum voltage in per-unit"
    )
    max_voltage_pu: float = Field(
        ..., ge=0, le=2.0, description="Maximum voltage in per-unit"
    )
    congestion_level: float = Field(
        ..., ge=0.0, le=1.0, description="Current congestion level (0.0=free, 1.0=fully congested)"
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


class CongestionSignal(BaseModel):
    """Real-time congestion signal for a feeder or grid segment.

    Published by the DSO when congestion levels change. Aggregators subscribe
    to these signals to adjust flexibility offers. Shared via Kafka
    'congestion-alerts' topic or REST API.
    """

    signal_id: str = Field(..., description="Unique signal identifier")
    feeder_id: str = Field(..., description="Feeder this signal applies to")
    congestion_level: float = Field(
        ..., ge=0.0, le=1.0, description="Current congestion level"
    )
    max_available_capacity_kw: float = Field(
        ..., ge=0, description="Remaining capacity before congestion threshold"
    )
    direction: str = Field(
        default="both",
        description="Congestion direction: 'import', 'export', or 'both'",
    )
    timestamp: datetime = Field(
        default_factory=_utc_now, description="When this signal was generated"
    )
    valid_from: datetime = Field(..., description="Start of validity window")
    valid_until: datetime = Field(..., description="End of validity window")
    sensitivity: SensitivityTier = Field(
        default=SensitivityTier.MEDIUM,
        description="Data sensitivity classification",
    )


class HostingCapacity(BaseModel):
    """Available hosting capacity at a grid node or feeder.

    Indicates how much additional generation or load can be connected.
    Shared by the DSO in aggregated form (per feeder) to authorized
    participants for DER planning.
    """

    node_id: str = Field(..., description="Grid node or feeder identifier")
    feeder_id: str = Field(..., description="Parent feeder identifier")
    max_generation_kw: float = Field(
        ..., ge=0, description="Maximum additional generation capacity in kW"
    )
    max_load_kw: float = Field(
        ..., ge=0, description="Maximum additional load capacity in kW"
    )
    current_generation_kw: float = Field(
        ..., ge=0, description="Current connected generation in kW"
    )
    current_load_kw: float = Field(
        ..., ge=0, description="Current connected load in kW"
    )
    voltage_headroom_pu: float = Field(
        ..., ge=0, description="Remaining voltage headroom in per-unit"
    )
    thermal_headroom_pct: float = Field(
        ..., ge=0, le=100.0, description="Remaining thermal capacity as percentage"
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


class GridNode(BaseModel):
    """CIM-based grid topology node.

    Represents a point in the distribution network topology. HIGH sensitivity
    because grid topology is restricted to operators and maintenance contractors.
    """

    node_id: str = Field(..., description="Unique node identifier")
    name: str = Field(..., description="Human-readable node name")
    node_type: NodeType = Field(..., description="Classification of this node")
    feeder_id: str = Field(..., description="Parent feeder this node belongs to")
    voltage_level_kv: float = Field(
        ..., gt=0, description="Nominal voltage level in kV"
    )
    latitude: Optional[float] = Field(
        default=None, ge=-90, le=90, description="Geographic latitude"
    )
    longitude: Optional[float] = Field(
        default=None, ge=-180, le=180, description="Geographic longitude"
    )
    is_energized: bool = Field(default=True, description="Whether the node is currently energized")
    valid_from: datetime = Field(..., description="Start of validity window")
    valid_until: datetime = Field(..., description="End of validity window")
    sensitivity: SensitivityTier = Field(
        default=SensitivityTier.HIGH,
        description="Data sensitivity classification",
    )
    updated_at: datetime = Field(
        default_factory=_utc_now, description="Timestamp of last update"
    )


class Feeder(BaseModel):
    """CIM-based distribution feeder model.

    Represents a distribution feeder in the grid topology. HIGH sensitivity
    because feeder configuration is restricted topology data.
    """

    feeder_id: str = Field(..., description="Unique feeder identifier")
    name: str = Field(..., description="Human-readable feeder name")
    substation_id: str = Field(..., description="Source substation identifier")
    voltage_level_kv: float = Field(
        ..., gt=0, description="Nominal voltage level in kV"
    )
    max_rated_power_kw: float = Field(
        ..., ge=0, description="Maximum rated power capacity in kW"
    )
    node_ids: list[str] = Field(
        default_factory=list, description="Ordered list of node IDs on this feeder"
    )
    switch_ids: list[str] = Field(
        default_factory=list, description="Switch IDs along this feeder"
    )
    valid_from: datetime = Field(..., description="Start of validity window")
    valid_until: datetime = Field(..., description="End of validity window")
    sensitivity: SensitivityTier = Field(
        default=SensitivityTier.HIGH,
        description="Data sensitivity classification",
    )
    updated_at: datetime = Field(
        default_factory=_utc_now, description="Timestamp of last update"
    )


class Switch(BaseModel):
    """CIM-based switch model for grid topology.

    Represents a switching device (breaker, disconnector, recloser) in the
    distribution network. HIGH sensitivity because protection settings and
    switch states are restricted to operators.
    """

    switch_id: str = Field(..., description="Unique switch identifier")
    name: str = Field(..., description="Human-readable switch name")
    feeder_id: str = Field(..., description="Parent feeder identifier")
    from_node_id: str = Field(..., description="Node ID on the source side")
    to_node_id: str = Field(..., description="Node ID on the load side")
    state: SwitchState = Field(
        default=SwitchState.CLOSED, description="Current operational state"
    )
    is_automatic: bool = Field(
        default=False, description="Whether the switch has automatic reclosing"
    )
    rated_current_a: float = Field(
        ..., gt=0, description="Rated current capacity in amperes"
    )
    valid_from: datetime = Field(..., description="Start of validity window")
    valid_until: datetime = Field(..., description="End of validity window")
    sensitivity: SensitivityTier = Field(
        default=SensitivityTier.HIGH,
        description="Data sensitivity classification",
    )
    updated_at: datetime = Field(
        default_factory=_utc_now, description="Timestamp of last update"
    )
