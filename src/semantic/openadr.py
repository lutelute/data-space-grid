"""OpenADR-style demand response event models for the Federated Data Space.

Models aligned to OpenADR 2.0b concepts for demand response (DR) event
signaling, dispatch, and baseline reporting. Every exchanged data type has
an explicit sensitivity tier, timestamps, and validity windows to support
contract-gated data sharing.

Data Sensitivity Classification (from spec):
  - DR event notifications: MEDIUM (subscribed participants)
  - Dispatch commands: MEDIUM (contract-gated, DSO -> Aggregator)
  - Dispatch actuals: MEDIUM (contract-gated, Aggregator -> DSO)
  - Baselines: MEDIUM (contract-gated, used for settlement verification)
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


class EventStatus(str, Enum):
    """Lifecycle status of a demand response event.

    Follows the OpenADR event lifecycle: events are created as PENDING,
    transition to ACTIVE during the event window, and move to COMPLETED
    or CANCELLED upon conclusion.
    """

    PENDING = "pending"  # Event created, not yet active
    ACTIVE = "active"  # Event currently in effect
    COMPLETED = "completed"  # Event concluded normally
    CANCELLED = "cancelled"  # Event cancelled before or during activation
    SUPERSEDED = "superseded"  # Replaced by a newer event


class SignalType(str, Enum):
    """Type of demand response signal.

    Classifies the nature of the DR signal, determining how participants
    should interpret and respond to the signal value.
    """

    LEVEL = "level"  # Discrete level (e.g., normal/moderate/high)
    PRICE = "price"  # Price signal in currency per kWh
    LOAD_DISPATCH = "load_dispatch"  # Direct load control target in kW
    LOAD_CONTROL = "load_control"  # Percentage adjustment from baseline
    SIMPLE = "simple"  # Simple event signal (0=normal, 1=event active)


class DRSignal(BaseModel):
    """OpenADR-style demand response signal.

    Represents a single signal within a DR event. A DR event may contain
    multiple signals (e.g., a price signal and a load-dispatch signal).
    Signals define the specific action or incentive for participants.
    """

    signal_id: str = Field(..., description="Unique signal identifier")
    signal_type: SignalType = Field(
        ..., description="Type of DR signal"
    )
    signal_name: str = Field(
        ..., description="Human-readable signal name (e.g., 'ELECTRICITY_PRICE')"
    )
    current_value: float = Field(
        ..., description="Current signal value (interpretation depends on signal_type)"
    )
    target_kw: Optional[float] = Field(
        default=None,
        description="Target power level in kW (for LOAD_DISPATCH signals)",
    )
    duration_minutes: float = Field(
        ..., ge=0, description="Duration of this signal interval in minutes"
    )
    valid_from: datetime = Field(..., description="Start of this signal interval")
    valid_until: datetime = Field(..., description="End of this signal interval")


class DREvent(BaseModel):
    """OpenADR-style demand response event.

    Represents a complete DR event issued by the DSO or market operator.
    Events contain one or more signals and are distributed via the Kafka
    'dr-events' topic to subscribed participants. Access requires an active
    contract with appropriate purpose tags.
    """

    event_id: str = Field(..., description="Unique event identifier")
    program_id: str = Field(
        ..., description="DR program this event belongs to"
    )
    issuer_id: str = Field(
        ..., description="Identifier of the event issuer (e.g., DSO participant ID)"
    )
    target_participant_ids: list[str] = Field(
        default_factory=list,
        description="List of targeted participant IDs (empty = all subscribed)",
    )
    status: EventStatus = Field(
        default=EventStatus.PENDING, description="Current event lifecycle status"
    )
    priority: int = Field(
        default=1, ge=0, le=3,
        description="Event priority (0=lowest, 3=emergency)",
    )
    signals: list[DRSignal] = Field(
        default_factory=list,
        description="DR signals associated with this event",
    )
    feeder_id: Optional[str] = Field(
        default=None,
        description="Target feeder ID for location-specific events",
    )
    event_start: datetime = Field(
        ..., description="Scheduled start time of the event"
    )
    event_end: datetime = Field(
        ..., description="Scheduled end time of the event"
    )
    notification_time: datetime = Field(
        default_factory=_utc_now,
        description="When participants were notified of this event",
    )
    ramp_up_minutes: float = Field(
        default=0.0, ge=0,
        description="Required ramp-up time before event start in minutes",
    )
    recovery_minutes: float = Field(
        default=0.0, ge=0,
        description="Recovery time after event end in minutes",
    )
    is_emergency: bool = Field(
        default=False,
        description="Whether this is an emergency event (bypasses normal constraints)",
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


class Baseline(BaseModel):
    """Baseline consumption or generation profile for DR settlement.

    Represents the expected power consumption or generation pattern in the
    absence of a DR event. Used to calculate the actual demand response
    delivered by comparing actuals against the baseline. Shared between
    DSO and Aggregator under active contract for settlement verification.
    """

    baseline_id: str = Field(..., description="Unique baseline identifier")
    event_id: str = Field(
        ..., description="DR event this baseline applies to"
    )
    participant_id: str = Field(
        ..., description="Participant this baseline belongs to"
    )
    feeder_id: Optional[str] = Field(
        default=None, description="Feeder for location-specific baselines"
    )
    methodology: str = Field(
        ...,
        description="Baseline calculation methodology (e.g., 'avg_10_of_10', 'regression')",
    )
    interval_minutes: float = Field(
        default=15.0, ge=1.0,
        description="Time resolution of baseline values in minutes",
    )
    values_kw: list[float] = Field(
        ..., description="Ordered list of baseline power values in kW per interval"
    )
    baseline_start: datetime = Field(
        ..., description="Start time of the baseline period"
    )
    baseline_end: datetime = Field(
        ..., description="End time of the baseline period"
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


class DispatchCommand(BaseModel):
    """Real-time dispatch command from DSO to Aggregator.

    Issued via the Kafka 'dispatch-commands' topic when the DSO needs
    an aggregator to activate flexibility. Contains the specific power
    target and timing requirements. Must reference an active contract
    and will generate an audit entry.
    """

    command_id: str = Field(..., description="Unique command identifier")
    event_id: str = Field(
        ..., description="DR event this dispatch is part of"
    )
    issuer_id: str = Field(
        ..., description="Identifier of the command issuer (DSO)"
    )
    target_participant_id: str = Field(
        ..., description="Target aggregator participant ID"
    )
    contract_id: str = Field(
        ..., description="Active contract authorizing this dispatch"
    )
    feeder_id: str = Field(
        ..., description="Target feeder for this dispatch"
    )
    target_power_kw: float = Field(
        ..., description="Requested power adjustment in kW (positive=reduce, negative=increase)"
    )
    target_reactive_kvar: Optional[float] = Field(
        default=None,
        description="Requested reactive power adjustment in kVAr",
    )
    activation_time: datetime = Field(
        ..., description="When the dispatch should be activated"
    )
    duration_minutes: float = Field(
        ..., ge=0, description="Requested duration of dispatch in minutes"
    )
    ramp_rate_kw_per_min: Optional[float] = Field(
        default=None, ge=0,
        description="Maximum ramp rate for the dispatch in kW per minute",
    )
    is_emergency: bool = Field(
        default=False,
        description="Whether this is an emergency dispatch (bypasses normal constraints)",
    )
    issued_at: datetime = Field(
        default_factory=_utc_now, description="When this command was issued"
    )
    sensitivity: SensitivityTier = Field(
        default=SensitivityTier.MEDIUM,
        description="Data sensitivity classification",
    )


class DispatchActual(BaseModel):
    """Actual dispatch response reported by Aggregator back to DSO.

    Sent via the Kafka 'dispatch-actuals' topic after a dispatch command
    has been executed. Reports what was actually delivered versus what was
    commanded, enabling settlement and performance tracking.
    """

    actual_id: str = Field(..., description="Unique identifier for this actual report")
    command_id: str = Field(
        ..., description="Dispatch command this actual responds to"
    )
    event_id: str = Field(
        ..., description="DR event this actual is part of"
    )
    participant_id: str = Field(
        ..., description="Aggregator that executed the dispatch"
    )
    feeder_id: str = Field(
        ..., description="Feeder where the dispatch was executed"
    )
    commanded_kw: float = Field(
        ..., description="Power adjustment that was commanded in kW"
    )
    delivered_kw: float = Field(
        ..., description="Power adjustment actually delivered in kW"
    )
    delivered_kvar: Optional[float] = Field(
        default=None,
        description="Reactive power adjustment actually delivered in kVAr",
    )
    delivery_start: datetime = Field(
        ..., description="When the actual delivery started"
    )
    delivery_end: datetime = Field(
        ..., description="When the actual delivery ended"
    )
    delivery_accuracy_pct: float = Field(
        ..., ge=0.0, le=100.0,
        description="Percentage of commanded power that was delivered",
    )
    interval_values_kw: list[float] = Field(
        default_factory=list,
        description="Time-series of actual power values per interval in kW",
    )
    interval_minutes: float = Field(
        default=5.0, ge=1.0,
        description="Time resolution of interval values in minutes",
    )
    reported_at: datetime = Field(
        default_factory=_utc_now, description="When this actual was reported"
    )
    sensitivity: SensitivityTier = Field(
        default=SensitivityTier.MEDIUM,
        description="Data sensitivity classification",
    )
