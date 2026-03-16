"""Core data models for the Federated Data Space Connector.

These models define the shared vocabulary for participant identity, data asset
metadata, contract negotiation, policy rules, and audit entries. Every
participant node imports this module to ensure consistent data exchange.

Key design decisions:
  - Contracts are machine-enforceable with purpose constraints, redistribution
    limits, retention limits, and anonymization requirements (spec Pattern 3).
  - Audit entries are immutable records with SHA-256 hashes of both request and
    response content for tamper evidence (spec Pattern 4).
  - Policy rules bind sensitivity tiers to allowed roles and operations.
  - All timestamps are timezone-aware UTC.
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


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class ContractStatus(str, Enum):
    """State machine for data usage contract lifecycle.

    Valid transitions:
      OFFERED -> NEGOTIATING -> ACTIVE -> EXPIRED
      OFFERED -> NEGOTIATING -> ACTIVE -> REVOKED
      OFFERED -> REJECTED
      NEGOTIATING -> REJECTED
    """

    OFFERED = "offered"
    NEGOTIATING = "negotiating"
    ACTIVE = "active"
    EXPIRED = "expired"
    REVOKED = "revoked"
    REJECTED = "rejected"


class AuditAction(str, Enum):
    """Classification of actions recorded in the audit trail."""

    READ = "read"
    WRITE = "write"
    DISPATCH = "dispatch"
    SUBSCRIBE = "subscribe"


class AuditOutcome(str, Enum):
    """Outcome of an audited data exchange."""

    SUCCESS = "success"
    DENIED = "denied"
    ERROR = "error"


class PolicyEffect(str, Enum):
    """Whether a policy rule allows or denies the matched action."""

    ALLOW = "allow"
    DENY = "deny"


# ---------------------------------------------------------------------------
# Core models
# ---------------------------------------------------------------------------


class Participant(BaseModel):
    """Identity of a participant in the federated data space.

    Each participant (DSO, Aggregator, Prosumer) is identified by a unique ID,
    organizational affiliation, assigned roles, and an mTLS certificate
    distinguished name for service-to-service trust.
    """

    id: str = Field(..., description="Unique participant identifier")
    name: str = Field(..., description="Human-readable participant name")
    organization: str = Field(
        ..., description="Organization the participant belongs to"
    )
    roles: list[str] = Field(
        default_factory=list,
        description="Assigned roles (e.g., 'dso_operator', 'aggregator', 'prosumer')",
    )
    certificate_dn: Optional[str] = Field(
        default=None,
        description="Distinguished name from the participant's mTLS certificate",
    )
    registered_at: datetime = Field(
        default_factory=_utc_now,
        description="Timestamp when the participant was registered",
    )


class DataAsset(BaseModel):
    """Metadata for a data asset published to the federated catalog.

    Participants register their data assets so that others can discover and
    negotiate access contracts. The catalog stores only metadata; actual data
    remains under the provider's local control.
    """

    id: str = Field(..., description="Unique asset identifier")
    provider_id: str = Field(
        ..., description="Participant ID of the data provider"
    )
    name: str = Field(..., description="Human-readable asset name")
    description: str = Field(
        default="", description="Detailed description of the data asset"
    )
    data_type: str = Field(
        ...,
        description="Semantic type of the data (e.g., 'feeder_constraint', 'flexibility_envelope')",
    )
    sensitivity: SensitivityTier = Field(
        ..., description="Data sensitivity classification"
    )
    endpoint: str = Field(
        ...,
        description="API endpoint URL where the data can be accessed (contract-gated)",
    )
    update_frequency: Optional[str] = Field(
        default=None,
        description="How often the data is updated (e.g., '5m', '1h', 'on_change')",
    )
    policy_metadata: dict[str, str] = Field(
        default_factory=dict,
        description="Policy hints for contract negotiation (e.g., allowed purposes, min retention)",
    )
    registered_at: datetime = Field(
        default_factory=_utc_now,
        description="Timestamp when the asset was registered in the catalog",
    )
    updated_at: datetime = Field(
        default_factory=_utc_now,
        description="Timestamp of last metadata update",
    )


class ContractOffer(BaseModel):
    """Proposal to establish a data usage contract between provider and consumer.

    A contract offer is the first step in negotiation. The consumer proposes
    terms (purpose, operations, retention) and the provider can accept, reject,
    or counter-propose.
    """

    offer_id: str = Field(..., description="Unique offer identifier")
    provider_id: str = Field(
        ..., description="Participant ID of the data provider"
    )
    consumer_id: str = Field(
        ..., description="Participant ID of the data consumer"
    )
    asset_id: str = Field(
        ..., description="ID of the data asset this offer targets"
    )
    purpose: str = Field(
        ...,
        description="Intended purpose for data usage (e.g., 'congestion_management')",
    )
    allowed_operations: list[str] = Field(
        default_factory=list,
        description="Requested operations (e.g., ['read', 'aggregate'])",
    )
    redistribution_allowed: bool = Field(
        default=False,
        description="Whether the consumer may redistribute the data to third parties",
    )
    retention_days: int = Field(
        ..., ge=1, description="Maximum number of days the consumer may retain the data"
    )
    anonymization_required: bool = Field(
        default=False,
        description="Whether the data must be anonymized before use",
    )
    emergency_override: bool = Field(
        default=False,
        description="Whether DSO emergency access is included in the contract",
    )
    valid_from: datetime = Field(
        ..., description="Proposed start of the contract validity window"
    )
    valid_until: datetime = Field(
        ..., description="Proposed end of the contract validity window"
    )
    created_at: datetime = Field(
        default_factory=_utc_now,
        description="Timestamp when the offer was created",
    )


class DataUsageContract(BaseModel):
    """Machine-enforceable data usage contract between a provider and consumer.

    No data is ever exchanged without a valid, active contract. The connector
    intercepts every request and checks the contract status, purpose constraints,
    and operational limits before forwarding to the participant's local API.

    Fields follow spec Pattern 3 (Contract-Gated Data Access).
    """

    contract_id: str = Field(..., description="Unique contract identifier")
    provider_id: str = Field(
        ..., description="Participant ID of the data provider"
    )
    consumer_id: str = Field(
        ..., description="Participant ID of the data consumer"
    )
    asset_id: str = Field(
        ..., description="ID of the data asset this contract covers"
    )
    purpose: str = Field(
        ...,
        description="Allowed purpose for data usage (e.g., 'congestion_management')",
    )
    allowed_operations: list[str] = Field(
        ...,
        description="Permitted operations (e.g., ['read', 'aggregate'])",
    )
    redistribution_allowed: bool = Field(
        default=False,
        description="Whether the consumer may redistribute the data to third parties",
    )
    retention_days: int = Field(
        ..., ge=1, description="Maximum number of days the consumer may retain the data"
    )
    anonymization_required: bool = Field(
        default=False,
        description="Whether the data must be anonymized before use",
    )
    emergency_override: bool = Field(
        default=False,
        description="Whether DSO emergency access is permitted under this contract",
    )
    status: ContractStatus = Field(
        default=ContractStatus.OFFERED,
        description="Current lifecycle state of the contract",
    )
    valid_from: datetime = Field(
        ..., description="Start of the contract validity window"
    )
    valid_until: datetime = Field(
        ..., description="End of the contract validity window"
    )
    created_at: datetime = Field(
        default_factory=_utc_now,
        description="Timestamp when the contract was created",
    )
    updated_at: datetime = Field(
        default_factory=_utc_now,
        description="Timestamp of last status change",
    )


class PolicyRule(BaseModel):
    """A single policy rule for data access control.

    Policy rules are evaluated by the PolicyEngine to determine whether a
    request is allowed or denied. Rules match on sensitivity tier, requester
    roles, allowed operations, and purpose constraints.
    """

    rule_id: str = Field(..., description="Unique rule identifier")
    asset_id: Optional[str] = Field(
        default=None,
        description="Specific asset this rule applies to (None = all assets)",
    )
    sensitivity: Optional[SensitivityTier] = Field(
        default=None,
        description="Sensitivity tier this rule applies to (None = all tiers)",
    )
    allowed_roles: list[str] = Field(
        default_factory=list,
        description="Roles permitted by this rule (empty = no role restriction)",
    )
    allowed_operations: list[str] = Field(
        default_factory=list,
        description="Operations permitted by this rule (empty = no operation restriction)",
    )
    allowed_purposes: list[str] = Field(
        default_factory=list,
        description="Purposes permitted by this rule (empty = no purpose restriction)",
    )
    effect: PolicyEffect = Field(
        default=PolicyEffect.ALLOW,
        description="Whether this rule allows or denies the matched action",
    )
    priority: int = Field(
        default=0,
        ge=0,
        description="Rule priority; higher values are evaluated first",
    )
    created_at: datetime = Field(
        default_factory=_utc_now,
        description="Timestamp when the rule was created",
    )


class AuditEntry(BaseModel):
    """Immutable audit record for a data exchange in the federated data space.

    Every data exchange generates an audit entry with SHA-256 hashes of both
    request and response content for tamper evidence. Audit entries are
    append-only and form the complete provenance trail.

    Fields follow spec Pattern 4 (Audit Trail on Every Exchange).
    """

    timestamp: datetime = Field(
        default_factory=_utc_now,
        description="UTC timestamp when the exchange occurred",
    )
    requester_id: str = Field(
        ..., description="Participant ID of the data requester"
    )
    provider_id: str = Field(
        ..., description="Participant ID of the data provider"
    )
    asset_id: str = Field(
        ..., description="ID of the data asset that was accessed"
    )
    purpose_tag: str = Field(
        ...,
        description="Purpose tag from the contract (must match allowed purpose)",
    )
    request_hash: str = Field(
        ..., description="SHA-256 hash of the request body for tamper evidence"
    )
    response_hash: str = Field(
        ..., description="SHA-256 hash of the response body for tamper evidence"
    )
    contract_id: str = Field(
        ..., description="ID of the contract authorizing this exchange"
    )
    action: AuditAction = Field(
        ..., description="Type of action performed"
    )
    outcome: AuditOutcome = Field(
        ..., description="Outcome of the exchange"
    )
