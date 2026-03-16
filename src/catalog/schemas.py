"""Pydantic schemas for the Federated Catalog API.

These schemas define the request/response models for asset registration,
discovery, and contract negotiation via the catalog service.  They are
intentionally separate from the core connector models so that the catalog
API surface can evolve independently while the underlying domain models
remain stable.

Key design decisions:
  - ``AssetRegistration`` accepts the minimum fields a provider must supply;
    the catalog assigns ``id``, ``created_at``, and ``updated_at``.
  - ``AssetResponse`` mirrors the full ``DataAsset`` model from connector-core
    so that consumers receive all metadata.
  - ``AssetSearchQuery`` supports filtering by provider, data type, and
    sensitivity tier for flexible discovery.
  - ``ContractInitiation`` maps closely to ``ContractOffer`` but is shaped
    for the REST API boundary (no ``offer_id`` – the catalog assigns one).
  - ``ContractResponse`` exposes the full ``DataUsageContract`` state.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field

from src.connector.models import ContractStatus
from src.semantic.cim import SensitivityTier


def _utc_now() -> datetime:
    """Return the current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Asset schemas
# ---------------------------------------------------------------------------


class AssetRegistration(BaseModel):
    """Request schema for registering a new data asset in the federated catalog.

    The provider supplies metadata about the asset; the catalog assigns the
    unique ``id`` and records the registration timestamp.
    """

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
    resolution: Optional[str] = Field(
        default=None,
        description="Data resolution (e.g., '15min', '1h', 'per_feeder')",
    )
    anonymized: bool = Field(
        default=False,
        description="Whether the data is pre-anonymized",
    )
    personal_data: bool = Field(
        default=False,
        description="Whether the asset contains personal data",
    )
    policy_metadata: dict[str, str] = Field(
        default_factory=dict,
        description="Policy hints for contract negotiation (e.g., allowed purposes, min retention)",
    )
    contract_template_id: Optional[str] = Field(
        default=None,
        description="ID of a predefined contract template for this asset",
    )


class AssetResponse(BaseModel):
    """Response schema for a data asset returned by the catalog.

    Contains the full metadata including catalog-assigned fields (``id``,
    ``created_at``, ``updated_at``).
    """

    id: str = Field(..., description="Unique asset identifier assigned by the catalog")
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
    resolution: Optional[str] = Field(
        default=None,
        description="Data resolution (e.g., '15min', '1h', 'per_feeder')",
    )
    anonymized: bool = Field(
        default=False,
        description="Whether the data is pre-anonymized",
    )
    personal_data: bool = Field(
        default=False,
        description="Whether the asset contains personal data",
    )
    policy_metadata: dict[str, str] = Field(
        default_factory=dict,
        description="Policy hints for contract negotiation",
    )
    contract_template_id: Optional[str] = Field(
        default=None,
        description="ID of a predefined contract template for this asset",
    )
    created_at: datetime = Field(
        ..., description="Timestamp when the asset was registered"
    )
    updated_at: datetime = Field(
        ..., description="Timestamp of last metadata update"
    )


class AssetSearchQuery(BaseModel):
    """Query parameters for searching/discovering assets in the catalog.

    All filters are optional; when omitted, the search returns all assets.
    Filters are combined with AND logic.
    """

    provider_id: Optional[str] = Field(
        default=None,
        description="Filter by data provider participant ID",
    )
    data_type: Optional[str] = Field(
        default=None,
        description="Filter by semantic data type (e.g., 'feeder_constraint')",
    )
    sensitivity: Optional[SensitivityTier] = Field(
        default=None,
        description="Filter by sensitivity tier",
    )
    name_contains: Optional[str] = Field(
        default=None,
        description="Case-insensitive substring match on asset name",
    )
    anonymized: Optional[bool] = Field(
        default=None,
        description="Filter by anonymization status",
    )
    personal_data: Optional[bool] = Field(
        default=None,
        description="Filter by personal data flag",
    )


# ---------------------------------------------------------------------------
# Contract schemas
# ---------------------------------------------------------------------------


class ContractInitiation(BaseModel):
    """Request schema for initiating a contract negotiation via the catalog.

    The consumer proposes contract terms; the catalog creates the contract
    in ``OFFERED`` state and assigns a ``contract_id``.
    """

    provider_id: str = Field(
        ..., description="Participant ID of the data provider"
    )
    consumer_id: str = Field(
        ..., description="Participant ID of the data consumer"
    )
    asset_id: str = Field(
        ..., description="ID of the data asset this contract targets"
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


class ContractResponse(BaseModel):
    """Response schema for a contract returned by the catalog.

    Contains the full contract state including catalog-assigned fields
    (``contract_id``, ``status``, timestamps).
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
        ..., description="Current lifecycle state of the contract"
    )
    valid_from: datetime = Field(
        ..., description="Start of the contract validity window"
    )
    valid_until: datetime = Field(
        ..., description="End of the contract validity window"
    )
    created_at: datetime = Field(
        ..., description="Timestamp when the contract was created"
    )
    updated_at: datetime = Field(
        ..., description="Timestamp of last status change"
    )
