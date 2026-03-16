"""Prosumer catalog publisher for registering data assets with the federated catalog.

On startup, the Prosumer node publishes metadata for its three primary data
assets (anonymized demand profiles, DR eligibility, controllable margin) to
the federated catalog so that other participants (DSO, aggregators) can
discover and negotiate contracts for access.

Key design decisions:
  - Uses :class:`~src.connector.catalog_client.CatalogClient` for all
    catalog interactions, inheriting retry logic and caching.
  - Asset definitions are hard-coded to the three Prosumer data types
    that can be shared externally: ``demand_profile`` (anonymized),
    ``dr_eligibility``, and ``controllable_margin``.
  - Each asset registration includes consent-aware policy metadata: the
    ``requires_consent`` flag and applicable disclosure levels so that
    consuming participants understand the consent requirements before
    negotiating contracts.
  - Raw meter readings are **never** registered in the catalog — they
    are HIGH_PRIVACY and only accessible locally by the prosumer.
  - The ``publish()`` method is idempotent at the application level:
    calling it multiple times creates new catalog entries (the catalog
    assigns unique IDs), but the Prosumer publisher tracks registered
    asset IDs to avoid re-registration within the same process lifetime.
  - The ``PROSUMER_PARTICIPANT_ID`` and ``PROSUMER_ENDPOINT_BASE`` are
    configurable via environment variables for deployment flexibility.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from src.catalog.schemas import AssetRegistration, AssetResponse
from src.connector.catalog_client import CatalogClient
from src.semantic.cim import SensitivityTier

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Default configuration
# ---------------------------------------------------------------------------

_DEFAULT_PROSUMER_PARTICIPANT_ID = "prosumer-campus-001"
_DEFAULT_PROSUMER_ENDPOINT_BASE = "http://localhost:8003"


# ---------------------------------------------------------------------------
# Asset definitions
# ---------------------------------------------------------------------------

def _prosumer_asset_definitions(
    participant_id: str, endpoint_base: str
) -> list[AssetRegistration]:
    """Return the list of Prosumer data asset registrations.

    Each asset corresponds to a data type the Prosumer publishes:
      - **Anonymized Demand Profiles**: Pre-anonymized consumption patterns
        shared at the disclosure level determined by the requesting purpose.
      - **DR Eligibility**: Demand response program eligibility per building,
        indicating which DR programs the prosumer can participate in.
      - **Controllable Margin**: Available load flexibility for DR dispatch,
        representing how much demand can be reduced or shifted.

    Note: Raw meter readings are **not** published to the catalog.  They are
    HIGH_PRIVACY and remain local to the prosumer node.

    Args:
        participant_id: The Prosumer's participant identifier.
        endpoint_base: Base URL of the Prosumer node's API.

    Returns:
        List of ``AssetRegistration`` objects ready for catalog submission.
    """
    return [
        AssetRegistration(
            provider_id=participant_id,
            name="Prosumer Anonymized Demand Profiles",
            description=(
                "Anonymized demand profiles for campus buildings. Data is "
                "transformed according to the requesting purpose's disclosure "
                "level (aggregated for research, anonymized for forecasting, "
                "controllability-only for DR dispatch). Requires active "
                "consent from the prosumer before data is released."
            ),
            data_type="demand_profile",
            sensitivity=SensitivityTier.MEDIUM,
            endpoint=f"{endpoint_base}/api/v1/demand-profile",
            update_frequency="1h",
            resolution="per_building",
            anonymized=True,
            personal_data=True,
            policy_metadata={
                "allowed_purposes": "research,forecasting,grid_analysis,dr_dispatch,billing",
                "min_retention_days": "1",
                "max_retention_days": "30",
                "requires_contract": "true",
                "requires_consent": "true",
                "disclosure_levels": "aggregated,anonymized,controllability,identified",
                "anonymization_method": "purpose_based_minimum_disclosure",
            },
        ),
        AssetRegistration(
            provider_id=participant_id,
            name="Prosumer DR Eligibility",
            description=(
                "Demand response program eligibility for campus buildings. "
                "Indicates which DR programs each building can participate in "
                "and the maximum demand reduction available. Updated when "
                "enrollment status changes."
            ),
            data_type="dr_eligibility",
            sensitivity=SensitivityTier.MEDIUM,
            endpoint=f"{endpoint_base}/api/v1/dr-eligibility",
            update_frequency="on_change",
            resolution="per_building",
            anonymized=False,
            personal_data=False,
            policy_metadata={
                "allowed_purposes": "congestion_management,dr_dispatch,grid_planning",
                "min_retention_days": "1",
                "max_retention_days": "90",
                "requires_contract": "true",
                "requires_consent": "true",
            },
        ),
        AssetRegistration(
            provider_id=participant_id,
            name="Prosumer Controllable Margin",
            description=(
                "Available controllable load margin per campus building for "
                "demand response dispatch. Represents how much demand can be "
                "reduced or shifted. Shared with authorized aggregators and "
                "DSO for flexibility planning."
            ),
            data_type="controllable_margin",
            sensitivity=SensitivityTier.MEDIUM,
            endpoint=f"{endpoint_base}/api/v1/controllable-margin",
            update_frequency="15min",
            resolution="per_building",
            anonymized=False,
            personal_data=False,
            policy_metadata={
                "allowed_purposes": "dr_dispatch,congestion_management,flexibility_trading",
                "min_retention_days": "1",
                "max_retention_days": "30",
                "requires_contract": "true",
                "requires_consent": "true",
            },
        ),
    ]


# ---------------------------------------------------------------------------
# ProsumerPublisher
# ---------------------------------------------------------------------------


class ProsumerPublisher:
    """Registers Prosumer data assets with the federated catalog on startup.

    Wraps :class:`~src.connector.catalog_client.CatalogClient` to publish
    the Prosumer's three shareable data assets (anonymized demand profiles,
    DR eligibility, controllable margin) and track the resulting
    catalog-assigned asset IDs.

    Raw meter readings are never published — they remain local to the
    prosumer node.

    Usage::

        publisher = ProsumerPublisher()
        responses = publisher.publish()
        for asset in responses:
            print(f"Registered: {asset.name} -> {asset.id}")

    Args:
        catalog_client: An optional pre-configured catalog client.  When
            ``None``, a new client is created using default settings
            (or the ``CATALOG_URL`` environment variable).
        participant_id: The Prosumer's participant identifier.  When
            ``None``, the ``PROSUMER_PARTICIPANT_ID`` environment variable
            is used (falling back to ``prosumer-campus-001``).
        endpoint_base: Base URL of the Prosumer node's API.  When
            ``None``, the ``PROSUMER_ENDPOINT_BASE`` environment variable
            is used (falling back to ``http://localhost:8003``).
    """

    def __init__(
        self,
        *,
        catalog_client: Optional[CatalogClient] = None,
        participant_id: Optional[str] = None,
        endpoint_base: Optional[str] = None,
    ) -> None:
        self._client = catalog_client or CatalogClient()
        self._participant_id = (
            participant_id
            or os.environ.get("PROSUMER_PARTICIPANT_ID")
            or _DEFAULT_PROSUMER_PARTICIPANT_ID
        )
        self._endpoint_base = (
            endpoint_base
            or os.environ.get("PROSUMER_ENDPOINT_BASE")
            or _DEFAULT_PROSUMER_ENDPOINT_BASE
        ).rstrip("/")

        # Track registered asset IDs to prevent duplicate registration
        # within the same process lifetime.
        self._registered_assets: dict[str, AssetResponse] = {}

    @property
    def participant_id(self) -> str:
        """The Prosumer's participant identifier."""
        return self._participant_id

    @property
    def registered_assets(self) -> dict[str, AssetResponse]:
        """Map of data_type -> AssetResponse for assets registered in this session."""
        return dict(self._registered_assets)

    def publish(self) -> list[AssetResponse]:
        """Register all Prosumer data assets with the federated catalog.

        Publishes anonymized demand profiles, DR eligibility, and
        controllable margin metadata.  Skips any asset type that has
        already been registered in this process lifetime.

        Returns:
            List of :class:`AssetResponse` objects for the registered assets.

        Raises:
            CatalogUnavailableError: If the catalog service cannot be reached
                after retry attempts.
            CatalogClientError: For unexpected HTTP errors from the catalog.
        """
        definitions = _prosumer_asset_definitions(
            self._participant_id, self._endpoint_base
        )
        responses: list[AssetResponse] = []

        for registration in definitions:
            if registration.data_type in self._registered_assets:
                logger.info(
                    "Asset type '%s' already registered (id=%s), skipping",
                    registration.data_type,
                    self._registered_assets[registration.data_type].id,
                )
                responses.append(self._registered_assets[registration.data_type])
                continue

            response = self._client.register_asset(registration)
            self._registered_assets[registration.data_type] = response
            logger.info(
                "Published Prosumer asset: name=%s type=%s id=%s",
                response.name,
                response.data_type,
                response.id,
            )
            responses.append(response)

        return responses

    def get_registered_asset(self, data_type: str) -> Optional[AssetResponse]:
        """Retrieve the catalog response for a previously registered asset.

        Args:
            data_type: The semantic data type (e.g.,
                ``'demand_profile'``).

        Returns:
            The :class:`AssetResponse` if the asset type has been registered
            in this session, or ``None`` otherwise.
        """
        return self._registered_assets.get(data_type)
