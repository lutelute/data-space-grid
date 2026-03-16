"""DSO catalog publisher for registering data assets with the federated catalog.

On startup, the DSO node publishes metadata for its three primary data assets
(feeder constraints, congestion signals, hosting capacity) to the federated
catalog so that other participants (aggregators, prosumers) can discover and
negotiate contracts for access.

Key design decisions:
  - Uses :class:`~src.connector.catalog_client.CatalogClient` for all
    catalog interactions, inheriting retry logic and caching.
  - Asset definitions are hard-coded to the three DSO data types defined
    in the spec: ``feeder_constraint``, ``congestion_signal``, and
    ``hosting_capacity``.
  - Each asset registration includes policy metadata hints (allowed
    purposes, required sensitivity tier) to guide contract negotiation.
  - The ``publish()`` method is idempotent at the application level:
    calling it multiple times creates new catalog entries (the catalog
    assigns unique IDs), but the DSO publisher tracks registered asset
    IDs to avoid re-registration within the same process lifetime.
  - The ``DSO_PARTICIPANT_ID`` and ``DSO_ENDPOINT_BASE`` are configurable
    via environment variables for deployment flexibility.
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

_DEFAULT_DSO_PARTICIPANT_ID = "dso-001"
_DEFAULT_DSO_ENDPOINT_BASE = "http://localhost:8001"


# ---------------------------------------------------------------------------
# Asset definitions
# ---------------------------------------------------------------------------

def _dso_asset_definitions(
    participant_id: str, endpoint_base: str
) -> list[AssetRegistration]:
    """Return the list of DSO data asset registrations.

    Each asset corresponds to a data type the DSO publishes:
      - **Feeder Constraints**: Operational limits on distribution feeders.
      - **Congestion Signals**: Real-time congestion levels per feeder.
      - **Hosting Capacity**: Available generation/load capacity at grid nodes.

    Args:
        participant_id: The DSO's participant identifier.
        endpoint_base: Base URL of the DSO node's API.

    Returns:
        List of ``AssetRegistration`` objects ready for catalog submission.
    """
    return [
        AssetRegistration(
            provider_id=participant_id,
            name="DSO Feeder Constraints",
            description=(
                "Operational limits on distribution feeders including maximum "
                "active power, voltage bounds, and current congestion levels. "
                "Updated every 15 minutes or on significant grid state changes."
            ),
            data_type="feeder_constraint",
            sensitivity=SensitivityTier.MEDIUM,
            endpoint=f"{endpoint_base}/api/v1/feeder-constraints",
            update_frequency="15min",
            resolution="per_feeder",
            anonymized=False,
            personal_data=False,
            policy_metadata={
                "allowed_purposes": "congestion_management,grid_planning",
                "min_retention_days": "1",
                "max_retention_days": "90",
                "requires_contract": "true",
            },
        ),
        AssetRegistration(
            provider_id=participant_id,
            name="DSO Congestion Signals",
            description=(
                "Real-time congestion signals for distribution feeders. "
                "Published when congestion levels change. Aggregators subscribe "
                "to these signals to adjust flexibility offers."
            ),
            data_type="congestion_signal",
            sensitivity=SensitivityTier.MEDIUM,
            endpoint=f"{endpoint_base}/api/v1/congestion-signals",
            update_frequency="on_change",
            resolution="per_feeder",
            anonymized=False,
            personal_data=False,
            policy_metadata={
                "allowed_purposes": "congestion_management,flexibility_trading",
                "min_retention_days": "1",
                "max_retention_days": "30",
                "requires_contract": "true",
            },
        ),
        AssetRegistration(
            provider_id=participant_id,
            name="DSO Hosting Capacity",
            description=(
                "Available hosting capacity at grid nodes aggregated per feeder. "
                "Indicates how much additional generation or load can be connected. "
                "Shared with authorized participants for DER planning."
            ),
            data_type="hosting_capacity",
            sensitivity=SensitivityTier.MEDIUM,
            endpoint=f"{endpoint_base}/api/v1/hosting-capacity",
            update_frequency="1h",
            resolution="per_feeder",
            anonymized=False,
            personal_data=False,
            policy_metadata={
                "allowed_purposes": "grid_planning,der_integration",
                "min_retention_days": "1",
                "max_retention_days": "365",
                "requires_contract": "true",
            },
        ),
    ]


# ---------------------------------------------------------------------------
# DSOPublisher
# ---------------------------------------------------------------------------


class DSOPublisher:
    """Registers DSO data assets with the federated catalog on startup.

    Wraps :class:`~src.connector.catalog_client.CatalogClient` to publish
    the DSO's three primary data assets (feeder constraints, congestion
    signals, hosting capacity) and track the resulting catalog-assigned
    asset IDs.

    Usage::

        publisher = DSOPublisher()
        responses = publisher.publish()
        for asset in responses:
            print(f"Registered: {asset.name} -> {asset.id}")

    Args:
        catalog_client: An optional pre-configured catalog client.  When
            ``None``, a new client is created using default settings
            (or the ``CATALOG_URL`` environment variable).
        participant_id: The DSO's participant identifier.  When ``None``,
            the ``DSO_PARTICIPANT_ID`` environment variable is used
            (falling back to ``dso-001``).
        endpoint_base: Base URL of the DSO node's API.  When ``None``,
            the ``DSO_ENDPOINT_BASE`` environment variable is used
            (falling back to ``http://localhost:8001``).
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
            or os.environ.get("DSO_PARTICIPANT_ID")
            or _DEFAULT_DSO_PARTICIPANT_ID
        )
        self._endpoint_base = (
            endpoint_base
            or os.environ.get("DSO_ENDPOINT_BASE")
            or _DEFAULT_DSO_ENDPOINT_BASE
        ).rstrip("/")

        # Track registered asset IDs to prevent duplicate registration
        # within the same process lifetime.
        self._registered_assets: dict[str, AssetResponse] = {}

    @property
    def participant_id(self) -> str:
        """The DSO's participant identifier."""
        return self._participant_id

    @property
    def registered_assets(self) -> dict[str, AssetResponse]:
        """Map of data_type -> AssetResponse for assets registered in this session."""
        return dict(self._registered_assets)

    def publish(self) -> list[AssetResponse]:
        """Register all DSO data assets with the federated catalog.

        Publishes feeder constraints, congestion signals, and hosting
        capacity metadata.  Skips any asset type that has already been
        registered in this process lifetime.

        Returns:
            List of :class:`AssetResponse` objects for the registered assets.

        Raises:
            CatalogUnavailableError: If the catalog service cannot be reached
                after retry attempts.
            CatalogClientError: For unexpected HTTP errors from the catalog.
        """
        definitions = _dso_asset_definitions(
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
                "Published DSO asset: name=%s type=%s id=%s",
                response.name,
                response.data_type,
                response.id,
            )
            responses.append(response)

        return responses

    def get_registered_asset(self, data_type: str) -> Optional[AssetResponse]:
        """Retrieve the catalog response for a previously registered asset.

        Args:
            data_type: The semantic data type (e.g., ``'feeder_constraint'``).

        Returns:
            The :class:`AssetResponse` if the asset type has been registered
            in this session, or ``None`` otherwise.
        """
        return self._registered_assets.get(data_type)
