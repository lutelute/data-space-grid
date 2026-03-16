"""Aggregator catalog publisher for registering data assets with the federated catalog.

On startup, the Aggregator node publishes metadata for its two primary data
assets (flexibility envelopes, availability windows) to the federated catalog
so that other participants (DSO, prosumers) can discover and negotiate
contracts for access.

Key design decisions:
  - Uses :class:`~src.connector.catalog_client.CatalogClient` for all
    catalog interactions, inheriting retry logic and caching.
  - Asset definitions are hard-coded to the two Aggregator data types
    defined in the spec: ``flexibility_envelope`` and
    ``availability_window``.
  - Each asset registration includes policy metadata hints (allowed
    purposes, required sensitivity tier) to guide contract negotiation.
  - The ``publish()`` method is idempotent at the application level:
    calling it multiple times creates new catalog entries (the catalog
    assigns unique IDs), but the Aggregator publisher tracks registered
    asset IDs to avoid re-registration within the same process lifetime.
  - The ``AGGREGATOR_PARTICIPANT_ID`` and ``AGGREGATOR_ENDPOINT_BASE``
    are configurable via environment variables for deployment flexibility.
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

_DEFAULT_AGGREGATOR_PARTICIPANT_ID = "aggregator-001"
_DEFAULT_AGGREGATOR_ENDPOINT_BASE = "http://localhost:8002"


# ---------------------------------------------------------------------------
# Asset definitions
# ---------------------------------------------------------------------------

def _aggregator_asset_definitions(
    participant_id: str, endpoint_base: str
) -> list[AssetRegistration]:
    """Return the list of Aggregator data asset registrations.

    Each asset corresponds to a data type the Aggregator publishes:
      - **Flexibility Envelopes**: Aggregate flexibility F(t) offered by the
        DER fleet without exposing individual device states.
      - **Availability Windows**: Time windows when DER flexibility is
        available, with power range and ramp capability details.

    Args:
        participant_id: The Aggregator's participant identifier.
        endpoint_base: Base URL of the Aggregator node's API.

    Returns:
        List of ``AssetRegistration`` objects ready for catalog submission.
    """
    return [
        AssetRegistration(
            provider_id=participant_id,
            name="Aggregator Flexibility Envelopes",
            description=(
                "Aggregate flexibility envelopes F(t) representing the total "
                "feasible operating region of the DER fleet. Published without "
                "exposing individual device states. Includes PQ range, device "
                "class mix, state of charge, and response confidence."
            ),
            data_type="flexibility_envelope",
            sensitivity=SensitivityTier.MEDIUM,
            endpoint=f"{endpoint_base}/api/v1/flexibility-offers",
            update_frequency="15min",
            resolution="per_feeder",
            anonymized=False,
            personal_data=False,
            policy_metadata={
                "allowed_purposes": "congestion_management,flexibility_trading",
                "min_retention_days": "1",
                "max_retention_days": "90",
                "requires_contract": "true",
            },
        ),
        AssetRegistration(
            provider_id=participant_id,
            name="Aggregator Availability Windows",
            description=(
                "Time windows when DER flexibility is available for dispatch. "
                "Includes power range, ramp rates, and duration constraints. "
                "Updated when fleet availability changes."
            ),
            data_type="availability_window",
            sensitivity=SensitivityTier.MEDIUM,
            endpoint=f"{endpoint_base}/api/v1/availability",
            update_frequency="on_change",
            resolution="per_envelope",
            anonymized=False,
            personal_data=False,
            policy_metadata={
                "allowed_purposes": "congestion_management,grid_planning",
                "min_retention_days": "1",
                "max_retention_days": "30",
                "requires_contract": "true",
            },
        ),
    ]


# ---------------------------------------------------------------------------
# AggregatorPublisher
# ---------------------------------------------------------------------------


class AggregatorPublisher:
    """Registers Aggregator data assets with the federated catalog on startup.

    Wraps :class:`~src.connector.catalog_client.CatalogClient` to publish
    the Aggregator's two primary data assets (flexibility envelopes,
    availability windows) and track the resulting catalog-assigned asset IDs.

    Usage::

        publisher = AggregatorPublisher()
        responses = publisher.publish()
        for asset in responses:
            print(f"Registered: {asset.name} -> {asset.id}")

    Args:
        catalog_client: An optional pre-configured catalog client.  When
            ``None``, a new client is created using default settings
            (or the ``CATALOG_URL`` environment variable).
        participant_id: The Aggregator's participant identifier.  When
            ``None``, the ``AGGREGATOR_PARTICIPANT_ID`` environment variable
            is used (falling back to ``aggregator-001``).
        endpoint_base: Base URL of the Aggregator node's API.  When
            ``None``, the ``AGGREGATOR_ENDPOINT_BASE`` environment variable
            is used (falling back to ``http://localhost:8002``).
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
            or os.environ.get("AGGREGATOR_PARTICIPANT_ID")
            or _DEFAULT_AGGREGATOR_PARTICIPANT_ID
        )
        self._endpoint_base = (
            endpoint_base
            or os.environ.get("AGGREGATOR_ENDPOINT_BASE")
            or _DEFAULT_AGGREGATOR_ENDPOINT_BASE
        ).rstrip("/")

        # Track registered asset IDs to prevent duplicate registration
        # within the same process lifetime.
        self._registered_assets: dict[str, AssetResponse] = {}

    @property
    def participant_id(self) -> str:
        """The Aggregator's participant identifier."""
        return self._participant_id

    @property
    def registered_assets(self) -> dict[str, AssetResponse]:
        """Map of data_type -> AssetResponse for assets registered in this session."""
        return dict(self._registered_assets)

    def publish(self) -> list[AssetResponse]:
        """Register all Aggregator data assets with the federated catalog.

        Publishes flexibility envelopes and availability windows metadata.
        Skips any asset type that has already been registered in this
        process lifetime.

        Returns:
            List of :class:`AssetResponse` objects for the registered assets.

        Raises:
            CatalogUnavailableError: If the catalog service cannot be reached
                after retry attempts.
            CatalogClientError: For unexpected HTTP errors from the catalog.
        """
        definitions = _aggregator_asset_definitions(
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
                "Published Aggregator asset: name=%s type=%s id=%s",
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
                ``'flexibility_envelope'``).

        Returns:
            The :class:`AssetResponse` if the asset type has been registered
            in this session, or ``None`` otherwise.
        """
        return self._registered_assets.get(data_type)
