"""Client library for interacting with the Federated Catalog service.

Participant nodes use this client to register data assets, discover assets
published by other participants, and negotiate data usage contracts through
the central catalog.

Key design decisions:
  - Uses ``httpx`` for HTTP requests (both sync and async support).
  - Configurable via the ``CATALOG_URL`` environment variable (default:
    ``http://localhost:8000``).
  - Retry logic with exponential backoff handles transient catalog
    unavailability.
  - Search results and individual asset lookups are cached so that
    participant nodes can continue operating when the catalog is
    temporarily unreachable (spec edge case 4: Catalog Unavailability).
  - All responses are parsed into the Pydantic schemas from
    :mod:`src.catalog.schemas` for type safety.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Optional

import httpx

from src.catalog.schemas import (
    AssetRegistration,
    AssetResponse,
    AssetSearchQuery,
    ContractInitiation,
    ContractResponse,
)
from src.semantic.cim import SensitivityTier

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Default configuration
# ---------------------------------------------------------------------------

_DEFAULT_CATALOG_URL = "http://localhost:8000"
_DEFAULT_TIMEOUT_SECONDS = 10.0
_DEFAULT_MAX_RETRIES = 3
_DEFAULT_RETRY_BACKOFF_FACTOR = 0.5


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class CatalogClientError(Exception):
    """Base exception for catalog client errors."""


class CatalogUnavailableError(CatalogClientError):
    """Raised when the catalog service cannot be reached after retries.

    If cached data is available, the caller may fall back to it via the
    :meth:`CatalogClient.get_cached_asset` and
    :meth:`CatalogClient.get_cached_search` helpers.
    """


class CatalogNotFoundError(CatalogClientError):
    """Raised when the requested resource (asset or contract) is not found."""


class CatalogConflictError(CatalogClientError):
    """Raised when a state-transition conflict occurs (e.g. invalid contract status)."""


# ---------------------------------------------------------------------------
# CatalogClient
# ---------------------------------------------------------------------------


class CatalogClient:
    """HTTP client for the Federated Catalog service.

    Provides methods for asset registration/discovery and contract
    negotiation.  Includes retry logic with exponential backoff and an
    in-memory cache so that previously fetched results remain available
    when the catalog is temporarily unreachable.

    Args:
        base_url: Base URL of the catalog service.  When ``None``, the
            ``CATALOG_URL`` environment variable is used (falling back to
            ``http://localhost:8000``).
        timeout: HTTP request timeout in seconds.
        max_retries: Maximum number of retry attempts for transient failures.
        retry_backoff_factor: Multiplier for exponential backoff between
            retries (delay = factor * 2^attempt).
    """

    def __init__(
        self,
        *,
        base_url: Optional[str] = None,
        timeout: float = _DEFAULT_TIMEOUT_SECONDS,
        max_retries: int = _DEFAULT_MAX_RETRIES,
        retry_backoff_factor: float = _DEFAULT_RETRY_BACKOFF_FACTOR,
    ) -> None:
        self._base_url = (
            base_url
            or os.environ.get("CATALOG_URL")
            or _DEFAULT_CATALOG_URL
        ).rstrip("/")
        self._timeout = timeout
        self._max_retries = max_retries
        self._retry_backoff_factor = retry_backoff_factor

        # In-memory caches for catalog unavailability resilience.
        # Keyed by asset_id for individual assets.
        self._asset_cache: dict[str, AssetResponse] = {}
        # Keyed by a stable string representation of search parameters.
        self._search_cache: dict[str, list[AssetResponse]] = {}
        # Keyed by contract_id for contract responses.
        self._contract_cache: dict[str, ContractResponse] = {}

    # -- Properties ----------------------------------------------------------

    @property
    def base_url(self) -> str:
        """The base URL of the catalog service."""
        return self._base_url

    # -- Internal helpers ----------------------------------------------------

    def _client(self) -> httpx.Client:
        """Create a new synchronous ``httpx.Client`` with configured timeout."""
        return httpx.Client(base_url=self._base_url, timeout=self._timeout)

    def _request_with_retry(
        self,
        method: str,
        path: str,
        *,
        json: Optional[dict] = None,
        params: Optional[dict] = None,
    ) -> httpx.Response:
        """Execute an HTTP request with retry logic and exponential backoff.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE).
            path: URL path relative to the catalog base URL.
            json: Optional JSON request body.
            params: Optional query parameters.

        Returns:
            The :class:`httpx.Response` on success.

        Raises:
            CatalogUnavailableError: If the catalog cannot be reached after
                all retry attempts.
        """
        last_exc: Optional[Exception] = None

        for attempt in range(self._max_retries):
            try:
                with self._client() as client:
                    response = client.request(
                        method,
                        path,
                        json=json,
                        params=params,
                    )
                return response
            except (httpx.ConnectError, httpx.TimeoutException) as exc:
                last_exc = exc
                delay = self._retry_backoff_factor * (2 ** attempt)
                logger.warning(
                    "Catalog request failed (attempt %d/%d): %s %s — %s. "
                    "Retrying in %.1fs…",
                    attempt + 1,
                    self._max_retries,
                    method,
                    path,
                    exc,
                    delay,
                )
                time.sleep(delay)

        raise CatalogUnavailableError(
            f"Catalog at {self._base_url} unreachable after "
            f"{self._max_retries} attempts: {last_exc}"
        )

    @staticmethod
    def _search_cache_key(query: AssetSearchQuery) -> str:
        """Derive a stable cache key from an ``AssetSearchQuery``."""
        parts: list[str] = []
        if query.provider_id is not None:
            parts.append(f"provider={query.provider_id}")
        if query.data_type is not None:
            parts.append(f"type={query.data_type}")
        if query.sensitivity is not None:
            parts.append(f"sensitivity={query.sensitivity.value}")
        return "&".join(sorted(parts)) if parts else "__all__"

    @staticmethod
    def _raise_for_status(response: httpx.Response, context: str) -> None:
        """Raise typed exceptions for non-success HTTP status codes.

        Args:
            response: The HTTP response to check.
            context: A human-readable description of the operation, used in
                error messages.

        Raises:
            CatalogNotFoundError: For 404 responses.
            CatalogConflictError: For 409 responses.
            CatalogClientError: For all other non-2xx responses.
        """
        if response.is_success:
            return

        detail = ""
        try:
            body = response.json()
            detail = body.get("detail", "")
        except Exception:
            detail = response.text

        if response.status_code == 404:
            raise CatalogNotFoundError(f"{context}: {detail}")
        if response.status_code == 409:
            raise CatalogConflictError(f"{context}: {detail}")

        raise CatalogClientError(
            f"{context}: HTTP {response.status_code} — {detail}"
        )

    # -- Asset methods -------------------------------------------------------

    def register_asset(self, registration: AssetRegistration) -> AssetResponse:
        """Register a new data asset in the federated catalog.

        Args:
            registration: The asset registration payload.

        Returns:
            The registered asset with catalog-assigned ``id`` and timestamps.

        Raises:
            CatalogUnavailableError: If the catalog is unreachable.
            CatalogClientError: For unexpected HTTP errors.
        """
        response = self._request_with_retry(
            "POST",
            "/api/v1/assets",
            json=registration.model_dump(mode="json"),
        )
        self._raise_for_status(response, "register_asset")

        asset = AssetResponse.model_validate(response.json())
        # Cache the newly registered asset.
        self._asset_cache[asset.id] = asset
        logger.info(
            "Registered asset: id=%s name=%s provider=%s",
            asset.id,
            asset.name,
            asset.provider_id,
        )
        return asset

    def search_assets(
        self,
        *,
        provider_id: Optional[str] = None,
        data_type: Optional[str] = None,
        sensitivity: Optional[SensitivityTier] = None,
    ) -> list[AssetResponse]:
        """Search and discover data assets in the federated catalog.

        All parameters are optional.  When omitted, all registered assets
        are returned.  Filters are combined with AND logic.

        If the catalog is unreachable, cached results from the most recent
        successful search with the same parameters are returned (edge case 4).

        Args:
            provider_id: Filter by data provider participant ID.
            data_type: Filter by semantic data type (e.g. ``'feeder_constraint'``).
            sensitivity: Filter by sensitivity tier.

        Returns:
            A list of matching :class:`AssetResponse` objects.

        Raises:
            CatalogUnavailableError: If the catalog is unreachable **and** no
                cached results exist for the given query.
        """
        query = AssetSearchQuery(
            provider_id=provider_id,
            data_type=data_type,
            sensitivity=sensitivity,
        )
        cache_key = self._search_cache_key(query)

        # Build query parameters matching the catalog's external API
        # (provider, type, sensitivity — NOT provider_id, data_type).
        params: dict[str, str] = {}
        if provider_id is not None:
            params["provider"] = provider_id
        if data_type is not None:
            params["type"] = data_type
        if sensitivity is not None:
            params["sensitivity"] = sensitivity.value

        try:
            response = self._request_with_retry("GET", "/api/v1/assets", params=params)
            self._raise_for_status(response, "search_assets")

            assets = [AssetResponse.model_validate(a) for a in response.json()]

            # Update caches.
            self._search_cache[cache_key] = assets
            for asset in assets:
                self._asset_cache[asset.id] = asset

            return assets

        except CatalogUnavailableError:
            cached = self._search_cache.get(cache_key)
            if cached is not None:
                logger.warning(
                    "Catalog unavailable — returning %d cached search results "
                    "for query '%s'",
                    len(cached),
                    cache_key,
                )
                return cached
            raise

    def get_asset(self, asset_id: str) -> AssetResponse:
        """Retrieve a single asset by its ID.

        If the catalog is unreachable, the cached version of the asset is
        returned when available (edge case 4).

        Args:
            asset_id: The unique asset identifier.

        Returns:
            The :class:`AssetResponse` for the requested asset.

        Raises:
            CatalogNotFoundError: If the asset does not exist.
            CatalogUnavailableError: If the catalog is unreachable **and** the
                asset is not in the local cache.
        """
        try:
            response = self._request_with_retry(
                "GET", f"/api/v1/assets/{asset_id}"
            )
            self._raise_for_status(response, f"get_asset({asset_id})")

            asset = AssetResponse.model_validate(response.json())
            self._asset_cache[asset.id] = asset
            return asset

        except CatalogUnavailableError:
            cached = self._asset_cache.get(asset_id)
            if cached is not None:
                logger.warning(
                    "Catalog unavailable — returning cached asset '%s'",
                    asset_id,
                )
                return cached
            raise

    def deregister_asset(self, asset_id: str) -> None:
        """Deregister a data asset from the federated catalog.

        Args:
            asset_id: The unique asset identifier.

        Raises:
            CatalogNotFoundError: If the asset does not exist.
            CatalogUnavailableError: If the catalog is unreachable.
            CatalogClientError: For unexpected HTTP errors.
        """
        response = self._request_with_retry(
            "DELETE", f"/api/v1/assets/{asset_id}"
        )
        self._raise_for_status(response, f"deregister_asset({asset_id})")

        # Remove from cache.
        self._asset_cache.pop(asset_id, None)
        logger.info("Deregistered asset: id=%s", asset_id)

    # -- Contract methods ----------------------------------------------------

    def initiate_contract(
        self, initiation: ContractInitiation
    ) -> ContractResponse:
        """Initiate a contract negotiation for a data asset.

        The consumer proposes contract terms; the catalog creates the contract
        in ``OFFERED`` state and assigns a ``contract_id``.

        Args:
            initiation: The contract initiation payload.

        Returns:
            The created :class:`ContractResponse` with ``status=OFFERED``.

        Raises:
            CatalogNotFoundError: If the target asset does not exist.
            CatalogUnavailableError: If the catalog is unreachable.
            CatalogClientError: For unexpected HTTP errors.
        """
        response = self._request_with_retry(
            "POST",
            "/api/v1/contracts",
            json=initiation.model_dump(mode="json"),
        )
        self._raise_for_status(response, "initiate_contract")

        contract = ContractResponse.model_validate(response.json())
        self._contract_cache[contract.contract_id] = contract
        logger.info(
            "Contract initiated: id=%s provider=%s consumer=%s asset=%s",
            contract.contract_id,
            contract.provider_id,
            contract.consumer_id,
            contract.asset_id,
        )
        return contract

    def get_contract_status(self, contract_id: str) -> ContractResponse:
        """Retrieve the current status of a contract.

        If the catalog is unreachable, the cached version of the contract is
        returned when available (edge case 4).

        Args:
            contract_id: The unique contract identifier.

        Returns:
            The :class:`ContractResponse` for the requested contract.

        Raises:
            CatalogNotFoundError: If the contract does not exist.
            CatalogUnavailableError: If the catalog is unreachable **and** the
                contract is not in the local cache.
        """
        try:
            response = self._request_with_retry(
                "GET", f"/api/v1/contracts/{contract_id}"
            )
            self._raise_for_status(
                response, f"get_contract_status({contract_id})"
            )

            contract = ContractResponse.model_validate(response.json())
            self._contract_cache[contract.contract_id] = contract
            return contract

        except CatalogUnavailableError:
            cached = self._contract_cache.get(contract_id)
            if cached is not None:
                logger.warning(
                    "Catalog unavailable — returning cached contract '%s'",
                    contract_id,
                )
                return cached
            raise

    def accept_contract(self, contract_id: str) -> ContractResponse:
        """Accept a contract, transitioning it to ACTIVE state.

        Performs the catalog's two-step transition:
        ``OFFERED -> NEGOTIATING -> ACTIVE`` (or ``NEGOTIATING -> ACTIVE``
        if already in the negotiating state).

        Args:
            contract_id: The unique contract identifier.

        Returns:
            The updated :class:`ContractResponse` with ``status=ACTIVE``.

        Raises:
            CatalogNotFoundError: If the contract does not exist.
            CatalogConflictError: If the contract is in an invalid state
                for acceptance.
            CatalogUnavailableError: If the catalog is unreachable.
        """
        response = self._request_with_retry(
            "PUT", f"/api/v1/contracts/{contract_id}/accept"
        )
        self._raise_for_status(
            response, f"accept_contract({contract_id})"
        )

        contract = ContractResponse.model_validate(response.json())
        self._contract_cache[contract.contract_id] = contract
        logger.info("Contract accepted: id=%s", contract_id)
        return contract

    def reject_contract(self, contract_id: str) -> ContractResponse:
        """Reject a contract, transitioning it to REJECTED state.

        A contract can be rejected from the ``OFFERED`` or ``NEGOTIATING``
        states.  Once rejected, it cannot be reactivated.

        Args:
            contract_id: The unique contract identifier.

        Returns:
            The updated :class:`ContractResponse` with ``status=REJECTED``.

        Raises:
            CatalogNotFoundError: If the contract does not exist.
            CatalogConflictError: If the contract is in an invalid state
                for rejection.
            CatalogUnavailableError: If the catalog is unreachable.
        """
        response = self._request_with_retry(
            "PUT", f"/api/v1/contracts/{contract_id}/reject"
        )
        self._raise_for_status(
            response, f"reject_contract({contract_id})"
        )

        contract = ContractResponse.model_validate(response.json())
        self._contract_cache[contract.contract_id] = contract
        logger.info("Contract rejected: id=%s", contract_id)
        return contract

    # -- Cache access (for catalog unavailability fallback) ------------------

    def get_cached_asset(self, asset_id: str) -> Optional[AssetResponse]:
        """Return a previously cached asset, or ``None`` if not cached.

        This is useful when the catalog is unreachable and the caller wants
        to explicitly check the cache without triggering a network request.
        """
        return self._asset_cache.get(asset_id)

    def get_cached_search(
        self,
        *,
        provider_id: Optional[str] = None,
        data_type: Optional[str] = None,
        sensitivity: Optional[SensitivityTier] = None,
    ) -> Optional[list[AssetResponse]]:
        """Return cached search results, or ``None`` if not cached.

        Parameters must match those used in the original
        :meth:`search_assets` call to find the corresponding cache entry.
        """
        query = AssetSearchQuery(
            provider_id=provider_id,
            data_type=data_type,
            sensitivity=sensitivity,
        )
        return self._search_cache.get(self._search_cache_key(query))

    def get_cached_contract(
        self, contract_id: str
    ) -> Optional[ContractResponse]:
        """Return a previously cached contract, or ``None`` if not cached."""
        return self._contract_cache.get(contract_id)

    def clear_cache(self) -> None:
        """Clear all in-memory caches."""
        self._asset_cache.clear()
        self._search_cache.clear()
        self._contract_cache.clear()
        logger.info("Catalog client cache cleared")
