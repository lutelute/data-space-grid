"""SQLite-backed catalog data store for the Federated Catalog service.

Provides persistent storage for data asset metadata and contract records
using SQLAlchemy with SQLite (dev) / PostgreSQL (prod).  The store is the
single source of truth for the catalog service – all reads and writes go
through :class:`CatalogStore`.

Key design decisions:
  - Two tables: ``assets`` and ``contracts``, matching the spec's catalog
    schema requirements.
  - The ``assets`` table stores all fields from ``AssetRegistration`` plus
    catalog-assigned ``id``, ``created_at``, and ``updated_at``.
  - The ``contracts`` table mirrors all ``DataUsageContract`` fields from
    connector-core for full contract lifecycle tracking.
  - ``policy_metadata`` and ``allowed_operations`` are stored as JSON strings
    since SQLite does not natively support JSON or array columns.
  - All timestamps are stored as timezone-aware UTC datetimes.
  - The store exposes synchronous methods; async wrappers can be added at
    the route layer when needed.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
    String,
    Text,
    create_engine,
)
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from src.catalog.schemas import (
    AssetRegistration,
    AssetResponse,
    AssetSearchQuery,
    ContractInitiation,
    ContractResponse,
)
from src.connector.models import ContractStatus
from src.semantic.cim import SensitivityTier

Base = declarative_base()


def _utc_now() -> datetime:
    """Return the current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# SQLAlchemy ORM models
# ---------------------------------------------------------------------------


class AssetRow(Base):  # type: ignore[misc]
    """SQLAlchemy model for the ``assets`` table.

    Stores data asset metadata registered by participants.  The ``id`` is
    a UUID assigned by the catalog at registration time.
    """

    __tablename__ = "assets"

    id = Column(String, primary_key=True)
    provider_id = Column(String, nullable=False, index=True)
    name = Column(String, nullable=False)
    description = Column(Text, nullable=False, default="")
    data_type = Column(String, nullable=False, index=True)
    sensitivity = Column(String, nullable=False)
    endpoint = Column(String, nullable=False)
    update_frequency = Column(String, nullable=True)
    resolution = Column(String, nullable=True)
    anonymized = Column(Boolean, nullable=False, default=False)
    personal_data = Column(Boolean, nullable=False, default=False)
    policy_metadata = Column(Text, nullable=False, default="{}")
    contract_template_id = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)


class ContractRow(Base):  # type: ignore[misc]
    """SQLAlchemy model for the ``contracts`` table.

    Mirrors all ``DataUsageContract`` fields from connector-core so the
    catalog can track the full contract lifecycle.
    """

    __tablename__ = "contracts"

    contract_id = Column(String, primary_key=True)
    provider_id = Column(String, nullable=False, index=True)
    consumer_id = Column(String, nullable=False, index=True)
    asset_id = Column(String, nullable=False, index=True)
    purpose = Column(String, nullable=False)
    allowed_operations = Column(Text, nullable=False, default="[]")
    redistribution_allowed = Column(Boolean, nullable=False, default=False)
    retention_days = Column(Integer, nullable=False)
    anonymization_required = Column(Boolean, nullable=False, default=False)
    emergency_override = Column(Boolean, nullable=False, default=False)
    status = Column(String, nullable=False, default=ContractStatus.OFFERED.value)
    valid_from = Column(DateTime(timezone=True), nullable=False)
    valid_until = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)


# ---------------------------------------------------------------------------
# CatalogStore
# ---------------------------------------------------------------------------


class CatalogStore:
    """Persistent catalog data store backed by SQLAlchemy.

    Usage::

        store = CatalogStore("sqlite:///data/catalog.db")
        asset = store.register_asset(registration)
        results = store.search_assets(AssetSearchQuery(data_type="feeder_constraint"))
        contract = store.create_contract(initiation)

    Args:
        database_url: SQLAlchemy database URL.  Defaults to an in-memory
            SQLite database for testing.
    """

    def __init__(self, database_url: str = "sqlite:///data/catalog.db") -> None:
        self._engine = create_engine(database_url, echo=False)
        Base.metadata.create_all(self._engine)
        self._session_factory = sessionmaker(bind=self._engine)

    def _session(self) -> Session:
        """Create a new database session."""
        return self._session_factory()

    # -- Asset operations ----------------------------------------------------

    def register_asset(self, registration: AssetRegistration) -> AssetResponse:
        """Register a new data asset in the catalog.

        Assigns a UUID and records the current UTC timestamp for
        ``created_at`` and ``updated_at``.

        Args:
            registration: The asset metadata supplied by the provider.

        Returns:
            The full asset response including catalog-assigned fields.
        """
        now = _utc_now()
        asset_id = str(uuid.uuid4())
        row = AssetRow(
            id=asset_id,
            provider_id=registration.provider_id,
            name=registration.name,
            description=registration.description,
            data_type=registration.data_type,
            sensitivity=registration.sensitivity.value,
            endpoint=registration.endpoint,
            update_frequency=registration.update_frequency,
            resolution=registration.resolution,
            anonymized=registration.anonymized,
            personal_data=registration.personal_data,
            policy_metadata=json.dumps(registration.policy_metadata),
            contract_template_id=registration.contract_template_id,
            created_at=now,
            updated_at=now,
        )
        with self._session() as session:
            session.add(row)
            session.commit()
            return self._row_to_asset_response(row)

    def get_asset(self, asset_id: str) -> Optional[AssetResponse]:
        """Retrieve a single asset by its ID.

        Args:
            asset_id: The unique asset identifier.

        Returns:
            The asset response, or ``None`` if not found.
        """
        with self._session() as session:
            row = session.get(AssetRow, asset_id)
            if row is None:
                return None
            return self._row_to_asset_response(row)

    def search_assets(self, query: AssetSearchQuery) -> list[AssetResponse]:
        """Search for assets matching the given query filters.

        All filters are combined with AND logic.  When no filters are
        provided, all registered assets are returned.

        Args:
            query: The search filters.

        Returns:
            List of matching asset responses.
        """
        with self._session() as session:
            q = session.query(AssetRow)

            if query.provider_id is not None:
                q = q.filter(AssetRow.provider_id == query.provider_id)
            if query.data_type is not None:
                q = q.filter(AssetRow.data_type == query.data_type)
            if query.sensitivity is not None:
                q = q.filter(AssetRow.sensitivity == query.sensitivity.value)
            if query.name_contains is not None:
                q = q.filter(AssetRow.name.ilike(f"%{query.name_contains}%"))
            if query.anonymized is not None:
                q = q.filter(AssetRow.anonymized == query.anonymized)
            if query.personal_data is not None:
                q = q.filter(AssetRow.personal_data == query.personal_data)

            return [self._row_to_asset_response(row) for row in q.all()]

    def delete_asset(self, asset_id: str) -> bool:
        """Delete an asset from the catalog.

        Args:
            asset_id: The unique asset identifier.

        Returns:
            ``True`` if the asset was found and deleted, ``False`` otherwise.
        """
        with self._session() as session:
            row = session.get(AssetRow, asset_id)
            if row is None:
                return False
            session.delete(row)
            session.commit()
            return True

    # -- Contract operations -------------------------------------------------

    def create_contract(self, initiation: ContractInitiation) -> ContractResponse:
        """Create a new contract in ``OFFERED`` state.

        Assigns a UUID and records the current UTC timestamp for
        ``created_at`` and ``updated_at``.

        Args:
            initiation: The contract terms proposed by the consumer.

        Returns:
            The full contract response including catalog-assigned fields.
        """
        now = _utc_now()
        contract_id = str(uuid.uuid4())
        row = ContractRow(
            contract_id=contract_id,
            provider_id=initiation.provider_id,
            consumer_id=initiation.consumer_id,
            asset_id=initiation.asset_id,
            purpose=initiation.purpose,
            allowed_operations=json.dumps(initiation.allowed_operations),
            redistribution_allowed=initiation.redistribution_allowed,
            retention_days=initiation.retention_days,
            anonymization_required=initiation.anonymization_required,
            emergency_override=initiation.emergency_override,
            status=ContractStatus.OFFERED.value,
            valid_from=initiation.valid_from,
            valid_until=initiation.valid_until,
            created_at=now,
            updated_at=now,
        )
        with self._session() as session:
            session.add(row)
            session.commit()
            return self._row_to_contract_response(row)

    def get_contract(self, contract_id: str) -> Optional[ContractResponse]:
        """Retrieve a single contract by its ID.

        Args:
            contract_id: The unique contract identifier.

        Returns:
            The contract response, or ``None`` if not found.
        """
        with self._session() as session:
            row = session.get(ContractRow, contract_id)
            if row is None:
                return None
            return self._row_to_contract_response(row)

    def update_contract_status(
        self, contract_id: str, new_status: ContractStatus
    ) -> Optional[ContractResponse]:
        """Update the status of an existing contract.

        Updates the ``status`` and ``updated_at`` fields.  Does **not**
        enforce state machine transition rules – that is the responsibility
        of the route layer or the ``ContractManager`` from connector-core.

        Args:
            contract_id: The unique contract identifier.
            new_status: The new contract status.

        Returns:
            The updated contract response, or ``None`` if not found.
        """
        with self._session() as session:
            row = session.get(ContractRow, contract_id)
            if row is None:
                return None
            row.status = new_status.value
            row.updated_at = _utc_now()
            session.commit()
            # Re-read to ensure we return committed state
            session.refresh(row)
            return self._row_to_contract_response(row)

    def list_contracts(
        self,
        *,
        provider_id: Optional[str] = None,
        consumer_id: Optional[str] = None,
        asset_id: Optional[str] = None,
        status: Optional[ContractStatus] = None,
    ) -> list[ContractResponse]:
        """List contracts matching the given filters.

        All filters are combined with AND logic.  When no filters are
        provided, all contracts are returned.

        Args:
            provider_id: Filter by data provider.
            consumer_id: Filter by data consumer.
            asset_id: Filter by target asset.
            status: Filter by contract status.

        Returns:
            List of matching contract responses.
        """
        with self._session() as session:
            q = session.query(ContractRow)

            if provider_id is not None:
                q = q.filter(ContractRow.provider_id == provider_id)
            if consumer_id is not None:
                q = q.filter(ContractRow.consumer_id == consumer_id)
            if asset_id is not None:
                q = q.filter(ContractRow.asset_id == asset_id)
            if status is not None:
                q = q.filter(ContractRow.status == status.value)

            return [self._row_to_contract_response(row) for row in q.all()]

    # -- Conversion helpers --------------------------------------------------

    @staticmethod
    def _row_to_asset_response(row: AssetRow) -> AssetResponse:
        """Convert an ``AssetRow`` ORM object to an ``AssetResponse`` schema."""
        return AssetResponse(
            id=row.id,
            provider_id=row.provider_id,
            name=row.name,
            description=row.description,
            data_type=row.data_type,
            sensitivity=SensitivityTier(row.sensitivity),
            endpoint=row.endpoint,
            update_frequency=row.update_frequency,
            resolution=row.resolution,
            anonymized=row.anonymized,
            personal_data=row.personal_data,
            policy_metadata=json.loads(row.policy_metadata),
            contract_template_id=row.contract_template_id,
            created_at=row.created_at,
            updated_at=row.updated_at,
        )

    @staticmethod
    def _row_to_contract_response(row: ContractRow) -> ContractResponse:
        """Convert a ``ContractRow`` ORM object to a ``ContractResponse`` schema."""
        return ContractResponse(
            contract_id=row.contract_id,
            provider_id=row.provider_id,
            consumer_id=row.consumer_id,
            asset_id=row.asset_id,
            purpose=row.purpose,
            allowed_operations=json.loads(row.allowed_operations),
            redistribution_allowed=row.redistribution_allowed,
            retention_days=row.retention_days,
            anonymization_required=row.anonymization_required,
            emergency_override=row.emergency_override,
            status=ContractStatus(row.status),
            valid_from=row.valid_from,
            valid_until=row.valid_until,
            created_at=row.created_at,
            updated_at=row.updated_at,
        )
