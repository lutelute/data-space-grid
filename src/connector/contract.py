"""Contract negotiation state machine for the Federated Data Space Connector.

This module implements the contract lifecycle as a state machine with strict
transition rules.  No data is ever exchanged without a valid, active contract
(spec Pattern 3 – Contract-Gated Data Access).

State machine transitions:
  OFFERED  ->  NEGOTIATING  ->  ACTIVE  ->  EXPIRED
  OFFERED  ->  NEGOTIATING  ->  ACTIVE  ->  REVOKED
  OFFERED  ->  REJECTED
  NEGOTIATING  ->  REJECTED

Invalid transitions raise ``InvalidContractTransitionError``.

Emergency override handling:
  When a contract carries ``emergency_override=True`` the DSO may bypass
  normal validity window checks.  Emergency access is always audited
  separately so that the override can be reviewed after the fact.

Key design decisions:
  - The ``ContractManager`` keeps an in-memory dict of contracts keyed by
    ``contract_id``.  For production use this store would be backed by a
    persistent database, but the interface stays the same.
  - ``check_contract_valid()`` automatically transitions an ACTIVE contract
    to EXPIRED when its validity window has passed.
  - All mutations update the ``updated_at`` timestamp on the contract.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

from src.connector.models import (
    ContractOffer,
    ContractStatus,
    DataUsageContract,
)


def _utc_now() -> datetime:
    """Return the current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class ContractError(Exception):
    """Base exception for contract negotiation errors."""


class InvalidContractTransitionError(ContractError):
    """Raised when a state transition is not permitted by the state machine."""

    def __init__(self, contract_id: str, current: ContractStatus, target: ContractStatus) -> None:
        self.contract_id = contract_id
        self.current = current
        self.target = target
        super().__init__(
            f"Invalid contract transition for '{contract_id}': "
            f"{current.value} -> {target.value}"
        )


class ContractNotFoundError(ContractError):
    """Raised when a referenced contract does not exist."""

    def __init__(self, contract_id: str) -> None:
        self.contract_id = contract_id
        super().__init__(f"Contract not found: '{contract_id}'")


# ---------------------------------------------------------------------------
# Valid transitions
# ---------------------------------------------------------------------------

_VALID_TRANSITIONS: dict[ContractStatus, set[ContractStatus]] = {
    ContractStatus.OFFERED: {ContractStatus.NEGOTIATING, ContractStatus.REJECTED},
    ContractStatus.NEGOTIATING: {ContractStatus.ACTIVE, ContractStatus.REJECTED},
    ContractStatus.ACTIVE: {ContractStatus.EXPIRED, ContractStatus.REVOKED},
    ContractStatus.EXPIRED: set(),
    ContractStatus.REVOKED: set(),
    ContractStatus.REJECTED: set(),
}


# ---------------------------------------------------------------------------
# ContractManager
# ---------------------------------------------------------------------------


class ContractManager:
    """Manages the lifecycle of data usage contracts.

    The manager maintains an in-memory registry of contracts and enforces the
    state machine rules defined in :data:`_VALID_TRANSITIONS`.

    Usage::

        cm = ContractManager()
        contract = cm.offer_contract(offer)
        cm.negotiate_contract(contract.contract_id)
        cm.accept_contract(contract.contract_id)
        assert cm.check_contract_valid(contract.contract_id)
    """

    def __init__(self) -> None:
        self._contracts: dict[str, DataUsageContract] = {}

    # -- helpers -------------------------------------------------------------

    def _get_contract(self, contract_id: str) -> DataUsageContract:
        """Retrieve a contract or raise :class:`ContractNotFoundError`."""
        try:
            return self._contracts[contract_id]
        except KeyError:
            raise ContractNotFoundError(contract_id) from None

    def _transition(self, contract_id: str, target: ContractStatus) -> DataUsageContract:
        """Apply a state transition after validating it against the state machine."""
        contract = self._get_contract(contract_id)
        if target not in _VALID_TRANSITIONS.get(contract.status, set()):
            raise InvalidContractTransitionError(contract_id, contract.status, target)
        contract.status = target
        contract.updated_at = _utc_now()
        return contract

    # -- public API ----------------------------------------------------------

    def get_contract(self, contract_id: str) -> DataUsageContract:
        """Return the contract with the given *contract_id*.

        Raises:
            ContractNotFoundError: If the contract does not exist.
        """
        return self._get_contract(contract_id)

    def list_contracts(
        self,
        *,
        provider_id: Optional[str] = None,
        consumer_id: Optional[str] = None,
        status: Optional[ContractStatus] = None,
    ) -> list[DataUsageContract]:
        """Return contracts matching the optional filters."""
        results: list[DataUsageContract] = []
        for contract in self._contracts.values():
            if provider_id is not None and contract.provider_id != provider_id:
                continue
            if consumer_id is not None and contract.consumer_id != consumer_id:
                continue
            if status is not None and contract.status != status:
                continue
            results.append(contract)
        return results

    def offer_contract(self, offer: ContractOffer) -> DataUsageContract:
        """Create a new contract from a :class:`ContractOffer`.

        The contract starts in the ``OFFERED`` state and must be negotiated
        and accepted before data can be exchanged.

        Args:
            offer: The contract offer proposed by the consumer.

        Returns:
            The newly created :class:`DataUsageContract`.
        """
        contract_id = str(uuid.uuid4())
        now = _utc_now()
        contract = DataUsageContract(
            contract_id=contract_id,
            provider_id=offer.provider_id,
            consumer_id=offer.consumer_id,
            asset_id=offer.asset_id,
            purpose=offer.purpose,
            allowed_operations=list(offer.allowed_operations),
            redistribution_allowed=offer.redistribution_allowed,
            retention_days=offer.retention_days,
            anonymization_required=offer.anonymization_required,
            emergency_override=offer.emergency_override,
            status=ContractStatus.OFFERED,
            valid_from=offer.valid_from,
            valid_until=offer.valid_until,
            created_at=now,
            updated_at=now,
        )
        self._contracts[contract_id] = contract
        return contract

    def negotiate_contract(self, contract_id: str) -> DataUsageContract:
        """Move the contract from ``OFFERED`` to ``NEGOTIATING``.

        This represents the provider acknowledging the offer and entering
        the negotiation phase (e.g. counter-proposals on terms).

        Raises:
            ContractNotFoundError: If the contract does not exist.
            InvalidContractTransitionError: If the current state is not ``OFFERED``.
        """
        return self._transition(contract_id, ContractStatus.NEGOTIATING)

    def accept_contract(self, contract_id: str) -> DataUsageContract:
        """Move the contract from ``NEGOTIATING`` to ``ACTIVE``.

        Once active, data can be exchanged under the contract terms.

        Raises:
            ContractNotFoundError: If the contract does not exist.
            InvalidContractTransitionError: If the current state is not ``NEGOTIATING``.
        """
        return self._transition(contract_id, ContractStatus.ACTIVE)

    def reject_contract(self, contract_id: str) -> DataUsageContract:
        """Move the contract to ``REJECTED``.

        A contract can be rejected from the ``OFFERED`` or ``NEGOTIATING``
        states.  Once rejected it cannot be reactivated.

        Raises:
            ContractNotFoundError: If the contract does not exist.
            InvalidContractTransitionError: If the current state does not
                permit rejection.
        """
        return self._transition(contract_id, ContractStatus.REJECTED)

    def revoke_contract(self, contract_id: str) -> DataUsageContract:
        """Move the contract from ``ACTIVE`` to ``REVOKED``.

        Revocation terminates the contract immediately.  Any in-flight data
        exchange that started before revocation may complete, but no new
        requests will be permitted.

        Raises:
            ContractNotFoundError: If the contract does not exist.
            InvalidContractTransitionError: If the current state is not ``ACTIVE``.
        """
        return self._transition(contract_id, ContractStatus.REVOKED)

    def expire_contract(self, contract_id: str) -> DataUsageContract:
        """Move the contract from ``ACTIVE`` to ``EXPIRED``.

        This is typically called automatically by :meth:`check_contract_valid`
        when the validity window has passed.

        Raises:
            ContractNotFoundError: If the contract does not exist.
            InvalidContractTransitionError: If the current state is not ``ACTIVE``.
        """
        return self._transition(contract_id, ContractStatus.EXPIRED)

    def check_contract_valid(
        self,
        contract_id: str,
        *,
        emergency: bool = False,
    ) -> bool:
        """Check whether the contract is currently valid for data exchange.

        A contract is valid when:
          1. Its status is ``ACTIVE``.
          2. The current time falls within the ``[valid_from, valid_until]``
             window.

        If the contract is ``ACTIVE`` but the validity window has passed, the
        contract is automatically transitioned to ``EXPIRED`` and the method
        returns ``False``.

        Emergency override:
          When *emergency* is ``True`` **and** the contract has
          ``emergency_override=True``, the validity window check is bypassed.
          The contract must still be ``ACTIVE``.  This enables DSO priority
          access during grid emergencies.

        Args:
            contract_id: The contract to validate.
            emergency: Whether this is an emergency access request.

        Returns:
            ``True`` if the contract permits data exchange, ``False`` otherwise.

        Raises:
            ContractNotFoundError: If the contract does not exist.
        """
        contract = self._get_contract(contract_id)

        if contract.status != ContractStatus.ACTIVE:
            return False

        now = _utc_now()

        # Emergency override: bypass validity window when both the request
        # flag and the contract flag are set.
        if emergency and contract.emergency_override:
            return True

        # Check validity window
        if now > contract.valid_until:
            self._transition(contract_id, ContractStatus.EXPIRED)
            return False

        if now < contract.valid_from:
            return False

        return True
