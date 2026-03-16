"""Unit tests for the contract negotiation state machine.

Tests valid state machine transitions (offered -> negotiating -> active ->
expired/revoked, offered -> rejected), and verifies that invalid transitions
raise InvalidContractTransitionError.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.connector.contract import (
    ContractManager,
    ContractNotFoundError,
    InvalidContractTransitionError,
)
from src.connector.models import ContractOffer, ContractStatus


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _make_offer(**overrides: object) -> ContractOffer:
    """Create a ContractOffer with sensible defaults for testing."""
    now = _utc_now()
    defaults: dict[str, object] = {
        "offer_id": "offer-test",
        "provider_id": "dso-001",
        "consumer_id": "agg-001",
        "asset_id": "asset-fc-01",
        "purpose": "congestion_management",
        "allowed_operations": ["read"],
        "retention_days": 30,
        "valid_from": now - timedelta(days=1),
        "valid_until": now + timedelta(days=90),
    }
    defaults.update(overrides)
    return ContractOffer(**defaults)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Happy path: full lifecycle
# ---------------------------------------------------------------------------


class TestContractLifecycleHappyPath:
    """Tests for valid contract state transitions."""

    def test_offer_creates_contract_in_offered_state(self) -> None:
        """offer_contract should create a contract in OFFERED status."""
        cm = ContractManager()
        contract = cm.offer_contract(_make_offer())
        assert contract.status == ContractStatus.OFFERED
        assert contract.contract_id
        assert contract.provider_id == "dso-001"
        assert contract.consumer_id == "agg-001"

    def test_offered_to_negotiating(self) -> None:
        """OFFERED -> NEGOTIATING should succeed."""
        cm = ContractManager()
        contract = cm.offer_contract(_make_offer())
        result = cm.negotiate_contract(contract.contract_id)
        assert result.status == ContractStatus.NEGOTIATING

    def test_negotiating_to_active(self) -> None:
        """NEGOTIATING -> ACTIVE should succeed."""
        cm = ContractManager()
        contract = cm.offer_contract(_make_offer())
        cm.negotiate_contract(contract.contract_id)
        result = cm.accept_contract(contract.contract_id)
        assert result.status == ContractStatus.ACTIVE

    def test_active_to_expired(self) -> None:
        """ACTIVE -> EXPIRED should succeed."""
        cm = ContractManager()
        contract = cm.offer_contract(_make_offer())
        cm.negotiate_contract(contract.contract_id)
        cm.accept_contract(contract.contract_id)
        result = cm.expire_contract(contract.contract_id)
        assert result.status == ContractStatus.EXPIRED

    def test_active_to_revoked(self) -> None:
        """ACTIVE -> REVOKED should succeed."""
        cm = ContractManager()
        contract = cm.offer_contract(_make_offer())
        cm.negotiate_contract(contract.contract_id)
        cm.accept_contract(contract.contract_id)
        result = cm.revoke_contract(contract.contract_id)
        assert result.status == ContractStatus.REVOKED

    def test_offered_to_rejected(self) -> None:
        """OFFERED -> REJECTED should succeed."""
        cm = ContractManager()
        contract = cm.offer_contract(_make_offer())
        result = cm.reject_contract(contract.contract_id)
        assert result.status == ContractStatus.REJECTED

    def test_negotiating_to_rejected(self) -> None:
        """NEGOTIATING -> REJECTED should succeed."""
        cm = ContractManager()
        contract = cm.offer_contract(_make_offer())
        cm.negotiate_contract(contract.contract_id)
        result = cm.reject_contract(contract.contract_id)
        assert result.status == ContractStatus.REJECTED

    def test_full_lifecycle_offered_to_expired(self) -> None:
        """Full happy path: OFFERED -> NEGOTIATING -> ACTIVE -> EXPIRED."""
        cm = ContractManager()
        contract = cm.offer_contract(_make_offer())
        assert contract.status == ContractStatus.OFFERED

        cm.negotiate_contract(contract.contract_id)
        assert cm.get_contract(contract.contract_id).status == ContractStatus.NEGOTIATING

        cm.accept_contract(contract.contract_id)
        assert cm.get_contract(contract.contract_id).status == ContractStatus.ACTIVE

        cm.expire_contract(contract.contract_id)
        assert cm.get_contract(contract.contract_id).status == ContractStatus.EXPIRED


# ---------------------------------------------------------------------------
# Invalid transitions
# ---------------------------------------------------------------------------


class TestContractInvalidTransitions:
    """Tests that invalid state transitions raise InvalidContractTransitionError."""

    def test_offered_to_active_is_invalid(self) -> None:
        """OFFERED -> ACTIVE should raise (must go through NEGOTIATING)."""
        cm = ContractManager()
        contract = cm.offer_contract(_make_offer())
        with pytest.raises(InvalidContractTransitionError) as exc_info:
            cm.accept_contract(contract.contract_id)
        assert exc_info.value.current == ContractStatus.OFFERED
        assert exc_info.value.target == ContractStatus.ACTIVE

    def test_offered_to_expired_is_invalid(self) -> None:
        """OFFERED -> EXPIRED should raise."""
        cm = ContractManager()
        contract = cm.offer_contract(_make_offer())
        with pytest.raises(InvalidContractTransitionError):
            cm.expire_contract(contract.contract_id)

    def test_offered_to_revoked_is_invalid(self) -> None:
        """OFFERED -> REVOKED should raise."""
        cm = ContractManager()
        contract = cm.offer_contract(_make_offer())
        with pytest.raises(InvalidContractTransitionError):
            cm.revoke_contract(contract.contract_id)

    def test_negotiating_to_expired_is_invalid(self) -> None:
        """NEGOTIATING -> EXPIRED should raise."""
        cm = ContractManager()
        contract = cm.offer_contract(_make_offer())
        cm.negotiate_contract(contract.contract_id)
        with pytest.raises(InvalidContractTransitionError):
            cm.expire_contract(contract.contract_id)

    def test_negotiating_to_revoked_is_invalid(self) -> None:
        """NEGOTIATING -> REVOKED should raise."""
        cm = ContractManager()
        contract = cm.offer_contract(_make_offer())
        cm.negotiate_contract(contract.contract_id)
        with pytest.raises(InvalidContractTransitionError):
            cm.revoke_contract(contract.contract_id)

    def test_active_to_negotiating_is_invalid(self) -> None:
        """ACTIVE -> NEGOTIATING should raise."""
        cm = ContractManager()
        contract = cm.offer_contract(_make_offer())
        cm.negotiate_contract(contract.contract_id)
        cm.accept_contract(contract.contract_id)
        with pytest.raises(InvalidContractTransitionError):
            cm.negotiate_contract(contract.contract_id)

    def test_expired_is_terminal(self) -> None:
        """EXPIRED is a terminal state; no transition should be allowed."""
        cm = ContractManager()
        contract = cm.offer_contract(_make_offer())
        cm.negotiate_contract(contract.contract_id)
        cm.accept_contract(contract.contract_id)
        cm.expire_contract(contract.contract_id)

        with pytest.raises(InvalidContractTransitionError):
            cm.revoke_contract(contract.contract_id)

    def test_revoked_is_terminal(self) -> None:
        """REVOKED is a terminal state; no transition should be allowed."""
        cm = ContractManager()
        contract = cm.offer_contract(_make_offer())
        cm.negotiate_contract(contract.contract_id)
        cm.accept_contract(contract.contract_id)
        cm.revoke_contract(contract.contract_id)

        with pytest.raises(InvalidContractTransitionError):
            cm.expire_contract(contract.contract_id)

    def test_rejected_is_terminal(self) -> None:
        """REJECTED is a terminal state; no transition should be allowed."""
        cm = ContractManager()
        contract = cm.offer_contract(_make_offer())
        cm.reject_contract(contract.contract_id)

        with pytest.raises(InvalidContractTransitionError):
            cm.negotiate_contract(contract.contract_id)


# ---------------------------------------------------------------------------
# Contract not found
# ---------------------------------------------------------------------------


class TestContractNotFound:
    """Tests for ContractNotFoundError on missing contracts."""

    def test_get_nonexistent_contract(self) -> None:
        """Getting a nonexistent contract should raise ContractNotFoundError."""
        cm = ContractManager()
        with pytest.raises(ContractNotFoundError) as exc_info:
            cm.get_contract("nonexistent-id")
        assert exc_info.value.contract_id == "nonexistent-id"

    def test_negotiate_nonexistent_contract(self) -> None:
        """Negotiating a nonexistent contract should raise ContractNotFoundError."""
        cm = ContractManager()
        with pytest.raises(ContractNotFoundError):
            cm.negotiate_contract("nonexistent-id")

    def test_accept_nonexistent_contract(self) -> None:
        """Accepting a nonexistent contract should raise ContractNotFoundError."""
        cm = ContractManager()
        with pytest.raises(ContractNotFoundError):
            cm.accept_contract("nonexistent-id")


# ---------------------------------------------------------------------------
# Contract validity checks
# ---------------------------------------------------------------------------


class TestContractValidityCheck:
    """Tests for check_contract_valid including auto-expiry and emergency override."""

    def test_active_contract_within_window_is_valid(self) -> None:
        """An ACTIVE contract within its validity window should be valid."""
        cm = ContractManager()
        contract = cm.offer_contract(_make_offer())
        cm.negotiate_contract(contract.contract_id)
        cm.accept_contract(contract.contract_id)
        assert cm.check_contract_valid(contract.contract_id) is True

    def test_offered_contract_is_not_valid(self) -> None:
        """An OFFERED (non-ACTIVE) contract should not be valid."""
        cm = ContractManager()
        contract = cm.offer_contract(_make_offer())
        assert cm.check_contract_valid(contract.contract_id) is False

    def test_expired_window_auto_expires_contract(self) -> None:
        """An ACTIVE contract past its validity window should auto-expire."""
        now = _utc_now()
        offer = _make_offer(
            valid_from=now - timedelta(days=100),
            valid_until=now - timedelta(days=1),
        )
        cm = ContractManager()
        contract = cm.offer_contract(offer)
        cm.negotiate_contract(contract.contract_id)
        cm.accept_contract(contract.contract_id)

        assert cm.check_contract_valid(contract.contract_id) is False
        assert cm.get_contract(contract.contract_id).status == ContractStatus.EXPIRED

    def test_future_valid_from_is_not_valid(self) -> None:
        """An ACTIVE contract whose valid_from is in the future is not valid."""
        now = _utc_now()
        offer = _make_offer(
            valid_from=now + timedelta(days=10),
            valid_until=now + timedelta(days=100),
        )
        cm = ContractManager()
        contract = cm.offer_contract(offer)
        cm.negotiate_contract(contract.contract_id)
        cm.accept_contract(contract.contract_id)

        assert cm.check_contract_valid(contract.contract_id) is False

    def test_emergency_override_bypasses_validity_window(self) -> None:
        """Emergency override should bypass validity window when flags are set."""
        now = _utc_now()
        offer = _make_offer(
            valid_from=now - timedelta(days=100),
            valid_until=now - timedelta(days=1),
            emergency_override=True,
        )
        cm = ContractManager()
        contract = cm.offer_contract(offer)
        cm.negotiate_contract(contract.contract_id)
        cm.accept_contract(contract.contract_id)

        # Without emergency flag, contract is expired
        assert cm.check_contract_valid(contract.contract_id, emergency=False) is False

    def test_emergency_override_requires_contract_flag(self) -> None:
        """Emergency flag on request is not enough; contract must also allow it."""
        now = _utc_now()
        offer = _make_offer(
            valid_from=now - timedelta(days=100),
            valid_until=now - timedelta(days=1),
            emergency_override=False,
        )
        cm = ContractManager()
        contract = cm.offer_contract(offer)
        cm.negotiate_contract(contract.contract_id)
        cm.accept_contract(contract.contract_id)

        # Emergency request but contract doesn't allow it
        assert cm.check_contract_valid(contract.contract_id, emergency=True) is False


# ---------------------------------------------------------------------------
# Contract listing and filtering
# ---------------------------------------------------------------------------


class TestContractListing:
    """Tests for listing and filtering contracts."""

    def test_list_contracts_empty(self) -> None:
        """Listing contracts on an empty manager returns an empty list."""
        cm = ContractManager()
        assert cm.list_contracts() == []

    def test_list_contracts_filter_by_provider(self) -> None:
        """Filtering by provider_id should return only matching contracts."""
        cm = ContractManager()
        cm.offer_contract(_make_offer(provider_id="dso-001"))
        cm.offer_contract(_make_offer(provider_id="dso-002"))

        results = cm.list_contracts(provider_id="dso-001")
        assert len(results) == 1
        assert results[0].provider_id == "dso-001"

    def test_list_contracts_filter_by_status(self) -> None:
        """Filtering by status should return only matching contracts."""
        cm = ContractManager()
        c1 = cm.offer_contract(_make_offer())
        cm.offer_contract(_make_offer())  # second offer stays OFFERED
        cm.negotiate_contract(c1.contract_id)

        results = cm.list_contracts(status=ContractStatus.NEGOTIATING)
        assert len(results) == 1
        assert results[0].contract_id == c1.contract_id

    def test_updated_at_changes_on_transition(self) -> None:
        """updated_at should be updated after a state transition."""
        cm = ContractManager()
        contract = cm.offer_contract(_make_offer())
        original_updated_at = contract.updated_at

        cm.negotiate_contract(contract.contract_id)
        updated_contract = cm.get_contract(contract.contract_id)
        assert updated_contract.updated_at >= original_updated_at
