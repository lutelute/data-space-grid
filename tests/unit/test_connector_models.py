"""Unit tests for connector core data models.

Tests that Participant, DataUsageContract, PolicyRule, and AuditEntry models
validate correctly and reject invalid data.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from src.connector.models import (
    AuditAction,
    AuditEntry,
    AuditOutcome,
    ContractOffer,
    ContractStatus,
    DataAsset,
    DataUsageContract,
    Participant,
    PolicyEffect,
    PolicyRule,
)
from src.semantic.cim import SensitivityTier


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Participant model
# ---------------------------------------------------------------------------


class TestParticipant:
    """Tests for the Participant model."""

    def test_valid_participant_minimal(self) -> None:
        """A participant with only required fields should validate."""
        p = Participant(id="dso-001", name="DSO Alpha", organization="GridCorp")
        assert p.id == "dso-001"
        assert p.name == "DSO Alpha"
        assert p.organization == "GridCorp"
        assert p.roles == []
        assert p.certificate_dn is None
        assert p.registered_at.tzinfo is not None

    def test_valid_participant_full(self) -> None:
        """A participant with all fields populated should validate."""
        now = _utc_now()
        p = Participant(
            id="agg-001",
            name="Aggregator Beta",
            organization="FlexPower",
            roles=["aggregator", "prosumer"],
            certificate_dn="CN=agg-001.flexpower.eu",
            registered_at=now,
        )
        assert p.id == "agg-001"
        assert p.roles == ["aggregator", "prosumer"]
        assert p.certificate_dn == "CN=agg-001.flexpower.eu"
        assert p.registered_at == now

    def test_participant_missing_required_id(self) -> None:
        """Omitting the required 'id' field should raise ValidationError."""
        with pytest.raises(ValidationError):
            Participant(name="DSO Alpha", organization="GridCorp")  # type: ignore[call-arg]

    def test_participant_missing_required_name(self) -> None:
        """Omitting the required 'name' field should raise ValidationError."""
        with pytest.raises(ValidationError):
            Participant(id="dso-001", organization="GridCorp")  # type: ignore[call-arg]

    def test_participant_missing_required_organization(self) -> None:
        """Omitting the required 'organization' field should raise ValidationError."""
        with pytest.raises(ValidationError):
            Participant(id="dso-001", name="DSO Alpha")  # type: ignore[call-arg]

    def test_participant_registered_at_is_utc(self) -> None:
        """The auto-generated registered_at timestamp should be timezone-aware UTC."""
        p = Participant(id="p-1", name="Test", organization="Org")
        assert p.registered_at.tzinfo is not None
        assert p.registered_at.tzinfo == timezone.utc


# ---------------------------------------------------------------------------
# DataAsset model
# ---------------------------------------------------------------------------


class TestDataAsset:
    """Tests for the DataAsset model."""

    def test_valid_data_asset(self) -> None:
        """A fully populated DataAsset should validate correctly."""
        asset = DataAsset(
            id="asset-fc-01",
            provider_id="dso-001",
            name="Feeder Constraint F-12",
            data_type="feeder_constraint",
            sensitivity=SensitivityTier.MEDIUM,
            endpoint="https://dso.local/api/feeders/F-12/constraint",
        )
        assert asset.id == "asset-fc-01"
        assert asset.sensitivity == SensitivityTier.MEDIUM
        assert asset.description == ""
        assert asset.policy_metadata == {}

    def test_data_asset_missing_sensitivity(self) -> None:
        """Omitting sensitivity should raise ValidationError."""
        with pytest.raises(ValidationError):
            DataAsset(
                id="asset-01",
                provider_id="dso-001",
                name="Test",
                data_type="test",
                endpoint="https://example.com/api",
            )  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# ContractOffer model
# ---------------------------------------------------------------------------


class TestContractOffer:
    """Tests for the ContractOffer model."""

    def test_valid_contract_offer(self) -> None:
        """A valid ContractOffer should pass validation."""
        now = _utc_now()
        offer = ContractOffer(
            offer_id="offer-001",
            provider_id="dso-001",
            consumer_id="agg-001",
            asset_id="asset-fc-01",
            purpose="congestion_management",
            allowed_operations=["read"],
            retention_days=30,
            valid_from=now,
            valid_until=now + timedelta(days=90),
        )
        assert offer.offer_id == "offer-001"
        assert offer.retention_days == 30
        assert offer.redistribution_allowed is False
        assert offer.anonymization_required is False
        assert offer.emergency_override is False

    def test_contract_offer_retention_days_must_be_positive(self) -> None:
        """retention_days < 1 should raise ValidationError (ge=1)."""
        now = _utc_now()
        with pytest.raises(ValidationError):
            ContractOffer(
                offer_id="offer-001",
                provider_id="dso-001",
                consumer_id="agg-001",
                asset_id="asset-fc-01",
                purpose="test",
                retention_days=0,
                valid_from=now,
                valid_until=now + timedelta(days=90),
            )

    def test_contract_offer_missing_purpose(self) -> None:
        """Omitting the required 'purpose' field should raise ValidationError."""
        now = _utc_now()
        with pytest.raises(ValidationError):
            ContractOffer(
                offer_id="offer-001",
                provider_id="dso-001",
                consumer_id="agg-001",
                asset_id="asset-fc-01",
                retention_days=30,
                valid_from=now,
                valid_until=now + timedelta(days=90),
            )  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# DataUsageContract model
# ---------------------------------------------------------------------------


class TestDataUsageContract:
    """Tests for the DataUsageContract model."""

    def test_valid_contract(self) -> None:
        """A fully populated DataUsageContract should validate."""
        now = _utc_now()
        contract = DataUsageContract(
            contract_id="c-001",
            provider_id="dso-001",
            consumer_id="agg-001",
            asset_id="asset-fc-01",
            purpose="congestion_management",
            allowed_operations=["read", "aggregate"],
            retention_days=30,
            valid_from=now,
            valid_until=now + timedelta(days=90),
        )
        assert contract.contract_id == "c-001"
        assert contract.status == ContractStatus.OFFERED
        assert contract.redistribution_allowed is False
        assert contract.anonymization_required is False
        assert contract.emergency_override is False

    def test_contract_retention_days_must_be_positive(self) -> None:
        """retention_days < 1 should raise ValidationError (ge=1)."""
        now = _utc_now()
        with pytest.raises(ValidationError):
            DataUsageContract(
                contract_id="c-001",
                provider_id="dso-001",
                consumer_id="agg-001",
                asset_id="asset-fc-01",
                purpose="test",
                allowed_operations=["read"],
                retention_days=0,
                valid_from=now,
                valid_until=now + timedelta(days=90),
            )

    def test_contract_missing_contract_id(self) -> None:
        """Omitting contract_id should raise ValidationError."""
        now = _utc_now()
        with pytest.raises(ValidationError):
            DataUsageContract(
                provider_id="dso-001",
                consumer_id="agg-001",
                asset_id="asset-fc-01",
                purpose="test",
                allowed_operations=["read"],
                retention_days=30,
                valid_from=now,
                valid_until=now + timedelta(days=90),
            )  # type: ignore[call-arg]

    def test_contract_status_enum_values(self) -> None:
        """ContractStatus should contain all expected lifecycle states."""
        expected = {"offered", "negotiating", "active", "expired", "revoked", "rejected"}
        actual = {s.value for s in ContractStatus}
        assert actual == expected


# ---------------------------------------------------------------------------
# PolicyRule model
# ---------------------------------------------------------------------------


class TestPolicyRule:
    """Tests for the PolicyRule model."""

    def test_valid_policy_rule_minimal(self) -> None:
        """A PolicyRule with only rule_id should validate with defaults."""
        rule = PolicyRule(rule_id="rule-001")
        assert rule.rule_id == "rule-001"
        assert rule.asset_id is None
        assert rule.sensitivity is None
        assert rule.allowed_roles == []
        assert rule.allowed_operations == []
        assert rule.allowed_purposes == []
        assert rule.effect == PolicyEffect.ALLOW
        assert rule.priority == 0

    def test_valid_policy_rule_full(self) -> None:
        """A fully populated PolicyRule should validate."""
        rule = PolicyRule(
            rule_id="rule-002",
            asset_id="asset-fc-01",
            sensitivity=SensitivityTier.HIGH,
            allowed_roles=["dso_operator"],
            allowed_operations=["read"],
            allowed_purposes=["congestion_management"],
            effect=PolicyEffect.DENY,
            priority=100,
        )
        assert rule.effect == PolicyEffect.DENY
        assert rule.priority == 100
        assert rule.sensitivity == SensitivityTier.HIGH

    def test_policy_rule_priority_must_be_non_negative(self) -> None:
        """priority < 0 should raise ValidationError (ge=0)."""
        with pytest.raises(ValidationError):
            PolicyRule(rule_id="rule-neg", priority=-1)

    def test_policy_rule_missing_rule_id(self) -> None:
        """Omitting rule_id should raise ValidationError."""
        with pytest.raises(ValidationError):
            PolicyRule()  # type: ignore[call-arg]

    def test_policy_effect_enum_values(self) -> None:
        """PolicyEffect should contain allow and deny."""
        assert PolicyEffect.ALLOW.value == "allow"
        assert PolicyEffect.DENY.value == "deny"


# ---------------------------------------------------------------------------
# AuditEntry model
# ---------------------------------------------------------------------------


class TestAuditEntry:
    """Tests for the AuditEntry model."""

    def test_valid_audit_entry(self) -> None:
        """A fully populated AuditEntry should validate."""
        entry = AuditEntry(
            requester_id="agg-001",
            provider_id="dso-001",
            asset_id="asset-fc-01",
            purpose_tag="congestion_management",
            request_hash="abc123" * 10 + "abcd",
            response_hash="def456" * 10 + "defg",
            contract_id="c-001",
            action=AuditAction.READ,
            outcome=AuditOutcome.SUCCESS,
        )
        assert entry.requester_id == "agg-001"
        assert entry.action == AuditAction.READ
        assert entry.outcome == AuditOutcome.SUCCESS
        assert entry.timestamp.tzinfo is not None

    def test_audit_entry_missing_requester_id(self) -> None:
        """Omitting requester_id should raise ValidationError."""
        with pytest.raises(ValidationError):
            AuditEntry(
                provider_id="dso-001",
                asset_id="asset-fc-01",
                purpose_tag="test",
                request_hash="abc",
                response_hash="def",
                contract_id="c-001",
                action=AuditAction.READ,
                outcome=AuditOutcome.SUCCESS,
            )  # type: ignore[call-arg]

    def test_audit_entry_missing_action(self) -> None:
        """Omitting action should raise ValidationError."""
        with pytest.raises(ValidationError):
            AuditEntry(
                requester_id="agg-001",
                provider_id="dso-001",
                asset_id="asset-fc-01",
                purpose_tag="test",
                request_hash="abc",
                response_hash="def",
                contract_id="c-001",
                outcome=AuditOutcome.SUCCESS,
            )  # type: ignore[call-arg]

    def test_audit_entry_missing_outcome(self) -> None:
        """Omitting outcome should raise ValidationError."""
        with pytest.raises(ValidationError):
            AuditEntry(
                requester_id="agg-001",
                provider_id="dso-001",
                asset_id="asset-fc-01",
                purpose_tag="test",
                request_hash="abc",
                response_hash="def",
                contract_id="c-001",
                action=AuditAction.READ,
            )  # type: ignore[call-arg]

    def test_audit_action_enum_values(self) -> None:
        """AuditAction should contain all expected action types."""
        expected = {"read", "write", "dispatch", "subscribe"}
        actual = {a.value for a in AuditAction}
        assert actual == expected

    def test_audit_outcome_enum_values(self) -> None:
        """AuditOutcome should contain all expected outcome types."""
        expected = {"success", "denied", "error"}
        actual = {o.value for o in AuditOutcome}
        assert actual == expected

    def test_audit_entry_all_fields_populated(self) -> None:
        """Every field on a valid AuditEntry should be non-empty."""
        entry = AuditEntry(
            requester_id="agg-001",
            provider_id="dso-001",
            asset_id="asset-fc-01",
            purpose_tag="congestion_management",
            request_hash="a" * 64,
            response_hash="b" * 64,
            contract_id="c-001",
            action=AuditAction.WRITE,
            outcome=AuditOutcome.DENIED,
        )
        assert entry.requester_id
        assert entry.provider_id
        assert entry.asset_id
        assert entry.purpose_tag
        assert entry.request_hash
        assert entry.response_hash
        assert entry.contract_id
        assert entry.action is not None
        assert entry.outcome is not None
        assert entry.timestamp is not None
