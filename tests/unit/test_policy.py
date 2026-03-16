"""Unit tests for the policy enforcement engine.

Tests purpose constraints, sensitivity tier checks, and emergency override
behavior for DSO operators.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.connector.models import (
    ContractStatus,
    DataAsset,
    DataUsageContract,
    Participant,
    PolicyEffect,
    PolicyRule,
)
from src.connector.policy import (
    AssetNotRegisteredError,
    ParticipantNotRegisteredError,
    PolicyDecision,
    PolicyEngine,
)
from src.semantic.cim import SensitivityTier


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _make_participant(
    pid: str = "agg-001",
    roles: list[str] | None = None,
) -> Participant:
    """Create a Participant with sensible defaults for testing."""
    return Participant(
        id=pid,
        name=f"Participant {pid}",
        organization="TestOrg",
        roles=roles or ["aggregator"],
    )


def _make_asset(
    asset_id: str = "asset-fc-01",
    sensitivity: SensitivityTier = SensitivityTier.MEDIUM,
) -> DataAsset:
    """Create a DataAsset with sensible defaults for testing."""
    return DataAsset(
        id=asset_id,
        provider_id="dso-001",
        name=f"Asset {asset_id}",
        data_type="feeder_constraint",
        sensitivity=sensitivity,
        endpoint=f"https://dso.local/api/{asset_id}",
    )


def _make_contract(
    consumer_id: str = "agg-001",
    asset_id: str = "asset-fc-01",
    purpose: str = "congestion_management",
    status: ContractStatus = ContractStatus.ACTIVE,
    emergency_override: bool = False,
    valid_from: datetime | None = None,
    valid_until: datetime | None = None,
) -> DataUsageContract:
    """Create a DataUsageContract with sensible defaults for testing."""
    now = _utc_now()
    return DataUsageContract(
        contract_id="c-test-001",
        provider_id="dso-001",
        consumer_id=consumer_id,
        asset_id=asset_id,
        purpose=purpose,
        allowed_operations=["read"],
        retention_days=30,
        redistribution_allowed=False,
        emergency_override=emergency_override,
        status=status,
        valid_from=valid_from or now - timedelta(days=1),
        valid_until=valid_until or now + timedelta(days=90),
    )


def _setup_engine(
    participant: Participant | None = None,
    asset: DataAsset | None = None,
) -> PolicyEngine:
    """Create a PolicyEngine with a registered participant and asset."""
    engine = PolicyEngine()
    engine.register_participant(participant or _make_participant())
    engine.register_asset(asset or _make_asset())
    return engine


# ---------------------------------------------------------------------------
# Purpose constraint checks
# ---------------------------------------------------------------------------


class TestPurposeConstraints:
    """Tests that purpose constraints block unauthorized access."""

    def test_matching_purpose_allows_access(self) -> None:
        """A request with purpose matching the contract should be allowed."""
        engine = _setup_engine()
        contract = _make_contract(purpose="congestion_management")
        decision = engine.evaluate(
            requester_id="agg-001",
            asset_id="asset-fc-01",
            contract=contract,
            purpose="congestion_management",
            operation="read",
        )
        assert decision.allowed is True

    def test_mismatched_purpose_denies_access(self) -> None:
        """A request with a purpose different from the contract should be denied."""
        engine = _setup_engine()
        contract = _make_contract(purpose="congestion_management")
        decision = engine.evaluate(
            requester_id="agg-001",
            asset_id="asset-fc-01",
            contract=contract,
            purpose="billing",
            operation="read",
        )
        assert decision.allowed is False
        assert "purpose" in decision.reason.lower()

    def test_empty_purpose_denied(self) -> None:
        """An empty purpose string should not match a non-empty contract purpose."""
        engine = _setup_engine()
        contract = _make_contract(purpose="congestion_management")
        decision = engine.evaluate(
            requester_id="agg-001",
            asset_id="asset-fc-01",
            contract=contract,
            purpose="",
            operation="read",
        )
        assert decision.allowed is False


# ---------------------------------------------------------------------------
# Sensitivity tier checks
# ---------------------------------------------------------------------------


class TestSensitivityTierChecks:
    """Tests that sensitivity tier restrictions are enforced based on roles."""

    def test_aggregator_can_access_medium_tier(self) -> None:
        """An aggregator should be allowed to access MEDIUM sensitivity assets."""
        engine = _setup_engine(
            participant=_make_participant(roles=["aggregator"]),
            asset=_make_asset(sensitivity=SensitivityTier.MEDIUM),
        )
        contract = _make_contract()
        decision = engine.evaluate(
            requester_id="agg-001",
            asset_id="asset-fc-01",
            contract=contract,
            purpose="congestion_management",
            operation="read",
        )
        assert decision.allowed is True

    def test_aggregator_cannot_access_high_tier(self) -> None:
        """An aggregator should be denied access to HIGH sensitivity assets."""
        engine = _setup_engine(
            participant=_make_participant(roles=["aggregator"]),
            asset=_make_asset(sensitivity=SensitivityTier.HIGH),
        )
        contract = _make_contract()
        decision = engine.evaluate(
            requester_id="agg-001",
            asset_id="asset-fc-01",
            contract=contract,
            purpose="congestion_management",
            operation="read",
        )
        assert decision.allowed is False
        assert "sensitivity" in decision.reason.lower() or "roles" in decision.reason.lower()

    def test_dso_operator_can_access_high_tier(self) -> None:
        """A DSO operator should be allowed to access HIGH sensitivity assets."""
        engine = _setup_engine(
            participant=_make_participant(pid="dso-op-001", roles=["dso_operator"]),
            asset=_make_asset(sensitivity=SensitivityTier.HIGH),
        )
        contract = _make_contract(consumer_id="dso-op-001")
        decision = engine.evaluate(
            requester_id="dso-op-001",
            asset_id="asset-fc-01",
            contract=contract,
            purpose="congestion_management",
            operation="read",
        )
        assert decision.allowed is True

    def test_dso_operator_can_access_medium_tier(self) -> None:
        """A DSO operator should be allowed to access MEDIUM sensitivity assets."""
        engine = _setup_engine(
            participant=_make_participant(pid="dso-op-001", roles=["dso_operator"]),
            asset=_make_asset(sensitivity=SensitivityTier.MEDIUM),
        )
        contract = _make_contract(consumer_id="dso-op-001")
        decision = engine.evaluate(
            requester_id="dso-op-001",
            asset_id="asset-fc-01",
            contract=contract,
            purpose="congestion_management",
            operation="read",
        )
        assert decision.allowed is True

    def test_prosumer_can_access_high_privacy_tier(self) -> None:
        """A prosumer should be allowed to access HIGH_PRIVACY assets."""
        engine = _setup_engine(
            participant=_make_participant(pid="pros-001", roles=["prosumer"]),
            asset=_make_asset(sensitivity=SensitivityTier.HIGH_PRIVACY),
        )
        contract = _make_contract(consumer_id="pros-001")
        decision = engine.evaluate(
            requester_id="pros-001",
            asset_id="asset-fc-01",
            contract=contract,
            purpose="congestion_management",
            operation="read",
        )
        assert decision.allowed is True

    def test_prosumer_cannot_access_high_tier(self) -> None:
        """A prosumer should not access HIGH sensitivity assets."""
        engine = _setup_engine(
            participant=_make_participant(pid="pros-001", roles=["prosumer"]),
            asset=_make_asset(sensitivity=SensitivityTier.HIGH),
        )
        contract = _make_contract(consumer_id="pros-001")
        decision = engine.evaluate(
            requester_id="pros-001",
            asset_id="asset-fc-01",
            contract=contract,
            purpose="congestion_management",
            operation="read",
        )
        assert decision.allowed is False

    def test_prosumer_cannot_access_medium_tier(self) -> None:
        """A prosumer should not access MEDIUM sensitivity assets (only dso_operator, aggregator)."""
        engine = _setup_engine(
            participant=_make_participant(pid="pros-001", roles=["prosumer"]),
            asset=_make_asset(sensitivity=SensitivityTier.MEDIUM),
        )
        contract = _make_contract(consumer_id="pros-001")
        decision = engine.evaluate(
            requester_id="pros-001",
            asset_id="asset-fc-01",
            contract=contract,
            purpose="congestion_management",
            operation="read",
        )
        assert decision.allowed is False


# ---------------------------------------------------------------------------
# Emergency override for DSO
# ---------------------------------------------------------------------------


class TestEmergencyOverride:
    """Tests that DSO emergency override grants access correctly."""

    def test_dso_emergency_override_allows_access(self) -> None:
        """A DSO operator with emergency flag and contract override should be allowed."""
        engine = _setup_engine(
            participant=_make_participant(pid="dso-op-001", roles=["dso_operator"]),
            asset=_make_asset(sensitivity=SensitivityTier.HIGH),
        )
        contract = _make_contract(
            consumer_id="dso-op-001",
            emergency_override=True,
        )
        decision = engine.evaluate(
            requester_id="dso-op-001",
            asset_id="asset-fc-01",
            contract=contract,
            purpose="congestion_management",
            operation="read",
            emergency=True,
        )
        assert decision.allowed is True
        assert decision.emergency_override is True

    def test_non_dso_emergency_override_denied(self) -> None:
        """A non-DSO role requesting emergency override should be denied."""
        engine = _setup_engine(
            participant=_make_participant(pid="agg-001", roles=["aggregator"]),
            asset=_make_asset(sensitivity=SensitivityTier.MEDIUM),
        )
        contract = _make_contract(
            consumer_id="agg-001",
            emergency_override=True,
        )
        decision = engine.evaluate(
            requester_id="agg-001",
            asset_id="asset-fc-01",
            contract=contract,
            purpose="congestion_management",
            operation="read",
            emergency=True,
        )
        assert decision.allowed is False
        assert decision.emergency_override is True
        assert "emergency" in decision.reason.lower()

    def test_emergency_without_contract_flag_denied(self) -> None:
        """Emergency request on a contract without emergency_override should be denied."""
        engine = _setup_engine(
            participant=_make_participant(pid="dso-op-001", roles=["dso_operator"]),
            asset=_make_asset(sensitivity=SensitivityTier.HIGH),
        )
        contract = _make_contract(
            consumer_id="dso-op-001",
            emergency_override=False,
        )
        decision = engine.evaluate(
            requester_id="dso-op-001",
            asset_id="asset-fc-01",
            contract=contract,
            purpose="congestion_management",
            operation="read",
            emergency=True,
        )
        assert decision.allowed is False
        assert decision.emergency_override is True

    def test_emergency_requires_active_contract(self) -> None:
        """Emergency override should require an ACTIVE contract."""
        engine = _setup_engine(
            participant=_make_participant(pid="dso-op-001", roles=["dso_operator"]),
            asset=_make_asset(sensitivity=SensitivityTier.HIGH),
        )
        contract = _make_contract(
            consumer_id="dso-op-001",
            emergency_override=True,
            status=ContractStatus.OFFERED,
        )
        decision = engine.evaluate(
            requester_id="dso-op-001",
            asset_id="asset-fc-01",
            contract=contract,
            purpose="congestion_management",
            operation="read",
            emergency=True,
        )
        assert decision.allowed is False


# ---------------------------------------------------------------------------
# Contract validity checks within policy engine
# ---------------------------------------------------------------------------


class TestPolicyContractValidity:
    """Tests that the policy engine checks contract validity."""

    def test_non_active_contract_denied(self) -> None:
        """A contract that is not ACTIVE should be denied."""
        engine = _setup_engine()
        contract = _make_contract(status=ContractStatus.OFFERED)
        decision = engine.evaluate(
            requester_id="agg-001",
            asset_id="asset-fc-01",
            contract=contract,
            purpose="congestion_management",
            operation="read",
        )
        assert decision.allowed is False
        assert "not active" in decision.reason.lower() or "status" in decision.reason.lower()

    def test_wrong_asset_id_denied(self) -> None:
        """A contract covering a different asset should be denied."""
        engine = _setup_engine()
        contract = _make_contract(asset_id="asset-other")
        decision = engine.evaluate(
            requester_id="agg-001",
            asset_id="asset-fc-01",
            contract=contract,
            purpose="congestion_management",
            operation="read",
        )
        assert decision.allowed is False

    def test_wrong_consumer_denied(self) -> None:
        """A contract for a different consumer should be denied."""
        engine = _setup_engine()
        contract = _make_contract(consumer_id="other-consumer")
        decision = engine.evaluate(
            requester_id="agg-001",
            asset_id="asset-fc-01",
            contract=contract,
            purpose="congestion_management",
            operation="read",
        )
        assert decision.allowed is False


# ---------------------------------------------------------------------------
# Redistribution and retention constraints
# ---------------------------------------------------------------------------


class TestRedistributionRetention:
    """Tests for redistribution and retention policy constraints."""

    def test_redistribution_denied_when_not_allowed(self) -> None:
        """Redistribution request should be denied when contract disallows it."""
        engine = _setup_engine()
        contract = _make_contract()
        assert contract.redistribution_allowed is False
        decision = engine.evaluate(
            requester_id="agg-001",
            asset_id="asset-fc-01",
            contract=contract,
            purpose="congestion_management",
            operation="read",
            redistribution_requested=True,
        )
        assert decision.allowed is False
        assert "redistribution" in decision.reason.lower()

    def test_retention_exceeding_limit_denied(self) -> None:
        """Retention days exceeding contract limit should be denied."""
        engine = _setup_engine()
        contract = _make_contract()
        assert contract.retention_days == 30
        decision = engine.evaluate(
            requester_id="agg-001",
            asset_id="asset-fc-01",
            contract=contract,
            purpose="congestion_management",
            operation="read",
            retention_days_requested=60,
        )
        assert decision.allowed is False
        assert "retention" in decision.reason.lower()

    def test_retention_within_limit_allowed(self) -> None:
        """Retention days within contract limit should be allowed."""
        engine = _setup_engine()
        contract = _make_contract()
        decision = engine.evaluate(
            requester_id="agg-001",
            asset_id="asset-fc-01",
            contract=contract,
            purpose="congestion_management",
            operation="read",
            retention_days_requested=15,
        )
        assert decision.allowed is True


# ---------------------------------------------------------------------------
# Custom policy rules
# ---------------------------------------------------------------------------


class TestCustomPolicyRules:
    """Tests for custom PolicyRule evaluation."""

    def test_explicit_deny_rule_blocks_access(self) -> None:
        """A DENY rule matching the request should block access."""
        engine = _setup_engine()
        deny_rule = PolicyRule(
            rule_id="deny-agg-read",
            sensitivity=SensitivityTier.MEDIUM,
            allowed_roles=["aggregator"],
            allowed_operations=["read"],
            effect=PolicyEffect.DENY,
            priority=100,
        )
        engine.add_rule(deny_rule)

        contract = _make_contract()
        decision = engine.evaluate(
            requester_id="agg-001",
            asset_id="asset-fc-01",
            contract=contract,
            purpose="congestion_management",
            operation="read",
        )
        assert decision.allowed is False
        assert "deny" in decision.reason.lower()

    def test_explicit_allow_rule_overrides_default(self) -> None:
        """An ALLOW rule should override default tier checks."""
        # Prosumer normally can't access MEDIUM; a custom rule can allow it.
        engine = _setup_engine(
            participant=_make_participant(pid="pros-001", roles=["prosumer"]),
            asset=_make_asset(sensitivity=SensitivityTier.MEDIUM),
        )
        allow_rule = PolicyRule(
            rule_id="allow-prosumer-medium",
            sensitivity=SensitivityTier.MEDIUM,
            allowed_roles=["prosumer"],
            allowed_operations=["read"],
            effect=PolicyEffect.ALLOW,
            priority=50,
        )
        engine.add_rule(allow_rule)

        contract = _make_contract(consumer_id="pros-001")
        decision = engine.evaluate(
            requester_id="pros-001",
            asset_id="asset-fc-01",
            contract=contract,
            purpose="congestion_management",
            operation="read",
        )
        assert decision.allowed is True


# ---------------------------------------------------------------------------
# Registration errors
# ---------------------------------------------------------------------------


class TestRegistrationErrors:
    """Tests that missing registrations raise appropriate errors."""

    def test_unregistered_participant_raises(self) -> None:
        """Evaluating with an unregistered participant should raise."""
        engine = PolicyEngine()
        engine.register_asset(_make_asset())
        contract = _make_contract()
        with pytest.raises(ParticipantNotRegisteredError):
            engine.evaluate(
                requester_id="unknown",
                asset_id="asset-fc-01",
                contract=contract,
                purpose="congestion_management",
                operation="read",
            )

    def test_unregistered_asset_raises(self) -> None:
        """Evaluating with an unregistered asset should raise."""
        engine = PolicyEngine()
        engine.register_participant(_make_participant())
        contract = _make_contract()
        with pytest.raises(AssetNotRegisteredError):
            engine.evaluate(
                requester_id="agg-001",
                asset_id="unknown-asset",
                contract=contract,
                purpose="congestion_management",
                operation="read",
            )
