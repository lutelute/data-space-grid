"""Policy enforcement engine for the Federated Data Space Connector.

This module evaluates access requests against contracts, sensitivity tiers,
role permissions, and purpose constraints.  No data exchange proceeds unless
the PolicyEngine returns an explicit ALLOW decision (spec Pattern 3 –
Contract-Gated Data Access, spec requirement 6 – Policy and Contract
Management).

Evaluation order:
  1. Emergency override – if the requester holds the ``dso_operator`` role,
     the ``emergency`` flag is set, and an active contract with
     ``emergency_override=True`` covers the asset, access is granted
     immediately.  Emergency decisions are tagged so the audit layer can
     record them separately.
  2. Contract check – the requester must hold a valid, ACTIVE contract for
     the requested asset whose validity window contains the current time.
  3. Purpose check – the stated purpose must match the contract's allowed
     purpose.
  4. Role / sensitivity tier check – the requester's roles must be permitted
     for the asset's sensitivity tier.  Custom ``PolicyRule`` objects can
     further restrict (or widen) access.
  5. Redistribution / retention constraints – if the request implies
     redistribution or exceeds the contract's retention window, the request
     is denied.

Key design decisions:
  - The ``PolicyEngine`` keeps in-memory registries of participants, assets,
    and policy rules.  For production use these would be backed by a
    persistent database, but the interface stays the same.
  - Default sensitivity-tier-to-role mappings follow the spec data
    classification table.  Custom :class:`PolicyRule` instances can override
    or extend these defaults.
  - ``PolicyDecision`` is a Pydantic model so it can be serialised directly
    into audit entries or API responses.
  - DENY rules have higher natural priority than ALLOW rules at the same
    priority level (deny-overrides).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field

from src.connector.models import (
    ContractStatus,
    DataAsset,
    DataUsageContract,
    Participant,
    PolicyEffect,
    PolicyRule,
)
from src.semantic.cim import SensitivityTier


def _utc_now() -> datetime:
    """Return the current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Default sensitivity-tier-to-role mapping (from spec Data Sensitivity table)
# ---------------------------------------------------------------------------

_DEFAULT_TIER_ROLES: dict[SensitivityTier, set[str]] = {
    SensitivityTier.HIGH: {"dso_operator"},
    SensitivityTier.MEDIUM: {"dso_operator", "aggregator"},
    SensitivityTier.HIGH_PRIVACY: {"dso_operator", "aggregator", "prosumer"},
}

# Roles that are permitted to invoke the emergency override path.
_EMERGENCY_ROLES: set[str] = {"dso_operator"}


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class PolicyError(Exception):
    """Base exception for policy enforcement errors."""


class ParticipantNotRegisteredError(PolicyError):
    """Raised when a referenced participant is not registered with the engine."""

    def __init__(self, participant_id: str) -> None:
        self.participant_id = participant_id
        super().__init__(f"Participant not registered: '{participant_id}'")


class AssetNotRegisteredError(PolicyError):
    """Raised when a referenced data asset is not registered with the engine."""

    def __init__(self, asset_id: str) -> None:
        self.asset_id = asset_id
        super().__init__(f"Asset not registered: '{asset_id}'")


class PolicyRuleNotFoundError(PolicyError):
    """Raised when a referenced policy rule does not exist."""

    def __init__(self, rule_id: str) -> None:
        self.rule_id = rule_id
        super().__init__(f"Policy rule not found: '{rule_id}'")


# ---------------------------------------------------------------------------
# Policy decision model
# ---------------------------------------------------------------------------


class PolicyDecision(BaseModel):
    """Result of a policy evaluation for a data access request.

    Every access attempt yields a PolicyDecision that is passed to the audit
    layer.  ``allowed=False`` decisions include a human-readable ``reason``
    explaining why the request was denied.
    """

    allowed: bool = Field(
        ..., description="Whether the request is permitted"
    )
    reason: str = Field(
        ..., description="Human-readable explanation of the decision"
    )
    emergency_override: bool = Field(
        default=False,
        description="Whether the decision was made via the emergency override path",
    )
    contract_id: Optional[str] = Field(
        default=None,
        description="ID of the contract that authorised the access (if allowed)",
    )


# ---------------------------------------------------------------------------
# PolicyEngine
# ---------------------------------------------------------------------------


class PolicyEngine:
    """Evaluates data access requests against contracts, roles, and policies.

    The engine maintains in-memory registries of participants, assets, and
    custom policy rules.  The main entry point is :meth:`evaluate`, which
    returns a :class:`PolicyDecision`.

    Usage::

        engine = PolicyEngine()
        engine.register_participant(participant)
        engine.register_asset(asset)
        decision = engine.evaluate(
            requester_id="agg-001",
            asset_id="asset-fc-01",
            contract=active_contract,
            purpose="congestion_management",
            operation="read",
        )
        if not decision.allowed:
            raise PermissionError(decision.reason)
    """

    def __init__(self) -> None:
        self._participants: dict[str, Participant] = {}
        self._assets: dict[str, DataAsset] = {}
        self._rules: dict[str, PolicyRule] = {}

    # -- registration helpers ------------------------------------------------

    def register_participant(self, participant: Participant) -> None:
        """Register a participant so the engine can resolve roles.

        Args:
            participant: The participant to register.
        """
        self._participants[participant.id] = participant

    def unregister_participant(self, participant_id: str) -> None:
        """Remove a participant from the registry.

        Raises:
            ParticipantNotRegisteredError: If the participant is not registered.
        """
        if participant_id not in self._participants:
            raise ParticipantNotRegisteredError(participant_id)
        del self._participants[participant_id]

    def get_participant(self, participant_id: str) -> Participant:
        """Retrieve a registered participant.

        Raises:
            ParticipantNotRegisteredError: If the participant is not registered.
        """
        try:
            return self._participants[participant_id]
        except KeyError:
            raise ParticipantNotRegisteredError(participant_id) from None

    def register_asset(self, asset: DataAsset) -> None:
        """Register a data asset so the engine can resolve sensitivity tiers.

        Args:
            asset: The data asset to register.
        """
        self._assets[asset.id] = asset

    def unregister_asset(self, asset_id: str) -> None:
        """Remove an asset from the registry.

        Raises:
            AssetNotRegisteredError: If the asset is not registered.
        """
        if asset_id not in self._assets:
            raise AssetNotRegisteredError(asset_id)
        del self._assets[asset_id]

    def get_asset(self, asset_id: str) -> DataAsset:
        """Retrieve a registered data asset.

        Raises:
            AssetNotRegisteredError: If the asset is not registered.
        """
        try:
            return self._assets[asset_id]
        except KeyError:
            raise AssetNotRegisteredError(asset_id) from None

    def add_rule(self, rule: PolicyRule) -> None:
        """Add a custom policy rule to the engine.

        Custom rules are evaluated after the default sensitivity-tier checks
        and can override decisions.  Higher ``priority`` rules are evaluated
        first.  DENY rules take precedence over ALLOW rules at the same
        priority level.

        Args:
            rule: The policy rule to add.
        """
        self._rules[rule.rule_id] = rule

    def remove_rule(self, rule_id: str) -> None:
        """Remove a policy rule from the engine.

        Raises:
            PolicyRuleNotFoundError: If the rule does not exist.
        """
        if rule_id not in self._rules:
            raise PolicyRuleNotFoundError(rule_id)
        del self._rules[rule_id]

    def list_rules(self, *, asset_id: Optional[str] = None) -> list[PolicyRule]:
        """Return policy rules, optionally filtered by asset ID."""
        results: list[PolicyRule] = []
        for rule in self._rules.values():
            if asset_id is not None and rule.asset_id is not None and rule.asset_id != asset_id:
                continue
            results.append(rule)
        return results

    # -- evaluation ----------------------------------------------------------

    def evaluate(
        self,
        *,
        requester_id: str,
        asset_id: str,
        contract: DataUsageContract,
        purpose: str,
        operation: str,
        emergency: bool = False,
        redistribution_requested: bool = False,
        retention_days_requested: Optional[int] = None,
    ) -> PolicyDecision:
        """Evaluate a data access request against all policy layers.

        Args:
            requester_id: Participant ID of the data requester.
            asset_id: ID of the data asset being requested.
            contract: The contract authorising this exchange.
            purpose: Stated purpose of the data access.
            operation: Requested operation (e.g. ``"read"``, ``"aggregate"``).
            emergency: Whether this is an emergency access request (DSO only).
            redistribution_requested: Whether the requester intends to
                redistribute the data.
            retention_days_requested: Number of days the requester intends to
                retain the data (``None`` means use contract default).

        Returns:
            A :class:`PolicyDecision` with the allow/deny verdict and reason.

        Raises:
            ParticipantNotRegisteredError: If the requester is not registered.
            AssetNotRegisteredError: If the asset is not registered.
        """
        participant = self.get_participant(requester_id)
        asset = self.get_asset(asset_id)

        # -- Step 1: Emergency override path ---------------------------------
        if emergency:
            decision = self._check_emergency_override(
                participant=participant,
                asset=asset,
                contract=contract,
            )
            if decision is not None:
                return decision

        # -- Step 2: Contract validity check ---------------------------------
        decision = self._check_contract_valid(contract=contract, requester_id=requester_id, asset_id=asset_id)
        if decision is not None:
            return decision

        # -- Step 3: Purpose check -------------------------------------------
        decision = self._check_purpose(contract=contract, purpose=purpose)
        if decision is not None:
            return decision

        # -- Step 4: Role / sensitivity tier check ---------------------------
        decision = self._check_role_sensitivity(
            participant=participant,
            asset=asset,
            operation=operation,
            purpose=purpose,
        )
        if decision is not None:
            return decision

        # -- Step 5: Redistribution / retention constraints ------------------
        decision = self._check_redistribution_retention(
            contract=contract,
            redistribution_requested=redistribution_requested,
            retention_days_requested=retention_days_requested,
        )
        if decision is not None:
            return decision

        # -- All checks passed -----------------------------------------------
        return PolicyDecision(
            allowed=True,
            reason="Access permitted under contract terms",
            contract_id=contract.contract_id,
        )

    # -- private evaluation steps --------------------------------------------

    def _check_emergency_override(
        self,
        *,
        participant: Participant,
        asset: DataAsset,
        contract: DataUsageContract,
    ) -> Optional[PolicyDecision]:
        """Handle emergency override for DSO operators.

        Returns a decision if the emergency path resolves (allow or deny).
        Returns ``None`` if the emergency path does not apply and normal
        evaluation should continue.
        """
        # Only DSO operators can invoke the emergency override.
        requester_roles = set(participant.roles)
        if not requester_roles & _EMERGENCY_ROLES:
            return PolicyDecision(
                allowed=False,
                reason=(
                    f"Emergency override denied: requester '{participant.id}' "
                    f"does not hold an emergency-eligible role "
                    f"(requires one of {sorted(_EMERGENCY_ROLES)})"
                ),
                emergency_override=True,
                contract_id=contract.contract_id,
            )

        # The contract must explicitly permit emergency override.
        if not contract.emergency_override:
            return PolicyDecision(
                allowed=False,
                reason=(
                    f"Emergency override denied: contract '{contract.contract_id}' "
                    f"does not include the emergency_override flag"
                ),
                emergency_override=True,
                contract_id=contract.contract_id,
            )

        # The contract must be ACTIVE (emergency bypasses validity window, not
        # status).
        if contract.status != ContractStatus.ACTIVE:
            return PolicyDecision(
                allowed=False,
                reason=(
                    f"Emergency override denied: contract '{contract.contract_id}' "
                    f"is not active (status={contract.status.value})"
                ),
                emergency_override=True,
                contract_id=contract.contract_id,
            )

        # Emergency access granted.
        return PolicyDecision(
            allowed=True,
            reason=(
                f"Emergency override: DSO operator '{participant.id}' granted "
                f"emergency access to asset '{asset.id}' under contract "
                f"'{contract.contract_id}'"
            ),
            emergency_override=True,
            contract_id=contract.contract_id,
        )

    def _check_contract_valid(
        self,
        *,
        contract: DataUsageContract,
        requester_id: str,
        asset_id: str,
    ) -> Optional[PolicyDecision]:
        """Verify the contract is ACTIVE and covers the requester + asset.

        Returns a DENY decision if the contract is invalid, ``None`` otherwise.
        """
        # Contract must be ACTIVE.
        if contract.status != ContractStatus.ACTIVE:
            return PolicyDecision(
                allowed=False,
                reason=(
                    f"Access denied: contract '{contract.contract_id}' is not "
                    f"active (status={contract.status.value})"
                ),
                contract_id=contract.contract_id,
            )

        # Contract must cover the requested asset.
        if contract.asset_id != asset_id:
            return PolicyDecision(
                allowed=False,
                reason=(
                    f"Access denied: contract '{contract.contract_id}' covers "
                    f"asset '{contract.asset_id}', not '{asset_id}'"
                ),
                contract_id=contract.contract_id,
            )

        # Requester must be the consumer on the contract.
        if contract.consumer_id != requester_id:
            return PolicyDecision(
                allowed=False,
                reason=(
                    f"Access denied: contract '{contract.contract_id}' is for "
                    f"consumer '{contract.consumer_id}', not '{requester_id}'"
                ),
                contract_id=contract.contract_id,
            )

        # Contract must be within its validity window.
        now = _utc_now()
        if now < contract.valid_from:
            return PolicyDecision(
                allowed=False,
                reason=(
                    f"Access denied: contract '{contract.contract_id}' is not "
                    f"yet valid (valid_from={contract.valid_from.isoformat()})"
                ),
                contract_id=contract.contract_id,
            )
        if now > contract.valid_until:
            return PolicyDecision(
                allowed=False,
                reason=(
                    f"Access denied: contract '{contract.contract_id}' has "
                    f"expired (valid_until={contract.valid_until.isoformat()})"
                ),
                contract_id=contract.contract_id,
            )

        return None

    def _check_purpose(
        self,
        *,
        contract: DataUsageContract,
        purpose: str,
    ) -> Optional[PolicyDecision]:
        """Verify the stated purpose matches the contract's allowed purpose.

        Returns a DENY decision if the purpose does not match, ``None``
        otherwise.
        """
        if contract.purpose != purpose:
            return PolicyDecision(
                allowed=False,
                reason=(
                    f"Access denied: stated purpose '{purpose}' does not match "
                    f"contract purpose '{contract.purpose}'"
                ),
                contract_id=contract.contract_id,
            )
        return None

    def _check_role_sensitivity(
        self,
        *,
        participant: Participant,
        asset: DataAsset,
        operation: str,
        purpose: str,
    ) -> Optional[PolicyDecision]:
        """Verify the requester's roles permit access to the asset's sensitivity tier.

        First checks custom policy rules (sorted by priority descending, DENY
        rules first at equal priority).  If no custom rule matches, falls back
        to the default sensitivity-tier-to-role mapping.

        Returns a DENY decision if the role check fails, ``None`` otherwise.
        """
        requester_roles = set(participant.roles)

        # Collect applicable custom rules (matching asset and/or sensitivity).
        applicable_rules = self._get_applicable_rules(
            asset_id=asset.id,
            sensitivity=asset.sensitivity,
            operation=operation,
            purpose=purpose,
            requester_roles=requester_roles,
        )

        # Evaluate custom rules in priority order.
        if applicable_rules:
            # Sort: highest priority first; DENY before ALLOW at same priority.
            applicable_rules.sort(
                key=lambda r: (r.priority, 0 if r.effect == PolicyEffect.DENY else 1),
                reverse=True,
            )
            top_rule = applicable_rules[0]
            if top_rule.effect == PolicyEffect.DENY:
                return PolicyDecision(
                    allowed=False,
                    reason=(
                        f"Access denied by policy rule '{top_rule.rule_id}': "
                        f"explicit DENY for sensitivity={asset.sensitivity.value}, "
                        f"operation='{operation}'"
                    ),
                )
            # Explicit ALLOW from custom rule – skip default tier check.
            return None

        # Fall back to default sensitivity-tier-to-role mapping.
        allowed_roles = _DEFAULT_TIER_ROLES.get(asset.sensitivity, set())
        if not requester_roles & allowed_roles:
            return PolicyDecision(
                allowed=False,
                reason=(
                    f"Access denied: requester roles {sorted(requester_roles)} "
                    f"are not permitted for sensitivity tier "
                    f"'{asset.sensitivity.value}' (requires one of "
                    f"{sorted(allowed_roles)})"
                ),
            )

        return None

    def _check_redistribution_retention(
        self,
        *,
        contract: DataUsageContract,
        redistribution_requested: bool,
        retention_days_requested: Optional[int],
    ) -> Optional[PolicyDecision]:
        """Verify redistribution and retention constraints.

        Returns a DENY decision if constraints are violated, ``None`` otherwise.
        """
        if redistribution_requested and not contract.redistribution_allowed:
            return PolicyDecision(
                allowed=False,
                reason=(
                    f"Access denied: contract '{contract.contract_id}' does "
                    f"not permit redistribution of the data"
                ),
                contract_id=contract.contract_id,
            )

        if (
            retention_days_requested is not None
            and retention_days_requested > contract.retention_days
        ):
            return PolicyDecision(
                allowed=False,
                reason=(
                    f"Access denied: requested retention of "
                    f"{retention_days_requested} days exceeds contract limit "
                    f"of {contract.retention_days} days"
                ),
                contract_id=contract.contract_id,
            )

        return None

    # -- rule matching helpers -----------------------------------------------

    def _get_applicable_rules(
        self,
        *,
        asset_id: str,
        sensitivity: SensitivityTier,
        operation: str,
        purpose: str,
        requester_roles: set[str],
    ) -> list[PolicyRule]:
        """Return custom rules that match the request context.

        A rule matches when ALL of its non-empty filter fields match the
        request.  Empty filter fields (e.g. ``allowed_roles=[]``) are treated
        as wildcards.
        """
        matched: list[PolicyRule] = []
        for rule in self._rules.values():
            # Asset filter.
            if rule.asset_id is not None and rule.asset_id != asset_id:
                continue

            # Sensitivity filter.
            if rule.sensitivity is not None and rule.sensitivity != sensitivity:
                continue

            # Role filter (at least one requester role must be in allowed_roles).
            if rule.allowed_roles and not requester_roles & set(rule.allowed_roles):
                continue

            # Operation filter.
            if rule.allowed_operations and operation not in rule.allowed_operations:
                continue

            # Purpose filter.
            if rule.allowed_purposes and purpose not in rule.allowed_purposes:
                continue

            matched.append(rule)
        return matched
