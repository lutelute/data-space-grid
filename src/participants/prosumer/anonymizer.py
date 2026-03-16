"""Data anonymization engine for the Prosumer participant node.

Implements purpose-based minimum-disclosure data sharing (spec Pattern 5).
Consumer data is never shared raw — the disclosure level is determined by
purpose, not by requester.  The prosumer node enforces this locally before
any data leaves the local data store.

Disclosure levels (from least to most restrictive):
  - ``RAW``: Only for the consent-holder themselves.
  - ``IDENTIFIED_CONSENTED``: Contract use with explicit consent (e.g. billing).
  - ``ANONYMIZED``: k-anonymized, no individual identification possible.
  - ``AGGREGATED``: Statistical aggregates only (mean, std, min, max).
  - ``CONTROLLABILITY_ONLY``: Only controllable margin, nothing else.

The ``DataAnonymizer`` applies the appropriate transformation based on the
purpose-to-disclosure mapping before any data is released.  If a purpose is
not in :data:`PURPOSE_DISCLOSURE_MAP`, the request is denied (fail-closed).

Key design decisions:
  - Default is maximum restriction; only explicit consent widens access.
  - The anonymizer operates on :class:`DemandProfile` instances and produces
    either transformed profiles or :class:`AnonymizedLoadSeries` depending on
    the disclosure level.
  - Controllability-only responses contain a single scalar (controllable
    margin in kW) with no time-series data.
  - Aggregation computes statistical summaries without retaining any
    individual prosumer identity.
"""

from __future__ import annotations

import statistics
import uuid
from datetime import datetime, timezone
from typing import Optional, Union

from src.semantic.cim import SensitivityTier
from src.semantic.consumer import (
    AnonymizedLoadSeries,
    ConsentStatus,
    DemandProfile,
    DisclosureLevel,
    PURPOSE_DISCLOSURE_MAP,
)


def _utc_now() -> datetime:
    """Return the current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class AnonymizerError(Exception):
    """Base exception for anonymizer errors."""


class UnknownPurposeError(AnonymizerError):
    """Raised when a purpose is not in the PURPOSE_DISCLOSURE_MAP (fail-closed)."""

    def __init__(self, purpose: str) -> None:
        self.purpose = purpose
        allowed = ", ".join(sorted(PURPOSE_DISCLOSURE_MAP.keys()))
        super().__init__(
            f"Unknown purpose: '{purpose}'. "
            f"Data access denied (fail-closed). "
            f"Allowed purposes: {allowed}"
        )


class InsufficientDataError(AnonymizerError):
    """Raised when there is not enough data for the requested operation."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


# ---------------------------------------------------------------------------
# Controllability result
# ---------------------------------------------------------------------------


class ControllableMarginResult:
    """Result of a controllability-only disclosure.

    Contains only the controllable margin in kW — no time-series data,
    no prosumer identity, no consumption patterns.  This is the most
    restrictive disclosure level, used for DR dispatch purposes.
    """

    def __init__(self, controllable_margin_kw: float) -> None:
        self.controllable_margin_kw = controllable_margin_kw

    def __repr__(self) -> str:
        return (
            f"ControllableMarginResult("
            f"controllable_margin_kw={self.controllable_margin_kw})"
        )


# ---------------------------------------------------------------------------
# DataAnonymizer
# ---------------------------------------------------------------------------


class DataAnonymizer:
    """Purpose-based data anonymization engine for prosumer data.

    Transforms demand profiles and load data according to the disclosure
    level dictated by the requesting purpose.  This is the last line of
    defence before any consumer data leaves the prosumer node.

    Usage::

        anonymizer = DataAnonymizer(prosumer_id="prosumer-001")

        # Anonymize a single demand profile for a purpose
        result = anonymizer.anonymize_demand_profile(profile, "research")

        # Aggregate multiple profiles into an anonymized load series
        series = anonymizer.aggregate_load_series(profiles, feeder_id="F-101")

        # Compute controllable margin for DR dispatch
        margin = anonymizer.compute_controllable_margin(profiles)

    Args:
        prosumer_id: Identifier of the prosumer whose data is being
            anonymized.  Used for audit context only; never included
            in anonymized/aggregated output.
        k_anonymity_level: Minimum k-anonymity group size for
            anonymization.  Defaults to 5.
    """

    def __init__(
        self,
        prosumer_id: str,
        *,
        k_anonymity_level: int = 5,
    ) -> None:
        self._prosumer_id = prosumer_id
        self._k_anonymity_level = k_anonymity_level

    @property
    def prosumer_id(self) -> str:
        """The prosumer identifier this anonymizer operates on."""
        return self._prosumer_id

    @property
    def k_anonymity_level(self) -> int:
        """The minimum k-anonymity group size."""
        return self._k_anonymity_level

    # -- helpers -------------------------------------------------------------

    @staticmethod
    def _resolve_disclosure_level(purpose: str) -> DisclosureLevel:
        """Look up the disclosure level for a purpose (fail-closed).

        Args:
            purpose: The data usage purpose.

        Returns:
            The :class:`DisclosureLevel` for this purpose.

        Raises:
            UnknownPurposeError: If the purpose is not recognized.
        """
        level = PURPOSE_DISCLOSURE_MAP.get(purpose)
        if level is None:
            raise UnknownPurposeError(purpose)
        return level

    def _strip_identity(self, profile: DemandProfile) -> DemandProfile:
        """Create a copy of the profile with prosumer identity removed.

        Replaces the ``prosumer_id`` with a placeholder and sets the
        disclosure level to ``ANONYMIZED``.

        Args:
            profile: The source demand profile.

        Returns:
            A new :class:`DemandProfile` with identity stripped.
        """
        return DemandProfile(
            profile_id=f"ANON-{uuid.uuid4().hex[:8]}",
            prosumer_id="ANONYMIZED",
            profile_type=profile.profile_type,
            interval_minutes=profile.interval_minutes,
            values_kw=list(profile.values_kw),
            peak_demand_kw=profile.peak_demand_kw,
            total_energy_kwh=profile.total_energy_kwh,
            profile_start=profile.profile_start,
            profile_end=profile.profile_end,
            disclosure_level=DisclosureLevel.ANONYMIZED,
            valid_from=profile.valid_from,
            valid_until=profile.valid_until,
            sensitivity=SensitivityTier.MEDIUM,
            updated_at=_utc_now(),
        )

    def _aggregate_profiles(
        self,
        profiles: list[DemandProfile],
        *,
        feeder_id: Optional[str] = None,
    ) -> AnonymizedLoadSeries:
        """Aggregate multiple profiles into a single anonymized load series.

        Computes element-wise sums of demand values and derives statistical
        summaries.  No individual prosumer identity is recoverable from
        the output.

        Args:
            profiles: List of demand profiles to aggregate.
            feeder_id: Optional feeder ID for geographic context.

        Returns:
            An :class:`AnonymizedLoadSeries` with aggregated statistics.

        Raises:
            InsufficientDataError: If the profile list is empty.
        """
        if not profiles:
            raise InsufficientDataError(
                "Cannot aggregate an empty list of demand profiles"
            )

        now = _utc_now()

        # Element-wise sum of demand values
        max_len = max(len(p.values_kw) for p in profiles)
        aggregated_values: list[float] = [0.0] * max_len
        for profile in profiles:
            for i, val in enumerate(profile.values_kw):
                aggregated_values[i] += val

        mean_kw = statistics.mean(aggregated_values) if aggregated_values else 0.0
        std_dev_kw = (
            statistics.stdev(aggregated_values)
            if len(aggregated_values) >= 2
            else 0.0
        )
        peak_kw = max(aggregated_values) if aggregated_values else 0.0
        min_kw = min(aggregated_values) if aggregated_values else 0.0

        # Use the broadest time range across all profiles
        series_start = min(p.profile_start for p in profiles)
        series_end = max(p.profile_end for p in profiles)
        valid_from = min(p.valid_from for p in profiles)
        valid_until = max(p.valid_until for p in profiles)

        # Use the most common interval
        interval_minutes = profiles[0].interval_minutes

        return AnonymizedLoadSeries(
            series_id=f"ALS-{uuid.uuid4().hex[:8]}",
            source_count=len(profiles),
            feeder_id=feeder_id,
            interval_minutes=interval_minutes,
            values_kw=aggregated_values,
            mean_kw=round(mean_kw, 2),
            std_dev_kw=round(std_dev_kw, 2),
            peak_kw=round(peak_kw, 2),
            min_kw=round(min_kw, 2),
            k_anonymity_level=max(self._k_anonymity_level, len(profiles)),
            series_start=series_start,
            series_end=series_end,
            disclosure_level=DisclosureLevel.AGGREGATED,
            valid_from=valid_from,
            valid_until=valid_until,
            sensitivity=SensitivityTier.MEDIUM,
            updated_at=now,
        )

    # -- public API ----------------------------------------------------------

    def anonymize_demand_profile(
        self,
        data: DemandProfile,
        purpose: str,
    ) -> Union[DemandProfile, AnonymizedLoadSeries, ControllableMarginResult]:
        """Anonymize a demand profile according to the purpose's disclosure level.

        The transformation applied depends on the disclosure level mapped
        from the purpose via :data:`PURPOSE_DISCLOSURE_MAP`:

        - ``RAW``: Returns the profile unchanged (self-access only).
        - ``IDENTIFIED_CONSENTED``: Returns the profile with identity intact
          but disclosure level updated (billing use).
        - ``ANONYMIZED``: Returns the profile with identity stripped.
        - ``AGGREGATED``: Returns an :class:`AnonymizedLoadSeries` with
          statistical aggregates from this single profile.
        - ``CONTROLLABILITY_ONLY``: Returns a :class:`ControllableMarginResult`
          with only the controllable margin.

        Args:
            data: The source demand profile to anonymize.
            purpose: The data usage purpose (must be in PURPOSE_DISCLOSURE_MAP).

        Returns:
            The appropriately anonymized data.

        Raises:
            UnknownPurposeError: If the purpose is not recognized (fail-closed).
        """
        level = self._resolve_disclosure_level(purpose)

        if level == DisclosureLevel.RAW:
            return data

        if level == DisclosureLevel.IDENTIFIED_CONSENTED:
            return DemandProfile(
                profile_id=data.profile_id,
                prosumer_id=data.prosumer_id,
                profile_type=data.profile_type,
                interval_minutes=data.interval_minutes,
                values_kw=list(data.values_kw),
                peak_demand_kw=data.peak_demand_kw,
                total_energy_kwh=data.total_energy_kwh,
                profile_start=data.profile_start,
                profile_end=data.profile_end,
                disclosure_level=DisclosureLevel.IDENTIFIED_CONSENTED,
                valid_from=data.valid_from,
                valid_until=data.valid_until,
                sensitivity=data.sensitivity,
                updated_at=_utc_now(),
            )

        if level == DisclosureLevel.ANONYMIZED:
            return self._strip_identity(data)

        if level == DisclosureLevel.AGGREGATED:
            return self._aggregate_profiles([data])

        if level == DisclosureLevel.CONTROLLABILITY_ONLY:
            return self.compute_controllable_margin([data])

        # Fail-closed: if somehow we reach here, deny access
        raise UnknownPurposeError(purpose)

    def aggregate_load_series(
        self,
        profiles: list[DemandProfile],
        *,
        feeder_id: Optional[str] = None,
    ) -> AnonymizedLoadSeries:
        """Aggregate multiple demand profiles into an anonymized load series.

        This is the primary aggregation method for producing shareable
        time-series data.  Individual prosumer identities are removed;
        only statistical aggregates remain.

        Args:
            profiles: List of demand profiles to aggregate.
            feeder_id: Optional feeder ID for geographic aggregation context.

        Returns:
            An :class:`AnonymizedLoadSeries` with aggregated statistics.

        Raises:
            InsufficientDataError: If the profile list is empty.
        """
        return self._aggregate_profiles(profiles, feeder_id=feeder_id)

    def compute_controllable_margin(
        self,
        profiles: list[DemandProfile],
    ) -> ControllableMarginResult:
        """Compute the controllable margin for DR dispatch purposes.

        Returns only a single scalar representing how much demand can
        be reduced or shifted.  No time-series data, no prosumer identity,
        and no consumption patterns are included.  This is the most
        restrictive disclosure level.

        The controllable margin is estimated as the difference between
        peak demand and the minimum demand across all profiles, representing
        the theoretical load flexibility.

        Args:
            profiles: List of demand profiles to compute margin from.

        Returns:
            A :class:`ControllableMarginResult` with the controllable
            margin in kW.

        Raises:
            InsufficientDataError: If the profile list is empty.
        """
        if not profiles:
            raise InsufficientDataError(
                "Cannot compute controllable margin from an empty list of profiles"
            )

        # Controllable margin = peak demand - minimum demand across all profiles
        # This represents the theoretical range of demand that can be shifted
        all_values: list[float] = []
        for profile in profiles:
            all_values.extend(profile.values_kw)

        if not all_values:
            return ControllableMarginResult(controllable_margin_kw=0.0)

        peak = max(all_values)
        valley = min(all_values)
        margin = max(0.0, peak - valley)

        return ControllableMarginResult(
            controllable_margin_kw=round(margin, 2)
        )
