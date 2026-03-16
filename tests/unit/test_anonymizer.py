"""Unit tests for the prosumer data anonymizer.

Tests that PURPOSE_DISCLOSURE_MAP mappings are correctly enforced, aggregation
produces valid output, raw data is never returned for restricted purposes, and
controllability-only returns only the margin scalar.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.participants.prosumer.anonymizer import (
    AnonymizerError,
    ControllableMarginResult,
    DataAnonymizer,
    InsufficientDataError,
    UnknownPurposeError,
)
from src.semantic.cim import SensitivityTier
from src.semantic.consumer import (
    AnonymizedLoadSeries,
    DemandProfile,
    DisclosureLevel,
    PURPOSE_DISCLOSURE_MAP,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _make_profile(
    prosumer_id: str = "P-001",
    values_kw: list[float] | None = None,
    **overrides,
) -> DemandProfile:
    """Create a valid DemandProfile with sensible defaults."""
    now = _utc_now()
    vals = values_kw if values_kw is not None else [2.0, 3.0, 5.0, 4.0, 1.0]
    defaults = dict(
        profile_id="DP-001",
        prosumer_id=prosumer_id,
        profile_type="historical",
        interval_minutes=15.0,
        values_kw=vals,
        peak_demand_kw=max(vals) if vals else 0.0,
        total_energy_kwh=sum(vals) * 0.25,
        profile_start=now,
        profile_end=now + timedelta(hours=1),
        valid_from=now,
        valid_until=now + timedelta(hours=24),
    )
    defaults.update(overrides)
    return DemandProfile(**defaults)


# ---------------------------------------------------------------------------
# PURPOSE_DISCLOSURE_MAP correctness
# ---------------------------------------------------------------------------


class TestPurposeDisclosureMap:
    """Verify that PURPOSE_DISCLOSURE_MAP contains the correct mappings."""

    def test_research_maps_to_aggregated(self) -> None:
        assert PURPOSE_DISCLOSURE_MAP["research"] == DisclosureLevel.AGGREGATED

    def test_dr_dispatch_maps_to_controllability_only(self) -> None:
        assert PURPOSE_DISCLOSURE_MAP["dr_dispatch"] == DisclosureLevel.CONTROLLABILITY_ONLY

    def test_billing_maps_to_identified_consented(self) -> None:
        assert PURPOSE_DISCLOSURE_MAP["billing"] == DisclosureLevel.IDENTIFIED_CONSENTED

    def test_grid_analysis_maps_to_aggregated(self) -> None:
        assert PURPOSE_DISCLOSURE_MAP["grid_analysis"] == DisclosureLevel.AGGREGATED

    def test_forecasting_maps_to_anonymized(self) -> None:
        assert PURPOSE_DISCLOSURE_MAP["forecasting"] == DisclosureLevel.ANONYMIZED

    def test_unknown_purpose_not_in_map(self) -> None:
        """Unknown purposes should not be present in the map (fail-closed)."""
        assert "marketing" not in PURPOSE_DISCLOSURE_MAP
        assert "surveillance" not in PURPOSE_DISCLOSURE_MAP


# ---------------------------------------------------------------------------
# DataAnonymizer initialization
# ---------------------------------------------------------------------------


class TestDataAnonymizerInit:
    """Tests for DataAnonymizer construction and properties."""

    def test_default_k_anonymity(self) -> None:
        """Default k_anonymity_level should be 5."""
        anon = DataAnonymizer(prosumer_id="P-001")
        assert anon.k_anonymity_level == 5

    def test_custom_k_anonymity(self) -> None:
        """Custom k_anonymity_level should be respected."""
        anon = DataAnonymizer(prosumer_id="P-001", k_anonymity_level=10)
        assert anon.k_anonymity_level == 10

    def test_prosumer_id_property(self) -> None:
        """prosumer_id should be accessible via property."""
        anon = DataAnonymizer(prosumer_id="P-042")
        assert anon.prosumer_id == "P-042"


# ---------------------------------------------------------------------------
# Unknown purpose → fail-closed
# ---------------------------------------------------------------------------


class TestUnknownPurpose:
    """Tests that unknown purposes are denied (fail-closed)."""

    def test_unknown_purpose_raises_error(self) -> None:
        """An unknown purpose should raise UnknownPurposeError."""
        anon = DataAnonymizer(prosumer_id="P-001")
        profile = _make_profile()
        with pytest.raises(UnknownPurposeError) as exc_info:
            anon.anonymize_demand_profile(profile, "marketing")
        assert "marketing" in str(exc_info.value)
        assert "fail-closed" in str(exc_info.value).lower()

    def test_unknown_purpose_error_is_anonymizer_error(self) -> None:
        """UnknownPurposeError should be a subclass of AnonymizerError."""
        assert issubclass(UnknownPurposeError, AnonymizerError)

    def test_unknown_purpose_lists_allowed(self) -> None:
        """The error message should list the allowed purposes."""
        anon = DataAnonymizer(prosumer_id="P-001")
        profile = _make_profile()
        with pytest.raises(UnknownPurposeError) as exc_info:
            anon.anonymize_demand_profile(profile, "invalid_purpose")
        error_msg = str(exc_info.value)
        for purpose in PURPOSE_DISCLOSURE_MAP:
            assert purpose in error_msg


# ---------------------------------------------------------------------------
# RAW disclosure — never returned for restricted purposes
# ---------------------------------------------------------------------------


class TestRawDataNeverReturned:
    """Verify raw data is never returned for restricted purposes.

    The only way to get RAW data back is if the purpose maps to RAW
    disclosure (which none of the current purposes do). For every
    known purpose in the map, the returned data should NOT be raw.
    """

    def test_research_does_not_return_raw(self) -> None:
        """Research purpose should return aggregated, not raw."""
        anon = DataAnonymizer(prosumer_id="P-001")
        profile = _make_profile()
        result = anon.anonymize_demand_profile(profile, "research")
        assert isinstance(result, AnonymizedLoadSeries)
        assert result.disclosure_level == DisclosureLevel.AGGREGATED

    def test_dr_dispatch_does_not_return_raw(self) -> None:
        """DR dispatch purpose should return controllable margin only."""
        anon = DataAnonymizer(prosumer_id="P-001")
        profile = _make_profile()
        result = anon.anonymize_demand_profile(profile, "dr_dispatch")
        assert isinstance(result, ControllableMarginResult)

    def test_billing_does_not_return_raw(self) -> None:
        """Billing purpose should return identified-consented profile."""
        anon = DataAnonymizer(prosumer_id="P-001")
        profile = _make_profile()
        result = anon.anonymize_demand_profile(profile, "billing")
        assert isinstance(result, DemandProfile)
        assert result.disclosure_level == DisclosureLevel.IDENTIFIED_CONSENTED

    def test_grid_analysis_does_not_return_raw(self) -> None:
        """Grid analysis purpose should return aggregated, not raw."""
        anon = DataAnonymizer(prosumer_id="P-001")
        profile = _make_profile()
        result = anon.anonymize_demand_profile(profile, "grid_analysis")
        assert isinstance(result, AnonymizedLoadSeries)
        assert result.disclosure_level == DisclosureLevel.AGGREGATED

    def test_forecasting_does_not_return_raw(self) -> None:
        """Forecasting purpose should return anonymized profile."""
        anon = DataAnonymizer(prosumer_id="P-001")
        profile = _make_profile()
        result = anon.anonymize_demand_profile(profile, "forecasting")
        assert isinstance(result, DemandProfile)
        assert result.disclosure_level == DisclosureLevel.ANONYMIZED

    def test_no_purpose_maps_to_raw(self) -> None:
        """No purpose in the map should map to RAW disclosure."""
        for purpose, level in PURPOSE_DISCLOSURE_MAP.items():
            assert level != DisclosureLevel.RAW, (
                f"Purpose '{purpose}' maps to RAW — raw data should never "
                f"be returned for any mapped purpose"
            )


# ---------------------------------------------------------------------------
# Anonymized (forecasting) disclosure
# ---------------------------------------------------------------------------


class TestAnonymizedDisclosure:
    """Tests for ANONYMIZED disclosure level (e.g., forecasting purpose)."""

    def test_identity_stripped(self) -> None:
        """Anonymized profile should have prosumer_id replaced."""
        anon = DataAnonymizer(prosumer_id="P-001")
        profile = _make_profile(prosumer_id="P-001")
        result = anon.anonymize_demand_profile(profile, "forecasting")
        assert isinstance(result, DemandProfile)
        assert result.prosumer_id == "ANONYMIZED"

    def test_profile_id_is_anonymized(self) -> None:
        """Anonymized profile should get an ANON-prefixed profile_id."""
        anon = DataAnonymizer(prosumer_id="P-001")
        profile = _make_profile()
        result = anon.anonymize_demand_profile(profile, "forecasting")
        assert isinstance(result, DemandProfile)
        assert result.profile_id.startswith("ANON-")

    def test_values_preserved(self) -> None:
        """Anonymized profile should preserve the demand values."""
        anon = DataAnonymizer(prosumer_id="P-001")
        values = [1.0, 2.0, 3.0, 4.0]
        profile = _make_profile(values_kw=values, peak_demand_kw=4.0)
        result = anon.anonymize_demand_profile(profile, "forecasting")
        assert isinstance(result, DemandProfile)
        assert result.values_kw == values

    def test_sensitivity_downgraded_to_medium(self) -> None:
        """Anonymized output should have MEDIUM sensitivity (no longer private)."""
        anon = DataAnonymizer(prosumer_id="P-001")
        profile = _make_profile()
        result = anon.anonymize_demand_profile(profile, "forecasting")
        assert isinstance(result, DemandProfile)
        assert result.sensitivity == SensitivityTier.MEDIUM


# ---------------------------------------------------------------------------
# Identified-consented (billing) disclosure
# ---------------------------------------------------------------------------


class TestIdentifiedConsentedDisclosure:
    """Tests for IDENTIFIED_CONSENTED disclosure level (e.g., billing)."""

    def test_identity_preserved(self) -> None:
        """Identified-consented profile should keep prosumer_id."""
        anon = DataAnonymizer(prosumer_id="P-001")
        profile = _make_profile(prosumer_id="P-001")
        result = anon.anonymize_demand_profile(profile, "billing")
        assert isinstance(result, DemandProfile)
        assert result.prosumer_id == "P-001"

    def test_disclosure_level_set(self) -> None:
        """Identified-consented profile should set the correct disclosure level."""
        anon = DataAnonymizer(prosumer_id="P-001")
        profile = _make_profile()
        result = anon.anonymize_demand_profile(profile, "billing")
        assert isinstance(result, DemandProfile)
        assert result.disclosure_level == DisclosureLevel.IDENTIFIED_CONSENTED

    def test_values_preserved(self) -> None:
        """Identified-consented profile should preserve values."""
        anon = DataAnonymizer(prosumer_id="P-001")
        values = [5.0, 6.0, 7.0]
        profile = _make_profile(values_kw=values, peak_demand_kw=7.0)
        result = anon.anonymize_demand_profile(profile, "billing")
        assert isinstance(result, DemandProfile)
        assert result.values_kw == values


# ---------------------------------------------------------------------------
# Aggregation (research, grid_analysis)
# ---------------------------------------------------------------------------


class TestAggregation:
    """Tests for AGGREGATED disclosure level and aggregate_load_series."""

    def test_single_profile_aggregation(self) -> None:
        """Aggregating a single profile should produce valid output."""
        anon = DataAnonymizer(prosumer_id="P-001")
        profile = _make_profile(values_kw=[10.0, 20.0, 30.0, 20.0])
        result = anon.anonymize_demand_profile(profile, "research")
        assert isinstance(result, AnonymizedLoadSeries)
        assert result.disclosure_level == DisclosureLevel.AGGREGATED
        assert result.source_count == 1

    def test_aggregation_computes_statistics(self) -> None:
        """Aggregated output should have correct mean, peak, min."""
        anon = DataAnonymizer(prosumer_id="P-001")
        values = [10.0, 20.0, 30.0, 40.0]
        profile = _make_profile(values_kw=values, peak_demand_kw=40.0)
        result = anon.anonymize_demand_profile(profile, "research")
        assert isinstance(result, AnonymizedLoadSeries)
        assert result.peak_kw == 40.0
        assert result.min_kw == 10.0
        assert result.mean_kw == 25.0

    def test_multi_profile_aggregation(self) -> None:
        """Aggregating multiple profiles should sum element-wise."""
        anon = DataAnonymizer(prosumer_id="P-001")
        p1 = _make_profile(prosumer_id="P-001", values_kw=[10.0, 20.0, 30.0])
        p2 = _make_profile(prosumer_id="P-002", values_kw=[5.0, 10.0, 15.0])
        result = anon.aggregate_load_series([p1, p2], feeder_id="F-101")
        assert isinstance(result, AnonymizedLoadSeries)
        assert result.values_kw == [15.0, 30.0, 45.0]
        assert result.source_count == 2
        assert result.feeder_id == "F-101"

    def test_aggregation_no_prosumer_identity(self) -> None:
        """Aggregated output should not contain any prosumer identifier."""
        anon = DataAnonymizer(prosumer_id="P-001")
        p1 = _make_profile(prosumer_id="P-001", values_kw=[10.0, 20.0])
        p2 = _make_profile(prosumer_id="P-002", values_kw=[5.0, 15.0])
        result = anon.aggregate_load_series([p1, p2])
        # AnonymizedLoadSeries has no prosumer_id field
        assert not hasattr(result, "prosumer_id")

    def test_aggregation_sensitivity_is_medium(self) -> None:
        """Aggregated output should have MEDIUM sensitivity."""
        anon = DataAnonymizer(prosumer_id="P-001")
        profile = _make_profile()
        result = anon.aggregate_load_series([profile])
        assert result.sensitivity == SensitivityTier.MEDIUM

    def test_aggregation_k_anonymity_at_least_k(self) -> None:
        """k_anonymity_level should be at least the configured k."""
        anon = DataAnonymizer(prosumer_id="P-001", k_anonymity_level=5)
        profiles = [_make_profile(prosumer_id=f"P-{i:03d}") for i in range(3)]
        result = anon.aggregate_load_series(profiles)
        assert result.k_anonymity_level >= 5

    def test_aggregation_k_anonymity_grows_with_profiles(self) -> None:
        """k_anonymity_level should grow when profiles > k."""
        anon = DataAnonymizer(prosumer_id="P-001", k_anonymity_level=3)
        profiles = [_make_profile(prosumer_id=f"P-{i:03d}") for i in range(10)]
        result = anon.aggregate_load_series(profiles)
        assert result.k_anonymity_level >= 10

    def test_aggregation_empty_profiles_raises(self) -> None:
        """Aggregating an empty list should raise InsufficientDataError."""
        anon = DataAnonymizer(prosumer_id="P-001")
        with pytest.raises(InsufficientDataError):
            anon.aggregate_load_series([])

    def test_aggregation_different_lengths(self) -> None:
        """Profiles with different value lengths should be handled."""
        anon = DataAnonymizer(prosumer_id="P-001")
        p1 = _make_profile(values_kw=[10.0, 20.0, 30.0])
        p2 = _make_profile(values_kw=[5.0, 10.0])
        result = anon.aggregate_load_series([p1, p2])
        # p2 only has 2 values, so position 2 should only include p1
        assert result.values_kw == [15.0, 30.0, 30.0]

    def test_aggregation_series_id_format(self) -> None:
        """Aggregated series_id should follow ALS-* format."""
        anon = DataAnonymizer(prosumer_id="P-001")
        profile = _make_profile()
        result = anon.aggregate_load_series([profile])
        assert result.series_id.startswith("ALS-")

    def test_aggregation_uses_broadest_time_range(self) -> None:
        """Aggregation should use the broadest time range across profiles."""
        now = _utc_now()
        anon = DataAnonymizer(prosumer_id="P-001")
        p1 = _make_profile(
            profile_start=now,
            profile_end=now + timedelta(hours=1),
            valid_from=now,
            valid_until=now + timedelta(hours=24),
        )
        p2 = _make_profile(
            profile_start=now - timedelta(hours=1),
            profile_end=now + timedelta(hours=2),
            valid_from=now - timedelta(hours=1),
            valid_until=now + timedelta(hours=48),
        )
        result = anon.aggregate_load_series([p1, p2])
        assert result.series_start == now - timedelta(hours=1)
        assert result.series_end == now + timedelta(hours=2)


# ---------------------------------------------------------------------------
# Controllability-only (dr_dispatch)
# ---------------------------------------------------------------------------


class TestControllabilityOnly:
    """Tests for CONTROLLABILITY_ONLY disclosure level (DR dispatch)."""

    def test_returns_margin_result(self) -> None:
        """DR dispatch should return a ControllableMarginResult."""
        anon = DataAnonymizer(prosumer_id="P-001")
        profile = _make_profile(values_kw=[2.0, 5.0, 3.0, 1.0])
        result = anon.anonymize_demand_profile(profile, "dr_dispatch")
        assert isinstance(result, ControllableMarginResult)

    def test_margin_is_peak_minus_valley(self) -> None:
        """Controllable margin should be peak - valley of all values."""
        anon = DataAnonymizer(prosumer_id="P-001")
        profile = _make_profile(values_kw=[2.0, 8.0, 5.0, 1.0])
        result = anon.anonymize_demand_profile(profile, "dr_dispatch")
        assert isinstance(result, ControllableMarginResult)
        assert result.controllable_margin_kw == 7.0  # 8.0 - 1.0

    def test_margin_no_time_series(self) -> None:
        """ControllableMarginResult should only have margin, not time-series."""
        anon = DataAnonymizer(prosumer_id="P-001")
        profile = _make_profile()
        result = anon.anonymize_demand_profile(profile, "dr_dispatch")
        assert isinstance(result, ControllableMarginResult)
        assert not hasattr(result, "values_kw")
        assert not hasattr(result, "prosumer_id")
        assert not hasattr(result, "profile_id")

    def test_margin_no_identity(self) -> None:
        """ControllableMarginResult should contain no prosumer identity."""
        anon = DataAnonymizer(prosumer_id="P-001")
        profile = _make_profile(prosumer_id="P-SENSITIVE")
        result = anon.anonymize_demand_profile(profile, "dr_dispatch")
        assert isinstance(result, ControllableMarginResult)
        result_repr = repr(result)
        assert "P-SENSITIVE" not in result_repr

    def test_compute_controllable_margin_multi_profiles(self) -> None:
        """Margin from multiple profiles should span all values."""
        anon = DataAnonymizer(prosumer_id="P-001")
        p1 = _make_profile(values_kw=[10.0, 20.0])
        p2 = _make_profile(values_kw=[5.0, 30.0])
        result = anon.compute_controllable_margin([p1, p2])
        # Peak = 30.0, valley = 5.0 → margin = 25.0
        assert result.controllable_margin_kw == 25.0

    def test_compute_controllable_margin_empty_raises(self) -> None:
        """Empty profile list should raise InsufficientDataError."""
        anon = DataAnonymizer(prosumer_id="P-001")
        with pytest.raises(InsufficientDataError):
            anon.compute_controllable_margin([])

    def test_compute_controllable_margin_empty_values(self) -> None:
        """Profiles with empty values should return 0.0 margin."""
        anon = DataAnonymizer(prosumer_id="P-001")
        profile = _make_profile(values_kw=[], peak_demand_kw=0.0, total_energy_kwh=0.0)
        result = anon.compute_controllable_margin([profile])
        assert result.controllable_margin_kw == 0.0

    def test_compute_controllable_margin_constant_values(self) -> None:
        """Constant demand values should yield 0.0 margin."""
        anon = DataAnonymizer(prosumer_id="P-001")
        profile = _make_profile(values_kw=[5.0, 5.0, 5.0, 5.0])
        result = anon.compute_controllable_margin([profile])
        assert result.controllable_margin_kw == 0.0

    def test_controllable_margin_result_repr(self) -> None:
        """ControllableMarginResult repr should include the margin value."""
        result = ControllableMarginResult(controllable_margin_kw=42.5)
        assert "42.5" in repr(result)
        assert "ControllableMarginResult" in repr(result)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestAnonymizerEdgeCases:
    """Edge cases and error handling for the DataAnonymizer."""

    def test_insufficient_data_error_is_anonymizer_error(self) -> None:
        """InsufficientDataError should be a subclass of AnonymizerError."""
        assert issubclass(InsufficientDataError, AnonymizerError)

    def test_resolve_disclosure_level_known_purpose(self) -> None:
        """Known purposes should resolve to the correct disclosure level."""
        level = DataAnonymizer._resolve_disclosure_level("research")
        assert level == DisclosureLevel.AGGREGATED

    def test_resolve_disclosure_level_unknown_purpose(self) -> None:
        """Unknown purposes should raise UnknownPurposeError."""
        with pytest.raises(UnknownPurposeError):
            DataAnonymizer._resolve_disclosure_level("unknown")

    def test_all_purposes_produce_valid_output(self) -> None:
        """Every known purpose should produce a valid (non-None) result."""
        anon = DataAnonymizer(prosumer_id="P-001")
        profile = _make_profile()
        for purpose in PURPOSE_DISCLOSURE_MAP:
            result = anon.anonymize_demand_profile(profile, purpose)
            assert result is not None, f"Purpose '{purpose}' returned None"
