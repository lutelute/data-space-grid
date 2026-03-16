"""Unit tests for semantic models: CIM, IEC 61850, OpenADR, and Consumer.

Tests that all domain models validate correctly, enforce sensitivity tiers,
validate timestamps, and that FlexibilityEnvelope construction works.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from src.semantic.cim import (
    CongestionSignal,
    Feeder,
    FeederConstraint,
    GridNode,
    HostingCapacity,
    NodeType,
    SensitivityTier,
    Switch,
    SwitchState,
)
from src.semantic.consumer import (
    AnonymizedLoadSeries,
    ConsentRecord,
    ConsentStatus,
    DemandProfile,
    DisclosureLevel,
    MeterReading,
    PURPOSE_DISCLOSURE_MAP,
)
from src.semantic.iec61850 import (
    AvailabilityWindow,
    ConfidenceLevel,
    DERType,
    DERUnit,
    DeviceClassMix,
    FlexibilityDirection,
    FlexibilityEnvelope,
    PQRange,
    ResponseConfidence,
    StateOfCharge,
)
from src.semantic.openadr import (
    Baseline,
    DispatchActual,
    DispatchCommand,
    DREvent,
    DRSignal,
    EventStatus,
    SignalType,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _validity_window() -> tuple[datetime, datetime]:
    """Return a (valid_from, valid_until) tuple spanning the next 24 hours."""
    now = _utc_now()
    return now, now + timedelta(hours=24)


# ---------------------------------------------------------------------------
# CIM models
# ---------------------------------------------------------------------------


class TestSensitivityTier:
    """Tests for the SensitivityTier enum."""

    def test_enum_values(self) -> None:
        """SensitivityTier should contain all expected tiers."""
        expected = {"high", "medium", "high_privacy"}
        actual = {t.value for t in SensitivityTier}
        assert actual == expected

    def test_string_enum(self) -> None:
        """SensitivityTier members should be usable as strings."""
        assert SensitivityTier.HIGH == "high"
        assert SensitivityTier.MEDIUM == "medium"
        assert SensitivityTier.HIGH_PRIVACY == "high_privacy"


class TestFeederConstraint:
    """Tests for the CIM FeederConstraint model."""

    def test_valid_feeder_constraint(self) -> None:
        """A fully populated FeederConstraint should validate."""
        vf, vu = _validity_window()
        fc = FeederConstraint(
            feeder_id="F-101",
            max_active_power_kw=5000.0,
            min_voltage_pu=0.95,
            max_voltage_pu=1.05,
            congestion_level=0.3,
            valid_from=vf,
            valid_until=vu,
        )
        assert fc.feeder_id == "F-101"
        assert fc.max_active_power_kw == 5000.0
        assert fc.sensitivity == SensitivityTier.MEDIUM
        assert fc.updated_at.tzinfo is not None

    def test_default_sensitivity_is_medium(self) -> None:
        """FeederConstraint defaults to MEDIUM sensitivity."""
        vf, vu = _validity_window()
        fc = FeederConstraint(
            feeder_id="F-102",
            max_active_power_kw=3000.0,
            min_voltage_pu=0.9,
            max_voltage_pu=1.1,
            congestion_level=0.0,
            valid_from=vf,
            valid_until=vu,
        )
        assert fc.sensitivity == SensitivityTier.MEDIUM

    def test_congestion_level_must_be_between_0_and_1(self) -> None:
        """Congestion level outside [0, 1] should raise ValidationError."""
        vf, vu = _validity_window()
        with pytest.raises(ValidationError):
            FeederConstraint(
                feeder_id="F-103",
                max_active_power_kw=5000.0,
                min_voltage_pu=0.95,
                max_voltage_pu=1.05,
                congestion_level=1.5,
                valid_from=vf,
                valid_until=vu,
            )

    def test_negative_power_rejected(self) -> None:
        """Negative max_active_power_kw should raise ValidationError."""
        vf, vu = _validity_window()
        with pytest.raises(ValidationError):
            FeederConstraint(
                feeder_id="F-104",
                max_active_power_kw=-100.0,
                min_voltage_pu=0.95,
                max_voltage_pu=1.05,
                congestion_level=0.5,
                valid_from=vf,
                valid_until=vu,
            )

    def test_missing_feeder_id(self) -> None:
        """Omitting feeder_id should raise ValidationError."""
        vf, vu = _validity_window()
        with pytest.raises(ValidationError):
            FeederConstraint(
                max_active_power_kw=5000.0,
                min_voltage_pu=0.95,
                max_voltage_pu=1.05,
                congestion_level=0.3,
                valid_from=vf,
                valid_until=vu,
            )  # type: ignore[call-arg]

    def test_voltage_exceeds_limit(self) -> None:
        """Voltage outside [0, 2.0] should raise ValidationError."""
        vf, vu = _validity_window()
        with pytest.raises(ValidationError):
            FeederConstraint(
                feeder_id="F-105",
                max_active_power_kw=5000.0,
                min_voltage_pu=0.95,
                max_voltage_pu=2.5,
                congestion_level=0.3,
                valid_from=vf,
                valid_until=vu,
            )


class TestCongestionSignal:
    """Tests for the CIM CongestionSignal model."""

    def test_valid_congestion_signal(self) -> None:
        """A fully populated CongestionSignal should validate."""
        vf, vu = _validity_window()
        sig = CongestionSignal(
            signal_id="CS-001",
            feeder_id="F-101",
            congestion_level=0.7,
            max_available_capacity_kw=1500.0,
            valid_from=vf,
            valid_until=vu,
        )
        assert sig.signal_id == "CS-001"
        assert sig.direction == "both"
        assert sig.sensitivity == SensitivityTier.MEDIUM
        assert sig.timestamp.tzinfo is not None

    def test_congestion_level_boundary_values(self) -> None:
        """Congestion level at boundaries (0.0, 1.0) should validate."""
        vf, vu = _validity_window()
        sig_low = CongestionSignal(
            signal_id="CS-002",
            feeder_id="F-101",
            congestion_level=0.0,
            max_available_capacity_kw=5000.0,
            valid_from=vf,
            valid_until=vu,
        )
        sig_high = CongestionSignal(
            signal_id="CS-003",
            feeder_id="F-101",
            congestion_level=1.0,
            max_available_capacity_kw=0.0,
            valid_from=vf,
            valid_until=vu,
        )
        assert sig_low.congestion_level == 0.0
        assert sig_high.congestion_level == 1.0


class TestHostingCapacity:
    """Tests for the CIM HostingCapacity model."""

    def test_valid_hosting_capacity(self) -> None:
        """A fully populated HostingCapacity should validate."""
        vf, vu = _validity_window()
        hc = HostingCapacity(
            node_id="N-001",
            feeder_id="F-101",
            max_generation_kw=2000.0,
            max_load_kw=3000.0,
            current_generation_kw=500.0,
            current_load_kw=1200.0,
            voltage_headroom_pu=0.03,
            thermal_headroom_pct=45.0,
            valid_from=vf,
            valid_until=vu,
        )
        assert hc.node_id == "N-001"
        assert hc.sensitivity == SensitivityTier.MEDIUM
        assert hc.updated_at.tzinfo is not None

    def test_thermal_headroom_exceeds_100(self) -> None:
        """thermal_headroom_pct > 100 should raise ValidationError."""
        vf, vu = _validity_window()
        with pytest.raises(ValidationError):
            HostingCapacity(
                node_id="N-002",
                feeder_id="F-101",
                max_generation_kw=2000.0,
                max_load_kw=3000.0,
                current_generation_kw=500.0,
                current_load_kw=1200.0,
                voltage_headroom_pu=0.03,
                thermal_headroom_pct=105.0,
                valid_from=vf,
                valid_until=vu,
            )


class TestGridNode:
    """Tests for the CIM GridNode model."""

    def test_valid_grid_node(self) -> None:
        """A fully populated GridNode should validate."""
        vf, vu = _validity_window()
        node = GridNode(
            node_id="N-001",
            name="Substation Alpha",
            node_type=NodeType.SUBSTATION,
            feeder_id="F-101",
            voltage_level_kv=20.0,
            latitude=52.52,
            longitude=13.405,
            valid_from=vf,
            valid_until=vu,
        )
        assert node.node_id == "N-001"
        assert node.node_type == NodeType.SUBSTATION
        assert node.is_energized is True
        assert node.sensitivity == SensitivityTier.HIGH

    def test_grid_node_default_sensitivity_is_high(self) -> None:
        """GridNode defaults to HIGH sensitivity (topology data)."""
        vf, vu = _validity_window()
        node = GridNode(
            node_id="N-002",
            name="Junction B",
            node_type=NodeType.JUNCTION,
            feeder_id="F-102",
            voltage_level_kv=10.0,
            valid_from=vf,
            valid_until=vu,
        )
        assert node.sensitivity == SensitivityTier.HIGH

    def test_grid_node_optional_coordinates(self) -> None:
        """Latitude and longitude should default to None."""
        vf, vu = _validity_window()
        node = GridNode(
            node_id="N-003",
            name="Load Point C",
            node_type=NodeType.LOAD_POINT,
            feeder_id="F-103",
            voltage_level_kv=0.4,
            valid_from=vf,
            valid_until=vu,
        )
        assert node.latitude is None
        assert node.longitude is None

    def test_grid_node_invalid_latitude(self) -> None:
        """Latitude outside [-90, 90] should raise ValidationError."""
        vf, vu = _validity_window()
        with pytest.raises(ValidationError):
            GridNode(
                node_id="N-004",
                name="Invalid Node",
                node_type=NodeType.JUNCTION,
                feeder_id="F-104",
                voltage_level_kv=10.0,
                latitude=95.0,
                valid_from=vf,
                valid_until=vu,
            )

    def test_grid_node_zero_voltage_rejected(self) -> None:
        """voltage_level_kv must be > 0."""
        vf, vu = _validity_window()
        with pytest.raises(ValidationError):
            GridNode(
                node_id="N-005",
                name="Zero Voltage",
                node_type=NodeType.JUNCTION,
                feeder_id="F-105",
                voltage_level_kv=0.0,
                valid_from=vf,
                valid_until=vu,
            )

    def test_node_type_enum_values(self) -> None:
        """NodeType should contain all expected classification types."""
        expected = {
            "substation", "distribution_transformer", "junction",
            "load_point", "generation_point", "measurement_point",
        }
        actual = {t.value for t in NodeType}
        assert actual == expected


class TestFeeder:
    """Tests for the CIM Feeder model."""

    def test_valid_feeder(self) -> None:
        """A fully populated Feeder should validate."""
        vf, vu = _validity_window()
        feeder = Feeder(
            feeder_id="F-101",
            name="Main Feeder Alpha",
            substation_id="SUB-001",
            voltage_level_kv=20.0,
            max_rated_power_kw=10000.0,
            node_ids=["N-001", "N-002", "N-003"],
            switch_ids=["SW-001", "SW-002"],
            valid_from=vf,
            valid_until=vu,
        )
        assert feeder.feeder_id == "F-101"
        assert feeder.sensitivity == SensitivityTier.HIGH
        assert len(feeder.node_ids) == 3

    def test_feeder_empty_node_list_default(self) -> None:
        """Feeder should default to empty node_ids and switch_ids."""
        vf, vu = _validity_window()
        feeder = Feeder(
            feeder_id="F-102",
            name="New Feeder",
            substation_id="SUB-001",
            voltage_level_kv=10.0,
            max_rated_power_kw=5000.0,
            valid_from=vf,
            valid_until=vu,
        )
        assert feeder.node_ids == []
        assert feeder.switch_ids == []


class TestSwitch:
    """Tests for the CIM Switch model."""

    def test_valid_switch(self) -> None:
        """A fully populated Switch should validate."""
        vf, vu = _validity_window()
        sw = Switch(
            switch_id="SW-001",
            name="Breaker Alpha",
            feeder_id="F-101",
            from_node_id="N-001",
            to_node_id="N-002",
            state=SwitchState.CLOSED,
            rated_current_a=400.0,
            valid_from=vf,
            valid_until=vu,
        )
        assert sw.switch_id == "SW-001"
        assert sw.state == SwitchState.CLOSED
        assert sw.is_automatic is False
        assert sw.sensitivity == SensitivityTier.HIGH

    def test_switch_state_enum_values(self) -> None:
        """SwitchState should contain all expected states."""
        expected = {"open", "closed", "unknown"}
        actual = {s.value for s in SwitchState}
        assert actual == expected

    def test_switch_rated_current_must_be_positive(self) -> None:
        """rated_current_a <= 0 should raise ValidationError."""
        vf, vu = _validity_window()
        with pytest.raises(ValidationError):
            Switch(
                switch_id="SW-002",
                name="Bad Switch",
                feeder_id="F-102",
                from_node_id="N-003",
                to_node_id="N-004",
                rated_current_a=0.0,
                valid_from=vf,
                valid_until=vu,
            )


# ---------------------------------------------------------------------------
# IEC 61850 models
# ---------------------------------------------------------------------------


class TestPQRange:
    """Tests for the IEC 61850 PQRange model."""

    def test_valid_pq_range(self) -> None:
        """A valid PQRange should validate."""
        pq = PQRange(
            p_min_kw=-100.0,
            p_max_kw=500.0,
            q_min_kvar=-50.0,
            q_max_kvar=50.0,
        )
        assert pq.p_min_kw == -100.0
        assert pq.p_max_kw == 500.0

    def test_pq_range_allows_negative_p_min(self) -> None:
        """Negative p_min_kw (consumption) should be valid."""
        pq = PQRange(
            p_min_kw=-500.0,
            p_max_kw=0.0,
            q_min_kvar=-100.0,
            q_max_kvar=100.0,
        )
        assert pq.p_min_kw == -500.0


class TestStateOfCharge:
    """Tests for the IEC 61850 StateOfCharge model."""

    def test_valid_state_of_charge(self) -> None:
        """A valid StateOfCharge should validate."""
        soc = StateOfCharge(
            aggregate_soc_pct=65.0,
            total_energy_capacity_kwh=1000.0,
            available_energy_kwh=650.0,
        )
        assert soc.aggregate_soc_pct == 65.0
        assert soc.min_soc_limit_pct == 10.0
        assert soc.max_soc_limit_pct == 90.0
        assert soc.timestamp.tzinfo is not None

    def test_soc_percentage_exceeds_100(self) -> None:
        """SOC percentage > 100 should raise ValidationError."""
        with pytest.raises(ValidationError):
            StateOfCharge(
                aggregate_soc_pct=105.0,
                total_energy_capacity_kwh=1000.0,
                available_energy_kwh=1050.0,
            )


class TestDeviceClassMix:
    """Tests for the IEC 61850 DeviceClassMix model."""

    def test_valid_device_class_mix(self) -> None:
        """A valid DeviceClassMix should validate."""
        mix = DeviceClassMix(
            der_type=DERType.BATTERY_STORAGE,
            share_pct=40.0,
            aggregate_capacity_kw=200.0,
        )
        assert mix.der_type == DERType.BATTERY_STORAGE
        assert mix.share_pct == 40.0

    def test_der_type_enum_values(self) -> None:
        """DERType should contain all expected device types."""
        expected = {
            "battery_storage", "solar_pv", "wind", "ev_charger",
            "heat_pump", "chp", "controllable_load", "fuel_cell", "other",
        }
        actual = {t.value for t in DERType}
        assert actual == expected

    def test_share_pct_exceeds_100(self) -> None:
        """share_pct > 100 should raise ValidationError."""
        with pytest.raises(ValidationError):
            DeviceClassMix(
                der_type=DERType.SOLAR_PV,
                share_pct=110.0,
                aggregate_capacity_kw=500.0,
            )


class TestResponseConfidence:
    """Tests for the IEC 61850 ResponseConfidence model."""

    def test_valid_response_confidence(self) -> None:
        """A valid ResponseConfidence should validate."""
        rc = ResponseConfidence(
            level=ConfidenceLevel.HIGH,
            probability_pct=95.0,
            historical_delivery_rate_pct=92.0,
        )
        assert rc.level == ConfidenceLevel.HIGH
        assert rc.probability_pct == 95.0

    def test_confidence_level_enum_values(self) -> None:
        """ConfidenceLevel should contain all expected levels."""
        expected = {"high", "medium", "low", "indicative"}
        actual = {c.value for c in ConfidenceLevel}
        assert actual == expected

    def test_historical_delivery_rate_optional(self) -> None:
        """historical_delivery_rate_pct should default to None."""
        rc = ResponseConfidence(
            level=ConfidenceLevel.INDICATIVE,
            probability_pct=40.0,
        )
        assert rc.historical_delivery_rate_pct is None


class TestAvailabilityWindow:
    """Tests for the IEC 61850 AvailabilityWindow model."""

    def test_valid_availability_window(self) -> None:
        """A fully populated AvailabilityWindow should validate."""
        vf, vu = _validity_window()
        pq = PQRange(p_min_kw=-50.0, p_max_kw=200.0, q_min_kvar=-20.0, q_max_kvar=20.0)
        aw = AvailabilityWindow(
            window_id="AW-001",
            available_from=vf,
            available_until=vu,
            pq_range=pq,
            ramp_up_rate_kw_per_min=10.0,
            ramp_down_rate_kw_per_min=15.0,
            min_duration_minutes=15.0,
            max_duration_minutes=120.0,
        )
        assert aw.window_id == "AW-001"
        assert aw.pq_range.p_max_kw == 200.0

    def test_negative_ramp_rate_rejected(self) -> None:
        """Negative ramp rates should raise ValidationError."""
        vf, vu = _validity_window()
        pq = PQRange(p_min_kw=0.0, p_max_kw=100.0, q_min_kvar=0.0, q_max_kvar=10.0)
        with pytest.raises(ValidationError):
            AvailabilityWindow(
                window_id="AW-002",
                available_from=vf,
                available_until=vu,
                pq_range=pq,
                ramp_up_rate_kw_per_min=-5.0,
                ramp_down_rate_kw_per_min=10.0,
                min_duration_minutes=15.0,
                max_duration_minutes=60.0,
            )


class TestDERUnit:
    """Tests for the IEC 61850 DERUnit model."""

    def test_valid_der_unit(self) -> None:
        """A fully populated DERUnit should validate."""
        vf, vu = _validity_window()
        unit = DERUnit(
            unit_id="DER-001",
            name="Portfolio Alpha",
            aggregator_id="AGG-001",
            feeder_id="F-101",
            total_rated_capacity_kw=500.0,
            current_output_kw=150.0,
            valid_from=vf,
            valid_until=vu,
        )
        assert unit.unit_id == "DER-001"
        assert unit.is_available is True
        assert unit.sensitivity == SensitivityTier.MEDIUM
        assert unit.state_of_charge is None
        assert unit.device_class_mix == []

    def test_der_unit_with_soc_and_device_mix(self) -> None:
        """DERUnit with state_of_charge and device_class_mix should validate."""
        vf, vu = _validity_window()
        soc = StateOfCharge(
            aggregate_soc_pct=70.0,
            total_energy_capacity_kwh=500.0,
            available_energy_kwh=350.0,
        )
        mix = [
            DeviceClassMix(der_type=DERType.BATTERY_STORAGE, share_pct=60.0, aggregate_capacity_kw=300.0),
            DeviceClassMix(der_type=DERType.SOLAR_PV, share_pct=40.0, aggregate_capacity_kw=200.0),
        ]
        unit = DERUnit(
            unit_id="DER-002",
            name="Mixed Portfolio",
            aggregator_id="AGG-001",
            feeder_id="F-101",
            device_class_mix=mix,
            total_rated_capacity_kw=500.0,
            current_output_kw=200.0,
            state_of_charge=soc,
            valid_from=vf,
            valid_until=vu,
        )
        assert len(unit.device_class_mix) == 2
        assert unit.state_of_charge is not None
        assert unit.state_of_charge.aggregate_soc_pct == 70.0


class TestFlexibilityEnvelope:
    """Tests for the IEC 61850 FlexibilityEnvelope model."""

    def _make_envelope(self, **overrides) -> FlexibilityEnvelope:
        """Create a valid FlexibilityEnvelope with sensible defaults."""
        vf, vu = _validity_window()
        pq = PQRange(p_min_kw=-100.0, p_max_kw=300.0, q_min_kvar=-30.0, q_max_kvar=30.0)
        rc = ResponseConfidence(level=ConfidenceLevel.HIGH, probability_pct=92.0)
        defaults = dict(
            envelope_id="ENV-001",
            unit_id="DER-001",
            aggregator_id="AGG-001",
            feeder_id="F-101",
            direction=FlexibilityDirection.BOTH,
            pq_range=pq,
            response_confidence=rc,
            valid_from=vf,
            valid_until=vu,
        )
        defaults.update(overrides)
        return FlexibilityEnvelope(**defaults)

    def test_valid_flexibility_envelope(self) -> None:
        """A fully populated FlexibilityEnvelope should validate."""
        env = self._make_envelope()
        assert env.envelope_id == "ENV-001"
        assert env.direction == FlexibilityDirection.BOTH
        assert env.sensitivity == SensitivityTier.MEDIUM
        assert env.availability_windows == []
        assert env.device_class_mix == []
        assert env.price_eur_per_kwh is None

    def test_flexibility_envelope_with_availability_windows(self) -> None:
        """FlexibilityEnvelope with availability windows should validate."""
        vf, vu = _validity_window()
        pq = PQRange(p_min_kw=0.0, p_max_kw=200.0, q_min_kvar=0.0, q_max_kvar=20.0)
        windows = [
            AvailabilityWindow(
                window_id="AW-001",
                available_from=vf,
                available_until=vf + timedelta(hours=4),
                pq_range=pq,
                ramp_up_rate_kw_per_min=10.0,
                ramp_down_rate_kw_per_min=10.0,
                min_duration_minutes=15.0,
                max_duration_minutes=60.0,
            ),
            AvailabilityWindow(
                window_id="AW-002",
                available_from=vf + timedelta(hours=6),
                available_until=vu,
                pq_range=pq,
                ramp_up_rate_kw_per_min=15.0,
                ramp_down_rate_kw_per_min=15.0,
                min_duration_minutes=30.0,
                max_duration_minutes=120.0,
            ),
        ]
        env = self._make_envelope(availability_windows=windows)
        assert len(env.availability_windows) == 2
        assert env.availability_windows[0].window_id == "AW-001"

    def test_flexibility_envelope_with_price(self) -> None:
        """FlexibilityEnvelope with optional price should validate."""
        env = self._make_envelope(price_eur_per_kwh=0.15)
        assert env.price_eur_per_kwh == 0.15

    def test_flexibility_envelope_negative_price_rejected(self) -> None:
        """Negative price should raise ValidationError."""
        with pytest.raises(ValidationError):
            self._make_envelope(price_eur_per_kwh=-0.05)

    def test_flexibility_envelope_with_soc(self) -> None:
        """FlexibilityEnvelope with SOC snapshot should validate."""
        soc = StateOfCharge(
            aggregate_soc_pct=55.0,
            total_energy_capacity_kwh=800.0,
            available_energy_kwh=440.0,
        )
        env = self._make_envelope(state_of_charge=soc)
        assert env.state_of_charge is not None
        assert env.state_of_charge.aggregate_soc_pct == 55.0

    def test_flexibility_envelope_with_device_mix(self) -> None:
        """FlexibilityEnvelope with device class mix should validate."""
        mix = [
            DeviceClassMix(der_type=DERType.BATTERY_STORAGE, share_pct=70.0, aggregate_capacity_kw=350.0),
            DeviceClassMix(der_type=DERType.EV_CHARGER, share_pct=30.0, aggregate_capacity_kw=150.0),
        ]
        env = self._make_envelope(device_class_mix=mix)
        assert len(env.device_class_mix) == 2

    def test_flexibility_direction_enum_values(self) -> None:
        """FlexibilityDirection should contain all expected directions."""
        expected = {"up", "down", "both"}
        actual = {d.value for d in FlexibilityDirection}
        assert actual == expected

    def test_flexibility_envelope_timestamps_are_utc(self) -> None:
        """FlexibilityEnvelope timestamps should be timezone-aware."""
        env = self._make_envelope()
        assert env.valid_from.tzinfo is not None
        assert env.valid_until.tzinfo is not None
        assert env.updated_at.tzinfo is not None


# ---------------------------------------------------------------------------
# OpenADR models
# ---------------------------------------------------------------------------


class TestDRSignal:
    """Tests for the OpenADR DRSignal model."""

    def test_valid_dr_signal(self) -> None:
        """A fully populated DRSignal should validate."""
        vf, vu = _validity_window()
        sig = DRSignal(
            signal_id="SIG-001",
            signal_type=SignalType.PRICE,
            signal_name="ELECTRICITY_PRICE",
            current_value=0.25,
            target_kw=None,
            duration_minutes=60.0,
            valid_from=vf,
            valid_until=vu,
        )
        assert sig.signal_id == "SIG-001"
        assert sig.signal_type == SignalType.PRICE
        assert sig.target_kw is None

    def test_signal_type_enum_values(self) -> None:
        """SignalType should contain all expected types."""
        expected = {"level", "price", "load_dispatch", "load_control", "simple"}
        actual = {s.value for s in SignalType}
        assert actual == expected

    def test_negative_duration_rejected(self) -> None:
        """Negative duration_minutes should raise ValidationError."""
        vf, vu = _validity_window()
        with pytest.raises(ValidationError):
            DRSignal(
                signal_id="SIG-002",
                signal_type=SignalType.LEVEL,
                signal_name="TEST",
                current_value=1.0,
                duration_minutes=-10.0,
                valid_from=vf,
                valid_until=vu,
            )


class TestDREvent:
    """Tests for the OpenADR DREvent model."""

    def test_valid_dr_event(self) -> None:
        """A fully populated DREvent should validate."""
        vf, vu = _validity_window()
        event = DREvent(
            event_id="EVT-001",
            program_id="PROG-001",
            issuer_id="DSO-001",
            event_start=vf + timedelta(hours=1),
            event_end=vf + timedelta(hours=3),
            valid_from=vf,
            valid_until=vu,
        )
        assert event.event_id == "EVT-001"
        assert event.status == EventStatus.PENDING
        assert event.priority == 1
        assert event.is_emergency is False
        assert event.sensitivity == SensitivityTier.MEDIUM
        assert event.signals == []

    def test_event_status_enum_values(self) -> None:
        """EventStatus should contain all expected lifecycle states."""
        expected = {"pending", "active", "completed", "cancelled", "superseded"}
        actual = {s.value for s in EventStatus}
        assert actual == expected

    def test_dr_event_with_signals(self) -> None:
        """A DREvent with attached signals should validate."""
        vf, vu = _validity_window()
        sig = DRSignal(
            signal_id="SIG-001",
            signal_type=SignalType.LOAD_DISPATCH,
            signal_name="LOAD_TARGET",
            current_value=500.0,
            target_kw=500.0,
            duration_minutes=120.0,
            valid_from=vf,
            valid_until=vu,
        )
        event = DREvent(
            event_id="EVT-002",
            program_id="PROG-001",
            issuer_id="DSO-001",
            signals=[sig],
            event_start=vf,
            event_end=vf + timedelta(hours=2),
            valid_from=vf,
            valid_until=vu,
        )
        assert len(event.signals) == 1
        assert event.signals[0].target_kw == 500.0

    def test_dr_event_priority_range(self) -> None:
        """Priority outside [0, 3] should raise ValidationError."""
        vf, vu = _validity_window()
        with pytest.raises(ValidationError):
            DREvent(
                event_id="EVT-003",
                program_id="PROG-001",
                issuer_id="DSO-001",
                priority=5,
                event_start=vf,
                event_end=vf + timedelta(hours=1),
                valid_from=vf,
                valid_until=vu,
            )


class TestBaseline:
    """Tests for the OpenADR Baseline model."""

    def test_valid_baseline(self) -> None:
        """A fully populated Baseline should validate."""
        vf, vu = _validity_window()
        bl = Baseline(
            baseline_id="BL-001",
            event_id="EVT-001",
            participant_id="AGG-001",
            methodology="avg_10_of_10",
            values_kw=[100.0, 110.0, 105.0, 95.0],
            baseline_start=vf,
            baseline_end=vf + timedelta(hours=1),
            valid_from=vf,
            valid_until=vu,
        )
        assert bl.baseline_id == "BL-001"
        assert bl.methodology == "avg_10_of_10"
        assert bl.interval_minutes == 15.0
        assert bl.sensitivity == SensitivityTier.MEDIUM


class TestDispatchCommand:
    """Tests for the OpenADR DispatchCommand model."""

    def test_valid_dispatch_command(self) -> None:
        """A fully populated DispatchCommand should validate."""
        now = _utc_now()
        cmd = DispatchCommand(
            command_id="CMD-001",
            event_id="EVT-001",
            issuer_id="DSO-001",
            target_participant_id="AGG-001",
            contract_id="C-001",
            feeder_id="F-101",
            target_power_kw=200.0,
            activation_time=now + timedelta(minutes=5),
            duration_minutes=60.0,
        )
        assert cmd.command_id == "CMD-001"
        assert cmd.is_emergency is False
        assert cmd.sensitivity == SensitivityTier.MEDIUM


class TestDispatchActual:
    """Tests for the OpenADR DispatchActual model."""

    def test_valid_dispatch_actual(self) -> None:
        """A fully populated DispatchActual should validate."""
        now = _utc_now()
        actual = DispatchActual(
            actual_id="ACT-001",
            command_id="CMD-001",
            event_id="EVT-001",
            participant_id="AGG-001",
            feeder_id="F-101",
            commanded_kw=200.0,
            delivered_kw=185.0,
            delivery_start=now,
            delivery_end=now + timedelta(hours=1),
            delivery_accuracy_pct=92.5,
        )
        assert actual.actual_id == "ACT-001"
        assert actual.delivery_accuracy_pct == 92.5
        assert actual.sensitivity == SensitivityTier.MEDIUM

    def test_dispatch_actual_accuracy_exceeds_100(self) -> None:
        """delivery_accuracy_pct > 100 should raise ValidationError."""
        now = _utc_now()
        with pytest.raises(ValidationError):
            DispatchActual(
                actual_id="ACT-002",
                command_id="CMD-001",
                event_id="EVT-001",
                participant_id="AGG-001",
                feeder_id="F-101",
                commanded_kw=200.0,
                delivered_kw=210.0,
                delivery_start=now,
                delivery_end=now + timedelta(hours=1),
                delivery_accuracy_pct=105.0,
            )


# ---------------------------------------------------------------------------
# Consumer models
# ---------------------------------------------------------------------------


class TestDisclosureLevel:
    """Tests for the Consumer DisclosureLevel enum."""

    def test_enum_values(self) -> None:
        """DisclosureLevel should contain all expected levels."""
        expected = {"raw", "identified", "anonymized", "aggregated", "controllability"}
        actual = {d.value for d in DisclosureLevel}
        assert actual == expected


class TestPurposeDisclosureMap:
    """Tests for the PURPOSE_DISCLOSURE_MAP."""

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

    def test_all_expected_purposes_present(self) -> None:
        """All expected purposes should be in the map."""
        expected_purposes = {"research", "dr_dispatch", "billing", "grid_analysis", "forecasting"}
        assert set(PURPOSE_DISCLOSURE_MAP.keys()) == expected_purposes


class TestMeterReading:
    """Tests for the Consumer MeterReading model."""

    def test_valid_meter_reading(self) -> None:
        """A fully populated MeterReading should validate."""
        now = _utc_now()
        mr = MeterReading(
            reading_id="MR-001",
            meter_id="MTR-001",
            prosumer_id="P-001",
            active_power_kw=3.5,
            reading_timestamp=now,
        )
        assert mr.reading_id == "MR-001"
        assert mr.sensitivity == SensitivityTier.HIGH_PRIVACY
        assert mr.quality_flag == "valid"
        assert mr.interval_minutes == 15.0
        assert mr.reactive_power_kvar is None
        assert mr.voltage_v is None

    def test_meter_reading_default_sensitivity_is_high_privacy(self) -> None:
        """MeterReading defaults to HIGH_PRIVACY sensitivity."""
        now = _utc_now()
        mr = MeterReading(
            reading_id="MR-002",
            meter_id="MTR-001",
            prosumer_id="P-001",
            active_power_kw=2.0,
            reading_timestamp=now,
        )
        assert mr.sensitivity == SensitivityTier.HIGH_PRIVACY

    def test_meter_reading_negative_voltage_rejected(self) -> None:
        """Negative voltage should raise ValidationError."""
        now = _utc_now()
        with pytest.raises(ValidationError):
            MeterReading(
                reading_id="MR-003",
                meter_id="MTR-001",
                prosumer_id="P-001",
                active_power_kw=3.0,
                voltage_v=-230.0,
                reading_timestamp=now,
            )


class TestDemandProfile:
    """Tests for the Consumer DemandProfile model."""

    def _make_profile(self, **overrides) -> DemandProfile:
        """Create a valid DemandProfile with sensible defaults."""
        vf, vu = _validity_window()
        defaults = dict(
            profile_id="DP-001",
            prosumer_id="P-001",
            profile_type="historical",
            values_kw=[2.0, 3.0, 4.0, 3.5, 2.5],
            peak_demand_kw=4.0,
            total_energy_kwh=15.0,
            profile_start=vf,
            profile_end=vu,
            valid_from=vf,
            valid_until=vu,
        )
        defaults.update(overrides)
        return DemandProfile(**defaults)

    def test_valid_demand_profile(self) -> None:
        """A fully populated DemandProfile should validate."""
        profile = self._make_profile()
        assert profile.profile_id == "DP-001"
        assert profile.disclosure_level == DisclosureLevel.RAW
        assert profile.sensitivity == SensitivityTier.HIGH_PRIVACY
        assert profile.interval_minutes == 15.0

    def test_demand_profile_default_disclosure_is_raw(self) -> None:
        """DemandProfile defaults to RAW disclosure level."""
        profile = self._make_profile()
        assert profile.disclosure_level == DisclosureLevel.RAW

    def test_demand_profile_default_sensitivity_is_high_privacy(self) -> None:
        """DemandProfile defaults to HIGH_PRIVACY sensitivity."""
        profile = self._make_profile()
        assert profile.sensitivity == SensitivityTier.HIGH_PRIVACY

    def test_demand_profile_negative_total_energy_rejected(self) -> None:
        """Negative total_energy_kwh should raise ValidationError."""
        with pytest.raises(ValidationError):
            self._make_profile(total_energy_kwh=-5.0)

    def test_demand_profile_timestamps_are_utc(self) -> None:
        """DemandProfile timestamps should be timezone-aware."""
        profile = self._make_profile()
        assert profile.valid_from.tzinfo is not None
        assert profile.valid_until.tzinfo is not None
        assert profile.updated_at.tzinfo is not None
        assert profile.profile_start.tzinfo is not None
        assert profile.profile_end.tzinfo is not None


class TestAnonymizedLoadSeries:
    """Tests for the Consumer AnonymizedLoadSeries model."""

    def test_valid_anonymized_load_series(self) -> None:
        """A fully populated AnonymizedLoadSeries should validate."""
        vf, vu = _validity_window()
        als = AnonymizedLoadSeries(
            series_id="ALS-001",
            source_count=10,
            feeder_id="F-101",
            values_kw=[50.0, 55.0, 60.0, 52.0],
            mean_kw=54.25,
            std_dev_kw=4.27,
            peak_kw=60.0,
            min_kw=50.0,
            k_anonymity_level=5,
            series_start=vf,
            series_end=vu,
            valid_from=vf,
            valid_until=vu,
        )
        assert als.series_id == "ALS-001"
        assert als.source_count == 10
        assert als.disclosure_level == DisclosureLevel.ANONYMIZED
        assert als.sensitivity == SensitivityTier.MEDIUM

    def test_anonymized_load_series_k_anonymity_minimum(self) -> None:
        """k_anonymity_level < 2 should raise ValidationError."""
        vf, vu = _validity_window()
        with pytest.raises(ValidationError):
            AnonymizedLoadSeries(
                series_id="ALS-002",
                source_count=1,
                values_kw=[10.0],
                mean_kw=10.0,
                std_dev_kw=0.0,
                peak_kw=10.0,
                min_kw=10.0,
                k_anonymity_level=1,
                series_start=vf,
                series_end=vu,
                valid_from=vf,
                valid_until=vu,
            )

    def test_anonymized_load_series_source_count_minimum(self) -> None:
        """source_count < 1 should raise ValidationError."""
        vf, vu = _validity_window()
        with pytest.raises(ValidationError):
            AnonymizedLoadSeries(
                series_id="ALS-003",
                source_count=0,
                values_kw=[10.0],
                mean_kw=10.0,
                std_dev_kw=0.0,
                peak_kw=10.0,
                min_kw=10.0,
                k_anonymity_level=5,
                series_start=vf,
                series_end=vu,
                valid_from=vf,
                valid_until=vu,
            )


class TestConsentRecord:
    """Tests for the Consumer ConsentRecord model."""

    def test_valid_consent_record(self) -> None:
        """A fully populated ConsentRecord should validate."""
        vf, vu = _validity_window()
        consent = ConsentRecord(
            consent_id="CON-001",
            prosumer_id="P-001",
            requester_id="AGG-001",
            purpose="research",
            allowed_data_types=["demand_profile", "meter_reading"],
            disclosure_level=DisclosureLevel.AGGREGATED,
            valid_from=vf,
            valid_until=vu,
        )
        assert consent.consent_id == "CON-001"
        assert consent.status == ConsentStatus.ACTIVE
        assert consent.sensitivity == SensitivityTier.HIGH_PRIVACY
        assert consent.revoked_at is None

    def test_consent_status_enum_values(self) -> None:
        """ConsentStatus should contain all expected statuses."""
        expected = {"active", "revoked", "expired"}
        actual = {s.value for s in ConsentStatus}
        assert actual == expected

    def test_consent_record_default_sensitivity_is_high_privacy(self) -> None:
        """ConsentRecord defaults to HIGH_PRIVACY sensitivity."""
        vf, vu = _validity_window()
        consent = ConsentRecord(
            consent_id="CON-002",
            prosumer_id="P-002",
            requester_id="DSO-001",
            purpose="billing",
            allowed_data_types=["meter_reading"],
            disclosure_level=DisclosureLevel.IDENTIFIED_CONSENTED,
            valid_from=vf,
            valid_until=vu,
        )
        assert consent.sensitivity == SensitivityTier.HIGH_PRIVACY
