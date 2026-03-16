"""Flexibility envelope computation for the Aggregator participant node.

Computes aggregate flexibility F(t) = {(P, Q) | feasible} for a DER fleet
without exposing individual device states x_i.  This is the core privacy-
preserving computation: the Aggregator publishes only the aggregate
feasibility region, never the underlying device-level operating points.

The computation takes stored fleet data (flexibility offers, availability
windows, state of charge) and produces a consolidated
:class:`FlexibilityEnvelope` that represents what the Aggregator can offer
at a given point in time.

Key design decisions:
  - Aggregation uses Minkowski-sum-style bounds: the aggregate P range is
    the sum of individual unit P ranges, and likewise for Q.  This is
    conservative (inner approximation of the true feasible set) but safe.
  - State of charge is aggregated as a capacity-weighted average to avoid
    leaking individual battery levels.
  - Device class mix is merged by summing capacities per DER type and
    recomputing shares relative to the total.
  - Availability windows are intersected so the aggregate window covers
    only time periods where all contributing units are available.
  - Response confidence uses the worst-case (minimum) probability among
    contributing offers, ensuring the aggregate confidence is not over-
    stated.
  - The module operates on data already in the store; it does not access
    any external systems or individual device telemetry.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

from src.semantic.cim import SensitivityTier
from src.semantic.iec61850 import (
    AvailabilityWindow,
    ConfidenceLevel,
    DeviceClassMix,
    FlexibilityDirection,
    FlexibilityEnvelope,
    PQRange,
    ResponseConfidence,
    StateOfCharge,
)


def _utc_now() -> datetime:
    """Return the current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)


def _merge_pq_ranges(ranges: list[PQRange]) -> PQRange:
    """Sum PQ ranges using Minkowski-sum-style aggregation.

    The aggregate P and Q bounds are the element-wise sums of the
    individual ranges, providing a conservative inner approximation
    of the true feasible set.

    Args:
        ranges: List of PQ ranges to aggregate.

    Returns:
        A single PQ range representing the aggregate capability.

    Raises:
        ValueError: If the input list is empty.
    """
    if not ranges:
        raise ValueError("Cannot merge an empty list of PQ ranges")
    return PQRange(
        p_min_kw=sum(r.p_min_kw for r in ranges),
        p_max_kw=sum(r.p_max_kw for r in ranges),
        q_min_kvar=sum(r.q_min_kvar for r in ranges),
        q_max_kvar=sum(r.q_max_kvar for r in ranges),
    )


def _merge_state_of_charge(
    states: list[StateOfCharge],
) -> Optional[StateOfCharge]:
    """Aggregate state of charge as a capacity-weighted average.

    Individual battery levels are never exposed; only the weighted
    aggregate SOC, total capacity, and available energy are computed.

    Args:
        states: List of SOC snapshots to aggregate.

    Returns:
        Aggregated SOC, or ``None`` if the input list is empty.
    """
    if not states:
        return None

    total_capacity = sum(s.total_energy_capacity_kwh for s in states)
    if total_capacity <= 0:
        return None

    total_available = sum(s.available_energy_kwh for s in states)
    weighted_soc = sum(
        s.aggregate_soc_pct * s.total_energy_capacity_kwh for s in states
    ) / total_capacity
    weighted_min = sum(
        s.min_soc_limit_pct * s.total_energy_capacity_kwh for s in states
    ) / total_capacity
    weighted_max = sum(
        s.max_soc_limit_pct * s.total_energy_capacity_kwh for s in states
    ) / total_capacity

    return StateOfCharge(
        aggregate_soc_pct=round(weighted_soc, 2),
        total_energy_capacity_kwh=total_capacity,
        available_energy_kwh=total_available,
        min_soc_limit_pct=round(weighted_min, 2),
        max_soc_limit_pct=round(weighted_max, 2),
        timestamp=_utc_now(),
    )


def _merge_device_class_mix(
    mixes: list[list[DeviceClassMix]],
) -> list[DeviceClassMix]:
    """Merge device class compositions from multiple offers.

    Sums capacities per DER type and recomputes percentage shares
    relative to the new total.  Individual device counts and
    identifiers are never included.

    Args:
        mixes: List of device class mix lists to merge.

    Returns:
        Consolidated device class mix with recalculated shares.
    """
    capacity_by_type: dict[str, float] = {}
    for mix_list in mixes:
        for entry in mix_list:
            key = entry.der_type.value
            capacity_by_type[key] = (
                capacity_by_type.get(key, 0.0) + entry.aggregate_capacity_kw
            )

    total_capacity = sum(capacity_by_type.values())
    if total_capacity <= 0:
        return []

    from src.semantic.iec61850 import DERType

    result: list[DeviceClassMix] = []
    for der_type_value, capacity in sorted(capacity_by_type.items()):
        result.append(
            DeviceClassMix(
                der_type=DERType(der_type_value),
                share_pct=round(capacity / total_capacity * 100.0, 2),
                aggregate_capacity_kw=capacity,
            )
        )
    return result


def _intersect_availability_windows(
    window_lists: list[list[AvailabilityWindow]],
) -> list[AvailabilityWindow]:
    """Compute intersected availability windows across multiple offers.

    For each pair of overlapping windows from different offers, produces
    a merged window covering the overlapping time period with aggregated
    PQ ranges and worst-case ramp rates.

    If only one offer contributes windows, those windows are returned
    as-is (with new IDs).

    Args:
        window_lists: List of availability window lists from each offer.

    Returns:
        List of merged availability windows covering common time periods.
    """
    if not window_lists:
        return []

    # Flatten all windows from all offers
    all_windows: list[AvailabilityWindow] = []
    for wl in window_lists:
        all_windows.extend(wl)

    if not all_windows:
        return []

    # For a single offer's windows, return them with fresh IDs
    if len(window_lists) == 1:
        result: list[AvailabilityWindow] = []
        for w in all_windows:
            result.append(
                AvailabilityWindow(
                    window_id=f"AW-AGG-{uuid.uuid4().hex[:8]}",
                    available_from=w.available_from,
                    available_until=w.available_until,
                    pq_range=w.pq_range,
                    ramp_up_rate_kw_per_min=w.ramp_up_rate_kw_per_min,
                    ramp_down_rate_kw_per_min=w.ramp_down_rate_kw_per_min,
                    min_duration_minutes=w.min_duration_minutes,
                    max_duration_minutes=w.max_duration_minutes,
                )
            )
        return result

    # For multiple offers, compute pairwise intersections
    merged = window_lists[0]
    for next_windows in window_lists[1:]:
        new_merged: list[AvailabilityWindow] = []
        for w_a in merged:
            for w_b in next_windows:
                overlap_start = max(w_a.available_from, w_b.available_from)
                overlap_end = min(w_a.available_until, w_b.available_until)
                if overlap_start < overlap_end:
                    new_merged.append(
                        AvailabilityWindow(
                            window_id=f"AW-AGG-{uuid.uuid4().hex[:8]}",
                            available_from=overlap_start,
                            available_until=overlap_end,
                            pq_range=_merge_pq_ranges(
                                [w_a.pq_range, w_b.pq_range]
                            ),
                            ramp_up_rate_kw_per_min=min(
                                w_a.ramp_up_rate_kw_per_min,
                                w_b.ramp_up_rate_kw_per_min,
                            ),
                            ramp_down_rate_kw_per_min=min(
                                w_a.ramp_down_rate_kw_per_min,
                                w_b.ramp_down_rate_kw_per_min,
                            ),
                            min_duration_minutes=max(
                                w_a.min_duration_minutes,
                                w_b.min_duration_minutes,
                            ),
                            max_duration_minutes=min(
                                w_a.max_duration_minutes,
                                w_b.max_duration_minutes,
                            ),
                        )
                    )
        merged = new_merged

    return merged


def _aggregate_direction(
    directions: list[FlexibilityDirection],
) -> FlexibilityDirection:
    """Determine the aggregate flexibility direction.

    If all contributing offers share the same direction, the aggregate
    inherits it.  Otherwise, the aggregate is BOTH (bidirectional).

    Args:
        directions: List of directions from contributing offers.

    Returns:
        The aggregate flexibility direction.
    """
    unique = set(directions)
    if len(unique) == 1:
        return unique.pop()
    return FlexibilityDirection.BOTH


def _aggregate_confidence(
    confidences: list[ResponseConfidence],
) -> ResponseConfidence:
    """Compute worst-case aggregate confidence.

    Uses the minimum probability and lowest confidence level among
    contributing offers to avoid overstating aggregate deliverability.

    Args:
        confidences: List of confidence assessments to aggregate.

    Returns:
        Conservative aggregate confidence assessment.

    Raises:
        ValueError: If the input list is empty.
    """
    if not confidences:
        raise ValueError("Cannot aggregate an empty list of confidences")

    level_order = {
        ConfidenceLevel.INDICATIVE: 0,
        ConfidenceLevel.LOW: 1,
        ConfidenceLevel.MEDIUM: 2,
        ConfidenceLevel.HIGH: 3,
    }

    min_level = min(confidences, key=lambda c: level_order[c.level]).level
    min_probability = min(c.probability_pct for c in confidences)

    historical_rates = [
        c.historical_delivery_rate_pct
        for c in confidences
        if c.historical_delivery_rate_pct is not None
    ]
    min_historical = min(historical_rates) if historical_rates else None

    return ResponseConfidence(
        level=min_level,
        probability_pct=round(min_probability, 2),
        historical_delivery_rate_pct=(
            round(min_historical, 2) if min_historical is not None else None
        ),
    )


def compute_aggregate_flexibility(
    offers: list[FlexibilityEnvelope],
    *,
    aggregator_id: str = "aggregator-001",
    feeder_id: Optional[str] = None,
) -> Optional[FlexibilityEnvelope]:
    """Compute the aggregate flexibility envelope F(t) for a DER fleet.

    Aggregates multiple flexibility offers into a single envelope that
    represents the total feasible operating region {(P, Q) | feasible}
    without exposing individual device states x_i.

    The aggregation follows these rules:
      - **PQ range**: Minkowski sum of individual ranges (conservative).
      - **Availability windows**: Time-intersection of contributing windows.
      - **State of charge**: Capacity-weighted average SOC.
      - **Device class mix**: Merged by DER type with recalculated shares.
      - **Confidence**: Worst-case (minimum) across all offers.
      - **Price**: Capacity-weighted average of offered prices.

    Args:
        offers: List of flexibility envelopes to aggregate.
        aggregator_id: Identifier for the aggregator producing the envelope.
        feeder_id: Optional feeder filter.  When provided, only offers
            matching this feeder are included.

    Returns:
        A single aggregate ``FlexibilityEnvelope``, or ``None`` if no
        valid offers are available after filtering.
    """
    if feeder_id is not None:
        offers = [o for o in offers if o.feeder_id == feeder_id]

    if not offers:
        return None

    now = _utc_now()

    # Aggregate PQ range (Minkowski sum)
    aggregate_pq = _merge_pq_ranges([o.pq_range for o in offers])

    # Aggregate availability windows (time intersection)
    aggregate_windows = _intersect_availability_windows(
        [o.availability_windows for o in offers]
    )

    # Aggregate state of charge (capacity-weighted average)
    soc_list = [o.state_of_charge for o in offers if o.state_of_charge is not None]
    aggregate_soc = _merge_state_of_charge(soc_list)

    # Aggregate device class mix
    aggregate_mix = _merge_device_class_mix(
        [o.device_class_mix for o in offers]
    )

    # Aggregate confidence (worst-case)
    aggregate_confidence = _aggregate_confidence(
        [o.response_confidence for o in offers]
    )

    # Aggregate direction
    aggregate_direction = _aggregate_direction(
        [o.direction for o in offers]
    )

    # Capacity-weighted average price
    aggregate_price: Optional[float] = None
    priced_offers = [
        o for o in offers if o.price_eur_per_kwh is not None
    ]
    if priced_offers:
        total_capacity = sum(o.pq_range.p_max_kw for o in priced_offers)
        if total_capacity > 0:
            aggregate_price = round(
                sum(
                    o.price_eur_per_kwh * o.pq_range.p_max_kw  # type: ignore[operator]
                    for o in priced_offers
                )
                / total_capacity,
                4,
            )

    # Determine validity window (union of offer windows)
    valid_from = min(o.valid_from for o in offers)
    valid_until = max(o.valid_until for o in offers)

    # Determine feeder_id for the aggregate
    feeder_ids = list({o.feeder_id for o in offers})
    agg_feeder_id = feeder_ids[0] if len(feeder_ids) == 1 else "MULTI"

    return FlexibilityEnvelope(
        envelope_id=f"FE-AGG-{uuid.uuid4().hex[:8]}",
        unit_id=f"DER-FLEET-{agg_feeder_id}",
        aggregator_id=aggregator_id,
        feeder_id=agg_feeder_id,
        direction=aggregate_direction,
        pq_range=aggregate_pq,
        availability_windows=aggregate_windows,
        state_of_charge=aggregate_soc,
        response_confidence=aggregate_confidence,
        device_class_mix=aggregate_mix,
        price_eur_per_kwh=aggregate_price,
        valid_from=valid_from,
        valid_until=valid_until,
        sensitivity=SensitivityTier.MEDIUM,
        updated_at=now,
    )
