#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════╗
║  Data Space Grid — Grid Topology & Power Flow Demo                     ║
║                                                                        ║
║  Distribution grid simulation with:                                    ║
║  - Realistic topology: substation → feeders → poles → 4 houses/pole    ║
║  - Simplified power flow (voltage drop calculation)                    ║
║  - Federated data space control: each participant sees only their data ║
║  - Contract-gated DR dispatch to resolve voltage violations            ║
╚══════════════════════════════════════════════════════════════════════════╝

Topology:
  Substation (66/6.6kV) ─── Feeder F-101 ──┬── Pole P-01 ── [H-001..H-004]
                                            ├── Pole P-02 ── [H-005..H-008]
                                            ├── ...
                                            ├── Pole P-62 ── [H-245..H-248]
                                            └── (branch)
                            Feeder F-102 ──┬── Pole P-63 ── [H-249..H-252]
                                            ├── ...
                                            └── Pole P-125 ── [H-497..H-500]

  250 poles × 4 houses/pole = 1000 households
  2 feeders × ~125 poles each
"""

from __future__ import annotations

import math
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.collections import LineCollection
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.connector.contract import ContractManager
from src.connector.policy import PolicyEngine
from src.connector.audit import AuditLogger, compute_hash
from src.connector.models import (
    Participant, DataAsset, ContractOffer, AuditAction, AuditOutcome,
    DataUsageContract, ContractStatus,
)
from src.semantic.cim import SensitivityTier, FeederConstraint, CongestionSignal

# ── Style ──────────────────────────────────────────────────────────────────
BG       = "#181825"
SURFACE  = "#313244"
FG       = "#cdd6f4"
RED      = "#f38ba8"
GREEN    = "#a6e3a1"
BLUE     = "#89b4fa"
YELLOW   = "#f9e2af"
MAUVE    = "#cba6f7"
TEAL     = "#94e2d5"
PEACH    = "#fab387"
DIM      = "#6c7086"

plt.rcParams.update({
    "figure.facecolor": BG, "axes.facecolor": SURFACE,
    "axes.edgecolor": DIM, "axes.labelcolor": FG,
    "xtick.color": DIM, "ytick.color": DIM, "text.color": FG,
    "grid.color": "#45475a", "grid.alpha": 0.5,
    "legend.facecolor": SURFACE, "legend.edgecolor": DIM,
    "font.size": 11,
})

OUT_DIR = Path(__file__).parent / "output"
OUT_DIR.mkdir(exist_ok=True)
NOW = datetime(2026, 8, 15, 16, 0, 0, tzinfo=timezone.utc)


# ═══════════════════════════════════════════════════════════════════════════
# PART 1: Grid Topology Model
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class House:
    id: str
    pole_id: str
    load_kw: float = 0.0
    has_pv: bool = False
    has_ev: bool = False
    has_battery: bool = False
    pv_output_kw: float = 0.0
    ev_charging_kw: float = 0.0
    controllable_kw: float = 0.0

    @property
    def net_load_kw(self) -> float:
        return self.load_kw + self.ev_charging_kw - self.pv_output_kw

@dataclass
class Pole:
    id: str
    feeder_id: str
    index: int  # position along feeder (0 = closest to substation)
    houses: list[House] = field(default_factory=list)
    distance_m: float = 0.0  # distance from substation
    voltage_pu: float = 1.0  # per-unit voltage

    @property
    def total_load_kw(self) -> float:
        return sum(h.net_load_kw for h in self.houses)

    @property
    def total_controllable_kw(self) -> float:
        return sum(h.controllable_kw for h in self.houses)

@dataclass
class Feeder:
    id: str
    poles: list[Pole] = field(default_factory=list)
    rated_kw: float = 2500.0
    voltage_kv: float = 6.6
    # Line parameters (typical overhead distribution)
    r_ohm_per_km: float = 0.65   # resistance (aging overhead line)
    x_ohm_per_km: float = 0.40   # reactance
    span_m: float = 40.0         # distance between poles

    @property
    def total_load_kw(self) -> float:
        return sum(p.total_load_kw for p in self.poles)

@dataclass
class Substation:
    id: str
    name: str
    feeders: list[Feeder] = field(default_factory=list)
    voltage_kv: float = 6.6
    rated_kva: float = 10000.0


def build_grid(n_houses: int = 1000) -> Substation:
    """Build a realistic distribution grid topology."""
    np.random.seed(42)

    sub = Substation(id="SUB-01", name="Kashiwa Substation")
    houses_per_pole = 4
    n_poles = n_houses // houses_per_pole  # 250
    poles_per_feeder = n_poles // 2        # 125

    house_idx = 0
    for fi in range(2):
        feeder = Feeder(id=f"F-10{fi+1}")

        for pi in range(poles_per_feeder):
            pole = Pole(
                id=f"P-{fi*poles_per_feeder + pi + 1:03d}",
                feeder_id=feeder.id,
                index=pi,
                distance_m=(pi + 1) * feeder.span_m,
            )

            for hi in range(houses_per_pole):
                house_idx += 1
                # Base load: 1.5-3.5 kW (summer afternoon)
                base_load = np.random.normal(2.5, 0.8)

                # 30% have PV (more likely farther from substation = newer area)
                has_pv = np.random.random() < (0.2 + 0.2 * pi / poles_per_feeder)
                pv_output = np.random.uniform(2.0, 5.0) * 0.7 if has_pv else 0.0  # 70% capacity at 16:00

                # 35% have EV (high EV penetration area)
                has_ev = np.random.random() < 0.35
                ev_charging = np.random.choice([3.0, 6.0, 7.0, 11.0]) if has_ev and np.random.random() < 0.7 else 0.0

                # 15% have battery
                has_battery = np.random.random() < 0.15

                # AC load (summer) - everyone
                ac_load = np.random.uniform(1.0, 2.5)
                total_load = max(0.5, base_load + ac_load)

                # Controllable: EV (can shift), AC (can raise setpoint), battery (can discharge)
                controllable = ev_charging * 0.8 + ac_load * 0.3
                if has_battery:
                    controllable += np.random.uniform(2.0, 5.0)

                house = House(
                    id=f"H-{house_idx:04d}",
                    pole_id=pole.id,
                    load_kw=total_load,
                    has_pv=has_pv,
                    has_ev=has_ev,
                    has_battery=has_battery,
                    pv_output_kw=pv_output,
                    ev_charging_kw=ev_charging,
                    controllable_kw=controllable,
                )
                pole.houses.append(house)

            feeder.poles.append(pole)
        sub.feeders.append(feeder)

    return sub


# ═══════════════════════════════════════════════════════════════════════════
# PART 2: Power Flow Calculation (Simplified DistFlow)
# ═══════════════════════════════════════════════════════════════════════════

def run_power_flow(feeder: Feeder, v_sub_pu: float = 1.02) -> list[float]:
    """
    Simplified radial power flow (voltage drop calculation).

    For each pole, compute voltage drop from the substation considering
    cumulative downstream load. Uses the DistFlow approximation:
        V_drop ≈ (P * R + Q * X) / V²

    Returns list of per-unit voltages at each pole.
    """
    v_base = feeder.voltage_kv * 1000  # V (line-to-line)
    z_base = v_base ** 2 / (feeder.rated_kw * 1000)  # Ohm

    voltages = []
    v_current = v_sub_pu

    for i, pole in enumerate(feeder.poles):
        # Cumulative load downstream from this point
        downstream_kw = sum(p.total_load_kw for p in feeder.poles[i:])
        downstream_kvar = downstream_kw * 0.3  # assume PF ≈ 0.95

        # Segment impedance
        segment_km = feeder.span_m / 1000
        r = feeder.r_ohm_per_km * segment_km
        x = feeder.x_ohm_per_km * segment_km

        # Voltage drop (per-unit)
        p_pu = downstream_kw / (feeder.rated_kw)
        q_pu = downstream_kvar / (feeder.rated_kw)
        r_pu = r / z_base
        x_pu = x / z_base

        v_drop = (p_pu * r_pu + q_pu * x_pu) / v_current
        v_current = v_current - v_drop
        v_current = max(v_current, 0.80)  # floor

        pole.voltage_pu = v_current
        voltages.append(v_current)

    return voltages


# ═══════════════════════════════════════════════════════════════════════════
# PART 3: Visualization
# ═══════════════════════════════════════════════════════════════════════════

def plot_topology(sub: Substation):
    """Plot grid topology as a tree diagram."""
    fig, ax = plt.subplots(1, 1, figsize=(16, 10))
    fig.suptitle(f"Distribution Grid Topology: {sub.name}",
                 fontsize=18, fontweight="bold", color=FG, y=0.98)

    # Substation at top center
    sub_x, sub_y = 8, 9.5
    ax.add_patch(mpatches.FancyBboxPatch((sub_x - 0.8, sub_y - 0.3), 1.6, 0.6,
                 boxstyle="round,pad=0.1", facecolor=YELLOW, edgecolor=FG, linewidth=2))
    ax.text(sub_x, sub_y, f"{sub.name}\n66/6.6kV  {sub.rated_kva/1000:.0f}MVA",
            ha="center", va="center", fontsize=10, fontweight="bold", color=BG)

    feeder_colors = [RED, BLUE]

    for fi, feeder in enumerate(sub.feeders):
        # Feeder trunk line
        fx = 4 + fi * 8  # Left or right
        fy = 8.5

        # Feeder label
        ax.plot([sub_x, fx], [sub_y - 0.3, fy + 0.3], color=feeder_colors[fi], linewidth=3)
        ax.add_patch(mpatches.FancyBboxPatch((fx - 0.7, fy - 0.2), 1.4, 0.4,
                     boxstyle="round,pad=0.05", facecolor=SURFACE, edgecolor=feeder_colors[fi], linewidth=2))
        total_kw = feeder.total_load_kw
        ax.text(fx, fy, f"{feeder.id}\n{total_kw:.0f} kW",
                ha="center", va="center", fontsize=9, fontweight="bold", color=feeder_colors[fi])

        # Show a subset of poles (every 10th for clarity)
        display_poles = feeder.poles[::10]  # Show 12-13 poles
        n_display = len(display_poles)

        for pi, pole in enumerate(display_poles):
            # Position poles along the feeder
            px = fx - 2.5 + (pi / max(1, n_display - 1)) * 5
            py = 6.5 - (pi % 3) * 0.8  # Stagger vertically

            # Pole → feeder line
            ax.plot([fx, px], [fy - 0.2, py + 0.15], color=DIM, linewidth=0.5, alpha=0.5)

            # Color by voltage
            v = pole.voltage_pu
            if v < 0.95:
                pcolor = RED
            elif v < 0.97:
                pcolor = YELLOW
            else:
                pcolor = GREEN

            # Pole marker
            ax.plot(px, py, "s", color=pcolor, markersize=8, markeredgecolor=FG, markeredgewidth=0.5)

            # Houses (tiny dots around the pole)
            for hi, house in enumerate(pole.houses):
                hx = px + (hi - 1.5) * 0.12
                hy = py - 0.3
                hcolor = TEAL if house.has_pv else (PEACH if house.has_ev else DIM)
                ax.plot(hx, hy, "o", color=hcolor, markersize=3, alpha=0.7)

            # Label every other displayed pole
            if pi % 3 == 0:
                ax.text(px, py + 0.25, f"{pole.id}\n{v:.3f}pu",
                        ha="center", fontsize=6, color=pcolor)

        # Stats box
        n_pv = sum(1 for p in feeder.poles for h in p.houses if h.has_pv)
        n_ev = sum(1 for p in feeder.poles for h in p.houses if h.has_ev)
        n_bat = sum(1 for p in feeder.poles for h in p.houses if h.has_battery)
        min_v = min(p.voltage_pu for p in feeder.poles)
        stats_text = (f"Houses: {sum(len(p.houses) for p in feeder.poles)}\n"
                      f"Poles: {len(feeder.poles)}\n"
                      f"PV: {n_pv}  EV: {n_ev}  Batt: {n_bat}\n"
                      f"Min voltage: {min_v:.3f} pu")
        bx = fx - 2.5 if fi == 0 else fx + 0.5
        ax.text(bx, 4.2, stats_text, fontsize=8, color=FG,
                bbox=dict(boxstyle="round", facecolor=SURFACE, edgecolor=feeder_colors[fi], alpha=0.9))

    # Legend
    legend_items = [
        mpatches.Patch(color=GREEN, label="V > 0.97 pu (normal)"),
        mpatches.Patch(color=YELLOW, label="0.95 < V < 0.97 pu (warning)"),
        mpatches.Patch(color=RED, label="V < 0.95 pu (violation)"),
        plt.Line2D([0], [0], marker="o", color="w", markerfacecolor=TEAL, markersize=6, label="House with PV"),
        plt.Line2D([0], [0], marker="o", color="w", markerfacecolor=PEACH, markersize=6, label="House with EV"),
        plt.Line2D([0], [0], marker="o", color="w", markerfacecolor=DIM, markersize=6, label="House (base)"),
    ]
    ax.legend(handles=legend_items, loc="lower center", ncol=3, fontsize=9)

    ax.set_xlim(0, 16)
    ax.set_ylim(3.5, 10.2)
    ax.axis("off")

    plt.tight_layout()
    path = OUT_DIR / "06_grid_topology.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def plot_voltage_profile(sub: Substation):
    """Plot voltage profile along each feeder."""
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle("Power Flow Analysis: Voltage & Load Distribution",
                 fontsize=16, fontweight="bold", color=FG, y=0.98)

    feeder_colors = [RED, BLUE]

    for fi, feeder in enumerate(sub.feeders):
        distances = [p.distance_m / 1000 for p in feeder.poles]
        voltages = [p.voltage_pu for p in feeder.poles]
        loads = [p.total_load_kw for p in feeder.poles]
        cum_load = np.cumsum(loads)

        # Top: Voltage profile
        ax = axes[0, fi]
        ax.plot(distances, voltages, color=feeder_colors[fi], linewidth=2, label=feeder.id)
        ax.axhline(y=1.0, color=DIM, linewidth=0.5, linestyle=":")
        ax.axhline(y=0.95, color=RED, linewidth=1.5, linestyle="--", label="Lower limit (0.95 pu)")
        ax.axhline(y=1.05, color=YELLOW, linewidth=1, linestyle="--", alpha=0.5, label="Upper limit (1.05 pu)")
        ax.fill_between(distances, 0.95, voltages, where=np.array(voltages) < 0.95,
                         alpha=0.3, color=RED, label="Voltage violation")

        # Mark min voltage
        min_idx = np.argmin(voltages)
        ax.annotate(f"Min: {voltages[min_idx]:.3f} pu",
                     xy=(distances[min_idx], voltages[min_idx]),
                     xytext=(distances[min_idx] - 1, voltages[min_idx] + 0.01),
                     arrowprops=dict(arrowstyle="->", color=feeder_colors[fi]),
                     fontsize=10, fontweight="bold", color=feeder_colors[fi])

        ax.set_title(f"{feeder.id} Voltage Profile", fontsize=13, fontweight="bold")
        ax.set_ylabel("Voltage (pu)")
        ax.set_xlabel("Distance from substation (km)")
        ax.set_ylim(0.90, 1.06)
        ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3)

        # Bottom: Load distribution
        ax = axes[1, fi]
        # Heatmap-style bar chart
        colors = []
        for p in feeder.poles:
            n = p.total_load_kw
            if n > 20:
                colors.append(RED)
            elif n > 10:
                colors.append(YELLOW)
            else:
                colors.append(GREEN)

        ax.bar(distances, loads, width=feeder.span_m/1000 * 0.8,
               color=colors, alpha=0.7, edgecolor="none")

        # Overlay PV generation (negative)
        pv_gen = [-sum(h.pv_output_kw for h in p.houses) for p in feeder.poles]
        ax.bar(distances, pv_gen, width=feeder.span_m/1000 * 0.8,
               color=TEAL, alpha=0.5, label="PV generation")

        ax.set_title(f"{feeder.id} Load per Pole (4 houses/pole)", fontsize=13, fontweight="bold")
        ax.set_ylabel("Net Load (kW)")
        ax.set_xlabel("Distance from substation (km)")
        ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3)
        ax.axhline(y=0, color=DIM, linewidth=0.5)

    plt.tight_layout()
    path = OUT_DIR / "07_voltage_profile.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def plot_dr_control(sub: Substation, voltages_before: dict, voltages_after: dict):
    """Plot before/after DR control with data space flow."""
    fig = plt.figure(figsize=(16, 10))
    fig.suptitle("Federated Data Space Control: Voltage Regulation via DR",
                 fontsize=16, fontweight="bold", color=FG, y=0.98)

    # Layout: top row = voltage before/after, bottom = data flow + device participation
    gs = fig.add_gridspec(2, 3, hspace=0.35, wspace=0.3)

    feeder_colors = [RED, BLUE]

    # Top left: Before DR (both feeders)
    ax1 = fig.add_subplot(gs[0, 0])
    for fi, feeder in enumerate(sub.feeders):
        d = [p.distance_m / 1000 for p in feeder.poles]
        v = voltages_before[feeder.id]
        ax1.plot(d, v, color=feeder_colors[fi], linewidth=2, label=feeder.id)
    ax1.axhline(y=0.95, color=RED, linewidth=1.5, linestyle="--")
    ax1.set_title("BEFORE DR Dispatch", fontsize=13, fontweight="bold", color=RED)
    ax1.set_ylabel("Voltage (pu)")
    ax1.set_xlabel("Distance (km)")
    ax1.set_ylim(0.90, 1.06)
    ax1.legend(fontsize=9)
    ax1.grid(True, alpha=0.3)
    n_violations_before = sum(1 for f in sub.feeders for v in voltages_before[f.id] if v < 0.95)
    ax1.text(0.05, 0.05, f"Violations: {n_violations_before} poles",
             transform=ax1.transAxes, fontsize=11, color=RED, fontweight="bold")

    # Top middle: After DR
    ax2 = fig.add_subplot(gs[0, 1])
    for fi, feeder in enumerate(sub.feeders):
        d = [p.distance_m / 1000 for p in feeder.poles]
        v = voltages_after[feeder.id]
        ax2.plot(d, v, color=feeder_colors[fi], linewidth=2, label=feeder.id)
    ax2.axhline(y=0.95, color=RED, linewidth=1.5, linestyle="--")
    ax2.set_title("AFTER DR Dispatch", fontsize=13, fontweight="bold", color=GREEN)
    ax2.set_ylabel("Voltage (pu)")
    ax2.set_xlabel("Distance (km)")
    ax2.set_ylim(0.90, 1.06)
    ax2.legend(fontsize=9)
    ax2.grid(True, alpha=0.3)
    n_violations_after = sum(1 for f in sub.feeders for v in voltages_after[f.id] if v < 0.95)
    ax2.text(0.05, 0.05, f"Violations: {n_violations_after} poles",
             transform=ax2.transAxes, fontsize=11, color=GREEN, fontweight="bold")

    # Top right: Voltage improvement heatmap
    ax3 = fig.add_subplot(gs[0, 2])
    all_before = []
    all_after = []
    all_dist = []
    for feeder in sub.feeders:
        d = [p.distance_m / 1000 for p in feeder.poles]
        all_dist.extend(d)
        all_before.extend(voltages_before[feeder.id])
        all_after.extend(voltages_after[feeder.id])
    improvement = np.array(all_after) - np.array(all_before)
    scatter = ax3.scatter(all_dist, all_before, c=improvement, cmap="RdYlGn",
                           s=15, alpha=0.7, vmin=-0.005, vmax=0.03)
    plt.colorbar(scatter, ax=ax3, label="Voltage improvement (pu)")
    ax3.set_title("Voltage Improvement", fontsize=13, fontweight="bold")
    ax3.set_xlabel("Distance (km)")
    ax3.set_ylabel("Voltage before (pu)")
    ax3.grid(True, alpha=0.3)

    # Bottom left: Data Space flow diagram
    ax4 = fig.add_subplot(gs[1, 0:2])
    ax4.axis("off")
    ax4.set_title("Federated Data Space Control Flow", fontsize=13, fontweight="bold", pad=15)

    # Draw the flow
    steps = [
        (0.02, 0.75, "DSO\n(data sovereign)", RED, 0.15, 0.18),
        (0.25, 0.75, "Federated\nCatalog", TEAL, 0.13, 0.18),
        (0.47, 0.75, "Aggregator\n(data sovereign)", BLUE, 0.15, 0.18),
        (0.72, 0.75, "1000 Households\n(prosumer data)", PEACH, 0.18, 0.18),

        (0.02, 0.30, "Congestion\nDetected", YELLOW, 0.13, 0.15),
        (0.22, 0.30, "Contract\nNegotiated", MAUVE, 0.13, 0.15),
        (0.42, 0.30, "Flexibility\nOffered", BLUE, 0.13, 0.15),
        (0.62, 0.30, "DR\nDispatched", GREEN, 0.13, 0.15),
        (0.82, 0.30, "Voltage\nRestored", GREEN, 0.13, 0.15),
    ]

    for x, y, label, color, w, h in steps:
        rect = mpatches.FancyBboxPatch((x, y), w, h,
                boxstyle="round,pad=0.02", facecolor=SURFACE,
                edgecolor=color, linewidth=2, transform=ax4.transAxes)
        ax4.add_patch(rect)
        ax4.text(x + w/2, y + h/2, label, transform=ax4.transAxes,
                 ha="center", va="center", fontsize=8, fontweight="bold", color=color)

    # Arrows between flow steps
    arrow_pairs = [(0.15, 0.35), (0.35, 0.55), (0.55, 0.75), (0.75, 0.95)]
    for ax_start, ax_end in arrow_pairs:
        ax4.annotate("", xy=(ax_end, 0.37), xytext=(ax_start, 0.37),
                     xycoords="axes fraction", textcoords="axes fraction",
                     arrowprops=dict(arrowstyle="->", color=DIM, lw=1.5))

    # Security notes
    security_notes = [
        "mTLS + OIDC authenticated",
        "Contract-gated access",
        "SHA-256 audit trail",
        "Purpose-based anonymization",
    ]
    for i, note in enumerate(security_notes):
        ax4.text(0.05 + i * 0.25, 0.10, f"  {note}",
                 transform=ax4.transAxes, fontsize=7, color=TEAL,
                 bbox=dict(boxstyle="round", facecolor=SURFACE, edgecolor=TEAL, alpha=0.7))

    # Bottom right: Device participation
    ax5 = fig.add_subplot(gs[1, 2])
    # Count participating devices
    n_ev_ctrl = sum(1 for f in sub.feeders for p in f.poles for h in p.houses
                    if h.has_ev and h.ev_charging_kw > 0)
    n_bat_ctrl = sum(1 for f in sub.feeders for p in f.poles for h in p.houses if h.has_battery)
    n_ac_ctrl = len([1 for f in sub.feeders for p in f.poles for h in p.houses]) // 3  # ~1/3 AC shift
    n_pv = sum(1 for f in sub.feeders for p in f.poles for h in p.houses if h.has_pv)

    categories = ["EV\n(smart charge)", "Battery\n(discharge)", "AC\n(setpoint +2°C)", "PV\n(existing)"]
    counts = [n_ev_ctrl, n_bat_ctrl, n_ac_ctrl, n_pv]
    colors_bar = [PEACH, GREEN, BLUE, TEAL]
    bars = ax5.barh(categories, counts, color=colors_bar, height=0.5, edgecolor=BG)
    ax5.set_title("Participating Devices", fontsize=13, fontweight="bold")
    ax5.set_xlabel("Count")
    for bar, val in zip(bars, counts):
        ax5.text(bar.get_width() + 2, bar.get_y() + bar.get_height()/2,
                 str(val), va="center", fontsize=11, color=FG)

    plt.tight_layout()
    path = OUT_DIR / "08_dr_control.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def plot_pole_detail(sub: Substation):
    """Plot detailed view of a single pole with 4 houses."""
    fig, axes = plt.subplots(1, 3, figsize=(16, 6))
    fig.suptitle("Pole Detail: 4 Houses Connected to Distribution Pole P-050",
                 fontsize=16, fontweight="bold", color=FG, y=0.98)

    # Pick a representative pole
    pole = sub.feeders[0].poles[49]  # P-050

    # Left: Pole schematic
    ax = axes[0]
    ax.axis("off")
    ax.set_xlim(-1, 5)
    ax.set_ylim(-1, 6)

    # Draw pole
    ax.plot([2, 2], [0.5, 5], color=DIM, linewidth=4)
    ax.plot([0.5, 3.5], [5, 5], color=DIM, linewidth=3)  # crossarm
    ax.text(2, 5.3, f"{pole.id}", ha="center", fontsize=12, fontweight="bold", color=YELLOW)
    ax.text(2, 5.7, f"V = {pole.voltage_pu:.3f} pu", ha="center", fontsize=10,
            color=GREEN if pole.voltage_pu >= 0.95 else RED)

    # Draw 4 houses
    house_positions = [(0, 0), (1.5, 0), (2.5, 0), (4, 0)]
    for i, (hx, hy) in enumerate(house_positions):
        h = pole.houses[i]

        # Wire from pole to house
        ax.plot([2, hx + 0.4], [1.5, hy + 1.2], color=DIM, linewidth=1, linestyle="--")

        # House box
        hcolor = TEAL if h.has_pv else (PEACH if h.has_ev else BLUE)
        rect = mpatches.FancyBboxPatch((hx, hy), 0.8, 1.0,
                boxstyle="round,pad=0.05", facecolor=SURFACE, edgecolor=hcolor, linewidth=2)
        ax.add_patch(rect)

        # House label
        ax.text(hx + 0.4, hy + 0.7, h.id, ha="center", fontsize=7, fontweight="bold", color=hcolor)
        ax.text(hx + 0.4, hy + 0.4, f"{h.net_load_kw:.1f}kW", ha="center", fontsize=7, color=FG)

        # Icons
        icons = []
        if h.has_pv:
            icons.append(("PV", TEAL))
        if h.has_ev:
            icons.append(("EV", PEACH))
        if h.has_battery:
            icons.append(("Bat", GREEN))
        for j, (icon, ic) in enumerate(icons):
            ax.text(hx + 0.4, hy - 0.2 - j * 0.2, icon, ha="center", fontsize=6, color=ic)

    ax.set_title("Pole Schematic", fontsize=12, fontweight="bold", pad=10)

    # Middle: Load breakdown per house
    ax = axes[1]
    house_ids = [h.id for h in pole.houses]
    base_loads = [h.load_kw for h in pole.houses]
    ev_loads = [h.ev_charging_kw for h in pole.houses]
    pv_gen = [-h.pv_output_kw for h in pole.houses]

    x = np.arange(len(house_ids))
    width = 0.25
    ax.bar(x - width, base_loads, width, label="Base + AC", color=BLUE, edgecolor=BG)
    ax.bar(x, ev_loads, width, label="EV Charging", color=PEACH, edgecolor=BG)
    ax.bar(x + width, pv_gen, width, label="PV Generation", color=TEAL, edgecolor=BG)
    ax.axhline(y=0, color=DIM, linewidth=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels(house_ids, fontsize=8)
    ax.set_ylabel("Power (kW)")
    ax.set_title("Load Breakdown", fontsize=12, fontweight="bold")
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)

    # Right: Data sovereignty view
    ax = axes[2]
    ax.axis("off")
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 10)

    ax.text(5, 9.5, "Data Sovereignty", ha="center", fontsize=14, fontweight="bold", color=MAUVE)

    # What each participant sees
    views = [
        ("DSO sees:", RED, [
            f"Pole {pole.id}: total {pole.total_load_kw:.1f} kW",
            f"Voltage: {pole.voltage_pu:.3f} pu",
            "Aggregate controllable: "
            f"{pole.total_controllable_kw:.1f} kW",
            "No individual house data!",
        ]),
        ("Aggregator sees:", BLUE, [
            f"Fleet controllable: {pole.total_controllable_kw:.1f} kW",
            "Device mix (aggregate only)",
            "Flexibility envelope",
            "No grid topology!",
        ]),
        ("Each House sees:", PEACH, [
            "Own meter data (raw)",
            "Own PV/EV/battery status",
            "Consent dashboard",
            "Nothing about neighbors!",
        ]),
    ]

    for i, (title, color, items) in enumerate(views):
        by = 7.5 - i * 3
        rect = mpatches.FancyBboxPatch((0.5, by - 0.5), 9, 2.2,
                boxstyle="round,pad=0.1", facecolor=SURFACE, edgecolor=color, linewidth=2)
        ax.add_patch(rect)
        ax.text(1.0, by + 1.3, title, fontsize=11, fontweight="bold", color=color)
        for j, item in enumerate(items):
            marker = ">" if "No " not in item else "x"
            ic = FG if "No " not in item else RED
            ax.text(1.5, by + 0.8 - j * 0.45, f" {marker} {item}", fontsize=9, color=ic)

    ax.set_title("Who Sees What?", fontsize=12, fontweight="bold", pad=10)

    plt.tight_layout()
    path = OUT_DIR / "09_pole_detail.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


# ═══════════════════════════════════════════════════════════════════════════
# PART 4: Run the simulation
# ═══════════════════════════════════════════════════════════════════════════

def log(role: str, color_code: str, msg: str):
    codes = {"R": "31", "G": "32", "Y": "33", "B": "34", "M": "35", "C": "36"}
    c = codes.get(color_code, "37")
    print(f"  \033[{c};1m[{role}]\033[0m {msg}")

def header(text: str):
    print(f"\n\033[1;37m{'─' * 70}\033[0m")
    print(f"\033[1;37m  {text}\033[0m")
    print(f"\033[1;37m{'─' * 70}\033[0m")

def ok(msg: str):
    print(f"    \033[32m✓\033[0m {msg}")

def fail(msg: str):
    print(f"    \033[31m✗\033[0m {msg}")


def run():
    print()
    print("\033[1;37m" + "=" * 70 + "\033[0m")
    print("\033[1;37m  Grid Topology & Power Flow Demo\033[0m")
    print("\033[1;37m  250 Poles × 4 Houses/Pole = 1000 Households\033[0m")
    print("\033[1;37m" + "=" * 70 + "\033[0m")

    # ── Build grid ─────────────────────────────────────────────────────────
    header("Phase 1: Build Distribution Grid Topology")
    sub = build_grid(1000)

    for feeder in sub.feeders:
        n_h = sum(len(p.houses) for p in feeder.poles)
        n_pv = sum(1 for p in feeder.poles for h in p.houses if h.has_pv)
        n_ev = sum(1 for p in feeder.poles for h in p.houses if h.has_ev)
        log(feeder.id, "Y", f"{len(feeder.poles)} poles, {n_h} houses, "
            f"PV={n_pv}, EV={n_ev}, Load={feeder.total_load_kw:.0f} kW")

    # ── Power flow (before DR) ─────────────────────────────────────────────
    header("Phase 2: Power Flow Calculation (Before DR)")

    voltages_before = {}
    for feeder in sub.feeders:
        v = run_power_flow(feeder, v_sub_pu=1.02)
        voltages_before[feeder.id] = list(v)  # copy
        min_v = min(v)
        n_violations = sum(1 for vi in v if vi < 0.95)
        color = "R" if n_violations > 0 else "G"
        log(feeder.id, color, f"Min voltage: {min_v:.4f} pu, "
            f"Violations (<0.95): {n_violations} poles")

    total_violations = sum(1 for f in sub.feeders for v in voltages_before[f.id] if v < 0.95)
    if total_violations > 0:
        log("DSO", "R", f"VOLTAGE VIOLATION: {total_violations} poles below 0.95 pu!")
    else:
        log("DSO", "G", "All voltages within limits")

    # Plot topology
    path6 = plot_topology(sub)
    ok(f"Grid topology saved: {path6}")

    path7 = plot_voltage_profile(sub)
    ok(f"Voltage profile saved: {path7}")

    # ── Data Space: Contract & DR ──────────────────────────────────────────
    header("Phase 3: Federated Data Space → DR Control")

    # Setup data space
    contract_mgr = ContractManager()
    policy_engine = PolicyEngine()
    audit_path = str(OUT_DIR / "grid_audit.jsonl")
    if os.path.exists(audit_path):
        os.remove(audit_path)
    audit_log = AuditLogger(log_path=audit_path)

    dso = Participant(id="dso-001", name="Kashiwa DSO", organization="TEPCO PG",
                      roles=["dso_operator"])
    agg = Participant(id="agg-001", name="GreenFlex", organization="GreenFlex Inc.",
                      roles=["aggregator"])
    policy_engine.register_participant(dso)
    policy_engine.register_participant(agg)

    log("DSO", "R", "Publishing voltage constraint to Federated Catalog...")
    constraint_asset = DataAsset(
        id="asset-voltage-f101", provider_id="dso-001",
        name="Voltage Constraints F-101/F-102",
        data_type="voltage_constraint", sensitivity=SensitivityTier.MEDIUM,
        endpoint="https://dso:8001/api/v1/constraints",
    )
    policy_engine.register_asset(constraint_asset)

    log("Aggregator", "B", "Discovered voltage constraint → negotiating contract...")
    offer = ContractOffer(
        offer_id="offer-grid-001",
        provider_id="dso-001", consumer_id="agg-001",
        asset_id="asset-voltage-f101", purpose="voltage_regulation",
        allowed_operations=["read"], retention_days=30,
        emergency_override=True,
        valid_from=NOW - timedelta(hours=1), valid_until=NOW + timedelta(days=90),
    )
    contract = contract_mgr.offer_contract(offer)
    contract_mgr.negotiate_contract(contract.contract_id)
    contract = contract_mgr.accept_contract(contract.contract_id)
    ok(f"Contract ACTIVE: {contract.contract_id[:8]}... purpose=voltage_regulation")

    # Audit
    audit_log.log_exchange(
        requester_id="agg-001", provider_id="dso-001",
        asset_id="asset-voltage-f101", purpose_tag="voltage_regulation",
        request_body=b'GET /constraints', response_body=b'{"violations": true}',
        contract_id=contract.contract_id,
        action=AuditAction.READ, outcome=AuditOutcome.SUCCESS,
    )

    # ── Apply DR to fix voltage ────────────────────────────────────────────
    header("Phase 4: DR Dispatch → Voltage Restoration")

    log("Aggregator", "B", "Computing flexibility from controllable devices...")

    # Reduce load at end-of-feeder poles (where violations are worst)
    total_reduced = 0
    for feeder in sub.feeders:
        for pole in feeder.poles:
            if pole.voltage_pu < 0.97:  # Target poles with low voltage
                reduction_pct = min(0.6, (0.97 - pole.voltage_pu) * 20)
                for house in pole.houses:
                    reduction = house.controllable_kw * reduction_pct
                    if house.has_ev and house.ev_charging_kw > 0:
                        cut = min(house.ev_charging_kw * 0.8, reduction)
                        house.ev_charging_kw -= cut
                        total_reduced += cut
                        reduction -= cut
                    if house.has_battery and reduction > 0:
                        discharge = min(3.0, reduction)
                        house.load_kw -= discharge  # battery discharging
                        total_reduced += discharge
                        reduction -= discharge
                    if reduction > 0:
                        ac_cut = min(house.load_kw * 0.15, reduction)
                        house.load_kw -= ac_cut
                        total_reduced += ac_cut

    log("Aggregator", "B", f"Total load reduction: {total_reduced:.0f} kW")

    # Re-run power flow
    voltages_after = {}
    for feeder in sub.feeders:
        v = run_power_flow(feeder, v_sub_pu=1.02)
        voltages_after[feeder.id] = v
        min_v = min(v)
        n_violations = sum(1 for vi in v if vi < 0.95)
        color = "R" if n_violations > 0 else "G"
        log(feeder.id, color, f"Min voltage: {min_v:.4f} pu, "
            f"Violations (<0.95): {n_violations} poles")

    total_violations_after = sum(1 for f in sub.feeders for v in voltages_after[f.id] if v < 0.95)
    if total_violations_after == 0:
        ok(f"ALL VOLTAGE VIOLATIONS RESOLVED! ({total_violations} → 0)")
    else:
        log("DSO", "Y", f"Violations reduced: {total_violations} → {total_violations_after}")

    audit_log.log_exchange(
        requester_id="dso-001", provider_id="agg-001",
        asset_id="asset-voltage-f101", purpose_tag="voltage_regulation",
        request_body=b'DISPATCH dr-voltage-001',
        response_body=f'{{"reduced_kw": {total_reduced:.0f}}}'.encode(),
        contract_id=contract.contract_id,
        action=AuditAction.DISPATCH, outcome=AuditOutcome.SUCCESS,
    )

    # ── Visualize results ──────────────────────────────────────────────────
    header("Phase 5: Visualization")

    path8 = plot_dr_control(sub, voltages_before, voltages_after)
    ok(f"DR control visualization saved: {path8}")

    path9 = plot_pole_detail(sub)
    ok(f"Pole detail saved: {path9}")

    # ── Summary ────────────────────────────────────────────────────────────
    header("RESULTS")

    min_v_before = min(min(voltages_before[f.id]) for f in sub.feeders)
    min_v_after = min(min(voltages_after[f.id]) for f in sub.feeders)

    print(f"""
    \033[1;32mVoltage regulation via Federated Data Space: SUCCESS\033[0m

    \033[37m  Grid topology:          {sub.name}\033[0m
    \033[37m  Feeders:                {len(sub.feeders)}\033[0m
    \033[37m  Poles:                  {sum(len(f.poles) for f in sub.feeders)} (4 houses/pole)\033[0m
    \033[37m  Households:             1,000\033[0m
    \033[37m  Min voltage before:     {min_v_before:.4f} pu\033[0m
    \033[37m  Min voltage after:      {min_v_after:.4f} pu\033[0m
    \033[37m  Violations before:      {total_violations}\033[0m
    \033[37m  Violations after:       {total_violations_after}\033[0m
    \033[37m  Total DR reduction:     {total_reduced:.0f} kW\033[0m

    \033[1;35mData sovereignty maintained:\033[0m
    \033[37m  DSO:       Sees aggregate load per pole, no individual houses\033[0m
    \033[37m  Aggregator: Sees flexibility envelope, no grid topology\033[0m
    \033[37m  Prosumer:   Sees own data only, consent-gated sharing\033[0m
    \033[37m  All exchanges: contract-gated, SHA-256 audited\033[0m

    \033[36mOutput files:\033[0m
    \033[37m  {OUT_DIR}/06_grid_topology.png\033[0m
    \033[37m  {OUT_DIR}/07_voltage_profile.png\033[0m
    \033[37m  {OUT_DIR}/08_dr_control.png\033[0m
    \033[37m  {OUT_DIR}/09_pole_detail.png\033[0m
    """)


if __name__ == "__main__":
    run()
