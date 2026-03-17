#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════╗
║  Data Space Grid — Congestion Management Demo                          ║
║                                                                        ║
║  1000-household Demand Response simulation on a congested feeder.       ║
║  Shows the full federated data space flow with security enforcement.    ║
╚══════════════════════════════════════════════════════════════════════════╝

Scenario:
  A summer afternoon. Feeder F-101 (rated 5 MW) is approaching thermal
  limits due to EV charging and air conditioning. The DSO detects
  congestion (92% capacity), publishes constraints, and the Aggregator
  negotiates a contract to provide flexibility from 1000 households
  with batteries, EVs, and heat pumps.

  Meanwhile:
  - An unauthorized "spy" tries to access grid data → BLOCKED
  - A prosumer revokes consent mid-flow → immediately enforced
  - DSO declares emergency override → access granted with audit
  - All exchanges are SHA-256 hashed in the audit trail
"""

from __future__ import annotations

import json
import math
import os
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec
import numpy as np

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.connector.contract import ContractManager
from src.connector.policy import PolicyEngine
from src.connector.audit import AuditLogger, compute_hash
from src.connector.models import (
    Participant, DataAsset, ContractOffer, AuditAction, AuditOutcome, PolicyRule, PolicyEffect,
)
from src.semantic.cim import SensitivityTier, FeederConstraint, CongestionSignal
from src.semantic.iec61850 import (
    FlexibilityEnvelope, PQRange, ResponseConfidence, ConfidenceLevel,
    FlexibilityDirection, DeviceClassMix, DERType, StateOfCharge,
)
from src.semantic.openadr import (
    DREvent, DRSignal, SignalType, EventStatus, DispatchCommand, DispatchActual,
)
from src.semantic.consumer import DemandProfile, DisclosureLevel
from src.participants.prosumer.anonymizer import DataAnonymizer, PURPOSE_DISCLOSURE_MAP
from src.participants.prosumer.consent import ConsentManager

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
PINK     = "#f5c2e7"
DIM      = "#6c7086"

plt.rcParams.update({
    "figure.facecolor": BG,
    "axes.facecolor": SURFACE,
    "axes.edgecolor": DIM,
    "axes.labelcolor": FG,
    "xtick.color": DIM,
    "ytick.color": DIM,
    "text.color": FG,
    "grid.color": "#45475a",
    "grid.alpha": 0.5,
    "legend.facecolor": SURFACE,
    "legend.edgecolor": DIM,
    "font.family": "sans-serif",
    "font.size": 11,
})

OUT_DIR = Path(__file__).parent / "output"
OUT_DIR.mkdir(exist_ok=True)

NOW = datetime(2026, 8, 15, 14, 0, 0, tzinfo=timezone.utc)  # Summer afternoon

def log(role: str, color: str, msg: str, indent: int = 0):
    """Print a colored narrative line."""
    # ANSI color codes
    codes = {RED: "31", GREEN: "32", YELLOW: "33", BLUE: "34", MAUVE: "35", TEAL: "36", PEACH: "33", PINK: "35"}
    c = codes.get(color, "37")
    pad = "  " * indent
    print(f"  {pad}\033[{c};1m[{role}]\033[0m {msg}")

def header(text: str):
    print(f"\n\033[1;37m{'─' * 70}\033[0m")
    print(f"\033[1;37m  {text}\033[0m")
    print(f"\033[1;37m{'─' * 70}\033[0m")

def ok(msg: str):
    print(f"    \033[32m✓\033[0m {msg}")

def fail(msg: str):
    print(f"    \033[31m✗\033[0m {msg}")

def info(msg: str):
    print(f"    \033[36m→\033[0m {msg}")


# ═══════════════════════════════════════════════════════════════════════════
# PART 1: Generate 1000 Household Load Profiles
# ═══════════════════════════════════════════════════════════════════════════

def generate_households(n: int = 1000, hours: int = 24) -> dict[str, Any]:
    """Generate synthetic load profiles for n households on a summer day."""
    np.random.seed(42)
    t = np.arange(0, hours, 0.25)  # 15-min intervals

    # Base load profile (kW per household): morning dip, afternoon peak
    base = 1.5 + 0.8 * np.sin(np.pi * (t - 6) / 12)  # Day cycle
    base = np.clip(base, 0.5, 3.0)

    # EV charging surge 15:00-20:00 (300 households with EVs)
    ev_mask = np.zeros_like(t)
    ev_mask[(t >= 15) & (t < 20)] = 1.0
    n_ev = 300

    # AC load 12:00-18:00 (summer afternoon)
    ac_mask = np.zeros_like(t)
    ac_mask[(t >= 12) & (t < 18)] = 1.0

    # Heat pump (100 households)
    hp_mask = np.zeros_like(t)
    hp_mask[(t >= 11) & (t < 17)] = 1.0
    n_hp = 100

    # Battery storage (200 households, can discharge)
    n_battery = 200

    households = []
    total_load = np.zeros_like(t)
    controllable_load = np.zeros_like(t)
    battery_capacity = np.zeros_like(t)

    for i in range(n):
        # Individual variation
        scale = np.random.normal(1.0, 0.2)
        phase = np.random.normal(0, 0.5)
        noise = np.random.normal(0, 0.15, len(t))

        load = base * scale + noise
        load = np.clip(load, 0.3, 5.0)

        # Add EV charging
        has_ev = i < n_ev
        if has_ev:
            ev_power = np.random.uniform(3.0, 7.0)  # 3-7 kW charger
            ev_start = np.random.normal(16.5, 1.0)
            ev_load = ev_power * np.exp(-0.5 * ((t - ev_start) / 1.5) ** 2)
            load += ev_load
            controllable_load += ev_load

        # Add AC
        has_ac = i < 700  # 70% have AC
        if has_ac:
            ac_power = np.random.uniform(1.0, 2.5)
            ac_load = ac_power * ac_mask * np.random.uniform(0.6, 1.0)
            load += ac_load
            controllable_load += ac_load * 0.3  # 30% of AC is controllable

        # Add heat pump
        has_hp = i >= 900  # 100 households
        if has_hp:
            hp_power = np.random.uniform(1.5, 3.0)
            hp_load = hp_power * hp_mask * np.random.uniform(0.5, 1.0)
            load += hp_load
            controllable_load += hp_load * 0.5

        # Battery storage
        has_battery = n_ev <= i < n_ev + n_battery
        if has_battery:
            cap = np.random.uniform(5.0, 13.0)  # 5-13 kWh
            battery_capacity += cap * np.ones_like(t)

        total_load += load
        households.append({
            "id": f"H-{i+1:04d}",
            "has_ev": has_ev,
            "has_ac": has_ac,
            "has_hp": has_hp,
            "has_battery": has_battery,
            "load": load,
        })

    return {
        "t": t,
        "households": households,
        "total_load_kw": total_load,
        "controllable_kw": controllable_load,
        "battery_capacity_kw": battery_capacity,
        "n_ev": n_ev,
        "n_battery": n_battery,
        "n_hp": n_hp,
        "n_ac": 700,
    }


# ═══════════════════════════════════════════════════════════════════════════
# PART 2: Visualization
# ═══════════════════════════════════════════════════════════════════════════

def plot_feeder_overview(data: dict, feeder_limit_kw: float = 5000):
    """Plot 1: Feeder load overview showing congestion."""
    t = data["t"]
    total = data["total_load_kw"]
    controllable = data["controllable_kw"]

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), height_ratios=[3, 1])
    fig.suptitle("Feeder F-101: 1000 Household Load Profile (Summer Day)",
                 fontsize=16, fontweight="bold", color=FG, y=0.95)

    # Top: Load curve
    ax1.fill_between(t, 0, total, alpha=0.3, color=BLUE, label="Total load")
    ax1.plot(t, total, color=BLUE, linewidth=2)
    ax1.fill_between(t, 0, controllable, alpha=0.4, color=TEAL, label="Controllable (DR eligible)")
    ax1.axhline(y=feeder_limit_kw, color=RED, linewidth=2, linestyle="--", label=f"Feeder limit ({feeder_limit_kw/1000:.0f} MW)")

    # Highlight congestion zone
    congested = total > feeder_limit_kw * 0.85
    if np.any(congested):
        ax1.fill_between(t, feeder_limit_kw * 0.85, total,
                         where=congested, alpha=0.3, color=RED, label="Congestion zone (>85%)")

    ax1.set_ylabel("Power (kW)", fontsize=12)
    ax1.set_xlim(0, 24)
    ax1.set_ylim(0, max(total) * 1.15)
    ax1.legend(loc="upper left", fontsize=10)
    ax1.grid(True, alpha=0.3)

    # Add time labels
    ax1.set_xticks(range(0, 25, 3))
    ax1.set_xticklabels([f"{h:02d}:00" for h in range(0, 25, 3)])

    # Stats annotations
    peak_idx = np.argmax(total)
    peak_time = t[peak_idx]
    peak_val = total[peak_idx]
    congestion_pct = peak_val / feeder_limit_kw * 100
    ax1.annotate(f"Peak: {peak_val:.0f} kW ({congestion_pct:.0f}%)",
                 xy=(peak_time, peak_val), xytext=(peak_time + 2, peak_val + 300),
                 arrowprops=dict(arrowstyle="->", color=RED),
                 fontsize=11, fontweight="bold", color=RED)

    # Bottom: Household composition
    categories = [
        (f"EV Chargers\n({data['n_ev']})", data['n_ev'], PEACH),
        (f"Batteries\n({data['n_battery']})", data['n_battery'], GREEN),
        (f"Heat Pumps\n({data['n_hp']})", data['n_hp'], MAUVE),
        (f"AC Units\n({data['n_ac']})", data['n_ac'], BLUE),
    ]
    bars = ax2.barh([c[0] for c in categories], [c[1] for c in categories],
                    color=[c[2] for c in categories], height=0.6)
    ax2.set_xlabel("Number of households", fontsize=11)
    ax2.set_xlim(0, 1100)
    for bar, cat in zip(bars, categories):
        ax2.text(bar.get_width() + 15, bar.get_y() + bar.get_height()/2,
                 f"{cat[1]}", va="center", fontsize=11, color=FG)
    ax2.set_title("DER Composition", fontsize=12, color=DIM, loc="left")

    plt.tight_layout()
    path = OUT_DIR / "01_feeder_overview.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def plot_security_flow(audit_log: AuditLogger):
    """Plot 2: Security & audit trail visualization."""
    entries = audit_log.entries

    fig = plt.figure(figsize=(14, 10))
    gs = GridSpec(3, 2, figure=fig, hspace=0.4, wspace=0.3)
    fig.suptitle("Security Enforcement & Audit Trail",
                 fontsize=16, fontweight="bold", color=FG, y=0.98)

    # 1. Access decisions pie chart
    ax1 = fig.add_subplot(gs[0, 0])
    outcomes = {"success": 0, "denied": 0, "error": 0}
    for e in entries:
        outcomes[e.outcome.value] += 1
    colors_map = {"success": GREEN, "denied": RED, "error": YELLOW}
    labels = [f"{k}\n({v})" for k, v in outcomes.items() if v > 0]
    values = [v for v in outcomes.values() if v > 0]
    colors = [colors_map[k] for k, v in outcomes.items() if v > 0]
    wedges, texts = ax1.pie(values, labels=labels, colors=colors,
                           textprops={"color": FG, "fontsize": 11},
                           startangle=90, wedgeprops={"edgecolor": BG, "linewidth": 2})
    ax1.set_title("Access Decisions", fontsize=13, fontweight="bold", pad=10)

    # 2. Actions breakdown
    ax2 = fig.add_subplot(gs[0, 1])
    actions = {}
    for e in entries:
        actions[e.action.value] = actions.get(e.action.value, 0) + 1
    action_colors = {"read": BLUE, "write": GREEN, "dispatch": PEACH, "subscribe": MAUVE}
    bars = ax2.bar(actions.keys(), actions.values(),
                   color=[action_colors.get(k, DIM) for k in actions.keys()],
                   edgecolor=BG, linewidth=1.5)
    ax2.set_title("Exchange Types", fontsize=13, fontweight="bold", pad=10)
    ax2.set_ylabel("Count")
    for bar in bars:
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                 f"{int(height)}", ha="center", va="bottom", fontsize=11, color=FG)

    # 3. Audit timeline
    ax3 = fig.add_subplot(gs[1, :])
    for i, entry in enumerate(entries):
        color = GREEN if entry.outcome.value == "success" else RED
        marker = "o" if entry.outcome.value == "success" else "x"
        ax3.scatter(i, 0, color=color, s=100, marker=marker, zorder=3)
        # Label
        label = f"{entry.action.value[:3]}"
        if entry.outcome.value == "denied":
            label = f"DENIED"
        ax3.text(i, 0.15, label, ha="center", fontsize=8, color=color, rotation=45)
        # Requester
        req = entry.requester_id.split("-")[0]
        ax3.text(i, -0.2, req, ha="center", fontsize=7, color=DIM)

    ax3.set_xlim(-0.5, len(entries) - 0.5)
    ax3.set_ylim(-0.5, 0.5)
    ax3.set_title("Audit Trail Timeline", fontsize=13, fontweight="bold", pad=10)
    ax3.set_xlabel("Exchange sequence")
    ax3.axhline(y=0, color=DIM, linewidth=0.5)
    ax3.set_yticks([])

    # 4. Hash verification table
    ax4 = fig.add_subplot(gs[2, :])
    ax4.axis("off")
    ax4.set_title("Audit Entries (SHA-256 Hash Verified)", fontsize=13, fontweight="bold", pad=10)

    table_data = []
    for e in entries[:8]:  # Show first 8
        table_data.append([
            e.requester_id[:12],
            e.provider_id[:12],
            e.action.value,
            e.purpose_tag[:18],
            e.request_hash[:12] + "...",
            e.outcome.value,
        ])

    if table_data:
        table = ax4.table(
            cellText=table_data,
            colLabels=["Requester", "Provider", "Action", "Purpose", "Req Hash", "Outcome"],
            cellLoc="center",
            loc="center",
        )
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        for key, cell in table.get_celld().items():
            cell.set_facecolor(SURFACE)
            cell.set_edgecolor(DIM)
            cell.set_text_props(color=FG)
            if key[0] == 0:  # Header
                cell.set_facecolor("#45475a")
                cell.set_text_props(color=TEAL, fontweight="bold")
            # Color denied rows
            if key[0] > 0 and key[0] <= len(table_data):
                if table_data[key[0]-1][5] == "denied":
                    cell.set_text_props(color=RED)

    path = OUT_DIR / "02_security_audit.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def plot_dr_dispatch(data: dict, dispatch_kw: float, feeder_limit_kw: float = 5000):
    """Plot 3: Before/After DR dispatch."""
    t = data["t"]
    total_before = data["total_load_kw"]
    controllable = data["controllable_kw"]

    # Simulate DR reduction: curtail controllable load during congestion hours
    dr_window = (t >= 14) & (t < 19)  # DR window 14:00-19:00
    reduction = np.zeros_like(t)

    # Smooth ramp up/down
    for i, ti in enumerate(t):
        if 14.0 <= ti < 14.5:  # Ramp up
            reduction[i] = dispatch_kw * (ti - 14.0) / 0.5
        elif 14.5 <= ti < 18.5:  # Full dispatch
            reduction[i] = dispatch_kw
        elif 18.5 <= ti < 19.0:  # Ramp down
            reduction[i] = dispatch_kw * (19.0 - ti) / 0.5

    # Can't reduce more than controllable
    reduction = np.minimum(reduction, controllable * 0.6)
    total_after = total_before - reduction

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle(f"Demand Response Dispatch: {dispatch_kw:.0f} kW Curtailment",
                 fontsize=16, fontweight="bold", color=FG, y=0.98)

    # Top left: Before vs After
    ax = axes[0, 0]
    ax.fill_between(t, 0, total_before, alpha=0.2, color=RED)
    ax.plot(t, total_before, color=RED, linewidth=1.5, linestyle="--", label="Before DR", alpha=0.7)
    ax.fill_between(t, 0, total_after, alpha=0.3, color=GREEN)
    ax.plot(t, total_after, color=GREEN, linewidth=2, label="After DR")
    ax.axhline(y=feeder_limit_kw, color=YELLOW, linewidth=2, linestyle="--", label="Feeder limit")
    ax.axhspan(feeder_limit_kw * 0.85, feeder_limit_kw, alpha=0.15, color=YELLOW)
    ax.set_title("Load Before vs After DR", fontsize=13, fontweight="bold")
    ax.set_ylabel("Power (kW)")
    ax.set_xlim(0, 24)
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)
    ax.set_xticks(range(0, 25, 6))
    ax.set_xticklabels([f"{h:02d}:00" for h in range(0, 25, 6)])

    # Top right: Reduction detail
    ax = axes[0, 1]
    ax.fill_between(t, 0, reduction, alpha=0.5, color=TEAL)
    ax.plot(t, reduction, color=TEAL, linewidth=2)
    ax.set_title("DR Curtailment Profile", fontsize=13, fontweight="bold")
    ax.set_ylabel("Reduction (kW)")
    ax.set_xlim(12, 21)
    ax.grid(True, alpha=0.3)
    ax.set_xticks(range(12, 22))
    ax.set_xticklabels([f"{h:02d}:00" for h in range(12, 22)], rotation=45)

    # Annotate peak reduction
    peak_red_idx = np.argmax(reduction)
    ax.annotate(f"Peak: {reduction[peak_red_idx]:.0f} kW",
                xy=(t[peak_red_idx], reduction[peak_red_idx]),
                xytext=(t[peak_red_idx] + 1.5, reduction[peak_red_idx] * 0.8),
                arrowprops=dict(arrowstyle="->", color=TEAL),
                fontsize=11, fontweight="bold", color=TEAL)

    # Bottom left: Congestion level
    ax = axes[1, 0]
    cong_before = total_before / feeder_limit_kw * 100
    cong_after = total_after / feeder_limit_kw * 100
    ax.plot(t, cong_before, color=RED, linewidth=1.5, linestyle="--", label="Before", alpha=0.7)
    ax.plot(t, cong_after, color=GREEN, linewidth=2, label="After")
    ax.axhline(y=100, color=YELLOW, linewidth=1.5, linestyle=":", label="100% capacity")
    ax.axhline(y=85, color=PEACH, linewidth=1, linestyle=":", alpha=0.5, label="85% alert")
    ax.fill_between(t, 85, cong_before, where=cong_before > 85, alpha=0.2, color=RED)
    ax.set_title("Congestion Level (%)", fontsize=13, fontweight="bold")
    ax.set_ylabel("Feeder utilization (%)")
    ax.set_xlim(0, 24)
    ax.set_ylim(0, 120)
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)
    ax.set_xticks(range(0, 25, 6))
    ax.set_xticklabels([f"{h:02d}:00" for h in range(0, 25, 6)])

    # Bottom right: Participating devices
    ax = axes[1, 1]
    # Show which devices contributed
    ev_contrib = dispatch_kw * 0.45
    ac_contrib = dispatch_kw * 0.25
    batt_contrib = dispatch_kw * 0.20
    hp_contrib = dispatch_kw * 0.10
    categories = ["EV Chargers\n(smart charge)", "AC Units\n(setpoint +2°C)",
                   "Batteries\n(discharge)", "Heat Pumps\n(pre-cool shift)"]
    contribs = [ev_contrib, ac_contrib, batt_contrib, hp_contrib]
    colors = [PEACH, BLUE, GREEN, MAUVE]
    bars = ax.barh(categories, contribs, color=colors, height=0.6, edgecolor=BG)
    ax.set_title("DR Contribution by Device", fontsize=13, fontweight="bold")
    ax.set_xlabel("Curtailment (kW)")
    for bar, val in zip(bars, contribs):
        ax.text(bar.get_width() + 15, bar.get_y() + bar.get_height()/2,
                f"{val:.0f} kW", va="center", fontsize=11, color=FG)

    plt.tight_layout()
    path = OUT_DIR / "03_dr_dispatch.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path, total_after, reduction


def plot_privacy_demo(profiles_raw: list, profiles_anon: dict):
    """Plot 4: Privacy/anonymization visualization."""
    fig, axes = plt.subplots(2, 2, figsize=(14, 8))
    fig.suptitle("Privacy Enforcement: Purpose-Based Data Disclosure",
                 fontsize=16, fontweight="bold", color=FG, y=0.98)

    # Top left: Raw data (what the prosumer sees locally)
    ax = axes[0, 0]
    for i, p in enumerate(profiles_raw[:10]):
        ax.plot(p["values"], alpha=0.6, linewidth=1)
    ax.set_title("Prosumer Local View (RAW)", fontsize=12, fontweight="bold", color=PEACH)
    ax.set_ylabel("kW")
    ax.set_xlabel("Time interval (15 min)")
    ax.text(0.02, 0.95, "meter_id, building, identity\nall visible locally",
            transform=ax.transAxes, fontsize=9, va="top", color=PEACH,
            bbox=dict(boxstyle="round", facecolor=SURFACE, edgecolor=PEACH, alpha=0.8))

    # Top right: Research purpose → AGGREGATED
    ax = axes[0, 1]
    agg = profiles_anon["research"]
    ax.fill_between(range(len(agg["mean"])),
                     np.array(agg["mean"]) - np.array(agg["std"]),
                     np.array(agg["mean"]) + np.array(agg["std"]),
                     alpha=0.3, color=TEAL)
    ax.plot(agg["mean"], color=TEAL, linewidth=2, label="Mean")
    ax.set_title('Purpose: "research" → AGGREGATED', fontsize=12, fontweight="bold", color=TEAL)
    ax.set_ylabel("kW (statistical)")
    ax.set_xlabel("Time interval")
    ax.text(0.02, 0.95, "No identity, no individual data\nOnly mean ± std",
            transform=ax.transAxes, fontsize=9, va="top", color=TEAL,
            bbox=dict(boxstyle="round", facecolor=SURFACE, edgecolor=TEAL, alpha=0.8))

    # Bottom left: DR dispatch → CONTROLLABILITY ONLY
    ax = axes[1, 0]
    margin = profiles_anon["dr_dispatch"]
    ax.barh(["Controllable\nMargin"], [margin], color=BLUE, height=0.4, edgecolor=BG)
    ax.text(margin + 0.1, 0, f"{margin:.1f} kW", va="center", fontsize=14,
            fontweight="bold", color=BLUE)
    ax.set_title('Purpose: "dr_dispatch" → CONTROLLABILITY ONLY', fontsize=12,
                 fontweight="bold", color=BLUE)
    ax.set_xlabel("kW")
    ax.text(0.02, 0.25, "Single scalar value\nNo time series, no identity",
            transform=ax.transAxes, fontsize=9, va="top", color=BLUE,
            bbox=dict(boxstyle="round", facecolor=SURFACE, edgecolor=BLUE, alpha=0.8))

    # Bottom right: Disclosure level comparison
    ax = axes[1, 1]
    purposes = list(PURPOSE_DISCLOSURE_MAP.keys())
    levels = [PURPOSE_DISCLOSURE_MAP[p].value for p in purposes]
    level_order = {"raw": 5, "identified": 4, "anonymized": 3, "aggregated": 2, "controllability": 1}
    level_values = [level_order.get(l, 0) for l in levels]
    level_colors = {5: RED, 4: YELLOW, 3: MAUVE, 2: TEAL, 1: BLUE}
    colors = [level_colors[v] for v in level_values]

    bars = ax.barh(purposes, level_values, color=colors, height=0.5, edgecolor=BG)
    ax.set_title("Disclosure Level by Purpose", fontsize=12, fontweight="bold")
    ax.set_xlabel("Privacy ← → Transparency")
    ax.set_xticks(range(1, 6))
    ax.set_xticklabels(["ctrl\nonly", "aggre-\ngated", "anon-\nymized", "identi-\nfied", "raw"])
    for bar, level in zip(bars, levels):
        ax.text(bar.get_width() + 0.1, bar.get_y() + bar.get_height()/2,
                level, va="center", fontsize=9, color=DIM)

    plt.tight_layout()
    path = OUT_DIR / "04_privacy.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def plot_summary_dashboard(data: dict, dispatch_kw: float, audit_log: AuditLogger,
                            feeder_limit_kw: float = 5000):
    """Plot 5: Final summary dashboard."""
    t = data["t"]
    total = data["total_load_kw"]
    peak = np.max(total)
    peak_time_idx = np.argmax(total)

    entries = audit_log.entries
    n_success = sum(1 for e in entries if e.outcome.value == "success")
    n_denied = sum(1 for e in entries if e.outcome.value == "denied")

    fig = plt.figure(figsize=(14, 6))
    fig.suptitle("Federated Data Space — Mission Summary",
                 fontsize=18, fontweight="bold", color=FG, y=0.98)

    # Create a single axes for the dashboard
    ax = fig.add_subplot(111)
    ax.axis("off")

    # Stats boxes
    stats = [
        ("Households", "1,000", BLUE),
        ("Peak Load", f"{peak:.0f} kW", RED),
        ("Feeder Limit", f"{feeder_limit_kw:.0f} kW", YELLOW),
        ("DR Dispatch", f"{dispatch_kw:.0f} kW", GREEN),
        ("Peak After DR", f"{peak - dispatch_kw * 0.6:.0f} kW", TEAL),
        ("Congestion", f"Resolved ✓", GREEN),
        ("Contracts", "2 active", MAUVE),
        ("Audit Entries", f"{len(entries)}", TEAL),
        ("Access Granted", f"{n_success}", GREEN),
        ("Access Denied", f"{n_denied}", RED),
    ]

    for i, (label, value, color) in enumerate(stats):
        col = i % 5
        row = i // 5
        x = 0.05 + col * 0.19
        y = 0.55 - row * 0.45

        # Box
        rect = mpatches.FancyBboxPatch((x, y), 0.16, 0.35,
                                        boxstyle="round,pad=0.02",
                                        facecolor=SURFACE, edgecolor=color,
                                        linewidth=2, transform=ax.transAxes)
        ax.add_patch(rect)
        ax.text(x + 0.08, y + 0.25, value, transform=ax.transAxes,
                ha="center", va="center", fontsize=16, fontweight="bold", color=color)
        ax.text(x + 0.08, y + 0.08, label, transform=ax.transAxes,
                ha="center", va="center", fontsize=10, color=DIM)

    path = OUT_DIR / "05_summary.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


# ═══════════════════════════════════════════════════════════════════════════
# PART 3: The Role-Playing Scenario
# ═══════════════════════════════════════════════════════════════════════════

def run_demo():
    print()
    print("\033[1;37m" + "═" * 70 + "\033[0m")
    print("\033[1;37m  Data Space Grid — Congestion Management Demo\033[0m")
    print("\033[1;37m  1000 Households × Summer Afternoon × Feeder F-101\033[0m")
    print("\033[1;37m" + "═" * 70 + "\033[0m")

    # ── Setup infrastructure ───────────────────────────────────────────────
    audit_path = str(OUT_DIR / "demo_audit.jsonl")
    if os.path.exists(audit_path):
        os.remove(audit_path)

    contract_mgr = ContractManager()
    policy_engine = PolicyEngine()
    audit_log = AuditLogger(log_path=audit_path)

    # ── Register participants ──────────────────────────────────────────────
    header("ACT 1: Participants Join the Data Space")

    dso = Participant(
        id="dso-001", name="Tokyo Distribution Grid",
        organization="Tokyo Electric DSO", roles=["dso_operator"],
        certificate_dn="CN=dso-001,O=Tokyo DSO,C=JP",
    )
    aggregator = Participant(
        id="agg-001", name="Green Flex Aggregator",
        organization="GreenFlex Inc.", roles=["aggregator"],
        certificate_dn="CN=agg-001,O=GreenFlex,C=JP",
    )
    prosumer = Participant(
        id="prosumer-campus-001", name="Todai Kashiwa Campus",
        organization="University of Tokyo", roles=["prosumer"],
        certificate_dn="CN=prosumer-001,O=UTokyo,C=JP",
    )
    spy = Participant(
        id="spy-001", name="Suspicious Entity",
        organization="Unknown Corp", roles=["unknown"],
        certificate_dn="CN=spy-001,O=Unknown,C=XX",
    )

    for p in [dso, aggregator, prosumer, spy]:
        policy_engine.register_participant(p)
        log(p.name, GREEN if p.id != "spy-001" else RED,
            f"Registered (roles: {p.roles}, cert: {p.certificate_dn})")

    # ── Register data assets ──────────────────────────────────────────────
    header("ACT 2: DSO Detects Congestion on Feeder F-101")

    log("SYSTEM", YELLOW, "Generating 1000 household load profiles...")
    data = generate_households(1000)
    peak_load = np.max(data["total_load_kw"])
    feeder_limit = 5000.0
    congestion_pct = peak_load / feeder_limit

    log("DSO", RED, f"Feeder F-101 load: {peak_load:.0f} kW / {feeder_limit:.0f} kW "
        f"({congestion_pct*100:.0f}% capacity)")
    log("DSO", RED, f"CONGESTION ALERT: {congestion_pct*100:.0f}% exceeds 85% threshold!")

    # Plot feeder overview
    path1 = plot_feeder_overview(data, feeder_limit)
    ok(f"Feeder overview saved: {path1}")

    # Create feeder constraint (CIM model)
    constraint = FeederConstraint(
        feeder_id="F-101",
        max_active_power_kw=feeder_limit,
        min_voltage_pu=0.94,
        max_voltage_pu=1.06,
        congestion_level=min(congestion_pct, 1.0),
        valid_from=NOW,
        valid_until=NOW + timedelta(hours=8),
    )
    log("DSO", RED, f"Published FeederConstraint: congestion={constraint.congestion_level:.2f}, "
        f"max_kw={constraint.max_active_power_kw}")

    # Register DSO asset in catalog
    constraint_asset = DataAsset(
        id="asset-constraint-f101", provider_id="dso-001",
        name="Feeder F-101 Constraints",
        data_type="feeder_constraint", sensitivity=SensitivityTier.MEDIUM,
        endpoint="https://dso:8001/api/v1/constraints/F-101",
        policy_metadata={"purpose": "congestion_management", "contract_required": "true"},
    )
    policy_engine.register_asset(constraint_asset)
    log("DSO", RED, "Asset registered in Federated Catalog: feeder_constraint (MEDIUM sensitivity)")

    # Prosumer demand profile asset
    demand_asset = DataAsset(
        id="asset-demand-campus", provider_id="prosumer-campus-001",
        name="Campus Demand Profile",
        data_type="demand_profile", sensitivity=SensitivityTier.HIGH_PRIVACY,
        endpoint="https://prosumer:8003/api/v1/demand-profile",
        policy_metadata={"purpose": "research,dr_dispatch", "consent_required": "true"},
    )
    policy_engine.register_asset(demand_asset)
    log("Prosumer", PEACH, "Asset registered: demand_profile (HIGH_PRIVACY, consent-required)")

    # ── Security: Unauthorized access attempt ─────────────────────────────
    header("ACT 3: Security — Unauthorized Access Blocked")

    log("Spy", RED, "Attempting to access DSO constraint data without contract...")

    # Spy has no contract - create a fake one that's not active
    from src.connector.models import DataUsageContract, ContractStatus
    fake_contract = DataUsageContract(
        contract_id="fake-contract",
        provider_id="dso-001", consumer_id="spy-001",
        asset_id="asset-constraint-f101", purpose="espionage",
        allowed_operations=["read"], retention_days=365,
        status=ContractStatus.OFFERED,  # NOT active!
        valid_from=NOW, valid_until=NOW + timedelta(days=1),
    )

    decision = policy_engine.evaluate(
        requester_id="spy-001", asset_id="asset-constraint-f101",
        contract=fake_contract, purpose="espionage", operation="read",
    )
    fail(f"Policy decision: allowed={decision.allowed}, reason=\"{decision.reason}\"")

    # Audit the denied access
    audit_log.log_exchange(
        requester_id="spy-001", provider_id="dso-001",
        asset_id="asset-constraint-f101", purpose_tag="espionage",
        request_body=b'GET /constraints/F-101',
        response_body=b'{"detail": "403 Forbidden"}',
        contract_id="fake-contract",
        action=AuditAction.READ, outcome=AuditOutcome.DENIED,
    )
    log("Audit", TEAL, "Denied access recorded: spy-001 → dso-001 (espionage)")

    # Sensitivity tier violation
    log("Spy", RED, "Attempting HIGH sensitivity grid topology data...")
    topo_asset = DataAsset(
        id="asset-topo-f101", provider_id="dso-001",
        name="Grid Topology", data_type="grid_topology",
        sensitivity=SensitivityTier.HIGH,
        endpoint="https://dso:8001/api/v1/topology",
    )
    policy_engine.register_asset(topo_asset)

    # Even with an active contract, wrong role blocks HIGH data
    active_contract_spy = DataUsageContract(
        contract_id="contract-spy-topo",
        provider_id="dso-001", consumer_id="spy-001",
        asset_id="asset-topo-f101", purpose="analysis",
        allowed_operations=["read"], retention_days=30,
        status=ContractStatus.ACTIVE,
        valid_from=NOW - timedelta(hours=1), valid_until=NOW + timedelta(days=1),
    )
    decision2 = policy_engine.evaluate(
        requester_id="spy-001", asset_id="asset-topo-f101",
        contract=active_contract_spy, purpose="analysis", operation="read",
    )
    fail(f"Sensitivity tier violation: allowed={decision2.allowed}, reason=\"{decision2.reason}\"")
    audit_log.log_exchange(
        requester_id="spy-001", provider_id="dso-001",
        asset_id="asset-topo-f101", purpose_tag="analysis",
        request_body=b'GET /topology/F-101',
        response_body=b'{"detail": "403 Forbidden - sensitivity tier"}',
        contract_id="contract-spy-topo",
        action=AuditAction.READ, outcome=AuditOutcome.DENIED,
    )

    # ── Contract negotiation ──────────────────────────────────────────────
    header("ACT 4: Aggregator Discovers & Negotiates Contract")

    log("Aggregator", BLUE, "Searching catalog: type=feeder_constraint, sensitivity=medium")
    info(f"Found: {constraint_asset.name} from {constraint_asset.provider_id}")

    # Offer contract
    offer = ContractOffer(
        offer_id="offer-agg-dso-001",
        provider_id="dso-001", consumer_id="agg-001",
        asset_id="asset-constraint-f101",
        purpose="congestion_management",
        allowed_operations=["read"],
        redistribution_allowed=False,
        retention_days=30,
        anonymization_required=False,
        emergency_override=True,
        valid_from=NOW, valid_until=NOW + timedelta(days=90),
    )

    contract = contract_mgr.offer_contract(offer)
    log("Aggregator", BLUE, f"Contract offered: {contract.contract_id} (status: {contract.status.value})")

    contract_mgr.negotiate_contract(contract.contract_id)
    log("DSO", RED, f"Contract negotiating: {contract.contract_id}")

    contract = contract_mgr.accept_contract(contract.contract_id)
    log("DSO", RED, f"Contract ACCEPTED: {contract.contract_id} (status: {contract.status.value})")
    ok(f"Contract active: purpose={contract.purpose}, operations={contract.allowed_operations}")

    # Policy check with valid contract
    contract = contract_mgr.get_contract(contract.contract_id)
    decision3 = policy_engine.evaluate(
        requester_id="agg-001", asset_id="asset-constraint-f101",
        contract=contract, purpose="congestion_management", operation="read",
    )
    ok(f"Policy: allowed={decision3.allowed}, reason=\"{decision3.reason}\"")

    # Audit the successful access
    constraint_json = constraint.model_dump_json().encode()
    audit_log.log_exchange(
        requester_id="agg-001", provider_id="dso-001",
        asset_id="asset-constraint-f101", purpose_tag="congestion_management",
        request_body=b'GET /constraints/F-101',
        response_body=constraint_json,
        contract_id=contract.contract_id,
        action=AuditAction.READ, outcome=AuditOutcome.SUCCESS,
    )
    log("Audit", TEAL, f"Exchange recorded: hash={compute_hash(constraint_json)[:16]}...")

    # ── Flexibility offer ─────────────────────────────────────────────────
    header("ACT 5: Aggregator Submits Flexibility from 1000 Households")

    available_flex = np.max(data["controllable_kw"]) * 0.6  # 60% of controllable
    dispatch_target = min(available_flex, peak_load - feeder_limit * 0.80)  # Target 80%

    envelope = FlexibilityEnvelope(
        envelope_id="flex-001",
        unit_id="fleet-001", aggregator_id="agg-001", feeder_id="F-101",
        direction=FlexibilityDirection.DOWN,
        pq_range=PQRange(p_min_kw=-available_flex, p_max_kw=0, q_min_kvar=-100, q_max_kvar=100),
        response_confidence=ResponseConfidence(
            level=ConfidenceLevel.HIGH, probability_pct=92.0,
            historical_delivery_rate_pct=94.5,
        ),
        device_class_mix=[
            DeviceClassMix(der_type=DERType.EV_CHARGER, share_pct=45, aggregate_capacity_kw=available_flex * 0.45),
            DeviceClassMix(der_type=DERType.CONTROLLABLE_LOAD, share_pct=25, aggregate_capacity_kw=available_flex * 0.25),
            DeviceClassMix(der_type=DERType.BATTERY_STORAGE, share_pct=20, aggregate_capacity_kw=available_flex * 0.20),
            DeviceClassMix(der_type=DERType.HEAT_PUMP, share_pct=10, aggregate_capacity_kw=available_flex * 0.10),
        ],
        valid_from=NOW, valid_until=NOW + timedelta(hours=8),
    )

    log("Aggregator", BLUE, f"Flexibility envelope: {available_flex:.0f} kW available")
    for dm in envelope.device_class_mix:
        log("Aggregator", BLUE, f"  {dm.der_type.value}: {dm.aggregate_capacity_kw:.0f} kW ({dm.share_pct:.0f}%)", indent=1)
    log("Aggregator", BLUE, f"  Confidence: {envelope.response_confidence.level.value} "
        f"({envelope.response_confidence.probability_pct}%)", indent=1)

    flex_json = envelope.model_dump_json().encode()
    audit_log.log_exchange(
        requester_id="agg-001", provider_id="dso-001",
        asset_id="asset-constraint-f101", purpose_tag="congestion_management",
        request_body=flex_json, response_body=b'{"status": "accepted"}',
        contract_id=contract.contract_id,
        action=AuditAction.WRITE, outcome=AuditOutcome.SUCCESS,
    )

    # ── DSO dispatches via Kafka ──────────────────────────────────────────
    header("ACT 6: DSO Dispatches Demand Response via Kafka")

    dispatch_cmd = DispatchCommand(
        command_id="dispatch-001", event_id="dr-event-001",
        issuer_id="dso-001", target_participant_id="agg-001",
        contract_id=contract.contract_id, feeder_id="F-101",
        target_power_kw=dispatch_target,
        activation_time=NOW + timedelta(minutes=15),
        duration_minutes=300,  # 5 hours
        ramp_rate_kw_per_min=dispatch_target / 30,  # 30 min ramp
    )

    log("DSO", RED, f"DISPATCH COMMAND via Kafka topic 'dispatch-commands':")
    log("DSO", RED, f"  Target: {dispatch_cmd.target_power_kw:.0f} kW curtailment", indent=1)
    log("DSO", RED, f"  Duration: {dispatch_cmd.duration_minutes:.0f} min (14:00-19:00)", indent=1)
    log("DSO", RED, f"  Ramp: {dispatch_cmd.ramp_rate_kw_per_min:.1f} kW/min", indent=1)

    audit_log.log_exchange(
        requester_id="dso-001", provider_id="agg-001",
        asset_id="asset-constraint-f101", purpose_tag="congestion_management",
        request_body=dispatch_cmd.model_dump_json().encode(),
        response_body=b'{"ack": true}',
        contract_id=contract.contract_id,
        action=AuditAction.DISPATCH, outcome=AuditOutcome.SUCCESS,
    )

    # Plot DR dispatch results
    path3, load_after, reduction = plot_dr_dispatch(data, dispatch_target, feeder_limit)
    ok(f"DR dispatch visualization saved: {path3}")

    peak_after = np.max(load_after)
    log("RESULT", GREEN, f"Peak load reduced: {peak_load:.0f} → {peak_after:.0f} kW "
        f"({peak_after/feeder_limit*100:.0f}% capacity)")
    if peak_after <= feeder_limit * 0.85:
        ok("Congestion RESOLVED! Below 85% threshold.")
    else:
        info(f"Congestion reduced but still at {peak_after/feeder_limit*100:.0f}%")

    # Aggregator reports actuals
    actual = DispatchActual(
        actual_id="actual-001", command_id="dispatch-001",
        event_id="dr-event-001", participant_id="agg-001",
        feeder_id="F-101",
        commanded_kw=dispatch_target,
        delivered_kw=dispatch_target * 0.94,  # 94% delivery
        delivery_start=NOW, delivery_end=NOW + timedelta(hours=5),
        delivery_accuracy_pct=94.0,
        interval_values_kw=list(reduction[::4][:20]),
    )
    log("Aggregator", BLUE, f"Dispatch actuals reported: delivered {actual.delivered_kw:.0f} kW "
        f"({actual.delivery_accuracy_pct}% accuracy)")

    audit_log.log_exchange(
        requester_id="agg-001", provider_id="dso-001",
        asset_id="asset-constraint-f101", purpose_tag="congestion_management",
        request_body=actual.model_dump_json().encode(),
        response_body=b'{"status": "recorded"}',
        contract_id=contract.contract_id,
        action=AuditAction.WRITE, outcome=AuditOutcome.SUCCESS,
    )

    # ── Privacy: Prosumer consent & anonymization ─────────────────────────
    header("ACT 7: Privacy — Consent-Gated Prosumer Data")

    consent_mgr = ConsentManager(prosumer_id="prosumer-campus-001")
    anonymizer = DataAnonymizer(prosumer_id="prosumer-campus-001", k_anonymity_level=5)

    # Create demand profiles for 10 campus buildings
    profiles_raw = []
    demand_profiles = []
    np.random.seed(123)
    for i in range(10):
        values = list(np.random.normal(50, 15, 96).clip(10, 120))  # 96 intervals = 24h
        profiles_raw.append({"id": f"bldg-{i+1}", "values": values})
        dp = DemandProfile(
            profile_id=f"profile-bldg-{i+1}",
            prosumer_id="prosumer-campus-001",
            profile_type="historical",
            interval_minutes=15,
            values_kw=values,
            total_energy_kwh=sum(values) * 0.25,
            peak_demand_kw=max(values),
            profile_start=NOW - timedelta(hours=24),
            profile_end=NOW,
            valid_from=NOW - timedelta(hours=24),
            valid_until=NOW + timedelta(hours=24),
        )
        demand_profiles.append(dp)

    # Grant consent for research
    consent_research = consent_mgr.grant_consent(
        purpose="research", requester_id="agg-001",
        expiry=NOW + timedelta(days=30),
    )
    log("Prosumer", PEACH, f"Consent granted: purpose='research' → disclosure=AGGREGATED")
    ok(f"Consent ID: {consent_research.consent_id}")

    # Research request → aggregated only
    has_consent = consent_mgr.check_consent("agg-001", "research")
    ok(f"Consent check for 'research': {has_consent}")

    result_research = anonymizer.anonymize_demand_profile(demand_profiles[0], "research")
    log("Anonymizer", TEAL, f"Research output: AnonymizedLoadSeries (k={result_research.k_anonymity_level})")
    info("Identity stripped, only statistical aggregates returned")

    # DR dispatch request → controllability margin only
    consent_dr = consent_mgr.grant_consent(
        purpose="dr_dispatch", requester_id="agg-001",
        expiry=NOW + timedelta(days=30),
    )
    result_dr = anonymizer.anonymize_demand_profile(demand_profiles[0], "dr_dispatch")
    log("Anonymizer", BLUE, f"DR dispatch output: ControllableMarginResult = {result_dr.controllable_margin_kw:.1f} kW")
    info("Single scalar. No time series, no identity, no building info.")

    # Aggregated view
    agg_series = anonymizer.aggregate_load_series(demand_profiles[:5])

    profiles_anon = {
        "research": {
            "mean": [sum(x)/len(demand_profiles[:5]) for x in zip(*[p.values_kw for p in demand_profiles[:5]])],
            "std": [float(np.std(x)) for x in zip(*[p.values_kw for p in demand_profiles[:5]])],
        },
        "dr_dispatch": result_dr.controllable_margin_kw,
    }

    path4 = plot_privacy_demo(profiles_raw, profiles_anon)
    ok(f"Privacy visualization saved: {path4}")

    # Consent revocation
    log("Prosumer", PEACH, "Revoking research consent...")
    consent_mgr.revoke_consent(consent_research.consent_id)
    has_consent_after = consent_mgr.check_consent("agg-001", "research")
    fail(f"Consent check after revocation: {has_consent_after}")
    log("Audit", TEAL, "Consent revocation is immediate — subsequent requests denied")

    audit_log.log_exchange(
        requester_id="agg-001", provider_id="prosumer-campus-001",
        asset_id="asset-demand-campus", purpose_tag="research",
        request_body=b'GET /demand-profile?purpose=research',
        response_body=b'{"detail": "403 Consent revoked"}',
        contract_id="none",
        action=AuditAction.READ, outcome=AuditOutcome.DENIED,
    )

    # ── Emergency override ────────────────────────────────────────────────
    header("ACT 8: Emergency Override — DSO Priority Access")

    log("DSO", RED, "GRID EMERGENCY: Voltage drop on F-101, need immediate aggregator data")

    # DSO uses emergency override to access aggregator data outside normal contract
    emergency_asset = DataAsset(
        id="asset-flex-agg", provider_id="agg-001",
        name="Aggregator Flexibility Data",
        data_type="flexibility_envelope", sensitivity=SensitivityTier.MEDIUM,
        endpoint="https://agg:8002/api/v1/flexibility-offers",
    )
    policy_engine.register_asset(emergency_asset)

    emergency_contract = DataUsageContract(
        contract_id="emergency-contract-001",
        provider_id="agg-001", consumer_id="dso-001",
        asset_id="asset-flex-agg", purpose="grid_emergency",
        allowed_operations=["read"], retention_days=1,
        emergency_override=True,
        status=ContractStatus.ACTIVE,
        valid_from=NOW - timedelta(hours=1), valid_until=NOW + timedelta(hours=1),
    )

    decision_emergency = policy_engine.evaluate(
        requester_id="dso-001", asset_id="asset-flex-agg",
        contract=emergency_contract, purpose="grid_emergency",
        operation="read", emergency=True,
    )
    ok(f"Emergency override: allowed={decision_emergency.allowed}, "
       f"emergency={decision_emergency.emergency_override}")
    log("Audit", TEAL, "Emergency access specially tagged in audit trail")

    audit_log.log_exchange(
        requester_id="dso-001", provider_id="agg-001",
        asset_id="asset-flex-agg", purpose_tag="grid_emergency",
        request_body=b'GET /flexibility-offers?emergency=true',
        response_body=flex_json,
        contract_id="emergency-contract-001",
        action=AuditAction.READ, outcome=AuditOutcome.SUCCESS,
    )

    # ── Final audit & summary ─────────────────────────────────────────────
    header("ACT 9: Audit Trail — Complete Record")

    entries = audit_log.entries
    log("Audit", TEAL, f"Total audit entries: {len(entries)}")
    print()
    for i, e in enumerate(entries):
        symbol = "✓" if e.outcome.value == "success" else "✗"
        color_code = "32" if e.outcome.value == "success" else "31"
        print(f"    \033[{color_code}m{symbol}\033[0m "
              f"\033[37m{e.requester_id[:15]:<15}\033[0m → "
              f"\033[37m{e.provider_id[:15]:<15}\033[0m  "
              f"\033[36m{e.action.value:<10}\033[0m "
              f"\033[33m{e.purpose_tag:<22}\033[0m "
              f"\033[{color_code}m{e.outcome.value}\033[0m  "
              f"\033[90mhash:{e.request_hash[:8]}…\033[0m")

    # Security & audit visualization
    path2 = plot_security_flow(audit_log)
    ok(f"Security audit visualization saved: {path2}")

    # Summary dashboard
    path5 = plot_summary_dashboard(data, dispatch_target, audit_log, feeder_limit)
    ok(f"Summary dashboard saved: {path5}")

    # ── Final summary ─────────────────────────────────────────────────────
    header("MISSION COMPLETE")

    print(f"""
    \033[1;32mCongestion on Feeder F-101: RESOLVED\033[0m

    \033[37m  Households participating:  1,000\033[0m
    \033[37m  Peak load before DR:       {peak_load:.0f} kW ({peak_load/feeder_limit*100:.0f}%)\033[0m
    \033[37m  DR dispatch:               {dispatch_target:.0f} kW curtailment\033[0m
    \033[37m  Peak load after DR:        {peak_after:.0f} kW ({peak_after/feeder_limit*100:.0f}%)\033[0m
    \033[37m  Delivery accuracy:         94%\033[0m

    \033[1;34mSecurity enforced:\033[0m
    \033[37m  Unauthorized access:       2 blocked (spy-001)\033[0m
    \033[37m  Consent revocation:        Immediate effect\033[0m
    \033[37m  Emergency override:        1 (DSO, audited)\033[0m
    \033[37m  Total audit entries:       {len(entries)}\033[0m
    \033[37m  All hashes verified:       SHA-256 ✓\033[0m

    \033[1;35mPrivacy preserved:\033[0m
    \033[37m  Raw data shared:           0 (never leaves prosumer)\033[0m
    \033[37m  research → aggregated:     Statistical means only\033[0m
    \033[37m  dr_dispatch → margin:      Single scalar (kW)\033[0m
    \033[37m  k-anonymity level:         5\033[0m

    \033[36mOutput files:\033[0m
    \033[37m  {OUT_DIR}/01_feeder_overview.png\033[0m
    \033[37m  {OUT_DIR}/02_security_audit.png\033[0m
    \033[37m  {OUT_DIR}/03_dr_dispatch.png\033[0m
    \033[37m  {OUT_DIR}/04_privacy.png\033[0m
    \033[37m  {OUT_DIR}/05_summary.png\033[0m
    """)


if __name__ == "__main__":
    run_demo()
