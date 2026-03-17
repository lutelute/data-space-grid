"""Generate conceptual architecture animation GIFs for the Data Space Grid README."""

from __future__ import annotations

import math
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

OUT = Path(__file__).parent

# ── Fonts ──────────────────────────────────────────────────────────────────
def _font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    try:
        if bold:
            return ImageFont.truetype("/System/Library/Fonts/HelveticaNeue.ttc", size, index=1)
        return ImageFont.truetype("/System/Library/Fonts/HelveticaNeue.ttc", size, index=0)
    except Exception:
        return ImageFont.load_default()

def _mono(size: int) -> ImageFont.FreeTypeFont:
    try:
        return ImageFont.truetype("/System/Library/Fonts/Menlo.ttc", size)
    except Exception:
        return ImageFont.load_default()

# ── Colors ─────────────────────────────────────────────────────────────────
BG       = (24, 24, 37)      # dark bg
FG       = (205, 214, 244)   # light text
DIM      = (108, 112, 134)   # dim text
RED      = (243, 139, 168)
GREEN    = (166, 227, 161)
BLUE     = (137, 180, 250)
YELLOW   = (249, 226, 175)
MAUVE    = (203, 166, 247)
TEAL     = (148, 226, 213)
PEACH    = (250, 179, 135)
PINK     = (245, 194, 231)
SURFACE0 = (49, 50, 68)
SURFACE1 = (69, 71, 90)
SURFACE2 = (88, 91, 112)

W, H = 900, 520

# ── Helpers ────────────────────────────────────────────────────────────────
def new_frame() -> tuple[Image.Image, ImageDraw.ImageDraw]:
    img = Image.new("RGB", (W, H), BG)
    return img, ImageDraw.Draw(img)

def draw_box(d: ImageDraw.ImageDraw, x: int, y: int, w: int, h: int,
             fill: tuple, outline: tuple | None = None, text: str = "",
             font: ImageFont.FreeTypeFont | None = None, text_color: tuple = FG,
             radius: int = 12):
    d.rounded_rectangle([x, y, x + w, y + h], radius=radius, fill=fill,
                        outline=outline or fill, width=2)
    if text:
        f = font or _font(14)
        bb = d.textbbox((0, 0), text, font=f)
        tw, th = bb[2] - bb[0], bb[3] - bb[1]
        d.text((x + (w - tw) // 2, y + (h - th) // 2), text, fill=text_color, font=f)

def draw_arrow(d: ImageDraw.ImageDraw, x1: int, y1: int, x2: int, y2: int,
               color: tuple = DIM, width: int = 2, dashed: bool = False):
    if dashed:
        length = math.hypot(x2 - x1, y2 - y1)
        dx, dy = (x2 - x1) / length, (y2 - y1) / length
        dash_len = 8
        i = 0.0
        while i < length - dash_len:
            sx = x1 + dx * i
            sy = y1 + dy * i
            ex = x1 + dx * min(i + dash_len, length)
            ey = y1 + dy * min(i + dash_len, length)
            d.line([(sx, sy), (ex, ey)], fill=color, width=width)
            i += dash_len * 2
    else:
        d.line([(x1, y1), (x2, y2)], fill=color, width=width)
    # arrowhead
    angle = math.atan2(y2 - y1, x2 - x1)
    size = 10
    d.polygon([
        (x2, y2),
        (x2 - size * math.cos(angle - 0.4), y2 - size * math.sin(angle - 0.4)),
        (x2 - size * math.cos(angle + 0.4), y2 - size * math.sin(angle + 0.4)),
    ], fill=color)

def draw_title(d: ImageDraw.ImageDraw, text: str, sub: str = ""):
    f = _font(20, bold=True)
    d.text((30, 18), text, fill=FG, font=f)
    if sub:
        sf = _font(13)
        d.text((30, 46), sub, fill=DIM, font=sf)

def save_gif(frames: list[Image.Image], name: str, durations: list[int] | None = None):
    if durations is None:
        durations = [1200] * len(frames)
    # hold last frame longer
    durations[-1] = max(durations[-1], 3000)
    frames[0].save(OUT / name, save_all=True, append_images=frames[1:],
                   duration=durations, loop=0, optimize=True)
    print(f"  -> {name} ({len(frames)} frames)")


# ═══════════════════════════════════════════════════════════════════════════
# 1. OVERALL ARCHITECTURE - 5 layers + 3 participants
# ═══════════════════════════════════════════════════════════════════════════
def gen_architecture():
    layers = [
        ("Layer 5: Access / Exchange", "REST APIs + Kafka event bus", BLUE),
        ("Layer 4: Policy / Contract / Consent", "Machine-enforceable usage agreements", MAUVE),
        ("Layer 3: Catalog / Discovery", "Federated data asset registry", TEAL),
        ("Layer 2: Semantic Model", "CIM / IEC 61850 / OpenADR", YELLOW),
        ("Layer 1: Identity / Trust", "Keycloak OIDC + mTLS", GREEN),
    ]
    participants = [
        ("DSO", ":8001", RED),
        ("Aggregator", ":8002", BLUE),
        ("Prosumer", ":8003", PEACH),
    ]

    frames = []
    durations = []

    for step in range(len(layers) + 2):  # layers + participants + final
        img, d = new_frame()
        draw_title(d, "Federated Data Space Architecture", "5-layer stack with 3 sovereign participants")

        # Draw layers revealed so far
        for i in range(min(step, len(layers))):
            li = len(layers) - 1 - i
            lname, ldesc, lcolor = layers[li]
            ly = 350 - li * 56
            # layer bg
            dark = tuple(max(0, c // 4) for c in lcolor)
            draw_box(d, 50, ly, 520, 48, fill=dark, outline=lcolor, radius=8)
            f = _font(14, bold=True)
            d.text((65, ly + 6), lname, fill=lcolor, font=f)
            sf = _font(11)
            d.text((65, ly + 26), ldesc, fill=DIM, font=sf)

        # Participants (shown from step >= len(layers))
        if step >= len(layers):
            for j, (pname, pport, pcolor) in enumerate(participants):
                px = 620 + j * 0  # stacked vertically
                py = 100 + j * 100
                if step == len(layers) and j > 0:
                    continue
                if step == len(layers) and j == 0:
                    pass
                draw_box(d, 640, py, 210, 80, fill=SURFACE0, outline=pcolor, radius=10)
                f = _font(16, bold=True)
                d.text((670, py + 10), pname, fill=pcolor, font=f)
                sf = _font(12)
                d.text((670, py + 34), f"Port {pport}", fill=DIM, font=sf)
                mf = _mono(10)
                if j == 0:
                    d.text((670, py + 52), "feeder constraints", fill=FG, font=mf)
                elif j == 1:
                    d.text((670, py + 52), "flexibility envelopes", fill=FG, font=mf)
                else:
                    d.text((670, py + 52), "demand profiles", fill=FG, font=mf)

                # connector arrow
                draw_arrow(d, 570, py + 40, 636, py + 40, color=pcolor, width=2)

        # Show all participants on last frame
        if step == len(layers) + 1:
            # "Each node has its own connector" label
            sf = _font(11)
            d.text((580, 390), "Each node wraps all exchanges with", fill=DIM, font=sf)
            d.text((580, 406), "the Data Space Connector (auth + policy + audit)", fill=DIM, font=sf)

        frames.append(img)
        durations.append(1500 if step < len(layers) else 2000)

    save_gif(frames, "concept-architecture.gif", durations)


# ═══════════════════════════════════════════════════════════════════════════
# 2. CONGESTION MANAGEMENT E2E FLOW
# ═══════════════════════════════════════════════════════════════════════════
def gen_congestion_flow():
    steps = [
        ("1", "DSO publishes feeder\nconstraints to Catalog", "DSO", "Catalog", "POST /assets", GREEN),
        ("2", "Aggregator discovers\nconstraint asset", "Aggregator", "Catalog", "GET /assets?type=...", BLUE),
        ("3", "Aggregator negotiates\ndata usage contract", "Aggregator", "DSO", "POST /contracts", MAUVE),
        ("4", "Aggregator reads\nconstraint data", "Aggregator", "DSO", "GET /constraints", YELLOW),
        ("5", "Aggregator submits\nflexibility offer", "Aggregator", "DSO", "POST /flexibility-offers", TEAL),
        ("6", "DSO dispatches\nvia Kafka", "DSO", "Aggregator", "Kafka: dispatch-commands", PEACH),
        ("7", "Aggregator reports\nactuals", "Aggregator", "DSO", "POST /dispatch-response", PINK),
        ("*", "All steps recorded\nin audit trail", "", "", "SHA-256 hashed", RED),
    ]

    actors = {"DSO": (120, RED), "Catalog": (380, TEAL), "Aggregator": (640, BLUE)}
    frames = []
    durations = []

    for step_idx in range(len(steps) + 1):
        img, d = new_frame()
        draw_title(d, "Congestion Management Flow", "End-to-end: DSO constraint -> Aggregator flexibility -> Dispatch")

        # Actor boxes
        for name, (ax, ac) in actors.items():
            draw_box(d, ax, 70, 130, 40, fill=SURFACE0, outline=ac, radius=8,
                     text=name, font=_font(15, bold=True), text_color=ac)
            # lifeline
            d.line([(ax + 65, 110), (ax + 65, 490)], fill=SURFACE1, width=1)

        # Steps up to current
        for i in range(min(step_idx, len(steps))):
            snum, sdesc, sfrom, sto, slabel, scolor = steps[i]
            sy = 130 + i * 45

            if sfrom and sto:
                x1 = actors[sfrom][0] + 65
                x2 = actors[sto][0] + 65
                draw_arrow(d, x1, sy, x2, sy, color=scolor, width=2)

                # step number circle
                mx = (x1 + x2) // 2
                d.ellipse([mx - 11, sy - 11, mx + 11, sy + 11], fill=scolor)
                nf = _font(12, bold=True)
                nb = d.textbbox((0, 0), snum, font=nf)
                d.text((mx - (nb[2] - nb[0]) // 2, sy - (nb[3] - nb[1]) // 2 - 1),
                       snum, fill=BG, font=nf)

                # label
                lf = _mono(10)
                if x1 < x2:
                    d.text((x1 + 10, sy - 16), slabel, fill=DIM, font=lf)
                else:
                    d.text((x2 + 10, sy - 16), slabel, fill=DIM, font=lf)
            else:
                # Audit trail banner
                draw_box(d, 100, sy - 5, 700, 30, fill=SURFACE0, outline=scolor, radius=6,
                         text=f"  {sdesc.replace(chr(10), ' ')}  |  {slabel}",
                         font=_font(12), text_color=scolor)

            # highlight current step
            if i == step_idx - 1 and sfrom and sto:
                desc_f = _font(11)
                lines = sdesc.split("\n")
                for li, line in enumerate(lines):
                    d.text((30, 470 + li * 16), line, fill=scolor, font=desc_f)

        frames.append(img)
        durations.append(2000)

    save_gif(frames, "concept-congestion-flow.gif", durations)


# ═══════════════════════════════════════════════════════════════════════════
# 3. CONTRACT NEGOTIATION STATE MACHINE
# ═══════════════════════════════════════════════════════════════════════════
def gen_contract_states():
    states = [
        ("OFFERED", 150, 200, YELLOW),
        ("NEGOTIATING", 370, 200, MAUVE),
        ("ACTIVE", 590, 200, GREEN),
        ("EXPIRED", 480, 360, DIM),
        ("REVOKED", 680, 360, RED),
        ("REJECTED", 260, 360, RED),
    ]
    transitions = [
        (0, 1, "negotiate()", MAUVE),
        (1, 2, "accept()", GREEN),
        (2, 3, "time expires", DIM),
        (2, 4, "revoke()", RED),
        (0, 5, "reject()", RED),
        (1, 5, "reject()", RED),
    ]

    frames = []
    durations = []

    # Frame 0: empty
    img, d = new_frame()
    draw_title(d, "Contract Negotiation", "State machine: data access requires an ACTIVE contract")
    frames.append(img)
    durations.append(1500)

    # Reveal states one by one (happy path: OFFERED -> NEGOTIATING -> ACTIVE)
    happy_path_order = [0, 1, 2]  # indices into states
    for reveal in range(1, 4):
        img, d = new_frame()
        draw_title(d, "Contract Negotiation", "State machine: data access requires an ACTIVE contract")

        for i in range(reveal):
            si = happy_path_order[i]
            name, sx, sy, sc = states[si]
            draw_box(d, sx, sy, 160, 50, fill=SURFACE0, outline=sc, radius=25,
                     text=name, font=_font(16, bold=True), text_color=sc)

        # transitions between revealed
        for ti, (fr, to, label, tc) in enumerate(transitions):
            if fr in happy_path_order[:reveal] and to in happy_path_order[:reveal]:
                _, fx, fy, _ = states[fr]
                _, tx, ty, _ = states[to]
                draw_arrow(d, fx + 160, fy + 25, tx, ty + 25, color=tc, width=2)
                lf = _font(11)
                mx = (fx + 160 + tx) // 2
                d.text((mx - 20, fy + 2), label, fill=tc, font=lf)

        frames.append(img)
        durations.append(1800)

    # Final frame: all states + all transitions
    for pass_num in range(2):
        img, d = new_frame()
        draw_title(d, "Contract Negotiation", "State machine: data access requires an ACTIVE contract")

        for i, (name, sx, sy, sc) in enumerate(states):
            is_terminal = i >= 3
            draw_box(d, sx, sy, 160, 50, fill=SURFACE0 if not is_terminal else SURFACE1,
                     outline=sc, radius=25,
                     text=name, font=_font(16, bold=True), text_color=sc)
            if is_terminal:
                sf = _font(10)
                d.text((sx + 45, sy + 52), "terminal", fill=DIM, font=sf)

        for fr, to, label, tc in transitions:
            _, fx, fy, _ = states[fr]
            _, tx, ty, _ = states[to]
            if fy == ty:
                draw_arrow(d, fx + 160, fy + 25, tx, ty + 25, color=tc, width=2)
                mx = (fx + 160 + tx) // 2
                lf = _font(11)
                d.text((mx - 20, fy + 2), label, fill=tc, font=lf)
            else:
                draw_arrow(d, fx + 80, fy + 50, tx + 80, ty, color=tc, width=2, dashed=True)
                mx = (fx + 80 + tx + 80) // 2
                my = (fy + 50 + ty) // 2
                lf = _font(10)
                d.text((mx + 5, my - 8), label, fill=tc, font=lf)

        if pass_num == 1:
            # Add "no data without contract" note
            draw_box(d, 180, 440, 540, 40, fill=SURFACE0, outline=YELLOW, radius=8,
                     text="No data exchange without an ACTIVE contract",
                     font=_font(14, bold=True), text_color=YELLOW)

        frames.append(img)
        durations.append(2500)

    save_gif(frames, "concept-contract-states.gif", durations)


# ═══════════════════════════════════════════════════════════════════════════
# 4. AUTH FLOW - OIDC + mTLS
# ═══════════════════════════════════════════════════════════════════════════
def gen_auth_flow():
    frames = []
    durations = []

    steps_data = [
        {
            "title": "Step 1: mTLS Handshake",
            "desc": "Service-to-service trust via mutual TLS certificates",
            "boxes": [
                (60, 150, 180, 70, "Aggregator\n(client cert)", BLUE),
                (660, 150, 180, 70, "DSO\n(server cert)", RED),
            ],
            "arrows": [(240, 170, 656, 170, "TLS ClientHello + cert", TEAL),
                       (656, 200, 240, 200, "TLS ServerHello + cert", GREEN)],
            "note": "Both sides verify certs signed by shared CA",
        },
        {
            "title": "Step 2: OIDC Token Request",
            "desc": "Aggregator authenticates with Keycloak to get JWT",
            "boxes": [
                (60, 150, 180, 70, "Aggregator", BLUE),
                (360, 150, 180, 70, "Keycloak\n(OIDC IdP)", MAUVE),
            ],
            "arrows": [(240, 170, 356, 170, "client_credentials grant", YELLOW),
                       (356, 200, 240, 200, "JWT access_token", GREEN)],
            "note": "Token contains: sub, roles, org, exp",
        },
        {
            "title": "Step 3: Token Validation (Local)",
            "desc": "DSO validates JWT locally using cached JWK keys",
            "boxes": [
                (60, 150, 180, 70, "Aggregator", BLUE),
                (360, 150, 180, 70, "DSO Node", RED),
                (660, 150, 180, 70, "Keycloak\nJWKS endpoint", MAUVE),
            ],
            "arrows": [(240, 175, 356, 175, "Bearer <JWT>", YELLOW),
                       (540, 190, 656, 190, "cached JWK keys", DIM)],
            "note": "No per-request introspection -> low latency",
        },
        {
            "title": "Step 4: Access Decision",
            "desc": "Connector middleware checks auth + policy + audit",
            "boxes": [],
            "arrows": [],
            "note": "",
        },
    ]

    for si, step in enumerate(steps_data):
        img, d = new_frame()
        draw_title(d, "Identity & Trust Layer", "OIDC + mTLS dual authentication")

        # Step title
        f = _font(16, bold=True)
        d.text((60, 90), step["title"], fill=TEAL, font=f)
        sf = _font(12)
        d.text((60, 114), step["desc"], fill=DIM, font=sf)

        if si < 3:
            for bx, by, bw, bh, btxt, bc in step["boxes"]:
                draw_box(d, bx, by, bw, bh, fill=SURFACE0, outline=bc, radius=10,
                         text=btxt, font=_font(13, bold=True), text_color=bc)

            for ax1, ay1, ax2, ay2, alabel, ac in step["arrows"]:
                draw_arrow(d, ax1, ay1, ax2, ay2, color=ac, width=2)
                lf = _mono(10)
                mx = (ax1 + ax2) // 2 - 50
                my = min(ay1, ay2) - 16
                d.text((mx, my), alabel, fill=ac, font=lf)

            nf = _font(12)
            d.text((60, 260), step["note"], fill=YELLOW, font=nf)
        else:
            # Access decision diagram
            checks = [
                ("mTLS cert valid?", GREEN, True),
                ("JWT signature valid?", GREEN, True),
                ("Token not expired?", GREEN, True),
                ("Participant registered?", GREEN, True),
                ("Contract ACTIVE?", GREEN, True),
                ("Purpose matches?", GREEN, True),
            ]
            for i, (label, color, passed) in enumerate(checks):
                cy = 160 + i * 42
                # checkbox
                draw_box(d, 100, cy, 28, 28, fill=SURFACE0, outline=color, radius=4)
                cf = _font(16, bold=True)
                d.text((105, cy + 2), "✓" if passed else "✗", fill=color, font=cf)
                lf = _font(14)
                d.text((140, cy + 4), label, fill=FG, font=lf)

            # Result
            draw_box(d, 500, 180, 300, 50, fill=SURFACE0, outline=GREEN, radius=10,
                     text="200 OK  +  Audit logged", font=_font(14, bold=True), text_color=GREEN)

            # Rejection examples
            d.text((500, 270), "Rejection examples:", fill=DIM, font=_font(12))
            rejects = [
                ("No token", "→ 401 Unauthorized", RED),
                ("Expired token", "→ 401 + WWW-Authenticate", RED),
                ("Wrong role", "→ 403 Forbidden", PEACH),
                ("No contract", "→ 403 + audit: denied", PEACH),
            ]
            for i, (cause, result, rc) in enumerate(rejects):
                ry = 295 + i * 28
                d.text((510, ry), cause, fill=rc, font=_font(12))
                d.text((650, ry), result, fill=DIM, font=_mono(10))

        frames.append(img)
        durations.append(2500)

    save_gif(frames, "concept-auth-flow.gif", durations)


# ═══════════════════════════════════════════════════════════════════════════
# 5. PRIVACY - PURPOSE-BASED ANONYMIZATION
# ═══════════════════════════════════════════════════════════════════════════
def gen_privacy():
    frames = []
    durations = []

    # Frame 1: Raw data at prosumer
    img, d = new_frame()
    draw_title(d, "Privacy & Data Sovereignty", "Purpose determines disclosure level — data never leaves raw")

    draw_box(d, 50, 90, 280, 120, fill=SURFACE0, outline=PEACH, radius=10)
    d.text((70, 100), "Prosumer Local Store", fill=PEACH, font=_font(14, bold=True))
    d.text((70, 125), "meter_id: CAMPUS-042", fill=FG, font=_mono(11))
    d.text((70, 143), "kWh: [12.3, 15.1, 8.7, ...]", fill=FG, font=_mono(11))
    d.text((70, 161), "voltage: [231.2, 230.8, ...]", fill=FG, font=_mono(11))
    d.text((70, 179), "building: Science Hall", fill=FG, font=_mono(11))

    d.text((60, 230), "This raw data NEVER leaves the prosumer node", fill=RED, font=_font(13, bold=True))

    frames.append(img)
    durations.append(2500)

    # Frame 2: Purpose -> Disclosure mapping
    img, d = new_frame()
    draw_title(d, "Privacy & Data Sovereignty", "Purpose determines disclosure level — data never leaves raw")

    purposes = [
        ("research", "AGGREGATED", "Statistical means/std only, no identity", TEAL),
        ("dr_dispatch", "CONTROLLABILITY", "Only controllable margin (kW), nothing else", BLUE),
        ("billing", "IDENTIFIED", "With explicit consent, identity preserved", YELLOW),
        ("forecasting", "ANONYMIZED", "k-anonymized, no individual identification", MAUVE),
    ]

    d.text((60, 85), "Requester's Purpose", fill=DIM, font=_font(13, bold=True))
    d.text((340, 85), "Disclosure Level", fill=DIM, font=_font(13, bold=True))
    d.text((560, 85), "What the requester receives", fill=DIM, font=_font(13, bold=True))

    for i, (purpose, level, desc, color) in enumerate(purposes):
        py = 115 + i * 90

        # Purpose box
        draw_box(d, 50, py, 230, 45, fill=SURFACE0, outline=color, radius=8,
                 text=purpose, font=_mono(14), text_color=color)

        # Arrow
        draw_arrow(d, 280, py + 22, 335, py + 22, color=color, width=2)

        # Level box
        draw_box(d, 340, py, 190, 45, fill=SURFACE0, outline=color, radius=8,
                 text=level, font=_font(12, bold=True), text_color=color)

        # Description
        df = _font(11)
        d.text((545, py + 14), desc, fill=DIM, font=df)

    # Bottom note
    draw_box(d, 100, 480, 700, 30, fill=SURFACE0, outline=RED, radius=6,
             text="Default = maximum restriction. Only explicit consent widens access.",
             font=_font(12, bold=True), text_color=RED)

    frames.append(img)
    durations.append(3000)

    # Frame 3: Anonymization pipeline
    img, d = new_frame()
    draw_title(d, "Privacy & Data Sovereignty", "Anonymization pipeline inside the Prosumer node")

    pipeline = [
        ("Raw\nMeter Data", PEACH, 50),
        ("Consent\nCheck", RED, 220),
        ("Purpose\nResolver", MAUVE, 390),
        ("Anonymizer\n(k-anon)", TEAL, 560),
        ("Disclosed\nData", GREEN, 730),
    ]

    for i, (label, color, px) in enumerate(pipeline):
        draw_box(d, px, 160, 130, 70, fill=SURFACE0, outline=color, radius=10,
                 text=label, font=_font(13, bold=True), text_color=color)
        if i < len(pipeline) - 1:
            nx = pipeline[i + 1][2]
            draw_arrow(d, px + 130, 195, nx, 195, color=DIM, width=2)

    # Rejection path
    draw_arrow(d, 285, 230, 285, 310, color=RED, width=2)
    draw_box(d, 200, 310, 170, 40, fill=SURFACE1, outline=RED, radius=8,
             text="403 Denied", font=_font(14, bold=True), text_color=RED)
    d.text((210, 355), "No consent = no data", fill=DIM, font=_font(11))

    # k-anonymity note
    d.text((480, 270), "k-anonymity guarantee:", fill=TEAL, font=_font(12, bold=True))
    d.text((480, 290), "Each output record is", fill=DIM, font=_font(11))
    d.text((480, 306), "indistinguishable from at", fill=DIM, font=_font(11))
    d.text((480, 322), "least k-1 other records", fill=DIM, font=_font(11))

    # Consent revocation
    d.text((60, 420), "Consent revocation is immediate:", fill=YELLOW, font=_font(13, bold=True))
    d.text((60, 442), "If prosumer revokes consent mid-contract,", fill=DIM, font=_font(12))
    d.text((60, 460), "all subsequent requests are denied.", fill=DIM, font=_font(12))

    frames.append(img)
    durations.append(3000)

    save_gif(frames, "concept-privacy.gif", durations)


# ═══════════════════════════════════════════════════════════════════════════
# 6. AUDIT TRAIL
# ═══════════════════════════════════════════════════════════════════════════
def gen_audit():
    frames = []
    durations = []

    # Frame 1: What gets logged
    img, d = new_frame()
    draw_title(d, "Immutable Audit Trail", "Every data exchange is hashed and logged — no exceptions")

    fields = [
        ("timestamp", "2026-03-17T09:33:00Z", TEAL),
        ("requester_id", "aggregator-001", BLUE),
        ("provider_id", "dso-001", RED),
        ("asset_id", "/api/v1/constraints", FG),
        ("purpose_tag", "congestion_management", MAUVE),
        ("contract_id", "c-a7b3e...", YELLOW),
        ("request_hash", "sha256:9f4a2b...", GREEN),
        ("response_hash", "sha256:e1c8d3...", GREEN),
        ("action", "read", FG),
        ("outcome", "success", GREEN),
    ]

    draw_box(d, 80, 80, 740, len(fields) * 36 + 30, fill=SURFACE0, outline=TEAL, radius=12)
    d.text((100, 90), "AuditEntry", fill=TEAL, font=_font(16, bold=True))

    for i, (fname, fval, fc) in enumerate(fields):
        fy = 120 + i * 36
        d.text((110, fy), fname, fill=DIM, font=_mono(13))
        d.text((310, fy), ":", fill=DIM, font=_mono(13))
        d.text((330, fy), fval, fill=fc, font=_mono(13))

    frames.append(img)
    durations.append(3000)

    # Frame 2: How it works
    img, d = new_frame()
    draw_title(d, "Immutable Audit Trail", "Connector middleware intercepts every request/response")

    # Flow
    actors = [
        ("Requester", 50, BLUE),
        ("Connector\nMiddleware", 250, MAUVE),
        ("Route\nHandler", 500, FG),
        ("Audit\nLog", 720, TEAL),
    ]
    for name, ax, ac in actors:
        draw_box(d, ax, 100, 140, 55, fill=SURFACE0, outline=ac, radius=10,
                 text=name, font=_font(12, bold=True), text_color=ac)

    # Steps
    steps = [
        (190, 190, 250, 190, "1. Request arrives", BLUE),
        (310, 200, 310, 230, "", DIM),
    ]

    sy = 180
    flow = [
        (50 + 70, 250 + 70, "1. Request", BLUE),
        (250 + 70, 500 + 70, "2. Auth + Policy check", MAUVE),
        (500 + 70, 250 + 70, "3. Response", GREEN),
        (250 + 70, 720 + 70, "4. Log (req_hash + resp_hash)", TEAL),
        (250 + 70, 50 + 70, "5. Forward response", GREEN),
    ]

    for i, (fx, tx, label, color) in enumerate(flow):
        y = 190 + i * 55
        draw_arrow(d, fx, y, tx, y, color=color, width=2)
        lf = _font(11)
        mx = (fx + tx) // 2 - 60
        d.text((mx, y - 16), label, fill=color, font=lf)

    frames.append(img)
    durations.append(3000)

    # Frame 3: Tamper evidence
    img, d = new_frame()
    draw_title(d, "Immutable Audit Trail", "Tamper evidence through SHA-256 hashing")

    d.text((60, 90), "Request body", fill=BLUE, font=_font(14, bold=True))
    draw_box(d, 60, 115, 350, 40, fill=SURFACE0, outline=SURFACE2, radius=6,
             text='GET /api/v1/constraints?feeder_id=F-101', font=_mono(11), text_color=FG)
    draw_arrow(d, 210, 155, 210, 190, color=GREEN, width=2)
    d.text((230, 165), "SHA-256", fill=GREEN, font=_font(12, bold=True))
    draw_box(d, 60, 195, 350, 35, fill=SURFACE0, outline=GREEN, radius=6,
             text='9f4a2b8c71d3e...', font=_mono(13), text_color=GREEN)

    d.text((480, 90), "Response body", fill=RED, font=_font(14, bold=True))
    draw_box(d, 480, 115, 370, 40, fill=SURFACE0, outline=SURFACE2, radius=6,
             text='{"feeder_id":"F-101","max_kw":5000}', font=_mono(11), text_color=FG)
    draw_arrow(d, 640, 155, 640, 190, color=GREEN, width=2)
    d.text((660, 165), "SHA-256", fill=GREEN, font=_font(12, bold=True))
    draw_box(d, 480, 195, 370, 35, fill=SURFACE0, outline=GREEN, radius=6,
             text='e1c8d3f5a902b...', font=_mono(13), text_color=GREEN)

    # Verification
    d.text((60, 270), "Verification:", fill=YELLOW, font=_font(14, bold=True))
    d.text((60, 295), "Recompute hash from stored data", fill=DIM, font=_font(12))
    d.text((60, 315), "→ Match = data integrity confirmed", fill=GREEN, font=_font(12))
    d.text((60, 335), "→ Mismatch = tampering detected", fill=RED, font=_font(12))

    # Properties
    props = [
        ("Append-only", "Entries cannot be modified or deleted"),
        ("Non-optional", "Failing to audit = failing the request"),
        ("Synchronous", "Audit completes before response is sent"),
    ]
    for i, (pname, pdesc) in enumerate(props):
        py = 390 + i * 35
        d.text((100, py), "•", fill=TEAL, font=_font(14))
        d.text((120, py), pname, fill=TEAL, font=_font(13, bold=True))
        d.text((260, py), pdesc, fill=DIM, font=_font(12))

    frames.append(img)
    durations.append(3000)

    save_gif(frames, "concept-audit.gif", durations)


# ═══════════════════════════════════════════════════════════════════════════
# 7. CATALOG DISCOVERY
# ═══════════════════════════════════════════════════════════════════════════
def gen_catalog():
    frames = []
    durations = []

    # Frame 1: Registration
    img, d = new_frame()
    draw_title(d, "Federated Catalog", "Decentralized discovery — data stays local, metadata is shared")

    # Three participants registering
    participants = [
        ("DSO", 50, RED, ["feeder_constraints", "congestion_signals", "hosting_capacity"]),
        ("Aggregator", 310, BLUE, ["flexibility_envelope", "availability_windows"]),
        ("Prosumer", 600, PEACH, ["demand_profile (consent-gated)"]),
    ]

    # Catalog in center bottom
    draw_box(d, 310, 350, 280, 60, fill=SURFACE0, outline=TEAL, radius=12,
             text="Federated Catalog", font=_font(16, bold=True), text_color=TEAL)
    d.text((345, 380), "metadata only — no raw data", fill=DIM, font=_font(11))

    for name, px, color, assets in participants:
        draw_box(d, px, 100, 230, 30 + len(assets) * 22, fill=SURFACE0, outline=color, radius=10)
        d.text((px + 15, 108), name, fill=color, font=_font(14, bold=True))
        for j, asset in enumerate(assets):
            d.text((px + 15, 135 + j * 22), f"• {asset}", fill=FG, font=_mono(10))

        # Arrow to catalog
        draw_arrow(d, px + 115, 130 + len(assets) * 22, 450, 346,
                   color=color, width=2, dashed=True)

    d.text((60, 435), "POST /api/v1/assets  →  register metadata (provider, type, sensitivity, policy, endpoint)",
           fill=DIM, font=_mono(10))

    frames.append(img)
    durations.append(3000)

    # Frame 2: Discovery
    img, d = new_frame()
    draw_title(d, "Federated Catalog", "Discovery: search by provider, type, or sensitivity tier")

    draw_box(d, 310, 100, 280, 50, fill=SURFACE0, outline=TEAL, radius=12,
             text="Federated Catalog", font=_font(16, bold=True), text_color=TEAL)

    # Search query
    draw_box(d, 60, 200, 300, 50, fill=SURFACE0, outline=BLUE, radius=10,
             text="Aggregator", font=_font(14, bold=True), text_color=BLUE)

    draw_arrow(d, 360, 200, 450, 154, color=BLUE, width=2)
    d.text((280, 175), "GET /assets?type=feeder_constraint", fill=BLUE, font=_mono(10))

    # Results
    draw_arrow(d, 450, 154, 450, 290, color=TEAL, width=2)
    draw_box(d, 350, 290, 500, 180, fill=SURFACE0, outline=TEAL, radius=10)
    d.text((370, 300), "Search Results:", fill=TEAL, font=_font(13, bold=True))

    result_lines = [
        ('  "asset_id": "asset-f101"', FG),
        ('  "provider_id": "dso-001"', RED),
        ('  "data_type": "feeder_constraint"', FG),
        ('  "sensitivity": "medium"', YELLOW),
        ('  "policy": {', MAUVE),
        ('    "purpose": "congestion_management"', MAUVE),
        ('    "contract_required": true', MAUVE),
        ('  }', MAUVE),
    ]
    for i, (line, color) in enumerate(result_lines):
        d.text((370, 325 + i * 18), line, fill=color, font=_mono(10))

    frames.append(img)
    durations.append(3000)

    save_gif(frames, "concept-catalog.gif", durations)


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("Generating concept GIFs...")
    gen_architecture()
    gen_congestion_flow()
    gen_contract_states()
    gen_auth_flow()
    gen_privacy()
    gen_audit()
    gen_catalog()
    print("Done!")
