"""Generate slideshow GIFs from the demo output PNGs."""
from pathlib import Path
from PIL import Image

OUT = Path(__file__).parent
EXAMPLE_OUT = Path(__file__).resolve().parent.parent.parent / "examples" / "output"

def make_slideshow(files: list[Path], out_name: str, duration: int = 3000):
    imgs = []
    target_w, target_h = 1400, 900
    for f in files:
        if not f.exists():
            print(f"  SKIP {f.name}")
            continue
        img = Image.open(f).convert("RGB")
        # Resize to uniform size, maintaining aspect ratio with padding
        ratio = min(target_w / img.width, target_h / img.height)
        new_w = int(img.width * ratio)
        new_h = int(img.height * ratio)
        img = img.resize((new_w, new_h), Image.LANCZOS)
        canvas = Image.new("RGB", (target_w, target_h), (24, 24, 37))
        canvas.paste(img, ((target_w - new_w) // 2, (target_h - new_h) // 2))
        imgs.append(canvas)

    if imgs:
        imgs[0].save(OUT / out_name, save_all=True, append_images=imgs[1:],
                     duration=[duration] * len(imgs), loop=0, optimize=True)
        print(f"  -> {out_name} ({len(imgs)} frames)")

# Congestion management slideshow
congestion_files = [
    EXAMPLE_OUT / "01_feeder_overview.png",
    EXAMPLE_OUT / "02_security_audit.png",
    EXAMPLE_OUT / "03_dr_dispatch.png",
    EXAMPLE_OUT / "04_privacy.png",
    EXAMPLE_OUT / "05_summary.png",
]
print("Congestion management slideshow:")
make_slideshow(congestion_files, "slideshow-congestion.gif", 3500)

# Grid topology slideshow
grid_files = [
    EXAMPLE_OUT / "06_grid_topology.png",
    EXAMPLE_OUT / "07_voltage_profile.png",
    EXAMPLE_OUT / "08_dr_control.png",
    EXAMPLE_OUT / "09_pole_detail.png",
]
print("Grid topology slideshow:")
make_slideshow(grid_files, "slideshow-grid.gif", 3500)

# Combined full slideshow
print("Full demo slideshow:")
make_slideshow(congestion_files + grid_files, "slideshow-full.gif", 3000)
