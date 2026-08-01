# Logo generator

`gen_logo.py` regenerates all four MASS logo assets deterministically:

| Asset | What |
| --- | --- |
| `web/public/MASS-Dark.png` | wordmark, white on transparent (dashboard dark themes) |
| `web/public/MASS-Light.png` | wordmark, black on transparent (dashboard light themes) |
| `internal/icon/icon.png` | 512px app icon, embedded via `go:embed` (window/taskbar/tray) |
| `winres/icon.png` | 256px source for the Windows `.ico` built by winres |

The wordmark is the symbol sequence **M A §** — dome arch, triangle,
section sign. The § is drawn from two S-strokes whose middle circles
coincide; since § originated as a double-S ligature, the mark still spells
"MASS". The M and § are drawn parametrically; the A is reused from the
original artwork, which the script reads from git history (`BASE_REV`),
so runs are reproducible regardless of the working tree.

## Usage

```sh
python3 assets/gen/gen_logo.py   # from anywhere inside the repo
```

Dependencies: `pillow`, `numpy`, `scipy` (`pip install --user pillow numpy scipy`).

After regenerating, rebuild the `mass` binary — the app icon is embedded
at compile time.

## Tweaking

All design decisions are constants at the top of the script, in pixels at
wordmark scale (cap height 242, stroke 19): `ROUND_R` (corner/cap
rounding), `M_WIDEN` (dome width), `GAP_EXTRA` (tracking), `SS_TIGHTEN`
(optical A-§ spacing), `FADE_START` / `FADE_END_ALPHA` (baseline fade).
Icons rescale these by their own stroke width automatically.
