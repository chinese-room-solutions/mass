"""MASS logo generator.

Regenerates the four logo assets from the pre-redesign artwork in git
history (BASE_REV) — nothing is read from the working tree, so the output
is fully reproducible:

    web/public/MASS-Dark.png    wordmark, white on transparent
    web/public/MASS-Light.png   wordmark, black on transparent
    internal/icon/icon.png      app icon 512x512 (embedded via go:embed)
    winres/icon.png             app icon 256x256 (Windows .ico source)

The wordmark is the symbol sequence "M A §":

  * M — a dome arch: half-ellipse silhouette with a concentric inner void
    and a center leg dropping from the apex.
  * A — the original triangle glyph, reused as raster, with the base bar's
    angled left end squared off.
  * § — drawn parametrically: two S-shaped strokes (each two circular
    arcs) whose middle circles coincide, forming the closed center loop.
    Historically § is a double-S ligature, so the mark still spells the
    "SS" of MASS.

Every stroke end and corner is rounded (ROUND_R) and the whole mark fades
toward the baseline (FADE_*). Icons carry the dome-M alone, widened to
nearly fill the square canvas.

Usage:  python3 assets/gen/gen_logo.py     (from anywhere in the repo)
Deps:   pillow, numpy, scipy
"""
import io
import math
import subprocess

import numpy as np
from PIL import Image, ImageDraw
from scipy.ndimage import distance_transform_edt as edt

# ---------------------------------------------------------------- constants

#: Supersampling factor: all drawing happens at SS x resolution, and the
#: final downscale (box filter) provides the antialiasing.
SS = 4

#: Revision holding the original (pre-redesign) raster artwork.
BASE_REV = '9e8b86239b52ceab40ad4faf66269d332fe29aab'

#: Repo root — the script may be invoked from any CWD inside the repo.
REPO = subprocess.run(['git', 'rev-parse', '--show-toplevel'],
                      capture_output=True, text=True).stdout.strip()

# Design parameters, all in pixels at wordmark scale (cap height 242,
# stroke 19). Icon rendering rescales them by its own stroke width.
STROKE = 19.0        #: uniform stroke width of the original letters
ROUND_R = 7.0        #: corner / stroke-cap rounding radius
M_WIDEN = 40         #: extra width given to the dome-M
GAP_EXTRA = 135      #: extra tracking between the three symbols
SS_TIGHTEN = 0       #: optical pull-in for the section sign's curved side
FADE_START = 0.30    #: fraction of cap height where the alpha fade begins
FADE_END_ALPHA = 0.35  #: alpha multiplier reached at the baseline


# ------------------------------------------------------------------ helpers

def load_original(path):
    """Load an asset as RGBA from the pinned pre-redesign revision."""
    blob = subprocess.run(['git', '-C', REPO, 'show', f'{BASE_REV}:{path}'],
                          capture_output=True).stdout
    return Image.open(io.BytesIO(blob)).convert('RGBA')


def letter_boxes(mask):
    """Split a wordmark mask into per-letter column spans.

    Returns ([(x0, x1), ...], y0, y1) with x1/y1 exclusive; letters are
    separated by gaps of more than 5 empty columns.
    """
    cols = np.where(mask.any(axis=0))[0]
    boxes, start, prev = [], cols[0], cols[0]
    for x in cols[1:]:
        if x > prev + 5:
            boxes.append((int(start), int(prev) + 1))
            start = x
        prev = x
    boxes.append((int(start), int(prev) + 1))
    rows = np.where(mask.any(axis=1))[0]
    return boxes, int(rows.min()), int(rows.max()) + 1


def round_corners(fg, radius_ss):
    """Morphological open + close: rounds convex and concave corners alike."""
    r = radius_ss
    opened = edt(~(edt(fg) > r)) <= r
    return ~(edt(~(edt(~opened) > r)) <= r)


def fade(alpha, y0, y1):
    """Vertical alpha fade: full strength until FADE_START, then linear
    down to FADE_END_ALPHA at the baseline."""
    rows = np.arange(alpha.shape[0], dtype=float)
    frac = np.clip((rows - (y0 + FADE_START * (y1 - y0)))
                   / ((1 - FADE_START) * (y1 - y0)), 0, 1)
    gain = 1.0 - (1.0 - FADE_END_ALPHA) * frac
    return (alpha.astype(float) * gain[:, None]).round().astype(np.uint8)


def colorize(alpha, fill):
    """Solid-color RGBA image from an alpha channel."""
    out = np.zeros(alpha.shape + (4,), np.uint8)
    out[..., 0], out[..., 1], out[..., 2] = fill
    out[..., 3] = alpha
    return Image.fromarray(out)


def downscale(fg, size):
    """Boolean supersampled mask -> antialiased uint8 alpha at `size`."""
    img = Image.fromarray((fg * 255).astype(np.uint8))
    return np.array(img.resize(size, Image.Resampling.BOX))


# ------------------------------------------------------------------- glyphs

def draw_dome_m(d, x0, x1, y0, y1, w, canvas_h_ss):
    """Dome-arch M: half-ellipse band plus a center leg, baseline-cropped."""
    cx = (x0 + x1) / 2
    d.ellipse([x0 * SS, y0 * SS, x1 * SS, (2 * y1 - y0) * SS], fill=255)
    d.ellipse([(x0 + w) * SS, (y0 + w) * SS,
               (x1 - w) * SS, (2 * y1 - y0) * SS], fill=0)
    d.rectangle([(x0 - 3) * SS, y1 * SS, (x1 + 3) * SS, canvas_h_ss], fill=0)
    d.rectangle([(cx - w / 2) * SS, (y0 + w - 2) * SS,
                 (cx + w / 2) * SS, y1 * SS], fill=255)


def a_bar_square_patch(mask, abox):
    """Triangle that squares off the A base bar's angled left end.

    The original A's base bar ends in a ~66 degree cut; filling this
    triangle makes the end vertical so the global rounding turns it into
    the same symmetric cap every other stroke end gets.
    """
    for y in range(mask.shape[0] - 1, -1, -1):
        if mask[y, abox[0]:abox[1]].any():
            break
    y_bottom = y + 1
    bar_top = x_top = x_bottom = None
    for yy in range(y_bottom - 40, y_bottom):
        xs = np.where(mask[yy, abox[0]:abox[1]])[0] + abox[0]
        if len(xs) == 0:
            continue
        runs, start, prev = [], xs[0], xs[0]
        for x in xs[1:]:
            if x != prev + 1:
                runs.append((start, prev))
                start = x
            prev = x
        runs.append((start, prev))
        bars = [r for r in runs if r[1] - r[0] > 120]
        if bars:
            if bar_top is None:
                bar_top, x_top = yy, bars[0][0]
            x_bottom = bars[0][0]
    return [(x_bottom, bar_top - 0.7), (x_top + 1.5, bar_top - 0.7),
            (x_bottom, y_bottom)]


def s_spine(cx, cy_top, r, samples=420):
    """Spine of an S stroke: two circular arcs, total height 4r.

    Image coordinates (y grows down). The top bowl opens right, the bottom
    bowl opens left; terminals sit at the upper-right and lower-left.
    """
    pts = []
    for a in np.linspace(math.radians(25), math.radians(270), samples // 2):
        pts.append((cx + r * math.cos(a), cy_top + r - r * math.sin(a)))
    for a in np.linspace(math.radians(90), math.radians(-155), samples // 2):
        pts.append((cx + r * math.cos(a), cy_top + 3 * r - r * math.sin(a)))
    return pts


def stamp_stroke(d, pts, w):
    """Stroke a spine by stamping discs along it.

    (PIL's wide-line joint rendering produces artifacts on dense curved
    paths, so discs it is — the spine sampling is dense enough that the
    envelope is smooth.)
    """
    hw = w * SS / 2
    for x, y in pts:
        d.ellipse([x * SS - hw, y * SS - hw, x * SS + hw, y * SS + hw],
                  fill=255)


def draw_section(d, x_left, y0, y1, w):
    """Section sign: two S spines whose middle circles coincide.

    The coinciding circles fuse into the closed center loop that defines
    the glyph. Total height is 6r == the cap band; returns the glyph's
    right edge in unscaled pixels.
    """
    r = (y1 - y0) / 6.0
    cx = x_left + r + w / 2
    stamp_stroke(d, s_spine(cx, y0 + w / 2, r), w)
    stamp_stroke(d, s_spine(cx, y0 + w / 2 + 2 * r, r), w)
    return int(cx + r + w / 2)


# ------------------------------------------------------------------- assets

def gen_wordmark(path, fill):
    """Render the MA-section wordmark over the original's letter metrics."""
    original = load_original(path)
    alpha = np.array(original)[..., 3]
    h_img, w_img = alpha.shape
    mask = alpha > 127
    boxes, y0, y1 = letter_boxes(mask)
    mbox, abox = boxes[0], boxes[1]

    raster = np.array(Image.fromarray(alpha).resize(
        (w_img * SS, h_img * SS), Image.Resampling.BILINEAR)) > 127

    # Canvas wide enough for the widened M and extra tracking.
    canvas = np.zeros((h_img * SS, (w_img + M_WIDEN + 2 * GAP_EXTRA + 40) * SS),
                      dtype=bool)
    # The A is reused as raster, shifted right to preserve the letter gap.
    a_shift = M_WIDEN + GAP_EXTRA
    a0, a1 = (abox[0] - 4) * SS, (abox[1] + 4) * SS
    canvas[:, a0 + a_shift * SS:a1 + a_shift * SS] = raster[:, a0:a1]

    img = Image.fromarray((canvas * 255).astype(np.uint8))
    d = ImageDraw.Draw(img)
    draw_dome_m(d, mbox[0], mbox[1] + M_WIDEN, y0, y1, STROKE, h_img * SS)
    patch = [(x + a_shift, y) for x, y in a_bar_square_patch(mask, abox)]
    d.polygon([(x * SS, y * SS) for x, y in patch], fill=255)
    letter_gap = abox[0] - mbox[1]
    x_sec = abox[1] + a_shift + letter_gap + GAP_EXTRA - SS_TIGHTEN
    right = draw_section(d, x_sec, y0, y1, STROKE)

    fg = round_corners(np.array(img) > 127, ROUND_R * SS)
    alpha_out = fade(downscale(fg, (canvas.shape[1] // SS, h_img)), y0, y1)
    width = right + mbox[0]  # right margin mirrors the left one
    colorize(alpha_out[:, :width], fill).save(f'{REPO}/{path}')
    return width, h_img


def gen_icon(path, fill=(254, 254, 254)):
    """Render the widened dome-M icon over the original icon's metrics."""
    original = load_original(path)
    mask = np.array(original)[..., 3] > 127
    ys, xs = np.where(mask)
    x0, x1 = int(xs.min()), int(xs.max()) + 1
    y0, y1 = int(ys.min()), int(ys.max()) + 1

    # Stroke width = the left stem's run length near the bottom.
    row = np.where(mask[int(y0 + 0.9 * (y1 - y0))])[0]
    stem = 1
    while stem < len(row) and row[stem] == row[stem - 1] + 1:
        stem += 1
    scale = stem / STROKE

    widened = (x1 - x0) + int(round(M_WIDEN * scale))
    x0 = (original.size[0] - widened) // 2
    img = Image.new('L', (original.size[0] * SS, original.size[1] * SS), 0)
    draw_dome_m(ImageDraw.Draw(img), x0, x0 + widened, y0, y1, float(stem),
                original.size[1] * SS)
    fg = round_corners(np.array(img) > 127, ROUND_R * scale * SS)
    alpha_out = fade(downscale(fg, original.size), y0, y1)
    colorize(alpha_out, fill).save(f'{REPO}/{path}')
    return original.size


def main():
    print('dark  ', gen_wordmark('web/public/MASS-Dark.png', (254, 254, 254)))
    print('light ', gen_wordmark('web/public/MASS-Light.png', (0, 0, 0)))
    print('icon  ', gen_icon('internal/icon/icon.png'))
    print('winres', gen_icon('winres/icon.png'))


if __name__ == '__main__':
    main()
