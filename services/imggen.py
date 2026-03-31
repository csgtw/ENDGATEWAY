"""
services/imggen.py
Génération d'images personnalisées par contact.
Port du script generate.py adapté au serveur Linux/Render.
Font : Roboto-Bold téléchargée au runtime si absente.
"""
import os
from datetime import date

import cv2
import numpy as np
from PIL import Image, ImageDraw, ImageFont

# ─── Chemins ──────────────────────────────────────────────────────────────────
_HERE      = os.path.dirname(os.path.abspath(__file__))
_APP_ROOT  = os.path.dirname(_HERE)
STATIC_DIR  = os.path.join(_APP_ROOT, "static")
UPLOADS_DIR = os.path.join(STATIC_DIR, "uploads")

FONT_BOLD_PATH    = os.path.join(STATIC_DIR, "Roboto-Bold.ttf")
FONT_REGULAR_PATH = os.path.join(STATIC_DIR, "Roboto-Regular.ttf")

_FONT_BOLD_URL    = "https://github.com/google/fonts/raw/main/apache/roboto/static/Roboto-Bold.ttf"
_FONT_REGULAR_URL = "https://github.com/google/fonts/raw/main/apache/roboto/static/Roboto-Regular.ttf"

# ─── Coins de la barre client — parallélogramme ───────────────────────────────
BAR_TL = (308, 588);  BAR_TR = (776, 665)
BAR_BR = (763, 776);  BAR_BL = (295, 699)

TEXT_RATIO = 0.72
WHITE  = (215, 225, 238)
W_FLAT = 480;  H_FLAT = 105
TEXT_W = int(W_FLAT * TEXT_RATIO)   # 346px
S = 3                                # facteur supersampling

_src_corners = [[0, 0], [W_FLAT, 0], [W_FLAT, H_FLAT], [0, H_FLAT]]
_dst_corners = [list(BAR_TL), list(BAR_TR), list(BAR_BR), list(BAR_BL)]

# ─── Champ "Date prévue" ──────────────────────────────────────────────────────
DATE_TL = (448, 809);  DATE_TR = (680, 848)
DATE_BR = (680, 870);  DATE_BL = (448, 831)

W_DATE = 232;  H_DATE = 22
DARK   = (40, 45, 50)
_src_date = [[0, 0], [W_DATE, 0], [W_DATE, H_DATE], [0, H_DATE]]
_dst_date  = [list(DATE_TL), list(DATE_TR), list(DATE_BR), list(DATE_BL)]


# ─── Font download ────────────────────────────────────────────────────────────
_fonts_ready = False


def _download_font(url: str, path: str):
    import requests
    os.makedirs(os.path.dirname(path), exist_ok=True)
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    with open(path, "wb") as fh:
        fh.write(r.content)


def _ensure_fonts():
    global _fonts_ready
    if _fonts_ready:
        return
    try:
        if not os.path.exists(FONT_BOLD_PATH):
            _download_font(_FONT_BOLD_URL, FONT_BOLD_PATH)
        if not os.path.exists(FONT_REGULAR_PATH):
            _download_font(_FONT_REGULAR_URL, FONT_REGULAR_PATH)
        _fonts_ready = True
    except Exception as e:
        try:
            from logger import log
            log(f"⚠️ imggen: téléchargement police échoué: {e}")
        except Exception:
            pass
        raise RuntimeError(f"Police introuvable et téléchargement échoué: {e}") from e


# ─── Génération ───────────────────────────────────────────────────────────────

def generate_image(names_text: str, template_path: str, output_path: str, seed: int = 1) -> str:
    """
    Génère une image personnalisée avec `names_text` sur la barre client.
    Retourne `output_path`.
    """
    _ensure_fonts()

    photo_orig = Image.open(template_path).convert("RGB")
    PW, PH = photo_orig.size

    # ── Warp date ─────────────────────────────────────────────────────────
    date_text = date.today().strftime("%d/%m/%Y")
    d_big = Image.new("RGBA", (W_DATE * S, H_DATE * S), (0, 0, 0, 0))
    dd    = ImageDraw.Draw(d_big)
    for ds in range(17 * S, 8, -1):
        dfont = ImageFont.truetype(FONT_REGULAR_PATH, ds)
        dbb   = dd.textbbox((0, 0), date_text, font=dfont)
        dtw, dth = dbb[2] - dbb[0], dbb[3] - dbb[1]
        if dtw <= W_DATE * S * 0.95 and dth <= H_DATE * S * 0.80:
            break
    dtx = 4 * S - dbb[0]
    dty = (H_DATE * S - dth) // 2 - dbb[1]
    dd.text((dtx, dty), date_text, fill=(*DARK, 255), font=dfont)

    date_flat = d_big.resize((W_DATE, H_DATE), Image.LANCZOS)
    darr = np.array(date_flat).astype(np.float32)
    darr[:, :, 3]  = cv2.GaussianBlur(darr[:, :, 3], (0, 0), 0.3)
    darr[:, :, :3] = cv2.GaussianBlur(darr[:, :, :3], (0, 0), 0.3)
    np.random.seed(0)
    noise_d = np.random.normal(0, 1.5, (H_DATE, W_DATE, 3))
    darr[:, :, :3] = np.clip(darr[:, :, :3] + noise_d * (darr[:, :, 3:4] / 255.0), 0, 255)
    date_np  = darr.astype(np.uint8)
    M_date   = cv2.getPerspectiveTransform(np.float32(_src_date), np.float32(_dst_date))
    dbgra    = cv2.cvtColor(date_np, cv2.COLOR_RGBA2BGRA)
    dwarped  = cv2.warpPerspective(dbgra, M_date, (PW, PH),
                                   flags=cv2.INTER_LANCZOS4,
                                   borderMode=cv2.BORDER_CONSTANT,
                                   borderValue=(0, 0, 0, 0))
    dwarped  = cv2.cvtColor(dwarped, cv2.COLOR_BGRA2RGBA)
    d_alpha  = dwarped[:, :, 3:4].astype(np.float32) / 255.0
    d_overlay = dwarped[:, :, :3].astype(np.float32)

    # ── Texte nom (3x supersampling) ──────────────────────────────────────
    flat_big = Image.new("RGBA", (W_FLAT * S, H_FLAT * S), (0, 0, 0, 0))
    dr = ImageDraw.Draw(flat_big)
    for size in range(35 * S, 8, -1):
        font = ImageFont.truetype(FONT_BOLD_PATH, size)
        bb   = dr.textbbox((0, 0), names_text, font=font)
        tw, th = bb[2] - bb[0], bb[3] - bb[1]
        if tw <= TEXT_W * S * 0.90 and th <= H_FLAT * S * 0.70:
            break
    tx = (TEXT_W * S - tw) // 2 - bb[0]
    ty = (H_FLAT * S - th) // 2 - bb[1]
    dr.text((tx, ty), names_text, fill=(*WHITE, 255), font=font)

    flat = flat_big.resize((W_FLAT, H_FLAT), Image.LANCZOS)
    arr  = np.array(flat).astype(np.float32)

    arr[:, :, 3] = cv2.GaussianBlur(arr[:, :, 3], (0, 0), 0.7)
    a    = arr[:, :, 3] / 255.0
    glow = cv2.GaussianBlur(a, (0, 0), 1.5) * 0.22
    arr[:, :, 3] = np.clip(arr[:, :, 3] + glow * (1.0 - a) * 255.0, 0, 255)
    arr[:, :, :3] = cv2.GaussianBlur(arr[:, :, :3], (0, 0), 0.55)

    np.random.seed(seed)
    noise = np.random.normal(0, 3, (H_FLAT, W_FLAT, 3))
    arr[:, :, :3] = np.clip(arr[:, :, :3] + noise * (arr[:, :, 3:4] / 255.0), 0, 255)
    flat_np = arr.astype(np.uint8)

    # ── Warp barre ────────────────────────────────────────────────────────
    M      = cv2.getPerspectiveTransform(np.float32(_src_corners), np.float32(_dst_corners))
    bgra   = cv2.cvtColor(flat_np, cv2.COLOR_RGBA2BGRA)
    warped = cv2.warpPerspective(bgra, M, (PW, PH),
                                  flags=cv2.INTER_LANCZOS4,
                                  borderMode=cv2.BORDER_CONSTANT,
                                  borderValue=(0, 0, 0, 0))
    warped = cv2.cvtColor(warped, cv2.COLOR_BGRA2RGBA)

    # ── Composite nom ─────────────────────────────────────────────────────
    alpha   = warped[:, :, 3:4].astype(np.float32) / 255.0
    overlay = warped[:, :, :3].astype(np.float32)
    base    = np.array(photo_orig).astype(np.float32)
    result  = (overlay * alpha + base * (1.0 - alpha)).astype(np.uint8)

    # ── Composite date ────────────────────────────────────────────────────
    result = (d_overlay * d_alpha + result.astype(np.float32) * (1.0 - d_alpha)).astype(np.uint8)

    # ── Flou global final ─────────────────────────────────────────────────
    result = cv2.GaussianBlur(result, (0, 0), 0.65)

    out_abs = os.path.abspath(output_path)
    os.makedirs(os.path.dirname(out_abs), exist_ok=True)
    Image.fromarray(result).save(out_abs, quality=97)
    return output_path
