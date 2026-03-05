from io import BytesIO
from urllib.parse import urlparse, parse_qs
from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageColor
import requests
import templateVars

# -----------------------------
# Helper functions
# -----------------------------
def get_color(entry, won: bool):
    if "win_color" in entry and "lose_color" in entry:
        return entry["win_color"] if won else entry["lose_color"]
    return entry.get("color", "#000000")

def draw_text(draw, text, entry, won=True, x_offset=0, y_offset=0):
    font = ImageFont.truetype(entry["font"], entry["font_size"])
    color = get_color(entry, won)
    x, y = entry["x"] + x_offset, entry["y"] + y_offset
    anchor = entry.get("anchor", "mm")
    if "stroke" in entry:
        draw.text(
            (x, y),
            text,
            font=font,
            fill=color,
            anchor=anchor,
            stroke_width=entry.get("stroke", 0),
            stroke_fill=entry.get("stroke_color", color),
            alpha=entry.get("alpha", 255)
        )
        return
    draw.text((x, y), text, font=font, fill=color, anchor=anchor, alpha=entry.get("alpha", 255))

def draw_box(draw, entry, won=True, x_offset=0, y_offset=0):
    x, y = entry["x"] + x_offset, entry["y"] + y_offset
    w, h = entry["width"], entry["height"]
    fill = entry.get("fill")
    outline = get_color(entry, won)
    stroke = entry.get("stroke", 1)
    draw.rectangle([x, y, x + w, y + h], fill=fill, outline=outline, width=stroke)

def draw_glow(draw, img, draw_func, *args, glow_color="#ffffff", blur_radius=6, offset_range=4, **kwargs):
    glow_layer = Image.new("RGBA", img.size, (0, 0, 0, 0))
    glow_draw = ImageDraw.Draw(glow_layer)
    draw_func(glow_draw, *args, **kwargs)

    # Multi-pass blur for natural falloff
    small_blur = glow_layer.filter(ImageFilter.GaussianBlur(blur_radius * 0.6))
    medium_blur = glow_layer.filter(ImageFilter.GaussianBlur(blur_radius))
    large_blur = glow_layer.filter(ImageFilter.GaussianBlur(blur_radius * 1.8))

    combined = Image.alpha_composite(large_blur, medium_blur)
    combined = Image.alpha_composite(combined, small_blur)

    r, g, b = ImageColor.getrgb(glow_color)
    tinted = Image.new("RGBA", img.size, (r, g, b, 0))
    tinted.putalpha(combined.split()[3])
    alpha = tinted.split()[3].point(lambda p: int(p * 0.6))
    tinted.putalpha(alpha)

    img.alpha_composite(tinted)
    draw_func(draw, *args, **kwargs)

def fetch_image(url):
    if not url:
        raise ValueError("Empty image URL")
    # Convert Google Drive share link to direct download
    if "drive.google.com" in url:
        parsed = urlparse(url)
        file_id = None
        if "/d/" in parsed.path:
            file_id = parsed.path.split("/d/")[1].split("/")[0]
        elif "id=" in parsed.query:
            file_id = parse_qs(parsed.query).get("id", [None])[0]
        if file_id:
            url = f"https://drive.google.com/uc?export=download&id={file_id}"
        else:
            raise ValueError(f"Cannot parse Google Drive URL: {url}")
    resp = requests.get(url)
    resp.raise_for_status()
    return Image.open(BytesIO(resp.content)).convert("RGBA")

def draw_logo_height_fit(img, logo_path, box_entry):
    logo = Image.open(logo_path).convert("RGBA")
    x, y, w, h = box_entry["x"], box_entry["y"], box_entry["width"], box_entry["height"]
    scale_w = w / logo.width
    scale_h = h / logo.height
    scale = max(scale_w, scale_h)
    new_width = int(logo.width * scale)
    new_height = int(logo.height * scale)
    logo_resized = logo.resize((new_width, new_height), Image.LANCZOS)
    left = (new_width - w) // 2
    top = (new_height - h) // 2
    right = left + w
    bottom = top + h
    logo_cropped = logo_resized.crop((left, top, right, bottom))
    img.paste(logo_cropped, (x, y), logo_cropped)

# -----------------------------
# Main render function
# -----------------------------
def render_image(
    output_path,
    home_won: bool,
    title_text: str = "Title",
    caption_text: str = "Caption",
    home_score: int = 0,
    away_score: int = 0,
    home_rank: str = "",
    away_rank: str = "",
    home_record: str = "",
    away_record: str = "",
    home_team: str = "",
    away_team: str = "",
    photo_text: str = "",
    template_png: str = "graphic.png",
    background_image: str = None
):
    template_img = Image.open(template_png).convert("RGBA")
    doc_width, doc_height = template_img.size

    # Load background image if provided
    if background_image:
        try:
            # fetch_image handles Google Drive URLs and requests
            bg = fetch_image(background_image)

            # Scale to cover template
            scale_w = doc_width / bg.width
            scale_h = doc_height / bg.height
            scale = max(scale_w, scale_h)

            bg_resized = bg.resize((int(bg.width*scale), int(bg.height*scale)), Image.LANCZOS)

            # Crop to template size
            left = (bg_resized.width - doc_width) // 2
            top = (bg_resized.height - doc_height) // 2
            bg_cropped = bg_resized.crop((left, top, left + doc_width, top + doc_height))

            img = bg_cropped

        except Exception as e:
            print(f"Failed to load background image: {e}")
            img = Image.new("RGBA", (doc_width, doc_height), (0, 0, 0, 0))
    else:
        img = Image.new("RGBA", (doc_width, doc_height), (0, 0, 0, 0))

    draw = ImageDraw.Draw(img)
    img.alpha_composite(template_img)
    away_won = not home_won

    # -----------------------------
    # Draw home team elements
    # -----------------------------
    draw_box(draw, templateVars.template["home_score_box"], home_won)
    draw_glow(draw, img, draw_box, templateVars.template["home_score_box"], home_won, glow_color="#f1f1f1" if home_won else "#E60054", blur_radius=10)
    draw_box(draw, templateVars.template["home_rank_box"], home_won)
    draw_glow(draw, img, draw_box, templateVars.template["home_rank_box"], home_won, glow_color="#f1f1f1" if home_won else "#E60054", blur_radius=15)
    draw_box(draw, templateVars.template["home_record_box"], home_won)
    draw_glow(draw, img, draw_box, templateVars.template["home_record_box"], home_won, glow_color="#f1f1f1" if home_won else "#E60054", blur_radius=15)
    draw_logo_height_fit(img, f"assets/logos/{home_team.replace(' ', '_')}.jpg", templateVars.template["home_logo_box"])
    draw_box(draw, templateVars.template["home_logo_box"], home_won)
    draw_glow(draw, img, draw_box, templateVars.template["home_logo_box"], home_won, glow_color="#f1f1f1" if home_won else "#E60054", blur_radius=15)

    draw_text(draw, str(home_score), templateVars.template["home_score_text"], home_won)
    draw_glow(draw, img, draw_text, str(home_score), templateVars.template["home_score_text"], home_won, glow_color="#f1f1f1" if home_won else "#E60054")
    draw_text(draw, str(home_rank), templateVars.template["home_rank_text"], home_won)
    draw_glow(draw, img, draw_text, str(home_rank), templateVars.template["home_rank_text"], home_won, glow_color="#f1f1f1" if home_won else "#E60054")
    draw_text(draw, str(home_record), templateVars.template["home_record_text"], home_won)
    draw_glow(draw, img, draw_text, str(home_record), templateVars.template["home_record_text"], home_won, glow_color="#f1f1f1" if home_won else "#E60054")

    # -----------------------------
    # Draw away team elements
    # -----------------------------
    draw_box(draw, templateVars.template["away_score_box"], away_won)
    draw_glow(draw, img, draw_box, templateVars.template["away_score_box"], away_won, glow_color="#f1f1f1" if away_won else "#E60054", blur_radius=10)
    draw_box(draw, templateVars.template["away_rank_box"], away_won)
    draw_glow(draw, img, draw_box, templateVars.template["away_rank_box"], away_won, glow_color="#f1f1f1" if away_won else "#E60054", blur_radius=15)
    draw_box(draw, templateVars.template["away_record_box"], away_won)
    draw_glow(draw, img, draw_box, templateVars.template["away_record_box"], away_won, glow_color="#f1f1f1" if away_won else "#E60054", blur_radius=15)
    draw_logo_height_fit(img, f"assets/logos/{away_team.replace(' ', '_')}.jpg", templateVars.template["away_logo_box"])
    draw_box(draw, templateVars.template["away_logo_box"], away_won)
    draw_glow(draw, img, draw_box, templateVars.template["away_logo_box"], away_won, glow_color="#f1f1f1" if away_won else "#E60054", blur_radius=15)

    draw_text(draw, str(away_score), templateVars.template["away_score_text"], away_won)
    draw_glow(draw, img, draw_text, str(away_score), templateVars.template["away_score_text"], away_won, glow_color="#f1f1f1" if away_won else "#E60054")
    draw_text(draw, str(away_rank), templateVars.template["away_rank_text"], away_won)
    draw_glow(draw, img, draw_text, str(away_rank), templateVars.template["away_rank_text"], away_won, glow_color="#f1f1f1" if away_won else "#E60054")
    draw_text(draw, str(away_record), templateVars.template["away_record_text"], away_won)
    draw_glow(draw, img, draw_text, str(away_record), templateVars.template["away_record_text"], away_won, glow_color="#f1f1f1" if away_won else "#E60054")

    # -----------------------------
    # Draw header / caption / photo text
    # -----------------------------
    draw_text(draw, photo_text, templateVars.template["photo_text"], home_won)
    draw_text(draw, title_text, templateVars.template["header_text"], home_won)
    draw_text(draw, caption_text, templateVars.template["caption_text"], home_won)
    draw_glow(draw, img, draw_text, caption_text, templateVars.template["caption_text"], home_won, glow_color="#f1f1f1")

    # -----------------------------
    # Save final image
    # -----------------------------
    img.save(output_path)
    print(f"Rendered image saved to {output_path}")