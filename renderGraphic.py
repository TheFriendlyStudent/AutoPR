from io import BytesIO
import os

from PIL import Image, ImageDraw, ImageFont, ImageFilter
import requests
import templateVars

# -----------------------------
# Helper functions
# -----------------------------
def get_color(entry, won: bool):
    if "win_color" in entry and "lose_color" in entry:
        return entry["win_color"] if won else entry["lose_color"]
    return entry.get("color", "#000000")

def draw_text(draw, text, entry, won=True):
    font = ImageFont.truetype(entry["font"], entry["font_size"])
    color = get_color(entry, won)
    x, y = entry["x"], entry["y"]
    anchor = entry.get("anchor", "mm")
    draw.text((x, y), text, font=font, fill=color, anchor=anchor)

def draw_box(draw, entry, won=True):
    x, y = entry["x"], entry["y"]
    w, h = entry["width"], entry["height"]
    fill = entry.get("fill")
    outline = get_color(entry, won)
    stroke = entry.get("stroke", 1)
    draw.rectangle([x, y, x + w, y + h], fill=fill, outline=outline, width=stroke)

def draw_glow(draw, img, draw_func, *args, glow_color=None, blur_radius=6, offset_range=4, **kwargs):
    if glow_color is None:
        glow_color = (255, 255, 255)

    # Create transparent glow layer
    glow_layer = Image.new("RGBA", img.size, (0, 0, 0, 0))
    glow_draw = ImageDraw.Draw(glow_layer)

    # Draw solid white base mask
    draw_func(
        glow_draw,
        *args,
        **kwargs
    )

    # Multi-pass blur for natural falloff
    small_blur = glow_layer.filter(ImageFilter.GaussianBlur(blur_radius * 0.6))
    medium_blur = glow_layer.filter(ImageFilter.GaussianBlur(blur_radius))
    large_blur = glow_layer.filter(ImageFilter.GaussianBlur(blur_radius * 1.8))

    # Combine layers
    combined = Image.alpha_composite(large_blur, medium_blur)
    combined = Image.alpha_composite(combined, small_blur)

    # Tint the glow
    from PIL import ImageColor
    r, g, b = ImageColor.getrgb(glow_color)

    tinted = Image.new("RGBA", img.size, (r, g, b, 0))
    tinted.putalpha(combined.split()[3])

    # Slightly reduce opacity for realism
    alpha = tinted.split()[3].point(lambda p: int(p * 0.6))
    tinted.putalpha(alpha)

    # Composite under main image
    img.alpha_composite(tinted)

    # Draw the real element on top
    draw_func(draw, *args, **kwargs)

def draw_box(draw, entry, won=True, x_offset=0, y_offset=0):
    x, y = entry["x"] + x_offset, entry["y"] + y_offset
    w, h = entry["width"], entry["height"]
    fill = entry.get("fill")
    outline = get_color(entry, won)
    stroke = entry.get("stroke", 1)
    draw.rectangle([x, y, x + w, y + h], fill=fill, outline=outline, width=stroke)

def draw_text(draw, text, entry, won=True, x_offset=0, y_offset=0):
    font = ImageFont.truetype(entry["font"], entry["font_size"])
    color = get_color(entry, won)
    x, y = entry["x"] + x_offset, entry["y"] + y_offset
    anchor = entry.get("anchor", "mm")
    if "stroke" in entry:
        draw.text((x, y), text, font=font, fill=color, anchor=anchor,
                  stroke_width=entry["stroke"], stroke_fill=entry["stroke_color"], alpha=entry["alpha"])
        return
    draw.text((x, y), text, font=font, fill=color, anchor=anchor, alpha=entry["alpha"])

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
    background_image: str = None  # NEW: optional background image
):
    # Load template
    template_img = Image.open(template_png).convert("RGBA")
    doc_width, doc_height = template_img.size

    # If a background image is provided, resize/crop to fill document
    if background_image:
        response = requests.get(background_image)
        response.raise_for_status()
        bg = Image.open(BytesIO(response.content)).convert("RGBA")

        scale_w = doc_width / bg.width
        scale_h = doc_height / bg.height
        scale = max(scale_w, scale_h)  # cover entire area

        new_width = int(bg.width * scale)
        new_height = int(bg.height * scale)
        bg_resized = bg.resize((new_width, new_height), Image.LANCZOS)

        # Crop centered
        left = (new_width - doc_width) // 2
        top = (new_height - doc_height) // 2
        right = left + doc_width
        bottom = top + doc_height
        bg_cropped = bg_resized.crop((left, top, right, bottom))

        # Start with the background
        img = bg_cropped
    else:
        img = Image.new("RGBA", (doc_width, doc_height), (0,0,0,0))

    draw = ImageDraw.Draw(img)

    # Overlay the template on top of background
    img.alpha_composite(template_img)

    # Determine winner for away team
    away_won = not home_won

    # -----------------------------
    # Draw helper: logos fully fill boxes
    # -----------------------------
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
    # Draw home elements
    # -----------------------------
    draw_box(draw, templateVars.template["home_score_box"], home_won)
    draw_glow(draw, img, draw_box, templateVars.template["home_score_box"], home_won, glow_color="#f1f1f1" if home_won else "#E60054", blur_radius=10, offset_range=4)
    draw_box(draw, templateVars.template["home_rank_box"], home_won)
    draw_glow(draw, img, draw_box, templateVars.template["home_rank_box"], home_won, glow_color="#f1f1f1" if home_won else "#E60054", blur_radius=15, offset_range=6)
    draw_box(draw, templateVars.template["home_record_box"], home_won)
    draw_glow(draw, img, draw_box, templateVars.template["home_record_box"], home_won, glow_color="#f1f1f1" if home_won else "#E60054", blur_radius=15, offset_range=6)
    draw_logo_height_fit(img, f"assets/logos/{home_team.replace(' ', '_')}.jpg", templateVars.template["home_logo_box"])
    draw_box(draw, templateVars.template["home_logo_box"], home_won)
    draw_glow(draw, img, draw_box, templateVars.template["home_logo_box"], home_won, glow_color="#f1f1f1" if home_won else "#E60054", blur_radius=15, offset_range=6)
    draw_text(draw, str(home_score), templateVars.template["home_score_text"], home_won)
    draw_glow(draw, img, draw_text, str(home_score), templateVars.template["home_score_text"], home_won, glow_color="#f1f1f1" if home_won else "#E60054", blur_radius=5, offset_range=3)
    draw_text(draw, str(home_rank), templateVars.template["home_rank_text"], home_won)
    draw_glow(draw, img, draw_text, str(home_rank), templateVars.template["home_rank_text"], home_won, glow_color="#f1f1f1" if home_won else "#E60054", blur_radius=5, offset_range=3)
    draw_text(draw, str(home_record), templateVars.template["home_record_text"], home_won)
    draw_glow(draw, img, draw_text, str(home_record), templateVars.template["home_record_text"], home_won, glow_color="#f1f1f1" if home_won else "#E60054", blur_radius=5, offset_range=3)

    # -----------------------------
    # Draw away elements
    # -----------------------------
    draw_box(draw, templateVars.template["away_score_box"], away_won)
    draw_glow(draw, img, draw_box, templateVars.template["away_score_box"], away_won, glow_color="#f1f1f1" if away_won else "#E60054", blur_radius=10, offset_range=4)
    draw_box(draw, templateVars.template["away_rank_box"], away_won)
    draw_glow(draw, img, draw_box, templateVars.template["away_rank_box"], away_won, glow_color="#f1f1f1" if away_won else "#E60054", blur_radius=15, offset_range=6)
    draw_box(draw, templateVars.template["away_record_box"], away_won)
    draw_glow(draw, img, draw_box, templateVars.template["away_record_box"], away_won, glow_color="#f1f1f1" if away_won else "#E60054", blur_radius=15, offset_range=6)
    draw_logo_height_fit(img, f"assets/logos/{away_team.replace(' ', '_')}.jpg", templateVars.template["away_logo_box"])
    draw_box(draw, templateVars.template["away_logo_box"], away_won)
    draw_glow(draw, img, draw_box, templateVars.template["away_logo_box"], away_won, glow_color="#f1f1f1" if away_won else "#E60054", blur_radius=15, offset_range=6)
    draw_text(draw, str(away_score), templateVars.template["away_score_text"], away_won)
    draw_glow(draw, img, draw_text, str(away_score), templateVars.template["away_score_text"], away_won, glow_color="#f1f1f1" if away_won else "#E60054", blur_radius=5, offset_range=3)
    draw_text(draw, str(away_rank), templateVars.template["away_rank_text"], away_won)
    draw_glow(draw, img, draw_text, str(away_rank), templateVars.template["away_rank_text"], away_won, glow_color="#f1f1f1" if away_won else "#E60054", blur_radius=5, offset_range=3)
    draw_text(draw, str(away_record), templateVars.template["away_record_text"], away_won)
    draw_glow(draw, img, draw_text, str(away_record), templateVars.template["away_record_text"], away_won, glow_color="#f1f1f1" if away_won else "#E60054", blur_radius=5, offset_range=3)

    draw_text(draw, photo_text, templateVars.template["photo_text"], home_won)
    draw_text(draw, title_text, templateVars.template["header_text"], home_won)
    draw_text(draw, caption_text, templateVars.template["caption_text"], home_won)
    draw_glow(draw, img, draw_text, str(caption_text), templateVars.template["caption_text"], home_won, glow_color="#f1f1f1", blur_radius=6, offset_range=3)

    # -----------------------------
    # Save final image
    # -----------------------------
    img.save(output_path)
    print(f"Rendered image saved to {output_path}")