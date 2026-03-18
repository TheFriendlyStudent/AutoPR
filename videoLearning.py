"""
videoLearning.py — Nutmeg Sports AI Scouting Engine v3
=======================================================
Input sources now supported:
  1. FILE UPLOAD   — browser POSTs a video to /api/upload_clip
                     Returns a session_id; frontend uses that everywhere.
  2. YOUTUBE LIVE  — POST /api/start_live_session with a YouTube URL.
                     yt-dlp resolves the HLS URL; FFmpeg captures rolling
                     10-second segments; each is analyzed automatically.
  3. LOCAL PATH    — legacy; file must exist on the server.

New endpoints:
  POST   /api/upload_clip              multipart video → {session_id, preview_url}
  POST   /api/start_live_session       start YouTube live buffer → {session_id}
  GET    /api/live_status/{sid}        poll for latest live report
  DELETE /api/stop_live/{sid}          stop buffer thread + cleanup

Unchanged endpoints:
  POST   /api/analyze_playhead         (now accepts session_id as video_source)
  POST   /api/scan_viral               (same)
  GET    /api/get_cached_timeline
  DELETE /api/clear_cache
  GET    /health                       (now reports ytdlp, active sessions)
"""

import os, time, subprocess, glob, math, json, re, shutil, uuid, threading, collections
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional, List, Dict

import numpy as np
from google import genai
from ultralytics import YOLO
from dotenv import load_dotenv
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import uvicorn

# ── Optional ngrok ─────────────────────────────────────────────────────────────
try:
    from pyngrok import ngrok as _ngrok
    _NGROK_AVAILABLE = True
except ImportError:
    _NGROK_AVAILABLE = False

load_dotenv()
client = genai.Client(api_key=os.environ.get("GOOGLE_API_KEY"))

# ── FFmpeg (Windows path; ignored on Linux/Mac) ────────────────────────────────
_ffmpeg_win = (
    r"C:\Users\vasub\AppData\Local\Microsoft\WinGet\Packages"
    r"\Gyan.FFmpeg_Microsoft.Winget.Source_8wekyb3d8bbwe\ffmpeg-8.1-full_build\bin"
)
if os.path.exists(_ffmpeg_win):
    os.environ["PATH"] += os.pathsep + _ffmpeg_win

# ── GPU detection ──────────────────────────────────────────────────────────────
try:
    import torch
    DEVICE = "0" if torch.cuda.is_available() else "cpu"
    HALF   = torch.cuda.is_available()
    CODEC  = "h264_nvenc" if torch.cuda.is_available() else "libx264"
except ImportError:
    DEVICE = "cpu"; HALF = False; CODEC = "libx264"

print(f"[AI Engine] Device: {'GPU ' + DEVICE if DEVICE != 'cpu' else 'CPU'} | Codec: {CODEC}")

# ── Directories ────────────────────────────────────────────────────────────────
for _d in ["cache_clips", "cache_reports", "uploads", "live_buffers"]:
    os.makedirs(_d, exist_ok=True)

# ── App ────────────────────────────────────────────────────────────────────────
app = FastAPI(title="Nutmeg Sports AI Scouting Engine")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.mount("/uploads", StaticFiles(directory="uploads"), name="uploads")

executor = ThreadPoolExecutor(max_workers=3)

# session_id → {thread, stop_event, results, status, latest, error}
live_sessions: dict = {}

MAX_UPLOAD_MB = int(os.environ.get("MAX_UPLOAD_MB", "2000"))


# ═══════════════════════════════════════════════════════════════════════════════
# REQUEST MODELS
# ═══════════════════════════════════════════════════════════════════════════════
class PlayheadRequest(BaseModel):
    video_source: str   # session_id UUID, or local file path
    current_time: float

class ViralScanRequest(BaseModel):
    video_source: str
    sensitivity:  float = 0.65

class LiveSessionRequest(BaseModel):
    youtube_url:  str
    auto_analyze: bool = True


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
def _is_uuid(s: str) -> bool:
    try:
        uuid.UUID(s); return True
    except ValueError:
        return False


def resolve_source(video_source: str) -> Optional[str]:
    """Map session_id or local path → real file path, or None."""
    if _is_uuid(video_source):
        matches = glob.glob(f"uploads/{video_source}.*")
        return matches[0] if matches else None
    if os.path.exists(video_source):
        return video_source
    return None


def _verify_ytdlp() -> bool:
    try:
        r = subprocess.run(["yt-dlp", "--version"], capture_output=True, timeout=5)
        return r.returncode == 0
    except FileNotFoundError:
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# AUDIO ENERGY — cheap excitement pre-filter + dead-time detection
# ═══════════════════════════════════════════════════════════════════════════════
def get_audio_energy_profile(video_path: str, chunk_sec: int = 5) -> list:
    cmd = ["ffmpeg", "-y", "-i", video_path,
           "-vn", "-ac", "1", "-ar", "16000", "-f", "s16le", "pipe:1"]
    try:
        proc  = subprocess.run(cmd, capture_output=True, timeout=300)
        audio = np.frombuffer(proc.stdout, dtype=np.int16).astype(np.float32)
    except Exception as e:
        print(f"[Audio] {e}"); return []
    rate, chunk, result = 16000, 16000 * chunk_sec, []
    for i in range(0, len(audio), chunk):
        seg = audio[i:i+chunk]
        if len(seg):
            result.append({"ts": round(i / rate, 1),
                           "energy": float(np.sqrt(np.mean(seg**2)))})
    return result


def detect_excitement_spikes(profile: list, sensitivity: float) -> list:
    if not profile: return []
    energies  = np.array([p["energy"] for p in profile])
    threshold = np.percentile(energies, sensitivity * 100)
    merged, last = [], -999
    for p in sorted(profile, key=lambda x: x["ts"]):
        if p["energy"] >= threshold and p["ts"] - last > 15:
            merged.append(p["ts"]); last = p["ts"]
    return merged


# ── Dead-time detection ────────────────────────────────────────────────────────
# Uses two fast heuristics on a raw clip (runs in ~0.2s, before GPU/Gemini):
#   1. Audio RMS below floor → near-silence (ads, music bed, break)
#   2. Frame motion below floor → static camera / graphic overlay
# Requiring both avoids misfires on quiet but active plays.
# Thresholds can be tuned via .env without code changes.

DEAD_TIME_AUDIO_FLOOR  = float(os.environ.get("DEAD_TIME_AUDIO_FLOOR",  "200"))
DEAD_TIME_MOTION_FLOOR = float(os.environ.get("DEAD_TIME_MOTION_FLOOR", "0.8"))


def _clip_audio_rms(clip_path: str) -> float:
    """Return mean RMS of clip audio. Takes ~0.1s."""
    cmd = ["ffmpeg", "-y", "-i", clip_path,
           "-vn", "-ac", "1", "-ar", "8000", "-t", "10",
           "-f", "s16le", "pipe:1"]
    try:
        proc  = subprocess.run(cmd, capture_output=True, timeout=15)
        audio = np.frombuffer(proc.stdout, dtype=np.int16).astype(np.float32)
        return float(np.sqrt(np.mean(audio**2))) if len(audio) else 0.0
    except Exception:
        return 9999.0  # assume active on error


def _clip_motion_score(clip_path: str) -> float:
    """
    Uses FFmpeg freezedetect filter — no cv2 needed.
    Returns 0.0 if frozen/static, 9999.0 if motion detected.
    Takes ~0.1s.
    """
    try:
        proc = subprocess.run([
            "ffmpeg", "-i", clip_path,
            "-vf", "freezedetect=n=0.003:d=3",
            "-f", "null", "-"
        ], capture_output=True, text=True, timeout=15)
        return 0.0 if "freeze_start" in proc.stderr else 9999.0
    except Exception:
        return 9999.0


def is_dead_time(clip_path: str) -> tuple:
    """
    Returns (True, reason_str) when clip looks like dead broadcast time:
    halftime, ad break, intermission, frozen/static graphic, muted feed.
    Returns (False, "") for normal live play.
    """
    rms    = _clip_audio_rms(clip_path)
    motion = _clip_motion_score(clip_path)
    print(f"[DeadTime] rms={rms:.1f}  motion={motion:.2f}")

    # Both quiet AND static → almost certainly a break
    if rms < DEAD_TIME_AUDIO_FLOOR and motion < DEAD_TIME_MOTION_FLOOR:
        return True, f"quiet (rms={rms:.0f}) + static (motion={motion:.2f})"
    # Very deep silence alone → muted ad or black screen
    if rms < DEAD_TIME_AUDIO_FLOOR * 0.3:
        return True, f"deep silence (rms={rms:.0f})"
    # Completely frozen → loss of signal or graphic card
    if motion < DEAD_TIME_MOTION_FLOOR * 0.2:
        return True, f"frozen frame (motion={motion:.2f})"
    return False, ""


# ═══════════════════════════════════════════════════════════════════════════════
# YOLO METRICS — calibrated tracking, speed, zone occupancy, shot detection
# ═══════════════════════════════════════════════════════════════════════════════

COURT_W_FT = 94.0
COURT_H_FT = 50.0

# Tunable via .env
YOLO_CONF          = float(os.environ.get("YOLO_CONF",           "0.45"))
MIN_PLAYER_HEIGHT  = float(os.environ.get("MIN_PLAYER_HEIGHT",   "0.10"))  # % of frame height
SHOT_RISE_PX       = float(os.environ.get("SHOT_RISE_PX",        "40"))    # px wrist must rise
SHOT_RISE_FRAMES   = int(os.environ.get("SHOT_RISE_FRAMES",      "5"))     # over N frames
# Speed smoothing: rolling window to kill single-frame noise
SPEED_SMOOTH_WIN   = int(os.environ.get("SPEED_SMOOTH_WIN",      "5"))

COURT_ZONES = {
    "paint_left":  (0.00, 0.19, 0.28, 0.72),
    "paint_right": (0.81, 1.00, 0.28, 0.72),
    "mid_range":   (0.19, 0.40, 0.00, 1.00),
    "perimeter":   (0.40, 0.60, 0.00, 1.00),
    "three_left":  (0.00, 0.19, 0.00, 0.28),
    "three_right": (0.81, 1.00, 0.72, 1.00),
    "backcourt":   (0.60, 1.00, 0.00, 1.00),
}


def _zone_for_point(x_pct: float, y_pct: float) -> str:
    for name, (x0, x1, y0, y1) in COURT_ZONES.items():
        if x0 <= x_pct <= x1 and y0 <= y_pct <= y1:
            return name
    return "perimeter"


def calibrate_px_per_ft(clip_path: str, w_px: int) -> float:
    """
    Attempt to measure pixels-per-foot from the free-throw lane lines
    using FFmpeg frame extraction + NumPy edge detection.
    The lane is exactly 12 ft wide — a reliable reference in any gym.
    Falls back to the 70%-court heuristic if detection fails.
    """
    # Extract one frame as raw RGB via FFmpeg pipe (no cv2 needed)
    cmd = [
        "ffmpeg", "-i", clip_path, "-vframes", "1",
        "-f", "rawvideo", "-pix_fmt", "gray", "pipe:1"
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, timeout=10)
        if not proc.stdout:
            raise ValueError("empty frame")

        # Infer height from byte count
        total_px = len(proc.stdout)
        h_px_est = total_px // w_px
        if h_px_est < 100:
            raise ValueError("bad dimensions")

        gray = np.frombuffer(proc.stdout[:w_px * h_px_est],
                             dtype=np.uint8).reshape(h_px_est, w_px).astype(np.float32)

        # Simple Sobel-like vertical edge detection (no cv2)
        # Kernel: [-1, 0, 1] applied horizontally
        kernel = np.array([-1, 0, 1], dtype=np.float32)
        edges  = np.abs(np.apply_along_axis(
            lambda row: np.convolve(row, kernel, mode="same"), 1, gray
        ))

        # Sum edges vertically → column profile
        col_profile = edges.sum(axis=0)

        # Find the two strongest vertical edge columns
        # that are plausibly lane-width apart (8–20% of frame width)
        min_gap = int(w_px * 0.08)
        max_gap = int(w_px * 0.22)

        # Get top-N column peaks
        threshold = np.percentile(col_profile, 90)
        peak_cols = np.where(col_profile > threshold)[0]

        best_gap = None
        for i in range(len(peak_cols)):
            for j in range(i + 1, len(peak_cols)):
                gap = int(peak_cols[j]) - int(peak_cols[i])
                if min_gap <= gap <= max_gap:
                    best_gap = gap
                    break
            if best_gap:
                break

        if best_gap:
            px_per_ft = best_gap / 12.0  # lane = 12 ft
            print(f"[Scale] Calibrated: {px_per_ft:.2f} px/ft "
                  f"(lane gap={best_gap}px)")
            return px_per_ft

    except Exception as e:
        print(f"[Scale] Calibration failed ({e}), using 70% heuristic")

    # Fallback
    return w_px / (COURT_W_FT * 0.70)


def _detect_shot(kp_history: list) -> bool:
    """
    Shot heuristic requiring ALL of:
      1. Right wrist rises > SHOT_RISE_PX pixels over SHOT_RISE_FRAMES frames
      2. Wrist ends above right shoulder (follow-through position)
      3. Both wrist and shoulder keypoints are confidently detected (non-zero)

    COCO keypoint indices:
      5=L shoulder, 6=R shoulder, 9=L wrist, 10=R wrist
    """
    N = SHOT_RISE_FRAMES
    if len(kp_history) < N + 1:
        return False

    for j in range(N, len(kp_history)):
        kp_now  = kp_history[j]      # shape [17, 2]
        kp_prev = kp_history[j - N]

        r_wrist_now   = kp_now[10]    # [x, y]
        r_wrist_prev  = kp_prev[10]
        r_shoulder_now = kp_now[6]

        # Skip if any keypoint not detected (zero coords)
        if (r_wrist_now[0] == 0 or r_wrist_prev[0] == 0
                or r_shoulder_now[0] == 0):
            continue

        # y decreases upward in image coords
        wrist_rose     = r_wrist_prev[1] - r_wrist_now[1] > SHOT_RISE_PX
        above_shoulder = r_wrist_now[1] < r_shoulder_now[1]

        if wrist_rose and above_shoulder:
            return True
    return False


def _smooth_speeds(speeds: list, window: int) -> list:
    """Rolling mean to eliminate single-frame tracking noise."""
    if len(speeds) < window:
        return speeds
    return [float(np.mean(speeds[max(0, i-window):i+1]))
            for i in range(len(speeds))]


def extract_yolo_metrics(clip_path: str, fps: float = 30.0) -> dict:
    """
    Run YOLO+ByteTrack with calibrated scale, height filtering,
    smoothed speeds, and improved shot detection.
    Returns structured metrics dict + Gemini-ready summary string.
    """
    model = YOLO("yolo11m-pose.pt")

    results_list = list(model.track(
        source=clip_path,
        tracker="bytetrack.yaml",
        device=DEVICE,
        stream=True,
        conf=YOLO_CONF,   # 0.45 — filters spectators, refs far away
        iou=0.5,
        classes=[0],       # person class only
        half=HALF,
        imgsz=640,
        vid_stride=2,
        persist=True,
        verbose=False,
    ))

    if not results_list:
        return {"metrics": {}, "summary": "No tracking data available."}

    h_px = results_list[0].orig_shape[0]
    w_px = results_list[0].orig_shape[1]

    # ── Calibrated scale ───────────────────────────────────────────────────
    px_per_ft = calibrate_px_per_ft(clip_path, w_px)

    # Per-track state
    tracks: Dict[int, dict] = collections.defaultdict(lambda: {
        "centers": [], "kp_history": [], "zones": [], "box_heights": []
    })
    frame_player_counts = []
    # For spacing: store filtered per-frame center lists
    filtered_frame_centers = []

    for r in results_list:
        if r.boxes is None or r.keypoints is None:
            continue
        ids   = r.boxes.id
        boxes = r.boxes.xyxy
        kps   = r.keypoints.xy  # [N, 17, 2]

        if ids is None:
            continue

        frame_centers = []
        valid_in_frame = 0

        for i, tid in enumerate(ids.int().tolist()):
            box = boxes[i].tolist()
            box_h_pct = (box[3] - box[1]) / h_px

            # ── Height filter: skip background people / spectators ──────────
            if box_h_pct < MIN_PLAYER_HEIGHT:
                continue

            valid_in_frame += 1
            cx = (box[0] + box[2]) / 2 / w_px
            cy = (box[1] + box[3]) / 2 / h_px
            tracks[tid]["centers"].append((cx, cy))
            tracks[tid]["zones"].append(_zone_for_point(cx, cy))
            tracks[tid]["box_heights"].append(box_h_pct)
            frame_centers.append(((box[0]+box[2])/2, (box[1]+box[3])/2))

            # Store full keypoint array for shot detection
            if kps.shape[1] >= 17:
                tracks[tid]["kp_history"].append(kps[i].tolist())

        if valid_in_frame > 0:
            frame_player_counts.append(valid_in_frame)
        if frame_centers:
            filtered_frame_centers.append(frame_centers)

    # ── Per-player metrics ─────────────────────────────────────────────────
    player_metrics = {}

    for tid, data in tracks.items():
        centers = data["centers"]
        # Require at least 5 frames of visibility to count as a real player
        if len(centers) < 5:
            continue

        # Raw speeds per step (ft/s, vid_stride=2 so 2 frames per step)
        raw_speeds = []
        dists_ft   = []
        for j in range(1, len(centers)):
            dx = (centers[j][0] - centers[j-1][0]) * w_px
            dy = (centers[j][1] - centers[j-1][1]) * h_px
            ft = math.hypot(dx, dy) / px_per_ft
            dists_ft.append(ft)
            raw_speeds.append(ft * (fps / 2))

        # Smooth to kill single-frame jumps
        smooth = _smooth_speeds(raw_speeds, SPEED_SMOOTH_WIN)

        total_dist_ft  = sum(dists_ft)
        peak_speed_fts = max(smooth) if smooth else 0
        avg_speed_fts  = float(np.mean(smooth)) if smooth else 0

        zone_counts  = collections.Counter(data["zones"])
        total_frames = len(data["zones"]) or 1
        zone_pct     = {z: round(c/total_frames*100) for z,c in zone_counts.items()}
        primary_zone = max(zone_counts, key=zone_counts.get) if zone_counts else "unknown"

        shot_detected = _detect_shot(data["kp_history"])

        player_metrics[tid] = {
            "total_dist_ft":  round(total_dist_ft, 1),
            "peak_speed_fts": round(peak_speed_fts, 1),
            "peak_speed_mph": round(peak_speed_fts * 0.6818, 1),
            "avg_speed_fts":  round(avg_speed_fts, 1),
            "primary_zone":   primary_zone,
            "zone_pct":       zone_pct,
            "shot_detected":  shot_detected,
        }

    # ── Team-level metrics ─────────────────────────────────────────────────
    avg_players   = round(float(np.mean(frame_player_counts)), 1) if frame_player_counts else 0
    top_speed_mph = round(max(
        (p["peak_speed_mph"] for p in player_metrics.values()), default=0
    ), 1)

    # Spacing: only between height-filtered players
    spacing_scores = []
    for centers_px in filtered_frame_centers:
        if len(centers_px) < 2:
            continue
        dists = [
            math.hypot(centers_px[a][0]-centers_px[b][0],
                       centers_px[a][1]-centers_px[b][1])
            for a in range(len(centers_px))
            for b in range(a+1, len(centers_px))
        ]
        spacing_scores.append(float(np.mean(dists)) / px_per_ft)
    avg_spacing_ft = round(float(np.mean(spacing_scores)), 1) if spacing_scores else 0

    shots = sum(1 for p in player_metrics.values() if p["shot_detected"])

    all_zones    = [z for d in tracks.values() for z in d["zones"]]
    zone_summary = dict(collections.Counter(all_zones).most_common(4))

    # Trajectory data for court visualizer (normalised 0-1, last 60 positions)
    trajectories = {}
    for tid, data in tracks.items():
        if len(data["centers"]) < 5:
            continue
        trail = data["centers"][-60:]
        trajectories[str(tid)] = [{"x": round(cx, 4), "y": round(cy, 4)} for cx, cy in trail]

    # Shot arc data — wrist + shoulder keypoint heights for detected shooters
    shot_arcs = {}
    for tid, data in tracks.items():
        pm = player_metrics.get(tid)
        if not pm or not pm["shot_detected"]:
            continue
        arc = []
        for kp in data["kp_history"][-30:]:
            wy = kp[10][1] if len(kp) > 10 and kp[10][0] != 0 else None
            sy = kp[6][1]  if len(kp) > 6  and kp[6][0]  != 0 else None
            arc.append({
                "wrist_y":    round(wy / h_px, 4) if wy else None,
                "shoulder_y": round(sy / h_px, 4) if sy else None,
            })
        shot_arcs[str(tid)] = arc

    metrics = {
        "player_count":         len(player_metrics),
        "avg_players_in_frame": avg_players,
        "top_speed_mph":        top_speed_mph,
        "avg_spacing_ft":       avg_spacing_ft,
        "shot_attempts":        shots,
        "zone_occupancy":       zone_summary,
        "players":              player_metrics,
        "px_per_ft":            round(px_per_ft, 2),
        "trajectories":         trajectories,
        "shot_arcs":            shot_arcs,
    }

    lines = [
        f"YOLO tracked {len(player_metrics)} on-court players (height-filtered).",
        f"Avg players visible per frame: {avg_players}.",
        f"Top speed: {top_speed_mph} mph (calibrated, smoothed).",
        f"Avg defensive spacing: {avg_spacing_ft:.1f} ft.",
        f"Shot attempts (wrist-above-shoulder, sustained rise): {shots}.",
        f"Top zones: { {z: c for z,c in list(zone_summary.items())[:3]} }.",
    ]
    for tid, pm in list(player_metrics.items())[:5]:
        lines.append(
            f"  Player #{tid}: {pm['total_dist_ft']} ft, "
            f"peak {pm['peak_speed_mph']} mph, zone: {pm['primary_zone']}"
            + (" [SHOT]" if pm["shot_detected"] else "")
        )
    summary = "\n".join(lines)

    return {"metrics": metrics, "summary": summary}


# ═══════════════════════════════════════════════════════════════════════════════
# GEMINI CALLS — dual-call architecture (fast JSON + qualitative)
# ═══════════════════════════════════════════════════════════════════════════════
def _upload_and_wait(path: str):
    vf = client.files.upload(file=path)
    while vf.state.name == "PROCESSING":
        time.sleep(2); vf = client.files.get(name=vf.name)
    return vf


def gemini_fast_json(video_file, yolo_summary: str) -> dict:
    """
    Call 1: fast structured JSON (~2s).
    Returns score, period, possession, play_type immediately
    so the scoreboard can update before the full report arrives.
    """
    prompt = (
        "You are a basketball scorekeeper. Watch this clip and reply ONLY with JSON "
        "(no markdown, no extra text):\n"
        '{"score_a": <int or null>, "team_a": "<color/name or null>", '
        '"score_b": <int or null>, "team_b": "<color/name or null>", '
        '"period": "<Q1/Q2/Q3/Q4/OT or null>", '
        '"possession": "<team_a/team_b/null>", '
        '"play_type": "<score/turnover/rebound/foul/timeout/other/null>", '
        '"shot_made": <true/false/null>}\n\n'
        f"YOLO data:\n{yolo_summary}"
    )
    try:
        resp = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=[prompt, video_file]
        )
        raw = resp.text.strip().strip("```json").strip("```").strip()
        return json.loads(raw)
    except Exception:
        return {}


def gemini_qualitative(video_file, yolo_summary: str) -> str:
    """
    Call 2: full scouting paragraph with YOLO context injected.
    Runs in parallel with or after the fast call.
    """
    prompt = (
        "You are an elite high school basketball scout. "
        "You have been given YOLO tracking data for context. "
        "Watch the clip and write a 4-6 sentence scouting report covering:\n"
        "1. The key play or moment (scoring, turnover, defensive stop, etc.).\n"
        "2. Defensive spacing — zone or man-to-man, gaps exploited.\n"
        "3. Shooter mechanics if a shot occurred — release, arc, footwork.\n"
        "4. Any player standing out for hustle, positioning, or error.\n"
        "Be specific and technical. Do NOT repeat the YOLO numbers verbatim.\n\n"
        f"YOLO tracking context:\n{yolo_summary}"
    )
    try:
        resp = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=[prompt, video_file]
        )
        return resp.text
    except Exception as e:
        return f"Gemini error: {e}"


def gemini_scout(clip_path: str, yolo_metrics: dict) -> dict:
    """
    Uploads raw clip once, fires both Gemini calls in parallel threads,
    returns combined result.
    """
    yolo_summary = yolo_metrics.get("summary", "No YOLO data.")
    try:
        vf = _upload_and_wait(clip_path)

        # Run both calls concurrently
        fast_result, qual_result = {}, ""
        def _fast():
            nonlocal fast_result
            fast_result = gemini_fast_json(vf, yolo_summary)
        def _qual():
            nonlocal qual_result
            qual_result = gemini_qualitative(vf, yolo_summary)

        t1 = threading.Thread(target=_fast)
        t2 = threading.Thread(target=_qual)
        t1.start(); t2.start()
        t1.join(); t2.join()

        client.files.delete(name=vf.name)

        # Build unified score string from structured JSON
        score_update = None
        if fast_result.get("team_a") and fast_result.get("score_a") is not None:
            score_update = (
                f"{fast_result['team_a']} {fast_result['score_a']} – "
                f"{fast_result.get('team_b','?')} {fast_result.get('score_b','?')}"
            )

        return {
            "report":       qual_result,
            "score_update": score_update,
            "period":       fast_result.get("period"),
            "play_type":    fast_result.get("play_type"),
            "possession":   fast_result.get("possession"),
            "shot_made":    fast_result.get("shot_made"),
            "structured":   fast_result,
        }
    except Exception as e:
        return {"report": f"Gemini error: {e}", "score_update": None, "period": None,
                "play_type": None, "possession": None, "shot_made": None, "structured": {}}


def gemini_confirm_viral(clip_path: str) -> dict:
    try:
        vf   = _upload_and_wait(clip_path)
        resp = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=[
                'Watch this sports clip. Reply ONLY with JSON (no markdown):\n'
                '{"excitement":<1-10>,"is_viral":<true/false>,'
                '"reason":"<one sentence>",'
                '"score_update":"<e.g. Blue 42 - White 38 or null>",'
                '"period":"<e.g. Q3 or null>"}\n'
                "is_viral=true if excitement>=7.",
                vf
            ]
        )
        client.files.delete(name=vf.name)
        raw = resp.text.strip().strip("```json").strip("```").strip()
        return json.loads(raw)
    except Exception as e:
        return {"excitement": 5, "is_viral": False,
                "reason": "Analysis failed.", "score_update": None, "period": None}


# ═══════════════════════════════════════════════════════════════════════════════
# CORE ANALYSIS — 10-second bucket
# ═══════════════════════════════════════════════════════════════════════════════
def analyze_time_bucket(source_path: str, bucket_start: float,
                        cache_prefix: str = "bucket") -> dict:
    clip_name   = f"cache_clips/{cache_prefix}_{int(bucket_start)}.mp4"
    report_name = f"cache_reports/{cache_prefix}_{int(bucket_start)}.json"

    if os.path.exists(report_name):
        print(f"[CACHE HIT] {cache_prefix}_{int(bucket_start)}s")
        with open(report_name) as f: return json.load(f)

    print(f"[CACHE MISS] {cache_prefix}_{int(bucket_start)}s")

    subprocess.run([
        "ffmpeg", "-y", "-ss", str(bucket_start), "-i", source_path,
        "-t", "10", "-c:v", CODEC, clip_name
    ], capture_output=True)
    if not os.path.exists(clip_name):
        return {"report": f"FFmpeg failed at {bucket_start}s.",
                "score_update": None, "period": None}

    # ── Dead-time gate: skip GPU + Gemini if this is an ad/break/halftime ──
    dead, reason = is_dead_time(clip_name)
    if dead:
        print(f"[DeadTime] Skipping analysis — {reason}")
        result = {
            "report": f"⏸ Broadcast pause detected ({reason}). Analysis skipped to save resources.",
            "score_update": None,
            "period": None,
            "dead_time": True,
            "dead_reason": reason,
        }
        with open(report_name, "w") as f: json.dump(result, f)
        try: os.remove(clip_name)
        except: pass
        return result

    # ── YOLO metrics + ByteTrack ────────────────────────────────────────────
    print("[YOLO] Running pose tracking (ByteTrack)...")
    yolo_metrics = extract_yolo_metrics(clip_name)

    # ── Gemini dual-call (fast JSON + qualitative in parallel) ───────────────
    print("[Gemini] Running dual-call analysis...")
    result = gemini_scout(clip_name, yolo_metrics)
    result["dead_time"] = False
    result["yolo_metrics"] = yolo_metrics.get("metrics", {})

    with open(report_name, "w") as f: json.dump(result, f)

    try:
        if os.path.exists(clip_name): os.remove(clip_name)
    except Exception as e:
        print(f"[Cleanup] {e}")

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# VIRAL SCAN
# ═══════════════════════════════════════════════════════════════════════════════
def scan_viral_moments(source_path: str, sensitivity: float,
                       cache_key: str = "viral_scan") -> list:
    viral_cache = f"cache_reports/{cache_key}.json"
    if os.path.exists(viral_cache):
        with open(viral_cache) as f: return json.load(f).get("moments", [])

    profile    = get_audio_energy_profile(source_path)
    candidates = detect_excitement_spikes(profile, sensitivity)
    print(f"[Viral] {len(candidates)} candidates")

    confirmed = []
    for ts in candidates[:12]:
        cp = f"cache_clips/viral_{int(ts)}.mp4"
        subprocess.run([
            "ffmpeg", "-y", "-ss", str(ts), "-i", source_path,
            "-t", "10", "-c:v", CODEC, cp
        ], capture_output=True)
        if not os.path.exists(cp): continue
        r = gemini_confirm_viral(cp)
        try: os.remove(cp)
        except: pass
        if r.get("is_viral"):
            confirmed.append({
                "ts": ts, "excitement": r.get("excitement", 7),
                "reason": r.get("reason", ""),
                "score_update": r.get("score_update"),
                "period": r.get("period"),
            })

    confirmed.sort(key=lambda x: -x["excitement"])
    with open(viral_cache, "w") as f: json.dump({"moments": confirmed}, f)
    return confirmed


# ═══════════════════════════════════════════════════════════════════════════════
# YOUTUBE LIVE BUFFER THREAD
# ═══════════════════════════════════════════════════════════════════════════════
def _live_buffer_worker(session_id: str, youtube_url: str, auto_analyze: bool):
    session = live_sessions[session_id]
    stop    = session["stop_event"]
    buf_dir = f"live_buffers/{session_id}"
    os.makedirs(buf_dir, exist_ok=True)

    try:
        # Resolve live HLS URL via yt-dlp
        session["status"] = "resolving"
        print(f"[Live {session_id[:8]}] Resolving stream URL via yt-dlp...")
        r = subprocess.run(
            ["yt-dlp", "-f", "best[ext=mp4]/best", "-g", youtube_url],
            capture_output=True, text=True, timeout=30
        )
        if r.returncode != 0 or not r.stdout.strip():
            session["status"] = "error"
            session["error"]  = f"yt-dlp failed: {r.stderr.strip()[:300]}"
            return

        stream_url = r.stdout.strip().split("\n")[0]
        print(f"[Live {session_id[:8]}] Stream URL resolved. Starting capture...")
        session["status"] = "buffering"

        # ── Analysis runs on a separate thread so capture never stalls ────────
        # The capture loop fills a queue; the analysis thread drains it.
        # This means we keep buffering even while YOLO+Gemini are running
        # (which can take 20-60s), so we never fall behind the live stream.
        seg_queue   = []
        queue_lock  = threading.Lock()
        analysis_done = threading.Event()

        def _analysis_worker():
            while not stop.is_set() or seg_queue:
                with queue_lock:
                    item = seg_queue.pop(0) if seg_queue else None
                if item is None:
                    time.sleep(0.5); continue

                idx, path = item["idx"], item["path"]
                session["status"] = f"analyzing seg {idx}"
                print(f"[Live {session_id[:8]}] Analyzing segment {idx}...")

                # Dead-time gate — skip ad breaks / halftime
                dead, reason = is_dead_time(path)
                if dead:
                    print(f"[Live {session_id[:8]}] Dead time ({reason}) — skipping seg {idx}")
                    result = {
                        "report": f"⏸ Broadcast pause ({reason}) — analysis paused.",
                        "score_update": None, "period": None,
                        "segment": idx, "wall_ts": time.time(),
                        "dead_time": True, "dead_reason": reason,
                    }
                    session["results"].append(result)
                    session["latest"] = result
                    session["status"] = "paused"
                    try: os.remove(path)
                    except: pass
                    continue

                # YOLO metrics + ByteTrack + dual Gemini
                yolo_metrics = extract_yolo_metrics(path)
                result = gemini_scout(path, yolo_metrics)
                result["segment"]      = idx
                result["wall_ts"]      = time.time()
                result["dead_time"]    = False
                result["yolo_metrics"] = yolo_metrics.get("metrics", {})
                session["results"].append(result)
                session["latest"]  = result
                session["status"]  = "live"

                try: os.remove(path)
                except: pass

            analysis_done.set()

        if auto_analyze:
            anal_thread = threading.Thread(target=_analysis_worker, daemon=True)
            anal_thread.start()

        # ── Capture loop — runs continuously regardless of analysis speed ────
        seg_index = 0
        while not stop.is_set():
            seg_path = f"{buf_dir}/seg_{seg_index:04d}.mp4"

            ffmpeg_proc = subprocess.run([
                "ffmpeg", "-y",
                "-i", stream_url,
                "-t", "10",
                "-c:v", CODEC, "-c:a", "aac",
                seg_path
            ], capture_output=True, timeout=40)

            if stop.is_set(): break

            if not os.path.exists(seg_path) or os.path.getsize(seg_path) < 1000:
                print(f"[Live {session_id[:8]}] Segment {seg_index} empty, retrying...")
                time.sleep(3)
                continue

            if auto_analyze:
                with queue_lock:
                    # Keep queue bounded: if analysis is very far behind,
                    # drop oldest items so we stay near real-time
                    if len(seg_queue) > 4:
                        dropped = seg_queue.pop(0)
                        try: os.remove(dropped["path"])
                        except: pass
                        print(f"[Live {session_id[:8]}] Queue full, dropped seg {dropped['idx']}")
                    seg_queue.append({"idx": seg_index, "path": seg_path})

            seg_index += 1

        if auto_analyze:
            analysis_done.wait(timeout=120)

    except Exception as e:
        session["status"] = "error"
        session["error"]  = str(e)
        print(f"[Live {session_id[:8]}] Fatal: {e}")
    finally:
        session["status"] = "stopped"


# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════

class YoutubeVodRequest(BaseModel):
    youtube_url: str
    start_time:  float = 0.0


@app.post("/api/analyze_youtube_vod")
def analyze_youtube_vod(request: YoutubeVodRequest):
    """
    Download a 30-second clip from a non-live YouTube video at a given
    timestamp and run the full dead-time → YOLO → Gemini pipeline on it.
    Results are cached by URL+timestamp hash so repeat requests are instant.
    """
    if not _verify_ytdlp():
        raise HTTPException(500, "yt-dlp not installed. Run: pip install yt-dlp")

    import hashlib
    cache_key   = hashlib.md5(f"{request.youtube_url}:{int(request.start_time)}".encode()).hexdigest()[:12]
    clip_path   = f"cache_clips/vod_{cache_key}.mp4"
    report_path = f"cache_reports/vod_{cache_key}.json"

    if os.path.exists(report_path):
        with open(report_path) as f:
            return json.load(f)

    print(f"[VOD] Downloading from {request.youtube_url} at {request.start_time}s")
    dl = subprocess.run([
        "yt-dlp",
        "-f", "best[ext=mp4]/best",
        "--download-sections", f"*{int(request.start_time)}-{int(request.start_time)+30}",
        "-o", clip_path,
        request.youtube_url
    ], capture_output=True, timeout=120)

    if not os.path.exists(clip_path):
        raise HTTPException(500, f"yt-dlp download failed: {dl.stderr.decode()[:300]}")

    dead, reason = is_dead_time(clip_path)
    if dead:
        return {"report": f"No active play detected ({reason}).", "dead_time": True,
                "score_update": None, "period": None, "yolo_metrics": {}}

    yolo_metrics = extract_yolo_metrics(clip_path)
    result       = gemini_scout(clip_path, yolo_metrics)
    result["dead_time"]    = False
    result["yolo_metrics"] = yolo_metrics.get("metrics", {})
    result["source"]       = "youtube_vod"
    result["start_time"]   = request.start_time

    with open(report_path, "w") as f:
        json.dump(result, f)

    try:
        if os.path.exists(clip_path): os.remove(clip_path)
    except: pass

    return result


@app.post("/api/upload_clip")
async def upload_clip(file: UploadFile = File(...)):
    """
    Accepts any video file from the browser.
    Streams it to disk in 8 MB chunks (memory-safe for large files).
    Returns session_id and a preview_url the browser <video> can load.
    """
    if not file.content_type or not file.content_type.startswith("video/"):
        raise HTTPException(400, "Must be a video file (video/*).")

    ext        = Path(file.filename or "upload").suffix or ".mp4"
    session_id = str(uuid.uuid4())
    dest       = f"uploads/{session_id}{ext}"
    max_bytes  = MAX_UPLOAD_MB * 1024 * 1024
    size       = 0

    with open(dest, "wb") as out:
        while True:
            chunk = await file.read(8 * 1024 * 1024)
            if not chunk: break
            size += len(chunk)
            if size > max_bytes:
                out.close(); os.remove(dest)
                raise HTTPException(413, f"File exceeds {MAX_UPLOAD_MB} MB limit.")
            out.write(chunk)

    print(f"[Upload] {file.filename} -> {dest} ({size/1e6:.1f} MB)")
    return {
        "session_id":  session_id,
        "filename":    file.filename,
        "size_mb":     round(size / 1e6, 2),
        "preview_url": f"/uploads/{session_id}{ext}",
    }


class DeadTimeRequest(BaseModel):
    video_source: str
    current_time: float


@app.post("/api/check_dead_time")
def check_dead_time(request: DeadTimeRequest):
    """
    Fast pre-check (~0.2s) before committing to full YOLO+Gemini analysis.
    Frontend calls this first; only calls analyze_playhead if dead_time=False.
    """
    source_path = resolve_source(request.video_source)
    if source_path is None:
        raise HTTPException(404, f"Source not found: {request.video_source}")

    # Slice a tiny clip for the check
    bucket_start = math.floor(request.current_time / 10.0) * 10
    tmp_clip     = f"cache_clips/deadcheck_{int(bucket_start)}_{os.getpid()}.mp4"
    subprocess.run([
        "ffmpeg", "-y", "-ss", str(bucket_start), "-i", source_path,
        "-t", "10", "-c:v", "libx264", "-preset", "ultrafast", tmp_clip
    ], capture_output=True, timeout=20)

    if not os.path.exists(tmp_clip):
        return {"dead_time": False, "reason": ""}  # assume active if slice fails

    dead, reason = is_dead_time(tmp_clip)
    try: os.remove(tmp_clip)
    except: pass

    return {"dead_time": dead, "reason": reason, "bucket_start": bucket_start}


@app.post("/api/analyze_playhead")
def handle_playhead(request: PlayheadRequest):
    source_path = resolve_source(request.video_source)
    if source_path is None:
        raise HTTPException(404, f"Source not found: {request.video_source}")
    prefix       = request.video_source if _is_uuid(request.video_source) else "bucket"
    bucket_start = math.floor(request.current_time / 10.0) * 10
    result       = analyze_time_bucket(source_path, bucket_start, cache_prefix=prefix)
    return {
        "bucket_start":    bucket_start,
        "scouting_report": result["report"],
        "score_update":    result.get("score_update"),
        "period":          result.get("period"),
        "play_type":       result.get("play_type"),
        "possession":      result.get("possession"),
        "shot_made":       result.get("shot_made"),
        "structured":      result.get("structured", {}),
        "yolo_metrics":    result.get("yolo_metrics", {}),
        "dead_time":       result.get("dead_time", False),
        "dead_reason":     result.get("dead_reason", ""),
    }


@app.post("/api/scan_viral")
def handle_viral_scan(request: ViralScanRequest):
    source_path = resolve_source(request.video_source)
    if source_path is None:
        raise HTTPException(404, f"Source not found: {request.video_source}")
    cache_key = f"viral_{request.video_source}" if _is_uuid(request.video_source) else "viral_scan"
    moments   = scan_viral_moments(source_path, request.sensitivity, cache_key)
    return {"viral_moments": moments, "count": len(moments)}


@app.post("/api/start_live_session")
def start_live_session(request: LiveSessionRequest):
    if not _verify_ytdlp():
        raise HTTPException(500, "yt-dlp not installed. Run: pip install yt-dlp")

    session_id = str(uuid.uuid4())
    stop_event = threading.Event()
    session    = {
        "youtube_url":  request.youtube_url,
        "auto_analyze": request.auto_analyze,
        "status":       "starting",
        "results":      [],
        "latest":       None,
        "error":        None,
        "stop_event":   stop_event,
        "thread":       None,
    }
    live_sessions[session_id] = session
    t = threading.Thread(
        target=_live_buffer_worker,
        args=(session_id, request.youtube_url, request.auto_analyze),
        daemon=True
    )
    session["thread"] = t
    t.start()
    print(f"[Live] Started session {session_id[:8]} for {request.youtube_url}")
    return {"session_id": session_id, "status": "starting"}


@app.get("/api/live_status/{session_id}")
def get_live_status(session_id: str):
    s = live_sessions.get(session_id)
    if not s:
        raise HTTPException(404, "Session not found.")
    return {
        "session_id":    session_id,
        "status":        s["status"],
        "error":         s["error"],
        "segments_done": len(s["results"]),
        "latest":        s["latest"],
    }


@app.delete("/api/stop_live/{session_id}")
def stop_live_session(session_id: str):
    s = live_sessions.get(session_id)
    if not s:
        raise HTTPException(404, "Session not found.")
    s["stop_event"].set()
    shutil.rmtree(f"live_buffers/{session_id}", ignore_errors=True)
    del live_sessions[session_id]
    return {"status": "stopped"}


@app.get("/api/get_cached_timeline")
def get_cached_timeline(session_id: Optional[str] = None):
    pattern = (f"cache_reports/{session_id}_*.json" if session_id
               else "cache_reports/bucket_*.json")
    cached = []
    for file in glob.glob(pattern):
        try:
            n = int(os.path.basename(file).rsplit("_", 1)[-1].split(".")[0])
            cached.append(n)
        except (IndexError, ValueError):
            pass
    return {"processed_timestamps": sorted(cached)}


@app.delete("/api/clear_cache")
def clear_cache(session_id: Optional[str] = None):
    if session_id:
        for f in (glob.glob(f"cache_reports/{session_id}_*.json") +
                  glob.glob(f"cache_clips/{session_id}_*.mp4") +
                  glob.glob(f"uploads/{session_id}.*")):
            try: os.remove(f)
            except: pass
    else:
        for d in ["cache_clips", "cache_reports", "uploads", "live_buffers"]:
            shutil.rmtree(d, ignore_errors=True); os.makedirs(d, exist_ok=True)
    return {"status": "cleared"}


@app.get("/health")
def health():
    return {
        "status":          "ok",
        "device":          DEVICE,
        "codec":           CODEC,
        "ytdlp_available": _verify_ytdlp(),
        "active_sessions": len(live_sessions),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# STARTUP
# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    ngrok_token = os.environ.get("NGROK_AUTHTOKEN")
    if ngrok_token and _NGROK_AVAILABLE:
        import pyngrok.conf
        pyngrok.conf.get_default().auth_token = ngrok_token
        tunnel = _ngrok.connect(8000)
        print(f"\n{'='*60}\n Public URL: {"https://aura-seemliest-vance.ngrok-free.dev"}\n{'='*60}\n")
    else:
        print("\n[Tip] Set NGROK_AUTHTOKEN in .env to auto-expose this server.")
        print("      Or: cloudflared tunnel --url http://localhost:8000\n")

    print("Starting Nutmeg AI Scouting Engine on port 8000...")
    uvicorn.run(app, host="0.0.0.0", port=8000)