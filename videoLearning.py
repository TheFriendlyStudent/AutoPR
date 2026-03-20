"""
videoLearning.py — Nutmeg Sports AI Scouting Engine v4.1
=========================================================
ARCHITECTURE CHANGE v4
-----------------------
All computer-vision detection is now handled exclusively by the Roboflow
hosted inference API (basketball-players-fy4c2/25).

ACTUAL classes returned by model v25 (verified from live API output):
  Player        — player bounding boxes
  Ball          — basketball position
  Hoop          — basket/rim location  (NOT "rim")
  Ref           — referee
  Team Name     — scoreboard team name overlay
  Team Points   — scoreboard score overlay
  Period        — scoreboard period overlay
  Shot Clock    — shot clock overlay
  Time Remaining — game clock overlay

There is NO "made" class in this model version. Shot detection is handled
by tracking ball-to-hoop proximity across frames.

The model also detects scoreboard overlays (Team Name, Team Points, Period,
Shot Clock, Time Remaining) which we OCR via Gemini to extract live scores
without needing a separate scoreboard reader.

Tracking (ByteTrack-style IoU matching) is implemented locally.

Reliable stats returned:
  • player_count, avg_players_in_frame
  • zone_occupancy (normalised pixel region membership)
  • shot_near_hoop (ball within proximity of hoop)
  • ball_detected, ball_possession_side
  • player_trajectories (normalised coords for court visualizer)
  • ball_trail (for visualizer)
  • hoop_center (stable median across sampled frames)
  • scoreboard (team names, scores, period, shot clock — from overlay detections)

Unchanged endpoints (all v3 callers still work):
  POST   /api/upload_clip
  POST   /api/analyze_playhead
  POST   /api/check_dead_time
  POST   /api/scan_viral
  POST   /api/start_live_session
  GET    /api/live_status/{sid}
  DELETE /api/stop_live/{sid}
  GET    /api/get_cached_timeline
  DELETE /api/clear_cache
  POST   /api/analyze_youtube_vod
  GET    /health
  GET    /api/debug_renders
"""

import os, time, subprocess, glob, math, json, re, shutil, uuid, threading
import collections, urllib.request, urllib.parse, base64, io
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional, Dict, List, Tuple

import numpy as np

# ── Optional pytubefix ────────────────────────────────────────────────────────
try:
    from pytubefix import YouTube as PyTube
    _PYTUBEFIX_AVAILABLE = True
except ImportError:
    _PYTUBEFIX_AVAILABLE = False


def _verify_streamlink() -> bool:
    try:
        r = subprocess.run(["streamlink", "--version"], capture_output=True, timeout=5)
        return r.returncode == 0
    except FileNotFoundError:
        return False


# ── Optional ngrok ────────────────────────────────────────────────────────────
try:
    from pyngrok import ngrok as _ngrok
    _NGROK_AVAILABLE = True
except ImportError:
    _NGROK_AVAILABLE = False

from google import genai
from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import uvicorn

# ── API clients ───────────────────────────────────────────────────────────────
client = genai.Client(api_key=os.environ.get("GOOGLE_API_KEY"))

# ── YouTube Data API ──────────────────────────────────────────────────────────
YT_API_KEY = os.environ.get("YOUTUBE_API_KEY", "")


def _extract_video_id(url_or_id: str) -> str:
    patterns = [
        r"(?:v=|youtu\.be/|embed/|shorts/|live/)([A-Za-z0-9_-]{11})",
        r"^([A-Za-z0-9_-]{11})$",
    ]
    for p in patterns:
        m = re.search(p, url_or_id)
        if m:
            return m.group(1)
    return url_or_id


def _yt_api_get(endpoint: str, params: dict) -> dict:
    if not YT_API_KEY:
        raise ValueError("YOUTUBE_API_KEY not set in .env")
    params["key"] = YT_API_KEY
    url = f"https://www.googleapis.com/youtube/v3/{endpoint}?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.loads(r.read())


def get_live_hls_url(video_id: str) -> Optional[str]:
    data  = _yt_api_get("videos", {"part": "liveStreamingDetails,snippet", "id": video_id})
    items = data.get("items", [])
    if not items:
        return None
    return items[0].get("liveStreamingDetails", {}).get("hlsManifestUrl")


def get_video_info(video_id: str) -> dict:
    data  = _yt_api_get("videos", {"part": "snippet,contentDetails,liveStreamingDetails", "id": video_id})
    items = data.get("items", [])
    if not items:
        return {}
    item    = items[0]
    snippet = item.get("snippet", {})
    details = item.get("liveStreamingDetails", {})
    return {
        "title":   snippet.get("title", ""),
        "channel": snippet.get("channelTitle", ""),
        "is_live": snippet.get("liveBroadcastContent") == "live",
        "hls_url": details.get("hlsManifestUrl"),
    }


# ── FFmpeg (Windows path; ignored on Linux/Mac) ───────────────────────────────
_ffmpeg_win = (
    r"C:\Users\vasub\AppData\Local\Microsoft\WinGet\Packages"
    r"\Gyan.FFmpeg_Microsoft.Winget.Source_8wekyb3d8bbwe\ffmpeg-8.1-full_build\bin"
)
if os.path.exists(_ffmpeg_win):
    os.environ["PATH"] += os.pathsep + _ffmpeg_win

# ── Codec detection — GPU not required for Roboflow-only pipeline ─────────────
try:
    import torch
    DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
    CODEC  = "h264_nvenc" if torch.cuda.is_available() else "libx264"
    print(f"[Engine] torch available — device: {DEVICE}")
except ImportError:
    DEVICE = "cpu"
    CODEC  = "libx264"
    print("[Engine] torch not installed — CPU mode (Roboflow is cloud-side)")

# ── Directories ───────────────────────────────────────────────────────────────
for _d in ["cache_clips", "cache_reports", "uploads", "live_buffers", "debug_renders"]:
    os.makedirs(_d, exist_ok=True)

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="Nutmeg Sports AI Scouting Engine v4")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.mount("/uploads", StaticFiles(directory="uploads"),       name="uploads")
app.mount("/debug",   StaticFiles(directory="debug_renders"), name="debug")

executor    = ThreadPoolExecutor(max_workers=4)
live_sessions: dict = {}
MAX_UPLOAD_MB = int(os.environ.get("MAX_UPLOAD_MB", "2000"))


# ═══════════════════════════════════════════════════════════════════════════════
# REQUEST MODELS
# ═══════════════════════════════════════════════════════════════════════════════
class PlayheadRequest(BaseModel):
    video_source: str
    current_time: float
    force: bool = False

class ViralScanRequest(BaseModel):
    video_source: str
    sensitivity:  float = 0.65

class LiveSessionRequest(BaseModel):
    youtube_url:  str
    auto_analyze: bool = True

class YoutubeVodRequest(BaseModel):
    youtube_url: str
    start_time:  float = 0.0

class DeadTimeRequest(BaseModel):
    video_source: str
    current_time: float


# ═══════════════════════════════════════════════════════════════════════════════
# ROBOFLOW INFERENCE — single model replaces yolo11m-pose
# ═══════════════════════════════════════════════════════════════════════════════
# Model: roboflow-universe-projects/basketball-players-fy4c2/25
# Classes (from model card): ball, made, player, rim

ROBOFLOW_API_KEY = os.environ.get("ROBOFLOW_API_KEY", "")
RF_PROJECT       = os.environ.get("RF_PROJECT",   "basketball-players-fy4c2")
RF_WORKSPACE     = os.environ.get("RF_WORKSPACE", "roboflow-universe-projects")
RF_VERSION       = int(os.environ.get("RF_VERSION", "25"))
RF_CONF          = float(os.environ.get("RF_CONF", "0.35"))
RF_OVERLAP       = int(os.environ.get("RF_OVERLAP", "30"))

# ── Actual class names from model v25 (verified from live API output) ─────────
# These are title-case strings exactly as the model returns them.
# Override via .env only if you switch to a different model version.
RF_CLS_BALL        = os.environ.get("RF_CLS_BALL",        "Ball")
RF_CLS_PLAYER      = os.environ.get("RF_CLS_PLAYER",      "Player")
RF_CLS_HOOP        = os.environ.get("RF_CLS_HOOP",        "Hoop")
RF_CLS_REF         = os.environ.get("RF_CLS_REF",         "Ref")
RF_CLS_TEAM_NAME   = os.environ.get("RF_CLS_TEAM_NAME",   "Team Name")
RF_CLS_TEAM_PTS    = os.environ.get("RF_CLS_TEAM_PTS",    "Team Points")
RF_CLS_PERIOD      = os.environ.get("RF_CLS_PERIOD",      "Period")
RF_CLS_SHOT_CLOCK  = os.environ.get("RF_CLS_SHOT_CLOCK",  "Shot Clock")
RF_CLS_TIME_REM    = os.environ.get("RF_CLS_TIME_REM",    "Time Remaining")

# All scoreboard overlay classes (used for filtering vs. on-court objects)
RF_SCOREBOARD_CLASSES = {
    RF_CLS_TEAM_NAME, RF_CLS_TEAM_PTS, RF_CLS_PERIOD,
    RF_CLS_SHOT_CLOCK, RF_CLS_TIME_REM,
}

# Ball-to-hoop distance threshold for shot detection (normalised [0,1]).
# If the ball centre is within this fraction of the frame width from the hoop
# it counts as a shot-near-hoop event. Tuned for typical broadcast zoom.
RF_SHOT_PROXIMITY = float(os.environ.get("RF_SHOT_PROXIMITY", "0.12"))

# Frames per second to sample for Roboflow inference (3 = good balance)
RF_SAMPLE_FPS = float(os.environ.get("RF_SAMPLE_FPS", "3"))

# Max parallel Roboflow requests
RF_WORKERS = int(os.environ.get("RF_WORKERS", "4"))

YOLO_DEBUG     = os.environ.get("YOLO_DEBUG", "0") == "1"
YOLO_DEBUG_DIR = os.environ.get("YOLO_DEBUG_DIR", "debug_renders")

_rf_call_count = 0


def _log_rf_startup():
    if ROBOFLOW_API_KEY:
        print(f"[Roboflow] Key set ✓  |  {RF_WORKSPACE}/{RF_PROJECT} v{RF_VERSION}")
        print(f"[Roboflow] Player='{RF_CLS_PLAYER}'  Ball='{RF_CLS_BALL}'  "
              f"Hoop='{RF_CLS_HOOP}'  Ref='{RF_CLS_REF}'")
        print(f"[Roboflow] Scoreboard classes: {RF_SCOREBOARD_CLASSES}")
        print(f"[Roboflow] conf={RF_CONF}  overlap={RF_OVERLAP}  "
              f"sample_fps={RF_SAMPLE_FPS}  workers={RF_WORKERS}  "
              f"shot_proximity={RF_SHOT_PROXIMITY}")
    else:
        print("[Roboflow] WARNING: ROBOFLOW_API_KEY not set.")
        print("           Add ROBOFLOW_API_KEY=your_key to .env")


_log_rf_startup()


# ── HTTP session (keep-alive for Roboflow calls) ──────────────────────────────
try:
    import requests as _requests
    _rf_http = _requests.Session()
    _rf_http.headers.update({"Content-Type": "application/x-www-form-urlencoded"})
    _RF_REQUESTS_AVAILABLE = True
except ImportError:
    _RF_REQUESTS_AVAILABLE = False
    print("[Roboflow] 'requests' not installed — falling back to urllib. "
          "pip install requests for better performance.")


def _roboflow_infer_frame(frame_bgr: np.ndarray) -> List[dict]:
    """
    Send a single BGR frame to the Roboflow hosted inference API.

    Returns a list of detections:
        [{"class": str, "confidence": float,
          "x": cx_px, "y": cy_px, "width": w_px, "height": h_px}, ...]

    Returns [] on error or if the API key is not configured.
    The caller is responsible for normalising pixel coords.
    """
    global _rf_call_count
    if not ROBOFLOW_API_KEY:
        return []

    try:
        import cv2 as _cv2
        _, buf = _cv2.imencode(".jpg", frame_bgr, [_cv2.IMWRITE_JPEG_QUALITY, 80])
        b64 = base64.b64encode(buf.tobytes()).decode("utf-8")

        url = (f"https://detect.roboflow.com/{RF_PROJECT}/{RF_VERSION}"
               f"?api_key={ROBOFLOW_API_KEY}"
               f"&confidence={RF_CONF}&overlap={RF_OVERLAP}")

        if _RF_REQUESTS_AVAILABLE:
            resp = _rf_http.post(url, data=b64, timeout=8)
            resp.raise_for_status()
            preds = resp.json().get("predictions", [])
        else:
            req  = urllib.request.Request(url, data=b64.encode(), method="POST",
                                          headers={"Content-Type": "application/x-www-form-urlencoded"})
            with urllib.request.urlopen(req, timeout=8) as r:
                preds = json.loads(r.read()).get("predictions", [])

        _rf_call_count += 1
        if _rf_call_count == 1 or _rf_call_count % 20 == 0:
            classes = list({p["class"] for p in preds})
            print(f"[Roboflow] call #{_rf_call_count} → {len(preds)} preds {classes}")
        return preds

    except Exception as e:
        print(f"[Roboflow] Inference error (call #{_rf_call_count}): {e}")
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# IoU TRACKER — lightweight ByteTrack-style matching (no model download)
# ═══════════════════════════════════════════════════════════════════════════════
# Tracks player detections across frames using IoU overlap.
# Gives stable integer IDs that persist across the clip.

class IoUTracker:
    """
    Simple IoU-based tracker for bounding boxes.

    Each track carries:
        id        — stable integer
        box       — [cx, cy, w, h] normalised [0,1]
        age       — frames since last matched
        hits      — total matched frames
        history   — list of (cx, cy) for trajectory
        zone_hist — list of zone strings
    """
    IOU_THRESHOLD  = 0.25   # min IoU to associate detection with existing track
    MAX_AGE        = 10     # frames before a lost track is removed
    MIN_HITS       = 3      # frames before a track is considered confirmed

    def __init__(self):
        self._next_id = 1
        self._tracks: List[dict] = []

    @staticmethod
    def _iou(a: list, b: list) -> float:
        """IoU between two [cx,cy,w,h] boxes (normalised)."""
        ax1, ay1 = a[0] - a[2]/2, a[1] - a[3]/2
        ax2, ay2 = a[0] + a[2]/2, a[1] + a[3]/2
        bx1, by1 = b[0] - b[2]/2, b[1] - b[3]/2
        bx2, by2 = b[0] + b[2]/2, b[1] + b[3]/2
        ix1, iy1 = max(ax1, bx1), max(ay1, by1)
        ix2, iy2 = min(ax2, bx2), min(ay2, by2)
        iw, ih   = max(0, ix2 - ix1), max(0, iy2 - iy1)
        inter    = iw * ih
        union    = (ax2-ax1)*(ay2-ay1) + (bx2-bx1)*(by2-by1) - inter
        return inter / union if union > 0 else 0.0

    def update(self, detections: List[dict], frame_w: int, frame_h: int) -> List[dict]:
        """
        detections: list of Roboflow player predictions for one frame.
        Returns list of confirmed tracks with their current state.
        """
        # Normalise to [0,1]
        boxes = []
        for d in detections:
            cx, cy = d["x"] / frame_w, d["y"] / frame_h
            w,  h  = d["width"] / frame_w, d["height"] / frame_h
            boxes.append([cx, cy, w, h])

        matched_det  = set()
        matched_trk  = set()

        # Greedy IoU match (sufficient for 10-s clips; Hungarian would be marginal gain)
        for ti, track in enumerate(self._tracks):
            best_iou, best_di = 0.0, -1
            for di, box in enumerate(boxes):
                if di in matched_det:
                    continue
                iou = self._iou(track["box"], box)
                if iou > best_iou:
                    best_iou, best_di = iou, di
            if best_iou >= self.IOU_THRESHOLD:
                matched_trk.add(ti)
                matched_det.add(best_di)
                track["box"]      = boxes[best_di]
                track["age"]      = 0
                track["hits"]    += 1
                cx, cy = boxes[best_di][0], boxes[best_di][1]
                track["history"].append((round(cx, 4), round(cy, 4)))
                track["zone_hist"].append(_zone_for_point(cx, cy))

        # Age unmatched tracks
        for ti, track in enumerate(self._tracks):
            if ti not in matched_trk:
                track["age"] += 1

        # Spawn new tracks for unmatched detections
        for di, box in enumerate(boxes):
            if di in matched_det:
                continue
            self._tracks.append({
                "id":       self._next_id,
                "box":      box,
                "age":      0,
                "hits":     1,
                "history":  [(round(box[0], 4), round(box[1], 4))],
                "zone_hist": [_zone_for_point(box[0], box[1])],
            })
            self._next_id += 1

        # Remove stale tracks
        self._tracks = [t for t in self._tracks if t["age"] <= self.MAX_AGE]

        # Return confirmed tracks
        return [t for t in self._tracks if t["hits"] >= self.MIN_HITS and t["age"] == 0]


# ═══════════════════════════════════════════════════════════════════════════════
# COURT ZONE DEFINITIONS  (NFHS 84ft × 50ft)
# ═══════════════════════════════════════════════════════════════════════════════
COURT_ZONES = {
    "paint_left":  (0.00,  0.226, 0.28, 0.72),
    "paint_right": (0.774, 1.00,  0.28, 0.72),
    "mid_range":   (0.226, 0.40,  0.00, 1.00),
    "perimeter":   (0.40,  0.60,  0.00, 1.00),
    "three_left":  (0.00,  0.226, 0.00, 0.28),
    "three_right": (0.774, 1.00,  0.72, 1.00),
    "backcourt":   (0.60,  1.00,  0.00, 1.00),
}

ZONE_LABELS = {
    "paint_left":  "Paint (L)",
    "paint_right": "Paint (R)",
    "mid_range":   "Mid-Range",
    "perimeter":   "Perimeter",
    "three_left":  "3PT (L)",
    "three_right": "3PT (R)",
    "backcourt":   "Backcourt",
}


def _zone_for_point(x: float, y: float) -> str:
    for name, (x0, x1, y0, y1) in COURT_ZONES.items():
        if x0 <= x <= x1 and y0 <= y <= y1:
            return name
    return "perimeter"


# ═══════════════════════════════════════════════════════════════════════════════
# FRAME EXTRACTION  (pure FFmpeg — no cv2 VideoCapture loop)
# ═══════════════════════════════════════════════════════════════════════════════

def _extract_frames(clip_path: str, fps: float = RF_SAMPLE_FPS) -> Tuple[List[np.ndarray], int, int]:
    """
    Extract frames from a clip at `fps` using FFmpeg piped to stdout.

    Returns (frames, width, height).
    Uses FFmpeg's select filter so we do one pass and avoid opening the file
    in cv2's per-frame read loop — faster on network mounts and large files.
    """
    import cv2 as _cv2

    # Probe dimensions once
    probe = subprocess.run([
        "ffprobe", "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=width,height",
        "-of", "csv=p=0", clip_path
    ], capture_output=True, text=True, timeout=10)
    try:
        w, h = [int(x) for x in probe.stdout.strip().split(",")]
    except Exception:
        # Fallback via cv2
        cap = _cv2.VideoCapture(clip_path)
        w, h = int(cap.get(_cv2.CAP_PROP_FRAME_WIDTH)), int(cap.get(_cv2.CAP_PROP_FRAME_HEIGHT))
        cap.release()

    # Extract at target fps
    cmd = [
        "ffmpeg", "-i", clip_path,
        "-vf", f"fps={fps}",
        "-f", "rawvideo",
        "-pix_fmt", "bgr24",
        "pipe:1"
    ]
    try:
        proc   = subprocess.run(cmd, capture_output=True, timeout=120)
        raw    = proc.stdout
        frame_bytes = w * h * 3
        n_frames    = len(raw) // frame_bytes
        frames = []
        for i in range(n_frames):
            chunk = raw[i * frame_bytes: (i + 1) * frame_bytes]
            if len(chunk) == frame_bytes:
                frames.append(np.frombuffer(chunk, dtype=np.uint8).reshape(h, w, 3).copy())
        return frames, w, h
    except Exception as e:
        print(f"[FrameExtract] FFmpeg pipe failed: {e} — falling back to cv2")
        import cv2 as _cv2
        cap    = _cv2.VideoCapture(clip_path)
        src_fps = cap.get(_cv2.CAP_PROP_FPS) or 30
        stride  = max(1, round(src_fps / fps))
        frames, fi = [], 0
        while True:
            ok, frame = cap.read()
            if not ok:
                break
            if fi % stride == 0:
                frames.append(frame.copy())
            fi += 1
        cap.release()
        return frames, w, h


# ═══════════════════════════════════════════════════════════════════════════════
# YOLO METRICS  (Roboflow-only pipeline)
# ═══════════════════════════════════════════════════════════════════════════════

def extract_yolo_metrics(clip_path: str, session_id: str = "default") -> dict:
    """
    Full detection pipeline using only the Roboflow hosted model.

    Steps:
      1. Extract frames at RF_SAMPLE_FPS via FFmpeg
      2. Run Roboflow inference in parallel (RF_WORKERS threads)
      3. Separate detections into: players, ball, hoop, refs, scoreboard overlays
      4. Track players with IoU tracker (refs excluded from player count)
      5. Shot detection: ball within RF_SHOT_PROXIMITY of hoop centre
      6. Scoreboard: collect all scoreboard bounding boxes for Gemini OCR
      7. Aggregate: zone occupancy, ball possession side, trajectories

    Returns dict with keys "metrics" and "summary".
    """
    frames, w, h = _extract_frames(clip_path, fps=RF_SAMPLE_FPS)
    if not frames:
        print("[RF] No frames extracted from clip.")
        return {"metrics": {}, "summary": "No frames extracted."}

    n = len(frames)
    print(f"[RF] {n} frames at {RF_SAMPLE_FPS}fps extracted ({w}×{h}) "
          f"from {os.path.basename(clip_path)}")

    # ── Parallel inference ────────────────────────────────────────────────────
    frame_preds: List[Optional[List[dict]]] = [None] * n

    def _infer(idx: int) -> Tuple[int, List[dict]]:
        return idx, _roboflow_infer_frame(frames[idx])

    with ThreadPoolExecutor(max_workers=RF_WORKERS) as pool:
        futs = {pool.submit(_infer, i): i for i in range(n)}
        for fut in as_completed(futs):
            idx, preds = fut.result()
            frame_preds[idx] = preds

    # ── Per-frame aggregation ─────────────────────────────────────────────────
    tracker              = IoUTracker()
    ball_positions       : List[Optional[Tuple[float, float]]] = []
    hoop_positions       : List[Optional[Tuple[float, float]]] = []
    shot_near_hoop_flags : List[bool]  = []
    frame_player_counts  : List[int]   = []
    # Scoreboard: collect the raw crop boxes from the LAST frame that has them
    # (scoreboard is usually static; last frame is most likely to be unoccluded)
    scoreboard_boxes_last: List[dict] = []

    for fi, preds in enumerate(frame_preds):
        if preds is None:
            ball_positions.append(None)
            hoop_positions.append(None)
            shot_near_hoop_flags.append(False)
            continue

        # Partition detections by class
        players    = [p for p in preds if p["class"] == RF_CLS_PLAYER]
        balls      = [p for p in preds if p["class"] == RF_CLS_BALL]
        hoops      = [p for p in preds if p["class"] == RF_CLS_HOOP]
        scoreboard = [p for p in preds if p["class"] in RF_SCOREBOARD_CLASSES]
        # Refs are detected but intentionally excluded from player count/tracking

        # Track only players (not refs)
        confirmed = tracker.update(players, w, h)
        frame_player_counts.append(len(confirmed))

        # Ball — highest confidence detection
        if balls:
            best = max(balls, key=lambda p: p["confidence"])
            bx, by = best["x"] / w, best["y"] / h
            ball_positions.append((round(bx, 4), round(by, 4)))
        else:
            bx, by = None, None
            ball_positions.append(None)

        # Hoop — highest confidence, normalised
        if hoops:
            best = max(hoops, key=lambda p: p["confidence"])
            hx, hy = best["x"] / w, best["y"] / h
            hoop_positions.append((round(hx, 4), round(hy, 4)))
        else:
            hx, hy = None, None
            hoop_positions.append(None)

        # Shot detection: ball within RF_SHOT_PROXIMITY of hoop
        if bx is not None and hx is not None:
            dist = math.hypot(bx - hx, by - hy)
            shot_near_hoop_flags.append(dist <= RF_SHOT_PROXIMITY)
        else:
            shot_near_hoop_flags.append(False)

        # Keep scoreboard boxes from this frame if any present
        if scoreboard:
            scoreboard_boxes_last = scoreboard  # overwrite; last frame wins

    # ── Player-level summaries from confirmed tracks ──────────────────────────
    all_tracks        = [t for t in tracker._tracks if t["hits"] >= IoUTracker.MIN_HITS]
    player_metrics    : dict = {}
    all_zone_counters = collections.Counter()

    for track in all_tracks:
        tid      = str(track["id"])
        zones    = track["zone_hist"]
        zone_ctr = collections.Counter(zones)
        primary  = zone_ctr.most_common(1)[0][0] if zone_ctr else "perimeter"
        all_zone_counters.update(zone_ctr)
        player_metrics[tid] = {
            "primary_zone": primary,
            "zone_pct":     {z: round(cnt / len(zones) * 100) for z, cnt in zone_ctr.items()},
            "shot_detected": False,
            "jersey": None,
        }

    # ── Shot attribution — closest player to ball when ball is near hoop ──────
    shot_event_indices = [i for i, f in enumerate(shot_near_hoop_flags) if f]
    total_shot_events  = len(shot_event_indices)

    for i in shot_event_indices:
        ball = ball_positions[i]
        if ball is None:
            continue
        bx, by = ball
        best_tid, best_dist = None, 999.0
        for track in all_tracks:
            if not track["history"]:
                continue
            hist_idx = min(i, len(track["history"]) - 1)
            tx, ty   = track["history"][hist_idx]
            dist     = math.hypot(bx - tx, by - ty)
            if dist < best_dist:
                best_dist, best_tid = dist, str(track["id"])
        if best_tid and best_dist < 0.25:
            player_metrics[best_tid]["shot_detected"] = True

    # ── Team-level aggregates ─────────────────────────────────────────────────
    avg_players    = round(float(np.mean(frame_player_counts)), 1) if frame_player_counts else 0
    zone_summary   = dict(all_zone_counters.most_common(7))

    detected_ball  = [p for p in ball_positions if p is not None]
    ball_detected  = len(detected_ball) > 0
    ball_side: Optional[str] = None
    if detected_ball:
        avg_bx = float(np.mean([p[0] for p in detected_ball]))
        ball_side = "left" if avg_bx < 0.5 else "right"

    # Hoop — stable median (hoop doesn't move; outliers from misdetections averaged out)
    detected_hoop = [p for p in hoop_positions if p is not None]
    hoop_center   = None
    if detected_hoop:
        hoop_center = {
            "x": round(float(np.median([p[0] for p in detected_hoop])), 3),
            "y": round(float(np.median([p[1] for p in detected_hoop])), 3),
        }

    # ── Scoreboard OCR — crop boxes from last scoreboard frame ───────────────
    scoreboard_data = _ocr_scoreboard_crops(frames[-1] if frames else None,
                                            scoreboard_boxes_last, w, h)

    # ── Trajectory arrays for visualizer ────────────────────────────────────
    trajectories = {
        str(t["id"]): [{"x": cx, "y": cy} for cx, cy in t["history"][-60:]]
        for t in all_tracks if len(t["history"]) >= 3
    }
    ball_trail = [{"x": p[0], "y": p[1]} for p in detected_ball[-60:]]

    # ── Terminal log ─────────────────────────────────────────────────────────
    rf_ok = sum(1 for p in frame_preds if p is not None)
    print(f"[RF] {rf_ok}/{n} frames inferred | "
          f"Players:{len(player_metrics)} ({avg_players}/frame) | "
          f"Ball:{'✓ ' + str(ball_side) if ball_detected else '✗'} | "
          f"Hoop:{'✓' if hoop_center else '✗'} | "
          f"Shot-events:{total_shot_events} | "
          f"Scoreboard:{bool(scoreboard_data)}")
    for tid, pm in list(player_metrics.items())[:8]:
        print(f"[RF]   P{tid:4s} → {pm['primary_zone']}"
              + (" 🏀 SHOT-NEAR" if pm["shot_detected"] else ""))

    metrics = {
        "player_count":          len(player_metrics),
        "avg_players_in_frame":  avg_players,
        "shot_attempts":         sum(1 for p in player_metrics.values() if p["shot_detected"]),
        "shot_near_hoop_events": total_shot_events,
        "ball_detected":         ball_detected,
        "ball_possession_side":  ball_side,
        "ball_trail":            ball_trail,
        "hoop_center":           hoop_center,
        # keep "rim_center" alias so frontend code doesn't break
        "rim_center":            hoop_center,
        "zone_occupancy":        zone_summary,
        "players":               player_metrics,
        "trajectories":          trajectories,
        "scoreboard":            scoreboard_data,
    }

    lines = [
        f"Roboflow tracked {len(player_metrics)} players (refs excluded).",
        f"Avg visible per frame: {avg_players}. "
        f"Ball-near-hoop events: {total_shot_events}.",
        f"Ball: {'detected, ' + str(ball_side) + ' side' if ball_detected else 'not detected'}.",
        f"Hoop: {'detected at ' + str(hoop_center) if hoop_center else 'not detected'}.",
        f"Zones: {dict(list(zone_summary.items())[:3])}.",
    ]
    if scoreboard_data:
        lines.append(f"Scoreboard OCR: {scoreboard_data}")
    for tid, pm in list(player_metrics.items())[:6]:
        lines.append(f"  P{tid}: {pm['primary_zone']}"
                     + (" [SHOT-NEAR]" if pm["shot_detected"] else ""))

    return {"metrics": metrics, "summary": "\n".join(lines)}


# ── Optional debug render ─────────────────────────────────────────────────────
def _render_debug_frames(frames, frame_preds, w, h, clip_path, trajectories, ball_positions):
    try:
        import cv2 as _cv2
        os.makedirs(YOLO_DEBUG_DIR, exist_ok=True)
        base   = os.path.splitext(os.path.basename(clip_path))[0]
        out    = os.path.join(YOLO_DEBUG_DIR, f"{base}_debug.mp4")
        fourcc = _cv2.VideoWriter_fourcc(*"mp4v")
        writer = _cv2.VideoWriter(out, fourcc, RF_SAMPLE_FPS, (w, h))
        for fi, (frame, preds) in enumerate(zip(frames, frame_preds)):
            if preds is None:
                writer.write(frame); continue
            vis = frame.copy()
            for det in preds:
                cls = det["class"]
                if cls == RF_CLS_PLAYER:
                    x1 = int(det["x"] - det["width"]/2)
                    y1 = int(det["y"] - det["height"]/2)
                    x2 = int(det["x"] + det["width"]/2)
                    y2 = int(det["y"] + det["height"]/2)
                    _cv2.rectangle(vis, (x1, y1), (x2, y2), (56, 189, 248), 2)
                    _cv2.putText(vis, f"P {det['confidence']:.2f}", (x1, y1-5),
                                 _cv2.FONT_HERSHEY_SIMPLEX, 0.4, (56, 189, 248), 1)
                elif cls == RF_CLS_BALL:
                    cx, cy = int(det["x"]), int(det["y"])
                    _cv2.circle(vis, (cx, cy), 12, (0, 165, 255), -1)
                    _cv2.putText(vis, "BALL", (cx+14, cy+5),
                                 _cv2.FONT_HERSHEY_SIMPLEX, 0.4, (0, 165, 255), 1)
                elif cls == RF_CLS_HOOP:
                    cx, cy = int(det["x"]), int(det["y"])
                    _cv2.circle(vis, (cx, cy), 16, (0, 255, 0), 2)
                    _cv2.putText(vis, "HOOP", (cx+18, cy+5),
                                 _cv2.FONT_HERSHEY_SIMPLEX, 0.4, (0, 255, 0), 1)
                elif cls == RF_CLS_REF:
                    x1 = int(det["x"] - det["width"]/2)
                    y1 = int(det["y"] - det["height"]/2)
                    x2 = int(det["x"] + det["width"]/2)
                    y2 = int(det["y"] + det["height"]/2)
                    _cv2.rectangle(vis, (x1, y1), (x2, y2), (128, 128, 128), 1)
                    _cv2.putText(vis, "REF", (x1, y1-5),
                                 _cv2.FONT_HERSHEY_SIMPLEX, 0.35, (180, 180, 180), 1)
                elif cls in RF_SCOREBOARD_CLASSES:
                    x1 = int(det["x"] - det["width"]/2)
                    y1 = int(det["y"] - det["height"]/2)
                    x2 = int(det["x"] + det["width"]/2)
                    y2 = int(det["y"] + det["height"]/2)
                    _cv2.rectangle(vis, (x1, y1), (x2, y2), (240, 180, 41), 1)
                    _cv2.putText(vis, cls[:8], (x1, y1-5),
                                 _cv2.FONT_HERSHEY_SIMPLEX, 0.35, (240, 180, 41), 1)
            writer.write(vis)
        writer.release()
        print(f"[Debug] {out}")
    except Exception as e:
        print(f"[Debug] Render failed: {e}")


# ── Scoreboard OCR — crop overlay boxes and read via Gemini Flash ─────────────
def _ocr_scoreboard_crops(frame: Optional[np.ndarray],
                           boxes: List[dict],
                           w: int, h: int) -> dict:
    """
    Crop each scoreboard overlay region detected by Roboflow, stitch them
    horizontally, and send to Gemini Flash for OCR.

    Returns a dict:
        {"team_a": str, "score_a": int, "team_b": str, "score_b": int,
         "period": str, "shot_clock": str, "time_remaining": str}
    All values may be None. Returns {} on error or if Gemini is unavailable.
    """
    if not boxes or frame is None or _gemini_quota_exhausted:
        return {}
    try:
        import cv2 as _cv2, tempfile
        crops, labels = [], []
        for det in boxes:
            x1 = max(0, int(det["x"] - det["width"]  / 2) - 4)
            y1 = max(0, int(det["y"] - det["height"] / 2) - 4)
            x2 = min(w, int(det["x"] + det["width"]  / 2) + 4)
            y2 = min(h, int(det["y"] + det["height"] / 2) + 4)
            crop = frame[y1:y2, x1:x2]
            if crop.size == 0:
                continue
            # Scale up tiny crops so text is readable
            scale = max(1, round(60 / max(crop.shape[:2])))
            if scale > 1:
                crop = _cv2.resize(crop, (crop.shape[1]*scale, crop.shape[0]*scale),
                                   interpolation=_cv2.INTER_CUBIC)
            crops.append(crop)
            labels.append(det["class"])

        if not crops:
            return {}

        max_h   = max(c.shape[0] for c in crops)
        padded  = [_cv2.copyMakeBorder(c, 0, max_h - c.shape[0], 0, 6,
                                        _cv2.BORDER_CONSTANT, value=(20, 20, 20))
                   for c in crops]
        combined = np.hstack(padded)

        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tf:
            tmp = tf.name
        _cv2.imwrite(tmp, combined)

        vf = client.files.upload(file=tmp)
        while vf.state.name == "PROCESSING":
            time.sleep(1); vf = client.files.get(name=vf.name)

        prompt = (
            f"These are scoreboard overlay crops from a basketball broadcast, "
            f"left-to-right they are: {labels}. "
            "Read the text in each crop carefully — these may include team abbreviations, "
            "scores, quarter/period numbers, shot-clock digits, and game-clock times. "
            "Reply ONLY with compact JSON (no markdown, no extra text):\n"
            '{"team_a":"<name or null>","score_a":<int or null>,'
            '"team_b":"<name or null>","score_b":<int or null>,'
            '"period":"<e.g. Q2 or null>","shot_clock":"<e.g. 14 or null>",'
            '"time_remaining":"<e.g. 3:42 or null>"}'
        )
        resp = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=[prompt, vf]
        )
        client.files.delete(name=vf.name)
        try: os.remove(tmp)
        except: pass

        raw = resp.text.strip().strip("```json").strip("```").strip()
        result = json.loads(raw)
        print(f"[ScoreboardOCR] {result}")
        return result

    except Exception as e:
        print(f"[ScoreboardOCR] {e}")
        return {}




# ═══════════════════════════════════════════════════════════════════════════════
# AUDIO ENERGY — cheap excitement pre-filter
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


# ═══════════════════════════════════════════════════════════════════════════════
# DEAD-TIME DETECTION  (unchanged from v3 — no model dependency)
# ═══════════════════════════════════════════════════════════════════════════════

DEAD_TIME_AUDIO_FLOOR  = float(os.environ.get("DEAD_TIME_AUDIO_FLOOR",  "200"))
DEAD_TIME_MOTION_FLOOR = float(os.environ.get("DEAD_TIME_MOTION_FLOOR", "0.8"))


def _clip_audio_rms(clip_path: str) -> float:
    cmd = ["ffmpeg", "-y", "-i", clip_path,
           "-vn", "-ac", "1", "-ar", "8000", "-t", "10",
           "-f", "s16le", "pipe:1"]
    try:
        proc  = subprocess.run(cmd, capture_output=True, timeout=15)
        audio = np.frombuffer(proc.stdout, dtype=np.int16).astype(np.float32)
        return float(np.sqrt(np.mean(audio**2))) if len(audio) else 0.0
    except Exception:
        return 9999.0


def _clip_motion_score(clip_path: str) -> float:
    try:
        proc = subprocess.run([
            "ffmpeg", "-i", clip_path,
            "-vf", "freezedetect=n=0.003:d=3",
            "-f", "null", "-"
        ], capture_output=True, text=True, timeout=15)
        return 0.0 if "freeze_start" in proc.stderr else 9999.0
    except Exception:
        return 9999.0


def is_dead_time(clip_path: str) -> Tuple[bool, str]:
    rms    = _clip_audio_rms(clip_path)
    motion = _clip_motion_score(clip_path)
    print(f"[DeadTime] rms={rms:.1f}  motion={motion:.2f}")
    if rms < DEAD_TIME_AUDIO_FLOOR and motion < DEAD_TIME_MOTION_FLOOR:
        return True, f"quiet (rms={rms:.0f}) + static (motion={motion:.2f})"
    if rms < DEAD_TIME_AUDIO_FLOOR * 0.3:
        return True, f"deep silence (rms={rms:.0f})"
    if motion < DEAD_TIME_MOTION_FLOOR * 0.2:
        return True, f"frozen frame (motion={motion:.2f})"
    return False, ""


# ═══════════════════════════════════════════════════════════════════════════════
# GEMINI CALLS — dual-call architecture (unchanged from v3)
# ═══════════════════════════════════════════════════════════════════════════════

_gemini_quota_exhausted = False


def _is_quota_error(e: Exception) -> bool:
    s = str(e)
    return "429" in s or "RESOURCE_EXHAUSTED" in s or "quota" in s.lower()


def _upload_and_wait(path: str):
    vf = client.files.upload(file=path)
    while vf.state.name == "PROCESSING":
        time.sleep(2); vf = client.files.get(name=vf.name)
    return vf


def gemini_fast_json(video_file, yolo_summary: str, scoreboard_hint: dict = None) -> dict:
    sb_hint = ""
    if scoreboard_hint:
        sb_hint = f"\nScoreboard OCR pre-read: {json.dumps(scoreboard_hint)}"
    prompt = (
        "You are a basketball scorekeeper. Watch this clip and reply ONLY with JSON "
        "(no markdown, no extra text):\n"
        '{"score_a": <int or null>, "team_a": "<color/name or null>", '
        '"score_b": <int or null>, "team_b": "<color/name or null>", '
        '"period": "<Q1/Q2/Q3/Q4/OT or null>", '
        '"possession": "<team_a/team_b/null>", '
        '"play_type": "<score/turnover/rebound/foul/timeout/other/null>", '
        '"shot_made": <true/false/null>}\n\n'
        f"Roboflow tracking data:\n{yolo_summary}{sb_hint}\n\n"
        "If scoreboard OCR data is provided above, use it to confirm team names and scores. "
        "Visual confirmation from the video takes precedence."
    )
    try:
        resp = client.models.generate_content(model="gemini-2.5-flash",
                                              contents=[prompt, video_file])
        raw  = resp.text.strip().strip("```json").strip("```").strip()
        return json.loads(raw)
    except Exception:
        return {}


def gemini_qualitative(video_file, yolo_summary: str, scoreboard_hint: dict = None) -> str:
    sb_hint = ""
    if scoreboard_hint:
        sb_hint = (f"\nScoreboard OCR: {json.dumps(scoreboard_hint)} "
                   "(use these as the authoritative team names and scores)")
    prompt = (
        "You are an elite high school basketball scout analyzing NFHS-regulated "
        "varsity basketball (84×50 ft court). "
        "You have Roboflow tracking data: player bounding boxes, ball position, "
        "hoop location, and shot-near-hoop events. "
        "Write a 4-6 sentence scouting report covering:\n"
        "1. The key play or moment (scoring, turnover, defensive stop, etc.).\n"
        "2. Defensive scheme — zone or man-to-man, breakdowns or gaps exploited.\n"
        "3. Shooter mechanics if a scoring event occurred — release, footwork, balance.\n"
        "4. Ball movement — how the offense created the look, reads by the ball-handler.\n"
        "5. Any player standing out for hustle, positioning, or a key error.\n"
        "Be specific and technical. Reference jersey colors or positions. "
        "Do NOT repeat the Roboflow numbers verbatim.\n\n"
        f"Roboflow tracking context:\n{yolo_summary}{sb_hint}"
    )
    try:
        resp = client.models.generate_content(model="gemini-2.5-flash",
                                              contents=[prompt, video_file])
        return resp.text
    except Exception as e:
        return f"Gemini error: {e}"


def _yolo_only_report(yolo_metrics: dict) -> dict:
    """Roboflow-only report when Gemini quota is exhausted."""
    m          = yolo_metrics.get("metrics", {})
    zones      = m.get("zone_occupancy", {})
    players    = m.get("players", {})
    scoreboard = m.get("scoreboard", {})

    lines = ["⚠️ Gemini unavailable (quota exhausted) — Roboflow data only.\n"]
    lines.append(f"**{m.get('player_count', 0)} players tracked** "
                 f"({m.get('avg_players_in_frame', 0)} avg/frame)")

    shot_events = m.get("shot_near_hoop_events", 0)
    if shot_events:
        lines.append(f"**{shot_events} ball-near-hoop event(s) detected**")

    shots = sum(1 for p in players.values() if p.get("shot_detected"))
    if shots:
        shooters = [f"P{tid}" for tid, p in players.items() if p.get("shot_detected")]
        lines.append(f"**{shots} closest player(s) to shot events:** {', '.join(shooters)}")

    if m.get("ball_detected"):
        lines.append(f"**Ball detected** — {m.get('ball_possession_side','?')} side of court")

    if zones:
        top = sorted(zones.items(), key=lambda x: -x[1])[:3]
        lines.append("**Most active zones:** " +
                     ", ".join(f"{ZONE_LABELS.get(z,z)} ({c})" for z, c in top))

    if players:
        lines.append("\n**Player breakdown:**")
        for tid, p in list(players.items())[:8]:
            label     = f"#{p['jersey']}" if p.get("jersey") else f"P{tid}"
            shot_flag = " 🏀 SHOT-NEAR" if p.get("shot_detected") else ""
            zone      = ZONE_LABELS.get(p.get("primary_zone", ""), p.get("primary_zone", ""))
            lines.append(f"  • {label} — {zone}{shot_flag}")

    # Build score_update from scoreboard OCR if available
    score_update = None
    if scoreboard.get("team_a") and scoreboard.get("score_a") is not None:
        score_update = (f"{scoreboard['team_a']} {scoreboard['score_a']} – "
                        f"{scoreboard.get('team_b','?')} {scoreboard.get('score_b','?')}")
        lines.append(f"\n**Scoreboard (OCR):** {score_update}")

    return {
        "report":       "\n".join(lines),
        "score_update": score_update,
        "period":       scoreboard.get("period"),
        "play_type":    None,
        "possession":   None,
        "shot_made":    shot_events > 0,
        "structured":   scoreboard,
        "gemini_used":  False,
    }


def gemini_scout(clip_path: str, yolo_metrics: dict) -> dict:
    global _gemini_quota_exhausted
    if _gemini_quota_exhausted:
        print("[Gemini] Quota exhausted — returning Roboflow-only report.")
        return _yolo_only_report(yolo_metrics)

    yolo_summary = yolo_metrics.get("summary", "No tracking data.")
    scoreboard   = yolo_metrics.get("metrics", {}).get("scoreboard", {})
    vf = None
    try:
        vf = _upload_and_wait(clip_path)
        fast_result, qual_result = {}, ""

        def _fast():
            nonlocal fast_result
            fast_result = gemini_fast_json(vf, yolo_summary, scoreboard_hint=scoreboard)

        def _qual():
            nonlocal qual_result
            qual_result = gemini_qualitative(vf, yolo_summary, scoreboard_hint=scoreboard)

        t1 = threading.Thread(target=_fast)
        t2 = threading.Thread(target=_qual)
        t1.start(); t2.start()
        t1.join(); t2.join()
        client.files.delete(name=vf.name)

        # Build score string: prefer Gemini's visual read, fall back to OCR
        score_update = None
        if fast_result.get("team_a") and fast_result.get("score_a") is not None:
            score_update = (f"{fast_result['team_a']} {fast_result['score_a']} – "
                            f"{fast_result.get('team_b','?')} {fast_result.get('score_b','?')}")
        elif scoreboard.get("team_a") and scoreboard.get("score_a") is not None:
            score_update = (f"{scoreboard['team_a']} {scoreboard['score_a']} – "
                            f"{scoreboard.get('team_b','?')} {scoreboard.get('score_b','?')}")

        period = fast_result.get("period") or scoreboard.get("period")

        print(f"[Gemini] Score:{score_update or '—'}  "
              f"Period:{period or '?'}  "
              f"Play:{fast_result.get('play_type','?')}  "
              f"Made:{fast_result.get('shot_made','?')}  "
              f"Scoreboard OCR:{bool(scoreboard)}")

        return {
            "report":       qual_result,
            "score_update": score_update,
            "period":       period,
            "play_type":    fast_result.get("play_type"),
            "possession":   fast_result.get("possession"),
            "shot_made":    fast_result.get("shot_made"),
            "structured":   {**fast_result, **({k: scoreboard[k] for k in scoreboard
                                                if k not in fast_result or fast_result[k] is None})},
            "gemini_used":  True,
        }
    except Exception as e:
        if _is_quota_error(e):
            _gemini_quota_exhausted = True
            print("[Gemini] Quota exhausted — switching to Roboflow-only mode.")
            if vf:
                try: client.files.delete(name=vf.name)
                except: pass
            return _yolo_only_report(yolo_metrics)
        return {"report": f"Gemini error: {e}", "score_update": None,
                "period": None, "play_type": None, "possession": None,
                "shot_made": None, "structured": {}, "gemini_used": False}


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
    except Exception:
        return {"excitement": 5, "is_viral": False,
                "reason": "Analysis failed.", "score_update": None, "period": None}


# ═══════════════════════════════════════════════════════════════════════════════
# CORE ANALYSIS — 10-second bucket
# ═══════════════════════════════════════════════════════════════════════════════

def analyze_time_bucket(source_path: str, bucket_start: float,
                        cache_prefix: str = "bucket",
                        force: bool = False) -> dict:
    clip_name   = f"cache_clips/{cache_prefix}_{int(bucket_start)}.mp4"
    report_name = f"cache_reports/{cache_prefix}_{int(bucket_start)}.json"

    if force:
        for f in [clip_name, report_name]:
            try: os.remove(f)
            except: pass
    elif os.path.exists(report_name):
        print(f"[Cache HIT] {cache_prefix}_{int(bucket_start)}s")
        with open(report_name) as f: return json.load(f)

    print(f"[Cache MISS] {cache_prefix}_{int(bucket_start)}s — slicing clip")
    subprocess.run([
        "ffmpeg", "-y", "-ss", str(bucket_start), "-i", source_path,
        "-t", "10", "-c:v", CODEC, clip_name
    ], capture_output=True)
    if not os.path.exists(clip_name):
        return {"report": f"FFmpeg failed at {bucket_start}s.",
                "score_update": None, "period": None}

    dead, reason = is_dead_time(clip_name)
    if dead:
        print(f"[DeadTime] Skipping — {reason}")
        result = {
            "report": f"⏸ Broadcast pause detected ({reason}). Analysis skipped.",
            "score_update": None, "period": None,
            "dead_time": True, "dead_reason": reason,
        }
        with open(report_name, "w") as f: json.dump(result, f)
        try: os.remove(clip_name)
        except: pass
        return result

    print("[Roboflow] Running detection pipeline...")
    yolo_metrics = extract_yolo_metrics(clip_name, session_id=cache_prefix)

    print("[Gemini] Running dual-call analysis...")
    result = gemini_scout(clip_name, yolo_metrics)
    result["dead_time"]    = False
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
    print(f"[Viral] {len(candidates)} candidates from audio energy analysis")

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
                "period":       r.get("period"),
            })

    confirmed.sort(key=lambda x: -x["excitement"])
    with open(viral_cache, "w") as f: json.dump({"moments": confirmed}, f)
    return confirmed


# ═══════════════════════════════════════════════════════════════════════════════
# YOUTUBE LIVE BUFFER THREAD
# ═══════════════════════════════════════════════════════════════════════════════

def _verify_ytdlp() -> bool:
    try:
        r = subprocess.run(["yt-dlp", "--version"], capture_output=True, timeout=5)
        return r.returncode == 0
    except FileNotFoundError:
        return False


def _live_buffer_worker(session_id: str, youtube_url: str, auto_analyze: bool):
    session = live_sessions[session_id]
    stop    = session["stop_event"]
    buf_dir = f"live_buffers/{session_id}"
    os.makedirs(buf_dir, exist_ok=True)

    try:
        session["status"] = "resolving"
        video_id   = _extract_video_id(youtube_url)
        stream_url = None

        # Method 1: YouTube Data API
        try:
            stream_url = get_live_hls_url(video_id)
            if stream_url:
                print(f"[Live {session_id[:8]}] HLS via YouTube Data API ✓")
        except Exception as e:
            print(f"[Live {session_id[:8]}] Data API failed: {e}")

        # Method 2: yt-dlp fallback
        if not stream_url:
            print(f"[Live {session_id[:8]}] Trying yt-dlp fallback...")
            cookies_from = os.environ.get("YTDLP_COOKIES_FROM_BROWSER", "")
            cmd = ["yt-dlp"]
            if cookies_from:
                cmd += ["--cookies-from-browser", cookies_from]
            cmd += ["-f", "best[ext=mp4]/best", "-g", youtube_url]
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if r.returncode == 0 and r.stdout.strip():
                stream_url = r.stdout.strip().split("\n")[0]
                print(f"[Live {session_id[:8]}] yt-dlp fallback ✓")
            else:
                session["status"] = "error"
                session["error"]  = (
                    "Could not resolve stream URL. "
                    "Check YOUTUBE_API_KEY in .env and confirm the stream is live."
                )
                return

        session["status"] = "buffering"

        seg_queue  = []
        queue_lock = threading.Lock()
        analysis_done = threading.Event()

        def _analysis_worker():
            while not stop.is_set() or seg_queue:
                with queue_lock:
                    item = seg_queue.pop(0) if seg_queue else None
                if item is None:
                    time.sleep(0.5); continue

                idx, path = item["idx"], item["path"]
                dead, reason = is_dead_time(path)
                if dead:
                    result = {
                        "report": f"⏸ Broadcast pause ({reason}).",
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

                yolo_metrics = extract_yolo_metrics(path)
                result = gemini_scout(path, yolo_metrics)
                result.update({
                    "segment":      idx,
                    "wall_ts":      time.time(),
                    "dead_time":    False,
                    "yolo_metrics": yolo_metrics.get("metrics", {}),
                })
                session["results"].append(result)
                session["latest"] = result
                session["status"] = "live"

                try: os.remove(path)
                except: pass

            analysis_done.set()

        if auto_analyze:
            threading.Thread(target=_analysis_worker, daemon=True).start()

        seg_index = 0
        while not stop.is_set():
            seg_path = f"{buf_dir}/seg_{seg_index:04d}.mp4"
            subprocess.run([
                "ffmpeg", "-y", "-i", stream_url,
                "-t", "10", "-c:v", CODEC, "-c:a", "aac", seg_path
            ], capture_output=True, timeout=40)

            if stop.is_set(): break

            if not os.path.exists(seg_path) or os.path.getsize(seg_path) < 1000:
                print(f"[Live {session_id[:8]}] Segment {seg_index} empty, retrying...")
                time.sleep(3); continue

            if auto_analyze:
                with queue_lock:
                    if len(seg_queue) > 4:
                        dropped = seg_queue.pop(0)
                        try: os.remove(dropped["path"])
                        except: pass
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
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _is_uuid(s: str) -> bool:
    try: uuid.UUID(s); return True
    except ValueError: return False


def resolve_source(video_source: str) -> Optional[str]:
    if _is_uuid(video_source):
        matches = glob.glob(f"uploads/{video_source}.*")
        return matches[0] if matches else None
    if os.path.exists(video_source):
        return video_source
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════

@app.post("/api/upload_clip")
async def upload_clip(file: UploadFile = File(...)):
    if not file.content_type or not file.content_type.startswith("video/"):
        raise HTTPException(400, "Must be a video file.")

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

    print(f"[Upload] {file.filename} → {dest} ({size/1e6:.1f} MB)")
    return {
        "session_id":  session_id,
        "filename":    file.filename,
        "size_mb":     round(size / 1e6, 2),
        "preview_url": f"/uploads/{session_id}{ext}",
    }


@app.post("/api/check_dead_time")
def check_dead_time(request: DeadTimeRequest):
    source_path = resolve_source(request.video_source)
    if source_path is None:
        raise HTTPException(404, f"Source not found: {request.video_source}")

    bucket_start = math.floor(request.current_time / 10.0) * 10
    tmp_clip     = f"cache_clips/deadcheck_{int(bucket_start)}_{os.getpid()}.mp4"
    subprocess.run([
        "ffmpeg", "-y", "-ss", str(bucket_start), "-i", source_path,
        "-t", "10", "-c:v", "libx264", "-preset", "ultrafast", tmp_clip
    ], capture_output=True, timeout=20)

    if not os.path.exists(tmp_clip):
        return {"dead_time": False, "reason": "", "bucket_start": bucket_start}

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
    result       = analyze_time_bucket(source_path, bucket_start,
                                       cache_prefix=prefix, force=request.force)
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
    cache_key = (f"viral_{request.video_source}" if _is_uuid(request.video_source)
                 else "viral_scan")
    moments   = scan_viral_moments(source_path, request.sensitivity, cache_key)
    return {"viral_moments": moments, "count": len(moments)}


@app.post("/api/start_live_session")
def start_live_session(request: LiveSessionRequest):
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
def stop_live_session_endpoint(session_id: str):
    s = live_sessions.get(session_id)
    if not s:
        raise HTTPException(404, "Session not found.")
    s["stop_event"].set()
    shutil.rmtree(f"live_buffers/{session_id}", ignore_errors=True)
    del live_sessions[session_id]
    return {"status": "stopped"}


@app.post("/api/analyze_youtube_vod")
def analyze_youtube_vod(request: YoutubeVodRequest):
    import hashlib
    cache_key   = hashlib.md5(
        f"{request.youtube_url}:{int(request.start_time)}".encode()
    ).hexdigest()[:12]
    clip_path   = f"cache_clips/vod_{cache_key}.mp4"
    report_path = f"cache_reports/vod_{cache_key}.json"

    if os.path.exists(report_path):
        with open(report_path) as f: return json.load(f)

    video_id = _extract_video_id(request.youtube_url)
    try:
        info = get_video_info(video_id)
        if info.get("is_live"):
            raise HTTPException(400, "Live stream — use /api/start_live_session instead.")
    except HTTPException:
        raise
    except Exception as e:
        print(f"[VOD] Metadata fetch (non-fatal): {e}")

    downloaded = False
    last_error = "No download method succeeded."
    yt_url     = f"https://www.youtube.com/watch?v={video_id}"
    start      = int(request.start_time)

    # Method 1: streamlink
    if not downloaded and _verify_streamlink():
        try:
            sl_tmp = f"cache_clips/sl_full_{cache_key}.mp4"
            sl = subprocess.run([
                "streamlink", "-o", sl_tmp, "--force", yt_url, "best"
            ], capture_output=True, timeout=120)
            if os.path.exists(sl_tmp) and os.path.getsize(sl_tmp) > 1000:
                subprocess.run([
                    "ffmpeg", "-y", "-ss", str(start), "-i", sl_tmp,
                    "-t", "30", "-c:v", CODEC, "-c:a", "aac", clip_path
                ], capture_output=True, timeout=60)
                try: os.remove(sl_tmp)
                except: pass
                if os.path.exists(clip_path) and os.path.getsize(clip_path) > 1000:
                    downloaded = True; print("[VOD] streamlink ✓")
        except Exception as e:
            last_error = f"streamlink: {e}"

    # Method 2: pytubefix
    if not downloaded and _PYTUBEFIX_AVAILABLE:
        try:
            yt     = PyTube(yt_url, use_oauth=True, allow_oauth_cache=True)
            tmp_full = f"cache_clips/vod_full_{cache_key}.mp4"
            stream = (
                yt.streams.filter(progressive=True, file_extension="mp4")
                  .order_by("resolution").desc().first()
                or yt.streams.filter(file_extension="mp4", only_video=True)
                  .order_by("resolution").desc().first()
            )
            if stream:
                stream.download(filename=tmp_full)
                subprocess.run([
                    "ffmpeg", "-y", "-ss", str(start), "-i", tmp_full,
                    "-t", "30", "-c:v", CODEC, "-c:a", "aac", clip_path
                ], capture_output=True, timeout=120)
                try: os.remove(tmp_full)
                except: pass
                if os.path.exists(clip_path) and os.path.getsize(clip_path) > 1000:
                    downloaded = True; print("[VOD] pytubefix ✓")
        except Exception as e:
            last_error = f"pytubefix: {e}"

    # Method 3: yt-dlp
    if not downloaded and _verify_ytdlp():
        try:
            cookies_from = os.environ.get("YTDLP_COOKIES_FROM_BROWSER", "")
            cmd = ["yt-dlp"]
            if cookies_from:
                cmd += ["--cookies-from-browser", cookies_from]
            cmd += ["-f", "best[ext=mp4]/best",
                    "--download-sections", f"*{start}-{start+30}",
                    "-o", clip_path, yt_url]
            dl = subprocess.run(cmd, capture_output=True, timeout=180)
            if os.path.exists(clip_path) and os.path.getsize(clip_path) > 1000:
                downloaded = True; print("[VOD] yt-dlp ✓")
            else:
                last_error = dl.stderr.decode(errors="replace")[:300]
        except Exception as e:
            last_error = f"yt-dlp: {e}"

    if not downloaded:
        raise HTTPException(500,
            f"All download methods failed. Last error: {last_error}\n"
            "Try: pip install streamlink")

    dead, reason = is_dead_time(clip_path)
    if dead:
        return {"report": f"No active play detected ({reason}).", "dead_time": True,
                "score_update": None, "period": None, "yolo_metrics": {}}

    yolo_metrics = extract_yolo_metrics(clip_path)
    result       = gemini_scout(clip_path, yolo_metrics)
    result.update({
        "dead_time":    False,
        "yolo_metrics": yolo_metrics.get("metrics", {}),
        "source":       "youtube_vod",
        "start_time":   request.start_time,
    })

    with open(report_path, "w") as f: json.dump(result, f)
    try:
        if os.path.exists(clip_path): os.remove(clip_path)
    except: pass

    return result


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


@app.get("/api/debug_renders")
def list_debug_renders():
    files = sorted(glob.glob("debug_renders/*_debug.mp4"),
                   key=os.path.getmtime, reverse=True)
    return {
        "renders": [
            {"filename": os.path.basename(f),
             "url":      f"/debug/{os.path.basename(f)}",
             "size_mb":  round(os.path.getsize(f) / 1e6, 2),
             "created":  os.path.getmtime(f)}
            for f in files
        ]
    }


@app.get("/health")
def health():
    return {
        "status":           "ok",
        "device":           DEVICE,
        "codec":            CODEC,
        "roboflow_key_set": bool(ROBOFLOW_API_KEY),
        "roboflow_model":   f"{RF_PROJECT}/{RF_VERSION}",
        "rf_calls_total":   _rf_call_count,
        "ytdlp_available":  _verify_ytdlp(),
        "active_sessions":  len(live_sessions),
        "gemini_quota_ok":  not _gemini_quota_exhausted,
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
        print(f"\n{'='*60}\n Public URL: {tunnel.public_url}\n{'='*60}\n")
    else:
        print("\n[Tip] Set NGROK_AUTHTOKEN in .env to auto-expose this server.")
        print("      Or: cloudflared tunnel --url http://localhost:8000\n")

    print("Starting Nutmeg AI Scouting Engine v4 on port 8000...")
    uvicorn.run(app, host="0.0.0.0", port=8000)