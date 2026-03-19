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

import os, time, subprocess, glob, math, json, re, shutil, uuid, threading, collections, urllib.request, urllib.parse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional, List, Dict

import numpy as np
try:
    from pytubefix import YouTube as PyTube
    from pytubefix.cli import on_progress
    _PYTUBEFIX_AVAILABLE = True
except ImportError:
    _PYTUBEFIX_AVAILABLE = False

def _verify_streamlink() -> bool:
    try:
        r = subprocess.run(["streamlink", "--version"], capture_output=True, timeout=5)
        return r.returncode == 0
    except FileNotFoundError:
        return False
from google import genai
from ultralytics import YOLO
from dotenv import load_dotenv
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import uvicorn

# ── YouTube API helpers ───────────────────────────────────────────────────────
YT_API_KEY = os.environ.get("YOUTUBE_API_KEY", "")

def _extract_video_id(url_or_id: str) -> str:
    """Extract 11-char video ID from any YouTube URL format."""
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
    """Make a YouTube Data API v3 GET request. Raises on error."""
    if not YT_API_KEY:
        raise ValueError("YOUTUBE_API_KEY not set in .env")
    params["key"] = YT_API_KEY
    url = f"https://www.googleapis.com/youtube/v3/{endpoint}?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            return json.loads(r.read())
    except Exception as e:
        raise ValueError(f"YouTube API error: {e}")


def get_live_hls_url(video_id: str) -> Optional[str]:
    """
    Use the YouTube Data API to get the HLS manifest URL for a live stream.
    Returns None if the video is not currently live.
    """
    data = _yt_api_get("videos", {
        "part": "liveStreamingDetails,snippet",
        "id":   video_id,
    })
    items = data.get("items", [])
    if not items:
        return None
    details = items[0].get("liveStreamingDetails", {})
    hls_url = details.get("hlsManifestUrl")
    if not hls_url:
        # Not live or HLS not available
        return None
    print(f"[YouTube API] HLS URL found for {video_id}")
    return hls_url


def get_video_info(video_id: str) -> dict:
    """Return basic metadata for a video (title, duration, live status)."""
    data = _yt_api_get("videos", {
        "part": "snippet,contentDetails,liveStreamingDetails",
        "id":   video_id,
    })
    items = data.get("items", [])
    if not items:
        return {}
    item = items[0]
    snippet = item.get("snippet", {})
    details = item.get("liveStreamingDetails", {})
    return {
        "title":       snippet.get("title", ""),
        "channel":     snippet.get("channelTitle", ""),
        "is_live":     snippet.get("liveBroadcastContent") == "live",
        "hls_url":     details.get("hlsManifestUrl"),
        "duration":    item.get("contentDetails", {}).get("duration", ""),
    }


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
    force: bool = False

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


def _ytdlp_js_flags() -> list:
    """
    Detect an available JS runtime and return the correct yt-dlp flag.
    yt-dlp uses --js-runtimes (plural) as of recent versions.
    On Windows, node.exe may only be found via full path or 'node.exe'.
    """
    candidates = ["node", "node.exe", "nodejs", "deno"]
    for runtime in candidates:
        try:
            r = subprocess.run(
                [runtime, "--version"],
                capture_output=True, timeout=3,
                # Windows needs shell=False but PATH lookup
            )
            if r.returncode == 0:
                name = "node" if "node" in runtime else runtime
                print(f"[yt-dlp] JS runtime found: {runtime} → passing --js-runtimes {name}")
                return ["--js-runtimes", name]
        except FileNotFoundError:
            continue
        except Exception:
            continue
    # Last resort: pass node anyway — if it's on PATH yt-dlp will find it
    print("[yt-dlp] Runtime detection failed, passing --js-runtimes node as fallback")
    return ["--js-runtimes", "node"]


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
# YOLO METRICS — multi-model pipeline
#
# Models used:
#   yolo11m-pose.pt     — person pose estimation (keypoints for shot detection)
#   basketball-players  — Roboflow fine-tuned player detector (higher accuracy
#                         on court vs stands, handles occlusion better)
#   basketball-ball     — Roboflow ball detector (tracks ball position/possession)
#
# Reliable stats kept (low error margin, no uncalibrated conversions):
#   • Player count per frame (direct observation)
#   • Zone occupancy (pixel region membership, normalised)
#   • Shot attempt detection (boolean, wrist-above-shoulder heuristic)
#   • Ball possession side (left/right half of court)
#   • Ball detected (boolean per frame)
#   • Player trajectories (normalised coords for visualizer)
#   • Shot arc keypoint data (for visualizer only, not displayed as a number)
#
# Removed (too inaccurate without fixed camera + calibration rig):
#   • Speed in mph / ft/s  (pixel displacement varies wildly with zoom/angle)
#   • Distance covered in feet (same issue)
#   • Defensive spacing in feet (same issue)
#
# NFHS court: 84ft × 50ft (high school standard, not NBA 94ft)
# ═══════════════════════════════════════════════════════════════════════════════

# NFHS high school court dimensions
COURT_W_FT = 84.0
COURT_H_FT = 50.0

# Tunable via .env
YOLO_CONF         = float(os.environ.get("YOLO_CONF",         "0.45"))
BALL_CONF         = float(os.environ.get("BALL_CONF",         "0.35"))
MIN_PLAYER_HEIGHT = float(os.environ.get("MIN_PLAYER_HEIGHT", "0.10"))
SHOT_RISE_PX      = float(os.environ.get("SHOT_RISE_PX",      "40"))
SHOT_RISE_FRAMES  = int(os.environ.get("SHOT_RISE_FRAMES",    "5"))

# Roboflow model auto-download — weights cached to models/ on first run
os.makedirs("models", exist_ok=True)
ROBOFLOW_API_KEY  = os.environ.get("ROBOFLOW_API_KEY", "")
BALL_ROBOFLOW_ID  = os.environ.get("BALL_ROBOFLOW_ID",  "cricket-qnb5l/basketball-xil7x/1")
PLAYER_ROBOFLOW_ID= os.environ.get("PLAYER_ROBOFLOW_ID","")          # optional
BALL_MODEL_PATH   = os.environ.get("BALL_MODEL_PATH",   "")          # override with local .pt
PLAYER_MODEL_PATH = os.environ.get("PLAYER_MODEL_PATH", "")          # override with local .pt


def _roboflow_download(model_id: str) -> Optional[str]:
    """Download Roboflow weights via REST API, cache locally. Returns .pt path or None."""
    safe  = model_id.replace("/", "_")
    dest  = f"models/{safe}.pt"
    if os.path.exists(dest):
        return dest
    if not ROBOFLOW_API_KEY:
        print(f"[Roboflow] ROBOFLOW_API_KEY not set — cannot download {model_id}")
        return None
    try:
        parts = model_id.split("/")
        ws, proj, ver = parts[0], parts[1], parts[2] if len(parts) > 2 else "1"
        url = (f"https://api.roboflow.com/{ws}/{proj}/{ver}"
               f"/yolov8pytorch?api_key={ROBOFLOW_API_KEY}")
        with urllib.request.urlopen(url, timeout=15) as r:
            meta = json.loads(r.read())
        w_url = (meta.get("model", {}).get("weightsUrl")
                 or meta.get("weightsUrl"))
        if not w_url:
            raise ValueError(f"No weightsUrl in response")
        print(f"[Roboflow] Downloading {model_id} → {dest}")
        urllib.request.urlretrieve(w_url, dest)
        return dest
    except Exception as e:
        print(f"[Roboflow] Download failed for {model_id}: {e}")
        return None

# Court zone boundaries as (x_min, x_max, y_min, y_max) in normalised [0,1] coords.
# Calibrated for NFHS 84x50 court viewed from a standard mid-court elevated camera.
# Paint = 19ft deep (19/84 = 0.226), lane width = 12ft (12/50 = 0.24)
COURT_ZONES = {
    "paint_left":  (0.00, 0.226, 0.28, 0.72),
    "paint_right": (0.774, 1.00, 0.28, 0.72),
    "mid_range":   (0.226, 0.40, 0.00, 1.00),
    "perimeter":   (0.40,  0.60, 0.00, 1.00),
    "three_left":  (0.00, 0.226, 0.00, 0.28),
    "three_right": (0.774, 1.00, 0.72, 1.00),
    "backcourt":   (0.60,  1.00, 0.00, 1.00),
}


def _zone_for_point(x_pct: float, y_pct: float) -> str:
    for name, (x0, x1, y0, y1) in COURT_ZONES.items():
        if x0 <= x_pct <= x1 and y0 <= y_pct <= y1:
            return name
    return "perimeter"


def _load_ball_model():
    """Try local path, then Roboflow download, then None."""
    pt = BALL_MODEL_PATH if (BALL_MODEL_PATH and os.path.exists(BALL_MODEL_PATH)) else None
    if pt is None and BALL_ROBOFLOW_ID:
        pt = _roboflow_download(BALL_ROBOFLOW_ID)
    if pt:
        try:
            m = YOLO(pt)
            print(f"[YOLO] Ball detector loaded: {pt}")
            return m
        except Exception as e:
            print(f"[YOLO] Ball model load failed: {e}")
    print("[YOLO] Ball detector unavailable — ball tracking disabled.")
    return None


def _load_player_model():
    """Try local path, then Roboflow download, then pose-model fallback."""
    pt = PLAYER_MODEL_PATH if (PLAYER_MODEL_PATH and os.path.exists(PLAYER_MODEL_PATH)) else None
    if pt is None and PLAYER_ROBOFLOW_ID:
        pt = _roboflow_download(PLAYER_ROBOFLOW_ID)
    if pt:
        try:
            m = YOLO(pt)
            print(f"[YOLO] Player model loaded: {pt}")
            return m, False   # no keypoints
        except Exception as e:
            print(f"[YOLO] Player model failed: {e} — using pose fallback")
    print("[YOLO] Using yolo11m-pose.pt (pose model fallback).")
    return YOLO("yolo11m-pose.pt"), True


# ═══════════════════════════════════════════════════════════════════════════════
# PLAYER IDENTITY REGISTRY
# Persists stable player identities across YOLO track-ID changes between clips.
# Two matching strategies (fastest-first):
#   1. Jersey number (Gemini reads crop) — most reliable, cached permanently
#   2. Colour histogram — fast local matching for same-session re-ID
# ═══════════════════════════════════════════════════════════════════════════════
_player_registries: Dict[str, "PlayerRegistry"] = {}


class PlayerRegistry:
    def __init__(self):
        self.players: Dict[str, dict] = {}   # stable_id → data
        self._tid_map: Dict[int, str] = {}   # bytetrack_id → stable_id
        self._idx = 1

    def _new_sid(self, jersey: Optional[str]) -> str:
        if jersey:
            return f"#{jersey}"
        s = f"P{self._idx}"; self._idx += 1; return s

    def _hist_sim(self, a: list, b: list) -> float:
        if not a or not b: return 0.0
        a, b = np.array(a, np.float32), np.array(b, np.float32)
        a /= a.sum() + 1e-6; b /= b.sum() + 1e-6
        return float(np.sum(np.sqrt(a * b)))

    def compute_hist(self, frame: np.ndarray, box: list) -> list:
        x1,y1,x2,y2 = [int(v) for v in box]
        crop = frame[max(0,y1):y2, max(0,x1):x2]
        if crop.size == 0: return []
        hist = []
        for ch in range(3):
            h, _ = np.histogram(crop[:,:,ch], bins=16, range=(0,256))
            hist.extend(h.tolist())
        return hist

    def resolve(self, tid: int, jersey: Optional[str],
                hist: list, center: tuple, zone: str) -> str:
        if tid in self._tid_map:
            sid = self._tid_map[tid]
            self._update(sid, zone, center, hist); return sid
        # Jersey match
        if jersey:
            sid = f"#{jersey}"
            if sid not in self.players:
                self.players[sid] = {"track_ids":{tid},"jersey":jersey,
                                     "hist":hist,"zones":collections.Counter({zone:1}),
                                     "shots":0,"last_center":center}
            else:
                self.players[sid]["track_ids"].add(tid)
                self._update(sid, zone, center, hist)
            self._tid_map[tid] = sid; return sid
        # Appearance match
        best_sid, best_sim = None, 0.40
        for sid, d in self.players.items():
            sim = self._hist_sim(hist, d["hist"])
            if sim > best_sim: best_sim, best_sid = sim, sid
        if best_sid:
            self.players[best_sid]["track_ids"].add(tid)
            if not self.players[best_sid]["jersey"] and jersey:
                self.players[best_sid]["jersey"] = jersey
            self._update(best_sid, zone, center, hist)
            self._tid_map[tid] = best_sid; return best_sid
        # New player
        sid = self._new_sid(None)
        self.players[sid] = {"track_ids":{tid},"jersey":None,
                             "hist":hist,"zones":collections.Counter({zone:1}),
                             "shots":0,"last_center":center}
        self._tid_map[tid] = sid; return sid

    def _update(self, sid: str, zone: str, center: tuple, hist: list):
        d = self.players[sid]
        d["zones"][zone] += 1; d["last_center"] = center
        if hist:
            old = np.array(d["hist"] or hist, np.float32)
            d["hist"] = (old*0.8 + np.array(hist,np.float32)*0.2).tolist()

    def record_shot(self, sid: str):
        if sid in self.players: self.players[sid]["shots"] += 1

    def upgrade_jersey(self, sid: str, jersey: str):
        """Rename anonymous P-id to jersey number."""
        if sid not in self.players or not sid.startswith("P"): return sid
        new_sid = f"#{jersey}"
        if new_sid in self.players:
            # Merge
            self.players[new_sid]["track_ids"] |= self.players[sid]["track_ids"]
            del self.players[sid]
        else:
            self.players[new_sid] = self.players.pop(sid)
            self.players[new_sid]["jersey"] = jersey
        for k,v in self._tid_map.items():
            if v == sid: self._tid_map[k] = new_sid
        return new_sid

    def summary(self) -> dict:
        return {
            sid: {"jersey": d["jersey"],
                  "primary_zone": d["zones"].most_common(1)[0][0] if d["zones"] else "unknown",
                  "shots": d["shots"],
                  "last_center": d["last_center"]}
            for sid, d in self.players.items()
        }


def get_registry(session_id: str) -> PlayerRegistry:
    if session_id not in _player_registries:
        _player_registries[session_id] = PlayerRegistry()
    return _player_registries[session_id]


def _read_jersey(frame: np.ndarray, box: list) -> Optional[str]:
    """Send player crop to Gemini to read jersey number. Returns digit string or None."""
    import tempfile
    try:
        x1,y1,x2,y2 = [int(v) for v in box]
        pad = 15
        crop = frame[max(0,y1-pad):y2+pad, max(0,x1-pad):x2+pad]
        if crop.size == 0: return None
        import cv2 as _cv2
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tf:
            tmp = tf.name
        _cv2.imwrite(tmp, crop)
        vf = client.files.upload(file=tmp)
        while vf.state.name == "PROCESSING":
            time.sleep(1); vf = client.files.get(name=vf.name)
        resp = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=["What is the jersey number on this player? Reply with ONLY "
                      "the digits (e.g. '23'). If unreadable reply 'unknown'.", vf]
        )
        client.files.delete(name=vf.name)
        os.remove(tmp)
        text = resp.text.strip().strip("'\"").lower()
        return text if text.isdigit() else None
    except Exception as e:
        print(f"[Jersey] {e}"); return None


def _zone_for_point(x_pct: float, y_pct: float) -> str:
    for name, (x0, x1, y0, y1) in COURT_ZONES.items():
        if x0 <= x_pct <= x1 and y0 <= y_pct <= y1:
            return name
    return "perimeter"


def _detect_shot(kp_history: list) -> bool:
    """
    Shot heuristic: right wrist rises > SHOT_RISE_PX pixels over
    SHOT_RISE_FRAMES frames AND ends above the right shoulder.
    Both keypoints must be detected (non-zero).
    COCO indices: 6=R shoulder, 10=R wrist.
    """
    N = SHOT_RISE_FRAMES
    if len(kp_history) < N + 1:
        return False
    for j in range(N, len(kp_history)):
        kp_now  = kp_history[j]
        kp_prev = kp_history[j - N]
        if len(kp_now) < 11 or len(kp_prev) < 11:
            continue
        rw_now, rw_prev, rs_now = kp_now[10], kp_prev[10], kp_now[6]
        if rw_now[0] == 0 or rw_prev[0] == 0 or rs_now[0] == 0:
            continue
        if rw_prev[1] - rw_now[1] > SHOT_RISE_PX and rw_now[1] < rs_now[1]:
            return True
    return False


def extract_yolo_metrics(clip_path: str, fps: float = 30.0, session_id: str = "default") -> dict:
    """
    Multi-model YOLO pipeline:
      1. Pose model (yolo11m-pose.pt) — player tracking + keypoints for shot detection
      2. Ball detector (optional)     — basketball position + possession side

    Only reliable, low-error-margin stats are returned:
      - player_count, avg_players_in_frame
      - zone_occupancy (normalised pixel membership)
      - shot_attempts (boolean heuristic per player)
      - ball_detected, ball_possession_side
      - trajectories (for visualizer)
      - shot_arcs (for visualizer)

    Stats that require a calibrated fixed camera (speed, distance, spacing in ft)
    are intentionally omitted.
    """
    # ── 1. Player pose tracking ────────────────────────────────────────────
    player_model, has_kps = _load_player_model()

    pose_results = list(player_model.track(
        source=clip_path,
        tracker="bytetrack.yaml",
        device=DEVICE,
        stream=True,
        conf=YOLO_CONF,
        iou=0.5,
        classes=[0],          # person only
        half=HALF,
        imgsz=640,
        vid_stride=2,
        persist=True,
        verbose=False,
    ))

    if not pose_results:
        return {"metrics": {}, "summary": "No tracking data available."}

    h_px = pose_results[0].orig_shape[0]
    w_px = pose_results[0].orig_shape[1]

    import cv2 as _cv2
    registry = get_registry(session_id)
    # local_tracks: bytetrack_id → accumulated data for this clip
    local_tracks: Dict[int, dict] = collections.defaultdict(lambda: {
        "centers": [], "kp_history": [], "zones": [],
        "stable_id": None, "first_frame": None, "first_box": None,
    })
    frame_player_counts = []
    cap = _cv2.VideoCapture(clip_path)
    frame_num = 0

    for r in pose_results:
        if r.boxes is None:
            frame_num += 1; continue
        ids   = r.boxes.id
        boxes = r.boxes.xyxy
        kps   = r.keypoints.xy if (has_kps and r.keypoints is not None) else None
        if ids is None:
            frame_num += 1; continue

        cap.set(_cv2.CAP_PROP_POS_FRAMES, frame_num * 2)  # vid_stride=2
        ok, frame_bgr = cap.read()

        valid = 0
        for i, tid in enumerate(ids.int().tolist()):
            box       = boxes[i].tolist()
            box_h_pct = (box[3] - box[1]) / h_px
            if box_h_pct < MIN_PLAYER_HEIGHT:
                continue
            valid += 1
            cx   = (box[0] + box[2]) / 2 / w_px
            cy   = (box[1] + box[3]) / 2 / h_px
            zone = _zone_for_point(cx, cy)

            hist = registry.compute_hist(frame_bgr, box) if ok else []

            # Resolve stable ID via registry (appearance match)
            if local_tracks[tid]["stable_id"] is None:
                sid = registry.resolve(tid, None, hist, (cx, cy), zone)
                local_tracks[tid]["stable_id"] = sid
                # Save first good frame for jersey reading
                if ok and local_tracks[tid]["first_frame"] is None:
                    local_tracks[tid]["first_frame"] = frame_bgr.copy()
                    local_tracks[tid]["first_box"]   = box
            else:
                registry._update(local_tracks[tid]["stable_id"], zone, (cx, cy), hist)

            local_tracks[tid]["centers"].append((cx, cy))
            local_tracks[tid]["zones"].append(zone)
            if has_kps and kps is not None and kps.shape[1] >= 17:
                local_tracks[tid]["kp_history"].append(kps[i].tolist())

        if valid > 0:
            frame_player_counts.append(valid)
        frame_num += 1

    cap.release()

    # ── Jersey reading for anonymous players (max 4 Gemini calls per clip) ──
    jersey_calls = 0
    for tid, data in local_tracks.items():
        sid = data["stable_id"]
        if not sid or not sid.startswith("P"): continue
        if jersey_calls >= 4: break
        if data["first_frame"] is None: continue
        jersey = _read_jersey(data["first_frame"], data["first_box"])
        if jersey:
            new_sid = registry.upgrade_jersey(sid, jersey)
            data["stable_id"] = new_sid
            print(f"[Registry] Track #{tid} → Jersey #{jersey} ({sid} → {new_sid})")
        jersey_calls += 1

    # Use stable IDs as the tracking key going forward
    tracks = local_tracks

    # ── 2. Ball detection (optional model) ────────────────────────────────
    ball_model = _load_ball_model()
    ball_detections = []   # list of (cx_norm, cy_norm) per frame

    if ball_model is not None:
        ball_results = list(ball_model.predict(
            source=clip_path,
            device=DEVICE,
            stream=True,
            conf=BALL_CONF,
            imgsz=640,
            vid_stride=2,
            verbose=False,
        ))
        for r in ball_results:
            if r.boxes is None or len(r.boxes) == 0:
                ball_detections.append(None)
                continue
            # Take the highest-confidence detection
            best = r.boxes[r.boxes.conf.argmax()]
            bx   = best.xyxy[0].tolist()
            ball_cx = (bx[0] + bx[2]) / 2 / w_px
            ball_cy = (bx[1] + bx[3]) / 2 / h_px
            ball_detections.append((round(ball_cx, 3), round(ball_cy, 3)))

    # Ball summary stats
    detected_positions = [p for p in ball_detections if p is not None]
    ball_detected      = len(detected_positions) > 0
    # Possession side: which half of court does the ball spend more time in?
    ball_possession_side = None
    if detected_positions:
        avg_ball_x = float(np.mean([p[0] for p in detected_positions]))
        ball_possession_side = "left" if avg_ball_x < 0.5 else "right"

    # ── 3. Per-player metrics — keyed by stable registry ID ──────────────
    player_metrics = {}

    for tid, data in tracks.items():
        if len(data["centers"]) < 5:
            continue
        sid  = data.get("stable_id") or str(tid)
        shot = _detect_shot(data["kp_history"]) if has_kps else False
        if shot:
            registry.record_shot(sid)

        zone_counts  = collections.Counter(data["zones"])
        primary_zone = max(zone_counts, key=zone_counts.get) if zone_counts else "unknown"

        player_metrics[sid] = {
            "primary_zone":  primary_zone,
            "zone_pct":      {z: round(c/len(data["zones"])*100) for z,c in zone_counts.items()},
            "shot_detected": shot,
            "jersey":        registry.players.get(sid, {}).get("jersey"),
        }

    # ── 4. Team-level metrics ──────────────────────────────────────────────
    avg_players  = round(float(np.mean(frame_player_counts)), 1) if frame_player_counts else 0
    shot_attempts = sum(1 for p in player_metrics.values() if p["shot_detected"])

    all_zones    = [z for d in tracks.values() for z in d["zones"]]
    zone_summary = dict(collections.Counter(all_zones).most_common(4))

    # ── 5. Trajectory data for court visualizer ────────────────────────────
    trajectories = {}
    for tid, data in tracks.items():
        if len(data["centers"]) < 5:
            continue
        sid   = data.get("stable_id") or str(tid)
        trail = data["centers"][-60:]
        trajectories[sid] = [{"x": round(cx,4), "y": round(cy,4)} for cx,cy in trail]

    # Ball trajectory for visualizer
    ball_trail = [{"x": p[0], "y": p[1]} for p in detected_positions[-60:]]

    # ── 6. Shot arc keypoints for visualizer (not a displayed stat) ────────
    shot_arcs = {}
    for tid, data in tracks.items():
        sid = data.get("stable_id") or str(tid)
        pm  = player_metrics.get(sid)
        if not pm or not pm["shot_detected"]:
            continue
        arc = []
        for kp in data["kp_history"][-30:]:
            wy = kp[10][1] if len(kp) > 10 and kp[10][0] != 0 else None
            sy = kp[6][1]  if len(kp) > 6  and kp[6][0]  != 0 else None
            arc.append({"wrist_y": round(wy/h_px,4) if wy else None,
                        "shoulder_y": round(sy/h_px,4) if sy else None})
        shot_arcs[sid] = arc

    registry_summary = registry.summary()

    metrics = {
        # Reliable counts
        "player_count":         len(player_metrics),
        "avg_players_in_frame": avg_players,
        # Shot detection (boolean heuristic, not a count claim)
        "shot_attempts":        shot_attempts,
        # Ball tracking
        "ball_detected":        ball_detected,
        "ball_possession_side": ball_possession_side,
        "ball_trail":           ball_trail,
        # Zone occupancy
        "zone_occupancy":       zone_summary,
        "players":              player_metrics,
        # Visualizer data
        "trajectories":         trajectories,
        "shot_arcs":            shot_arcs,
    }

    metrics["registry"] = registry_summary

    lines = [
        f"YOLO tracked {len(player_metrics)} on-court players (stable IDs across clips).",
        f"Avg visible per frame: {avg_players}.",
        f"Shot attempts this clip: {shot_attempts}.",
    ]
    if ball_detected:
        lines.append(f"Ball detected — {ball_possession_side} side of court.")
    else:
        lines.append("Ball not detected (ball model may not be loaded).")
    lines.append(f"Zone activity: { {z: cnt for z, cnt in list(zone_summary.items())[:3]} }.")
    for sid, pm in list(player_metrics.items())[:6]:
        label = f"Jersey #{pm['jersey']}" if pm.get("jersey") else sid
        lines.append(f"  {label}: {pm['primary_zone']}"
                     + (" [SHOT]" if pm["shot_detected"] else ""))
    for sid, rp in list(registry_summary.items())[:5]:
        if rp["shots"] > 0:
            lines.append(f"  {sid} session shots: {rp['shots']}")
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
        "You are an elite high school basketball scout analyzing NFHS-regulated "
        "varsity basketball (84x50 ft court). "
        "You have YOLO tracking data including ball position and player zones. "
        "Watch the clip and write a 4-6 sentence scouting report covering:\n"
        "1. The key play or moment (scoring, turnover, defensive stop, etc.).\n"
        "2. Defensive scheme — zone or man-to-man, any breakdowns or gaps exploited.\n"
        "3. Shooter mechanics if a shot occurred — release point, arc, balance, footwork.\n"
        "4. Ball movement — how the offense created the look, any reads by the ball-handler.\n"
        "5. Any player standing out for hustle, positioning, or a key error.\n"
        "Be specific and technical. Reference jersey colors or positions. "
        "Do NOT repeat the YOLO numbers verbatim.\n\n"
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
                        cache_prefix: str = "bucket",
                        force: bool = False) -> dict:
    clip_name   = f"cache_clips/{cache_prefix}_{int(bucket_start)}.mp4"
    report_name = f"cache_reports/{cache_prefix}_{int(bucket_start)}.json"

    if force:
        for f in [clip_name, report_name]:
            try: os.remove(f)
            except: pass
        print(f"[FORCE] Re-analyzing {cache_prefix}_{int(bucket_start)}s")
    elif os.path.exists(report_name):
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
    yolo_metrics = extract_yolo_metrics(clip_name, session_id=cache_prefix)

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
        # ── Resolve HLS URL via YouTube Data API ──────────────────────────
        # The Data API returns hlsManifestUrl directly for live streams —
        # no yt-dlp, no JS challenge, no bot detection issues.
        video_id = _extract_video_id(youtube_url)
        stream_url = None

        try:
            stream_url = get_live_hls_url(video_id)
            if stream_url:
                print(f"[Live {session_id[:8]}] HLS URL from YouTube Data API: OK")
        except Exception as e:
            print(f"[Live {session_id[:8]}] Data API failed: {e} — trying yt-dlp fallback")

        # ── yt-dlp fallback if API fails ───────────────────────────────────
        if not stream_url:
            print(f"[Live {session_id[:8]}] Falling back to yt-dlp...")
            cookies_from = os.environ.get("YTDLP_COOKIES_FROM_BROWSER", "")
            live_cmd = ["yt-dlp"]
            if cookies_from:
                live_cmd += ["--cookies-from-browser", cookies_from]
            live_cmd += ["-f", "best[ext=mp4]/best", "-g", youtube_url]
            r = subprocess.run(live_cmd, capture_output=True, text=True, timeout=30)
            if r.returncode == 0 and r.stdout.strip():
                stream_url = r.stdout.strip().split("\n")[0]
                print(f"[Live {session_id[:8]}] yt-dlp fallback OK")
            else:
                session["status"] = "error"
                session["error"]  = (
                    "Could not resolve stream URL. "
                    "Check YOUTUBE_API_KEY in .env and that the stream is currently live."
                )
                return

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
    # ── Get video metadata from YouTube Data API ──────────────────────────
    video_id = _extract_video_id(request.youtube_url)
    try:
        info = get_video_info(video_id)
        print(f"[VOD] '{info.get('title', video_id)}' by {info.get('channel', '?')}")
        if info.get("is_live"):
            raise HTTPException(400, "This video is a live stream — use /api/start_live_session instead.")
    except HTTPException:
        raise
    except Exception as e:
        print(f"[VOD] Metadata fetch failed (non-fatal): {e}")

    # ── Download via pytubefix (OAuth, no JS challenge) ────────────────────
    # pytubefix on first run opens a browser tab for one-time OAuth login.
    # After that it caches the token in a local file automatically.
    # This completely bypasses YouTube bot detection and signature challenges.
    downloaded = False

    # Download cascade — three methods tried in order:
    #   1. streamlink   — most reliable for YouTube, no JS parsing needed
    #   2. pytubefix    — OAuth authenticated, good when streamlink not installed
    #   3. yt-dlp       — last resort
    downloaded  = False
    last_error  = "No download method succeeded."
    tmp_full    = f"cache_clips/vod_full_{cache_key}.mp4"
    yt_url      = f"https://www.youtube.com/watch?v={video_id}"
    start       = int(request.start_time)

    # ── Method 1: streamlink (pip install streamlink) ──────────────────────
    # streamlink fetches the HLS manifest directly — no JS signature parsing,
    # no bot detection, no OAuth needed. It pipes directly into FFmpeg.
    if not downloaded and _verify_streamlink():
        try:
            print("[VOD] Trying streamlink...")
            r = subprocess.run([
                "streamlink",
                "--stdout",               # pipe stream to stdout
                "-o", "-",
                yt_url,
                "best",
            ], capture_output=False, stdout=subprocess.PIPE, timeout=20)
            # Pipe streamlink stdout → ffmpeg for slicing
            proc = subprocess.Popen([
                "streamlink", "--stdout", yt_url, "best",
            ], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            slice_proc = subprocess.run([
                "ffmpeg", "-y",
                "-ss", str(start),
                "-i", "pipe:0",
                "-t", "30",
                "-c:v", CODEC, "-c:a", "aac",
                clip_path
            ], stdin=proc.stdout, capture_output=True, timeout=120)
            proc.stdout.close()
            proc.wait()
            if os.path.exists(clip_path) and os.path.getsize(clip_path) > 1000:
                downloaded = True
                print("[VOD] streamlink OK")
        except Exception as e:
            last_error = f"streamlink: {e}"
            print(f"[VOD] streamlink failed: {e}")

    # ── Method 2: pytubefix (OAuth) ───────────────────────────────────────
    if not downloaded and _PYTUBEFIX_AVAILABLE:
        try:
            print("[VOD] Trying pytubefix (OAuth)...")
            yt = PyTube(yt_url, use_oauth=True, allow_oauth_cache=True)
            print("[VOD] If prompted below, open the URL and enter the code.")
            stream = (
                yt.streams.filter(progressive=True, file_extension="mp4")
                  .order_by("resolution").desc().first()
                or
                yt.streams.filter(file_extension="mp4", only_video=True)
                  .order_by("resolution").desc().first()
            )
            if stream:
                stream.download(filename=tmp_full)
                subprocess.run([
                    "ffmpeg", "-y",
                    "-ss", str(start), "-i", tmp_full,
                    "-t", "30", "-c:v", CODEC, "-c:a", "aac", clip_path
                ], capture_output=True, timeout=120)
                try: os.remove(tmp_full)
                except: pass
                if os.path.exists(clip_path) and os.path.getsize(clip_path) > 1000:
                    downloaded = True
                    print("[VOD] pytubefix OK")
                else:
                    last_error = "pytubefix: FFmpeg slice failed"
            else:
                last_error = "pytubefix: no suitable stream found"
        except Exception as e:
            last_error = f"pytubefix: {e}"
            print(f"[VOD] pytubefix failed: {e}")

    # ── Method 3: yt-dlp ─────────────────────────────────────────────────
    if not downloaded:
        try:
            print("[VOD] Trying yt-dlp...")
            cookies_from = os.environ.get("YTDLP_COOKIES_FROM_BROWSER", "")
            cmd = ["yt-dlp"]
            if cookies_from:
                cmd += ["--cookies-from-browser", cookies_from]
            cmd += [
                "-f", "best[ext=mp4]/best",
                "--download-sections", f"*{start}-{start+30}",
                "-o", clip_path,
                yt_url,
            ]
            dl = subprocess.run(cmd, capture_output=True, timeout=180)
            if os.path.exists(clip_path) and os.path.getsize(clip_path) > 1000:
                downloaded = True
                print("[VOD] yt-dlp OK")
            else:
                last_error = dl.stderr.decode(errors="replace")[:300]
        except Exception as e:
            last_error = f"yt-dlp: {e}"

    if not downloaded:
        raise HTTPException(500,
            f"All download methods failed. Last error: {last_error}\n"
            "Install streamlink for best results: pip install streamlink"
        )

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
    result       = analyze_time_bucket(source_path, bucket_start, cache_prefix=prefix, force=request.force)
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