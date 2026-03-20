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
load_dotenv()  # must run before reading os.environ
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
os.makedirs("debug_renders", exist_ok=True)
app.mount("/debug", StaticFiles(directory="debug_renders"), name="debug")

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

# Zone boundaries: (x_min, x_max, y_min, y_max) normalised [0,1]
# Paint depth = 19ft (19/84 = 0.226), lane half-width = 6ft (6/50 = 0.12)
COURT_ZONES = {
    "paint_left":  (0.00,  0.226, 0.28, 0.72),
    "paint_right": (0.774, 1.00,  0.28, 0.72),
    "mid_range":   (0.226, 0.40,  0.00, 1.00),
    "perimeter":   (0.40,  0.60,  0.00, 1.00),
    "three_left":  (0.00,  0.226, 0.00, 0.28),
    "three_right": (0.774, 1.00,  0.72, 1.00),
    "backcourt":   (0.60,  1.00,  0.00, 1.00),
}

# Tunable via .env
YOLO_CONF         = float(os.environ.get("YOLO_CONF",         "0.35"))
MIN_PLAYER_HEIGHT = float(os.environ.get("MIN_PLAYER_HEIGHT", "0.05"))
SHOT_RISE_PX      = float(os.environ.get("SHOT_RISE_PX",      "40"))
SHOT_RISE_FRAMES  = int(os.environ.get("SHOT_RISE_FRAMES",    "5"))
YOLO_DEBUG        = os.environ.get("YOLO_DEBUG", "0") == "1"  # set YOLO_DEBUG=1 in .env
YOLO_DEBUG_DIR    = os.environ.get("YOLO_DEBUG_DIR", "debug_renders")  # output folder

# ── Roboflow hosted inference config ─────────────────────────────────────────
# Uses Roboflow's cloud API — no model download needed.
# Model: roboflow-universe-projects/basketball-players-fy4c2/25
# Classes: ball(0), made(1), player(2), rim(3)
ROBOFLOW_API_KEY    = os.environ.get("ROBOFLOW_API_KEY", "")
RF_PROJECT          = os.environ.get("RF_PROJECT",    "basketball-players-fy4c2")
RF_WORKSPACE        = os.environ.get("RF_WORKSPACE",  "roboflow-universe-projects")
RF_VERSION          = int(os.environ.get("RF_VERSION", "25"))
RF_BALL_CLASS       = os.environ.get("RF_BALL_CLASS",   "ball")
RF_PLAYER_CLASS     = os.environ.get("RF_PLAYER_CLASS", "player")
RF_RIM_CLASS        = os.environ.get("RF_RIM_CLASS",    "rim")
RF_MADE_CLASS       = os.environ.get("RF_MADE_CLASS",   "made")
RF_CONF             = float(os.environ.get("RF_CONF",   "0.40"))

# Roboflow inference endpoint (built lazily so .env key is always current)
_rf_session = None   # requests.Session, lazy-init

def _get_rf_url() -> str:
    return (f"https://detect.roboflow.com/{RF_PROJECT}/{RF_VERSION}"
            f"?api_key={ROBOFLOW_API_KEY}")

def _log_rf_status():
    if ROBOFLOW_API_KEY:
        print(f"[Roboflow] Hosted inference ready: {RF_WORKSPACE}/{RF_PROJECT} v{RF_VERSION}")
        print(f"[Roboflow] Classes: {RF_BALL_CLASS}(ball) {RF_PLAYER_CLASS}(player) {RF_RIM_CLASS}(rim) {RF_MADE_CLASS}(made)")
    else:
        print("[Roboflow] WARNING: ROBOFLOW_API_KEY not set — ball/rim tracking disabled.")
        print("[Roboflow] Add ROBOFLOW_API_KEY=your_key to .env")

_log_rf_status()  # runs at startup, after load_dotenv()
_rf_call_count = 0

def _rf_predict_frame(frame_bgr: "np.ndarray") -> list:
    """
    Send one BGR frame to the Roboflow hosted inference API.
    Returns list of dicts: [{class, confidence, x, y, width, height}, ...]
    Coordinates are centre-x, centre-y, width, height in PIXELS.
    Returns [] on error or if API key not set.
    """
    global _rf_session, _rf_call_count
    if not ROBOFLOW_API_KEY:
        return []
    try:
        import requests as _req, cv2 as _cv2, base64 as _b64
        if _rf_session is None:
            _rf_session = _req.Session()
        _, buf = _cv2.imencode(".jpg", frame_bgr, [_cv2.IMWRITE_JPEG_QUALITY, 85])
        b64 = _b64.b64encode(buf.tobytes()).decode("utf-8")
        resp = _rf_session.post(
            _get_rf_url(),
            data=b64,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            params={"confidence": RF_CONF, "overlap": 30},
            timeout=5,
        )
        resp.raise_for_status()
        preds = resp.json().get("predictions", [])
        _rf_call_count += 1
        # Log first call and every 10th so you can confirm it's working
        if _rf_call_count == 1 or _rf_call_count % 10 == 0:
            classes_seen = list({p["class"] for p in preds})
            print(f"[Roboflow] API call #{_rf_call_count} → {len(preds)} predictions {classes_seen}")
        return preds
    except Exception as e:
        print(f"[Roboflow] Inference error: {e}")
        return []


def _load_player_model():
    """Always use yolo11m-pose for player tracking — gives keypoints for shot detection."""
    print("[YOLO] Loading yolo11m-pose.pt for player tracking + keypoints.")
    return YOLO("yolo11m-pose.pt"), True


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



# ═══════════════════════════════════════════════════════════════════════════════
# PLAYER IDENTITY REGISTRY — stable IDs across YOLO track resets
# ═══════════════════════════════════════════════════════════════════════════════
_player_registries: Dict[str, "PlayerRegistry"] = {}
_gemini_quota_exhausted = False  # set True on first 429; clears on restart

class PlayerRegistry:
    def __init__(self):
        self.players: Dict[str, dict] = {}
        self._tid_map: Dict[int, str] = {}
        self._idx = 1

    def _new_sid(self) -> str:
        s = f"P{self._idx}"; self._idx += 1; return s

    def _hist_sim(self, a: list, b: list) -> float:
        if not a or not b: return 0.0
        a, b = np.array(a, np.float32), np.array(b, np.float32)
        a /= a.sum()+1e-6; b /= b.sum()+1e-6
        return float(np.sum(np.sqrt(a*b)))

    def compute_hist(self, frame: np.ndarray, box: list) -> list:
        x1,y1,x2,y2 = [int(v) for v in box]
        crop = frame[max(0,y1):y2, max(0,x1):x2]
        if crop.size == 0: return []
        hist = []
        for ch in range(3):
            h,_ = np.histogram(crop[:,:,ch], bins=16, range=(0,256))
            hist.extend(h.tolist())
        return hist

    def resolve(self, tid: int, jersey: Optional[str], hist: list,
                center: tuple, zone: str) -> str:
        if tid in self._tid_map:
            sid = self._tid_map[tid]; self._update(sid,zone,center,hist); return sid
        if jersey:
            sid = f"#{jersey}"
            if sid not in self.players:
                self.players[sid] = {"track_ids":{tid},"jersey":jersey,"hist":hist,
                                     "zones":collections.Counter({zone:1}),"shots":0,"last_center":center}
            else:
                self.players[sid]["track_ids"].add(tid); self._update(sid,zone,center,hist)
            self._tid_map[tid] = sid; return sid
        best_sid, best_sim = None, 0.40
        for sid, d in self.players.items():
            sim = self._hist_sim(hist, d["hist"])
            if sim > best_sim: best_sim, best_sid = sim, sid
        if best_sid:
            self.players[best_sid]["track_ids"].add(tid)
            self._update(best_sid, zone, center, hist)
            self._tid_map[tid] = best_sid; return best_sid
        sid = self._new_sid()
        self.players[sid] = {"track_ids":{tid},"jersey":None,"hist":hist,
                             "zones":collections.Counter({zone:1}),"shots":0,"last_center":center}
        self._tid_map[tid] = sid; return sid

    def _update(self, sid, zone, center, hist):
        d = self.players[sid]; d["zones"][zone]+=1; d["last_center"]=center
        if hist:
            old = np.array(d["hist"] or hist, np.float32)
            d["hist"] = (old*0.8 + np.array(hist,np.float32)*0.2).tolist()

    def record_shot(self, sid):
        if sid in self.players: self.players[sid]["shots"] += 1

    def upgrade_jersey(self, old_sid: str, jersey: str) -> str:
        if old_sid not in self.players or not old_sid.startswith("P"): return old_sid
        new_sid = f"#{jersey}"
        if new_sid in self.players:
            self.players[new_sid]["track_ids"] |= self.players[old_sid]["track_ids"]
            del self.players[old_sid]
        else:
            self.players[new_sid] = self.players.pop(old_sid)
            self.players[new_sid]["jersey"] = jersey
        for k,v in self._tid_map.items():
            if v == old_sid: self._tid_map[k] = new_sid
        return new_sid

    def summary(self) -> dict:
        return {sid: {"jersey":d["jersey"],
                      "primary_zone": d["zones"].most_common(1)[0][0] if d["zones"] else "unknown",
                      "shots":d["shots"],"last_center":d["last_center"]}
                for sid,d in self.players.items()}


def get_registry(session_id: str) -> "PlayerRegistry":
    if session_id not in _player_registries:
        _player_registries[session_id] = PlayerRegistry()
    return _player_registries[session_id]


def _is_quota_error(e: Exception) -> bool:
    s = str(e)
    return "429" in s or "RESOURCE_EXHAUSTED" in s or "quota" in s.lower()


def _read_jersey(frame: np.ndarray, box: list) -> Optional[str]:
    """Read jersey number from a player crop. Returns digit string or None."""
    global _gemini_quota_exhausted
    if _gemini_quota_exhausted:
        return None
    import tempfile, cv2 as _cv2
    tmp = None
    try:
        x1,y1,x2,y2 = [int(v) for v in box]; pad=15
        crop = frame[max(0,y1-pad):y2+pad, max(0,x1-pad):x2+pad]
        if crop.size == 0: return None
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tf: tmp=tf.name
        _cv2.imwrite(tmp, crop)
        vf = client.files.upload(file=tmp)
        while vf.state.name == "PROCESSING": time.sleep(1); vf = client.files.get(name=vf.name)
        resp = client.models.generate_content(model="gemini-2.5-flash",
            contents=["What is the jersey number? Reply ONLY with digits (e.g. '23'). "
                      "If unreadable reply 'unknown'.", vf])
        client.files.delete(name=vf.name)
        text = resp.text.strip().strip("'\"").lower()
        return text if text.isdigit() else None
    except Exception as e:
        if _is_quota_error(e):
            _gemini_quota_exhausted = True
            print("[Jersey] Quota exhausted — jersey reading disabled.")
        else:
            print(f"[Jersey] {e}")
        return None
    finally:
        if tmp:
            try: os.remove(tmp)
            except: pass


def render_debug_video(clip_path: str, pose_results: list,
                       ball_detections: list, registry: "PlayerRegistry",
                       local_tracks: dict, h_px: int, w_px: int) -> str:
    """
    Render an annotated debug video showing:
      - Player bounding boxes with stable ID labels (coloured by player)
      - Pose keypoint skeleton overlay
      - Ball position (orange circle)
      - Shot detection flag (red dot over shooter)
      - Zone label under each player

    Saves to YOLO_DEBUG_DIR/<clip_basename>_debug.mp4
    Returns the output path.
    """
    import cv2 as _cv2

    os.makedirs(YOLO_DEBUG_DIR, exist_ok=True)
    base    = os.path.splitext(os.path.basename(clip_path))[0]
    out_path = os.path.join(YOLO_DEBUG_DIR, f"{base}_debug.mp4")

    cap = _cv2.VideoCapture(clip_path)
    fps_src = cap.get(_cv2.CAP_PROP_FPS) or 30.0
    fourcc  = _cv2.VideoWriter_fourcc(*"mp4v")
    writer  = _cv2.VideoWriter(out_path, fourcc, fps_src / 2,  # vid_stride=2
                               (w_px, h_px))

    # Colour palette — consistent across players
    PALETTE = [
        (56, 189, 248), (244, 63, 94), (74, 222, 128), (251, 146, 60),
        (167, 139, 250), (52, 211, 153), (251, 191, 36), (232, 121, 249),
        (96, 165, 250), (248, 113, 113),
    ]
    # Map stable_id → colour
    sid_colors: dict = {}
    def _color(sid: str):
        if sid not in sid_colors:
            sid_colors[sid] = PALETTE[len(sid_colors) % len(PALETTE)]
        return sid_colors[sid]

    # Build reverse map: frame_num → [(box, sid, shot, zone, kps)]
    frame_data: dict = collections.defaultdict(list)
    for tid, data in local_tracks.items():
        sid  = data.get("stable_id") or str(tid)
        # We need per-frame boxes — re-derive from pose_results
        pass  # handled below via pose_results directly

    # COCO skeleton pairs for keypoint drawing
    SKELETON = [
        (5,6),(5,7),(7,9),(6,8),(8,10),      # shoulders + arms
        (5,11),(6,12),(11,12),                 # torso
        (11,13),(13,15),(12,14),(14,16),       # legs
        (0,5),(0,6),                           # nose-shoulder
    ]

    # Build tid → stable_id map from local_tracks
    tid_to_sid = {tid: (data.get("stable_id") or str(tid))
                  for tid, data in local_tracks.items()}

    frame_num = 0
    for r, ball_pos in zip(pose_results,
                           ball_detections + [None]*len(pose_results)):
        cap.set(_cv2.CAP_PROP_POS_FRAMES, frame_num * 2)
        ok, frame = cap.read()
        if not ok:
            frame_num += 1; continue

        # ── Draw player boxes + labels ─────────────────────────────────
        if r.boxes is not None and r.boxes.id is not None:
            ids   = r.boxes.id.int().tolist()
            boxes = r.boxes.xyxy.tolist()
            kps   = r.keypoints.xy.tolist() if (r.keypoints is not None) else None

            for i, tid in enumerate(ids):
                sid   = tid_to_sid.get(tid, str(tid))
                color = _color(sid)
                box   = [int(v) for v in boxes[i]]
                x1,y1,x2,y2 = box

                # Skip height filter
                if (y2-y1)/h_px < MIN_PLAYER_HEIGHT:
                    continue

                # Box
                _cv2.rectangle(frame, (x1,y1), (x2,y2), color, 2)

                # Label: stable_id + zone
                zone = _zone_for_point((x1+x2)/2/w_px, (y1+y2)/2/h_px)
                zone_short = {"paint_left":"PNT-L","paint_right":"PNT-R",
                              "mid_range":"MID","perimeter":"PERIM",
                              "three_left":"3PT-L","three_right":"3PT-R",
                              "backcourt":"BACK"}.get(zone, zone)
                label = f"{sid} | {zone_short}"
                lw, lh = _cv2.getTextSize(label, _cv2.FONT_HERSHEY_SIMPLEX, 0.5, 1)[0]
                _cv2.rectangle(frame, (x1, y1-lh-6), (x1+lw+4, y1), color, -1)
                _cv2.putText(frame, label, (x1+2, y1-4),
                             _cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0,0,0), 1)

                # Skeleton keypoints
                if kps and i < len(kps):
                    pts = kps[i]
                    for a, b in SKELETON:
                        if a < len(pts) and b < len(pts):
                            pa = (int(pts[a][0]), int(pts[a][1]))
                            pb = (int(pts[b][0]), int(pts[b][1]))
                            if pa != (0,0) and pb != (0,0):
                                _cv2.line(frame, pa, pb, color, 1)
                    for pt in pts:
                        px, py = int(pt[0]), int(pt[1])
                        if px != 0 or py != 0:
                            _cv2.circle(frame, (px,py), 3, color, -1)

        # ── Draw ball ─────────────────────────────────────────────────
        if ball_pos is not None:
            bx = int(ball_pos[0] * w_px)
            by = int(ball_pos[1] * h_px)
            _cv2.circle(frame, (bx, by), 14, (0, 165, 255), -1)   # orange fill
            _cv2.circle(frame, (bx, by), 14, (0, 80, 200), 2)     # darker border
            _cv2.putText(frame, "BALL", (bx+16, by+5),
                         _cv2.FONT_HERSHEY_SIMPLEX, 0.45, (0,165,255), 1)

        # ── Frame counter ─────────────────────────────────────────────
        _cv2.putText(frame, f"f{frame_num*2}", (8, 20),
                     _cv2.FONT_HERSHEY_SIMPLEX, 0.5, (200,200,200), 1)

        writer.write(frame)
        frame_num += 1

    cap.release()
    writer.release()
    print(f"[Debug] Rendered: {out_path}")
    return out_path


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
        iou=0.45,
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
        cap.set(_cv2.CAP_PROP_POS_FRAMES, frame_num * 2)
        ok, frame_bgr = cap.read()
        valid = 0
        for i, tid in enumerate(ids.int().tolist()):
            box = boxes[i].tolist()
            if (box[3] - box[1]) / h_px < MIN_PLAYER_HEIGHT:
                continue
            valid += 1
            cx   = (box[0]+box[2])/2/w_px
            cy   = (box[1]+box[3])/2/h_px
            zone = _zone_for_point(cx, cy)
            hist = registry.compute_hist(frame_bgr, box) if ok else []
            if local_tracks[tid]["stable_id"] is None:
                sid = registry.resolve(tid, None, hist, (cx,cy), zone)
                local_tracks[tid]["stable_id"] = sid
                if ok and local_tracks[tid]["first_frame"] is None:
                    local_tracks[tid]["first_frame"] = frame_bgr.copy()
                    local_tracks[tid]["first_box"]   = box
            else:
                registry._update(local_tracks[tid]["stable_id"], zone, (cx,cy), hist)
            local_tracks[tid]["centers"].append((cx, cy))
            local_tracks[tid]["zones"].append(zone)
            if has_kps and kps is not None and kps.shape[1] >= 17:
                local_tracks[tid]["kp_history"].append(kps[i].tolist())
        if valid > 0: frame_player_counts.append(valid)
        frame_num += 1
    cap.release()

    # Jersey reading — skip entirely if Gemini quota is exhausted
    jersey_calls = 0
    if not _gemini_quota_exhausted:
        for tid, data in local_tracks.items():
            sid = data["stable_id"]
            if not sid or not sid.startswith("P") or jersey_calls >= 4: continue
            if data["first_frame"] is None: continue
            jersey = _read_jersey(data["first_frame"], data["first_box"])
            if jersey:
                new_sid = registry.upgrade_jersey(sid, jersey)
                data["stable_id"] = new_sid
                print(f"[Registry] #{tid} → #{jersey}")
            jersey_calls += 1
    else:
        print("[Jersey] Skipping — Gemini quota exhausted.")
    tracks = local_tracks

    # ── 2. Ball + rim detection via Roboflow hosted inference ────────────────
    # Sends sampled frames to Roboflow cloud API (basketball-players-fy4c2/25).
    # Classes: ball, made, player, rim — we use ball and rim here.
    # Falls back gracefully if ROBOFLOW_API_KEY is not set.
    import cv2 as _cv2r
    ball_detections  = []   # (cx_norm, cy_norm) or None per sampled frame
    rim_detections   = []   # (cx_norm, cy_norm) or None per sampled frame
    shot_made_flags  = []   # bool per sampled frame (Roboflow "made" class)
    
    cap_rf = _cv2r.VideoCapture(clip_path)
    total_frames = int(cap_rf.get(_cv2r.CAP_PROP_FRAME_COUNT)) or 1
    # Sample every 6th frame (~5fps from 30fps source) — fast enough, not hammering the API
    STRIDE = 6
    sampled = range(0, total_frames, STRIDE)
    
    if ROBOFLOW_API_KEY:
        for fn in sampled:
            cap_rf.set(_cv2r.CAP_PROP_POS_FRAMES, fn)
            ok, frame = cap_rf.read()
            if not ok:
                ball_detections.append(None)
                rim_detections.append(None)
                shot_made_flags.append(False)
                continue
            preds = _rf_predict_frame(frame)
            # Pick highest-confidence ball prediction
            balls = [p for p in preds if p["class"] == RF_BALL_CLASS]
            if balls:
                best = max(balls, key=lambda p: p["confidence"])
                ball_detections.append((
                    round(best["x"] / w_px, 3),
                    round(best["y"] / h_px, 3),
                ))
            else:
                ball_detections.append(None)
            # Rim
            rims = [p for p in preds if p["class"] == RF_RIM_CLASS]
            if rims:
                best = max(rims, key=lambda p: p["confidence"])
                rim_detections.append((round(best["x"]/w_px,3), round(best["y"]/h_px,3)))
            else:
                rim_detections.append(None)
            # Made shot flag
            shot_made_flags.append(any(p["class"] == RF_MADE_CLASS for p in preds))
    else:
        print("[Roboflow] ROBOFLOW_API_KEY not set — ball/rim tracking disabled.")
        ball_detections = [None] * len(sampled)
        rim_detections  = [None] * len(sampled)
        shot_made_flags = [False] * len(sampled)
    cap_rf.release()

    # Log RF detection summary
    n_ball = sum(1 for p in ball_detections if p is not None)
    n_rim  = sum(1 for p in rim_detections  if p is not None)
    n_made = sum(shot_made_flags)
    n_frames = len(list(sampled))
    print(f"[Roboflow] Sampled {n_frames} frames → ball:{n_ball} rim:{n_rim} made:{n_made}")

    # Ball summary stats
    detected_positions = [p for p in ball_detections if p is not None]
    ball_detected      = len(detected_positions) > 0
    # Possession side: which half of court does the ball spend more time in?
    ball_possession_side = None
    if detected_positions:
        avg_ball_x = float(np.mean([p[0] for p in detected_positions]))
        ball_possession_side = "left" if avg_ball_x < 0.5 else "right"

    # ── 3. Per-player metrics — keyed by stable ID ──────────────────────
    player_metrics = {}
    for tid, data in tracks.items():
        if len(data["centers"]) < 5: continue
        sid  = data.get("stable_id") or str(tid)
        shot = _detect_shot(data["kp_history"]) if has_kps else False
        if shot: registry.record_shot(sid)
        zone_counts  = collections.Counter(data["zones"])
        primary_zone = max(zone_counts, key=zone_counts.get) if zone_counts else "unknown"
        player_metrics[sid] = {
            "primary_zone":  primary_zone,
            "zone_pct":      {z: round(cnt/len(data["zones"])*100) for z,cnt in zone_counts.items()},
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
        if len(data["centers"]) < 5: continue
        sid   = data.get("stable_id") or str(tid)
        trajectories[sid] = [{"x": round(cx,4), "y": round(cy,4)} for cx,cy in data["centers"][-60:]]

    # Ball trajectory for visualizer
    ball_trail = [{"x": p[0], "y": p[1]} for p in detected_positions[-60:]]

    # ── 6. Shot arc keypoints for visualizer (not a displayed stat) ────────
    shot_arcs = {}
    for tid, data in tracks.items():
        sid = data.get("stable_id") or str(tid)
        pm  = player_metrics.get(sid)
        if not pm or not pm["shot_detected"]: continue
        arc = [{"wrist_y": round(kp[10][1]/h_px,4) if len(kp)>10 and kp[10][0]!=0 else None,
                "shoulder_y": round(kp[6][1]/h_px,4) if len(kp)>6 and kp[6][0]!=0 else None}
               for kp in data["kp_history"][-30:]]
        shot_arcs[sid] = arc

    # ── Debug render ──────────────────────────────────────────────────────
    debug_path = None
    if YOLO_DEBUG:
        try:
            debug_path = render_debug_video(
                clip_path, pose_results, ball_detections,
                registry, local_tracks, h_px, w_px
            )
        except Exception as e:
            print(f"[Debug] Render failed: {e}")

    registry_summary = registry.summary()

    # Rim position (use median of detected positions — rim doesn't move)
    rim_positions = [p for p in rim_detections if p is not None]
    rim_center = None
    if rim_positions:
        rim_center = {
            "x": round(float(np.median([p[0] for p in rim_positions])), 3),
            "y": round(float(np.median([p[1] for p in rim_positions])), 3),
        }

    # Shot made: Roboflow "made" class appeared in any frame
    rf_shot_made = any(shot_made_flags)

    metrics = {
        "player_count":         len(player_metrics),
        "avg_players_in_frame": avg_players,
        "shot_attempts":        shot_attempts,
        "rf_shot_made":         rf_shot_made,   # Roboflow visual confirmation
        "ball_detected":        ball_detected,
        "ball_possession_side": ball_possession_side,
        "ball_trail":           ball_trail,
        "rim_center":           rim_center,
        "zone_occupancy":       zone_summary,
        "players":              player_metrics,
        "trajectories":         trajectories,
        "shot_arcs":            shot_arcs,
        "debug_video":          debug_path,
    }

    registry_summary = registry.summary()
    metrics["registry"] = registry_summary
    lines = [
        f"YOLO tracked {len(player_metrics)} players (stable IDs).",
        f"Avg visible/frame: {avg_players}. Shot attempts: {shot_attempts}.",
        f"Ball: {'detected, ' + str(ball_possession_side) + ' side' if ball_detected else 'not detected'}.",
        f"Zones: { {z: cnt for z, cnt in list(zone_summary.items())[:3]} }.",
    ]
    for sid, pm in list(player_metrics.items())[:6]:
        label = f"#{pm['jersey']}" if pm.get("jersey") else sid
        lines.append(f"  {label}: {pm['primary_zone']}" + (" [SHOT]" if pm["shot_detected"] else ""))
    for sid, rp in list(registry_summary.items())[:5]:
        if rp["shots"] > 0:
            lines.append(f"  {sid} session shots: {rp['shots']}")
    summary = "\n".join(lines)

    # ── Terminal summary ──────────────────────────────────────────────────
    print(f"[YOLO] Players:{metrics['player_count']} ({metrics['avg_players_in_frame']}/frame) | "
          f"Shots:{metrics['shot_attempts']} | "
          f"Ball:{'✓ ' + str(metrics.get('ball_possession_side','')) if metrics['ball_detected'] else '✗'} | "
          f"Rim:{'✓' if metrics.get('rim_center') else '✗'} | "
          f"RF-Made:{'✓' if metrics.get('rf_shot_made') else '✗'}")
    for sid, pm in list(player_metrics.items())[:6]:
        label = f"#{pm['jersey']}" if pm.get("jersey") else sid
        print(f"[YOLO]   {label:8s} → {pm['primary_zone']}" + (" 🏀SHOT" if pm["shot_detected"] else ""))

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


_gemini_quota_exhausted = False  # set True on first 429; cleared at midnight

def _is_quota_error(e: Exception) -> bool:
    """Return True if the exception is a Gemini API quota exhaustion."""
    s = str(e)
    return "429" in s or "RESOURCE_EXHAUSTED" in s or "quota" in s.lower()


def _yolo_only_report(yolo_metrics: dict) -> dict:
    """
    Build a structured report from YOLO data alone when Gemini is unavailable.
    Returns the same shape as gemini_scout so callers need no changes.
    """
    m    = yolo_metrics.get("metrics", {})
    reg  = m.get("registry", {})
    zones = m.get("zone_occupancy", {})
    ZONE_LABELS = {
        "paint_left": "Paint (L)", "paint_right": "Paint (R)",
        "mid_range": "Mid-Range", "perimeter": "Perimeter",
        "three_left": "3PT (L)", "three_right": "3PT (R)", "backcourt": "Backcourt",
    }

    lines = ["⚠️ Gemini unavailable (API quota exhausted) — YOLO data only.\n"]

    # Players
    players = m.get("players", {})
    lines.append(f"**{m.get('player_count', 0)} players tracked** "
                 f"({m.get('avg_players_in_frame', 0)} avg/frame)")

    # Shots
    shots = m.get("shot_attempts", 0)
    if shots:
        shooters = [sid for sid, p in players.items() if p.get("shot_detected")]
        labels   = [p.get("jersey") and f"#{p['jersey']}" or sid for sid in shooters]
        lines.append(f"**{shots} shot attempt(s)** detected: {', '.join(labels)}")

    # Ball
    if m.get("ball_detected"):
        side = m.get("ball_possession_side", "unknown")
        lines.append(f"**Ball detected** — {side} side of court")
    else:
        lines.append("Ball not detected this clip.")

    # Zone breakdown
    if zones:
        top = sorted(zones.items(), key=lambda x: -x[1])[:3]
        zone_str = ", ".join(f"{ZONE_LABELS.get(z,z)} ({c})" for z, c in top)
        lines.append(f"**Most active zones:** {zone_str}")

    # Per-player summary
    if players:
        lines.append("\n**Player breakdown:**")
        for sid, p in list(players.items())[:8]:
            label = f"#{p['jersey']}" if p.get("jersey") else sid
            shot_flag = " 🏀 SHOT" if p.get("shot_detected") else ""
            zone = ZONE_LABELS.get(p.get("primary_zone", ""), p.get("primary_zone", ""))
            lines.append(f"  • {label} — {zone}{shot_flag}")

    # Session cumulative shots
    session_shots = [(sid, rp["shots"]) for sid, rp in reg.items() if rp.get("shots", 0) > 0]
    if session_shots:
        lines.append("\n**Session shot totals:**")
        for sid, n in sorted(session_shots, key=lambda x: -x[1]):
            lines.append(f"  • {sid}: {n} shot(s)")

    report = "\n".join(lines)
    return {
        "report":       report,
        "score_update": None,
        "period":       None,
        "play_type":    None,
        "possession":   None,
        "shot_made":    None,
        "structured":   {},
        "gemini_used":  False,
    }


def gemini_scout(clip_path: str, yolo_metrics: dict) -> dict:
    """
    Uploads raw clip once, fires both Gemini calls in parallel threads.
    If Gemini quota is exhausted, falls back to a YOLO-only report immediately
    without retrying or waiting.
    """
    yolo_summary = yolo_metrics.get("summary", "No YOLO data.")
    try:
        vf = _upload_and_wait(clip_path)

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

        score_update = None
        if fast_result.get("team_a") and fast_result.get("score_a") is not None:
            score_update = (
                f"{fast_result['team_a']} {fast_result['score_a']} – "
                f"{fast_result.get('team_b','?')} {fast_result.get('score_b','?')}"
            )

        out = {
            "report":       qual_result,
            "score_update": score_update,
            "period":       fast_result.get("period"),
            "play_type":    fast_result.get("play_type"),
            "possession":   fast_result.get("possession"),
            "shot_made":    fast_result.get("shot_made"),
            "structured":   fast_result,
            "gemini_used":  True,
        }
        print(f"[Gemini] ── Results ────────────────────────────────")
        print(f"[Gemini]  Score      : {score_update or 'not detected'}")
        print(f"[Gemini]  Period     : {fast_result.get('period','?')}")
        print(f"[Gemini]  Play type  : {fast_result.get('play_type','?')}")
        print(f"[Gemini]  Shot made  : {fast_result.get('shot_made','?')}")
        print(f"[Gemini]  Report     : {qual_result[:120].strip()}...")
        print(f"[Gemini] ────────────────────────────────────────────")
        return out
    except Exception as e:
        if _is_quota_error(e):
            global _gemini_quota_exhausted
            _gemini_quota_exhausted = True
            print("[Gemini] Quota exhausted — returning YOLO-only report.")
            try: client.files.delete(name=vf.name)
            except: pass
            result = _yolo_only_report(yolo_metrics)
            result["gemini_used"] = False
            return result
        return {"report": f"Gemini error: {e}", "score_update": None, "period": None,
                "play_type": None, "possession": None, "shot_made": None,
                "structured": {}, "gemini_used": False}


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
            # streamlink writes to a temp file; ffmpeg slices from it
            # --stdout and -o are mutually exclusive — use -o only
            sl_tmp = f"cache_clips/sl_full_{cache_key}.mp4"
            sl = subprocess.run([
                "streamlink",
                "-o", sl_tmp,
                "--force",          # overwrite without prompting
                yt_url, "best",
            ], capture_output=True, timeout=120)
            if os.path.exists(sl_tmp) and os.path.getsize(sl_tmp) > 1000:
                subprocess.run([
                    "ffmpeg", "-y",
                    "-ss", str(start), "-i", sl_tmp,
                    "-t", "30", "-c:v", CODEC, "-c:a", "aac", clip_path
                ], capture_output=True, timeout=60)
                try: os.remove(sl_tmp)
                except: pass
                if os.path.exists(clip_path) and os.path.getsize(clip_path) > 1000:
                    downloaded = True
                    print("[VOD] streamlink OK")
            else:
                last_error = sl.stderr.decode(errors="replace")[:200]
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


@app.get("/api/debug_renders")
def list_debug_renders():
    """List all rendered debug videos. Served at /debug/<filename>"""
    files = sorted(glob.glob("debug_renders/*_debug.mp4"), key=os.path.getmtime, reverse=True)
    return {
        "renders": [
            {"filename": os.path.basename(f),
             "url": f"/debug/{os.path.basename(f)}",
             "size_mb": round(os.path.getsize(f)/1e6, 2),
             "created": os.path.getmtime(f)}
            for f in files
        ]
    }


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