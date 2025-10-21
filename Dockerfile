# syntax=docker/dockerfile:1
FROM python:3.12-slim

# --- Install system dependencies ---
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

# --- FIX: Create a non-root user for security ---
RUN groupadd -r appuser && useradd --no-log-init -r -g appuser appuser

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

WORKDIR /app

# ---- Python dependencies. Added gunicorn for robustness ----
RUN <<'EOF' bash
set -e
cat > /app/requirements.txt <<'REQS'
fastapi==0.115.5
uvicorn[standard]==0.31.1
gunicorn==22.0.0
Jinja2==3.1.4
python-multipart==0.0.9
aiofiles==24.1.0
websockets==12.0
httpx==0.27.0
REQS
pip install --no-cache-dir -r /app/requirements.txt
EOF

# ---- App code (Combined Server, VPN Checker, and Process Manager) ----
RUN mkdir -p /app/app /app/templates /data/input /data/output /data/logs \
 && touch /app/app/__init__.py

# ---------- server.py (The new all-in-one cockpit application) ----------
RUN <<'PY' bash
cat > /app/app/server.py <<'PYCODE'
import asyncio
import os
import json
import io
import zipfile
from pathlib import Path
import httpx
import logging
from fastapi import FastAPI, Request, UploadFile, File, HTTPException, WebSocket, Response
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from contextlib import asynccontextmanager
import aiofiles

# --- FIX: Set up proper logging for better debugging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# --- Configuration ---
DATA_DIR = Path("/data")
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"
LOG_DIR = DATA_DIR / "logs"
URLS_CSV_PATH = INPUT_DIR / "urls.csv"
LOG_FILE_PATH = LOG_DIR / "mirror.log"
STATUS_FILE_PATH = DATA_DIR / "status.json"
TEMPLATES_DIR = Path("/app/templates")

# --- Global State & Concurrency Lock ---
mirror_lock = asyncio.Lock()

def get_status():
    if not STATUS_FILE_PATH.exists():
        return {"status": "idle", "pid": None}
    try:
        return json.loads(STATUS_FILE_PATH.read_text())
    except (json.JSONDecodeError, FileNotFoundError):
        return {"status": "idle", "pid": None}

def set_status(status, pid=None):
    log.info(f"Updating status to: {status}, PID: {pid}")
    STATUS_FILE_PATH.write_text(json.dumps({"status": status, "pid": pid}))

# --- App Lifespan ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Starting up Mirror Cockpit...")
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    set_status("idle")
    yield
    log.info("Shutting down Mirror Cockpit...")
    set_status("idle")

# --- FastAPI App Initialization ---
app = FastAPI(title="Website Mirror Cockpit", lifespan=lifespan)
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
app.mount("/sites", StaticFiles(directory=OUTPUT_DIR), name="sites")

# --- Background Mirroring Task ---
async def run_mirror_process():
    if not URLS_CSV_PATH.exists():
        log.error("urls.csv not found. Aborting mirror process.")
        set_status("error", "urls.csv not found")
        return

    if LOG_FILE_PATH.exists():
        LOG_FILE_PATH.unlink()

    command = [
        "wget", "--mirror", "--page-requisites", "--adjust-extension", "--convert-links", 
        "--span-hosts", "--no-parent", "--directory-prefix", str(OUTPUT_DIR),
        "--input-file", str(URLS_CSV_PATH), "--no-check-certificate",
        "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
        "--append-output", str(LOG_FILE_PATH)
    ]
    
    process = None
    try:
        log.info(f"Executing wget command: {' '.join(command)}")
        process = await asyncio.create_subprocess_exec(*command)
        set_status("mirroring", process.pid)
        await process.wait()
        log.info(f"Wget process finished with exit code: {process.returncode}")
        set_status("finished" if process.returncode == 0 else f"error: wget exited with code {process.returncode}")
    except Exception as e:
        log.error(f"Error running mirror process: {e}", exc_info=True)
        set_status("error", str(e))
    finally:
        if process and process.returncode is None:
            log.warning(f"Terminating runaway process PID {process.pid}")
            process.terminate()
            await process.wait()

# --- HTML Template (unchanged, but placed in its own file creation step) ---
RUN <<'HTML' bash
cat > /app/templates/index.html <<'HTMLCODE'
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Website Mirror Cockpit</title>
    <style>
        :root { --primary-color: #3498db; --secondary-color: #2c3e50; --success-color: #2ecc71; --warning-color: #f39c12; --danger-color: #e74c3c; --light-gray: #ecf0f1; --dark-gray: #34495e; --white: #fff; }
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; margin: 0; background-color: #f8f9fa; color: var(--dark-gray); line-height: 1.6; }
        .container { max-width: 1200px; margin: 2rem auto; padding: 0 1rem; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(350px, 1fr)); gap: 2rem; }
        .card { background: var(--white); border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.08); overflow: hidden; }
        .card-header { padding: 1.5rem; background: var(--secondary-color); color: var(--white); }
        .card-header h2 { margin: 0; font-size: 1.25rem; }
        .card-body { padding: 1.5rem; }
        h1 { color: var(--secondary-color); text-align: center; margin-bottom: 2rem; }
        .status-bar { display: flex; align-items: center; gap: 1rem; flex-wrap: wrap; }
        .status-indicator { display: flex; align-items: center; gap: 0.5rem; font-weight: bold; padding: 0.5rem 1rem; border-radius: 20px; color: var(--white); }
        .status-indicator.idle { background-color: var(--primary-color); }
        .status-indicator.mirroring { background-color: var(--warning-color); }
        .status-indicator.finished { background-color: var(--success-color); }
        .status-indicator.error { background-color: var(--danger-color); }
        .status-indicator .dot { width: 12px; height: 12px; border-radius: 50%; background: currentColor; }
        .vpn-status .dot.connected { background: var(--success-color); }
        .vpn-status .dot.disconnected { background: var(--danger-color); }
        .vpn-info { font-size: 0.9rem; color: #7f8c8d; }
        .button-group { display: flex; gap: 1rem; margin-top: 1rem; }
        button, input[type=submit] { background-color: var(--primary-color); color: var(--white); border: none; padding: 10px 18px; border-radius: 5px; cursor: pointer; font-size: 1rem; font-weight: 500; transition: background-color 0.2s; }
        button:hover, input[type=submit]:hover { background-color: #2980b9; }
        button:disabled { background-color: #bdc3c7; cursor: not-allowed; }
        #log-viewer { background: #282c34; color: #abb2bf; font-family: 'SF Mono', 'Fira Code', 'Fira Mono', 'Roboto Mono', monospace; padding: 1rem; border-radius: 5px; height: 300px; overflow-y: auto; white-space: pre-wrap; margin-top: 1rem; }
        table { width: 100%; border-collapse: collapse; margin-top: 1rem; }
        th, td { padding: 12px 15px; border-bottom: 1px solid #e0e0e0; }
        th { background-color: #f2f2f2; font-weight: 600; }
        tr:last-child td { border-bottom: none; }
        tr:hover { background-color: #fafafa; }
        a { color: var(--primary-color); text-decoration: none; font-weight: 500; }
        .upload-form input[type=file] { border: 1px solid #ccc; padding: 8px; border-radius: 5px; }
    </style>
</head>
<body>
<div class="container">
    <h1>Website Mirror Cockpit</h1>
    <div class="grid">
        <div class="card">
            <div class="card-header"><h2>Process Control</h2></div>
            <div class="card-body">
                <div class="status-bar">
                    <div id="status" class="status-indicator idle"><div class="dot"></div><span>Idle</span></div>
                </div>
                <div class="button-group">
                    <form action="/start" method="post"><button id="startButton">Start Mirroring</button></form>
                    <form action="/stop" method="post"><button id="stopButton" disabled>Stop Mirroring</button></form>
                </div>
                <div id="log-viewer">Connecting to log stream...</div>
            </div>
        </div>
        <div class="card">
            <div class="card-header"><h2>VPN Status</h2></div>
            <div class="card-body">
                <div class="status-bar vpn-status">
                    <div id="vpn-connection-status" class="status-indicator"><div id="vpn-dot" class="dot"></div><span id="vpn-status-text">Checking...</span></div>
                </div>
                <p class="vpn-info"><strong>Public IP:</strong> <span id="vpn-ip">N/A</span></p>
                <p class="vpn-info"><strong>Location:</strong> <span id="vpn-location">N/A</span></p>
            </div>
            <div class="card-header" style="border-top: 1px solid #e0e0e0;"><h2>Upload URLs</h2></div>
            <div class="card-body upload-form">
                <p>Upload a <code>urls.csv</code> file. This will overwrite any existing list.</p>
                <form action="/upload" method="post" enctype="multipart/form-data">
                    <input type="file" name="file" accept=".csv" required>
                    <input type="submit" value="Upload CSV">
                </form>
                <p id="csv-status" style="margin-top:1rem; font-style: italic;"></p>
            </div>
        </div>
    </div>
    <div class="card">
        <div class="card-header"><h2>Mirrored Websites</h2></div>
        <div class="card-body">
            <table id="sites-table">
                <thead><tr><th>Site Domain</th><th>Actions</th></tr></thead>
                <tbody><tr><td colspan="2">No sites mirrored yet.</td></tr></tbody>
            </table>
        </div>
    </div>
</div>
<script>
    const statusSpan = document.getElementById('status');
    const startButton = document.getElementById('startButton');
    const stopButton = document.getElementById('stopButton');
    const logViewer = document.getElementById('log-viewer');
    const sitesTableBody = document.querySelector('#sites-table tbody');
    const csvStatus = document.getElementById('csv-status');
    const vpnDot = document.getElementById('vpn-dot');
    const vpnStatusText = document.getElementById('vpn-status-text');
    const vpnIp = document.getElementById('vpn-ip');
    const vpnLocation = document.getElementById('vpn-location');

    function updateUI(statusData) {
        const statusText = statusData.status.replace(/_/g, " ");
        statusSpan.innerHTML = `<div class="dot"></div><span>${statusText}</span>`;
        statusSpan.className = `status-indicator ${statusData.status}`;
        startButton.disabled = statusData.status === 'mirroring';
        stopButton.disabled = statusData.status !== 'mirroring';
    }
    
    function updateVpnUI(vpnData) {
        vpnStatusText.textContent = vpnData.status;
        vpnDot.className = `dot ${vpnData.status.toLowerCase()}`;
        vpnIp.textContent = vpnData.ip || 'N/A';
        vpnLocation.textContent = vpnData.location || 'N/A';
        vpnStatusText.parentElement.className = `status-indicator ${vpnData.status.toLowerCase()}`;
    }

    async function fetchInitialData() {
        try {
            const statusRes = await fetch('/status'); updateUI(await statusRes.json());
            const sitesRes = await fetch('/sites-list'); updateSitesTable(await sitesRes.json());
            const csvRes = await fetch('/csv-status'); csvStatus.textContent = (await csvRes.json()).message;
            const vpnRes = await fetch('/vpn-status'); updateVpnUI(await vpnRes.json());
        } catch (error) {
            console.error("Failed to fetch initial data:", error);
        }
    }
    
    setInterval(async () => {
        try {
            const vpnRes = await fetch('/vpn-status');
            updateVpnUI(await vpnRes.json());
        } catch (error) {
            console.error("Failed to fetch VPN status:", error);
        }
    }, 10000); // Check VPN status every 10 seconds

    function updateSitesTable(sites) {
        if (!sites || sites.length === 0) {
            sitesTableBody.innerHTML = '<tr><td colspan="2">No sites mirrored yet.</td></tr>'; return;
        }
        sitesTableBody.innerHTML = '';
        sites.forEach(site => {
            const row = `<tr><td>${site.name}</td><td><a href="/sites/${site.name}/${site.index_path}" target="_blank">View Site</a> | <a href="/download/${site.name}">Download ZIP</a></td></tr>`;
            sitesTableBody.innerHTML += row;
        });
    }

    function connectWebSocket() {
        const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const ws = new WebSocket(`${wsProtocol}//${window.location.host}/ws/status`);
        ws.onopen = () => { logViewer.textContent = 'Log stream connected.\\n'; };
        ws.onmessage = (event) => {
            const data = JSON.parse(event.data);
            if (data.type === 'status') {
                updateUI(data.payload);
                if (data.payload.status === 'finished' || data.payload.status.startsWith('error')) {
                    fetchInitialData();
                }
            } else if (data.type === 'log') {
                logViewer.textContent += data.payload;
                logViewer.scrollTop = logViewer.scrollHeight;
            }
        };
        ws.onerror = (error) => { console.error('WebSocket Error:', error); };
        ws.onclose = () => { logViewer.textContent += '\\nConnection closed. Reconnecting in 5s...'; setTimeout(connectWebSocket, 5000); };
    }

    document.addEventListener('DOMContentLoaded', () => { fetchInitialData(); connectWebSocket(); });
</script>
</body></html>
HTMLCODE
HTML

# ---------- server.py continued... ----------
RUN <<'PY' bash
cat >> /app/app/server.py <<'PYCODE'

# --- API Endpoints ---
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# --- FIX: Added a healthcheck endpoint for Docker ---
@app.get("/health", status_code=200)
async def health_check():
    return {"status": "ok"}

@app.get("/status", response_class=JSONResponse)
async def get_status_endpoint():
    return get_status()

@app.get("/csv-status", response_class=JSONResponse)
async def get_csv_status():
    return {"message": f"'{URLS_CSV_PATH.name}' is ready."} if URLS_CSV_PATH.exists() else {"message": "No CSV uploaded yet."}

@app.get("/vpn-status", response_class=JSONResponse)
async def get_vpn_status():
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get("https://ipinfo.io/json", timeout=5)
            resp.raise_for_status()
            data = resp.json()
            return {
                "status": "Connected",
                "ip": data.get("ip"),
                "location": f"{data.get('city', '')}, {data.get('country', '')}"
            }
    except Exception as e:
        log.warning(f"VPN status check failed: {e}")
        return {"status": "Disconnected", "ip": "Error", "location": str(e)}

@app.post("/start")
async def start_mirroring():
    log.info("Received request to start mirroring.")
    async with mirror_lock:
        if get_status()['status'] == 'mirroring':
            log.warning("Attempted to start mirroring process while one is already running.")
            raise HTTPException(409, "Mirroring process is already running.")
        asyncio.create_task(run_mirror_process())
    return RedirectResponse(url="/", status_code=303)

@app.post("/stop")
async def stop_mirroring():
    log.info("Received request to stop mirroring.")
    status = get_status()
    pid = status.get('pid')
    if status.get('status') != 'mirroring' or not pid:
        log.warning("Attempted to stop mirroring process but none was running.")
        raise HTTPException(404, "No mirroring process is running.")
    try:
        os.kill(pid, 15) # SIGTERM
        log.info(f"Sent SIGTERM to process {pid}")
        set_status("idle")
    except ProcessLookupError:
        log.warning(f"Process {pid} not found, it may have already finished.")
        set_status("idle")
    return RedirectResponse(url="/", status_code=303)

@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    if not file.filename.endswith('.csv'):
        raise HTTPException(400, "Invalid file type. Please upload a .csv file.")
    try:
        async with aiofiles.open(URLS_CSV_PATH, "wb") as buffer:
            content = await file.read()
            await buffer.write(content)
        log.info(f"Successfully uploaded '{file.filename}' to {URLS_CSV_PATH}")
    except Exception as e:
        log.error(f"Failed to write uploaded file: {e}", exc_info=True)
        raise HTTPException(500, "Could not save uploaded file.")
    return RedirectResponse(url="/", status_code=303)

@app.get("/sites-list", response_class=JSONResponse)
async def get_sites_list():
    sites_info = []
    if OUTPUT_DIR.exists():
        for site_dir in sorted(OUTPUT_DIR.iterdir()):
            if site_dir.is_dir():
                index_path = next(site_dir.rglob("index.html"), None) or next(site_dir.rglob("*.html"), None)
                if index_path:
                    sites_info.append({"name": site_dir.name, "index_path": str(index_path.relative_to(site_dir))})
    return sites_info

@app.get("/download/{site_name}")
def download_zip(site_name: str):
    site_path = (OUTPUT_DIR / site_name).resolve()
    if not site_path.is_dir() or OUTPUT_DIR.resolve() not in site_path.parents:
        raise HTTPException(404, "Site not found or access denied")
    
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for file in site_path.rglob("*"):
            zf.write(file, file.relative_to(site_path))
    zip_buffer.seek(0)
    return StreamingResponse(zip_buffer, media_type="application/zip", headers={"Content-Disposition": f"attachment; filename={site_name}.zip"})

@app.websocket("/ws/status")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    log.info("WebSocket client connected.")
    last_pos = 0
    try:
        while True:
            await websocket.send_json({"type": "status", "payload": get_status()})
            if LOG_FILE_PATH.exists():
                async with aiofiles.open(LOG_FILE_PATH, 'r') as f:
                    await f.seek(last_pos)
                    if new_lines := await f.read():
                        await websocket.send_json({"type": "log", "payload": new_lines})
                        last_pos = await f.tell()
            await asyncio.sleep(2)
    except Exception:
        log.info("WebSocket client disconnected.")
PYCODE
PY

# --- FIX: Set correct permissions and switch to non-root user ---
RUN chown -R appuser:appuser /app /data
USER appuser
