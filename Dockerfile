# syntax=docker/dockerfile:1
FROM python:3.12-slim

# --- Install system dependencies ---
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

# --- Environment variables ---
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

WORKDIR /app

# ---- Python dependencies. Added pandas for CSV and beautifulsoup4 for HTML parsing ----
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
pandas==2.2.2
beautifulsoup4==4.12.3
python-dateutil==2.9.0
REQS
pip install --no-cache-dir -r /app/requirements.txt
EOF

# ---- App code (Combined Server, VPN Checker, and Process Manager) ----
RUN mkdir -p /app/app /app/templates /data/input /data/output /data/logs \
 && touch /app/app/__init__.py

# --- Create the availability parsing script provided by the user ---
RUN <<'PARSER' bash
cat > /app/app/cyaeb_availability_parser.py <<'PARSERCODE'
import json
import re
from typing import Optional, Tuple, List, Dict
from dateutil import parser as dateparser

# ---------- Helpers ----------
def norm(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    return re.sub(r"\s+", " ", s).strip()

NAME_PATTERNS = [
    re.compile(r"^\s*([A-Z0-9'’\- ]{3,})\s+Yacht Charter[s]?\b", re.IGNORECASE),
    re.compile(r"^\s*([A-Z0-9'’\- ]{3,})\s+Home\b", re.IGNORECASE),
    re.compile(r"^\s*([A-Z0-9'’\- ]{3,})\b", re.IGNORECASE),
]

def extract_name(page_text: str) -> Optional[str]:
    head = page_text[:400]
    for rx in NAME_PATTERNS:
        m = rx.search(head)
        if m:
            return norm(m.group(1))
    return None

def parse_date_safe(s: str) -> Optional[str]:
    try:
        return dateparser.parse(s, dayfirst=False, yearfirst=False, fuzzy=True).date().isoformat()
    except Exception:
        return None

def canonical_status(label: str) -> str:
    t = (label or "").lower()
    if "book" in t: return "booked"
    if "hold" in t or "pencil" in t or "option" in t: return "hold"
    if "unavailable" in t: return "unavailable"
    if "available" in t: return "available"
    if "request" in t: return "request"
    if "tentative" in t or "tbc" in t or "tbd" in t: return "tentative"
    if "wait" in t: return "waitlist"
    return t or "unknown"

BLOCK_INLINE_RE = re.compile(
    r"start\s*date\s*:\s*(?P<start>[^:]+?)\s+"
    r"end\s*date\s*:\s*(?P<end>[^:]+?)\s+"
    r"(?P<status>[A-Za-z][A-Za-z \-]{3,40})\s*:\s*(?P<locs>.*?)(?=(?:start\s*date\s*:|\Z))",
    re.IGNORECASE | re.DOTALL
)

ROUTE_RE = re.compile(r"^(.*?)\s+\bto\b\s+(.*?)(?:$|[.,;\n])", re.IGNORECASE)

def split_route(locs: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not isinstance(locs, str):
        return None, None, None
    first = locs.strip().splitlines()[0]
    m = ROUTE_RE.search(first)
    if m:
        from_loc = norm(m.group(1))
        to_loc = norm(m.group(2))
        return from_loc, to_loc, f"{from_loc} → {to_loc}"
    first_norm = norm(first)
    return first_norm, None, first_norm

# ---------- Core parsing function ----------
def parse_html_content(html_content: str, source_url: str) -> List[Dict]:
    from bs4 import BeautifulSoup
    rows = []
    
    # Extract text from HTML
    soup = BeautifulSoup(html_content, 'html.parser')
    text = soup.get_text()
    if not text:
        return rows

    yacht_name = extract_name(text)
    if not yacht_name:
        return rows

    for m in BLOCK_INLINE_RE.finditer(text):
        start_iso = parse_date_safe(m.group("start"))
        end_iso   = parse_date_safe(m.group("end"))
        if not (start_iso and end_iso):
            continue

        status = canonical_status(m.group("status"))
        from_loc, to_loc, route = split_route(m.group("locs"))

        rows.append({
            "yacht_name": yacht_name,
            "status": status,
            "start_date": start_iso,
            "end_date": end_iso,
            "from": from_loc,
            "to": to_loc,
            "route": route,
            "source_url": source_url,
        })
    return rows
PARSERCODE
PARSER

# --- Create the main server.py file in a single, atomic RUN command ---
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
import pandas as pd
from fastapi import FastAPI, Request, UploadFile, File, HTTPException, WebSocket, Response
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from contextlib import asynccontextmanager
import aiofiles
from app.cyaeb_availability_parser import parse_html_content

# --- Set up proper logging for better debugging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# --- Configuration ---
DATA_DIR = Path("/data")
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"
LOG_DIR = DATA_DIR / "logs"
URLS_CSV_PATH = INPUT_DIR / "urls.csv"
FILTERED_URLS_PATH = INPUT_DIR / "filtered_urls.txt"
AVAILABILITY_DATA_PATH = DATA_DIR / "availability.json"
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
    try:
        STATUS_FILE_PATH.write_text(json.dumps({"status": status, "pid": pid}))
    except Exception as e:
        log.error(f"!!! FAILED to write status file at {STATUS_FILE_PATH}: {e}")

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

# --- Data Processing for Calendar ---
async def process_mirrored_data_for_availability():
    log.info("Starting availability data extraction from mirrored files...")
    all_availability = []
    
    # Re-read the filtered URLs list to link HTML files back to their source URL
    url_map = {}
    if FILTERED_URLS_PATH.exists():
        df_urls = pd.read_csv(FILTERED_URLS_PATH, header=None, names=['url'])
        for url in df_urls['url']:
            # Create a simple mapping from domain/path to the original URL
            try:
                key = Path(httpx.URL(url).host) / Path(httpx.URL(url).path).relative_to('/')
                url_map[str(key)] = url
            except Exception:
                continue

    html_files = list(OUTPUT_DIR.rglob("*.html"))
    log.info(f"Found {len(html_files)} HTML files to process.")

    for html_file in html_files:
        try:
            relative_path = html_file.relative_to(OUTPUT_DIR)
            source_url = url_map.get(str(relative_path).replace('index.html', '')) or str(relative_path)
            
            async with aiofiles.open(html_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = await f.read()
            
            avail_rows = parse_html_content(content, source_url)
            if avail_rows:
                all_availability.extend(avail_rows)
        except Exception as e:
            log.warning(f"Could not process file {html_file}: {e}")

    log.info(f"Extracted {len(all_availability)} availability records.")
    async with aiofiles.open(AVAILABILITY_DATA_PATH, 'w') as f:
        await f.write(json.dumps(all_availability, indent=2))
    log.info(f"Availability data saved to {AVAILABILITY_DATA_PATH}")

# --- Background Mirroring Task ---
async def run_mirror_process():
    try:
        if not URLS_CSV_PATH.exists():
            raise FileNotFoundError("urls.csv not found.")
        
        log.info(f"Processing CSV file: {URLS_CSV_PATH}")
        df = pd.read_csv(URLS_CSV_PATH)
        
        filtered_df = df[df['status_code'] == 200]
        urls_to_mirror = filtered_df['final_url'].dropna().unique().tolist()

        if not urls_to_mirror:
            log.warning("No URLs with status_code 200 found. Nothing to mirror.")
            set_status("finished", "No valid URLs")
            return

        async with aiofiles.open(FILTERED_URLS_PATH, 'w') as f:
            await f.write("\n".join(urls_to_mirror))
        log.info(f"Found {len(urls_to_mirror)} URLs to mirror.")
    except Exception as e:
        log.error(f"Failed to process CSV: {e}", exc_info=True)
        set_status("error", f"CSV Error: {e}")
        return

    if LOG_FILE_PATH.exists():
        LOG_FILE_PATH.unlink()

    command = [
        "wget", "--mirror", "--page-requisites", "--adjust-extension",
        "--convert-links", "--span-hosts", "--no-parent", "--directory-prefix", str(OUTPUT_DIR),
        "--input-file", str(FILTERED_URLS_PATH), "--no-check-certificate", "-e", "robots=off",
        "--timeout=30", "--tries=3",
        "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
        "--append-output", str(LOG_FILE_PATH)
    ]
    
    process = None
    try:
        log.info(f"Executing wget command: {' '.join(command)}")
        process = await asyncio.create_subprocess_exec(*command)
        set_status("mirroring", process.pid)
        await process.wait()
        
        if process.returncode == 0:
            log.info("Wget finished successfully. Starting availability parsing.")
            await process_mirrored_data_for_availability()
            set_status("finished")
        else:
            log.error(f"Wget process failed with exit code: {process.returncode}")
            set_status("error", f"wget exited with code {process.returncode}")

    except Exception as e:
        log.error(f"Error running mirror process: {e}", exc_info=True)
        set_status("error", str(e))
    finally:
        if process and process.returncode is None:
            log.warning(f"Terminating runaway process PID {process.pid}")
            process.terminate()
            await process.wait()

# --- API Endpoints ---
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/availability", response_class=HTMLResponse)
async def availability_page(request: Request):
    return templates.TemplateResponse("availability.html", {"request": request})

@app.get("/availability-data", response_class=JSONResponse)
async def get_availability_data():
    if not AVAILABILITY_DATA_PATH.exists():
        return []
    async with aiofiles.open(AVAILABILITY_DATA_PATH, 'r') as f:
        data = await f.read()
    return json.loads(data)

@app.api_route("/health", methods=["GET", "HEAD"], status_code=200)
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
            return {"status": "Connected", "ip": data.get("ip"), "location": f"{data.get('city', '')}, {data.get('country', '')}"}
    except Exception as e:
        log.warning(f"VPN status check failed: {e}")
        return {"status": "Disconnected", "ip": "Error", "location": str(e)}

@app.post("/start")
async def start_mirroring():
    log.info("Received request to start mirroring.")
    async with mirror_lock:
        if get_status()['status'] == 'mirroring':
            raise HTTPException(409, "Mirroring process already running.")
        asyncio.create_task(run_mirror_process())
    return RedirectResponse(url="/", status_code=303)

@app.post("/stop")
async def stop_mirroring():
    log.info("Received request to stop mirroring.")
    status = get_status()
    pid = status.get('pid')
    if status.get('status') != 'mirroring' or not pid:
        raise HTTPException(404, "No mirroring process is running.")
    try:
        os.kill(pid, 15) # SIGTERM
        set_status("idle")
    except ProcessLookupError:
        set_status("idle")
    return RedirectResponse(url="/", status_code=303)

@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    if not file.filename.endswith('.csv'):
        raise HTTPException(400, "Invalid file type.")
    async with aiofiles.open(URLS_CSV_PATH, "wb") as buffer:
        await buffer.write(await file.read())
    return RedirectResponse(url="/", status_code=303)

@app.get("/sites-list", response_class=JSONResponse)
async def get_sites_list():
    sites_info = []
    if OUTPUT_DIR.exists():
        for site_dir in sorted(OUTPUT_DIR.iterdir()):
            if site_dir.is_dir():
                # Find any html file to serve as the entry point
                index_path = next(site_dir.rglob("*.html"), None)
                if index_path:
                    sites_info.append({"name": site_dir.name, "index_path": str(index_path.relative_to(site_dir))})
    return sites_info

@app.get("/download/{site_name}")
def download_zip(site_name: str):
    site_path = (OUTPUT_DIR / site_name).resolve()
    if not site_path.is_dir() or OUTPUT_DIR.resolve() not in site_path.parents:
        raise HTTPException(404, "Site not found.")
    
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

# --- Create the main index.html template file ---
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
        .card-header { padding: 1.5rem; background: var(--secondary-color); color: var(--white); display: flex; justify-content: space-between; align-items: center;}
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
        .button, .button-group { display: flex; gap: 1rem; margin-top: 1rem; }
        button, a.button, input[type=submit] { background-color: var(--primary-color); color: var(--white); border: none; padding: 10px 18px; border-radius: 5px; cursor: pointer; font-size: 1rem; font-weight: 500; transition: background-color 0.2s; text-decoration: none; }
        button:hover, a.button:hover, input[type=submit]:hover { background-color: #2980b9; }
        button:disabled { background-color: #bdc3c7; cursor: not-allowed; }
        #log-viewer { background: #282c34; color: #abb2bf; font-family: 'SF Mono', 'Fira Code', 'Fira Mono', 'Roboto Mono', monospace; padding: 1rem; border-radius: 5px; height: 300px; overflow-y: auto; white-space: pre-wrap; margin-top: 1rem; }
        table { width: 100%; border-collapse: collapse; margin-top: 1rem; }
        th, td { padding: 12px 15px; border-bottom: 1px solid #e0e0e0; text-align: left;}
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
            <div class="card-header"><h2>System Status</h2></div>
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
        <div class="card-header">
            <h2>Mirrored Websites</h2>
            <a href="/availability" class="button">View Availability Calendar</a>
        </div>
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
    
    async function fetchInitialData() {
        try {
            const [statusRes, sitesRes, csvRes, vpnRes] = await Promise.all([
                fetch('/status'), fetch('/sites-list'), fetch('/csv-status'), fetch('/vpn-status')
            ]);
            updateUI(await statusRes.json());
            updateSitesTable(await sitesRes.json());
            csvStatus.textContent = (await csvRes.json()).message;
            updateVpnUI(await vpnRes.json());
        } catch (error) { console.error("Failed to fetch initial data:", error); }
    }
    
    function updateVpnUI(vpnData) {
        vpnStatusText.textContent = vpnData.status;
        vpnDot.className = `dot ${vpnData.status.toLowerCase()}`;
        vpnIp.textContent = vpnData.ip || 'N/A';
        vpnLocation.textContent = vpnData.location || 'N/A';
        vpnStatusText.parentElement.className = `status-indicator ${vpnData.status.toLowerCase()}`;
    }
    
    setInterval(async () => {
        try { const vpnRes = await fetch('/vpn-status'); updateVpnUI(await vpnRes.json()); }
        catch (error) { console.error("Failed to fetch VPN status:", error); }
    }, 10000);

    function updateSitesTable(sites) {
        if (!sites || sites.length === 0) {
            sitesTableBody.innerHTML = '<tr><td colspan="2">No sites mirrored yet.</td></tr>'; return;
        }
        sitesTableBody.innerHTML = sites.map(site => 
            `<tr><td>${site.name}</td><td><a href="/sites/${site.name}/${site.index_path}" target="_blank">View Site</a> | <a href="/download/${site.name}">Download ZIP</a></td></tr>`
        ).join('');
    }

    function connectWebSocket() {
        const ws = new WebSocket(`${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.host}/ws/status`);
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

# --- Create the new availability.html template file ---
RUN <<'AVAILHTML' bash
cat > /app/templates/availability.html <<'HTMLCODE'
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Yacht Availability Calendar</title>
    <style>
        :root { --primary-color: #3498db; --secondary-color: #2c3e50; --light-gray: #ecf0f1; --white: #fff; --booked: #e74c3c; --hold: #f39c12; --available: #2ecc71; --unavailable: #95a5a6; }
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; margin: 0; background-color: #f8f9fa; color: var(--secondary-color); }
        .container { max-width: 1200px; margin: 2rem auto; padding: 0 1rem; }
        h1 { color: var(--secondary-color); text-align: center; margin-bottom: 1rem; }
        .controls { display: flex; justify-content: space-between; align-items: center; margin-bottom: 2rem; padding: 1rem; background: var(--white); border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.05); }
        #month-year { font-size: 1.5rem; font-weight: 600; }
        .controls button { background-color: var(--primary-color); color: var(--white); border: none; padding: 10px 18px; border-radius: 5px; cursor: pointer; font-size: 1rem; }
        #yacht-selector { padding: 10px; border-radius: 5px; border: 1px solid #ccc; font-size: 1rem; }
        .calendar { display: grid; grid-template-columns: repeat(7, 1fr); gap: 5px; }
        .day, .header { background: var(--white); padding: 10px; border-radius: 4px; min-height: 120px; display: flex; flex-direction: column; }
        .header { min-height: auto; text-align: center; font-weight: bold; background-color: var(--secondary-color); color: var(--white); }
        .day .date-num { font-weight: bold; margin-bottom: 5px; }
        .day.other-month { background-color: var(--light-gray); color: #aaa; }
        .event { font-size: 0.8rem; padding: 3px 5px; border-radius: 3px; margin-bottom: 3px; color: white; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .event.booked { background-color: var(--booked); }
        .event.hold { background-color: var(--hold); }
        .event.available { background-color: var(--available); }
        .event.unavailable { background-color: var(--unavailable); }
        .event.unknown { background-color: #bdc3c7; }
        #back-link { display: inline-block; margin-bottom: 1rem; font-weight: 500;}
    </style>
</head>
<body>
<div class="container">
    <a href="/" id="back-link">&larr; Back to Main Cockpit</a>
    <h1>Yacht Availability Calendar</h1>
    <div class="controls">
        <button id="prev-month">&lt;</button>
        <select id="yacht-selector"><option value="">All Yachts</option></select>
        <h2 id="month-year"></h2>
        <button id="next-month">&gt;</button>
    </div>
    <div class="calendar" id="calendar-grid"></div>
</div>
<script>
    document.addEventListener('DOMContentLoaded', async () => {
        const calendarGrid = document.getElementById('calendar-grid');
        const monthYearEl = document.getElementById('month-year');
        const prevMonthBtn = document.getElementById('prev-month');
        const nextMonthBtn = document.getElementById('next-month');
        const yachtSelector = document.getElementById('yacht-selector');

        let currentDate = new Date();
        let allEvents = [];
        
        async function fetchData() {
            try {
                const response = await fetch('/availability-data');
                allEvents = await response.json();
                populateYachtFilter();
                renderCalendar();
            } catch (error) {
                console.error("Failed to fetch availability data:", error);
                calendarGrid.innerHTML = "<p>Could not load availability data. Please run a mirror process first.</p>";
            }
        }

        function populateYachtFilter() {
            const yachtNames = [...new Set(allEvents.map(e => e.yacht_name))].sort();
            yachtSelector.innerHTML = '<option value="">All Yachts</option>';
            yachtNames.forEach(name => {
                const option = document.createElement('option');
                option.value = name;
                option.textContent = name;
                yachtSelector.appendChild(option);
            });
        }

        function renderCalendar() {
            calendarGrid.innerHTML = '';
            const month = currentDate.getMonth();
            const year = currentDate.getFullYear();
            
            monthYearEl.textContent = `${currentDate.toLocaleString('default', { month: 'long' })} ${year}`;

            const firstDay = new Date(year, month, 1);
            const lastDay = new Date(year, month + 1, 0);
            const daysInMonth = lastDay.getDate();
            const startDayOfWeek = firstDay.getDay();

            // Headers
            ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'].forEach(day => {
                const header = document.createElement('div');
                header.className = 'header';
                header.textContent = day;
                calendarGrid.appendChild(header);
            });

            // Blank days
            for (let i = 0; i < startDayOfWeek; i++) {
                const blank = document.createElement('div');
                blank.className = 'day other-month';
                calendarGrid.appendChild(blank);
            }

            const selectedYacht = yachtSelector.value;
            const filteredEvents = selectedYacht 
                ? allEvents.filter(e => e.yacht_name === selectedYacht) 
                : allEvents;

            for (let i = 1; i <= daysInMonth; i++) {
                const dayEl = document.createElement('div');
                dayEl.className = 'day';
                const dayDate = new Date(year, month, i);

                const dateNum = document.createElement('div');
                dateNum.className = 'date-num';
                dateNum.textContent = i;
                dayEl.appendChild(dateNum);

                const eventsForDay = filteredEvents.filter(event => {
                    const startDate = new Date(event.start_date);
                    const endDate = new Date(event.end_date);
                    return dayDate >= startDate && dayDate <= endDate;
                });
                
                eventsForDay.forEach(event => {
                    const eventEl = document.createElement('div');
                    eventEl.className = `event ${event.status}`;
                    eventEl.textContent = event.yacht_name;
                    eventEl.title = `${event.yacht_name} - ${event.status.toUpperCase()}\\n${event.start_date} to ${event.end_date}`;
                    dayEl.appendChild(eventEl);
                });

                calendarGrid.appendChild(dayEl);
            }
        }

        prevMonthBtn.addEventListener('click', () => {
            currentDate.setMonth(currentDate.getMonth() - 1);
            renderCalendar();
        });

        nextMonthBtn.addEventListener('click', () => {
            currentDate.setMonth(currentDate.getMonth() + 1);
            renderCalendar();
        });
        
        yachtSelector.addEventListener('change', renderCalendar);

        fetchData();
    });
</script>
</body>
</html>
HTMLCODE
AVAILHTML
