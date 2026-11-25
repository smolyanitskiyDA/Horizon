import datetime as dt
import json
import os
from pathlib import Path
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from zoneinfo import ZoneInfo

DATA = Path("/data")
AUTH = DATA / "chatgpt"
STATE = DATA / "radar"
BIN = Path("/app/.venv/bin")
os.umask(0o077)
for directory in (AUTH, STATE):
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    if directory.is_symlink():
        raise RuntimeError("Unexpected storage symlink")
    if os.getuid() == 0:
        os.chown(directory, 10001, 10001)
    directory.chmod(0o700)
if os.getuid() == 0:
    os.setgroups([])
    os.setgid(10001)
    os.setuid(10001)

status = "waiting_for_authorization"
class Health(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in ("/", "/health"):
            self.send_error(404)
            return
        body = json.dumps({"service": "horizon", "status": status}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *args):
        pass

threading.Thread(target=HTTPServer(("0.0.0.0", 8080), Health).serve_forever, daemon=True).start()
print("Horizon worker started; waiting for protected configuration and authorization", flush=True)
zone = ZoneInfo("Europe/Moscow")
marker = STATE / "last_attempt"
while True:
    now = dt.datetime.now(zone)
    today = now.date().isoformat()
    model = os.environ.get("HORIZON_CHATGPT_MODEL", "").strip()
    config = STATE / "config.json"
    if not (AUTH / "credentials.json").is_file() or not model or not config.is_file():
        status = "waiting_for_authorization_or_configuration"
    else:
        status = "ready"
        if os.environ.get("RADAR_ENABLE_SCHEDULE") == "1" and now.hour == 8 and (not marker.exists() or marker.read_text().strip() != today):
            try:
                settings = json.loads(config.read_text())
                ai = settings.get("ai", {})
                if ai.get("provider") != "openai-chatgpt" or ai.get("model") != model or ai.get("provider_chain"):
                    raise ValueError("Only the explicitly authorized subscription model is allowed")
                marker.write_text(today)
                status = "running"
                subprocess.run([str(BIN / "horizon"), "--data-dir", str(STATE), "--config", str(config), "--hours", "24"], cwd="/app", check=True, timeout=3600)
                status = "completed"
            except Exception as exc:
                status = "failed"
                print("Daily run failed:", type(exc).__name__, flush=True)
    time.sleep(30)
