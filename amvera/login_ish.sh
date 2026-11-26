#!/bin/sh
# Run the same Horizon OAuth client on the device that runs Safari.
set -eu
umask 077
apk add --no-cache ca-certificates python3 py3-pip py3-cryptography
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
mkdir "$scratch/horizon_auth"
touch "$scratch/horizon_auth/__init__.py"
base=https://raw.githubusercontent.com/smolyanitskiyDA/Horizon/3f3671e49e794a8f2c80cf09dcba5ac29b4fc614/src/ai
for module in chatgpt_auth errors; do
    wget -q -O "$scratch/horizon_auth/$module.py" "$base/$module.py"
done
(cd "$scratch/horizon_auth" && sha256sum -c <<'SUMS'
d3aa6e93ed1e3ec6aac2565ea3fa6e95a2d0489dd90f97ba86753b0c17a2baf5  chatgpt_auth.py
8a442f3bc2dfca0e6fd7d653929ec0544dc9fa28751032bb0780bd5c073332cb  errors.py
SUMS
)
python3 -m pip install -q --disable-pip-version-check --only-binary=:all: --target "$scratch/deps" \
    'httpx==0.28.1' 'filelock==3.18.0' 'PyJWT==2.10.1'
cd "$scratch"
PYTHONPATH="$scratch:$scratch/deps" python3 - "${1:-login}" <<'PY'
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import sys
import tempfile
import threading

from jwt import algorithms
from horizon_auth.chatgpt_auth import ChatGPTAuth

if "RS256" not in algorithms.get_default_algorithms():
    raise SystemExit("Отсутствует поддержка проверки подписи RS256.")
if sys.argv[1] == "--check":
    with tempfile.TemporaryDirectory() as tmp:
        assert ChatGPTAuth(Path(tmp)).initialize()["ext_agent_host_id"].startswith("urn:uuid:")
    print("Horizon OAuth client ready")
    raise SystemExit

root = Path.home() / "Horizon-login"
root.mkdir(mode=0o700, exist_ok=True)
directory = Path(tempfile.mkdtemp(prefix="session-", dir=root))
auth = ChatGPTAuth(directory)
class Start(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/" or self.headers.get("Host") != local_host:
            self.send_error(404)
            return
        self.send_response(302)
        self.send_header("Location", self.server.authorization_url)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Referrer-Policy", "no-referrer")
        self.end_headers()
    def log_message(self, *args):
        pass

with ThreadingHTTPServer(("127.0.0.1", 0), Start) as local:
    local_host = "127.0.0.1:%d" % local.server_port
    def show(url):
        local.authorization_url = url
        threading.Thread(target=local.serve_forever, daemon=True).start()
        print("Откройте в Safari на этом iPhone: http://%s/" % local_host, flush=True)
        return True
    session = auth.sign_in(enable_plan=True, timeout=900, open_browser=show)
if not session["plan_usage"]:
    raise SystemExit("Разрешение на использование подписки не получено.")
print("Подписка подтверждена. В Amvera загрузите только файл:")
print(directory / "credentials.json")
PY
