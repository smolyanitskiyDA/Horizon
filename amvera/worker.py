"""Daily preparation and Telegram delivery on Amvera's persistent volume."""

import argparse
import asyncio
import datetime as dt
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import threading
import time
from zoneinfo import ZoneInfo

from filelock import FileLock, Timeout

from src.ai.chatgpt import ChatGPTClient
from src.ai.chatgpt_auth import ChatGPTAuth
from .digest import configuration, prepare, select_model

ZONE = ZoneInfo("Europe/Moscow")
PREPARE_AT = dt.time(7, 30)
SEND_AT = dt.time(8)
AUTH = Path("/data/chatgpt")
STATE = Path("/data/radar")
status = "starting"


def protected_storage():
    os.umask(0o077)
    for directory in (AUTH, STATE):
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        if not stat.S_ISDIR(directory.lstat().st_mode):
            raise RuntimeError("Unexpected storage path")
        if os.getuid() == 0:
            os.chown(directory, 10001, 10001)
        directory.chmod(0o700)
    # Also support files uploaded through the authenticated Amvera file manager.
    for name in ("host.json", "credentials.json", ".lock"):
        path = AUTH / name
        try:
            info = path.lstat()
        except FileNotFoundError:
            continue
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise RuntimeError("Unexpected authorization file")
        if os.getuid() == 0:
            os.chown(path, 10001, 10001)
        path.chmod(0o600)
    if os.getuid() == 0:
        os.setgroups([])
        os.setgid(10001)
        os.setuid(10001)
    os.environ["HORIZON_CHATGPT_AUTH_DIR"] = str(AUTH)
    ChatGPTAuth().initialize()


class Health(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in ("/", "/health"):
            self.send_error(404)
            return
        body = json.dumps({"service": "horizon", "status": status, "schedule": "08:00 Europe/Moscow"}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


class TelegramRejected(Exception):
    def __init__(self, delay=300):
        self.delay = delay


def telegram_settings():
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if re.fullmatch(r"[0-9]+:[A-Za-z0-9_-]+", token) and re.fullmatch(r"-?[0-9]+", chat):
        return token, chat
    return None


def bot_call(token, method, payload):
    # Keep the bot token out of HTTP client request logs and exception messages.
    conn = http.client.HTTPSConnection("api.telegram.org", timeout=25)
    try:
        try:
            conn.connect()
        except OSError:
            raise TelegramRejected(60) from None  # No request has been written.
        conn.request("POST", f"/bot{token}/{method}", json.dumps(payload).encode(),
                     {"Content-Type": "application/json"})
        response = conn.getresponse()
        body = json.loads(response.read())
        if response.status == 200 and body.get("ok") is True:
            return body["result"]
        if 400 <= response.status < 500 and body.get("ok") is False:
            raise TelegramRejected(max(60, int(body.get("parameters", {}).get("retry_after", 300))))
        raise RuntimeError("Unconfirmed Telegram response")
    finally:
        conn.close()


def open_db(state):
    db = sqlite3.connect(state / "editions.sqlite3", isolation_level=None)
    db.execute("""CREATE TABLE IF NOT EXISTS editions (
        day TEXT PRIMARY KEY, body TEXT NOT NULL, stage TEXT NOT NULL,
        retry_at REAL NOT NULL DEFAULT 0, message_id INTEGER)""")
    return db


def tick(now, db, state, settings, ready, *, build=prepare, call=bot_call):
    day = now.date().isoformat()
    cutoff = dt.datetime.combine(now.date(), PREPARE_AT, ZONE)
    if now < cutoff:
        return "ready" if ready and settings else "waiting_for_configuration"
    row = db.execute("SELECT body, stage, retry_at FROM editions WHERE day=?", (day,)).fetchone()
    if row and row[1] != "prepared":
        return "sent" if row[1] == "sent" else "delivery_unconfirmed"
    if not settings:
        return "waiting_for_telegram"
    if row is None:
        if not ready:
            return "waiting_for_chatgpt"
        async def generate():
            async with asyncio.timeout(1200):
                return await build(cutoff, state, os.getenv("HORIZON_CHATGPT_MODEL", "").strip())
        body = asyncio.run(generate())
        db.execute("INSERT INTO editions(day, body, stage) VALUES (?, ?, 'prepared')", (day, body))
        row = (body, "prepared", 0)
    if now.time() < SEND_AT or now.timestamp() < row[2]:
        return "prepared"
    token, chat = settings
    # A process crash or lost acknowledgement after sending must not resend the edition.
    db.execute("UPDATE editions SET stage='sending' WHERE day=?", (day,))
    try:
        result = call(token, "sendMessage", {"chat_id": chat, "text": row[0], "parse_mode": "HTML",
                                            "link_preview_options": {"is_disabled": True}})
        message_id = result["message_id"]
        if type(message_id) is not int or message_id <= 0 or str(result["chat"]["id"]) != chat:
            raise ValueError("Unconfirmed Telegram receipt")
    except TelegramRejected as err:
        db.execute("UPDATE editions SET stage='prepared', retry_at=? WHERE day=?",
                   (now.timestamp() + err.delay, day))
        return "telegram_retry"
    except Exception:
        return "delivery_unconfirmed"
    db.execute("UPDATE editions SET stage='sent', message_id=? WHERE day=?", (message_id, day))
    return "sent"


async def check_live(settings):
    if not settings:
        raise ValueError("Telegram settings missing")
    auth = ChatGPTAuth()
    account = auth.status()["client_id"]
    model = select_model(auth, account, os.getenv("HORIZON_CHATGPT_MODEL", "").strip())
    client = ChatGPTClient(configuration(model, account).ai, auth=auth)
    try:
        answer = await client.complete("Return exactly the JSON object requested.", '{"ok":true}')
        if json.loads(answer) != {"ok": True}:
            raise ValueError("Model check failed")
    finally:
        await client.client.close()
    token, chat = settings
    if not bot_call(token, "getMe", {})["is_bot"] or str(bot_call(token, "getChat", {"chat_id": chat})["id"]) != chat:
        raise ValueError("Telegram check failed")
    print("ChatGPT plan inference and Telegram bot/chat verified.", flush=True)


def main():
    global status
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="Verify the live ChatGPT plan and Telegram recipient")
    args = parser.parse_args()
    protected_storage()
    if args.check:
        asyncio.run(check_live(telegram_settings()))
        return
    server = ThreadingHTTPServer(("0.0.0.0", 8080), Health)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    db = open_db(STATE)
    while True:
        pause = 5
        try:
            with FileLock(STATE / "edition.lock", timeout=0, mode=0o600):
                if os.getenv("RADAR_ENABLE_SCHEDULE", "1") == "1":
                    now = dt.datetime.now(ZONE)
                    new_status = tick(now, db, STATE, telegram_settings(), ChatGPTAuth().status()["plan_usage"])
                else:
                    new_status = "paused"
        except Timeout:
            new_status = "worker_busy"
        except Exception as exc:
            new_status = "generation_or_configuration_failed"
            print("Horizon:", type(exc).__name__, flush=True)
            pause = 600
        if new_status != status:
            status = new_status
            print("Horizon:", status, flush=True)
        now = dt.datetime.now(ZONE)
        until_send = (dt.datetime.combine(now.date(), SEND_AT, ZONE) - now).total_seconds()
        time.sleep(min(pause, until_send) if until_send > 0 else pause)


if __name__ == "__main__":
    main()
