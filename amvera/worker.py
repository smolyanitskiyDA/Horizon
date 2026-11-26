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
from src.ai.errors import AIProviderUnavailableError
from .digest import configuration, prepare, select_model

ZONE = ZoneInfo("Europe/Moscow")
PREPARE_AT = dt.time(7, 30)
SEND_AT = dt.time(8)
PLAN_RETRY_SECONDS = 8 * 60 * 60
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
    db.execute("CREATE TABLE IF NOT EXISTS plan_backoff (id INTEGER PRIMARY KEY CHECK(id=1), retry_at REAL NOT NULL)")
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
        backoff = db.execute("SELECT retry_at FROM plan_backoff WHERE id=1").fetchone()
        if backoff and now.timestamp() < backoff[0]:
            return "waiting_for_plan_limit"
        async def generate():
            async with asyncio.timeout(1200):
                return await build(cutoff, state, os.getenv("HORIZON_CHATGPT_MODEL", "").strip())
        try:
            body = asyncio.run(generate())
        except AIProviderUnavailableError as exc:
            if "subscription_sharing_usage_limit_exceeded" not in str(exc):
                raise
            retry_at = time.time() + PLAN_RETRY_SECONDS
            db.execute("INSERT OR REPLACE INTO plan_backoff(id, retry_at) VALUES (1, ?)", (retry_at,))
            log_failure(exc)
            print("Horizon plan retry at:", dt.datetime.fromtimestamp(retry_at, ZONE).isoformat(), flush=True)
            return "waiting_for_plan_limit"
        db.execute("DELETE FROM plan_backoff WHERE id=1")
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


def log_failure(exc):
    # Never print arbitrary exception text, URLs, response bodies or account IDs.
    print("Horizon:", type(exc).__name__, flush=True)
    message = str(exc)
    if isinstance(exc, (AIProviderUnavailableError, RuntimeError)):
        # These are fixed messages emitted by the first-party Horizon auth adapter.
        known = {
            "ChatGPT session expired or was revoked. Run horizon-chatgpt login.": "session_expired_or_revoked",
            "Invalid ChatGPT client registration; reconnect the saved account.": "invalid_client_registration",
            "ChatGPT plan usage is not enabled. Run horizon-chatgpt login --enable-plan.": "plan_permission_missing",
            "ChatGPT plan permission was removed. Run horizon-chatgpt login --enable-plan.": "plan_permission_removed",
            "ChatGPT is not connected. Run horizon-chatgpt login.": "not_connected",
            "ChatGPT is signed out. Run horizon-chatgpt login.": "signed_out",
            "No ChatGPT account selected. Run horizon-chatgpt login.": "no_account",
            "Could not reach the ChatGPT service; saved credentials were retained.": "auth_network_failure",
            "ChatGPT connection failed; partial output was discarded.": "inference_network_failure",
            "Invalid ChatGPT model catalog.": "invalid_model_catalog",
            "ChatGPT refresh identity mismatch; credentials retained.": "refresh_identity_mismatch",
        }
        if message in known:
            print("Horizon diagnostic:", known[message], flush=True)
        elif message.startswith(("ChatGPT request failed", "ChatGPT plan usage limit reached.")):
            # Allowlist known service codes only; never echo arbitrary API text.
            code = next((c for c in (
                "subscription_sharing_usage_limit_exceeded",
                "subscription_sharing_user_not_eligible",
                "subscription_sharing_unsupported_capability",
                "subscription_sharing_route_not_supported",
                "subscription_sharing_invalid_user",
                "chatpass_v2_scope_not_authorized",
                "chatpass_v2_invalid_authorization_context",
                "model_not_found", "invalid_api_key", "invalid_request_error",
                "rate_limit_exceeded", "insufficient_quota", "unsupported_country_region_territory",
            ) if re.search(r"(?<![A-Za-z0-9_])" + c + r"(?![A-Za-z0-9_])", message)), "unknown_error")
            http = re.search(r"\bHTTP ([1-5][0-9]{2})\b", message)
            print("Horizon diagnostic: OpenAI code=" + code + " HTTP=" + (http.group(1) if http else "not_available"), flush=True)
        elif message.startswith("ChatGPT token refresh failed (HTTP "):
            http = re.search(r"\bHTTP ([1-5][0-9]{2})\b", message)
            print("Horizon diagnostic: refresh_failed HTTP=" + (http.group(1) if http else "not_available"), flush=True)
    elif isinstance(exc, ValueError) and message == "No matching model in the connected plan":
        print("Horizon diagnostic: no_matching_model", flush=True)


def log_auth_state():
    try:
        session = ChatGPTAuth().status()
        expires = session.get("expires_at")
        refresh_due = isinstance(expires, (int, float)) and expires <= time.time() + 60
        print("Horizon auth: connected=" + str(bool(session.get("connected"))).lower()
              + " plan_usage=" + str(bool(session.get("plan_usage"))).lower()
              + " refresh_due=" + str(refresh_due).lower()
              + " model_override=" + str(bool(os.getenv("HORIZON_CHATGPT_MODEL", "").strip())).lower(), flush=True)
    except Exception as exc:
        print("Horizon auth status unavailable:", type(exc).__name__, flush=True)


def main():
    global status
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="Verify the live ChatGPT plan and Telegram recipient")
    args = parser.parse_args()
    protected_storage()
    log_auth_state()
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
            log_failure(exc)
            log_auth_state()
            pause = 600
        if new_status != status:
            status = new_status
            print("Horizon:", status, flush=True)
        now = dt.datetime.now(ZONE)
        until_send = (dt.datetime.combine(now.date(), SEND_AT, ZONE) - now).total_seconds()
        time.sleep(min(pause, until_send) if until_send > 0 else pause)


if __name__ == "__main__":
    main()
