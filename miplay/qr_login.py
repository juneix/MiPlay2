"""Xiaomi Passport QR Code Login Service."""

from __future__ import annotations

import asyncio
import json
import logging
import secrets
import time

import aiohttp

log = logging.getLogger("miplay")

ACCOUNT_BASE_URL = "https://account.xiaomi.com"
LONG_POLLING_URL = "https://account.xiaomi.com/longPolling/loginUrl"
QR_LOGIN_SID = "mijia"
USER_AGENT_TEMPLATE = (
    "Android-7.1.1-1.0.0-ONEPLUS A3010-136-%s APP/xiaomi.smarthome APPV/62830"
)
POLL_TIMEOUT_SECONDS = 35
MAX_POLL_COUNT = 20
SESSION_TTL_SECONDS = 300

STATE_WAITING = "waiting"
STATE_CONFIRMED = "confirmed"
STATE_EXPIRED = "expired"
STATE_FAILED = "failed"


def _strip_json_prefix(body: str) -> str:
    """Remove Xiaomi API response JSON prefix &&&START&&&."""
    return body.replace("&&&START&&&", "").strip()


def _get_str(obj: dict, key: str, default: str = "") -> str:
    v = obj.get(key)
    if v is None:
        return default
    if isinstance(v, str):
        return v
    if isinstance(v, (int, float)):
        return str(int(v))
    return str(v)


def _normalize_url(url: str) -> str:
    if url.startswith("http://") or url.startswith("https://"):
        return url
    if not url.startswith("/"):
        url = "/" + url
    return f"{ACCOUNT_BASE_URL}{url}"


class QRCodeSession:
    """Single QR code login session."""

    def __init__(self):
        self.device_id = secrets.token_hex(16)
        self.user_agent = USER_AGENT_TEMPLATE % self.device_id
        self.state = STATE_WAITING
        self.poll_url = ""
        self.poll_count = 0
        self.created_at = time.time()
        self.credentials: dict[str, str] | None = None
        self._session = aiohttp.ClientSession(
            cookie_jar=aiohttp.CookieJar(unsafe=True)
        )

    async def get_qrcode(self) -> dict | None:
        """Fetch QR code info and long-polling URL."""
        try:
            url1 = f"{ACCOUNT_BASE_URL}/pass/serviceLogin?sid={QR_LOGIN_SID}&_json=true"
            headers1 = {
                "User-Agent": self.user_agent,
                "Cookie": f"sdkVersion=3.8.6; deviceId={self.device_id}",
            }
            async with self._session.get(url1, headers=headers1) as resp1:
                data1 = json.loads(_strip_json_prefix(await resp1.text()))

            sign = _get_str(data1, "_sign")
            qs = _get_str(data1, "qs")
            callback = _get_str(data1, "callback")
            if not (sign and qs and callback):
                log.warning("[QRLogin] Missing sign/qs/callback from serviceLogin")
                return None

            params2 = {
                "bizDevice": "",
                "appName": "",
                "showType": "qr-code",
                "sid": QR_LOGIN_SID,
                "qs": qs,
                "callback": callback,
                "_sign": sign,
                "_json": "true",
            }
            headers2 = {
                "User-Agent": self.user_agent,
                "Cookie": f"sdkVersion=3.8.6; deviceId={self.device_id}",
            }
            async with self._session.get(LONG_POLLING_URL, params=params2, headers=headers2) as resp2:
                data2 = json.loads(_strip_json_prefix(await resp2.text()))

            login_url = _get_str(data2, "loginUrl")
            qr_url = _get_str(data2, "qr")
            lp_url = _get_str(data2, "lp")
            if not (login_url and qr_url and lp_url):
                log.warning("[QRLogin] Missing loginUrl/qr/lp from longPolling")
                return None

            self.poll_url = _normalize_url(lp_url)
            qrcode_img_url = _normalize_url(qr_url)
            return {
                "qrcode_url": qrcode_img_url,
                "login_url": login_url,
            }
        except Exception as exc:
            log.warning("[QRLogin] Error in get_qrcode: %s", exc)
            return None

    async def poll_once(self) -> str:
        """Poll Xiaomi passport endpoint once."""
        if self.state != STATE_WAITING or not self.poll_url:
            return self.state

        if self.poll_count >= MAX_POLL_COUNT:
            self.state = STATE_EXPIRED
            return self.state

        if time.time() - self.created_at > SESSION_TTL_SECONDS:
            self.state = STATE_EXPIRED
            return self.state

        self.poll_count += 1
        headers = {
            "User-Agent": self.user_agent,
            "Cookie": f"sdkVersion=3.8.6; deviceId={self.device_id}",
        }

        try:
            timeout = aiohttp.ClientTimeout(total=POLL_TIMEOUT_SECONDS)
            async with self._session.get(self.poll_url, headers=headers, timeout=timeout) as resp:
                raw_text = await resp.text()
                data = json.loads(_strip_json_prefix(raw_text))

            code = data.get("code")
            if code == 0:
                pass_token = _get_str(data, "passToken")
                user_id = _get_str(data, "userId")
                p_user_id = _get_str(data, "pUserId")
                final_user_id = user_id or p_user_id
                if pass_token and final_user_id:
                    self.credentials = {
                        "userId": final_user_id,
                        "passToken": pass_token,
                    }
                    self.state = STATE_CONFIRMED
                    log.info("[QRLogin] QR login confirmed for userId=%s", final_user_id)
                    return self.state
            elif code in (70016, 70000):
                pass
            elif code in (87001, 70014):
                self.state = STATE_EXPIRED
                return self.state
        except asyncio.TimeoutError:
            pass
        except Exception as exc:
            log.warning("[QRLogin] Poll exception: %s", exc)

        return self.state

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()


class QRLoginManager:
    """Manages active QR login sessions."""

    def __init__(self):
        self._sessions: dict[str, QRCodeSession] = {}
        self._lock = asyncio.Lock()

    async def start(self) -> tuple[str, dict] | None:
        async with self._lock:
            self._cleanup_expired()
            session = QRCodeSession()
            info = await session.get_qrcode()
            if not info:
                await session.close()
                return None

            session_id = secrets.token_hex(16)
            self._sessions[session_id] = session
            return session_id, info

    async def poll(self, session_id: str) -> dict:
        session = self._sessions.get(session_id)
        if not session:
            return {"state": STATE_EXPIRED, "credentials": None}

        state = await session.poll_once()
        res = {
            "state": state,
            "credentials": session.credentials if state == STATE_CONFIRMED else None,
        }
        if state in (STATE_CONFIRMED, STATE_EXPIRED, STATE_FAILED):
            await session.close()
            self._sessions.pop(session_id, None)
        return res

    async def cancel(self, session_id: str):
        async with self._lock:
            session = self._sessions.pop(session_id, None)
            if session:
                await session.close()

    def _cleanup_expired(self):
        now = time.time()
        expired = [
            sid for sid, sess in self._sessions.items()
            if now - sess.created_at > SESSION_TTL_SECONDS
        ]
        for sid in expired:
            sess = self._sessions.pop(sid, None)
            if sess:
                asyncio.create_task(sess.close())
