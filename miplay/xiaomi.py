# ============================================================
# ⚠️ 强同步警示 (Sync Warning)
# ------------------------------------------------------------
# 本模块为 小米账号登录鉴权、云端设备同步及 Mina 播放控制器。
# 任何 Mina API 下发、设备同步修改请务必严格遵照项目技术文档:
# 📖 /docs/airplay.md
# ============================================================

"""Xiaomi account auth, device sync, and playback target control."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re

import aiohttp
from miservice import MiAccount, MiIOService, MiNAService

from miplay.config import Config, TargetConfig, get_device_id, MIIO_UA
from miplay.notify import Notifier

log = logging.getLogger("miplay")

MUSIC_API_MODELS = [
    # ---- 2.4G 系列 ----
    "LX04",   # 小爱触屏音箱
    "LX5A",   # 小爱音箱 万能遥控版 (铭牌/MIoT: LX05A)
    "L07A",   # Redmi 小爱音箱 Play
    "X08C",   # Redmi 小爱触屏音箱 8
    "L05B",   # 小米小爱音箱 Play
    "L05C",   # 小米小爱音箱 Play 增强版
    "X6A",    # Xiaomi 智能家庭屏 6
    "ASX4B",  # Xiaomi 智能家庭屏 Mini (部分批次: X4B / X8F)
    "X4B",    # Xiaomi 智能家庭屏 Mini
    # ---- 5G 系列 ----
    "M01",    # 小米小爱音箱 HD (铭牌: XMYX01JY, MIoT: SM4)
    "L06A",   # 小爱音箱
    "LX06",   # 小爱音箱 Pro
    "X08A",   # 小米小爱触屏音箱 Pro 8
    "L09A",   # 小爱音箱 Art
    "X08E",   # Redmi 小爱触屏音箱 Pro 8
    "L09B",   # 小爱音箱 Art 电池版
    "L15A",   # 小米 AI 音箱 2
    "X10A",   # Xiaomi 智能家庭屏 10
    "OH2",    # Xiaomi 智能音箱
    "OH2P",   # Xiaomi 智能音箱 Pro
    "OH11",   # Xiaomi 智能家庭屏 11
    # ---- Sound 系列 ----
    "L16A",   # Xiaomi Sound
    "L16B",   # Xiaomi Sound (UWB 改款)
    "L17A",   # Xiaomi Sound Pro
    "OH1P",   # Xiaomi Sound 2 Pro
    "OH1M",   # Xiaomi Sound 2 Max
]

DEFAULT_AUDIO_ID = "1674538785546746632"  # 石进 - 《夜的钢琴曲五》 (高品质触屏封面与歌词)



def parse_cookie_string(cookie_str: str) -> dict:
    result = {}
    for item in cookie_str.split(";"):
        item = item.strip()
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        if key.strip() in ("userId", "passToken"):
            result[key.strip()] = value.strip()
    return result


class DeviceListError(RuntimeError):
    """Raised when Xiaomi device discovery fails."""


class AuthManager:
    def __init__(self, config: Config):
        self.config = config
        self.session: aiohttp.ClientSession | None = None
        self.account: MiAccount | None = None
        self.mina_service: MiNAService | None = None
        self.miio_service: MiIOService | None = None
        self._logged_in = False
        self._cookie_loaded = False
        self._login_lock = asyncio.Lock()
        self._device_list_lock = asyncio.Lock()
        self._device_list_task: asyncio.Task | None = None
        self.notifier: Notifier | None = None
        if config.notify.channel and config.notify.key:
            self.notifier = Notifier(config.notify.channel, config.notify.key)

    @property
    def token_store(self) -> str:
        return os.path.join(self.config.conf_path, ".mi.token")

    @staticmethod
    def _has_basic_token(account: MiAccount | None) -> bool:
        token = getattr(account, "token", None) or {}
        has_user = bool(token.get("userId"))
        has_pass = bool(token.get("passToken"))
        has_service = bool(isinstance(token.get("micoapi"), (list, tuple)) and len(token["micoapi"]) == 2)
        return has_user and (has_pass or has_service)

    def _handle_auth_failure(self, raw_error: str):
        self._logged_in = False
        log.error("[Xiaomi] 登录失败: %s", raw_error)
        if self.notifier:
            self._safe_notify(self.notifier.notify_login_failed(raw_error))
        raise DeviceListError(raw_error)

    async def _execute_login(self, sid: str = "micoapi"):
        try:
            resp = await self.account._serviceLogin(f"serviceLogin?sid={sid}&_json=true")
            if isinstance(resp, dict) and resp.get("code") != 0:
                code = resp.get("code")
                desc = resp.get("description") or resp.get("desc") or resp.get("result") or ""
                raw_error = f"[{code}] {desc}".strip() if desc else f"[{code}]"
                self._handle_auth_failure(raw_error)

            if not isinstance(resp, dict) or "location" not in resp or "nonce" not in resp or "ssecurity" not in resp:
                self._handle_auth_failure("登录换票响应数据不完整")

            if "userId" in resp:
                self.account.token["userId"] = str(resp["userId"])
            if "passToken" in resp:
                self.account.token["passToken"] = resp["passToken"]

            service_token = await self.account._securityTokenService(
                resp["location"], resp["nonce"], resp["ssecurity"]
            )
            self.account.token[sid] = (resp["ssecurity"], service_token)
            if self.account.token_store:
                self.account.token_store.save_token(self.account.token)
            self._logged_in = True
        except DeviceListError:
            raise
        except Exception as exc:
            self._handle_auth_failure(str(exc))

    async def login(self):
        async with self._login_lock:
            if (
                self.session is not None
                and not self.session.closed
                and self.account is not None
                and self.mina_service is not None
                and self.miio_service is not None
            ):
                return

            os.makedirs(self.config.conf_path, exist_ok=True)
            if self.session is None or self.session.closed:
                self.session = aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=15, connect=5, sock_read=10)
                )

            dev_id = get_device_id(self.config.conf_path)
            self.account = MiAccount(self.session, "", "", token_store=self.token_store)
            cached_token = self.account.token or {}

            token_data = {}
            if self.config.xiaomi.cookie:
                token_data = parse_cookie_string(self.config.xiaomi.cookie)

            # 如果用户在配置中提供了 Cookie，同步注入 Token
            if token_data.get("userId") and token_data.get("passToken"):
                cached_token["userId"] = token_data["userId"]
                cached_token["passToken"] = token_data["passToken"]
                self._cookie_loaded = True
            else:
                self._cookie_loaded = False

            if cached_token.get("userId") and (cached_token.get("passToken") or "micoapi" in cached_token):
                cached_token["deviceId"] = dev_id
                self.account.token = cached_token
                self.account.now_ua = MIIO_UA % dev_id

                # 若本地缓存已有有效的 micoapi serviceToken，直接判定登录成功，无需重复换票
                if isinstance(cached_token.get("micoapi"), (list, tuple)) and len(cached_token["micoapi"]) == 2:
                    self._logged_in = True
                    log.info("Xiaomi login succeeded (using cached serviceToken)")
                else:
                    await self._execute_login("micoapi")
                    log.info("Xiaomi login succeeded")
            else:
                self._logged_in = False
                return

            self.mina_service = MiNAService(self.account)
            self.miio_service = MiIOService(self.account)

    async def ensure_login(self):
        if self.mina_service is None or not self._logged_in:
            await self.login()

    def is_logged_in(self) -> bool:
        return self._logged_in

    async def get_device_list(self) -> list[dict]:
        await self.ensure_login()
        if self.mina_service is None or not self._logged_in:
            return []

        current_task = self._device_list_task
        if current_task and not current_task.done():
            return await asyncio.shield(current_task)

        async with self._device_list_lock:
            current_task = self._device_list_task
            if current_task and not current_task.done():
                return await asyncio.shield(current_task)
            self._device_list_task = asyncio.create_task(self._fetch_device_list_once())

        try:
            return await asyncio.shield(self._device_list_task)
        finally:
            if self._device_list_task and self._device_list_task.done():
                self._device_list_task = None

    async def _fetch_device_list_once(self) -> list[dict]:
        max_retries = 3
        retry_delays = [5, 15, 30]

        for attempt in range(max_retries):
            try:
                devices = await self.mina_service.device_list()
                devices = devices or []
                self._logged_in = True
                if self._cookie_loaded:
                    log.info("[Xiaomi] Cookie 验证成功")
                    self._cookie_loaded = False
                return devices
            except Exception as exc:
                self._logged_in = False
                log.warning("[Xiaomi] 获取设备列表失败 (attempt %d/%d): %s", attempt + 1, max_retries, exc)

                if attempt < max_retries - 1:
                    await self.close()
                    try:
                        await self.login()
                    except Exception as login_exc:
                        log.warning("[Xiaomi] 尝试重新登录失败: %s", login_exc)
                        continue
                    if self.mina_service and self._logged_in:
                        delay = retry_delays[attempt]
                        log.info("[Xiaomi] 重登录成功，%ds 后重试获取设备列表", delay)
                        await asyncio.sleep(delay)
                        continue

                if self.notifier:
                    self._safe_notify(self.notifier.notify_token_expired())
                raise DeviceListError(str(exc)) from exc

    async def update_targets_info(self) -> set[str]:
        devices = await self.get_device_list()
        # 若配置中尚未选择任何 targets，默认将云端查找到的音箱自动添加并启用
        if not self.config.targets and devices:
            from miplay.config import TargetConfig
            for dev in devices:
                did = dev.get("miotDID", "")
                if did:
                    name = dev.get("name", "").strip() or dev.get("hardware", "").strip() or f"小爱音箱-{did}"
                    hardware = dev.get("hardware", "").strip()
                    device_id = dev.get("deviceID", "").strip()
                    self.config.targets.append(TargetConfig(did=did, name=name, airplay_name=name, hardware=hardware, device_id=device_id, enabled=True))
            self.config.save()
            log.info("Auto-populated %d Xiaomi target speaker(s) from cloud", len(self.config.targets))

        selected_dids = {target.did for target in self.config.targets if target.did}
        synced_dids: set[str] = set()
        changed = False

        for device in devices:
            did = device.get("miotDID", "")
            if did not in selected_dids:
                continue
            target = self.config.get_target_by_did(did)
            if target is None:
                continue
            device_id = device.get("deviceID", "") or ""
            hardware = device.get("hardware", "") or ""
            name = device.get("name", "") or ""
            if target.device_id != device_id:
                target.device_id = device_id
                changed = True
            if target.hardware != hardware:
                target.hardware = hardware
                changed = True
            if name and target.name != name:
                target.name = name
                if not target.airplay_name:
                    target.airplay_name = name
                changed = True
            target.ensure_names()
            if target.device_id:
                synced_dids.add(did)
                log.info(
                    "Synced Xiaomi target %s (did=%s, device_id=%s, hardware=%s)",
                    target.name,
                    did,
                    target.device_id,
                    target.hardware,
                )
        if changed:
            self.config.save()
        return synced_dids

    @staticmethod
    def extract_error_code(err_msg: str) -> str:
        match = re.search(r"\b(\d{4,6})\b", err_msg)
        return match.group(1) if match else ""

    def _safe_notify(self, coro):
        async def _wrapper():
            try:
                await coro
            except Exception as exc:
                log.warning("[ServerChan] 通知发送失败: %s", exc)

        task = asyncio.create_task(_wrapper())
        task.add_done_callback(
            lambda t: None
            if t.cancelled() or not t.exception()
            else log.warning("[ServerChan] 通知异常: %s", t.exception())
        )

    async def close(self):
        if self._device_list_task and not self._device_list_task.done():
            self._device_list_task.cancel()
            try:
                await self._device_list_task
            except Exception:
                pass
        self._device_list_task = None
        if self.session and not self.session.closed:
            await self.session.close()
        self.session = None
        self.account = None
        self.mina_service = None
        self.miio_service = None
        self._logged_in = False
        self._cookie_loaded = False


class TargetController:
    def __init__(self, target: TargetConfig, auth: AuthManager):
        self.target = target
        self.auth = auth
        self._last_volume = 50

    @property
    def id(self) -> str:
        return self.target.id

    @property
    def did(self) -> str:
        return self.target.did

    @property
    def device_id(self) -> str:
        return self.target.device_id

    def _should_use_music_api(self) -> bool:
        return self.target.use_music_api or self.target.hardware in MUSIC_API_MODELS

    async def search_audio_id(self, title: str, artist: str = "", fuzzy_fallback: bool = True) -> str:
        """从小米曲库检索对应歌曲的 audioID (用于触屏音箱显示海报与歌词)。"""
        title = (title or "").strip()
        if not title:
            return ""
        artist = (artist or "").strip()
        query_artist = artist
        for sep in ("--", " — ", " · ", "—", "·", "-", "/"):
            if sep in artist:
                parts = [p.strip() for p in artist.split(sep)]
                if len(parts) == 2 and parts[0] and parts[1]:
                    if parts[0].lower() == title.lower():
                        query_artist = parts[1]
                    elif parts[1].lower() == title.lower():
                        query_artist = parts[0]
                break
        query = f"{title} {query_artist}".strip() if query_artist else title
        try:
            await self.auth.ensure_login()
            if not self.auth.mina_service:
                return ""
            result = await self.auth.mina_service.mina_request(
                "/music/search",
                {
                    "query": query,
                    "queryType": "1",
                    "offset": "0",
                    "count": "6",
                },
            )
        except Exception as exc:
            log.warning("Mina music search error (%s): %s", query, exc)
            return ""

        song_list = (result or {}).get("data", {}).get("songList") or []
        if not song_list:
            return ""

        first_artist = re.split(r"[;；,，&、/·・—\-]", artist)[0].strip() if artist else ""
        artist_l = artist.lower()
        for song in song_list:
            name = (song.get("name") or "").strip()
            song_artist = (song.get("artist") or {}).get("name") or ""
            if name.lower() != title.lower():
                continue
            if first_artist:
                if first_artist.lower() not in song_artist.lower() and not (
                    song_artist and song_artist.lower() in artist_l
                ):
                    continue
            audio_id = str(song.get("audioID") or "")
            if audio_id:
                log.info("Mina music search exact hit (%s) audioID=%s", query, audio_id)
                return audio_id

        if fuzzy_fallback and song_list:
            audio_id = str(song_list[0].get("audioID") or "")
            if audio_id:
                log.info("Mina music search fallback to top result (%s) audioID=%s", query, audio_id)
                return audio_id

        return ""

    async def play_url(self, url: str, audio_id: str | None = None) -> bool:
        effective_audio_id = (audio_id or "").strip() or DEFAULT_AUDIO_ID
        for attempt in range(2):
            try:
                await self.auth.ensure_login()
                if self._should_use_music_api():
                    result = await self.auth.mina_service.play_by_music_url(
                        self.device_id, url, audio_id=effective_audio_id
                    )
                else:
                    result = await self.auth.mina_service.play_by_url(self.device_id, url)
                log.info("play_url target=%s device_id=%s audio_id=%s result=%s", self.target.airplay_name, self.device_id, effective_audio_id, result)
                return result is not None
            except Exception as exc:
                if attempt == 0:
                    log.warning("play_url target=%s attempt 1 failed, retrying: %s", self.target.airplay_name, exc)
                    await asyncio.sleep(0.5)
                    continue
                log.error("play_url failed for %s: %s", self.target.airplay_name, exc)
                return False

    async def stop(self) -> bool:
        for attempt in range(2):
            try:
                await self.auth.ensure_login()
                result = await self.auth.mina_service.player_stop(self.device_id)
                await self.pause()
                log.info("stop target=%s result=%s", self.target.airplay_name, result)
                return True
            except Exception as exc:
                if attempt == 0:
                    log.warning("stop target=%s attempt 1 failed, retrying: %s", self.target.airplay_name, exc)
                    await asyncio.sleep(0.5)
                    continue
                log.error("stop failed for %s: %s", self.target.airplay_name, exc)
                return False

    async def pause(self) -> bool:
        for attempt in range(2):
            try:
                await self.auth.ensure_login()
                result = await self.auth.mina_service.player_pause(self.device_id)
                log.info("pause target=%s result=%s", self.target.airplay_name, result)
                return True
            except Exception as exc:
                if attempt == 0:
                    log.warning("pause target=%s attempt 1 failed, retrying: %s", self.target.airplay_name, exc)
                    await asyncio.sleep(0.5)
                    continue
                log.error("pause failed for %s: %s", self.target.airplay_name, exc)
                return False

    async def set_volume(self, volume: int) -> bool:
        volume = max(0, min(100, volume))
        for attempt in range(2):
            try:
                await self.auth.ensure_login()
                await self.auth.mina_service.player_set_volume(self.device_id, volume)
                if volume > 0:
                    self._last_volume = volume
                log.info("set_volume target=%s volume=%s", self.target.airplay_name, volume)
                return True
            except Exception as exc:
                if attempt == 0:
                    log.warning("set_volume target=%s attempt 1 failed, retrying: %s", self.target.airplay_name, exc)
                    await asyncio.sleep(0.5)
                    continue
                log.error("set_volume failed for %s: %s", self.target.airplay_name, exc)
                return False

    async def get_status(self) -> dict:
        try:
            await self.auth.ensure_login()
            playing_info = await self.auth.mina_service.player_get_status(self.device_id)
            if playing_info.get("code") != 0:
                raise RuntimeError(f"Mina API error: {playing_info}")
            data = playing_info.get("data", {})
            info_str = data.get("info")
            if not info_str:
                raise RuntimeError(f"Mina API missing info: {playing_info}")
            info = json.loads(info_str)
            volume = int(info.get("volume", 0))
            if volume > 0:
                self._last_volume = volume
            return {
                "status": info.get("status", 0),
                "volume": volume,
                "cur_time": int(info.get("cur_time", 0)),
                "duration": int(info.get("duration", 0)),
            }
        except Exception as exc:
            raise RuntimeError(f"get_status failed for {self.target.airplay_name}: {exc}") from exc


class TargetManager:
    def __init__(self, config: Config, auth: AuthManager):
        self.config = config
        self.auth = auth
        self.controllers: dict[str, TargetController] = {}

    async def init_targets(self) -> set[str]:
        synced_dids = await self.auth.update_targets_info()
        self.controllers.clear()
        for target in self.config.targets:
            if not target.did or target.did not in synced_dids:
                log.warning("Skipping target did=%s because it was not found in Xiaomi cloud", target.did)
                continue
            if not target.device_id:
                log.warning("Skipping target did=%s because device_id is missing", target.did)
                continue
            target.ensure_names()
            self.controllers[target.id] = TargetController(target, self.auth)
            log.info("Initialized Xiaomi target: %s (did=%s, enabled=%s)", target.airplay_name, target.did, target.enabled)
        return synced_dids
