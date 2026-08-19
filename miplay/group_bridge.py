# ============================================================
# ⚠️ 强同步警示 (Sync Warning)
# ------------------------------------------------------------
# 本模块为 全屋虚拟分组 AirPlay 音频广播核心算法。
# 任何并发广播、全屋控制器修改请务必严格遵照项目技术文档:
# 📖 /docs/airplay.md
# ============================================================

"""AirPlay-to-Xiaomi group bridge runtime."""

from __future__ import annotations

import asyncio
import logging
import time

from zeroconf import Zeroconf

from miplay.airplay.server import AirPlayServer
from miplay.config import Config, TargetConfig
from miplay.xiaomi import TargetController

log = logging.getLogger("miplay")


# <!-- Section: Group AirPlay Controller & Bridge -->


class GroupController:
    """组目标控制器：使用 asyncio.gather 向全屋组成员并发广播控制指令。"""

    def __init__(self, config: Config, controllers_provider: callable):
        self.config = config
        self._get_controllers = controllers_provider

    @property
    def id(self) -> str:
        return "group_all"

    @property
    def did(self) -> str:
        return "group_all"

    @property
    def hardware(self) -> str:
        return "GROUP"

    def get_member_controllers(self) -> list[TargetController]:
        all_controllers = self._get_controllers()
        member_dids = set(self.config.group.member_dids)
        return [ctrl for ctrl in all_controllers.values() if ctrl.did in member_dids]

    async def play_url(self, url: str, audio_id: str | None = None) -> bool:
        members = self.get_member_controllers()
        if not members:
            log.warning("Group play: no active member speakers selected in group mode")
            return False

        # 毫秒级并发下发 URL 给组内所有音箱
        tasks = [ctrl.play_url(url, audio_id) for ctrl in members]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        success_count = sum(1 for r in results if r is True)
        log.info("Group play_url pushed to %d/%d member speakers (audio_id=%s)", success_count, len(members), audio_id)
        return success_count > 0

    async def stop(self) -> bool:
        members = self.get_member_controllers()
        if not members:
            return True
        tasks = [ctrl.stop() for ctrl in members]
        await asyncio.gather(*tasks, return_exceptions=True)
        return True

    async def pause(self) -> bool:
        members = self.get_member_controllers()
        if not members:
            return True
        tasks = [ctrl.pause() for ctrl in members]
        await asyncio.gather(*tasks, return_exceptions=True)
        return True

    async def set_volume(self, volume: int) -> bool:
        members = self.get_member_controllers()
        if not members:
            return True
        tasks = [ctrl.set_volume(volume) for ctrl in members]
        await asyncio.gather(*tasks, return_exceptions=True)
        return True

    async def get_status(self) -> dict:
        members = self.get_member_controllers()
        if not members:
            return {"status": 0, "volume": 50, "cur_time": 0, "duration": 0}
        try:
            return await members[0].get_status()
        except Exception:
            return {"status": 0, "volume": 50, "cur_time": 0, "duration": 0}


class GroupBridge:
    """全屋虚拟 AirPlay 桥接器，连接全屋组控制器与独立 AirPlayServer。"""

    def __init__(
        self,
        host: str,
        group_controller: GroupController,
        shared_zeroconf: Zeroconf | None = None,
        config: Config | None = None,
    ):
        self.host = host
        self.controller = group_controller
        self.shared_zeroconf = shared_zeroconf
        self.config = config
        self.airplay_server: AirPlayServer | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stream_url = ""
        self._airplay_active = False
        self._poll_task: asyncio.Task | None = None
        self._play_grace_until = 0.0
        self._session_audio_id: str | None = None

    @property
    def device_name(self) -> str:
        return self.config.group.airplay_name if self.config else "MiPlay 全屋播放"

    async def start(self):
        self._loop = asyncio.get_running_loop()
        self.airplay_server = AirPlayServer(
            self.host,
            self.device_name,
            self.shared_zeroconf,
            speaker_hardware="GROUP",
        )
        self.airplay_server.on_play_start = self._on_play_start
        self.airplay_server.on_play_stop = self._on_play_stop
        self.airplay_server.on_volume_change = self._on_volume_change
        await self.airplay_server.start()
        log.info("Started Group AirPlay bridge %s on rtsp=%s", self.device_name, self.airplay_server.rtsp_port)

    async def stop(self):
        self._airplay_active = False
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None
        if self.airplay_server:
            await self.airplay_server.stop()
            self.airplay_server = None

    def _on_play_start(self, stream_url: str):
        if self._loop and self._loop.is_running():
            asyncio.run_coroutine_threadsafe(self._play_on_target(stream_url), self._loop)

    def _on_play_stop(self):
        if self._loop and self._loop.is_running():
            asyncio.run_coroutine_threadsafe(self._stop_target(), self._loop)

    @staticmethod
    def _vol_pct_to_db(volume: int) -> float:
        if volume <= 0:
            return -144.0
        if volume >= 100:
            return 0.0
        if volume <= 6:
            return -28.125
        return (volume - 6) / 94.0 * 28.125 - 28.125

    def _on_volume_change(self, vol_db: float):
        if vol_db <= -144:
            volume = 0
        elif vol_db >= 0:
            volume = 100
        else:
            # 标准 Apple 16 档等分衰减映射: -28.125 dB ~ 0 dB -> 6% ~ 100%
            volume = round(6 + (vol_db + 28.125) / 28.125 * 94)
            volume = max(0, min(100, volume))
            if volume == 0 and vol_db > -144:
                volume = 1

        if self._loop and self._loop.is_running():
            asyncio.run_coroutine_threadsafe(self.controller.set_volume(volume), self._loop)

    async def _resolve_audio_id(self) -> str | None:
        """从当前 AirPlay 元数据中解析并检索曲库匹配的 audioID。"""
        meta = self.airplay_server.metadata if self.airplay_server else {}
        title = (meta.get("title") or "").strip()
        artist = (meta.get("artist") or "").strip()
        if not title:
            return None
        members = self.controller.get_member_controllers()
        if not members:
            return None
        try:
            audio_id = await asyncio.wait_for(
                members[0].search_audio_id(title, artist, fuzzy_fallback=True),
                timeout=3.0,
            )
            return audio_id or None
        except Exception as exc:
            log.warning("Group audio ID search error for %s: %s", title, exc)
            return None

    async def _play_on_target(self, stream_url: str):
        self._stream_url = stream_url
        self._airplay_active = True
        self._play_grace_until = time.time() + 10.0
        custom_default = getattr(self.config, "default_audio_id", "") if self.config else ""
        self._session_audio_id = await self._resolve_audio_id() or custom_default or None

        if await self.controller.play_url(stream_url, self._session_audio_id):
            try:
                from miplay.web.api import broadcast_ws
                asyncio.create_task(broadcast_ws({
                    "type": "control_command",
                    "action": "play_url",
                    "target": "group_all",
                    "url": f"/stream/live.wav?virtual_delay={self.config.virtual_delay}",
                }))
            except Exception:
                pass
            self._start_poll()
            log.info("Group AirPlay stream attached to speakers (%s)", self.device_name)
        else:
            log.warning("Group Xiaomi target rejected AirPlay stream for %s", self.device_name)

    async def _stop_target(self):
        self._airplay_active = False
        self._stream_url = ""
        self._session_audio_id = None
        try:
            from miplay.web.api import broadcast_ws
            asyncio.create_task(broadcast_ws({
                "type": "control_command",
                "action": "stop",
                "target": "group_all",
            }))
        except Exception:
            pass
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None
        try:
            await self.controller.stop()
        except Exception:
            pass

    def _start_poll(self):
        if self._poll_task and not self._poll_task.done():
            return
        self._poll_task = asyncio.create_task(self._poll_target_state())

    async def _poll_target_state(self):
        try:
            while self._airplay_active and self._stream_url:
                await asyncio.sleep(3)
                if not self._airplay_active or not self._stream_url:
                    break
                if self.airplay_server and not self.airplay_server.is_playing:
                    break
                if time.time() < self._play_grace_until:
                    continue
                try:
                    status = await asyncio.wait_for(self.controller.get_status(), timeout=8)
                    if status.get("status", 0) == 1:
                        continue
                    log.info("[Group] 全屋播放检测到中断，等待后自动续播...")
                    await asyncio.sleep(4)
                    if not self._airplay_active or not self._stream_url:
                        break
                    if self.airplay_server and not self.airplay_server.is_playing:
                        break
                    base_url = self._stream_url.split("?")[0]
                    fresh_url = f"{base_url}?sid={int(time.time())}"
                    self._play_grace_until = time.time() + 10.0
                    await self.controller.play_url(fresh_url, self._session_audio_id)
                except Exception:
                    pass
        except asyncio.CancelledError:
            pass

    def snapshot(self) -> dict:
        members = self.controller.get_member_controllers()
        return {
            "id": "group_all",
            "did": "group_all",
            "name": self.device_name,
            "airplay_name": self.device_name,
            "hardware": "GROUP",
            "active": bool(self.airplay_server and self.airplay_server.is_playing),
            "client_name": self.airplay_server.client_name if self.airplay_server else "",
            "metadata": self.airplay_server.metadata if self.airplay_server else {},
            "artwork": self.airplay_server.artwork if self.airplay_server else None,
            "rtsp_port": self.airplay_server.rtsp_port if self.airplay_server else 0,
            "stream_url": self._stream_url,
            "use_music_api": False,
            "member_count": len(members),
        }
