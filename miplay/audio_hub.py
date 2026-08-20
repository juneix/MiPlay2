# ============================================================
# ⚠️ 强同步警示 (Sync Warning)
# ------------------------------------------------------------
# 本模块为 MiPlay 音频流转中枢核心管理器 (Audio Hub)。
# 统一协调上游输入源 (AirPlay / Ingest API) 与下游输出端 (小米音箱 / Egress 流)。
# 任何架构修改请务必严格遵照项目技术文档:
# 📖 /docs/arch.md
# ============================================================

"""Audio Hub for MiPlay - Bidirectional Ingest and Egress streaming."""

from __future__ import annotations

import asyncio
import logging
import queue
import struct
import threading
import time
import copy
from typing import TYPE_CHECKING, Any, Awaitable, Callable

if TYPE_CHECKING:
    from miplay.app import MiPlay

log = logging.getLogger("miplay")

_QUEUE_MAXSIZE = 25


class AudioHub:
    """音频流转中枢：管理音频输入源仲裁与多订阅者输出分发。"""

    def __init__(self, app_instance: MiPlay):
        self.app = app_instance
        self.active_source: str = "idle"  # "idle" | "airplay" | "api_ingest"
        self._source_lock = threading.Lock()
        
        # 订阅者广播队列管理
        self._client_queues: list[queue.Queue[bytes | None]] = []
        self._client_lock = threading.Lock()
        self._sample_rate = 44100
        self._channels = 2
        self._sample_width = 2  # 16-bit PCM
        
        # 活跃会话标识
        self._session_id = int(time.time())
        self._session_counter = time.monotonic_ns()
        self._is_streaming = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._session: dict[str, Any] = {
            "session_id": self._session_id,
            "state": "idle",
            "source": "",
            "target": "group_all",
            "metadata": {},
            "progress": None,
            "capabilities": {"previous": False, "play_pause": False, "next": False, "seek": False},
        }
        self._dropped_bytes = 0
        self._control_handler: Callable[[str, int | None], Awaitable[bool] | bool] | None = None

    def set_loop(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop

    @property
    def listener_count(self) -> int:
        with self._client_lock:
            return len(self._client_queues)

    def create_listener_queue(self) -> queue.Queue[bytes | None]:
        """为 Egress 客户端 (如手机浏览器 / APK) 创建独立的流队列。"""
        q: queue.Queue[bytes | None] = queue.Queue(maxsize=_QUEUE_MAXSIZE)
        with self._client_lock:
            self._client_queues.append(q)
            log.info("[Audio] 新增 Egress 监听客户端 (当前活跃听众: %d)", len(self._client_queues))
        return q

    def remove_listener_queue(self, q: queue.Queue[bytes | None]):
        """移除已断开的 Egress 监听客户端。"""
        with self._client_lock:
            if q in self._client_queues:
                self._client_queues.remove(q)
                log.info("[Audio] 移除 Egress 监听客户端 (剩余活跃听众: %d)", len(self._client_queues))

    def broadcast_pcm(self, data: bytes):
        """向所有注册的 Egress 客户端分发 PCM 音频块。"""
        if not data:
            return
        with self._client_lock:
            for q in list(self._client_queues):
                try:
                    q.put_nowait(data)
                except queue.Full:
                    try:
                        dropped = q.get_nowait()
                        if isinstance(dropped, bytes):
                            self._dropped_bytes += len(dropped)
                    except queue.Empty:
                        pass
                    try:
                        q.put_nowait(data)
                    except queue.Full:
                        pass

    def _next_session_id(self) -> int:
        self._session_counter += 1
        return self._session_counter

    def _publish_session(self):
        if not self._loop or not self._loop.is_running():
            return
        snapshot = self.get_session()

        async def publish():
            try:
                from miplay.web.api import broadcast_ws
                await broadcast_ws({"type": "playback_state", "session": snapshot})
                await broadcast_ws({"type": "now_playing", "session": snapshot})
            except Exception:
                log.debug("[Audio] 无法广播播放会话状态", exc_info=True)

        self._loop.call_soon_threadsafe(lambda: asyncio.create_task(publish()))

    def begin_session(
        self,
        source: str,
        target: str = "group_all",
        metadata: dict | None = None,
        stream_url: str = "",
        capabilities: dict | None = None,
    ) -> int:
        with self._source_lock:
            session_id = self._next_session_id()
            self._session_id = session_id
            self._session = {
                "session_id": session_id,
                "state": "playing",
                "source": source,
                "target": target,
                "metadata": copy.deepcopy(metadata or {}),
                "stream_url": stream_url,
                "progress": None,
                "capabilities": self._normalize_capabilities(capabilities),
            }
        self._publish_session()
        return session_id

    @staticmethod
    def _normalize_capabilities(capabilities: dict | None) -> dict[str, bool]:
        return {key: bool((capabilities or {}).get(key, False)) for key in ("previous", "play_pause", "next", "seek")}

    def update_session_metadata(self, metadata: dict | None = None, *, state: str | None = None):
        with self._source_lock:
            if metadata:
                self._session["metadata"].update({k: v for k, v in metadata.items() if v is not None})
            if state:
                self._session["state"] = state
        self._publish_session()

    def update_session_progress(self, position_ms: int | float | None, duration_ms: int | float | None = None):
        with self._source_lock:
            if position_ms is None or float(position_ms) < 0:
                self._session["progress"] = None
            else:
                current = self._session.get("progress") or {}
                duration = duration_ms if duration_ms is not None else current.get("duration_ms")
                self._session["progress"] = {
                    "position_ms": max(0, int(position_ms)),
                    "duration_ms": max(0, int(duration)) if duration is not None else None,
                    "updated_at_ms": int(time.time() * 1000),
                }
        self._publish_session()

    def update_session_capabilities(self, capabilities: dict | None):
        with self._source_lock:
            self._session["capabilities"] = self._normalize_capabilities(capabilities)
        self._publish_session()

    def set_session_control_handler(self, handler: Callable[[str, int | None], Awaitable[bool] | bool] | None):
        self._control_handler = handler

    async def control_session(self, session_id: int, action: str, position_ms: int | None = None) -> tuple[bool, str]:
        with self._source_lock:
            if int(session_id) != int(self._session.get("session_id", 0)):
                return False, "stale_session"
            capability = "play_pause" if action in ("play", "pause") else action
            if capability not in ("previous", "play_pause", "next", "seek"):
                return False, "invalid_action"
            if not self._session.get("capabilities", {}).get(capability, False):
                return False, "unsupported"
            handler = self._control_handler
        if not handler:
            return False, "no_handler"
        result = handler(action, position_ms)
        if asyncio.iscoroutine(result):
            result = await result
        return (True, "") if result else (False, "failed")

    def end_session(self, source: str | None = None):
        with self._source_lock:
            if source and self._session.get("source") not in (source, ""):
                return
            self._session["state"] = "stopped"
        self._publish_session()

    def get_session(self) -> dict:
        with self._source_lock:
            return copy.deepcopy(self._session)

    def start_source(self, source: str) -> bool:
        """激活输入源 (AirPlay 优先于 api_ingest)。"""
        with self._source_lock:
            if source == "airplay":
                self.active_source = "airplay"
                self._is_streaming = True
                self._session_id = int(time.time())
                return True
            elif source == "api_ingest":
                if self.active_source != "idle":
                    log.warning("[Audio] 当前音频通道繁忙，拒绝 API 推流请求")
                    return False
                self.active_source = "api_ingest"
                self._is_streaming = True
                self._session_id = int(time.time())
                return True
        return False

    def stop_source(self, source: str):
        """释放输入源。"""
        stopped = False
        with self._source_lock:
            if self.active_source == source:
                self.active_source = "idle"
                self._is_streaming = False
                stopped = True
                # 通知所有监听器当前音频流已结束
                with self._client_lock:
                    for q in list(self._client_queues):
                        while True:
                            try:
                                q.get_nowait()
                            except queue.Empty:
                                break
                        try:
                            q.put_nowait(None)
                        except queue.Full:
                            pass
                log.info("[Audio] 音频输入源已释放: %s", source)
        if stopped:
            self.end_session(source)

    def build_wav_header(self, data_size: int = 0x7FFFFF00) -> bytes:
        """生成标准 PCM WAV 头部。"""
        byte_rate = self._sample_rate * self._channels * self._sample_width
        block_align = self._channels * self._sample_width
        fmt_chunk = struct.pack(
            "<4sIHHIIHH",
            b"fmt ",
            16,
            1,
            self._channels,
            self._sample_rate,
            byte_rate,
            block_align,
            self._sample_width * 8,
        )
        return struct.pack("<4sI4s", b"RIFF", data_size + len(fmt_chunk) + 12, b"WAVE") + fmt_chunk + struct.pack(
            "<4sI", b"data", data_size
        )

    def get_status(self) -> dict:
        """获取当前 Audio Hub 运行状态快照。"""
        return {
            "active_source": self.active_source,
            "is_streaming": self._is_streaming,
            "listener_count": self.listener_count,
            "session_id": self._session_id,
            "sample_rate": self._sample_rate,
            "channels": self._channels,
            "bitrate": f"{self._sample_rate}Hz/{self._sample_width * 8}bit/{self._channels}ch",
            "dropped_bytes": self._dropped_bytes,
            "session": self.get_session(),
        }
