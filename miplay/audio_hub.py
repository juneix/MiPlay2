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
from typing import TYPE_CHECKING, Any

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
        self._is_streaming = False
        self._loop: asyncio.AbstractEventLoop | None = None

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
            log.info("[AudioHub] 新增 Egress 监听客户端 (当前活跃听众: %d)", len(self._client_queues))
        return q

    def remove_listener_queue(self, q: queue.Queue[bytes | None]):
        """移除已断开的 Egress 监听客户端。"""
        with self._client_lock:
            if q in self._client_queues:
                self._client_queues.remove(q)
                log.info("[AudioHub] 移除 Egress 监听客户端 (剩余活跃听众: %d)", len(self._client_queues))

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
                        q.get_nowait()
                    except queue.Empty:
                        pass
                    try:
                        q.put_nowait(data)
                    except queue.Full:
                        pass

    def start_source(self, source: str) -> bool:
        """激活输入源 (AirPlay 优先于 api_ingest)。"""
        with self._source_lock:
            if source == "airplay":
                self.active_source = "airplay"
                self._is_streaming = True
                self._session_id = int(time.time())
                return True
            elif source == "api_ingest":
                if self.active_source == "airplay":
                    log.warning("[AudioHub] AirPlay 正在播放中，拒绝 API 推流请求")
                    return False
                self.active_source = "api_ingest"
                self._is_streaming = True
                self._session_id = int(time.time())
                return True
        return False

    def stop_source(self, source: str):
        """释放输入源。"""
        with self._source_lock:
            if self.active_source == source:
                self.active_source = "idle"
                self._is_streaming = False
                # 通知所有监听器当前音频流已结束
                with self._client_lock:
                    for q in list(self._client_queues):
                        try:
                            q.put_nowait(None)
                        except queue.Full:
                            pass
                log.info("[AudioHub] 音频输入源已释放: %s", source)

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
        }
