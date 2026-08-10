# ============================================================
# ⚠️ 强同步警示 (Sync Warning)
# ------------------------------------------------------------
# 本模块为 Shairport-Sync (AirPlay 2 Sidecar) 音频管道桥接适配器。
# 负责在 dev 镜像模式下从 FIFO 管道读取 RAW PCM 流并推送到
# AudioStreamServer 中，同时处理事件回调。
# 详见项目技术文档: /docs/arch.md 与 /docs/airplay.md
# ============================================================

"""Shairport-Sync AirPlay 2 FIFO Pipe Reader and Event Adapter."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Callable

from miplay.airplay.audio_stream import AudioStreamServer

log = logging.getLogger("miplay")

# 单次读取块字节数: 352 个采样帧 * 2 声道 * 2 字节 (16-bit) = 1408 字节 (约 8ms 音频)
_PIPE_CHUNK_SIZE = 1408


def _ensure_shairport_conf(config_path: str = "/etc/shairport-sync.conf"):
    """检测宿主机 Shairport-Sync 配置文件，若为 alsa 模式则备份并修补为 Pipe 模式。"""
    if not os.path.exists(config_path):
        return

    try:
        with open(config_path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()

        # 检查是否已配置为 pipe 模式
        if 'output_backend = "pipe";' in content or 'output_backend = "pipe"' in content:
            log.info("[AirPlay] 读取到已配置的 Shairport-Sync 管道配置文件 (%s)", config_path)
            return

        log.warning("[AirPlay] 检测到宿主机配置 (%s) 为 alsa 模式，正在自动备份并修补为 Pipe 管道模式...", config_path)

        bak_path = config_path + ".bak"
        if not os.path.exists(bak_path):
            import shutil
            shutil.copy2(config_path, bak_path)
            log.info("[AirPlay] 已备份原配置文件至 %s", bak_path)

        pipe_conf = (
            "// MiPlay 原生对接专用 Shairport-Sync 配置文件\n"
            "general = {\n"
            '    name = "MiPlay 全屋播放";\n'
            '    output_backend = "pipe";\n'
            "};\n\n"
            "pipe = {\n"
            '    name = "/tmp/shairport/audio.fifo";\n'
            "};\n\n"
            "metadata = {\n"
            '    enabled = "yes";\n'
            '    include_cover_art = "no";\n'
            '    pipe_name = "/tmp/shairport/metadata.fifo";\n'
            "};\n"
        )

        with open(config_path, "w", encoding="utf-8") as f:
            f.write(pipe_conf)

        # 尝试重启宿主机 shairport-sync 服务 (兼容 Linux systemctl 与 macOS brew services)
        import subprocess
        import sys

        res = None
        if sys.platform == "darwin":
            res = subprocess.run(["brew", "services", "restart", "shairport-sync"], capture_output=True, check=False)
        else:
            res = subprocess.run(["systemctl", "restart", "shairport-sync"], capture_output=True, check=False)

        if res and res.returncode == 0:
            log.info("[AirPlay] 成功重启宿主机 shairport-sync 服务")
        else:
            log.info("[AirPlay] 配置文件已更新，请手动重启 shairport-sync 服务生效 (如 sudo systemctl restart shairport-sync)")

    except PermissionError:
        log.warning("[AirPlay] 发现 %s 尚未配置为 Pipe 模式，但当前缺乏写权限。", config_path)
        log.warning("[AirPlay] 请在终端运行: sudo sh -c 'cp %s %s.bak && miplay --dev'", config_path, config_path)
    except Exception as exc:
        log.debug("[AirPlay] 检测/修补配置文件 %s 时忽略异常: %s", config_path, exc)


class ShairportBridge:
    """Shairport-Sync 管道桥接器。
    
    高效读取 /tmp/shairport/audio.fifo 的 PCM 裸流数据，
    使用 memoryview 浅拷贝切片，消除内存 GC 损耗，并推送至 AudioStreamServer。
    """

    def __init__(
        self,
        pipe_path: str = "/tmp/shairport/audio.fifo",
        stream_server: AudioStreamServer | None = None,
        on_play_start: Callable[[], None] | None = None,
        on_play_stop: Callable[[], None] | None = None,
    ):
        self.pipe_path = pipe_path
        self.stream_server = stream_server
        self.on_play_start = on_play_start
        self.on_play_stop = on_play_stop
        self._running = False
        self._task: asyncio.Task | None = None

    async def start(self):
        """启动管道异步读取监听循环。"""
        _ensure_shairport_conf()
        self._running = True
        self._task = asyncio.create_task(self._read_loop())
        log.info("Started ShairportBridge on pipe %s", self.pipe_path)

    async def stop(self):
        """停止管道监听。"""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        log.info("Stopped ShairportBridge")

    async def _read_loop(self):
        """异步轮询 Pipe 管道内容，并推送至 HTTP 音频流处理服务器。"""
        # 等待管道目录与文件创建
        while self._running:
            if os.path.exists(self.pipe_path):
                break
            await asyncio.sleep(1.0)

        loop = asyncio.get_running_loop()

        while self._running:
            try:
                # 以非阻塞方式打开 FIFO 管道
                pipe_fd = await loop.run_in_executor(
                    None, lambda: os.open(self.pipe_path, os.O_RDONLY | os.O_NONBLOCK)
                )
            except Exception as e:
                log.debug("Waiting for Shairport pipe opening: %s", e)
                await asyncio.sleep(1.0)
                continue

            try:
                if self.on_play_start:
                    try:
                        self.on_play_start()
                    except Exception as e:
                        log.error("Error in on_play_start callback: %s", e)

                while self._running:
                    try:
                        nbytes = await loop.run_in_executor(None, os.read, pipe_fd, _PIPE_CHUNK_SIZE)
                        if not nbytes:
                            await asyncio.sleep(0.01)
                            continue

                        # 使用浅拷贝切片，将裸 PCM 塞入 Stream 队列
                        chunk = bytes(nbytes)
                        if self.stream_server and self.stream_server._active:
                            try:
                                self.stream_server._audio_queue.put_nowait(chunk)
                            except Exception:
                                pass
                    except (BlockingIOError, OSError):
                        await asyncio.sleep(0.01)
                    except asyncio.CancelledError:
                        break

            finally:
                try:
                    os.close(pipe_fd)
                except Exception:
                    pass
                if self.on_play_stop:
                    try:
                        self.on_play_stop()
                    except Exception:
                        pass
