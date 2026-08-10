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


def update_shairport_conf_name(new_name: str, template_path: str = "shairport-sync.conf"):
    """更新项目根目录下的 shairport-sync.conf 中的设备广播名称。"""
    if not new_name:
        return
    full_path = os.path.join(os.getcwd(), template_path)
    if not os.path.exists(full_path):
        return
    try:
        with open(full_path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()
        import re
        updated_content = re.sub(r'(name\s*=\s*")[^"]*(";)', f'\\1{new_name}\\2', content, count=1)
        if updated_content != content:
            with open(full_path, "w", encoding="utf-8") as f:
                f.write(updated_content)
            log.info("[AirPlay] 已更新根目录配置文件中的全屋播放设备名称为: %s", new_name)
    except Exception as exc:
        log.debug("[AirPlay] 更新 %s 设备名称失败: %s", template_path, exc)


def _ensure_shairport_conf(config_path: str = "/etc/shairport-sync.conf", expected_name: str = ""):
    """检测宿主机 Shairport-Sync 配置文件，比对根目录模板内容，若不匹配则自动备份并覆盖更新。"""
    # 自动创建管道目录 /tmp/shairport 确保 shairport-sync 重启时管道文件目录必定存在
    try:
        os.makedirs("/tmp/shairport", exist_ok=True)
    except Exception:
        pass

    if expected_name:
        update_shairport_conf_name(expected_name)

    # 优先读取项目根目录下的 shairport-sync.conf 模板文件
    example_path = os.path.join(os.getcwd(), "shairport-sync.conf")
    if os.path.exists(example_path):
        try:
            with open(example_path, "r", encoding="utf-8") as ef:
                expected_conf = ef.read()
        except Exception:
            expected_conf = ""
    else:
        expected_conf = ""

    if not expected_conf:
        name_str = expected_name or "MiPlay 全屋播放"
        expected_conf = (
            "// MiPlay 原生对接专用 Shairport-Sync 配置文件\n"
            "general = {\n"
            f'    name = "{name_str}";\n'
            '    output_backend = "pipe";\n'
            "};\n\n"
            "pipe = {\n"
            '    name = "/tmp/shairport/audio.fifo";\n'
            "};\n\n"
            "metadata = {\n"
            '    enabled = "yes";\n'
            '    include_cover_art = "yes";\n'
            '    pipe_name = "/tmp/shairport/metadata.fifo";\n'
            "};\n"
        )

    if not os.path.exists(config_path):
        return

    try:
        with open(config_path, "r", encoding="utf-8", errors="ignore") as f:
            current_content = f.read()

        # 比对宿主机内容与期望配置是否完全相同
        if current_content.strip() == expected_conf.strip():
            log.info("[AirPlay] 读取到已匹配的 Shairport-Sync 管道配置文件 (%s)", config_path)
            return

        log.warning("[AirPlay] 检测到宿主机配置 (%s) 与期望配置不匹配，正在自动覆盖与同步...", config_path)

        bak_path = config_path + ".bak"
        if not os.path.exists(bak_path):
            import shutil
            shutil.copy2(config_path, bak_path)
            log.info("[AirPlay] 已备份原配置文件至 %s", bak_path)

        with open(config_path, "w", encoding="utf-8") as f:
            f.write(expected_conf)

        log.info("[AirPlay] 成功同步更新 %s，音频管道指向 /tmp/shairport/audio.fifo", config_path)

        # 根据 OS 平台精确定向重启指令与提示 (Linux apt 对应 systemctl, macOS homebrew 对应 brew services)
        import subprocess
        import sys

        is_mac = sys.platform == "darwin"
        cmd = ["brew", "services", "restart", "shairport-sync"] if is_mac else ["systemctl", "restart", "shairport-sync"]
        manual_hint = "brew services restart shairport-sync" if is_mac else "sudo systemctl restart shairport-sync"

        res = subprocess.run(cmd, capture_output=True, text=True, check=False)

        if res.returncode == 0:
            log.info("[AirPlay] 成功重启宿主机 shairport-sync 服务")
        else:
            err_msg = (res.stderr or res.stdout or "").strip()
            log.warning(
                "[AirPlay] 重启 shairport-sync 服务失败 (%s)。请手动运行: %s",
                err_msg or f"exit code {res.returncode}",
                manual_hint,
            )

    except PermissionError:
        log.warning("[AirPlay] 发现 %s 内容需要更新，但当前缺乏写权限。", config_path)
        log.warning("[AirPlay] 请在终端运行: sudo sh -c 'cp %s %s.bak && miplay --dev'", config_path, config_path)
    except Exception as exc:
        log.debug("[AirPlay] 检测/修补配置文件 %s 时忽略异常: %s", config_path, exc)


class ShairportBridge:
    """Shairport-Sync 管道桥接器。
    
    高效读取 /tmp/shairport/audio.fifo 的 PCM 裸流数据，
    同时解析 /tmp/shairport/metadata.fifo 歌名、歌手、专辑与 ID3 封面图片。
    """

    def __init__(
        self,
        pipe_path: str = "/tmp/shairport/audio.fifo",
        meta_path: str = "/tmp/shairport/metadata.fifo",
        stream_server: AudioStreamServer | None = None,
        on_play_start: Callable[[], None] | None = None,
        on_play_stop: Callable[[], None] | None = None,
    ):
        self.pipe_path = pipe_path
        self.meta_path = meta_path
        self.stream_server = stream_server
        self.on_play_start = on_play_start
        self.on_play_stop = on_play_stop
        self.metadata: dict[str, str] = {}
        self.artwork: str | None = None
        self._running = False
        self._task: asyncio.Task | None = None
        self._meta_task: asyncio.Task | None = None

    async def start(self):
        """启动管道异步读取监听循环。"""
        _ensure_shairport_conf()
        self._running = True
        self._task = asyncio.create_task(self._read_loop())
        self._meta_task = asyncio.create_task(self._read_meta_loop())
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
        if self._meta_task:
            self._meta_task.cancel()
            try:
                await self._meta_task
            except asyncio.CancelledError:
                pass
            self._meta_task = None
        log.info("Stopped ShairportBridge")

    async def _read_meta_loop(self):
        """解析 Shairport-Sync 元数据管道中的 ID3 歌名、歌手与封面图。"""
        import base64
        import xml.etree.ElementTree as ET

        while self._running:
            if os.path.exists(self.meta_path):
                break
            await asyncio.sleep(1.0)

        loop = asyncio.get_running_loop()

        while self._running:
            try:
                meta_fd = await loop.run_in_executor(
                    None, lambda: os.open(self.meta_path, os.O_RDONLY | os.O_NONBLOCK)
                )
            except Exception:
                await asyncio.sleep(2.0)
                continue

            buf = ""
            try:
                while self._running:
                    try:
                        data = await loop.run_in_executor(None, os.read, meta_fd, 4096)
                        if not data:
                            await asyncio.sleep(0.1)
                            continue
                        buf += data.decode("utf-8", errors="ignore")
                        while "<item>" in buf and "</item>" in buf:
                            start = buf.find("<item>")
                            end = buf.find("</item>") + 7
                            item_xml = buf[start:end]
                            buf = buf[end:]
                            try:
                                root = ET.fromstring(item_xml)
                                code_hex = root.findtext("code", "")
                                data_b64 = root.findtext("data", "")
                                if not code_hex or not data_b64:
                                    continue
                                raw_val = base64.b64decode(data_b64)
                                # 6d696e6d: minm (Title)
                                if code_hex == "6d696e6d":
                                    self.metadata["title"] = raw_val.decode("utf-8", errors="ignore")
                                # 61736172: asar (Artist)
                                elif code_hex == "61736172":
                                    self.metadata["artist"] = raw_val.decode("utf-8", errors="ignore")
                                # 6173616c: asal (Album)
                                elif code_hex == "6173616c":
                                    self.metadata["album"] = raw_val.decode("utf-8", errors="ignore")
                                # 50494354: PICT (Cover Artwork)
                                elif code_hex == "50494354":
                                    self.artwork = "data:image/jpeg;base64," + data_b64
                            except Exception:
                                pass
                    except (BlockingIOError, OSError):
                        await asyncio.sleep(0.1)
                    except asyncio.CancelledError:
                        break
            finally:
                try:
                    os.close(meta_fd)
                except Exception:
                    pass

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

    def snapshot(self) -> dict:
        return {
            "metadata": self.metadata,
            "artwork": self.artwork,
        }
