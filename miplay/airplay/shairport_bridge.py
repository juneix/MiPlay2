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
import base64
import logging
import os
import subprocess
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Callable

from miplay.airplay.audio_stream import AudioStreamServer, PCMFormat

log = logging.getLogger("miplay")

# FIFO I/O 读取块；不代表 AirPlay 包，PCM 帧边界由 odsc 动态确定。
_PIPE_CHUNK_SIZE = 64 * 1024
_SESSION_BUFFER_MAX = 2 * 1024 * 1024


@dataclass(frozen=True)
class MetadataEvent:
    type: str
    code: str
    length: int
    data: bytes


def _metadata_code(value: str) -> str:
    try:
        return bytes.fromhex((value or "").strip()).decode("ascii")
    except (ValueError, UnicodeDecodeError):
        return (value or "").strip()


def parse_metadata_item(item_xml: str) -> MetadataEvent:
    root = ET.fromstring(item_xml)
    length_text = (root.findtext("length") or "0").strip()
    try:
        length = int(length_text)
    except ValueError:
        length = 0
    data_node = root.find("data")
    data = b""
    if data_node is not None and data_node.text:
        data = base64.b64decode("".join(data_node.text.split()), validate=False)
    return MetadataEvent(
        (root.findtext("type") or "").strip(),
        _metadata_code(root.findtext("code") or ""),
        length,
        data,
    )


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


_shairport_proc: subprocess.Popen | None = None


def _prepare_shairport_env(config_path: str = "/etc/shairport-sync.conf", expected_name: str = "") -> str:
    """创建管道目录/FIFO文件，同步配置文件。返回最终使用的 conf 路径。"""
    os.makedirs("/tmp/shairport", exist_ok=True)

    # 预建 FIFO 特殊文件（shairport-sync 写端 open 前读端必须已存在）
    for fifo in ("/tmp/shairport/audio.fifo", "/tmp/shairport/metadata.fifo"):
        if not os.path.exists(fifo):
            os.mkfifo(fifo)

    if expected_name:
        update_shairport_conf_name(expected_name)

    example_path = os.path.join(os.getcwd(), "shairport-sync.conf")
    expected_conf = ""
    if os.path.exists(example_path):
        try:
            with open(example_path, "r", encoding="utf-8") as ef:
                expected_conf = ef.read()
        except Exception:
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

    if os.path.exists(config_path):
        try:
            with open(config_path, "r", encoding="utf-8", errors="ignore") as f:
                current_content = f.read()
            if current_content.strip() != expected_conf.strip():
                log.warning("[AirPlay] 检测到宿主机配置 (%s) 与期望配置不匹配，正在自动覆盖与同步...", config_path)
                bak_path = config_path + ".bak"
                if not os.path.exists(bak_path):
                    import shutil
                    shutil.copy2(config_path, bak_path)
                    log.info("[AirPlay] 已备份原配置文件至 %s", bak_path)
                with open(config_path, "w", encoding="utf-8") as f:
                    f.write(expected_conf)
                log.info("[AirPlay] 成功同步更新 %s 的配置文件", config_path)
        except PermissionError:
            log.warning("[AirPlay] 发现 %s 内容需要更新，但当前缺乏写权限。", config_path)
        except Exception as exc:
            log.debug("[AirPlay] 检测/修补配置文件 %s 时忽略异常: %s", config_path, exc)

    return config_path if os.path.exists(config_path) else example_path


def _start_shairport_proc(conf_path: str) -> None:
    """清理残留进程后用 Popen 拉起 shairport-sync，要求调用前读端 FIFO 已打开。"""
    global _shairport_proc
    import subprocess

    if _shairport_proc and _shairport_proc.poll() is None:
        log.info("[AirPlay] Shairport-Sync 已由 Python 托管运行 (PID: %d)", _shairport_proc.pid)
        return
    try:
        subprocess.run(["pkill", "-9", "shairport-sync"], capture_output=True, check=False)
        version = subprocess.run(
            ["shairport-sync", "--version"], capture_output=True, text=True, check=False
        )
        version_text = (version.stdout or version.stderr).strip().splitlines()
        if version_text:
            log.info("[AirPlay] Shairport-Sync 版本: %s", version_text[0])
        _shairport_proc = subprocess.Popen(
            ["shairport-sync", "-c", conf_path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        time.sleep(0.05)
        if _shairport_proc.poll() is None:
            log.info("[AirPlay] 成功自动托管拉起 Shairport-Sync 后台进程 (PID: %d)", _shairport_proc.pid)
        else:
            log.error("[AirPlay] Shairport-Sync 启动后立即退出 (code=%s)", _shairport_proc.returncode)
    except Exception as exc:
        log.error("[AirPlay] 自动托管拉起 Shairport-Sync 进程失败: %s", exc)


# 保持向后兼容（app.py 调用入口）
def _ensure_shairport_conf(config_path: str = "/etc/shairport-sync.conf", expected_name: str = "") -> None:
    _prepare_shairport_env(config_path, expected_name)


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
        self._stderr_task: asyncio.Task | None = None
        self._format_wait_task: asyncio.Task | None = None
        self._session_active = False
        self._play_url_sent = False
        self._session_buffer = bytearray()
        self._partial_frame = bytearray()
        self._pcm_format: PCMFormat | None = None
        self._last_pcm_format: PCMFormat | None = None
        self.session_state = "idle"
        self.input_format = ""
        self.output_format = ""
        self.last_error = ""

    def _stop_group_session(self, notify_stop: bool = True):
        was_started = self._play_url_sent
        if self.stream_server and self.stream_server._active:
            self.stream_server.stop_streaming()
        self._play_url_sent = False
        if notify_stop and was_started and self.on_play_stop:
            try:
                self.on_play_stop()
            except Exception as exc:
                log.error("[AirPlay] on_play_stop 回调失败: %s", exc)

    def _begin_session(self):
        if self._session_active:
            self._stop_group_session()
        if self._format_wait_task:
            self._format_wait_task.cancel()
            self._format_wait_task = None
        self._session_active = True
        self._pcm_format = None
        self._session_buffer.clear()
        self._partial_frame.clear()
        self.session_state = "waiting_format"
        self.output_format = ""
        self.last_error = ""
        log.info("[AirPlay] Shairport 会话开始 (pbeg)")

    def _end_session(self, error: str = ""):
        if self._format_wait_task:
            self._format_wait_task.cancel()
            self._format_wait_task = None
        if error:
            self.last_error = error
            log.error("[AirPlay] %s", error)
        self._stop_group_session()
        self._session_active = False
        self._pcm_format = None
        self._session_buffer.clear()
        self._partial_frame.clear()
        self.session_state = "idle"

    def _start_group_session(self):
        if not self._session_active or not self._pcm_format or not self._session_buffer:
            return
        frame_size = self._pcm_format.bytes_per_frame
        aligned = len(self._session_buffer) - len(self._session_buffer) % frame_size
        if aligned <= 0:
            return
        first_pcm = bytes(self._session_buffer[:aligned])
        self._partial_frame.extend(self._session_buffer[aligned:])
        self._session_buffer.clear()
        if self._format_wait_task:
            self._format_wait_task.cancel()
            self._format_wait_task = None
        if self.stream_server:
            self.stream_server.start_streaming(self._pcm_format)
            self.stream_server.write_pcm(first_pcm, bootstrap=True)
        self.session_state = "playing"
        self._play_url_sent = True
        if self.on_play_start:
            try:
                self.on_play_start()
            except Exception as exc:
                log.error("[AirPlay] on_play_start 回调失败: %s", exc)

    async def _resolve_cached_format(self, delay: float):
        try:
            await asyncio.sleep(delay)
            if not self._session_active or self._pcm_format or not self._session_buffer:
                return
            if self._last_pcm_format:
                self._pcm_format = self._last_pcm_format
                self.output_format = self._pcm_format.describe()
                self._start_group_session()
            else:
                self._end_session("首次 AirPlay 2 会话 2 秒内未收到 odsc，已停止")
        except asyncio.CancelledError:
            pass

    def _handle_pcm(self, data: bytes):
        if not self._session_active or not data:
            return
        if not self._pcm_format:
            self._session_buffer.extend(data)
            if len(self._session_buffer) > _SESSION_BUFFER_MAX:
                del self._session_buffer[:-_SESSION_BUFFER_MAX]
            if not self._format_wait_task:
                delay = 0.5 if self._last_pcm_format else 2.0
                self._format_wait_task = asyncio.create_task(self._resolve_cached_format(delay))
            return

        self._partial_frame.extend(data)
        frame_size = self._pcm_format.bytes_per_frame
        aligned = len(self._partial_frame) - len(self._partial_frame) % frame_size
        if aligned <= 0:
            return
        chunk = bytes(self._partial_frame[:aligned])
        del self._partial_frame[:aligned]
        if self.session_state == "waiting_format":
            self._session_buffer.extend(chunk)
            self._start_group_session()
        elif self.session_state == "playing" and self.stream_server:
            self.stream_server.write_pcm(chunk)

    def _handle_metadata_event(self, event: MetadataEvent):
        if event.code == "pbeg":
            self._begin_session()
        elif event.code == "pend":
            log.info("[AirPlay] Shairport 会话结束 (pend)")
            self._end_session()
        elif event.code == "paus" and self._play_url_sent:
            self.session_state = "paused"
        elif event.code == "pres" and self._session_active:
            self.session_state = "playing" if self._play_url_sent else "waiting_format"
        elif event.code == "sdsc":
            self.input_format = event.data.decode("utf-8", errors="replace").strip().rstrip("\x00")
        elif event.code == "odsc":
            value = event.data.decode("utf-8", errors="replace").strip().rstrip("\x00")
            try:
                pcm_format = PCMFormat.from_odsc(value)
            except ValueError as exc:
                self._end_session(str(exc))
                return
            changed = self._pcm_format is not None and pcm_format != self._pcm_format
            if changed and self._play_url_sent:
                self._stop_group_session(notify_stop=False)
                self._session_buffer.clear()
                self._partial_frame.clear()
                self.session_state = "waiting_format"
            self._pcm_format = pcm_format
            self._last_pcm_format = pcm_format
            self.output_format = pcm_format.describe()
            log.info("[AirPlay] Shairport 输出格式 (odsc): %s", self.output_format)
            self._start_group_session()
        elif event.code == "minm":
            self.metadata["title"] = event.data.decode("utf-8", errors="ignore")
        elif event.code == "asar":
            self.metadata["artist"] = event.data.decode("utf-8", errors="ignore")
        elif event.code == "asal":
            self.metadata["album"] = event.data.decode("utf-8", errors="ignore")
        elif event.code == "PICT":
            self.artwork = "data:image/jpeg;base64," + base64.b64encode(event.data).decode("ascii")

    async def start(self):
        """启动管道异步读取监听循环。

        正确时序：先打开 FIFO 读端 fd → 再拉起 shairport-sync 写端进程。
        shairport-sync 的 O_WRONLY open 会阻塞等待读端，因此读端必须先就绪。
        """
        conf_path = _prepare_shairport_env()  # 建目录 / mkfifo / 同步配置
        self._running = True
        self._task = asyncio.create_task(self._read_loop())
        self._meta_task = asyncio.create_task(self._read_meta_loop())
        # 让事件循环跑一轮，_read_loop 用 O_RDONLY|O_NONBLOCK 把读端 fd 打开
        await asyncio.sleep(0.05)
        # 读端已就绪，现在拉起 shairport-sync（写端 open 不再阻塞）
        _start_shairport_proc(conf_path)
        if _shairport_proc and _shairport_proc.stderr:
            self._stderr_task = asyncio.create_task(self._read_shairport_stderr())
        log.info("Started ShairportBridge on pipe %s", self.pipe_path)

    async def stop(self):
        """停止管道监听。"""
        global _shairport_proc
        self._running = False
        self._end_session()
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
        if self._stderr_task:
            self._stderr_task.cancel()
            try:
                await self._stderr_task
            except asyncio.CancelledError:
                pass
            self._stderr_task = None
        if _shairport_proc:
            try:
                _shairport_proc.terminate()
                _shairport_proc.wait(timeout=2)
            except Exception:
                pass
            _shairport_proc = None

        log.info("Stopped ShairportBridge")

    async def _read_shairport_stderr(self):
        while self._running and _shairport_proc and _shairport_proc.stderr:
            line = await asyncio.to_thread(_shairport_proc.stderr.readline)
            if not line:
                if _shairport_proc.poll() is not None:
                    self._end_session(
                        f"Shairport-Sync 进程退出 (code={_shairport_proc.returncode})"
                    )
                return
            message = line.decode("utf-8", errors="replace").strip()
            if message:
                log.info("[AirPlay] Shairport-Sync: %s", message)

    async def _read_meta_loop(self):
        """解析 Shairport-Sync 元数据管道中的 ID3 歌名、歌手与封面图。"""
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
                                self._handle_metadata_event(parse_metadata_item(item_xml))
                            except Exception as exc:
                                log.debug("[AirPlay] metadata 解析失败: %s", exc)
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
                while self._running:
                    try:
                        nbytes = await loop.run_in_executor(None, os.read, pipe_fd, _PIPE_CHUNK_SIZE)
                        if not nbytes:
                            await asyncio.sleep(0.01)
                            continue

                        self._handle_pcm(bytes(nbytes))
                    except (BlockingIOError, OSError):
                        await asyncio.sleep(0.01)
                    except asyncio.CancelledError:
                        break

            finally:
                try:
                    os.close(pipe_fd)
                except Exception:
                    pass

    def snapshot(self) -> dict:
        return {
            "metadata": self.metadata,
            "artwork": self.artwork,
            "session_state": self.session_state,
            "input_format": self.input_format,
            "output_format": self.output_format,
            "shairport_alive": bool(_shairport_proc and _shairport_proc.poll() is None),
            "last_error": self.last_error,
        }
