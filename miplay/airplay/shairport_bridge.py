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
import grp
import logging
import os
import pwd
import stat
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
_SHAIRPORT_SERVICE = "shairport-sync.service"
_SERVICE_START_TIMEOUT = 5.0
_FIFO_READY_TIMEOUT = 2.0


@dataclass(frozen=True)
class MetadataEvent:
    type: str
    code: str
    length: int
    data: bytes


@dataclass(frozen=True)
class ShairportEnvironment:
    config_path: str
    config_changed: bool
    service_user: str
    service_group: str


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


def update_shairport_conf_name(new_name: str, template_path: str = "shairport-sync.conf") -> bool:
    """更新项目根目录下的 shairport-sync.conf 中的设备广播名称。"""
    if not new_name:
        return False
    full_path = os.path.join(os.getcwd(), template_path)
    if not os.path.exists(full_path):
        raise RuntimeError(f"根目录 Shairport 配置不存在: {full_path}")
    try:
        with open(full_path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()
        import re
        updated_content, replacements = re.subn(
            r'(name\s*=\s*")[^"]*(";)',
            lambda match: f"{match.group(1)}{new_name}{match.group(2)}",
            content,
            count=1,
        )
        if replacements != 1:
            raise RuntimeError(f"根目录 Shairport 配置缺少唯一的 name 项: {full_path}")
        if updated_content != content:
            with open(full_path, "w", encoding="utf-8") as f:
                f.write(updated_content)
            log.info("[AirPlay] 已更新根目录配置文件中的全屋播放设备名称为: %s", new_name)
            return True
        return False
    except Exception as exc:
        raise RuntimeError(f"更新根目录 Shairport 配置失败: {exc}") from exc


def _service_identity() -> tuple[str, str, int, int]:
    result = subprocess.run(
        ["systemctl", "show", _SHAIRPORT_SERVICE, "--property=User", "--property=Group", "--no-pager"],
        capture_output=True, text=True, check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"无法读取 {_SHAIRPORT_SERVICE} 身份: {(result.stderr or result.stdout).strip()}")
    values = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
    user = values.get("User", "") or "root"
    try:
        uid = int(user)
        user_record = pwd.getpwuid(uid)
        service_user = str(uid)
    except ValueError:
        user_record = pwd.getpwnam(user)
        uid = user_record.pw_uid
        service_user = user
    group = values.get("Group", "")
    if not group:
        gid = user_record.pw_gid
        service_group = grp.getgrgid(gid).gr_name
    else:
        try:
            gid = int(group)
            service_group = str(gid)
        except ValueError:
            gid = grp.getgrnam(group).gr_gid
            service_group = group
    return service_user, service_group, uid, gid


def _prepare_fifo(path: str, uid: int, gid: int) -> None:
    if os.path.exists(path):
        if not stat.S_ISFIFO(os.stat(path).st_mode):
            raise RuntimeError(f"Shairport FIFO 路径不是 FIFO，拒绝覆盖: {path}")
    else:
        os.mkfifo(path, 0o660)
    os.chown(path, uid, gid)
    os.chmod(path, 0o660)


def _prepare_shairport_env(config_path: str = "/etc/shairport-sync.conf", expected_name: str = "") -> ShairportEnvironment:
    """同步配置并准备 systemd Shairport-Sync 使用的 FIFO。"""
    service_user, service_group, service_uid, service_gid = _service_identity()
    os.makedirs("/tmp/shairport", exist_ok=True)
    os.chmod("/tmp/shairport", 0o755)
    os.chown("/tmp/shairport", service_uid, service_gid)

    for fifo in ("/tmp/shairport/audio.fifo", "/tmp/shairport/metadata.fifo"):
        _prepare_fifo(fifo, service_uid, service_gid)

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

    config_changed = not os.path.exists(config_path)
    try:
        try:
            with open(config_path, "r", encoding="utf-8", errors="ignore") as f:
                current_content = f.read()
        except FileNotFoundError:
            current_content = ""
        if current_content.strip() != expected_conf.strip():
            bak_path = config_path + ".bak"
            if os.path.exists(config_path) and not os.path.exists(bak_path):
                import shutil
                shutil.copy2(config_path, bak_path)
            with open(config_path, "w", encoding="utf-8") as f:
                f.write(expected_conf)
            config_changed = True
            log.info("[AirPlay] 已同步更新 %s", config_path)
    except PermissionError as exc:
        raise RuntimeError(f"无法写入 Shairport 配置 {config_path}: {exc}") from exc

    return ShairportEnvironment(config_path, config_changed, service_user, service_group)


def is_shairport_service_active() -> bool:
    return subprocess.run(
        ["systemctl", "is-active", "--quiet", _SHAIRPORT_SERVICE], check=False
    ).returncode == 0


def get_shairport_service_main_pid() -> int:
    result = subprocess.run(
        ["systemctl", "show", _SHAIRPORT_SERVICE, "--property=MainPID", "--value", "--no-pager"],
        capture_output=True, text=True, check=False,
    )
    try:
        return int(result.stdout.strip() or "0")
    except ValueError:
        return 0


def _service_restart_count() -> int:
    result = subprocess.run(
        ["systemctl", "show", _SHAIRPORT_SERVICE, "--property=NRestarts", "--value", "--no-pager"],
        capture_output=True, text=True, check=False,
    )
    try:
        return int(result.stdout.strip() or "0")
    except ValueError:
        return 0


def _log_shairport_journal() -> None:
    journal = subprocess.run(
        ["journalctl", "-u", _SHAIRPORT_SERVICE, "-n", "50", "--no-pager"],
        capture_output=True, text=True, check=False,
    )
    if journal.stdout:
        log.error("[AirPlay] Shairport-Sync 最近日志:\n%s", journal.stdout.strip())


def restart_shairport_service() -> int:
    result = subprocess.run(
        ["systemctl", "restart", _SHAIRPORT_SERVICE], capture_output=True, text=True, check=False,
    )
    if result.returncode != 0:
        log.error("[AirPlay] Shairport-Sync systemd 重启失败: %s", (result.stderr or result.stdout).strip())
        _log_shairport_journal()
        raise RuntimeError("systemd 重启 shairport-sync.service 失败")
    deadline = time.monotonic() + _SERVICE_START_TIMEOUT
    while time.monotonic() < deadline:
        if is_shairport_service_active():
            pid = get_shairport_service_main_pid()
            if pid > 0:
                restart_count = _service_restart_count()
                time.sleep(0.5)
                if (
                    is_shairport_service_active()
                    and get_shairport_service_main_pid() > 0
                    and _service_restart_count() == restart_count
                ):
                    return get_shairport_service_main_pid()
        time.sleep(0.1)
    _log_shairport_journal()
    raise RuntimeError("shairport-sync.service 未在限定时间内进入 active/MainPID 状态")


class ShairportBridge:
    """Shairport-Sync 管道桥接器。
    
    高效读取 /tmp/shairport/audio.fifo 的 PCM 裸流数据，
    同时解析 /tmp/shairport/metadata.fifo 歌名、歌手、专辑与 ID3 封面图片。
    """

    def __init__(
        self,
        pipe_path: str = "/tmp/shairport/audio.fifo",
        meta_path: str = "/tmp/shairport/metadata.fifo",
        airplay_name: str = "MiPlay 全屋播放",
        stream_server: AudioStreamServer | None = None,
        on_play_start: Callable[[], None] | None = None,
        on_play_stop: Callable[[], None] | None = None,
    ):
        self.pipe_path = pipe_path
        self.meta_path = meta_path
        self.airplay_name = airplay_name
        self.stream_server = stream_server
        self.on_play_start = on_play_start
        self.on_play_stop = on_play_stop
        self.metadata: dict[str, str] = {}
        self.artwork: str | None = None
        self._running = False
        self._task: asyncio.Task | None = None
        self._meta_task: asyncio.Task | None = None
        self._service_task: asyncio.Task | None = None
        self._audio_reader_ready: asyncio.Event | None = None
        self._metadata_reader_ready: asyncio.Event | None = None
        self._service_alive = False
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
        """准备 FIFO 读端后，通过 systemd 受控重启唯一的 Shairport-Sync。"""
        try:
            env = await asyncio.to_thread(
                _prepare_shairport_env, "/etc/shairport-sync.conf", self.airplay_name
            )
            self._audio_reader_ready = asyncio.Event()
            self._metadata_reader_ready = asyncio.Event()
            self._running = True
            self._task = asyncio.create_task(self._read_loop())
            self._meta_task = asyncio.create_task(self._read_meta_loop())
            await asyncio.wait_for(
                asyncio.gather(
                    self._audio_reader_ready.wait(),
                    self._metadata_reader_ready.wait(),
                ),
                timeout=_FIFO_READY_TIMEOUT,
            )
            pid = await asyncio.to_thread(restart_shairport_service)
            self._service_alive = True
            self._service_task = asyncio.create_task(self._monitor_service())
            log.info(
                "[AirPlay] systemd Shairport-Sync 接管成功 "
                "(PID: %d, User: %s, Group: %s, config_changed=%s)",
                pid, env.service_user, env.service_group, env.config_changed,
            )
            log.info("Started ShairportBridge on pipe %s", self.pipe_path)
        except Exception as exc:
            self._running = False
            for task in (self._task, self._meta_task):
                if task:
                    task.cancel()
            await asyncio.gather(
                *(task for task in (self._task, self._meta_task) if task),
                return_exceptions=True,
            )
            self._task = None
            self._meta_task = None
            self._service_alive = False
            self.session_state = "error"
            self.last_error = str(exc)
            raise RuntimeError(f"AirPlay 2 systemd 接管失败: {exc}") from exc

    async def stop(self):
        """停止 MiPlay FIFO 监听；保留 systemd Shairport-Sync 后台服务。"""
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
        if self._service_task:
            self._service_task.cancel()
            try:
                await self._service_task
            except asyncio.CancelledError:
                pass
            self._service_task = None

        log.info("Stopped ShairportBridge")

    async def _monitor_service(self):
        while self._running:
            await asyncio.sleep(2.0)
            if not await asyncio.to_thread(is_shairport_service_active):
                self._service_alive = False
                self._end_session("shairport-sync.service 已停止")
                self.session_state = "error"
                return

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
                if self._metadata_reader_ready:
                    self._metadata_reader_ready.set()
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
                if self._audio_reader_ready:
                    self._audio_reader_ready.set()
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
            "shairport_alive": self._service_alive,
            "last_error": self.last_error,
        }
