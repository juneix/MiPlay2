# ============================================================
# ⚠️ 强同步警示 (Sync Warning)
# ------------------------------------------------------------
# 本模块为 MiPlay 应用核心主入口及状态快照聚合中心。
# 任何架构修改、状态快照修改请务必严格遵照项目技术文档:
# 📖 /docs/airplay.md
# ============================================================

"""MiPlay runtime."""

from __future__ import annotations

import asyncio
import io
import logging
import os
import sys
import time

from aiohttp import web

from miplay.audio_hub import AudioHub
from miplay.bridge import BridgeManager
from miplay.config import Config, detect_name_conflicts
from miplay.logger import ColoredFormatter, PlainTextFormatter, RateLimitFilter
from miplay.version import __version__, check_for_updates
from miplay.web.api import create_web_app
from miplay.xiaomi import AuthManager, TargetManager

log = logging.getLogger("miplay")


# <!-- Section: MiPlay App Runtime -->
class MiPlay:
    def __init__(self, config: Config):
        self.config = config
        self.auth = AuthManager(config)
        self.target_manager = TargetManager(config, self.auth)
        self.bridge_manager: BridgeManager | None = None
        self.audio_hub = AudioHub(self)
        self._web_runner: web.AppRunner | None = None
        self.running = False
        self.status_message = ""
        self.warnings: list[str] = []

    async def get_all_devices(self) -> list[dict]:
        if not self.config.xiaomi.cookie:
            return []
        await self.auth.ensure_login()
        return await self.auth.get_device_list()

    def _refresh_warnings(self):
        self.warnings = detect_name_conflicts(self.config.targets)

    async def start(self):
        self._setup_logging()
        self._refresh_warnings()
        self.audio_hub.set_loop(asyncio.get_running_loop())
        web_app = create_web_app(self.config, self)
        self._web_runner = web.AppRunner(web_app, access_log=None)
        await self._web_runner.setup()
        web_site = web.TCPSite(self._web_runner, "0.0.0.0", self.config.web_port)
        await web_site.start()
        log.info("MiPlay Web UI: http://%s:%s", self.config.host, self.config.web_port)

        # 启动后台检测 GitHub 最新 Release 更新任务
        notifier = self.target_manager.notifier if hasattr(self.target_manager, "notifier") else None
        asyncio.create_task(check_for_updates(notifier))

        if self.config.xiaomi.cookie:
            await self._start_bridges()
        else:
            self.status_message = "Configure Xiaomi credentials to enable wireless bridge targets."
            log.info(self.status_message)

    async def _start_bridges(self):
        try:
            await self.auth.login()
            await self.target_manager.init_targets()
            if not self.target_manager.controllers:
                self.status_message = "No Xiaomi targets are ready; sync devices and verify credentials."
                log.warning(self.status_message)
                return
            self.bridge_manager = BridgeManager(self.config.host, self.config, self.audio_hub)
            await self.bridge_manager.start_for_targets(self.target_manager.controllers)
            self.running = True
            self.status_message = f"MiPlay running with {len(self.target_manager.controllers)} Xiaomi AirPlay target(s)."
            log.info(self.status_message)
        except Exception as exc:
            self.status_message = f"Startup failed: {exc}"
            log.error(self.status_message)

    async def stop(self):
        self.running = False
        if self.bridge_manager:
            await self.bridge_manager.stop()
            self.bridge_manager = None
        if self._web_runner:
            try:
                await asyncio.wait_for(self._web_runner.cleanup(), timeout=3.0)
            except asyncio.TimeoutError:
                log.warning("Web cleanup timed out")
        await self.auth.close()

    async def run_forever(self):
        await self.start()
        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()

    def get_runtime_targets(self) -> list[dict]:
        snapshots = {}
        if self.bridge_manager:
            for item in self.bridge_manager.snapshot():
                if item.get("id"):
                    snapshots[item["id"]] = item
                if item.get("did"):
                    snapshots[item["did"]] = item

        result = []
        for target in self.config.targets:
            item = {
                "id": target.id,
                "did": target.did,
                "name": target.name,
                "airplay_name": target.airplay_name,
                "enabled": target.enabled,
                "device_id": target.device_id,
                "hardware": target.hardware,
                "active": False,
                "client_name": "",
                "metadata": {},
                "artwork": None,
                "rtsp_port": 0,
            }
            snap = snapshots.get(target.did) or snapshots.get(target.id) or {}
            item.update(snap)
            result.append(item)

        # 包含全屋虚拟分组 group_all 的运行时快照数据 (包含实际 RTSP 端口号)
        if "group_all" in snapshots:
            result.append(snapshots["group_all"])

        return result

    def get_active_audio_stream_server(self):
        """获取当前全屋组播或首个桥接器的音频流服务。"""
        if self.bridge_manager:
            server = self.bridge_manager.get_group_audio_stream_server()
            if server:
                return server
            for bridge in self.bridge_manager.bridges.values():
                if bridge.airplay_server and bridge.airplay_server.audio_stream:
                    return bridge.airplay_server.audio_stream
        return None

    def get_status_snapshot(self) -> dict:
        return {
            "version": __version__,
            "running": self.running,
            "host": self.config.host,
            "web_port": self.config.web_port,
            "virtual_delay": self.config.virtual_delay,
            "targets_count": len(self.config.get_enabled_targets()),
            "bridges_count": len(self.bridge_manager.bridges) if self.bridge_manager else 0,
            "status_message": self.status_message,
            "warnings": self.warnings,
            "hub": self.audio_hub.get_status() if self.audio_hub else {},
            "session": self.audio_hub.get_session() if self.audio_hub else {},
        }

    async def play_url_to_targets(self, target: str, url: str) -> bool:
        """下发拉流 URL 到全屋组播或单个音箱。"""
        if target in ("group_all", "group", "all"):
            if self.bridge_manager and hasattr(self.bridge_manager, "group_controller") and self.bridge_manager.group_controller:
                return await self.bridge_manager.group_controller.play_url(url)
            log.warning("No active group controller for group play_url")
            return False
        
        controller = self.target_manager.controllers.get(target)
        if not controller:
            # 尝试通过 did 查找
            for ctrl in self.target_manager.controllers.values():
                if ctrl.did == target:
                    controller = ctrl
                    break
        if controller:
            return await controller.play_url(url)
        log.warning("Target speaker %s not found for play_url", target)
        return False

    async def stop_targets(self, target: str = "group_all") -> bool:
        """通知全屋组播或单个音箱停止播放。"""
        if target in ("group_all", "group", "all"):
            if self.bridge_manager and hasattr(self.bridge_manager, "group_controller") and self.bridge_manager.group_controller:
                return await self.bridge_manager.group_controller.stop()
            return True
        controller = self.target_manager.controllers.get(target)
        if controller:
            return await controller.stop()
        return True

    async def set_volume_to_targets(self, target: str, volume: int) -> bool:
        """调整全屋组播或单个音箱的音量。"""
        if target in ("group_all", "group", "all"):
            if self.bridge_manager and hasattr(self.bridge_manager, "group_controller") and self.bridge_manager.group_controller:
                return await self.bridge_manager.group_controller.set_volume(volume)
            return False
        controller = self.target_manager.controllers.get(target)
        if controller:
            return await controller.set_volume(volume)
        return False

    async def control_target(self, target_id: str, action: str) -> bool:
        controller = self.target_manager.controllers.get(target_id)
        if not controller:
            raise ValueError(f"Target {target_id} not active")
        
        if action == "stop":
            return await controller.stop()
        elif action == "pause":
            return await controller.pause()
        elif action == "play":
            return False
        return False

    def _setup_logging(self):
        root_logger = logging.getLogger()
        level = logging.DEBUG if self.config.verbose else logging.INFO
        root_logger.setLevel(level)
        logging.getLogger("miplay").setLevel(level)
        logging.getLogger("miservice").setLevel(logging.WARNING)

        if root_logger.handlers:
            return

        date_fmt = "%Y-%m-%d %H:%M:%S"
        rate_filter = RateLimitFilter(interval_sec=5.0)

        # 终端 Handler (带 ANSI 颜色高亮)
        if sys.platform == "win32":
            stream = io.TextIOWrapper(
                sys.stderr.buffer,
                encoding="utf-8",
                errors="replace",
                line_buffering=True,
            )
            console = logging.StreamHandler(stream)
        else:
            console = logging.StreamHandler(sys.stdout)

        console.setFormatter(ColoredFormatter(datefmt=date_fmt))
        console.addFilter(rate_filter)
        root_logger.addHandler(console)

        # 文件 Handler (干净纯文本，无 ANSI 码，防止文件与前端弹窗乱码)
        log_path = os.path.join(self.config.conf_path, "miplay.log")
        os.makedirs(self.config.conf_path, exist_ok=True)
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(PlainTextFormatter(datefmt=date_fmt))
        file_handler.addFilter(rate_filter)
        root_logger.addHandler(file_handler)

        def handle_unhandled_exception(exc_type, exc_value, exc_traceback):
            if issubclass(exc_type, KeyboardInterrupt):
                sys.__excepthook__(exc_type, exc_value, exc_traceback)
                return
            root_logger.critical("Unhandled exception", exc_info=(exc_type, exc_value, exc_traceback))

        sys.excepthook = handle_unhandled_exception
