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

from miplay.bridge import AirPlayBridgeManager
from miplay.config import Config, build_external_status, detect_name_conflicts
from miplay.logger import ColoredFormatter, PlainTextFormatter, RateLimitFilter
from miplay.version import check_for_updates
from miplay.web.api import create_web_app
from miplay.xiaomi import XiaomiAuthManager, XiaomiTargetManager

log = logging.getLogger("miplay")


# <!-- Section: MiPlay App Runtime -->
class MiPlay:
    def __init__(self, config: Config):
        self.config = config
        self.auth = XiaomiAuthManager(config)
        self.target_manager = XiaomiTargetManager(config, self.auth)
        self.bridge_manager: AirPlayBridgeManager | None = None
        self._web_runner: web.AppRunner | None = None
        self.running = False
        self.status_message = ""
        self.warnings: list[str] = []

    async def get_all_devices(self) -> list[dict]:
        if not self.config.xiaomi.account and not self.config.xiaomi.cookie:
            return []
        await self.auth.ensure_login()
        return await self.auth.get_device_list()

    def _refresh_warnings(self):
        self.warnings = detect_name_conflicts(
            self.config.targets,
            self.config.external.wired_airplay_name,
        )

    async def start(self):
        self._setup_logging()
        self._refresh_warnings()
        web_app = create_web_app(self.config, self)
        self._web_runner = web.AppRunner(web_app, access_log=None)
        await self._web_runner.setup()
        web_site = web.TCPSite(self._web_runner, "0.0.0.0", self.config.web_port)
        await web_site.start()
        log.info("MiPlay Web UI: http://%s:%s", self.config.host, self.config.web_port)

        # 启动后台检测 GitHub 最新 Release 更新任务
        notifier = self.target_manager.notifier if hasattr(self.target_manager, "notifier") else None
        asyncio.create_task(check_for_updates(notifier))

        if self.config.xiaomi.account or self.config.xiaomi.cookie:
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
            self.bridge_manager = AirPlayBridgeManager(self.config.host, self.config)
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
                snapshots[item["id"]] = item

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
            item.update(snapshots.get(target.id, {}))
            result.append(item)

        # 包含全屋虚拟分组 group_all 的运行时快照数据 (包含实际 RTSP 端口号)
        if "group_all" in snapshots:
            result.append(snapshots["group_all"])

        return result

    def get_status_snapshot(self) -> dict:
        external = build_external_status(self.config)
        return {
            "version": "0.2.0",
            "running": self.running,
            "host": self.config.host,
            "web_port": self.config.web_port,
            "targets_count": len(self.config.get_enabled_targets()),
            "bridges_count": len(self.bridge_manager.bridges) if self.bridge_manager else 0,
            "status_message": self.status_message,
            "warnings": self.warnings,
            "external": external,
        }

    async def control_target(self, target_id: str, action: str) -> bool:
        controller = self.target_manager.controllers.get(target_id)
        if not controller:
            raise ValueError(f"Target {target_id} not active")
        
        if action == "pause":
            # AirPlay bridge does not support reverse control reliably
            return False
        elif action == "play":
            # AirPlay bridge does not support reverse control reliably
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
