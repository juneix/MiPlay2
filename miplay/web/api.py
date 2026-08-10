"""Web API for MiPlay."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
import os
import sys

from aiohttp import web

from miplay.config import Config
from miplay.notify import Notifier
from miplay.qr_login import QRLoginManager

log = logging.getLogger("miplay")
qr_manager = QRLoginManager()


def _restart_process():
    args = [sys.executable, "-m", "miplay.cli", *sys.argv[1:]]
    if sys.platform == "win32":
        import subprocess

        subprocess.Popen(args)
        os._exit(0)
    os.execv(sys.executable, args)


async def _send_test_notification(notifier: Notifier):
    ok = await notifier.send(
        title="[MiPlay] 通知测试",
        content="推送通知配置成功，后续登录异常与消息将通过此通道推送。",
    )
    if ok:
        log.info("[Notify] 测试通知发送成功")
    else:
        log.warning("[Notify] 测试通知发送失败，请检查配置")


def create_web_app(config: Config, app_instance) -> web.Application:
    web_app = web.Application()

    async def handle_index(request: web.Request):
        index_path = os.path.join(os.path.dirname(__file__), "index.html")
        return web.FileResponse(index_path)

    async def handle_get_setting(request: web.Request):
        need_device_list = request.query.get("need_device_list", "false") == "true"
        payload = {
            "xiaomi": {
                "account": config.xiaomi.account,
                "cookie": config.xiaomi.cookie,
                "has_credentials": bool(config.xiaomi.account or config.xiaomi.cookie),
            },
            "notify": {
                "channel": config.notify.channel,
                "key": config.notify.key,
            },
            "targets": [
                {
                    "id": target.id,
                    "did": target.did,
                    "name": target.name,
                    "airplay_name": target.airplay_name,
                    "enabled": target.enabled,
                    "device_id": target.device_id,
                    "hardware": target.hardware,
                }
                for target in config.targets
            ],
            "group": {
                "airplay_name": config.group.airplay_name,
                "member_dids": config.group.member_dids,
            },
            "status": app_instance.get_status_snapshot(),
        }
        # ⚠️ 防坑警示：设备列表必须仅由 get_all_devices() 动态鉴权获取，严禁降级为静态 Targets 兜底。
        if need_device_list:
            try:
                payload["device_list"] = await app_instance.get_all_devices()
                payload["device_list_error"] = ""
            except Exception as exc:
                payload["device_list"] = []
                payload["device_list_error"] = str(exc)
        return web.json_response(payload)

    async def handle_save_setting(request: web.Request):
        data = await request.json()
        need_restart = False

        if "xiaomi" in data:
            xiaomi = data["xiaomi"]
            if "cookie" in xiaomi:
                config.xiaomi.cookie = str(xiaomi["cookie"]).strip()
                need_restart = True

        if "notify" in data:
            notify_data = data["notify"]
            if "channel" in notify_data:
                config.notify.channel = str(notify_data["channel"]).strip()
            if "key" in notify_data:
                config.notify.key = str(notify_data["key"]).strip()

        if "targets" in data:
            new_targets = data["targets"]
            if new_targets or not config.targets:
                config.set_targets(new_targets)
                need_restart = True

        if "group" in data:
            group_data = data["group"]
            if "airplay_name" in group_data:
                new_name = str(group_data["airplay_name"]).strip()
                config.group.airplay_name = new_name
                from miplay.airplay.shairport_bridge import update_shairport_conf_name
                update_shairport_conf_name(new_name)
            if "member_dids" in group_data:
                config.group.member_dids = [str(d) for d in group_data["member_dids"]]
            need_restart = True

        config.save()

        if need_restart:
            response = web.json_response({"ok": True, "message": "已保存，服务重启中..."})
            await response.prepare(request)
            await response.write_eof()
            asyncio.get_running_loop().call_soon(_restart_process)
            return response
        else:
            return web.json_response({"ok": True, "message": "通知保存成功"})

    async def handle_test_notify(request: web.Request):
        data = await request.json()
        channel = str(data.get("channel", config.notify.channel)).strip()
        key = str(data.get("key", config.notify.key)).strip()

        if not key:
            return web.json_response({"ok": False, "message": "请先输入推送地址"}, status=400)

        notifier = Notifier(channel, key)
        ok = await notifier.send(
            title="[MiPlay] 通知测试",
            content="这是一条来自 MiPlay 的测试推送提醒",
        )
        if ok:
            return web.json_response({"ok": True, "message": "推送测试成功，请查看通知"})
        else:
            return web.json_response({"ok": False, "message": "推送测试失败，请检查设置"}, status=400)

    async def handle_get_log_content(request: web.Request):
        log_path = os.path.join(config.conf_path, "miplay.log")
        if not os.path.exists(log_path):
            return web.json_response({"ok": True, "content": "暂无日志记录。"})
        try:
            with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()
                # 返回最后 300 行日志，避免过大
                content = "".join(lines[-300:])
                return web.json_response({"ok": True, "content": content})
        except Exception as exc:
            return web.json_response({"ok": False, "message": str(exc)}, status=500)

    async def handle_download_logs(request: web.Request):
        log_path = os.path.join(config.conf_path, "miplay.log")
        if not os.path.exists(log_path):
            return web.Response(text="暂无日志文件", status=404)
        return web.FileResponse(log_path, headers={"Content-Disposition": "attachment; filename=miplay.log"})

    async def handle_qr_start(request: web.Request):
        try:
            res = await qr_manager.start()
            if not res:
                return web.json_response({"success": False, "message": "获取二维码失败，请稍后重试"}, status=500)
            session_id, info = res
            return web.json_response({
                "success": True,
                "session_id": session_id,
                "qrcode_url": info.get("qrcode_url", "")
            })
        except Exception as exc:
            return web.json_response({"success": False, "message": str(exc)}, status=500)

    async def handle_qr_poll(request: web.Request):
        session_id = request.query.get("session_id")
        if not session_id:
            return web.json_response({"state": "failed", "message": "Missing session_id"}, status=400)
        res = await qr_manager.poll(session_id)
        if res.get("state") == "confirmed" and res.get("credentials"):
            creds = res["credentials"]
            cookie = f"userId={creds['userId']}; passToken={creds['passToken']}"
            config.xiaomi.cookie = cookie
            config.save()
            asyncio.get_running_loop().call_soon(_restart_process)
        return web.json_response(res)

    async def handle_qr_cancel(request: web.Request):
        try:
            data = await request.json()
            session_id = data.get("session_id")
            if session_id:
                await qr_manager.cancel(session_id)
        except Exception:
            pass
        return web.json_response({"ok": True})

    async def handle_restart(request: web.Request):
        response = web.json_response({"ok": True, "message": "已保存，服务重启中..."})
        await response.prepare(request)
        await response.write_eof()
        asyncio.get_running_loop().call_soon(_restart_process)
        return response

    async def handle_get_devices(request: web.Request):
        devices = await app_instance.get_all_devices()
        return web.json_response({"devices": devices})

    async def handle_get_targets(request: web.Request):
        return web.json_response(app_instance.get_runtime_targets())

    async def handle_status(request: web.Request):
        return web.json_response(app_instance.get_status_snapshot())

    async def handle_control(request: web.Request):
        data = await request.json()
        target_id = data.get("id")
        action = data.get("action")
        if not target_id or not action:
            return web.json_response({"ok": False, "message": "Missing id or action"}, status=400)
        try:
            ok = await app_instance.control_target(target_id, action)
            return web.json_response({"ok": ok})
        except Exception as exc:
            return web.json_response({"ok": False, "message": str(exc)}, status=500)

    web_app.router.add_get("/", handle_index)
    web_app.router.add_get("/api/setting", handle_get_setting)
    web_app.router.add_post("/api/setting", handle_save_setting)
    web_app.router.add_post("/api/notify/test", handle_test_notify)
    web_app.router.add_get("/api/logs/content", handle_get_log_content)
    web_app.router.add_get("/api/logs/download", handle_download_logs)
    web_app.router.add_post("/api/qr/start", handle_qr_start)
    web_app.router.add_get("/api/qr/poll", handle_qr_poll)
    web_app.router.add_post("/api/qr/cancel", handle_qr_cancel)
    web_app.router.add_post("/api/restart", handle_restart)
    web_app.router.add_get("/api/devices", handle_get_devices)
    web_app.router.add_get("/api/targets", handle_get_targets)
    web_app.router.add_get("/api/status", handle_status)
    web_app.router.add_post("/api/control", handle_control)

    static_dir = os.path.join(os.path.dirname(__file__), "static")
    if os.path.exists(static_dir):
        web_app.router.add_static("/static", static_dir)
    return web_app

