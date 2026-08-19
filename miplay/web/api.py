# ============================================================
# ⚠️ 强同步警示 (Sync Warning)
# ------------------------------------------------------------
# 本模块为 MiPlay Web API 与 WebSocket 实时会话协同中心。
# 任何架构修改请务必严格遵照项目技术文档:
# 📖 /docs/miplay-hub.md
# ============================================================

"""Web API for MiPlay."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
import os
import sys
import time
import uuid

import aiohttp
from aiohttp import web

from miplay.config import Config
from miplay.notify import Notifier
from miplay.qr_login import QRLoginManager

log = logging.getLogger("miplay")
qr_manager = QRLoginManager()

# 全局在线 Web 虚拟音箱会话管理 (按 device_id 唯一去重)
_ws_sessions: dict[str, dict] = {}


def _restart_process():
    args = [sys.executable, "-m", "miplay.cli", *sys.argv[1:]]
    if sys.platform == "win32":
        import subprocess

        subprocess.Popen(args)
        os._exit(0)
    os.execv(sys.executable, args)


async def _send_test_notification(notifier: Notifier):
    ok = await notifier.send(
        title="MiPlay · 通知测试",
        content="推送通知配置成功，后续登录异常与消息将通过此通道推送。",
    )
    if ok:
        log.info("[Notify] 测试通知发送成功")
    else:
        log.warning("[Notify] 测试通知发送失败，请检查配置")


def get_virtual_speakers() -> list[dict]:
    """获取当前所有在线存活的 Web 虚拟音箱列表 (按物理 IP 归一聚合，先到且保持活跃)。"""
    # 剔除已断开的僵尸会话
    for did in list(_ws_sessions.keys()):
        s = _ws_sessions.get(did)
        if not s or s.get("ws") is None or s["ws"].closed:
            _ws_sessions.pop(did, None)

    ip_map = {}
    for s in sorted(_ws_sessions.values(), key=lambda x: x.get("connected_at", 0)):
        ip = s.get("ip") or "127.0.0.1"
        if ip not in ip_map:
            ip_map[ip] = {
                "id": s["id"],
                "ip": ip,
                "name": s["name"],
                "connected_at": s["connected_at"],
            }
    return list(ip_map.values())


async def broadcast_ws(message: dict):
    """向所有连接的 Web 客户端广播实时消息。"""
    if not _ws_sessions:
        return
    data = json.dumps(message)
    coros = []
    for s in list(_ws_sessions.values()):
        ws: web.WebSocketResponse = s.get("ws")
        if ws and not ws.closed:
            coros.append(ws.send_str(data))
    if coros:
        await asyncio.gather(*coros, return_exceptions=True)


def create_web_app(config: Config, app_instance) -> web.Application:
    qr_manager.conf_path = config.conf_path
    web_app = web.Application()

    async def handle_ws(request: web.Request) -> web.WebSocketResponse:
        """原生 WebSocket 路由：管理 Web 虚拟音箱在线生命周期与断连即刻注销。"""
        ws = web.WebSocketResponse(heartbeat=15.0)
        await ws.prepare(request)

        device_id = request.query.get("device_id") or request.query.get("id") or str(uuid.uuid4())
        client_ip = request.remote or "127.0.0.1"
        device_name = request.query.get("name") or client_ip

        _ws_sessions[device_id] = {
            "id": device_id,
            "ip": client_ip,
            "name": device_name,
            "connected_at": time.time(),
            "ws": ws,
        }
        log.info("[WebSocket] Web 虚拟音箱已连接: %s (%s - %s), 当前在线: %d", device_name, client_ip, device_id, len(_ws_sessions))

        # 即刻向当前连接客户端单播自身 IP 与在线列表 (零等待即时绑定真实 IP)
        await ws.send_json({
            "type": "session_init",
            "client_ip": client_ip,
            "virtual_speakers": get_virtual_speakers(),
        })

        # 广播最新虚拟音箱在线状态
        await broadcast_ws({
            "type": "presence_update",
            "virtual_speakers": get_virtual_speakers(),
        })

        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        payload = json.loads(msg.data)
                        if payload.get("type") == "ping":
                            await ws.send_json({"type": "pong"})
                    except Exception:
                        pass
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    break
        finally:
            _ws_sessions.pop(device_id, None)
            log.info("[WebSocket] Web 虚拟音箱已断开: %s (%s), 剩余在线: %d", device_name, client_ip, len(_ws_sessions))
            await broadcast_ws({
                "type": "presence_update",
                "virtual_speakers": get_virtual_speakers(),
            })

        return ws

    async def handle_index(request: web.Request):
        index_path = os.path.join(os.path.dirname(__file__), "index.html")
        return web.FileResponse(index_path)

    async def handle_test_page(request: web.Request):
        test_path = os.path.join(os.path.dirname(__file__), "test.html")
        return web.FileResponse(test_path)

    async def handle_get_setting(request: web.Request):
        need_device_list = request.query.get("need_device_list", "false") == "true"
        client_ip = request.remote or "127.0.0.1"
        payload = {
            "client_ip": client_ip,
            "virtual_speakers": get_virtual_speakers(),
            "default_audio_id": config.default_audio_id,
            "xiaomi": {
                "cookie": config.xiaomi.cookie,
                "has_credentials": bool(config.xiaomi.cookie),
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
                    "default_audio_id": target.default_audio_id,
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

        if "default_audio_id" in data:
            config.default_audio_id = str(data["default_audio_id"]).strip()
            need_restart = True

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
            title="MiPlay · 通知测试",
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
        res = app_instance.get_status_snapshot()
        res["client_ip"] = request.remote or "127.0.0.1"
        res["virtual_speakers"] = get_virtual_speakers()
        return web.json_response(res)

    async def handle_stream_live_wav(request: web.Request) -> web.StreamResponse:
        """Egress 实时音频广播流 (支持手机浏览器、自研 APK、VLC 等即开即听)。"""
        response = web.StreamResponse(
            status=200,
            headers={
                "Content-Type": "audio/wav",
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Connection": "close",
                "Transfer-Encoding": "chunked",
                "Access-Control-Allow-Origin": "*",
            },
        )
        await response.prepare(request)

        # 注册成为 Egress 监听队列
        queue_listener = app_instance.audio_hub.create_listener_queue()
        try:
            # 写入标准 WAV 头部 (44.1k/16bit/2ch)
            await response.write(app_instance.audio_hub.build_wav_header())

            loop = asyncio.get_running_loop()
            while True:
                # 非阻塞等待队列中的 PCM 音频数据
                chunk = await loop.run_in_executor(None, queue_listener.get)
                if chunk is None:
                    break
                await response.write(chunk)
        except Exception:
            pass
        finally:
            app_instance.audio_hub.remove_listener_queue(queue_listener)
        return response

    async def handle_audio_stream_ingest(request: web.Request) -> web.Response:
        """Ingest 实时音频推流 (外部自研音乐工具/脚本持续推流到小米音箱)。"""
        if not app_instance.audio_hub.start_source("api_ingest"):
            return web.json_response(
                {"ok": False, "message": "AirPlay 正在播放中或通道繁忙，拒绝推流"},
                status=409,
            )

        target = request.headers.get("X-Target-ID") or request.query.get("target", "group_all")
        stream_server = app_instance.get_active_audio_stream_server()
        stream_url = stream_server.stream_url if stream_server else f"http://{config.host}:{config.web_port}/stream/live.wav"

        if stream_server:
            stream_server.start_streaming()

        # 下发播放指令给目标真实音箱与 Web 虚拟音箱
        await broadcast_ws({
            "type": "control_command",
            "action": "play_url",
            "target": target,
            "url": stream_url,
        })
        if not target.startswith("virtual_") or target in ("group_all", "group", "all"):
            await app_instance.play_url_to_targets(target, stream_url)
        log.info("[AudioHub] 外部 API 推流已连接，下发播放目标: %s", target)

        try:
            async for chunk, _ in request.content.iter_chunks():
                if chunk:
                    if stream_server:
                        stream_server.write_pcm(chunk)
                    app_instance.audio_hub.broadcast_pcm(chunk)
            return web.json_response({"ok": True, "message": "推流完成"})
        except Exception as exc:
            log.warning("[AudioHub] 外部 API 推流中断: %s", exc)
            return web.json_response({"ok": False, "message": str(exc)}, status=500)
        finally:
            app_instance.audio_hub.stop_source("api_ingest")
            if stream_server:
                stream_server.stop_streaming()
            await broadcast_ws({
                "type": "control_command",
                "action": "stop",
                "target": target,
            })
            if not target.startswith("virtual_") or target in ("group_all", "group", "all"):
                await app_instance.stop_targets(target)
            log.info("[AudioHub] 外部 API 推流已结束并通知音箱停止: %s", target)

    async def handle_control_play(request: web.Request) -> web.Response:
        """通用音频播放与控制指令 RPC 接口 (兼容 /api/control 与 /api/v1/control/play)。"""
        data = await request.json()
        action = data.get("action", "play_url")
        target = data.get("target") or data.get("id") or "group_all"
        url = data.get("url", "")
        volume = data.get("volume")

        log.info("[Audio] 控制指令: action=%s target=%s url=%s (客户端: %s)", action, target, url, request.remote or "127.0.0.1")

        # 若目标是具体的 Web 虚拟音箱，先校验其是否在线
        if target.startswith("virtual_") and target not in ("group_all", "group", "all", "virtual_all", "virtual_web"):
            dev_id = target[8:]  # remove 'virtual_'
            matched = [s for s in _ws_sessions.values() if s["id"] == dev_id or s["ip"] == dev_id]
            if not matched:
                return web.json_response(
                    {"ok": False, "message": f"Web 虚拟音箱 ({dev_id}) 当前已离线或未连接"},
                    status=404,
                )

        # 广播 WebSocket 控制指令到在线 Web 虚拟音箱
        await broadcast_ws({
            "type": "control_command",
            "action": action,
            "target": target,
            "url": url,
            "volume": volume,
        })

        if action == "play_url":
            if not url:
                return web.json_response({"ok": False, "message": "Missing url"}, status=400)
            if target.startswith("virtual_") and target not in ("group_all", "group", "all"):
                return web.json_response({"ok": True, "target": target})
            ok = await app_instance.play_url_to_targets(target, url)
            return web.json_response({"ok": ok})
        elif action in ("stop", "pause"):
            if target.startswith("virtual_") and target not in ("group_all", "group", "all"):
                return web.json_response({"ok": True, "target": target})
            ok = await app_instance.stop_targets(target)
            return web.json_response({"ok": ok})
        elif action == "set_volume":
            if volume is None:
                return web.json_response({"ok": False, "message": "Missing volume"}, status=400)
            if target.startswith("virtual_") and target not in ("group_all", "group", "all"):
                return web.json_response({"ok": True, "target": target})
            ok = await app_instance.set_volume_to_targets(target, int(volume))
            return web.json_response({"ok": ok})
        else:
            return web.json_response({"ok": False, "message": f"Unsupported action: {action}"}, status=400)

    async def handle_hub_status(request: web.Request) -> web.Response:
        """获取 Audio Hub 运行状态与监听者统计。"""
        return web.json_response(app_instance.audio_hub.get_status())

    async def handle_proxy(request: web.Request) -> web.StreamResponse:
        """通用音频流 / 封面图透明反向代理 (解除浏览器 CORS 与防盗链限制)。"""
        url = request.query.get("url", "").strip()
        if not url:
            return web.Response(status=400, text="Missing url parameter")
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": url,
        }
        try:
            connector = aiohttp.TCPConnector(ssl=False)
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=20)) as upstream:
                    content_type = upstream.headers.get("Content-Type", "")
                    # 若为 M3U8 播放列表文本，自动补齐重写内部切片路径
                    if "mpegurl" in content_type.lower() or "m3u8" in content_type.lower() or url.endswith(".m3u8"):
                        text = await upstream.text(encoding="utf-8", errors="ignore")
                        from urllib.parse import urljoin
                        lines = []
                        for line in text.splitlines():
                            line_s = line.strip()
                            if line_s and not line_s.startswith("#"):
                                full_chunk_url = urljoin(url, line_s)
                                lines.append(f"/api/v1/proxy?url={full_chunk_url}")
                            else:
                                lines.append(line)
                        rewritten = "\n".join(lines).encode("utf-8")
                        return web.Response(
                            body=rewritten,
                            content_type="application/vnd.apple.mpegurl",
                            headers={
                                "Access-Control-Allow-Origin": "*",
                                "Cache-Control": "no-cache",
                            },
                        )

                    # 媒体切片 / 普通音频 / 封面图二进制透传
                    resp = web.StreamResponse(
                        status=upstream.status,
                        headers={
                            "Content-Type": content_type or "application/octet-stream",
                            "Access-Control-Allow-Origin": "*",
                            "Cache-Control": "no-cache",
                        },
                    )
                    await resp.prepare(request)
                    async for chunk in upstream.content.iter_chunked(65536):
                        await resp.write(chunk)
                    return resp
        except Exception as exc:
            return web.Response(status=502, text=f"Proxy error: {exc}", headers={"Access-Control-Allow-Origin": "*"})

    web_app.router.add_get("/", handle_index)
    web_app.router.add_get("/test", handle_test_page)
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
    web_app.router.add_get("/api/ws", handle_ws)
    web_app.router.add_post("/api/control", handle_control_play)

    # 开放 Ingest 与 Egress 音频中枢端点
    web_app.router.add_get("/stream/live.wav", handle_stream_live_wav)
    web_app.router.add_get("/stream/group.wav", handle_stream_live_wav)
    web_app.router.add_post("/api/v1/audio/stream", handle_audio_stream_ingest)
    web_app.router.add_post("/api/v1/control/play", handle_control_play)
    web_app.router.add_get("/api/v1/hub/status", handle_hub_status)
    web_app.router.add_get("/api/v1/proxy", handle_proxy)

    static_dir = os.path.join(os.path.dirname(__file__), "static")
    if os.path.exists(static_dir):
        web_app.router.add_static("/static", static_dir)
    return web_app

