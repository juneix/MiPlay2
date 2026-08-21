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
from collections import deque
import json
import logging
from datetime import datetime
import os
import queue
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

_PCM_FRAME_BYTES = 4
_PCM_BYTE_RATE = 44100 * 2 * 2


class _PcmDelayBuffer:
    """Retain a fixed PCM window and emit only audio older than that window."""

    def __init__(self, virtual_delay: int):
        target_bytes = _PCM_BYTE_RATE * virtual_delay // 1000
        self.target_bytes = target_bytes - (target_bytes % _PCM_FRAME_BYTES)
        self.buffered_bytes = 0
        self._chunks: deque[bytes] = deque()

    def push(self, data: bytes) -> bytes:
        if data:
            self._chunks.append(data)
            self.buffered_bytes += len(data)

        emit_bytes = self.buffered_bytes - self.target_bytes
        emit_bytes -= emit_bytes % _PCM_FRAME_BYTES
        if emit_bytes <= 0:
            return b""

        output = bytearray()
        while emit_bytes > 0:
            chunk = self._chunks[0]
            take = min(emit_bytes, len(chunk))
            output.extend(chunk[:take])
            if take == len(chunk):
                self._chunks.popleft()
            else:
                self._chunks[0] = chunk[take:]
            self.buffered_bytes -= take
            emit_bytes -= take
        return bytes(output)


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
                "name": ip,
                "connected_at": s["connected_at"],
            }
    return list(ip_map.values())


async def broadcast_ws(message: dict):
    """向所有连接的 Web 客户端广播实时消息。"""
    data = json.dumps(message)
    coros = []
    for s in list(_ws_sessions.values()):
        ws: web.WebSocketResponse = s.get("ws")
        if ws and not ws.closed:
            coros.append(ws.send_str(data))
    if message.get("type") == "control_command":
        log.info(
            "[Audio] WebSocket 广播控制指令: action=%s target=%s clients=%d server_ts=%d",
            message.get("action", ""),
            message.get("target", ""),
            len(coros),
            int(time.time() * 1000),
        )
    if coros:
        await asyncio.gather(*coros, return_exceptions=True)


def create_web_app(config: Config, app_instance) -> web.Application:
    qr_manager.conf_path = config.conf_path
    web_app = web.Application()

    async def handle_ws(request: web.Request) -> web.WebSocketResponse:
        """原生 WebSocket 路由：管理 Web 虚拟音箱在线生命周期与断连即刻注销。"""
        if request.query.get("role") != "pod":
            raise web.HTTPForbidden(text="Only /pod may register as a virtual speaker")
        ws = web.WebSocketResponse(heartbeat=15.0)
        await ws.prepare(request)

        device_id = request.query.get("device_id") or request.query.get("id") or str(uuid.uuid4())
        client_ip = request.remote or "127.0.0.1"
        device_name = client_ip

        _ws_sessions[device_id] = {
            "id": device_id,
            "ip": client_ip,
            "name": device_name,
            "connected_at": time.time(),
            "ws": ws,
        }
        log.info("[System] Web 虚拟音箱已连接: name=%s ip=%s device=%s online=%d", device_name, client_ip, device_id, len(_ws_sessions))

        # 即刻向当前连接客户端单播自身 IP 与在线列表 (零等待即时绑定真实 IP)
        session = app_instance.audio_hub.get_session()
        await ws.send_json({
            "type": "session_init",
            "client_ip": client_ip,
            "virtual_speakers": get_virtual_speakers(),
            "virtual_delay": config.virtual_delay,
            "session": session,
        })
        # session_init 与封面/进度更新可能并发到达；连接建立后再补发一次
        # 当前快照，避免刷新时只拿到开播初始的空 artwork。
        await ws.send_json({"type": "playback_state", "session": app_instance.audio_hub.get_session()})

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
                        elif payload.get("type") == "virtual_audio_event":
                            event = str(payload.get("event") or "")
                            allowed_events = {
                                "command_received",
                                "scheduled",
                                "play_call",
                                "playing",
                                "paused",
                                "ended",
                                "play_error",
                                "stopped",
                                "stream_connected",
                                "buffer_ready",
                                "reconnecting",
                                "disconnected",
                            }
                            if event in allowed_events:
                                fields = [
                                    f"device={device_id}",
                                    f"ip={client_ip}",
                                    f"event={event}",
                                ]
                                target = payload.get("target")
                                if target:
                                    fields.append(f"target={target}")
                                client_ts = payload.get("client_ts")
                                if client_ts is not None:
                                    fields.append(f"client_ts={client_ts}")
                                virtual_delay = payload.get("virtual_delay")
                                if virtual_delay is not None:
                                    fields.append(f"virtual_delay={virtual_delay}")
                                error = str(payload.get("error") or "")[:300]
                                if event == "play_error" and error:
                                    fields.append(f"error={error}")
                                session_id = payload.get("session_id")
                                if session_id is not None:
                                    fields.append(f"session_id={session_id}")
                                log.info("[Audio] Web 虚拟音箱 %s", " ".join(fields))
                    except Exception:
                        pass
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    break
        finally:
            _ws_sessions.pop(device_id, None)
            log.info("[System] Web 虚拟音箱已断开: name=%s ip=%s device=%s online=%d", device_name, client_ip, device_id, len(_ws_sessions))
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

    async def handle_pod_page(request: web.Request):
        pod_path = os.path.join(os.path.dirname(__file__), "pod.html")
        return web.FileResponse(pod_path)

    async def handle_pod_manifest(request: web.Request):
        manifest_path = os.path.join(os.path.dirname(__file__), "pod-manifest.webmanifest")
        return web.FileResponse(manifest_path, headers={"Content-Type": "application/manifest+json"})

    async def handle_pod_service_worker(request: web.Request):
        worker_path = os.path.join(os.path.dirname(__file__), "pod-sw.js")
        return web.FileResponse(
            worker_path,
            headers={
                "Content-Type": "application/javascript",
                "Service-Worker-Allowed": "/pod",
                "Cache-Control": "no-cache",
            },
        )

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
        res["session"] = app_instance.audio_hub.get_session()
        return web.json_response(res)

    async def handle_stream_live_wav(request: web.Request) -> web.StreamResponse:
        """Egress 实时音频广播流 (支持手机浏览器、自研 APK、VLC 等即开即听)。"""
        try:
            virtual_delay = max(0, min(5000, int(request.query.get("virtual_delay", "0"))))
        except (TypeError, ValueError):
            virtual_delay = 0
        client_ip = request.remote or "127.0.0.1"
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
        delay_buffer = _PcmDelayBuffer(virtual_delay) if virtual_delay else None
        buffer_ready = virtual_delay == 0
        if "virtual_delay" in request.query:
            log.info(
                "[Audio] Web 虚拟音箱流已连接: ip=%s virtual_delay=%d",
                client_ip,
                virtual_delay,
            )
        try:
            # 写入标准 WAV 头部 (44.1k/16bit/2ch)
            await response.write(app_instance.audio_hub.build_wav_header())
            log.info("[Audio] Web 虚拟音箱流连接成功: ip=%s session_id=%s", client_ip, request.query.get("session_id", ""))

            loop = asyncio.get_running_loop()
            while True:
                # 非阻塞等待队列中的 PCM 音频数据
                try:
                    chunk = await loop.run_in_executor(None, queue_listener.get, True, 0.5)
                except queue.Empty:
                    continue
                if chunk is None:
                    break
                if delay_buffer is None:
                    await response.write(chunk)
                    continue
                output = delay_buffer.push(chunk)
                if not buffer_ready and delay_buffer.buffered_bytes >= delay_buffer.target_bytes:
                    buffer_ready = True
                    log.info(
                        "[Audio] Web 虚拟音箱缓冲就绪: ip=%s virtual_delay=%d buffered_bytes=%d",
                        client_ip,
                        virtual_delay,
                        delay_buffer.buffered_bytes,
                    )
                if output:
                    await response.write(output)
        except (ConnectionResetError, asyncio.CancelledError):
            log.info("[Audio] Web 虚拟音箱流断开: ip=%s session_id=%s", client_ip, request.query.get("session_id", ""))
        except Exception as exc:
            log.debug("[Audio] Web 虚拟音箱流异常: ip=%s error=%s", client_ip, exc)
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

        if not target.startswith("virtual_") or target in ("group_all", "group", "all"):
            ok = await app_instance.play_url_to_targets(target, stream_url)
            if not ok:
                app_instance.audio_hub.stop_source("api_ingest")
                if stream_server:
                    stream_server.stop_streaming()
                return web.json_response(
                    {"ok": False, "message": "小米音箱播放指令下发失败"},
                    status=502,
                )

        await broadcast_ws({
            "type": "control_command",
            "action": "play_url",
            "target": target,
            "url": f"/stream/live.wav?virtual_delay={config.virtual_delay}",
            "session_id": app_instance.audio_hub.begin_session(
                "api_ingest",
                target,
                stream_url=f"/stream/live.wav?virtual_delay={config.virtual_delay}",
            ),
        })
        log.info("[Audio] 外部 API 推流已连接，下发播放目标: %s", target)

        try:
            async for chunk, _ in request.content.iter_chunks():
                if chunk:
                    if stream_server:
                        stream_server.write_pcm(chunk)
                    if not stream_server or not stream_server.on_pcm_chunk:
                        app_instance.audio_hub.broadcast_pcm(chunk)
            return web.json_response({"ok": True, "message": "推流完成"})
        except Exception as exc:
            log.warning("[Audio] 外部 API 推流中断: %s", exc)
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
            log.info("[Audio] 外部 API 推流已结束并通知音箱停止: %s", target)

    async def handle_control_play(request: web.Request) -> web.Response:
        """通用音频播放与控制指令 RPC 接口 (兼容 /api/control 与 /api/v1/control/play)。"""
        data = await request.json()
        action = data.get("action", "play_url")
        target = data.get("target") or data.get("id") or "group_all"
        url = data.get("url", "")
        volume = data.get("volume")
        metadata = data.get("metadata") if isinstance(data.get("metadata"), dict) else None

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

        if action == "play_url":
            if not url:
                return web.json_response({"ok": False, "message": "Missing url"}, status=400)
            if target.startswith("virtual_") and target not in ("group_all", "group", "all"):
                await broadcast_ws({
                    "type": "control_command",
                    "action": action,
                    "target": target,
                    "url": url,
                    "volume": volume,
                })
                return web.json_response({"ok": True, "target": target})
            ok = await app_instance.play_url_to_targets(target, url)
            if ok:
                session_id = app_instance.audio_hub.begin_session(
                    "control", target, metadata, url
                ) if target in ("group_all", "group", "all") else app_instance.audio_hub.get_session().get("session_id", 0)
                await broadcast_ws({
                    "type": "control_command",
                    "action": action,
                    "target": target,
                    "url": url,
                    "volume": volume,
                    "session_id": session_id,
                    "metadata": metadata or {},
                })
            return web.json_response({"ok": ok})
        elif action in ("stop", "pause"):
            session_id = app_instance.audio_hub.get_session().get("session_id", 0)
            await broadcast_ws({
                "type": "control_command",
                "action": action,
                "target": target,
                "url": url,
                "volume": volume,
                "session_id": session_id,
            })
            if target.startswith("virtual_") and target not in ("group_all", "group", "all"):
                return web.json_response({"ok": True, "target": target})
            ok = await app_instance.stop_targets(target)
            if target in ("group_all", "group", "all"):
                app_instance.audio_hub.end_session()
            return web.json_response({"ok": ok})
        elif action == "set_volume":
            if volume is None:
                return web.json_response({"ok": False, "message": "Missing volume"}, status=400)
            await broadcast_ws({
                "type": "control_command",
                "action": action,
                "target": target,
                "url": url,
                "volume": volume,
            })
            if target.startswith("virtual_") and target not in ("group_all", "group", "all"):
                return web.json_response({"ok": True, "target": target})
            ok = await app_instance.set_volume_to_targets(target, int(volume))
            return web.json_response({"ok": ok})
        else:
            return web.json_response({"ok": False, "message": f"Unsupported action: {action}"}, status=400)

    async def handle_hub_status(request: web.Request) -> web.Response:
        """获取 Audio Hub 运行状态与监听者统计。"""
        return web.json_response(app_instance.audio_hub.get_status())

    async def handle_session_control(request: web.Request) -> web.Response:
        try:
            data = await request.json()
            session_id = int(data.get("session_id"))
        except (TypeError, ValueError, json.JSONDecodeError):
            return web.json_response({"ok": False, "message": "Invalid session_id"}, status=400)

        action = str(data.get("action") or "")
        position_ms = data.get("position_ms")
        if action == "seek":
            try:
                position_ms = max(0, int(position_ms))
            except (TypeError, ValueError):
                return web.json_response({"ok": False, "message": "Invalid position_ms"}, status=400)

        ok, reason = await app_instance.audio_hub.control_session(session_id, action, position_ms)
        if ok:
            return web.json_response({"ok": True})
        status = 400 if reason == "invalid_action" else 409
        return web.json_response({"ok": False, "message": reason}, status=status)

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
    web_app.router.add_get("/pod", handle_pod_page)
    web_app.router.add_get("/pod/manifest.webmanifest", handle_pod_manifest)
    web_app.router.add_get("/pod/sw.js", handle_pod_service_worker)
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
    web_app.router.add_post("/api/v1/session/control", handle_session_control)
    web_app.router.add_get("/api/v1/proxy", handle_proxy)

    static_dir = os.path.join(os.path.dirname(__file__), "static")
    if os.path.exists(static_dir):
        web_app.router.add_static("/static", static_dir)
    return web_app
