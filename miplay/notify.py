"""Notification push service supporting ServerChan 3 and Bark."""

from __future__ import annotations

import asyncio
import logging
from urllib.parse import urlparse

import aiohttp

log = logging.getLogger("miplay")


# <!-- Sync Warning: Docs sync required with docs/ntf.md -->

class Notifier:
    """Unified notification sender supporting ServerChan 3 and Bark via raw URL POST dispatch."""

    def __init__(self, channel: str, key: str):
        self.channel = (channel or "").strip().lower()
        self.key = (key or "").strip()

    async def send(self, title: str, content: str = "") -> bool:
        if not self.key:
            log.warning("[Notify] 推送 URL 为空")
            return False

        if self.channel == "serverchan":
            return await self._send_serverchan(title, content)
        elif self.channel == "bark":
            return await self._send_bark(title, content)
        else:
            # 默认兜底根据渠道下发
            return await self._send_serverchan(title, content)

    async def _send_serverchan(self, title: str, content: str) -> bool:
        url = self.key
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10)
            ) as session:
                async with session.post(url, data={"title": title, "desp": content}) as resp:
                    if resp.status == 200:
                        log.info("[Notify] ServerChan 推送成功: %s", title)
                        return True
                    log.warning("[Notify] ServerChan 推送失败 HTTP %s", resp.status)
                    return False
        except asyncio.CancelledError:
            return False
        except Exception as exc:
            log.warning("[Notify] ServerChan 推送异常: %s", exc)
            return False

    async def _send_bark(self, title: str, content: str) -> bool:
        raw_url = self.key.strip().rstrip("/")
        if not raw_url:
            return False

        # 从用户 URL 解析 scheme://host 和 device_key
        parsed = urlparse(raw_url)
        path_parts = [p for p in parsed.path.split("/") if p and p.lower() != "push"]
        device_key = path_parts[-1] if path_parts else ""
        if not device_key:
            log.warning("[Notify] Bark 无法从 URL 解析 device_key")
            return False

        # 官方 API V2: POST https://{host}/push + JSON Body 含 device_key
        push_url = f"{parsed.scheme}://{parsed.netloc}/push"
        payload = {"title": title, "body": content, "device_key": device_key}

        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10)
            ) as session:
                async with session.post(push_url, json=payload) as resp:
                    if resp.status == 200:
                        log.info("[Notify] Bark 推送成功: %s", title)
                        return True
                    log.warning("[Notify] Bark 推送失败 HTTP %s", resp.status)
                    return False
        except asyncio.CancelledError:
            return False
        except Exception as exc:
            log.warning("[Notify] Bark 推送异常: %s", exc)
            return False

    async def notify_token_expired(self) -> bool:
        return await self.send(
            title="[MiPlay] 登录过期",
            content="小米账号 Token 已过期，请重新登录获取。",
        )

    async def notify_login_failed(self, reason: str = "") -> bool:
        return await self.send(
            title="[MiPlay] 登录失败",
            content=f"请检查配置信息：\n{reason}",
        )


# Backward compatibility alias
ServerChanNotifier = Notifier
