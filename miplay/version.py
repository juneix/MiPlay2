"""Version information and GitHub Release update checker."""

from __future__ import annotations

import asyncio
import logging
import re
import aiohttp

log = logging.getLogger("miplay")

__version__ = "v1.0"

# 3 个 GitHub Release API 检查渠道 (包含官方与代理加速)
CHECK_CHANNELS = [
    "https://api.github.com/repos/juneix/MiPlay2/releases/latest",
    "https://gh-proxy.org/https://api.github.com/repos/juneix/MiPlay2/releases/latest",
    "https://ghfast.top/https://api.github.com/repos/juneix/MiPlay2/releases/latest",
]


def _parse_version(v_str: str) -> tuple[int, ...]:
    """提取版本号数字元组进行比较，如 'v0.3.1' -> (0, 3, 1)。"""
    nums = re.findall(r"\d+", v_str)
    return tuple(int(n) for n in nums) if nums else (0, 0, 0)


async def fetch_latest_release() -> str | None:
    """尝试通过 3 个渠道异步获取 GitHub 最新 Release 的 tag_name。"""
    headers = {
        "User-Agent": "MiPlay-Update-Checker",
        "Accept": "application/vnd.github.v3+json",
    }
    timeout = aiohttp.ClientTimeout(total=8)
    
    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        for url in CHECK_CHANNELS:
            try:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        tag = data.get("tag_name", "").strip()
                        if tag:
                            log.info("[UpdateChecker] 通过渠道成功获取最新 Release 版本: %s", tag)
                            return tag
            except asyncio.CancelledError:
                return None
            except Exception as exc:
                log.debug("[UpdateChecker] 渠道 %s 获取失败: %s, 切换下一渠道", url, exc)
                continue

    log.warning("[UpdateChecker] 所有 3 个 GitHub Release 检测渠道均响应超时或失败")
    return None


async def check_for_updates(notifier=None):
    """检测最新 Release 版本，若本地版本落后则发起更新提醒推送。"""
    try:
        latest_tag = await fetch_latest_release()
        if not latest_tag:
            return

        latest_ver = _parse_version(latest_tag)
        current_ver = _parse_version(__version__)

        if latest_ver > current_ver:
            log.info("[UpdateChecker] 发现新版本 %s (当前本地版本 %s)", latest_tag, __version__)
            if notifier:
                await notifier.send(
                    title="[MiPlay] 新版本更新提醒",
                    content=f"发现新版本 {latest_tag} (当前本地版本 {__version__})，请及时更新！",
                )
        else:
            log.info("[UpdateChecker] 当前已是最新版本 (%s)", __version__)
    except asyncio.CancelledError:
        pass
    except Exception as exc:
        log.warning("[UpdateChecker] 版本检测异常: %s", exc)
