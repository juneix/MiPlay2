"""Logging formatters, filters, and tag extraction utilities for MiPlay."""

from __future__ import annotations

import asyncio
import logging
import time


class RateLimitFilter(logging.Filter):
    """Filter to prevent log spam by rate limiting identical warning/error messages."""

    def __init__(self, interval_sec: float = 5.0):
        super().__init__()
        self.interval_sec = interval_sec
        self.last_seen: dict[str, float] = {}

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno < logging.WARNING:
            return True
        msg = record.getMessage()
        now = asyncio.get_event_loop().time() if asyncio._get_running_loop() else time.time()
        last = self.last_seen.get(msg, 0.0)
        if now - last < self.interval_sec:
            return False
        self.last_seen[msg] = now
        return True


def extract_tag_and_message(record: logging.LogRecord) -> tuple[str, str]:
    """Extract tag and clean message from record based on domain boundaries."""
    msg = record.getMessage()

    # 1. 显式前缀匹配
    if msg.startswith("[Notify]"):
        return "[Notify]", msg[8:].lstrip()
    elif msg.startswith("[AirPlay]"):
        return "[AirPlay]", msg[9:].lstrip()
    elif msg.startswith("[Audio]"):
        return "[Audio]", msg[7:].lstrip()
    elif msg.startswith("[System]"):
        return "[System]", msg[8:].lstrip()
    elif msg.startswith("[Xiaomi]"):
        return "[System]", msg

    lower_msg = msg.lower()

    # 2. 【音频播放相关】判定 (歌名/元数据, 封面, 音量, 音频编码格式, 音箱推流/播放/暂停/停止)
    audio_keywords = (
        "歌曲", "元数据", "封面", "音量", "音频流", "pcm", "wav", "mp3",
        "play_url", "set_volume", "pause target=", "stop target=", "play target=",
        "stream_url", "解码", "采样"
    )
    if any(k in lower_msg for k in audio_keywords):
        return "[Audio]", msg

    # 3. 【AirPlay 通信协议】判定 (mDNS, RAOP, RTSP, 握手, 客户端连接/关闭)
    airplay_keywords = (
        "mdns", "raop", "rtsp", "cseq", "setup", "announce", "teardown",
        "apple-challenge", "apple-response", "客户端连接", "客户端关闭",
        "服务已注册", "服务已注销", "zeroconf"
    )
    if any(k in lower_msg for k in airplay_keywords):
        return "[AirPlay]", msg

    # 4. 【通知推送】判定 (Bark, ServerChan, 推送)
    if any(k in lower_msg for k in ("推送", "bark", "serverchan")):
        return "[Notify]", msg

    # 5. 【基础环节与云服务】判定 (Xiaomi 登录/Cookie/设备列表、配置、Web API、应用启动)
    return "[System]", msg


class ColoredFormatter(logging.Formatter):
    """Terminal formatter with ANSI color coding for tags and error levels."""

    TAG_COLORS = {
        "[System]": "\033[35m",   # Magenta (Purple)
        "[AirPlay]": "\033[36m",  # Cyan
        "[Audio]": "\033[32m",    # Green
        "[Notify]": "\033[33m",   # Yellow
    }
    RESET = "\033[0m"
    RED = "\033[31m"

    def format(self, record: logging.LogRecord) -> str:
        tag, clean_msg = extract_tag_and_message(record)
        tag_color = self.TAG_COLORS.get(tag, self.TAG_COLORS["[System]"])
        colored_tag = f"{tag_color}{tag:<8}{self.RESET}"
        time_str = f"[{self.formatTime(record, self.datefmt)}]"

        if record.levelno >= logging.ERROR:
            level_str = f"{self.RED}[{record.levelname:<7}]{self.RESET}"
            msg_str = f"{self.RED}{clean_msg}{self.RESET}"
        else:
            level_str = f"[{record.levelname:<7}]"
            msg_str = clean_msg

        return f"{time_str} {level_str} {colored_tag} {msg_str}"


class PlainTextFormatter(logging.Formatter):
    """Clean plain-text formatter for log files without ANSI codes."""

    def format(self, record: logging.LogRecord) -> str:
        tag, clean_msg = extract_tag_and_message(record)
        level_str = f"[{record.levelname:<7}]"
        time_str = f"[{self.formatTime(record, self.datefmt)}]"
        return f"{time_str} {level_str} {tag:<8} {clean_msg}"
