import re
import socket
import ipaddress
import logging
import logging.config
import platform
import subprocess


def resolve_advertise_ip(hostname: str) -> str:
    """优先使用配置中的局域网 IP，避免误用 tun/虚拟网卡地址。"""
    try:
        ipaddress.ip_address(hostname)
        if hostname not in {"0.0.0.0", "127.0.0.1"}:
            return hostname
    except ValueError:
        pass

    try:
        resolved = socket.gethostbyname(hostname)
        if resolved not in {"0.0.0.0", "127.0.0.1"}:
            return resolved
    except OSError:
        pass

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("223.5.5.5", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


# AirPlay 子系统日志配置
# 允许所有日志向上冒泡传递至 Root Logger 统一处理，
# 确保控制台（彩色）、miplay.log 文件与前端日志弹窗 100% 同步。
logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'loggers': {
        'ap2.playfair': {'level': 'INFO', 'propagate': True},
        'Audio.debug': {'level': 'DEBUG', 'propagate': True},
        'Audio': {'level': 'INFO', 'propagate': True},
        'AudioBuffered': {'level': 'DEBUG', 'propagate': True},
        'AudioRealtime': {'level': 'DEBUG', 'propagate': True},
        'Control': {'level': 'INFO', 'propagate': True},
        'events': {'level': 'INFO', 'propagate': True},
        'HAP': {'level': 'INFO', 'propagate': True},
        'Receiver': {'level': 'INFO', 'propagate': True},
        'RTPBuffer': {'level': 'DEBUG', 'propagate': True},
    }
})


def get_free_socket(addr=None, tcp=False):
    v4 = True
    stype = socket.SOCK_STREAM if tcp else socket.SOCK_DGRAM
    free_socket = None

    if addr:
        if len(addr.split(".")) == 4:
            free_socket = socket.socket(socket.AF_INET, stype)
        else:
            free_socket = socket.socket(socket.AF_INET6, stype)
            v4 = False
        free_socket.bind((addr, 0))
    else:
        if v4:
            free_socket = socket.socket(socket.AF_INET, stype)
            free_socket.bind(('0.0.0.0', 0))
        else:
            free_socket = socket.socket(socket.AF_INET6, stype)
            free_socket.bind(('::', 0))
    if tcp:
        free_socket.listen(5)
    return free_socket
