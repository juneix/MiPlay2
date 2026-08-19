from __future__ import annotations

import json
import os
import re
import socket
import threading
import uuid
from collections import Counter
from dataclasses import asdict, dataclass, field


import sys

# 现代主流小米客户端 UA 模板 (Xiaomi 14 / HyperOS Android 14 / 米家 9.x+)
MIIO_UA = (
    "Android-14-1.0.0-Xiaomi 23127PN0CC-136-%s APP/xiaomi.smarthome APPV/90800"
)


def get_device_id(conf_path: str = "conf") -> str:
    """获取或初始化当前运行主机的固定设备指纹 (16位大写Hex)。
    不同主机相互独立，单机上持久化保存于 conf/.device_id 永不漂移。
    """
    if not os.path.isabs(conf_path):
        conf_path = os.path.abspath(conf_path)
    os.makedirs(conf_path, exist_ok=True)
    dev_file = os.path.join(conf_path, ".device_id")
    if os.path.exists(dev_file):
        try:
            with open(dev_file, "r", encoding="utf-8") as f:
                dev_id = f.read().strip().upper()
                if dev_id and len(dev_id) == 16:
                    return dev_id
        except Exception:
            pass

    # 基于本机物理 MAC 硬件特征生成确定性 16 位 Hex 设备 ID
    node_val = uuid.getnode()
    raw_hex = f"{node_val:012X}A8F1"[:16].upper()
    try:
        with open(dev_file, "w", encoding="utf-8") as f:
            f.write(raw_hex)
    except Exception:
        pass
    return raw_hex


def _is_invalid_lan_ip(ip: str) -> bool:
    if not ip or ip in ("127.0.0.1", "0.0.0.0", "localhost"):
        return True
    # 过滤 198.18.0.0/15 (TUN / Clash / Surge Fake IP 代理网段)
    if ip.startswith("198.18.") or ip.startswith("198.19."):
        return True
    # 过滤 169.254.0.0/16 (APIPA 自动私有地址)
    if ip.startswith("169.254."):
        return True
    # 过滤 Docker 常用桥接虚拟网段
    if ip.startswith("172.17.") or ip.startswith("172.18.") or ip.startswith("172.19.") or ip.startswith("172.20."):
        return True
    return False


def _get_ip_score(ip: str, ifname: str = "") -> int:
    """计算 IP 权重得分（得分高的优先）：
    100+ 分：物理有线以太网 (LAN/Ethernet，如 en0, eth0, eno1, enp*s*)
    80 分：公共 Socket 出口路由得出的有效局域网 IP
    50 分：无线 Wi-Fi (wlan0, wlp*s*, en1 等)
    0 分：非法/代理/虚拟网卡
    """
    if _is_invalid_lan_ip(ip):
        return 0
    score = 50
    ifname_lower = ifname.lower()
    is_wired = False
    if ifname_lower:
        if (
            ifname_lower == "en0"
            or ifname_lower.startswith("eth")
            or ifname_lower.startswith("eno")
            or ifname_lower.startswith("enp")
            or "ethernet" in ifname_lower
            or "lan" in ifname_lower
        ):
            is_wired = True

    if is_wired:
        score += 50

    if ip.startswith("192.168.") or ip.startswith("10."):
        score += 20

    return score


def _detect_local_ip() -> str:
    candidates: list[tuple[str, str]] = []

    # 1. macOS / Linux 解析 ifconfig 绑定的真实接口与 IP
    if sys.platform != "win32":
        try:
            import subprocess

            output = subprocess.check_output(["ifconfig"], text=True, errors="replace")
            current_if = ""
            for line in output.splitlines():
                if line and not line.startswith("\t") and not line.startswith(" "):
                    current_if = line.split(":")[0]
                elif "inet " in line:
                    parts = line.strip().split()
                    if len(parts) >= 2 and parts[0] == "inet":
                        found_ip = parts[1]
                        if not _is_invalid_lan_ip(found_ip):
                            candidates.append((found_ip, current_if))
        except Exception:
            pass

    # 2. UDP Socket 出口路由检测
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("223.5.5.5", 80))
        ip = sock.getsockname()[0]
        sock.close()
        if not _is_invalid_lan_ip(ip):
            candidates.append((ip, "socket_outbound"))
    except Exception:
        pass

    # 3. getaddrinfo 域名解析备选
    try:
        hostname = socket.gethostname()
        for item in socket.getaddrinfo(hostname, None, socket.AF_INET):
            ip = item[4][0]
            if not _is_invalid_lan_ip(ip):
                candidates.append((ip, ""))
    except Exception:
        pass

    if not candidates:
        return "127.0.0.1"

    # 全局打分排序：确保有线以太网 IP 处于最顶端
    scored = []
    seen = set()
    for ip, ifname in candidates:
        if ip in seen:
            continue
        seen.add(ip)
        score = _get_ip_score(ip, ifname)
        scored.append((score, ip, ifname))

    scored.sort(key=lambda x: x[0], reverse=True)
    best_score, best_ip, best_if = scored[0]
    return best_ip


def normalize_service_name(name: str) -> str:
    return " ".join(name.strip().split())


def detect_name_conflicts(targets: list["TargetConfig"]) -> list[str]:
    enabled_targets = [target for target in targets if target.enabled]
    counts = Counter(
        normalize_service_name(target.airplay_name).casefold()
        for target in enabled_targets
        if normalize_service_name(target.airplay_name)
    )
    duplicates = {name for name, count in counts.items() if count > 1}
    if duplicates:
        return [f"存在重复的 AirPlay 广播名称: '{name}'" for name in duplicates]
    return []


@dataclass
class XiaomiConfig:
    cookie: str = ""

    def __post_init__(self):
        if not self.cookie:
            self.cookie = os.getenv("XIAOMI_COOKIE", "")


@dataclass
class NotifyConfig:
    channel: str = ""
    key: str = ""

    def __post_init__(self):
        if not self.key:
            self.key = os.getenv("NOTIFY_KEY", os.getenv("SERVERCHAN_KEY", "")).strip()
        if not self.channel and self.key:
            if self.key.startswith("sctp"):
                self.channel = "serverchan"
            else:
                self.channel = "bark"


@dataclass
class TargetConfig:
    id: str = ""
    did: str = ""
    name: str = ""
    airplay_name: str = ""
    enabled: bool = True
    device_id: str = ""
    hardware: str = ""
    use_music_api: bool = False
    default_audio_id: str = ""

    def __post_init__(self):
        self.id = self.id or self.did or str(uuid.uuid4())
        self.name = self.name.strip()
        self.airplay_name = self.airplay_name.strip()
        self.default_audio_id = (self.default_audio_id or "").strip()
        self.ensure_names()

    def ensure_names(self):
        if not self.name:
            self.name = self.hardware or (f"小爱音箱-{self.did}" if self.did else "小爱音箱")
        if not self.airplay_name:
            self.airplay_name = self.name


@dataclass
class GroupConfig:
    """全屋播放配置（默认自动开启，无需显式启用开关）"""
    airplay_name: str = "MiPlay 全屋播放"
    member_dids: list[str] = field(default_factory=list)

    def __post_init__(self):
        if not self.airplay_name:
            self.airplay_name = "MiPlay 全屋播放"
        self.airplay_name = self.airplay_name.strip()
        if self.member_dids is None:
            self.member_dids = []


@dataclass
class Config:
    host: str = ""
    web_port: int = 8820
    verbose: bool = False
    virtual_delay: int = 2000  # 全屋虚拟音箱对齐延时毫秒 (范围: 0~5000ms, 默认: 2000ms)
    default_audio_id: str = ""  # 全局触屏默认 AudioID (留空使用内置 DEFAULT_AUDIO_ID)
    xiaomi: XiaomiConfig = field(default_factory=XiaomiConfig)
    notify: NotifyConfig = field(default_factory=NotifyConfig)
    group: GroupConfig = field(default_factory=GroupConfig)
    targets: list[TargetConfig] = field(default_factory=list)
    conf_path: str = "conf"

    _save_lock = threading.Lock()

    def __post_init__(self):
        # 限制参数安全范围，防止用户异常配置
        try:
            self.virtual_delay = max(0, min(5000, int(self.virtual_delay)))
        except (ValueError, TypeError):
            self.virtual_delay = 2000

        if isinstance(self.xiaomi, dict):
            self.xiaomi = XiaomiConfig(**self.xiaomi)
        if isinstance(self.notify, dict):
            self.notify = NotifyConfig(**self.notify)
        if isinstance(self.group, dict):
            group_dict = {k: v for k, v in self.group.items() if k in ("airplay_name", "member_dids")}
            self.group = GroupConfig(**group_dict)
        
        # 动态检测 host：如果 host 为空或无效 (TUN/Docker/Loopback)，强制实时探测
        if not self.host or _is_invalid_lan_ip(self.host):
            self.host = os.getenv("MIPLAY_HOST", "").strip()
        if not self.host or _is_invalid_lan_ip(self.host):
            self.host = _detect_local_ip()

        env_web_port = os.getenv("WEB_PORT")
        if env_web_port:
            try:
                self.web_port = int(env_web_port)
            except ValueError:
                pass
        self.targets = [self._coerce_target(item) for item in self.targets]
        if self.targets and not self.group.member_dids:
            self.group.member_dids = [t.did for t in self.targets if t.enabled and t.did]

    @property
    def config_file(self) -> str:
        return os.path.join(self.conf_path, "config.json")

    def _coerce_target(self, item: TargetConfig | dict) -> TargetConfig:
        if isinstance(item, TargetConfig):
            item.ensure_names()
            return item
        target = TargetConfig(**item)
        target.ensure_names()
        return target

    def get_enabled_targets(self) -> list[TargetConfig]:
        return [target for target in self.targets if target.enabled]

    def get_target(self, target_id: str) -> TargetConfig | None:
        for target in self.targets:
            if target.id == target_id:
                return target
        return None

    def get_target_by_did(self, did: str) -> TargetConfig | None:
        for target in self.targets:
            if target.did == did:
                return target
        return None

    def set_targets(self, targets: list[TargetConfig | dict]):
        self.targets = [self._coerce_target(item) for item in targets]

    def save(self):
        with self._save_lock:
            os.makedirs(self.conf_path, exist_ok=True)
            data = asdict(self)
            # host 属于纯动态局域网网络属性，绝对不能在 config.json 磁盘文件中持久化保存
            data.pop("host", None)
            with open(self.config_file, "w", encoding="utf-8") as file:
                json.dump(data, file, ensure_ascii=False, indent=2)

    @classmethod
    def load(cls, conf_path: str = "conf") -> "Config":
        if not os.path.isabs(conf_path):
            conf_path = os.path.abspath(conf_path)
        config_file = os.path.join(conf_path, "config.json")
        if not os.path.exists(config_file):
            config = cls(conf_path=conf_path)
            config.save()
            return config

        with open(config_file, "r", encoding="utf-8") as file:
            raw_text = file.read()
        # 剥离 // 与 /* */ 注释 (保留 JSON 字符串内部的 URL 双斜杠)，原生支持 JSONC 语法
        clean_text = re.sub(
            r'("(?:\\.|[^"\\])*")|//.*?$|/\*.*?\*/',
            lambda m: m.group(1) if m.group(1) else "",
            raw_text,
            flags=re.MULTILINE | re.DOTALL,
        )
        raw = json.loads(clean_text)
        # 擦除磁盘历史文件中可能残留存盘的旧 host 坏值与废弃字段
        raw.pop("host", None)
        raw.pop("legacy", None)
        raw.pop("db_range", None)
        raw["conf_path"] = conf_path
        config = cls(**raw)
        return config
