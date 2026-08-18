"""AirPlay mDNS 广播服务

使用 zeroconf 发布 AirPlay 服务，让 iOS 设备可以发现并连接。
注册 AirPlay 音频服务，让 iOS 将本设备识别为音频接收器。
"""

import logging
import socket
import threading
import time

from zeroconf import ServiceInfo, Zeroconf, IPVersion
from zeroconf._exceptions import ServiceNameAlreadyRegistered, NonUniqueNameException

from miplay.airplay.utils import resolve_advertise_ip

log = logging.getLogger("miplay")





class AirPlayMDNS:
    """AirPlay mDNS 广播器"""

    def __init__(self, hostname: str, device_name: str, device_id: str, rtsp_port: int, shared_zeroconf: Zeroconf | None = None):
        self.hostname = hostname
        self.device_name = device_name
        self.device_id = device_id
        self.rtsp_port = rtsp_port
        self.shared_zeroconf = shared_zeroconf
        self.zeroconf: Zeroconf | None = None
        self.airplay_info: ServiceInfo | None = None
        self.raop_info: ServiceInfo | None = None
        self._thread: threading.Thread | None = None
        self._running = False

    def start(self):
        """启动 mDNS 广播 - 在线程中运行 zeroconf"""
        self._running = True
        self._thread = threading.Thread(target=self._run_mdns, daemon=True)
        self._thread.start()

    def _run_mdns(self):
        """在独立线程中运行 mDNS"""
        try:
            # 获取本机 IP 地址
            ip = resolve_advertise_ip(self.hostname)
            ip_bytes = socket.inet_aton(ip)

            log.info(f"[AirPlay] mDNS 启动中，IP: {ip}:{self.rtsp_port}")

            # 使用共享的 zeroconf 或创建新的
            if self.shared_zeroconf:
                self.zeroconf = self.shared_zeroconf
                log.info("[AirPlay] 使用共享 Zeroconf 实例")
            else:
                self.zeroconf = Zeroconf(ip_version=IPVersion.All)
                log.info("[AirPlay] 创建新的 Zeroconf 实例")

            # 构建设备 ID (去掉冒号的 MAC 地址格式，用于 RAOP 服务名)
            device_id_clean = self.device_id.replace(":", "")

            # ===== AirPlay 服务 (_airplay._tcp) =====
            # 这是 AirPlay 主服务，iOS 首先会查找这个服务
            # AirPlay 1 features for audio & metadata (AirPort Express)
            # Ft09=AirPlayAudio, Ft14=MFiSoft, Ft15=MetaCovers, Ft16=MetaProgress, Ft17=MetaTxtDAAP
            # Ft18=PCM, Ft19=ALAC, Ft20=AAC, Ft22=Unencrypted, Ft23=RSA_Auth, Ft27=LegacyPairing
            features = (
                (1 << 9)
                | (1 << 14)
                | (1 << 15)
                | (1 << 16)
                | (1 << 17)
                | (1 << 18)
                | (1 << 19)
                | (1 << 20)
                | (1 << 22)
                | (1 << 23)
                | (1 << 27)
            )
            # iOS 期望 features 为 "0xLOW,0xHIGH" 格式（高低 32 位分开）
            features_lo = features & 0xFFFFFFFF
            features_hi = (features >> 32) & 0xFFFFFFFF
            if features_hi > 0:
                features_str = f"0x{features_lo:X},0x{features_hi:X}"
            else:
                features_str = f"0x{features_lo:X}"

            airplay_properties = {
                # _airplay._tcp 的 deviceid 必须使用带冒号的 MAC 地址格式
                b"deviceid": self.device_id.encode(),
                b"txtvers": b"1",
                b"features": features_str.encode(),
                b"flags": b"0x4",
                b"model": b"AirPort4,107",
                b"name": self.device_name.encode(),
                b"protovers": b"1.1",
                b"srcvers": b"105.1",
                b"pi": b"aa5cb8df-7f14-4249-901a-5e748ce57a93",
                b"pk": b"b077f0e1e2e4f5d6c7b8a90123456789abcdef0123456789abcdef0123456789",
                b"gcgl": b"0",
                b"gid": b"5dccfd20-b166-49cc-a593-6abd5f724ddb",
            }

            self.airplay_info = ServiceInfo(
                type_="_airplay._tcp.local.",
                name=f"{self.device_name}._airplay._tcp.local.",
                addresses=[ip_bytes],
                port=self.rtsp_port,
                properties=airplay_properties,
                server=f"{self.hostname}.local.",
            )

            # ===== RAOP 服务 (_raop._tcp) =====
            # 这是纯音频 AirPlay (AirPort Express) 的服务类型
            raop_properties = {
                b"txtvers": b"1",
                b"ch": b"2",
                b"cn": b"0,1,2,3",  # 支持 PCM, ALAC, AAC, AAC-ELD
                b"et": b"0,1",       # 加密类型: none, RSA
                b"sv": b"false",
                b"da": b"true",
                b"sr": b"44100",
                b"ss": b"16",
                b"vn": b"65537",
                b"tp": b"UDP",
                b"vs": b"105.1",
                b"am": b"AirPort4,107",
                b"sf": b"0x4",
                b"ft": features_str.encode(),  # RAOP 也需要 features
                b"md": b"0,1,2",
                b"pw": b"false",
                b"fn": self.device_name.encode(),
            }

            self.raop_info = ServiceInfo(
                type_="_raop._tcp.local.",
                name=f"{device_id_clean}@{self.device_name}._raop._tcp.local.",
                addresses=[ip_bytes],
                port=self.rtsp_port,
                properties=raop_properties,
                server=f"{self.hostname}.local.",
            )

            # 注册 RAOP 纯音频 AirPlay 服务
            registered = False
            for attempt in range(2):
                if not self._running:
                    return
                try:
                    self.zeroconf.register_service(self.raop_info, allow_name_change=True)
                    log.info(f"[AirPlay] RAOP 服务已注册: {device_id_clean}@{self.device_name}._raop._tcp.local.")
                    registered = True
                    break
                except Exception as e:
                    if attempt == 0:
                        time.sleep(0.5)
                    else:
                        log.warning(f"[AirPlay] RAOP 服务注册异常 ({e})，跳过重试")

            if not registered:
                return

            log.info(f"[AirPlay] mDNS 广播已启动")
            log.info(f"[AirPlay]   设备名称: {self.device_name}")
            log.info(f"[AirPlay]   设备 ID: {self.device_id}")
            log.info(f"[AirPlay]   RTSP 端口: {self.rtsp_port}")

            # 保持线程运行 (快速响应退出)
            while self._running:
                time.sleep(0.2)

        except Exception as e:
            log.error(f"[AirPlay] 启动 mDNS 失败: {e}")
            import traceback
            log.error(traceback.format_exc())

    def stop(self):
        """停止 mDNS 广播"""
        self._running = False
        if self.zeroconf and not self.shared_zeroconf:
            try:
                self.zeroconf.close()
            except Exception:
                pass
        elif self.zeroconf and self.shared_zeroconf:
            try:
                if self.raop_info and hasattr(self.zeroconf, "unregister_service"):
                    self.zeroconf.unregister_service(self.raop_info)
            except Exception:
                pass
            try:
                if self.airplay_info and hasattr(self.zeroconf, "unregister_service"):
                    self.zeroconf.unregister_service(self.airplay_info)
            except Exception:
                pass
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=0.2)

    def update_port(self, port: int):
        """更新 RTSP 端口（动态分配后调用）"""
        self.rtsp_port = port
