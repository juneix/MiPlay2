# MiPlay 隔空妙播

MiPlay（隔空妙播）是小米音箱的隔空播放`桥接器`，专为苹果用户打造，支持独立 AirPlay 1 设备、首发🔥`MiPlay 全屋组播`、接入 `OwnTone` 等功能。
> 本项目参考并整合了 [MiAir](https://github.com/KiriChen-Wind/MiAir)、[miair-next](https://github.com/deerwan/miair-next)、[miservice-fork](https://pypi.org/project/miservice-fork/)、[AirPlay2-Receiver](https://github.com/openairplay/airplay2-receiver)、[XiaoMusic](https://github.com/hanxi/xiaomusic) 等项目的思路与部分实现，面向自用场景进行了大量重构。

![miplay-1.webp](./img/miplay-1.webp)

![miplay-2.webp](./img/miplay-2.webp)

## ✨ 功能特色

- 🚀 **桥接 AirPlay 1**：局域网直连播放
- 🔥 **全屋组播 Beta**，苹果系统级兼容`小米妙播·全屋播放`
  - 全屋组播 Beta 本质还是 AirPlay 1
  - 建议使用 5G WiFi 的音箱
- 🔥 **接入 OwnTone**：可跨协议实现`多房间播放`
  - OwnTone 支持 AirPlay 1&2、Chromecast、DLNA 等协议
- 📦 **双架构通用**：支持 x86、arm64 架构的 PC、Mac、Linux 设备

> MiPlay 可以实现小米无线音箱 ➡️ AirPlay1，虽然很便利，但普遍音质一般。若想要更好的音质表现，推荐传统有线音箱 ➡️ AirPlay 1&2（🔍 Shairport-Sync）

### 📊 音频方案与协议对比

| 对比维度 | MiPlay | AirPlay 1| AirPlay 2 | DLNA | 小米妙播 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **系统级音频投射** | ✅ 全家桶 | ✅ 全家桶 | ✅ 全家桶 | ❌ 部分 App |  ✅ 全家桶 |
| **多房间同步播放** | ✅ 组播 Beta | ☑️ 仅限 iTunes | ✅ 支持 | ❌ 不支持 | ✅ 支持 |
| **音频通信链路** | ☑️ 同步串流 | ☑️ 同步串流 | ✅ 独立协同 | ☑️ 分离遥控 | ✅ 独立协同 |
| **小米音箱兼容性** | ✅ 全系音箱 | ☑️ Sound 系列  | ☑️ Sound 系列 | ☑️ 部分音箱 | ☑️ 部分音箱 |
| **OwnTone兼容性** | ✅ 支持 | ✅ 支持  | ✅ 支持| ☑️ 部分音箱 | ❌ 不支持 |
| **硬件加密门槛** |  ✅ 无门槛 | ☑️ 苹果授权 | ☑️ 苹果授权 | ✅ 无门槛 | 🔒 小米独占 |

⚠️ 本项目主要是完善苹果用户的小米音箱 ✖️ AirPlay 体验，暂不考虑 DLNA 功能。
- DLNA 是一个古早的音频协议，虽然新老设备都能用，但整体体验不太好、稳定性欠佳
- 小米小爱音箱自带 DLNA 功能不完整，第三方 DLNA 需额外适配，体验依然不完美
- 如果需要第三方 DLNA 功能，推荐使用 MiAir、miair-next 等项目

## 🔄 工作流程图

```mermaid
flowchart TD
    subgraph Side ["☁️ 云服务"]
        direction LR
        Cloud["☁️ 小米云端 API<br/>(登录鉴权)"] --> Notify["📲 通知推送<br/> (Server酱 / Bark)"]
    end

    subgraph LAN ["🌐 局域网"]
        direction TD
        A["📱 iPhone / Mac 发射端<br/>(ALAC / AAC / PCM)"] --> B["📡 MiPlay 服务器 (AirPlay 桥接)<br/>• 无损直通: PCM / WAV<br/>• 动态转码: AAC / MP3"]
        B --> C{"播放模式"}
        
        C -->|单播| D["📢 独立 AirPlay 音箱"]
        C -->|组播| E
        
        subgraph E ["🏠 全屋播放"]
            direction LR
            E1["📢 音箱 1"] --- E2["📢 音箱 2"] --- E3["📢 音箱 N..."]
        end
    end

    Side -.- LAN
```

### 🔐 **小米账号鉴权说明**：
1. **登录方式区别？**
  - **米家 App 扫码登录 (推荐)**：  
    官方渠道获取长效凭证 `passToken`，支持长期无感自动续期。
  - **手动 Cookie 登录 (备用)**：  
    理论上与扫码登录效果相同，保留手动填写方式，作为后备登录方案。
2. **不支持账号密码登录？**   
此方式容易触发小米安全风控限制（如验证码拦截、异地设备封禁），导致登录成功率极低，故不提供。
3. **自动续期原理**   
基于系统存储的 `passToken`（保存于 `conf/config.json`），后台会自动静默换取小米音箱 API 所需的临时 `serviceToken` 令牌，无需手动干预。
4. **安全风险提示**  
⚠️ 小米的 `passToken` 请勿公开泄露，本项目纯内网个人使用，Web 控制台不设置密码访问限制。如需外网调试，推荐使用 `Tailscale` 或 `Zerotier` 更安全。


### 🎵 音频处理与格式支持

| 环节 | 格式 / 协议 | 说明 |
| :--- | :--- | :--- |
| **AirPlay 输入** | **ALAC (Apple Lossless)**, **AAC**, **PCM** | 44.1kHz / 16bit 音频接收 |
| **无损直通 (默认)** | **WAV / PCM 局域网 HTTP 流** | 零二次压缩开销，CPU 占用极低，无损高品质输出 |
| **动态转码 (兼容)** | **AAC / MP3** | 针对特定曲库 API 音箱动态封装，保证最高兼容性 |

### 🔊 音量说明

苹果（分贝）VS 小米音箱（百分比）对照参考表

| 苹果音量 | 滑块位置 | 小米音量 | 说明 |
| :--- | :--- | :--- | :--- |
| **`0.0 dB`** | 100%| **100** | 最大音量 |
| `-5.0 dB` | 约 85% | **83** | |
| `-10.0 dB` | 约 70% | **66** | |
| `-15.0 dB` | 约 50% | **50** | |
| **`-20.0 dB`** | **约 30%** | **33** | 默认安全音量 |
| `-25.0 dB` | 约 15% | **16** | 原版代码音量 |
| **`-30.0 dB`** | 约 1% | **0** | 最小音量 |
| `-144.0 dB` | 0% | **0** | 静音状态 |

## 🚀 部署方式

### 1、Docker Compose（推荐）

```
services:
  miplay:
    image: ghcr.io/juneix/miplay2
    # image: docker.1ms.run/juneix/miplay2  # 毫秒镜像加速
    container_name: miplay
    network_mode: host
    restart: unless-stopped
    environment:
      WEB_PORT: 8820 #访问端口
    volumes:
      - ./conf:/app/conf
# 如需搭配 Shairport-Sync 使用，请取消注释
#  shairport-sync:
#    image: mikebrady/shairport-sync
#    container_name: airplay2
#    network_mode: host
#    restart: always
#    devices:
#      - /dev/snd:/dev/snd
#    cap_add:
#      - SYS_NICE
```

### 2、Docker CLI

```bash
docker run -d \
  --name miplay \
  --network host \
  --restart unless-stopped \
  -e WEB_PORT=8820 \
  -v "${PWD}/conf:/app/conf" \
  ghcr.io/juneix/miplay2
  # docker.1ms.run/juneix/miplay2 # 毫秒镜像加速 
```

### 3、飞牛应用

飞牛商店的【AirPlay 2 - 隔空播放】已经整合 MiPlay。

![miplay-3.webp](./img/miplay-3.webp)

### 4、uv 直接运行

```bash
# 安装 uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# 克隆项目
git clone https://github.com/juneix/MiPlay2.git
cd MiPlay2

# 启动
uv run miplay.py
```

## ❤️ 支持项目

- 打赏鼓励：支持我开发更多有趣应用
- 互动群聊：加入 💬 [QQ 群](https://qm.qq.com/q/ZzOD5Qbhce) 可在线催更
- 更多内容：访问 ➡️ [谢週五の藏经阁](https://5nav.eu.org)

<div align="center">
  <table>
    <tr>
      <td align="center">
        <img src="./miplay/web/static/wechat.webp" width="128" /><br/>
        <sub>微信</sub>
      </td>
      <td align="center">
        <img src="./miplay/web/static/alipay.webp" width="128" /><br/>
        <sub>支付宝</sub>
      </td>
    </tr>
  </table>
</div>
