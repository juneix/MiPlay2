"""CLI entrypoint for MiPlay."""

from __future__ import annotations

import argparse
import asyncio
import signal
import sys

from miplay.app import MiPlay
from miplay.config import Config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MiPlay - AirPlay bridge for Xiaomi speakers")
    subparsers = parser.add_subparsers(dest="command")

    serve = subparsers.add_parser("serve", help="Start the MiPlay runtime")
    serve.add_argument("--conf-path", default="conf", help="Configuration directory")
    serve.add_argument("--host", default="", help="Advertised LAN IP or hostname")
    serve.add_argument("--web-port", type=int, default=0, help="Web UI port")
    serve.add_argument("--verbose", action="store_true", help="Enable debug logging")
    serve.add_argument("--dev", action="store_true", help="Enable AirPlay 2 Pipe Bridge dev mode")
    serve.add_argument("--pipe-path", default="/tmp/shairport/audio.fifo", help="Custom FIFO pipe path for Shairport-Sync")

    search = subparsers.add_parser("search", help="Search Xiaomi music library for songs and audioIDs")
    search.add_argument("query", help="Song or artist keyword to search")
    search.add_argument("--conf-path", default="conf", help="Configuration directory")

    return parser.parse_args()


async def run_search(conf_path: str, query: str):
    """从小米曲库搜索歌曲信息、封面与 audioID。"""
    from miplay.xiaomi import AuthManager
    config = Config.load(conf_path)
    auth = AuthManager(config)
    try:
        await auth.ensure_login()
        if not auth.mina_service:
            print("[错误] 未能成功登录小米账号，请先在 conf/config.json 填入 Cookie 或执行扫码登录。")
            return
        res = await auth.mina_service.mina_request(
            "/music/search",
            {"query": query, "queryType": "1", "offset": "0", "count": "8"},
        )
        song_list = (res or {}).get("data", {}).get("songList") or []
        if not song_list:
            print(f"未找到与 '{query}' 匹配的歌曲。")
            return

        print(f"\n🎵 找到 {len(song_list)} 条小米曲库搜索结果 (关键字: '{query}'):\n")
        print(f"{'序号':<4} {'歌名':<22} {'歌手':<15} {'AudioID (用于触屏)':<22} {'封面预览链接'}")
        print("-" * 110)
        for i, song in enumerate(song_list, 1):
            name = (song.get("name") or "未知")[:20]
            artist = ((song.get("artist") or {}).get("name") or "未知")[:14]
            audio_id = str(song.get("audioID") or "")
            cover = song.get("coverURL") or ""
            print(f"[{i:<2}] {name:<22} {artist:<15} {audio_id:<22} {cover}")
        print("\n💡 提示: 可将心仪歌曲的 AudioID 配置到系统作为触屏默认封面。\n")
    finally:
        await auth.close()


def main():
    # 如果没有提供子命令，默认使用 'serve'
    if len(sys.argv) == 1:
        sys.argv.append("serve")
    elif len(sys.argv) > 1 and sys.argv[1] not in ["serve", "search", "-h", "--help"]:
        sys.argv.insert(1, "serve")

    args = parse_args()
    command = args.command or "serve"
    if command == "search":
        asyncio.run(run_search(args.conf_path, args.query))
        return
    elif command != "serve":
        raise SystemExit(f"Unsupported command: {command}")

    import os
    if args.dev:
        os.environ["MIPLAY_AIRPLAY2_DEV"] = "1"
    if args.pipe_path:
        os.environ["MIPLAY_PIPE_PATH"] = args.pipe_path

    config = Config.load(args.conf_path)
    if args.host:
        config.host = args.host
    if args.web_port:
        config.web_port = args.web_port
    if args.verbose:
        config.verbose = True
    config.save()

    app = MiPlay(config)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    main_task = loop.create_task(app.run_forever())

    def shutdown():
        import os
        # 立即退出，防止 zeroconf 注销死锁导致卡死
        os._exit(0)

    if sys.platform != "win32":
        loop.add_signal_handler(signal.SIGINT, shutdown)
        loop.add_signal_handler(signal.SIGTERM, shutdown)

    try:
        loop.run_until_complete(main_task)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        if not loop.is_closed():
            try:
                loop.run_until_complete(asyncio.wait_for(app.stop(), timeout=1.5))
            except Exception:
                pass
            finally:
                pending = asyncio.all_tasks(loop)
                for task in pending:
                    task.cancel()
                if pending:
                    try:
                        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                    except Exception:
                        pass
                loop.close()


if __name__ == "__main__":
    main()

