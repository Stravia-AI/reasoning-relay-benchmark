#!/usr/bin/env python3
"""宿主侧单目标转发：Unix socket → 一个固定的 TCP 目标（每次试验独占一条）。

用途
- agent 容器的 `network_mode: none` 里没有任何外部网络，唯一通向外部的路径是宿主
  用 bind mount 挂进容器的 `trial_dir/agent/` 目录。本脚本在该目录里监听一个
  Unix socket，把收到的每个连接原样转发到**一个**写死的 `host:port`（本试验所属
  路由的客户端入口端口）。容器侧再用 `upstream-relay.js` 把 `127.0.0.1:<port>`
  接到这个 socket 上，于是 DSH 只需要一个普通 HTTP baseURL。
- 目标写死在这里是刻意的：即使容器内的进程想做别的事，这条通道也只通向一个端口。
  不同试验各有自己的 socket 文件（各自 trial 目录），因此并发试验互相看不到对方的通道。

输入（命令行）
- `--dir`：Unix socket 所在目录（宿主 trial 的 agent 目录，必须已存在）。
- `--name`：socket 文件名（默认 `upstream.sock`）。用相对名 bind 是为了绕开
  `sun_path` 108 字节上限：Trial 目录的绝对路径远超该上限，而相对名只有十几个字节；
  容器侧用自己的短路径 `/logs/agent/<name>` 连接，解析到同一个 inode。
- `--target`：`host:port`，唯一允许的目标。
- `--ready-file`：监听就绪后写出的标记文件（默认 `<dir>/<name>.ready`），
  供调用方同步；调用方删除它即可。

副作用
- 只在 `--dir` 下创建 socket 与 ready 文件；不写其它位置，不发起除 `--target` 外的连接。

异常
- 目标不可达时直接关闭该连接（容器侧看到连接被拒/对端关闭），不影响其它连接；
  bind 失败（目录不存在、路径过长）以非零退出码终止，由调用方报错。
"""

from __future__ import annotations

import argparse
import os
import signal
import socket
import sys
import threading
import time

CHUNK = 65536


def pump(src: socket.socket, dst: socket.socket) -> None:
    """单向搬运；任一端 EOF 或出错就半关对端，让另一方向继续把剩余数据送完。"""
    try:
        while True:
            data = src.recv(CHUNK)
            if not data:
                break
            dst.sendall(data)
    except OSError:
        pass
    finally:
        try:
            dst.shutdown(socket.SHUT_WR)
        except OSError:
            pass


def serve_connection(conn: socket.socket, target: tuple[str, int]) -> None:
    try:
        upstream = socket.create_connection(target, timeout=30)
    except OSError:
        conn.close()
        return
    upstream.settimeout(None)
    back = threading.Thread(target=pump, args=(upstream, conn), daemon=True)
    back.start()
    pump(conn, upstream)
    back.join(timeout=300)
    conn.close()
    upstream.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dir", required=True)
    parser.add_argument("--name", default="upstream.sock")
    parser.add_argument("--target", required=True, help="host:port，唯一允许的目标")
    parser.add_argument("--ready-file", default="")
    parser.add_argument(
        "--parent-pid",
        type=int,
        default=0,
        help="父进程退出后自行结束（默认 0 = 不监视），避免 Pier 异常结束时残留进程",
    )
    args = parser.parse_args()

    host, _, port = args.target.rpartition(":")
    if not host or not port.isdigit():
        print(f"目标必须是 host:port：{args.target}", file=sys.stderr)
        return 2
    target = (host, int(port))

    directory = os.path.abspath(args.dir)
    ready_file = args.ready_file or os.path.join(directory, f"{args.name}.ready")
    for stale in (os.path.join(directory, args.name), ready_file):
        try:
            os.unlink(stale)
        except FileNotFoundError:
            pass

    # 相对名 bind：见模块 docstring 的 sun_path 说明。chdir 在 main 线程里发生，
    # 早于任何工作线程，之后不再改动 cwd。
    os.chdir(directory)
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(args.name)
    os.chmod(args.name, 0o600)
    listener.listen(16)

    with open(ready_file, "w", encoding="utf-8") as handle:
        handle.write(f"{os.getpid()}\n")

    def stop(_signum, _frame):
        listener.close()
        for path in (os.path.join(directory, args.name), ready_file):
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass
        os._exit(0)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    if args.parent_pid > 0:
        def watch_parent() -> None:
            while True:
                time.sleep(2.0)
                try:
                    os.kill(args.parent_pid, 0)
                except OSError:
                    stop(signal.SIGTERM, None)

        threading.Thread(target=watch_parent, daemon=True).start()

    print(f"relay ready: {directory}/{args.name} -> {host}:{port}", flush=True)
    while True:
        try:
            conn, _ = listener.accept()
        except OSError:
            break
        threading.Thread(target=serve_connection, args=(conn, target), daemon=True).start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
