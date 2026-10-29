#!/usr/bin/env python3
"""统计 DeepSWE 任务镜像的基础环境（发行版 / libc / node / git / 工作目录 / 大小）。

用途
- 自定义 agent 要把宿主编好的 node 与 DSH 上传进容器离线运行，必须先确认镜像里是
  glibc 还是 musl（node 官方 linux-x64 构建只吃 glibc），以及 git 是否存在
  （verifier 用 git diff 导出 commit）。这份统计是排除规则的依据之一。

输入
- 任务目录（默认 SCRATCH/pier/deep-swe/tasks）
- 任务 id 列表（命令行参数）；不给则取 --count 个跨语言的样本

输出
- SCRATCH/pier/probes/image-survey.json：每个任务一行（镜像、大小、发行版、libc、node、
  git、工作目录、探针耗时、错误类别）

副作用
- 会 podman pull 镜像（每个约 2-3 GB，落在 rootless podman 的存储里）并临时起容器；
  不联网做任何业务请求。

异常
- 单个镜像失败只记 error 类别（pull_failed / probe_failed），不中断整轮统计。
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import tomllib
from pathlib import Path

REPO = Path(__file__).resolve().parents[5]
SCRATCH = REPO / ".scratch/research/chat-reasoning-replay/pier"

PROBE = r"""
cat /etc/os-release 2>/dev/null | sed -n 's/^PRETTY_NAME=//p'
if [ -e /lib/ld-musl-x86_64.so.1 ] || [ -e /lib/libc.musl-x86_64.so.1 ]; then echo "libc=musl";
elif [ -e /lib/x86_64-linux-gnu/libc.so.6 ]; then echo "libc=glibc";
else echo "libc=unknown"; fi
echo "node=$(command -v node >/dev/null 2>&1 && node -v || echo none)"
echo "git=$(command -v git >/dev/null 2>&1 && git --version | awk '{print $3}' || echo none)"
echo "python=$(command -v python3 >/dev/null 2>&1 && python3 -V 2>&1 | awk '{print $2}' || echo none)"
echo "cwd=$(pwd)"
echo "user=$(id -u):$(id -g)"
"""


def sh(args: list[str], timeout: int = 900) -> subprocess.CompletedProcess:
    return subprocess.run(args, capture_output=True, text=True, timeout=timeout)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task_ids", nargs="*")
    parser.add_argument("--src", type=Path, default=SCRATCH / "deep-swe/tasks")
    parser.add_argument("--out", type=Path, default=SCRATCH / "probes/image-survey.json")
    args = parser.parse_args()

    task_ids = args.task_ids
    if not task_ids:
        # 跨语言各取一个，保证覆盖 go / python / typescript / rust / javascript。
        wanted = {"go": 2, "python": 2, "typescript": 2, "rust": 2, "javascript": 2}
        for path in sorted(args.src.iterdir()):
            toml_path = path / "task.toml"
            if not toml_path.is_file():
                continue
            config = tomllib.loads(toml_path.read_text())
            language = (config.get("metadata") or {}).get("language")
            if wanted.get(language, 0) > 0:
                task_ids.append(path.name)
                wanted[language] -= 1

    records = []
    for task_id in task_ids:
        toml_path = args.src / task_id / "task.toml"
        if not toml_path.is_file():
            print(f"跳过 {task_id}：没有 task.toml", file=sys.stderr)
            continue
        config = tomllib.loads(toml_path.read_text())
        image = (config.get("environment") or {}).get("docker_image", "")
        record = {
            "task_id": task_id,
            "language": (config.get("metadata") or {}).get("language"),
            "image": image,
        }
        started = time.monotonic()
        pull = sh(["podman", "pull", "-q", image], timeout=3600)
        record["pull_seconds"] = round(time.monotonic() - started, 1)
        if pull.returncode != 0:
            record["error"] = "pull_failed"
            records.append(record)
            print(json.dumps(record, ensure_ascii=False), flush=True)
            continue
        inspect = sh(
            [
                "podman",
                "image",
                "inspect",
                image,
                "--format",
                "{{.Size}}|{{.Config.WorkingDir}}|{{.Config.User}}",
            ],
            timeout=120,
        )
        if inspect.returncode == 0:
            size, workdir, user = (inspect.stdout.strip().split("|") + ["", "", ""])[:3]
            record.update(size_bytes=int(size or 0), image_workdir=workdir, image_user=user)
        probe = sh(["podman", "run", "--rm", image, "sh", "-c", PROBE], timeout=600)
        if probe.returncode != 0:
            record["error"] = "probe_failed"
        else:
            for line in probe.stdout.splitlines():
                if "=" in line:
                    key, value = line.split("=", 1)
                    record[key] = value
                elif line.strip():
                    record["pretty_name"] = line.strip()
        records.append(record)
        print(json.dumps(record, ensure_ascii=False), flush=True)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(records, ensure_ascii=False, indent=2))
    print(f"写入 {args.out}（{len(records)} 条）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
