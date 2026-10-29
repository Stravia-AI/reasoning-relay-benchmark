#!/usr/bin/env python3
"""把 DeepSWE 任务复制成 pilot 副本，只改 agent 超时（不改 deep-swe clone）。

用途
- DeepSWE 原始 task.toml 是 `[agent] network_mode = "no-network"`，Pier 会追加
  `network_mode: none`——这正是我们要的：容器没有任何外部接口、没有 DNS，出网一律不通。
  agent 到宿主路由的唯一通道是 Pier 默认挂进容器的 `trial_dir/agent` 目录里那条
  试验专属 Unix socket（见 `dsh_agent.py` 与 docs/pier-adapter.md §3），因此这里
  **不需要、也不允许**再改网络模式。
- 只把 `[agent] timeout_sec` 改成 1 小时墙钟 + 余量，让 Pier 一侧的兜底与 DSH 内部的
  `timeout` 对齐；verifier 的 `[verifier] network_mode = "no-network"` 保持原样。

输入
- 源任务目录（默认 SCRATCH/pier/deep-swe/tasks/<id>）
- 输出目录（默认 SCRATCH/pier/pilot-tasks/<id>）
- agent 超时 PIER_AGENT_TIMEOUT_SEC（默认 3900：1 小时墙钟 + 5 分钟余量）

副作用
- 只写输出目录；不动 deep-swe clone 与原任务。
- 打印改动摘要（不涉及任何凭据）。

异常
- 任务不存在或 task.toml 缺 `[agent]` 段时退出码 1，不写半成品。
- 任务自带 `environment/docker-compose.yaml` 时只告警：那种任务无法保证 `network_mode: none`
  语义（Pier 会以任务自己的 compose 为准），清单里记 `network_isolation: false` 由调用方决定。
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[5]
SCRATCH = REPO / ".scratch/research/chat-reasoning-replay/pier"


def set_table_key(text: str, table: str, key: str, rendered: str) -> str:
    """在指定 TOML 表里设置一个标量键，保留文件其余内容与顺序。"""
    lines = text.splitlines(keepends=True)
    header = re.compile(rf"^\s*\[{re.escape(table)}\]\s*$")
    start = next((i for i, line in enumerate(lines) if header.match(line)), None)
    if start is None:
        raise SystemExit(f"task.toml 缺少 [{table}] 段")
    end = len(lines)
    for i in range(start + 1, len(lines)):
        if re.match(r"^\s*\[", lines[i]):
            end = i
            break
    key_re = re.compile(rf"^\s*{re.escape(key)}\s*=")
    for i in range(start + 1, end):
        if key_re.match(lines[i]):
            indent = re.match(r"^(\s*)", lines[i]).group(1)
            lines[i] = f"{indent}{key} = {rendered}\n"
            return "".join(lines)
    lines.insert(start + 1, f"{key} = {rendered}\n")
    return "".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task_ids", nargs="+")
    parser.add_argument("--src", type=Path, default=SCRATCH / "deep-swe/tasks")
    parser.add_argument("--out", type=Path, default=SCRATCH / "pilot-tasks")
    parser.add_argument(
        "--agent-timeout",
        type=int,
        default=int(os.environ.get("PIER_AGENT_TIMEOUT_SEC", "3900")),
    )
    args = parser.parse_args()

    prepared = []
    for task_id in args.task_ids:
        src = args.src / task_id
        if not (src / "task.toml").is_file():
            print(f"跳过：{task_id} 没有 task.toml", file=sys.stderr)
            return 1
        dst = args.out / task_id
        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(src, dst)

        toml_path = dst / "task.toml"
        text = toml_path.read_text()
        text = set_table_key(text, "agent", "network_mode", '"no-network"')
        text = set_table_key(text, "agent", "timeout_sec", str(args.agent_timeout))
        toml_path.write_text(text)

        own_compose = dst / "environment/docker-compose.yaml"
        isolated = not own_compose.exists()
        if not isolated:
            print(
                f"告警：{task_id} 自带 environment/docker-compose.yaml，"
                "Pier 会以任务自己的 compose 为准，network_mode: none 不再有保证",
                file=sys.stderr,
            )

        prepared.append(
            {
                "task_id": task_id,
                "path": str(dst),
                "agent_network_mode": "no-network",
                "container_network_mode": "none" if isolated else "task-compose",
                "network_isolation": isolated,
                "agent_timeout_sec": args.agent_timeout,
            }
        )
        print(json.dumps(prepared[-1], ensure_ascii=False))

    (args.out / "pilot-manifest.json").write_text(
        json.dumps(prepared, ensure_ascii=False, indent=2)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
