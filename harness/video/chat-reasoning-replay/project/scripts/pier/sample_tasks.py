#!/usr/bin/env python3
"""从 DeepSWE 任务池里按固定种子随机抽样，并冻结排除规则与结果。

用途
- 三路转发对比需要同一批任务；抽样必须可复现、可审计，不能按通过率挑任务。
- 只排除「明显跑不动」的任务（无 GPU、单任务内存超 8 GB、镜像拉不下来），
  不按难度、语言或历史通过率过滤。

输入
- 任务目录（默认 SCRATCH/pier/deep-swe/tasks）
- 镜像普查结果（默认 SCRATCH/pier/probes/image-survey.json，用于剔除 pull 失败）
- --seed（默认 0）、--n（默认 20）

输出
- SCRATCH/pier/sample.json：seed、n、规则、被排除项及原因、候选数、抽中的任务、
  语言分布、以及可直接拼给 pier 的 --n-tasks/--sample-seed 参数

副作用
- 只读任务目录与普查结果，写一个 JSON；不联网、不改任务。

异常
- 候选数少于 n 时退出码 2 并列出缺口，不静默缩量。
"""

from __future__ import annotations

import argparse
import collections
import datetime
import json
import random
import tomllib
from pathlib import Path

REPO = Path(__file__).resolve().parents[5]
SCRATCH = REPO / ".scratch/research/chat-reasoning-replay/pier"

# 冻结的排除规则：改规则必须同时改 RULES_VERSION，否则不同批次不可比。
RULES_VERSION = "1"
MAX_MEMORY_MB = 8192


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--src", type=Path, default=SCRATCH / "deep-swe/tasks")
    parser.add_argument("--survey", type=Path, default=SCRATCH / "probes/image-survey.json")
    parser.add_argument("--out", type=Path, default=SCRATCH / "sample.json")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--n", type=int, default=20)
    args = parser.parse_args()

    pull_failed = set()
    if args.survey.is_file():
        for record in json.loads(args.survey.read_text()):
            if record.get("error") == "pull_failed":
                pull_failed.add(record["task_id"])

    excluded: list[dict] = []
    candidates: list[dict] = []
    for path in sorted(args.src.iterdir()):
        toml_path = path / "task.toml"
        if not toml_path.is_file():
            continue
        try:
            config = tomllib.loads(toml_path.read_text())
        except tomllib.TOMLDecodeError:
            excluded.append({"task_id": path.name, "reason": "task_toml_unparsable"})
            continue
        env = config.get("environment") or {}
        memory_mb = env.get("memory_mb")
        gpus = env.get("gpus") or 0
        language = (config.get("metadata") or {}).get("language")
        if gpus:
            excluded.append({"task_id": path.name, "reason": f"gpus={gpus}"})
        elif isinstance(memory_mb, int) and memory_mb > MAX_MEMORY_MB:
            excluded.append({"task_id": path.name, "reason": f"memory_mb={memory_mb}"})
        elif path.name in pull_failed:
            excluded.append({"task_id": path.name, "reason": "image_pull_failed"})
        else:
            candidates.append(
                {"task_id": path.name, "language": language, "memory_mb": memory_mb, "gpus": gpus}
            )

    if len(candidates) < args.n:
        print(f"候选只有 {len(candidates)} 个，不足 n={args.n}")
        return 2

    rng = random.Random(args.seed)
    selected = rng.sample([item["task_id"] for item in candidates], args.n)
    selected.sort()
    languages = collections.Counter(
        item["language"] for item in candidates if item["task_id"] in set(selected)
    )

    payload = {
        "rules_version": RULES_VERSION,
        "generated_at": datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "seed": args.seed,
        "n": args.n,
        "rules": [
            "gpus == 0（本机无 GPU）",
            f"memory_mb <= {MAX_MEMORY_MB}（单任务 8 GB 上限）",
            "镜像 pull 失败（来自 probes/image-survey.json）",
            "task.toml 无法解析",
            "不按难度 / 语言 / 历史通过率过滤",
        ],
        "pool_size": len(candidates) + len(excluded),
        "candidate_count": len(candidates),
        "excluded": excluded,
        "selected": selected,
        "selected_languages": dict(languages),
        "pier_args": {"n_tasks": args.n, "sample_seed": args.seed},
    }
    args.out.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    print(json.dumps({k: payload[k] for k in ("seed", "n", "candidate_count", "selected_languages")}, ensure_ascii=False))
    print(f"写入 {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
