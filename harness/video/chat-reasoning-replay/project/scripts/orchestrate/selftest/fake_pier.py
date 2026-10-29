#!/usr/bin/env python3
"""Pier 替身：给 scripts/orchestrate/selftest.py 用，按场景伪造 trial 目录与 outcome.json。

用途
- `run_pier.sh` 用 `PIER=<本文件>` 指向这里，于是自测走的是「真 run_pier.sh + 真
  prepare_pilot_task.py」，但完全不起容器、不联网、不产生任何模型请求。
- 场景由 FAKE_PIER_SCENARIOS 指向的 JSON 决定：键为 "<route>" 或 "<route>:<attempt>"，
  未命中依次退回 "<task>" / "default"。场景字段：kind、reward、sleep、pier_exit、usage。
- 路线/尝试号从编排器的 plan.json（FAKE_PIER_PLAN）按 job-name 反查，找不到时按 job-name
  的 `<route>-a<attempt>` 后缀兜底。

副作用
- 只在 `-o` 指定的 jobs 目录下写 trial 目录；把收到的环境（密钥只留 sha256 前 12 位）写到
  agent/fake-env.json 供断言。不写任何凭据明文。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path


# 终态错误场景：kind → outcome.json 里的 terminal_error（与适配器契约同形）。
# concurrency 没有真实 HTTP 响应，status 必须是 null，不能拿 429 冒充。
TERMINAL_KINDS = {
    "infra429": {"kind": "http", "status": 429},
    "infra503": {"kind": "http", "status": 503},
    "http400": {"kind": "http", "status": 400},
    "conn": {"kind": "connection", "status": None},
    "concurrency": {"kind": "concurrency", "status": None},
    "policy": {"kind": "other", "status": None},
    "invalid-prompt": {"kind": "other", "status": 400},
}


def record_lifecycle(event: str, job_name: str, route: str, attempt: int) -> None:
    """可选公共事件账本：每次进程尝试的起止，不记录环境或凭据。"""
    ledger = os.environ.get("FAKE_PIER_LEDGER")
    if not ledger:
        return
    row = {
        "event": event,
        "time_ns": time.monotonic_ns(),
        "job_name": job_name,
        "route": route,
        "attempt": attempt,
    }
    fd = os.open(ledger, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.write(fd, (json.dumps(row) + "\n").encode("utf-8"))
    finally:
        os.close(fd)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("cmd", nargs="?")
    parser.add_argument("-p", "--path")
    parser.add_argument("-o", "--jobs-dir")
    parser.add_argument("--job-name")
    parser.add_argument("--ae", action="append", default=[])
    parser.add_argument("-y", action="store_true")
    parser.add_argument("--max-retries")
    parser.add_argument("--agent-import-path")
    known, _ = parser.parse_known_args(argv)
    return known


def lookup_plan(job_name: str) -> tuple[str, int] | None:
    plan_path = os.environ.get("FAKE_PIER_PLAN")
    if not plan_path or not Path(plan_path).is_file():
        return None
    plan = json.loads(Path(plan_path).read_text(encoding="utf-8"))
    for group in plan.get("groups", []):
        for trial in group.get("trials", []):
            if job_name.startswith(trial["job_name_prefix"] + "-a"):
                attempt = int(job_name.rsplit("-a", 1)[1])
                return trial["route"], attempt
    return None


def guess_route(job_name: str) -> tuple[str, int] | None:
    match = re.search(r"--(?P<route>[A-Za-z0-9_]+)-a(?P<attempt>\d+)$", job_name)
    if not match:
        return None
    return match.group("route"), int(match.group("attempt"))


def write_trial(
    trial_dir: Path,
    task: str,
    scenario: dict,
    started: datetime,
    finished: datetime,
    env: dict,
    job_name: str,
    route: str,
    attempt: int,
    extra_argv: list[str],
) -> None:
    kind = scenario.get("kind", "pass")
    (trial_dir / "agent").mkdir(parents=True, exist_ok=True)
    (trial_dir / "verifier").mkdir(parents=True, exist_ok=True)

    reward = scenario.get("reward")
    if reward is None:
        reward = 1.0 if kind == "pass" else 0.0
    usage = scenario.get(
        "usage", {"input": 1000, "cached": 100, "output": 50, "reasoning": 20}
    )
    terminal = None
    exit_code = 0
    timed_out = False
    wall_seconds = round((finished - started).total_seconds(), 3)
    if kind in TERMINAL_KINDS:
        exit_code = 1
        terminal = TERMINAL_KINDS[kind]
    elif kind == "timeout":
        timed_out = True
        exit_code = None
        wall_seconds = float(scenario.get("wall_seconds", 3600.0))

    result = {
        "task_name": f"datacurve/{task}",
        "trial_name": trial_dir.name,
        "started_at": started.isoformat(timespec="microseconds"),
        "finished_at": finished.isoformat(timespec="microseconds"),
        "verifier_result": None if kind in ("missing-reward", "trial-exception") else {"rewards": {"reward": reward}},
        "exception_info": (
            {"exception_type": "VerifierTimeoutError", "exception_message": "fake", "exception_traceback": ""}
            if kind == "trial-exception"
            else None
        ),
    }
    (trial_dir / "result.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    if kind not in ("missing-reward", "trial-exception"):
        (trial_dir / "verifier/reward.json").write_text(
            json.dumps({"reward": reward, "partial": reward}, indent=2), encoding="utf-8"
        )
    if kind != "no-outcome":
        outcome = {
            "exit_code": exit_code,
            "timed_out": timed_out,
            "wall_seconds": wall_seconds,
            "terminal_error": terminal,
            "llm_calls": 2,
            "usage": {
                "input": usage.get("input"),
                "cached": usage.get("cached"),
                "output": usage.get("output"),
                "reasoning": usage.get("reasoning"),
            },
        }
        (trial_dir / "agent/outcome.json").write_text(
            json.dumps(outcome, indent=2), encoding="utf-8"
        )

    api_key = env.get("BENCH_API_KEY") or ""
    fake_env = {
        "base_url": env.get("PIER_DSH_BASE_URL"),
        "model": env.get("PIER_DSH_MODEL"),
        "reasoning": env.get("PIER_DSH_REASONING"),
        "wall_seconds": env.get("PIER_DSH_WALL_SECONDS"),
        "api_key_sha256_prefix": hashlib.sha256(api_key.encode()).hexdigest()[:12] if api_key else None,
        "api_key_present": bool(api_key),
        "ae": extra_argv,
        "argv": sys.argv[1:],
        "job_name": job_name,
        "task": task,
        "route": route,
        "attempt": attempt,
        "kind": kind,
    }
    (trial_dir / "agent/fake-env.json").write_text(json.dumps(fake_env, indent=2), encoding="utf-8")


def main() -> int:
    args = parse_args(sys.argv[1:])
    if args.cmd != "run":
        print(f"fake pier 只支持 run，收到 {args.cmd!r}", file=sys.stderr)
        return 2
    if not args.jobs_dir or not args.job_name or not args.path:
        print("fake pier 需要 -p/-o/--job-name", file=sys.stderr)
        return 2
    task = Path(args.path).name
    located = lookup_plan(args.job_name) or guess_route(args.job_name)
    if located is None:
        print(f"fake pier 无法从 {args.job_name} 判断路线", file=sys.stderr)
        return 2
    route, attempt = located

    scenarios_path = os.environ.get("FAKE_PIER_SCENARIOS")
    scenarios = (
        json.loads(Path(scenarios_path).read_text(encoding="utf-8")) if scenarios_path else {}
    )
    scenario = (
        scenarios.get(f"{task}|{route}:{attempt}")
        or scenarios.get(f"{task}|{route}")
        or scenarios.get(f"{route}:{attempt}")
        or scenarios.get(f"{route}")
        or scenarios.get(task)
        or scenarios.get("default")
        or {"kind": "pass"}
    )

    record_lifecycle("start", args.job_name, route, attempt)
    barrier = os.environ.get("FAKE_PIER_BARRIER")
    if barrier:
        barrier_dir = Path(barrier)
        if not (barrier_dir / "released").exists():
            with (barrier_dir / "arrivals").open("wb", buffering=0) as arrivals:
                arrivals.write(b"1")
            with (barrier_dir / "release").open("rb", buffering=0) as release:
                release.read(1)
    started = datetime.now()
    sleep_seconds = float(scenario.get("sleep", 0.15))
    if sleep_seconds > 0:
        time.sleep(sleep_seconds)
    finished = datetime.now()

    job_dir = Path(args.jobs_dir) / args.job_name
    trial_dir = job_dir / f"{task}__fake{attempt}"
    write_trial(
        trial_dir,
        task,
        scenario,
        started,
        finished,
        dict(os.environ),
        args.job_name,
        route,
        attempt,
        args.ae,
    )
    print(f"fake-pier {task} {route} a{attempt} -> {scenario.get('kind', 'pass')}", flush=True)
    record_lifecycle("finish", args.job_name, route, attempt)
    return int(scenario.get("pier_exit", 0))


if __name__ == "__main__":
    raise SystemExit(main())
