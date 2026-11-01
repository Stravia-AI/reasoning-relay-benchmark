#!/usr/bin/env python3
"""批次编排器：把「同一任务 × 同一轮次」的所选路线试验当成一组调度。

用途
- 读冻结任务清单（默认 SCRATCH/pier/sample.json 的 selected 顺序），按 组=（task, round）
  展开；组顺序、组内所选路线的槽位与启动顺序由种子随机决定，先落盘 plan.json 再执行。
- 每个试验：prepare_pilot_task.py 生成任务副本 → run_pier.sh 跑 Pier → 读
  agent/outcome.json 与 verifier/reward.json，按设计判定 pass / fail / infra-error /
  harness-error。
- 模型侧基础设施错误（连接错误、429、5xx）在全新容器里整次重跑，最多 3 次；任何一路三次
  仍失败则该组所有路线一并作废（void）。Pier/容器/镜像等自身异常归 harness-error，同样最多
  3 次，与模型侧分开统计。
- 结果 append-only 落 results.jsonl，生命周期 append-only 落 state.jsonl；批次结束写
  summary.json 与 runs.json（后者可直接喂 scripts/meter/summarize.py --runs）。

不做什么
- 不发真实推理：真实模式必须显式 --confirm-real-inference；密钥只从私有目录读该路线的
  client.json，只经子进程环境传递，不进 argv、不进日志、不进任何落盘文件。
- 不改任务清单、不挑结果、不删 trial 目录。

约定
- 「每条结果」以 results.jsonl 中该 (task, round, route) 的最后一条为准（latest wins）：
  作废组会给所选路线各补一条 classification=void 的记录，因此无需回写旧记录。
- 计时：attempt 窗口由编排器测（pier 进程起止），container 窗口取 Pier trial 的
  started_at/finished_at；runs.json 优先用 container 窗口，供 meterd 按窗口归因。

已知限制见 docs/orchestrator.md。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import random
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[5]
SCRIPTS = REPO / "video/chat-reasoning-replay/project/scripts"
SCRATCH = REPO / ".scratch/research/chat-reasoning-replay/pier"

PREPARE = SCRIPTS / "pier/prepare_pilot_task.py"
RUN_PIER = SCRIPTS / "pier/run_pier.sh"
DEFAULT_PRIVATE = Path.home() / ".local/state/chat-reasoning-replay/private"

MODEL = "gpt-6-luna"
REASONING = "high"
MAX_ATTEMPTS = 3
MOCK_API_KEY = "mock-zero-inference"

# 默认三路入口端口；显式 --route NAME:PORT 只运行指定路线。
ROUTE_DEFAULTS = (("stravia", 23481), ("cpa", 23482), ("sub2api", 23483))

SCHEMA_PLAN = "orchestrate-plan/v1"
SCHEMA_RESULT = "orchestrate-result/v1"
SCHEMA_SUMMARY = "orchestrate-summary/v1"

SCORED = ("pass", "fail")
RETRYABLE = ("infra-error", "harness-error")
TERMINAL_TRIAL = SCORED + ("void",)

ZERO_TOKENS = {
    "input": 0,
    "cached": 0,
    "output": 0,
    "reasoning": 0,
    "llm_calls": 0,
}


# --------------------------------------------------------------------- 小工具
def now_epoch() -> float:
    return time.time()


def epoch_iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts).astimezone().isoformat(timespec="seconds")


def iso_epoch(text: str | None) -> float | None:
    """Pier 的 started_at/finished_at 是本地时间的无时区 ISO 串。"""
    if not text:
        return None
    try:
        return datetime.fromisoformat(text).timestamp()
    except ValueError:
        return None


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def atomic_write_json(path: Path, payload: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def read_jsonl(path: Path) -> list[dict]:
    if not path.is_file():
        return []
    rows = []
    with path.open("r", encoding="utf-8") as handle:
        for number, line in enumerate(handle, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                # 半行只可能来自掉电/被 kill；保留可读部分继续，不让整批结果不可用。
                print(f"[warn] {path.name}:{number} 不是合法 JSON 行，已跳过", file=sys.stderr)
    return rows


def read_json(path: Path) -> dict | None:
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    return payload if isinstance(payload, dict) else None


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    pos = (len(ordered) - 1) * q
    low = math.floor(pos)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (pos - low)


def wall_stats(values: list[float]) -> dict:
    values = [v for v in values if v is not None]
    if not values:
        return {"count": 0, "sum": None, "median": None, "p90": None, "max": None}
    return {
        "count": len(values),
        "sum": round(sum(values), 3),
        "median": round(percentile(values, 0.5) or 0.0, 3),
        "p90": round(percentile(values, 0.9) or 0.0, 3),
        "max": round(max(values), 3),
    }


def parse_routes(values: list[str] | None) -> list[tuple[str, int]]:
    if not values:
        return list(ROUTE_DEFAULTS)
    routes = []
    for item in values:
        name, sep, port = item.partition(":")
        if not sep or not name or not port.isdigit():
            raise SystemExit(f"--route 需要 NAME:PORT，收到 {item!r}")
        routes.append((name, int(port)))
    if len({name for name, _ in routes}) != len(routes):
        raise SystemExit("--route 里有重复的路线名")
    return routes


def parse_rerun_specs(specs: list[str] | None) -> list[tuple[str, str, int]]:
    """解析 --rerun-trial GROUP_ID:ROUTE:ORIGINAL_ATTEMPT；重复项只保留一次。"""
    parsed: list[tuple[str, str, int]] = []
    for item in specs or []:
        parts = item.rsplit(":", 2)
        if len(parts) != 3 or not parts[0] or not parts[1]:
            raise SystemExit(f"--rerun-trial 需要 GROUP_ID:ROUTE:ORIGINAL_ATTEMPT，收到 {item!r}")
        group_id, route, attempt_text = parts
        if not attempt_text.isdigit() or int(attempt_text) < 1:
            raise SystemExit(f"--rerun-trial 的原尝试号必须是正整数，收到 {item!r}")
        spec = (group_id, route, int(attempt_text))
        if spec not in parsed:
            parsed.append(spec)
    return parsed


def load_task_list(path: Path) -> tuple[list[str], str]:
    raw = path.read_bytes()
    payload = json.loads(raw.decode("utf-8"))
    selected = payload.get("selected")
    if not isinstance(selected, list) or not selected:
        raise SystemExit(f"{path} 没有 selected 数组")
    tasks = [str(item) for item in selected]
    if len(set(tasks)) != len(tasks):
        raise SystemExit(f"{path} 的 selected 有重复任务")
    return tasks, sha256_bytes(raw)


def prepare_supports_route(prepare: Path) -> bool:
    """P2 的 prepare_pilot_task.py 还在演进；--route 出现与否用 --help 探测一次。

    探测而不是写死：真实批次要按 P2 的最终 CLI 调用，缺参数时不能把整个批次卡死。
    """
    try:
        proc = subprocess.run(
            [sys.executable, str(prepare), "--help"],
            capture_output=True,
            text=True,
            timeout=60,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return "--route" in (proc.stdout + proc.stderr)


def build_plan(args: argparse.Namespace, tasks: list[str], tasks_sha: str) -> dict:
    routes = args.routes

    rounds = args.rounds
    if args.mode == "pilot":
        # pilot：只跑冻结清单里的第一个任务的第一轮。
        tasks = tasks[:1]
        rounds = 1

    group_keys = [(task, round_no) for task in tasks for round_no in range(1, rounds + 1)]
    rng = random.Random(args.seed)
    # 组顺序与组内槽位都用同一个种子流，保证「同一 seed + 同一清单 → 同一计划」。
    rng.shuffle(group_keys)

    def base_url(port: int) -> str:
        if args.mock_base_url:
            return args.mock_base_url.rstrip("/")
        return f"http://{args.upstream_host}:{port}/v1"

    groups = []
    for task, round_no in group_keys:
        order = list(routes)
        rng.shuffle(order)
        trials = [
            {
                "route": name,
                "slot": slot,
                "port": port,
                "base_url": base_url(port),
                "job_name_prefix": f"{args.batch_id}--{task}--r{round_no}--{name}",
            }
            for slot, (name, port) in enumerate(order)
        ]
        groups.append(
            {
                "group_id": f"{task}@r{round_no}",
                "task": task,
                "round": round_no,
                "trials": trials,
            }
        )

    signature_payload = {
        "tasks": tasks,
        "tasks_sha256": tasks_sha,
        "rounds": rounds,
        "mode": args.mode,
        "seed": args.seed,
        "routes": [{"name": name, "port": port} for name, port in routes],
        "mock_base_url": args.mock_base_url,
        "upstream_host": args.upstream_host,
        "agent_wall_seconds": args.agent_wall_seconds,
        "trial_wall_seconds": args.trial_wall_seconds,
    }
    if args.reasoning != REASONING:
        signature_payload["reasoning"] = args.reasoning
    signature = sha256_bytes(
        json.dumps(signature_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    )

    return {
        "schema": SCHEMA_PLAN,
        "batch_id": args.batch_id,
        "created_at": epoch_iso(now_epoch()),
        "mode": args.mode,
        "seed": args.seed,
        "tasks_source": str(args.tasks),
        "tasks_sha256": tasks_sha,
        "tasks": tasks,
        "rounds": rounds,
        "routes": [
            {"name": name, "port": port, "base_url": base_url(port)} for name, port in routes
        ],
        "mock_base_url": args.mock_base_url,
        "upstream_host": args.upstream_host,
        "model": MODEL,
        "reasoning": args.reasoning,
        "agent_wall_seconds": args.agent_wall_seconds,
        "trial_wall_seconds": args.trial_wall_seconds,
        "agent_timeout_sec": args.agent_wall_seconds + 300,
        "max_attempts": MAX_ATTEMPTS,
        "prepare_route_flag": args.prepare_route_flag,
        "max_groups": args.max_groups,
        "signature": signature,
        "n_groups": len(groups),
        "n_trials": len(groups) * len(routes),
        "groups": groups,
    }


def print_plan(plan: dict, args: argparse.Namespace) -> None:
    routes = ", ".join(f"{r['name']}:{r['port']}→{r['base_url']}" for r in plan["routes"])
    n_routes = len(plan["routes"])
    key_source = "mock（不读私有目录）" if plan["mock_base_url"] else str(args.private_dir)
    print(f"批次 {plan['batch_id']}  mode={plan['mode']}  seed={plan['seed']}")
    print(f"任务 {len(plan['tasks'])} 个 × {plan['rounds']} 轮 = {plan['n_groups']} 组 × {n_routes} 路 = {plan['n_trials']} 次试验")
    print(f"最大并发组 {plan['max_groups']}（≤ {plan['max_groups'] * n_routes} 个 Pier 进程）")
    print(f"路线 {routes}")
    print(f"密钥来源 {key_source}")
    print(
        f"墙钟：agent {plan['agent_wall_seconds']}s，单次试验上限 {plan['trial_wall_seconds']}s，"
        f"最多尝试 {plan['max_attempts']} 次"
    )
    print(f"prepare --route：{'有' if plan['prepare_route_flag'] else '无（跳过该参数）'}")
    print("")
    for index, group in enumerate(plan["groups"], 1):
        slots = "  ".join(
            f"slot{t['slot']}={t['route']}({t['port']})" for t in group["trials"]
        )
        print(f"#{index:<3} {group['group_id']:<52} {slots}")
    print("")
    print(f"计划文件：{args.out / 'plan.json'}" + ("（dry-run 不写盘）" if args.dry_run else ""))
    print(f"结果文件：{args.out / 'results.jsonl'}  状态：{args.out / 'state.jsonl'}")


# ------------------------------------------------------------------- 批次状态
def classify(
    pier_rc: int | None,
    trial_result: dict | None,
    outcome: dict | None,
    reward: float | None,
    outer_timeout: bool,
    interrupted: bool,
    spawn_error: str | None,
) -> tuple[str, str, int | None]:
    """按设计判定一次尝试。返回 (classification, reason, status_code)。

    先排除编排器与可重试基础设施错误，再以 verifier 满分判通过。
    agent 超时或异常退出仍保留在 outcome 中，不覆盖已经验证通过的工作区。
    """
    if interrupted:
        return "interrupted", "interrupted:signal", None
    if spawn_error:
        return "harness-error", spawn_error, None
    if outer_timeout:
        return "harness-error", "orchestrator-wall", None
    if trial_result is None:
        return "harness-error", "missing-trial-dir", None
    exception = trial_result.get("exception_info")
    if exception:
        return "harness-error", f"trial-exception:{exception.get('exception_type')}", None
    if pier_rc != 0:
        return "harness-error", f"pier-exit-{pier_rc}", None
    if outcome is None:
        return "harness-error", "missing-outcome", None
    if reward is None:
        return "harness-error", "missing-reward", None

    terminal = outcome.get("terminal_error")
    if terminal:
        kind = terminal.get("kind")
        status = terminal.get("status")
        if kind == "connection":
            return "infra-error", "infra:connection", None
        if kind == "concurrency":
            # 适配器在并发上限处终止请求；没有真实 HTTP 响应，状态码原样保留（通常为 null），
            # 不拿 429 冒充。与连接错误一样按模型侧基础设施错误重跑。
            return "infra-error", "infra:concurrency", status if isinstance(status, int) else None
        if kind == "http" and isinstance(status, int) and (status == 429 or status >= 500):
            return "infra-error", f"infra:http:{status}", status
    # verifier 在 agent 停止后检查最终工作区；进程未正常结束不等于补丁未通过测试。
    if reward >= 1.0:
        return "pass", "reward", None
    if terminal:
        # 未通过验证时，401/400 等仍按任务失败记录，状态码保留供排查。
        return "fail", f"agent-error:{kind}:{status}", status
    if outcome.get("timed_out"):
        return "fail", "timed_out", None
    exit_code = outcome.get("exit_code")
    if exit_code not in (None, 0):
        return "fail", f"agent-exit-{exit_code}", None
    return "fail", "reward", None


class Batch:
    """一次批次运行的可变状态。线程之间只经 lock 保护的结构与 append-only 文件交互。"""

    def __init__(self, args: argparse.Namespace, plan: dict):
        self.args = args
        self.plan = plan
        self.out = Path(args.out)
        self.results_path = self.out / "results.jsonl"
        self.state_path = self.out / "state.jsonl"
        self.jobs_dir = self.out / "jobs"
        self.logs_dir = self.out / "logs"
        self.tasks_dir = self.out / "tasks"
        for path in (self.out, self.jobs_dir, self.logs_dir, self.tasks_dir):
            path.mkdir(parents=True, exist_ok=True)

        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.records: list[dict] = read_jsonl(self.results_path)
        self.state_events: list[dict] = read_jsonl(self.state_path)
        self.latest: dict[tuple[str, int, str], dict] = {}
        self.attempt_counts: dict[tuple[str, int, str], int] = {}
        self.active_procs: dict[int, subprocess.Popen] = {}
        self.group_void: dict[str, threading.Event] = {}
        self.key_leak_events = 0
        self._index_records()
        self.api_keys = self._resolve_api_keys()

    # ------------------------------------------------------------ 索引与落盘
    @staticmethod
    def key_of(record: dict) -> tuple[str, int, str]:
        return (record["task"], int(record["round"]), record["route"])

    def _index_records(self) -> None:
        """latest wins：作废组补的 void 记录天然覆盖同一试验的旧结果。"""
        for record in self.records:
            key = self.key_of(record)
            self.latest[key] = record
            if record.get("classification") != "void":
                self.attempt_counts[key] = self.attempt_counts.get(key, 0) + 1

    def _append_line(self, path: Path, payload: dict) -> None:
        line = json.dumps(payload, ensure_ascii=False) + "\n"
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line)
            handle.flush()
            os.fsync(handle.fileno())

    def append_result(self, record: dict) -> None:
        with self.lock:
            self._append_line(self.results_path, record)
            self.records.append(record)
            key = self.key_of(record)
            self.latest[key] = record
            if record.get("classification") != "void":
                self.attempt_counts[key] = self.attempt_counts.get(key, 0) + 1

    def append_state(self, event: dict) -> None:
        event = {"ts": now_epoch(), "ts_iso": epoch_iso(now_epoch()), **event}
        with self.lock:
            self._append_line(self.state_path, event)
            self.state_events.append(event)

    # ------------------------------------------------------------------ 凭据
    def _resolve_api_keys(self) -> dict[str, str]:
        """mock 模式用假 value：绝不读私有目录里已有的真实凭据。"""
        if self.plan["mock_base_url"]:
            return {route["name"]: MOCK_API_KEY for route in self.plan["routes"]}
        private = Path(self.args.private_dir)
        values = {}
        for route in self.plan["routes"]:
            path = private / "run" / route["name"] / "client.json"
            payload = read_json(path)
            value = (payload or {}).get("api_key")
            if not isinstance(value, str) or not value:
                raise SystemExit(
                    f"路线 {route['name']} 缺少客户端密钥：{path}（只读该文件的 api_key 字段）"
                )
            values[route["name"]] = value
        return values

    # -------------------------------------------------------------- 中断恢复
    def recover_interrupted(self) -> int:
        """把「有 trial-start、没有 trial-end / 结果」的尝试补成 interrupted 记录。

        设计语义：已经开始但未结束的试验视为未完成、重跑并计入重试次数，因此这条记录占用
        一次尝试名额，后续尝试编号从它之后继续。
        """
        started: dict[tuple[str, int, str, int], float] = {}
        ended: set[tuple[str, int, str, int]] = set()
        for event in self.state_events:
            if event.get("event") == "trial-start":
                key = (event["task"], int(event["round"]), event["route"], int(event["attempt"]))
                started.setdefault(key, float(event.get("ts") or now_epoch()))
            elif event.get("event") == "trial-end":
                ended.add(
                    (event["task"], int(event["round"]), event["route"], int(event["attempt"]))
                )
        recorded = {
            (r["task"], int(r["round"]), r["route"], int(r["attempt"]))
            for r in self.records
            if r.get("classification") != "void"
        }
        recovered = 0
        for (task, round_no, route, attempt), ts in sorted(started.items()):
            if (task, round_no, route, attempt) in ended:
                continue
            if (task, round_no, route, attempt) in recorded:
                continue
            group_id = next(
                (g["group_id"] for g in self.plan["groups"] if g["task"] == task and g["round"] == round_no),
                f"{task}@r{round_no}",
            )
            slot = next(
                (
                    t["slot"]
                    for g in self.plan["groups"]
                    if g["group_id"] == group_id
                    for t in g["trials"]
                    if t["route"] == route
                ),
                0,
            )
            finished = now_epoch()
            job_name = next(
                (
                    event["job_name"]
                    for event in self.state_events
                    if event.get("event") == "trial-start"
                    and event["task"] == task
                    and int(event["round"]) == round_no
                    and event["route"] == route
                    and int(event["attempt"]) == attempt
                ),
                None,
            )
            record = {
                "schema": SCHEMA_RESULT,
                "batch_id": self.plan["batch_id"],
                "group_id": group_id,
                "task": task,
                "round": round_no,
                "route": route,
                "slot": slot,
                "attempt": attempt,
                "job_name": job_name,
                "trial_dir": None,
                "classification": "interrupted",
                "reason": "interrupted:resume",
                "reward": None,
                "partial": None,
                "tokens": dict(ZERO_TOKENS),
                "agent_wall_seconds": None,
                "attempt_wall_seconds": round(finished - ts, 3),
                "trial_wall_seconds": None,
                "started_at": ts,
                "finished_at": finished,
                "started_at_iso": epoch_iso(ts),
                "finished_at_iso": epoch_iso(finished),
                "container_started_at": None,
                "container_finished_at": None,
                "exit_code": None,
                "timed_out": False,
                "terminal_error": None,
                "status_code": None,
                "pier_exit_code": None,
                "outcome_present": False,
                "reward_present": False,
                "mock": bool(self.plan["mock_base_url"]),
                "orchestrator_timeout": False,
                "interrupted": True,
                "recovered": True,
            }
            self.append_result(record)
            self.append_state(
                {
                    "event": "trial-end",
                    "batch_id": self.plan["batch_id"],
                    "group_id": group_id,
                    "task": task,
                    "round": round_no,
                    "route": route,
                    "attempt": attempt,
                    "classification": "interrupted",
                    "reason": "interrupted:resume",
                    "recovered": True,
                }
            )
            recovered += 1
        return recovered

    # ------------------------------------------------------------ 单次试验
    def _prepare_task(self, group: dict, trial: dict, task_out: Path, task_copy: Path) -> int:
        """任务副本按 (组, 路线) 只准备一次；同一路线多次尝试复用同一份副本。"""
        if (task_copy / "task.toml").is_file():
            return 0
        cmd = [
            sys.executable,
            str(PREPARE),
            group["task"],
            "--out",
            str(task_out),
            "--agent-timeout",
            str(self.plan["agent_timeout_sec"]),
        ]
        if self.plan["prepare_route_flag"]:
            cmd += ["--route", trial["route"]]
        log_path = self.logs_dir / f"{trial['job_name_prefix']}-prepare.log"
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        except (OSError, subprocess.SubprocessError) as exc:
            log_path.write_text(f"prepare 失败：{type(exc).__name__}\n", encoding="utf-8")
            return 1
        log_path.write_text(proc.stdout + proc.stderr, encoding="utf-8")
        return proc.returncode

    def _terminate(self, proc: subprocess.Popen) -> None:
        """按进程组杀：run_pier.sh 是 exec，但 --wrap-devenv 时会多一层壳。"""
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except ProcessLookupError:
            return
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass

    def _run_pier(self, trial: dict, job_name: str, task_copy: Path) -> dict:
        cmd = [
            str(RUN_PIER),
            "-p",
            str(task_copy),
            "--agent-import-path",
            "dsh_agent:DshAgent",
            "-o",
            str(self.jobs_dir),
            "--job-name",
            job_name,
            "-y",
            "--max-retries",
            "0",
            # Pier 默认的 compose down --rmi all 会删除并发 trial 共享的官方镜像。
            "--no-delete",
        ]
        if self.args.wrap_devenv:
            # 裸 shell 里没有 docker（devenv.nix 才提供包装）；由调用方显式选择。
            cmd = ["devenv", "shell", "--"] + cmd
        env = os.environ.copy()
        env["PIER_DSH_BASE_URL"] = trial["base_url"]
        env["PIER_DSH_MODEL"] = MODEL
        env["PIER_DSH_REASONING"] = self.plan["reasoning"]
        env["PIER_DSH_WALL_SECONDS"] = str(self.plan["agent_wall_seconds"])
        # 密钥只经子进程环境传递；run_pier.sh 用 ${BENCH_API_KEY} 模板补 --ae，不进 argv。
        env["BENCH_API_KEY"] = self.api_keys[trial["route"]]

        log_path = self.logs_dir / f"{job_name}.log"
        deadline = now_epoch() + self.plan["trial_wall_seconds"]
        with log_path.open("w", encoding="utf-8") as log:
            try:
                proc = subprocess.Popen(
                    cmd,
                    env=env,
                    cwd=str(REPO),
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                    text=True,
                )
            except OSError as exc:
                return {
                    "pier_exit_code": None,
                    "outer_timeout": False,
                    "interrupted": False,
                    "spawn_error": f"spawn-failed:{type(exc).__name__}",
                }
            with self.lock:
                self.active_procs[proc.pid] = proc
            rc: int | None = None
            outer_timeout = False
            interrupted = False
            try:
                while True:
                    rc = proc.poll()
                    if rc is not None:
                        break
                    if self.stop.is_set():
                        interrupted = True
                        self._terminate(proc)
                        rc = proc.wait()
                        break
                    if now_epoch() > deadline:
                        outer_timeout = True
                        self._terminate(proc)
                        rc = proc.wait()
                        break
                    time.sleep(0.5)
            finally:
                with self.lock:
                    self.active_procs.pop(proc.pid, None)
        return {
            "pier_exit_code": rc,
            "outer_timeout": outer_timeout,
            "interrupted": interrupted,
            "spawn_error": None,
        }

    def _find_trial_dir(self, job_name: str) -> Path | None:
        job_dir = self.jobs_dir / job_name
        if not job_dir.is_dir():
            return None
        candidates = [
            path for path in job_dir.iterdir() if path.is_dir() and (path / "result.json").is_file()
        ]
        if candidates:
            return max(candidates, key=lambda path: path.stat().st_mtime)
        # 没有 result.json 也返回目录：里面可能有 outcome.json，便于事后排查。
        dirs = [path for path in job_dir.iterdir() if path.is_dir()]
        return max(dirs, key=lambda path: path.stat().st_mtime) if dirs else None

    def execute_attempt(self, group: dict, trial: dict, attempt: int) -> dict:
        started_at = now_epoch()
        job_name = f"{trial['job_name_prefix']}-a{attempt}"
        task_out = self.tasks_dir / group["group_id"] / trial["route"]
        task_copy = task_out / group["task"]
        record: dict = {
            "schema": SCHEMA_RESULT,
            "batch_id": self.plan["batch_id"],
            "group_id": group["group_id"],
            "task": group["task"],
            "round": group["round"],
            "route": trial["route"],
            "slot": trial["slot"],
            "attempt": attempt,
            "job_name": job_name,
            "trial_dir": None,
            "classification": None,
            "reason": None,
            "reward": None,
            "partial": None,
            "tokens": dict(ZERO_TOKENS),
            "agent_wall_seconds": None,
            "attempt_wall_seconds": None,
            "trial_wall_seconds": None,
            "started_at": started_at,
            "finished_at": None,
            "started_at_iso": epoch_iso(started_at),
            "finished_at_iso": None,
            "container_started_at": None,
            "container_finished_at": None,
            "exit_code": None,
            "timed_out": False,
            "terminal_error": None,
            "status_code": None,
            "pier_exit_code": None,
            "outcome_present": False,
            "reward_present": False,
            "mock": bool(self.plan["mock_base_url"]),
            "orchestrator_timeout": False,
            "interrupted": False,
            "prepare_exit_code": None,
        }

        prepare_rc = self._prepare_task(group, trial, task_out, task_copy)
        record["prepare_exit_code"] = prepare_rc
        if prepare_rc != 0:
            record.update(
                classification="harness-error",
                reason=f"prepare-exit-{prepare_rc}",
                finished_at=now_epoch(),
            )
            record["finished_at_iso"] = epoch_iso(record["finished_at"])
            record["attempt_wall_seconds"] = round(record["finished_at"] - started_at, 3)
            return record

        run = self._run_pier(trial, job_name, task_copy)
        trial_dir = self._find_trial_dir(job_name)
        trial_result = None
        outcome = None
        reward_value = None
        partial = None
        if trial_dir is not None:
            record["trial_dir"] = str(trial_dir)
            trial_result = read_json(trial_dir / "result.json")
            outcome = read_json(trial_dir / "agent/outcome.json")
            reward_payload = read_json(trial_dir / "verifier/reward.json")
            if reward_payload is not None:
                value = reward_payload.get("reward")
                if isinstance(value, (int, float)):
                    reward_value = float(value)
                partial_value = reward_payload.get("partial")
                if isinstance(partial_value, (int, float)):
                    partial = float(partial_value)
            elif trial_result is not None:
                # Pier 自己的 reward_stats 只作兜底：reward.json 才是 verifier 的原始产物。
                verifier_result = trial_result.get("verifier_result") or {}
                rewards = verifier_result.get("rewards") or {}
                value = rewards.get("reward")
                if isinstance(value, (int, float)):
                    reward_value = float(value)
        record["outcome_present"] = outcome is not None
        record["reward_present"] = reward_value is not None
        record["reward"] = reward_value
        record["partial"] = partial

        if outcome is not None:
            usage = outcome.get("usage") or {}
            record["tokens"] = {
                "input": usage.get("input"),
                "cached": usage.get("cached"),
                "output": usage.get("output"),
                "reasoning": usage.get("reasoning"),
                "llm_calls": outcome.get("llm_calls"),
            }
            record["agent_wall_seconds"] = outcome.get("wall_seconds")
            record["exit_code"] = outcome.get("exit_code")
            record["timed_out"] = bool(outcome.get("timed_out"))
            record["terminal_error"] = outcome.get("terminal_error")
        if trial_result is not None:
            container_start = iso_epoch(trial_result.get("started_at"))
            container_end = iso_epoch(trial_result.get("finished_at"))
            record["container_started_at"] = container_start
            record["container_finished_at"] = container_end
            if container_start is not None and container_end is not None:
                record["trial_wall_seconds"] = round(container_end - container_start, 3)
        record["pier_exit_code"] = run["pier_exit_code"]
        record["orchestrator_timeout"] = run["outer_timeout"]
        record["interrupted"] = run["interrupted"]

        classification, reason, status_code = classify(
            pier_rc=run["pier_exit_code"],
            trial_result=trial_result,
            outcome=outcome,
            reward=reward_value,
            outer_timeout=run["outer_timeout"],
            interrupted=run["interrupted"],
            spawn_error=run["spawn_error"],
        )
        record["classification"] = classification
        record["reason"] = reason
        record["status_code"] = status_code
        record["finished_at"] = now_epoch()
        record["finished_at_iso"] = epoch_iso(record["finished_at"])
        record["attempt_wall_seconds"] = round(record["finished_at"] - started_at, 3)

        # 安全网：密钥若出现在 Pier 日志里，说明传递方式被上游改动，必须显式暴露（不打印密钥本身）。
        log_path = self.logs_dir / f"{job_name}.log"
        api_key = self.api_keys[trial["route"]]
        if log_path.is_file() and api_key:
            try:
                text = log_path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                text = ""
            if api_key in text:
                record["key_leak_in_log"] = True
                with self.lock:
                    self.key_leak_events += 1
        return record


    # ------------------------------------------------------------ 组与调度
    def _exhausted(self, key: tuple[str, int, str]) -> bool:
        with self.lock:
            record = self.latest.get(key)
            used = self.attempt_counts.get(key, 0)
        if record is None or record.get("classification") not in RETRYABLE:
            return False
        return used >= MAX_ATTEMPTS

    def group_status(self, group: dict) -> str | None:
        with self.lock:
            records = [
                self.latest.get((group["task"], group["round"], trial["route"]))
                for trial in group["trials"]
            ]
        if any(record is not None and record.get("classification") == "void" for record in records):
            return "void"
        if all(record is not None and record.get("classification") in SCORED for record in records):
            if any(
                self.pending_reopen((group["task"], group["round"], trial["route"]))
                for trial in group["trials"]
            ):
                # 已请求重开、新尝试还没落结果：旧的计分失败不能当成这组的最终结论。
                return None
            return "done"
        return None

    def prior_group_end(self, group_id: str) -> str | None:
        """最近一次 trial-reopen 之前的 group-end 已失效，只有之后的才算这组的结论。"""
        reopened_at = -1
        for index, event in enumerate(self.state_events):
            if event.get("event") == "trial-reopen" and event.get("group_id") == group_id:
                reopened_at = index
        for event in reversed(self.state_events[reopened_at + 1 :]):
            if event.get("event") == "group-end" and event.get("group_id") == group_id:
                if event.get("status") in ("done", "void"):
                    return event["status"]
        return None

    # -------------------------------------------------------------- 显式重开
    def pending_reopen(self, key: tuple[str, int, str]) -> dict | None:
        """该试验是否「已请求重开、新尝试还没落结果」。

        判据：最新结果仍是计分失败（fail），且落过一条 original_attempt 与它相同的
        trial-reopen。新尝试一旦落盘（含中断补记），attempt 前进，重开即视为已消费。
        """
        with self.lock:
            record = self.latest.get(key)
        if record is None or record.get("classification") != "fail":
            return None
        attempt = int(record.get("attempt") or 0)
        for event in reversed(self.state_events):
            if event.get("event") != "trial-reopen":
                continue
            if (event.get("task"), int(event.get("round") or 0), event.get("route")) != key:
                continue
            if int(event.get("original_attempt") or 0) == attempt:
                return event
        return None

    def plan_reruns(
        self, specs: list[tuple[str, str, int]]
    ) -> tuple[list[dict], list[tuple[str, str, int]]]:
        """先验证全部重开请求再决定追加哪些事件；任何一条不合法都整体拒绝、不写盘。

        幂等：同一 (group_id, route, original_attempt) 已有 trial-reopen 事件时只跳过，
        不重复落事件、也不重跑（所以重跑完成后再次执行同一命令不会触发下一次尝试）。
        """
        groups = {group["group_id"]: group for group in self.plan["groups"]}
        existing = {
            (event.get("group_id"), event.get("route"), int(event.get("original_attempt") or 0))
            for event in self.state_events
            if event.get("event") == "trial-reopen"
        }
        events: list[dict] = []
        skipped: list[tuple[str, str, int]] = []
        for group_id, route, attempt in specs:
            spec = f"{group_id}:{route}:{attempt}"
            group = groups.get(group_id)
            if group is None:
                raise SystemExit(f"--rerun-trial {spec}：plan 里没有组 {group_id!r}")
            if not any(trial["route"] == route for trial in group["trials"]):
                raise SystemExit(f"--rerun-trial {spec}：{group_id} 没有路线 {route!r}")
            if (group_id, route, attempt) in existing:
                skipped.append((group_id, route, attempt))
                continue
            key = (group["task"], int(group["round"]), route)
            with self.lock:
                record = self.latest.get(key)
                used = self.attempt_counts.get(key, 0)
            if record is None:
                raise SystemExit(f"--rerun-trial {spec}：还没有任何结果，不能重开")
            classification = record.get("classification")
            if classification != "fail":
                raise SystemExit(
                    f"--rerun-trial {spec}：最新结果是 {classification}"
                    f"（{record.get('reason')}），只有计分失败（fail）可重开"
                )
            if int(record.get("attempt") or 0) != attempt:
                raise SystemExit(
                    f"--rerun-trial {spec}：原尝试是 a{record.get('attempt')}，"
                    f"请求的 a{attempt} 已过期"
                )
            if used >= MAX_ATTEMPTS:
                raise SystemExit(
                    f"--rerun-trial {spec}：已用满 {MAX_ATTEMPTS} 次尝试预算，不能再重开"
                )
            events.append(
                {
                    "event": "trial-reopen",
                    "batch_id": self.plan["batch_id"],
                    "group_id": group_id,
                    "task": group["task"],
                    "round": int(group["round"]),
                    "route": route,
                    "original_attempt": attempt,
                }
            )
        return events, skipped

    def apply_reruns(self, events: list[dict]) -> None:
        for event in events:
            self.append_state(event)

    def run_trial(self, group: dict, trial: dict) -> str:
        key = (group["task"], group["round"], trial["route"])
        void_event = self.group_void.setdefault(group["group_id"], threading.Event())
        while True:
            if self.stop.is_set():
                return "stopped"
            if void_event.is_set():
                return "voided"
            with self.lock:
                record = self.latest.get(key)
                used = self.attempt_counts.get(key, 0)
            if record is not None and record.get("classification") in TERMINAL_TRIAL:
                if self.pending_reopen(key) is None:
                    return record["classification"]
            if used >= MAX_ATTEMPTS:
                void_event.set()
                return "exhausted"
            attempt = used + 1
            self.append_state(
                {
                    "event": "trial-start",
                    "batch_id": self.plan["batch_id"],
                    "group_id": group["group_id"],
                    "task": group["task"],
                    "round": group["round"],
                    "route": trial["route"],
                    "slot": trial["slot"],
                    "attempt": attempt,
                    "job_name": f"{trial['job_name_prefix']}-a{attempt}",
                    "base_url": trial["base_url"],
                }
            )
            print(
                f"[{time.strftime('%H:%M:%S')}] 起 {group['group_id']} {trial['route']} "
                f"a{attempt}（slot{trial['slot']}）",
                flush=True,
            )
            result = self.execute_attempt(group, trial, attempt)
            self.append_result(result)
            self.append_state(
                {
                    "event": "trial-end",
                    "batch_id": self.plan["batch_id"],
                    "group_id": group["group_id"],
                    "task": group["task"],
                    "round": group["round"],
                    "route": trial["route"],
                    "attempt": attempt,
                    "classification": result["classification"],
                    "reason": result["reason"],
                    "status_code": result.get("status_code"),
                    "reward": result.get("reward"),
                }
            )
            print(
                f"[{time.strftime('%H:%M:%S')}] 完 {group['group_id']} {trial['route']} "
                f"a{attempt} → {result['classification']}（{result['reason']}）"
                f" reward={result.get('reward')} wall={result.get('attempt_wall_seconds')}s",
                flush=True,
            )
            classification = result["classification"]
            if classification in RETRYABLE:
                if attempt < MAX_ATTEMPTS:
                    continue
                void_event.set()
                return "exhausted"
            return classification

    def void_group(self, group: dict, by_route: str, reason_code: str) -> str:
        """整组作废：给所选路线各补一条 void 记录（latest wins 覆盖旧结果），不再重试。"""
        group_id = group["group_id"]
        void_event = self.group_void.setdefault(group_id, threading.Event())
        void_event.set()
        by_key = (group["task"], group["round"], by_route)
        with self.lock:
            by_record = self.latest.get(by_key)
            by_attempts = self.attempt_counts.get(by_key, 0)
        detail = (
            f"{reason_code}: {by_route} 尝试 {by_attempts} 次仍失败"
            f"（{by_record.get('classification') if by_record else 'no-record'}"
            f":{by_record.get('reason') if by_record else '-'}）"
        )
        for trial in group["trials"]:
            key = (group["task"], group["round"], trial["route"])
            with self.lock:
                record = self.latest.get(key)
            if record is not None and record.get("classification") == "void":
                continue
            finished = now_epoch()
            self.append_result(
                {
                    "schema": SCHEMA_RESULT,
                    "batch_id": self.plan["batch_id"],
                    "group_id": group_id,
                    "task": group["task"],
                    "round": group["round"],
                    "route": trial["route"],
                    "slot": trial["slot"],
                    "attempt": int(record.get("attempt") or 0) if record else 0,
                    "job_name": record.get("job_name") if record else None,
                    "trial_dir": record.get("trial_dir") if record else None,
                    "classification": "void",
                    "reason": f"group-void: {detail}",
                    "voided_by_route": by_route,
                    "superseded_classification": record.get("classification") if record else None,
                    "superseded_reason": record.get("reason") if record else None,
                    "reward": record.get("reward") if record else None,
                    "partial": record.get("partial") if record else None,
                    "tokens": record.get("tokens") if record else dict(ZERO_TOKENS),
                    "agent_wall_seconds": record.get("agent_wall_seconds") if record else None,
                    "attempt_wall_seconds": record.get("attempt_wall_seconds") if record else 0,
                    "trial_wall_seconds": record.get("trial_wall_seconds") if record else None,
                    "started_at": record.get("started_at") if record else finished,
                    "finished_at": record.get("finished_at") if record else finished,
                    "started_at_iso": record.get("started_at_iso") if record else epoch_iso(finished),
                    "finished_at_iso": record.get("finished_at_iso") if record else epoch_iso(finished),
                    "container_started_at": record.get("container_started_at") if record else None,
                    "container_finished_at": record.get("container_finished_at") if record else None,
                    "exit_code": record.get("exit_code") if record else None,
                    "timed_out": bool(record.get("timed_out")) if record else False,
                    "terminal_error": record.get("terminal_error") if record else None,
                    "status_code": record.get("status_code") if record else None,
                    "pier_exit_code": record.get("pier_exit_code") if record else None,
                    "outcome_present": bool(record.get("outcome_present")) if record else False,
                    "reward_present": bool(record.get("reward_present")) if record else False,
                    "mock": bool(self.plan["mock_base_url"]),
                }
            )
        self.append_state(
            {
                "event": "group-void",
                "batch_id": self.plan["batch_id"],
                "group_id": group_id,
                "task": group["task"],
                "round": group["round"],
                "voided_by_route": by_route,
                "reason": detail,
            }
        )
        self.append_state(
            {
                "event": "group-end",
                "batch_id": self.plan["batch_id"],
                "group_id": group_id,
                "task": group["task"],
                "round": group["round"],
                "status": "void",
                "voided_by_route": by_route,
            }
        )
        print(f"[{time.strftime('%H:%M:%S')}] 作废 {group_id}：{detail}", flush=True)
        return "void"

    def run_group(self, group: dict) -> str:
        group_id = group["group_id"]
        prior = self.prior_group_end(group_id)
        if prior is not None:
            return prior
        self.group_void.setdefault(group_id, threading.Event())
        exhausted = [
            trial["route"]
            for trial in group["trials"]
            if self._exhausted((group["task"], group["round"], trial["route"]))
        ]
        if exhausted:
            return self.void_group(group, exhausted[0], "retries-exhausted")
        status = self.group_status(group)
        if status is not None:
            if status == "void":
                # 作废跑到一半（只补了部分 void 记录）时补齐，再落 group-end。
                by_route = next(
                    (
                        record.get("voided_by_route")
                        for trial in group["trials"]
                        for record in [self.latest.get((group["task"], group["round"], trial["route"]))]
                        if record is not None and record.get("classification") == "void"
                    ),
                    "unknown",
                )
                return self.void_group(group, by_route or "unknown", "retries-exhausted")
            # 上次跑到一半：组已经全 terminal，只是 group-end 没落盘。
            self.append_state(
                {
                    "event": "group-end",
                    "batch_id": self.plan["batch_id"],
                    "group_id": group_id,
                    "task": group["task"],
                    "round": group["round"],
                    "status": status,
                    "recovered": True,
                }
            )
            return status
        if self.stop.is_set():
            return "stopped"

        self.append_state(
            {
                "event": "group-start",
                "batch_id": self.plan["batch_id"],
                "group_id": group_id,
                "task": group["task"],
                "round": group["round"],
                "slots": {trial["route"]: trial["slot"] for trial in group["trials"]},
            }
        )
        print(
            f"[{time.strftime('%H:%M:%S')}] 组 {group_id} 开始："
            + "  ".join(f"slot{t['slot']}={t['route']}" for t in group["trials"]),
            flush=True,
        )
        threads = [
            threading.Thread(
                target=self.run_trial,
                args=(group, trial),
                name=f"{group_id}/{trial['route']}",
                daemon=True,
            )
            for trial in group["trials"]
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        exhausted = [
            trial["route"]
            for trial in group["trials"]
            if self._exhausted((group["task"], group["round"], trial["route"]))
        ]
        if exhausted:
            return self.void_group(group, exhausted[0], "retries-exhausted")
        status = self.group_status(group)
        if status is None:
            status = "interrupted" if self.stop.is_set() else "incomplete"
        self.append_state(
            {
                "event": "group-end",
                "batch_id": self.plan["batch_id"],
                "group_id": group_id,
                "task": group["task"],
                "round": group["round"],
                "status": status,
            }
        )
        return status

    # -------------------------------------------------------------- 汇总产物
    def build_summary(self) -> dict:
        plan = self.plan
        routes = [route["name"] for route in plan["routes"]]
        group_starts: dict[str, float] = {}
        group_ends: dict[str, float] = {}
        for event in self.state_events:
            group_id = event.get("group_id")
            if not group_id:
                continue
            if event.get("event") == "group-start":
                group_starts[group_id] = min(group_starts.get(group_id, event["ts"]), event["ts"])
            elif event.get("event") == "group-end":
                group_ends[group_id] = max(group_ends.get(group_id, event["ts"]), event["ts"])

        with self.lock:
            latest = dict(self.latest)
            attempt_counts = dict(self.attempt_counts)
            records = list(self.records)
        attempts = [record for record in records if record.get("classification") != "void"]

        group_rows = []
        for group in plan["groups"]:
            group_id = group["group_id"]
            trials = []
            for trial in group["trials"]:
                record = latest.get((group["task"], group["round"], trial["route"]))
                trials.append(
                    {
                        "route": trial["route"],
                        "slot": trial["slot"],
                        "classification": record.get("classification") if record else None,
                        "reason": record.get("reason") if record else None,
                        "reward": record.get("reward") if record else None,
                        "attempts": attempt_counts.get(
                            (group["task"], group["round"], trial["route"]), 0
                        ),
                        "agent_wall_seconds": record.get("agent_wall_seconds") if record else None,
                        "trial_dir": record.get("trial_dir") if record else None,
                    }
                )
            status = self.group_status(group) or "incomplete"
            start = group_starts.get(group_id)
            end = group_ends.get(group_id)
            group_rows.append(
                {
                    "group_id": group_id,
                    "task": group["task"],
                    "round": group["round"],
                    "status": status,
                    "voided_by_route": next(
                        (
                            event.get("voided_by_route")
                            for event in reversed(self.state_events)
                            if event.get("event") == "group-void" and event.get("group_id") == group_id
                        ),
                        None,
                    ),
                    "started_at": start,
                    "finished_at": end,
                    "wall_seconds": round(end - start, 3) if start and end else None,
                    "trials": trials,
                }
            )

        route_stats = {}
        for route in routes:
            planned = sum(1 for group in plan["groups"] for trial in group["trials"] if trial["route"] == route)
            route_records = [
                record
                for key, record in latest.items()
                if key[2] == route
            ]
            scored = [record for record in route_records if record.get("classification") in SCORED]
            passes = sum(1 for record in scored if record.get("classification") == "pass")
            route_attempts = [record for record in attempts if record.get("route") == route]
            retries = sum(
                max(0, count - 1) for key, count in attempt_counts.items() if key[2] == route
            )
            status_codes: dict[str, int] = {}
            for record in route_attempts:
                code = record.get("status_code")
                if code is not None:
                    status_codes[str(code)] = status_codes.get(str(code), 0) + 1
            terminal_kinds: dict[str, int] = {}
            for record in route_attempts:
                terminal = record.get("terminal_error") or {}
                kind = terminal.get("kind")
                if kind:
                    terminal_kinds[str(kind)] = terminal_kinds.get(str(kind), 0) + 1
            token_totals = {"input": 0, "cached": 0, "output": 0, "reasoning": 0, "llm_calls": 0}
            token_known = 0
            for record in route_attempts:
                tokens = record.get("tokens") or {}
                known = False
                for field in ("input", "cached", "output", "reasoning", "llm_calls"):
                    value = tokens.get(field)
                    if isinstance(value, (int, float)):
                        token_totals[field] += int(value)
                        known = True
                token_known += 1 if known else 0
            route_stats[route] = {
                "trials_planned": planned,
                "trials_terminal": len(route_records),
                "trials_scored": len(scored),
                "passes": passes,
                "pass_rate": round(passes / len(scored), 4) if scored else None,
                "attempts": len(route_attempts),
                "retries": retries,
                "infra_errors": sum(
                    1 for record in route_attempts if record.get("classification") == "infra-error"
                ),
                "harness_errors": sum(
                    1 for record in route_attempts if record.get("classification") == "harness-error"
                ),
                "interrupted": sum(
                    1 for record in route_attempts if record.get("classification") == "interrupted"
                ),
                "void_groups": sum(
                    1
                    for record in route_records
                    if record.get("classification") == "void"
                ),
                "status_codes": status_codes,
                "terminal_error_kinds": terminal_kinds,
                "tokens": {**token_totals, "known_attempts": token_known},
                "wall_seconds": wall_stats(
                    [record.get("agent_wall_seconds") for record in route_attempts]
                ),
            }

        scored_all = [record for record in latest.values() if record.get("classification") in SCORED]
        totals = {
            "groups_planned": len(plan["groups"]),
            "groups_done": sum(1 for row in group_rows if row["status"] == "done"),
            "groups_void": sum(1 for row in group_rows if row["status"] == "void"),
            "groups_incomplete": sum(1 for row in group_rows if row["status"] == "incomplete"),
            "trials_planned": sum(len(group["trials"]) for group in plan["groups"]),
            "trials_scored": len(scored_all),
            "passes": sum(1 for record in scored_all if record.get("classification") == "pass"),
            "fails": sum(1 for record in scored_all if record.get("classification") == "fail"),
            "attempts": len(attempts),
            "retries": sum(max(0, count - 1) for count in attempt_counts.values()),
            "infra_errors": sum(
                1 for record in attempts if record.get("classification") == "infra-error"
            ),
            "harness_errors": sum(
                1 for record in attempts if record.get("classification") == "harness-error"
            ),
            "interrupted": sum(
                1 for record in attempts if record.get("classification") == "interrupted"
            ),
            "key_leak_events": self.key_leak_events,
        }
        return {
            "schema": SCHEMA_SUMMARY,
            "batch_id": plan["batch_id"],
            "generated_at": epoch_iso(now_epoch()),
            "plan": {
                "mode": plan["mode"],
                "seed": plan["seed"],
                "tasks": len(plan["tasks"]),
                "rounds": plan["rounds"],
                "groups": plan["n_groups"],
                "max_groups": plan["max_groups"],
                "mock": bool(plan["mock_base_url"]),
                "routes": plan["routes"],
            },
            "totals": totals,
            "routes": route_stats,
            "groups": group_rows,
        }

    def write_summary(self) -> dict:
        summary = self.build_summary()
        atomic_write_json(self.out / "summary.json", summary)
        atomic_write_json(self.out / "runs.json", self.build_runs())
        return summary

    def build_runs(self) -> list[dict]:
        """meterd 窗口：每条尝试一条，优先 container 窗口（模型请求都落在里面）。"""
        runs = []
        seen = set()
        for record in self.records:
            if record.get("classification") == "void":
                continue
            start = record.get("container_started_at")
            end = record.get("container_finished_at")
            if start is None or end is None:
                start = record.get("started_at")
                end = record.get("finished_at")
            if start is None or end is None:
                continue
            window = {
                "route": record["route"],
                "experiment": record["group_id"],
                "started_at": round(float(start), 3),
                "finished_at": round(float(end), 3),
                "task": record["task"],
                "round": record["round"],
                "attempt": record["attempt"],
            }
            signature = (
                window["route"],
                window["experiment"],
                window["started_at"],
                window["finished_at"],
            )
            if signature in seen:
                continue
            seen.add(signature)
            runs.append(window)
        runs.sort(key=lambda item: (item["started_at"], item["route"]))
        return runs


# --------------------------------------------------------------------- 入口
def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="批次编排器（一个组 = 同一任务 × 同一轮次 × 所选路线，默认三路）",
    )
    parser.add_argument("--tasks", type=Path, default=SCRATCH / "sample.json", help="冻结任务清单")
    parser.add_argument(
        "--out", type=Path, default=None, help="输出目录（默认 SCRATCH/batches/<batch-id>）"
    )
    parser.add_argument("--batch-id", default=None, help="批次 id（默认 <mode>-<UTC 时间戳>）")
    parser.add_argument("--mode", choices=("pilot", "batch"), default="batch")
    parser.add_argument("--rounds", type=int, default=4, help="轮数（pilot 模式固定 1）")
    parser.add_argument("--reasoning", choices=("high", "xhigh"), default=REASONING, help="DSH 推理强度（冻结进计划，不能跨强度续跑）")
    parser.add_argument(
        "--route",
        action="append",
        default=None,
        metavar="NAME:PORT",
        help="只运行指定路线及其宿主入口端口，可重复；不指定时默认 stravia:23481 cpa:23482 sub2api:23483",
    )
    parser.add_argument("--max-groups", type=int, default=2, help="最大并发组数（每组每条所选路线各 1 个试验）")
    parser.add_argument("--seed", type=int, default=0, help="计划随机种子（组顺序与组内槽位）")
    parser.add_argument("--agent-wall-seconds", type=int, default=3600, help="DSH 单次墙钟上限")
    parser.add_argument(
        "--trial-wall-seconds",
        type=int,
        default=None,
        help="单次尝试的外层硬上限（默认 agent 上限 + 1800）",
    )
    parser.add_argument(
        "--upstream-host",
        default="127.0.0.1",
        help="宿主侧网关地址（PIER_DSH_BASE_URL 的 host 部分；容器经 Pier 的 socket 通道访问）",
    )
    parser.add_argument(
        "--mock-base-url",
        default=None,
        help="把所选路线都指向本地假上游（零推理冒烟），例如 http://127.0.0.1:29081/v1",
    )
    parser.add_argument("--private-dir", type=Path, default=DEFAULT_PRIVATE)
    parser.add_argument("--wrap-devenv", action="store_true", help="裸 shell 里用 devenv shell 包一层")
    parser.add_argument("--dry-run", action="store_true", help="只打印计划，不起任何容器")
    parser.add_argument("--no-preflight", action="store_true", help="跳过启动前检查（自测用）")
    parser.add_argument(
        "--rerun-trial",
        action="append",
        default=None,
        metavar="GROUP_ID:ROUTE:ORIGINAL_ATTEMPT",
        help="显式重开一条已计分失败（fail）的试验，可重复；只追加 trial-reopen，旧结果保留",
    )
    parser.add_argument(
        "--confirm-real-inference",
        action="store_true",
        help="确认真实批次会消耗账号额度（不带 --mock-base-url 时必填）",
    )
    args = parser.parse_args(argv)
    args.routes = parse_routes(args.route)
    args.batch_id = args.batch_id or f"{args.mode}-{datetime.now().strftime('%Y%m%dT%H%M%S')}"
    args.out = args.out or SCRATCH / "batches" / args.batch_id
    if args.rounds < 1:
        raise SystemExit("--rounds 至少为 1")
    if args.max_groups < 1:
        raise SystemExit("--max-groups 至少为 1")
    if args.trial_wall_seconds is None:
        args.trial_wall_seconds = args.agent_wall_seconds + 1800
    if args.trial_wall_seconds <= args.agent_wall_seconds:
        raise SystemExit("--trial-wall-seconds 必须大于 --agent-wall-seconds，否则会先杀掉 DSH")
    args.prepare_route_flag = False
    return args


def preflight(args: argparse.Namespace, plan: dict) -> None:
    problems = []
    for path in (PREPARE, RUN_PIER):
        if not path.is_file():
            problems.append(f"缺少 {path}")
    if not args.mock_base_url:
        for route in plan["routes"]:
            path = Path(args.private_dir) / "run" / route["name"] / "client.json"
            payload = read_json(path)
            if not payload or not payload.get("api_key"):
                problems.append(f"路线 {route['name']} 缺少客户端密钥（{path} 的 api_key）")
    if not args.wrap_devenv and shutil.which("docker") is None:
        problems.append("当前 shell 没有 docker：在 devenv shell 内启动，或加 --wrap-devenv")
    missing = [
        task for task in plan["tasks"] if not (SCRATCH / "deep-swe/tasks" / task / "task.toml").is_file()
    ]
    if missing:
        problems.append(f"源任务缺失 {len(missing)} 个：{', '.join(missing[:3])}…")
    if args.mock_base_url:
        url = args.mock_base_url.rstrip("/") + "/models"
        try:
            with urllib.request.urlopen(url, timeout=5) as response:
                response.read(64)
        except (urllib.error.URLError, OSError) as exc:
            problems.append(f"假上游不可达 {url}（{type(exc).__name__}）")
    if problems:
        raise SystemExit("preflight 失败：\n- " + "\n- ".join(problems))


def install_signal_handlers(batch: Batch) -> None:
    def handler(signum, frame):  # noqa: ANN001 - signal 回调签名固定
        if batch.stop.is_set():
            print("已在收尾：等待运行中的尝试结束（再按一次也不会强杀，避免留下半写状态）", flush=True)
            return
        batch.stop.set()
        print(
            f"\n收到 {signal.Signals(signum).name}：停止发起新尝试；运行中的 Pier 会被终止并记为 interrupted",
            flush=True,
        )

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


def print_summary(summary: dict, out: Path) -> None:
    totals = summary["totals"]
    print("")
    print(
        f"批次 {summary['batch_id']}：组 {totals['groups_done']} 完成 / "
        f"{totals['groups_void']} 作废 / {totals['groups_incomplete']} 未完成；"
        f"试验 {totals['trials_scored']} 计分（通过 {totals['passes']}），"
        f"尝试 {totals['attempts']}（重试 {totals['retries']}），"
        f"infra {totals['infra_errors']} / harness {totals['harness_errors']} / "
        f"中断 {totals['interrupted']}"
    )
    for route, stats in summary["routes"].items():
        rate = stats["pass_rate"]
        rate_text = f"{rate:.3f}" if rate is not None else "n/a"
        print(
            f"  {route:<8} 通过 {stats['passes']}/{stats['trials_scored']} = {rate_text}；"
            f"尝试 {stats['attempts']} 重试 {stats['retries']}；"
            f"作废组 {stats['void_groups']}；状态码 {stats['status_codes'] or '-'}"
        )
    if totals["key_leak_events"]:
        print(f"  [warn] {totals['key_leak_events']} 条 Pier 日志里出现了密钥，检查 run_pier.sh/dsh_agent.py")
    print(f"汇总：{out / 'summary.json'}")
    print(f"meter 窗口：{out / 'runs.json'}")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    rerun_specs = parse_rerun_specs(args.rerun_trial)
    tasks, tasks_sha = load_task_list(args.tasks)
    args.prepare_route_flag = prepare_supports_route(PREPARE)
    plan = build_plan(args, tasks, tasks_sha)

    if args.dry_run:
        print_plan(plan, args)
        return 0

    if not args.mock_base_url and not args.confirm_real_inference:
        raise SystemExit(
            "真实批次会消耗账号额度且不可撤销：确认后加 --confirm-real-inference；"
            "只想验证管线请加 --mock-base-url http://127.0.0.1:29081/v1"
        )
    if not args.no_preflight:
        preflight(args, plan)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    plan_path = out / "plan.json"
    if plan_path.is_file():
        existing = read_json(plan_path)
        if not existing or existing.get("signature") != plan["signature"]:
            raise SystemExit(
                f"{plan_path} 与本次参数不一致（任务清单/轮数/路线/seed/模式/墙钟/推理强度不同）；"
                "换一个 --out 或删掉旧批次目录"
            )
        plan = existing
        print(f"续跑已有计划：{plan_path}")
    else:
        atomic_write_json(plan_path, plan)
        print(f"计划已落盘：{plan_path}")

    batch = Batch(args, plan)
    install_signal_handlers(batch)
    # 先只校验（拒绝时账本一个字节都不写），recover 之后再落 trial-reopen，最后才计 done。
    rerun_events, rerun_skipped = batch.plan_reruns(rerun_specs)
    recovered = batch.recover_interrupted()
    batch.apply_reruns(rerun_events)
    if rerun_events:
        print(
            "重开 "
            + "、".join(
                f"{event['group_id']}:{event['route']}:{event['original_attempt']}"
                for event in rerun_events
            )
            + "（原尝试仍计入预算）"
        )
    if rerun_skipped:
        print(
            "跳过已重开请求 "
            + "、".join(f"{group_id}:{route}:{attempt}" for group_id, route, attempt in rerun_skipped)
        )
    done = sum(1 for group in plan["groups"] if batch.prior_group_end(group["group_id"]))
    print(
        f"组 {len(plan['groups'])} 个，已完成 {done}；补记中断尝试 {recovered} 条；"
        f"并发组上限 {args.max_groups}"
    )
    batch.append_state(
        {
            "event": "batch-start",
            "batch_id": plan["batch_id"],
            "pid": os.getpid(),
            "resumed": bool(done or recovered),
            "max_groups": args.max_groups,
            "mock": bool(plan["mock_base_url"]),
        }
    )

    started = now_epoch()
    with ThreadPoolExecutor(max_workers=args.max_groups) as pool:
        futures = [pool.submit(batch.run_group, group) for group in plan["groups"]]
        statuses = [future.result() for future in futures]

    summary = batch.write_summary()
    batch.append_state(
        {
            "event": "batch-end",
            "batch_id": plan["batch_id"],
            "wall_seconds": round(now_epoch() - started, 3),
            "statuses": {
                status: statuses.count(status) for status in sorted(set(statuses))
            },
            "stopped": batch.stop.is_set(),
        }
    )
    print_summary(summary, out)
    if batch.stop.is_set() or summary["totals"]["groups_incomplete"]:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
