#!/usr/bin/env python3
"""编排器自测：dry-run 计划、mock 批次（429/concurrency 重试、超时、三路作废）、续跑、中断恢复、
普通 fail 不重试、显式重开（--rerun-trial）的幂等与预算、分析脚本。

零推理：PIER 指向 selftest/fake_pier.py，三路都指向本地假上游地址；不读私有目录、不起容器、
不发任何真实请求。所有产物落在
.devenv/state/desktop/Desktop/20261026-chat-reasoning-replay/orchestrate-selftest/<时间戳>/。

用法
  python3 video/chat-reasoning-replay/project/scripts/orchestrate/selftest.py [--keep-failed]

退出码：0 全部通过；1 有失败项（逐项打印 PASS/FAIL 与证据路径）。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import select
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]
REPO = SCRIPTS.parents[3]
ORCH = SCRIPTS / "orchestrate"
RUN_BATCH = ORCH / "run_batch.py"
ANALYZE = ORCH / "analyze.py"
FAKE_PIER = ORCH / "selftest/fake_pier.py"
STATE_ROOT = (
    REPO
    / ".devenv/state/desktop/Desktop/20261026-chat-reasoning-replay/orchestrate-selftest"
)

MOCK_URL = "http://127.0.0.1:29081/v1"
DUMMY_KEY = "mock-zero-inference"

TASKS = [
    "anko-default-function-arguments",
    "cliffy-config-file-parsing",
    "expr-try-catch-errors",
]

CHECKS: list[tuple[bool, str, str]] = []


def check(condition: bool, name: str, detail: str = "") -> bool:
    CHECKS.append((bool(condition), name, detail))
    print(f"{'PASS' if condition else 'FAIL'}  {name}" + (f"  [{detail}]" if detail and not condition else ""))
    return bool(condition)


def run(cmd: list[str], env: dict | None = None, timeout: float = 300) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=timeout)


def load_jsonl(path: Path) -> list[dict]:
    if not path.is_file():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def latest(records: list[dict]) -> dict[tuple[str, int, str], dict]:
    out = {}
    for record in records:
        out[(record["task"], int(record["round"]), record["route"])] = record
    return out


def write_tasks(path: Path, tasks: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"selected": tasks}, ensure_ascii=False), encoding="utf-8")
    return path


def batch_env(out_dir: Path, scenarios: Path) -> dict:
    env = os.environ.copy()
    env.pop("BENCH_API_KEY", None)
    env["PIER"] = str(FAKE_PIER)
    env["FAKE_PIER_SCENARIOS"] = str(scenarios)
    env["FAKE_PIER_PLAN"] = str(out_dir / "plan.json")
    return env


def base_cmd(out_dir: Path, tasks: Path, extra: list[str]) -> list[str]:
    return [
        sys.executable,
        str(RUN_BATCH),
        "--out",
        str(out_dir),
        "--tasks",
        str(tasks),
        "--mock-base-url",
        MOCK_URL,
        "--no-preflight",
        *extra,
    ]


def scan_for_secret(root: Path, secret: str) -> list[str]:
    hits = []
    for path in root.rglob("*"):
        if not path.is_file() or path.stat().st_size > 5_000_000:
            continue
        try:
            data = path.read_bytes()
        except OSError:
            continue
        if secret.encode() in data:
            hits.append(str(path))
    return hits


# ---------------------------------------------------------------- 1. dry-run
def test_dry_run(root: Path) -> None:
    print("\n== dry-run 计划 ==")
    tasks = write_tasks(root / "tasks3.json", TASKS)
    out = root / "dry" / "pilot"
    proc = run(base_cmd(out, tasks, ["--dry-run", "--mode", "pilot", "--seed", "0"]))
    text = proc.stdout
    check(proc.returncode == 0, "dry-run 退出码 0", proc.stderr[-400:])
    check("任务 1 个 × 1 轮 = 1 组 × 3 路 = 3 次试验" in text, "pilot 只计划第 1 个任务第 1 轮")
    check(f"{TASKS[0]}@r1" in text, "pilot 组是清单第一个任务")
    check(text.count("slot0=") == 1 and text.count("slot1=") == 1 and text.count("slot2=") == 1, "一组三路三槽位")
    check(MOCK_URL in text and text.count(MOCK_URL) == 3, "三路都指向假上游")
    check("密钥来源 mock（不读私有目录）" in text, "mock 模式不读私有目录")
    check(not (out / "plan.json").exists(), "dry-run 不落盘计划")

    proc_batch = run(base_cmd(root / "dry" / "batch", tasks, ["--dry-run", "--rounds", "2", "--seed", "5"]))
    lines = [line for line in proc_batch.stdout.splitlines() if line.startswith("#")]
    check(len(lines) == 6, f"batch 3 任务 × 2 轮 = 6 组（实际 {len(lines)}）")
    check("任务 3 个 × 2 轮 = 6 组 × 3 路 = 18 次试验" in proc_batch.stdout, "batch 计划计数正确")

    # 同 seed 同计划、换 seed 换顺序
    again = run(base_cmd(root / "dry" / "batch2", tasks, ["--dry-run", "--rounds", "2", "--seed", "5"]))
    lines_again = [line for line in again.stdout.splitlines() if line.startswith("#")]
    check(lines == lines_again, "同 seed 计划可复现")
    other = run(base_cmd(root / "dry" / "batch3", tasks, ["--dry-run", "--rounds", "2", "--seed", "6"]))
    lines_other = [line for line in other.stdout.splitlines() if line.startswith("#")]
    check(lines != lines_other, "换 seed 组顺序/槽位变化")
    slots = {tuple(sorted(part.strip() for part in line.split("slot")[1:])) for line in lines}
    check(len(slots) > 1, "组内槽位确实被随机化")


# ------------------------------------------------------- 2. mock 批次与判定
def test_batch(root: Path) -> Path:
    print("\n== mock 批次（429 重试 / 超时 / 三路作废）==")
    tasks = write_tasks(root / "batch-tasks.json", TASKS)
    out = root / "batch"
    scenarios = root / "batch-scenarios.json"
    # 组顺序由 seed 决定，先 dry-run 拿到顺序，再按「任务|路线」写场景：
    #   第 1 组：cpa 首次 429 后重试通过，sub2api 超时（不重试）；
    #   第 3 组：sub2api 三次 429 → 该任务×轮次三路一并作废。
    dry = run(base_cmd(root / "batch-dry", tasks, ["--dry-run", "--seed", "3", "--rounds", "1"]))
    order = [
        line.split()[1].split("@")[0]
        for line in dry.stdout.splitlines()
        if line.startswith("#")
    ]
    first_task, third_task = order[0], order[2]
    scenarios.write_text(
        json.dumps(
            {
                f"{first_task}|cpa:1": {"kind": "infra429", "sleep": 0.2},
                f"{first_task}|sub2api": {"kind": "timeout", "sleep": 0.2},
                f"{third_task}|sub2api": {"kind": "infra429", "sleep": 0.2},
                "default": {"kind": "pass", "sleep": 0.2},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    proc = run(
        base_cmd(out, tasks, ["--seed", "3", "--max-groups", "2", "--rounds", "1"]),
        env=batch_env(out, scenarios),
    )
    check(proc.returncode == 0, "批次退出码 0", (proc.stdout + proc.stderr)[-600:])

    records = load_jsonl(out / "results.jsonl")
    summary = json.loads((out / "summary.json").read_text(encoding="utf-8"))
    runs = json.loads((out / "runs.json").read_text(encoding="utf-8"))
    newest = latest(records)

    totals = summary["totals"]
    check(totals["groups_planned"] == 3 and totals["groups_done"] == 2, f"组：3 计划 / 2 完成（实际 {totals}）")
    check(totals["groups_void"] == 1 and totals["groups_incomplete"] == 0, "1 组作废、0 组未完成")

    stravia = summary["routes"]["stravia"]
    cpa = summary["routes"]["cpa"]
    sub2api = summary["routes"]["sub2api"]
    check(stravia["trials_scored"] == 2 and stravia["passes"] == 2, f"stravia 2/2 通过（实际 {stravia['passes']}/{stravia['trials_scored']}）")
    check(
        cpa["attempts"] == 4 and cpa["retries"] == 1 and cpa["infra_errors"] == 1,
        f"cpa 一次 429 重试（实际 {cpa['attempts']} 次尝试、重试 {cpa['retries']}）",
    )
    check(cpa["passes"] == 2 and cpa["trials_scored"] == 2, "cpa 重试后计入通过")
    check(sub2api["trials_scored"] == 2 and sub2api["passes"] == 1, "sub2api 超时计失败且不重试")
    check(sub2api["attempts"] == 5, f"sub2api 共 5 次尝试（超时 1 + 通过 1 + 作废组 3，实际 {sub2api['attempts']}）")
    check(sub2api["infra_errors"] == 3, f"sub2api 三次 429（实际 {sub2api['infra_errors']}）")
    check(sub2api["status_codes"].get("429") == 3, f"sub2api 状态码计数（实际 {sub2api['status_codes']}）")
    check(cpa["status_codes"].get("429") == 1, "cpa 状态码计数")
    for route in ("stravia", "cpa", "sub2api"):
        check(summary["routes"][route]["void_groups"] == 1, f"{route} 记到 1 个作废组")

    void_records = [record for record in records if record["classification"] == "void"]
    check(len(void_records) == 3, f"作废组补 3 条 void 记录（实际 {len(void_records)}）")
    check(all(record["voided_by_route"] == "sub2api" for record in void_records), "作废由 sub2api 触发")
    voided_group = void_records[0]["group_id"] if void_records else ""
    check(
        all(newest[(record["task"], record["round"], record["route"])]["classification"] == "void" for record in void_records),
        "latest-wins：作废组三路都以 void 收尾",
    )
    check(voided_group == f"{third_task}@r1", f"作废的是第 3 组（{third_task}@r1）")

    timeout_record = next(
        record
        for record in records
        if record["classification"] == "fail" and record["reason"] == "timed_out"
    )
    check(timeout_record["route"] == "sub2api" and timeout_record["timed_out"], "超时记为 fail/timed_out")
    infra_record = next(record for record in records if record["reason"] == "infra:http:429")
    check(infra_record["status_code"] == 429 and infra_record["attempt"] == 1, "429 记 infra-error 且带状态码")

    check(len(runs) == len([r for r in records if r["classification"] != "void"]), f"runs.json 每条尝试一个窗口（实际 {len(runs)}）")
    check(all(window["started_at"] <= window["finished_at"] for window in runs), "窗口起止有序")
    check(all(window["experiment"].endswith("@r1") for window in runs), "窗口 experiment 带轮次")

    fake_envs = list(out.rglob("agent/fake-env.json"))
    check(len(fake_envs) == 12, f"12 次尝试都落盘 fake-env（实际 {len(fake_envs)}）")
    check(all(json.loads(path.read_text())["base_url"] == MOCK_URL for path in fake_envs), "每次尝试都用假上游地址")
    check(
        all(json.loads(path.read_text())["model"] == "gpt-6-luna" for path in fake_envs),
        "模型 id 固定 gpt-6-luna",
    )
    check(
        all("BENCH_API_KEY=${BENCH_API_KEY}" in json.loads(path.read_text())["ae"] for path in fake_envs),
        "run_pier.sh 以模板形式传 --ae（密钥不进 argv）",
    )
    expected_sha = hashlib.sha256(DUMMY_KEY.encode()).hexdigest()[:12]
    check(
        all(json.loads(path.read_text())["api_key_sha256_prefix"] == expected_sha for path in fake_envs),
        "密钥经环境变量到达子进程",
    )
    leaks = scan_for_secret(out, DUMMY_KEY)
    check(not leaks, "假密钥没出现在任何落盘文件/日志里", ", ".join(leaks[:3]))

    analysis_path = root / "batch-analysis.json"
    proc_analyze = run(
        [sys.executable, str(ANALYZE), str(out / "results.jsonl"), "--out", str(analysis_path)]
    )
    check(proc_analyze.returncode == 0, "analyze.py 在真实结果上可运行", proc_analyze.stderr[-400:])
    analysis = json.loads(analysis_path.read_text(encoding="utf-8"))
    check(analysis["routes"]["stravia"]["pass_rate"] == 1.0, "分析：stravia 通过率 1.0")
    check(analysis["routes"]["cpa"]["pass_rate"] == 1.0, "分析：cpa 通过率 1.0")
    check(analysis["routes"]["sub2api"]["pass_rate"] == 0.5, "分析：sub2api 通过率 0.5")
    check(abs(analysis["contrast"]["diff"] - 0.25) < 1e-9, f"分析：差值 0.25（实际 {analysis['contrast']['diff']}）")
    check(analysis["excluded_trials"].get("void") == 3, "分析：作废组排除在分母外")
    return out


# ------------------------------------------------------------------- 3. 续跑
def test_resume(root: Path, out: Path) -> None:
    print("\n== 续跑（已完成的组不重跑）==")
    before = (out / "results.jsonl").read_text(encoding="utf-8")
    tasks = root / "batch-tasks.json"
    scenarios = root / "batch-scenarios.json"
    proc = run(
        base_cmd(out, tasks, ["--seed", "3", "--max-groups", "2", "--rounds", "1"]),
        env=batch_env(out, scenarios),
    )
    after = (out / "results.jsonl").read_text(encoding="utf-8")
    check(proc.returncode == 0, "续跑退出码 0", (proc.stdout + proc.stderr)[-400:])
    check(before == after, "续跑没有追加任何尝试记录")
    check("已完成 3" in proc.stdout, "续跑报告 3 组已完成")


# --------------------------------------------------------------- 4. 中断恢复
def test_interrupt(root: Path) -> None:
    print("\n== 中断与恢复 ==")
    tasks = write_tasks(root / "interrupt-tasks.json", [TASKS[0]])
    out = root / "interrupt"
    scenarios = root / "interrupt-scenarios.json"
    scenarios.write_text(
        json.dumps({"stravia": {"kind": "hang", "sleep": 60}, "default": {"kind": "pass", "sleep": 0.3}}),
        encoding="utf-8",
    )
    env = batch_env(out, scenarios)
    proc = subprocess.Popen(
        base_cmd(out, tasks, ["--seed", "1", "--max-groups", "1", "--rounds", "1"]),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
    )
    state_path = out / "state.jsonl"
    deadline = time.time() + 60
    started = False
    while time.time() < deadline:
        if state_path.is_file():
            events = load_jsonl(state_path)
            started = any(
                event.get("event") == "trial-start" and event.get("route") == "stravia"
                for event in events
            )
        if started:
            break
        time.sleep(0.2)
    check(started, "stravia 尝试已启动（可被中断）")
    time.sleep(0.5)
    proc.send_signal(signal.SIGINT)
    stdout, _ = proc.communicate(timeout=120)
    check(proc.returncode == 2, f"中断后退出码 2（实际 {proc.returncode}）")

    records = load_jsonl(out / "results.jsonl")
    stravia_records = [record for record in records if record["route"] == "stravia"]
    check(
        any(record["classification"] == "interrupted" for record in stravia_records),
        "被中断的尝试记为 interrupted",
    )
    check(
        any(
            event.get("event") == "trial-end" and event.get("classification") == "interrupted"
            for event in load_jsonl(out / "state.jsonl")
        ),
        "中断尝试有 trial-end 事件（续跑不会重复补记）",
    )

    scenarios.write_text(json.dumps({"default": {"kind": "pass", "sleep": 0.2}}), encoding="utf-8")
    proc2 = run(base_cmd(out, tasks, ["--seed", "1", "--max-groups", "1", "--rounds", "1"]), env=env)
    check(proc2.returncode == 0, "恢复后续跑成功", (proc2.stdout + proc2.stderr)[-400:])
    records = load_jsonl(out / "results.jsonl")
    stravia_records = [record for record in records if record["route"] == "stravia"]
    newest = latest(records)[(TASKS[0], 1, "stravia")]
    check(len(stravia_records) == 2, f"stravia 共 2 次尝试（实际 {len(stravia_records)}）")
    check(newest["classification"] == "pass" and newest["attempt"] == 2, "中断算一次尝试，重跑后通过")
    summary = json.loads((out / "summary.json").read_text(encoding="utf-8"))
    check(summary["totals"]["groups_done"] == 1, "恢复后组完成")
    check(summary["totals"]["attempts"] == 4, f"总尝试 4 次（stravia 2 + 两路各 1，实际 {summary['totals']['attempts']}）")


# ---------------------------------------- 4b. harness-error 分类、重试与作废
def test_harness(root: Path) -> None:
    print("\n== harness-error 单独分类 / 重试 / 作废 / 外层墙钟 ==")
    tasks = write_tasks(root / "harness-tasks.json", TASKS[:2])
    out = root / "harness"
    dry = run(base_cmd(root / "harness-dry", tasks, ["--dry-run", "--seed", "2", "--rounds", "1"]))
    order = [
        line.split()[1].split("@")[0]
        for line in dry.stdout.splitlines()
        if line.startswith("#")
    ]
    first, second = order[0], order[1]
    scenarios = root / "harness-scenarios.json"
    scenarios.write_text(
        json.dumps(
            {
                # 非基础设施的 4xx：按任务失败，不重试
                f"{first}|stravia": {"kind": "http400"},
                # verifier 产物缺失 → harness-error，重试一次后通过
                f"{first}|cpa:1": {"kind": "missing-reward"},
                # Pier 自己非零退出 → harness-error，重试一次后通过
                f"{first}|sub2api:1": {"kind": "pier-exit", "pier_exit": 3},
                # 三路里有一路三次缺 outcome → 与模型侧无关，但仍作废该组
                f"{second}|stravia": {"kind": "no-outcome"},
                "default": {"kind": "pass", "sleep": 0.2},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    proc = run(
        base_cmd(out, tasks, ["--seed", "2", "--max-groups", "1", "--rounds", "1"]),
        env=batch_env(out, scenarios),
    )
    check(proc.returncode == 0, "harness 批次退出码 0", (proc.stdout + proc.stderr)[-600:])
    summary = json.loads((out / "summary.json").read_text(encoding="utf-8"))
    records = load_jsonl(out / "results.jsonl")
    totals = summary["totals"]
    check(totals["groups_done"] == 1 and totals["groups_void"] == 1, f"1 完成 1 作废（实际 {totals['groups_done']}/{totals['groups_void']}）")
    check(totals["harness_errors"] == 5, f"harness-error 5 次（实际 {totals['harness_errors']}）")
    check(totals["infra_errors"] == 0, f"模型侧 infra 0 次（实际 {totals['infra_errors']}）")
    check(summary["routes"]["stravia"]["status_codes"].get("400") == 1, "4xx 记 fail 且保留状态码")
    four_xx = next(record for record in records if record["reason"] == "agent-error:http:400")
    check(four_xx["classification"] == "fail", "4xx 不是 infra-error")
    check(summary["routes"]["cpa"]["retries"] == 1 and summary["routes"]["cpa"]["attempts"] == 3, "harness-error 重试一次")
    check(summary["routes"]["cpa"]["harness_errors"] == 1, "cpa harness 计数")
    check(summary["routes"]["sub2api"]["retries"] == 1, "pier 非零退出也算 harness 并重试")
    check(summary["routes"]["stravia"]["harness_errors"] == 3, "缺 outcome 三次")
    void_records = [record for record in records if record["classification"] == "void"]
    check(len(void_records) == 3 and all("harness-error" in record["reason"] for record in void_records), "作废原因标明 harness-error")

    # 作废写到一半（只落 1 条 void 就没有 group-end）后续跑必须补齐
    out3 = root / "harness-void-repair"
    tasks1 = write_tasks(root / "harness-void-tasks.json", [first])
    scenarios3 = root / "harness-void-scenarios.json"
    scenarios3.write_text(
        json.dumps(
            {
                f"{first}|sub2api": {"kind": "no-outcome"},
                "default": {"kind": "pass", "sleep": 0.1},
            }
        ),
        encoding="utf-8",
    )
    cmd3 = base_cmd(out3, tasks1, ["--seed", "9", "--max-groups", "1", "--rounds", "1"])
    run(cmd3, env=batch_env(out3, scenarios3))
    kept = [json.loads(line) for line in (out3 / "results.jsonl").read_text().splitlines() if line.strip()]
    void_lines = [row for row in kept if row["classification"] == "void"]
    partial = [row for row in kept if row["classification"] != "void"] + void_lines[:1]
    (out3 / "results.jsonl").write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in partial) + "\n", encoding="utf-8"
    )
    events = [json.loads(line) for line in (out3 / "state.jsonl").read_text().splitlines() if line.strip()]
    events = [event for event in events if event.get("event") not in ("group-end", "group-void")]
    (out3 / "state.jsonl").write_text(
        "\n".join(json.dumps(event, ensure_ascii=False) for event in events) + "\n", encoding="utf-8"
    )
    proc3 = run(cmd3, env=batch_env(out3, scenarios3))
    check(proc3.returncode == 0, "作废补记后续跑退出码 0", (proc3.stdout + proc3.stderr)[-300:])
    repaired = [
        json.loads(line)
        for line in (out3 / "results.jsonl").read_text().splitlines()
        if line.strip()
    ]
    check(
        len([row for row in repaired if row["classification"] == "void"]) == 3,
        "补齐三路 void 记录",
    )
    check(
        len([row for row in repaired if row["classification"] != "void"]) == len(
            [row for row in kept if row["classification"] != "void"]
        ),
        "补记不新增尝试",
    )

    # 外层墙钟：不依赖 DSH 自己超时，编排器必须硬杀并重试
    out2 = root / "harness-wall"
    scenarios2 = root / "harness-wall-scenarios.json"
    scenarios2.write_text(
        json.dumps(
            {
                f"{first}|cpa:1": {"kind": "hang", "sleep": 120},
                "default": {"kind": "pass", "sleep": 0.2},
            }
        ),
        encoding="utf-8",
    )
    proc = run(
        base_cmd(
            out2,
            write_tasks(root / "harness-wall-tasks.json", [first]),
            [
                "--seed",
                "4",
                "--max-groups",
                "1",
                "--rounds",
                "1",
                "--agent-wall-seconds",
                "1",
                "--trial-wall-seconds",
                "4",
            ],
        ),
        env=batch_env(out2, scenarios2),
        timeout=300,
    )
    check(proc.returncode == 0, "外层墙钟批次退出码 0", (proc.stdout + proc.stderr)[-400:])
    records2 = load_jsonl(out2 / "results.jsonl")
    wall_record = next(
        (record for record in records2 if record["reason"] == "orchestrator-wall"), None
    )
    check(wall_record is not None and wall_record["orchestrator_timeout"], "外层墙钟记 orchestrator-wall")
    check(wall_record["classification"] == "harness-error", "外层墙钟归 harness-error")
    check(
        latest(records2)[(first, 1, "cpa")]["classification"] == "pass"
        and latest(records2)[(first, 1, "cpa")]["attempt"] == 2,
        "被杀后第二次尝试通过",
    )


# ------------------------------------- 4c. concurrency（并发上限）分类与重试
def test_concurrency(root: Path) -> None:
    print("\n== concurrency 终态错误：重试成功 / 三次作废 / 不可重开 ==")
    task = TASKS[0]
    group_id = f"{task}@r1"
    tasks = write_tasks(root / "concurrency-tasks.json", [task])
    cmd = base_cmd(root / "concurrency-retry", tasks, ["--seed", "1", "--max-groups", "1", "--rounds", "1"])

    out = root / "concurrency-retry"
    scenarios = root / "concurrency-scenarios.json"
    scenarios.write_text(
        json.dumps(
            {
                f"{task}|sub2api:1": {"kind": "concurrency", "sleep": 0.1},
                "default": {"kind": "pass", "sleep": 0.1},
            }
        ),
        encoding="utf-8",
    )
    proc = run(cmd, env=batch_env(out, scenarios))
    check(proc.returncode == 0, "concurrency 批次退出码 0", (proc.stdout + proc.stderr)[-400:])
    records = load_jsonl(out / "results.jsonl")
    summary = json.loads((out / "summary.json").read_text(encoding="utf-8"))
    infra = next((record for record in records if record["classification"] == "infra-error"), None)
    check(
        infra is not None and infra["reason"] == "infra:concurrency",
        f"concurrency 记 infra-error/infra:concurrency（实际 {infra and (infra['classification'], infra['reason'])}）",
    )
    check(
        summary["routes"]["sub2api"]["attempts"] == 2 and summary["routes"]["sub2api"]["retries"] == 1,
        f"concurrency 自动重试一次（实际 {summary['routes']['sub2api']['attempts']} 次尝试）",
    )
    check(summary["routes"]["sub2api"]["infra_errors"] == 1, "sub2api 记 1 次 infra-error")
    check(summary["routes"]["sub2api"]["status_codes"] == {}, "concurrency 不伪造 HTTP 状态码")
    newest = latest(records)[(task, 1, "sub2api")]
    check(newest["classification"] == "pass" and newest["attempt"] == 2, "重试后按最新结果通过")
    check(summary["totals"]["groups_done"] == 1 and summary["totals"]["infra_errors"] == 1, "组完成、infra 计 1 次")

    # 同一路三次 concurrency → 整组三路一并作废
    out2 = root / "concurrency-void"
    scenarios2 = root / "concurrency-void-scenarios.json"
    scenarios2.write_text(
        json.dumps(
            {
                f"{task}|sub2api": {"kind": "concurrency", "sleep": 0.1},
                "default": {"kind": "pass", "sleep": 0.1},
            }
        ),
        encoding="utf-8",
    )
    cmd2 = base_cmd(out2, tasks, ["--seed", "1", "--max-groups", "1", "--rounds", "1"])
    proc2 = run(cmd2, env=batch_env(out2, scenarios2))
    check(proc2.returncode == 0, "三次 concurrency 批次退出码 0", (proc2.stdout + proc2.stderr)[-400:])
    records2 = load_jsonl(out2 / "results.jsonl")
    summary2 = json.loads((out2 / "summary.json").read_text(encoding="utf-8"))
    infra_records = [record for record in records2 if record["classification"] == "infra-error"]
    check(
        len(infra_records) == 3 and all(record["reason"] == "infra:concurrency" for record in infra_records),
        f"三次尝试都记 infra:concurrency（实际 {len(infra_records)}）",
    )
    check(summary2["totals"]["infra_errors"] == 3, "infra-error 计 3 次")
    check(summary2["totals"]["groups_void"] == 1 and summary2["totals"]["groups_done"] == 0, "三次仍失败 → 组作废")
    void_records = [record for record in records2 if record["classification"] == "void"]
    check(len(void_records) == 3 and all("retries-exhausted" in record["reason"] for record in void_records), "三路各补一条作废记录")
    check(
        all(latest(records2)[(task, 1, route)]["classification"] == "void" for route in ("stravia", "cpa", "sub2api")),
        "latest-wins：作废组三路都以 void 收尾",
    )

    before_results = (out2 / "results.jsonl").read_bytes()
    before_state = (out2 / "state.jsonl").read_bytes()
    rejected = run(cmd2 + ["--rerun-trial", f"{group_id}:sub2api:1"], env=batch_env(out2, scenarios2))
    check(rejected.returncode != 0, "作废组不可显式重开（非零退出）", (rejected.stdout + rejected.stderr)[-300:])
    check((out2 / "results.jsonl").read_bytes() == before_results, "拒绝重开不追加结果")
    check((out2 / "state.jsonl").read_bytes() == before_state, "拒绝重开不追加状态事件")


# ------------------------------- 4d. 普通 fail（策略拒绝 / 非法请求）不自动重试
def test_ordinary_fail(root: Path) -> None:
    print("\n== 普通 fail（policy / invalid prompt）不自动重试 ==")
    task = TASKS[0]
    tasks = write_tasks(root / "ordinary-tasks.json", [task])
    out = root / "ordinary"
    scenarios = root / "ordinary-scenarios.json"
    scenarios.write_text(
        json.dumps(
            {
                f"{task}|cpa": {"kind": "policy", "sleep": 0.1},
                f"{task}|sub2api": {"kind": "invalid-prompt", "sleep": 0.1},
                "default": {"kind": "pass", "sleep": 0.1},
            }
        ),
        encoding="utf-8",
    )
    proc = run(
        base_cmd(out, tasks, ["--seed", "1", "--max-groups", "1", "--rounds", "1"]),
        env=batch_env(out, scenarios),
    )
    check(proc.returncode == 0, "普通 fail 批次退出码 0", (proc.stdout + proc.stderr)[-400:])
    records = load_jsonl(out / "results.jsonl")
    summary = json.loads((out / "summary.json").read_text(encoding="utf-8"))
    policy = next(record for record in records if record["route"] == "cpa")
    invalid = next(record for record in records if record["route"] == "sub2api")
    check(
        policy["classification"] == "fail" and policy["reason"] == "agent-error:other:None",
        f"策略拒绝记 fail（实际 {policy['classification']}/{policy['reason']}）",
    )
    check(
        invalid["classification"] == "fail" and invalid["reason"] == "agent-error:other:400",
        f"非法请求记 fail（实际 {invalid['classification']}/{invalid['reason']}）",
    )
    check(
        summary["routes"]["cpa"]["attempts"] == 1 and summary["routes"]["sub2api"]["attempts"] == 1,
        f"普通 fail 不重试（实际 cpa {summary['routes']['cpa']['attempts']} / sub2api {summary['routes']['sub2api']['attempts']}）",
    )
    check(summary["routes"]["cpa"]["retries"] == 0 and summary["routes"]["sub2api"]["retries"] == 0, "普通 fail 重试计数为 0")
    check(summary["totals"]["groups_done"] == 1 and summary["totals"]["infra_errors"] == 0, "三路计分后组完成、infra 为 0")

    # 预算耗尽：a1/a2 是 concurrency（自动重试），a3 是策略拒绝；latest 仍是 fail 但已用满 3 次。
    out2 = root / "ordinary-exhausted"
    scenarios2 = root / "ordinary-exhausted-scenarios.json"
    scenarios2.write_text(
        json.dumps(
            {
                f"{task}|sub2api:1": {"kind": "concurrency", "sleep": 0.1},
                f"{task}|sub2api:2": {"kind": "concurrency", "sleep": 0.1},
                f"{task}|sub2api:3": {"kind": "policy", "sleep": 0.1},
                "default": {"kind": "pass", "sleep": 0.1},
            }
        ),
        encoding="utf-8",
    )
    cmd2 = base_cmd(out2, tasks, ["--seed", "1", "--max-groups", "1", "--rounds", "1"])
    proc2 = run(cmd2, env=batch_env(out2, scenarios2))
    check(proc2.returncode == 0, "预算用例批次退出码 0", (proc2.stdout + proc2.stderr)[-400:])
    exhausted = latest(load_jsonl(out2 / "results.jsonl"))[(task, 1, "sub2api")]
    check(
        exhausted["classification"] == "fail" and exhausted["attempt"] == 3,
        f"第 3 次策略拒绝后仍计分失败（实际 a{exhausted['attempt']}/{exhausted['classification']}）",
    )
    before_results = (out2 / "results.jsonl").read_bytes()
    before_state = (out2 / "state.jsonl").read_bytes()
    rejected = run(cmd2 + ["--rerun-trial", f"{task}@r1:sub2api:3"], env=batch_env(out2, scenarios2))
    check(rejected.returncode != 0, "预算耗尽的重开被拒绝（非零退出）", (rejected.stdout + rejected.stderr)[-300:])
    check((out2 / "results.jsonl").read_bytes() == before_results, "预算拒绝不追加结果")
    check((out2 / "state.jsonl").read_bytes() == before_state, "预算拒绝不追加状态事件")


# ------------------------------ 4e. 显式重开（--rerun-trial）：幂等、预算、历史保留
def test_rerun(root: Path) -> None:
    print("\n== 显式重开：只跑目标路线、历史保留、latest wins、同命令幂等 ==")
    task = TASKS[0]
    group_id = f"{task}@r1"
    tasks = write_tasks(root / "rerun-tasks.json", [task])
    out = root / "rerun"
    scenarios = root / "rerun-scenarios.json"
    # a1 策略拒绝（fail，不重试）；显式重开的 a2 仍被拒；再显式重开的 a3 通过。
    scenarios.write_text(
        json.dumps(
            {
                f"{task}|sub2api:1": {"kind": "policy", "sleep": 0.1},
                f"{task}|sub2api:2": {"kind": "policy", "sleep": 0.1},
                "default": {"kind": "pass", "sleep": 0.1},
            }
        ),
        encoding="utf-8",
    )
    cmd = base_cmd(out, tasks, ["--seed", "1", "--max-groups", "1", "--rounds", "1"])
    env = batch_env(out, scenarios)
    results_path = out / "results.jsonl"
    state_path = out / "state.jsonl"

    proc = run(cmd, env=env)
    check(proc.returncode == 0, "重开前批次退出码 0", (proc.stdout + proc.stderr)[-400:])
    baseline = load_jsonl(results_path)
    check(len(baseline) == 3, f"基线 3 次尝试（实际 {len(baseline)}）")
    check(latest(baseline)[(task, 1, "sub2api")]["classification"] == "fail", "基线里 sub2api 计分失败")

    before_results = results_path.read_bytes()
    before_state = state_path.read_bytes()
    cases = [
        ("未知组", "nope@r9:sub2api:1"),
        ("未知路线", f"{group_id}:api9:1"),
        ("pass 试验", f"{group_id}:cpa:1"),
        ("过期尝试号", f"{group_id}:sub2api:2"),
        ("格式错误", "abc"),
    ]
    for label, spec in cases:
        rejected = run(cmd + ["--rerun-trial", spec], env=env)
        check(
            rejected.returncode != 0,
            f"{label} 的重开被拒绝（非零退出）",
            (rejected.stdout + rejected.stderr)[-300:],
        )
    check(results_path.read_bytes() == before_results, "非法/过期请求不追加结果")
    check(state_path.read_bytes() == before_state, "非法/过期请求不追加状态事件")

    # 一条合法 + 一条非法：整体拒绝，不能半应用
    mixed = run(
        cmd + ["--rerun-trial", f"{group_id}:sub2api:1", "--rerun-trial", "nope@r9:cpa:1"],
        env=env,
    )
    check(mixed.returncode != 0, "合法+非法混合请求整体拒绝", (mixed.stdout + mixed.stderr)[-300:])
    check(results_path.read_bytes() == before_results, "整体拒绝时结果不变")
    check(state_path.read_bytes() == before_state, "整体拒绝时状态不变（合法请求也没落盘）")

    rerun = run(cmd + ["--rerun-trial", f"{group_id}:sub2api:1"], env=env)
    check(rerun.returncode == 0, "显式重开退出码 0", (rerun.stdout + rerun.stderr)[-400:])
    after = load_jsonl(results_path)
    added = after[len(baseline) :]
    check(
        len(added) == 1 and added[0]["route"] == "sub2api" and added[0]["attempt"] == 2,
        f"只追加目标路线的 a2（实际 {[(r['route'], r['attempt']) for r in added]}）",
    )
    check(added and added[0]["classification"] == "fail", "a2 仍按最新结果计分失败")
    check(
        len([record for record in after if record["route"] == "cpa"]) == 1
        and len([record for record in after if record["route"] == "stravia"]) == 1,
        "已评分的其他路线不重跑",
    )
    events = load_jsonl(state_path)
    reopens = [event for event in events if event.get("event") == "trial-reopen"]
    check(len(reopens) == 1, f"落 1 条 trial-reopen 事件（实际 {len(reopens)}）")
    check(
        bool(reopens)
        and reopens[0]["group_id"] == group_id
        and reopens[0]["task"] == task
        and int(reopens[0]["round"]) == 1
        and reopens[0]["route"] == "sub2api"
        and int(reopens[0]["original_attempt"]) == 1,
        "trial-reopen 记录组/任务/轮次/路线/原尝试",
    )
    group_ends = [event for event in events if event.get("event") == "group-end" and event.get("group_id") == group_id]
    check(len(group_ends) == 2 and group_ends[-1]["status"] == "done", "旧 group-end 失效并重新落盘")
    summary = json.loads((out / "summary.json").read_text(encoding="utf-8"))
    check(summary["totals"]["groups_done"] == 1 and summary["totals"]["groups_incomplete"] == 0, "重开后组仍完成")
    check(summary["totals"]["attempts"] == 4 and summary["totals"]["retries"] == 1, "原尝试仍计入预算（历史成本保留）")
    check(summary["routes"]["sub2api"]["trials_scored"] == 1, "latest wins：sub2api 只按最新一次计分")

    # 同一条命令再来一次：靠已有 trial-reopen 幂等，不触发 a3
    before_results = results_path.read_bytes()
    again = run(cmd + ["--rerun-trial", f"{group_id}:sub2api:1"], env=env)
    check(again.returncode == 0, "重复同一重开命令退出码 0", (again.stdout + again.stderr)[-300:])
    check(results_path.read_bytes() == before_results, "重复命令不追加任何尝试")
    events = load_jsonl(state_path)
    check(
        len([event for event in events if event.get("event") == "trial-reopen"]) == 1,
        "重复命令不重复落 trial-reopen",
    )
    check(
        len(
            [
                event
                for event in events
                if event.get("event") == "trial-start"
                and event.get("group_id") == group_id
                and event.get("route") == "sub2api"
            ]
        )
        == 2,
        "重复命令不起 a3",
    )

    # 对 a2（同样计分失败）再显式重开一次 → a3，预算从原尝试继续
    rerun2 = run(cmd + ["--rerun-trial", f"{group_id}:sub2api:2"], env=env)
    check(rerun2.returncode == 0, "对 a2 的显式重开退出码 0", (rerun2.stdout + rerun2.stderr)[-400:])
    final = load_jsonl(results_path)
    check(len(final) == len(after) + 1, f"第二次重开只追加 1 条（实际 {len(final) - len(after)}）")
    newest = latest(final)[(task, 1, "sub2api")]
    check(newest["classification"] == "pass" and newest["attempt"] == 3, "a3 通过并按最新结果生效")
    sub2api_records = [record for record in final if record["route"] == "sub2api"]
    check(
        [record["attempt"] for record in sub2api_records] == [1, 2, 3],
        f"三次尝试的历史记录都保留（实际 {[record['attempt'] for record in sub2api_records]}）",
    )
    summary = json.loads((out / "summary.json").read_text(encoding="utf-8"))
    check(summary["totals"]["attempts"] == 5 and summary["totals"]["retries"] == 2, "两次重开各消耗一次预算")
    check(summary["routes"]["sub2api"]["passes"] == 1 and summary["routes"]["sub2api"]["trials_scored"] == 1, "latest wins：sub2api 计 1 次通过")
    check(len(json.loads((out / "runs.json").read_text(encoding="utf-8"))) == 5, "runs.json 保留每次尝试的计量窗口")

    rejected = run(cmd + ["--rerun-trial", f"{group_id}:sub2api:3"], env=env)
    check(rejected.returncode != 0, "已通过的最新结果不可再重开", (rejected.stdout + rejected.stderr)[-300:])


# ------------------------------------------------------- 5. 分析脚本的正确性
def write_results(path: Path, rows: list[dict]) -> Path:
    path.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n", encoding="utf-8"
    )
    return path


def analyze_json(path: Path, out: Path, iterations: int = 10000) -> dict:
    proc = run([sys.executable, str(ANALYZE), str(path), "--iterations", str(iterations), "--out", str(out)])
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr)
    return json.loads(out.read_text(encoding="utf-8"))


def record(task: str, round_no: int, route: str, passed: bool, classification: str | None = None) -> dict:
    return {
        "task": task,
        "round": round_no,
        "route": route,
        "classification": classification or ("pass" if passed else "fail"),
        "reward": 1.0 if passed else 0.0,
    }


def test_analyze(root: Path) -> None:
    print("\n== analyze.py 在造的数据上 ==")
    routes = ("stravia", "cpa", "sub2api")
    tasks = [f"t{index}" for index in range(10)]

    identical = [
        record(task, 1, route, task in tasks[:5]) for task in tasks for route in routes
    ]
    path = write_results(root / "syn-identical.jsonl", identical)
    result = analyze_json(path, root / "syn-identical.json")
    check(result["contrast"]["diff"] == 0.0, "两路完全相同时差值为 0")
    check(result["contrast"]["ci95"] == [0.0, 0.0], f"差值 CI 覆盖 0（实际 {result['contrast']['ci95']}）")
    check(all(result["routes"][route]["pass_rate"] == 0.5 for route in routes), "三路通过率都是 0.5")

    known = []
    for index, task in enumerate(tasks):
        known.append(record(task, 1, "stravia", index < 8))
        known.append(record(task, 1, "cpa", index < 5))
        known.append(record(task, 1, "sub2api", index < 5))
    path = write_results(root / "syn-known.jsonl", known)
    result = analyze_json(path, root / "syn-known.json")
    check(result["routes"]["stravia"]["pass_rate"] == 0.8, "已知通过率 stravia 0.8")
    check(result["routes"]["cpa"]["pass_rate"] == 0.5, "已知通过率 cpa 0.5")
    check(result["routes"]["sub2api"]["pass_rate"] == 0.5, "已知通过率 sub2api 0.5")
    check(abs(result["contrast"]["diff"] - 0.3) < 1e-12, f"差值恰为 0.3（实际 {result['contrast']['diff']}）")
    low, high = result["contrast"]["ci95"]
    check(low is not None and high is not None and low <= 0.3 <= high, f"CI 覆盖真值 0.3（实际 {result['contrast']['ci95']}）")
    check(result["contrast"]["bootstrap"]["valid_replicates"] == 10000, "10000 次重抽样全部有效")
    check(result["contrast"]["bootstrap"]["clusters"] == 10, "聚类单位是任务（10 个）")

    all_pass = [record(task, 1, route, True) for task in tasks for route in routes]
    path = write_results(root / "syn-allpass.jsonl", all_pass)
    result = analyze_json(path, root / "syn-allpass.json")
    check(result["contrast"]["diff"] == 0.0 and result["contrast"]["ci95"] == [0.0, 0.0], "全通过时差值 0、CI 退化为 0")

    # 聚类效应：只有 2 个任务、每任务 10 轮。行级 bootstrap 会给出很窄的区间，
    # 任务级重抽样必须给出约 [-0.5, 0.5]。
    clustered = []
    for round_no in range(1, 11):
        clustered.append(record("A", round_no, "stravia", True))
        clustered.append(record("B", round_no, "stravia", False))
        for route in ("cpa", "sub2api"):
            clustered.append(record("A", round_no, route, round_no <= 5))
            clustered.append(record("B", round_no, route, round_no <= 5))
    path = write_results(root / "syn-cluster.jsonl", clustered)
    result = analyze_json(path, root / "syn-cluster.json")
    low, high = result["contrast"]["ci95"]
    check(abs(result["contrast"]["diff"]) < 1e-12, "聚类样例差值 0")
    check(high - low >= 0.9, f"任务级 CI 明显宽于行级（实际 [{low}, {high}]）")
    check(low <= -0.49 and high >= 0.49, f"聚类 CI 覆盖 ±0.5（实际 [{low}, {high}]）")

    # void 记录覆盖旧结果：t0 的 stravia 被作废后不计入分母。
    mixed = [record(task, 1, route, task in tasks[:5]) for task in tasks for route in routes]
    mixed.append(
        {
            "task": "t0",
            "round": 1,
            "route": "stravia",
            "classification": "void",
            "reason": "group-void: test",
            "reward": 1.0,
        }
    )
    path = write_results(root / "syn-void.jsonl", mixed)
    result = analyze_json(path, root / "syn-void.json")
    check(result["excluded_trials"].get("void") == 1, "void 记录被排除")
    check(result["routes"]["stravia"]["trials"] == 9, "stravia 分母 9")
    check(result["routes"]["stravia"]["passes"] == 4, "stravia 通过 4")
    check(abs(result["routes"]["stravia"]["pass_rate"] - 4 / 9) < 1e-4, "stravia 通过率 4/9")


def test_single_route(root: Path) -> None:
    print("\n== 单路公开 CLI：80 试验 / 15 并发 / 重试 / 续跑隔离 ==")
    tasks = REPO / ".scratch/research/chat-reasoning-replay/pier/sample.json"
    out = root / "single-route"
    scenarios = root / "single-route-scenarios.json"
    scenarios.write_text(json.dumps({"default": {"kind": "pass", "sleep": 0}}), encoding="utf-8")
    ledger = root / "single-route-ledger.jsonl"
    env = batch_env(out, scenarios)
    env["FAKE_PIER_LEDGER"] = str(ledger)
    barrier = root / "single-route-barrier"
    barrier.mkdir()
    os.mkfifo(barrier / "arrivals", 0o600)
    os.mkfifo(barrier / "release", 0o600)
    env["FAKE_PIER_BARRIER"] = str(barrier)
    options = ["--route", "stravia:23481", "--max-groups", "15", "--rounds", "4", "--seed", "0"]
    cmd = base_cmd(out, tasks, options)
    # 每个 fake 在写入真实 start 后通知 FIFO 并阻塞；收到 15 个通知才统一放行。
    # 父进程持有双向端点，避免 FIFO open/EOF 竞争；后续尝试看 released 标记直接通过。
    arrivals_fd = os.open(barrier / "arrivals", os.O_RDWR | os.O_NONBLOCK)
    release_fd = os.open(barrier / "release", os.O_RDWR | os.O_NONBLOCK)
    process = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    deadline = time.monotonic() + 300
    try:
        arrived = 0
        while arrived < 15 and process.poll() is None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(cmd, 300)
            readable, _, _ = select.select([arrivals_fd], [], [], min(1, remaining))
            if readable:
                arrived += len(os.read(arrivals_fd, 15 - arrived))
        (barrier / "released").touch()
        os.write(release_fd, b"1" * 15)
        stdout, stderr = process.communicate(timeout=max(0.1, deadline - time.monotonic()))
        proc = subprocess.CompletedProcess(cmd, process.returncode, stdout, stderr)
    finally:
        # 失败也释放已在屏障等待的 fake，不能把测试替身遗留在工作站上。
        (barrier / "released").touch(exist_ok=True)
        os.write(release_fd, b"1" * 15)
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
            try:
                process.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate()
        os.close(arrivals_fd)
        os.close(release_fd)
    check(proc.returncode == 0, "单路 80 试验 CLI 成功", (proc.stdout + proc.stderr)[-600:])
    # 红阶段也保留原有全部检查，不因单路尚未支持而异常中止。
    if proc.returncode != 0:
        return
    events = load_jsonl(ledger)
    starts = [row for row in events if row["event"] == "start"]
    active = peak = 0
    for row in sorted(events, key=lambda row: row["time_ns"]):
        active += 1 if row["event"] == "start" else -1
        peak = max(peak, active)
    check(len(starts) == 80 and len({row["job_name"] for row in starts}) == 80, "公开 ledger 恰有 80 个独立尝试")
    check({row["route"] for row in events} == {"stravia"}, "公开 ledger 没有 CPA / sub2api 尝试")
    check(peak == 15 and active == 0, "单路实际达到且不超过 15 个活跃试验", f"peak={peak}, active={active}")
    records = load_jsonl(out / "results.jsonl")
    summary = json.loads((out / "summary.json").read_text(encoding="utf-8"))
    check(len(records) == 80 and all(row["route"] == "stravia" and row["classification"] == "pass" for row in records), "单路仅产出 80 个 Stravia pass，无假对照结果")
    check(summary["totals"]["trials_planned"] == 80 and summary["totals"]["groups_done"] == 80, "单路汇总为 80 计划 / 80 完成")
    check(set(summary["routes"]) == {"stravia"}, "汇总只包含选中路线")
    before_ledger = ledger.read_bytes()
    before_results = (out / "results.jsonl").read_bytes()
    resumed = run(cmd, env=env)
    check(resumed.returncode == 0 and ledger.read_bytes() == before_ledger and (out / "results.jsonl").read_bytes() == before_results, "单路完成后续跑不追加尝试或结果")

    retry_tasks = write_tasks(root / "single-retry-tasks.json", TASKS[:2])
    retry_out = root / "single-retry"
    retry_scenarios = root / "single-retry-scenarios.json"
    retry_scenarios.write_text(json.dumps({
        f"{TASKS[0]}|stravia:1": {"kind": "concurrency"},
        f"{TASKS[1]}|stravia": {"kind": "infra429"},
        "default": {"kind": "pass"},
    }), encoding="utf-8")
    retry_ledger = root / "single-retry-ledger.jsonl"
    retry_env = batch_env(retry_out, retry_scenarios)
    retry_env["FAKE_PIER_LEDGER"] = str(retry_ledger)
    retry_cmd = base_cmd(retry_out, retry_tasks, ["--route", "stravia:23481", "--rounds", "1"])
    retried = run(retry_cmd, env=retry_env)
    check(retried.returncode == 0, "单路重试 / 耗尽 CLI 成功", (retried.stdout + retried.stderr)[-600:])
    retry_starts = [row for row in load_jsonl(retry_ledger) if row["event"] == "start"]
    check(len(retry_starts) == 5 and {row["route"] for row in retry_starts} == {"stravia"}, "单路公共 ledger 仅 Stravia：成功重试 2 次 + 耗尽 3 次")
    retry_records = load_jsonl(retry_out / "results.jsonl")
    successful = [row for row in retry_records if row["task"] == TASKS[0]]
    exhausted = [row for row in retry_records if row["task"] == TASKS[1]]
    check([row["classification"] for row in successful] == ["infra-error", "pass"], "单路并发错误自动重试并通过")
    check([row["classification"] for row in exhausted] == ["infra-error"] * 3 + ["void"], "单路三次耗尽只补自身 void")
    retry_before = retry_ledger.read_bytes()
    retry_results_before = (retry_out / "results.jsonl").read_bytes()
    retry_resume = run(retry_cmd, env=retry_env)
    check(retry_resume.returncode == 0 and retry_ledger.read_bytes() == retry_before and (retry_out / "results.jsonl").read_bytes() == retry_results_before, "单路 pass / void 续跑均不重复")

    # 复用已有公开 CLI 生成的三路批，不构造或修改私有计划结构。
    old_out = root / "batch"
    old_results = (old_out / "results.jsonl").read_bytes()
    old_state = (old_out / "state.jsonl").read_bytes()
    old_plan = (old_out / "plan.json").read_bytes()
    rejected = run(base_cmd(old_out, root / "batch-tasks.json", [
        "--route", "stravia:23481", "--seed", "3", "--max-groups", "15", "--rounds", "1",
    ]), env=batch_env(old_out, root / "batch-scenarios.json"))
    check(rejected.returncode != 0, "单路不能误续旧三路批")
    check((old_out / "results.jsonl").read_bytes() == old_results and (old_out / "state.jsonl").read_bytes() == old_state and (old_out / "plan.json").read_bytes() == old_plan, "拒绝误续时旧三路计划与双账本字节不变")


def test_reasoning(root: Path) -> None:
    print("\n== reasoning 公开 CLI：强度隔离与续跑 ==")
    tasks = write_tasks(root / "reasoning-tasks.json", [TASKS[0]])
    scenarios = root / "reasoning-scenarios.json"
    scenarios.write_text(json.dumps({"default": {"kind": "pass", "sleep": 0}}), encoding="utf-8")
    out = root / "reasoning-xhigh"
    ledger = root / "reasoning-xhigh-ledger.jsonl"
    env = batch_env(out, scenarios)
    env["FAKE_PIER_LEDGER"] = str(ledger)
    cmd = base_cmd(out, tasks, ["--reasoning", "xhigh", "--rounds", "1"])
    proc = run(cmd, env=env)
    check(proc.returncode == 0, "xhigh 公开 CLI 完成批次", (proc.stdout + proc.stderr)[-600:])
    if proc.returncode != 0:
        return
    records = load_jsonl(out / "results.jsonl")
    check(len(records) == 3 and all(row["classification"] == "pass" for row in records), "xhigh 三路均消费 outcome / reward 得到 pass")
    preserved = ("plan.json", "results.jsonl", "state.jsonl")
    before = {name: (out / name).read_bytes() for name in preserved}
    before_ledger = ledger.read_bytes()
    resumed = run(cmd, env=env)
    check(
        resumed.returncode == 0
        and ledger.read_bytes() == before_ledger
        and (out / "results.jsonl").read_bytes() == before["results.jsonl"],
        "xhigh 完成后同强度续跑不追加尝试或结果",
        (resumed.stdout + resumed.stderr)[-600:],
    )
    # 正常续跑可追加 batch-start/end；拒绝混强度则连状态账本也不能变化。
    before = {name: (out / name).read_bytes() for name in preserved}
    rejected = run(base_cmd(out, tasks, ["--reasoning", "high", "--rounds", "1"]), env=env)
    check(rejected.returncode != 0, "xhigh 批不能改 high 续跑")
    check(
        all((out / name).read_bytes() == data for name, data in before.items())
        and ledger.read_bytes() == before_ledger,
        "拒绝 xhigh→high 时计划、双账本与尝试账本不变",
    )

    high_out = root / "reasoning-high"
    high_ledger = root / "reasoning-high-ledger.jsonl"
    high_env = batch_env(high_out, scenarios)
    high_env["FAKE_PIER_LEDGER"] = str(high_ledger)
    high_cmd = base_cmd(high_out, tasks, ["--rounds", "1"])
    high = run(high_cmd, env=high_env)
    check(high.returncode == 0, "默认 high 公开 CLI 完成批次", (high.stdout + high.stderr)[-600:])
    if high.returncode != 0:
        return
    high_before = {name: (high_out / name).read_bytes() for name in preserved}
    high_ledger_before = high_ledger.read_bytes()
    rejected = run(base_cmd(high_out, tasks, ["--reasoning", "xhigh", "--rounds", "1"]), env=high_env)
    check(rejected.returncode != 0, "high 批不能改 xhigh 续跑")
    check(
        all((high_out / name).read_bytes() == data for name, data in high_before.items())
        and high_ledger.read_bytes() == high_ledger_before,
        "拒绝 high→xhigh 时计划、双账本与尝试账本不变",
    )
    explicit_high = run(base_cmd(high_out, tasks, ["--reasoning", "high", "--rounds", "1"]), env=high_env)
    default_high = run(high_cmd, env=high_env)
    check(
        explicit_high.returncode == default_high.returncode == 0
        and high_ledger.read_bytes() == high_ledger_before
        and (high_out / "results.jsonl").read_bytes() == high_before["results.jsonl"]
        and (high_out / "plan.json").read_bytes() == high_before["plan.json"],
        "默认与显式 high 完成后续跑幂等且共享原计划",
        (explicit_high.stdout + explicit_high.stderr + default_high.stdout + default_high.stderr)[-600:],
    )

    invalid_out = root / "reasoning-invalid"
    invalid_ledger = root / "reasoning-invalid-ledger.jsonl"
    invalid_env = batch_env(invalid_out, scenarios)
    invalid_env["FAKE_PIER_LEDGER"] = str(invalid_ledger)
    invalid = run(base_cmd(invalid_out, tasks, ["--reasoning", "medium", "--rounds", "1"]), env=invalid_env)
    check(invalid.returncode != 0, "非法推理强度被公开 CLI 拒绝")
    check(not invalid_out.exists() and not invalid_ledger.exists(), "非法推理强度不创建批次或发起尝试")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--keep-failed", action="store_true", help="保留失败目录（默认也保留）")
    args = parser.parse_args()

    stamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    root = STATE_ROOT / stamp
    root.mkdir(parents=True, exist_ok=True)
    print(f"自测目录：{root}")

    try:
        test_dry_run(root)
        out = test_batch(root)
        test_resume(root, out)
        test_interrupt(root)
        test_harness(root)
        test_concurrency(root)
        test_ordinary_fail(root)
        test_rerun(root)
        test_analyze(root)
        test_single_route(root)
        test_reasoning(root)
    except Exception as exc:  # noqa: BLE001 - 自测要把异常变成一条 FAIL
        check(False, f"自测异常中止：{type(exc).__name__}", str(exc)[:500])
        import traceback

        traceback.print_exc()

    failures = [item for item in CHECKS if not item[0]]
    print("")
    print(f"共 {len(CHECKS)} 项，通过 {len(CHECKS) - len(failures)}，失败 {len(failures)}")
    if failures:
        for _, name, detail in failures:
            print(f"  FAIL {name} {detail}")
        return 1
    print("全部通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
