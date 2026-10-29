#!/usr/bin/env python3
"""批量验证冻结任务的官方 oracle：参考解 -> 任务自带 verifier -> 判分证据。

用途
- 对 sample.json 里冻结的 20 个 DeepSWE v1.1 任务，逐个用 Pier 内置
  `--agent oracle` 跑官方 solution，再由任务自己的 tests/verifier 判分。
  oracle 只执行 solution/solve.sh，不调用任何模型，也不需要密钥或 mock。
- 每个任务一个唯一 job（job 名带 run-id），完整保留 Pier 的 result.json、
  verifier 日志、reward 与 artifacts；逐题增量落盘，最后写 summary.json。
- 不是看 Pier 退出码：每题必须找到 verifier_result / reward.json 才算真正判分。

关键取舍
- 通过 run_pier.sh 运行，继承 rootless podman 兼容层；整批可以在一个
  `devenv shell -- ...` 里跑完。
- 传 `--no-delete`。Pier 默认在 trial 结束执行
  `docker compose down --rmi all`，而 agent 环境的 compose service 直接
  `image: <task.toml 的官方预构建镜像>`；`--rmi all` 会删除共享镜像，可能
  影响其它正在跑的验证。`--no-delete` 只执行 `docker compose down`，清掉
  本 trial 的容器与网络，保留所有镜像。脚本自身从不删除镜像。
- 并发固定 2：每个任务一个 pier 进程（`-n 1`），线程池最多同时两个。
- 不传 BENCH_API_KEY：脚本从子进程环境里删除它，避免 run_pier.sh 自动追加
  `--ae`；oracle 不需要任何上游。
- 不改官方内容：只读 task.toml / solution / tests；运行前后对这些文件做
  sha256 对照，结果写进每题 JSON。清单与任务目录也不写。

输入
- --run-id     本次批次标识（默认 UTC 时间戳），决定输出目录与 job 名
- --tasks      只跑指定任务（逗号分隔，诊断用；默认 sample.json 全部）
- --note       写进 environment.json 的说明（例如诊断性重跑原因）
- --dry-run    只做预检并打印命令，不启动 Pier

输出
- SCRATCH/probes/oracle-alignment/<run-id>/tasks/<task>.json   逐题结果
- SCRATCH/probes/oracle-alignment/<run-id>/evidence/<task>/    关键证据副本
- SCRATCH/probes/oracle-alignment/<run-id>/logs/<task>.pier.log
- SCRATCH/probes/oracle-alignment/<run-id>/events.jsonl
- SCRATCH/probes/oracle-alignment/<run-id>/environment.json
- SCRATCH/probes/oracle-alignment/<run-id>/summary.json
- Pier 原始 job 仍保留在 SCRATCH/jobs/<job-name>/

退出码
- 0：所选任务全部 reward == 1 且都有判分证据
- 2：有任务未通过或未判分（原始失败保留）
- 1：预检失败（环境/清单/路径问题），没有启动任何 trial
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime
import hashlib
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import tomllib
from pathlib import Path

REPO = Path(__file__).resolve().parents[5]
SCRIPTS = Path(__file__).resolve().parent
SCRATCH = REPO / ".scratch/research/chat-reasoning-replay/pier"
SAMPLE_PATH = SCRATCH / "sample.json"
TASKS_ROOT = SCRATCH / "deep-swe/tasks"
JOBS_ROOT = SCRATCH / "jobs"
RUNS_ROOT = SCRATCH / "probes/oracle-alignment"
RUN_PIER = SCRIPTS / "run_pier.sh"

CONCURRENCY = 2
DEFAULT_TIMEOUT_MARGIN_SEC = 1800

# 证据里要复制的关键文件（相对 trial 目录）。存在才复制，缺失也记录。
EVIDENCE_FILES = [
    "result.json",
    "trial.log",
    "agent/oracle.txt",
    "verifier/reward.json",
    "verifier/reward.txt",
    "verifier/run.log",
    "verifier/test-stdout.txt",
    "verifier/ctrf.json",
    "artifacts/model.patch",
    "artifacts/manifest.json",
]

_print_lock = threading.Lock()


def utc_now() -> str:
    return datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_ts(value: str | None) -> datetime.datetime | None:
    if not value:
        return None
    try:
        return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def span_seconds(start: str | None, end: str | None) -> float | None:
    a, b = parse_ts(start), parse_ts(end)
    if a is None or b is None:
        return None
    return round((b - a).total_seconds(), 3)


def sha256_file(path: Path) -> str | None:
    if not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def task_hashes(task_dir: Path) -> dict[str, str]:
    """对官方内容做只读指纹：task.toml、solution/、tests/（跳过 pycache）。"""
    hashes: dict[str, str] = {}
    candidates = [task_dir / "task.toml"]
    for sub in ("solution", "tests"):
        root = task_dir / sub
        if root.is_dir():
            candidates.extend(sorted(p for p in root.rglob("*") if p.is_file()))
    for path in candidates:
        if "__pycache__" in path.parts or path.suffix == ".pyc":
            continue
        rel = path.relative_to(task_dir).as_posix()
        digest = sha256_file(path)
        if digest is not None:
            hashes[rel] = digest
    return hashes


def write_json_atomic(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    tmp.replace(path)


def append_event(path: Path, payload: dict) -> None:
    with _print_lock:
        with path.open("a") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def log(message: str) -> None:
    with _print_lock:
        print(message, flush=True)


def read_task_meta(task_dir: Path) -> dict:
    config = tomllib.loads((task_dir / "task.toml").read_text())
    env = config.get("environment") or {}
    verifier = config.get("verifier") or {}
    agent = config.get("agent") or {}
    metadata = config.get("metadata") or {}
    return {
        "docker_image": env.get("docker_image"),
        "language": metadata.get("language"),
        "memory_mb": env.get("memory_mb"),
        "cpus": env.get("cpus"),
        "gpus": env.get("gpus") or 0,
        "agent_network_mode": agent.get("network_mode"),
        "agent_timeout_sec": agent.get("timeout_sec"),
        "verifier_network_mode": verifier.get("network_mode"),
        "verifier_environment_mode": verifier.get("environment_mode"),
        "verifier_timeout_sec": verifier.get("timeout_sec"),
        "build_timeout_sec": env.get("build_timeout_sec"),
        "artifacts": config.get("artifacts") or [],
    }


_runtime_prefix: list[str] | None = None
_runtime_lock = threading.Lock()


def runtime_prefix() -> list[str]:
    """返回与 Pier 使用同一 store 的命令前缀。

    Pier 的 `docker compose` 包装器把 DOCKER_HOST 指向
    unix:///run/user/<uid>/podman/podman.sock，对应 rootless podman 的
    remote store；直接 `podman` / `docker`（非 compose）看的是本地 store，
    两者镜像与容器列表不同。这里优先 `podman --remote`，保证检查/清理的
    就是 Pier 真正在用的那一套。
    """
    global _runtime_prefix
    with _runtime_lock:
        if _runtime_prefix is not None:
            return _runtime_prefix
        candidates: list[list[str]] = []
        if shutil.which("podman"):
            candidates.append(["podman", "--remote"])
            candidates.append(["podman"])
        if shutil.which("docker"):
            candidates.append(["docker"])
        for candidate in candidates:
            try:
                result = subprocess.run(
                    [*candidate, "info", "--format", "{{.Store.GraphRoot}}"],
                    capture_output=True,
                    text=True,
                    timeout=30,
                    check=False,
                )
            except (OSError, subprocess.TimeoutExpired):
                continue
            if result.returncode == 0:
                _runtime_prefix = candidate
                return candidate
        _runtime_prefix = []
        return _runtime_prefix


def runtime_command(*args: str, timeout: float = 30) -> subprocess.CompletedProcess | None:
    """只读地调用与 Pier 同 store 的运行时；失败返回 None，不影响主流程。"""
    prefix = runtime_prefix()
    if not prefix:
        return None
    try:
        return subprocess.run(
            [*prefix, *args],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None


def image_present(image: str | None) -> bool | None:
    if not image:
        return None
    result = runtime_command("image", "inspect", image, "--format", "{{.Id}}")
    if result is None:
        return None
    return result.returncode == 0


def list_containers() -> list[str]:
    result = runtime_command("ps", "-a", "--format", "{{.Names}}")
    if result is None or result.returncode != 0:
        return []
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def cleanup_own_containers(prefixes: list[str]) -> list[str]:
    """只删除本次 trial 前缀命名的容器；不碰镜像，也不碰其它前缀。"""
    if not prefixes:
        return []
    names = list_containers()
    victims = [
        name
        for name in names
        if any(name.startswith(prefix.lower()) for prefix in prefixes)
    ]
    removed: list[str] = []
    for name in victims:
        result = runtime_command("rm", "-f", name, timeout=60)
        if result is not None and result.returncode == 0:
            removed.append(name)
    return removed


def parse_ctrf(trial_dir: Path) -> dict:
    """汇总 verifier 目录下所有 CTRF 报告，返回测试计数与失败用例名。"""
    ctrf_files = sorted(
        p
        for p in (trial_dir / "verifier").rglob("*.json")
        if "ctrf" in p.name.lower()
    )
    tests: dict[str, str] = {}
    summaries: list[dict] = []
    for path in ctrf_files:
        try:
            data = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        results = (data or {}).get("results") or {}
        summary = results.get("summary")
        if isinstance(summary, dict):
            summaries.append({"path": str(path), **summary})
        for test in results.get("tests") or []:
            name = test.get("name")
            status = test.get("status")
            if isinstance(name, str) and isinstance(status, str):
                tests[name] = status
    failed = sorted(name for name, status in tests.items() if status != "passed")
    return {
        "files": [str(p) for p in ctrf_files],
        "summaries": summaries,
        "tests": len(tests),
        "passed": sum(1 for s in tests.values() if s == "passed"),
        "failed": len(failed),
        "failed_tests": failed,
    }


def classify_failed_tests(failed: list[str], config_path: Path) -> dict:
    """把失败用例对回 tests/config.json 的 f2p/p2p 白名单（尽力而为）。"""
    try:
        config = json.loads(config_path.read_text())
    except (OSError, json.JSONDecodeError):
        return {"f2p": [], "p2p": []}
    f2p = set(config.get("f2p_node_ids") or [])
    p2p = set(config.get("p2p_node_ids") or [])

    def normalize(name: str) -> str:
        return re.sub(r"^\[(f2p|p2p)\]\s*", "", name)

    out = {"f2p": [], "p2p": []}
    for name in failed:
        bare = normalize(name)
        if name in f2p or bare in f2p:
            out["f2p"].append(name)
        elif name in p2p or bare in p2p:
            out["p2p"].append(name)
    return out


def copy_evidence(trial_dir: Path, target_dir: Path) -> dict[str, str]:
    """复制关键证据到批次目录；原始 job 目录保持不动。"""
    copied: dict[str, str] = {}
    for rel in EVIDENCE_FILES:
        source = trial_dir / rel
        if not source.is_file():
            continue
        destination = target_dir / rel
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        copied[rel] = str(destination)
    return copied


def find_trial_dirs(job_dir: Path) -> list[Path]:
    if not job_dir.is_dir():
        return []
    return sorted(
        path
        for path in job_dir.iterdir()
        if path.is_dir() and (path / "result.json").is_file()
    )


def summarize_trial(trial_dir: Path, task_dir: Path) -> dict:
    result_path = trial_dir / "result.json"
    trial: dict = {}
    try:
        trial = json.loads(result_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        return {
            "trial_name": trial_dir.name,
            "trial_dir": str(trial_dir),
            "status": "error",
            "notes": [f"无法读取 trial result.json: {exc}"],
        }

    rewards = ((trial.get("verifier_result") or {}).get("rewards")) or {}
    reward = rewards.get("reward")
    exception = trial.get("exception_info")

    verifier_dir = trial_dir / "verifier"
    reward_json = verifier_dir / "reward.json"
    reward_txt = verifier_dir / "reward.txt"
    run_log = verifier_dir / "run.log"
    test_stdout = verifier_dir / "test-stdout.txt"

    # 真正的判分证据：verifier_result 非空，且 verifier 写了 reward 文件/日志。
    graded = bool(trial.get("verifier_result")) and (
        reward_json.is_file() or reward_txt.is_file()
    )
    verifier_output_present = run_log.is_file() or test_stdout.is_file()

    if exception:
        status = "error"
    elif not graded:
        status = "not_graded"
    elif reward == 1:
        status = "passed"
    else:
        status = "failed"

    timings = {
        "environment_setup_sec": span_seconds(
            (trial.get("environment_setup") or {}).get("started_at"),
            (trial.get("environment_setup") or {}).get("finished_at"),
        ),
        "agent_setup_sec": span_seconds(
            (trial.get("agent_setup") or {}).get("started_at"),
            (trial.get("agent_setup") or {}).get("finished_at"),
        ),
        "agent_execution_sec": span_seconds(
            (trial.get("agent_execution") or {}).get("started_at"),
            (trial.get("agent_execution") or {}).get("finished_at"),
        ),
        "verifier_sec": span_seconds(
            (trial.get("verifier") or {}).get("started_at"),
            (trial.get("verifier") or {}).get("finished_at"),
        ),
        "trial_total_sec": span_seconds(
            trial.get("started_at"), trial.get("finished_at")
        ),
    }

    patch_path = trial_dir / "artifacts" / "model.patch"
    patch = {
        "path": str(patch_path) if patch_path.is_file() else None,
        "bytes": patch_path.stat().st_size if patch_path.is_file() else None,
        "sha256": sha256_file(patch_path),
    }

    ctrf = parse_ctrf(trial_dir)
    config_path = task_dir / "tests" / "config.json"
    failed_by_kind = classify_failed_tests(ctrf["failed_tests"], config_path)

    evidence = {
        "trial_result": str(result_path),
        "reward_json": str(reward_json) if reward_json.is_file() else None,
        "reward_txt": str(reward_txt) if reward_txt.is_file() else None,
        "verifier_run_log": str(run_log) if run_log.is_file() else None,
        "verifier_test_stdout": str(test_stdout) if test_stdout.is_file() else None,
        "agent_oracle_log": str(trial_dir / "agent" / "oracle.txt")
        if (trial_dir / "agent" / "oracle.txt").is_file()
        else None,
        "trial_log": str(trial_dir / "trial.log")
        if (trial_dir / "trial.log").is_file()
        else None,
        "ctrf_files": ctrf["files"],
        "artifacts_manifest": str(trial_dir / "artifacts" / "manifest.json")
        if (trial_dir / "artifacts" / "manifest.json").is_file()
        else None,
    }

    notes: list[str] = []
    if graded and not verifier_output_present:
        notes.append("有 reward 文件但没有 verifier run.log/test-stdout，需人工确认")
    if graded and ctrf["tests"] == 0:
        notes.append("CTRF 没有解析到测试用例，需人工确认 verifier 是否真的跑了测试")

    return {
        "trial_name": trial_dir.name,
        "trial_dir": str(trial_dir),
        "status": status,
        "reward": reward,
        "f2p": {
            "ratio": rewards.get("f2p"),
            "passed": rewards.get("f2p_passed"),
            "total": rewards.get("f2p_total"),
        },
        "p2p": {
            "ratio": rewards.get("p2p"),
            "passed": rewards.get("p2p_passed"),
            "total": rewards.get("p2p_total"),
        },
        "partial": rewards.get("partial"),
        "rewards_raw": rewards,
        "exception_info": exception,
        "timings": timings,
        "patch": patch,
        "verifier_tests": ctrf,
        "failed_tests_by_kind": failed_by_kind,
        "evidence": evidence,
        "graded": graded,
        "notes": notes,
    }


def build_command(task_dir: Path, job_name: str) -> list[str]:
    return [
        str(RUN_PIER),
        "-p",
        str(task_dir),
        "--agent",
        "oracle",
        "--max-retries",
        "0",
        "-n",
        "1",
        "-o",
        str(JOBS_ROOT),
        "--job-name",
        job_name,
        # Pier 默认 `down --rmi all` 会删掉官方共享预构建镜像；只 down 容器。
        "--no-delete",
        "-y",
    ]


def child_env() -> dict[str, str]:
    env = os.environ.copy()
    # oracle 不需要上游；删掉后 run_pier.sh 不会追加 --ae BENCH_API_KEY。
    env.pop("BENCH_API_KEY", None)
    env["SCRATCH"] = str(SCRATCH)
    env["PYTHONUNBUFFERED"] = "1"
    return env


def run_pier_process(
    command: list[str], log_path: Path, timeout_sec: float
) -> tuple[int | None, float, bool]:
    """运行一个 pier 进程；超时则杀整个进程组，返回 (rc, wall, timed_out)。"""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    timed_out = False
    with log_path.open("wb") as handle:
        process = subprocess.Popen(
            command,
            cwd=str(REPO),
            env=child_env(),
            stdout=handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            returncode = process.wait(timeout=timeout_sec)
        except subprocess.TimeoutExpired:
            timed_out = True
            try:
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
            returncode = process.wait()
    return returncode, round(time.monotonic() - started, 3), timed_out


def run_one(task: str, run_id: str, run_dir: Path, meta: dict, timeout_sec: float) -> dict:
    task_dir = TASKS_ROOT / task
    job_name = f"oracle-align-{run_id}-{task}"
    job_dir = JOBS_ROOT / job_name
    log_path = run_dir / "logs" / f"{task}.pier.log"
    task_result_path = run_dir / "tasks" / f"{task}.json"

    record: dict = {
        "task_id": task,
        "task_dir": str(task_dir),
        "job_name": job_name,
        "job_dir": str(job_dir),
        "pier_log": str(log_path),
        "run_id": run_id,
        "task_meta": meta,
        "status": "pending",
        "started_at": utc_now(),
        "notes": [],
    }
    hashes_before = task_hashes(task_dir)
    record["official_hashes_before"] = hashes_before
    record["image_present_before"] = image_present(meta.get("docker_image"))

    if job_dir.exists():
        record["status"] = "error"
        record["notes"].append(f"job 目录已存在，拒绝覆盖：{job_dir}")
        record["finished_at"] = utc_now()
        write_json_atomic(task_result_path, record)
        return record

    command = build_command(task_dir, job_name)
    record["pier_command"] = command
    log(f"[{task}] 启动 oracle job（日志 {log_path}）")

    returncode, wall_sec, timed_out = run_pier_process(command, log_path, timeout_sec)
    record["pier_exit_code"] = returncode
    record["pier_wall_sec"] = wall_sec
    record["pier_timed_out"] = timed_out

    trial_dirs = find_trial_dirs(job_dir)
    record["n_trials_found"] = len(trial_dirs)
    trials = [summarize_trial(trial_dir, task_dir) for trial_dir in trial_dirs]

    if len(trials) == 1:
        primary = trials[0]
        record.update(
            {
                "trial_name": primary.get("trial_name"),
                "trial_dir": primary.get("trial_dir"),
                "status": primary.get("status"),
                "reward": primary.get("reward"),
                "f2p": primary.get("f2p"),
                "p2p": primary.get("p2p"),
                "partial": primary.get("partial"),
                "rewards_raw": primary.get("rewards_raw"),
                "exception_info": primary.get("exception_info"),
                "timings": primary.get("timings"),
                "patch": primary.get("patch"),
                "verifier_tests": primary.get("verifier_tests"),
                "failed_tests_by_kind": primary.get("failed_tests_by_kind"),
                "evidence": primary.get("evidence"),
                "graded": primary.get("graded"),
                "notes": record["notes"] + list(primary.get("notes") or []),
            }
        )
    elif len(trials) > 1:
        record["status"] = "error"
        record["notes"].append(f"一个 job 出现 {len(trials)} 个 trial，预期 1 个")
        record["trials"] = trials
    else:
        record["status"] = "error"
        record["notes"].append("job 目录里没有 trial result.json，Pier 可能在建环境阶段失败")

    # 只在能确定 trial 名时清理本 trial 的残留容器；镜像一律不删。
    prefixes: list[str] = []
    if record.get("trial_name"):
        prefixes.append(str(record["trial_name"]))
    removed = cleanup_own_containers(prefixes)
    record["containers_removed"] = removed
    record["images_removed"] = []

    hashes_after = task_hashes(task_dir)
    record["official_hashes_after"] = hashes_after
    record["official_unchanged"] = hashes_after == hashes_before
    if hashes_after != hashes_before:
        record["status"] = "error"
        record["notes"].append("官方 task.toml/solution/tests 在运行期间发生变化")

    if record.get("trial_dir"):
        evidence_dir = run_dir / "evidence" / task
        record["evidence_copies"] = copy_evidence(Path(record["trial_dir"]), evidence_dir)

    record["finished_at"] = utc_now()
    write_json_atomic(task_result_path, record)

    reward_text = record.get("reward")
    log(
        f"[{task}] status={record['status']} reward={reward_text} "
        f"pier_rc={returncode} wall={wall_sec}s"
    )
    return record


def preflight(tasks: list[str]) -> tuple[list[str], dict]:
    errors: list[str] = []
    if not RUN_PIER.is_file():
        errors.append(f"缺少运行器：{RUN_PIER}")
    if not SAMPLE_PATH.is_file():
        errors.append(f"缺少清单：{SAMPLE_PATH}")
    for task in tasks:
        task_dir = TASKS_ROOT / task
        if not task_dir.is_dir():
            errors.append(f"任务目录不存在：{task_dir}")
            continue
        for rel in ("task.toml", "solution/solve.sh", "tests/test.sh", "tests/config.json"):
            if not (task_dir / rel).is_file():
                errors.append(f"{task} 缺少官方文件 {rel}")
        try:
            meta = read_task_meta(task_dir)
        except (OSError, tomllib.TOMLDecodeError) as exc:
            errors.append(f"{task} task.toml 无法解析：{exc}")
            continue
        if not meta.get("docker_image"):
            errors.append(f"{task} task.toml 没有 environment.docker_image")
    runtime = runtime_command("info", "--format", "{{.Store.GraphRoot}}")
    runtime_ok = runtime is not None and runtime.returncode == 0
    runtime_root = runtime.stdout.strip() if runtime_ok and runtime.stdout else None
    if not runtime_ok:
        errors.append(
            "找不到与 Pier 同 store 的 podman remote socket；请在 `devenv shell -- ...` 内运行"
        )
    sample_sha = sha256_file(SAMPLE_PATH)
    return errors, {
        "runtime_ok": runtime_ok,
        "runtime": " ".join(runtime_prefix()) if runtime_ok else None,
        "runtime_graphroot": runtime_root,
        "sample_sha256": sample_sha,
        "run_pier": str(RUN_PIER),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--run-id",
        default=datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ"),
        help="批次标识；同时决定输出目录和 job 名前缀",
    )
    parser.add_argument(
        "--tasks",
        default="",
        help="只跑这些任务（逗号分隔），默认 sample.json 的 selected 全部",
    )
    parser.add_argument("--note", default="", help="写进 environment.json 的批次说明")
    parser.add_argument("--dry-run", action="store_true", help="只预检并打印命令")
    args = parser.parse_args()

    run_id = re.sub(r"[^0-9A-Za-z._-]", "-", args.run_id)
    run_dir = RUNS_ROOT / run_id

    sample = json.loads(SAMPLE_PATH.read_text())
    selected = list(sample.get("selected") or [])
    if not selected:
        print(f"sample.json 没有 selected：{SAMPLE_PATH}", file=sys.stderr)
        return 1

    if args.tasks:
        requested = [item.strip() for item in args.tasks.split(",") if item.strip()]
        unknown = [item for item in requested if item not in selected]
        if unknown:
            print(f"--tasks 不在冻结清单里：{unknown}", file=sys.stderr)
            return 1
        tasks = requested
    else:
        tasks = selected

    if run_dir.exists():
        print(f"批次目录已存在，拒绝覆盖：{run_dir}", file=sys.stderr)
        return 1

    errors, preflight_info = preflight(tasks)
    if errors:
        for message in errors:
            print(f"预检失败：{message}", file=sys.stderr)
        return 1

    if args.dry_run:
        print(f"run_id={run_id} tasks={len(tasks)} concurrency={CONCURRENCY}")
        for task in tasks:
            print(" ".join(build_command(TASKS_ROOT / task, f"oracle-align-{run_id}-{task}")))
        return 0

    run_dir.mkdir(parents=True)
    (run_dir / "tasks").mkdir()
    (run_dir / "logs").mkdir()
    events_path = run_dir / "events.jsonl"
    started_at = utc_now()

    environment = {
        "run_id": run_id,
        "started_at": started_at,
        "note": args.note,
        "sample_path": str(SAMPLE_PATH),
        "sample_sha256": preflight_info["sample_sha256"],
        "tasks_root": str(TASKS_ROOT),
        "jobs_root": str(JOBS_ROOT),
        "run_dir": str(run_dir),
        "selected_count": len(selected),
        "run_count": len(tasks),
        "concurrency": CONCURRENCY,
        "pier": {
            "runner": str(RUN_PIER),
            "agent": "oracle",
            "max_retries": 0,
            "n_concurrent_per_job": 1,
            "delete_environment": False,
            "extra_flags": ["--no-delete", "-y"],
            "note": (
                "默认 --delete 会对 agent 环境执行 docker compose down --rmi all，"
                "而 agent service 直接使用 task.toml 的官方预构建镜像；"
                "这里用 --no-delete 只清本 trial 容器，保留共享镜像。"
            ),
        },
        "bench_api_key": {
            "present_in_parent": "BENCH_API_KEY" in os.environ,
            "passed_to_pier": False,
            "note": "脚本从子进程环境删除 BENCH_API_KEY，run_pier.sh 不会追加 --ae。",
        },
        "llm_used": False,
        "official_files_modified": False,
        "images_removed_by_script": False,
        "runtime": preflight_info,
        "tasks": tasks,
    }
    write_json_atomic(run_dir / "environment.json", environment)
    log(f"批次 {run_id}：{len(tasks)} 题，并发 {CONCURRENCY}，输出 {run_dir}")

    # 超时只是防挂死的保护；不改变 Pier 自己的 task/verifier 超时。
    default_timeout = 4 * 60 * 60
    results: list[dict] = []

    def work(task: str) -> dict:
        try:
            meta = read_task_meta(TASKS_ROOT / task)
        except Exception as exc:  # noqa: BLE001 - 记录后继续其它题
            record = {
                "task_id": task,
                "status": "error",
                "notes": [f"读取 task.toml 失败：{exc}"],
                "finished_at": utc_now(),
            }
            write_json_atomic(run_dir / "tasks" / f"{task}.json", record)
            return record
        budget = sum(
            float(value or 0)
            for value in (
                meta.get("build_timeout_sec"),
                meta.get("agent_timeout_sec"),
                meta.get("verifier_timeout_sec"),
            )
        )
        timeout_sec = (budget or default_timeout) + DEFAULT_TIMEOUT_MARGIN_SEC
        return run_one(task, run_id, run_dir, meta, timeout_sec)

    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        futures = {pool.submit(work, task): task for task in tasks}
        for future in concurrent.futures.as_completed(futures):
            task = futures[future]
            try:
                record = future.result()
            except Exception as exc:  # noqa: BLE001 - 单题异常不拖垮整批
                record = {
                    "task_id": task,
                    "status": "error",
                    "notes": [f"未捕获异常：{type(exc).__name__}: {exc}"],
                    "finished_at": utc_now(),
                }
                write_json_atomic(run_dir / "tasks" / f"{task}.json", record)
            results.append(record)
            append_event(
                events_path,
                {
                    "ts": utc_now(),
                    "task_id": task,
                    "status": record.get("status"),
                    "reward": record.get("reward"),
                    "pier_exit_code": record.get("pier_exit_code"),
                    "task_result": str(run_dir / "tasks" / f"{task}.json"),
                },
            )

    results.sort(key=lambda item: item["task_id"])
    counts = {
        "passed": sum(1 for r in results if r.get("status") == "passed"),
        "failed": sum(1 for r in results if r.get("status") == "failed"),
        "error": sum(1 for r in results if r.get("status") == "error"),
        "not_graded": sum(1 for r in results if r.get("status") == "not_graded"),
        "pending": sum(1 for r in results if r.get("status") == "pending"),
    }
    summary = {
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": utc_now(),
        "run_dir": str(run_dir),
        "sample_sha256": preflight_info["sample_sha256"],
        "selected_count": len(selected),
        "run_count": len(tasks),
        "concurrency": CONCURRENCY,
        "counts": counts,
        "all_graded": all(r.get("graded") for r in results),
        "all_reward_one": all(r.get("reward") == 1 for r in results),
        "images_removed_by_script": False,
        "results": [
            {
                "task_id": r.get("task_id"),
                "status": r.get("status"),
                "reward": r.get("reward"),
                "f2p": r.get("f2p"),
                "p2p": r.get("p2p"),
                "exception_type": (r.get("exception_info") or {}).get("exception_type")
                if isinstance(r.get("exception_info"), dict)
                else None,
                "pier_exit_code": r.get("pier_exit_code"),
                "pier_wall_sec": r.get("pier_wall_sec"),
                "agent_execution_sec": (r.get("timings") or {}).get(
                    "agent_execution_sec"
                ),
                "verifier_sec": (r.get("timings") or {}).get("verifier_sec"),
                "patch_bytes": (r.get("patch") or {}).get("bytes"),
                "graded": r.get("graded"),
                "official_unchanged": r.get("official_unchanged"),
                "job_dir": r.get("job_dir"),
                "task_result": str(run_dir / "tasks" / f"{r.get('task_id')}.json"),
                "notes": r.get("notes") or [],
            }
            for r in results
        ],
    }
    write_json_atomic(run_dir / "summary.json", summary)

    log(
        f"完成：passed={counts['passed']} failed={counts['failed']} "
        f"error={counts['error']} not_graded={counts['not_graded']}；"
        f"summary={run_dir / 'summary.json'}"
    )
    return 0 if summary["all_reward_one"] and summary["all_graded"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
