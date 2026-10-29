#!/usr/bin/env python3
"""官方镜像 ↔ DSH 适配层运行环境对齐验证器（仅标准库）。

用途
- 对冻结清单 `SCRATCH/sample.json` 的 20 个任务逐题做两件事：
  1. 原镜像基线：直接用官方任务镜像起一次性容器（docker run --network none -w /app
     --user 0），采集工作目录、用户、HOME、PATH、语言工具链位置、包管理器与缓存路径。
  2. 适配器运行：走真实 run_pier.sh + DshAgent + 本地假上游（mock），让 DSH 在任务容器里
     真正调用一次 bash 工具；mock 只把探针脚本交给 DSH 执行，探针结果写入 /logs/agent，
     不注入任何任务解决方案。之后照常运行官方 verifier 并记录 reward。
- 逐项比较基线/适配器 fingerprint；Anko 额外在 DSH bash 工具内部执行完整
  `go test -count=1 ./...`，作为旧 HOME=/tmp/dsh-home 回归。
- 只要 `--task` 可只跑子集（例如 Anko 回归）；没有、也不会提供旧 HOME 运行开关。
  旧失败的证据只以引用形式写入 summary.json，不重发模型。

输入/输出
- 输入：SCRATCH/sample.json、SCRATCH/deep-swe/tasks/<id>、官方任务镜像。
- 输出：SCRATCH/probes/environment-alignment/<run-id>/ 下 baseline/、tasks/、jobs/、
  logs/、mock/、adapter/、attempts.jsonl、cleanup.json、summary.json。
- 所有尝试都保留；job 目录/日志不清理。容器只在确认属于本 run 的 trial 时回收。

安全边界
- 只用假 key（BENCH_API_KEY=env-align-dummy-not-a-real-key），不读取任何真实凭据。
- 任务容器保持 no-network；宿主 mock 只 bind 127.0.0.1，端口默认 29120-29149。
- 不修改官方任务、不会改 sample.json；pilot 副本写到本 run 目录。
- 并发上限硬编码为 2；一次 run 不跑 oracle。

用法（必须在 devenv shell 内；脚本也会尝试自动 re-exec）：
    devenv shell -- python3 video/chat-reasoning-replay/project/scripts/pier/validate_environment.py
    devenv shell -- python3 .../validate_environment.py --task anko-default-function-arguments --run-id 20261026T...Z
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import tomllib
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

SCHEMA = 1
MAX_CONCURRENCY = 2
DSH_VERSION = "0.1.0-rc.8"
DSH_HOME = "/opt/pier-dsh/home"
ANKO_TASK = "anko-default-function-arguments"
INK_TASK = "ink-grid-box-layout"
KEY_ENV = "BENCH_API_KEY"
DUMMY_KEY = "env-align-dummy-not-a-real-key"
SENSITIVE = re.compile(r"(KEY|PASSWORD|SECRET|TOKEN|CREDENTIAL)", re.IGNORECASE)
IGNORED_ENV_NAMES = {"_", "SHLVL", "OLDPWD"}
EXPECTED_ADAPTER_ENV = {
    "DSH_HOME", "DSH_SHELL", "DSH_SESSION_ID", "DSH_SESSION_JSONL",
    "NO_COLOR", "PAGER", "GIT_PAGER", "TERM",
    # DSH 的 Node 运行时（sharp/libvips）在进程内设置；不是任务工具链路径。
    "VIPSHOME",
}
EXPECTED_ADAPTER_ENV_REASONS = {
    "VIPSHOME": "DSH Node 运行时 sharp/libvips 在进程内设置；非任务工具链变量",
}
EXPECTED_BASELINE_ENV = {
    "HOSTNAME", "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY",
    "http_proxy", "https_proxy", "no_proxy",
}
EXPECTED_BASELINE_ENV_REASONS = {
    "HOSTNAME": "docker run 直跑时 runtime 注入；Pier no-network 容器不带，任务不依赖",
    "HTTP_PROXY": "docker CLI 代理配置注入；任务容器 no-network，不使用",
    "HTTPS_PROXY": "docker CLI 代理配置注入；任务容器 no-network，不使用",
    "NO_PROXY": "docker CLI 代理配置注入；任务容器 no-network，不使用",
    "http_proxy": "docker CLI 代理配置注入；任务容器 no-network，不使用",
    "https_proxy": "docker CLI 代理配置注入；任务容器 no-network，不使用",
    "no_proxy": "docker CLI 代理配置注入；任务容器 no-network，不使用",
}

REPO = Path(__file__).resolve().parents[5]
SCRIPTS = Path(__file__).resolve().parent
SCRATCH = REPO / ".scratch/research/chat-reasoning-replay/pier"
SAMPLE = SCRATCH / "sample.json"
TASK_SRC = SCRATCH / "deep-swe/tasks"
RUN_ROOT = SCRATCH / "probes/environment-alignment"
PIER_RUN = SCRIPTS / "run_pier.sh"
PREPARE = SCRIPTS / "prepare_pilot_task.py"
MOCK = SCRIPTS / "mock_openai_server.py"
DSH_BUNDLE = SCRATCH / "dsh-bundle.tar.gz"

LANGUAGE_TOOLS = {
    "go": ["go", "git", "bash", "timeout"],
    "typescript": ["node", "git", "bash", "timeout"],
    "javascript": ["node", "git", "bash", "timeout"],
    "python": ["python3", "pip3", "pytest", "git", "bash", "timeout"],
    "rust": ["cargo", "rustc", "git", "bash", "timeout"],
}
LEGACY_HOME_FAILURE_EVIDENCE = [
    {
        "task_id": ANKO_TASK,
        "kind": "old-adapter-real-model-run",
        "path": "batches/pilot-20261026/jobs/pilot-20261026--anko-default-function-arguments--r1--sub2api-a1/"
                "anko-default-function-arguments__nNFHL4d/agent/dsh-stdout.txt",
        "quote": "The full `go test ./...` run was blocked by the existing `TestRunInteractive` "
                 "fixture setup: it could not open `/tmp/dsh-home/go/bin/anko_test.log`.",
    },
    {
        "task_id": ANKO_TASK,
        "kind": "old-adapter-real-model-run",
        "path": "batches/pilot-20261026/jobs/pilot-20261026--anko-default-function-arguments--r1--stravia-a1/"
                "anko-default-function-arguments__sesZUaE/agent/dsh-stdout.txt",
        "quote": "`go test ./...` also fails in the existing top-level test because "
                 "`/tmp/dsh-home/go/bin/anko_test.log` is missing.",
    },
    {
        "task_id": ANKO_TASK,
        "kind": "old-adapter-real-model-run",
        "path": "batches/pilot-20261026/jobs/pilot-20261026--anko-default-function-arguments--r1--cpa-a1/"
                "anko-default-function-arguments__3xTKBsB/agent/dsh-stdout.txt",
        "quote": "an unrelated existing failure in `TestRunInteractive` due to a missing "
                 "`/tmp/dsh-home/go/bin` directory.",
    },
    {
        "task_id": ANKO_TASK,
        "kind": "old-launcher-source",
        "path": "batches/pilot-20261026/jobs/pilot-20261026--anko-default-function-arguments--r1--sub2api-a1/"
                "anko-default-function-arguments__nNFHL4d/agent/run-dsh.sh",
        "quote": "export HOME=/tmp/dsh-home",
        "note": "旧 launcher 的 HOME 覆盖；当前 dsh_agent.py 已删除该行。本验证器不重跑旧配置。",
    },
]


def now_utc() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    try:
        return sha256_bytes(path.read_bytes())
    except OSError:
        return ""


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    os.replace(tmp, path)


def read_json(path: Path):
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None


def tail_text(path: Path, limit: int = 4000) -> str:
    try:
        return path.read_text(errors="replace")[-limit:]
    except OSError:
        return ""


def run_capture(cmd, *, timeout=300, input_text=None, cwd=None, env=None):
    started = time.monotonic()
    try:
        proc = subprocess.run(
            cmd,
            input=input_text,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            cwd=str(cwd) if cwd else None,
            env=env,
        )
        return {
            "cmd": [str(item) for item in cmd],
            "rc": proc.returncode,
            "stdout": proc.stdout,
            "stderr": proc.stderr,
            "timed_out": False,
            "duration_seconds": round(time.monotonic() - started, 3),
        }
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout or ""
        stderr = exc.stderr or ""
        if isinstance(stdout, bytes):
            stdout = stdout.decode("utf-8", "replace")
        if isinstance(stderr, bytes):
            stderr = stderr.decode("utf-8", "replace")
        return {
            "cmd": [str(item) for item in cmd],
            "rc": None,
            "stdout": stdout,
            "stderr": stderr,
            "timed_out": True,
            "duration_seconds": round(time.monotonic() - started, 3),
        }
    except FileNotFoundError as exc:
        return {
            "cmd": [str(item) for item in cmd],
            "rc": None,
            "stdout": "",
            "stderr": f"{type(exc).__name__}: {exc}",
            "timed_out": False,
            "duration_seconds": round(time.monotonic() - started, 3),
        }


def _kill_tree(proc: subprocess.Popen) -> None:
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except OSError:
        try:
            proc.terminate()
        except OSError:
            pass
    try:
        proc.wait(timeout=10)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except OSError:
        try:
            proc.kill()
        except OSError:
            pass
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        pass


def run_logged(cmd, *, log_path: Path, timeout=3000, cwd=None, env=None):
    started = time.monotonic()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    timed_out = False
    with log_path.open("wb") as log:
        proc = subprocess.Popen(
            [str(item) for item in cmd],
            cwd=str(cwd) if cwd else None,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            rc = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            _kill_tree(proc)
            rc = proc.returncode
    return {
        "cmd": [str(item) for item in cmd],
        "rc": rc,
        "timed_out": timed_out,
        "duration_seconds": round(time.monotonic() - started, 3),
        "log_path": str(log_path),
    }


def docker_capture(args, *, timeout=300, input_text=None):
    return run_capture(["docker", *args], timeout=timeout, input_text=input_text)


def docker_server_ok() -> bool:
    if not shutil.which("docker"):
        return False
    res = docker_capture(["version", "--format", "{{.Server.Version}}"], timeout=30)
    return res.get("rc") == 0


def ensure_docker(no_reexec: bool = False) -> None:
    if docker_server_ok():
        return
    if no_reexec or os.environ.get("ENV_ALIGN_DEVENV") == "1":
        raise SystemExit(
            "docker CLI/服务不可用；请在 devenv shell 内运行："
            "devenv shell -- python3 " + str(Path(__file__).resolve())
        )
    devenv = shutil.which("devenv")
    if not devenv:
        raise SystemExit("找不到 docker 与 devenv；无法在宿主运行容器")
    env = os.environ.copy()
    env["ENV_ALIGN_DEVENV"] = "1"
    os.chdir(REPO)
    os.execvpe(
        devenv,
        [devenv, "shell", "--", "python3", str(Path(__file__).resolve()), *sys.argv[1:]],
        env,
    )


def sanitize_env_list(items):
    out = []
    for item in items or []:
        if "=" not in item:
            out.append(item)
            continue
        name, value = item.split("=", 1)
        out.append(f"{name}=<redacted>" if SENSITIVE.search(name) else f"{name}={value}")
    return out


def docker_ps_names():
    res = docker_capture(["ps", "-a", "--format", "{{.Names}}"], timeout=60)
    return [line.strip() for line in (res.get("stdout") or "").splitlines() if line.strip()]


def load_sample():
    data = json.loads(SAMPLE.read_text())
    selected = list(data.get("selected") or [])
    if len(selected) != 20:
        raise SystemExit(f"sample.json selected 不是 20 题：{len(selected)}")
    digest = sha256_bytes(
        json.dumps(selected, ensure_ascii=False, separators=(",", ":")).encode()
    )
    return data, selected, digest


def parse_require_cmds(text: str):
    names = []
    for match in re.finditer(r"require_cmd\s+([^\s;]+)", text):
        name = match.group(1).strip("\"'")
        if re.fullmatch(r"[A-Za-z0-9_.+-]+", name):
            names.append(name)
    return sorted(set(names))


def task_meta(task_id: str):
    src = TASK_SRC / task_id
    toml_path = src / "task.toml"
    cfg = tomllib.loads(toml_path.read_text())
    env_cfg = cfg.get("environment") or {}
    agent_cfg = cfg.get("agent") or {}
    verifier_cfg = cfg.get("verifier") or {}
    metadata = cfg.get("metadata") or {}
    instruction = (src / "instruction.md").read_bytes()
    test_sh = (src / "tests" / "test.sh").read_text(errors="replace")
    tests_dockerfile = (src / "tests" / "Dockerfile").read_text(errors="replace")
    from_match = re.search(r"^FROM\s+(\S+)", tests_dockerfile, re.MULTILINE)
    config_json = read_json(src / "tests" / "config.json") or {}
    return {
        "task_id": task_id,
        "source_path": str(src),
        "language": metadata.get("language"),
        "image": env_cfg.get("docker_image"),
        "image_workdir_declared": env_cfg.get("workdir"),
        "cpus": env_cfg.get("cpus"),
        "memory_mb": env_cfg.get("memory_mb"),
        "storage_mb": env_cfg.get("storage_mb"),
        "gpus": env_cfg.get("gpus"),
        "agent_network_mode": agent_cfg.get("network_mode"),
        "agent_timeout_sec": agent_cfg.get("timeout_sec"),
        "verifier_network_mode": verifier_cfg.get("network_mode"),
        "verifier_environment_mode": verifier_cfg.get("environment_mode"),
        "verifier_timeout_sec": verifier_cfg.get("timeout_sec"),
        "environment_env_keys": sorted((env_cfg.get("env") or {}).keys()),
        "verifier_env_keys": sorted(((verifier_cfg.get("env")) or {}).keys()),
        "tests_base_image": from_match.group(1) if from_match else None,
        "tests_dockerfile_sha256": sha256_bytes(tests_dockerfile.encode()),
        "tests_dockerfile_installs": bool(
            re.search(r"^\s*RUN\s+.*(pip|npm|pnpm|yarn|apt-get|cargo install)", tests_dockerfile, re.MULTILINE)
        ),
        "test_sh_sha256": sha256_bytes(test_sh.encode()),
        "verifier_required_commands": parse_require_cmds(test_sh),
        "instruction_path": str(src / "instruction.md"),
        "instruction_bytes": instruction,
        "instruction_sha256": sha256_bytes(instruction),
        "instruction_length": len(instruction),
        "instruction_first_char": instruction[:1].decode("utf-8", "replace"),
        "instruction_leading_dash": instruction.startswith(b"-"),
        "base_commit": config_json.get("base_commit"),
        "f2p_node_ids": config_json.get("f2p_node_ids") or [],
        "p2p_node_ids": config_json.get("p2p_node_ids") or [],
        "self_compose": (src / "environment" / "docker-compose.yaml").exists(),
    }


def ensure_image(meta, run_dir: Path):
    image = meta["image"]
    record = {"image": image, "pull": None, "inspect": None, "error": None}
    inspect = docker_capture(["image", "inspect", image], timeout=180)
    if inspect.get("rc") != 0:
        pull_log = run_dir / "pull" / f"{meta['task_id']}.log"
        record["pull"] = run_logged(
            ["docker", "pull", image], log_path=pull_log, timeout=1800
        )
        inspect = docker_capture(["image", "inspect", image], timeout=180)
    if inspect.get("rc") != 0:
        record["error"] = (inspect.get("stderr") or "docker image inspect 失败")[-1000:]
        return record
    try:
        data = json.loads(inspect["stdout"])[0]
    except (json.JSONDecodeError, IndexError) as exc:
        record["error"] = f"无法解析 image inspect：{exc}"
        return record
    config = data.get("Config") or {}
    summary = {
        "Id": data.get("Id"),
        "Size": data.get("Size"),
        "Created": data.get("Created"),
        "Architecture": data.get("Architecture"),
        "Os": data.get("Os"),
        "RepoTags": data.get("RepoTags"),
        "Config": {
            "Env": sanitize_env_list(config.get("Env") or []),
            "Cmd": config.get("Cmd"),
            "Entrypoint": config.get("Entrypoint"),
            "WorkingDir": config.get("WorkingDir"),
            "User": config.get("User"),
            "Labels": config.get("Labels"),
        },
    }
    write_json(run_dir / "baseline" / f"{meta['task_id']}.image.json", summary)
    record["inspect"] = summary
    return record


def run_baseline_probe(meta, run_dir: Path):
    image = meta["image"]
    cmd = [
        "docker", "run", "--rm", "--network", "none", "--workdir", "/app",
        "--user", "0", "-i", image, "bash", "-c",
        'python3 - --mode baseline --out - --tag "$1"', "bash", meta["task_id"],
    ]
    res = run_capture(cmd, timeout=300, input_text=PROBE_SOURCE)
    out_dir = run_dir / "baseline"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / f"{meta['task_id']}.probe.stdout.txt").write_text(res.get("stdout") or "")
    (out_dir / f"{meta['task_id']}.probe.stderr.txt").write_text(res.get("stderr") or "")
    record = {
        "command": cmd,
        "rc": res.get("rc"),
        "timed_out": res.get("timed_out"),
        "duration_seconds": res.get("duration_seconds"),
        "stderr_tail": (res.get("stderr") or "")[-2000:],
    }
    if res.get("rc") != 0:
        record["error"] = "baseline probe 非零退出"
        return record, None
    try:
        fingerprint = json.loads(res.get("stdout") or "")
    except json.JSONDecodeError as exc:
        record["error"] = f"baseline probe 输出不是 JSON：{exc}"
        return record, None
    record["fingerprint"] = fingerprint
    return record, fingerprint


def capture_baseline_diff(meta, run_dir: Path):
    """官方镜像若在构建期修改了工作树（如 numba 的 llvmlite pin），记录完整 diff 作为基线。"""
    path = run_dir / "baseline" / f"{meta['task_id']}.diff"
    path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "docker", "run", "--rm", "--network", "none", "--workdir", "/app",
        "--user", "0", meta["image"], "git", "-C", "/app", "diff",
    ]
    res = run_capture(cmd, timeout=180)
    text = res.get("stdout") or ""
    path.write_text(text)
    return {
        "cmd": cmd,
        "rc": res.get("rc"),
        "sha256": sha256_bytes(text.encode()),
        "path": str(path),
        "stderr_tail": (res.get("stderr") or "")[-1000:],
    }


def baseline_task(meta, run_dir: Path):
    started = now_utc()
    record = {"task_id": meta["task_id"], "started_at": started}
    image_record = ensure_image(meta, run_dir)
    record["image_record"] = image_record
    if image_record.get("error"):
        record.update({"ok": False, "error": image_record["error"], "finished_at": now_utc()})
        return record
    probe, fingerprint = run_baseline_probe(meta, run_dir)
    record["probe"] = {k: value for k, value in probe.items() if k != "fingerprint"}
    if fingerprint and get_path(fingerprint, "repo.status.stdout"):
        diff = capture_baseline_diff(meta, run_dir)
        fingerprint.setdefault("repo", {})["diff_sha256"] = diff["sha256"]
        fingerprint["repo"]["diff_path"] = diff["path"]
        record["baseline_diff"] = diff
    record["fingerprint"] = fingerprint
    record["ok"] = fingerprint is not None
    if not record["ok"]:
        record["error"] = probe.get("error") or probe.get("stderr_tail")
    record["finished_at"] = now_utc()
    return record


def prepare_task_copies(task_ids, run_dir: Path):
    out_dir = run_dir / "tasks"
    cmd = [sys.executable, str(PREPARE), "--out", str(out_dir), *task_ids]
    res = run_capture(cmd, timeout=600, cwd=REPO)
    log_path = run_dir / "logs" / "prepare.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text((res.get("stdout") or "") + "\n--- stderr ---\n" + (res.get("stderr") or ""))
    manifest = read_json(out_dir / "pilot-manifest.json")
    copies = {}
    for task_id in task_ids:
        copy_dir = out_dir / task_id
        copy_toml = copy_dir / "task.toml"
        source_toml = TASK_SRC / task_id / "task.toml"
        entry = {"path": str(copy_dir), "exists": copy_toml.is_file()}
        if copy_toml.is_file():
            copy_cfg = tomllib.loads(copy_toml.read_text())
            source_cfg = tomllib.loads(source_toml.read_text())
            entry.update({
                "agent_network_mode": (copy_cfg.get("agent") or {}).get("network_mode"),
                "agent_timeout_sec": (copy_cfg.get("agent") or {}).get("timeout_sec"),
                "verifier_unchanged": copy_cfg.get("verifier") == source_cfg.get("verifier"),
                "environment_unchanged": copy_cfg.get("environment") == source_cfg.get("environment"),
                "source_toml_sha256": sha256_file(source_toml),
                "copy_toml_sha256": sha256_file(copy_toml),
            })
        copies[task_id] = entry
    return {"cmd": cmd, "rc": res.get("rc"), "manifest": manifest, "copies": copies}


def build_probe_tool_cmd(task_id: str) -> str:
    encoded = base64.b64encode(PROBE_SOURCE.encode("utf-8")).decode("ascii")
    args = [
        "--mode", "dsh",
        "--out", "/logs/agent/env-fingerprint.json",
        "--tag", task_id,
    ]
    if task_id == ANKO_TASK:
        args.extend(["--go-test", "--go-test-out", "/logs/agent/anko-go-test.txt"])
    quoted_args = " ".join(shlex.quote(item) for item in args)
    command = (
        f"set -o pipefail; printf %s {shlex.quote(encoded)} | base64 -d > /tmp/.env-align-probe.py "
        f"&& python3 /tmp/.env-align-probe.py {quoted_args}; rc=$?; "
        f"rm -f /tmp/.env-align-probe.py; "
        f"if [ \"$rc\" -eq 0 ]; then echo ENV_ALIGN_PROBE_OK; fi; exit \"$rc\""
    )
    # mock_openai_server.py 会做 TOOL_CMD.format(file=...)，这里不能出现裸花括号。
    return command.replace("{", "{{").replace("}", "}}")


def port_free(port: int) -> bool:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", port))
        return True
    except OSError:
        return False
    finally:
        sock.close()


_PORT_LOCK = threading.Lock()
_PORT_USED = set()
_OWN_PROCS = {}
_PROC_LOCK = threading.Lock()


def _register_proc(proc, info):
    with _PROC_LOCK:
        _OWN_PROCS[proc] = info


def _unregister_proc(proc):
    with _PROC_LOCK:
        _OWN_PROCS.pop(proc, None)


def pick_port(base: int, index: int) -> int:
    with _PORT_LOCK:
        preferred = base + index
        candidates = [preferred] + [item for item in range(base, base + 30) if item != preferred]
        for port in candidates:
            if 29120 <= port <= 29149 and port not in _PORT_USED and port_free(port):
                _PORT_USED.add(port)
                return port
    raise RuntimeError(f"在 {base}-{base + 29} 内找不到空闲 mock 端口")


def release_port(port: int) -> None:
    with _PORT_LOCK:
        _PORT_USED.discard(port)


def wait_mock_ready(port: int, proc: subprocess.Popen, timeout=20.0) -> None:
    deadline = time.monotonic() + timeout
    url = f"http://127.0.0.1:{port}/v1/models"
    last_error = None
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"mock 进程提前退出，rc={proc.returncode}")
        try:
            with urllib.request.urlopen(url, timeout=1) as response:
                if response.status == 200:
                    return
        except Exception as exc:  # noqa: BLE001 - 只用于就绪轮询
            last_error = exc
        time.sleep(0.1)
    raise RuntimeError(f"mock 未在 {timeout}s 内就绪：{last_error}")


def start_mock(task_id: str, port: int, run_dir: Path) -> dict:
    log_path = run_dir / "mock" / f"{task_id}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.update({
        "MOCK_LOG": str(run_dir / "mock" / f"{task_id}.jsonl"),
        "MOCK_PORT": str(port),
        "MOCK_BIND": "127.0.0.1",
        "MOCK_MODEL": "gpt-6-luna",
        "MOCK_STEPS": "1",
        "MOCK_DELAY": "0",
        "MOCK_MARKER": "[[env-align-probe]]",
        "MOCK_TOOL_CMD": build_probe_tool_cmd(task_id),
    })
    with log_path.open("ab") as log:
        proc = subprocess.Popen(
            [sys.executable, str(MOCK)],
            cwd=str(REPO),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    _register_proc(proc, {"task_id": task_id, "port": port, "log": str(log_path)})
    try:
        wait_mock_ready(port, proc)
    except Exception:
        _kill_tree(proc)
        _unregister_proc(proc)
        release_port(port)
        raise
    return {
        "proc": proc,
        "port": port,
        "log_path": str(log_path),
        "jsonl_path": str(run_dir / "mock" / f"{task_id}.jsonl"),
        "started_at": now_utc(),
    }


def stop_mock(handle) -> None:
    if not handle:
        return
    proc = handle.get("proc")
    if proc is not None and proc.poll() is None:
        _kill_tree(proc)
    if proc is not None:
        _unregister_proc(proc)
    release_port(handle.get("port"))
    handle["stopped_at"] = now_utc()
    handle["returncode"] = proc.poll() if proc is not None else None


def find_trial_dir(job_dir: Path, task_id: str):
    candidates = [
        path for path in sorted(job_dir.iterdir())
        if path.is_dir() and (path / "result.json").is_file()
    ] if job_dir.is_dir() else []
    named = [path for path in candidates if path.name.startswith(task_id + "__")]
    pool = named or candidates
    if not pool:
        return None, [path.name for path in candidates]
    pool.sort(key=lambda path: (path / "result.json").stat().st_mtime)
    return pool[-1], [path.name for path in candidates]


def analyze_mock(log_path: Path, instruction_text: str):
    info = {
        "path": str(log_path),
        "exists": log_path.is_file(),
        "request_count": 0,
        "main_request_count": 0,
        "tool_call_count": 0,
        "tool_names": [],
        "max_tool_rounds": 0,
        "instruction_exact_match": False,
        "instruction_match_kind": None,
        "instruction_match_request": None,
        "tool_probe_ok": False,
        "tool_result_preview": "",
        "parse_error": None,
    }
    if not log_path.is_file():
        return info
    entries = []
    try:
        for line in log_path.read_text(errors="replace").splitlines():
            if line.strip():
                entries.append(json.loads(line))
    except json.JSONDecodeError as exc:
        info["parse_error"] = str(exc)
    requests = [entry for entry in entries if entry.get("kind") == "request"]
    info["request_count"] = len(requests)
    for index, entry in enumerate(requests):
        body = entry.get("body") or {}
        messages = body.get("messages") or []
        if body.get("tools"):
            info["main_request_count"] += 1
        info["max_tool_rounds"] = max(info["max_tool_rounds"], int(entry.get("tool_rounds") or 0))
        for message in messages:
            if message.get("role") == "user":
                content = message.get("content")
                if isinstance(content, str):
                    if content == instruction_text:
                        info["instruction_exact_match"] = True
                        info["instruction_match_kind"] = "raw"
                        info["instruction_match_request"] = index
                    elif content == instruction_text.rstrip("\n"):
                        info["instruction_exact_match"] = True
                        info["instruction_match_kind"] = "rstrip-newline"
                        info["instruction_match_request"] = index
                    elif content.strip() == instruction_text.strip():
                        info["instruction_exact_match"] = True
                        info["instruction_match_kind"] = "strip"
                        info["instruction_match_request"] = index
            elif message.get("role") == "assistant":
                for call in message.get("tool_calls") or []:
                    name = ((call.get("function") or {}).get("name"))
                    if name:
                        info["tool_call_count"] += 1
                        info["tool_names"].append(name)
            elif message.get("role") == "tool":
                content = message.get("content")
                if isinstance(content, str):
                    if "ENV_ALIGN_PROBE_OK" in content:
                        info["tool_probe_ok"] = True
                    if not info["tool_result_preview"]:
                        info["tool_result_preview"] = content[-1000:]
    info["tool_names"] = sorted(set(info["tool_names"]))
    return info


def parse_ctrf_failures(verifier_dir: Path):
    failures = []
    seen = set()
    candidates = [
        verifier_dir / "ctrf.json",
        verifier_dir / "reports" / "base-ctrf.json",
        verifier_dir / "reports" / "new-ctrf.json",
    ]
    for path in candidates:
        data = read_json(path)
        if not isinstance(data, dict):
            continue
        tests = ((data.get("results") or {}).get("tests")) or []
        for test in tests:
            status = (test.get("status") or "").lower()
            name = test.get("name") or test.get("fullName") or ""
            if status in ("failed", "broken") and name and name not in seen:
                seen.add(name)
                failures.append(name)
    return failures


def parse_trial(run_dir: Path, job_name: str, task_id: str, meta) -> dict:
    job_dir = run_dir / "jobs" / job_name
    result = {
        "job_dir": str(job_dir),
        "job_exists": job_dir.is_dir(),
        "trial_dir": None,
        "trial_names": [],
        "job_result": None,
        "trial_result": None,
        "exception_info": None,
        "exception_stats": None,
        "rewards": None,
        "dsh_run": None,
        "outcome": None,
        "fingerprint": None,
        "fingerprint_error": None,
        "instruction_file": None,
        "instruction_sha256": None,
        "instruction_matches_source": None,
        "mock": analyze_mock(run_dir / "mock" / f"{task_id}.jsonl", meta["instruction_bytes"].decode("utf-8", "replace")),
        "verifier_reward": None,
        "verifier_run_log_tail": "",
        "verifier_failed_tests": [],
        "git_commit": "",
        "model_patch_size": None,
        "model_patch_head": "",
        "dsh_sessions_count": 0,
        "dsh_session_files": [],
        "relay_log_present": False,
        "errors": [],
    }
    if not job_dir.is_dir():
        result["errors"].append("job 目录不存在")
        return result
    result["job_result"] = read_json(job_dir / "result.json")
    trial_dir, trial_names = find_trial_dir(job_dir, task_id)
    result["trial_names"] = trial_names
    if trial_dir is None:
        result["errors"].append("没有 trial 目录")
        return result
    result["trial_dir"] = str(trial_dir)
    trial = read_json(trial_dir / "result.json") or {}
    result["trial_result"] = trial
    result["exception_info"] = trial.get("exception_info")
    result["rewards"] = ((trial.get("verifier_result") or {}).get("rewards"))
    stats = ((result["job_result"] or {}).get("stats") or {})
    evals = stats.get("evals") or {}
    for eval_data in evals.values():
        result["exception_stats"] = eval_data.get("exception_stats")
    agent_dir = trial_dir / "agent"
    verifier_dir = trial_dir / "verifier"
    result["dsh_run"] = read_json(agent_dir / "dsh-run.json")
    result["outcome"] = read_json(agent_dir / "outcome.json")
    fp_path = agent_dir / "env-fingerprint.json"
    fp = read_json(fp_path)
    if fp is None:
        result["fingerprint_error"] = (
            f"{fp_path} 缺失或不是 JSON；stderr={tail_text(agent_dir / 'dsh-stderr.txt', 1500)}"
        )
    else:
        result["fingerprint"] = fp
    instruction_path = agent_dir / "instruction.txt"
    if instruction_path.is_file():
        data = instruction_path.read_bytes()
        result["instruction_file"] = str(instruction_path)
        result["instruction_sha256"] = sha256_bytes(data)
        result["instruction_matches_source"] = data == meta["instruction_bytes"]
    result["verifier_reward"] = read_json(verifier_dir / "reward.json")
    result["verifier_run_log_tail"] = tail_text(verifier_dir / "run.log", 4000)
    result["verifier_failed_tests"] = parse_ctrf_failures(verifier_dir)
    commit_path = agent_dir / "git-commit.txt"
    if commit_path.is_file():
        result["git_commit"] = tail_text(commit_path, 2000)
    patch_path = trial_dir / "artifacts" / "model.patch"
    if patch_path.is_file():
        result["model_patch_size"] = patch_path.stat().st_size
        result["model_patch_sha256"] = sha256_file(patch_path)
        result["model_patch_head"] = patch_path.read_text(errors="replace")[:2000]
    sessions_dir = agent_dir / "dsh-sessions"
    if sessions_dir.is_dir():
        session_files = [str(path) for path in sessions_dir.rglob("session.jsonl.zstd")]
        result["dsh_session_files"] = session_files
        result["dsh_sessions_count"] = len(session_files)
    launcher_path = agent_dir / "run-dsh.sh"
    if launcher_path.is_file():
        launcher = launcher_path.read_text(errors="replace")
        result["launcher_sha256"] = sha256_bytes(launcher.encode())
        result["launcher_has_double_dash"] = "-- --" in launcher
        result["launcher_exports_home"] = bool(
            re.search(r"^\s*export\s+HOME=", launcher, re.MULTILINE)
        )
    result["relay_log_present"] = (agent_dir / "upstream-relay.log").is_file()
    for key in ("dsh-stdout.txt", "dsh-stderr.txt"):
        path = agent_dir / key
        result[key.replace(".txt", "_tail")] = tail_text(path, 2500)
    return result


def get_path(obj, dotted: str):
    current = obj
    for part in dotted.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def required_tools_for(meta) -> dict:
    language = meta.get("language") or ""
    core = list(LANGUAGE_TOOLS.get(language, []))
    verifier = list(meta.get("verifier_required_commands") or [])
    # 去掉 test.sh 里函数体的位置参数等噪声（parse_require_cmds 已过滤）。
    return {
        "language": language,
        "core": sorted(set(core)),
        "verifier": sorted(set(verifier)),
        "all": sorted(set(core) | set(verifier)),
    }


def compare_fingerprints(base_fp, adapter_fp, meta) -> dict:
    result = {
        "ok": True,
        "fields_compared": 0,
        "mismatches": [],
        "expected_deltas": [],
        "checks": [],
        "env_name_diff": {"baseline_only": [], "adapter_only": [], "unexpected": []},
        "tools": {},
        "required": required_tools_for(meta),
        "toolchain_consistent": None,
        "toolchain_complete": None,
    }
    if not base_fp or not adapter_fp:
        result["ok"] = False
        result["mismatches"].append({
            "field": "fingerprint", "baseline": bool(base_fp), "adapter": bool(adapter_fp),
            "kind": "presence",
        })
        return result

    def add(name, baseline, adapter, kind="exact", expected=None):
        result["fields_compared"] += 1
        if baseline == adapter:
            return True
        item = {"field": name, "baseline": baseline, "adapter": adapter, "kind": kind}
        if expected:
            item["reason"] = expected
            result["expected_deltas"].append(item)
        else:
            result["mismatches"].append(item)
            result["ok"] = False
        return False

    # 用户、工作目录、HOME/PATH 等基础事实。
    for field in ("cwd", "home", "user.uid", "user.gid", "user.euid", "user.name",
                  "user.passwd_home", "user.passwd_shell"):
        add(field, get_path(base_fp, field), get_path(adapter_fp, field), kind="identity")

    base_env = base_fp.get("env") or {}
    adapter_env = adapter_fp.get("env") or {}
    for key in sorted(set(base_env) | set(adapter_env)):
        if key in IGNORED_ENV_NAMES:
            continue
        baseline_value = base_env.get(key)
        adapter_value = adapter_env.get(key)
        if key == "DSH_HOME" and adapter_value:
            add("env.DSH_HOME", DSH_HOME, adapter_value, kind="dsh")
        elif key.startswith("DSH_"):
            if baseline_value != adapter_value:
                result["expected_deltas"].append({
                    "field": f"env.{key}", "baseline": baseline_value, "adapter": adapter_value,
                    "kind": "dsh", "reason": "DSH shell-env 注入的会话事实（每 trial 不同）",
                })
        elif key in EXPECTED_ADAPTER_ENV and key not in base_env:
            result["expected_deltas"].append({
                "field": f"env.{key}", "baseline": None, "adapter": adapter_value,
                "kind": "dsh", "reason": "DSH bash 工具固定的模型友好 env",
            })
        elif key == "TERM" and baseline_value != adapter_value:
            result["expected_deltas"].append({
                "field": "env.TERM", "baseline": baseline_value, "adapter": adapter_value,
                "kind": "dsh", "reason": "DSH bash 工具固定 TERM=dumb",
            })
        else:
            add(f"env.{key}", baseline_value, adapter_value, kind="env")

    # env 名字集合：只允许 DSH 注入项与 DSH 凭据 scrub 的已知差异。
    base_names = set(base_fp.get("env_names") or []) - IGNORED_ENV_NAMES
    adapter_names = set(adapter_fp.get("env_names") or []) - IGNORED_ENV_NAMES
    baseline_only = sorted(base_names - adapter_names)
    adapter_only = sorted(adapter_names - base_names)
    unexpected = []
    for name in baseline_only:
        if SENSITIVE.search(name):
            result["expected_deltas"].append({
                "field": f"env_names.{name}", "baseline": "present", "adapter": "absent",
                "kind": "env-name", "reason": "DSH subprocess 的凭据名 scrub（KEY/PASSWORD/SECRET/TOKEN）",
            })
        elif name in EXPECTED_BASELINE_ENV:
            result["expected_deltas"].append({
                "field": f"env_names.{name}", "baseline": "present", "adapter": "absent",
                "kind": "env-name", "reason": EXPECTED_BASELINE_ENV_REASONS[name],
            })
        else:
            unexpected.append(name)
    for name in adapter_only:
        if name in EXPECTED_ADAPTER_ENV or name.startswith("DSH_"):
            result["expected_deltas"].append({
                "field": f"env_names.{name}", "baseline": "absent", "adapter": "present",
                "kind": "env-name",
                "reason": EXPECTED_ADAPTER_ENV_REASONS.get(name, "DSH bash 工具注入项"),
            })
        else:
            unexpected.append(name)
    result["env_name_diff"] = {
        "baseline_only": baseline_only,
        "adapter_only": adapter_only,
        "unexpected": sorted(unexpected),
    }
    for name in sorted(unexpected):
        result["mismatches"].append({
            "field": f"env_names.{name}", "baseline": "absent" if name in adapter_only else "present",
            "adapter": "present" if name in adapter_only else "absent", "kind": "env-name",
        })
        result["ok"] = False

    # 工具链位置与版本。
    base_tools = base_fp.get("tools") or {}
    adapter_tools = adapter_fp.get("tools") or {}
    for name in sorted(set(base_tools) | set(adapter_tools)):
        baseline = base_tools.get(name) or {}
        adapter = adapter_tools.get(name) or {}
        baseline_present = bool(baseline.get("present"))
        adapter_present = bool(adapter.get("present"))
        if baseline_present != adapter_present:
            add(f"tool.{name}.present", baseline_present, adapter_present, kind="tool")
            continue
        if not baseline_present:
            continue
        add(f"tool.{name}.path", baseline.get("path"), adapter.get("path"), kind="tool")
        add(
            f"tool.{name}.versions",
            [item.get("version") for item in baseline.get("versions") or []],
            [item.get("version") for item in adapter.get("versions") or []],
            kind="tool-version",
        )
        if name == "go":
            for key in ("GOPATH", "GOMODCACHE", "GOCACHE", "GOROOT", "GOFLAGS",
                        "GOPROXY", "GOTOOLCHAIN", "GOENV"):
                add(f"go.env.{key}", get_path(baseline, f"env.{key}"),
                    get_path(adapter, f"env.{key}"), kind="tool-path")
        elif name == "npm":
            for key in ("cache", "prefix", "global_root"):
                add(f"npm.env.{key}", get_path(baseline, f"env.{key}"),
                    get_path(adapter, f"env.{key}"), kind="tool-path")
        elif name == "pip3":
            add("pip3.cache", baseline.get("cache"), adapter.get("cache"), kind="tool-path")
        elif name == "python3":
            for key in ("executable", "version", "prefix", "base_prefix", "purelib", "platlib"):
                add(f"python3.env.{key}", get_path(baseline, f"env.{key}"),
                    get_path(adapter, f"env.{key}"), kind="tool-path")
        elif name == "rustup":
            for key in ("home", "active_toolchain"):
                add(f"rustup.{key}", baseline.get(key), adapter.get(key), kind="tool-path")

    # 缓存目录。
    base_caches = base_fp.get("caches") or {}
    adapter_caches = adapter_fp.get("caches") or {}
    for path in sorted(base_caches):
        baseline = base_caches.get(path) or {}
        adapter = adapter_caches.get(path) or {}
        for key in ("exists", "is_dir", "mode"):
            add(f"cache {path}.{key}", baseline.get(key), adapter.get(key), kind="cache")

    # 仓库状态：HEAD、分支、干净状态、manifest 哈希。
    for field in ("repo.head.stdout", "repo.branch.stdout", "repo.log1.stdout",
                  "repo.status.stdout", "repo.hooks_path.stdout", "repo.diff_stat.stdout"):
        add(field, get_path(base_fp, field), get_path(adapter_fp, field), kind="repo")
    add("repo.head_sha", get_path(base_fp, "repo.head.stdout"),
        get_path(adapter_fp, "repo.head.stdout"), kind="repo")
    add("repo.manifests", get_path(base_fp, "repo.manifests"),
        get_path(adapter_fp, "repo.manifests"), kind="repo")
    base_top = sorted(get_path(base_fp, "repo.top_level") or [])
    adapter_top = sorted(get_path(adapter_fp, "repo.top_level") or [])
    add("repo.top_level", base_top, adapter_top, kind="repo")

    for key in ("ID", "VERSION_ID", "PRETTY_NAME"):
        add(f"os_release.{key}", get_path(base_fp, f"os_release.{key}"),
            get_path(adapter_fp, f"os_release.{key}"), kind="os")

    # 任务声明的工具链要求：存在性、路径一致性、verifier 要求。
    required = result["required"]
    for name in required["all"]:
        baseline = (base_tools.get(name) or {})
        adapter = (adapter_tools.get(name) or {})
        baseline_present = bool(baseline.get("present"))
        adapter_present = bool(adapter.get("present"))
        result["tools"][name] = {
            "baseline_present": baseline_present,
            "adapter_present": adapter_present,
            "baseline_path": baseline.get("path"),
            "adapter_path": adapter.get("path"),
            "consistent": baseline_present == adapter_present
            and (not baseline_present or baseline.get("path") == adapter.get("path")),
            "both_present": baseline_present and adapter_present,
        }
        add(f"required.{name}.presence", baseline_present, adapter_present, kind="required-tool")
        if baseline_present and adapter_present:
            add(f"required.{name}.path", baseline.get("path"), adapter.get("path"),
                kind="required-tool")
    result["toolchain_consistent"] = all(
        item["consistent"] for item in result["tools"].values()
    )
    result["toolchain_complete"] = all(
        item["both_present"] for name, item in result["tools"].items()
        if name in required["core"]
    )

    # DSH 会话/state 位置。
    base_dsh = base_fp.get("dsh") or {}
    adapter_dsh = adapter_fp.get("dsh") or {}
    add("dsh.home_env", DSH_HOME, adapter_dsh.get("home_env"), kind="dsh")
    session_jsonl = adapter_dsh.get("session_jsonl_env") or ""
    add("dsh.session_jsonl_under_home", True,
        bool(session_jsonl.startswith(DSH_HOME + "/")), kind="dsh")
    add("dsh.session_id_present", True, bool(adapter_dsh.get("session_id_env")), kind="dsh")
    add("dsh.sessions_present", True, int(adapter_dsh.get("session_count") or 0) >= 1,
        kind="dsh")
    add("dsh.sessions_all_under_home", True,
        all(str(path).startswith(DSH_HOME + "/") for path in adapter_dsh.get("sessions") or []),
        kind="dsh")
    add("dsh.outside_sessions_or_state",
        [], adapter_dsh.get("outside_sessions_or_state") or [], kind="dsh")
    add("dsh.home_tree_exists", True,
        bool((adapter_dsh.get("home_tree") or {}).get("exists")), kind="dsh")
    add("dsh.install_has_home", True,
        "home" in (adapter_dsh.get("install_top_level") or []), kind="dsh")
    add("dsh.root_home_has_dot_dsh", False,
        ".dsh" in (adapter_dsh.get("root_home_listing") or []), kind="dsh")
    add("dsh.baseline_home_absent", False,
        bool((base_dsh.get("home_tree") or {}).get("exists")), kind="dsh")

    return result


_ATTEMPT_LOCK = threading.Lock()


def append_attempt(run_dir: Path, record: dict) -> None:
    with _ATTEMPT_LOCK:
        with (run_dir / "attempts.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
            handle.flush()
            os.fsync(handle.fileno())


def cleanup_own_containers(run_dir: Path) -> dict:
    """只回收 compose working_dir 落在本 run 的容器；绝不碰别人的容器。"""
    removed = []
    inspected = []
    for name in docker_ps_names():
        labels_res = docker_capture(
            ["inspect", name, "--format", "{{json .Config.Labels}}"], timeout=30
        )
        if labels_res.get("rc") != 0:
            continue
        try:
            labels = json.loads(labels_res.get("stdout") or "{}")
        except json.JSONDecodeError:
            continue
        inspected.append({"name": name, "labels": labels})
        working_dir = str(labels.get("com.docker.compose.project.working_dir") or "")
        if str(run_dir.resolve()) not in working_dir:
            continue
        rm = docker_capture(["rm", "-f", name], timeout=120)
        removed.append({
            "name": name,
            "working_dir": working_dir,
            "rc": rm.get("rc"),
            "stderr": (rm.get("stderr") or "")[-500:],
        })
    return {"removed": removed, "inspected": inspected}


def run_adapter_task(task_id, index, meta, run_dir, task_copy, baseline_fp,
                     wall_seconds, timeout, summary_state):
    started = now_utc()
    rec = {
        "task_id": task_id,
        "attempt": 1,
        "started_at": started,
        "mock": None,
        "pier": None,
        "trial": None,
        "comparison": None,
        "assessment": None,
        "cleanup": None,
        "error": None,
    }
    mock_handle = None
    job_name = f"env-align-{run_dir.name}-{task_id}"
    if len(job_name) > 110:
        job_name = f"env-align-{run_dir.name[:12]}-{hashlib.sha1(task_id.encode()).hexdigest()[:12]}"
    rec["job_name"] = job_name
    try:
        port = pick_port(29120, index)
        rec["port"] = port
        mock_handle = start_mock(task_id, port, run_dir)
        rec["mock"] = {k: value for k, value in mock_handle.items() if k != "proc"}
        env = os.environ.copy()
        env[KEY_ENV] = DUMMY_KEY
        env["PIER_DSH_BASE_URL"] = f"http://127.0.0.1:{port}/v1"
        env["PIER_DSH_WALL_SECONDS"] = str(wall_seconds)
        env["PIER_DSH_MODEL"] = "gpt-6-luna"
        env["PIER_DSH_REASONING"] = "high"
        env["SCRATCH"] = str(SCRATCH)
        if not env.get("DOCKER_HOST"):
            env["DOCKER_HOST"] = f"unix:///run/user/{os.getuid()}/podman/podman.sock"
        pier_cmd = [str(PIER_RUN)] if os.access(PIER_RUN, os.X_OK) else ["bash", str(PIER_RUN)]
        cmd = pier_cmd + [
            "-p", str(task_copy),
            "--agent-import-path", "dsh_agent:DshAgent",
            "-o", str(run_dir / "jobs"),
            "--job-name", job_name,
            "-n", "1",
            "--max-retries", "0",
            "-y",
            "--no-delete",
        ]
        rec["pier_no_delete"] = True
        rec["command"] = cmd
        rec["pier"] = run_logged(
            cmd,
            log_path=run_dir / "logs" / f"{task_id}.pier.log",
            timeout=timeout,
            cwd=REPO,
            env=env,
        )
        parsed = parse_trial(run_dir, job_name, task_id, meta)
        rec["trial"] = parsed
        rec["comparison"] = compare_fingerprints(baseline_fp, parsed.get("fingerprint"), meta)
        rec["assessment"] = assess_task(meta, parsed, rec["comparison"], baseline_fp)
        trial_dir = parsed.get("trial_dir")
        trial_name = Path(trial_dir).name if trial_dir else None
        rec["cleanup"] = cleanup_own_containers(run_dir)
        rec["status"] = rec["assessment"].get("status")
    except Exception as exc:  # noqa: BLE001 - 单题失败不能中断整批
        rec["error"] = f"{type(exc).__name__}: {exc}"
        rec["status"] = "error"
    finally:
        stop_mock(mock_handle)
        if rec.get("cleanup") is None:
            try:
                rec["cleanup"] = cleanup_own_containers(run_dir)
            except Exception as exc:  # noqa: BLE001
                rec["cleanup"] = {"error": str(exc)}
        rec["finished_at"] = now_utc()
        write_json(run_dir / "adapter" / f"{task_id}.json", rec)
        append_attempt(run_dir, rec)
        with summary_state["lock"]:
            summary_state["adapter"][task_id] = rec
            write_summary(run_dir, summary_state)
    return rec


def assess_task(meta, parsed, comparison, baseline_fp) -> dict:
    task_id = meta["task_id"]
    trial = parsed.get("trial_result") or {}
    metadata = ((trial.get("agent_result") or {}).get("metadata") or {})
    dsh_run = parsed.get("dsh_run") or {}
    dsh_exit = dsh_run.get("dsh_exit_code", metadata.get("dsh_exit_code"))
    timed_out = bool(dsh_run.get("timed_out", metadata.get("dsh_timed_out")))
    terminal_error = dsh_run.get("terminal_error", metadata.get("dsh_terminal_error"))
    dsh_ok = dsh_exit == 0 and not timed_out and terminal_error is None
    fp = parsed.get("fingerprint")
    fingerprint_present = isinstance(fp, dict)
    mock = parsed.get("mock") or {}
    mock_ok = bool(
        mock.get("tool_probe_ok")
        and mock.get("instruction_exact_match")
        and int(mock.get("tool_call_count") or 0) >= 1
        and int(mock.get("max_tool_rounds") or 0) >= 1
    )
    instruction_ok = bool(
        parsed.get("instruction_matches_source")
        and mock.get("instruction_exact_match")
    )
    ink_ok = None
    if task_id == INK_TASK:
        ink_ok = bool(
            instruction_ok
            and meta.get("instruction_leading_dash")
            and mock.get("instruction_match_kind") in ("raw", "rstrip-newline")
            and parsed.get("launcher_has_double_dash") is True
        )
    repo_status = get_path(fp, "repo.status.stdout") if fp else None
    head = get_path(fp, "repo.head.stdout") if fp else None
    repo_diffstat = get_path(fp, "repo.diff_stat.stdout") if fp else None
    base_status = get_path(baseline_fp, "repo.status.stdout") if baseline_fp else None
    base_diffstat = get_path(baseline_fp, "repo.diff_stat.stdout") if baseline_fp else None
    base_head = get_path(baseline_fp, "repo.head.stdout") if baseline_fp else None
    baseline_dirty = bool(base_status)
    baseline_diff_sha = get_path(baseline_fp, "repo.diff_sha256") if baseline_fp else None
    model_patch_sha = parsed.get("model_patch_sha256")
    model_patch_matches_baseline = None
    if baseline_dirty and baseline_diff_sha and model_patch_sha:
        model_patch_matches_baseline = model_patch_sha == baseline_diff_sha
    status_relative_clean = bool(
        repo_status == base_status
        and repo_diffstat == base_diffstat
        and head == base_head
    )
    dsh = (fp or {}).get("dsh") or {}
    outside = dsh.get("outside_sessions_or_state") or []
    session_jsonl = dsh.get("session_jsonl_env") or ""
    dsh_home_ok = bool(
        dsh.get("home_env") == DSH_HOME
        and session_jsonl.startswith(DSH_HOME + "/")
        and int(dsh.get("session_count") or 0) >= 1
        and not outside
        and (dsh.get("home_tree") or {}).get("exists")
        and ".dsh" not in (dsh.get("root_home_listing") or [])
        and parsed.get("launcher_exports_home") is not True
    )
    source_clean = bool(
        status_relative_clean
        and not outside
        and (
            model_patch_matches_baseline is True
            if (baseline_dirty and baseline_diff_sha)
            else (
                parsed.get("model_patch_size") == 0
                if not baseline_dirty
                else True
            )
        )
    )
    rewards = parsed.get("rewards") or {}
    verifier_present = bool(
        isinstance(rewards, dict)
        and rewards
        and isinstance(parsed.get("verifier_reward"), dict)
        and parsed.get("exception_info") is None
        and not parsed.get("exception_stats")
    )
    p2p_full = bool(
        rewards.get("p2p_total") is not None
        and rewards.get("p2p_passed") == rewards.get("p2p_total")
    )
    f2p_zero = rewards.get("f2p_passed") == 0
    reward_zero = rewards.get("reward") == 0
    anko_ok = None
    anko_detail = None
    if task_id == ANKO_TASK:
        gt = (fp or {}).get("go_test") or {}
        output = ""
        gt_path = parsed.get("trial_dir")
        if gt_path:
            text_path = Path(gt_path) / "agent" / "anko-go-test.txt"
            if text_path.is_file():
                output = text_path.read_text(errors="replace")
        packages_ok = bool(
            "ok  \tgithub.com/mattn/anko\t" in output
            or "ok  \tgithub.com/mattn/anko\n" in output
            or "ok  \tgithub.com/mattn/anko " in output
        )
        go_bin_exists = bool(get_path(fp, "caches./root/go/bin.exists"))
        anko_ok = bool(
            gt.get("rc") == 0
            and "FAIL" not in (gt.get("stdout") or "")
            and "FAIL" not in (gt.get("stderr") or "")
            and packages_ok
            and go_bin_exists
        )
        anko_detail = {
            "rc": gt.get("rc"),
            "duration_seconds": gt.get("duration_seconds"),
            "gopath": get_path(fp, "tools.go.env.GOPATH"),
            "go_bin_exists": go_bin_exists,
            "package_line_ok": packages_ok,
            "output_path": str(Path(parsed.get("trial_dir")) / "agent" / "anko-go-test.txt")
            if parsed.get("trial_dir") else None,
            "output_tail": output[-3000:],
        }

    env_ok = bool(
        comparison.get("ok")
        and comparison.get("toolchain_consistent")
        and comparison.get("toolchain_complete")
    )
    baseline_ok = bool(baseline_fp)
    adapter_ok = bool(
        baseline_ok and env_ok and dsh_ok and mock_ok and instruction_ok
        and source_clean and dsh_home_ok and verifier_present
        and (anko_ok is not False) and (ink_ok is not False)
    )
    task_ok = bool(adapter_ok and p2p_full and f2p_zero and reward_zero)
    failures = []
    if not baseline_ok:
        failures.append("baseline fingerprint 缺失")
    if not fingerprint_present:
        failures.append("适配器 fingerprint 缺失")
    if not env_ok:
        failures.append("环境 fingerprint/工具链逐项比较未通过")
    if not dsh_ok:
        failures.append(f"DSH exit={dsh_exit} timed_out={timed_out} terminal_error={terminal_error}")
    if not mock_ok:
        failures.append("mock/DSH 工具调用证据不足（probe 未执行或 instruction 未送达）")
    if not instruction_ok:
        failures.append("instruction 未按原文送达 DSH")
    if not source_clean:
        failures.append("任务源码不干净（status/patch/HEAD）")
    if not dsh_home_ok:
        failures.append("DSH 会话/state 不在 /opt/pier-dsh/home 或出现外部污染")
    if not verifier_present:
        failures.append("官方 verifier 未产出 reward 或出现 exception")
    if anko_ok is False:
        failures.append("Anko 完整 go test -count=1 ./... 未通过")
    if ink_ok is False:
        failures.append("Ink 以 '-' 开头的 instruction 未按原文送达（double '--' 失败）")
    anomalies = []
    if baseline_dirty:
        anomalies.append({
            "kind": "official_baseline_dirty_worktree",
            "detail": "官方镜像构建期修改了工作树；该 diff 是环境烘焙，不是适配器或模型改动。"
                      "source_clean 按相对 baseline 判定。",
            "baseline_status": base_status,
            "baseline_diff_stat": base_diffstat,
            "baseline_diff_sha256": baseline_diff_sha,
            "model_patch_sha256": model_patch_sha,
            "model_patch_matches_baseline_diff": model_patch_matches_baseline,
        })
    if verifier_present and not p2p_full:
        anomalies.append({
            "kind": "official_p2p_failure",
            "detail": "no-op mock 下官方 verifier 的既有 p2p 测试失败；不是适配器行为",
            "failed_tests": parsed.get("verifier_failed_tests") or [],
        })
    if verifier_present and not f2p_zero:
        anomalies.append({
            "kind": "unexpected_f2p_pass",
            "detail": "no-op mock 不应让 f2p 通过；需检查 mock 是否注入改动",
            "f2p_passed": rewards.get("f2p_passed"),
        })
    if verifier_present and not reward_zero:
        anomalies.append({
            "kind": "unexpected_reward",
            "detail": "no-op mock 预期 reward 0",
            "reward": rewards.get("reward"),
        })
    if parsed.get("exception_info") or parsed.get("exception_stats"):
        anomalies.append({
            "kind": "verifier_or_harness_exception",
            "detail": "trial/verifier 报告异常",
            "exception_info": parsed.get("exception_info"),
            "exception_stats": parsed.get("exception_stats"),
        })
    if adapter_ok and not task_ok:
        status = "adapter_aligned_official_anomaly"
    elif adapter_ok:
        status = "adapter_aligned"
    else:
        status = "adapter_failed"
    return {
        "status": status,
        "adapter_ok": adapter_ok,
        "task_ok": task_ok,
        "baseline_ok": baseline_ok,
        "environment_ok": env_ok,
        "dsh_ok": dsh_ok,
        "dsh_exit_code": dsh_exit,
        "dsh_timed_out": timed_out,
        "terminal_error": terminal_error,
        "mock_ok": mock_ok,
        "instruction_ok": instruction_ok,
        "ink_ok": ink_ok,
        "launcher_home_ok": parsed.get("launcher_exports_home") is not True,
        "launcher_has_double_dash": parsed.get("launcher_has_double_dash"),
        "source_clean": source_clean,
        "baseline_dirty": baseline_dirty,
        "model_patch_matches_baseline": model_patch_matches_baseline,
        "baseline_diff_sha256": baseline_diff_sha,
        "model_patch_sha256": model_patch_sha,
        "dsh_home_ok": dsh_home_ok,
        "verifier_present": verifier_present,
        "p2p_full": p2p_full,
        "f2p_zero": f2p_zero,
        "reward_zero": reward_zero,
        "anko_ok": anko_ok,
        "anko": anko_detail,
        "failures": failures,
        "anomalies": anomalies,
    }


def fingerprint_summary(fp) -> dict:
    if not isinstance(fp, dict):
        return {"present": False}
    tools = {}
    for name, item in (fp.get("tools") or {}).items():
        versions = [entry.get("version") for entry in item.get("versions") or []]
        tools[name] = {
            "present": bool(item.get("present")),
            "path": item.get("path"),
            "versions": versions,
        }
        if name == "go":
            tools[name]["env"] = {
                key: (item.get("env") or {}).get(key)
                for key in ("GOPATH", "GOMODCACHE", "GOCACHE", "GOROOT", "GOTOOLCHAIN")
            }
        if name == "npm":
            tools[name]["env"] = item.get("env")
        if name == "python3":
            tools[name]["env"] = item.get("env")
    caches = {}
    for path, item in (fp.get("caches") or {}).items():
        caches[path] = {
            "exists": item.get("exists"),
            "is_dir": item.get("is_dir"),
            "mode": item.get("mode"),
            "entry_count": item.get("entry_count"),
        }
    return {
        "present": True,
        "cwd": fp.get("cwd"),
        "home": fp.get("home"),
        "user": fp.get("user"),
        "env": fp.get("env"),
        "env_names": fp.get("env_names"),
        "path_entries": fp.get("path_entries"),
        "tools": tools,
        "caches": caches,
        "repo": {
            "head": get_path(fp, "repo.head.stdout"),
            "branch": get_path(fp, "repo.branch.stdout"),
            "status": get_path(fp, "repo.status.stdout"),
            "hooks_path": get_path(fp, "repo.hooks_path.stdout"),
            "manifests": get_path(fp, "repo.manifests"),
            "diff_stat": get_path(fp, "repo.diff_stat.stdout"),
            "diff_sha256": get_path(fp, "repo.diff_sha256"),
            "top_level": get_path(fp, "repo.top_level"),
        },
        "dsh": {
            "home_env": get_path(fp, "dsh.home_env"),
            "session_jsonl_env": get_path(fp, "dsh.session_jsonl_env"),
            "session_count": get_path(fp, "dsh.session_count"),
            "sessions": get_path(fp, "dsh.sessions"),
            "outside_sessions_or_state": get_path(fp, "dsh.outside_sessions_or_state"),
            "home_tree": get_path(fp, "dsh.home_tree"),
            "install_top_level": get_path(fp, "dsh.install_top_level"),
        },
        "os_release": fp.get("os_release"),
    }


def baseline_summary(record) -> dict:
    if not record:
        return {"ok": False}
    image = record.get("image_record") or {}
    inspect = image.get("inspect") or {}
    return {
        "ok": bool(record.get("ok")),
        "error": record.get("error"),
        "image": image.get("image"),
        "image_id": inspect.get("Id"),
        "image_size": inspect.get("Size"),
        "image_config": inspect.get("Config"),
        "pull": image.get("pull"),
        "probe": record.get("probe"),
        "fingerprint": fingerprint_summary(record.get("fingerprint")),
        "evidence_path": str(record.get("evidence_path") or ""),
    }


def adapter_summary(record) -> dict:
    if not record:
        return {"status": "missing"}
    trial = record.get("trial") or {}
    return {
        "status": record.get("status") or ("error" if record.get("error") else "unknown"),
        "error": record.get("error"),
        "job_name": record.get("job_name"),
        "port": record.get("port"),
        "pier": record.get("pier"),
        "cleanup": record.get("cleanup"),
        "dsh": {
            "exit_code": get_path(trial, "dsh_run.dsh_exit_code"),
            "timed_out": get_path(trial, "dsh_run.timed_out"),
            "terminal_error": get_path(trial, "dsh_run.terminal_error"),
            "wall_seconds": get_path(trial, "dsh_run.wall_seconds"),
            "usage": get_path(trial, "dsh_run.usage"),
        },
        "verifier": {
            "rewards": trial.get("rewards"),
            "exception_info": trial.get("exception_info"),
            "exception_stats": trial.get("exception_stats"),
            "reward_json": trial.get("verifier_reward"),
            "failed_tests": trial.get("verifier_failed_tests"),
            "run_log_tail": trial.get("verifier_run_log_tail"),
        },
        "mock": trial.get("mock"),
        "instruction": {
            "matches_source": trial.get("instruction_matches_source"),
            "sha256": trial.get("instruction_sha256"),
            "exact_match_in_mock": get_path(trial, "mock.instruction_exact_match"),
        },
        "source": {
            "git_commit": trial.get("git_commit"),
            "model_patch_size": trial.get("model_patch_size"),
            "model_patch_sha256": trial.get("model_patch_sha256"),
            "model_patch_head": trial.get("model_patch_head"),
            "baseline_dirty": get_path(record, "assessment.baseline_dirty"),
            "baseline_diff_sha256": get_path(record, "assessment.baseline_diff_sha256"),
            "model_patch_matches_baseline": get_path(record, "assessment.model_patch_matches_baseline"),
        },
        "dsh_sessions": {
            "count": trial.get("dsh_sessions_count"),
            "files": trial.get("dsh_session_files"),
        },
        "anko": (record.get("assessment") or {}).get("anko"),
        "ink": (record.get("assessment") or {}).get("ink_ok"),
        "launcher": {
            "sha256": trial.get("launcher_sha256"),
            "has_double_dash": trial.get("launcher_has_double_dash"),
            "exports_home": trial.get("launcher_exports_home"),
        },
        "comparison": record.get("comparison"),
        "assessment": record.get("assessment"),
        "evidence": {
            "job_dir": trial.get("job_dir"),
            "trial_dir": trial.get("trial_dir"),
            "env_fingerprint": str(Path(trial["trial_dir"]) / "agent/env-fingerprint.json")
            if trial.get("trial_dir") else None,
            "anko_go_test": str(Path(trial["trial_dir"]) / "agent/anko-go-test.txt")
            if trial.get("trial_dir") else None,
            "verifier_run_log": str(Path(trial["trial_dir"]) / "verifier/run.log")
            if trial.get("trial_dir") else None,
            "pier_log": (record.get("pier") or {}).get("log_path"),
            "mock_log": get_path(trial, "mock.path"),
        },
    }


def write_summary(run_dir: Path, state: dict) -> dict:
    task_ids = state["task_ids"]
    tasks = []
    for task_id in task_ids:
        baseline_record = (state.get("baseline") or {}).get(task_id)
        adapter_record = (state.get("adapter") or {}).get(task_id)
        base = baseline_summary(baseline_record)
        adapter = adapter_summary(adapter_record)
        tasks.append({
            "task_id": task_id,
            "language": (state.get("metas") or {}).get(task_id, {}).get("language"),
            "image": (state.get("metas") or {}).get(task_id, {}).get("image"),
            "declaration": state.get("declarations", {}).get(task_id),
            "baseline": base,
            "adapter": adapter,
        })
    assessments = [
        item["adapter"].get("assessment") or {} for item in tasks
        if item["adapter"].get("assessment")
    ]
    totals = {
        "tasks": len(task_ids),
        "baseline_ok": sum(1 for item in tasks if item["baseline"].get("ok")),
        "adapter_ok": sum(1 for item in assessments if item.get("adapter_ok")),
        "dsh_exit_zero": sum(1 for item in assessments if item.get("dsh_ok")),
        "environment_match": sum(1 for item in assessments if item.get("environment_ok")),
        "source_clean": sum(1 for item in assessments if item.get("source_clean")),
        "dsh_home_ok": sum(1 for item in assessments if item.get("dsh_home_ok")),
        "verifier_reward_present": sum(1 for item in assessments if item.get("verifier_present")),
        "p2p_full": sum(1 for item in assessments if item.get("p2p_full")),
        "anko_go_test_pass": sum(1 for item in assessments if item.get("anko_ok") is True),
        "ink_instruction_ok": sum(1 for item in assessments if item.get("ink_ok") is True),
    }
    failures = [
        {"task_id": item["task_id"], "failures": (item["adapter"].get("assessment") or {}).get("failures") or [],
         "status": item["adapter"].get("status")}
        for item in tasks
        if not (item["adapter"].get("assessment") or {}).get("adapter_ok")
    ]
    anomalies = [
        {"task_id": item["task_id"], "anomalies": (item["adapter"].get("assessment") or {}).get("anomalies") or []}
        for item in tasks
        if (item["adapter"].get("assessment") or {}).get("anomalies")
    ]
    legacy = []
    for entry in LEGACY_HOME_FAILURE_EVIDENCE:
        item = dict(entry)
        item["resolved_path"] = str(SCRATCH / entry["path"])
        legacy.append(item)
    summary = {
        "schema": SCHEMA,
        "tool": "validate_environment.py",
        "run_id": state["run_id"],
        "phase": state.get("phase"),
        "started_at": state.get("started_at"),
        "updated_at": now_utc(),
        "subset": len(task_ids) < 20,
        "task_ids": task_ids,
        "sample": state.get("sample"),
        "run_meta": state.get("run_meta"),
        "dsh_version": DSH_VERSION,
        "mock_only": True,
        "real_model_inference": False,
        "pier_no_delete": all(
            bool((state.get("adapter", {}).get(task_id) or {}).get("pier_no_delete"))
            for task_id in task_ids
        ),
        "expected_reward": 0,
        "notes": [
            "mock 不实施任务功能，reward=0 是预期；重点是 DSH exit、环境 fingerprint、官方 verifier 是否产出 reward。",
            "旧 HOME=/tmp/dsh-home 的失败只作为引用证据保留，本工具不提供、也不运行旧 HOME 模式。",
        ],
        "legacy_home_failure_evidence": legacy,
        "prepare": state.get("prepare"),
        "tasks": tasks,
        "totals": totals,
        "failures": failures,
        "official_anomalies": anomalies,
        "cleanup": state.get("cleanup"),
        "verdict": "adapter_environment_aligned" if not failures else "adapter_failures_present",
        "verdict_detail": {
            "exit_code": 0 if not failures else 1,
            "failed_tasks": [item["task_id"] for item in failures],
            "official_anomaly_tasks": [item["task_id"] for item in anomalies],
        },
    }
    write_json(run_dir / "summary.json", summary)
    return summary


def cleanup_processes(_run_dir: Path = None):
    """只回收本进程登记过的 mock；不扫描 /proc，不碰其他进程。"""
    with _PROC_LOCK:
        tracked = list(_OWN_PROCS.items())
    found = [{"pid": proc.pid, **info} for proc, info in tracked]
    for proc, _info in tracked:
        if proc.poll() is None:
            _kill_tree(proc)
        _unregister_proc(proc)
    remaining = [
        {"pid": proc.pid, **info}
        for proc, info in tracked
        if proc.poll() is None
    ]
    return {"found": found, "terminated": found, "remaining": remaining}


def open_ports(start=29120, end=29149):
    open_list = []
    for port in range(start, end + 1):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(0.2)
        try:
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                open_list.append(port)
        finally:
            sock.close()
    return open_list


def declaration_for(meta) -> dict:
    return {
        "language": meta.get("language"),
        "image": meta.get("image"),
        "image_workdir_declared": meta.get("image_workdir_declared"),
        "agent_network_mode": meta.get("agent_network_mode"),
        "agent_timeout_sec": meta.get("agent_timeout_sec"),
        "verifier_network_mode": meta.get("verifier_network_mode"),
        "verifier_environment_mode": meta.get("verifier_environment_mode"),
        "verifier_timeout_sec": meta.get("verifier_timeout_sec"),
        "environment_env_keys": meta.get("environment_env_keys"),
        "verifier_env_keys": meta.get("verifier_env_keys"),
        "tests_base_image": meta.get("tests_base_image"),
        "tests_base_matches_agent_image": meta.get("tests_base_image") == meta.get("image"),
        "tests_dockerfile_installs": meta.get("tests_dockerfile_installs"),
        "verifier_required_commands": meta.get("verifier_required_commands"),
        "f2p_total": len(meta.get("f2p_node_ids") or []),
        "p2p_total": len(meta.get("p2p_node_ids") or []),
        "instruction_sha256": meta.get("instruction_sha256"),
        "instruction_length": meta.get("instruction_length"),
        "instruction_first_char": meta.get("instruction_first_char"),
        "instruction_leading_dash": meta.get("instruction_leading_dash"),
        "self_compose": meta.get("self_compose"),
        "base_commit": meta.get("base_commit"),
    }


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="官方镜像 ↔ DSH 适配层运行环境对齐验证（mock-only，max 2 并发）"
    )
    parser.add_argument(
        "--task", action="append", default=None,
        help="只跑指定 task id（可多次）；默认 sample.json 的 20 题。用于 Anko 回归子集。",
    )
    parser.add_argument("--run-id", default=None, help="结果目录名；默认 UTC 时间戳")
    parser.add_argument("--concurrency", type=int, default=2, help="并发 trial 数（硬上限 2）")
    parser.add_argument("--port-base", type=int, default=29120, help="mock 端口起点（29120-29149）")
    parser.add_argument("--pier-wall-seconds", type=int, default=600, help="DSH 墙钟上限")
    parser.add_argument("--pier-timeout", type=int, default=3000, help="单题 Pier 进程上限")
    parser.add_argument("--no-devenv-reexec", action="store_true", help="禁止自动 re-exec 到 devenv shell")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    ensure_docker(no_reexec=args.no_devenv_reexec)
    sample, selected, selected_digest = load_sample()
    requested = list(args.task or [])
    unknown = [item for item in requested if item not in selected]
    if unknown:
        raise SystemExit(f"--task 不在冻结清单中：{unknown}")
    task_ids = [task for task in selected if not requested or task in requested]
    concurrency = max(1, min(MAX_CONCURRENCY, args.concurrency))
    if not (29120 <= args.port_base <= 29149):
        raise SystemExit("--port-base 必须在 29120-29149")
    run_id = args.run_id or time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    run_dir = RUN_ROOT / run_id
    blockers = [name for name in ("summary.json", "jobs", "tasks", "baseline", "adapter", "attempts.jsonl")
                if (run_dir / name).exists()]
    if blockers:
        raise SystemExit(f"run 目录已有结果 {run_dir}：{blockers}；请换 --run-id，保留旧尝试")
    run_dir.mkdir(parents=True, exist_ok=True)

    docker_version = docker_capture(["version", "--format", "{{.Server.Version}}"], timeout=30)
    repo_head = run_capture(["git", "-C", str(REPO), "rev-parse", "HEAD"], timeout=30)
    repo_status = run_capture(["git", "-C", str(REPO), "status", "--porcelain"], timeout=60)
    run_meta = {
        "argv": [sys.executable, str(Path(__file__).resolve()), *sys.argv[1:]],
        "host": socket.gethostname(),
        "cwd": str(Path.cwd()),
        "python": sys.version,
        "docker_server_version": (docker_version.get("stdout") or "").strip(),
        "repo_head": (repo_head.get("stdout") or "").strip(),
        "repo_status": (repo_status.get("stdout") or "").splitlines(),
        "sample": {
            "path": str(SAMPLE),
            "n": sample.get("n"),
            "seed": sample.get("seed"),
            "rules_version": sample.get("rules_version"),
            "selected_sha256": selected_digest,
            "file_sha256": sha256_file(SAMPLE),
        },
        "file_sha256": {
            "dsh_agent.py": sha256_file(SCRIPTS / "dsh_agent.py"),
            "validate_environment.py": sha256_file(Path(__file__).resolve()),
            "sample.json": sha256_file(SAMPLE),
            "mock_openai_server.py": sha256_file(MOCK),
            "prepare_pilot_task.py": sha256_file(PREPARE),
            "run_pier.sh": sha256_file(PIER_RUN),
            "dsh-bundle.tar.gz": sha256_file(DSH_BUNDLE),
        },
    }
    write_json(run_dir / "run-meta.json", run_meta)
    state = {
        "run_id": run_id,
        "task_ids": task_ids,
        "metas": {},
        "declarations": {},
        "baseline": {},
        "adapter": {},
        "prepare": None,
        "cleanup": None,
        "started_at": now_utc(),
        "phase": "baseline",
        "run_meta": run_meta,
        "sample": run_meta["sample"],
        "lock": threading.Lock(),
    }
    print(f"[env-align] run_id={run_id} tasks={len(task_ids)} concurrency={concurrency}", flush=True)

    for task_id in task_ids:
        meta = task_meta(task_id)
        state["metas"][task_id] = meta
        state["declarations"][task_id] = declaration_for(meta)
        record = baseline_task(meta, run_dir)
        record["evidence_path"] = str(run_dir / "baseline" / f"{task_id}.json")
        write_json(run_dir / "baseline" / f"{task_id}.json", record)
        state["baseline"][task_id] = record
        print(f"[baseline] {task_id}: ok={record.get('ok')} error={record.get('error')}", flush=True)
        with state["lock"]:
            write_summary(run_dir, state)

    state["phase"] = "prepare"
    state["prepare"] = prepare_task_copies(task_ids, run_dir)
    with state["lock"]:
        write_summary(run_dir, state)
    print(f"[prepare] rc={state['prepare'].get('rc')}", flush=True)

    state["phase"] = "adapter"
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {}
        for index, task_id in enumerate(task_ids):
            task_copy = Path(state["prepare"]["copies"][task_id]["path"])
            baseline_fp = (state["baseline"].get(task_id) or {}).get("fingerprint")
            futures[pool.submit(
                run_adapter_task, task_id, index, state["metas"][task_id], run_dir,
                task_copy, baseline_fp, args.pier_wall_seconds, args.pier_timeout, state,
            )] = task_id
        for future in as_completed(futures):
            task_id = futures[future]
            try:
                record = future.result()
                print(f"[adapter] {task_id}: status={record.get('status')} error={record.get('error')}", flush=True)
            except Exception as exc:  # noqa: BLE001
                print(f"[adapter] {task_id}: future 异常 {type(exc).__name__}: {exc}", flush=True)

    state["phase"] = "cleanup"
    cleanup = {
        "processes": cleanup_processes(run_dir),
        "containers": cleanup_own_containers(run_dir),
        "open_ports_29120_29149": open_ports(),
        "docker_ps_after": docker_ps_names(),
    }
    state["cleanup"] = cleanup
    write_json(run_dir / "cleanup.json", cleanup)
    with state["lock"]:
        summary = write_summary(run_dir, state)
    print(json.dumps({
        "run_id": run_id,
        "run_dir": str(run_dir),
        "verdict": summary["verdict"],
        "totals": summary["totals"],
        "failed_tasks": summary["verdict_detail"]["failed_tasks"],
        "official_anomaly_tasks": summary["verdict_detail"]["official_anomaly_tasks"],
    }, ensure_ascii=False, indent=2), flush=True)
    return 0 if not summary["failures"] else 1


PROBE_SOURCE = (
    '"""容器内环境探针：采集运行环境指纹，不写任务工作区（仅写 --out）。"""\nfrom __future__ import annotations\n\nimport argparse\nimport hashlib\nimport json\nimport os\nimport pwd\nimport re\nimport shutil\nimport subprocess\nimport sys\nimport sysconfig\nimport time\nfrom pathlib import Path\n\nSCHEMA = 1\nDSH_HOME = "/opt/pier-dsh/home"\nDSH_INSTALL = "/opt/pier-dsh"\nSENSITIVE = re.compile(r"(KEY|PASSWORD|SECRET|TOKEN|CREDENTIAL)", re.IGNORECASE)\nENV_ALLOW = {\n    "PATH", "HOME", "PWD", "OLDPWD", "SHELL", "TERM", "NO_COLOR", "PAGER", "GIT_PAGER",\n    "LANG", "LANGUAGE", "LC_ALL", "LC_CTYPE", "TZ", "NODE_ENV", "NODE_OPTIONS",\n    "PYTHON_VERSION", "PYTHONUNBUFFERED", "PIP_BREAK_SYSTEM_PACKAGES", "PIP_ROOT_USER_ACTION",\n    "VIRTUAL_ENV", "CONDA_PREFIX", "DENO_DIR", "BUN_INSTALL", "RYE_HOME",\n    "GOPATH", "GOMODCACHE", "GOCACHE", "GOROOT", "GOFLAGS", "GOPROXY", "GOTOOLCHAIN",\n    "CARGO_HOME", "RUSTUP_HOME", "RUSTUP_TOOLCHAIN", "NPM_CONFIG_CACHE", "npm_config_cache",\n    "NPM_CONFIG_PREFIX", "npm_config_prefix", "YARN_CACHE_FOLDER", "PNPM_HOME",\n}\nSKIP_WALK = {"node_modules", ".git", "target", ".venv", "venv", "vendor", ".cargo", ".rustup",\n             ".npm", ".cache", ".local", ".bun", ".rye", "dist", "build", "coverage",\n             ".pytest_cache", "__pycache__", ".gocache", ".yarn", ".pnpm-store"}\nCACHE_PATHS = [\n    "/root/.cache/go-build", "/root/go", "/root/go/pkg/mod", "/root/go/bin",\n    "/root/.npm", "/root/.cache/pip", "/root/.cache/uv", "/root/.cargo",\n    "/root/.cargo/registry", "/root/.cargo/git", "/root/.cargo/bin", "/root/.rustup",\n    "/root/.rustup/toolchains", "/root/.local", "/root/.bun", "/root/.rye",\n    "/root/.deno", "/app/node_modules", "/app/.venv", "/app/venv", "/app/target",\n    "/app/.gocache", "/app/vendor", "/app/.pytest_cache", "/app/.yarn",\n    "/app/.pnpm-store", "/app/.next", "/app/dist", "/app/build",\n]\nMANIFESTS = [\n    "go.mod", "go.sum", "package.json", "package-lock.json", "pnpm-lock.yaml",\n    "yarn.lock", "pyproject.toml", "uv.lock", "poetry.lock", "requirements.txt",\n    "Cargo.toml", "Cargo.lock", "deno.json", "deno.lock", "tsconfig.json",\n    ".npmrc", ".python-version", "rust-toolchain.toml", "rust-toolchain",\n]\nTOOL_VERSION_CMDS = {\n    "go": [["go", "version"]],\n    "node": [["node", "--version"]],\n    "npm": [["npm", "--version"]],\n    "npx": [["npx", "--version"]],\n    "pnpm": [["pnpm", "--version"]],\n    "yarn": [["yarn", "--version"]],\n    "deno": [["deno", "--version"]],\n    "bun": [["bun", "--version"]],\n    "python3": [["python3", "--version"]],\n    "pip3": [["pip3", "--version"]],\n    "pytest": [["pytest", "--version"]],\n    "rustc": [["rustc", "--version"]],\n    "cargo": [["cargo", "--version"]],\n    "cargo-nextest": [["cargo-nextest", "--version"]],\n    "git": [["git", "--version"]],\n    "bash": [["bash", "--version"]],\n    "sh": [["sh", "-c", "echo $0"]],\n    "timeout": [["timeout", "--version"]],\n    "make": [["make", "--version"]],\n    "gcc": [["gcc", "--version"]],\n    "g++": [["g++", "--version"]],\n    "cc": [["cc", "--version"]],\n    "tsc": [["tsc", "--version"]],\n}\n\n\ndef run(cmd, timeout=25, cwd=None, env=None):\n    try:\n        proc = subprocess.run(\n            cmd, capture_output=True, text=True, timeout=timeout, cwd=cwd, env=env\n        )\n        return {\n'
    '            "rc": proc.returncode,\n            "stdout": proc.stdout.strip()[:4000],\n            "stderr": proc.stderr.strip()[:2000],\n        }\n    except FileNotFoundError:\n        return {"rc": None, "error": "not-found"}\n    except subprocess.TimeoutExpired:\n        return {"rc": None, "error": "timeout"}\n    except OSError as exc:\n        return {"rc": None, "error": f"{type(exc).__name__}: {exc}"}\n\n\ndef first_line(result):\n    text = (result.get("stdout") or result.get("stderr") or "").strip()\n    return text.splitlines()[0] if text else ""\n\n\ndef stat_path(path):\n    try:\n        st = os.stat(path)\n        info = {\n            "path": path,\n            "exists": True,\n            "is_dir": os.path.isdir(path),\n            "is_file": os.path.isfile(path),\n            "mode": oct(st.st_mode & 0o7777),\n            "uid": st.st_uid,\n            "gid": st.st_gid,\n        }\n        if info["is_dir"]:\n            try:\n                info["entry_count"] = len(os.listdir(path))\n            except OSError:\n                info["entry_count"] = None\n        return info\n    except FileNotFoundError:\n        return {"path": path, "exists": False}\n    except OSError as exc:\n        return {"path": path, "exists": None, "error": f"{type(exc).__name__}: {exc}"}\n\n\ndef walk_tree(root, max_depth=4, limit=400):\n    root_path = Path(root)\n    if not root_path.is_dir():\n        return {"path": root, "exists": False, "entries": [], "truncated": False}\n    entries = []\n    truncated = False\n    base_depth = len(root_path.parts)\n    for dirpath, dirnames, filenames in os.walk(root_path):\n        current = Path(dirpath)\n        depth = len(current.parts) - base_depth\n        dirnames[:] = sorted(d for d in dirnames if d not in SKIP_WALK)\n        if depth >= max_depth:\n            dirnames[:] = []\n        for name in sorted(dirnames) + sorted(filenames):\n            path = current / name\n            rel = str(path.relative_to(root_path))\n            try:\n                st = path.stat()\n                entries.append({\n                    "path": rel,\n                    "type": "dir" if path.is_dir() else "file",\n                    "size": None if path.is_dir() else st.st_size,\n                })\n            except OSError:\n                entries.append({"path": rel, "type": "unknown"})\n            if len(entries) >= limit:\n                truncated = True\n                break\n        if truncated:\n            break\n    return {"path": root, "exists": True, "entries": entries, "truncated": truncated}\n\n\ndef scan_outside(roots, max_depth=4, limit=200):\n    found = []\n    for root in roots:\n        root_path = Path(root)\n        if not root_path.is_dir():\n            continue\n'
    '        base_depth = len(root_path.parts)\n        for dirpath, dirnames, filenames in os.walk(root_path):\n            current = Path(dirpath)\n            depth = len(current.parts) - base_depth\n            dirnames[:] = sorted(\n                d for d in dirnames\n                if d not in SKIP_WALK and str(current / d) != DSH_HOME\n            )\n            if depth >= max_depth:\n                dirnames[:] = []\n            for name in list(dirnames) + list(filenames):\n                lowered = name.lower()\n                if (\n                    lowered == "session.jsonl.zstd"\n                    or lowered == ".dsh"\n                    or lowered in ("dsh-home", "dsh-sessions")\n                ):\n                    found.append(str(current / name))\n                    if len(found) >= limit:\n                        return found\n    return found\n\n\ndef collect_env():\n    values = {}\n    names = sorted(os.environ)\n    for key in names:\n        if SENSITIVE.search(key):\n            continue\n        if key in ENV_ALLOW or key.startswith("DSH_"):\n            value = os.environ.get(key, "")\n            values[key] = value[:500]\n    return values, names\n\n\ndef collect_tools():\n    tools = {}\n    for name, commands in TOOL_VERSION_CMDS.items():\n        path = shutil.which(name)\n        item = {"path": path, "present": bool(path)}\n        if path:\n            versions = []\n            for cmd in commands:\n                res = run(cmd, timeout=25)\n                versions.append({\n                    "cmd": cmd,\n                    "rc": res.get("rc"),\n                    "version": first_line(res),\n                    "error": res.get("error"),\n                })\n            item["versions"] = versions\n        tools[name] = item\n    # Go 的持久路径事实。\n    if shutil.which("go"):\n        res = run(["go", "env", "-json"], timeout=30)\n        try:\n            tools["go"]["env"] = json.loads(res.get("stdout") or "{}")\n        except json.JSONDecodeError:\n            tools["go"]["env"] = {"_error": res.get("stderr") or res.get("stdout")}\n        tools["go"]["env_rc"] = res.get("rc")\n    if shutil.which("npm"):\n        npm = {}\n        for key, cmd in (\n            ("cache", ["npm", "config", "get", "cache"]),\n            ("prefix", ["npm", "config", "get", "prefix"]),\n            ("global_root", ["npm", "root", "-g"]),\n        ):\n            res = run(cmd, timeout=30)\n            npm[key] = first_line(res)\n        tools["npm"]["env"] = npm\n    if shutil.which("pip3"):\n        tools["pip3"]["cache"] = first_line(run(["pip3", "cache", "dir"], timeout=30))\n    if shutil.which("python3"):\n        code = (\n            "import json,sys,sysconfig;"\n            "print(json.dumps({\'executable\':sys.executable,\'version\':sys.version.split()[0],"\n            "\'prefix\':sys.prefix,\'base_prefix\':sys.base_prefix,"\n            "\'purelib\':sysconfig.get_paths().get(\'purelib\'),"\n            "\'platlib\':sysconfig.get_paths().get(\'platlib\'),"\n            "\'include\':sysconfig.get_paths().get(\'include\')}))"\n'
    '        )\n        res = run(["python3", "-c", code], timeout=30)\n        try:\n            tools["python3"]["env"] = json.loads(res.get("stdout") or "{}")\n        except json.JSONDecodeError:\n            tools["python3"]["env"] = {"_error": res.get("stderr")}\n    if shutil.which("rustup"):\n        home = run(["rustup", "show", "home"], timeout=30)\n        active = run(["rustup", "show", "active-toolchain"], timeout=30)\n        tools["rustup"] = {\n            "path": shutil.which("rustup"),\n            "present": True,\n            "home": first_line(home),\n            "active_toolchain": first_line(active),\n            "home_rc": home.get("rc"),\n            "active_rc": active.get("rc"),\n        }\n    return tools\n\n\ndef collect_repo():\n    repo = {"path": "/app", "exists": os.path.isdir("/app")}\n    if not repo["exists"]:\n        return repo\n\n    def git(*args):\n        res = run(["git", "-C", "/app", *args], timeout=60)\n        return {"rc": res.get("rc"), "stdout": res.get("stdout"), "stderr": res.get("stderr")}\n\n    repo["branch"] = git("rev-parse", "--abbrev-ref", "HEAD")\n    repo["head"] = git("rev-parse", "HEAD")\n    repo["log1"] = git("log", "-1", "--format=%H %s")\n    repo["status"] = git("status", "--porcelain")\n    repo["hooks_path"] = git("config", "--get", "core.hooksPath")\n    repo["diff_stat"] = git("diff", "--stat")\n    try:\n        names = sorted(os.listdir("/app"))\n        repo["top_level"] = names[:200]\n        repo["top_level_count"] = len(names)\n    except OSError as exc:\n        repo["top_level_error"] = str(exc)\n    manifests = {}\n    for name in MANIFESTS:\n        path = Path("/app") / name\n        if path.is_file():\n            try:\n                data = path.read_bytes()\n                manifests[name] = {\n                    "size": len(data),\n                    "sha256": hashlib.sha256(data).hexdigest(),\n                }\n            except OSError as exc:\n                manifests[name] = {"error": str(exc)}\n    repo["manifests"] = manifests\n    return repo\n\n\ndef collect_dsh():\n    info = {\n        "install_dir": DSH_INSTALL,\n        "home": DSH_HOME,\n        "home_env": os.environ.get("DSH_HOME"),\n        "session_jsonl_env": os.environ.get("DSH_SESSION_JSONL"),\n        "session_id_env": os.environ.get("DSH_SESSION_ID"),\n        "home_tree": walk_tree(DSH_HOME, max_depth=4, limit=400),\n    }\n    sessions = []\n    home_path = Path(DSH_HOME)\n    if home_path.is_dir():\n        for dirpath, dirnames, filenames in os.walk(home_path):\n            dirnames[:] = [d for d in dirnames if d not in SKIP_WALK]\n            for name in filenames:\n                if name == "session.jsonl.zstd":\n                    sessions.append(str(Path(dirpath) / name))\n    info["sessions"] = sessions\n    info["session_count"] = len(sessions)\n    if os.path.isdir(DSH_INSTALL):\n        try:\n            info["install_top_level"] = sorted(os.listdir(DSH_INSTALL))[:100]\n        except OSError:\n'
    '            info["install_top_level"] = None\n    info["outside_sessions_or_state"] = scan_outside(\n        ["/app", "/root", "/tmp", "/var/tmp", "/logs", "/opt"], max_depth=4, limit=200\n    )\n    root_listing = []\n    try:\n        root_listing = sorted(os.listdir("/root"))[:200]\n    except OSError:\n        pass\n    info["root_home_listing"] = root_listing\n    return info\n\n\ndef collect_caches():\n    caches = {}\n    for path in CACHE_PATHS:\n        caches[path] = stat_path(path)\n    return caches\n\n\ndef collect_user():\n    uid = os.getuid()\n    gid = os.getgid()\n    try:\n        entry = pwd.getpwuid(uid)\n        name, home, shell = entry.pw_name, entry.pw_dir, entry.pw_shell\n    except KeyError:\n        name, home, shell = None, None, None\n    return {\n        "uid": uid,\n        "gid": gid,\n        "euid": os.geteuid(),\n        "egid": os.getegid(),\n        "name": name,\n        "passwd_home": home,\n        "passwd_shell": shell,\n    }\n\n\ndef go_test(path_out):\n    record = {\n        "command": ["go", "test", "-count=1", "./..."],\n        "cwd": "/app",\n        "started_at": time.time(),\n    }\n    started = time.monotonic()\n    if not shutil.which("go"):\n        record.update({"rc": None, "error": "go-not-found"})\n        return record\n    try:\n        proc = subprocess.run(\n            ["go", "test", "-count=1", "./..."],\n            cwd="/app",\n            capture_output=True,\n            text=True,\n            timeout=900,\n        )\n        record.update({\n            "rc": proc.returncode,\n            "stdout": proc.stdout[-20000:],\n            "stderr": proc.stderr[-20000:],\n        })\n        combined = (\n            f"$ cd /app && go test -count=1 ./...\\n"\n            f"# exit_code={proc.returncode}\\n"\n            f"{proc.stdout}{proc.stderr}"\n        )\n    except subprocess.TimeoutExpired as exc:\n        record.update({"rc": None, "error": "timeout"})\n        combined = f"$ cd /app && go test -count=1 ./...\\n# TIMEOUT\\n{exc.stdout or \'\'}\\n{exc.stderr or \'\'}"\n    except OSError as exc:\n        record.update({"rc": None, "error": f"{type(exc).__name__}: {exc}"})\n        combined = f"$ cd /app && go test -count=1 ./...\\n# ERROR {exc}\\n"\n    record["duration_seconds"] = round(time.monotonic() - started, 3)\n    if path_out:\n        try:\n            Path(path_out).write_text(combined)\n            record["output_path"] = path_out\n        except OSError as exc:\n            record["output_error"] = str(exc)\n'
    '    return record\n\n\ndef write_json(path, payload):\n    if path == "-":\n        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))\n        return\n    target = Path(path)\n    target.parent.mkdir(parents=True, exist_ok=True)\n    tmp = target.with_name(target.name + ".tmp")\n    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))\n    os.replace(tmp, target)\n\n\ndef main():\n    parser = argparse.ArgumentParser()\n    parser.add_argument("--mode", choices=("baseline", "dsh"), default="baseline")\n    parser.add_argument("--out", default="-")\n    parser.add_argument("--tag", default="")\n    parser.add_argument("--go-test", action="store_true")\n    parser.add_argument("--go-test-out", default="")\n    args = parser.parse_args()\n\n    env, env_names = collect_env()\n    payload = {\n        "schema": SCHEMA,\n        "mode": args.mode,\n        "tag": args.tag,\n        "collected_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),\n        "hostname": os.uname().nodename,\n        "uname": " ".join(os.uname()),\n        "cwd": os.getcwd(),\n        "user": collect_user(),\n        "home": os.environ.get("HOME"),\n        "env": env,\n        "env_names": env_names,\n        "path_entries": [\n            {\n                "entry": item,\n                "exists": os.path.exists(item),\n                "is_dir": os.path.isdir(item),\n                "realpath": os.path.realpath(item) if os.path.exists(item) else None,\n            }\n            for item in os.environ.get("PATH", "").split(":")\n            if item\n        ],\n        "tools": collect_tools(),\n        "caches": collect_caches(),\n        "repo": collect_repo(),\n        "dsh": collect_dsh(),\n        "os_release": {},\n    }\n    try:\n        for line in Path("/etc/os-release").read_text().splitlines():\n            if "=" in line:\n                key, value = line.split("=", 1)\n                payload["os_release"][key] = value.strip().strip(\'"\')\n    except OSError:\n        pass\n\n    # 先落盘指纹（Anko 的 go test 可能较慢/失败，不能影响指纹证据）。\n    if args.mode == "dsh" and args.out != "-":\n        write_json(args.out, payload)\n\n    if args.go_test:\n        payload["go_test"] = go_test(args.go_test_out)\n\n    if args.out == "-":\n        write_json("-", payload)\n        return\n    write_json(args.out, payload)\n    print(f"ENV_ALIGN_PROBE_OK mode={args.mode} out={args.out}")\n\n\nif __name__ == "__main__":\n    main()\n'
)


if __name__ == "__main__":
    raise SystemExit(main())
