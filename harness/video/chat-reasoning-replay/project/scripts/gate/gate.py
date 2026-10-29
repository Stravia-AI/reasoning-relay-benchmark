#!/usr/bin/env python3
"""gate：在宿主上用 DSH headless 跑一次紧凑任务，并按 meterd 计量判定推理上下文是否被保留。

判定（只对 stravia 路线）
  该路线在本次试验时间窗内的**第 2 个及之后**上游模型请求，必须每个都带 ≥1 个
  `encrypted_content` 推理输入项；否则 gate 失败。CPA 与 sub2api 只记录同样的统计，不设通过线。
  少于 2 个上游模型请求时无法验证回放，记为 `insufficient_requests`（不算通过）。

为什么要 gate
  Stravia 的 Chat 入口把 Post-Text Thinking 写进 assistant `content`（Markdown 引用预览 +
  Projection Delimiter + History Marker），只有客户端原样回传才恢复；它没有 Chat 多轮工具循环
  的端到端测试，所以这条真实链路只能靠 gate 验证。gate 任务本身就是一次多轮工具循环。

输入
  --route stravia|cpa|sub2api（必填）、--run-id、--base-url、--meter-file、--client-config、
  --model（默认 gpt-6-luna）、--reasoning（默认 high）、--seed（默认 20261026）、
  --bundle（DSH 离线包目录）、--out-root（默认 SCRATCH/gate）、--wall-seconds（默认 900）、
  --mock（自带假上游跑通全流程，零推理）、--mock-port（默认 29082）。

凭据
  客户端 key 只从私有目录的 client.json 读（0600，不进仓库），只经环境变量传给 DSH，
  不打印、不写进结果文件；`--mock` 且没有 client.json 时用假 key。

副作用
  只在 --out-root/<run-id>/ 下写：工作区、provider.patch.yml（0600）、DSH 会话、结果 JSON。
  不修改仓库其它文件；不 stop/start 任何既有服务；不占用 23481–23483 / 24481–24483 / 1455–1457。

退出码
  0 = 跑完（stravia 且 gate.pass 为真，或非 stravia 只记录）；1 = gate 失败/DSH 失败；
  2 = 前置条件不满足（缺 key、缺 meter 文件、端口占用等）。
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent
REPO = SCRIPTS.parents[4]
SCRATCH = REPO / ".scratch/research/chat-reasoning-replay"
PRIVATE = Path.home() / ".local/state/chat-reasoning-replay/private/run"
DEFAULT_BUNDLE = SCRATCH / "pier/dsh-bundle"
DEFAULT_OUT = SCRATCH / "gate"
METER_SUMMARY = REPO / "video/chat-reasoning-replay/project/scripts/meter/summarize.py"
MOCK = SCRIPTS / "mock_stravia_upstream.py"
GEN = SCRIPTS / "gen_task.py"

sys.path.insert(0, str(SCRIPTS))
import gen_task  # noqa: E402 - 同目录生成器，与假上游共用同一份工作区语义

ROUTE_PORTS = {"stravia": 23481, "cpa": 23482, "sub2api": 23483}
SCHEMA = "gate-run/v1"


# ── 环境 ──────────────────────────────────────────────────────────────────────
def provider_patch(base_url: str, model: str, reasoning: str) -> str:
    """provider 配置：与 scripts/pier/dsh_agent.py 同构，只换 baseURL。"""
    return f"""- id: hmr
  disabled: false
# 关掉用 LLM 生成会话标题的旁路请求：它不走工具循环、没有历史，会混进「第 2 个及之后
# 上游请求」里，让判定误伤。关掉后 DSH 用本地 fallback 标题。
- id: session-title-llm
  disabled: true
- id: agent-default-model
  config:
    provider: dsh-bench
    model: {json.dumps(model)}
- id: llm-pi-ai
  config:
    providers:
      dsh-bench:
        api: openai-completions
        baseURL: {json.dumps(base_url)}
        apiKeyEnv: BENCH_API_KEY
        reasoning: {reasoning}
        models:
          - id: {json.dumps(model)}
            contextWindow: 500000
            maxTokens: 32768
            reasoningEfforts:
              {reasoning}: {reasoning}
"""


def load_client_key(path: Path) -> str | None:
    """从私有 client.json 读 api_key；只返回字符串，不打印、不落盘。"""
    try:
        return json.loads(path.read_text())["api_key"]
    except (OSError, KeyError, json.JSONDecodeError):
        return None


def port_free(port: int) -> bool:
    with socket.socket() as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


def wait_ready(port: int, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"假上游 {port} 未就绪")


# ── DSH ──────────────────────────────────────────────────────────────────────
def run_dsh(
    bundle: Path, run_dir: Path, patch: Path, workspace: Path, prompt: str, key: str, wall: int
) -> dict:
    node = shutil.which("node") or str(bundle / "node/bin/node")
    command = [
        node,
        "--expose-internals",
        str(bundle / "node_modules/@deepseek-ai/dsh/lib/bin.js"),
        "--profile",
        "headless",
        "--patch",
        str(patch),
        prompt,
    ]
    env = {
        **os.environ,
        "DSH_HOME": str(run_dir / "dsh-home"),
        "HOME": str(run_dir / "home"),
        "DSH_PERMISSION_MODE": "danger-full-access",
        "BENCH_API_KEY": key,
    }
    (run_dir / "home").mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    started_at = time.time()
    try:
        completed = subprocess.run(
            command, cwd=workspace, env=env, capture_output=True, text=True, timeout=wall
        )
        code, out, err, timed_out = completed.returncode, completed.stdout, completed.stderr, False
    except subprocess.TimeoutExpired as exc:
        code, out, err, timed_out = None, exc.stdout or "", exc.stderr or "", True
    elapsed = time.monotonic() - started
    finished_at = time.time()
    (run_dir / "dsh-stdout.txt").write_text(out if isinstance(out, str) else "", encoding="utf-8")
    (run_dir / "dsh-stderr.txt").write_text(err if isinstance(err, str) else "", encoding="utf-8")
    return {
        "exit_code": code,
        "timed_out": timed_out,
        "wall_seconds": round(elapsed, 2),
        "started_at": started_at,
        "finished_at": finished_at,
    }


def read_session(run_dir: Path) -> dict:
    """解压 DSH 会话：用量、工具调用、轮次与每轮 assistant 字段摘要。"""
    sessions = sorted((run_dir / "dsh-home" / "sessions").rglob("session.jsonl.zstd"))
    info: dict = {
        "session_files": [str(path.relative_to(run_dir)) for path in sessions],
        "usage": {"input": 0, "cached": 0, "output": 0, "reasoning": 0, "usage_events": 0},
        "tool_calls": 0,
        "turns": 0,
        "assistant_messages": 0,
        "stop_reasons": {},
    }
    if not sessions:
        return info
    raw = subprocess.run(["zstd", "-dc", str(sessions[-1])], capture_output=True, check=True).stdout
    # 解压一份到运行目录：会话事件是原始证据，分析时不该再依赖 zstd 与目录结构。
    (run_dir / "dsh-session.jsonl").write_bytes(raw)
    for line in raw.decode("utf-8", "replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        kind = event.get("type")
        data = event.get("data") or {}
        if kind == "assistant/chunk":
            chunk = data.get("chunk") or {}
            if chunk.get("type") == "usage":
                # DSH 把用量放在 chunk.usage 里（inputTokens/outputTokens/cacheReadTokens），
                # 推理 token 只在有值时出现。
                usage = chunk.get("usage") or {}
                info["usage"]["usage_events"] += 1
                info["usage"]["input"] += int(usage.get("inputTokens") or 0)
                info["usage"]["output"] += int(usage.get("outputTokens") or 0)
                info["usage"]["reasoning"] += int(usage.get("reasoningTokens") or 0)
                info["usage"]["cached"] += int(usage.get("cacheReadTokens") or 0)
        elif kind == "tool/call":
            info["tool_calls"] += 1
        elif kind == "turn/start":
            info["turns"] += 1
        elif kind == "assistant/message":
            info["assistant_messages"] += 1
        elif kind == "turn/end":
            reason = data.get("reason") or data.get("stopReason")
            if reason:
                info["stop_reasons"][str(reason)] = info["stop_reasons"].get(str(reason), 0) + 1
    return info


# ── 计量 ──────────────────────────────────────────────────────────────────────
def load_meter_summary(meter_file: Path, route: str, run_id: str, started_at: float, finished_at: float) -> dict:
    """用 summarize.py 的能力统计时间窗内该路线的上游模型请求。"""
    spec = __import__("importlib.util").util.spec_from_file_location("meter_summarize", METER_SUMMARY)
    module = __import__("importlib.util").util.module_from_spec(spec)
    spec.loader.exec_module(module)
    rows = module.load_rows(meter_file)
    window = [
        {
            "route": route,
            "experiment": run_id,
            "started_at": started_at,
            "finished_at": finished_at,
        }
    ]
    summary = module.summarize(rows, window)
    bucket = next(
        (item for item in summary["buckets"] if item["route"] == route and item["experiment"] == run_id),
        None,
    )
    return {
        "summary": summary,
        "bucket": bucket,
        "rows_total": summary["row_counts"]["total"],
        "rows_model_requests": summary["row_counts"]["model_requests"],
    }


def gate_verdict(route: str, bucket: dict | None) -> dict:
    """stravia：第 2 个及之后的上游请求必须都带 encrypted reasoning item。"""
    if bucket is None:
        return {"pass": None, "valid": False, "reason": "no meter bucket for this route/window", "requests": []}
    detail = sorted(bucket["requests_detail"], key=lambda item: item.get("at") or 0)
    requests = [
        {
            "index": index + 1,
            "at": item.get("at"),
            "transport": item.get("transport"),
            "reasoning_items": item.get("reasoning_items"),
            "reasoning_encrypted_items": item.get("reasoning_encrypted_items"),
            "encrypted_lengths": item.get("encrypted_lengths"),
            "status": item.get("status"),
            "request_shape": item.get("request_shape"),
            "tools_count": item.get("tools_count"),
        }
        for index, item in enumerate(detail)
    ]
    # 旁路请求（无 tools、无历史，例如被关掉之前的标题生成）从构造上不可能带推理项，
    # 不参与通过线，但照常记录。
    tool_loop = [request for request in requests if (request.get("tools_count") or 0) > 0]
    auxiliary = [request for request in requests if (request.get("tools_count") or 0) == 0]
    verdict: dict = {
        "requests": requests,
        "upstream_requests": len(requests),
        "tool_loop_requests": len(tool_loop),
        "auxiliary_requests": len(auxiliary),
        # 状态码计数沿用 summarize.py 的口径（每个请求按 http-response-headers 归属）。
        "status_counts": bucket.get("status_counts") or {},
        "status_class_counts": bucket.get("status_class_counts") or {},
    }
    if route != "stravia":
        verdict.update(
            {
                "pass": None,
                "valid": len(requests) > 0,
                "reason": "record-only route (no pass line)",
            }
        )
        return verdict
    if len(tool_loop) < 2:
        verdict.update(
            {
                "pass": False,
                "valid": False,
                "reason": f"insufficient_requests: {len(tool_loop)} 个工具循环上游请求，无法验证第 2 个及之后",
                "requests_missing_encrypted": [],
            }
        )
        return verdict
    missing = []
    unknown = []
    for request in tool_loop[1:]:
        encrypted = request["reasoning_encrypted_items"]
        if encrypted is None:
            unknown.append(request["index"])
        elif encrypted < 1:
            missing.append(request["index"])
    if unknown:
        verdict.update(
            {
                "pass": False,
                "valid": False,
                "reason": f"meter_missing_reasoning_fields: 请求 {unknown} 缺 input_reasoning_encrypted_items",
                "requests_missing_encrypted": missing,
                "requests_unknown": unknown,
            }
        )
        return verdict
    verdict.update(
        {
            "pass": not missing,
            "valid": True,
            "reason": "ok" if not missing else f"请求 {missing} 没有带 encrypted_content 的推理输入项",
            "requests_missing_encrypted": missing,
        }
    )
    return verdict


def synthesize_meter_rows(
    route: str, started_at: float, finished_at: float, request_count: int, mode: str
) -> list[dict]:
    """给 --mock 演练造一份 meterd 形态的样例行（只用于假上游，绝不用于真实路线）。

    行字段与 docs/meterd.md 的口径一致：模型请求是 `http-request` + `model_request=true`，
    状态码来自 `http-response-headers`，用量与终态来自 `http-response-end`。
    mode="fail" 时从第 2 个请求起都不带 encrypted 推理项，用来验证判定的失败分支。
    """
    span = max(finished_at - started_at, 1.0)
    step = span / (request_count + 1)
    rows: list[dict] = []
    for index in range(request_count):
        at = started_at + step * (index + 1)
        flow = f"gate-{index + 1}"
        if index == 0:
            reasoning_items, encrypted_items, lengths = 0, 0, []
        elif mode == "fail":
            reasoning_items, encrypted_items, lengths = 2, 0, []
        else:
            reasoning_items, encrypted_items, lengths = 2, 1, [1180 + 40 * index]
        rows.append(
            {
                "at": at,
                "event": "http-request",
                "flow": flow,
                "route": route,
                "model_request": True,
                "transport": "http",
                "model": "gpt-6-luna",
                "reasoning_effort": "high",
                "reasoning_summary": None,
                "include_reasoning_encrypted": True,
                "input_reasoning_items": reasoning_items,
                "input_reasoning_encrypted_items": encrypted_items,
                "input_encrypted_lengths": lengths,
                "has_previous_response_id": index > 0,
                "store": False,
                "tools_count": 12,
                "has_prompt_cache_key": True,
                "request_shape": "ok",
            }
        )
        rows.append({"at": at, "event": "http-response-headers", "flow": flow, "route": route, "status": 200, "status_class": "2xx"})
        rows.append(
            {
                "at": at + step / 2,
                "event": "http-response-end",
                "flow": flow,
                "route": route,
                "status": 200,
                "status_class": "2xx",
                "response_terminal": "completed",
                "response_status": "completed",
                "response_reasoning_items": 1 if index else 0,
                "response_parse": "ok",
                "usage_input_tokens": 1200 + 100 * index,
                "usage_cached_tokens": 1024,
                "usage_output_tokens": 80,
                "usage_reasoning_tokens": 40,
            }
        )
    return rows


# ── 主流程 ────────────────────────────────────────────────────────────────────
def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="gate：DSH headless + meterd 判定")
    parser.add_argument("--route", choices=sorted(ROUTE_PORTS), required=True)
    parser.add_argument("--run-id")
    parser.add_argument("--base-url", help="默认 http://127.0.0.1:<路线端口>/v1")
    parser.add_argument("--meter-file", type=Path, help="默认私有目录的 meter.jsonl")
    parser.add_argument("--meter-window-start", type=float, help="覆盖计量窗口起点（Unix 秒）")
    parser.add_argument("--meter-window-end", type=float, help="覆盖计量窗口终点（Unix 秒）")
    parser.add_argument("--meter-pad-seconds", type=float, default=5.0, help="窗口两侧的容差，吸收时钟偏差")
    parser.add_argument("--client-config", type=Path)
    parser.add_argument("--model", default="gpt-6-luna")
    parser.add_argument("--reasoning", default="high")
    parser.add_argument("--seed", type=int, default=gen_task.DEFAULT_SEED)
    parser.add_argument("--bundle", type=Path, default=DEFAULT_BUNDLE)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--wall-seconds", type=int, default=900)
    parser.add_argument("--mock", action="store_true", help="用自带假上游跑通全流程（零推理）")
    parser.add_argument("--mock-port", type=int, default=29082)
    parser.add_argument(
        "--synthetic-meter",
        choices=("pass", "fail"),
        default="pass",
        help="--mock 且 meter 文件不存在时，造样例计量的分支（pass/fail），只用于演练判定",
    )
    args = parser.parse_args(argv)

    run_id = args.run_id or f"{args.route}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    run_dir = args.out_root / run_id
    if run_dir.exists():
        print(f"运行目录已存在：{run_dir}（换 --run-id）", file=sys.stderr)
        return 2
    run_dir.mkdir(parents=True)
    workspace = run_dir / "workspace"
    workspace.mkdir()

    # 1) 工作区（固定种子，确定性）
    spec = gen_task.generate(workspace, args.seed)
    prompt = gen_task.render_prompt(workspace)
    (run_dir / "prompt.txt").write_text(prompt, encoding="utf-8")

    # 2) 上游地址与 key
    mock_server = None
    if args.mock:
        if not port_free(args.mock_port):
            print(f"端口 {args.mock_port} 被占用（换 --mock-port）", file=sys.stderr)
            return 2
        base_url = f"http://127.0.0.1:{args.mock_port}/v1"
        key = "sk-gate-mock"
    else:
        base_url = args.base_url or f"http://127.0.0.1:{ROUTE_PORTS[args.route]}/v1"
        client_config = args.client_config or PRIVATE / args.route / "client.json"
        key = load_client_key(client_config)
        if not key:
            print(f"缺少客户端 key：{client_config}（不打印内容）", file=sys.stderr)
            return 2

    patch = run_dir / "provider.patch.yml"
    patch.write_text(provider_patch(base_url, args.model, args.reasoning), encoding="utf-8")
    patch.chmod(0o600)

    # 3) 假上游（可选）：必须用同一份工作区，才能算出脚本化的命令序列
    if args.mock:
        mock_server = subprocess.Popen(
            [
                sys.executable,
                str(MOCK),
                "--port",
                str(args.mock_port),
                "--log",
                str(run_dir / "mock-requests.jsonl"),
                "--scenario",
                "gate",
                "--workspace",
                str(workspace),
                "--model",
                args.model,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        try:
            wait_ready(args.mock_port)
        except RuntimeError as exc:
            mock_server.kill()
            print(str(exc), file=sys.stderr)
            return 2

    # 4) 跑 DSH
    try:
        run = run_dsh(args.bundle, run_dir, patch, workspace, prompt, key, args.wall_seconds)
    finally:
        if mock_server is not None:
            mock_server.terminate()
            try:
                mock_server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                mock_server.kill()

    session = read_session(run_dir)
    answer = gen_task.check(workspace)

    # 5) 计量与判定
    window_start = args.meter_window_start if args.meter_window_start is not None else run["started_at"]
    window_end = args.meter_window_end if args.meter_window_end is not None else run["finished_at"]
    # 两侧容差吸收 meterd 与 gate 的时钟偏差；真实试验窗口由调用方决定。
    window_start -= args.meter_pad_seconds
    window_end += args.meter_pad_seconds
    explicit_meter = args.meter_file
    meter_file = explicit_meter or (PRIVATE / "meter.jsonl")
    synthetic = False
    # 假上游的试验永远不读真实 meterd 数据：没显式给 --meter-file 时造同口径样例。
    if args.mock and (explicit_meter is None or not meter_file.is_file()):
        # 演练模式没有真实 meterd 数据：按假上游实际收到的请求数造一份同口径样例，
        # 让「判定 → 结果 JSON」这条路径也被执行。真实路线永远走 meterd 的真实行。
        mock_requests = [
            json.loads(line)
            for line in (run_dir / "mock-requests.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        request_count = sum(1 for entry in mock_requests if entry.get("kind") == "request")
        request_count = max(request_count, session["tool_calls"] + 1)
        rows = synthesize_meter_rows(
            args.route, window_start, window_end, request_count, args.synthetic_meter
        )
        meter_file = run_dir / "meter-synthetic.jsonl"
        meter_file.write_text(
            "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8"
        )
        synthetic = True

    meter: dict = {
        "file": str(meter_file),
        "synthetic": synthetic,
        "synthetic_mode": args.synthetic_meter if synthetic else None,
        "window": {"started_at": window_start, "finished_at": window_end},
    }
    verdict: dict = {"pass": None, "valid": False, "reason": "meter file missing", "requests": []}
    if meter_file.is_file():
        measured = load_meter_summary(meter_file, args.route, run_id, window_start, window_end)
        meter.update(
            {
                "rows_total": measured["rows_total"],
                "rows_model_requests": measured["rows_model_requests"],
                "bucket": measured["bucket"],
            }
        )
        verdict = gate_verdict(args.route, measured["bucket"])
        (run_dir / "meter-window.json").write_text(
            json.dumps(measured, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    else:
        print(f"meter 文件不存在：{meter_file}", file=sys.stderr)

    result = {
        "schema": SCHEMA,
        "run_id": run_id,
        "route": args.route,
        "started_at": run["started_at"],
        "finished_at": run["finished_at"],
        "wall_seconds": run["wall_seconds"],
        "env": {
            "base_url": base_url,
            "model": args.model,
            "reasoning": args.reasoning,
            "seed": args.seed,
            "mock": bool(args.mock),
            "bundle": str(args.bundle),
            "workspace": str(workspace),
        },
        "dsh": {
            "exit_code": run["exit_code"],
            "timed_out": run["timed_out"],
            **session,
        },
        "answer": answer,
        "meter": meter,
        "gate": verdict,
    }
    (run_dir / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    (run_dir / "workspace-manifest.json").write_text(
        json.dumps(
            {
                "seed": spec["seed"],
                "answer_sha256": gen_task.sha(spec["answer"]),
                "files": sorted(
                    str(path.relative_to(workspace)) for path in workspace.rglob("*") if path.is_file()
                ),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    # 6) 控制台摘要（不含 key）
    gate_text = "record-only" if verdict["pass"] is None else ("PASS" if verdict["pass"] else "FAIL")
    print(f"run:      {run_id}  route={args.route}  mock={bool(args.mock)}")
    print(f"dsh:      exit={run['exit_code']} timeout={run['timed_out']} wall={run['wall_seconds']}s "
          f"tool_calls={session['tool_calls']} turns={session['turns']}")
    print(f"usage:    in={session['usage']['input']} cached={session['usage']['cached']} "
          f"out={session['usage']['output']} reasoning={session['usage']['reasoning']}")
    print(f"answer:   {'ok' if answer['ok'] else 'wrong'}  ({answer.get('reason') or 'matched'})")
    print(f"upstream: {verdict.get('upstream_requests')} requests  gate={gate_text}  {verdict['reason']}")
    for request in verdict["requests"]:
        print(
            f"  #{request['index']} at={request['at']} transport={request['transport']} "
            f"reasoning={request['reasoning_items']} encrypted={request['reasoning_encrypted_items']} "
            f"status={request['status']}"
        )
    print(f"result:   {run_dir / 'result.json'}")

    if run["exit_code"] != 0 or run["timed_out"]:
        return 1
    if verdict["pass"] is False:
        return 1
    # stravia 必须显式通过；任何路线只要计量不可用（没有桶 / 没有请求）都算这次 gate 没跑成。
    if args.route == "stravia" and verdict["pass"] is not True:
        return 1
    if not verdict.get("valid"):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
