#!/usr/bin/env python3
"""零推理验证：DSH（`api: openai-completions`）是否原样回传 Stravia 的标记字节。

做法
  对每个变体：起一个假上游（`mock_stravia_upstream.py`，默认 127.0.0.1:29082），让它第一轮返回
  带 History Marker / Projection Delimiter 的 assistant 消息 + 一个工具调用；DSH headless 跑一轮
  工具调用后，第二轮请求里就带着它回放的 assistant 消息。假上游把「上一轮实际发出的字符串」与
  「这一轮回传的字段」逐字节比对，写进 JSONL；本脚本汇总成表格与 `probe.json`。

为什么不用真实路线
  真实推理会消耗账号额度，且本阶段还没有凭据。假上游只验证客户端回放，不产生任何模型推理。

输入
  --bundle（默认 SCRATCH/pier/dsh-bundle）、--node（默认 PATH 里的 node）、
  --port（默认 29082）、--variant（可重复；默认全部）、--mode stream|nonstream-echo、
  --out（默认 SCRATCH/gate/probe）、--keep（保留每个变体的 DSH 会话与日志）。

副作用
  只在 --out 下写文件；不读凭据、不联网（只连 127.0.0.1 的假上游）、不改任何既有脚本。

退出码
  0 = 所有变体的判定项都符合预期；1 = 有变体失败（含 DSH 运行失败）。
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
import urllib.request
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent
REPO = SCRIPTS.parents[4]
SCRATCH = REPO / ".scratch/research/chat-reasoning-replay"
DEFAULT_BUNDLE = SCRATCH / "pier/dsh-bundle"
MOCK = SCRIPTS / "mock_stravia_upstream.py"

# 判定预期：按字段给出 True（必须逐字节一致）/ False（已知会被 DSH 改写，只记录不算失败）。
# 这些预期来自 DSH 源码（convertMessages 过滤全空白文本块、content 为空时发 null、
# 同一字段的 thinking delta 合并进一个 block），目的是把「已知改写」与「意外改写」分开。
EXPECTATIONS: dict[str, dict[str, bool]] = {
    "content-preview-marker": {"content": True},
    "content-marker-only": {"content": True},
    "content-preview-only": {"content": True},
    "content-whitespace-edges": {"content": True},
    "content-zero-width": {"content": True},
    "content-crlf-tabs": {"content": True},
    "content-trailing-no-newline": {"content": True},
    "content-unicode-emoji": {"content": True},
    "content-two-markers": {"content": True},
    "content-marker-split-bytes": {"content": True},
    # 空串 content：SSE 不承载空 delta，DSH 回放时 content 为 null（字段被改写，字节无处可比）
    "content-empty-with-toolcall": {"content": False},
    # 全空白 content：DSH 丢弃全空白文本块（源码 trim().length > 0 过滤）
    "content-whitespace-only": {"content": False},
    "reasoning-preview-marker": {"content": True, "reasoning_content": True},
    "reasoning-and-content": {"content": True, "reasoning_content": True},
    # 只有思考、正文为空：正文变 null，思考必须逐字节一致
    "reasoning-only-no-content": {"content": False, "reasoning_content": True},
    # reasoning_content 空串：SSE 不发送空 delta，回放里该字段不存在
    "reasoning-empty": {"reasoning_content": False},
    # reasoning → content → reasoning：DSH 把两段 thinking 合并到同一 block，顺序被改写
    "interleaved-reasoning": {"content": True, "reasoning_content": False},
}

# 需要特殊 SSE 切块大小的变体（默认 7 码点；1 用来验证标记被拆到逐字符 chunk 时的拼回）。
CHUNK_SIZES = {"content-marker-split-bytes": 1, "content-two-markers": 5}

PROMPT = "请用 bash 运行 `echo probe-ok`，然后把输出原样告诉我。"


def free_port(port: int) -> bool:
    with socket.socket() as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


def wait_ready(port: int, timeout: float = 10.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"假上游 {port} 未就绪")


def provider_patch(base_url: str, model: str, reasoning: str) -> str:
    """与 docs/pier-adapter.md §6 同构的 provider 补丁（只换 baseURL）。"""
    return f"""- id: hmr
  disabled: false
# 关掉 LLM 标题生成：否则它会插在首轮之后，成为「第二轮请求」但与本探测无关。
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


def dsh_command(bundle: Path, patch: Path, prompt: str) -> list[str]:
    node = shutil.which("node") or str(bundle / "node/bin/node")
    return [
        node,
        "--expose-internals",
        str(bundle / "node_modules/@deepseek-ai/dsh/lib/bin.js"),
        "--profile",
        "headless",
        "--patch",
        str(patch),
        prompt,
    ]


def run_dsh(bundle: Path, run_dir: Path, patch: Path, workspace: Path, prompt: str, wall: int) -> dict:
    env = {
        **os.environ,
        "DSH_HOME": str(run_dir / "dsh-home"),
        "HOME": str(run_dir / "home"),
        "DSH_PERMISSION_MODE": "danger-full-access",
        "BENCH_API_KEY": "sk-probe-not-a-real-key",
    }
    (run_dir / "home").mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    try:
        completed = subprocess.run(
            dsh_command(bundle, patch, prompt),
            cwd=workspace,
            env=env,
            capture_output=True,
            text=True,
            timeout=wall,
        )
        code, out, err = completed.returncode, completed.stdout, completed.stderr
        timed_out = False
    except subprocess.TimeoutExpired as exc:
        code, out, err, timed_out = None, exc.stdout or "", exc.stderr or "", True
    elapsed = time.monotonic() - started
    (run_dir / "dsh-stdout.txt").write_text(out if isinstance(out, str) else "", encoding="utf-8")
    (run_dir / "dsh-stderr.txt").write_text(err if isinstance(err, str) else "", encoding="utf-8")
    return {"exit_code": code, "timed_out": timed_out, "wall_seconds": round(elapsed, 2)}


def read_session(run_dir: Path) -> dict:
    """解压 DSH 会话，抽 usage、工具调用数、第一条 assistant 消息的字段。"""
    sessions = sorted((run_dir / "dsh-home" / "sessions").rglob("session.jsonl.zstd"))
    info: dict = {"session_files": [str(path) for path in sessions], "usage": {}, "tool_calls": 0}
    if not sessions:
        return info
    raw = subprocess.run(["zstd", "-dc", str(sessions[-1])], capture_output=True, check=True).stdout
    usage = {"input": 0, "output": 0, "reasoning": 0, "cached": 0}
    seen_usage = 0
    first_assistant = None
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
                # 用量在 chunk.usage 里；推理 token 只在有值时出现。
                reported = chunk.get("usage") or {}
                seen_usage += 1
                usage["input"] += int(reported.get("inputTokens") or 0)
                usage["output"] += int(reported.get("outputTokens") or 0)
                usage["reasoning"] += int(reported.get("reasoningTokens") or 0)
                usage["cached"] += int(reported.get("cacheReadTokens") or 0)
        elif kind == "tool/call":
            info["tool_calls"] += 1
        elif kind == "assistant/message" and first_assistant is None:
            first_assistant = data.get("message") or data
    info["usage"] = {**usage, "usage_events": seen_usage}
    if isinstance(first_assistant, dict):
        blocks = first_assistant.get("content") or []
        info["assistant_blocks"] = [
            {"type": block.get("type"), "text_repr": repr(block.get("text") or block.get("thinking") or "")}
            for block in blocks
            if isinstance(block, dict)
        ]
        info["assistant_provider"] = first_assistant.get("provider")
        info["assistant_model"] = first_assistant.get("model")
    return info


def load_log(path: Path) -> list[dict]:
    entries = []
    if not path.is_file():
        return entries
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            entries.append(json.loads(line))
    return entries


def probe_variant(bundle: Path, out: Path, port: int, variant: str, wall: int) -> dict:
    run_dir = out / variant
    if run_dir.exists():
        shutil.rmtree(run_dir)
    (run_dir / "workspace").mkdir(parents=True)
    (run_dir / "workspace/README.txt").write_text("probe workspace\n", encoding="utf-8")
    log_path = run_dir / "requests.jsonl"
    base_url = f"http://127.0.0.1:{port}/v1"
    patch = run_dir / "provider.patch.yml"
    patch.write_text(provider_patch(base_url, "gpt-6-luna", "high"), encoding="utf-8")
    patch.chmod(0o600)

    server = subprocess.Popen(
        [
            sys.executable,
            str(MOCK),
            "--port",
            str(port),
            "--log",
            str(log_path),
            "--scenario",
            "probe",
            "--variant",
            variant,
            "--chunk-size",
            str(CHUNK_SIZES.get(variant, 7)),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    try:
        wait_ready(port)
        run = run_dsh(bundle, run_dir, patch, run_dir / "workspace", PROMPT, wall)
    finally:
        server.terminate()
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()

    entries = load_log(log_path)
    replay = next((entry for entry in entries if entry.get("kind") == "replay"), None)
    requests = [entry for entry in entries if entry.get("kind") == "request"]
    session = read_session(run_dir)
    expected = EXPECTATIONS[variant]
    result = {
        "variant": variant,
        "run": run,
        "requests": len(requests),
        "first_request_stream": requests[0].get("stream") if requests else None,
        "replay": replay.get("verdict") if replay else None,
        "session": session,
        "expected_verbatim": expected,
        "chunk_size": CHUNK_SIZES.get(variant, 7),
    }
    result["ok"] = bool(
        not run["timed_out"]
        and run["exit_code"] == 0
        and replay
        and replay["verdict"].get("fields")
    )
    if result["ok"]:
        fields = replay["verdict"]["fields"]
        # 逐字段判定：只有「上一轮真的发过」的字段才有 equal 键，其它字段记 absent/extra。
        observed = {}
        for field, want in expected.items():
            entry = fields.get(field) or {}
            if "equal" in entry:
                observed[field] = "same" if entry["equal"] else "changed"
            elif entry.get("extra"):
                observed[field] = "added-by-client"
            else:
                observed[field] = "absent"
        result["observed"] = observed
        result["verbatim"] = all(
            state == "same" for field in observed for state in [observed[field]] if state in ("same", "changed")
        )
        result["matches_expectation"] = all(
            (observed[field] == "same") == want for field, want in expected.items()
        )
    else:
        result["observed"] = {}
        result["verbatim"] = None
        result["matches_expectation"] = False
    (run_dir / "probe-result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def probe_nonstream_echo(out: Path, port: int, variant: str = "content-preview-marker") -> dict:
    """非流式形态的独立回显验证（DSH 永远发 stream:true，这条路径 DSH 不会走）。

    一个符合 Chat Completions 规范的最小客户端：取回一次性 JSON，把 assistant 消息连同工具结果
    原样放进下一轮请求，让假上游做同样的逐字节比对。
    """
    run_dir = out / f"nonstream-{variant}"
    if run_dir.exists():
        shutil.rmtree(run_dir)
    run_dir.mkdir(parents=True)
    log_path = run_dir / "requests.jsonl"
    base_url = f"http://127.0.0.1:{port}/v1"
    server = subprocess.Popen(
        [
            sys.executable,
            str(MOCK),
            "--port",
            str(port),
            "--log",
            str(log_path),
            "--scenario",
            "probe",
            "--variant",
            variant,
            "--nonstream",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )

    def post(body: dict) -> dict:
        request = urllib.request.Request(
            f"{base_url}/chat/completions",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json", "Authorization": "Bearer sk-probe-not-a-real-key"},
        )
        with urllib.request.urlopen(request, timeout=20) as response:
            return json.loads(response.read())

    try:
        wait_ready(port)
        first = post(
            {
                "model": "gpt-6-luna",
                "stream": False,
                "messages": [{"role": "user", "content": "probe"}],
                "tools": [
                    {
                        "type": "function",
                        "function": {
                            "name": "bash",
                            "parameters": {
                                "type": "object",
                                "properties": {"command": {"type": "string"}},
                                "required": ["command"],
                            },
                        },
                    }
                ],
            }
        )
        message = first["choices"][0]["message"]
        tool_call = message["tool_calls"][0]
        second = post(
            {
                "model": "gpt-6-luna",
                "stream": False,
                "messages": [
                    {"role": "user", "content": "probe"},
                    message,
                    {"role": "tool", "tool_call_id": tool_call["id"], "content": "probe-ok"},
                ],
                "tools": [
                    {
                        "type": "function",
                        "function": {
                            "name": "bash",
                            "parameters": {
                                "type": "object",
                                "properties": {"command": {"type": "string"}},
                                "required": ["command"],
                            },
                        },
                    }
                ],
            }
        )
    finally:
        server.terminate()
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()

    entries = load_log(log_path)
    replay = next((entry for entry in entries if entry.get("kind") == "replay"), None)
    result = {
        "variant": variant,
        "mode": "nonstream-echo",
        "nonstream_shape_ok": bool(second.get("choices")),
        "replay": replay.get("verdict") if replay else None,
    }
    result["verbatim"] = bool(
        replay
        and all(
            entry["equal"]
            for field in replay["verdict"]["fields"]
            for entry in [replay["verdict"]["fields"][field]]
            if isinstance(entry, dict) and "equal" in entry
        )
    )
    (run_dir / "probe-result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="零推理验证 DSH 是否原样回传 Stravia 标记")
    parser.add_argument("--bundle", type=Path, default=DEFAULT_BUNDLE)
    parser.add_argument("--port", type=int, default=29082)
    parser.add_argument("--variant", action="append", help="只跑指定变体（可重复）")
    parser.add_argument("--mode", choices=("stream", "nonstream-echo"), default="stream")
    parser.add_argument("--out", type=Path, default=SCRATCH / "gate/probe")
    parser.add_argument("--wall", type=int, default=180)
    args = parser.parse_args(argv)

    if not free_port(args.port):
        print(f"端口 {args.port} 已被占用；换 --port", file=sys.stderr)
        return 2
    args.out.mkdir(parents=True, exist_ok=True)

    if args.mode == "nonstream-echo":
        result = probe_nonstream_echo(args.out, args.port, (args.variant or ["content-preview-marker"])[0])
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if result["verbatim"] else 1

    variants = args.variant or list(EXPECTATIONS)
    results = []
    for variant in variants:
        result = probe_variant(args.bundle, args.out, args.port, variant, args.wall)
        results.append(result)
        observed = result.get("observed") or {}
        fields = " ".join(f"{field}={observed[field]}" for field in sorted(observed))
        print(
            f"{variant:<30} run={result['run']['exit_code']}/{result['run']['wall_seconds']}s "
            f"{fields or 'no-replay'} "
            f"expectation={'ok' if result.get('matches_expectation') else 'MISMATCH'}",
            flush=True,
        )

    summary = {
        "generated_at": time.time(),
        "bundle": str(args.bundle),
        "dsh_version": dsh_version(args.bundle),
        "results": results,
    }
    (args.out / "probe.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    failures = [result["variant"] for result in results if not result.get("matches_expectation")]
    print(f"\n写出 {args.out / 'probe.json'}")
    if failures:
        print(f"未达预期：{', '.join(failures)}", file=sys.stderr)
        return 1
    return 0


def dsh_version(bundle: Path) -> str | None:
    try:
        return json.loads((bundle / "package.json").read_text())["dependencies"]["@deepseek-ai/dsh"]
    except (OSError, KeyError, json.JSONDecodeError):
        return None


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
