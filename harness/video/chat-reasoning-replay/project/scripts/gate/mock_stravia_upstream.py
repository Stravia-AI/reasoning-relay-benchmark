#!/usr/bin/env python3
"""假上游：按 Stravia 的字节形态回 assistant content / reasoning_content（零推理）。

用途
  1. `--scenario probe --variant NAME`：给 `probe_dsh_replay.py` 用。第一轮返回带
     History Marker / Projection Delimiter 的 assistant 消息 + 一个工具调用；第二轮
     把 DSH 回传的 assistant 消息与「上一轮实际发出的字符串」逐字节比对并写进 JSONL。
  2. `--scenario gate`：给 `gate.py --base-url …` 的端到端演练用。扮演一个脚本化的
     模型，按 `gen_task.py` 生成的同一份工作区产出 5 个顺序 bash 工具调用，最后收尾，
     让 gate 的计量判定有 ≥2 个上游模型请求可看。

输入
  --port（默认 29082）、--bind（默认 127.0.0.1）、--log（JSONL，必填）、
  --model（默认 gpt-6-luna）、--variant、--scenario、--workspace（gate 场景的工作区）、
  --nonstream（返回一次性 JSON 而不是 SSE；DSH 永远发 stream:true，此模式只给独立
  回显客户端用）、--chunk-size（SSE 切块大小，按 Unicode 码点切，默认 7）。

副作用
  只往 --log 追加 JSONL；不联网、不发任何出站请求、不读凭据。
  请求头一律不落盘（Authorization 里是假 key，但保持与真实链路同样的纪律）。

异常
  非法 JSON 记 `invalid` 条目并返回 400；未知 variant 直接退出（不静默降级）。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

# Stravia History Marker 的 reference 是 28 个 ASCII 小写字母（identifier::ID_LEN）。
REF_A = "abcdefghijklmnopqrstuvwxyzab"
REF_B = "bcdefghijklmnopqrstuvwxyzabc"
REF_C = "cdefghijklmnopqrstuvwxyzabcd"
REF_D = "defghijklmnopqrstuvwxyzabcde"


def marker(ref: str) -> str:
    """History Marker 的字节形态：`<!--sh:<ref>-->` + 一个换行。"""
    return f"<!--sh:{ref}-->\n"


def preview(ref: str, text: str, ordinal: int = 0) -> str:
    """Projection Delimiter（preview 模式）包住可见字节：`<!--sp:ref:p:N:s-->…<!--sp:ref:p:N:e-->`。"""
    return f"<!--sp:{ref}:p:{ordinal}:s-->{text}<!--sp:{ref}:p:{ordinal}:e-->"


def quoted(ref: str, lines: list[str]) -> str:
    """Post-Text Thinking 的 Markdown 引用预览，字节形态对齐投影编码器。

    编码器在文本前加 `> `，之后每遇一个换行就插入 `\n> `（空行因此变成 `> `，行尾带空格），
    span 前后各有一个空行。这里用 `\n> ` 拼接等价地复现同一串字节。
    """
    body = "> " + "\n> ".join(lines)
    return f"<!--sp:{ref}:p:0:s-->\n\n{body}\n\n<!--sp:{ref}:p:0:e-->"


# ── probe 变体 ────────────────────────────────────────────────────────────────
# 每个变体给出第一轮要发的 assistant 字段（content / reasoning_content）与可选的
# 额外 SSE 事件（interleaved 变体用）。字符串里的换行、制表、零宽字符都是故意的。

VARIANTS: dict[str, dict] = {
    # 正常形态：正文 + 引用预览 + 分隔符 + 标记 + 正文
    "content-preview-marker": {
        "content": "第一段答案。\n\n"
        + quoted(REF_A, ["预览行一", "预览行二"])
        + marker(REF_A)
        + "第二段答案。",
    },
    # 只有标记，没有正文、没有预览（protected payload 无公开摘要时的形态）
    "content-marker-only": {"content": marker(REF_B)},
    # 只有预览 span，没有标记
    "content-preview-only": {"content": quoted(REF_C, ["只有预览"])},
    # 标记前后都是空白/换行
    "content-whitespace-edges": {"content": "\n\n  " + marker(REF_D) + "\n\n\n"},
    # 零宽字符：正文里的 U+200B，以及紧贴标记两侧的 U+200B / U+FEFF
    "content-zero-width": {
        "content": "前\u200b文\n\u200b" + marker(REF_A) + "\ufeff\n后\u200b文",
    },
    # CRLF、制表、行尾空格
    "content-crlf-tabs": {
        "content": "行一\r\n\t" + marker(REF_B) + "\r\n> 引用\r\n\r\n行二  ",
    },
    # 标记尾部没有换行
    "content-trailing-no-newline": {"content": "答案<!--sh:" + REF_C + "-->"},
    # 含 emoji（合法代理对）与 CJK：验证 sanitizeSurrogates 不会误伤
    "content-unicode-emoji": {
        "content": "实验🧪结果：\n" + marker(REF_D) + "完成 ✅ 数据 42",
    },
    # 内容为空串 + 工具调用（DSH 会发 content: null）
    "content-empty-with-toolcall": {"content": ""},
    # 内容全空白 + 工具调用（DSH 会丢弃该文本块）
    "content-whitespace-only": {"content": "   \n\t "},
    # 同一轮里两个 Post-Text Thinking block：两个预览 + 两个标记
    "content-two-markers": {
        "content": "开头。\n\n"
        + quoted(REF_A, ["第一段思考"])
        + marker(REF_A)
        + "中间。\n\n"
        + quoted(REF_B, ["第二段思考"])
        + marker(REF_B)
        + "结尾。",
    },
    # 与 content-preview-marker 同字节，但 SSE 按 1 个码点切块：标记被拆到多个 chunk
    "content-marker-split-bytes": {
        "content": "第一段答案。\n\n"
        + quoted(REF_C, ["逐字符切块"])
        + marker(REF_C)
        + "第二段答案。",
    },
    # 前文本 Thinking 走 reasoning_content，正文走 content
    "reasoning-preview-marker": {
        "reasoning_content": preview(REF_A, "\n\n思考一\n\n") + marker(REF_A),
        "content": "答案。",
    },
    # 两路都带标记
    "reasoning-and-content": {
        "reasoning_content": preview(REF_A, "\n\n思考一\n\n") + marker(REF_A),
        "content": "答案一。\n\n" + quoted(REF_B, ["思考二"]) + marker(REF_B) + "答案二。",
    },
    # reasoning_content 空串：DSH 是否仍写这个字段
    "reasoning-empty": {"reasoning_content": "", "content": "答案。"},
    # 只有思考、正文为空（思考轮的典型形态）
    "reasoning-only-no-content": {
        "reasoning_content": preview(REF_C, "\n\n只有思考\n\n") + marker(REF_C),
        "content": "",
    },
    # reasoning → content → reasoning：暴露 DSH 按字段聚合导致的顺序合并
    "interleaved-reasoning": {
        "reasoning_content": "思考A",
        "content": "正文B",
        "reasoning_after": "思考C",
    },
}


def sse_chunks(payload: dict, chunk_size: int) -> list[dict]:
    """把一个 assistant 负载切成 OpenAI Chat Completions 的 SSE 增量。

    content / reasoning_content 按 Unicode 码点切块（不切断 UTF-8），
    用来验证「标记被拆到两个 chunk 里」时 DSH 是否仍逐字节拼回。
    """
    chunks: list[dict] = []
    base = {
        "id": "chatcmpl-mock",
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": payload["model"],
    }
    chunks.append({**base, "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}]})

    def emit(field: str, text: str) -> None:
        if not text:
            return
        for start in range(0, len(text), chunk_size):
            chunks.append(
                {
                    **base,
                    "choices": [
                        {"index": 0, "delta": {field: text[start : start + chunk_size]}, "finish_reason": None}
                    ],
                }
            )

    emit("reasoning_content", payload.get("reasoning_content") or "")
    emit("content", payload.get("content") or "")
    # interleaved 变体：正文之后再补一段 reasoning，制造 reasoning→content→reasoning
    if payload.get("reasoning_after"):
        emit("reasoning_content", payload["reasoning_after"])
    tool_calls = payload.get("tool_calls") or []
    if tool_calls:
        chunks.append(
            {
                **base,
                "choices": [
                    {
                        "index": 0,
                        "delta": {
                            "tool_calls": [
                                {
                                    "index": 0,
                                    "id": tool_calls[0]["id"],
                                    "type": "function",
                                    "function": {
                                        "name": tool_calls[0]["function"]["name"],
                                        "arguments": tool_calls[0]["function"]["arguments"],
                                    },
                                }
                            ]
                        },
                        "finish_reason": None,
                    }
                ],
            }
        )
    chunks.append(
        {
            **base,
            "choices": [
                {
                    "index": 0,
                    "delta": {},
                    "finish_reason": "tool_calls" if tool_calls else "stop",
                }
            ],
        }
    )
    # usage 用 OpenAI 的字段名（含 details），让客户端的用量解析路径也被覆盖。
    chunks.append(
        {
            **base,
            "choices": [],
            "usage": {
                "prompt_tokens": 1200,
                "completion_tokens": 80,
                "total_tokens": 1280,
                "prompt_tokens_details": {"cached_tokens": 1024},
                "completion_tokens_details": {"reasoning_tokens": 40},
            },
        }
    )
    return chunks


def nonstream_body(payload: dict) -> dict:
    """一次性响应（非流式）的 assistant 消息形态。"""
    message: dict = {"role": "assistant", "content": payload.get("content", "")}
    if payload.get("reasoning_content"):
        message["reasoning_content"] = payload["reasoning_content"]
    if payload.get("reasoning_after"):
        message["reasoning_content"] = (message.get("reasoning_content") or "") + payload["reasoning_after"]
    if payload.get("tool_calls"):
        message["tool_calls"] = payload["tool_calls"]
    return {
        "id": "chatcmpl-mock",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": payload["model"],
        "choices": [
            {
                "index": 0,
                "message": message,
                "finish_reason": "tool_calls" if payload.get("tool_calls") else "stop",
            }
        ],
        "usage": {
            "prompt_tokens": 1200,
            "completion_tokens": 80,
            "total_tokens": 1280,
            "prompt_tokens_details": {"cached_tokens": 1024},
            "completion_tokens_details": {"reasoning_tokens": 40},
        },
    }


# ── 工具调用 ──────────────────────────────────────────────────────────────────
def pick_shell_tool(body: dict) -> tuple[str, dict]:
    """从请求的 tools 里挑一个 shell 类工具，返回 (名字, 参数 schema)。"""
    tools = body.get("tools") or []
    fallback = None
    for tool in tools:
        fn = tool.get("function") or tool
        name = fn.get("name") or ""
        if not name:
            continue
        if fallback is None:
            fallback = (name, fn.get("parameters") or {})
        if name.lower() in ("bash", "shell", "sh", "run_bash", "bash_tool"):
            return name, fn.get("parameters") or {}
    for tool in tools:
        fn = tool.get("function") or tool
        name = fn.get("name") or ""
        if "bash" in name.lower() or "shell" in name.lower():
            return name, fn.get("parameters") or {}
    return fallback or ("bash", {})


def build_arguments(schema: dict, command: str) -> dict:
    """按 JSON Schema 的必填字段构造参数；命令填进 command 类字段。"""
    props = (schema or {}).get("properties") or {}
    required = (schema or {}).get("required") or list(props)
    command_key = next((key for key in ("command", "cmd", "script", "input") if key in props), "command")
    arguments: dict = {}
    for key in required:
        kind = (props.get(key) or {}).get("type")
        if key == command_key:
            arguments[key] = command
        elif kind == "string":
            arguments[key] = "run the probe command"
        elif kind == "boolean":
            arguments[key] = False
        elif kind == "integer":
            arguments[key] = 0
        elif kind == "array":
            arguments[key] = []
        elif kind == "object":
            arguments[key] = {}
        else:
            arguments[key] = command
    if command_key not in arguments:
        arguments[command_key] = command
    return arguments


def tool_call(tool_name: str, schema: dict, call_id: str, command: str) -> dict:
    return {
        "id": call_id,
        "type": "function",
        "function": {
            "name": tool_name,
            "arguments": json.dumps(build_arguments(schema, command), ensure_ascii=False),
        },
    }


# ── gate 场景：脚本化模型 ─────────────────────────────────────────────────────
def gate_chain(workspace: Path) -> tuple[list[str], str]:
    """按 gen_task 的同一份工作区算出 5 步命令与最终答案（只在假上游里用）。"""
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import gen_task  # noqa: PLC0415 - 脚本同级模块，按需导入

    steps = gen_task.shell_steps(workspace)
    return steps, gen_task.expected_answer(workspace)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "mock-stravia-upstream"

    # 由 main() 注入
    args = None
    sent: dict = {}          # turn -> {"content": str|None, "reasoning_content": str|None}
    turn = 0
    gate_steps: list[str] = []
    gate_answer: str = ""

    def log_message(self, fmt, *args):  # 静音访问日志
        return

    def _log(self, entry: dict) -> None:
        entry = {"ts": time.time(), **entry}
        with open(self.args.log, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, ensure_ascii=False) + "\n")

    def _send_json(self, payload: dict, status: int = 200) -> None:
        raw = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _send_sse(self, chunks: list[dict]) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        # 必须 close：无 Content-Length 的 keep-alive 会让客户端一直等 EOF。
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        for chunk in chunks:
            self.wfile.write(f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n".encode())
            self.wfile.flush()
        self.wfile.write(b"data: [DONE]\n\n")
        self.wfile.flush()

    def do_GET(self):
        if self.path.rstrip("/") in ("/v1/models", "/models"):
            self._send_json({"object": "list", "data": [{"id": self.args.model, "object": "model"}]})
            return
        self._send_json({"error": "not found"}, status=404)

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        if not self.path.rstrip("/").endswith("/chat/completions"):
            self._log({"kind": "unexpected_path", "path": self.path})
            self._send_json({"error": "not found"}, status=404)
            return
        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            self._log({"kind": "invalid_json", "bytes": len(raw)})
            self._send_json({"error": "invalid json"}, status=400)
            return

        self.__class__.turn += 1
        turn = self.__class__.turn
        messages = body.get("messages") or []
        roles = [message.get("role") for message in messages]
        has_tools = bool(body.get("tools"))
        self._log(
            {
                "kind": "request",
                "turn": turn,
                "roles": roles,
                "has_tools": has_tools,
                "model": body.get("model"),
                "stream": body.get("stream"),
                "stream_options": body.get("stream_options"),
                "reasoning_effort": body.get("reasoning_effort"),
                "tool_choice": body.get("tool_choice"),
                "tool_names": [((t.get("function") or t).get("name")) for t in (body.get("tools") or [])],
                "body": body,
            }
        )

        tool_name, tool_schema = pick_shell_tool(body)

        if self.args.scenario == "probe":
            payload = self._probe_payload(turn, messages, has_tools, tool_name, tool_schema)
        else:
            payload = self._gate_payload(messages, has_tools, tool_name, tool_schema)

        if self.args.nonstream:
            self._send_json(nonstream_body(payload))
        else:
            self._send_sse(sse_chunks(payload, self.args.chunk_size))

    # -- probe ---------------------------------------------------------------
    def _probe_payload(
        self, turn: int, messages: list[dict], has_tools: bool, tool_name: str, tool_schema: dict
    ) -> dict:
        variant = VARIANTS[self.args.variant]
        # DSH 会在首轮之后另发一个「生成会话标题」的请求（无 tools、无 assistant 历史）。
        # 它不参与回放验证，回一条普通文本即可，否则会污染比对与轮次。
        if not has_tools:
            self._log({"kind": "side_request", "turn": turn, "purpose": "no-tools"})
            return {"model": self.args.model, "content": "probe title", "tool_calls": []}

        replayed = self._probe_assistant(messages)
        if replayed is None:
            call = tool_call(tool_name, tool_schema, "call_probe_1", "echo probe-ok")
            payload = {
                "model": self.args.model,
                "content": variant.get("content"),
                "reasoning_content": variant.get("reasoning_content"),
                "reasoning_after": variant.get("reasoning_after"),
                "tool_calls": [call],
            }
            # 记下「实际发出的字符串」，回放请求据此逐字节比对。
            self.__class__.sent = {
                "content": payload["content"],
                "reasoning_content": payload["reasoning_content"],
            }
            return payload

        verdict = compare(self.__class__.sent, replayed)
        self._log({"kind": "replay", "turn": turn, "variant": self.args.variant, "verdict": verdict})
        return {"model": self.args.model, "content": "PROBE-DONE", "tool_calls": []}

    @staticmethod
    def _probe_assistant(messages: list[dict]) -> dict | None:
        """找到带着本轮探测工具调用的 assistant 消息（回放请求才有）。"""
        for message in messages:
            if message.get("role") != "assistant":
                continue
            for call in message.get("tool_calls") or []:
                if call.get("id") == "call_probe_1":
                    return message
        return None

    # -- gate ----------------------------------------------------------------
    def _gate_payload(self, messages: list[dict], has_tools: bool, tool_name: str, tool_schema: dict) -> dict:
        # 标题生成等旁路请求没有 tools：回一条普通文本，不消耗步骤。
        if not has_tools:
            self._log({"kind": "side_request", "purpose": "no-tools"})
            return {"model": self.args.model, "content": "gate title", "tool_calls": []}
        # 用「已经收到的工具结果条数」定位步骤：与请求序号无关，标题请求插队也不会错位。
        done = sum(1 for message in messages if message.get("role") == "tool")
        if done < len(self.gate_steps):
            call = tool_call(tool_name, tool_schema, f"call_gate_{done + 1}", self.gate_steps[done])
            return {"model": self.args.model, "content": None, "tool_calls": [call]}
        return {
            "model": self.args.model,
            "content": f"全部步骤完成，答案已写入 answer.txt。\n\n{self.gate_answer}",
            "tool_calls": [],
        }


def compare(sent: dict, received: dict | None) -> dict:
    """逐字节比对：sent 是上一轮实际发出的字符串，received 是客户端回传的字段。"""
    if received is None:
        return {"ok": False, "error": "no assistant message in replay request"}
    out: dict = {"ok": True, "fields": {}}
    for field in ("content", "reasoning_content"):
        expected = sent.get(field)
        actual = received.get(field)
        if expected is None:
            # 上一轮没发这个字段：只记录客户端有没有凭空加上
            out["fields"][field] = {"sent": None, "received": actual, "extra": actual is not None}
            continue
        same = isinstance(actual, str) and actual == expected
        entry = {
            "sent_sha256": hashlib.sha256(expected.encode()).hexdigest(),
            "received_sha256": hashlib.sha256(actual.encode()).hexdigest() if isinstance(actual, str) else None,
            "sent_len": len(expected),
            "received_len": len(actual) if isinstance(actual, str) else None,
            "sent_repr": repr(expected),
            "received_repr": repr(actual) if isinstance(actual, str) else None,
            "equal": same,
        }
        if not same and isinstance(actual, str):
            entry["first_diff"] = first_difference(expected, actual)
        out["fields"][field] = entry
        out["ok"] = out["ok"] and same
    # 工具调用是否也被原样回放（不是本门禁的判定项，但一并记录）
    out["tool_calls_replayed"] = len(received.get("tool_calls") or [])
    return out


def first_difference(expected: str, actual: str) -> dict:
    limit = min(len(expected), len(actual))
    for index in range(limit):
        if expected[index] != actual[index]:
            return {
                "index": index,
                "expected": repr(expected[max(0, index - 20) : index + 20]),
                "actual": repr(actual[max(0, index - 20) : index + 20]),
            }
    return {
        "index": limit,
        "expected": repr(expected[max(0, limit - 20) : limit + 20]),
        "actual": repr(actual[max(0, limit - 20) : limit + 20]),
    }


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="假上游：Stravia 形态的 Chat Completions 响应")
    parser.add_argument("--port", type=int, default=29082)
    parser.add_argument("--bind", default="127.0.0.1")
    parser.add_argument("--log", required=True)
    parser.add_argument("--model", default="gpt-6-luna")
    parser.add_argument("--scenario", choices=("probe", "gate"), default="probe")
    parser.add_argument("--variant", choices=sorted(VARIANTS), help="probe 场景的变体名")
    parser.add_argument("--workspace", help="gate 场景的工作区（读它算脚本化命令）")
    parser.add_argument("--nonstream", action="store_true")
    parser.add_argument("--chunk-size", type=int, default=7)
    args = parser.parse_args(argv)

    if args.scenario == "probe" and not args.variant:
        parser.error("probe 场景需要 --variant")
    if args.scenario == "gate":
        if not args.workspace:
            parser.error("gate 场景需要 --workspace")
        steps, answer = gate_chain(Path(args.workspace))
        Handler.gate_steps = steps
        Handler.gate_answer = answer
    Handler.args = args

    server = ThreadingHTTPServer((args.bind, args.port), Handler)
    print(f"mock stravia upstream on {args.bind}:{args.port} scenario={args.scenario}", flush=True)
    server.serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
