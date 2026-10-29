"""本地假上游：OpenAI Chat Completions（零推理，只验证管线）。

用途
- 给容器里的 DSH 提供一个 `/v1/chat/completions` 端点：按轮次回 bash 工具调用，
  看到足够的工具结果后再回一条结束消息。用于验证「Pier 自定义 agent → 容器内 DSH →
  宿主假上游 → git commit → verifier 判分」这条链路，不产生任何真实模型推理。
- 每个请求体原样落盘（JSONL），并单独抽出 assistant 的 `content` 序列，
  便于之后核对「客户端是否原样回传 assistant content」（marker 回传检查）。
- 故障注入（给 outcome.json 的五类场景和并发压测用，`MOCK_FAULT`）：
  - `none`（默认）：正常应答；
  - `http_429` / `http_502`：直接回该状态码的 OpenAI 风格错误体；
  - `reset`：读到请求后立刻 RST（不发任何响应字节），对应"连接被拒/中断"；
  - `truncate`：先发 SSE 头和第一块，再 RST，对应"流中途断掉"；
  - `hang`：不回任何字节，挂住 MOCK_HANG_SECONDS 秒后关闭（对应客户端空闲超时）；
  - `pi_ai_error`：HTTP 200 + 单个 SSE error 帧
    （`data: {"error": {"message": MOCK_ERROR_MESSAGE}}`，不发 [DONE]），对应上游在流里
    报错、但没有 HTTP 状态码的情形——DSH 的 pi-ai 适配器据此给出 `PI_AI_ERROR: <message>`。
    只有这一种情形能复现真实观测到的并发/策略拒绝文本（带状态码的错误会被 pi-ai 归成
    RATE_LIMIT / SERVER / INVALID_REQUEST，走不到 PI_AI_ERROR）。

输入
- 环境变量 MOCK_LOG（JSONL 路径，必填）、MOCK_PORT（默认 29081）、MOCK_BIND（默认 127.0.0.1）、
  MOCK_MODEL（默认 gpt-6-luna）、MOCK_BASH_TOOL（默认自动探测；找不到 bash 类工具时用 bash）、
  MOCK_SCRIPT（写进 /app 的假改动文件名，默认 MOCK_TOOL_RAN）。
- MOCK_STEPS（默认 1）：回多少轮工具调用后才收尾；MOCK_DELAY（默认 0 秒）：每次应答前等待，
  用来把试验拉长到能观察到稳态内存。
- MOCK_FAULT（默认 none）、MOCK_FAULT_REQUESTS（默认 0 = 所有请求都故障；N>0 时只有前 N 个
  请求故障，之后自动恢复正常——用来验证"重试用尽"和"重试成功"两条路径）、
  MOCK_HANG_SECONDS（默认 600）、MOCK_ERROR_MESSAGE（`pi_ai_error` 帧里的 message 文本）。

副作用
- 只在 MOCK_LOG 追加写；不联网、不发起任何出站请求。

异常
- 请求体非法只记 `invalid` 条目并返回 400，不打印请求内容（避免把可能的凭据打到终端）。
"""

import json
import os
import socket
import struct
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LOG = os.environ["MOCK_LOG"]
PORT = int(os.environ.get("MOCK_PORT", "29081"))
BIND = os.environ.get("MOCK_BIND", "127.0.0.1")
MODEL = os.environ.get("MOCK_MODEL", "gpt-6-luna")
BASH_TOOL = os.environ.get("MOCK_BASH_TOOL", "")
MARKER_FILE = os.environ.get("MOCK_SCRIPT", "MOCK_TOOL_RAN")
# 模拟上游返回的 assistant content 里带一个可识别的 marker，便于后续核对客户端回传。
MARKER = os.environ.get("MOCK_MARKER", "[[mock-reasoning-marker-1]]")
# 假 agent 要执行的 shell 命令：在任务工作区留下一个可提交的改动。
TOOL_CMD = os.environ.get("MOCK_TOOL_CMD", "cd /app && echo mock-tool-ran > {file} && pwd")
# 故障注入与节奏控制，见模块 docstring。
FAULT = os.environ.get("MOCK_FAULT", "none")
FAULT_REQUESTS = int(os.environ.get("MOCK_FAULT_REQUESTS", "0"))
HANG_SECONDS = float(os.environ.get("MOCK_HANG_SECONDS", "600"))
# pi_ai_error 帧的 message：上游在流内报错时没有状态码，文本就是 DSH 的 errorMessage。
ERROR_MESSAGE = os.environ.get("MOCK_ERROR_MESSAGE", "mock injected PI_AI_ERROR")
STEPS = max(1, int(os.environ.get("MOCK_STEPS", "1")))
DELAY = float(os.environ.get("MOCK_DELAY", "0"))

_fault_lock = threading.Lock()
_fault_seen = 0


def fault_for_this_request():
    """按出现顺序决定本次请求是否注入故障（0 个故障时返回 none）。"""
    global _fault_seen
    if FAULT == "none":
        return "none"
    with _fault_lock:
        _fault_seen += 1
        hit = FAULT_REQUESTS <= 0 or _fault_seen <= FAULT_REQUESTS
    return FAULT if hit else "none"


def log_entry(entry):
    with open(LOG, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=False) + "\n")


def pick_bash_tool(body):
    """从请求的 tools 里挑一个能跑 shell 的工具。

    返回 (name, schema)；schema 是该工具 parameters 的 JSON Schema，用于补齐必填字段
    （DSH 的 bash 工具要求 command + description，缺字段会直接被判非法参数）。
    """
    tools = body.get("tools") or []
    if BASH_TOOL:
        for tool in tools:
            fn = tool.get("function") or {}
            if fn.get("name") == BASH_TOOL:
                return BASH_TOOL, fn.get("parameters") or {}
        return BASH_TOOL, {}
    names = []
    for tool in tools:
        fn = tool.get("function") or {}
        name = fn.get("name") or ""
        names.append(name)
        if name.lower() in ("bash", "shell", "sh", "run_bash", "bash_tool"):
            return name, fn.get("parameters") or {}
    for tool in tools:
        fn = tool.get("function") or {}
        name = fn.get("name") or ""
        if "bash" in name.lower() or "shell" in name.lower():
            return name, fn.get("parameters") or {}
    return (names[0] if names else "bash"), {}


def build_arguments(schema, command):
    """按 JSON Schema 的必填字段构造参数；shell 命令填进 command 类字段。"""
    props = (schema or {}).get("properties") or {}
    required = (schema or {}).get("required") or list(props)
    command_key = next(
        (key for key in ("command", "cmd", "script", "input") if key in props), "command"
    )
    arguments = {}
    for key in required:
        kind = (props.get(key) or {}).get("type")
        if key == command_key:
            arguments[key] = command
        elif kind == "string":
            arguments[key] = "run a shell command to make the mock change"
        elif kind == "boolean":
            arguments[key] = False
        elif kind == "integer":
            arguments[key] = 0
        elif kind == "array":
            arguments[key] = []
        elif kind == "object":
            arguments[key] = {}
        else:
            arguments[key] = command if key == command_key else "mock"
    if command_key not in arguments:
        arguments[command_key] = command
    return arguments


def summarize(body):
    messages = body.get("messages") or []
    assistants = [
        {
            "index": index,
            "content": message.get("content"),
            "reasoning_content": message.get("reasoning_content"),
            "tool_calls": [call.get("function", {}).get("name") for call in (message.get("tool_calls") or [])],
        }
        for index, message in enumerate(messages)
        if message.get("role") == "assistant"
    ]
    tool_names = [
        (tool.get("function") or {}).get("name") for tool in (body.get("tools") or [])
    ]
    return {
        "message_count": len(messages),
        "roles": [message.get("role") for message in messages],
        "assistant_messages": assistants,
        "tool_names": tool_names,
        "model": body.get("model"),
        "stream": body.get("stream"),
        "reasoning_effort": body.get("reasoning_effort"),
    }


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # 静音默认访问日志
        return

    def _send_json(self, payload, status=200):
        raw = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _send_sse(self, chunks):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        # 必须以 close 结束：HTTP/1.1 keep-alive 且无 Content-Length 时，
        # 客户端会一直等 EOF，即使已收到 [DONE]。
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        for chunk in chunks:
            self.wfile.write(f"data: {json.dumps(chunk)}\n\n".encode())
            self.wfile.flush()
        self.wfile.write(b"data: [DONE]\n\n")
        self.wfile.flush()

    def _abort(self):
        """用 RST 直接断链，让客户端看到"连接被重置"而不是正常的 EOF。"""
        self.close_connection = True
        try:
            self.connection.setsockopt(
                socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)
            )
        except OSError:
            pass
        try:
            self.connection.close()
        except OSError:
            pass

    def _fault_response(self, fault, first_chunk=None, stream=False):
        """按故障模式回一个非正常响应；返回 True 表示本次请求已处理完。"""
        if fault in ("http_429", "http_502"):
            status = 429 if fault == "http_429" else 502
            code = "rate_limit_exceeded" if status == 429 else "bad_gateway"
            self._send_json(
                {
                    "error": {
                        "message": f"mock injected {status}",
                        "type": code,
                        "code": code,
                    }
                },
                status=status,
            )
            return True
        if fault == "pi_ai_error":
            # 200 + 流内 error 帧：客户端（openai SDK → pi-ai）把它折成没有 HTTP 状态码的
            # errorMessage，DSH 记为 PI_AI_ERROR；不能改成 429，否则测的就不是这条路径。
            payload = {"error": {"message": ERROR_MESSAGE, "type": "pi_ai_error"}}
            if not stream:
                self._send_json(payload)
                return True
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "close")
            self.end_headers()
            self.close_connection = True
            try:
                self.wfile.write(f"data: {json.dumps(payload)}\n\n".encode())
                self.wfile.flush()
            except OSError:
                pass
            return True
        if fault == "reset":
            self._abort()
            return True
        if fault == "hang":
            time.sleep(HANG_SECONDS)
            self._abort()
            return True
        if fault == "truncate":
            # 只发 SSE 头（+首块），然后 RST：对应"流在中途断掉"。
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "close")
            self.end_headers()
            self.close_connection = True
            try:
                if first_chunk is not None:
                    self.wfile.write(f"data: {json.dumps(first_chunk)}\n\n".encode())
                self.wfile.flush()
            except OSError:
                pass
            self._abort()
            return True
        return False

    def do_GET(self):
        if self.path.rstrip("/") in ("/v1/models", "/models"):
            self._send_json({"object": "list", "data": [{"id": MODEL, "object": "model"}]})
            return
        self._send_json({"error": "not found"}, status=404)

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        if not self.path.rstrip("/").endswith("/chat/completions"):
            log_entry({"ts": time.time(), "kind": "unexpected_path", "path": self.path})
            self._send_json({"error": "not found"}, status=404)
            return
        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            log_entry({"ts": time.time(), "kind": "invalid_json", "bytes": len(raw)})
            self._send_json({"error": "invalid json"}, status=400)
            return

        info = summarize(body)
        tool_rounds = info["roles"].count("tool")
        ask_tool = bool(body.get("tools")) and tool_rounds < STEPS
        tool_name, tool_schema = pick_bash_tool(body)
        fault = fault_for_this_request()
        log_entry(
            {
                "ts": time.time(),
                "kind": "request",
                "request_bytes": len(raw),
                "summary": info,
                "tool_rounds": tool_rounds,
                "fault": fault,
                "chosen_tool": tool_name,
                "body": body,
            }
        )

        created = int(time.time())
        base = {
            "id": "chatcmpl-mock-1",
            "object": "chat.completion",
            "created": created,
            "model": MODEL,
        }
        usage = {"prompt_tokens": 100, "completion_tokens": 20, "total_tokens": 120}
        if not (body.get("tools") or []):
            # 无工具请求是会话标题之类的辅助调用，回纯文本，别塞工具调用。
            message = {"role": "assistant", "content": "mock title"}
            finish = "stop"
        elif not ask_tool:
            message = {"role": "assistant", "content": f"mock 完成 {MARKER}"}
            finish = "stop"
        else:
            call_id = f"call_mock_{tool_rounds + 1}"
            message = {
                "role": "assistant",
                "content": f"准备执行一次 shell 命令 {MARKER}",
                "tool_calls": [
                    {
                        "id": call_id,
                        "type": "function",
                        "function": {
                            "name": tool_name,
                            "arguments": json.dumps(
                                build_arguments(
                                    tool_schema,
                                    TOOL_CMD.format(file=f"{MARKER_FILE}{tool_rounds or ''}"),
                                )
                            ),
                        },
                    }
                ],
            }
            finish = "tool_calls"

        if DELAY > 0:
            time.sleep(DELAY)

        if body.get("stream"):
            delta_base = {"role": "assistant"}
            chunks = [
                {**base, "choices": [{"index": 0, "delta": {**delta_base}, "finish_reason": None}]}
            ]
            if finish == "tool_calls":
                chunks.append(
                    {
                        **base,
                        "choices": [
                            {
                                "index": 0,
                                "delta": {
                                    "content": message["content"],
                                    "tool_calls": [
                                        {
                                            "index": 0,
                                            "id": message["tool_calls"][0]["id"],
                                            "type": "function",
                                            "function": {
                                                "name": tool_name,
                                                "arguments": message["tool_calls"][0]["function"]["arguments"],
                                            },
                                        }
                                    ],
                                },
                                "finish_reason": None,
                            }
                        ],
                    }
                )
            else:
                chunks.append(
                    {
                        **base,
                        "choices": [
                            {"index": 0, "delta": {"content": message["content"]}, "finish_reason": None}
                        ],
                    }
                )
            chunks.append(
                {
                    **base,
                    "choices": [{"index": 0, "delta": {}, "finish_reason": finish}],
                    "usage": usage,
                }
            )
            if self._fault_response(fault, chunks[0], stream=True):
                return
            self._send_sse(chunks)
            return

        if self._fault_response(fault):
            return
        self._send_json(
            {
                **base,
                "choices": [{"index": 0, "message": message, "finish_reason": finish}],
                "usage": usage,
            }
        )


if __name__ == "__main__":
    server = ThreadingHTTPServer((BIND, PORT), Handler)
    print(f"mock upstream on {BIND}:{PORT}", flush=True)
    server.serve_forever()
