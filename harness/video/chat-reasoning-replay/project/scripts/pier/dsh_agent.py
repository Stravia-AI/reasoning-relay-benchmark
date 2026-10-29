"""Pier 自定义 agent：在任务容器里以 headless 方式跑 DSH，并把结果交回 trial 目录。

用途
- `pier run --agent-import-path <本文件>:DshAgent` 时，Pier 会用本类替代内置 agent。
- setup：把宿主预先打好的离线运行包（portable node + @deepseek-ai/dsh）上传进容器并解包，
  写 DSH 的 provider 配置（OpenAI Chat Completions，模型 gpt-6-luna，reasoning high）；
  另起一条**试验专属**的上游通道（见下）。
- run：以 headless profile 跑一条任务，硬上限 1 小时；结束后把 /app 的改动 git commit
  （DeepSWE 的 `[[verifier.collect]]` 只导出 commit），并把 DSH 会话、token 用量、墙钟与
  失败归类写到 trial 的 agent 目录（容器内 /logs/agent，宿主 trial_dir/agent）。

上游通道（agent 容器 network_mode: none）
- 容器没有任何外部接口、没有 DNS。唯一出口是 Pier 默认挂进容器的 `trial_dir/agent`
  → `/logs/agent`：本类在宿主该目录里起 `upstream_relay.py`（Unix socket → **一个**写死的
  `host:port`，即本试验所属路由的客户端入口），容器侧 `upstream-relay.js` 再把
  `127.0.0.1:<本地端口>` 接到这个 socket。DSH 只看到普通 HTTP baseURL。
- 每次试验一个 socket（各自 trial 目录），并发试验互相看不到对方的通道；socket 目标写死，
  容器里没有任何办法访问其它端口。

输入（环境变量）
- PIER_DSH_BUNDLE：宿主上的离线包路径，默认 SCRATCH/pier/dsh-bundle.tar.gz
- PIER_DSH_BASE_URL：**宿主侧**上游 URL，默认 http://127.0.0.1:29081/v1；只取 host:port
  作为 relay 目标，路径不用（客户端始终请求 /v1/chat/completions）
- PIER_DSH_UPSTREAM：直接给 `host:port` 时优先于 PIER_DSH_BASE_URL
- PIER_DSH_LOCAL_PORT：容器内 DSH 用的回环端口，默认 18080
- PIER_DSH_MODEL（默认 gpt-6-luna）、PIER_DSH_REASONING（默认 high）、
  PIER_DSH_CONTEXT_WINDOW（默认 500000）、PIER_DSH_MAX_TOKENS（默认 32768）
- PIER_DSH_WALL_SECONDS（默认 3600）
- 通过 `--ae` 传入的 BENCH_API_KEY（或 PIER_DSH_API_KEY_ENV 指定的名字）会被写进容器内的
  0600 文件，由启动脚本 source；不进任何日志、不上 exec 命令行。

副作用
- 只在容器内 /opt/pier-dsh、/app、/logs/agent 与宿主 trial_dir/agent 下写文件
  （含 `upstream.sock`、`upstream.sock.ready` 与 relay 进程）；relay 退出时删除 socket。
  不在宿主留下凭据文件（临时文件用后即删）；不访问外网。

异常
- relay 起不来或容器里看不到 socket 时 setup 直接失败（宁可早失败，也不要跑完一小时才发现）。
- 运行失败只记录状态、类别与状态码（http/connection/concurrency/other），不把可能含秘密的异常文本
  写进日志；判据见 docs/pier-adapter.md 的 outcome.json 小节。
"""

from __future__ import annotations

import atexit
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.parse import urlsplit

from pier.agents.installed.base import BaseInstalledAgent
from pier.environments.base import BaseEnvironment
from pier.models.agent.context import AgentContext
from pier.models.agent.network import NetworkAllowlist

DSH_VERSION = "0.1.0-rc.8"
INSTALL_DIR = "/opt/pier-dsh"
DSH_HOME = f"{INSTALL_DIR}/home"
LAUNCHER = f"{INSTALL_DIR}/run-dsh.sh"
KEY_FILE = f"{INSTALL_DIR}/api-key.env"
PATCH_FILE = f"{INSTALL_DIR}/provider.patch.yml"
RELAY_JS_CONTAINER = f"{INSTALL_DIR}/upstream-relay.js"
WORKSPACE = "/app"
AGENT_LOG_DIR = "/logs/agent"
SOCKET_NAME = "upstream.sock"
CONTAINER_SOCKET = f"{AGENT_LOG_DIR}/{SOCKET_NAME}"
DEFAULT_LOCAL_PORT = 18080

HOST_RELAY_SCRIPT = Path(__file__).with_name("upstream_relay.py")
HOST_RELAY_JS = Path(__file__).with_name("upstream-relay.js")

# DSH 把 LLM 失败的稳定代码放在 turn/end 的 reason.error.code；这里只做「基础设施 or 非」
# 的归类，具体证据（代码 + 消息）原样留在 dsh-run.json。判据来源：pi-ai 适配器
# classifyPiAiError()（429/rate limit → RATE_LIMIT，5xx → SERVER，连接类 → TRANSPORT，
# 空闲超时 → TIMEOUT），以及 headless runner 退出时的 `dsh: <code>: <message>`。
_HTTP_LIKE_CODES = {
    "RATE_LIMIT": 429,
    "SERVER": 500,  # 只由 5xx 触发；消息里没状态码时按 500 记下界，保证编排仍按基础设施错误处理
}
_CONNECTION_CODES = {"TIMEOUT", "TRANSPORT", "LLM_STREAM_IDLE_TIMEOUT"}
# 并发限额拒绝可能只出现在流内错误文本里（HTTP 200、没有状态码），DSH 原样记成
# PI_AI_ERROR: <message>。只有明确的「并发数超限」措辞才算基础设施错误；泛化的
# "please retry later"、quota、context window、usage policy 都不匹配，仍是 other。
# 真实 429 由 RATE_LIMIT 分支处理，这里不伪造状态码。
_CONCURRENCY_PATTERNS = (
    re.compile(
        r"\bconcurren(?:cy|t)[\s_-]*(?:requests?[\s_-]*)?(?:limit|cap)[\s_-]*(?:exceeded|reached|hit)\b",
        re.IGNORECASE,
    ),
    re.compile(r"\bmax(?:imum)?[\s_-]*concurrency[\s_-]*(?:exceeded|reached)\b", re.IGNORECASE),
    re.compile(
        r"\btoo[\s_-]*many[\s_-]*concurrent[\s_-]*(?:requests?|connections?|calls?)\b",
        re.IGNORECASE,
    ),
    re.compile(r"\bconcurrent[\s_-]*requests?[\s_-]*(?:exceeded|reached)\b", re.IGNORECASE),
)
_CONCURRENCY_CODES = {"CONCURRENCY", "CONCURRENCY_LIMIT", "CONCURRENCY_LIMIT_EXCEEDED"}
_STATUS_PATTERNS = (
    re.compile(r"\banswered\s+(\d{3})\b"),
    re.compile(r"^\s*(?:HTTP\s*)?(\d{3})\b"),
    re.compile(r"\((\d{3})\)"),
    re.compile(r"\b(?:status|code)[^0-9]{0,8}(\d{3})\b", re.IGNORECASE),
)
_STDERR_FAILURE = re.compile(r"^dsh: ([A-Z][A-Z0-9_]*): (.*)$", re.MULTILINE)


class DshAgent(BaseInstalledAgent):
    """DSH（DeepSeek Harness）headless profile 的 Pier 适配层。"""

    _relay: subprocess.Popen | None = None

    @staticmethod
    def name() -> str:
        return "dsh"

    def version(self) -> str | None:
        return DSH_VERSION

    def install_spec(self):
        # 在 setup 中安装，才能使用 trial 的日志挂载与专属 Unix socket。
        return None

    def network_allowlist(self) -> NetworkAllowlist:
        # 上游通道走宿主挂载的 Unix socket，不需要域名白名单；返回空表让 Pier 不要加
        # egress proxy sidecar。任务保持 no-network（容器 network_mode: none）。
        return NetworkAllowlist()

    # ---------------------------------------------------------------- setup
    async def setup(self, environment: BaseEnvironment) -> None:
        bundle = Path(
            os.environ.get(
                "PIER_DSH_BUNDLE",
                str(
                    Path(__file__).resolve().parents[5]
                    / ".scratch/research/chat-reasoning-replay/pier/dsh-bundle.tar.gz"
                ),
            )
        )
        if not bundle.is_file():
            raise RuntimeError(f"离线运行包不存在：{bundle}（先跑 build_dsh_bundle.sh）")

        await environment.exec(
            f"mkdir -p {INSTALL_DIR} {DSH_HOME} && chmod 755 {INSTALL_DIR}",
            user="root",
        )
        # 上传（离线）：tar.gz 经 docker compose cp 进入容器，不在容器里联网安装。
        await environment.upload_file(source_path=bundle, target_path=f"{INSTALL_DIR}/bundle.tar.gz")
        await environment.exec(
            " ".join(
                [
                    f"cd {INSTALL_DIR} &&",
                    "tar -xzf bundle.tar.gz &&",
                    "rm -f bundle.tar.gz &&",
                    "chmod +x node/bin/node &&",
                    f"node/bin/node -v > {INSTALL_DIR}/node-version.txt &&",
                    f"node/bin/node {INSTALL_DIR}/node_modules/@deepseek-ai/dsh/lib/bin.js --version > {INSTALL_DIR}/dsh-version.txt",
                ]
            ),
            user="root",
            timeout_sec=300,
        )
        await environment.upload_file(source_path=HOST_RELAY_JS, target_path=RELAY_JS_CONTAINER)

        model = os.environ.get("PIER_DSH_MODEL", "gpt-6-luna")
        reasoning = os.environ.get("PIER_DSH_REASONING", "high")
        context_window = int(os.environ.get("PIER_DSH_CONTEXT_WINDOW", "500000"))
        max_tokens = int(os.environ.get("PIER_DSH_MAX_TOKENS", "32768"))
        local_port = int(os.environ.get("PIER_DSH_LOCAL_PORT", str(DEFAULT_LOCAL_PORT)))
        key_env = os.environ.get("PIER_DSH_API_KEY_ENV", "BENCH_API_KEY")
        container_base_url = f"http://127.0.0.1:{local_port}/v1"

        # 与上一批 run_fourway.py 的 provider 结构一致，只把 api 换成 openai-completions。
        # hmr 需要显式启用：headless bundle 默认 disabled，而 launcher 的 user patch 监听依赖它。
        patch = f"""- id: hmr
  disabled: false
- id: agent-default-model
  config:
    provider: dsh-bench
    model: {json.dumps(model)}
- id: llm-pi-ai
  config:
    providers:
      dsh-bench:
        api: openai-completions
        baseURL: {json.dumps(container_base_url)}
        apiKeyEnv: {key_env}
        reasoning: {reasoning}
        models:
          - id: {json.dumps(model)}
            contextWindow: {context_window}
            maxTokens: {max_tokens}
            reasoningEfforts:
              {reasoning}: {reasoning}
"""
        patch_host = self.logs_dir / "provider.patch.yml"
        patch_host.parent.mkdir(parents=True, exist_ok=True)
        patch_host.write_text(patch)
        await environment.upload_file(source_path=patch_host, target_path=PATCH_FILE)

        # 启动脚本：先起容器内回环转发并等它就绪，再前台跑 DSH；DSH 退出后收掉转发进程。
        # 直接 exec 会把转发进程留给容器 init，超时被杀时会一起变成孤儿，所以用前台 + trap。
        launcher = f"""#!/usr/bin/env bash
# 由 Pier 适配层生成：读 0600 的 key 文件后跑 DSH，避免 key 出现在 exec 命令行/日志里。
set -uo pipefail
if [[ -f {KEY_FILE} ]]; then set -a; . {KEY_FILE}; set +a; fi
export DSH_HOME={DSH_HOME}
# 保留镜像 HOME 与工具链缓存；覆盖 HOME 会连带改写 Go/Rust 等工具的默认路径。
export DSH_PERMISSION_MODE=danger-full-access
cd {WORKSPACE}
NODE={INSTALL_DIR}/node/bin/node
"$NODE" {RELAY_JS_CONTAINER} {local_port} {CONTAINER_SOCKET} > {AGENT_LOG_DIR}/upstream-relay.log 2>&1 &
relay=$!
trap 'kill "$relay" 2>/dev/null || true' EXIT
for _ in $(seq 1 100); do
  if (exec 3<>/dev/tcp/127.0.0.1/{local_port}) 2>/dev/null; then break; fi
  sleep 0.1
done
"$NODE" --expose-internals \\
  {INSTALL_DIR}/node_modules/@deepseek-ai/dsh/lib/bin.js \\
  --profile headless --patch {PATCH_FILE} -- -- "$@"
"""
        launcher_host = self.logs_dir / "run-dsh.sh"
        launcher_host.write_text(launcher)
        await environment.upload_file(source_path=launcher_host, target_path=LAUNCHER)
        await environment.exec(f"chmod 755 {LAUNCHER}", user="root")

        key_name = key_env
        key_value = self._extra_env.get(key_name) or self._get_env(key_name)
        if key_value:
            # trial 日志位于仓库内；凭据暂存必须留在仓库外，上传失败也立即回收。
            private_dir = Path.home() / ".local/state/chat-reasoning-replay/private"
            private_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                mode="w", dir=private_dir, prefix="dsh-api-key-", suffix=".env"
            ) as tmp:
                tmp.write(f"export {key_name}={shlex.quote(key_value)}\n")
                tmp.flush()
                await environment.upload_file(source_path=Path(tmp.name), target_path=KEY_FILE)
            await environment.exec(f"chmod 600 {KEY_FILE}", user="root")

        self._start_relay()
        probe = await environment.exec(f"test -S {CONTAINER_SOCKET}", user="root", timeout_sec=60)
        if probe.return_code != 0:
            raise RuntimeError(
                f"容器里看不到上游 socket {CONTAINER_SOCKET}：{AGENT_LOG_DIR} 没有按预期挂载"
                "宿主 trial 的 agent 目录（见 docs/pier-adapter.md §3）"
            )

    # ------------------------------------------------------------------ run
    async def run(
        self,
        instruction: str,
        environment: BaseEnvironment,
        context: AgentContext,
    ) -> None:
        instruction_path = self.logs_dir / "instruction.txt"
        instruction_path.write_text(instruction)
        container_instruction = f"{AGENT_LOG_DIR}/instruction.txt"

        wall_seconds = int(os.environ.get("PIER_DSH_WALL_SECONDS", "3600"))
        started = time.monotonic()
        try:
            # timeout 是硬上限：超时先 TERM 再 KILL，退出码 124/137 记失败，不抛基础设施错误。
            command = (
                f"timeout --signal=TERM --kill-after=30 {wall_seconds} "
                f"{LAUNCHER} \"$(cat {container_instruction})\" "
                f"> {AGENT_LOG_DIR}/dsh-stdout.txt 2> {AGENT_LOG_DIR}/dsh-stderr.txt"
            )
            result = await environment.exec(
                command=command,
                user="root",
                timeout_sec=wall_seconds + 120,
            )
            elapsed = time.monotonic() - started
            exit_code = result.return_code
            timed_out = exit_code in (124, 137)

            await self._commit_workspace(environment)
            await self._collect_sessions(environment)

            events = self._read_session_events()
            usage = self._summarize_usage(events)
            terminal_error = self._classify_terminal(events, exit_code, timed_out)
            outcome = {
                # 契约（编排器消费）：超时被杀时退出码记 null，原始 124/137 留在 dsh-run.json。
                "exit_code": None if timed_out else exit_code,
                "timed_out": timed_out,
                "wall_seconds": round(elapsed, 3),
                "terminal_error": terminal_error,
                "llm_calls": usage["llm_calls"],
                "usage": {
                    "input": usage["input_tokens"],
                    "cached": usage["cache_read_tokens"],
                    "output": usage["output_tokens"],
                    "reasoning": None,  # pi-ai 把 reasoning tokens 折进 output，无法单独给出
                },
            }
            (self.logs_dir / "outcome.json").write_text(
                json.dumps(outcome, ensure_ascii=False, indent=2) + "\n"
            )

            record = {
                "agent": "dsh",
                "dsh_version": DSH_VERSION,
                "wall_seconds": round(elapsed, 3),
                "wall_limit_seconds": wall_seconds,
                "timed_out": timed_out,
                "dsh_exit_code": exit_code,
                "model": os.environ.get("PIER_DSH_MODEL", "gpt-6-luna"),
                "reasoning": os.environ.get("PIER_DSH_REASONING", "high"),
                "upstream": self._upstream_target_text(),
                "container_base_url": self._container_base_url(),
                "usage": usage,
                "terminal_error": terminal_error,
                "stderr_tail": self._stderr_tail(),
            }
            (self.logs_dir / "dsh-run.json").write_text(
                json.dumps(record, ensure_ascii=False, indent=2) + "\n"
            )

            context.n_input_tokens = usage["input_tokens"]
            context.n_output_tokens = usage["output_tokens"]
            context.n_agent_steps = usage["steps"]
            context.metadata = {
                "dsh_wall_seconds": record["wall_seconds"],
                "dsh_timed_out": timed_out,
                "dsh_exit_code": exit_code,
                "dsh_terminal_error": terminal_error,
            }
        except BaseException:
            # Pier 层面的失败（exec 抛错/被取消）：仍落一份 outcome，避免编排把「没有结果」
            # 当成可重试的基础设施错误；具体异常由 Pier 自己记录。
            outcome_path = self.logs_dir / "outcome.json"
            if not outcome_path.exists():
                outcome_path.write_text(
                    json.dumps(
                        {
                            "exit_code": None,
                            "timed_out": False,
                            "wall_seconds": round(time.monotonic() - started, 3),
                            "terminal_error": {"kind": "other", "status": None},
                            "llm_calls": 0,
                            "usage": {
                                "input": None,
                                "cached": None,
                                "output": None,
                                "reasoning": None,
                            },
                        },
                        ensure_ascii=False,
                        indent=2,
                    )
                    + "\n"
                )
            raise
        finally:
            self._stop_relay()

    def populate_context_post_run(self, context: AgentContext) -> None:
        """run() 已经把用量写进 context；这里只在 Pier 需要兜底时补齐记录。"""
        record_path = self.logs_dir / "dsh-run.json"
        if not record_path.exists():
            return
        try:
            record = json.loads(record_path.read_text())
        except json.JSONDecodeError:
            return
        usage = record.get("usage") or {}
        context.n_input_tokens = context.n_input_tokens or usage.get("input_tokens")
        context.n_output_tokens = context.n_output_tokens or usage.get("output_tokens")
        context.metadata = context.metadata or {
            "dsh_wall_seconds": record.get("wall_seconds"),
            "dsh_timed_out": record.get("timed_out"),
        }

    # -------------------------------------------------------------- upstream
    def _upstream_target(self) -> tuple[str, int]:
        """把配置的宿主侧 URL 折成 relay 唯一的转发目标 host:port。"""
        raw = os.environ.get("PIER_DSH_UPSTREAM") or os.environ.get(
            "PIER_DSH_BASE_URL", "http://127.0.0.1:29081/v1"
        )
        if "://" not in raw:
            raw = f"http://{raw}"
        parts = urlsplit(raw)
        host = parts.hostname or "127.0.0.1"
        port = parts.port or (443 if parts.scheme == "https" else 80)
        return host, port

    def _upstream_target_text(self) -> str:
        host, port = self._upstream_target()
        return f"{host}:{port}"

    def _container_base_url(self) -> str:
        local_port = int(os.environ.get("PIER_DSH_LOCAL_PORT", str(DEFAULT_LOCAL_PORT)))
        return f"http://127.0.0.1:{local_port}/v1"

    def _start_relay(self) -> None:
        """在宿主 trial 的 agent 目录里起一次性 relay，目标写死为本试验的路由端口。"""
        if self._relay is not None:
            return
        # Pier 给的 logs_dir 是相对路径；relay 会在该目录里 chdir 后 bind，
        # 所以这里必须传绝对路径，ready 文件也由 relay 自己按绝对目录推导。
        socket_dir = self.logs_dir.resolve()
        ready_file = socket_dir / f"{SOCKET_NAME}.ready"
        ready_file.unlink(missing_ok=True)
        host, port = self._upstream_target()
        log_path = self.logs_dir / "upstream-relay.log"
        with log_path.open("ab") as log:
            self._relay = subprocess.Popen(
                [
                    sys.executable or "python3",
                    str(HOST_RELAY_SCRIPT),
                    "--dir",
                    str(socket_dir),
                    "--name",
                    SOCKET_NAME,
                    "--target",
                    f"{host}:{port}",
                    "--parent-pid",
                    str(os.getpid()),
                ],
                stdout=log,
                stderr=log,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
            )
        atexit.register(self._stop_relay)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if ready_file.exists():
                return
            if self._relay.poll() is not None:
                break
            time.sleep(0.05)
        tail = ""
        try:
            tail = log_path.read_text()[-500:]
        except OSError:
            pass
        self._stop_relay()
        raise RuntimeError(f"上游 relay 启动失败（目标 {host}:{port}）：{tail}")

    def _stop_relay(self) -> None:
        relay, self._relay = self._relay, None
        if relay is not None:
            try:
                relay.terminate()
                relay.wait(timeout=10)
            except (OSError, subprocess.TimeoutExpired):
                try:
                    relay.kill()
                except OSError:
                    pass
        for suffix in ("", ".ready"):
            try:
                (self.logs_dir / f"{SOCKET_NAME}{suffix}").unlink()
            except FileNotFoundError:
                pass

    # ------------------------------------------------------------- workspace
    async def _commit_workspace(self, environment: BaseEnvironment) -> None:
        await environment.exec(
            " ".join(
                [
                    f"cd {WORKSPACE} &&",
                    "git config --global --add safe.directory /app 2>/dev/null;",
                    "git add -A &&",
                    "git -c user.name='pier-dsh' -c user.email='pier-dsh@example.invalid'",
                    "commit -m 'dsh agent changes' --allow-empty > /logs/agent/git-commit.txt 2>&1;",
                    "git rev-parse HEAD >> /logs/agent/git-commit.txt",
                ]
            ),
            user="root",
            timeout_sec=300,
        )

    async def _collect_sessions(self, environment: BaseEnvironment) -> None:
        # DSH 会话落在 $DSH_HOME/sessions/<cwd-slug>/session-*/session.jsonl.zstd。
        await environment.exec(
            f"mkdir -p {AGENT_LOG_DIR}/dsh-sessions && "
            f"cp -a {DSH_HOME}/sessions/. {AGENT_LOG_DIR}/dsh-sessions/ 2>/dev/null || true",
            user="root",
            timeout_sec=120,
        )

    # ---------------------------------------------------------------- 记录解析
    def _read_session_events(self) -> list[dict]:
        """把宿主上的 zstd 会话解成事件列表；没有 zstd 时返回空表（不阻断 trial）。"""
        zstd = shutil.which("zstd")
        events: list[dict] = []
        sessions = sorted((self.logs_dir / "dsh-sessions").rglob("session.jsonl.zstd"))
        if not sessions or not zstd:
            return events
        for session in sessions:
            try:
                raw = subprocess.run(
                    [zstd, "-dc", str(session)], capture_output=True, check=True, timeout=120
                ).stdout
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                continue
            for line in raw.splitlines():
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(event, dict):
                    events.append(event)
        return events

    def _summarize_usage(self, events: list[dict]) -> dict:
        """汇总 token 用量、步数与模型请求次数。

        `llm_calls` 记**模型请求次数**（含重试）：agent loop 里每个 step 恰好发起一次请求，
        重试在同一 step 上多发起一次（`llm/retry`），所以 `steps + retries` 就是总请求数，
        与是否有响应无关（失败请求没有 usage 事件）。成功的响应另外单独统计
        `model_calls`，用于和 usage 对账。
        """
        input_tokens = output_tokens = cache_read = steps = model_calls = retries = 0
        for event in events:
            kind = event.get("type")
            data = event.get("data") or {}
            if kind == "assistant/chunk":
                chunk = data.get("chunk") or {}
                if chunk.get("type") == "usage":
                    usage = chunk.get("usage") or {}
                    model_calls += 1
                    input_tokens += int(usage.get("inputTokens") or 0)
                    output_tokens += int(usage.get("outputTokens") or 0)
                    cache_read += int(usage.get("cacheReadTokens") or 0)
            elif kind == "step/start":
                steps += 1
            elif kind == "llm/retry":
                retries += 1
        return {
            "input_tokens": input_tokens or None,
            "output_tokens": output_tokens or None,
            "cache_read_tokens": cache_read or None,
            "steps": steps or None,
            "model_calls": model_calls or None,
            "retries": retries,
            "llm_calls": steps + retries,
        }

    def _stderr_tail(self, limit: int = 4000) -> str:
        path = self.logs_dir / "dsh-stderr.txt"
        try:
            text = path.read_text(errors="replace")
        except OSError:
            return ""
        return text[-limit:]

    def _classify_terminal(
        self,
        events: list[dict],
        exit_code: int | None,
        timed_out: bool,
    ) -> dict | None:
        """判定「DSH 因 LLM 请求错误终止」，返回契约里的 terminal_error（否则 None）。

        证据优先级：
        1. 最后一个 `turn/end` 的 `reason = {kind: "error", error: {code, message, status?}}`
           —— headless runner 就是据此退出 1 并打印 stderr；
        2. stderr 里的 `dsh: <CODE>: <message>`（例如启动期缺凭据时没有会话事件）；
        3. 非零退出但没有任何 LLM 失败线索 → `other`（例如 DSH 自己崩了）。
        超时被杀时 DSH 没机会写 turn/end，按契约记 None（编排不重试）。
        """
        if timed_out:
            return None
        reason = None
        for event in reversed(events):
            if event.get("type") == "turn/end":
                reason = (event.get("data") or {}).get("reason")
                break
        if isinstance(reason, dict) and reason.get("kind") == "error":
            failure = reason.get("error") or {}
            classified = self._classify_failure(
                failure.get("code"), failure.get("message") or "", failure.get("status")
            )
            if classified is not None:
                return classified
        match = _STDERR_FAILURE.search(self._stderr_tail())
        if match:
            classified = self._classify_failure(match.group(1), match.group(2), None)
            if classified is not None:
                return classified
        if exit_code not in (0, None):
            return {"kind": "other", "status": None}
        return None

    @staticmethod
    def _parse_status(message: str) -> int | None:
        for pattern in _STATUS_PATTERNS:
            match = pattern.search(message or "")
            if match:
                status = int(match.group(1))
                if 100 <= status <= 599:
                    return status
        return None

    @classmethod
    def _classify_failure(cls, code: str | None, message: str, status: object) -> dict | None:
        """把 DSH 的稳定失败代码折成契约里的 {kind, status}。"""
        code = code or ""
        parsed_status = status if isinstance(status, int) and 100 <= status <= 599 else None
        if parsed_status is None:
            parsed_status = cls._parse_status(message)
        if code in _HTTP_LIKE_CODES:
            # RATE_LIMIT 只由 429/限流措辞触发；SERVER 只由 5xx 触发。
            return {"kind": "http", "status": parsed_status or _HTTP_LIKE_CODES[code]}
        if code in _CONNECTION_CODES:
            # 没有 HTTP 响应、或响应中途断掉/超时：编排按连接类基础设施错误处理。
            return {"kind": "connection", "status": None}
        if code in _CONCURRENCY_CODES or any(
            pattern.search(message or "") for pattern in _CONCURRENCY_PATTERNS
        ):
            # 明确的并发超限：编排按基础设施错误重试。上游没给 HTTP 状态码时 status=null，
            # 不伪造 429；真给了状态码就原样保留（真实 429 已被 RATE_LIMIT 分支拦在上面）。
            return {"kind": "concurrency", "status": parsed_status}
        if code:
            # AUTH / INVALID_REQUEST / QUOTA / CONTEXT_WINDOW_EXCEEDED / EMPTY_RESPONSE /
            # MISSING_CREDENTIAL / INVALID_CREDENTIAL / UNKNOWN …：都是「不是基础设施抖动」，
            # 不触发整次重跑。仍保留解析到的状态码便于排查。
            return {"kind": "other", "status": parsed_status}
        return None
