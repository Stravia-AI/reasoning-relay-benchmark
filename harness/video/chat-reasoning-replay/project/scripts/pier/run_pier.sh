#!/usr/bin/env bash
# Pier 运行器：统一带上 rootless podman 兼容层与离线包路径。
#
# 用途：在 devenv shell 里跑 pier 时必须带 PIER_ROOTLESS_COMPAT=1 与
# PYTHONPATH=<本目录>/rootless_compat，否则 Pier 的 chown 会按宿主 uid 改容器文件，
# rootless podman 下 uid 不可映射直接报错；顺带固定 DSH 离线包、上游地址、模型等参数。
#
# 输入（环境变量，均可覆盖）：
#   BENCH_API_KEY        上游密钥，只透传不打印；用 ${BENCH_API_KEY} 模板交给 Pier 在作业
#                        启动时从宿主环境解析，避免出现在命令行里。
#   PIER_DSH_BASE_URL    **宿主侧**上游 URL，默认 http://127.0.0.1:29081/v1
#                        （容器内的 baseURL 由 dsh_agent 换成 http://127.0.0.1:<本地端口>/v1，
#                        经宿主挂进容器的 Unix socket 转发，见 docs/pier-adapter.md §3）
#   PIER_DSH_LOCAL_PORT  容器内 DSH 用的回环端口，默认 18080
#   PIER_DSH_MODEL / PIER_DSH_REASONING / PIER_DSH_WALL_SECONDS
#   其余参数原样传给 pier run。
#
# 副作用：启动 pier、写 jobs 目录；不主动联网（除任务镜像拉取）。
#
# 示例：
#   scripts/pier/run_pier.sh -p $SCRATCH/pier/pilot-tasks/<id> \
#     --agent-import-path dsh_agent:DshAgent \
#     -o $SCRATCH/pier/jobs --job-name dsh-mock-01 -y

set -euo pipefail

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../../.." && pwd)
SCRIPTS=$REPO/video/chat-reasoning-replay/project/scripts/pier
SCRATCH=${SCRATCH:-$REPO/.scratch/research/chat-reasoning-replay/pier}
PIER=${PIER:-$SCRATCH/bin/pier}

export PIER_ROOTLESS_COMPAT=1
# $SCRIPTS 在 PYTHONPATH 里，--agent-import-path 才能写成 dsh_agent:DshAgent。
export PYTHONPATH="$SCRIPTS:$SCRIPTS/rootless_compat${PYTHONPATH:+:$PYTHONPATH}"
export PIER_DSH_BUNDLE=${PIER_DSH_BUNDLE:-$SCRATCH/dsh-bundle.tar.gz}
export PIER_DSH_BASE_URL=${PIER_DSH_BASE_URL:-http://127.0.0.1:29081/v1}
export PIER_DSH_LOCAL_PORT=${PIER_DSH_LOCAL_PORT:-18080}
export PIER_DSH_MODEL=${PIER_DSH_MODEL:-gpt-6-luna}
export PIER_DSH_REASONING=${PIER_DSH_REASONING:-high}
export PIER_DSH_WALL_SECONDS=${PIER_DSH_WALL_SECONDS:-3600}

args=("$@")
if [[ -n "${BENCH_API_KEY:-}" && " ${args[*]} " != *" --ae "* ]]; then
  args+=(--ae "BENCH_API_KEY=\${BENCH_API_KEY}")
fi

exec "$PIER" run "${args[@]}"
