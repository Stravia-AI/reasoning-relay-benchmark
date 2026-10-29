#!/usr/bin/env bash
# 打包离线 DSH 运行包：portable node + @deepseek-ai/dsh 及其依赖。
#
# 用途：任务容器没有外网，DSH 与 node 必须在宿主准备好后上传进容器。
# 输入：
#   - 宿主的 DSH 依赖树（默认 SCRATCH/pier/dsh-bundle/node_modules，由 npm install 生成）
#   - portable node（默认取上一个批次留在 /tmp 的官方 node-v24.21.0-linux-x64，缺失则从 nodejs.org 下载）
# 输出：SCRATCH/pier/dsh-bundle.tar.gz（含 node、node_modules/、MANIFEST.json）
# 副作用：只写 SCRATCH/pier 下的文件；下载 node 时访问 nodejs.org；不接触任何凭据。
#
# 注意：node 是 glibc 构建。Alpine/musl 任务镜像里不能直接跑，见 docs/pier-adapter.md。

set -euo pipefail

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../../.." && pwd)
SCRATCH=${SCRATCH:-$REPO/.scratch/research/chat-reasoning-replay/pier}
BUNDLE=${BUNDLE:-$SCRATCH/dsh-bundle}
NODE_DIR=${NODE_DIR:-}
OUT=${OUT:-$SCRATCH/dsh-bundle.tar.gz}
DSH_VERSION=${DSH_VERSION:-0.1.0-rc.8}

stage=$SCRATCH/.bundle-stage
rm -rf "$stage"
mkdir -p "$stage/node/bin"

if [[ -z "$NODE_DIR" ]]; then
  NODE_DIR=$(ls -d /tmp/stravia-dsh-*/node-v24.21.0-linux-x64 2>/dev/null | head -1 || true)
fi
if [[ -z "$NODE_DIR" || ! -x "$NODE_DIR/bin/node" ]]; then
  echo "未找到 portable node，尝试从 nodejs.org 下载 v24.21.0" >&2
  tmp=$(mktemp -d)
  curl -fsSL -o "$tmp/node.tar.xz" https://nodejs.org/dist/v24.21.0/node-v24.21.0-linux-x64.tar.xz
  tar -xJf "$tmp/node.tar.xz" -C "$tmp"
  NODE_DIR=$tmp/node-v24.21.0-linux-x64
fi

cp "$NODE_DIR/bin/node" "$stage/node/bin/node"
chmod +x "$stage/node/bin/node"

# 只保留运行时需要的包；node_modules 里的 .bin、文档与类型定义对 DSH 无用。
cp -a "$BUNDLE/node_modules" "$stage/node_modules"
find "$stage/node_modules" -maxdepth 3 -type d \( -name "*.d.ts" -o -name "docs" -o -name "test" -o -name "tests" \) -prune -exec rm -rf {} + 2>/dev/null || true
find "$stage/node_modules" -name "*.md" -delete 2>/dev/null || true

node_version=$("$stage/node/bin/node" -v)
dsh_version=$(python3 -c "import json,sys;print(json.load(open('$stage/node_modules/@deepseek-ai/dsh/package.json'))['version'])")
hmr_version=$(python3 -c "import json,sys;print(json.load(open('$stage/node_modules/@deepseek-ai/cordis-plugin-hmr/package.json'))['version'])")
if [[ "$dsh_version" != "$DSH_VERSION" ]]; then
  echo "DSH 版本不符：期望 $DSH_VERSION，实际 $dsh_version" >&2
  exit 1
fi

cat > "$stage/MANIFEST.json" <<EOF
{
  "dsh_version": "$dsh_version",
  "node_version": "$node_version",
  "cordis_plugin_hmr_version": "$hmr_version",
  "built_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "note": "hmr 固定 1.0.16：1.0.20 去掉了 registerConfig，dsh-app-boot 0.1.0-rc.8 的 user patch 监听会崩"
}
EOF

tar -C "$stage" -czf "$OUT" node node_modules MANIFEST.json
rm -rf "$stage"
ls -lh "$OUT"
