#!/usr/bin/env bash
# 跑一条命令，同时采样容器内存、宿主内存与磁盘，输出峰值（给并发规划用）。
#
# 用途：`pier run` 期间定期抓 `podman stats`（每个容器）、`/proc/meminfo`（宿主）与 `df`
# （podman 存储 + 试验目录），跑完打印每个容器峰值、容器总量峰值、宿主可用内存最低点与
# 磁盘增量。并发建议只用实测值，不靠推算。
#
# 输入（环境变量，均可覆盖）：
#   SAMPLE_SECONDS  采样间隔，默认 2 秒
#   SAMPLE_OUT      TSV 明细路径，默认 /tmp/pier-mem-$$.tsv
#   SAMPLE_SUMMARY  JSON 摘要路径，默认 /tmp/pier-mem-$$.json
#   DISK_PATHS      额外要监控的磁盘路径，冒号分隔（默认再自动加上 podman 存储根与 $PWD）
#   PODMAN          podman 命令前缀，默认 `podman`；当 DOCKER_HOST 指向 podman 的
#                   Docker 兼容 socket 时自动改成 `podman --remote`——Pier 的容器建在
#                   那个（system rootless）store 里，本地 store 里看不到它们。
# 参数：`--` 之后是要测量的命令（必须是能在当前 shell 里跑的命令；需要 podman 的命令
#       应先通过 `devenv shell --` 进入环境再调用本脚本）。
# 输出：stdout 一行一行的摘要；TSV 与 JSON 落盘供复查。
# 副作用：只读 podman 状态、/proc 与 df，不改容器、不写业务目录。
#
# 示例：
#   devenv shell -- measure_run.sh -- bash -c 'for t in a b c; do run_pier.sh … & done; wait'

set -uo pipefail

if [[ "${1:-}" == "--" ]]; then shift; fi
SAMPLE_SECONDS=${SAMPLE_SECONDS:-2}
SAMPLE_OUT=${SAMPLE_OUT:-/tmp/pier-mem-$$.tsv}
SAMPLE_SUMMARY=${SAMPLE_SUMMARY:-/tmp/pier-mem-$$.json}
: > "$SAMPLE_OUT"

PODMAN=${PODMAN:-podman}
if [[ -n "${DOCKER_HOST:-}" ]] && $PODMAN --remote ps >/dev/null 2>&1; then
  PODMAN="$PODMAN --remote"
fi
DOCKER_HOST=${DOCKER_HOST:-}

GRAPHROOT=$($PODMAN info --format '{{.Store.GraphRoot}}' 2>/dev/null || echo "")
DISK_PATHS=${DISK_PATHS:-"${GRAPHROOT:+$GRAPHROOT:}$PWD"}

parse_mib() { # "123.4MiB / 1.2GiB" -> MiB
  awk '{v=$1; u=v; gsub(/[0-9.]+/,"",u); n=v+0;
        if (u=="GiB") n*=1024; else if (u=="KiB") n/=1024; else if (u=="B") n/=1048576;
        printf "%d", n}'
}

meminfo() { # $1=key -> kB
  awk -v k="$1" '$1==k":"{print $2}' /proc/meminfo
}

sample_disk() { # 输出 "ts<TAB>disk<TAB>path<TAB>used_mib"
  local ts=$1 path
  IFS=':' read -r -a paths <<< "$DISK_PATHS"
  for path in "${paths[@]}"; do
    [[ -n "$path" && -e "$path" ]] || continue
    local used
    used=$(df -Pm "$path" 2>/dev/null | awk 'NR==2{print $3}')
    [[ -n "$used" ]] && printf '%s\tdisk\t%s\t%s\n' "$ts" "$path" "$used"
  done
}

(
  while true; do
    ts=$(date +%s)
    $PODMAN stats --no-stream --format '{{.Name}} {{.MemUsage}}' 2>/dev/null \
      | while read -r name usage; do
          printf '%s\tcontainer\t%s\t%s\n' "$ts" "$name" "$(printf '%s' "$usage" | parse_mib)"
        done
    printf '%s\thost\tMemAvailable\t%s\n' "$ts" "$(( $(meminfo MemAvailable) / 1024 ))"
    printf '%s\thost\tMemTotal\t%s\n' "$ts" "$(( $(meminfo MemTotal) / 1024 ))"
    sample_disk "$ts"
    sleep "$SAMPLE_SECONDS"
  done >> "$SAMPLE_OUT"
) &
sampler=$!
trap 'kill $sampler 2>/dev/null || true' EXIT

set +e
"$@"
rc=$?
set -e
kill $sampler 2>/dev/null || true
sleep 1

python3 - "$SAMPLE_OUT" "$SAMPLE_SUMMARY" <<'PY'
import json
import sys
from collections import defaultdict

tsv, summary_path = sys.argv[1], sys.argv[2]
peaks = defaultdict(int)
totals = defaultdict(int)
hosts = defaultdict(list)
disks = defaultdict(list)
samples = 0
for line in open(tsv):
    parts = line.rstrip("\n").split("\t")
    if len(parts) != 4:
        continue
    ts, kind, name, value = parts
    value = int(value or 0)
    if kind == "container":
        peaks[name] = max(peaks[name], value)
        totals[ts] += value
        samples = max(samples, 0)
    elif kind == "host":
        hosts[name].append((int(ts), value))
    elif kind == "disk":
        disks[name].append((int(ts), value))

summary = {
    "containers": {name: round(mib / 1024, 2) for name, mib in sorted(peaks.items(), key=lambda kv: -kv[1])},
    "container_total_peak_gib": round(max(totals.values() or [0]) / 1024, 2),
    "container_peak_at": int(max(totals, key=totals.get)) if totals else None,
    "host_mem_available_min_gib": round(min(v for _, v in hosts.get("MemAvailable", [(0, 0)])) / 1024, 2),
    "host_mem_total_gib": round(max((v for _, v in hosts.get("MemTotal", [(0, 0)])), default=0) / 1024, 2),
    "host_samples": len(hosts.get("MemAvailable", [])),
}
disks_summary = {}
for path, points in disks.items():
    if not points:
        continue
    start = points[0][1]
    disks_summary[path] = {
        "start_used_gib": round(start / 1024, 2),
        "peak_used_gib": round(max(v for _, v in points) / 1024, 2),
        "delta_gib": round((max(v for _, v in points) - start) / 1024, 2),
    }
summary["disks"] = disks_summary

for name, gib in summary["containers"].items():
    print(f"peak {name}: {gib:.2f} GiB")
print(f"peak container total: {summary['container_total_peak_gib']:.2f} GiB")
print(f"host MemAvailable min: {summary['host_mem_available_min_gib']:.2f} GiB "
      f"(total {summary['host_mem_total_gib']:.1f} GiB, {summary['host_samples']} samples)")
for path, info in disks_summary.items():
    print(f"disk {path}: +{info['delta_gib']:.2f} GiB (peak {info['peak_used_gib']:.2f} GiB)")
with open(summary_path, "w", encoding="utf-8") as handle:
    json.dump(summary, handle, ensure_ascii=False, indent=2)
print(f"summary: {summary_path}")
print(f"samples tsv: {tsv}")
PY
exit $rc
