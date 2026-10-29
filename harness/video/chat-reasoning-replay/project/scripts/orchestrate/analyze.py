#!/usr/bin/env python3
"""分析编排结果：各路通过率、Stravia 与另两路平均的差、按任务聚类的 bootstrap CI。

用途
- 读批次目录里的 results.jsonl（每条试验以该 (task, round, route) 的最后一条记录为准）。
- 只把 classification ∈ {pass, fail} 的记录计入通过率：void（三路一并作废）、infra-error /
  harness-error（还没跑出分数的尝试）、interrupted（被打断的尝试）都不进分母，单独计数。
- 通过 = reward >= 1.0（DeepSWE 的 reward 是 0/1；partial 只记录不参与判定）。
- 差值 = 对照路线（默认 stravia）的通过率 − 其余路线通过率的平均。
- 置信区间用按任务聚类的 bootstrap：以任务为重抽样单位（同一任务的 4 轮是相关的，不能当
  独立样本），固定种子，默认 10000 次，取 2.5%/97.5% 分位。

输入
  <results.jsonl>  必填
  --contrast-route stravia --baseline-routes cpa,sub2api
  --iterations 10000 --seed 0 --alpha 0.05
  --out <analysis.json>  可选，写完整 JSON

副作用
  只读输入；仅在给 --out 时写一个 JSON 文件。不读凭据。

示例
  python3 analyze.py .scratch/research/chat-reasoning-replay/pier/batches/<id>/results.jsonl
"""

from __future__ import annotations

import argparse
import json
import math
import random
import sys
from collections import defaultdict
from pathlib import Path

SCHEMA = "orchestrate-analysis/v1"
SCORED = ("pass", "fail")


def load_records(path: Path) -> list[dict]:
    rows = []
    with path.open("r", encoding="utf-8") as handle:
        for number, line in enumerate(handle, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                raise SystemExit(f"{path}:{number}: 不是合法 JSON 行") from None
    return rows


def latest_by_trial(records: list[dict]) -> dict[tuple[str, int, str], dict]:
    latest: dict[tuple[str, int, str], dict] = {}
    for record in records:
        latest[(record["task"], int(record["round"]), record["route"])] = record
    return latest


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    pos = (len(ordered) - 1) * q
    low = math.floor(pos)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (pos - low)


def round_or_none(value: float | None, digits: int = 4) -> float | None:
    return None if value is None else round(value, digits)


def route_rates(rows: list[tuple[str, str, bool]]) -> dict[str, dict]:
    buckets: dict[str, list[bool]] = defaultdict(list)
    for _, route, passed in rows:
        buckets[route].append(passed)
    return {
        route: {
            "trials": len(values),
            "passes": sum(1 for value in values if value),
            "pass_rate": round(sum(1 for value in values if value) / len(values), 4),
        }
        for route, values in sorted(buckets.items())
    }


def cluster_bootstrap(
    rows: list[tuple[str, str, bool]],
    routes: list[str],
    contrast: str,
    baselines: list[str],
    iterations: int,
    seed: int,
    alpha: float,
) -> dict:
    """以任务为单位的 bootstrap。返回差值与各路通过率的区间。"""
    per_task: dict[str, dict[str, list[int]]] = defaultdict(
        lambda: {route: [0, 0] for route in routes}
    )
    for task, route, passed in rows:
        if route not in per_task[task]:
            per_task[task][route] = [0, 0]
        per_task[task][route][1] += 1
        per_task[task][route][0] += 1 if passed else 0
    tasks = sorted(per_task)
    rng = random.Random(seed)

    diffs: list[float] = []
    per_route: dict[str, list[float]] = {route: [] for route in routes}
    invalid = 0
    for _ in range(iterations):
        sample = rng.choices(tasks, k=len(tasks)) if tasks else []
        counts = {route: [0, 0] for route in routes}
        for task in sample:
            for route in routes:
                counts[route][0] += per_task[task][route][0]
                counts[route][1] += per_task[task][route][1]
        rates = {}
        complete = True
        for route in routes:
            if counts[route][1] == 0:
                complete = False
                break
            rates[route] = counts[route][0] / counts[route][1]
        if not complete or contrast not in rates or not all(r in rates for r in baselines):
            invalid += 1
            continue
        for route in routes:
            per_route[route].append(rates[route])
        diffs.append(rates[contrast] - sum(rates[r] for r in baselines) / len(baselines))

    low_q, high_q = alpha / 2, 1 - alpha / 2
    return {
        "iterations": iterations,
        "seed": seed,
        "alpha": alpha,
        "clusters": len(tasks),
        "valid_replicates": len(diffs),
        "invalid_replicates": invalid,
        "diff_ci": [
            round_or_none(percentile(diffs, low_q)),
            round_or_none(percentile(diffs, high_q)),
        ],
        "route_ci": {
            route: [
                round_or_none(percentile(values, low_q)),
                round_or_none(percentile(values, high_q)),
            ]
            for route, values in per_route.items()
        },
    }


def analyze(
    records: list[dict],
    contrast: str,
    baselines: list[str],
    iterations: int,
    seed: int,
    alpha: float,
) -> dict:
    latest = latest_by_trial(records)
    excluded: dict[str, int] = defaultdict(int)
    rows: list[tuple[str, str, bool]] = []
    for record in latest.values():
        classification = record.get("classification")
        if classification in SCORED:
            rows.append((record["task"], record["route"], classification == "pass"))
        else:
            excluded[str(classification)] += 1

    routes = sorted({route for _, route, _ in rows})
    if contrast not in routes:
        raise SystemExit(f"对照路线 {contrast} 没有计分试验（有：{', '.join(routes) or '无'}）")
    present_baselines = [route for route in baselines if route in routes]
    if not present_baselines:
        raise SystemExit(f"基线路线 {baselines} 都没有计分试验")

    per_route = route_rates(rows)
    rates = {route: stats["pass_rate"] for route, stats in per_route.items()}
    baseline_mean = sum(rates[route] for route in present_baselines) / len(present_baselines)
    diff = rates[contrast] - baseline_mean
    bootstrap = cluster_bootstrap(
        rows, routes, contrast, present_baselines, iterations, seed, alpha
    )
    return {
        "schema": SCHEMA,
        "scored_trials": len(rows),
        "excluded_trials": dict(sorted(excluded.items())),
        "tasks": len({task for task, _, _ in rows}),
        "routes": per_route,
        "contrast": {
            "route": contrast,
            "baselines": present_baselines,
            "missing_baselines": [route for route in baselines if route not in routes],
            "route_rate": rates[contrast],
            "baseline_mean": round(baseline_mean, 4),
            "diff": round(diff, 4),
            "ci95": bootstrap["diff_ci"],
            "bootstrap": bootstrap,
        },
    }


def print_report(result: dict) -> None:
    print(f"计分试验 {result['scored_trials']} 条，任务 {result['tasks']} 个")
    if result["excluded_trials"]:
        excluded = ", ".join(f"{name}={count}" for name, count in result["excluded_trials"].items())
        print(f"未计分（不计入分母）：{excluded}")
    print("")
    print(f"{'route':<10}{'trials':>8}{'passes':>8}{'rate':>8}{'95% CI':>18}")
    contrast_route = result["contrast"]["route"]
    ordered = [contrast_route] + [r for r in result["routes"] if r != contrast_route]
    for route in ordered:
        stats = result["routes"][route]
        ci = result["contrast"]["bootstrap"]["route_ci"].get(route) or [None, None]
        ci_text = f"[{ci[0]}, {ci[1]}]" if ci[0] is not None else "-"
        print(
            f"{route:<10}{stats['trials']:>8}{stats['passes']:>8}"
            f"{stats['pass_rate']:>8.3f}{ci_text:>18}"
        )
    contrast = result["contrast"]
    print("")
    print(
        f"对照：{contrast['route']} − mean({', '.join(contrast['baselines'])}) = "
        f"{contrast['route_rate']:.3f} − {contrast['baseline_mean']:.3f} = "
        f"{contrast['diff']:+.4f}"
    )
    bootstrap = contrast["bootstrap"]
    ci = contrast["ci95"]
    print(
        f"按任务聚类的 bootstrap 95% CI（{bootstrap['iterations']} 次，seed={bootstrap['seed']}，"
        f"任务 {bootstrap['clusters']} 个，有效重抽样 {bootstrap['valid_replicates']} 次）："
        f"[{ci[0]}, {ci[1]}]"
    )
    if ci[0] is not None and ci[1] is not None and ci[0] <= 0 <= ci[1]:
        print("区间跨 0：这个样本量下不能排除「差值为 0」。")
    if contrast["missing_baselines"]:
        print(f"[warn] 基线路线缺计分试验，已从平均里去掉：{contrast['missing_baselines']}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("results", type=Path, help="results.jsonl")
    parser.add_argument("--contrast-route", default="stravia")
    parser.add_argument("--baseline-routes", default="cpa,sub2api")
    parser.add_argument("--iterations", type=int, default=10000)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--alpha", type=float, default=0.05)
    parser.add_argument("--out", type=Path, default=None, help="写完整 JSON")
    args = parser.parse_args(argv)

    baselines = [item.strip() for item in args.baseline_routes.split(",") if item.strip()]
    if not baselines:
        raise SystemExit("--baseline-routes 不能为空")
    result = analyze(
        load_records(args.results),
        args.contrast_route,
        baselines,
        args.iterations,
        args.seed,
        args.alpha,
    )
    print_report(result)
    if args.out:
        args.out.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
