#!/usr/bin/env python3
"""汇总 meterd JSONL：按路线（可选按试验窗口）统计上游模型请求的推理项与用量。

用途
  读取 meterd 的 JSONL，输出每个（路线, 试验）桶的：
  - 上游模型请求数（HTTP POST 与 WS response.create，按 meterd 的 model_request 标记）
  - 每个请求 input 里的 reasoning item 数，以及带 encrypted_content 的请求占比
  - encrypted_content 长度分布（meterd 只记录长度）
  - 各 HTTP 状态码计数（429 与 5xx 可分辨，见 status / status_class）
  - 每个响应终态事件的 reasoning item 数与 usage 四类 token 总和

输入
  <meter.jsonl>                 meterd -out 生成的 JSONL
  --runs <runs.json>            可选：试验窗口 [{route, experiment, started_at, finished_at}, ...]
                                时间与 meterd 的 at 同一时钟（Unix 秒，浮点）。没有窗口时每路线一个 "all" 桶。
  --out <summary.json>          可选：写出完整 JSON（含逐请求明细）；不写则只打印表格。

副作用
  只读输入；仅在给出 --out 时写一个 JSON 文件。不读取、不打印任何凭据。
  缺字段一律按“未知”处理：不参与均值/求和的字段记为 null 并单独计数，绝不当作 0。

输出口径
  - requests[].reasoning_items 是“客户端/网关发给上游的 input 里 type=reasoning 的 item 数”；
  - responses[].reasoning_items 是“上游终态响应 output 里 type=reasoning 的 item 数”；
  - usage 只累加非 null 值，known 表示有多少条终态事件带 usage。
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path

SCHEMA = "meterd-summary/v1"


def load_rows(path: Path) -> list[dict]:
    rows = []
    with path.open("r", encoding="utf-8") as fh:
        for number, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise SystemExit(f"{path}:{number}: 不是合法 JSON 行（{exc.msg}）") from None
    return rows


def load_windows(path: Path | None) -> list[dict]:
    if path is None:
        return []
    windows = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(windows, list):
        raise SystemExit("runs.json 必须是数组")
    out = []
    for item in windows:
        try:
            out.append(
                {
                    "route": str(item["route"]),
                    "experiment": str(item.get("experiment", item.get("round", "default"))),
                    "started_at": float(item["started_at"]),
                    "finished_at": float(item["finished_at"]),
                }
            )
        except (KeyError, TypeError, ValueError):
            raise SystemExit("runs.json 每项需要 route/experiment/started_at/finished_at") from None
    return out


def is_model_request(row: dict) -> bool:
    return row.get("event") in ("http-request", "ws-message") and row.get("model_request") is True


def response_records(rows: list[dict]) -> list[dict]:
    """响应终态事件：HTTP 的 http-response-end 与 WS 的终态 ws-message。"""
    out = []
    for row in rows:
        event = row.get("event")
        if event == "http-response-end":
            if row.get("response_terminal") is None and row.get("response_parse") is None:
                # 非模型响应（例如 403 拦截页）也保留 status，但不当作模型响应统计。
                pass
            out.append(
                {
                    "flow": row.get("flow"),
                    "route": row.get("route"),
                    "at": row.get("at"),
                    "kind": "http",
                    "status": row.get("status"),
                    "status_class": row.get("status_class"),
                    "terminal": row.get("response_terminal"),
                    "response_status": row.get("response_status"),
                    "reasoning_items": row.get("response_reasoning_items"),
                    "input_tokens": row.get("usage_input_tokens"),
                    "cached_tokens": row.get("usage_cached_tokens"),
                    "output_tokens": row.get("usage_output_tokens"),
                    "reasoning_tokens": row.get("usage_reasoning_tokens"),
                    "parse": row.get("response_parse"),
                }
            )
        elif event == "ws-message" and row.get("from_client") is False and row.get("response_terminal") is not None:
            out.append(
                {
                    "flow": row.get("flow"),
                    "route": row.get("route"),
                    "at": row.get("at"),
                    "kind": "ws",
                    "status": None,
                    "status_class": None,
                    "terminal": row.get("response_terminal"),
                    "response_status": row.get("response_status"),
                    "reasoning_items": row.get("response_reasoning_items"),
                    "input_tokens": row.get("usage_input_tokens"),
                    "cached_tokens": row.get("usage_cached_tokens"),
                    "output_tokens": row.get("usage_output_tokens"),
                    "reasoning_tokens": row.get("usage_reasoning_tokens"),
                    "parse": None,
                }
            )
    return out


def status_by_flow(rows: list[dict]) -> dict:
    out = {}
    for row in rows:
        if row.get("event") == "http-response-headers":
            out[row.get("flow")] = {
                "status": row.get("status"),
                "status_class": row.get("status_class"),
            }
    return out


def assign_bucket(row: dict, windows: list[dict]) -> tuple[str, str]:
    route = str(row.get("route", "unknown"))
    at = row.get("at")
    if not windows:
        return route, "all"
    for window in windows:
        if window["route"] != route:
            continue
        if at is not None and window["started_at"] <= at <= window["finished_at"]:
            return route, window["experiment"]
    return route, "outside-windows"


def number_or_none(value):
    return value if isinstance(value, (int, float)) else None


def dist(values: list[float]) -> dict | None:
    if not values:
        return None
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "min": ordered[0],
        "p50": statistics.median(ordered),
        "p90": ordered[min(len(ordered) - 1, int(round(0.9 * (len(ordered) - 1))))],
        "max": ordered[-1],
        "mean": statistics.fmean(ordered),
    }


def summarize(rows: list[dict], windows: list[dict]) -> dict:
    statuses = status_by_flow(rows)
    responses = response_records(rows)
    responses_by_flow = defaultdict(list)
    for response in responses:
        responses_by_flow[response["flow"]].append(response)

    buckets = defaultdict(list)
    for row in rows:
        if is_model_request(row):
            buckets[assign_bucket(row, windows)].append(row)
    if windows:
        for window in windows:
            buckets.setdefault((window["route"], window["experiment"]), [])

    result = []
    for (route, experiment), requests in sorted(buckets.items()):
        requests.sort(key=lambda r: r.get("at") or 0)
        reasoning_counts = []
        reasoning_unknown = 0
        encrypted = 0
        encrypted_unknown = 0
        lengths = []
        transports = Counter()
        per_request = []
        flows = set()
        for row in requests:
            flow = row.get("flow")
            flows.add(flow)
            transports[row.get("transport") or ("ws" if row.get("event") == "ws-message" else "http")] += 1
            items = number_or_none(row.get("input_reasoning_items"))
            # 优先用 reasoning 专有的计数；兼容只有 input_encrypted_items 的旧格式。
            if "input_reasoning_encrypted_items" in row:
                enc_items = number_or_none(row.get("input_reasoning_encrypted_items"))
            else:
                enc_items = number_or_none(row.get("input_encrypted_items"))
            if items is None:
                reasoning_unknown += 1
            else:
                reasoning_counts.append(items)
            if enc_items is None:
                encrypted_unknown += 1
            else:
                if enc_items > 0:
                    encrypted += 1
            for length in row.get("input_encrypted_lengths") or []:
                if isinstance(length, (int, float)):
                    lengths.append(length)
            http_response = None
            if row.get("event") == "http-request":
                http_response = responses_by_flow.get(flow, [None])[-1]
            status = statuses.get(flow, {})
            per_request.append(
                {
                    "at": row.get("at"),
                    "flow": flow,
                    "transport": row.get("transport")
                    or ("ws" if row.get("event") == "ws-message" else "http"),
                    "reasoning_items": items,
                    "reasoning_encrypted_items": enc_items,
                    "encrypted_items": row.get("input_encrypted_items"),
                    "encrypted_lengths": row.get("input_encrypted_lengths"),
                    "previous_response_id": row.get("has_previous_response_id"),
                    "store": row.get("store"),
                    "reasoning_effort": row.get("reasoning_effort"),
                    "reasoning_summary": row.get("reasoning_summary"),
                    "include_reasoning_encrypted": row.get("include_reasoning_encrypted"),
                    "model": row.get("model"),
                    "tools_count": row.get("tools_count"),
                    "prompt_cache_key_present": row.get("has_prompt_cache_key"),
                    "request_shape": row.get("request_shape"),
                    "status": status.get("status"),
                    "status_class": status.get("status_class"),
                    "response_reasoning_items": (http_response or {}).get("reasoning_items"),
                    "usage_input_tokens": (http_response or {}).get("input_tokens"),
                    "usage_cached_tokens": (http_response or {}).get("cached_tokens"),
                    "usage_output_tokens": (http_response or {}).get("output_tokens"),
                    "usage_reasoning_tokens": (http_response or {}).get("reasoning_tokens"),
                }
            )

        # 响应按 flow 归属：flow 的请求落在哪个桶，响应就属于哪个桶（窗口边界不影响已开始的请求）。
        bucket_responses = [r for r in responses if r.get("route") == route and r.get("flow") in flows]

        status_counts = Counter()
        status_class_counts = Counter()
        for request in per_request:
            if request["status"] is not None:
                status_counts[str(request["status"])] += 1
            if request["status_class"] is not None:
                status_class_counts[request["status_class"]] += 1

        usage = {}
        known_usage = 0
        for key, attr in (
            ("input_tokens", "input_tokens"),
            ("cached_tokens", "cached_tokens"),
            ("output_tokens", "output_tokens"),
            ("reasoning_tokens", "reasoning_tokens"),
        ):
            values = [r[attr] for r in bucket_responses if isinstance(r.get(attr), (int, float))]
            usage[key] = sum(values) if values else None
        known_usage = sum(
            1
            for r in bucket_responses
            if isinstance(r.get("output_tokens"), (int, float)) or isinstance(r.get("input_tokens"), (int, float))
        )
        response_reasoning = [r["reasoning_items"] for r in bucket_responses if isinstance(r.get("reasoning_items"), (int, float))]

        request_total = len(per_request)
        result.append(
            {
                "route": route,
                "experiment": experiment,
                "requests": request_total,
                "requests_by_transport": dict(transports),
                "requests_unknown_shape": sum(1 for r in per_request if r["request_shape"] not in ("ok",)),
                "requests_with_reasoning": sum(1 for v in reasoning_counts if v > 0),
                "reasoning_items_unknown": reasoning_unknown,
                "reasoning_items_total": int(sum(reasoning_counts)) if reasoning_counts else None,
                "reasoning_items_per_request": dist(reasoning_counts),
                "reasoning_items_histogram": {str(k): v for k, v in sorted(Counter(int(v) for v in reasoning_counts).items())},
                "requests_with_encrypted_reasoning": encrypted,
                "encrypted_reasoning_unknown": encrypted_unknown,
                "encrypted_reasoning_share": (encrypted / (request_total - encrypted_unknown)) if (request_total - encrypted_unknown) > 0 else None,
                "encrypted_reasoning_lengths": dist(lengths),
                "status_counts": dict(sorted(status_counts.items())),
                "status_class_counts": dict(sorted(status_class_counts.items())),
                "responses": len(bucket_responses),
                "response_reasoning_items_total": int(sum(response_reasoning)) if response_reasoning else None,
                "response_reasoning_items_per_response": dist(response_reasoning),
                "terminals": dict(Counter(r["terminal"] for r in bucket_responses if r.get("terminal"))),
                "usage": {**usage, "known_responses": known_usage},
                "requests_detail": per_request,
            }
        )

    return {
        "schema": SCHEMA,
        "routes": sorted({b["route"] for b in result}),
        "buckets": result,
        "row_counts": {
            "total": len(rows),
            "model_requests": sum(1 for row in rows if is_model_request(row)),
            "responses": len(responses),
        },
    }


def fmt(value, digits=0):
    if value is None:
        return "?"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def print_table(summary: dict) -> None:
    header = f"{'route':<10} {'experiment':<16} {'req':>4} {'http':>4} {'ws':>3} {'reason/req':>10} {'enc%':>6} {'status':<22} {'tokens in/cached/out/reason':<28}"
    print(header)
    print("-" * len(header))
    for bucket in summary["buckets"]:
        per = bucket["reasoning_items_per_request"]
        share = bucket["encrypted_reasoning_share"]
        status = " ".join(f"{k}:{v}" for k, v in bucket["status_counts"].items()) or "-"
        usage = bucket["usage"]
        tokens = "/".join(
            fmt(usage.get(k)) for k in ("input_tokens", "cached_tokens", "output_tokens", "reasoning_tokens")
        )
        print(
            f"{bucket['route']:<10} {bucket['experiment']:<16} {bucket['requests']:>4} "
            f"{bucket['requests_by_transport'].get('http', 0):>4} {bucket['requests_by_transport'].get('ws', 0):>3} "
            f"{fmt(per['mean'] if per else None, 2):>10} {('%d%%' % round(share * 100)) if share is not None else '?':>6} "
            f"{status:<22} {tokens:<28}"
        )


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="汇总 meterd JSONL 的推理项与用量")
    parser.add_argument("jsonl", type=Path, help="meterd -out 输出的 JSONL")
    parser.add_argument("--runs", type=Path, help="试验窗口 JSON（可选）")
    parser.add_argument("--out", type=Path, help="写出完整 summary JSON")
    parser.add_argument("--per-request", action="store_true", help="额外打印逐请求推理项")
    args = parser.parse_args(argv)

    summary = summarize(load_rows(args.jsonl), load_windows(args.runs))
    print_table(summary)
    if args.per_request:
        print()
        print(f"{'route':<10} {'experiment':<16} {'at':>16} {'flow':<14} {'tr':<4} {'reason':>6} {'enc':>3} {'enc_len':<14} {'prev':>5} {'store':>5} {'effort':<6} {'inc':>4} {'tools':>5} {'pck':>4} {'status':>6}")
        for bucket in summary["buckets"]:
            for req in bucket["requests_detail"]:
                lengths = req["encrypted_lengths"]
                lengths_text = "-" if not lengths else ",".join(str(int(v)) for v in lengths[:4]) + ("+" if len(lengths) > 4 else "")
                print(
                    f"{bucket['route']:<10} {bucket['experiment']:<16} {fmt(req['at'], 3):>16} {str(req['flow']):<14} "
                    f"{str(req['transport']):<4} {fmt(req['reasoning_items']):>6} {fmt(req['reasoning_encrypted_items']):>3} {lengths_text:<14} "
                    f"{str(req['previous_response_id']):>5} {str(req['store']):>5} {str(req['reasoning_effort']):<6} "
                    f"{str(req['include_reasoning_encrypted']):>4} {fmt(req['tools_count']):>5} {str(req['prompt_cache_key_present']):>4} {fmt(req['status']):>6}"
                )
    if args.out:
        args.out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\n写出 {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
