#!/usr/bin/env python3
"""从 results/ 的脱敏证据重建公开数字，并与提交的汇总逐项比对。

用法（仓库根目录，Python 3.11+，只用标准库）：
  python3 tools/reproduce_summary.py          重建 results/summary/ 下的历史复核与修正结果
  python3 tools/reproduce_summary.py --check  重建后与已提交文件比对；任一不一致退出 1

做什么
- 通过率与置信区间：直接加载 harness/ 里正式批次实际使用的 analyze.py，
  在脱敏后的 results.jsonl 上重算，要求与批次目录里的 analysis*.json 完全相同。
- 其余数字（配对格、超时、token、耗时分位数、官方测试计数、上游请求里的加密推理）
  从 results.jsonl、attempts/ 与 meter-summary.json 重算，再与 final-report.json 比对。
- 逐次尝试核对 attempts/ 下的 outcome.json / dsh-run.json / reward.json 与账本一致，
  并确认模型、推理强度和客户端版本。
- token 只累计已知值；未知记 null，不补 0。

输出
  results/summary/reproduction.json  每项检查的期望值、实际值与结果
  results/summary/headline.json      视频与 README 使用的派生数字
  results/summary/corrected.json     分类修正、逐项变更清单及分口径耗时；当前结论以此为准
"""

import argparse
import collections
import importlib.util
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
RESULTS = REPO / 'results'
SUMMARY = RESULTS / 'summary'
ANALYZE = REPO / 'harness/video/chat-reasoning-replay/project/scripts/orchestrate/analyze.py'
MAIN = RESULTS / 'formal-xhigh-threeway-20261028'
ROUTES = ('stravia', 'cpa', 'sub2api')
BASELINES = ('cpa', 'sub2api')
TOKEN_FIELDS = ('input', 'cached', 'output')
BOOTSTRAP = {'iterations': 10000, 'seed': 0, 'alpha': 0.05}


def load_script(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


analyze = load_script('analyze', ANALYZE)
corrected_summary = load_script('corrected_summary', REPO / 'tools/corrected_summary.py')
checks = []


def check(name, expected, actual):
    checks.append({'name': name, 'expected': expected, 'actual': actual, 'ok': expected == actual})


def read_json(path: Path):
    return json.loads(path.read_text(encoding='utf-8'))


def read_jsonl(path: Path):
    return [json.loads(line) for line in path.read_text(encoding='utf-8').splitlines() if line.strip()]


def attempts(rows):
    """每次实际尝试一条。组作废时会为同一尝试追加一条 void 标记行，这里保留最早的原始行。"""
    seen = {}
    for row in rows:
        seen.setdefault((row['group_id'], row['route'], row['attempt']), row)
    return list(seen.values())


def scored(latest, route):
    return [row for row in latest.values() if row['route'] == route and row['classification'] in ('pass', 'fail')]


def token_totals(rows):
    known = [row for row in rows if (row.get('tokens') or {}).get('input') is not None]
    totals = {field: sum(row['tokens'][field] for row in known) for field in TOKEN_FIELDS}
    totals['llm_calls'] = sum((row.get('tokens') or {}).get('llm_calls') or 0 for row in rows)
    totals['llm_calls_with_known_tokens'] = sum(row['tokens'].get('llm_calls') or 0 for row in known)
    totals['attempts'] = len(rows)
    totals['attempts_with_known_tokens'] = len(known)
    return totals


def check_analysis(batch: Path, label: str, rows):
    pairs = [('analysis.json', list(BASELINES))] + [(f'analysis-stravia-vs-{b}.json', [b]) for b in BASELINES]
    for name, baselines in pairs:
        path = batch / name
        if not path.exists():
            continue
        rebuilt = analyze.analyze(rows, 'stravia', baselines, **BOOTSTRAP)
        check(f'{label}: {name} 与 analyze.py 重算一致', read_json(path), rebuilt)


def check_attempt_files(batch: Path, label: str, rows, reasoning: str):
    mismatches, checked = [], 0
    for row in rows:
        folder = batch / 'attempts' / row['job_name']
        outcome = read_json(folder / 'outcome.json')
        run = read_json(folder / 'dsh-run.json')
        checked += 1
        usage = outcome.get('usage') or {}
        tokens = row.get('tokens') or {}
        for field in TOKEN_FIELDS:
            if usage.get(field) != tokens.get(field):
                mismatches.append({'job': row['job_name'], 'field': f'usage.{field}'})
        if outcome.get('timed_out') != row.get('timed_out'):
            mismatches.append({'job': row['job_name'], 'field': 'timed_out'})
        for field, wanted in (('model', 'gpt-6-luna'), ('reasoning', reasoning), ('dsh_version', '0.1.0-rc.8')):
            if run.get(field) != wanted:
                mismatches.append({'job': row['job_name'], 'field': field})
        reward_path = folder / 'reward.json'
        if row.get('reward_present'):
            if read_json(reward_path).get('reward') != row.get('reward'):
                mismatches.append({'job': row['job_name'], 'field': 'reward'})
        elif reward_path.exists():
            mismatches.append({'job': row['job_name'], 'field': 'unexpected reward.json'})
    check(f'{label}: attempts/ 与账本逐次一致（{checked} 次尝试）', [], mismatches)


def official_counts(batch: Path, latest, route):
    totals = collections.Counter()
    for row in scored(latest, route):
        reward = read_json(batch / 'attempts' / row['job_name'] / 'reward.json')
        for field in ('f2p_total', 'f2p_passed', 'p2p_total', 'p2p_passed'):
            totals[field] += reward[field]
    return dict(totals)


def clock(rows, route):
    grouped = collections.defaultdict(float)
    for row in rows:
        if row['route'] == route:
            grouped[(row['task'], row['round'])] += row['agent_wall_seconds']
    values = list(grouped.values())
    return {'median': analyze.percentile(values, .5), 'p90': analyze.percentile(values, .9),
            'all_attempt_seconds': round(sum(row['agent_wall_seconds'] for row in rows if row['route'] == route), 3)}


def meter_routes(batch: Path):
    routes = {}
    for bucket in read_json(batch / 'meter-summary.json')['buckets']:
        if bucket.get('experiment') == 'outside-windows':
            continue
        totals = routes.setdefault(bucket['route'], collections.Counter())
        rollup = bucket['requests_detail_rollup']
        totals['requests'] += bucket['requests']
        totals['tools_present'] += rollup['tools_present']
        totals['with_encrypted_reasoning'] += rollup['with_encrypted_reasoning']
        totals['reasoning_items'] += rollup['reasoning_items']
        totals['previous_response_id_present'] += rollup['previous_response_id_present']
        for transport, count in rollup['transport'].items():
            totals[f'transport_{transport}'] += count
        for effort, count in rollup['tools_present_reasoning_effort'].items():
            totals[f'tools_present_effort_{effort}'] += count
    return {route: dict(values) for route, values in sorted(routes.items())}


def main_batch():
    report = read_json(MAIN / 'final-report.json')
    rows = read_jsonl(MAIN / 'results.jsonl')
    tries = attempts(rows)
    latest = analyze.latest_by_trial(rows)
    check_analysis(MAIN, 'xhigh', rows)
    check('xhigh: 实际尝试数', report['totals']['attempts'], len(tries))
    check_attempt_files(MAIN, 'xhigh', tries, 'xhigh')

    routes = {}
    for route in ROUTES:
        passes = sum(1 for row in scored(latest, route) if row['classification'] == 'pass')
        denominator = len(scored(latest, route))
        expected = report['current_threeway_scores'][route]
        check(f'xhigh/{route}: 通过数与分母', [expected['passes'], expected['scored_denominator']], [passes, denominator])

        route_tries = [row for row in tries if row['route'] == route]
        tokens = token_totals(route_tries)
        reported = report['all_attempt_usage']['by_route'][route]['tokens_and_calls']
        check(f'xhigh/{route}: 全部尝试 token 与调用数',
              [reported[field] for field in (*TOKEN_FIELDS, 'llm_calls')],
              [tokens[field] for field in (*TOKEN_FIELDS, 'llm_calls')])

        timeouts_all = sum(1 for row in route_tries if row.get('timed_out'))
        check(f'xhigh/{route}: 全部尝试超时数', report['all_attempt_agent_timeout_counts'][route], timeouts_all)
        timeouts_scored = sum(1 for row in scored(latest, route) if row.get('timed_out'))
        check(f'xhigh/{route}: 计分试验超时数',
              report['scored_failure_reason_counts_by_route'][route].get('agent-timeout', 0), timeouts_scored)

        clocks = clock(tries, route)
        reported_clock = report['clocks_by_route'][route]['agent_wall_seconds']
        check(f'xhigh/{route}: agent 耗时（每试验含重试）中位数 / P90 / 合计',
              [reported_clock['median_per_trial_including_retries'], reported_clock['p90_per_trial_including_retries'],
               reported_clock['all_attempt_seconds']],
              [clocks['median'], clocks['p90'], clocks['all_attempt_seconds']])

        counts = official_counts(MAIN, latest, route)
        check(f'xhigh/{route}: 官方测试计数', report['official_test_counts_latest'][route], counts)
        routes[route] = {'passes': passes, 'scored': denominator, 'pass_rate': passes / denominator,
                         'timeouts_scored': timeouts_scored, 'tokens': tokens, 'agent_seconds': clocks,
                         'official_tests': counts}

    for baseline in BASELINES:
        cells = collections.Counter()
        for (task, round_, route), row in latest.items():
            if route != 'stravia':
                continue
            other = latest.get((task, round_, baseline))
            if row['classification'] not in ('pass', 'fail') or not other or other['classification'] not in ('pass', 'fail'):
                continue
            mine, theirs = row['classification'] == 'pass', other['classification'] == 'pass'
            cells['both_pass' if mine and theirs else 'stravia_only' if mine else 'baseline_only' if theirs else 'both_fail'] += 1
        check(f'xhigh: 与 {baseline} 的配对格', report['paired_cells'][baseline], dict(cells))
        routes[baseline]['paired_with_stravia'] = dict(cells)

    meter = meter_routes(MAIN)
    for route in ROUTES:
        audit = report['meter']['accepted_window_audit_by_route'][route]
        main_loop = audit['request_groups']['main_tool_loop']
        check(f'xhigh/{route}: 计量窗口内上游请求 / 带工具主循环请求 / 强度分布',
              [audit['accepted_window_requests'], main_loop['requests'], main_loop['reasoning_effort_distribution']],
              [meter[route]['requests'], meter[route]['tools_present'],
               {key.removeprefix('tools_present_effort_'): value for key, value in meter[route].items()
                if key.startswith('tools_present_effort_')}])
    return report, routes, meter


def build():
    report, routes, meter = main_batch()
    baseline_rate = sum(routes[b]['pass_rate'] for b in BASELINES) / len(BASELINES)
    baseline_median = sum(routes[b]['agent_seconds']['median'] for b in BASELINES) / len(BASELINES)
    contrast = read_json(MAIN / 'analysis.json')['contrast']
    tokens = {field: sum(routes[route]['tokens'][field] for route in ROUTES) for field in TOKEN_FIELDS}
    tokens['total'] = sum(tokens[field] for field in TOKEN_FIELDS)
    tokens['cached_share'] = tokens['cached'] / tokens['total']
    tokens['attempts'] = sum(routes[route]['tokens']['attempts'] for route in ROUTES)
    tokens['attempts_with_known_tokens'] = sum(routes[route]['tokens']['attempts_with_known_tokens'] for route in ROUTES)
    headline = {
        'batch': MAIN.name,
        'design': report['design'],
        'pass_rate': {route: routes[route]['pass_rate'] for route in ROUTES},
        'passes': {route: f"{routes[route]['passes']}/{routes[route]['scored']}" for route in ROUTES},
        'stravia_over_baseline_mean_pass_rate': routes['stravia']['pass_rate'] / baseline_rate,
        'stravia_minus_baseline_mean_pp': {'diff': contrast['diff'], 'ci95': contrast['ci95']},
        'agent_median_minutes': {route: routes[route]['agent_seconds']['median'] / 60 for route in ROUTES},
        'baseline_mean_median_over_stravia_median': baseline_median / routes['stravia']['agent_seconds']['median'],
        'llm_calls': {route: routes[route]['tokens']['llm_calls'] for route in ROUTES},
        # 全部尝试的 agent 耗时 ÷ 全部 DSH 调用数；一步 = 模型回一次 + 执行这一步的工具。
        'agent_seconds_per_llm_call': {route: routes[route]['agent_seconds']['all_attempt_seconds'] / routes[route]['tokens']['llm_calls']
                                       for route in ROUTES},
        'output_tokens_per_call': {route: routes[route]['tokens']['output'] / routes[route]['tokens']['llm_calls_with_known_tokens']
                                   for route in ROUTES},
        'scored_timeouts': {route: routes[route]['timeouts_scored'] for route in ROUTES},
        'paired_with_stravia': {b: routes[b]['paired_with_stravia'] for b in BASELINES},
        'official_tests': {route: routes[route]['official_tests'] for route in ROUTES},
        'upstream_requests_with_encrypted_reasoning': {
            route: {'with': meter[route]['with_encrypted_reasoning'], 'requests': meter[route]['requests'],
                    'reasoning_items': meter[route]['reasoning_items']} for route in ROUTES},
        'upstream_transport': {route: {k.removeprefix('transport_'): v for k, v in meter[route].items() if k.startswith('transport_')}
                               for route in ROUTES},
        # 总 token = 输入（不含缓存）+ 缓存输入 + 输出；未知用量不计入，因此是下限。
        'tokens_all_attempts': tokens,
    }
    failed = [item['name'] for item in checks if not item['ok']]
    reproduction = {'schema': 'reproduction/v1', 'checks': len(checks), 'failed': failed, 'items': checks}
    return {'reproduction.json': reproduction, 'headline.json': headline,
            'corrected.json': corrected_summary.build(MAIN, read_jsonl(MAIN / 'results.jsonl'))}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    outputs = {name: json.dumps(data, ensure_ascii=False, indent=2) + '\n' for name, data in build().items()}
    failed = json.loads(outputs['reproduction.json'])['failed']
    if args.check:
        stale = [name for name, text in outputs.items()
                 if not (SUMMARY / name).exists() or (SUMMARY / name).read_text(encoding='utf-8') != text]
        print(f'checks={len(checks)} failed={len(failed)} stale={stale}')
        return 1 if failed or stale else 0
    SUMMARY.mkdir(parents=True, exist_ok=True)
    for name, text in outputs.items():
        (SUMMARY / name).write_text(text, encoding='utf-8', newline='\n')
    print(f'checks={len(checks)} failed={len(failed)}')
    for name in failed:
        print(f'FAIL {name}')
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
