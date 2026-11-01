"""在保留历史账本的前提下，重算分类修正后的通过率及分口径耗时。"""

import collections
import hashlib
import importlib.util
import math
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
ORCHESTRATE = REPO / 'harness/video/chat-reasoning-replay/project/scripts/orchestrate'
ROUTES = ('stravia', 'cpa', 'sub2api')
BASELINES = ('cpa', 'sub2api')


def load_module(name: str):
    spec = importlib.util.spec_from_file_location(name, ORCHESTRATE / f'{name}.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


run_batch = load_module('run_batch')
analyze = load_module('analyze')


def reclassify_scored(records: list[dict]) -> tuple[list[dict], list[dict]]:
    """仅重判最后一条有效计分记录；作废组及其历史尝试保持原样。"""
    latest = analyze.latest_by_trial(records)
    corrected, changes = [], []
    for row in records:
        revised = dict(row)
        if row is latest[(row['task'], int(row['round']), row['route'])] and row['classification'] in ('pass', 'fail'):
            # 原计分条目已通过 Pier 完整性检查；不为缺失或作废条目补造验证证据。
            outcome = {key: row.get(key) for key in ('terminal_error', 'timed_out', 'exit_code')}
            classification, reason, status = run_batch.classify(
                pier_rc=row['pier_exit_code'], trial_result={}, outcome=outcome,
                reward=row['reward'], outer_timeout=row['orchestrator_timeout'],
                interrupted=row['interrupted'], spawn_error=None,
            )
            revised.update(classification=classification, reason=reason, status_code=status)
            if (classification, reason, status) != (row['classification'], row['reason'], row['status_code']):
                changes.append({
                    'group_id': row['group_id'], 'route': row['route'], 'job_name': row['job_name'],
                    'before': {'classification': row['classification'], 'reason': row['reason']},
                    'after': {'classification': classification, 'reason': reason},
                    'reward': row['reward'], 'timed_out': row['timed_out'],
                    'terminal_error': row['terminal_error'], 'exit_code': row['exit_code'],
                    'agent_wall_seconds': row['agent_wall_seconds'],
                })
        corrected.append(revised)
    return corrected, sorted(changes, key=lambda r: (r['group_id'], r['route']))


def normal_exit(row: dict) -> bool:
    return not row.get('timed_out') and not row.get('terminal_error') and row.get('exit_code') == 0


def duration_stats(rows: list[dict]) -> dict:
    values = [row['agent_wall_seconds'] for row in rows]
    if any(isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or v < 0
           for v in values):
        raise ValueError('agent_wall_seconds 必须是有限的非负数；不以 0 填补未知耗时')
    return {'count': len(values), 'median': analyze.percentile(values, .5),
            'p90': analyze.percentile(values, .9)}


def timing_summary(records: list[dict]) -> dict:
    """区分运行时长与正常结束耗时，不把超时截断值当作完成时间。"""
    latest = analyze.latest_by_trial(records)
    scored = [r for r in latest.values() if r['classification'] in ('pass', 'fail')]
    normal = [r for r in scored if normal_exit(r)]
    groups = collections.defaultdict(set)
    for row in normal:
        groups[(row['task'], row['round'])].add(row['route'])
    common = {key for key, routes in groups.items() if set(ROUTES).issubset(routes)}
    by_route, common_by_route = {}, {}
    for route in ROUTES:
        route_rows = [r for r in scored if r['route'] == route]
        completed = [r for r in normal if r['route'] == route]
        by_route[route] = {
            'scored_final_attempt_agent_seconds': duration_stats(route_rows),
            'normal_exit_agent_seconds': duration_stats(completed),
            'normal_exit_pass_agent_seconds': duration_stats([r for r in completed if r['classification'] == 'pass']),
            'timed_out': sum(bool(r['timed_out']) for r in route_rows),
            'timed_out_with_full_reward': sum(bool(r['timed_out']) and r['reward'] >= 1 for r in route_rows),
            'other_abnormal_end': sum(not normal_exit(r) and not r['timed_out'] for r in route_rows),
        }
        shared = [r for r in completed if (r['task'], r['round']) in common]
        common_by_route[route] = {**duration_stats(shared),
                                  'passes': sum(r['classification'] == 'pass' for r in shared)}
    return {
        'unit': 'seconds',
        'scope': '仅含最新计分条目的最后一次 agent 运行；排除作废组与先前重试。',
        'completion_rule': '正常结束要求 timed_out=false、terminal_error 为空且 exit_code=0；不要求 reward=1。',
        'timeout_rule': '超时样本仅有截断运行时长；即使最终工作区满分，也无法推定首次正确完成时间。',
        'comparison_limit': '各路正常结束子集不同；共有子集同样受完成条件筛选，均不能代替全体任务的速度结论。',
        'by_route': by_route,
        'all_routes_normal_exit': {'groups': len(common), 'by_route': common_by_route},
    }


def build(batch: Path, records: list[dict]) -> dict:
    corrected, changes = reclassify_scored(records)
    latest = analyze.latest_by_trial(corrected)
    analyses = {'threeway': analyze.analyze(corrected, 'stravia', list(BASELINES), 10000, 0, .05)}
    analyses.update({f'stravia_vs_{route}': analyze.analyze(corrected, 'stravia', [route], 10000, 0, .05)
                     for route in BASELINES})
    counts = {}
    for route in ROUTES:
        rows = [r for r in latest.values() if r['route'] == route and r['classification'] in ('pass', 'fail')]
        passes = sum(r['classification'] == 'pass' for r in rows)
        counts[route] = {'passes': passes, 'scored': len(rows), 'pass_rate': passes / len(rows),
                         'new_passes': sum(r['route'] == route and r['after']['classification'] == 'pass' for r in changes)}
    baseline_mean = sum(counts[r]['pass_rate'] for r in BASELINES) / len(BASELINES)
    paired = {}
    for baseline in BASELINES:
        cells = collections.Counter()
        for (task, round_, route), row in latest.items():
            if route != 'stravia' or row['classification'] not in ('pass', 'fail'):
                continue
            other = latest[(task, round_, baseline)]
            if other['classification'] not in ('pass', 'fail'):
                continue
            mine, theirs = row['classification'] == 'pass', other['classification'] == 'pass'
            cells['both_pass' if mine and theirs else 'stravia_only' if mine else 'baseline_only' if theirs else 'both_fail'] += 1
        paired[baseline] = dict(cells)
    return {
        'batch': batch.name,
        'source_results_sha256': hashlib.sha256((batch / 'results.jsonl').read_bytes()).hexdigest(),
        'classifier_sha256': hashlib.sha256((ORCHESTRATE / 'run_batch.py').read_bytes()).hexdigest(),
        'policy': '保留原计分分母及三路作废规则；排除编排器和可重试基础设施错误后，verifier 满分优先于 agent 终态。',
        'routes': counts,
        'stravia_over_baseline_mean_pass_rate': counts['stravia']['pass_rate'] / baseline_mean,
        'analysis': analyses,
        'paired_with_stravia': paired,
        'timing': timing_summary(corrected),
        'changes': changes,
    }
