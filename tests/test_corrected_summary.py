"""覆盖保留作废、重试去重、完成时间截断以及真实批次的分类回归。"""

import copy
import importlib.util
import json
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('corrected_summary', REPO / 'tools/corrected_summary.py')
summary = importlib.util.module_from_spec(spec)
spec.loader.exec_module(summary)
BATCH = REPO / 'results/formal-xhigh-threeway-20261028'


def row(task, route, seconds, classification='pass', **kwargs):
    return {'task': task, 'round': 1, 'route': route, 'agent_wall_seconds': seconds,
            'classification': classification, 'timed_out': False, 'exit_code': 0,
            'terminal_error': None, 'reward': 1, **kwargs}


class TimingTests(unittest.TestCase):
    def setUp(self):
        self.rows = [row('a', 'cpa', 99, 'infra-error'),
                     row('a', 'stravia', 9), row('a', 'cpa', 20), row('a', 'sub2api', 30),
                     row('b', 'stravia', 11), row('b', 'cpa', 3600, timed_out=True, exit_code=None),
                     row('b', 'sub2api', 40, exit_code=1),
                     *(row('void', route, 100, 'void') for route in summary.ROUTES)]

    def test_completion_excludes_timeouts_abnormal_exits_retries_and_voids(self):
        timing = summary.timing_summary(self.rows)
        routes = timing['by_route']
        self.assertEqual({'count': 2, 'median': 10.0, 'p90': 10.8}, routes['stravia']['normal_exit_agent_seconds'])
        self.assertEqual(20, routes['cpa']['normal_exit_agent_seconds']['median'])
        self.assertEqual(30, routes['sub2api']['normal_exit_agent_seconds']['median'])
        self.assertEqual(1810, routes['cpa']['scored_final_attempt_agent_seconds']['median'])
        self.assertEqual(1, routes['cpa']['timed_out_with_full_reward'])
        self.assertEqual(1, routes['sub2api']['other_abnormal_end'])
        self.assertEqual(1, timing['all_routes_normal_exit']['groups'])
        self.assertEqual(9, timing['all_routes_normal_exit']['by_route']['stravia']['median'])

    def test_no_completed_samples_are_null_instead_of_zero(self):
        timing = summary.timing_summary([row('a', 'cpa', 3600, timed_out=True)])
        self.assertEqual({'count': 0, 'median': None, 'p90': None},
                         timing['by_route']['cpa']['normal_exit_agent_seconds'])

    def test_unknown_duration_is_rejected(self):
        for value in (None, float('nan'), -1, True):
            with self.subTest(value=value), self.assertRaises(ValueError):
                summary.timing_summary([row('a', 'cpa', value)])


class PublishedEvidenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rows = [json.loads(line) for line in (BATCH / 'results.jsonl').read_text(encoding='utf-8').splitlines()]
        cls.original = copy.deepcopy(cls.rows)
        cls.corrected, cls.changes = summary.reclassify_scored(cls.rows)

    def test_reclassifies_all_21_full_reward_failures_without_mutating_evidence(self):
        latest = summary.analyze.latest_by_trial(self.corrected)
        for route, wanted in (('stravia', 49), ('cpa', 23), ('sub2api', 27)):
            rows = [r for r in latest.values() if r['route'] == route]
            self.assertEqual(74, sum(r['classification'] in ('pass', 'fail') for r in rows))
            self.assertEqual(6, sum(r['classification'] == 'void' for r in rows))
            self.assertEqual(wanted, sum(r['classification'] == 'pass' for r in rows))
        self.assertEqual(21, len(self.changes))
        self.assertEqual(16, sum(r['timed_out'] for r in self.changes))
        self.assertEqual(self.original, self.rows)
        self.assertEqual([r for r in self.original if r['classification'] not in ('pass', 'fail')],
                         [r for r in self.corrected if r['classification'] not in ('pass', 'fail')])

    def test_published_completed_and_common_sample_counts(self):
        timing = summary.timing_summary(self.corrected)
        self.assertEqual([72, 29, 42], [timing['by_route'][r]['normal_exit_agent_seconds']['count'] for r in summary.ROUTES])
        self.assertEqual(25, timing['all_routes_normal_exit']['groups'])


if __name__ == '__main__':
    unittest.main()
