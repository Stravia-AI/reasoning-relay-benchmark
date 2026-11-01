"""验证官方满分结果与进程终态的分类边界。"""

import importlib.util
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
SOURCE = REPO / 'harness/video/chat-reasoning-replay/project/scripts/orchestrate/run_batch.py'
spec = importlib.util.spec_from_file_location('run_batch', SOURCE)
run_batch = importlib.util.module_from_spec(spec)
spec.loader.exec_module(run_batch)


def classify(outcome, reward=1.0, **overrides):
    args = dict(pier_rc=0, trial_result={}, outcome=outcome, reward=reward,
                outer_timeout=False, interrupted=False, spawn_error=None)
    args.update(overrides)
    return run_batch.classify(**args)


class ClassificationTests(unittest.TestCase):
    def test_full_reward_survives_agent_timeout_or_exit(self):
        for outcome in ({'timed_out': True}, {'exit_code': 1}, {'exit_code': 0},
                        {'exit_code': 1, 'terminal_error': {'kind': 'other', 'status': None}}):
            with self.subTest(outcome=outcome):
                self.assertEqual(('pass', 'reward', None), classify(outcome))

    def test_unsolved_timeout_and_exit_still_fail(self):
        self.assertEqual(('fail', 'timed_out', None), classify({'timed_out': True}, 0))
        self.assertEqual(('fail', 'agent-exit-1', None), classify({'exit_code': 1}, .5))

    def test_infrastructure_and_harness_errors_keep_priority(self):
        for terminal, expected in (
            ({'kind': 'concurrency'}, ('infra-error', 'infra:concurrency', None)),
            ({'kind': 'http', 'status': 503}, ('infra-error', 'infra:http:503', 503)),
        ):
            with self.subTest(terminal=terminal):
                self.assertEqual(expected, classify({'terminal_error': terminal}))
        self.assertEqual(('fail', 'agent-error:http:400', 400),
                         classify({'terminal_error': {'kind': 'http', 'status': 400}}, 0))
        self.assertEqual(('harness-error', 'orchestrator-wall', None),
                         classify({}, outer_timeout=True))
        self.assertEqual(('harness-error', 'missing-reward', None), classify({}, None))


if __name__ == '__main__':
    unittest.main()
