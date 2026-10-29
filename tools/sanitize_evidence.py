#!/usr/bin/env python3
"""把批次证据脱敏复制到 results/，并在末尾断言没有残留。

用法（仓库根目录）：
  python3 tools/sanitize_evidence.py --source /path/to/batches/<batch-id> \
    --dest results/<batch-id> [--attempts] [--only plan.json,results.jsonl]

- 默认只复制源目录顶层的 .json / .jsonl；`--only` 改为只复制列出的文件。
  不复制 jobs/、tasks/、logs/、会话记录、补丁、测试输出或任何私有目录。
- `--attempts`：对 results.jsonl 里每次实际尝试（同一 group/route/attempt 只取一次），
  从它的 trial_dir 导出三份文件到 attempts/<job_name>/：
    agent/outcome.json   DSH 退出、超时、终态类别、调用数与 token
    agent/dsh-run.json   版本、模型、强度、时限与用量；删除 stderr_tail（可能含任意错误原文）
    verifier/reward.json 官方 verifier 的 reward 与 f2p / p2p 计数
  源文件缺失时记入清单的 missing，不补造。
- meter-summary.json 只保留本批时间窗内的 bucket，逐请求明细 buckets[].requests_detail
  折叠为 requests_detail_rollup 计数；bucket 原有汇总字段不变。
- 批次与审计记录里指向本批以外批次的冻结哈希、核对位和说明句在发布前删除（见 TRANSFORMS）。
- 替换只针对路径、邮箱、令牌等字符串；每个 JSON / JSONL 在替换前后比较数值与结构，
  不一致即失败。目标目录会先清空再重建，只允许位于仓库的 results/ 之下。
"""

import argparse
import hashlib
import json
import re
import shutil
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
RESULTS = REPO / 'results'
TEXT_SUFFIXES = {'.json', '.jsonl'}
ATTEMPT_FILES = {
    'outcome.json': Path('agent/outcome.json'),
    'dsh-run.json': Path('agent/dsh-run.json'),
    'reward.json': Path('verifier/reward.json'),
}


def drop_stderr_tail(data):
    # stderr_tail 是 DSH 进程尾部输出，可能夹带任意错误原文；归一化后的失败类别见 final-report.json。
    data.pop('stderr_tail', None)
    return data


def rollup_meter_detail(data):
    """逐请求明细（时间戳、flow 编号、每条密文长度）体量约 100 MB 且不必公开，
    这里折叠成每个 bucket 的计数；bucket 自带的汇总字段原样保留。"""
    for bucket in data.get('buckets', []):
        detail = bucket.pop('requests_detail', None) or []
        counts = {
            'rows': len(detail),
            'request_shape': {},
            'transport': {},
            'tools_present': 0,
            'tools_zero': 0,
            'tools_unreported': 0,
            'tools_present_reasoning_effort': {},
            'with_encrypted_reasoning': 0,
            'reasoning_items': 0,
            'previous_response_id_present': 0,
            'store_false': 0,
            'include_reasoning_encrypted': 0,
        }
        for row in detail:
            shape_name = str(row.get('request_shape'))
            counts['request_shape'][shape_name] = counts['request_shape'].get(shape_name, 0) + 1
            transport = str(row.get('transport'))
            counts['transport'][transport] = counts['transport'].get(transport, 0) + 1
            if shape_name != 'ok':
                continue
            tools = row.get('tools_count')
            if tools is None:
                counts['tools_unreported'] += 1
            elif tools > 0:
                counts['tools_present'] += 1
                effort = str(row.get('reasoning_effort'))
                efforts = counts['tools_present_reasoning_effort']
                efforts[effort] = efforts.get(effort, 0) + 1
            else:
                counts['tools_zero'] += 1
            counts['with_encrypted_reasoning'] += (row.get('reasoning_encrypted_items') or 0) > 0
            counts['reasoning_items'] += row.get('reasoning_items') or 0
            counts['previous_response_id_present'] += row.get('previous_response_id') not in (None, '')
            counts['store_false'] += row.get('store') is False
            counts['include_reasoning_encrypted'] += bool(row.get('include_reasoning_encrypted'))
        bucket['requests_detail_rollup'] = counts
    return data


# 本仓库只发布 xhigh 正式批。批次与审计记录里有几处指向本批以外批次的冻结哈希、核对位和说明句，
# 发布前删除；计量日志是跨批次累积的，时间窗外的 bucket 与全日志行数同样删除。
OTHER_BATCH_KEYS = {
    'historical_baselines', 'baseline_sha256', 'historical_baselines_are_concurrent_controls',
    'baseline_frozen_hashes_unchanged', 'frozen_historical_baselines_unchanged',
}
OTHER_BATCH_SENTENCE = 'Historical high'


def drop_other_batches(data):
    if isinstance(data, dict):
        return {key: drop_other_batches(value) for key, value in data.items() if key not in OTHER_BATCH_KEYS}
    if isinstance(data, list):
        return [drop_other_batches(item) for item in data
                if not (isinstance(item, str) and item.startswith(OTHER_BATCH_SENTENCE))]
    return data


def meter_in_window(data):
    data['buckets'] = [bucket for bucket in data.get('buckets', []) if bucket.get('experiment') != 'outside-windows']
    data.pop('row_counts', None)
    return rollup_meter_detail(data)


OTHER_BATCH_LABEL = 'drop references to batches outside this release'

# 按文件名在脱敏前做的结构变换；数值与结构比对在变换之后进行。
TRANSFORMS = {
    'dsh-run.json': ('drop stderr_tail', drop_stderr_tail),
    'meter-summary.json': ('drop outside-windows buckets and row_counts; fold buckets[].requests_detail '
                           'into requests_detail_rollup', meter_in_window),
    'environment.json': (OTHER_BATCH_LABEL, drop_other_batches),
    'final-report.json': (OTHER_BATCH_LABEL, drop_other_batches),
    'strict-audit-result.json': (OTHER_BATCH_LABEL, drop_other_batches),
    'completion-accounting.json': (OTHER_BATCH_LABEL, drop_other_batches),
    'final-runtime.json': (OTHER_BATCH_LABEL, drop_other_batches),
    'closure.json': (OTHER_BATCH_LABEL, drop_other_batches),
}

# 顺序有意义：先替换仓库根，再处理其余绝对路径与凭据。
REPLACEMENTS = [
    ('repo path', re.compile(r'/srv/nas/user/[^/\s"\']+/home/projects/StraviaMarketing'), '<repo>'),
    ('private path', re.compile(r'/srv/nas/[^\s"\',]+'), '<private-path>'),
    ('home path', re.compile(r'/(?:home|Users)/[^\s"\',]+'), '<home>'),
    ('runtime dir', re.compile(r'/run/user/\d+[^\s"\',]*'), '<runtime-dir>'),
    ('tmp path', re.compile(r'/tmp/[^\s"\',]+'), '<tmp>'),
    ('shm path', re.compile(r'/dev/shm/[^\s"\',]+'), '<private>'),
    ('cgnat address', re.compile(r'(?<![\d.])100\.(?:6[4-9]|[7-9]\d|1[01]\d|12[0-7])\.\d{1,3}\.\d{1,3}(?![\d.])'), '<private-ip>'),
    ('lan address', re.compile(r'(?<![\d.])(?:10\.\d{1,3}|192\.168|172\.(?:1[6-9]|2\d|3[01]))\.\d{1,3}\.\d{1,3}(?![\d.])'), '<private-ip>'),
    ('email', re.compile(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}'), '<email>'),
    ('github token', re.compile(r'gh[opsu]_[A-Za-z0-9]{20,}'), '<redacted-token>'),
    ('github pat', re.compile(r'github_pat_[A-Za-z0-9_]{20,}'), '<redacted-token>'),
    ('api key', re.compile(r'(?<![A-Za-z0-9])sk-[A-Za-z0-9_-]{16,}'), '<redacted-token>'),
    ('jwt', re.compile(r'eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{8,}'), '<redacted-token>'),
    ('bearer token', re.compile(r'Bearer\s+[A-Za-z0-9._~+/-]{16,}'), 'Bearer <redacted-token>'),
    ('token field', re.compile(r'("(?:access|refresh|id)_token"\s*:\s*)"[^"]*"'), r'\1""'),
    ('private key', re.compile(r'-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----', re.S), '<redacted-private-key>'),
]

# 残留检查用「凭据形状」而不是裸关键字，避免误伤字段名或文档。
RESIDUAL = [
    ('/srv/nas', re.compile(r'/srv/nas')),
    ('/home/', re.compile(r'/home/')),
    ('/run/user/', re.compile(r'/run/user/')),
    ('/dev/shm/', re.compile(r'/dev/shm/')),
    ('github token', re.compile(r'gh[opsu]_[A-Za-z0-9]{20,}')),
    ('sk- credential', re.compile(r'(?<![A-Za-z0-9])sk-[A-Za-z0-9_-]{16,}')),
    ('jwt', re.compile(r'eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.')),
    ('Bearer credential', re.compile(r'Bearer\s+[A-Za-z0-9._~+/-]{16,}')),
    ('refresh_token value', re.compile(r'refresh_token"\s*:\s*"[^"]+')),
    ('access_token value', re.compile(r'access_token"\s*:\s*"[^"]+')),
    ('email', re.compile(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}')),
    ('private key block', re.compile(r'BEGIN [A-Z ]*PRIVATE KEY')),
    ('other batch reference', re.compile(
        r'(?:formal|pilot)-(?!xhigh-threeway-20261028)[a-z0-9]+(?:-[a-z0-9]+)*-2026\d{4}\b|stravia-dev-[0-9a-f]{8}|'
        + OTHER_BATCH_SENTENCE)),
]


def shape(value):
    """数值与结构指纹：字符串只保留类型，不比较内容。"""
    if isinstance(value, bool):
        return ('bool', value)
    if isinstance(value, (int, float)):
        return ('number', value)
    if isinstance(value, dict):
        return ('dict', tuple((key, shape(item)) for key, item in value.items()))
    if isinstance(value, list):
        return ('list', tuple(shape(item) for item in value))
    if isinstance(value, str):
        return ('str',)
    if value is None:
        return ('null',)
    return ('other',)


def parse(text: str, suffix: str):
    if suffix == '.json':
        return [json.loads(text)]
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def transform(text: str, name: str, applied: dict) -> str:
    entry = TRANSFORMS.get(name)
    if not entry:
        return text
    label, function = entry
    applied[name] = label
    return json.dumps(function(json.loads(text)), ensure_ascii=False, indent=2) + '\n'


def sanitize_text(text: str, counts: dict) -> str:
    for name, pattern, replacement in REPLACEMENTS:
        text, hits = pattern.subn(replacement, text)
        counts[name] = counts.get(name, 0) + hits
    return text


def copy_one(source: Path, target: Path, counts: dict, manifest: list, applied: dict):
    original = transform(source.read_text(encoding='utf-8'), source.name, applied)
    sanitized = sanitize_text(original, counts)
    before = [shape(item) for item in parse(original, source.suffix)]
    after = [shape(item) for item in parse(sanitized, source.suffix)]
    if before != after:
        raise SystemExit(f'{target}: 数值或结构在脱敏后发生变化')
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(sanitized, encoding='utf-8')
    # source_sha256 是脱敏前原文件的哈希，可与 final-report.json 的 input_sha256 对照。
    manifest.append({'path': target.relative_to(REPO).as_posix(), 'bytes': target.stat().st_size,
                     'sha256': hashlib.sha256(target.read_bytes()).hexdigest(),
                     'source_sha256': hashlib.sha256(source.read_bytes()).hexdigest()})


def attempts(source: Path):
    """results.jsonl 里每次实际尝试一条；作废标记行与原尝试同 (group, route, attempt)，不重复导出。"""
    seen = {}
    for line in (source / 'results.jsonl').read_text(encoding='utf-8').splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        key = (row['group_id'], row['route'], row['attempt'])
        if key not in seen and row.get('trial_dir'):
            seen[key] = row
    return list(seen.values())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', required=True, type=Path)
    parser.add_argument('--dest', required=True, type=Path)
    parser.add_argument('--attempts', action='store_true')
    parser.add_argument('--only', default='')
    args = parser.parse_args()

    source = args.source.resolve()
    dest = (REPO / args.dest).resolve() if not args.dest.is_absolute() else args.dest.resolve()
    if RESULTS.resolve() not in dest.parents:
        raise SystemExit('--dest 必须位于仓库 results/ 之下')
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)

    names = [name for name in args.only.split(',') if name] or sorted(
        path.name for path in source.iterdir() if path.is_file() and path.suffix in TEXT_SUFFIXES)
    counts, manifest, missing, applied = {}, [], [], {}
    for name in names:
        copy_one(source / name, dest / name, counts, manifest, applied)

    if args.attempts:
        for row in attempts(source):
            trial = Path(row['trial_dir'])
            for name, relative in ATTEMPT_FILES.items():
                path = trial / relative
                if path.is_file():
                    copy_one(path, dest / 'attempts' / row['job_name'] / name, counts, manifest, applied)
                else:
                    missing.append({'job_name': row['job_name'], 'file': name})

    residual = []
    for entry in manifest:
        text = (REPO / entry['path']).read_text(encoding='utf-8')
        for name, pattern in RESIDUAL:
            if pattern.search(text):
                residual.append({'path': entry['path'], 'pattern': name})
    combined = hashlib.sha256(''.join(f"{item['path']}\0{item['sha256']}\n" for item in manifest).encode()).hexdigest()
    report = {
        'schema': 'sanitization/v2',
        'source_batch': source.name,
        'files': len(manifest),
        'attempt_files_missing': missing,
        'transforms': applied,
        'replacement_counts': counts,
        'numeric_structure_preserved': True,
        'residual_hits': residual,
        'combined_sha256': combined,
        'manifest': manifest,
    }
    (dest / 'SANITIZATION.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    if residual:
        raise SystemExit(f'残留 {len(residual)} 处，见 {dest / "SANITIZATION.json"}')
    print(f'{source.name}: files={len(manifest)} missing={len(missing)} residual=0')


if __name__ == '__main__':
    main()
