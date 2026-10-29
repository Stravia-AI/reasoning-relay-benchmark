#!/usr/bin/env python3
"""三路隔离实例与计量代理：start / status / stop / smoke。

三条路线各一份全新空白的隔离实例（Stravia / CLIProxyAPI / sub2api），客户端入口固定
23481/23482/23483，出站计量代理固定 24481/24482/24483；每路出站（网关 → chatgpt.com）
都经自己的计量端口（meterd 的 MITM）。

设计取舍：
- 只做必要访问配置（监听地址、客户端 API key、出站代理、OAuth 凭据存放位置），
  不改任何一路的业务参数。
- 凭据与运行态数据只落私有目录（0700，文件 0600），脚本输出不打印 key 与密码。
- `--zero-inference` 把 meterd 的上游指向本地沉洞：CONNECT 一律 503 并记下外连意图，
  用来在没有凭据时验证三路都起得来、端口通、计量有记录。
- `smoke` 只做零推理连通性验证：给每路临时放一份一次性假凭据，发出一个 Chat 请求，
  让上游连接落到沉洞；用完即删，不会遮住用户后来登录的真实凭据。
"""

from __future__ import annotations

import argparse
import contextlib
import http.cookiejar
import json
import os
import re
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[5]
SCRATCH = ROOT / '.scratch/research/chat-reasoning-replay'
PRIVATE = Path.home() / '.local/state/chat-reasoning-replay/private'
RUN = PRIVATE / 'run'
STATE = RUN / 'state.json'

# 固定契约：客户端入口端口与计量端口一一对应。
PORTS = {
    'stravia': {'client': 23481, 'meter': 24481},
    'cpa': {'client': 23482, 'meter': 24482},
    'sub2api': {'client': 23483, 'meter': 24483},
}
LABELS = tuple(PORTS)

BINARIES = {
    'stravia': SCRATCH / 'tools/stravia-e67b6e0f/target/release/stravia-server',
    'cpa': SCRATCH / 'tools/cpa-8.0.43/cli-proxy-api',
    'sub2api': SCRATCH / 'tools/sub2api-0.2.18/sub2api',
    'meterd': SCRATCH / 'tools/meterd/meterd',
}
WASM = SCRATCH / 'tools/stravia-e67b6e0f/target/vendor-plugins-all'

# Stravia 空白实例必须走官方 setup 流程（`recover-admin` 只能改已有管理员）。
# 字段名取自 backend/apps/stravia-server/src/setup.rs 的 CompleteInput。
SETUP_PASSWORD_FIELD = 'password'
SETUP_BASE_URL_FIELD = 'client_base_url'
SETTINGS_VALUE_FIELD = 'value'
# ProviderCredentialInput::ApiKey 的字段名与设置值是同一个词（都是 `value`）。
CREDENTIAL_KEY_FIELD = 'value'


def emit(event, **fields):
    print(json.dumps({'event': event, **fields}, ensure_ascii=False), flush=True)


def _write_private(path, text):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as stream:
        stream.write(text)


def _mkdir_private(path):
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    return path


def port_open(port, host='127.0.0.1', timeout=0.4):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def wait_for(check, process, timeout, what):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process is not None and process.poll() is not None:
            raise RuntimeError(f'{what}: process exited with {process.returncode}')
        try:
            if check():
                return
        except Exception:
            pass
        time.sleep(0.2)
    raise RuntimeError(f'{what}: not ready after {timeout}s')


def http_json(base, path, body=None, method='POST', opener=None, headers=None, timeout=60):
    """管理 API 调用；错误只报状态码，不回显响应体（可能含凭据）。"""
    data = None
    if body is not None:
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
    request = urllib.request.Request(base + path, data=data, method=method)
    request.add_header('Content-Type', 'application/json')
    request.add_header('Origin', base)
    request.add_header('x-stravia-csrf', '1')
    for name, value in (headers or {}).items():
        request.add_header(name, value)
    opener = opener or urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=timeout) as response:
            raw = response.read()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as error:
        detail = ''
        try:
            payload = error.read().decode('utf-8', 'replace')
            detail = payload[:400]
        except Exception:
            pass
        raise RuntimeError(f'{method} {path}: HTTP {error.code} {detail}'.strip()) from None


class Processes:
    """子进程登记：start_new_session 让 stop 能整组回收（含 postgres/redis 的子进程）。"""

    def __init__(self):
        self.items = {}

    def start(self, argv, env, log_path, name):
        log = open(log_path, 'ab', buffering=0)
        process = subprocess.Popen(argv, env=env, stdout=log, stderr=subprocess.STDOUT,
                                   stdin=subprocess.DEVNULL, start_new_session=True, cwd=str(log_path.parent))
        self.items[name] = {'pid': process.pid, 'log': str(log_path), 'argv': [str(a) for a in argv]}
        return process

    @staticmethod
    def alive(pid):
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        return True

    def stop(self, name, grace=10):
        entry = self.items.get(name)
        if not entry:
            return
        pid = entry['pid']
        if not self.alive(pid):
            return
        with contextlib.suppress(OSError):
            os.killpg(pid, signal.SIGTERM)
        deadline = time.monotonic() + grace
        while time.monotonic() < deadline:
            if not self.alive(pid):
                return
            time.sleep(0.2)
        with contextlib.suppress(OSError):
            os.killpg(pid, signal.SIGKILL)


def gateway_env(extra=None, ca_bundle=None):
    """网关进程不继承宿主代理变量；出站代理只由被测工具自己的配置决定。"""
    env = {k: v for k, v in os.environ.items() if 'proxy' not in k.lower()}
    if ca_bundle:
        env['SSL_CERT_FILE'] = str(ca_bundle)
        env['SSL_CERT_DIR'] = str(Path(ca_bundle).parent)
    env.update(extra or {})
    return env


def ca_bundle_for(run_dir, meter_ca):
    """把 meterd 的 CA 与系统根证书拼成一个 bundle；Go 工具认 SSL_CERT_FILE。"""
    target = run_dir / 'ca-bundle.pem'
    system = Path('/etc/ssl/certs/ca-bundle.crt')
    parts = []
    if system.exists():
        parts.append(system.read_text())
    parts.append(Path(meter_ca).read_text())
    _write_private(target, '\n'.join(parts))
    return target


def token(nbytes=24):
    return secrets.token_urlsafe(nbytes)


def hex_secret(nbytes=32):
    return secrets.token_hex(nbytes)


# ── 沉洞 ──────────────────────────────────────────────────────────────
def run_sinkhole(port_file, out_file):
    """零推理上游：只记 CONNECT/请求的目标主机，一律回 503，不建立任何外部连接。"""
    server = socket.create_server(('127.0.0.1', 0))
    _write_private(Path(port_file), str(server.getsockname()[1]))
    stop = threading.Event()
    out = open(out_file, 'ab', buffering=0)
    lock = threading.Lock()

    def refuse(conn):
        with conn:
            try:
                conn.settimeout(5)
                head = b''
                while b'\r\n\r\n' not in head and len(head) < 16384:
                    chunk = conn.recv(4096)
                    if not chunk:
                        break
                    head += chunk
                line = head.split(b'\r\n', 1)[0].decode('latin-1').split(' ')
                method = line[0] if line else ''
                target = line[1] if len(line) > 1 else ''
                host = target.rsplit(':', 1)[0] if method == 'CONNECT' else (urllib.parse.urlsplit(target).hostname or '')
                row = json.dumps({'at': time.time(), 'method': method, 'host': host, 'target': target[:300]})
                with lock:
                    out.write((row + '\n').encode())
                conn.sendall(b'HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n')
            except OSError:
                pass

    def accept():
        while not stop.is_set():
            try:
                conn, _ = server.accept()
            except OSError:
                return
            threading.Thread(target=refuse, args=(conn,), daemon=True).start()

    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    threading.Thread(target=accept, daemon=True).start()
    while not stop.is_set():
        time.sleep(0.2)
    server.close()
    out.close()


# ── meterd ────────────────────────────────────────────────────────────
def meterd_help(binary):
    try:
        result = subprocess.run([str(binary), '-h'], capture_output=True, text=True, timeout=20)
    except (OSError, subprocess.SubprocessError):
        return ''
    return (result.stdout or '') + (result.stderr or '')


def start_meterd(manager, run_dir, bind, connect_host, upstream, binary):
    """meterd 的接口是 `-out/-confdir/-routes/-upstream`；`-listen` 是分工 M 新加的（旧二进制没有）。"""
    help_text = meterd_help(binary)
    if not help_text:
        raise RuntimeError(f'meterd 无法执行: {binary}')
    confdir = _mkdir_private(run_dir / 'meter-ca')
    argv = [str(binary), '-out', str(run_dir / 'meter.jsonl'), '-confdir', str(confdir),
            '-routes', json.dumps({str(spec['meter']): label for label, spec in PORTS.items()}),
            '-upstream', upstream or '']
    if '-listen' in help_text:
        argv += ['-listen', bind]
    if '-block-token-refresh' in help_text:
        # 本批不剥 refresh token，各工具默认自动刷新，所以必须放行刷新请求。
        argv += ['-block-token-refresh=false']
    if '-block-account-writes' in help_text:
        argv += ['-block-account-writes=true']
    process = manager.start(argv, gateway_env(), run_dir / 'meterd.log', 'meterd')
    ca = confdir / 'meterd-ca-cert.pem'
    wait_for(lambda: ca.exists() and all(port_open(spec['meter'], connect_host) for spec in PORTS.values()),
             process, 60, 'meterd')
    return {'ca': str(ca), 'argv': argv}


# ── Stravia ───────────────────────────────────────────────────────────
def stravia_bootstrap(base, log_path, username, password, run_dir):
    """空白数据目录：官方 setup token → claim → complete（建一次性管理员）。"""
    token_re = re.compile(r'Stravia setup token: (\S+)')
    setup_token = None
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline and setup_token is None:
        text = log_path.read_text(errors='replace') if log_path.exists() else ''
        found = token_re.search(text)
        setup_token = found.group(1) if found else None
        if setup_token is None:
            time.sleep(0.2)
    if setup_token is None:
        raise RuntimeError('stravia: setup token 未出现在日志里')

    jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPCookieProcessor(jar))
    http_json(base, '/api/v1/setup/claim', {'token': setup_token}, opener=opener)
    http_json(base, '/api/v1/setup/complete', {
        'database': {'backend': 'sqlite'},
        'username': username,
        SETUP_PASSWORD_FIELD: password,
        SETUP_BASE_URL_FIELD: base,
    }, opener=opener)
    http_json(base, '/api/v1/auth/login', {'username': username, SETUP_PASSWORD_FIELD: password}, opener=opener)
    return opener


def stravia_install_components(base, opener, components):
    installed = []
    for component in components:
        payload = Path(component).read_bytes()
        preview = http_json(base, '/api/v1/vendor-plugins/import', payload, opener=opener,
                            headers={'Content-Type': 'application/wasm'})['data']
        if preview['discarded_data']:
            raise RuntimeError(f"stravia: 组件 {Path(component).name} 会丢弃 provider 数据，拒绝导入")
        http_json(base, '/api/v1/vendor-plugins/confirm',
                  {'preview_id': preview['id'], 'allow_data_discard': False}, opener=opener)
        installed.append({'vendor_id': preview['vendor_id'], 'file': Path(component).name,
                          'new_version': preview['new_version']})
    summaries = {row['vendor_id']: row for row in http_json(base, '/api/v1/vendor-plugins', method='GET', opener=opener)['data']}
    for vendor in [row['vendor_id'] for row in installed] + ['base']:
        summary = summaries.get(vendor) or {}
        if summary.get('status') != 'ready' or summary.get('error'):
            raise RuntimeError(f'stravia: 组件 {vendor} 未就绪: {summary.get("status")}')
    return installed


def start_stravia(manager, run_dir, bind, connect_host, meter_port, ca_bundle, wasm_dir, fresh=True):
    data = run_dir / 'stravia/data'
    _mkdir_private(data)
    log_path = run_dir / 'stravia.log'
    base = f'http://{connect_host}:{PORTS["stravia"]["client"]}'
    origins = [base, f'http://127.0.0.1:{PORTS["stravia"]["client"]}', f'http://localhost:{PORTS["stravia"]["client"]}']
    env = gateway_env({
        'STRAVIA_DATA_DIR': str(data),
        'STRAVIA_HOST': bind,
        'STRAVIA_PORT': str(PORTS['stravia']['client']),
        'STRAVIA_ADMIN_ORIGINS': ','.join(dict.fromkeys(origins)),
        'STRAVIA_LOG_LEVEL': 'info',
    }, ca_bundle)
    process = manager.start([str(BINARIES['stravia'])], env, log_path, 'stravia')
    wait_for(lambda: port_open(PORTS['stravia']['client'], connect_host), process, 60, 'stravia')

    if not fresh:
        # 迁移和组件加载期间也会监听；管理请求必须等网关就绪，不能只看 TCP。
        wait_for(lambda: http_json(base, '/readyz', method='GET', timeout=5) is not None,
                 process, 120, 'stravia readyz')
        return {'base_url': base + '/v1', 'admin_url': base, 'components': 'reused'}

    username = 'bench-' + token(6).replace('-', '').replace('_', '')[:8]
    password = token(24)
    opener = stravia_bootstrap(base, log_path, username, password, run_dir)
    wait_for(lambda: http_json(base, '/readyz', method='GET', opener=opener) is not None, process, 60, 'stravia readyz')

    # 出站只改地址：Stravia 的 outbound_proxy 是全局出站代理，指向本路计量端口。
    http_json(base, '/api/v1/settings/outbound_proxy',
              {SETTINGS_VALUE_FIELD: json.dumps({'url': f'http://{connect_host}:{meter_port}',
                                                 'bypass': '', 'force_http1': False})},
              method='PUT', opener=opener)
    installed = stravia_install_components(base, opener, sorted(wasm_dir.glob('openai-codex-*.wasm')))
    key = http_json(base, '/api/v1/api-keys', {'name': 'chat-reasoning-replay'}, opener=opener)['data']['key']
    _write_private(run_dir / 'stravia/client.json',
                   json.dumps({'base_url': base + '/v1', 'api_key': key, 'admin_username': username}, indent=2))
    _write_private(run_dir / 'stravia/admin.json', json.dumps({'username': username, SETUP_PASSWORD_FIELD: password}, indent=2))
    return {'base_url': base + '/v1', 'admin_url': base, 'components': installed}


# ── CPA ───────────────────────────────────────────────────────────────
def start_cpa(manager, run_dir, bind, connect_host, meter_port, ca_bundle):
    root = _mkdir_private(run_dir / 'cpa')
    auths = _mkdir_private(root / 'auths')
    existing = run_dir / 'cpa/client.json'
    # 重启沿用原 key，避免客户端配置在重启后失效。
    key = json.loads(existing.read_text())['api_key'] if existing.exists() else 'sk-cpa-' + token(24)
    config = '\n'.join([
        'config-version: 8',
        'server:',
        f'  host: "{bind}"',
        f'  port: {PORTS["cpa"]["client"]}',
        'access:',
        '  api-keys:',
        f'    - {json.dumps(key)}',
        'requests:',
        f'  proxy-url: {json.dumps(f"http://{connect_host}:{meter_port}")}',
        'oauth:',
        f'  auth-dir: {json.dumps(str(auths))}',
        '',
    ])
    _write_private(root / 'config.yaml', config)
    process = manager.start([str(BINARIES['cpa']), '-config', str(root / 'config.yaml')],
                            gateway_env(ca_bundle=ca_bundle), root / 'cpa.log', 'cpa')
    wait_for(lambda: port_open(PORTS['cpa']['client'], connect_host), process, 90, 'cpa')
    _write_private(run_dir / 'cpa/client.json',
                   json.dumps({'base_url': f'http://{connect_host}:{PORTS["cpa"]["client"]}/v1', 'api_key': key}, indent=2))
    return {'base_url': f'http://{connect_host}:{PORTS["cpa"]["client"]}/v1', 'auth_dir': str(auths),
            'config': str(root / 'config.yaml')}


# ── sub2api ───────────────────────────────────────────────────────────
def _free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def start_sub2api(manager, run_dir, bind, connect_host, meter_port, ca_bundle):
    """sub2api 没有 SQLite 模式：必须 PostgreSQL 15+ 与 Redis 7+，这里各起一个私有实例。"""
    root = _mkdir_private(run_dir / 'sub2api')
    data = _mkdir_private(root / 'data')
    pg = root / 'postgres'
    env = gateway_env({'PGHOST': str(root), 'PGUSER': 'sub2api'}, ca_bundle)
    pgport, redisport = _free_port(), _free_port()
    if not pg.exists():
        subprocess.run(['initdb', '-D', str(pg), '-U', 'sub2api', '-A', 'trust', '--no-locale'],
                       env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, check=True)
    postgres = manager.start(['postgres', '-D', str(pg), '-k', str(root), '-p', str(pgport), '-h', '127.0.0.1',
                              '-c', 'shared_buffers=16MB', '-c', 'max_connections=100'], env, root / 'postgres.log', 'sub2api-postgres')
    # TCP 监听先于恢复完成；必须等数据库接受连接后才能查询或建库。
    wait_for(lambda: subprocess.run(['pg_isready', '-q', '-h', '127.0.0.1', '-p', str(pgport),
                                    '-U', 'sub2api', '-d', 'postgres'], env=env,
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0,
             postgres, 60, 'postgres SQL readiness')
    # 重启时库已存在，建库要幂等。
    existing_db = subprocess.run(['psql', '-p', str(pgport), '-d', 'postgres', '-h', '127.0.0.1', '-tAc',
                                  "SELECT 1 FROM pg_database WHERE datname='sub2api'"],
                                 env=env, capture_output=True, text=True, check=True).stdout.strip()
    if not existing_db:
        subprocess.run(['psql', '-p', str(pgport), '-d', 'postgres', '-h', '127.0.0.1', '-c', 'CREATE DATABASE sub2api'],
                       env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, check=True)
    redis = manager.start(['redis-server', '--bind', '127.0.0.1', '--port', str(redisport), '--dir', str(root),
                           '--save', '', '--appendonly', 'no'], env, root / 'redis.log', 'sub2api-redis')
    wait_for(lambda: port_open(redisport), redis, 60, 'redis')

    # 重启必须沿用同一套管理员凭据与 JWT/TOTP 密钥：AUTO_SETUP 只在首次安装时建管理员，
    # 换了凭据就再也登不进去；JWT 密钥变了旧会话全部失效。
    admin_file = run_dir / 'sub2api/admin.json'
    secrets_file = run_dir / 'sub2api/secrets.json'
    if admin_file.exists():
        saved = json.loads(admin_file.read_text())
        email, password = saved['email'], saved[SETUP_PASSWORD_FIELD]
    else:
        email = 'bench-' + token(6).replace('-', '').replace('_', '')[:8] + '@example.invalid'
        password = token(24)
    secrets = json.loads(secrets_file.read_text()) if secrets_file.exists() else {
        'jwt_secret': hex_secret(32), 'totp_key': hex_secret(32)}
    _write_private(secrets_file, json.dumps(secrets, indent=2))
    base = f'http://{connect_host}:{PORTS["sub2api"]["client"]}'
    env.update(AUTO_SETUP='true', DATA_DIR=str(data), SERVER_HOST=bind, SERVER_PORT=str(PORTS['sub2api']['client']),
               SERVER_MODE='release', DATABASE_HOST='127.0.0.1', DATABASE_PORT=str(pgport), DATABASE_USER='sub2api',
               DATABASE_PASSWORD='', DATABASE_DBNAME='sub2api', DATABASE_SSLMODE='disable',
               DATABASE_MAX_OPEN_CONNS='20', DATABASE_MAX_IDLE_CONNS='5', REDIS_HOST='127.0.0.1',
               REDIS_PORT=str(redisport), ADMIN_EMAIL=email, ADMIN_PASSWORD=password,
               JWT_SECRET=secrets['jwt_secret'], TOTP_ENCRYPTION_KEY=secrets['totp_key'])
    process = manager.start([str(BINARIES['sub2api'])], env, root / 'sub2api.log', 'sub2api')
    wait_for(lambda: port_open(PORTS['sub2api']['client'], connect_host), process, 120, 'sub2api')
    wait_for(lambda: http_json(base, '/health', method='GET') is not None, process, 120, 'sub2api health')
    with contextlib.suppress(OSError):
        (data / 'config.yaml').chmod(0o600)

    def call(path, body=None, method='POST', headers=None):
        payload = http_json(base, path, body, method=method, headers=headers)
        return payload.get('data') if isinstance(payload, dict) and 'data' in payload else payload

    admin = call('/api/v1/auth/login', {'email': email, 'password': password})['access_token']
    headers = {'Authorization': 'Bearer ' + admin}
    compliance = call('/api/v1/admin/compliance', method='GET', headers=headers)
    call('/api/v1/admin/compliance/accept', {'phrase': compliance['ack_phrase_en'], 'language': 'en'}, headers=headers)
    context_file = run_dir / 'sub2api/context.json'
    client_file = run_dir / 'sub2api/client.json'
    if context_file.exists() and client_file.exists():
        context = json.loads(context_file.read_text())
        proxy_id, group_id, key = context['proxy_id'], context['group_id'], json.loads(client_file.read_text())['api_key']
    else:
        proxy = urllib.parse.urlsplit(f'http://{connect_host}:{meter_port}')
        proxy_id = call('/api/v1/admin/proxies',
                        {'name': 'meter', 'protocol': proxy.scheme, 'host': proxy.hostname, 'port': proxy.port},
                        headers=headers)['id']
        group_id = call('/api/v1/admin/groups',
                        {'name': 'bench', 'platform': 'openai', 'rate_multiplier': 1,
                         'subscription_type': 'standard'}, headers=headers)['id']
        key = call('/api/v1/keys', {'name': 'chat-reasoning-replay', 'group_id': group_id}, headers=headers)['key']
    me = call('/api/v1/auth/me', method='GET', headers=headers)
    call(f'/api/v1/admin/users/{me["id"]}/balance',
         {'balance': 100, 'operation': 'add', 'notes': 'isolated bench credit'}, headers=headers)
    _write_private(run_dir / 'sub2api/client.json',
                   json.dumps({'base_url': base + '/v1', 'api_key': key, 'admin_email': email}, indent=2))
    _write_private(run_dir / 'sub2api/admin.json', json.dumps({'email': email, 'password': password}, indent=2))
    _write_private(run_dir / 'sub2api/context.json', json.dumps({'proxy_id': proxy_id, 'group_id': group_id}, indent=2))
    return {'base_url': base + '/v1', 'admin_url': base, 'proxy_id': proxy_id, 'group_id': group_id}


# ── 状态 ──────────────────────────────────────────────────────────────
def load_state():
    if not STATE.exists():
        return None
    return json.loads(STATE.read_text())


def save_state(state):
    _write_private(STATE, json.dumps(state, indent=2))


def stop_all(state, grace=15):
    for name in ('sub2api', 'sub2api-redis', 'sub2api-postgres', 'cpa', 'stravia', 'meterd', 'sinkhole'):
        entry = (state.get('processes') or {}).get(name)
        if not entry:
            continue
        pid = entry['pid']
        if Processes.alive(pid):
            with contextlib.suppress(OSError):
                os.killpg(pid, signal.SIGTERM)
    deadline = time.monotonic() + grace
    remaining = [entry['pid'] for entry in (state.get('processes') or {}).values() if Processes.alive(entry['pid'])]
    while remaining and time.monotonic() < deadline:
        time.sleep(0.2)
        remaining = [pid for pid in remaining if Processes.alive(pid)]
    for pid in remaining:
        with contextlib.suppress(OSError):
            os.killpg(pid, signal.SIGKILL)


def route_status(state):
    """每路：客户端端口、计量端口、进程、key 文件。就绪 = 端口在听 + 进程活着。"""
    connect_host = state['connect_host']
    rows = []
    for label in LABELS:
        client, meter = PORTS[label]['client'], PORTS[label]['meter']
        key_file = RUN / label / 'client.json'
        alive = Processes.alive((state['processes'].get(label) or {}).get('pid', 0))
        rows.append({
            'route': label,
            'client_port': client,
            'meter_port': meter,
            'client_listening': port_open(client, connect_host),
            'meter_listening': port_open(meter, connect_host),
            'process_alive': alive,
            'client_config': str(key_file) if key_file.exists() else None,
            'ready': bool(alive and port_open(client, connect_host) and port_open(meter, connect_host)),
        })
    meter_rows = {}
    meter_file = RUN / 'meter.jsonl'
    if meter_file.exists():
        for line in meter_file.read_text(errors='replace').splitlines():
            try:
                row = json.loads(line)
            except ValueError:
                continue
            meter_rows[row.get('route', '?')] = meter_rows.get(row.get('route', '?'), 0) + 1
    return {'bind': state['bind'], 'connect_host': connect_host, 'zero_inference': state['zero_inference'],
            'routes': rows, 'meter_rows': meter_rows, 'meterd': state['processes'].get('meterd', {}).get('pid')}


def cmd_status(args):
    state = load_state()
    if not state:
        emit('status', running=False)
        return 1
    if all(not Processes.alive((entry or {}).get('pid', 0)) for entry in (state.get('processes') or {}).values()):
        emit('status', running=False, data_dir=str(RUN))
        return 1
    report = route_status(state)
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(f"bind={report['bind']} connect={report['connect_host']} zero_inference={report['zero_inference']}")
        for row in report['routes']:
            print(f"  {row['route']:<8} client {row['client_port']} {'up' if row['client_listening'] else 'DOWN'}"
                  f"  meter {row['meter_port']} {'up' if row['meter_listening'] else 'DOWN'}"
                  f"  process {'alive' if row['process_alive'] else 'DEAD'}"
                  f"  {'READY' if row['ready'] else 'NOT READY'}")
            print(f"           config: {row['client_config']}")
        print(f"  meter rows: {json.dumps(report['meter_rows'], ensure_ascii=False)}")
    return 0 if all(row['ready'] for row in report['routes']) else 1


# ── 启动 ──────────────────────────────────────────────────────────────
def cmd_start(args):
    bind = args.bind or os.environ.get('BENCH_BIND') or '127.0.0.1'
    connect_host = '127.0.0.1' if bind in ('0.0.0.0', '::', '') else bind
    previous = load_state()
    if previous and any(Processes.alive((p or {}).get('pid', 0)) for p in (previous.get('processes') or {}).values()):
        emit('start-refused', reason='已有实例在运行，先 stop')
        return 1
    if previous and not args.fresh:
        # 重启用已有数据目录：保留用户登录过的 OAuth 凭据。
        for label in LABELS:
            if not (RUN / label).exists():
                emit('start-refused', reason=f'缺少 {label} 数据目录，用 --fresh 重建')
                return 1
        print('restart: 复用已有数据目录（保留登录凭据）')
    else:
        if RUN.exists():
            shutil.rmtree(RUN)
        _mkdir_private(RUN)
        previous = None

    for label, binary in (('stravia', BINARIES['stravia']), ('cpa', BINARIES['cpa']), ('sub2api', BINARIES['sub2api'])):
        if not Path(binary).exists():
            emit('start-failed', reason=f'{label} 二进制不存在: {binary}')
            return 1
    meterd = Path(args.meterd) if args.meterd else BINARIES['meterd']
    if not meterd.exists():
        emit('start-failed', reason=f'meterd 不存在: {meterd}')
        return 1

    state = {'bind': bind, 'connect_host': connect_host, 'zero_inference': bool(args.zero_inference), 'stopped': False,
             'meterd': str(meterd), 'processes': {}, 'routes': {}}
    manager = Processes()
    try:
        upstream = args.upstream_proxy or ''
        if args.zero_inference:
            port_file = RUN / 'sinkhole.port'
            sink = manager.start([sys.executable, str(Path(__file__).resolve()), '_sinkhole',
                                  str(port_file), str(RUN / 'sinkhole.jsonl')],
                                 gateway_env(), RUN / 'sinkhole.log', 'sinkhole')
            wait_for(lambda: port_file.exists() and port_file.read_text().strip(), sink, 30, 'sinkhole')
            upstream = f'http://127.0.0.1:{port_file.read_text().strip()}'
        state['processes'].update({k: v for k, v in manager.items.items()})

        meter = start_meterd(manager, RUN, bind, connect_host, upstream, meterd)
        state['processes'].update({k: v for k, v in manager.items.items()})
        state['meter_ca'] = meter['ca']
        ca_bundle = ca_bundle_for(RUN, meter['ca'])

        # 复用数据目录时不再重建管理员、组件与 key，只拉起进程；全新实例走完整官方初始化。
        state['routes']['stravia'] = start_stravia(manager, RUN, bind, connect_host, PORTS['stravia']['meter'],
                                                   ca_bundle, WASM, fresh=previous is None)
        state['processes'].update({k: v for k, v in manager.items.items()})
        state['routes']['cpa'] = start_cpa(manager, RUN, bind, connect_host, PORTS['cpa']['meter'], ca_bundle)
        state['processes'].update({k: v for k, v in manager.items.items()})
        state['routes']['sub2api'] = start_sub2api(manager, RUN, bind, connect_host, PORTS['sub2api']['meter'], ca_bundle)
        state['processes'].update({k: v for k, v in manager.items.items()})
        save_state(state)
    except Exception as error:
        state['processes'].update({k: v for k, v in manager.items.items()})
        stop_all(state)
        emit('start-failed', error=str(error))
        for log in sorted(RUN.glob('*.log')) + sorted(RUN.glob('*/*.log')):
            if log.exists():
                tail = log.read_text(errors='replace').splitlines()[-12:]
                print(f'--- {log.relative_to(RUN)} ---')
                print('\n'.join(tail))
        return 1

    report = route_status(state)
    emit('started', bind=bind, zero_inference=bool(args.zero_inference), meterd=str(meterd),
         routes=[{'route': row['route'], 'client': row['client_port'], 'meter': row['meter_port'], 'ready': row['ready']}
                 for row in report['routes']])
    print(f'客户端配置（含 key，0600）：{RUN}/<route>/client.json')
    return 0 if all(row['ready'] for row in report['routes']) else 1


def cmd_wire(args):
    """登录后接线：确认 Stravia 的出站代理指向本路计量端口，并给 Codex provider 打开「使用代理」。

    Stravia 只对 use_proxy=true 的 provider 走全局出站代理（gateway/runtime.rs effective_vendor_proxy），
    不打开的话这一路的上游流量根本不经过计量代理。
    """
    state = load_state()
    if not state:
        emit('wire-failed', reason='未启动')
        return 1
    connect_host = state['connect_host']
    base = f'http://{connect_host}:{PORTS["stravia"]["client"]}'
    admin = json.loads((RUN / 'stravia/admin.json').read_text())
    jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPCookieProcessor(jar))
    http_json(base, '/api/v1/auth/login',
              {'username': admin['username'], SETUP_PASSWORD_FIELD: admin[SETUP_PASSWORD_FIELD]}, opener=opener)
    http_json(base, '/api/v1/settings/outbound_proxy',
              {SETTINGS_VALUE_FIELD: json.dumps({'url': f'http://{connect_host}:{PORTS["stravia"]["meter"]}',
                                                 'bypass': '', 'force_http1': False})}, method='PUT', opener=opener)
    changed = []
    for provider in http_json(base, '/api/v1/providers', method='GET', opener=opener)['data']:
        if provider.get('vendor') != 'openai-codex':
            continue
        if provider.get('use_proxy') is True:
            continue
        http_json(base, f'/api/v1/providers/{provider["id"]}', {'use_proxy': True}, method='PUT', opener=opener)
        changed.append(provider.get('name') or provider['id'])
    emit('wired', outbound_proxy=f'http://{connect_host}:{PORTS["stravia"]["meter"]}', providers_enabled=changed)
    return 0


def cmd_stop(args):
    state = load_state()
    if not state:
        emit('stopped', running=False)
        return 0
    stop_all(state)
    # 保留 state.json：不带 --fresh 的 start 靠它判断“数据目录里已有登录过的实例”。
    state['stopped'] = True
    save_state(state)
    emit('stopped', running=False, data_dir=str(RUN))
    return 0


# ── 零推理冒烟 ────────────────────────────────────────────────────────
def _chat(base, key, model='gpt-6-luna'):
    body = {'model': model, 'messages': [{'role': 'user', 'content': 'ping'}],
            'reasoning_effort': 'high', 'stream': False}
    request = urllib.request.Request(base + '/chat/completions', data=json.dumps(body).encode(), method='POST')
    request.add_header('Content-Type', 'application/json')
    request.add_header('Authorization', 'Bearer ' + key)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=90) as response:
            return {'status': response.status, 'body': response.read(200).decode('utf-8', 'replace')}
    except urllib.error.HTTPError as error:
        return {'status': error.code, 'body': error.read(300).decode('utf-8', 'replace')}
    except Exception as error:
        return {'status': None, 'body': str(error)[:300]}


def _stravia_temp_route(base, opener):
    """为零推理冒烟临时建 provider/model/route；已有 gpt-6-luna 路由时返回 None（直接打真实路由）。"""
    routes = http_json(base, '/api/v1/models', method='GET', opener=opener).get('data') or []
    if any(route.get('model_id') == 'gpt-6-luna' for route in routes):
        return None
    provider = http_json(base, '/api/v1/providers', {
        'name': 'smoke-dummy', 'use_proxy': True,
        'source': {'type': 'custom', 'vendor': '302ai', 'channel': 'default', 'protocol': 'openai-compatible',
                   'base_url': 'https://chatgpt.com/v1', 'models_source': 'static',
                   'static_models': 'gpt-6-luna'},
        'credential': {'type': 'api_key', CREDENTIAL_KEY_FIELD: 'sk-smoke-dummy'},
    }, opener=opener)['data']
    provider_id = provider['id']
    model = http_json(base, f'/api/v1/providers/{provider_id}/models',
                      {'model_id': 'gpt-6-luna',
                       'metadata': {'modalities': {'input': ['text'], 'output': ['text']}}},
                      opener=opener)['data']
    route = http_json(base, '/api/v1/models', {
        'output_modality': 'text', 'model_id': 'gpt-6-luna',
        'targets': [{'provider_id': provider_id, 'model': 'gpt-6-luna', 'enabled': True}],
    }, opener=opener)['data']
    return {'route_id': route['id'], 'provider_id': provider_id,
            'model_revision': model.get('revision', 1)}


def _stravia_drop_temp_route(base, opener, temp):
    if not temp:
        return
    # 顺序：路由 → provider model → provider；任一步失败都不该遮住真实配置，所以吞掉错误。
    with contextlib.suppress(RuntimeError):
        http_json(base, f"/api/v1/models/{temp['route_id']}", method='DELETE', opener=opener)
    with contextlib.suppress(RuntimeError):
        http_json(base, f"/api/v1/providers/{temp['provider_id']}/model?model=gpt-6-luna",
                  method='DELETE', opener=opener)
    with contextlib.suppress(RuntimeError):
        http_json(base, f"/api/v1/providers/{temp['provider_id']}", method='DELETE', opener=opener)


def cmd_smoke(args):
    """每路放一份一次性假凭据，发一个 Chat 请求，确认上游连接意图落到沉洞。"""
    state = load_state()
    if not state:
        emit('smoke-failed', reason='未启动')
        return 1
    if not state['zero_inference']:
        emit('smoke-refused', reason='smoke 只在 --zero-inference 下运行，避免真实推理')
        return 1
    connect_host = state['connect_host']
    meter_file = RUN / 'meter.jsonl'
    offset = meter_file.stat().st_size if meter_file.exists() else 0
    results = {}

    # CPA：临时 auth 文件（假 token，过期时间在未来，避免先触发刷新）。
    cpa_dir = RUN / 'cpa/auths'
    dummy = cpa_dir / 'zz-smoke-dummy.json'
    _write_private(dummy, json.dumps({'type': 'codex', 'id_token': 'smoke', 'access_token': 'smoke',
                                      'refresh_token': '', 'account_id': 'smoke', 'email': 'smoke@example.invalid',
                                      'expired': '2099-01-01T00:00:00Z', 'last_refresh': '',
                                      'plan_type': 'pro'}))
    try:
        client = json.loads((RUN / 'cpa/client.json').read_text())
        results['cpa'] = _chat(client['base_url'], client['api_key'])
    finally:
        with contextlib.suppress(OSError):
            dummy.unlink()

    # sub2api：临时 OpenAI OAuth 账号，绑到计量代理与分组。
    client = json.loads((RUN / 'sub2api/client.json').read_text())
    context = json.loads((RUN / 'sub2api/context.json').read_text())
    admin = json.loads((RUN / 'sub2api/admin.json').read_text())
    base = client['base_url'].removesuffix('/v1')
    def s2(path, body=None, method='POST', headers=None):
        payload = http_json(base, path, body, method=method, headers=headers)
        return payload.get('data') if isinstance(payload, dict) and 'data' in payload else payload

    headers = {'Authorization': 'Bearer ' + s2('/api/v1/auth/login',
                                               {'email': admin['email'], 'password': admin['password']})['access_token']}
    account = s2('/api/v1/admin/accounts', {
        'name': 'smoke-dummy', 'platform': 'openai', 'type': 'oauth',
        'credentials': {'access_token': 'smoke', 'chatgpt_account_id': 'smoke', 'email': 'smoke@example.invalid',
                        'expires_at': '2099-01-01T00:00:00Z', 'plan_type': 'pro'},
        'proxy_id': context['proxy_id'], 'group_ids': [context['group_id']], 'concurrency': 1, 'priority': 1,
        'upstream_billing_probe_enabled': False}, headers=headers)
    try:
        results['sub2api'] = _chat(client['base_url'], client['api_key'])
    finally:
        with contextlib.suppress(OSError):
            s2(f"/api/v1/admin/accounts/{account['id']}", method='DELETE', headers=headers)

    # Stravia：空白实例不允许用合成凭据建 OAuth provider（AUTH_SESSION_REQUIRED），
    # 所以临时建一个 base 目录里的 openai-compatible provider（假 key，base_url 指向 chatgpt.com，
    # use_proxy=true）+ provider model + 路由 gpt-6-luna，让 Chat 请求真的出站到本路计量端口 → 沉洞。
    # 用完按路由 → 模型 → provider 顺序删掉，不遮住用户之后的真实登录。
    stravia_base = f'http://{connect_host}:{PORTS["stravia"]["client"]}'
    admin_cfg = json.loads((RUN / 'stravia/admin.json').read_text())
    jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPCookieProcessor(jar))
    http_json(stravia_base, '/api/v1/auth/login',
              {'username': admin_cfg['username'], SETUP_PASSWORD_FIELD: admin_cfg[SETUP_PASSWORD_FIELD]}, opener=opener)
    stravia_client = json.loads((RUN / 'stravia/client.json').read_text())
    temp = _stravia_temp_route(stravia_base, opener)
    try:
        results['stravia'] = _chat(stravia_client['base_url'], stravia_client['api_key'])
    finally:
        _stravia_drop_temp_route(stravia_base, opener, temp)
    time.sleep(1.5)  # meterd 同步写文件，但给沉洞的记账线程一点余量
    with meter_file.open('rb') as stream:
        stream.seek(offset)
        rows = [json.loads(line) for line in stream.read().decode('utf-8', 'replace').splitlines() if line.strip()]
    meter = {label: {'rows': 0, 'hosts': []} for label in LABELS}
    for row in rows:
        label = row.get('route')
        if label in meter:
            meter[label]['rows'] += 1
            host = row.get('host')
            if host and host not in meter[label]['hosts']:
                meter[label]['hosts'].append(host)
    sink_file = RUN / 'sinkhole.jsonl'
    sink_rows = []
    if sink_file.exists():
        sink_rows = [json.loads(line) for line in sink_file.read_text(errors='replace').splitlines() if line.strip()]
    report = {'results': results, 'meter_since_start': meter,
              'sinkhole': {'total': len(sink_rows), 'hosts': sorted({row['host'] for row in sink_rows if row.get('host')})}}
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0 if all(meter[label]['rows'] for label in LABELS) and all(results[label].get('status') for label in LABELS) else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest='command', required=True)

    start = sub.add_parser('start', help='启动三路隔离实例与计量代理')
    start.add_argument('--bind', default=None, help='监听地址，默认取 BENCH_BIND，否则 127.0.0.1')
    start.add_argument('--fresh', action='store_true', help='清空运行目录，重建全新空白实例')
    start.add_argument('--zero-inference', action='store_true', help='上游指向本地沉洞（CONNECT 一律 503）')
    start.add_argument('--upstream-proxy', default=None, help='meterd 的上游 HTTP 代理；留空直连')
    start.add_argument('--meterd', default=None, help='meterd 二进制路径')
    start.set_defaults(func=cmd_start)

    status = sub.add_parser('status', help='查看三路与计量端口状态')
    status.add_argument('--json', action='store_true')
    status.set_defaults(func=cmd_status)

    stop = sub.add_parser('stop', help='停止全部进程')
    stop.set_defaults(func=cmd_stop)

    wire = sub.add_parser('wire', help='Stravia 登录后接线：出站代理指向计量端口 + provider 打开使用代理')
    wire.set_defaults(func=cmd_wire)

    smoke = sub.add_parser('smoke', help='零推理冒烟：每路一个 Chat 请求，确认沉洞记到外连意图')
    smoke.set_defaults(func=cmd_smoke)

    sink = sub.add_parser('_sinkhole')
    sink.add_argument('port_file')
    sink.add_argument('out_file')
    sink.set_defaults(func=lambda args: run_sinkhole(args.port_file, args.out_file))

    args = parser.parse_args()
    return args.func(args)


if __name__ == '__main__':
    raise SystemExit(main())
