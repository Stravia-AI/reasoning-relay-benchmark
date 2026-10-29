// meterd 是评测专用的流式 TLS 拦截计量代理：每个监听端口对应一条被测路线，
// 记录每次上游模型请求的应用层负载大小与结构计数，不落盘正文与凭据。
//
// 与 mitmproxy 版计量的区别是转发路径不缓冲：请求体、SSE 响应与 WebSocket 帧
// 都边读边写，计量在已转发的副本上进行；HTTP/2 与 HTTP/1.1 按客户端协商结果
// 选择相应的上游协议偏好（上游未提供 h2 时仍可回退到 h1），并放大流控窗口。
//
// 记录口径（详见 docs/meterd.md）：HTTP 请求体与 WebSocket 文本消息经
// gzip/deflate/zstd 解码后，只提取 input 各 item 类型计数、reasoning item 的
// encrypted_content 长度、previous_response_id/store/reasoning/include/tools/
// prompt_cache_key 等结构字段；响应侧只提取终态事件的 status、output 里
// reasoning item 数与 usage 四类 token。解析失败写 null（未知），不写 0。
//
// 安全约束：默认拦截 chatgpt.com /backend-api/settings/ 的写请求（防止改动用户
// 账号设置）；OAuth token 端点的拦截默认关闭（本批要求被测工具自行刷新），
// 需要时用 -block-token-refresh 打开。任何请求头都不会被记录。
//
// 输入：命令行 -out/-confdir/-routes/-listen/-upstream 与两个拦截开关；环境变量
// BENCH_BIND 只作为 -listen 的默认值。数据来自经本代理的 HTTP/WS 流量。
// 副作用：监听 -routes 里的端口；生成/读取 CA 证书与私钥；追加写 JSONL（0600）。
// 不发起任何模型请求，不读取项目或用户凭据，不修改被测工具。
package main

import (
	"bufio"
	"bytes"
	"compress/flate"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ---------- 事件输出 ----------

type event map[string]any

// job 的 fill 在写出协程里执行：JSON 解析等计量开销不占转发路径，且事件顺序与入队顺序一致。
type job struct {
	ev   event
	fill func(event)
}

type emitter struct {
	ch     chan job
	done   chan struct{}
	failed chan struct{}
	err    error
}

func newEmitter(path string) (*emitter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	e := &emitter{ch: make(chan job, 4), done: make(chan struct{}), failed: make(chan struct{})}
	go func() {
		defer close(e.done)
		defer func() {
			if err := f.Close(); err != nil && e.err == nil {
				e.err = err
				close(e.failed)
			}
		}()
		for j := range e.ch {
			ev := j.ev
			if j.fill != nil {
				j.fill(ev)
			}
			line, err := json.Marshal(ev)
			if err == nil {
				line = append(line, '\n')
				var n int
				n, err = f.Write(line)
				if err == nil && n != len(line) {
					err = io.ErrShortWrite
				}
			}
			if err != nil {
				e.err = err
				close(e.failed)
				return
			}
		}
		if err := f.Sync(); err != nil {
			e.err = err
			close(e.failed)
		}
	}()
	return e, nil
}

func now() float64 { return float64(time.Now().UnixNano()) / 1e9 }

// emit 阻塞直到入队：计量不得丢事件。
func (e *emitter) emit(ev event) { e.later(ev, nil) }

// later 立即打时间戳并入队，fill 稍后在写出协程里补全字段。
func (e *emitter) later(ev event, fill func(event)) {
	ev["at"] = now()
	select {
	case e.ch <- job{ev: ev, fill: fill}:
	case <-e.failed:
	}
}

// drain 只能在所有请求处理器退出后调用。
func (e *emitter) drain() error {
	close(e.ch)
	<-e.done
	return e.err
}

// ---------- 证书 ----------

type authority struct {
	cert  *x509.Certificate
	key   *ecdsa.PrivateKey
	leafs sync.Map // host -> *tls.Certificate
}

func loadOrCreateCA(dir string) (*authority, error) {
	certPath, keyPath := filepath.Join(dir, "meterd-ca-cert.pem"), filepath.Join(dir, "meterd-ca-key.pem")
	if certPEM, err := os.ReadFile(certPath); err == nil {
		keyPEM, err := os.ReadFile(keyPath)
		if err != nil {
			return nil, err
		}
		pair, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			return nil, err
		}
		cert, err := x509.ParseCertificate(pair.Certificate[0])
		if err != nil {
			return nil, err
		}
		return &authority{cert: cert, key: pair.PrivateKey.(*ecdsa.PrivateKey)}, nil
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	template := &x509.Certificate{
		SerialNumber:          randomSerial(),
		Subject:               pkix.Name{CommonName: "meterd benchmark CA", Organization: []string{"meterd"}},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().AddDate(1, 0, 0),
		IsCA:                  true,
		BasicConstraintsValid: true,
		MaxPathLenZero:        true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		return nil, err
	}
	// 证书最后写：编排脚本以证书文件出现作为 CA 就绪信号。
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return &authority{cert: cert, key: key}, nil
}

func randomSerial() *big.Int {
	n, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	return n
}

func (a *authority) leaf(host string) (*tls.Certificate, error) {
	if c, ok := a.leafs.Load(host); ok {
		return c.(*tls.Certificate), nil
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	template := &x509.Certificate{
		SerialNumber: randomSerial(),
		Subject:      pkix.Name{CommonName: host},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().AddDate(0, 3, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	if ip := net.ParseIP(host); ip != nil {
		template.IPAddresses = []net.IP{ip}
	} else {
		template.DNSNames = []string{host}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, a.cert, &key.PublicKey, a.key)
	if err != nil {
		return nil, err
	}
	c := &tls.Certificate{Certificate: [][]byte{der, a.cert.Raw}, PrivateKey: key}
	actual, _ := a.leafs.LoadOrStore(host, c)
	return actual.(*tls.Certificate), nil
}

// ---------- 负载结构 ----------

// 请求/响应的结构提取在 shape.go，内容编码解码在 decode.go。
// 这里只保留与转发路径相关的上限：捕获上限只影响计量，不影响转发。

// Limits apply only to measurement; forwarding continues unchanged.
// Oversized captures and unsupported encodings report unknown decoded sizes.
const captureLimit = 8 << 20

// responseCaptureLimit 约束响应侧解析：SSE 正文要攒到流结束才能读终态事件。
const responseCaptureLimit = 8 << 20

// ---------- HTTP 计量 ----------

// meteredBody 在转发的同时累计请求体；读到 EOF 或关闭时发出一条 http-request。
type meteredBody struct {
	io.ReadCloser
	buf        bytes.Buffer
	keep       bool
	n          int64
	once       sync.Once
	finish     func(n int64, body []byte)
	err        error
	truncated  bool
	knownEmpty bool // explicit Content-Length: 0, with no chunked transfer
	mu         sync.Mutex
	readMu     sync.Mutex
}

func (b *meteredBody) Read(p []byte) (int, error) {
	b.readMu.Lock()
	defer b.readMu.Unlock()
	n, err := b.ReadCloser.Read(p)
	b.mu.Lock()
	defer b.mu.Unlock()
	if n > 0 {
		b.n += int64(n)
		if b.keep && b.buf.Len()+n <= captureLimit {
			b.buf.Write(p[:n])
		} else if b.keep {
			b.truncated = true
			b.keep = false
			b.buf.Reset()
		}
	}
	if err != nil {
		b.err = err
		b.done()
	}
	return n, err
}

func (b *meteredBody) Close() error {
	err := b.ReadCloser.Close()
	b.readMu.Lock()
	defer b.readMu.Unlock()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err == nil {
		b.err = err
		if err == nil && (!b.knownEmpty || b.n != 0) {
			b.err = io.ErrUnexpectedEOF
		}
	}
	b.done()
	return err
}

func (b *meteredBody) done() { b.once.Do(func() { b.finish(b.n, b.buf.Bytes()) }) }

type meteredResponse struct {
	io.ReadCloser
	n         int64
	once      sync.Once
	finish    func(n int64, body []byte, truncated bool, err error)
	buf       bytes.Buffer
	capture   bool
	truncated bool
	mu        sync.Mutex
}

func (b *meteredResponse) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.mu.Lock()
	b.n += int64(n)
	if n > 0 && b.capture {
		if b.buf.Len()+n <= responseCaptureLimit {
			b.buf.Write(p[:n])
		} else {
			b.truncated = true
			b.capture = false
			b.buf.Reset()
		}
	}
	b.mu.Unlock()
	if err != nil {
		b.once.Do(func() {
			n, body, truncated := b.snapshot()
			b.finish(n, body, truncated, err)
		})
	}
	return n, err
}

func (b *meteredResponse) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(func() {
		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		n, body, truncated := b.snapshot()
		b.finish(n, body, truncated, err)
	})
	return err
}

func (b *meteredResponse) snapshot() (int64, []byte, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.n, b.buf.Bytes(), b.truncated
}

type ctxKey int

const (
	keyTarget ctxKey = iota
	keyFlow
	keyStart
	keyH2
)

type meter struct {
	route    string
	emit     *emitter
	ca       *authority
	upstream *url.URL
	h1, h2   *http.Transport
	proxy    *httputil.ReverseProxy
	inner    *http.Server
	tunnels  chan net.Conn
	targets  sync.Map // *tls.Conn -> CONNECT 目标 host:port
	seq      *atomic.Uint64
	// 本批设计允许被测工具自行刷新令牌；账号设置写入仍默认拦截，避免改动用户账号。
	blockTokenRefresh  bool
	blockAccountWrites bool
	mu                 sync.Mutex
	stopping           bool
	conns              map[net.Conn]struct{}
	active             sync.WaitGroup
	stop               chan struct{}
	// 测试用：仅 WebSocket 上游拨号使用；生产保持 nil，按系统根证书校验。
	upstreamTLS *tls.Config
}

func (m *meter) track(c net.Conn) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopping {
		c.Close()
		return false
	}
	m.conns[c] = struct{}{}
	return true
}

// Keep the original error, but distinguish deliberate cancellation from a
// truncated or failed stream. Shutdown alone does not reclassify arbitrary errors.
func (m *meter) classifyError(ev event, err error) {
	ev["error_kind"] = "transport-error"
	if errors.Is(err, io.ErrUnexpectedEOF) {
		ev["error_kind"] = "incomplete-stream"
	}
	if errors.Is(err, http.ErrAbortHandler) {
		ev["error_kind"] = "forwarding-aborted"
	}
	if errors.Is(err, net.ErrClosed) || errors.Is(err, context.Canceled) {
		ev["error_kind"] = "request-cancellation"
		select {
		case <-m.stop:
			ev["error_kind"] = "shutdown-cancellation"
		default:
		}
	}
}

func (m *meter) reportError(flow any, err error) {
	ev := event{"route": m.route, "event": "error", "flow": flow, "message": err.Error()}
	m.classifyError(ev, err)
	m.emit.emit(ev)
}

func (m *meter) beginShutdown() {
	m.mu.Lock()
	if !m.stopping {
		m.stopping = true
		close(m.stop)
	}
	m.mu.Unlock()
}

func (m *meter) shutdown() {
	m.beginShutdown()
	m.mu.Lock()
	conns := make([]net.Conn, 0, len(m.conns))
	for c := range m.conns {
		conns = append(conns, c)
	}
	m.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
	m.inner.Close()
	m.active.Wait()
	m.h1.CloseIdleConnections()
	m.h2.CloseIdleConnections()
}

func newTransport(upstream *url.URL, h2 bool) *http.Transport {
	t := &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          512,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   20 * time.Second,
		ExpectContinueTimeout: time.Second,
		DisableCompression:    true, // 不替客户端加 Accept-Encoding，保持原请求头
		ReadBufferSize:        64 << 10,
		WriteBufferSize:       64 << 10,
		TLSClientConfig:       &tls.Config{ClientSessionCache: tls.NewLRUClientSessionCache(256)},
		HTTP2: &http.HTTP2Config{
			MaxReceiveBufferPerConnection: 64 << 20,
			MaxReceiveBufferPerStream:     32 << 20,
			SendPingTimeout:               30 * time.Second,
		},
	}
	if upstream != nil {
		t.Proxy = http.ProxyURL(upstream)
	}
	var p http.Protocols
	p.SetHTTP1(true)
	if h2 {
		p.SetHTTP2(true)
		t.ForceAttemptHTTP2 = true
	}
	t.Protocols = &p
	return t
}

type chanListener struct {
	ch     chan net.Conn
	closed chan struct{}
	once   sync.Once
}

func (l *chanListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.ch:
		return c, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}
func (l *chanListener) Close() error   { l.once.Do(func() { close(l.closed) }); return nil }
func (l *chanListener) Addr() net.Addr { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)} }

// 按客户端协商结果选上游协议偏好：h2 客户端优先 h2，h1 客户端只用 h1。
type protocolMirror struct{ h1, h2 http.RoundTripper }

func (m protocolMirror) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Context().Value(keyH2) == true {
		return m.h2.RoundTrip(r)
	}
	return m.h1.RoundTrip(r)
}

func newMeter(route string, em *emitter, ca *authority, upstream *url.URL, seq *atomic.Uint64, blockTokenRefresh, blockAccountWrites bool) *meter {
	m := &meter{route: route, emit: em, ca: ca, upstream: upstream, h1: newTransport(upstream, false), h2: newTransport(upstream, true), tunnels: make(chan net.Conn), seq: seq, blockTokenRefresh: blockTokenRefresh, blockAccountWrites: blockAccountWrites, conns: make(map[net.Conn]struct{}), stop: make(chan struct{})}
	m.proxy = &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			if pr.Out.URL.Scheme == "" {
				pr.Out.URL.Scheme = "https"
			}
			if pr.Out.URL.Host == "" {
				pr.Out.URL.Host = pr.In.Context().Value(keyTarget).(string)
			}
			pr.Out.Host = pr.In.Host
		},
		Transport:     protocolMirror{h1: m.h1, h2: m.h2},
		FlushInterval: -1, // 每次写都立即下发，SSE 不被合并
		BufferPool:    bufferPool{},
		ErrorLog:      log.New(io.Discard, "", 0),
		ModifyResponse: func(resp *http.Response) error {
			ctx := resp.Request.Context()
			flow, start := ctx.Value(keyFlow).(string), ctx.Value(keyStart).(time.Time)
			contentType, contentEncoding := resp.Header.Get("content-type"), resp.Header.Get("content-encoding")
			m.emit.emit(event{"route": m.route, "event": "http-response-headers", "flow": flow, "status": resp.StatusCode, "status_class": statusClass(resp.StatusCode), "content_type": contentType, "content_encoding": contentEncoding, "ttfb_s": time.Since(start).Seconds()})
			body := &meteredResponse{ReadCloser: resp.Body, capture: strings.Contains(strings.ToLower(contentType), "event-stream") || isJSONContentType(contentType)}
			captureBody := body.capture
			body.finish = func(n int64, captured []byte, truncated bool, err error) {
				ev := event{"route": m.route, "event": "http-response-end", "flow": flow, "status": resp.StatusCode, "status_class": statusClass(resp.StatusCode), "body_bytes": n, "duration_s": time.Since(start).Seconds()}
				if err != nil && err != io.EOF {
					ev["error"] = err.Error()
					m.classifyError(ev, err)
				}
				// 解析放写出协程，不占转发路径；captured 在读完前不再被改动。
				m.emit.later(ev, func(ev event) {
					fillResponseStats(ev, resp.StatusCode, contentType, contentEncoding, captured, truncated, captureBody)
				})
			}
			resp.Body = body
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			m.reportError(r.Context().Value(keyFlow), err)
			w.WriteHeader(http.StatusBadGateway)
		},
	}
	listener := &chanListener{ch: m.tunnels, closed: make(chan struct{})}
	m.inner = &http.Server{
		Handler:  http.HandlerFunc(m.serveIntercepted),
		ErrorLog: log.New(io.Discard, "", 0),
		HTTP2: &http.HTTP2Config{
			MaxConcurrentStreams:          1000,
			MaxReceiveBufferPerConnection: 64 << 20,
			MaxReceiveBufferPerStream:     32 << 20,
		},
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			if t, ok := m.targets.Load(c); ok {
				return context.WithValue(ctx, keyTarget, t.(string))
			}
			return ctx
		},
		ConnState: func(c net.Conn, s http.ConnState) {
			if s == http.StateClosed || s == http.StateHijacked {
				m.targets.Delete(c)
				if s == http.StateClosed {
					m.mu.Lock()
					delete(m.conns, c)
					m.mu.Unlock()
				}
			}
		},
	}
	go m.inner.Serve(listener)
	return m
}

type bufferPool struct{}

var pool = sync.Pool{New: func() any { b := make([]byte, 64<<10); return &b }}

func (bufferPool) Get() []byte  { return *(pool.Get().(*[]byte)) }
func (bufferPool) Put(b []byte) { pool.Put(&b) }

func (m *meter) nextFlow() string { return m.route + "-" + strconv.FormatUint(m.seq.Add(1), 10) }

// ServeHTTP 是对外的正向代理入口：CONNECT 进入 TLS 拦截，绝对 URI 直接转发。
func (m *meter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodConnect {
		m.tunnel(w, r)
		return
	}
	if !r.URL.IsAbs() {
		http.Error(w, "meterd is a forward proxy", http.StatusBadRequest)
		return
	}
	ctx := context.WithValue(r.Context(), keyTarget, r.URL.Host)
	m.serveIntercepted(w, r.WithContext(ctx))
}

func (m *meter) tunnel(w http.ResponseWriter, r *http.Request) {
	target := r.Host
	if _, _, err := net.SplitHostPort(target); err != nil {
		target = net.JoinHostPort(target, "443")
	}
	hostname, _, _ := net.SplitHostPort(target)
	conn, brw, err := w.(http.Hijacker).Hijack()
	if err != nil {
		return
	}
	if _, err := conn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n")); err != nil {
		conn.Close()
		return
	}
	var raw net.Conn = conn
	if brw.Reader.Buffered() > 0 {
		raw = &prefixedConn{Conn: conn, r: io.MultiReader(bytes.NewReader(peek(brw.Reader)), conn)}
	}
	tlsConn := tls.Server(raw, &tls.Config{
		NextProtos: []string{"h2", "http/1.1"},
		GetCertificate: func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
			name := hello.ServerName
			if name == "" {
				name = hostname
			}
			return m.ca.leaf(name)
		},
	})
	m.targets.Store(tlsConn, target)
	if !m.track(tlsConn) {
		m.targets.Delete(tlsConn)
		return
	}
	select {
	case m.tunnels <- tlsConn:
	case <-m.stop:
		tlsConn.Close()
	}
}

func peek(r *bufio.Reader) []byte {
	b, _ := r.Peek(r.Buffered())
	return append([]byte(nil), b...)
}

type prefixedConn struct {
	net.Conn
	r io.Reader
}

func (c *prefixedConn) Read(p []byte) (int, error) { return c.r.Read(p) }

func isTokenEndpoint(path string) bool {
	return strings.HasSuffix(path, "/token") || strings.HasSuffix(path, "/oauth/token")
}

func hostOnly(h string) string {
	if host, _, err := net.SplitHostPort(h); err == nil {
		return host
	}
	return h
}

func (m *meter) serveIntercepted(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	// 空闲 WS 与 SSE 都会长期占用处理器，不能据此限制转发并发。
	// 试验编排控制负载；计量仅按单条消息限制捕获大小。
	m.mu.Lock()
	if m.stopping {
		m.mu.Unlock()
		http.Error(w, "proxy shutting down", http.StatusServiceUnavailable)
		return
	}
	m.active.Add(1)
	m.mu.Unlock()
	defer m.active.Done()
	flow := m.nextFlow()
	if r.ProtoMajor == 1 {
		if err := http.NewResponseController(w).EnableFullDuplex(); err != nil {
			m.emit.emit(event{"route": m.route, "event": "error", "flow": flow, "message": "enable full duplex: " + err.Error()})
			http.Error(w, "full duplex unavailable", http.StatusInternalServerError)
			return
		}
	}
	defer func() {
		if v := recover(); v != nil {
			if err, ok := v.(error); ok {
				m.reportError(flow, err)
			} else {
				m.reportError(flow, fmt.Errorf("%v", v))
			}
			panic(v)
		}
	}()
	host := hostOnly(r.Host)
	if host == "" {
		host = hostOnly(r.Context().Value(keyTarget).(string))
	}
	path := r.URL.Path
	switch {
	case m.blockAccountWrites && host == "chatgpt.com" && strings.HasPrefix(path, "/backend-api/settings/") && r.Method != http.MethodGet:
		m.emit.emit(event{"route": m.route, "event": "blocked-account-setting-write", "host": host, "path": path, "method": r.Method})
		writeJSONError(w, "account setting write blocked by benchmark meter")
		return
	case m.blockTokenRefresh && isTokenEndpoint(path):
		m.emit.emit(event{"route": m.route, "event": "blocked-token-refresh", "host": host, "path": path, "method": r.Method})
		writeJSONError(w, "token refresh blocked by benchmark meter")
		return
	}
	upgrade := strings.EqualFold(r.Header.Get("Upgrade"), "websocket")
	if upgrade && r.ProtoMajor == 1 {
		m.websocket(w, r, flow, host, start)
		return
	}
	encoding := r.Header.Get("Content-Encoding")
	// 只把 Codex Responses 的模型调用标记出来；同一端口上的其它请求仍记录，但不计入模型请求。
	modelRequest := r.Method == http.MethodPost && strings.HasSuffix(path, "/responses")
	base := event{"route": m.route, "event": "http-request", "flow": flow, "transport": "http", "model_request": modelRequest,
		"host": host, "path": path, "method": r.Method,
		"http_version": r.Proto, "content_encoding": encoding, "websocket_upgrade": upgrade, "started_at": float64(start.UnixNano()) / 1e9}
	jsonBody := strings.Contains(r.Header.Get("Content-Type"), "json")
	var mb *meteredBody
	finish := func(n int64, body []byte) {
		base["body_wire_bytes"] = n
		if mb != nil {
			if mb.err != nil && mb.err != io.EOF {
				base["error"] = mb.err.Error()
				m.classifyError(base, mb.err)
			}
			if mb.truncated {
				base["measurement_error"] = "request-exceeds-capture-limit"
				base["body_decoded_bytes"] = nil
				base["request_shape"] = "too-large"
				m.emit.emit(base)
				return
			}
		}
		m.emit.later(base, func(base event) { fillRequestBody(base, encoding, jsonBody, n, body) })
	}
	if r.Body == nil || r.Body == http.NoBody {
		finish(0, nil)
	} else {
		mb = &meteredBody{ReadCloser: r.Body, keep: jsonBody, finish: finish, knownEmpty: r.ContentLength == 0 && len(r.TransferEncoding) == 0}
		r.Body = mb
	}
	if mb != nil {
		defer mb.Close()
	}
	ctx := context.WithValue(r.Context(), keyFlow, flow)
	ctx = context.WithValue(ctx, keyStart, start)
	ctx = context.WithValue(ctx, keyH2, r.ProtoMajor == 2)
	m.proxy.ServeHTTP(w, r.WithContext(ctx))
}

// fillRequestBody 解码请求体后写入结构与计数；任何失败都把结构字段留在 null 并写明确类别。
func fillRequestBody(base event, encoding string, jsonBody bool, n int64, body []byte) {
	if !jsonBody {
		// 非 JSON 请求体不保留副本，也不解码；线上字节就是它的可观测大小。
		base["body_decoded_bytes"] = n
		base["request_shape"] = "not-json"
		return
	}
	decoded, kind := decodeBody(encoding, body)
	if kind != decodeOK {
		base["body_decoded_bytes"] = nil
		base["request_shape"] = kind
		return
	}
	base["body_decoded_bytes"] = len(decoded)
	trimmed := bytes.TrimSpace(decoded)
	if len(trimmed) == 0 {
		base["request_shape"] = "empty"
		return
	}
	if trimmed[0] != '{' {
		base["request_shape"] = "not-object"
		return
	}
	applyRequestShape(base, decoded)
}

func writeJSONError(w http.ResponseWriter, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusForbidden)
	fmt.Fprintf(w, `{"error":%q}`, message)
}

// ---------- WebSocket ----------

func (m *meter) dialUpstreamTLS(ctx context.Context, target string) (net.Conn, error) {
	d := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
	var conn net.Conn
	var err error
	if m.upstream == nil {
		conn, err = d.DialContext(ctx, "tcp", target)
	} else {
		conn, err = d.DialContext(ctx, "tcp", m.upstream.Host)
		if err == nil {
			fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target, target)
			br := bufio.NewReader(conn)
			resp, rerr := http.ReadResponse(br, &http.Request{Method: http.MethodConnect})
			if rerr != nil {
				conn.Close()
				return nil, rerr
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				conn.Close()
				return nil, fmt.Errorf("upstream proxy CONNECT %s: %s", target, resp.Status)
			}
			if br.Buffered() > 0 {
				conn = &prefixedConn{Conn: conn, r: io.MultiReader(bytes.NewReader(peek(br)), conn)}
			}
		}
	}
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{}
	if m.upstreamTLS != nil {
		cfg = m.upstreamTLS.Clone()
	}
	cfg.ServerName = hostOnly(target)
	cfg.NextProtos = []string{"http/1.1"}
	tc := tls.Client(conn, cfg)
	if err := tc.HandshakeContext(ctx); err != nil {
		conn.Close()
		return nil, err
	}
	return tc, nil
}

func (m *meter) websocket(w http.ResponseWriter, r *http.Request, flow, host string, start time.Time) {
	target := r.Context().Value(keyTarget).(string)
	m.emit.emit(event{"route": m.route, "event": "http-request", "flow": flow, "transport": "ws", "model_request": false, "host": host, "path": r.URL.Path, "method": r.Method,
		"http_version": r.Proto, "content_encoding": "", "body_wire_bytes": 0, "body_decoded_bytes": 0, "websocket_upgrade": true, "started_at": float64(start.UnixNano()) / 1e9})
	up, err := m.dialUpstreamTLS(r.Context(), target)
	if err != nil {
		m.reportError(flow, err)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	if !m.track(up) {
		http.Error(w, "proxy shutting down", http.StatusServiceUnavailable)
		return
	}
	defer func() { up.Close(); m.mu.Lock(); delete(m.conns, up); m.mu.Unlock() }()
	out := r.Clone(context.Background())
	out.RequestURI = ""
	out.URL.Scheme, out.URL.Host = "https", target
	out.Header.Del("Proxy-Connection")
	out.Header.Del("Proxy-Authorization")
	if err := out.Write(up); err != nil {
		up.Close()
		m.reportError(flow, err)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	upReader := bufio.NewReaderSize(up, 64<<10)
	resp, err := http.ReadResponse(upReader, out)
	if err != nil {
		up.Close()
		m.reportError(flow, err)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	m.emit.emit(event{"route": m.route, "event": "http-response-headers", "flow": flow, "status": resp.StatusCode, "status_class": statusClass(resp.StatusCode), "content_type": resp.Header.Get("content-type"), "content_encoding": resp.Header.Get("content-encoding"), "ttfb_s": time.Since(start).Seconds()})
	if resp.StatusCode != http.StatusSwitchingProtocols {
		for k, v := range resp.Header {
			w.Header()[k] = v
		}
		w.WriteHeader(resp.StatusCode)
		n, copyErr := io.Copy(w, resp.Body)
		resp.Body.Close()
		up.Close()
		ev := event{"route": m.route, "event": "http-response-end", "flow": flow, "status": resp.StatusCode, "status_class": statusClass(resp.StatusCode), "body_bytes": n, "duration_s": time.Since(start).Seconds()}
		if copyErr != nil {
			ev["error"] = copyErr.Error()
			m.classifyError(ev, copyErr)
		}
		m.emit.emit(ev)
		return
	}
	client, clientBuf, err := w.(http.Hijacker).Hijack()
	if err != nil {
		up.Close()
		m.reportError(flow, err)
		return
	}
	if !m.track(client) {
		return
	}
	defer func() { client.Close(); m.mu.Lock(); delete(m.conns, client); m.mu.Unlock() }()
	var head bytes.Buffer
	fmt.Fprintf(&head, "HTTP/1.1 %s\r\n", resp.Status)
	resp.Header.Write(&head)
	head.WriteString("\r\n")
	if _, err := client.Write(head.Bytes()); err != nil {
		client.Close()
		up.Close()
		m.reportError(flow, err)
		return
	}
	ext := resp.Header.Get("Sec-WebSocket-Extensions")
	m.emit.emit(event{"route": m.route, "event": "ws-start", "flow": flow, "host": host, "path": r.URL.Path, "extensions": ext})
	deflate := strings.Contains(ext, "permessage-deflate")
	supportedExtensions := true
	for _, extension := range strings.Split(ext, ",") {
		name := strings.TrimSpace(strings.SplitN(extension, ";", 2)[0])
		if name != "" && name != "permessage-deflate" {
			supportedExtensions = false
		}
	}
	var clientIn io.Reader = client
	if clientBuf.Reader.Buffered() > 0 {
		clientIn = io.MultiReader(bytes.NewReader(peek(clientBuf.Reader)), client)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	closeBoth := sync.OnceFunc(func() { client.Close(); up.Close() })
	pump := func(dst net.Conn, src io.Reader, p *wsParser) {
		defer wg.Done()
		defer p.end()
		buf := make([]byte, 64<<10)
		for {
			n, err := src.Read(buf)
			if n > 0 {
				// 先转发再计量：解析不在关键路径上。
				written, werr := dst.Write(buf[:n])
				p.feed(buf[:written])
				if werr == nil && written != n {
					werr = io.ErrShortWrite
				}
				if werr != nil {
					m.reportError(flow, werr)
					closeBoth()
					return
				}
			}
			if err != nil {
				if err == io.EOF {
					if cw, ok := dst.(interface{ CloseWrite() error }); ok {
						if cerr := cw.CloseWrite(); cerr != nil {
							m.reportError(flow, cerr)
							closeBoth()
						}
					} else {
						closeBoth()
					}
				} else {
					m.reportError(flow, err)
					closeBoth()
				}
				return
			}
		}
	}
	clientParser := &wsParser{m: m, flow: flow, fromClient: true, deflate: deflate, noContext: strings.Contains(ext, "client_no_context_takeover")}
	serverParser := &wsParser{m: m, flow: flow, fromClient: false, deflate: deflate, noContext: strings.Contains(ext, "server_no_context_takeover")}
	if !supportedExtensions {
		clientParser.fail("unsupported websocket extension")
		serverParser.fail("unsupported websocket extension")
	}
	go pump(up, clientIn, clientParser)
	go pump(client, upReader, serverParser)
	wg.Wait()
	closeBoth()
	m.emit.emit(event{"route": m.route, "event": "ws-end", "flow": flow, "duration_s": time.Since(start).Seconds()})
}

// wsParser 在已转发字节的副本上重组 WebSocket 消息（RFC 6455），只为计量。
type wsParser struct {
	m          *meter
	flow       string
	fromClient bool
	deflate    bool
	noContext  bool
	pending    []byte
	message    []byte
	opcode     byte
	compressed bool
	window     []byte // permessage-deflate 上下文接管时的 LZ77 窗口
	broken     bool
}

func (p *wsParser) fail(message string) {
	p.broken = true
	p.pending, p.message, p.window = nil, nil, nil
	p.m.emit.emit(event{"route": p.m.route, "event": "error", "flow": p.flow, "from_client": p.fromClient, "message": message})
}

func (p *wsParser) end() {
	if !p.broken && (len(p.pending) != 0 || p.opcode != 0) {
		p.fail("incomplete websocket frame or message")
	}
}

func (p *wsParser) feed(chunk []byte) {
	if p.broken {
		return
	}
	p.pending = append(p.pending, chunk...)
	for {
		consumed, ok := p.frame(p.pending)
		if p.broken {
			p.pending, p.message = nil, nil
			return
		}
		if !ok {
			break
		}
		p.pending = p.pending[consumed:]
	}
	if len(p.pending) == 0 {
		p.pending = p.pending[:0:0]
	} else if cap(p.pending) > 4<<20 && len(p.pending) < cap(p.pending)/4 {
		p.pending = append([]byte(nil), p.pending...)
	}
}

func (p *wsParser) frame(b []byte) (int, bool) {
	if len(b) < 2 {
		return 0, false
	}
	fin, rsv1, opcode := b[0]&0x80 != 0, b[0]&0x40 != 0, b[0]&0x0f
	masked := b[1]&0x80 != 0
	length := uint64(b[1] & 0x7f)
	off := 2
	switch length {
	case 126:
		if len(b) < 4 {
			return 0, false
		}
		length = uint64(binary.BigEndian.Uint16(b[2:4]))
		off = 4
	case 127:
		if len(b) < 10 {
			return 0, false
		}
		length = binary.BigEndian.Uint64(b[2:10])
		off = 10
	}
	var mask [4]byte
	if masked {
		if len(b) < off+4 {
			return 0, false
		}
		copy(mask[:], b[off:off+4])
		off += 4
	}
	if length > captureLimit {
		p.fail("websocket frame exceeds capture limit")
		return 0, false
	}
	if b[0]&0x30 != 0 || (rsv1 && (!p.deflate || opcode == 0 || opcode >= 8)) || masked != p.fromClient ||
		(opcode != 0 && opcode != 1 && opcode != 2 && opcode != 8 && opcode != 9 && opcode != 10) ||
		(opcode >= 8 && (!fin || length > 125)) ||
		(opcode == 0 && p.opcode == 0) || ((opcode == 1 || opcode == 2) && p.opcode != 0) {
		p.fail("unsupported or invalid websocket framing")
		return 0, false
	}
	end := off + int(length)
	if len(b) < end {
		return 0, false
	}
	if opcode >= 0x8 { // 控制帧不计入消息
		return end, true
	}
	if len(p.message)+int(length) > captureLimit {
		p.fail("websocket message exceeds capture limit")
		return 0, false
	}
	payload := b[off:end]
	start := len(p.message)
	p.message = append(p.message, payload...)
	if masked {
		for i := range payload {
			p.message[start+i] ^= mask[i&3]
		}
	}
	if opcode != 0 {
		p.opcode, p.compressed = opcode, rsv1 && p.deflate
	}
	if fin {
		p.finish()
	}
	return end, true
}

func (p *wsParser) finish() {
	ev := event{"route": p.m.route, "event": "ws-message", "flow": p.flow, "from_client": p.fromClient, "is_text": p.opcode == 1, "wire_payload_bytes": len(p.message), "compressed": p.compressed}
	content := p.message
	if p.compressed {
		// 解压要按顺序维护 LZ77 窗口，只能在本协程做；只有声明了 permessage-deflate 的连接才会走到这里。
		inflated, err := p.inflate(p.message)
		if err != nil {
			ev["inflate_error"] = err.Error()
			content = nil
			p.broken = true
			p.window = nil
		} else {
			content = inflated
		}
	} else {
		content = append([]byte(nil), p.message...)
	}
	fromClient := p.fromClient
	p.m.emit.later(ev, func(ev event) {
		ev["transport"] = "ws"
		if content == nil {
			ev["payload_bytes"] = nil
			return
		}
		ev["payload_bytes"] = len(content)
		if fromClient {
			applyRequestShape(ev, content)
			ev["model_request"] = ev["type"] == "response.create"
			return
		}
		var v struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(content, &v) == nil {
			ev["type"] = v.Type
		} else {
			ev["type"] = nil
		}
		if st := wsTerminalStats(content); st != nil {
			applyResponseStats(ev, st)
		}
	})
	p.message = p.message[:0]
	p.opcode = 0
	if cap(p.message) > 8<<20 {
		p.message = nil
	}
}

func (p *wsParser) inflate(data []byte) ([]byte, error) {
	var dict []byte
	if !p.noContext {
		dict = p.window
	}
	r := flate.NewReaderDict(io.MultiReader(bytes.NewReader(data), bytes.NewReader([]byte{0, 0, 0xff, 0xff, 1, 0, 0, 0xff, 0xff})), dict)
	defer r.Close()
	out, err := io.ReadAll(io.LimitReader(r, decodedLimit+1))
	if len(out) > decodedLimit {
		return nil, fmt.Errorf("websocket decoded message exceeds limit")
	}
	if err != nil {
		return nil, err
	}
	if !p.noContext {
		if len(out) >= 32<<10 {
			p.window = append(p.window[:0], out[len(out)-(32<<10):]...)
		} else {
			p.window = append(p.window, out...)
			if len(p.window) > 32<<10 {
				copy(p.window, p.window[len(p.window)-(32<<10):])
				p.window = p.window[:32<<10]
			}
		}
	}
	return out, nil
}

// ---------- 入口 ----------

func main() {
	listenDefault := os.Getenv("BENCH_BIND")
	if listenDefault == "" {
		listenDefault = "127.0.0.1"
	}
	out := flag.String("out", "", "JSONL 计量输出文件")
	confdir := flag.String("confdir", "", "CA 证书目录（不存在则生成 meterd-ca-cert.pem / meterd-ca-key.pem）")
	routes := flag.String("routes", "", `监听端口到路线名的 JSON，如 {"24481":"stravia"}`)
	upstream := flag.String("upstream", "", "上游 HTTP 代理，如 http://127.0.0.1:7890；留空直连")
	listen := flag.String("listen", listenDefault, "监听地址（BENCH_BIND 优先，默认 127.0.0.1）；所有路由端口共用")
	blockToken := flag.Bool("block-token-refresh", false, "403 拦截 /token 与 /oauth/token；本批默认放行，让被测工具自行刷新")
	blockSettings := flag.Bool("block-account-writes", true, "403 拦截 chatgpt.com /backend-api/settings/ 的非 GET 写")
	flag.Parse()
	if *out == "" || *confdir == "" || *routes == "" {
		flag.Usage()
		os.Exit(2)
	}
	var portRoutes map[string]string
	if err := json.Unmarshal([]byte(*routes), &portRoutes); err != nil {
		log.Fatalf("routes: %v", err)
	}
	var up *url.URL
	if *upstream != "" {
		u, err := url.Parse(*upstream)
		if err != nil {
			log.Fatalf("upstream: %v", err)
		}
		up = u
	}
	ca, err := loadOrCreateCA(*confdir)
	if err != nil {
		log.Fatalf("ca: %v", err)
	}
	em, err := newEmitter(*out)
	if err != nil {
		log.Fatalf("out: %v", err)
	}
	seq := new(atomic.Uint64)
	var servers []*http.Server
	var meters []*meter
	for port, route := range portRoutes {
		ln, err := net.Listen("tcp", net.JoinHostPort(*listen, port))
		if err != nil {
			log.Fatalf("listen %s: %v", port, err)
		}
		m := newMeter(route, em, ca, up, seq, *blockToken, *blockSettings)
		meters = append(meters, m)
		srv := &http.Server{Handler: m, ErrorLog: log.New(io.Discard, "", 0), ReadHeaderTimeout: 30 * time.Second}
		servers = append(servers, srv)
		go srv.Serve(ln)
	}
	log.Printf("meterd ready: %d routes, upstream=%q", len(servers), *upstream)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sig:
	case <-em.failed:
	}
	for _, m := range meters {
		m.beginShutdown()
	}
	for _, s := range servers {
		s.Close()
	}
	for _, m := range meters {
		m.shutdown()
	}
	if err := em.drain(); err != nil {
		log.Printf("meter output failed: %v", err)
		os.Exit(1)
	}
}
