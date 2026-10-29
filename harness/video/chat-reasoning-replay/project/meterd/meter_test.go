package main

// 用途：meterd 的结构计量回归测试。用进程内假上游（HTTP/WS）覆盖：
// gzip/zstd 请求体、带/不带 encrypted_content 的 reasoning item、SSE 终态 usage、
// 429、解析失败、WebSocket response.create，以及“凭据不落盘”的隐私断言。
// 输入：无外部数据；所有上游与客户端都在本进程/回环端口内。
// 副作用：只在 t.TempDir() 下生成 CA 与 JSONL，测试结束自动清理；不发起真实推理。

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

// ---------- 进程内 meterd ----------

type testMeter struct {
	addr   string
	proxy  *url.URL
	out    string
	roots  *x509.CertPool
	meter  *meter
	em     *emitter
	server *http.Server
}

func startTestMeter(t *testing.T, route string) *testMeter {
	t.Helper()
	dir := t.TempDir()
	ca, err := loadOrCreateCA(filepath.Join(dir, "ca"))
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "meter.jsonl")
	em, err := newEmitter(out)
	if err != nil {
		t.Fatal(err)
	}
	m := newMeter(route, em, ca, nil, new(atomic.Uint64), false, true)
	// 假上游用自签证书；只在这个测试实例里放开校验。
	m.upstreamTLS = &tls.Config{InsecureSkipVerify: true}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: m}
	go srv.Serve(ln)
	roots := x509.NewCertPool()
	roots.AddCert(ca.cert)
	u, _ := url.Parse("http://" + ln.Addr().String())
	return &testMeter{addr: ln.Addr().String(), proxy: u, out: out, roots: roots, meter: m, em: em, server: srv}
}

// close 必须在所有请求结束后调用：先停入口，再等计量器退出，最后刷出 JSONL。
func (h *testMeter) close(t *testing.T) {
	t.Helper()
	h.server.Close()
	h.meter.shutdown()
	if err := h.em.drain(); err != nil {
		t.Fatal(err)
	}
}

func (h *testMeter) events(t *testing.T) []map[string]any {
	t.Helper()
	raw, err := os.ReadFile(h.out)
	if err != nil {
		t.Fatal(err)
	}
	var rows []map[string]any
	for _, line := range bytes.Split(raw, []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var row map[string]any
		if err := json.Unmarshal(line, &row); err != nil {
			t.Fatalf("jsonl: %v", err)
		}
		rows = append(rows, row)
	}
	return rows
}

func (h *testMeter) raw(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile(h.out)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func findEvent(t *testing.T, rows []map[string]any, event string, match func(map[string]any) bool) map[string]any {
	t.Helper()
	for _, row := range rows {
		if row["event"] == event && (match == nil || match(row)) {
			return row
		}
	}
	t.Fatalf("event %q not found in %d rows", event, len(rows))
	return nil
}

// ---------- HTTP 假上游 ----------

const encryptedBlob = "ENCRYPTED-BLOB-AAA"

func httpRequestBody() string {
	return `{
		"model":"gpt-6-luna",
		"input":[
			{"type":"message","role":"user","content":[{"type":"input_text","text":"message body must not be recorded"}]},
			{"type":"reasoning","encrypted_content":"` + encryptedBlob + `","summary":[]},
			{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]},
			{"type":"function_call","name":"shell","arguments":"{}","call_id":"c1"},
			{"type":"function_call_output","call_id":"c1","output":"ok"},
			{"type":"reasoning","summary":[{"type":"summary_text","text":"plain"}]}
		],
		"tools":[{"type":"function","name":"shell","parameters":{"type":"object"}},{"type":"web_search"}],
		"reasoning":{"effort":"high","summary":"auto"},
		"include":["reasoning.encrypted_content"],
		"store":false,
		"prompt_cache_key":"CACHE-KEY-SECRET",
		"previous_response_id":"resp_123",
		"stream":true
	}`
}

const sseBody = "event: response.created\n" +
	`data: {"type":"response.created","response":{"id":"r1","status":"in_progress"}}` + "\n\n" +
	"event: response.output_item.done\n" +
	`data: {"type":"response.output_item.done","item":{"type":"reasoning"}}` + "\n\n" +
	"event: response.completed\n" +
	`data: {"type":"response.completed","response":{"status":"completed","output":[{"type":"reasoning"},{"type":"reasoning"},{"type":"message"}],"usage":{"input_tokens":100,"input_tokens_details":{"cached_tokens":40},"output_tokens":42,"output_tokens_details":{"reasoning_tokens":17}}}}` + "\n\n"

func gzipBytes(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func zstdBytes(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := zstd.NewWriter(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func proxyClient(h *testMeter) *http.Client {
	return &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(h.proxy), DisableCompression: true}}
}

func postCodex(t *testing.T, h *testMeter, upstream string, contentType string, body []byte, headers map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, upstream+"/backend-api/codex/responses?token=query-secret", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", contentType)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := proxyClient(h).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp
}

func TestHTTPGzipRequestWithEncryptedReasoningAndSSEUsage(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, sseBody)
	}))
	defer upstream.Close()

	h := startTestMeter(t, "stravia")
	plain := []byte(httpRequestBody())
	wire := gzipBytes(t, plain)
	headers := map[string]string{
		"Content-Encoding":   "gzip",
		"Authorization":      "Bearer sk-secret-token",
		"Cookie":             "session=cookie-secret",
		"Chatgpt-Account-Id": "acct-secret",
	}
	resp := postCodex(t, h, upstream.URL, "application/json", wire, headers)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	h.close(t)
	rows := h.events(t)

	req := findEvent(t, rows, "http-request", func(e map[string]any) bool { return e["model_request"] == true })
	checks := map[string]any{
		"transport":                       "http",
		"request_shape":                   "ok",
		"model":                           "gpt-6-luna",
		"type":                            nil,
		"body_wire_bytes":                 float64(len(wire)),
		"body_decoded_bytes":              float64(len(plain)),
		"content_encoding":                "gzip",
		"input_items":                     float64(6),
		"input_form":                      "array",
		"input_reasoning_items":           float64(2),
		"input_reasoning_encrypted_items": float64(1),
		"input_encrypted_items":           float64(1),
		"has_previous_response_id":        true,
		"store":                           false,
		"reasoning_effort":                "high",
		"reasoning_summary":               "auto",
		"include_reasoning_encrypted":     true,
		"tools_count":                     float64(2),
		"has_prompt_cache_key":            true,
	}
	for k, want := range checks {
		if got := req[k]; got != want {
			t.Errorf("%s=%v (%T), want %v", k, got, got, want)
		}
	}
	types, ok := req["input_item_types"].(map[string]any)
	if !ok {
		t.Fatalf("input_item_types=%T", req["input_item_types"])
	}
	wantTypes := map[string]float64{"message": 2, "reasoning": 2, "function_call": 1, "function_call_output": 1}
	for k, v := range wantTypes {
		if types[k] != v {
			t.Errorf("type %s=%v, want %v", k, types[k], v)
		}
	}
	lengths, ok := req["input_encrypted_lengths"].([]any)
	if !ok || len(lengths) != 1 || lengths[0] != float64(len(encryptedBlob)) {
		t.Fatalf("lengths=%v", req["input_encrypted_lengths"])
	}

	end := findEvent(t, rows, "http-response-end", func(e map[string]any) bool { return e["status"] == float64(200) })
	wantEnd := map[string]any{
		"status_class":             "ok",
		"response_parse":           "sse",
		"response_events":          float64(3),
		"response_terminal":        "response.completed",
		"response_status":          "completed",
		"response_reasoning_items": float64(2),
		"usage_input_tokens":       float64(100),
		"usage_cached_tokens":      float64(40),
		"usage_output_tokens":      float64(42),
		"usage_reasoning_tokens":   float64(17),
	}
	for k, want := range wantEnd {
		if got := end[k]; got != want {
			t.Errorf("%s=%v, want %v", k, got, want)
		}
	}
}

func TestHTTPZstdRequestWithoutEncryptedReasoning(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if enc := r.Header.Get("Content-Encoding"); enc != "zstd" {
			t.Errorf("upstream content-encoding=%q", enc)
		}
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, sseBody)
	}))
	defer upstream.Close()

	h := startTestMeter(t, "cpa")
	plain := []byte(`{"model":"gpt-6-luna","input":[{"type":"message","role":"user","content":"hi"},{"type":"reasoning","summary":[]}],"store":true}`)
	resp := postCodex(t, h, upstream.URL, "application/json", zstdBytes(t, plain), map[string]string{"Content-Encoding": "zstd"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	h.close(t)
	rows := h.events(t)

	req := findEvent(t, rows, "http-request", func(e map[string]any) bool { return e["model_request"] == true })
	if req["body_decoded_bytes"] != float64(len(plain)) || req["request_shape"] != "ok" {
		t.Fatalf("decoded=%v shape=%v", req["body_decoded_bytes"], req["request_shape"])
	}
	if req["input_reasoning_items"] != float64(1) || req["input_encrypted_items"] != float64(0) || req["input_reasoning_encrypted_items"] != float64(0) {
		t.Fatalf("reasoning=%v encrypted=%v", req["input_reasoning_items"], req["input_encrypted_items"])
	}
	if lengths, ok := req["input_encrypted_lengths"].([]any); !ok || len(lengths) != 0 {
		t.Fatalf("lengths=%v", req["input_encrypted_lengths"])
	}
	if req["store"] != true {
		t.Fatalf("store=%v", req["store"])
	}
	if req["has_previous_response_id"] != nil {
		t.Fatalf("absent previous_response_id should be null, got %v", req["has_previous_response_id"])
	}
	if req["has_prompt_cache_key"] != nil || req["include_reasoning_encrypted"] != nil {
		t.Fatalf("absent fields should be null: %v %v", req["has_prompt_cache_key"], req["include_reasoning_encrypted"])
	}
}

func TestHTTPGzipSSEResponseIsDecoded(t *testing.T) {
	body := gzipBytes(t, []byte(sseBody))
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Content-Encoding", "gzip")
		w.Write(body)
	}))
	defer upstream.Close()

	h := startTestMeter(t, "sub2api")
	postCodex(t, h, upstream.URL, "application/json", []byte(`{"model":"m","input":[]}`), nil)
	h.close(t)
	rows := h.events(t)
	end := findEvent(t, rows, "http-response-end", nil)
	if end["response_parse"] != "sse" || end["usage_output_tokens"] != float64(42) {
		t.Fatalf("parse=%v usage=%v", end["response_parse"], end["usage_output_tokens"])
	}
}

func TestHTTPRateLimitedIsDistinguished(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		io.WriteString(w, `{"error":{"message":"rate limited"}}`)
	}))
	defer upstream.Close()

	h := startTestMeter(t, "stravia")
	resp := postCodex(t, h, upstream.URL, "application/json", []byte(`{"model":"gpt-6-luna","input":[]}`), nil)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	h.close(t)
	rows := h.events(t)
	headers := findEvent(t, rows, "http-response-headers", nil)
	if headers["status"] != float64(429) || headers["status_class"] != "rate-limited" {
		t.Fatalf("headers=%v", headers)
	}
	end := findEvent(t, rows, "http-response-end", nil)
	if end["status"] != float64(429) || end["status_class"] != "rate-limited" {
		t.Fatalf("end=%v", end)
	}
	if end["usage_input_tokens"] != nil || end["response_reasoning_items"] != nil {
		t.Fatalf("unexpected stats for 429: %v", end)
	}
}

func TestHTTPInvalidJSONIsUnknownNotZero(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, sseBody)
	}))
	defer upstream.Close()

	h := startTestMeter(t, "cpa")
	postCodex(t, h, upstream.URL, "application/json", []byte(`{"model":"gpt-6-luna",`), nil)
	h.close(t)
	rows := h.events(t)
	req := findEvent(t, rows, "http-request", nil)
	if req["request_shape"] != "invalid-json" {
		t.Fatalf("request_shape=%v", req["request_shape"])
	}
	for _, key := range []string{"input_items", "input_reasoning_items", "input_reasoning_encrypted_items", "input_encrypted_items", "input_encrypted_lengths", "tools_count"} {
		if _, present := req[key]; present {
			t.Errorf("%s should be absent on parse failure, got %v", key, req[key])
		}
	}
}

// ---------- WebSocket ----------

func serverFrame(op byte, payload []byte) []byte {
	first := op | 0x80
	b := []byte{first}
	switch {
	case len(payload) < 126:
		b = append(b, byte(len(payload)))
	case len(payload) < 1<<16:
		b = append(b, 126)
		b = binary.BigEndian.AppendUint16(b, uint16(len(payload)))
	default:
		b = append(b, 127)
		b = binary.BigEndian.AppendUint64(b, uint64(len(payload)))
	}
	return append(b, payload...)
}

func clientFrame(op byte, payload []byte) []byte {
	first := op | 0x80
	b := []byte{first}
	switch {
	case len(payload) < 126:
		b = append(b, 0x80|byte(len(payload)))
	case len(payload) < 1<<16:
		b = append(b, 0x80|126)
		b = binary.BigEndian.AppendUint16(b, uint16(len(payload)))
	default:
		b = append(b, 0x80|127)
		b = binary.BigEndian.AppendUint64(b, uint64(len(payload)))
	}
	mask := [4]byte{7, 21, 33, 45}
	b = append(b, mask[:]...)
	for i, v := range payload {
		b = append(b, v^mask[i&3])
	}
	return b
}

func readFrame(r *bufio.Reader) (byte, []byte, error) {
	var head [2]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return 0, nil, err
	}
	op := head[0] & 0x0f
	masked := head[1]&0x80 != 0
	length := uint64(head[1] & 0x7f)
	switch length {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			return 0, nil, err
		}
		length = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			return 0, nil, err
		}
		length = binary.BigEndian.Uint64(ext[:])
	}
	var mask [4]byte
	if masked {
		if _, err := io.ReadFull(r, mask[:]); err != nil {
			return 0, nil, err
		}
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i&3]
		}
	}
	return op, payload, nil
}

const wsCompleted = `{"type":"response.completed","response":{"status":"completed","output":[{"type":"reasoning"},{"type":"reasoning"}],"usage":{"input_tokens":11,"input_tokens_details":{"cached_tokens":3},"output_tokens":9,"output_tokens_details":{"reasoning_tokens":5}}}}`

func newWSUpstream(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			http.Error(w, "websocket only", http.StatusBadRequest)
			return
		}
		key := r.Header.Get("Sec-WebSocket-Key")
		sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
		accept := base64.StdEncoding.EncodeToString(sum[:])
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Error("hijack unavailable")
			return
		}
		conn, brw, err := hj.Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		fmt.Fprintf(conn, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", accept)
		for {
			op, _, err := readFrame(brw.Reader)
			if err != nil {
				return
			}
			if op == 8 {
				return
			}
			if op != 1 {
				continue
			}
			conn.Write(serverFrame(1, []byte(`{"type":"response.created","response":{"id":"r2","status":"in_progress"}}`)))
			conn.Write(serverFrame(1, []byte(wsCompleted)))
		}
	}))
}

func connectWS(t *testing.T, h *testMeter, target string) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := net.Dial("tcp", h.addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target, target)
	br := bufio.NewReader(conn)
	statusLine, err := br.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(statusLine, "200") {
		t.Fatalf("connect=%q", statusLine)
	}
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line == "\r\n" || line == "\n" {
			break
		}
	}
	var raw net.Conn = conn
	if br.Buffered() > 0 {
		raw = &prefixedConn{Conn: conn, r: io.MultiReader(bytes.NewReader(peek(br)), conn)}
	}
	tlsConn := tls.Client(raw, &tls.Config{RootCAs: h.roots, ServerName: "chatgpt.com"})
	if err := tlsConn.Handshake(); err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(tlsConn, "GET /backend-api/codex/responses HTTP/1.1\r\nHost: chatgpt.com\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: MDEyMzQ1Njc4OWFiY2RlZg==\r\nSec-WebSocket-Version: 13\r\n\r\n")
	rr := bufio.NewReader(tlsConn)
	status, err := rr.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(status, "101") {
		t.Fatalf("handshake status=%q", status)
	}
	for {
		line, err := rr.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line == "\r\n" || line == "\n" {
			break
		}
	}
	if err := tlsConn.SetDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	return tlsConn, rr
}

func TestIdleWebSocketsDoNotBlockNewTraffic(t *testing.T) {
	wsUpstream := newWSUpstream(t)
	defer wsUpstream.Close()
	httpUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, sseBody)
	}))
	defer httpUpstream.Close()
	h := startTestMeter(t, "stravia")
	defer h.close(t)
	target := strings.TrimPrefix(wsUpstream.URL, "https://")
	create := clientFrame(1, []byte(`{"type":"response.create","model":"gpt-6-luna","input":[]}`))
	complete := func(conn net.Conn, reader *bufio.Reader) {
		t.Helper()
		if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Write(create); err != nil {
			t.Fatal(err)
		}
		for i := range 2 {
			op, payload, err := readFrame(reader)
			if err != nil {
				t.Fatal(err)
			}
			var response struct {
				Type string `json:"type"`
			}
			if op != 1 || json.Unmarshal(payload, &response) != nil {
				t.Fatalf("invalid response frame: op=%d payload=%q", op, payload)
			}
			if i == 1 && response.Type != "response.completed" {
				t.Fatalf("terminal event=%q, want response.completed", response.Type)
			}
		}
		if err := conn.SetDeadline(time.Time{}); err != nil {
			t.Fatal(err)
		}
	}
	var first net.Conn
	var firstReader *bufio.Reader
	// 模拟连接池：响应完成后保持连接，不用关闭连接来给下一次请求腾名额。
	for i := range 64 {
		conn, reader := connectWS(t, h, target)
		complete(conn, reader)
		if i == 0 {
			first, firstReader = conn, reader
		}
	}
	client := proxyClient(h)
	client.Timeout = 5 * time.Second
	defer client.CloseIdleConnections()
	resp, err := client.Post(httpUpstream.URL+"/backend-api/codex/responses", "application/json", strings.NewReader(`{"model":"gpt-6-luna","input":[]}`))
	if err != nil {
		t.Fatalf("HTTP blocked by idle websocket pool: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || resp.StatusCode != http.StatusOK || !bytes.Contains(body, []byte(`"type":"response.completed"`)) {
		t.Fatalf("HTTP completion: status=%d body=%q error=%v", resp.StatusCode, body, err)
	}
	conn, reader := connectWS(t, h, target)
	complete(conn, reader)
	complete(first, firstReader)
}

func TestWebSocketResponseCreateAndTerminal(t *testing.T) {
	upstream := newWSUpstream(t)
	defer upstream.Close()
	target := strings.TrimPrefix(upstream.URL, "https://")

	h := startTestMeter(t, "sub2api")
	conn, rr := connectWS(t, h, target)
	blob := "BLOB-WS-12345678"
	create := `{"type":"response.create","model":"gpt-6-luna","input":[{"type":"message","role":"user","content":"hello"},{"type":"reasoning","encrypted_content":"` + blob + `"}],"reasoning":{"effort":"high","summary":"auto"},"include":["reasoning.encrypted_content"],"store":false,"prompt_cache_key":"pk"}`
	if _, err := conn.Write(clientFrame(1, []byte(create))); err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		op, payload, err := readFrame(rr)
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if op != 1 || len(payload) == 0 {
			t.Fatalf("frame %d op=%d len=%d", i, op, len(payload))
		}
	}
	conn.Write(clientFrame(8, nil))
	conn.Close()
	h.close(t)
	rows := h.events(t)

	msg := findEvent(t, rows, "ws-message", func(e map[string]any) bool {
		return e["from_client"] == true && e["model_request"] == true
	})
	if msg["transport"] != "ws" || msg["request_shape"] != "ok" || msg["type"] != "response.create" {
		t.Fatalf("client message=%v", msg)
	}
	if msg["input_reasoning_items"] != float64(1) || msg["input_reasoning_encrypted_items"] != float64(1) {
		t.Fatalf("reasoning=%v encrypted=%v", msg["input_reasoning_items"], msg["input_reasoning_encrypted_items"])
	}
	if lengths, ok := msg["input_encrypted_lengths"].([]any); !ok || len(lengths) != 1 || lengths[0] != float64(len(blob)) {
		t.Fatalf("lengths=%v", msg["input_encrypted_lengths"])
	}
	if msg["reasoning_effort"] != "high" || msg["include_reasoning_encrypted"] != true || msg["store"] != false {
		t.Fatalf("fields=%v", msg)
	}

	terminal := findEvent(t, rows, "ws-message", func(e map[string]any) bool {
		return e["from_client"] == false && e["response_terminal"] == terminalCompleted
	})
	if terminal["response_reasoning_items"] != float64(2) || terminal["usage_output_tokens"] != float64(9) || terminal["usage_cached_tokens"] != float64(3) || terminal["usage_reasoning_tokens"] != float64(5) {
		t.Fatalf("terminal=%v", terminal)
	}
	headers := findEvent(t, rows, "http-response-headers", nil)
	if headers["status"] != float64(101) || headers["status_class"] != "switching-protocols" {
		t.Fatalf("headers=%v", headers)
	}
}

// ---------- 隐私 ----------

func TestCredentialsAndContentNeverReachJSONL(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, sseBody)
	}))
	defer upstream.Close()

	h := startTestMeter(t, "stravia")
	body := []byte(httpRequestBody())
	postCodex(t, h, upstream.URL, "application/json", body, map[string]string{
		"Authorization":       "Bearer sk-live-SECRET-TOKEN",
		"Cookie":              "session=COOKIE-SECRET",
		"Chatgpt-Account-Id":  "acct-ACCOUNT-SECRET",
		"Proxy-Authorization": "Basic PROXY-SECRET",
	})
	h.close(t)
	raw := h.raw(t)
	for _, secret := range []string{"sk-live-SECRET-TOKEN", "COOKIE-SECRET", "acct-ACCOUNT-SECRET", "PROXY-SECRET", "query-secret", "CACHE-KEY-SECRET", "message body must not be recorded", encryptedBlob} {
		if bytes.Contains(raw, []byte(secret)) {
			t.Errorf("JSONL contains sensitive secret %q", secret)
		}
	}
	if !bytes.Contains(raw, []byte(`"input_encrypted_lengths":[18]`)) {
		t.Errorf("expected encrypted length to be recorded: %s", raw)
	}
	if bytes.Contains(raw, []byte("Bearer")) {
		t.Errorf("JSONL contains authorization scheme")
	}
}

// ---------- 纯解析回归 ----------

func TestDecodedStringLen(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int
	}{
		{`""`, 0},
		{`"abc"`, 3},
		{`"a\"b"`, 3},
		{`"\u0041"`, 1},
		{`"\u00e9"`, 2},
		{`"\ud83d\ude00"`, 4},
		{`"\\"`, 1},
		{`"a\nb"`, 3},
	} {
		if got := decodedStringLen([]byte(tc.in)); got != tc.want {
			t.Errorf("%s: %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestCountTopLevelItems(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int
	}{
		{`[]`, 0},
		{`[{}]`, 1},
		{`[{},{}]`, 2},
		{`[{"a":[1,2]},3]`, 2},
		{`["a,b"]`, 1},
		{`[{"s":"x,y"}, {"t":"[1]"}]`, 2},
	} {
		if got := countTopLevelItems([]byte(tc.in)); got != tc.want {
			t.Errorf("%s: %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestParseHTTPResponseUnknownWhenTerminalMissingUsage(t *testing.T) {
	st, kind := parseHTTPResponse("text/event-stream", []byte("data: {\"type\":\"response.created\",\"response\":{\"status\":\"in_progress\"}}\n\n"))
	if kind != "sse" || st.terminal != "" || st.reasoning != nil || st.input != nil {
		t.Fatalf("kind=%s stats=%+v", kind, st)
	}
	var ev = event{}
	applyResponseStats(ev, &st)
	if ev["response_terminal"] != nil || ev["usage_input_tokens"] != nil || ev["response_reasoning_items"] != nil {
		t.Fatalf("ev=%v", ev)
	}
}

func TestStatusClass(t *testing.T) {
	for status, want := range map[int]string{200: "ok", 302: "redirect", 400: "client-error", 429: "rate-limited", 500: "server-error", 503: "server-error"} {
		if got := statusClass(status); got != want {
			t.Errorf("%d: %s, want %s", status, got, want)
		}
	}
}

func TestFillResponseStatsTooLargeIsUnknown(t *testing.T) {
	ev := event{}
	fillResponseStats(ev, 200, "text/event-stream", "", nil, true, true)
	if ev["response_parse"] != "too-large" || ev["usage_input_tokens"] != nil {
		t.Fatalf("ev=%v", ev)
	}
	ev = event{}
	fillResponseStats(ev, 200, "application/octet-stream", "", nil, false, false)
	if ev["response_parse"] != "not-captured" || ev["usage_input_tokens"] != nil {
		t.Fatalf("ev=%v", ev)
	}
}

func TestInputStatsCountsEncryptedOnlyForReasoningItems(t *testing.T) {
	var stats inputStats
	body := []byte(`[{"type":"reasoning","encrypted_content":"12345"},{"type":"message","encrypted_content":"1234567"}]`)
	if err := json.Unmarshal(body, &stats); err != nil {
		t.Fatal(err)
	}
	if stats.count != 2 || stats.reasoning != 1 || stats.reasoningEncrypted != 1 || stats.encrypted != 2 {
		t.Fatalf("stats=%+v", stats)
	}
	if len(stats.lengths) != 1 || stats.lengths[0] != 5 {
		t.Fatalf("lengths=%v", stats.lengths)
	}
}

func TestMistypedFieldsStayUnknownWithoutLosingRequest(t *testing.T) {
	ev := event{}
	applyRequestShape(ev, []byte(`{"model":123,"store":"yes","reasoning":{"effort":7},"input":[{"type":"reasoning","encrypted_content":"abcd"}]}`))
	if ev["request_shape"] != "ok" {
		t.Fatalf("shape=%v", ev["request_shape"])
	}
	if ev["model"] != nil || ev["store"] != nil || ev["reasoning_effort"] != nil {
		t.Fatalf("mistyped fields should be null: %v %v %v", ev["model"], ev["store"], ev["reasoning_effort"])
	}
	if ev["input_reasoning_encrypted_items"] != 1 || ev["input_encrypted_items"] != 1 {
		t.Fatalf("input lost: %v", ev)
	}
}

func TestTerminalWithoutUsageOrOutputStaysUnknown(t *testing.T) {
	st, kind := parseHTTPResponse("text/event-stream", []byte("data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"))
	if kind != "sse" || st.terminal != terminalCompleted || st.status != "completed" {
		t.Fatalf("kind=%s stats=%+v", kind, st)
	}
	if st.reasoning != nil || st.input != nil || st.output != nil || st.cached != nil || st.reasoningTk != nil {
		t.Fatalf("missing fields must stay nil: %+v", st)
	}
}
