package main

// 用途：meterd 转发骨架的单元回归：请求体计数/截断、错误分类、事件写出、
// WebSocket 分片与 permessage-deflate 重组。
// 输入：无外部数据。副作用：只在 t.TempDir() 下写事件文件。

import (
	"bytes"
	"compress/flate"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBodyCloseBeforeReadKnownEmptyVersusIncomplete(t *testing.T) {
	for _, tc := range []struct {
		name           string
		knownEmpty     bool
		wantIncomplete bool
	}{
		{"declared-zero-length", true, false},
		{"unread-positive-length", false, true},
		{"unread-chunked-or-unknown-length", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			var b *meteredBody
			b = &meteredBody{ReadCloser: io.NopCloser(strings.NewReader("")), knownEmpty: tc.knownEmpty,
				finish: func(n int64, body []byte) {
					calls++
					if n != 0 || len(body) != 0 {
						t.Error("nonzero empty-body accounting")
					}
					if errors.Is(b.err, io.ErrUnexpectedEOF) != tc.wantIncomplete {
						t.Errorf("completion error=%v", b.err)
					}
				}}
			if err := b.Close(); err != nil {
				t.Fatal(err)
			}
			if err := b.Close(); err != nil {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Fatalf("completion events=%d", calls)
			}
		})
	}
}

func TestShutdownCancellationClassificationPreservesOtherErrors(t *testing.T) {
	m := &meter{stop: make(chan struct{})}
	close(m.stop)
	for _, tc := range []struct {
		err  error
		kind string
	}{
		{net.ErrClosed, "shutdown-cancellation"},
		{context.Canceled, "shutdown-cancellation"},
		{io.ErrUnexpectedEOF, "incomplete-stream"},
		{os.ErrPermission, "transport-error"},
	} {
		ev := event{"error": tc.err.Error()}
		m.classifyError(ev, tc.err)
		if ev["error_kind"] != tc.kind || ev["error"] != tc.err.Error() {
			t.Fatalf("classification=%#v", ev)
		}
	}
}

func TestEmitterDrainsAndReportsWriteFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	e, err := newEmitter(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 20 {
		e.emit(event{"event": "test", "index": i})
	}
	if err := e.drain(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := bytes.Count(b, []byte{'\n'}); got != 20 {
		t.Fatalf("events=%d, want 20", got)
	}
	if _, err := os.Stat("/dev/full"); err != nil {
		t.Skip("/dev/full unavailable")
	}
	failed, err := newEmitter("/dev/full")
	if err != nil {
		t.Fatal(err)
	}
	failed.emit(event{"event": "test"})
	if err := failed.drain(); err == nil {
		t.Fatal("output failure silently succeeded")
	}
}

func maskedTestFrame(op byte, final, compressed bool, body []byte) []byte {
	first := op
	if final {
		first |= 0x80
	}
	if compressed {
		first |= 0x40
	}
	b := []byte{first, 0x80}
	if len(body) < 126 {
		b[1] |= byte(len(body))
	} else {
		b[1] |= 126
		b = binary.BigEndian.AppendUint16(b, uint16(len(body)))
	}
	mask := [4]byte{3, 11, 19, 27}
	b = append(b, mask[:]...)
	for i, v := range body {
		b = append(b, v^mask[i&3])
	}
	return b
}

func TestWebsocketMaskedFragmentedDeflateContextTakeover(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	e, err := newEmitter(path)
	if err != nil {
		t.Fatal(err)
	}
	p := &wsParser{m: &meter{route: "test", emit: e}, flow: "flow", fromClient: true, deflate: true}
	var compressed bytes.Buffer
	compressor, err := flate.NewWriter(&compressed, flate.DefaultCompression)
	if err != nil {
		t.Fatal(err)
	}
	bodies := []string{`{"type":"first","input":["` + strings.Repeat("context", 1000) + `",{}]}`, `{"type":"second","input":["` + strings.Repeat("context", 1000) + `"]}`}
	var wireSizes []int
	for _, body := range bodies {
		compressed.Reset()
		if _, err := io.WriteString(compressor, body); err != nil {
			t.Fatal(err)
		}
		if err := compressor.Flush(); err != nil {
			t.Fatal(err)
		}
		payload := compressed.Bytes()
		if len(payload) < 4 {
			t.Fatal("missing deflate flush marker")
		}
		payload = payload[:len(payload)-4]
		wireSizes = append(wireSizes, len(payload))
		cut := len(payload) / 2
		frames := maskedTestFrame(1, false, true, payload[:cut])
		frames = append(frames, maskedTestFrame(9, true, false, []byte("ping"))...)
		frames = append(frames, maskedTestFrame(0, true, false, payload[cut:])...)
		for _, v := range frames {
			p.feed([]byte{v})
		}
	}
	compressor.Close()
	p.end()
	if err := e.drain(); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	for i, body := range bodies {
		var ev map[string]any
		if err := dec.Decode(&ev); err != nil {
			t.Fatal(err)
		}
		if ev["event"] != "ws-message" || ev["payload_bytes"] != float64(len(body)) || ev["wire_payload_bytes"] != float64(wireSizes[i]) || ev["input_items"] != float64(2-i) {
			t.Fatalf("message %d: %#v", i, ev)
		}
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		t.Fatalf("unexpected extra event: %v %#v", err, extra)
	}
}

func TestMeteredBodyFinalBytesAndCaptureLimit(t *testing.T) {
	for _, size := range []int{7, captureLimit + 1} {
		calls := 0
		var b *meteredBody
		b = &meteredBody{ReadCloser: io.NopCloser(strings.NewReader(strings.Repeat("x", size))), keep: true, finish: func(n int64, body []byte) {
			calls++
			if n != int64(size) {
				t.Errorf("bytes=%d, want %d", n, size)
			}
			if size <= captureLimit && len(body) != size {
				t.Errorf("capture=%d", len(body))
			}
			if size > captureLimit && (!b.truncated || len(body) != 0) {
				t.Error("oversized capture not marked")
			}
		}}
		if _, err := io.Copy(io.Discard, b); err != nil {
			t.Fatal(err)
		}
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
		if calls != 1 {
			t.Fatalf("completion events=%d", calls)
		}
	}
}
