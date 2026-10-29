package main

// 用途：把 HTTP 请求/响应体按 Content-Encoding 解码成字节，供计量器做结构与计数解析。
// 输入：Content-Encoding 头值与原始负载字节（调用方已按上限截取）。
// 副作用：无文件写入、无网络、不解密任何载荷；失败只返回类别，不返回可能含正文的错误文本。
//
// 只支持评测链路上会出现的编码：identity、gzip、deflate（zlib 与裸 deflate）、zstd。
// 其他编码（br 等）明确返回 unknown，调用方据此把结构字段记为“未知”，不得用线上字节代替。

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"io"
	"strings"

	"github.com/klauspost/compress/zstd"
)

// 解码上限只约束计量解析，不影响转发；超限记为“未知”。
const decodedLimit = 32 << 20

// 解码失败类别是稳定字符串，便于汇总脚本按类别统计。
const (
	decodeOK            = ""
	decodeUnsupported   = "unsupported-encoding"
	decodeInvalid       = "invalid-compressed-body"
	decodeTooLarge      = "decoded-too-large"
	decodeMultiEncoding = "multiple-content-encodings"
)

// normalizeEncoding 返回小写编码名；多段编码（"gzip, zstd"）不在评测链路上，按不支持处理。
func normalizeEncoding(encoding string) (string, bool) {
	e := strings.ToLower(strings.TrimSpace(encoding))
	if e == "" || e == "identity" {
		return "", true
	}
	if strings.Contains(e, ",") {
		return e, false
	}
	switch e {
	case "gzip", "x-gzip", "deflate", "zstd":
		return e, true
	}
	return e, false
}

// decodeBody 解码失败时返回 nil 与类别；成功返回解码字节与空类别。
func decodeBody(encoding string, raw []byte) ([]byte, string) {
	enc, single := normalizeEncoding(encoding)
	if !single {
		if strings.Contains(enc, ",") {
			return nil, decodeMultiEncoding
		}
		return nil, decodeUnsupported
	}
	switch enc {
	case "":
		return raw, decodeOK
	case "gzip", "x-gzip":
		r, err := gzip.NewReader(bytes.NewReader(raw))
		if err != nil {
			return nil, decodeInvalid
		}
		defer r.Close()
		return readLimited(r)
	case "deflate":
		// HTTP 的 deflate 名义上是 zlib 包装；部分客户端发裸 deflate，两种都试。
		if r, err := zlib.NewReader(bytes.NewReader(raw)); err == nil {
			defer r.Close()
			if out, kind := readLimited(r); kind == decodeOK {
				return out, decodeOK
			}
		}
		r := flate.NewReader(bytes.NewReader(raw))
		defer r.Close()
		return readLimited(r)
	case "zstd":
		// 限制解码内存，避免压缩炸弹把计量进程拖垮；低内存模式与单并发足够计量吞吐。
		dec, err := zstd.NewReader(bytes.NewReader(raw),
			zstd.WithDecoderConcurrency(1),
			zstd.WithDecoderLowmem(true),
			zstd.WithDecoderMaxMemory(decodedLimit),
		)
		if err != nil {
			return nil, decodeInvalid
		}
		defer dec.Close()
		return readLimited(dec)
	}
	return nil, decodeUnsupported
}

func readLimited(r io.Reader) ([]byte, string) {
	out, err := io.ReadAll(io.LimitReader(r, decodedLimit+1))
	if err != nil {
		return nil, decodeInvalid
	}
	if len(out) > decodedLimit {
		return nil, decodeTooLarge
	}
	return out, decodeOK
}

func isJSONContentType(contentType string) bool {
	return strings.Contains(strings.ToLower(contentType), "json")
}
