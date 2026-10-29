package main

// 用途：从上游模型请求/响应里只提取结构与计数，供 JSONL 计量使用。
// 输入：已解码的请求体或响应体字节（可能是 SSE）。
// 副作用：无；不保留消息正文、工具 schema、encrypted_content 内容或任何请求头。
//
// 本批上游是 Codex Responses（HTTP POST 或 WebSocket 的 response.create），
// 因此请求侧关心：input 各类 item 计数、reasoning item 与其 encrypted_content 长度、
// previous_response_id、store、reasoning.effort/summary、include、tools、prompt_cache_key。
// 响应侧只关心终态事件的 status、output 里 reasoning item 数、usage 四类 token。
// 解析失败时字段写 null（未知），不写 0。

import (
	"bytes"
	"encoding/json"
	"strings"
	"unicode/utf8"
)

// 单个请求里最多记录多少个 encrypted_content 长度；超出只标记截断，不影响计数。
const encryptedLengthLimit = 512

// ---------- 请求侧 ----------

type requestEnvelope struct {
	Type             jsonString      `json:"type"`
	Model            jsonString      `json:"model"`
	PreviousResponse jsonPresence    `json:"previous_response_id"`
	Store            jsonBool        `json:"store"`
	PromptCacheKey   jsonPresence    `json:"prompt_cache_key"`
	Include          stringArray     `json:"include"`
	Reasoning        reasoningConfig `json:"reasoning"`
	Tools            arrayCount      `json:"tools"`
	Input            inputStats      `json:"input"`
}

type reasoningConfig struct {
	present bool
	effort  jsonString
	summary jsonString
}

func (r *reasoningConfig) UnmarshalJSON(b []byte) error {
	r.present = true
	t := bytes.TrimSpace(b)
	if len(t) == 0 || t[0] != '{' {
		return nil
	}
	var v struct {
		Effort  jsonString `json:"effort"`
		Summary jsonString `json:"summary"`
	}
	if err := json.Unmarshal(t, &v); err != nil {
		return nil
	}
	r.effort, r.summary = v.Effort, v.Summary
	return nil
}

// jsonString 只接受字符串 value；类型不符时字段记为未知，不拖垮整个请求的解析。
type jsonString struct {
	present bool
	valid   bool
	value   string
}

func (s *jsonString) UnmarshalJSON(b []byte) error {
	s.present = true
	t := bytes.TrimSpace(b)
	if len(t) < 2 || t[0] != '"' {
		return nil
	}
	var v string
	if err := json.Unmarshal(t, &v); err != nil {
		return nil
	}
	s.value, s.valid = v, true
	return nil
}

func (s jsonString) any() any {
	if !s.valid {
		return nil
	}
	return s.value
}

// jsonBool 只接受 true/false；其它类型记为未知。
type jsonBool struct {
	present bool
	valid   bool
	value   bool
}

func (b *jsonBool) UnmarshalJSON(raw []byte) error {
	b.present = true
	switch string(bytes.TrimSpace(raw)) {
	case "true":
		b.value, b.valid = true, true
	case "false":
		b.value, b.valid = false, true
	}
	return nil
}

func (b jsonBool) any() any {
	if !b.valid {
		return nil
	}
	return b.value
}

// jsonPresence 只回答“字段是否出现且非空”，不保留字符串值（prompt_cache_key 可能是标识符）。
type jsonPresence struct {
	present  bool
	nonEmpty bool
}

func (p *jsonPresence) UnmarshalJSON(b []byte) error {
	p.present = true
	t := bytes.TrimSpace(b)
	if len(t) == 0 || bytes.Equal(t, []byte("null")) {
		return nil
	}
	if t[0] == '"' {
		p.nonEmpty = decodedStringLen(t) > 0
		return nil
	}
	p.nonEmpty = true
	return nil
}

// stringArray 用于 include：需要区分“字段缺失”和“出现但为空”。
type stringArray struct {
	present bool
	valid   bool
	values  []string
}

func (a *stringArray) UnmarshalJSON(b []byte) error {
	a.present = true
	t := bytes.TrimSpace(b)
	if len(t) == 0 || t[0] != '[' {
		return nil
	}
	if err := json.Unmarshal(t, &a.values); err != nil {
		a.values = nil
		return nil
	}
	a.valid = true
	return nil
}

func (a *stringArray) has(value string) bool {
	for _, v := range a.values {
		if v == value {
			return true
		}
	}
	return false
}

// arrayCount 数数组元素，不保留元素内容。
type arrayCount struct {
	present bool
	form    string // "array" | "null" | "other"
	n       int
}

func (a *arrayCount) UnmarshalJSON(b []byte) error {
	a.present = true
	t := bytes.TrimSpace(b)
	switch {
	case len(t) == 0 || bytes.Equal(t, []byte("null")):
		a.form = "null"
	case t[0] != '[':
		a.form = "other"
	default:
		a.form = "array"
		a.n = countTopLevelItems(t)
	}
	return nil
}

// countTopLevelItems 在已确认是数组的字节上数顶层元素，跳过字符串与嵌套结构。
func countTopLevelItems(b []byte) int {
	if len(b) < 2 {
		return 0
	}
	depth, count := 0, 0
	quoted, escaped, item := false, false, false
	for _, ch := range b[1 : len(b)-1] {
		if quoted {
			if escaped {
				escaped = false
			} else if ch == '\\' {
				escaped = true
			} else if ch == '"' {
				quoted = false
			}
			continue
		}
		switch ch {
		case '"':
			quoted, item = true, true
		case '[', '{':
			depth, item = depth+1, true
		case ']', '}':
			depth--
		case ',':
			if depth == 0 {
				count, item = count+1, false
			}
		case ' ', '\t', '\r', '\n':
		default:
			item = true
		}
	}
	if item {
		count++
	}
	return count
}

// inputStats 统计 input 数组的 item 类型与 encrypted_content 长度。
// UnmarshalJSON 收到的字节指向解码器缓冲，不复制正文；错误不向上抛，避免一个坏 item 丢掉全部字段。
type inputStats struct {
	form               string
	count              int
	types              map[string]int
	reasoning          int
	reasoningEncrypted int
	encrypted          int
	lengths            []int
	truncated          bool
	malformed          bool
}

func (s *inputStats) UnmarshalJSON(b []byte) error {
	t := bytes.TrimSpace(b)
	switch {
	case len(t) == 0 || bytes.Equal(t, []byte("null")):
		s.form = "null"
		return nil
	case t[0] == '"':
		s.form = "string"
		return nil
	case t[0] != '[':
		s.form = "other"
		return nil
	}
	s.form = "array"
	s.types = make(map[string]int)
	dec := json.NewDecoder(bytes.NewReader(t))
	if _, err := dec.Token(); err != nil {
		s.malformed = true
		return nil
	}
	for dec.More() {
		var item inputItem
		if err := dec.Decode(&item); err != nil {
			s.malformed = true
			return nil
		}
		s.count++
		name := item.kind
		if name == "" {
			name = "untyped"
		}
		s.types[name]++
		if name == "reasoning" {
			s.reasoning++
		}
		if item.encrypted.present && item.encrypted.n > 0 {
			s.encrypted++
			// 规范里 encrypted_content 只出现在 reasoning item；长度分布只统计这一类。
			if name == "reasoning" {
				s.reasoningEncrypted++
				if len(s.lengths) < encryptedLengthLimit {
					s.lengths = append(s.lengths, item.encrypted.n)
				} else {
					s.truncated = true
				}
			}
		}
	}
	if _, err := dec.Token(); err != nil {
		s.malformed = true
	}
	return nil
}

// inputItem 接受对象、字符串、数组等任意 item 形态；只有对象带 type / encrypted_content。
type inputItem struct {
	kind      string
	encrypted jsonStringLen
}

func (it *inputItem) UnmarshalJSON(b []byte) error {
	t := bytes.TrimSpace(b)
	if len(t) == 0 {
		return nil
	}
	switch t[0] {
	case '{':
		var v struct {
			Type      *string       `json:"type"`
			Encrypted jsonStringLen `json:"encrypted_content"`
		}
		if err := json.Unmarshal(t, &v); err != nil {
			return err
		}
		if v.Type != nil {
			it.kind = *v.Type
		}
		it.encrypted = v.Encrypted
	case '"':
		it.kind = "string"
	case '[':
		it.kind = "array"
	case 'n':
		it.kind = "null"
	default:
		it.kind = "scalar"
	}
	return nil
}

// jsonStringLen 只算 JSON 字符串解码后的字节长度，不保留内容。
type jsonStringLen struct {
	present bool
	n       int
}

func (s *jsonStringLen) UnmarshalJSON(b []byte) error {
	t := bytes.TrimSpace(b)
	if len(t) < 2 || t[0] != '"' {
		return nil
	}
	s.present = true
	s.n = decodedStringLen(t)
	return nil
}

// decodedStringLen 计算带引号 JSON 字符串解码后的 UTF-8 字节数。
func decodedStringLen(t []byte) int {
	n := 0
	for i := 1; i < len(t)-1; {
		c := t[i]
		if c != '\\' {
			n++
			i++
			continue
		}
		i++
		if i >= len(t)-1 {
			break
		}
		if t[i] != 'u' {
			n++
			i++
			continue
		}
		r, _ := decodeUnicodeEscape(t, i+1)
		i += 5
		if r >= 0xD800 && r <= 0xDBFF && i+1 < len(t)-1 && t[i] == '\\' && t[i+1] == 'u' {
			low, _ := decodeUnicodeEscape(t, i+2)
			if low >= 0xDC00 && low <= 0xDFFF {
				n += 4
				i += 6
				continue
			}
		}
		switch {
		case r < 0x80:
			n++
		case r < 0x800:
			n += 2
		case r < 0x10000:
			n += 3 // 含单独代理：Go 解码时变成 U+FFFD
		default:
			n += 4
		}
	}
	return n
}

// decodeUnicodeEscape 读取 \u 后的 4 位十六进制（t[pos:] 起），返回码点与消费的字符数。
func decodeUnicodeEscape(t []byte, pos int) (rune, int) {
	if pos+4 > len(t) {
		return utf8.RuneError, 0
	}
	var r rune
	for i := range 4 {
		c := t[pos+i]
		r <<= 4
		switch {
		case c >= '0' && c <= '9':
			r |= rune(c - '0')
		case c >= 'a' && c <= 'f':
			r |= rune(c-'a') + 10
		case c >= 'A' && c <= 'F':
			r |= rune(c-'A') + 10
		default:
			return utf8.RuneError, 0
		}
	}
	return r, 4
}

// applyRequestShape 把请求结构写进事件。调用方只在解码成功且 body 是 JSON 对象时调用。
func applyRequestShape(ev event, body []byte) {
	var env requestEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		ev["request_shape"] = "invalid-json"
		return
	}
	ev["request_shape"] = "ok"
	ev["type"] = env.Type.any()
	ev["model"] = env.Model.any()
	ev["has_previous_response_id"] = triState(env.PreviousResponse)
	ev["store"] = env.Store.any()
	ev["has_prompt_cache_key"] = triState(env.PromptCacheKey)
	if env.Reasoning.present {
		ev["reasoning_effort"] = env.Reasoning.effort.any()
		ev["reasoning_summary"] = env.Reasoning.summary.any()
	} else {
		ev["reasoning_effort"] = nil
		ev["reasoning_summary"] = nil
	}
	if env.Include.valid {
		v := env.Include.has("reasoning.encrypted_content")
		ev["include_reasoning_encrypted"] = v
	} else {
		ev["include_reasoning_encrypted"] = nil
	}
	switch env.Tools.form {
	case "array":
		ev["tools_count"] = env.Tools.n
	case "null":
		ev["tools_count"] = 0
	default:
		ev["tools_count"] = nil
	}
	applyInputStats(ev, &env.Input)
}

func applyInputStats(ev event, s *inputStats) {
	form := s.form
	if form == "" {
		form = "absent"
	}
	ev["input_form"] = form
	switch form {
	case "array":
		if s.malformed {
			// 数组没完整走完：所有计数都不可信，统一记为未知。
			ev["input_items"] = nil
			ev["input_item_types"] = nil
			ev["input_reasoning_items"] = nil
			ev["input_reasoning_encrypted_items"] = nil
			ev["input_encrypted_items"] = nil
			ev["input_encrypted_lengths"] = nil
			ev["input_parse"] = "invalid"
			return
		}
		ev["input_items"] = s.count
		ev["input_item_types"] = s.types
		ev["input_reasoning_items"] = s.reasoning
		ev["input_reasoning_encrypted_items"] = s.reasoningEncrypted
		ev["input_encrypted_items"] = s.encrypted
		if s.reasoningEncrypted > 0 {
			ev["input_encrypted_lengths"] = s.lengths
			if s.truncated {
				ev["input_encrypted_lengths_truncated"] = true
			}
		} else {
			ev["input_encrypted_lengths"] = []int{}
		}
	default:
		ev["input_items"] = nil
		ev["input_item_types"] = nil
		ev["input_reasoning_items"] = nil
		ev["input_reasoning_encrypted_items"] = nil
		ev["input_encrypted_items"] = nil
		ev["input_encrypted_lengths"] = nil
	}
}

func triState(p jsonPresence) any {
	if !p.present {
		return nil
	}
	return p.nonEmpty
}

// ---------- 响应侧 ----------

type responseStats struct {
	events      int
	eventsValid bool
	terminal    string
	status      string
	reasoning   *int
	input       *int64
	cached      *int64
	output      *int64
	reasoningTk *int64
}

type terminalKind string

const (
	terminalCompleted  = "response.completed"
	terminalIncomplete = "response.incomplete"
	terminalFailed     = "response.failed"
	terminalDone       = "response.done"
)

func isTerminal(t string) bool {
	switch t {
	case terminalCompleted, terminalIncomplete, terminalFailed, terminalDone:
		return true
	}
	return false
}

type streamEnvelope struct {
	Type     string           `json:"type"`
	Response terminalResponse `json:"response"`
}

type terminalResponse struct {
	Status *string `json:"status"`
	Output []struct {
		Type string `json:"type"`
	} `json:"output"`
	Usage *struct {
		InputTokens        *int64 `json:"input_tokens"`
		InputTokensDetails *struct {
			CachedTokens *int64 `json:"cached_tokens"`
		} `json:"input_tokens_details"`
		OutputTokens        *int64 `json:"output_tokens"`
		OutputTokensDetails *struct {
			ReasoningTokens *int64 `json:"reasoning_tokens"`
		} `json:"output_tokens_details"`
	} `json:"usage"`
}

// parseHTTPResponse 返回响应侧统计与解析类别。
// 类别：sse / json / not-event-stream / empty；压缩或超限由调用方先处理。
func parseHTTPResponse(contentType string, body []byte) (responseStats, string) {
	var st responseStats
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return st, "empty"
	}
	if strings.Contains(strings.ToLower(contentType), "text/event-stream") ||
		bytes.HasPrefix(trimmed, []byte("data:")) || bytes.HasPrefix(trimmed, []byte("event:")) {
		st.eventsValid = true
		st.events = scanSSE(trimmed, func(data []byte) {
			applyStreamPayload(&st, data)
		})
		return st, "sse"
	}
	if trimmed[0] == '{' {
		applyTerminalJSON(&st, trimmed)
		return st, "json"
	}
	return st, "not-event-stream"
}

// scanSSE 逐行扫描已解码的 SSE 正文；只把 data 负载交给回调，不复制整段正文。
func scanSSE(body []byte, handle func(data []byte)) int {
	events := 0
	var data bytes.Buffer
	for len(body) > 0 {
		var line []byte
		if i := bytes.IndexByte(body, '\n'); i >= 0 {
			line, body = body[:i], body[i+1:]
		} else {
			line, body = body, nil
		}
		line = bytes.TrimSuffix(line, []byte("\r"))
		if len(line) == 0 {
			if data.Len() > 0 {
				handle(data.Bytes())
				events++
			}
			data.Reset()
			continue
		}
		if line[0] == ':' {
			continue
		}
		if v, ok := bytes.CutPrefix(line, []byte("data:")); ok {
			v = bytes.TrimPrefix(v, []byte(" "))
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.Write(v)
		}
	}
	if data.Len() > 0 {
		handle(data.Bytes())
		events++
	}
	return events
}

// terminalMarker 只做廉价的子串预筛，真正的类型判断仍交给 JSON 解析。
func terminalMarker(data []byte) string {
	for _, kind := range []string{terminalCompleted, terminalIncomplete, terminalFailed, terminalDone} {
		if bytes.Contains(data, []byte(`"`+kind+`"`)) {
			return kind
		}
	}
	return ""
}

// applyStreamPayload 只对包含终态关键字的负载做 JSON 解析，避免每段 delta 都分配。
func applyStreamPayload(st *responseStats, data []byte) {
	if terminalMarker(data) == "" {
		return
	}
	var env streamEnvelope
	if err := json.Unmarshal(data, &env); err != nil || !isTerminal(env.Type) {
		return
	}
	applyTerminal(st, env.Type, &env.Response)
}

// wsTerminalStats 处理 WebSocket 服务端消息：只有终态事件返回统计，其余返回 nil。
func wsTerminalStats(content []byte) *responseStats {
	if terminalMarker(content) == "" {
		return nil
	}
	var env streamEnvelope
	if err := json.Unmarshal(content, &env); err != nil || !isTerminal(env.Type) {
		return nil
	}
	st := &responseStats{}
	applyTerminal(st, env.Type, &env.Response)
	return st
}

func applyTerminalJSON(st *responseStats, body []byte) {
	var env streamEnvelope
	if err := json.Unmarshal(body, &env); err == nil && isTerminal(env.Type) {
		applyTerminal(st, env.Type, &env.Response)
		return
	}
	// 非流式响应体本身就是 response 对象，没有外层 type；错误 JSON 没有这些字段，保持未知。
	var resp terminalResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return
	}
	if resp.Status == nil && resp.Output == nil && resp.Usage == nil {
		return
	}
	applyTerminal(st, "response", &resp)
}

func applyTerminal(st *responseStats, kind string, resp *terminalResponse) {
	st.terminal = kind
	if resp.Status != nil {
		st.status = *resp.Status
	}
	if resp.Output != nil {
		n := 0
		for _, item := range resp.Output {
			if item.Type == "reasoning" {
				n++
			}
		}
		st.reasoning = &n
	}
	if u := resp.Usage; u != nil {
		st.input = u.InputTokens
		st.output = u.OutputTokens
		if u.InputTokensDetails != nil {
			st.cached = u.InputTokensDetails.CachedTokens
		}
		if u.OutputTokensDetails != nil {
			st.reasoningTk = u.OutputTokensDetails.ReasoningTokens
		}
	}
}

func applyResponseStats(ev event, st *responseStats) {
	if st.eventsValid {
		ev["response_events"] = st.events
	}
	if st.terminal == "" {
		ev["response_terminal"] = nil
		ev["response_status"] = nil
	} else {
		ev["response_terminal"] = st.terminal
		if st.status == "" {
			ev["response_status"] = nil
		} else {
			ev["response_status"] = st.status
		}
	}
	ev["response_reasoning_items"] = pointerOrNil(st.reasoning)
	ev["usage_input_tokens"] = pointerOrNil(st.input)
	ev["usage_cached_tokens"] = pointerOrNil(st.cached)
	ev["usage_output_tokens"] = pointerOrNil(st.output)
	ev["usage_reasoning_tokens"] = pointerOrNil(st.reasoningTk)
}

// pointerOrNil 把 nil 指针写成 interface nil，保证 JSON 里是 null 而不是带类型的空值。
func pointerOrNil[T any](p *T) any {
	if p == nil {
		return nil
	}
	return *p
}

// statusClass 把状态码归到汇总用的固定类别；429 单独一类，避免与其它 4xx 混在一起。
func statusClass(status int) string {
	switch {
	case status == 101:
		return "switching-protocols"
	case status == 429:
		return "rate-limited"
	case status >= 500 && status <= 599:
		return "server-error"
	case status >= 400 && status <= 499:
		return "client-error"
	case status >= 200 && status <= 299:
		return "ok"
	case status >= 300 && status <= 399:
		return "redirect"
	default:
		return "other"
	}
}

// fillResponseStats 在写出协程里解析响应副本；失败类别写进 response_parse，统计字段保持 null。
func fillResponseStats(ev event, status int, contentType, contentEncoding string, body []byte, truncated, captured bool) {
	if !captured {
		ev["response_parse"] = "not-captured"
		return
	}
	if truncated {
		ev["response_parse"] = "too-large"
		return
	}
	decoded, kind := decodeBody(contentEncoding, body)
	if kind != decodeOK {
		ev["response_parse"] = kind
		return
	}
	st, parseKind := parseHTTPResponse(contentType, decoded)
	ev["response_parse"] = parseKind
	applyResponseStats(ev, &st)
}
