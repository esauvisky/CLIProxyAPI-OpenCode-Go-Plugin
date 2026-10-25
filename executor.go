package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type rpcExecutorRequest struct {
	pluginapi.ExecutorRequest
	StreamID       string `json:"stream_id,omitempty"`
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

type rpcExecutorHTTPRequest struct {
	pluginapi.ExecutorHTTPRequest
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

type rpcHostHTTPStreamResponse struct {
	StatusCode int         `json:"status_code"`
	Headers    http.Header `json:"headers,omitempty"`
	StreamID   string      `json:"stream_id,omitempty"`
}

type rpcHostHTTPStreamReadResponse struct {
	Payload []byte `json:"payload,omitempty"`
	Error   string `json:"error,omitempty"`
	Done    bool   `json:"done,omitempty"`
}

type rpcStreamEmitRequest struct {
	StreamID string `json:"stream_id"`
	Payload  []byte `json:"payload,omitempty"`
	Error    string `json:"error,omitempty"`
}

type rpcStreamCloseRequest struct {
	StreamID string `json:"stream_id"`
	Error    string `json:"error,omitempty"`
}

func handleExecutorExecute(raw []byte) ([]byte, error) {
	var req rpcExecutorRequest
	if errUnmarshal := json.Unmarshal(raw, &req); errUnmarshal != nil {
		return nil, errUnmarshal
	}
	resp, errExec := executeNonStream(context.Background(), req)
	if errExec != nil {
		return nil, errExec
	}
	return okEnvelope(resp)
}

func handleExecutorExecuteStream(raw []byte) ([]byte, error) {
	var req rpcExecutorRequest
	if errUnmarshal := json.Unmarshal(raw, &req); errUnmarshal != nil {
		return nil, errUnmarshal
	}
	streamID := strings.TrimSpace(req.StreamID)
	if streamID == "" {
		return nil, fmt.Errorf("stream_id is required for executor.execute_stream")
	}

	upstreamStream, prepared, errStart := openUpstreamStream(context.Background(), req)
	if errStart != nil {
		return nil, errStart
	}

	go forwardUpstreamStream(req.HostCallbackID, req.AuthID, prepared, upstreamStream.StreamID, streamID)

	return okEnvelope(map[string]any{
		"headers": http.Header{
			"Content-Type": []string{"text/event-stream; charset=utf-8"},
		},
	})
}

func handleExecutorCountTokens(_ []byte) ([]byte, error) {
	return okEnvelope(pluginapi.ExecutorResponse{
		Payload: []byte(`{"input_tokens":0,"total_tokens":0}`),
		Headers: http.Header{"Content-Type": []string{"application/json"}},
	})
}

func handleExecutorHTTPRequest(raw []byte) ([]byte, error) {
	var req rpcExecutorHTTPRequest
	if errUnmarshal := json.Unmarshal(raw, &req); errUnmarshal != nil {
		return nil, errUnmarshal
	}
	token, orgID := extractAuthCredentials(req.StorageJSON, req.Attributes)
	headers := cloneHTTPHeader(req.Headers)
	if headers == nil {
		headers = make(http.Header)
	}
	if token != "" && headers.Get("Authorization") == "" {
		headers.Set("Authorization", "Bearer "+token)
	}
	if orgID != "" && headers.Get("x-opencode-org-id") == "" {
		headers.Set("x-opencode-org-id", orgID)
	}
	if headers.Get("x-opencode-session") == "" {
		headers.Set("x-opencode-session", resolveSessionHeader(req.Headers, nil))
	}
	resp, errHTTP := doHostHTTP(req.HostCallbackID, req.Method, req.URL, headers, req.Body)
	if errHTTP != nil {
		return nil, errHTTP
	}
	return okEnvelope(pluginapi.ExecutorHTTPResponse{
		StatusCode: resp.StatusCode,
		Headers:    resp.Headers,
		Body:       resp.Body,
	})
}

type preparedUpstreamCall struct {
	baseModel       string
	suffix          string
	hasSuffix       bool
	spec            goModelSpec
	clientFormat    sdktranslator.Format
	wireFormat      sdktranslator.Format
	originalRequest []byte
	translatedReq   []byte
	upstreamURL     string
	upstreamHeaders http.Header
}

func prepareUpstreamRequest(req rpcExecutorRequest, stream bool) (preparedUpstreamCall, error) {
	baseModel, suffix, hasSuffix := splitModelThinkingSuffix(strings.TrimSpace(req.Model))
	baseModel = strings.TrimPrefix(baseModel, "models/")
	if idx := strings.LastIndex(baseModel, "/"); idx >= 0 {
		baseModel = baseModel[idx+1:]
	}
	spec := lookupModelSpec(baseModel)

	clientFormatStr := strings.ToLower(strings.TrimSpace(firstNonEmpty(req.Format, req.SourceFormat, "openai")))
	clientFormat := normalizeTranslatorFormat(clientFormatStr)
	wireFormat := normalizeTranslatorFormat(string(spec.Protocol))

	sourcePayload := req.Payload
	if len(sourcePayload) == 0 {
		sourcePayload = req.OriginalRequest
	}
	originalRequest := req.OriginalRequest
	if len(originalRequest) == 0 {
		originalRequest = sourcePayload
	}

	translated := bytes.Clone(sourcePayload)
	if clientFormat != wireFormat {
		env := sdktranslator.TranslateRequestEnvelope(context.Background(), clientFormat, wireFormat, sdktranslator.RequestEnvelope{
			Format: clientFormat,
			Model:  baseModel,
			Stream: stream,
			Body:   sourcePayload,
		})
		if env.Err != nil {
			return preparedUpstreamCall{}, env.Err
		}
		translated = env.Body
	}

	translated = finalizeWirePayload(translated, spec, baseModel, suffix, hasSuffix, stream)

	token, orgID := extractAuthCredentials(req.StorageJSON, req.AuthAttributes)
	if token == "" {
		return preparedUpstreamCall{}, statusError{
			code:       "unauthorized",
			message:    "OpenCode Go credential has no access token or API key",
			httpStatus: http.StatusUnauthorized,
		}
	}

	cfg := currentConfig()
	upstreamURL := buildUpstreamEndpointURL(cfg.InferenceBaseURL, spec.Protocol, baseModel, stream)

	headers := http.Header{
		"Authorization":      []string{"Bearer " + token},
		"Content-Type":       []string{"application/json"},
		"User-Agent":         []string{cfg.UserAgent},
		"x-opencode-session": []string{resolveSessionHeader(req.Headers, req.Metadata)},
	}
	if orgID != "" {
		headers.Set("x-opencode-org-id", orgID)
	}
	if stream {
		headers.Set("Accept", "text/event-stream")
	} else {
		headers.Set("Accept", "application/json")
	}
	if spec.Protocol == wireProtocolAnthropic {
		headers.Set("anthropic-version", "2023-06-01")
		headers.Set("anthropic-beta", "interleaved-thinking-2025-05-14")
	}

	return preparedUpstreamCall{
		baseModel:       baseModel,
		suffix:          suffix,
		hasSuffix:       hasSuffix,
		spec:            spec,
		clientFormat:    clientFormat,
		wireFormat:      wireFormat,
		originalRequest: originalRequest,
		translatedReq:   translated,
		upstreamURL:     upstreamURL,
		upstreamHeaders: headers,
	}, nil
}

func finalizeWirePayload(payload []byte, spec goModelSpec, baseModel, suffix string, hasSuffix, stream bool) []byte {
	out := bytes.Clone(payload)
	switch spec.Protocol {
	case wireProtocolChatCompletions:
		out, _ = sjson.SetBytes(out, "model", baseModel)
		out, _ = sjson.SetBytes(out, "stream", stream)
		if stream {
			out, _ = sjson.SetBytes(out, "stream_options.include_usage", true)
		} else {
			out, _ = sjson.DeleteBytes(out, "stream_options")
		}
		if hasSuffix {
			out = applyChatThinkingSuffix(out, spec, suffix)
		}
	case wireProtocolResponses:
		out, _ = sjson.SetBytes(out, "model", baseModel)
		out, _ = sjson.SetBytes(out, "stream", stream)
		out, _ = sjson.DeleteBytes(out, "previous_response_id")
		out, _ = sjson.DeleteBytes(out, "prompt_cache_retention")
		if hasSuffix {
			out = applyResponsesThinkingSuffix(out, spec, suffix)
		}
	case wireProtocolAnthropic:
		out, _ = sjson.SetBytes(out, "model", baseModel)
		out, _ = sjson.SetBytes(out, "stream", stream)
		if !gjson.GetBytes(out, "max_tokens").Exists() {
			maxTok := spec.MaxOutput
			if maxTok <= 0 || maxTok > 64000 {
				maxTok = 32000
			}
			out, _ = sjson.SetBytes(out, "max_tokens", maxTok)
		}
		if hasSuffix {
			out = applyAnthropicThinkingSuffix(out, spec, suffix)
		}
	case wireProtocolGemini:
		out, _ = sjson.DeleteBytes(out, "model")
		if hasSuffix {
			out = applyGeminiThinkingSuffix(out, spec, suffix)
		}
	}
	return out
}

func applyChatThinkingSuffix(payload []byte, spec goModelSpec, suffix string) []byte {
	s := strings.ToLower(strings.TrimSpace(suffix))
	if s == "" {
		return payload
	}
	if s == "none" || s == "off" || s == "0" || s == "false" {
		if containsLevel(spec.Levels, "none") {
			out, _ := sjson.SetBytes(payload, "reasoning_effort", "none")
			return out
		}
		out, _ := sjson.DeleteBytes(payload, "reasoning_effort")
		return out
	}
	level := clampSupportedLevel(s, spec.Levels)
	if level != "" {
		out, _ := sjson.SetBytes(payload, "reasoning_effort", level)
		return out
	}
	return payload
}

func applyResponsesThinkingSuffix(payload []byte, spec goModelSpec, suffix string) []byte {
	s := strings.ToLower(strings.TrimSpace(suffix))
	if s == "" {
		return payload
	}
	if s == "none" || s == "off" || s == "0" || s == "false" {
		if containsLevel(spec.Levels, "none") {
			out, _ := sjson.SetBytes(payload, "reasoning.effort", "none")
			return out
		}
		out, _ := sjson.DeleteBytes(payload, "reasoning")
		return out
	}
	level := clampSupportedLevel(s, spec.Levels)
	if level == "" {
		level = s
	}
	out, _ := sjson.SetBytes(payload, "reasoning.effort", level)
	return out
}

func applyAnthropicThinkingSuffix(payload []byte, spec goModelSpec, suffix string) []byte {
	s := strings.ToLower(strings.TrimSpace(suffix))
	if s == "" {
		return payload
	}
	if s == "none" || s == "off" || s == "0" || s == "false" {
		out, _ := sjson.SetBytes(payload, "thinking", map[string]any{"type": "disabled"})
		return out
	}
	budget := 16384
	if parsed, errInt := strconv.Atoi(s); errInt == nil && parsed > 0 {
		budget = parsed
	} else {
		switch s {
		case "minimal", "low":
			budget = 4096
		case "medium", "auto":
			budget = 16384
		case "high":
			budget = 32768
		case "xhigh", "max", "ultra":
			if spec.MaxBudget > 0 {
				budget = spec.MaxBudget / 2
			} else {
				budget = 65536
			}
		}
	}
	if spec.MaxBudget > 0 && budget > spec.MaxBudget {
		budget = spec.MaxBudget
	}
	maxTokens := int(gjson.GetBytes(payload, "max_tokens").Int())
	if maxTokens <= budget {
		maxTokens = budget + 8192
		if spec.MaxOutput > 0 && int64(maxTokens) > spec.MaxOutput {
			maxTokens = int(spec.MaxOutput)
			if budget >= maxTokens && maxTokens > 2048 {
				budget = maxTokens - 1024
			}
		}
		payload, _ = sjson.SetBytes(payload, "max_tokens", maxTokens)
	}
	out, _ := sjson.SetBytes(payload, "thinking", map[string]any{
		"type":          "enabled",
		"budget_tokens": budget,
	})
	return out
}

func applyGeminiThinkingSuffix(payload []byte, _ goModelSpec, suffix string) []byte {
	s := strings.ToLower(strings.TrimSpace(suffix))
	if s == "" {
		return payload
	}
	if s == "none" || s == "off" || s == "0" || s == "false" {
		out, _ := sjson.SetBytes(payload, "generationConfig.thinkingConfig.thinkingBudget", 0)
		return out
	}
	if parsed, errInt := strconv.Atoi(s); errInt == nil && parsed > 0 {
		out, _ := sjson.SetBytes(payload, "generationConfig.thinkingConfig.thinkingBudget", parsed)
		return out
	}
	level := strings.ToUpper(s)
	switch level {
	case "LOW", "MEDIUM", "HIGH":
		out, _ := sjson.SetBytes(payload, "generationConfig.thinkingConfig.thinkingLevel", level)
		return out
	case "MINIMAL":
		out, _ := sjson.SetBytes(payload, "generationConfig.thinkingConfig.thinkingLevel", "LOW")
		return out
	default:
		out, _ := sjson.SetBytes(payload, "generationConfig.thinkingConfig.thinkingLevel", "HIGH")
		return out
	}
}

func clampSupportedLevel(requested string, supported []string) string {
	if len(supported) == 0 {
		return requested
	}
	for _, lvl := range supported {
		if strings.EqualFold(lvl, requested) {
			return lvl
		}
	}
	order := []string{"none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra"}
	reqRank := -1
	for i, name := range order {
		if name == requested {
			reqRank = i
			break
		}
	}
	if reqRank < 0 {
		return supported[len(supported)-1]
	}
	best := supported[len(supported)-1]
	bestDist := 999
	for _, cand := range supported {
		for i, name := range order {
			if strings.EqualFold(cand, name) {
				dist := i - reqRank
				if dist < 0 {
					dist = -dist
				}
				if dist < bestDist {
					bestDist = dist
					best = cand
				}
			}
		}
	}
	return best
}

func containsLevel(levels []string, target string) bool {
	for _, l := range levels {
		if strings.EqualFold(l, target) {
			return true
		}
	}
	return false
}

func buildUpstreamEndpointURL(baseURL string, proto wireProtocol, modelID string, stream bool) string {
	baseURL = strings.TrimRight(baseURL, "/")
	switch proto {
	case wireProtocolResponses:
		return baseURL + "/openai/v1/responses"
	case wireProtocolAnthropic:
		return baseURL + "/anthropic/v1/messages"
	case wireProtocolGemini:
		action := "generateContent"
		if stream {
			action = "streamGenerateContent?alt=sse"
		}
		return fmt.Sprintf("%s/google/v1beta/models/%s:%s", baseURL, modelID, action)
	default:
		return baseURL + "/openai/v1/chat/completions"
	}
}

func executeNonStream(ctx context.Context, req rpcExecutorRequest) (pluginapi.ExecutorResponse, error) {
	prepared, errPrep := prepareUpstreamRequest(req, false)
	if errPrep != nil {
		return pluginapi.ExecutorResponse{}, errPrep
	}
	httpResp, errHTTP := doHostHTTP(req.HostCallbackID, http.MethodPost, prepared.upstreamURL, prepared.upstreamHeaders, prepared.translatedReq)
	if errHTTP != nil {
		return pluginapi.ExecutorResponse{}, errHTTP
	}
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		return pluginapi.ExecutorResponse{}, statusError{
			code:       "upstream_error",
			message:    extractUpstreamErrorMessage(httpResp.StatusCode, httpResp.Body),
			httpStatus: httpResp.StatusCode,
		}
	}

	recordOpenCodeUsagePayload(req.AuthID, prepared.baseModel, prepared.spec.Protocol, httpResp.Body)

	outPayload := bytes.Clone(httpResp.Body)
	if prepared.wireFormat != prepared.clientFormat {
		var param any
		outPayload = sdktranslator.TranslateNonStream(
			ctx,
			prepared.wireFormat,
			prepared.clientFormat,
			prepared.baseModel,
			prepared.originalRequest,
			prepared.translatedReq,
			httpResp.Body,
			&param,
		)
	}
	return pluginapi.ExecutorResponse{
		Payload: outPayload,
		Headers: httpResp.Headers,
	}, nil
}

func openUpstreamStream(_ context.Context, req rpcExecutorRequest) (rpcHostHTTPStreamResponse, preparedUpstreamCall, error) {
	prepared, errPrep := prepareUpstreamRequest(req, true)
	if errPrep != nil {
		return rpcHostHTTPStreamResponse{}, preparedUpstreamCall{}, errPrep
	}
	payload := map[string]any{
		"host_callback_id": req.HostCallbackID,
		"method":           http.MethodPost,
		"url":              prepared.upstreamURL,
		"headers":          prepared.upstreamHeaders,
		"body":             prepared.translatedReq,
	}
	raw, errCall := callHost(pluginabi.MethodHostHTTPDoStream, payload)
	if errCall != nil {
		return rpcHostHTTPStreamResponse{}, preparedUpstreamCall{}, errCall
	}
	var streamResp rpcHostHTTPStreamResponse
	if errUnmarshal := json.Unmarshal(raw, &streamResp); errUnmarshal != nil {
		return rpcHostHTTPStreamResponse{}, preparedUpstreamCall{}, errUnmarshal
	}
	if streamResp.StatusCode < 200 || streamResp.StatusCode >= 300 {
		errBody := readAndCloseErrorStream(streamResp.StreamID)
		return rpcHostHTTPStreamResponse{}, preparedUpstreamCall{}, statusError{
			code:       "upstream_error",
			message:    extractUpstreamErrorMessage(streamResp.StatusCode, errBody),
			httpStatus: streamResp.StatusCode,
		}
	}
	if strings.TrimSpace(streamResp.StreamID) == "" {
		return rpcHostHTTPStreamResponse{}, preparedUpstreamCall{}, fmt.Errorf("upstream stream opened with empty stream_id")
	}
	return streamResp, prepared, nil
}

func forwardUpstreamStream(_ string, authID string, prepared preparedUpstreamCall, httpStreamID, pluginStreamID string) {
	defer closeHostHTTPStream(httpStreamID)

	ctx := context.Background()
	needsTranslation := prepared.wireFormat != prepared.clientFormat
	var param any
	var pending []byte
	var streamUsageObserved openCodeUsageSample

	emitChunk := func(chunk []byte) bool {
		if len(chunk) == 0 {
			return true
		}
		_, errEmit := callHost(pluginabi.MethodHostStreamEmit, rpcStreamEmitRequest{
			StreamID: pluginStreamID,
			Payload:  chunk,
		})
		return errEmit == nil
	}

	processLine := func(line []byte) bool {
		trimmed := bytes.TrimRight(line, "\r\n")
		observeOpenCodeStreamLineUsage(prepared.spec.Protocol, trimmed, &streamUsageObserved)
		if !needsTranslation {
			return emitChunk(line)
		}
		if len(bytes.TrimSpace(trimmed)) == 0 {
			return true
		}
		frames := sdktranslator.TranslateStream(
			ctx,
			prepared.wireFormat,
			prepared.clientFormat,
			prepared.baseModel,
			prepared.originalRequest,
			prepared.translatedReq,
			trimmed,
			&param,
		)
		for _, frame := range frames {
			if len(frame) == 0 {
				continue
			}
			if !bytes.HasSuffix(frame, []byte("\n\n")) {
				if bytes.HasSuffix(frame, []byte("\n")) {
					frame = append(frame, '\n')
				} else {
					frame = append(frame, '\n', '\n')
				}
			}
			if !emitChunk(frame) {
				return false
			}
		}
		return true
	}

	var streamErr string
	for {
		rawRead, errCall := callHost(pluginabi.MethodHostHTTPStreamRead, map[string]any{
			"stream_id": httpStreamID,
		})
		if errCall != nil {
			streamErr = errCall.Error()
			break
		}
		var readResp rpcHostHTTPStreamReadResponse
		if errUnmarshal := json.Unmarshal(rawRead, &readResp); errUnmarshal != nil {
			streamErr = errUnmarshal.Error()
			break
		}
		if len(readResp.Payload) > 0 {
			pending = append(pending, readResp.Payload...)
			for {
				idx := bytes.IndexByte(pending, '\n')
				if idx < 0 {
					break
				}
				line := append([]byte(nil), pending[:idx+1]...)
				pending = pending[idx+1:]
				if !processLine(line) {
					streamErr = "downstream stream closed"
					break
				}
			}
			if streamErr != "" {
				break
			}
		}
		if readResp.Error != "" {
			streamErr = readResp.Error
			break
		}
		if readResp.Done {
			break
		}
	}

	if streamErr == "" && len(pending) > 0 {
		_ = processLine(pending)
	}
	if streamErr == "" && needsTranslation && prepared.wireFormat == sdktranslator.FormatOpenAI {
		// Flush terminal [DONE] for translators that finalize on data: [DONE]
		frames := sdktranslator.TranslateStream(
			ctx,
			prepared.wireFormat,
			prepared.clientFormat,
			prepared.baseModel,
			prepared.originalRequest,
			prepared.translatedReq,
			[]byte("data: [DONE]"),
			&param,
		)
		for _, frame := range frames {
			if len(frame) == 0 {
				continue
			}
			if !bytes.HasSuffix(frame, []byte("\n\n")) {
				frame = append(frame, '\n', '\n')
			}
			_ = emitChunk(frame)
		}
	}

	commitOpenCodeStreamUsage(authID, prepared.baseModel, streamUsageObserved)

	_, _ = callHost(pluginabi.MethodHostStreamClose, rpcStreamCloseRequest{
		StreamID: pluginStreamID,
		Error:    streamErr,
	})
}

func readAndCloseErrorStream(httpStreamID string) []byte {
	if strings.TrimSpace(httpStreamID) == "" {
		return nil
	}
	defer closeHostHTTPStream(httpStreamID)
	var out []byte
	for i := 0; i < 8; i++ {
		raw, errCall := callHost(pluginabi.MethodHostHTTPStreamRead, map[string]any{
			"stream_id": httpStreamID,
		})
		if errCall != nil {
			break
		}
		var resp rpcHostHTTPStreamReadResponse
		if json.Unmarshal(raw, &resp) != nil {
			break
		}
		if len(resp.Payload) > 0 {
			out = append(out, resp.Payload...)
		}
		if resp.Done || resp.Error != "" || len(out) > 16384 {
			break
		}
	}
	return out
}

func closeHostHTTPStream(httpStreamID string) {
	if strings.TrimSpace(httpStreamID) == "" {
		return
	}
	_, _ = callHost(pluginabi.MethodHostHTTPStreamClose, map[string]any{
		"stream_id": httpStreamID,
	})
}

func extractUpstreamErrorMessage(status int, body []byte) string {
	trimmed := strings.TrimSpace(string(body))
	if msg := gjson.GetBytes(body, "error.message").String(); strings.TrimSpace(msg) != "" {
		return strings.TrimSpace(msg)
	}
	if msg := gjson.GetBytes(body, "message").String(); strings.TrimSpace(msg) != "" {
		return strings.TrimSpace(msg)
	}
	if trimmed != "" {
		return trimmed
	}
	return fmt.Sprintf("OpenCode Go upstream returned HTTP %d", status)
}

func extractAuthCredentials(storageJSON []byte, attrs map[string]string) (token, orgID string) {
	if cred, ok := parseStoredCredential(storageJSON); ok {
		token = firstNonEmpty(cred.AccessToken, cred.APIKey)
		orgID = cred.OrgID
	}
	if attrs != nil {
		if token == "" {
			token = strings.TrimSpace(attrs["api_key"])
		}
		if orgID == "" {
			orgID = firstNonEmpty(attrs["org_id"], attrs["header:x-opencode-org-id"])
		}
	}
	return token, orgID
}

func resolveSessionHeader(headers http.Header, metadata map[string]any) string {
	if headers != nil {
		for _, key := range []string{"x-opencode-session", "X-Session-Affinity", "X-Claude-Code-Session-Id", "Session_id", "X-Session-Id"} {
			if val := strings.TrimSpace(headers.Get(key)); val != "" {
				return val
			}
		}
	}
	if metadata != nil {
		for _, key := range []string{"execution_session_id", "session_id"} {
			if val, ok := metadata[key].(string); ok && strings.TrimSpace(val) != "" {
				return strings.TrimSpace(val)
			}
		}
	}
	return currentConfig().SessionID
}

func normalizeTranslatorFormat(format string) sdktranslator.Format {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "claude", "anthropic", "anthropic-messages":
		return sdktranslator.FormatClaude
	case "codex", "openai-response", "responses", "response":
		return sdktranslator.FormatOpenAIResponse
	case "gemini", "google":
		return sdktranslator.FormatGemini
	default:
		return sdktranslator.FormatOpenAI
	}
}

func cloneHTTPHeader(h http.Header) http.Header {
	if h == nil {
		return nil
	}
	out := make(http.Header, len(h))
	for k, vals := range h {
		out[k] = append([]string(nil), vals...)
	}
	return out
}
