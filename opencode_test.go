package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
	"github.com/tidwall/gjson"
)

func TestDeviceOAuthFlowAndCredentialParse(t *testing.T) {
	prevHook := hostCallHook
	t.Cleanup(func() { hostCallHook = prevHook })

	hostCallHook = func(method string, payload any) (json.RawMessage, error) {
		if method != pluginabi.MethodHostHTTPDo {
			t.Fatalf("unexpected host method: %s", method)
		}
		raw, _ := json.Marshal(payload)
		targetURL := gjson.GetBytes(raw, "url").String()
		switch {
		case strings.HasSuffix(targetURL, "/auth/device/code"):
			return marshalJSONNoEscape(pluginapi.HTTPResponse{
				StatusCode: http.StatusOK,
				Body:       []byte(`{"device_code":"dev-123","user_code":"ABCD-EFGH","verification_uri":"/console/device","verification_uri_complete":"/console/device?user_code=ABCD-EFGH&client_id=opencode-cli","expires_in":600,"interval":5}`),
			})
		case strings.HasSuffix(targetURL, "/auth/device/token"):
			return marshalJSONNoEscape(pluginapi.HTTPResponse{
				StatusCode: http.StatusOK,
				Body:       []byte(`{"access_token":"st_test_token","refresh_token":"rt_test_refresh","expires_in":3600,"org_id":"org_123"}`),
			})
		case strings.HasSuffix(targetURL, "/api/user"):
			return marshalJSONNoEscape(pluginapi.HTTPResponse{
				StatusCode: http.StatusOK,
				Body:       []byte(`{"id":"user_123","email":"user@example.com"}`),
			})
		case strings.HasSuffix(targetURL, "/api/orgs"):
			return marshalJSONNoEscape(pluginapi.HTTPResponse{
				StatusCode: http.StatusOK,
				Body:       []byte(`[{"id":"org_123","name":"Personal"}]`),
			})
		case strings.HasSuffix(targetURL, "/openai/v1/chat/completions"):
			return marshalJSONNoEscape(pluginapi.HTTPResponse{
				StatusCode: http.StatusOK,
				Body:       []byte(`{"usage":{"opencode":{"billing_source":"go"}}}`),
			})
		default:
			t.Fatalf("unexpected URL: %s", targetURL)
			return nil, nil
		}
	}

	startRaw, errStart := handleAuthLoginStart([]byte(`{}`))
	if errStart != nil {
		t.Fatalf("handleAuthLoginStart: %v", errStart)
	}
	var startEnv struct {
		OK     bool                             `json:"ok"`
		Result pluginapi.AuthLoginStartResponse `json:"result"`
	}
	if err := json.Unmarshal(startRaw, &startEnv); err != nil {
		t.Fatalf("unmarshal start: %v", err)
	}
	if !strings.HasPrefix(startEnv.Result.URL, "https://opencode.ai/console/device?user_code=ABCD-EFGH") {
		t.Fatalf("unexpected verification URL: %s", startEnv.Result.URL)
	}

	pollReq, _ := json.Marshal(rpcAuthLoginPollRequest{
		AuthLoginPollRequest: pluginapi.AuthLoginPollRequest{
			Provider: pluginID,
			State:    startEnv.Result.State,
		},
	})
	pollRaw, errPoll := handleAuthLoginPoll(pollReq)
	if errPoll != nil {
		t.Fatalf("handleAuthLoginPoll: %v", errPoll)
	}
	var pollEnv struct {
		OK     bool                            `json:"ok"`
		Result pluginapi.AuthLoginPollResponse `json:"result"`
	}
	if err := json.Unmarshal(pollRaw, &pollEnv); err != nil {
		t.Fatalf("unmarshal poll: %v", err)
	}
	if pollEnv.Result.Status != pluginapi.AuthLoginStatusSuccess {
		t.Fatalf("poll status = %s, message = %s", pollEnv.Result.Status, pollEnv.Result.Message)
	}
	if pollEnv.Result.Auth.Attributes["header:x-opencode-session"] != "cpa-main" {
		t.Fatalf("expected x-opencode-session=cpa-main, got %#v", pollEnv.Result.Auth.Attributes)
	}
	if pollEnv.Result.Auth.Attributes["org_id"] != "org_123" {
		t.Fatalf("expected org_id=org_123, got %#v", pollEnv.Result.Auth.Attributes)
	}
}

func TestExecuteRoutesAcrossAllFourProtocolsAndTracksAllowance(t *testing.T) {
	prevHook := hostCallHook
	t.Cleanup(func() { hostCallHook = prevHook })
	_, _ = handleQuotaReset(nil)

	var seenURL string
	var seenSession string
	var seenOrg string
	hostCallHook = func(method string, payload any) (json.RawMessage, error) {
		raw, _ := json.Marshal(payload)
		seenURL = gjson.GetBytes(raw, "url").String()
		seenSession = gjson.GetBytes(raw, "headers.X-Opencode-Session.0").String()
		if seenSession == "" {
			seenSession = gjson.GetBytes(raw, "headers.x-opencode-session.0").String()
		}
		seenOrg = gjson.GetBytes(raw, "headers.X-Opencode-Org-Id.0").String()
		if seenOrg == "" {
			seenOrg = gjson.GetBytes(raw, "headers.x-opencode-org-id.0").String()
		}
		switch {
		case strings.Contains(seenURL, "/openai/v1/chat/completions"):
			return marshalJSONNoEscape(pluginapi.HTTPResponse{
				StatusCode: http.StatusOK,
				Headers:    http.Header{"Content-Type": []string{"application/json"}},
				Body:       []byte(`{"id":"chatcmpl-1","object":"chat.completion","model":"glm-5.3","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"hello"}}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15,"opencode":{"billing_source":"go-plus","list_cost":0.001,"allowance":0.004,"tokens":{"input":10,"output":5}}}}`),
			})
		case strings.Contains(seenURL, "/openai/v1/responses"):
			return marshalJSONNoEscape(pluginapi.HTTPResponse{
				StatusCode: http.StatusOK,
				Headers:    http.Header{"Content-Type": []string{"application/json"}},
				Body:       []byte(`{"id":"resp_1","object":"response","status":"completed","model":"gpt-6-luna","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"hello"}]}],"usage":{"input_tokens":10,"output_tokens":5,"total_tokens":15,"opencode":{"billing_source":"go-plus","list_cost":0.002,"allowance":0.008,"tokens":{"input":10,"output":5}}}}`),
			})
		case strings.Contains(seenURL, "/anthropic/v1/messages"):
			return marshalJSONNoEscape(pluginapi.HTTPResponse{
				StatusCode: http.StatusOK,
				Headers:    http.Header{"Content-Type": []string{"application/json"}},
				Body:       []byte(`{"id":"msg_1","type":"message","role":"assistant","model":"qwen3.8-max","stop_reason":"end_turn","content":[{"type":"text","text":"hello"}],"usage":{"input_tokens":10,"output_tokens":5,"opencode":{"billing_source":"go-plus","list_cost":0.003,"allowance":0.006,"tokens":{"input":10,"output":5}}}}`),
			})
		case strings.Contains(seenURL, "/google/v1beta/models/gemini-4-argon:generateContent"):
			return marshalJSONNoEscape(pluginapi.HTTPResponse{
				StatusCode: http.StatusOK,
				Headers:    http.Header{"Content-Type": []string{"application/json"}},
				Body:       []byte(`{"candidates":[{"content":{"role":"model","parts":[{"text":"hello"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":5,"totalTokenCount":15,"opencode":{"billing_source":"go-plus","list_cost":0.004,"allowance":0.008,"tokens":{"input":10,"output":5}}}}`),
			})
		default:
			t.Fatalf("unexpected upstream URL: %s", seenURL)
			return nil, nil
		}
	}

	cases := []struct {
		model    string
		wantPath string
	}{
		{"glm-5.3(high)", "/openai/v1/chat/completions"},
		{"gpt-6-luna(high)", "/openai/v1/responses"},
		{"qwen3.8-max(high)", "/anthropic/v1/messages"},
		{"gemini-4-argon(high)", "/google/v1beta/models/gemini-4-argon:generateContent"},
	}

	for _, tc := range cases {
		execReq, _ := json.Marshal(rpcExecutorRequest{
			ExecutorRequest: pluginapi.ExecutorRequest{
				AuthID:         "opencode-go.json",
				AuthProvider:   pluginID,
				Model:          tc.model,
				Format:         "claude",
				SourceFormat:   "claude",
				Payload:        []byte(`{"model":"` + tc.model + `","max_tokens":128,"messages":[{"role":"user","content":"hi"}]}`),
				StorageJSON:    []byte(`{"type":"opencode-go","access_token":"st_tok","org_id":"org_abc"}`),
				AuthAttributes: map[string]string{"org_id": "org_abc"},
			},
		})
		rawResp, errExec := handleExecutorExecute(execReq)
		if errExec != nil {
			t.Fatalf("execute %s: %v", tc.model, errExec)
		}
		if !strings.HasSuffix(seenURL, tc.wantPath) {
			t.Fatalf("model %s routed to %s, want suffix %s", tc.model, seenURL, tc.wantPath)
		}
		if seenSession != "cpa-main" || seenOrg != "org_abc" {
			t.Fatalf("headers session=%q org=%q, want cpa-main / org_abc", seenSession, seenOrg)
		}
		var env struct {
			OK     bool                       `json:"ok"`
			Result pluginapi.ExecutorResponse `json:"result"`
		}
		if err := json.Unmarshal(rawResp, &env); err != nil {
			t.Fatalf("decode execute response: %v", err)
		}
		if len(env.Result.Payload) == 0 {
			t.Fatalf("empty translated payload for %s", tc.model)
		}
	}

	_, _, _, _, _, _, models := computeWindowTotals("", time.Now().UTC())
	if len(models) == 0 {
		t.Fatal("expected locally tracked model usage")
	}
}
