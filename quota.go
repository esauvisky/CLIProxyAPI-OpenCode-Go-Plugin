package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
	"github.com/tidwall/gjson"
)

const (
	rolling5hWindow     = 5 * time.Hour
	rollingWeeklyWindow = 7 * 24 * time.Hour
	rollingMonthWindow  = 30 * 24 * time.Hour
	maxRecordedEvents   = 20000
)

type openCodeUsageSample struct {
	Recorded      bool
	BillingSource string
	ListCost      float64
	Allowance     float64
	Cost          float64
	InputTokens   int64
	OutputTokens  int64
	ReasoningToks int64
	CacheReadToks int64
}

type allowanceEvent struct {
	Timestamp     time.Time `json:"timestamp"`
	AuthID        string    `json:"auth_id"`
	Model         string    `json:"model"`
	BillingSource string    `json:"billing_source,omitempty"`
	ListCost      float64   `json:"list_cost"`
	Allowance     float64   `json:"allowance"`
	InputTokens   int64     `json:"input_tokens"`
	OutputTokens  int64     `json:"output_tokens"`
	ReasoningToks int64     `json:"reasoning_tokens"`
	CacheReadToks int64     `json:"cache_read_tokens"`
}

var (
	usageMu      sync.RWMutex
	usageEvents  []allowanceEvent
	planByAuthID = make(map[string]string)
)

type rpcQuotaFetchRequest struct {
	pluginapi.QuotaFetchRequest
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

func normalizePlanTier(raw string) string {
	s := strings.ToLower(strings.TrimSpace(raw))
	switch {
	case s == "go-plus" || s == "goplus" || s == "plus" || strings.Contains(s, "go-plus") || strings.Contains(s, "go plus") || strings.Contains(s, "$40") || s == "40":
		return "go-plus"
	case s == "go" || strings.Contains(s, "$10") || s == "10":
		return "go"
	default:
		return ""
	}
}

func probeAccountPlanTier(hostCallbackID, token, orgID string) string {
	if strings.TrimSpace(hostCallbackID) == "" || strings.TrimSpace(token) == "" {
		return ""
	}
	cfg := currentConfig()
	headers := http.Header{
		"Authorization":      []string{"Bearer " + strings.TrimSpace(token)},
		"Content-Type":       []string{"application/json"},
		"Accept":             []string{"application/json"},
		"User-Agent":         []string{cfg.UserAgent},
		"x-opencode-session": []string{cfg.SessionID},
	}
	if strings.TrimSpace(orgID) != "" {
		headers.Set("x-opencode-org-id", strings.TrimSpace(orgID))
	}
	body := []byte(`{"model":"beam-free","messages":[{"role":"user","content":"hi"}],"max_tokens":1,"stream":false}`)
	resp, errHTTP := doHostHTTP(hostCallbackID, http.MethodPost, cfg.InferenceBaseURL+"/openai/v1/chat/completions", headers, body)
	if errHTTP != nil || resp.StatusCode != http.StatusOK {
		return ""
	}
	return normalizePlanTier(gjson.GetBytes(resp.Body, "usage.opencode.billing_source").String())
}

func parseOpenCodeUsageNode(u gjson.Result) (openCodeUsageSample, bool) {
	if !u.Exists() {
		return openCodeUsageSample{}, false
	}
	oc := u.Get("opencode")
	if !oc.Exists() {
		return openCodeUsageSample{}, false
	}
	toks := oc.Get("tokens")
	sample := openCodeUsageSample{
		Recorded:      true,
		BillingSource: strings.TrimSpace(oc.Get("billing_source").String()),
		ListCost:      oc.Get("list_cost").Float(),
		Allowance:     oc.Get("allowance").Float(),
		Cost:          u.Get("cost").Float(),
		InputTokens:   toks.Get("input").Int(),
		OutputTokens:  toks.Get("output").Int(),
		ReasoningToks: toks.Get("reasoning").Int(),
		CacheReadToks: toks.Get("cache_read").Int(),
	}
	if sample.InputTokens == 0 {
		sample.InputTokens = firstPositiveInt64(u.Get("prompt_tokens").Int(), u.Get("input_tokens").Int(), u.Get("promptTokenCount").Int())
	}
	if sample.OutputTokens == 0 {
		sample.OutputTokens = firstPositiveInt64(u.Get("completion_tokens").Int(), u.Get("output_tokens").Int(), u.Get("candidatesTokenCount").Int())
	}
	return sample, true
}

func recordOpenCodeUsagePayload(authID, model string, proto wireProtocol, body []byte) {
	if len(body) == 0 {
		return
	}
	var u gjson.Result
	if proto == wireProtocolGemini {
		u = gjson.GetBytes(body, "usageMetadata")
	} else {
		u = gjson.GetBytes(body, "usage")
	}
	if sample, ok := parseOpenCodeUsageNode(u); ok {
		commitOpenCodeStreamUsage(authID, model, sample)
	}
}

func observeOpenCodeStreamLineUsage(proto wireProtocol, line []byte, dst *openCodeUsageSample) {
	if dst == nil || len(line) == 0 {
		return
	}
	trimmed := bytes.TrimSpace(line)
	if !bytes.HasPrefix(trimmed, []byte("data:")) {
		return
	}
	jsonPart := bytes.TrimSpace(bytes.TrimPrefix(trimmed, []byte("data:")))
	if len(jsonPart) == 0 || bytes.Equal(jsonPart, []byte("[DONE]")) {
		return
	}
	for _, path := range []string{"usage", "response.usage", "message.usage", "usageMetadata"} {
		if sample, ok := parseOpenCodeUsageNode(gjson.GetBytes(jsonPart, path)); ok {
			*dst = sample
			return
		}
	}
	_ = proto
}

func commitOpenCodeStreamUsage(authID, model string, sample openCodeUsageSample) {
	if !sample.Recorded {
		return
	}
	now := time.Now().UTC()
	model = strings.TrimSpace(model)
	if model == "" {
		model = "unknown"
	}

	usageMu.Lock()
	defer usageMu.Unlock()

	if tier := normalizePlanTier(sample.BillingSource); tier != "" && strings.TrimSpace(authID) != "" {
		planByAuthID[strings.TrimSpace(authID)] = tier
	}

	usageEvents = append(usageEvents, allowanceEvent{
		Timestamp:     now,
		AuthID:        strings.TrimSpace(authID),
		Model:         model,
		BillingSource: sample.BillingSource,
		ListCost:      sample.ListCost,
		Allowance:     sample.Allowance,
		InputTokens:   sample.InputTokens,
		OutputTokens:  sample.OutputTokens,
		ReasoningToks: sample.ReasoningToks,
		CacheReadToks: sample.CacheReadToks,
	})
	if len(usageEvents) > maxRecordedEvents {
		usageEvents = append([]allowanceEvent(nil), usageEvents[len(usageEvents)-maxRecordedEvents:]...)
	}
}

// monthlyCapByPlan is each model's monthly allowance cap in dollars, from https://opencode.ai/docs/go/ (Usage limits).
// Each model's 5h window is 20% of its monthly cap and its weekly window 50%. Models missing here have no
// published cap and get no quota group.
var monthlyCapByPlan = map[string]map[string]float64{
	"go": {
		"glm-5.3-flash": 60, "glm-5.3": 15, "glm-5.2": 60,
		"kimi-k3.1-pro": 15, "kimi-k3": 15, "kimi-k2.7-code": 60,
		"longcat-2.0": 60, "step-5-preview": 15,
		"mimo-v2.6-flash": 60, "mimo-v2.6-pro": 15, "mimo-v2.5": 60, "mimo-v2.5-pro": 15,
		"minimax-m3.1-flash": 20, "minimax-m3": 60,
		"muse-spark-1.3-contributor": 60, "muse-spark-1.2-contributor": 60,
		"qwen3.8-max": 15, "qwen3.8-flash": 30, "qwen3.7-plus": 60,
		"deepseek-v4.1-flash": 100, "deepseek-v4-pro": 15,
		"hy4": 60, "hy4-preview": 30, "hy3": 60,
		"grok-4.7": 15, "gpt-6-luna": 15, "gpt-5.6-luna": 15, "claude-haiku-5-5": 15,
		"gemini-4-argon": 30,
	},
	"go-plus": {
		"glm-5.3-flash": 180, "glm-5.3": 120, "glm-5.2": 180,
		"kimi-k3.1-pro": 60, "kimi-k3": 60, "kimi-k2.7-code": 180,
		"longcat-2.0": 240, "step-5-preview": 60,
		"mimo-v2.6-flash": 120, "mimo-v2.6-pro": 60, "mimo-v2.5": 120, "mimo-v2.5-pro": 60,
		"minimax-m3.1-flash": 80, "minimax-m3": 180,
		"muse-spark-1.3-contributor": 120, "muse-spark-1.2-contributor": 120,
		"qwen3.8-max": 60, "qwen3.8-flash": 90, "qwen3.7-plus": 180,
		"deepseek-v4.1-flash": 200, "deepseek-v4-pro": 60,
		"hy4": 240, "hy4-preview": 120, "hy3": 240,
		"grok-4.7": 60, "gpt-6-luna": 60, "gpt-5.6-luna": 60, "claude-haiku-5-5": 60,
		"gemini-4-argon": 120,
	},
}

// unlimitedModels are the free models the docs list as "Unlimited (limited time)" on every plan.
var unlimitedModels = map[string]bool{
	"beam-free":                true,
	"iceman-contributor-free":  true,
	"longcat-2.5-preview-free": true,
}

var windowShareOfMonthly = [3]float64{0.2, 0.5, 1.0}

func planTierAndLabel(plan string) (tier, label string) {
	if normalizePlanTier(plan) == "go-plus" {
		return "go-plus", "OpenCode Go Plus ($40/mo)"
	}
	return "go", "OpenCode Go ($10/mo)"
}

func extractPlanOverrideFromAuth(req rpcQuotaFetchRequest) string {
	if req.Attributes != nil {
		for _, k := range []string{"header:x-opencode-plan", "plan", "plan_type", "note"} {
			if tier := normalizePlanTier(req.Attributes[k]); tier != "" {
				return tier
			}
		}
	}
	if req.Metadata != nil {
		for _, k := range []string{"note", "plan", "plan_type"} {
			if tier := normalizePlanTier(stringFromMap(req.Metadata, k)); tier != "" {
				return tier
			}
		}
		if hdrs, ok := req.Metadata["headers"].(map[string]any); ok {
			if tier := normalizePlanTier(stringFromMap(hdrs, "x-opencode-plan", "X-Opencode-Plan")); tier != "" {
				return tier
			}
		}
	}
	if len(req.StorageJSON) > 0 {
		for _, path := range []string{"note", "plan", "plan_type", "headers.x-opencode-plan", "headers.X-Opencode-Plan"} {
			if tier := normalizePlanTier(gjson.GetBytes(req.StorageJSON, path).String()); tier != "" {
				return tier
			}
		}
	}
	return ""
}

func resolvePlanForAuth(req rpcQuotaFetchRequest) string {
	// 1. Explicit per-credential override in Auth File Details / Edit (Note, Headers x-opencode-plan, or plan field in JSON)
	if override := extractPlanOverrideFromAuth(req); override != "" {
		return override
	}
	authID := strings.TrimSpace(req.AuthID)
	if authID != "" {
		usageMu.RLock()
		cached := planByAuthID[authID]
		usageMu.RUnlock()
		if cached != "" {
			return cached
		}
	}
	// 2. Live $0.00 probe on beam-free for this specific credential
	token, orgID := extractAuthCredentials(req.StorageJSON, req.Attributes)
	if probed := probeAccountPlanTier(req.HostCallbackID, token, orgID); probed != "" {
		if authID != "" {
			usageMu.Lock()
			planByAuthID[authID] = probed
			usageMu.Unlock()
		}
		return probed
	}
	return currentConfig().Plan
}

var rollingWindows = [3]struct {
	name string
	dur  time.Duration
}{
	{"5h", rolling5hWindow},
	{"weekly", rollingWeeklyWindow},
	{"monthly", rollingMonthWindow},
}

// modelWindowUsage holds one model's tracked allowance spend and oldest event per window (5h, weekly, monthly).
type modelWindowUsage struct {
	allowance [3]float64
	oldest    [3]time.Time
}

func computeModelWindowTotals(authID string, now time.Time) map[string]modelWindowUsage {
	usageMu.RLock()
	defer usageMu.RUnlock()

	out := make(map[string]modelWindowUsage)
	for _, ev := range usageEvents {
		if authID != "" && ev.AuthID != "" && ev.AuthID != authID {
			continue
		}
		usage := out[ev.Model]
		for i, w := range rollingWindows {
			if ev.Timestamp.After(now.Add(-w.dur)) {
				usage.allowance[i] += ev.Allowance
				if usage.oldest[i].IsZero() || ev.Timestamp.Before(usage.oldest[i]) {
					usage.oldest[i] = ev.Timestamp
				}
			}
		}
		out[ev.Model] = usage
	}
	return out
}

func handleQuotaDescribe(_ []byte) ([]byte, error) {
	return okEnvelope(pluginapi.QuotaDescribeResponse{
		SupportedProviders: []string{providerID, pluginID},
		DisplayName:        "OpenCode Go Allowance",
		SupportsReset:      true,
	})
}

func handleQuotaFetch(raw []byte) ([]byte, error) {
	var req rpcQuotaFetchRequest
	if len(raw) > 0 {
		if errUnmarshal := json.Unmarshal(raw, &req); errUnmarshal != nil {
			return nil, errUnmarshal
		}
	}
	now := time.Now().UTC()
	tierID, planLabel := planTierAndLabel(resolvePlanForAuth(req))
	monthlyCaps := monthlyCapByPlan[tierID]

	enabled, errEnabled := enabledModelsForAuth(req.AuthID)
	if errEnabled != nil {
		return nil, errEnabled
	}

	perModel := computeModelWindowTotals(req.AuthID, now)
	groups := make([]pluginapi.QuotaGroup, 0, len(enabled))
	for _, id := range enabled {
		buckets := make([]pluginapi.QuotaBucket, 0, len(rollingWindows))
		if unlimitedModels[id] {
			for _, w := range rollingWindows {
				buckets = append(buckets, pluginapi.QuotaBucket{
					Window:            w.name,
					RemainingFraction: 1,
					Description:       "unlimited (free model, limited time)",
				})
			}
		} else {
			monthly, documented := monthlyCaps[id]
			if !documented {
				continue
			}
			usage := perModel[id]
			for i, w := range rollingWindows {
				capVal := monthly * windowShareOfMonthly[i]
				rem := math.Min(math.Max(1.0-usage.allowance[i]/capVal, 0), 1)
				resetAt := now.Add(w.dur)
				if !usage.oldest[i].IsZero() {
					resetAt = usage.oldest[i].Add(w.dur)
				}
				buckets = append(buckets, pluginapi.QuotaBucket{
					Window:            w.name,
					RemainingFraction: math.Round(rem*10000) / 10000,
					ResetTime:         resetAt.Format(time.RFC3339),
					Description:       fmt.Sprintf("$%.4f / $%.2f allowance used (%.1f%% remaining)", usage.allowance[i], capVal, rem*100),
				})
			}
		}
		groups = append(groups, pluginapi.QuotaGroup{
			DisplayName: lookupModelSpec(id).DisplayName,
			Buckets:     buckets,
		})
	}

	return okEnvelope(pluginapi.QuotaFetchResponse{
		Subscription: &pluginapi.QuotaSubscription{
			Plan:     planLabel,
			TierName: planLabel,
			TierID:   tierID,
		},
		Groups: groups,
	})
}

func handleQuotaReset(_ []byte) ([]byte, error) {
	usageMu.Lock()
	usageEvents = nil
	usageMu.Unlock()
	return okEnvelope(pluginapi.QuotaResetResponse{
		Success: true,
		Message: "Cleared tracked OpenCode Go usage counters",
	})
}

func firstPositiveInt64(vals ...int64) int64 {
	for _, v := range vals {
		if v > 0 {
			return v
		}
	}
	return 0
}
