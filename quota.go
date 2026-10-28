package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
	"github.com/tidwall/gjson"
)

const (
	statsResourcePath   = "/stats"
	mgmtStatsRoutePath  = "/plugins/opencode-go-oauth/stats"
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

type perModelUsageSummary struct {
	Model          string  `json:"model"`
	DisplayName    string  `json:"display_name"`
	Protocol       string  `json:"protocol"`
	Requests       int64   `json:"requests"`
	InputTokens    int64   `json:"input_tokens"`
	OutputTokens   int64   `json:"output_tokens"`
	ReasoningToks  int64   `json:"reasoning_tokens"`
	CacheReadToks  int64   `json:"cache_read_tokens"`
	ListCostUSD    float64 `json:"list_cost_usd"`
	AllowanceUSD   float64 `json:"allowance_usd"`
	Allowance5hUSD float64 `json:"allowance_5h_usd"`
	Allowance7dUSD float64 `json:"allowance_7d_usd"`
	Allowance30d   float64 `json:"allowance_30d_usd"`
	WeightRatio    float64 `json:"weight_ratio"`
}

var (
	usageMu            sync.RWMutex
	usageEvents        []allowanceEvent
	usageTotalsByModel = make(map[string]*perModelUsageSummary)
	planByAuthID       = make(map[string]string)
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
	spec := lookupModelSpec(model)

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

	summary := usageTotalsByModel[model]
	if summary == nil {
		summary = &perModelUsageSummary{
			Model:       model,
			DisplayName: firstNonEmpty(spec.DisplayName, model),
			Protocol:    string(spec.Protocol),
		}
		usageTotalsByModel[model] = summary
	}
	summary.Requests++
	summary.InputTokens += sample.InputTokens
	summary.OutputTokens += sample.OutputTokens
	summary.ReasoningToks += sample.ReasoningToks
	summary.CacheReadToks += sample.CacheReadToks
	summary.ListCostUSD += sample.ListCost
	summary.AllowanceUSD += sample.Allowance
	if summary.ListCostUSD > 0 {
		summary.WeightRatio = math.Round((summary.AllowanceUSD/summary.ListCostUSD)*100) / 100
	}
}

func resolveAllowanceCapsForPlan(plan string) (planLabel string, cap5h, capWeekly, capMonth float64) {
	cfg := currentConfig()
	if normalizePlanTier(plan) == "go-plus" {
		planLabel = "OpenCode Go Plus ($40/mo)"
		cap5h = defaultGoPlus5h
		capWeekly = defaultGoPlusWeekly
		capMonth = defaultGoPlusMonth
	} else {
		planLabel = "OpenCode Go ($10/mo)"
		cap5h = defaultGoAllowance5h
		capWeekly = defaultGoAllowanceWeekly
		capMonth = defaultGoAllowanceMonth
	}
	if cfg.Allowance5h > 0 {
		cap5h = cfg.Allowance5h
	}
	if cfg.AllowanceWeekly > 0 {
		capWeekly = cfg.AllowanceWeekly
	}
	if cfg.AllowanceMonthly > 0 {
		capMonth = cfg.AllowanceMonthly
	}
	return planLabel, cap5h, capWeekly, capMonth
}

func resolveAllowanceCaps() (planLabel string, cap5h, capWeekly, capMonth float64) {
	return resolveAllowanceCapsForPlan(currentConfig().Plan)
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

func resolveAllowanceCapsForAuth(req rpcQuotaFetchRequest) (planLabel string, cap5h, capWeekly, capMonth float64) {
	// 1. Explicit per-credential override in Auth File Details / Edit (Note, Headers x-opencode-plan, or plan field in JSON)
	if override := extractPlanOverrideFromAuth(req); override != "" {
		return resolveAllowanceCapsForPlan(override)
	}
	authID := strings.TrimSpace(req.AuthID)
	if authID != "" {
		usageMu.RLock()
		cached := planByAuthID[authID]
		usageMu.RUnlock()
		if cached != "" {
			return resolveAllowanceCapsForPlan(cached)
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
		return resolveAllowanceCapsForPlan(probed)
	}
	return resolveAllowanceCaps()
}

func computeWindowTotals(authID string, now time.Time) (used5h, used7d, used30d float64, oldest5h, oldest7d, oldest30d time.Time, perModel []perModelUsageSummary) {
	cutoff5h := now.Add(-rolling5hWindow)
	cutoff7d := now.Add(-rollingWeeklyWindow)
	cutoff30d := now.Add(-rollingMonthWindow)

	usageMu.RLock()
	defer usageMu.RUnlock()

	byModel5h := make(map[string]float64)
	byModel7d := make(map[string]float64)
	byModel30d := make(map[string]float64)

	for _, ev := range usageEvents {
		if authID != "" && ev.AuthID != "" && ev.AuthID != authID {
			continue
		}
		if ev.Timestamp.After(cutoff30d) {
			used30d += ev.Allowance
			byModel30d[ev.Model] += ev.Allowance
			if oldest30d.IsZero() || ev.Timestamp.Before(oldest30d) {
				oldest30d = ev.Timestamp
			}
		}
		if ev.Timestamp.After(cutoff7d) {
			used7d += ev.Allowance
			byModel7d[ev.Model] += ev.Allowance
			if oldest7d.IsZero() || ev.Timestamp.Before(oldest7d) {
				oldest7d = ev.Timestamp
			}
		}
		if ev.Timestamp.After(cutoff5h) {
			used5h += ev.Allowance
			byModel5h[ev.Model] += ev.Allowance
			if oldest5h.IsZero() || ev.Timestamp.Before(oldest5h) {
				oldest5h = ev.Timestamp
			}
		}
	}

	perModel = make([]perModelUsageSummary, 0, len(usageTotalsByModel))
	for _, item := range usageTotalsByModel {
		copied := *item
		copied.Allowance5hUSD = byModel5h[copied.Model]
		copied.Allowance7dUSD = byModel7d[copied.Model]
		copied.Allowance30d = byModel30d[copied.Model]
		perModel = append(perModel, copied)
	}
	sort.Slice(perModel, func(i, j int) bool {
		if perModel[i].AllowanceUSD != perModel[j].AllowanceUSD {
			return perModel[i].AllowanceUSD > perModel[j].AllowanceUSD
		}
		return perModel[i].Requests > perModel[j].Requests
	})
	return
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
	planLabel, cap5h, cap7d, cap30d := resolveAllowanceCapsForAuth(req)
	used5h, used7d, used30d, oldest5h, oldest7d, oldest30d, _ := computeWindowTotals(req.AuthID, now)

	makeBucket := func(window string, used, capVal float64, oldest time.Time, dur time.Duration) pluginapi.QuotaBucket {
		rem := 1.0
		if capVal > 0 {
			rem = 1.0 - (used / capVal)
		}
		if rem < 0 {
			rem = 0
		} else if rem > 1 {
			rem = 1
		}
		resetAt := now.Add(dur)
		if !oldest.IsZero() {
			resetAt = oldest.Add(dur)
		}
		return pluginapi.QuotaBucket{
			Window:            window,
			RemainingFraction: math.Round(rem*10000) / 10000,
			ResetTime:         resetAt.Format(time.RFC3339),
			Description:       fmt.Sprintf("$%.4f / $%.2f allowance used (%.1f%% remaining)", used, capVal, rem*100),
		}
	}

	tierID := "go-plus"
	if strings.Contains(strings.ToLower(planLabel), "($10") {
		tierID = "go"
	}

	return okEnvelope(pluginapi.QuotaFetchResponse{
		Subscription: &pluginapi.QuotaSubscription{
			Plan:     planLabel,
			TierName: planLabel,
			TierID:   tierID,
		},
		Groups: []pluginapi.QuotaGroup{
			{
				DisplayName: "OpenCode Go Shared Allowance (Tracked by CPA)",
				Buckets: []pluginapi.QuotaBucket{
					makeBucket("5h", used5h, cap5h, oldest5h, rolling5hWindow),
					makeBucket("weekly", used7d, cap7d, oldest7d, rollingWeeklyWindow),
					makeBucket("monthly", used30d, cap30d, oldest30d, rollingMonthWindow),
				},
			},
		},
	})
}

func handleQuotaReset(_ []byte) ([]byte, error) {
	usageMu.Lock()
	usageEvents = nil
	usageTotalsByModel = make(map[string]*perModelUsageSummary)
	usageMu.Unlock()
	return okEnvelope(pluginapi.QuotaResetResponse{
		Success: true,
		Message: "Cleared tracked OpenCode Go usage counters",
	})
}

type managementRegistration struct {
	Routes    []managementRoute    `json:"routes,omitempty"`
	Resources []managementResource `json:"resources,omitempty"`
}

type managementRoute struct {
	Method      string `json:"Method"`
	Path        string `json:"Path"`
	Description string `json:"Description,omitempty"`
}

type managementResource struct {
	Path        string `json:"Path"`
	Menu        string `json:"Menu"`
	Description string `json:"Description"`
}

type managementRequest struct {
	Method         string      `json:"Method"`
	Path           string      `json:"Path"`
	Headers        http.Header `json:"Headers"`
	Query          url.Values  `json:"Query"`
	Body           []byte      `json:"Body"`
	HostCallbackID string      `json:"host_callback_id,omitempty"`
}

type managementResponse struct {
	StatusCode int         `json:"StatusCode"`
	Headers    http.Header `json:"Headers"`
	Body       []byte      `json:"Body"`
}

func managementRegistrationPayload() managementRegistration {
	return managementRegistration{
		Routes: []managementRoute{
			{
				Method:      http.MethodGet,
				Path:        mgmtStatsRoutePath,
				Description: "Returns per-model OpenCode Go allowance and token usage stats.",
			},
			{
				Method:      http.MethodPost,
				Path:        mgmtStatsRoutePath,
				Description: "Resets tracked OpenCode Go allowance and token usage counters.",
			},
		},
		Resources: []managementResource{
			{
				Path:        statsResourcePath,
				Menu:        "OpenCode Go Stats",
				Description: "Per-model OpenCode Go allowance, list cost, and token breakdown.",
			},
		},
	}
}

func resolveStatsPageAllowanceCaps() (planLabel string, cap5h, capWeekly, capMonth float64) {
	usageMu.RLock()
	goCount := 0
	plusCount := 0
	for _, tier := range planByAuthID {
		switch normalizePlanTier(tier) {
		case "go-plus":
			plusCount++
		case "go":
			goCount++
		}
	}
	usageMu.RUnlock()

	if goCount == 0 && plusCount == 0 {
		return resolveAllowanceCaps()
	}
	if goCount == 1 && plusCount == 0 {
		return resolveAllowanceCapsForPlan("go")
	}
	if plusCount == 1 && goCount == 0 {
		return resolveAllowanceCapsForPlan("go-plus")
	}
	cap5h = float64(goCount)*defaultGoAllowance5h + float64(plusCount)*defaultGoPlus5h
	capWeekly = float64(goCount)*defaultGoAllowanceWeekly + float64(plusCount)*defaultGoPlusWeekly
	capMonth = float64(goCount)*defaultGoAllowanceMonth + float64(plusCount)*defaultGoPlusMonth
	switch {
	case goCount > 0 && plusCount > 0:
		planLabel = fmt.Sprintf("%d× Go + %d× Go Plus", goCount, plusCount)
	case plusCount > 1:
		planLabel = fmt.Sprintf("%d× OpenCode Go Plus", plusCount)
	default:
		planLabel = fmt.Sprintf("%d× OpenCode Go", goCount)
	}
	return planLabel, cap5h, capWeekly, capMonth
}

func handleManagement(raw []byte) ([]byte, error) {
	var req managementRequest
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &req)
	}
	cleanedPath := strings.TrimRight(strings.TrimSpace(req.Path), "/")
	if cleanedPath == "/v0/management"+mgmtStatsRoutePath {
		if strings.EqualFold(req.Method, http.MethodPost) {
			_, _ = handleQuotaReset(nil)
		}
		now := time.Now().UTC()
		planLabel, cap5h, cap7d, cap30d := resolveStatsPageAllowanceCaps()
		used5h, used7d, used30d, _, _, _, models := computeWindowTotals("", now)

		payload := map[string]any{
			"plan":             planLabel,
			"used_5h_usd":      used5h,
			"cap_5h_usd":       cap5h,
			"used_weekly_usd":  used7d,
			"cap_weekly_usd":   cap7d,
			"used_monthly_usd": used30d,
			"cap_monthly_usd":  cap30d,
			"models":           models,
		}
		body, _ := marshalJSONNoEscape(payload)
		return okEnvelope(managementResponse{
			StatusCode: http.StatusOK,
			Headers: http.Header{
				"Content-Type":  []string{"application/json; charset=utf-8"},
				"Cache-Control": []string{"no-store"},
			},
			Body: body,
		})
	}

	// /v0/resource/plugins/opencode-go-oauth/stats serves only the static UI asset.
	return okEnvelope(managementResponse{
		StatusCode: http.StatusOK,
		Headers: http.Header{
			"Content-Type":  []string{"text/html; charset=utf-8"},
			"Cache-Control": []string{"no-store"},
		},
		Body: []byte(statsHTMLTemplate),
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

const statsHTMLTemplate = `<!doctype html>
<html lang="en" data-theme="dark">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>OpenCode Go Stats</title>
<style>
  :root, [data-theme='dark'] {
    --bg-secondary: #151412;
    --bg-primary: #1d1b18;
    --bg-tertiary: #262320;
    --bg-hover: #2e2a26;
    --text-primary: #f6f4f1;
    --text-secondary: #c9c3bb;
    --text-tertiary: #9c958d;
    --border-color: #3a3530;
    --border-primary: #4a453f;
    --border-hover: #5a544d;
    --primary-color: #8b8680;
    --primary-hover: #9a948e;
    --primary-active: #a6a099;
    --primary-contrast: #ffffff;
    --success-color: #10b981;
    --quota-medium-color: #ffd862;
    --warning-color: #c65746;
    --error-color: #c65746;
    --viz-success: #0fae78;
    --viz-failure: #cf4b3a;
    --success-badge-bg: rgba(6, 78, 59, 0.3);
    --success-badge-text: #6ee7b7;
    --failure-badge-bg: rgba(198, 87, 70, 0.24);
    --failure-badge-text: #f1b0a6;
    --count-badge-bg: rgba(139, 134, 128, 0.28);
    --shadow-sm: 0 1px 3px 0 rgb(0 0 0 / 0.3);
    --radius-md: 8px;
    --radius-lg: 12px;
    --radius-full: 9999px;
  }

  [data-theme='light'] {
    --bg-secondary: #faf9f5;
    --bg-primary: #f0eee8;
    --bg-tertiary: #e9e6df;
    --bg-hover: #e9e6df;
    --text-primary: #2d2a26;
    --text-secondary: #6d6760;
    --text-tertiary: #a29c95;
    --border-color: #e3e1db;
    --border-primary: #d5d2cb;
    --border-hover: #cecac4;
    --primary-color: #8b8680;
    --primary-hover: #7f7a74;
    --primary-active: #726d67;
    --primary-contrast: #ffffff;
    --quota-medium-color: #e0aa14;
    --viz-success: #10b981;
    --viz-failure: #c65746;
    --success-badge-bg: #d1fae5;
    --success-badge-text: #065f46;
    --failure-badge-bg: rgba(198, 87, 70, 0.14);
    --failure-badge-text: #8a3a30;
    --count-badge-bg: rgba(139, 134, 128, 0.18);
    --shadow-sm: 0 1px 2px 0 rgb(0 0 0 / 0.05);
  }

  [data-theme='white'] {
    --bg-secondary: #ffffff;
    --bg-primary: #ffffff;
    --bg-tertiary: #f6f6f6;
    --bg-hover: #f6f6f6;
    --text-primary: #2d2a26;
    --text-secondary: #6d6760;
    --text-tertiary: #a29c95;
    --border-color: #e5e5e5;
    --border-primary: #d9d9d9;
    --border-hover: #cccccc;
    --primary-color: #8b8680;
    --primary-hover: #7f7a74;
    --primary-active: #726d67;
    --primary-contrast: #ffffff;
    --quota-medium-color: #e0aa14;
    --viz-success: #10b981;
    --viz-failure: #c65746;
    --success-badge-bg: #d1fae5;
    --success-badge-text: #065f46;
    --failure-badge-bg: rgba(198, 87, 70, 0.14);
    --failure-badge-text: #8a3a30;
    --count-badge-bg: rgba(139, 134, 128, 0.18);
    --shadow-sm: 0 1px 2px 0 rgb(0 0 0 / 0.05);
  }

  * { box-sizing: border-box; }
  html, body {
    margin: 0;
    padding: 0;
    background-color: var(--bg-secondary);
    color: var(--text-primary);
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Oxygen', 'Ubuntu', 'Cantarell', sans-serif;
    font-size: 14px;
    line-height: 1.5;
    -webkit-font-smoothing: antialiased;
  }

  ::-webkit-scrollbar { width: 8px; height: 8px; }
  ::-webkit-scrollbar-track { background: var(--bg-secondary); }
  ::-webkit-scrollbar-thumb { background: var(--border-color); border-radius: 9999px; }
  ::-webkit-scrollbar-thumb:hover { background: var(--border-hover); }

  .main-content {
    padding: 70px clamp(20px, 3vw, 48px) 40px;
    display: flex;
    flex-direction: column;
    gap: 20px;
    max-width: 100%;
  }

  .page-header {
    display: flex;
    flex-direction: column;
    gap: 6px;
  }
  .page-title {
    margin: 0;
    color: var(--text-primary);
    font-size: 28px;
    font-weight: 700;
    line-height: 1.2;
    letter-spacing: -0.01em;
  }
  .page-desc {
    margin: 0;
    color: var(--text-secondary);
    font-size: 14px;
  }

  .status-bar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    padding: 10px 14px;
    border-radius: 10px;
    border: 1px solid color-mix(in srgb, var(--border-color) 75%, transparent);
    background: var(--bg-primary);
    flex-wrap: wrap;
  }
  .status-pills {
    display: flex;
    align-items: center;
    gap: 8px;
    flex-wrap: wrap;
  }
  .status-pill {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 5px 12px;
    border-radius: var(--radius-full);
    border: 1px solid color-mix(in srgb, var(--border-color) 75%, transparent);
    background: var(--bg-secondary);
    font-size: 12px;
    font-weight: 600;
    white-space: nowrap;
  }
  .status-dot {
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: var(--success-color);
    box-shadow: 0 0 6px rgba(16, 185, 129, 0.5);
  }
  .status-label { color: var(--text-secondary); }
  .status-value { color: var(--text-primary); font-weight: 700; }

  .toolbar {
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .btn {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 6px;
    border: 1px solid var(--border-color);
    border-radius: var(--radius-md);
    padding: 8px 14px;
    font-size: 13px;
    font-weight: 600;
    cursor: pointer;
    transition: all 150ms ease;
    background-color: var(--bg-tertiary);
    color: var(--text-primary);
    white-space: nowrap;
  }
  .btn:hover {
    background-color: var(--bg-hover);
    border-color: var(--border-hover);
  }
  .btn-danger {
    background-color: var(--failure-badge-bg);
    color: var(--failure-badge-text);
    border-color: color-mix(in srgb, var(--error-color) 40%, var(--border-color));
  }
  .btn-danger:hover {
    background-color: var(--error-color);
    color: #ffffff;
  }

  .cards {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
    gap: 16px;
  }
  .card {
    background: var(--bg-primary);
    border: 1px solid var(--border-color);
    border-radius: var(--radius-lg);
    padding: 16px;
    box-shadow: var(--shadow-sm);
  }
  .card-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    margin-bottom: 12px;
    padding-bottom: 10px;
    border-bottom: 1px solid color-mix(in srgb, var(--border-color) 70%, transparent);
  }
  .card-title {
    margin: 0;
    font-size: 15px;
    font-weight: 700;
    color: var(--text-primary);
  }

  .bucket-header {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 8px;
    margin-bottom: 6px;
  }
  .bucket-title {
    color: var(--text-secondary);
    font-size: 12px;
    font-weight: 700;
    letter-spacing: 0.05em;
    text-transform: uppercase;
  }
  .bucket-pct {
    font-family: ui-monospace, 'SF Mono', 'Cascadia Mono', 'JetBrains Mono', Menlo, Consolas, monospace;
    font-size: 12px;
    font-weight: 700;
    color: var(--text-primary);
  }
  .bucket-val {
    font-size: 22px;
    font-weight: 700;
    color: var(--text-primary);
    margin: 4px 0 10px;
    font-variant-numeric: tabular-nums;
  }
  .bucket-cap {
    font-size: 13px;
    font-weight: 500;
    color: var(--text-tertiary);
  }

  /* CPA QuotaMeter */
  .quota-bar {
    background: var(--bg-tertiary);
    height: 7px;
    border-radius: var(--radius-full);
    overflow: hidden;
  }
  .quota-fill {
    height: 100%;
    border-radius: var(--radius-full);
    transition: width 260ms ease;
  }
  .quota-fill.high { background: var(--viz-success); }
  .quota-fill.medium { background: var(--quota-medium-color); }
  .quota-fill.low { background: var(--viz-failure); }

  .table-wrap {
    overflow-x: auto;
  }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
  }
  th, td {
    padding: 8px 10px;
    border-bottom: 1px solid color-mix(in srgb, var(--border-color) 65%, transparent);
    text-align: left;
    white-space: nowrap;
  }
  th {
    color: var(--text-tertiary);
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 0.06em;
    text-transform: uppercase;
  }
  tbody tr:hover {
    background-color: color-mix(in srgb, var(--bg-tertiary) 65%, transparent);
  }
  tbody tr:last-child td { border-bottom: 0; }

  .badge {
    display: inline-flex;
    align-items: center;
    padding: 2px 8px;
    font-size: 11px;
    font-weight: 600;
    border-radius: var(--radius-full);
    background: var(--count-badge-bg);
    color: var(--text-secondary);
  }
  .mono {
    font-family: ui-monospace, 'SF Mono', 'Cascadia Mono', 'JetBrains Mono', Menlo, Consolas, monospace;
    font-size: 12px;
    font-variant-numeric: tabular-nums;
  }
  .subtle { color: var(--text-tertiary); }
</style>
</head>
<body>
<main class="main-content">
  <header class="page-header">
    <h1 class="page-title">OpenCode Go Stats</h1>
    <p class="page-desc">Rolling allowance windows and per-model token and cost telemetry reported by OpenCode Go.</p>
  </header>

  <div class="status-bar">
    <div class="status-pills">
      <span class="status-pill">
        <span class="status-dot"></span>
        <span class="status-label">Plan</span>
        <span class="status-value" id="planBadge">OpenCode Go</span>
      </span>
      <span class="status-pill">
        <span class="status-label">Active Models</span>
        <span class="status-value" id="activeModelsCount">0</span>
      </span>
      <span class="status-pill">
        <span class="status-label">Total Requests</span>
        <span class="status-value" id="totalReqCount">0</span>
      </span>
    </div>
  </div>

  <div class="toolbar">
    <button class="btn" onclick="loadStats()">Refresh</button>
    <button class="btn btn-danger" onclick="resetStats()">Reset Counters</button>
  </div>

  <div class="cards" id="bucketCards"></div>

  <section class="card">
    <div class="card-header">
      <h2 class="card-title">Per-Model Allowance &amp; Token Breakdown</h2>
      <span class="badge">Exact <code>usage.opencode</code> Telemetry</span>
    </div>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Model</th>
            <th>Protocol</th>
            <th>Requests</th>
            <th>Input Tok</th>
            <th>Output Tok</th>
            <th>Reasoning Tok</th>
            <th>Cache Read</th>
            <th>List Cost</th>
            <th>Allowance Used</th>
            <th>Weight</th>
            <th>5h Allowance</th>
            <th>7d Allowance</th>
          </tr>
        </thead>
        <tbody id="modelsBody"></tbody>
      </table>
    </div>
  </section>
</main>
<script>
const STATS_API_URL = '/v0/management/plugins/opencode-go-oauth/stats';
let data = { plan: 'OpenCode Go ($10/mo)', used_5h_usd: 0, cap_5h_usd: 12, used_weekly_usd: 0, cap_weekly_usd: 30, used_monthly_usd: 0, cap_monthly_usd: 60, models: [] };

function syncThemeWithCPA() {
  const apply = (mode) => {
    if (mode === 'white') {
      document.documentElement.setAttribute('data-theme', 'white');
    } else if (mode === 'light') {
      document.documentElement.setAttribute('data-theme', 'light');
    } else {
      document.documentElement.setAttribute('data-theme', 'dark');
    }
  };
  try {
    if (window.parent && window.parent !== window && window.parent.document) {
      const parentDoc = window.parent.document.documentElement;
      const syncFromParent = () => {
        const attr = parentDoc.getAttribute('data-theme');
        if (attr === 'dark' || attr === 'white') {
          apply(attr);
        } else {
          apply('light');
        }
      };
      syncFromParent();
      new MutationObserver(syncFromParent).observe(parentDoc, { attributes: true, attributeFilter: ['data-theme'] });
      return;
    }
  } catch (_) {}
  try {
    const raw = localStorage.getItem('cli-proxy-theme');
    if (raw) {
      const parsed = JSON.parse(raw);
      const t = parsed?.state?.theme;
      const resolved = parsed?.state?.resolvedTheme;
      if (t === 'dark' || (t === 'auto' && resolved === 'dark')) {
        apply('dark');
        return;
      }
      if (t === 'white' || (t === 'auto' && resolved !== 'dark')) {
        apply('white');
        return;
      }
      if (t === 'light') {
        apply('light');
        return;
      }
    }
  } catch (_) {}
  apply(window.matchMedia && window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark');
}

function deobfuscateCPASecret(payload) {
  const prefix = 'enc::v1::';
  if (!payload || !payload.startsWith(prefix)) return payload;
  try {
    const bin = atob(payload.slice(prefix.length));
    const arr = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) arr[i] = bin.charCodeAt(i);
    const keyStr = 'cli-proxy-api-webui::secure-storage|' + window.location.host + '|' + navigator.userAgent;
    const keyBytes = new TextEncoder().encode(keyStr);
    const out = new Uint8Array(arr.length);
    for (let i = 0; i < arr.length; i++) out[i] = arr[i] ^ keyBytes[i % keyBytes.length];
    return new TextDecoder().decode(out);
  } catch (_) {
    return payload;
  }
}

function detectStoredManagementKey() {
  try {
    const authRaw = localStorage.getItem('cli-proxy-auth');
    if (authRaw) {
      const parsed = JSON.parse(authRaw);
      const k = parsed?.state?.managementKey;
      if (k && typeof k === 'string' && k.trim()) return k.trim();
    }
    const legacy = localStorage.getItem('managementKey');
    if (legacy) {
      const dec = deobfuscateCPASecret(legacy);
      try {
        const parsed = JSON.parse(dec);
        if (typeof parsed === 'string' && parsed.trim()) return parsed.trim();
      } catch (_) {
        if (typeof dec === 'string' && dec.trim()) return dec.trim();
      }
    }
  } catch (_) {}
  return '';
}

function authHeaders() {
  const key = detectStoredManagementKey();
  const headers = { 'Content-Type': 'application/json' };
  if (key) {
    headers['Authorization'] = 'Bearer ' + key;
    headers['X-Management-Key'] = key;
  }
  return headers;
}

function renderBucket(label, used, cap) {
  const usedPct = cap > 0 ? Math.min(100, (used / cap) * 100) : 0;
  const remPct = Math.max(0, 100 - usedPct);
  const tier = remPct >= 70 ? 'high' : (remPct >= 30 ? 'medium' : 'low');
  return '<div class="card">' +
    '<div class="bucket-header">' +
      '<span class="bucket-title">' + label + '</span>' +
      '<span class="bucket-pct">' + remPct.toFixed(1) + '% remaining</span>' +
    '</div>' +
    '<div class="bucket-val">$' + used.toFixed(4) + ' <span class="bucket-cap">/ $' + cap.toFixed(2) + '</span></div>' +
    '<div class="quota-bar"><div class="quota-fill ' + tier + '" style="width:' + remPct.toFixed(1) + '%"></div></div>' +
  '</div>';
}

function renderStats() {
  const models = data.models || [];
  const totalReqs = models.reduce((acc, m) => acc + (m.requests || 0), 0);
  document.getElementById('planBadge').textContent = data.plan || 'OpenCode Go';
  document.getElementById('activeModelsCount').textContent = String(models.length);
  document.getElementById('totalReqCount').textContent = totalReqs.toLocaleString();

  document.getElementById('bucketCards').innerHTML =
    renderBucket('5-Hour Rolling Window (20%)', data.used_5h_usd || 0, data.cap_5h_usd || 12) +
    renderBucket('Weekly Rolling Window (50%)', data.used_weekly_usd || 0, data.cap_weekly_usd || 30) +
    renderBucket('Monthly Rolling Window (100%)', data.used_monthly_usd || 0, data.cap_monthly_usd || 60);

  const tbody = document.getElementById('modelsBody');
  if (models.length === 0) {
    tbody.innerHTML = '<tr><td colspan="12" class="subtle" style="text-align:center;padding:24px;">No OpenCode Go requests recorded yet in this session.</td></tr>';
  } else {
    tbody.innerHTML = models.map(m => '<tr>' +
      '<td><strong>' + m.display_name + '</strong> <span class="mono subtle">(' + m.model + ')</span></td>' +
      '<td><span class="badge mono">' + m.protocol + '</span></td>' +
      '<td class="mono">' + m.requests.toLocaleString() + '</td>' +
      '<td class="mono">' + m.input_tokens.toLocaleString() + '</td>' +
      '<td class="mono">' + m.output_tokens.toLocaleString() + '</td>' +
      '<td class="mono">' + m.reasoning_tokens.toLocaleString() + '</td>' +
      '<td class="mono">' + m.cache_read_tokens.toLocaleString() + '</td>' +
      '<td class="mono">$' + m.list_cost_usd.toFixed(5) + '</td>' +
      '<td class="mono"><strong>$' + m.allowance_usd.toFixed(5) + '</strong></td>' +
      '<td class="mono">' + (m.weight_ratio ? m.weight_ratio + '×' : '—') + '</td>' +
      '<td class="mono">$' + m.allowance_5h_usd.toFixed(5) + '</td>' +
      '<td class="mono">$' + m.allowance_7d_usd.toFixed(5) + '</td>' +
    '</tr>').join('');
  }
}

async function loadStats() {
  try {
    const resp = await fetch(STATS_API_URL, { method: 'GET', headers: authHeaders(), cache: 'no-store' });
    if (resp.ok) {
      data = await resp.json();
      renderStats();
    }
  } catch (_) {}
}

async function resetStats() {
  if (!confirm('Reset tracked OpenCode Go usage counters?')) return;
  try {
    const resp = await fetch(STATS_API_URL, { method: 'POST', headers: authHeaders(), cache: 'no-store' });
    if (resp.ok) {
      data = await resp.json();
      renderStats();
    }
  } catch (_) {}
}

syncThemeWithCPA();
renderStats();
void loadStats();
</script>
</body>
</html>
`
