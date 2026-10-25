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
	detectedPlanTier   = "go-plus"
)

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

	if sample.BillingSource != "" {
		lower := strings.ToLower(sample.BillingSource)
		if lower == "go" || lower == "go-plus" {
			detectedPlanTier = lower
		}
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

func resolveAllowanceCaps() (planLabel string, cap5h, capWeekly, capMonth float64) {
	cfg := currentConfig()
	usageMu.RLock()
	plan := cfg.Plan
	if plan == "" || plan == "auto" {
		plan = detectedPlanTier
	}
	usageMu.RUnlock()

	if strings.EqualFold(plan, "go") {
		planLabel = "OpenCode Go ($10/mo)"
		cap5h = defaultGoAllowance5h
		capWeekly = defaultGoAllowanceWeekly
		capMonth = defaultGoAllowanceMonth
	} else {
		planLabel = "OpenCode Go Plus ($40/mo)"
		cap5h = defaultGoPlus5h
		capWeekly = defaultGoPlusWeekly
		capMonth = defaultGoPlusMonth
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
		SupportedProviders: []string{pluginID},
		DisplayName:        "OpenCode Go Allowance",
		SupportsReset:      true,
	})
}

func handleQuotaFetch(raw []byte) ([]byte, error) {
	var req pluginapi.QuotaFetchRequest
	if len(raw) > 0 {
		if errUnmarshal := json.Unmarshal(raw, &req); errUnmarshal != nil {
			return nil, errUnmarshal
		}
	}
	now := time.Now().UTC()
	planLabel, cap5h, cap7d, cap30d := resolveAllowanceCaps()
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

func handleManagement(raw []byte) ([]byte, error) {
	var req managementRequest
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &req)
	}
	now := time.Now().UTC()
	if req.Query != nil && strings.EqualFold(req.Query.Get("op"), "reset") {
		_, _ = handleQuotaReset(nil)
	}
	planLabel, cap5h, cap7d, cap30d := resolveAllowanceCaps()
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

	if (req.Query != nil && strings.EqualFold(req.Query.Get("format"), "json")) ||
		strings.HasSuffix(strings.TrimRight(req.Path, "/"), mgmtStatsRoutePath) {
		body, _ := marshalJSONNoEscape(payload)
		return okEnvelope(managementResponse{
			StatusCode: http.StatusOK,
			Headers:    http.Header{"Content-Type": []string{"application/json; charset=utf-8"}},
			Body:       body,
		})
	}

	stateJSON, _ := marshalJSONNoEscape(payload)
	htmlDoc := strings.Replace(statsHTMLTemplate, "__INITIAL_STATS_JSON__", string(stateJSON), 1)
	return okEnvelope(managementResponse{
		StatusCode: http.StatusOK,
		Headers: http.Header{
			"Content-Type":  []string{"text/html; charset=utf-8"},
			"Cache-Control": []string{"no-store"},
		},
		Body: []byte(htmlDoc),
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
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>OpenCode Go Usage Stats</title>
<style>
  :root {
    --bg: #0f172a;
    --panel: #1e293b;
    --panel-alt: #0b1120;
    --border: #334155;
    --text: #f8fafc;
    --muted: #94a3b8;
    --accent: #38bdf8;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    padding: 24px;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    background: var(--bg);
    color: var(--text);
  }
  .container { max-width: 1280px; margin: 0 auto; }
  header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 20px;
    padding-bottom: 14px;
    border-bottom: 1px solid var(--border);
  }
  h1 { margin: 0; font-size: 1.45rem; }
  .subtitle { color: var(--muted); font-size: 0.88rem; margin-top: 4px; }
  .cards {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
    gap: 16px;
    margin-bottom: 20px;
  }
  .card {
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 16px;
  }
  .metric-title { color: var(--muted); font-size: 0.8rem; text-transform: uppercase; font-weight: 600; }
  .metric-val { font-size: 1.4rem; font-weight: 700; margin: 6px 0; }
  .bar-bg { background: var(--panel-alt); height: 8px; border-radius: 4px; overflow: hidden; }
  .bar-fill { background: var(--accent); height: 100%; }
  table { width: 100%; border-collapse: collapse; font-size: 0.85rem; }
  th, td { padding: 8px 10px; border-bottom: 1px solid var(--border); text-align: left; white-space: nowrap; }
  th { color: var(--muted); font-size: 0.76rem; text-transform: uppercase; }
  .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
  button {
    background: var(--panel);
    color: var(--text);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 7px 12px;
    cursor: pointer;
  }
  button:hover { border-color: var(--accent); }
</style>
</head>
<body>
<div class="container">
  <header>
    <div>
      <h1>OpenCode Go Usage Stats</h1>
      <div class="subtitle" id="planSubtitle"></div>
    </div>
    <div>
      <button onclick="location.reload()">Refresh</button>
      <button onclick="if(confirm('Reset tracked counters?')) location.search='?op=reset'">Reset Counters</button>
    </div>
  </header>

  <div class="cards" id="bucketCards"></div>

  <div class="card">
    <h2 style="margin:0 0 12px 0;font-size:1.05rem;">Per-Model Breakdown (Exact <code>usage.opencode</code> Telemetry)</h2>
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
          <th>Allowance Deducted</th>
          <th>Weight</th>
          <th>5h Allowance</th>
          <th>7d Allowance</th>
        </tr>
      </thead>
      <tbody id="modelsBody"></tbody>
    </table>
  </div>
</div>
<script>
const data = __INITIAL_STATS_JSON__;
document.getElementById('planSubtitle').textContent = data.plan + ' · Exact per-request allowance & list_cost reported by OpenCode Go';

function renderBucket(label, used, cap) {
  const pct = cap > 0 ? Math.min(100, (used / cap) * 100) : 0;
  const remPct = Math.max(0, 100 - pct);
  return '<div class="card">' +
    '<div class="metric-title">' + label + '</div>' +
    '<div class="metric-val">$' + used.toFixed(4) + ' <span style="font-size:0.9rem;color:#94a3b8;font-weight:400;">/ $' + cap.toFixed(2) + ' (' + remPct.toFixed(1) + '% left)</span></div>' +
    '<div class="bar-bg"><div class="bar-fill" style="width:' + pct.toFixed(1) + '%"></div></div>' +
  '</div>';
}

document.getElementById('bucketCards').innerHTML =
  renderBucket('5-Hour Window (20%)', data.used_5h_usd || 0, data.cap_5h_usd || 48) +
  renderBucket('Weekly Window (50%)', data.used_weekly_usd || 0, data.cap_weekly_usd || 120) +
  renderBucket('Monthly Window (100%)', data.used_monthly_usd || 0, data.cap_monthly_usd || 240);

const tbody = document.getElementById('modelsBody');
const models = data.models || [];
if (models.length === 0) {
  tbody.innerHTML = '<tr><td colspan="12" style="color:#94a3b8;text-align:center;padding:18px;">No OpenCode Go requests recorded yet in this session.</td></tr>';
} else {
  tbody.innerHTML = models.map(m => '<tr>' +
    '<td><strong>' + m.display_name + '</strong> <span class="mono" style="color:#94a3b8;">(' + m.model + ')</span></td>' +
    '<td class="mono">' + m.protocol + '</td>' +
    '<td>' + m.requests + '</td>' +
    '<td>' + m.input_tokens.toLocaleString() + '</td>' +
    '<td>' + m.output_tokens.toLocaleString() + '</td>' +
    '<td>' + m.reasoning_tokens.toLocaleString() + '</td>' +
    '<td>' + m.cache_read_tokens.toLocaleString() + '</td>' +
    '<td class="mono">$' + m.list_cost_usd.toFixed(5) + '</td>' +
    '<td class="mono"><strong>$' + m.allowance_usd.toFixed(5) + '</strong></td>' +
    '<td class="mono">' + (m.weight_ratio ? m.weight_ratio + 'x' : '—') + '</td>' +
    '<td class="mono">$' + m.allowance_5h_usd.toFixed(5) + '</td>' +
    '<td class="mono">$' + m.allowance_7d_usd.toFixed(5) + '</td>' +
  '</tr>').join('');
}
</script>
</body>
</html>
`
