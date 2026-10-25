package main

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

type wireProtocol string

const (
	wireProtocolChatCompletions wireProtocol = "openai"
	wireProtocolResponses       wireProtocol = "openai-response"
	wireProtocolAnthropic       wireProtocol = "claude"
	wireProtocolGemini          wireProtocol = "gemini"
)

type goModelSpec struct {
	ID          string
	DisplayName string
	Protocol    wireProtocol
	ContextLen  int64
	MaxOutput   int64
	Levels      []string
	ToggleThink bool
	MaxBudget   int
}

type rpcAuthModelRequest struct {
	pluginapi.AuthModelRequest
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

var (
	catalogMu          sync.RWMutex
	catalogByID        map[string]goModelSpec
	catalogInitialized bool
	lastCatalogFetch   time.Time
)

var builtinGoCatalog = []goModelSpec{
	{ID: "beam-free", DisplayName: "Beam Free", Protocol: wireProtocolChatCompletions, ContextLen: 1048576, MaxOutput: 131072, Levels: []string{"low", "medium", "high", "xhigh", "max"}},
	{ID: "cat-jam-free", DisplayName: "Cat Jam Free", Protocol: wireProtocolChatCompletions, ContextLen: 1048576, MaxOutput: 131072, Levels: []string{"low", "medium", "high", "xhigh", "max"}},
	{ID: "claude-haiku-5-5", DisplayName: "Claude Haiku 5.5", Protocol: wireProtocolAnthropic, ContextLen: 1000000, MaxOutput: 128000, Levels: []string{"low", "medium", "high", "xhigh", "max"}, ToggleThink: true},
	{ID: "deepseek-v4-pro", DisplayName: "DeepSeek V4 Pro", Protocol: wireProtocolChatCompletions, ContextLen: 1000000, MaxOutput: 384000, Levels: []string{"high", "max"}},
	{ID: "deepseek-v4.1-flash", DisplayName: "DeepSeek V4.1 Flash", Protocol: wireProtocolChatCompletions, ContextLen: 1000000, MaxOutput: 384000, Levels: []string{"low", "high", "max"}},
	{ID: "gemini-4-argon", DisplayName: "Gemini 4 Argon", Protocol: wireProtocolGemini, ContextLen: 1048576, MaxOutput: 65536, Levels: []string{"low", "medium", "high"}},
	{ID: "gemini-4-argon-contributor", DisplayName: "Gemini 4 Argon Contributor", Protocol: wireProtocolGemini, ContextLen: 1048576, MaxOutput: 65536, Levels: []string{"low", "medium", "high"}},
	{ID: "glm-5.1", DisplayName: "GLM-5.1", Protocol: wireProtocolChatCompletions, ContextLen: 204800, MaxOutput: 131072, ToggleThink: true},
	{ID: "glm-5.2", DisplayName: "GLM-5.2", Protocol: wireProtocolChatCompletions, ContextLen: 1000000, MaxOutput: 131072, Levels: []string{"high", "max"}},
	{ID: "glm-5.3", DisplayName: "GLM-5.3", Protocol: wireProtocolChatCompletions, ContextLen: 1000000, MaxOutput: 131072, Levels: []string{"low", "high", "max"}},
	{ID: "glm-5.3-flash", DisplayName: "GLM-5.3-Flash", Protocol: wireProtocolChatCompletions, ContextLen: 1000000, MaxOutput: 131072, Levels: []string{"low", "high", "max"}},
	{ID: "gpt-5.6-luna", DisplayName: "GPT-5.6 Luna", Protocol: wireProtocolResponses, ContextLen: 1050000, MaxOutput: 128000, Levels: []string{"none", "low", "medium", "high", "xhigh", "max"}},
	{ID: "gpt-6-luna", DisplayName: "GPT-6 Luna", Protocol: wireProtocolResponses, ContextLen: 1050000, MaxOutput: 128000, Levels: []string{"none", "low", "medium", "high", "xhigh", "max"}},
	{ID: "grok-4.6", DisplayName: "Grok 4.6", Protocol: wireProtocolResponses, ContextLen: 500000, MaxOutput: 500000, Levels: []string{"low", "medium", "high", "xhigh"}},
	{ID: "grok-4.7", DisplayName: "Grok 4.7", Protocol: wireProtocolResponses, ContextLen: 500000, MaxOutput: 500000, Levels: []string{"low", "medium", "high", "xhigh"}},
	{ID: "hy3", DisplayName: "Hy3", Protocol: wireProtocolChatCompletions, ContextLen: 256000, MaxOutput: 128000, Levels: []string{"none", "low", "high"}},
	{ID: "hy4-preview", DisplayName: "Hy4 Preview", Protocol: wireProtocolChatCompletions, ContextLen: 1024000, MaxOutput: 64000, Levels: []string{"none", "high"}},
	{ID: "iceman-contributor-free", DisplayName: "Iceman Contributor Free", Protocol: wireProtocolChatCompletions, ContextLen: 262144, MaxOutput: 131072},
	{ID: "kimi-k2.6", DisplayName: "Kimi K2.6", Protocol: wireProtocolChatCompletions, ContextLen: 262144, MaxOutput: 65536, ToggleThink: true},
	{ID: "kimi-k2.7-code", DisplayName: "Kimi K2.7 Code", Protocol: wireProtocolChatCompletions, ContextLen: 262144, MaxOutput: 262144},
	{ID: "kimi-k3", DisplayName: "Kimi K3", Protocol: wireProtocolChatCompletions, ContextLen: 1048576, MaxOutput: 131072, Levels: []string{"max"}},
	{ID: "kimi-k3.1-pro", DisplayName: "Kimi K3.1 Pro", Protocol: wireProtocolChatCompletions, ContextLen: 1048576, MaxOutput: 131072, Levels: []string{"low", "high", "max"}},
	{ID: "longcat-2.0", DisplayName: "LongCat-2.0", Protocol: wireProtocolChatCompletions, ContextLen: 1000000, MaxOutput: 131072, ToggleThink: true},
	{ID: "longcat-2.5-preview-free", DisplayName: "LongCat 2.5 Preview Free", Protocol: wireProtocolChatCompletions, ContextLen: 1000000, MaxOutput: 131072, ToggleThink: true},
	{ID: "mimo-v2.5", DisplayName: "MiMo V2.5", Protocol: wireProtocolChatCompletions, ContextLen: 1000000, MaxOutput: 128000},
	{ID: "mimo-v2.5-pro", DisplayName: "MiMo V2.5 Pro", Protocol: wireProtocolChatCompletions, ContextLen: 1048576, MaxOutput: 128000},
	{ID: "mimo-v2.6-flash", DisplayName: "MiMo-V2.6-Flash", Protocol: wireProtocolChatCompletions, ContextLen: 1048576, MaxOutput: 131072},
	{ID: "mimo-v2.6-pro", DisplayName: "MiMo-V2.6-Pro", Protocol: wireProtocolChatCompletions, ContextLen: 1048576, MaxOutput: 131072},
	{ID: "minimax-m2.5", DisplayName: "MiniMax-M2.5", Protocol: wireProtocolChatCompletions, ContextLen: 204800, MaxOutput: 131072},
	{ID: "minimax-m2.7", DisplayName: "MiniMax-M2.7", Protocol: wireProtocolAnthropic, ContextLen: 204800, MaxOutput: 131072},
	{ID: "minimax-m3", DisplayName: "MiniMax-M3", Protocol: wireProtocolAnthropic, ContextLen: 1000000, MaxOutput: 131072, ToggleThink: true},
	{ID: "minimax-m3.1-flash", DisplayName: "MiniMax M3.1 Flash", Protocol: wireProtocolChatCompletions, ContextLen: 1000000, MaxOutput: 512000, Levels: []string{"low", "medium", "high", "xhigh", "max"}},
	{ID: "minimax-m3.1-flash-free", DisplayName: "MiniMax M3.1 Flash Free", Protocol: wireProtocolChatCompletions, ContextLen: 1000000, MaxOutput: 512000, Levels: []string{"low", "medium", "high", "xhigh", "max"}},
	{ID: "muse-spark-1.2-contributor", DisplayName: "Muse Spark 1.2 Contributor", Protocol: wireProtocolResponses, ContextLen: 1048576, MaxOutput: 131072, Levels: []string{"minimal", "low", "medium", "high", "xhigh"}},
	{ID: "muse-spark-1.3-contributor", DisplayName: "Muse Spark 1.3 Contributor", Protocol: wireProtocolResponses, ContextLen: 1048576, MaxOutput: 131072, Levels: []string{"minimal", "low", "medium", "high", "xhigh"}},
	{ID: "omen-alpha", DisplayName: "Omen Alpha", Protocol: wireProtocolChatCompletions, ContextLen: 262144, MaxOutput: 131072},
	{ID: "qwen3.6-plus", DisplayName: "Qwen3.6 Plus", Protocol: wireProtocolAnthropic, ContextLen: 262144, MaxOutput: 65536, ToggleThink: true, MaxBudget: 65536},
	{ID: "qwen3.7-max", DisplayName: "Qwen3.7 Max", Protocol: wireProtocolAnthropic, ContextLen: 1000000, MaxOutput: 65536, ToggleThink: true, MaxBudget: 262144},
	{ID: "qwen3.7-plus", DisplayName: "Qwen3.7 Plus", Protocol: wireProtocolAnthropic, ContextLen: 1000000, MaxOutput: 65536, ToggleThink: true, MaxBudget: 262144},
	{ID: "qwen3.8-flash", DisplayName: "Qwen3.8 Flash", Protocol: wireProtocolAnthropic, ContextLen: 1000000, MaxOutput: 131072, Levels: []string{"low", "medium", "xhigh"}, ToggleThink: true, MaxBudget: 131072},
	{ID: "qwen3.8-max", DisplayName: "Qwen3.8 Max", Protocol: wireProtocolAnthropic, ContextLen: 1000000, MaxOutput: 131072, Levels: []string{"low", "medium", "xhigh"}, ToggleThink: true, MaxBudget: 262144},
	{ID: "space-bunny", DisplayName: "Space Bunny", Protocol: wireProtocolChatCompletions, ContextLen: 1048576, MaxOutput: 524288, Levels: []string{"low", "medium", "high", "xhigh", "max"}},
	{ID: "step-5-preview", DisplayName: "Step 5 Preview", Protocol: wireProtocolChatCompletions, ContextLen: 1000000, MaxOutput: 65536, Levels: []string{"low", "medium", "high"}},
	{ID: "step-5-preview-free", DisplayName: "Step 5 Preview Free", Protocol: wireProtocolChatCompletions, ContextLen: 1000000, MaxOutput: 65536, Levels: []string{"low", "medium", "high"}},
}

func ensureCatalogInitializedLocked() {
	if catalogInitialized {
		return
	}
	catalogByID = make(map[string]goModelSpec, len(builtinGoCatalog))
	for _, spec := range builtinGoCatalog {
		catalogByID[spec.ID] = spec
	}
	catalogInitialized = true
}

func lookupModelSpec(modelID string) goModelSpec {
	baseID, _, _ := splitModelThinkingSuffix(strings.TrimSpace(modelID))
	baseID = strings.TrimPrefix(baseID, "models/")
	if idx := strings.LastIndex(baseID, "/"); idx >= 0 {
		baseID = baseID[idx+1:]
	}
	catalogMu.RLock()
	if catalogInitialized {
		if spec, ok := catalogByID[baseID]; ok {
			catalogMu.RUnlock()
			return spec
		}
		for k, spec := range catalogByID {
			if strings.EqualFold(k, baseID) {
				catalogMu.RUnlock()
				return spec
			}
		}
	}
	catalogMu.RUnlock()

	catalogMu.Lock()
	ensureCatalogInitializedLocked()
	spec, ok := catalogByID[baseID]
	catalogMu.Unlock()
	if ok {
		return spec
	}
	return inferModelSpec(baseID)
}

func inferModelSpec(modelID string) goModelSpec {
	lower := strings.ToLower(strings.TrimSpace(modelID))
	proto := wireProtocolChatCompletions
	switch {
	case strings.HasPrefix(lower, "claude-"), strings.HasPrefix(lower, "qwen"), strings.HasPrefix(lower, "minimax-m3"), strings.HasPrefix(lower, "minimax-m2.7"):
		if !strings.Contains(lower, "m3.1-flash") {
			proto = wireProtocolAnthropic
		}
	case strings.HasPrefix(lower, "gpt-"), strings.HasPrefix(lower, "grok-"), strings.HasPrefix(lower, "muse-spark"):
		proto = wireProtocolResponses
	case strings.HasPrefix(lower, "gemini-"):
		proto = wireProtocolGemini
	}
	return goModelSpec{
		ID:          modelID,
		DisplayName: modelID,
		Protocol:    proto,
		ContextLen:  262144,
		MaxOutput:   65536,
	}
}

func handleModelStatic(_ []byte) ([]byte, error) {
	return okEnvelope(pluginapi.ModelResponse{
		Provider: providerID,
		Models:   currentCatalogModelInfos(),
	})
}

func handleModelForAuth(raw []byte) ([]byte, error) {
	var req rpcAuthModelRequest
	if errUnmarshal := json.Unmarshal(raw, &req); errUnmarshal != nil {
		return nil, errUnmarshal
	}
	refreshCatalogFromUpstream(req.HostCallbackID)
	return okEnvelope(pluginapi.ModelResponse{
		Provider: providerID,
		Models:   currentCatalogModelInfos(),
	})
}

func refreshCatalogFromUpstream(hostCallbackID string) {
	if strings.TrimSpace(hostCallbackID) == "" {
		return
	}
	catalogMu.RLock()
	recentlyFetched := !lastCatalogFetch.IsZero() && time.Since(lastCatalogFetch) < 10*time.Minute
	catalogMu.RUnlock()
	if recentlyFetched {
		return
	}

	cfg := currentConfig()
	headers := http.Header{
		"Accept":     []string{"application/json"},
		"User-Agent": []string{cfg.UserAgent},
	}

	// 1. Fetch live OpenCode Go /v1/models list
	var liveIDs []string
	if resp, errHTTP := doHostHTTP(hostCallbackID, http.MethodGet, cfg.InferenceBaseURL+"/v1/models", headers, nil); errHTTP == nil && resp.StatusCode == http.StatusOK {
		var parsed struct {
			Data []struct {
				ID string `json:"id"`
			} `json:"data"`
		}
		if json.Unmarshal(resp.Body, &parsed) == nil {
			for _, item := range parsed.Data {
				if id := strings.TrimSpace(item.ID); id != "" {
					liveIDs = append(liveIDs, id)
				}
			}
		}
	}

	// 2. Fetch models.opencode.ai/api.json for rich metadata & wire protocol
	type modelsDevEntry struct {
		ID       string `json:"id"`
		Name     string `json:"name"`
		Provider struct {
			NPM string `json:"npm"`
			API string `json:"api"`
		} `json:"provider"`
		Limit struct {
			Context int64 `json:"context"`
			Output  int64 `json:"output"`
		} `json:"limit"`
		ReasoningOptions []struct {
			Type   string   `json:"type"`
			Values []string `json:"values"`
			Max    int      `json:"max"`
		} `json:"reasoning_options"`
	}
	var modelsDevRoot map[string]struct {
		Models map[string]modelsDevEntry `json:"models"`
	}
	if resp, errHTTP := doHostHTTP(hostCallbackID, http.MethodGet, "https://models.opencode.ai/api.json", headers, nil); errHTTP == nil && resp.StatusCode == http.StatusOK {
		_ = json.Unmarshal(resp.Body, &modelsDevRoot)
	}

	catalogMu.Lock()
	defer catalogMu.Unlock()
	ensureCatalogInitializedLocked()
	lastCatalogFetch = time.Now()

	applyDevEntry := func(id string, entry modelsDevEntry) {
		existing, hasExisting := catalogByID[id]
		if !hasExisting {
			existing = inferModelSpec(id)
		}
		if strings.TrimSpace(entry.Name) != "" {
			existing.DisplayName = strings.TrimSpace(entry.Name)
		}
		switch strings.TrimSpace(entry.Provider.NPM) {
		case "@ai-sdk/openai":
			existing.Protocol = wireProtocolResponses
		case "@ai-sdk/anthropic":
			existing.Protocol = wireProtocolAnthropic
		case "@ai-sdk/google":
			existing.Protocol = wireProtocolGemini
		default:
			if strings.Contains(entry.Provider.API, "/anthropic") {
				existing.Protocol = wireProtocolAnthropic
			} else if strings.Contains(entry.Provider.API, "/google") {
				existing.Protocol = wireProtocolGemini
			}
		}
		if entry.Limit.Context > 0 {
			existing.ContextLen = entry.Limit.Context
		}
		if entry.Limit.Output > 0 {
			existing.MaxOutput = entry.Limit.Output
		}
		for _, ro := range entry.ReasoningOptions {
			switch ro.Type {
			case "effort":
				if len(ro.Values) > 0 {
					existing.Levels = append([]string(nil), ro.Values...)
				}
			case "toggle":
				existing.ToggleThink = true
			case "budget_tokens":
				if ro.Max > 0 {
					existing.MaxBudget = ro.Max
				}
			}
		}
		catalogByID[id] = existing
	}

	if goSection, ok := modelsDevRoot["opencode-go"]; ok {
		for id, entry := range goSection.Models {
			applyDevEntry(id, entry)
		}
	}
	for _, id := range liveIDs {
		if goSection, ok := modelsDevRoot["opencode-go"]; ok {
			if entry, okEntry := goSection.Models[id]; okEntry {
				applyDevEntry(id, entry)
				continue
			}
		}
		if ocSection, ok := modelsDevRoot["opencode"]; ok {
			if entry, okEntry := ocSection.Models[id]; okEntry {
				applyDevEntry(id, entry)
				continue
			}
		}
		if _, exists := catalogByID[id]; !exists {
			catalogByID[id] = inferModelSpec(id)
		}
	}
}

func currentCatalogModelInfos() []pluginapi.ModelInfo {
	catalogMu.Lock()
	ensureCatalogInitializedLocked()
	specs := make([]goModelSpec, 0, len(catalogByID))
	for _, spec := range catalogByID {
		specs = append(specs, spec)
	}
	catalogMu.Unlock()

	sort.Slice(specs, func(i, j int) bool {
		return specs[i].ID < specs[j].ID
	})

	created := int64(1792920218)
	out := make([]pluginapi.ModelInfo, 0, len(specs))
	for _, spec := range specs {
		ctxLen := spec.ContextLen
		if ctxLen <= 0 {
			ctxLen = 262144
		}
		maxOut := spec.MaxOutput
		if maxOut <= 0 {
			maxOut = 65536
		}
		var thinking *pluginapi.ThinkingSupport
		if len(spec.Levels) > 0 || spec.ToggleThink || spec.MaxBudget > 0 {
			maxBudget := spec.MaxBudget
			if maxBudget <= 0 {
				maxBudget = 32768
			}
			levels := spec.Levels
			if len(levels) == 0 && spec.ToggleThink {
				levels = []string{"low", "medium", "high"}
			}
			thinking = &pluginapi.ThinkingSupport{
				Min:            1024,
				Max:            maxBudget,
				ZeroAllowed:    true,
				DynamicAllowed: true,
				Levels:         levels,
			}
		}
		out = append(out, pluginapi.ModelInfo{
			ID:                        spec.ID,
			Object:                    "model",
			Created:                   created,
			OwnedBy:                   providerID,
			Type:                      string(spec.Protocol),
			DisplayName:               firstNonEmpty(spec.DisplayName, spec.ID),
			Name:                      spec.ID,
			Version:                   spec.ID,
			ContextLength:             ctxLen,
			InputTokenLimit:           ctxLen,
			MaxCompletionTokens:       maxOut,
			OutputTokenLimit:          maxOut,
			SupportedParameters:       []string{"tools", "temperature", "top_p", "max_tokens"},
			SupportedInputModalities:  []string{"text", "image"},
			SupportedOutputModalities: []string{"text"},
			Thinking:                  thinking,
			UserDefined:               true,
		})
	}
	return out
}

func splitModelThinkingSuffix(model string) (base, suffix string, hasSuffix bool) {
	lastOpen := strings.LastIndex(model, "(")
	if lastOpen == -1 || !strings.HasSuffix(model, ")") {
		return model, "", false
	}
	return model[:lastOpen], model[lastOpen+1 : len(model)-1], true
}
