package main

import (
	"encoding/json"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestDashboardUsesUpstreamQuotaAndCredentials(t *testing.T) {
	prev := hostCallHook
	defer func() { hostCallHook = prev }()
	hostCallHook = func(method string, payload any) (json.RawMessage, error) {
		switch method {
		case pluginabi.MethodHostAuthList:
			return json.RawMessage(`{"files":[{"auth_index":"1","name":"account","type":"opencode-go"}]}`), nil
		case pluginabi.MethodHostAuthGet:
			return json.RawMessage(`{"json":{"type":"opencode-go","access_token":"test-token","org_id":"org"}}`), nil
		case pluginabi.MethodHostHTTPDo:
			raw, _ := json.Marshal(payload)
			if strings.Contains(string(raw), "/console/api/go/status") {
				return marshalJSONNoEscape(pluginapi.HTTPResponse{StatusCode: 200, Body: []byte(`{"access":{"meters":{"fiveHour":{"usedMicroCents":"36000000","limitMicroCents":"1200000000"},"week":{"usedMicroCents":"600000000","limitMicroCents":"3000000000"},"month":{"usedMicroCents":"600000000","limitMicroCents":"6000000000"}}}}`)})
			}
			if !strings.Contains(string(raw), "/zen/go/v1/usage") {
				t.Fatalf("unexpected request: %s", raw)
			}
			return marshalJSONNoEscape(pluginapi.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(`{"usage":{"rolling":{"status":"ok","percent":1,"resetsAt":"2026-11-01T00:00:00Z"},"weekly":{"status":"ok","percent":18},"monthly":{"status":"ok","percent":9}}}`)})
		default:
			t.Fatalf("unexpected method %s", method)
			return nil, nil
		}
	}
	accounts, err := dashboardAccounts("callback")
	if err != nil || len(accounts) != 1 || accounts[0].Usage == nil || *accounts[0].Usage.Weekly.Percent != 20 || *accounts[0].Usage.Weekly.UsedUSD != 6 || *accounts[0].Usage.Weekly.LimitUSD != 30 {
		t.Fatalf("accounts=%+v err=%v", accounts, err)
	}
	request, _ := json.Marshal(rpcQuotaFetchRequest{QuotaFetchRequest: pluginapi.QuotaFetchRequest{StorageJSON: []byte(`{"type":"opencode-go","access_token":"test-token","org_id":"org"}`)}})
	quota, err := handleQuotaFetch(request)
	if err != nil || !strings.Contains(string(quota), "20% ($6.00 / $30.00)") {
		t.Fatalf("quota=%s err=%v", quota, err)
	}
}
func TestFailedUsageIsNotZeroQuota(t *testing.T) {
	prev := hostCallHook
	defer func() { hostCallHook = prev }()
	hostCallHook = func(method string, payload any) (json.RawMessage, error) {
		return marshalJSONNoEscape(pluginapi.HTTPResponse{StatusCode: 401})
	}
	usage, err := fetchUpstreamUsage("cb", "token", "")
	if usage != nil || err == nil {
		t.Fatalf("usage=%+v err=%v", usage, err)
	}
}

func TestLiveCatalogReplacesSnapshotAndSurvivesOutage(t *testing.T) {
	prev := hostCallHook
	catalogMu.Lock()
	old, init, when := catalogByID, catalogInitialized, lastCatalogFetch
	catalogByID = map[string]goModelSpec{"stale": {ID: "stale"}}
	catalogInitialized = true
	lastCatalogFetch = time.Time{}
	catalogMu.Unlock()
	defer func() {
		hostCallHook = prev
		catalogMu.Lock()
		catalogByID, catalogInitialized, lastCatalogFetch = old, init, when
		catalogMu.Unlock()
	}()
	hostCallHook = func(method string, payload any) (json.RawMessage, error) {
		raw, _ := json.Marshal(payload)
		if strings.Contains(string(raw), "/v1/models") {
			return marshalJSONNoEscape(pluginapi.HTTPResponse{StatusCode: 200, Body: []byte(`{"data":[{"id":"actual"}]}`)})
		}
		return marshalJSONNoEscape(pluginapi.HTTPResponse{StatusCode: 200, Body: []byte(`{"opencode-go":{"models":{"stale":{"name":"Stale"},"actual":{"name":"Actual"}}}}`)})
	}
	refreshCatalogFromUpstream("cb")
	infos := currentCatalogModelInfos()
	if len(infos) != 1 || infos[0].ID != "actual" {
		t.Fatalf("catalog = %+v", infos)
	}
	catalogMu.Lock()
	lastCatalogFetch = time.Time{}
	catalogMu.Unlock()
	hostCallHook = func(string, any) (json.RawMessage, error) {
		return marshalJSONNoEscape(pluginapi.HTTPResponse{StatusCode: 503})
	}
	refreshCatalogFromUpstream("cb")
	infos = currentCatalogModelInfos()
	if len(infos) != 1 || infos[0].ID != "actual" {
		t.Fatalf("lost catalog after outage: %+v", infos)
	}
}

func TestExplicitExtraModel(t *testing.T) {
	old := currentConfig()
	defer activeConfig.Store(old)
	config := old
	config.ExtraModels = map[string]string{"verified-outside-list": "claude", "bad": "bogus"}
	activeConfig.Store(config)
	found := false
	for _, m := range currentCatalogModelInfos() {
		if m.ID == "bad" {
			t.Fatal("invalid protocol advertised")
		}
		if m.ID == "verified-outside-list" {
			found = true
			if m.Type != "claude" {
				t.Fatalf("wrong protocol: %s", m.Type)
			}
		}
	}
	if !found || lookupModelSpec("verified-outside-list").Protocol != wireProtocolAnthropic {
		t.Fatal("extra model not registered")
	}
}
