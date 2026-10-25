package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

var pendingLoginStates sync.Map

type rpcAuthLoginStartRequest struct {
	pluginapi.AuthLoginStartRequest
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

type rpcAuthLoginPollRequest struct {
	pluginapi.AuthLoginPollRequest
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

type rpcAuthRefreshRequest struct {
	pluginapi.AuthRefreshRequest
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

type deviceCodeResponse struct {
	DeviceCode              string `json:"device_code"`
	UserCode                string `json:"user_code"`
	VerificationURI         string `json:"verification_uri"`
	VerificationURIComplete string `json:"verification_uri_complete"`
	ExpiresIn               int    `json:"expires_in"`
	Interval                int    `json:"interval"`
}

type deviceTokenResponse struct {
	AccessToken  string  `json:"access_token"`
	RefreshToken string  `json:"refresh_token"`
	ExpiresIn    int     `json:"expires_in"`
	OrgID        *string `json:"org_id"`
	Error        string  `json:"error"`
}

type consoleUser struct {
	ID    string `json:"id"`
	Email string `json:"email"`
}

type consoleOrg struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type loginStatePayload struct {
	ConsoleURL string `json:"console_url"`
	DeviceCode string `json:"device_code"`
	UserCode   string `json:"user_code"`
}

type storedCredential struct {
	Type         string `json:"type"`
	AccessToken  string `json:"access_token,omitempty"`
	RefreshToken string `json:"refresh_token,omitempty"`
	APIKey       string `json:"api_key,omitempty"`
	ExpiresAtMs  int64  `json:"expires_at_ms,omitempty"`
	ConsoleURL   string `json:"console_url,omitempty"`
	AccountID    string `json:"account_id,omitempty"`
	Email        string `json:"email,omitempty"`
	OrgID        string `json:"org_id,omitempty"`
	OrgName      string `json:"org_name,omitempty"`
}

func handleAuthParse(raw []byte) ([]byte, error) {
	var req pluginapi.AuthParseRequest
	if errUnmarshal := json.Unmarshal(raw, &req); errUnmarshal != nil {
		return nil, errUnmarshal
	}
	cred, okParse := parseStoredCredential(req.RawJSON)
	if !okParse {
		return okEnvelope(pluginapi.AuthParseResponse{Handled: false})
	}
	authData := buildAuthDataFromCredential(cred, req.FileName, req.Path)
	return okEnvelope(pluginapi.AuthParseResponse{
		Handled: true,
		Auth:    authData,
	})
}

func parseStoredCredential(rawJSON []byte) (storedCredential, bool) {
	trimmed := bytes.TrimSpace(rawJSON)
	if len(trimmed) == 0 {
		return storedCredential{}, false
	}
	var raw map[string]any
	if errUnmarshal := json.Unmarshal(trimmed, &raw); errUnmarshal != nil || raw == nil {
		return storedCredential{}, false
	}
	rawType := strings.ToLower(strings.TrimSpace(stringFromMap(raw, "type", "provider")))
	isOpenCodeType := rawType == pluginID || rawType == "opencode-go" || rawType == "opencode_go"
	if !isOpenCodeType {
		// Also accept OpenCode CLI's native credential shape if server points to opencode.ai/console
		if meta, ok := raw["metadata"].(map[string]any); ok {
			srv := stringFromMap(meta, "server")
			if strings.Contains(strings.ToLower(srv), "opencode.ai") && stringFromMap(raw, "access") != "" {
				isOpenCodeType = true
			}
		}
	}
	if !isOpenCodeType {
		return storedCredential{}, false
	}

	cred := storedCredential{
		Type:         pluginID,
		AccessToken:  firstNonEmpty(stringFromMap(raw, "access_token", "access")),
		RefreshToken: firstNonEmpty(stringFromMap(raw, "refresh_token", "refresh")),
		APIKey:       firstNonEmpty(stringFromMap(raw, "api_key", "key")),
		ConsoleURL:   firstNonEmpty(stringFromMap(raw, "console_url", "server"), currentConfig().ConsoleURL),
		AccountID:    stringFromMap(raw, "account_id", "accountID"),
		Email:        stringFromMap(raw, "email"),
		OrgID:        stringFromMap(raw, "org_id", "orgID", "project_id"),
		OrgName:      stringFromMap(raw, "org_name", "orgName"),
	}
	if meta, ok := raw["metadata"].(map[string]any); ok {
		cred.ConsoleURL = firstNonEmpty(stringFromMap(meta, "server"), cred.ConsoleURL)
		cred.AccountID = firstNonEmpty(cred.AccountID, stringFromMap(meta, "accountID", "account_id"))
		cred.Email = firstNonEmpty(cred.Email, stringFromMap(meta, "email"))
		cred.OrgID = firstNonEmpty(cred.OrgID, stringFromMap(meta, "orgID", "org_id"))
		cred.OrgName = firstNonEmpty(cred.OrgName, stringFromMap(meta, "orgName", "org_name"))
	}
	if expMs := int64FromMap(raw, "expires_at_ms", "expires"); expMs > 0 {
		cred.ExpiresAtMs = expMs
	} else if expStr := stringFromMap(raw, "expires_at", "expired"); expStr != "" {
		if t, errParse := time.Parse(time.RFC3339, expStr); errParse == nil {
			cred.ExpiresAtMs = t.UnixMilli()
		}
	}
	if cred.AccessToken == "" && cred.APIKey == "" {
		return storedCredential{}, false
	}
	return cred, true
}

func buildAuthDataFromCredential(cred storedCredential, fileName, path string) pluginapi.AuthData {
	cred.Type = pluginID
	if cred.ConsoleURL == "" {
		cred.ConsoleURL = currentConfig().ConsoleURL
	}
	label := firstNonEmpty(cred.Email, cred.OrgName, "OpenCode Go")
	if cred.OrgName != "" && cred.Email != "" && !strings.EqualFold(cred.OrgName, cred.Email) {
		label = fmt.Sprintf("%s (%s)", cred.Email, cred.OrgName)
	}
	if strings.TrimSpace(fileName) == "" {
		slug := sanitizeFileNamePart(firstNonEmpty(cred.Email, cred.OrgID, "account"))
		fileName = fmt.Sprintf("opencode-go-%s.json", slug)
	}
	id := strings.TrimSpace(fileName)
	if id == "" {
		id = strings.TrimSpace(path)
	}
	if id == "" {
		id = "opencode-go.json"
	}

	storageMap := map[string]any{
		"type":          pluginID,
		"access_token":  cred.AccessToken,
		"refresh_token": cred.RefreshToken,
		"api_key":       cred.APIKey,
		"expires_at_ms": cred.ExpiresAtMs,
		"console_url":   cred.ConsoleURL,
		"account_id":    cred.AccountID,
		"email":         cred.Email,
		"org_id":        cred.OrgID,
		"org_name":      cred.OrgName,
	}
	var nextRefresh time.Time
	if cred.ExpiresAtMs > 0 {
		expTime := time.UnixMilli(cred.ExpiresAtMs).UTC()
		storageMap["expired"] = expTime.Format(time.RFC3339)
		if cred.RefreshToken != "" {
			refreshLead := 5 * time.Minute
			nextRefresh = expTime.Add(-refreshLead)
			if nextRefresh.Before(time.Now().UTC()) {
				nextRefresh = time.Now().UTC().Add(10 * time.Second)
			}
		}
	}
	storageBytes, _ := marshalJSONNoEscape(storageMap)

	token := firstNonEmpty(cred.AccessToken, cred.APIKey)
	attrs := map[string]string{
		"api_key":  token,
		"base_url": currentConfig().InferenceBaseURL + "/openai/v1",
	}
	if cred.OrgID != "" {
		attrs["org_id"] = cred.OrgID
		attrs["header:x-opencode-org-id"] = cred.OrgID
	}
	if cred.Email != "" {
		attrs["email"] = cred.Email
	}
	attrs["header:x-opencode-session"] = currentConfig().SessionID

	meta := map[string]any{
		"type":       pluginID,
		"email":      cred.Email,
		"project_id": cred.OrgID,
		"account":    cred.OrgName,
	}
	if cred.ExpiresAtMs > 0 {
		meta["expired"] = time.UnixMilli(cred.ExpiresAtMs).UTC().Format(time.RFC3339)
	}

	return pluginapi.AuthData{
		Provider:         pluginID,
		ID:               id,
		FileName:         fileName,
		Label:            label,
		StorageJSON:      storageBytes,
		Metadata:         meta,
		Attributes:       attrs,
		NextRefreshAfter: nextRefresh,
	}
}

func handleAuthLoginStart(raw []byte) ([]byte, error) {
	var req rpcAuthLoginStartRequest
	if errUnmarshal := json.Unmarshal(raw, &req); errUnmarshal != nil {
		return nil, errUnmarshal
	}
	cfg := currentConfig()
	consoleURL := strings.TrimRight(cfg.ConsoleURL, "/")
	bodyBytes, _ := marshalJSONNoEscape(map[string]any{
		"client_id":          defaultOAuthClientID,
		"supports_org_scope": true,
	})
	resp, errHTTP := doHostHTTP(req.HostCallbackID, http.MethodPost, consoleURL+"/auth/device/code", http.Header{
		"Content-Type": []string{"application/json"},
		"Accept":       []string{"application/json"},
		"User-Agent":   []string{cfg.UserAgent},
	}, bodyBytes)
	if errHTTP != nil {
		return nil, fmt.Errorf("request OpenCode device code: %w", errHTTP)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("OpenCode device code HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(resp.Body)))
	}
	var codeResp deviceCodeResponse
	if errUnmarshal := json.Unmarshal(resp.Body, &codeResp); errUnmarshal != nil {
		return nil, fmt.Errorf("decode OpenCode device code response: %w", errUnmarshal)
	}
	if strings.TrimSpace(codeResp.DeviceCode) == "" {
		return nil, fmt.Errorf("OpenCode device code response missing device_code")
	}

	verifyURL := strings.TrimSpace(codeResp.VerificationURIComplete)
	if verifyURL == "" {
		verifyURL = strings.TrimSpace(codeResp.VerificationURI)
	}
	if strings.HasPrefix(verifyURL, "/") {
		// verification_uri_complete is e.g. "/console/device?user_code=..." relative to origin
		origin := strings.TrimSuffix(consoleURL, "/console")
		verifyURL = origin + verifyURL
	}

	stateKey := sanitizeOAuthState(codeResp.DeviceCode, codeResp.UserCode)
	pendingLoginStates.Store(stateKey, loginStatePayload{
		ConsoleURL: consoleURL,
		DeviceCode: codeResp.DeviceCode,
		UserCode:   codeResp.UserCode,
	})

	expiresIn := codeResp.ExpiresIn
	if expiresIn <= 0 {
		expiresIn = 600
	}
	return okEnvelope(pluginapi.AuthLoginStartResponse{
		Provider:  pluginID,
		URL:       verifyURL,
		State:     stateKey,
		ExpiresAt: time.Now().UTC().Add(time.Duration(expiresIn) * time.Second),
		Metadata: map[string]any{
			"device_code": codeResp.DeviceCode,
			"user_code":   codeResp.UserCode,
			"console_url": consoleURL,
			"interval":    codeResp.Interval,
		},
	})
}

func sanitizeOAuthState(deviceCode, userCode string) string {
	raw := strings.TrimSpace(deviceCode)
	if raw == "" {
		raw = "oc-" + strings.TrimSpace(userCode)
	}
	var b strings.Builder
	for _, r := range raw {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			b.WriteRune(r)
		}
	}
	out := b.String()
	if len(out) > 96 {
		out = out[:96]
	}
	if out == "" {
		out = fmt.Sprintf("oc-%d", time.Now().UnixNano())
	}
	return out
}

func handleAuthLoginPoll(raw []byte) ([]byte, error) {
	var req rpcAuthLoginPollRequest
	if errUnmarshal := json.Unmarshal(raw, &req); errUnmarshal != nil {
		return nil, errUnmarshal
	}
	stateKey := strings.TrimSpace(req.State)
	var state loginStatePayload
	if loaded, ok := pendingLoginStates.Load(stateKey); ok {
		if typed, okTyped := loaded.(loginStatePayload); okTyped {
			state = typed
		}
	}
	if state.DeviceCode == "" && req.Metadata != nil {
		state.DeviceCode = stringFromMap(req.Metadata, "device_code")
		state.UserCode = stringFromMap(req.Metadata, "user_code")
		state.ConsoleURL = stringFromMap(req.Metadata, "console_url")
	}
	if state.DeviceCode == "" {
		state.DeviceCode = stateKey
	}
	if state.DeviceCode == "" {
		return okEnvelope(pluginapi.AuthLoginPollResponse{
			Status:  pluginapi.AuthLoginStatusError,
			Message: "invalid login state",
		})
	}

	cfg := currentConfig()
	consoleURL := strings.TrimRight(firstNonEmpty(state.ConsoleURL, cfg.ConsoleURL), "/")
	pollBody, _ := marshalJSONNoEscape(map[string]any{
		"grant_type":  "urn:ietf:params:oauth:grant-type:device_code",
		"device_code": state.DeviceCode,
		"client_id":   defaultOAuthClientID,
	})
	resp, errHTTP := doHostHTTP(req.HostCallbackID, http.MethodPost, consoleURL+"/auth/device/token", http.Header{
		"Content-Type": []string{"application/json"},
		"Accept":       []string{"application/json"},
		"User-Agent":   []string{cfg.UserAgent},
	}, pollBody)
	if errHTTP != nil {
		return okEnvelope(pluginapi.AuthLoginPollResponse{
			Status:  pluginapi.AuthLoginStatusPending,
			Message: errHTTP.Error(),
		})
	}

	var tokenResp deviceTokenResponse
	if errUnmarshal := json.Unmarshal(resp.Body, &tokenResp); errUnmarshal != nil {
		return okEnvelope(pluginapi.AuthLoginPollResponse{
			Status:  pluginapi.AuthLoginStatusPending,
			Message: "waiting for device authorization",
		})
	}

	if tokenResp.AccessToken == "" {
		switch strings.ToLower(strings.TrimSpace(tokenResp.Error)) {
		case "", "authorization_pending", "slow_down":
			msg := "Waiting for browser approval"
			if state.UserCode != "" {
				msg = fmt.Sprintf("Waiting for approval (code: %s)", state.UserCode)
			}
			return okEnvelope(pluginapi.AuthLoginPollResponse{
				Status:  pluginapi.AuthLoginStatusPending,
				Message: msg,
			})
		default:
			return okEnvelope(pluginapi.AuthLoginPollResponse{
				Status:  pluginapi.AuthLoginStatusError,
				Message: "Device authorization failed: " + tokenResp.Error,
			})
		}
	}

	cred, errComplete := completeOAuthCredential(req.HostCallbackID, consoleURL, tokenResp)
	if errComplete != nil {
		return okEnvelope(pluginapi.AuthLoginPollResponse{
			Status:  pluginapi.AuthLoginStatusError,
			Message: errComplete.Error(),
		})
	}
	pendingLoginStates.Delete(stateKey)
	authData := buildAuthDataFromCredential(cred, "", "")
	return okEnvelope(pluginapi.AuthLoginPollResponse{
		Status:  pluginapi.AuthLoginStatusSuccess,
		Message: "OpenCode Go login complete",
		Auth:    authData,
	})
}

func completeOAuthCredential(hostCallbackID, consoleURL string, tokenResp deviceTokenResponse) (storedCredential, error) {
	cfg := currentConfig()
	headers := http.Header{
		"Authorization": []string{"Bearer " + tokenResp.AccessToken},
		"Accept":        []string{"application/json"},
		"User-Agent":    []string{cfg.UserAgent},
	}

	var user consoleUser
	if userResp, errUser := doHostHTTP(hostCallbackID, http.MethodGet, consoleURL+"/api/user", headers, nil); errUser == nil && userResp.StatusCode == http.StatusOK {
		_ = json.Unmarshal(userResp.Body, &user)
	}

	var orgs []consoleOrg
	if orgsResp, errOrgs := doHostHTTP(hostCallbackID, http.MethodGet, consoleURL+"/api/orgs", headers, nil); errOrgs == nil && orgsResp.StatusCode == http.StatusOK {
		_ = json.Unmarshal(orgsResp.Body, &orgs)
	}

	var selectedOrg consoleOrg
	if tokenResp.OrgID != nil && strings.TrimSpace(*tokenResp.OrgID) != "" {
		targetOrg := strings.TrimSpace(*tokenResp.OrgID)
		selectedOrg.ID = targetOrg
		for _, o := range orgs {
			if o.ID == targetOrg {
				selectedOrg = o
				break
			}
		}
	} else if len(orgs) > 0 {
		sort.Slice(orgs, func(i, j int) bool {
			if orgs[i].Name != orgs[j].Name {
				return orgs[i].Name < orgs[j].Name
			}
			return orgs[i].ID < orgs[j].ID
		})
		selectedOrg = orgs[0]
	}

	expiresIn := tokenResp.ExpiresIn
	if expiresIn <= 0 {
		expiresIn = 2592000
	}
	return storedCredential{
		Type:         pluginID,
		AccessToken:  tokenResp.AccessToken,
		RefreshToken: tokenResp.RefreshToken,
		ExpiresAtMs:  time.Now().UTC().Add(time.Duration(expiresIn) * time.Second).UnixMilli(),
		ConsoleURL:   consoleURL,
		AccountID:    user.ID,
		Email:        user.Email,
		OrgID:        selectedOrg.ID,
		OrgName:      selectedOrg.Name,
	}, nil
}

func handleAuthRefresh(raw []byte) ([]byte, error) {
	var req rpcAuthRefreshRequest
	if errUnmarshal := json.Unmarshal(raw, &req); errUnmarshal != nil {
		return nil, errUnmarshal
	}
	cred, okParse := parseStoredCredential(req.StorageJSON)
	if !okParse {
		return nil, fmt.Errorf("cannot parse OpenCode Go credential for refresh")
	}
	if strings.TrimSpace(cred.RefreshToken) == "" {
		// API key credentials do not need refresh
		authData := buildAuthDataFromCredential(cred, "", "")
		authData.ID = req.AuthID
		return okEnvelope(pluginapi.AuthRefreshResponse{
			Auth: authData,
		})
	}

	cfg := currentConfig()
	consoleURL := strings.TrimRight(firstNonEmpty(cred.ConsoleURL, cfg.ConsoleURL), "/")
	refreshBody, _ := marshalJSONNoEscape(map[string]any{
		"grant_type":    "refresh_token",
		"refresh_token": cred.RefreshToken,
		"client_id":     defaultOAuthClientID,
	})
	resp, errHTTP := doHostHTTP(req.HostCallbackID, http.MethodPost, consoleURL+"/auth/device/token", http.Header{
		"Content-Type": []string{"application/json"},
		"Accept":       []string{"application/json"},
		"User-Agent":   []string{cfg.UserAgent},
	}, refreshBody)
	if errHTTP != nil {
		return nil, fmt.Errorf("refresh OpenCode token: %w", errHTTP)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, statusError{
			code:       "refresh_failed",
			message:    fmt.Sprintf("refresh OpenCode token HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(resp.Body))),
			httpStatus: resp.StatusCode,
		}
	}

	var tokenResp deviceTokenResponse
	if errUnmarshal := json.Unmarshal(resp.Body, &tokenResp); errUnmarshal != nil || tokenResp.AccessToken == "" {
		return nil, fmt.Errorf("invalid OpenCode refresh token response")
	}

	cred.AccessToken = tokenResp.AccessToken
	if tokenResp.RefreshToken != "" {
		cred.RefreshToken = tokenResp.RefreshToken
	}
	expiresIn := tokenResp.ExpiresIn
	if expiresIn <= 0 {
		expiresIn = 2592000
	}
	cred.ExpiresAtMs = time.Now().UTC().Add(time.Duration(expiresIn) * time.Second).UnixMilli()
	if tokenResp.OrgID != nil && strings.TrimSpace(*tokenResp.OrgID) != "" {
		newOrg := strings.TrimSpace(*tokenResp.OrgID)
		if cred.OrgID != newOrg {
			cred.OrgID = newOrg
			cred.OrgName = newOrg
		}
	}

	authData := buildAuthDataFromCredential(cred, "", "")
	authData.ID = req.AuthID
	return okEnvelope(pluginapi.AuthRefreshResponse{
		Auth:             authData,
		NextRefreshAfter: authData.NextRefreshAfter,
	})
}

func doHostHTTP(hostCallbackID, method, targetURL string, headers http.Header, body []byte) (pluginapi.HTTPResponse, error) {
	payload := map[string]any{
		"host_callback_id": hostCallbackID,
		"method":           method,
		"url":              targetURL,
		"headers":          headers,
		"body":             body,
	}
	raw, errCall := callHost(pluginabi.MethodHostHTTPDo, payload)
	if errCall != nil {
		return pluginapi.HTTPResponse{}, errCall
	}
	var resp pluginapi.HTTPResponse
	if errUnmarshal := json.Unmarshal(raw, &resp); errUnmarshal != nil {
		return pluginapi.HTTPResponse{}, errUnmarshal
	}
	return resp, nil
}

func sanitizeFileNamePart(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			b.WriteRune(r)
		} else if r == '@' {
			b.WriteRune('-')
		}
	}
	out := strings.Trim(b.String(), "-_.")
	if out == "" {
		return "account"
	}
	return out
}

func stringFromMap(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if s, okStr := v.(string); okStr && strings.TrimSpace(s) != "" {
				return strings.TrimSpace(s)
			}
		}
	}
	return ""
}

func int64FromMap(m map[string]any, keys ...string) int64 {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			switch typed := v.(type) {
			case float64:
				return int64(typed)
			case int64:
				return typed
			case int:
				return int64(typed)
			case json.Number:
				if n, err := typed.Int64(); err == nil {
					return n
				}
			}
		}
	}
	return 0
}
