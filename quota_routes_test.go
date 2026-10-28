package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestStatsResourceAndManagementRoutes(t *testing.T) {
	cases := []struct{ path, contentType, body string }{
		{"/v0/resource/plugins/opencode-go-oauth/stats", "text/html", "<html"},
		{"/v0/management/plugins/opencode-go-oauth/stats", "application/json", `"models"`},
	}
	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			request, err := json.Marshal(managementRequest{Method: http.MethodGet, Path: tc.path})
			if err != nil {
				t.Fatal(err)
			}
			raw, err := handleManagement(request)
			if err != nil {
				t.Fatal(err)
			}
			var env envelope
			if err := json.Unmarshal(raw, &env); err != nil {
				t.Fatal(err)
			}
			if !env.OK {
				t.Fatalf("request failed: %s", raw)
			}
			var response managementResponse
			if err := json.Unmarshal(env.Result, &response); err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != http.StatusOK {
				t.Fatalf("status: %d", response.StatusCode)
			}
			if got := response.Headers.Get("Content-Type"); !strings.HasPrefix(got, tc.contentType) {
				t.Errorf("content type = %q, want %q", got, tc.contentType)
			}
			if !strings.Contains(string(response.Body), tc.body) {
				t.Errorf("body does not contain %q", tc.body)
			}
		})
	}
}
