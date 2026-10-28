# CLIProxyAPI OpenCode Go Plugin

This ABI v1 plugin adds **OpenCode Go / Go Plus** (`opencode-go`) support to CLIProxyAPI, including:
- **OAuth Device Login**: Sign in directly from the CLIProxyAPI Management Console via `https://opencode.ai/console/auth/device/code` (or upload a JSON credential with `api_key` / `access_token`). Automatically refreshes tokens and attaches `x-opencode-org-id` and `x-opencode-session` (`cpa-main` by default).
- **Model catalog**: Uses the live OpenCode Go `/inference/go/v1/models` list and enriches it with metadata from `models.opencode.ai`. A verified snapshot is used at startup until a live refresh succeeds; the last successful list is retained if a refresh fails. The live list is not a definitive test of every model that may accept requests: verified models omitted from it can be added with `extra_models`.
- **Quota & usage**: Reads each credential's 5-hour, weekly, and monthly meters from OpenCode. The dashboard displays upstream percentages and reset times, and labels per-model request telemetry as local to the current CPA process. The CPA quota card includes actual dollar amounts when the Console status endpoint returns them; if it does not, it falls back to percentage-only usage. These upstream endpoints are not documented as a stable public API.
- **Localized dashboard**: The plugin resource page follows the CPA language setting. To load account data inside the resource page, the management key must be available to the page (for example, by using CPA's “Remember password” option); otherwise it displays an error instead of fabricated zero usage.


## Build

```bash
make test
```

Produces `build/opencode-go.so`.

## Install from the management interface

Add this registry URL to `plugins.store-sources`:

```yaml
plugins:
  enabled: true
  store-sources:
    - "https://raw.githubusercontent.com/esauvisky/CLIProxyAPI-OpenCode-Go-Plugin/main/registry.json"
```

## Configuration

```yaml
plugins:
  enabled: true
  configs:
    opencode-go:
      enabled: true
      priority: 10
      session_id: "cpa-main"
      # Optional: models you have verified work upstream but are absent from its list.
      # Map each model ID to its upstream protocol: openai, openai-response, claude, or gemini.
      extra_models: {}
      # Example: extra_models: {"verified-model-id": "openai"}
```
