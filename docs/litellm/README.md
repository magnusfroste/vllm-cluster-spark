# Web search as an MCP tool in LiteLLM

Not part of vllmapp: how our LiteLLM gives clients (VS Code, OpenCode, Hermes) a `web_search`
tool backed by our own SearXNG.

1. A search tool `searxng` in LiteLLM (provider `searxng`). LiteLLM finds SearXNG through the
   env `SEARXNG_API_BASE`, since saving the tool in the UI drops its `api_base`.
2. A LiteLLM key that may only call `/v1/search/searxng` (`allowed_routes`).
3. An MCP server in LiteLLM built from `web-search.openapi.json` in this folder, which calls
   that endpoint with the key. Clients connect to LiteLLM's MCP endpoint with their own key.

Give the key as `static_headers` (`{"Authorization": "Bearer …"}`), not as `auth_type:
bearer_token`: with `auth_type` LiteLLM 1.102 fails to list the server's tools.

## Connecting a client

The tool shows up as `web_search-web_search`. In VS Code, `.vscode/mcp.json`:

```json
{
  "servers": {
    "liteit": {
      "type": "http",
      "url": "https://api.liteit.se/mcp/",
      "headers": { "x-litellm-api-key": "Bearer ${input:litellm-key}" }
    }
  },
  "inputs": [
    { "id": "litellm-key", "type": "promptString", "description": "LiteLLM key", "password": true }
  ]
}
```

SearXNG must allow `format=json` (`search.formats` in its settings.yml) and use engines that
don't block the server's IP.
