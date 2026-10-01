# Web search as an MCP tool in LiteLLM

Not part of vllmapp: how our LiteLLM gives clients (VS Code, OpenCode, Hermes) a `web_search`
tool backed by our own SearXNG.

1. A search tool `searxng` in LiteLLM (provider `searxng`). LiteLLM finds SearXNG through the
   env `SEARXNG_API_BASE`, since saving the tool in the UI drops its `api_base`.
2. A LiteLLM key that may only call `/v1/search/searxng` (`allowed_routes`).
3. An MCP server in LiteLLM built from `web-search.openapi.json` in this folder, which calls
   that endpoint with the key. Clients connect to LiteLLM's MCP endpoint with their own key.

SearXNG must allow `format=json` (`search.formats` in its settings.yml) and use engines that
don't block the server's IP.
