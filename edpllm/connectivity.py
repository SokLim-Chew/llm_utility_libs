"""
LLM gateway connectivity.

Goal: nobody on the team should ever hardcode an API key or gateway URL in
a pipeline again. Credentials are resolved in this order:

  1. Airflow Connection (conn_id, default "edp_llm_gateway")
       - Connection Type: HTTP
       - Host:            <gateway base URL>            e.g. https://llm-gateway.mycorp.internal/v1
       - Password:        <API token>
       - Extra (JSON):    {"headers": {"X-Team": "data-eng"}}   (optional)

  2. Environment variables, for the case where the key is uploaded directly
     to the Airflow worker/server and exposed as an env var:
       - EDPLLM_API_KEY      (required)
       - EDPLLM_GATEWAY_URL  (optional, has a dummy default below)

This module never logs the resolved API key.
"""
import os
from dataclasses import dataclass, field
from typing import Dict

AIRFLOW_CONN_ID_DEFAULT = "edp_llm_gateway"
ENV_VAR_API_KEY = "EDPLLM_API_KEY"
ENV_VAR_BASE_URL = "EDPLLM_GATEWAY_URL"

# Dummy placeholder -- replace with your real internal gateway endpoint.
DUMMY_GATEWAY_URL = "https://llm-gateway.internal.example.com/v1"


@dataclass
class LLMGatewayCredentials:
    base_url: str
    api_key: str
    extra_headers: Dict[str, str] = field(default_factory=dict)


def get_llm_credentials(conn_id: str = AIRFLOW_CONN_ID_DEFAULT) -> LLMGatewayCredentials:
    # 1. Try Airflow Connection first.
    try:
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection(conn_id)
        base_url = conn.host
        api_key = conn.password
        if base_url and api_key:
            extra = conn.extra_dejson or {}
            return LLMGatewayCredentials(
                base_url=base_url.rstrip("/"),
                api_key=api_key,
                extra_headers=extra.get("headers", {}),
            )
    except Exception:
        # Airflow not available, connection not configured, etc. -- fall
        # through to the env var path.
        pass

    # 2. Fall back to environment variables (key uploaded to the server).
    api_key = os.environ.get(ENV_VAR_API_KEY)
    base_url = os.environ.get(ENV_VAR_BASE_URL, DUMMY_GATEWAY_URL)
    if not api_key:
        raise RuntimeError(
            f"No LLM credentials found. Configure an Airflow connection "
            f"named '{conn_id}' (host=gateway URL, password=API key), or "
            f"set the '{ENV_VAR_API_KEY}' environment variable on the "
            f"Airflow server/worker."
        )
    return LLMGatewayCredentials(base_url=base_url.rstrip("/"), api_key=api_key, extra_headers={})
