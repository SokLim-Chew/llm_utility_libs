"""
LLM gateway client.

Design choice -- async with bounded concurrency, not sequential loop and
not unbounded parallel:

  - LLM calls are I/O-bound (mostly waiting on network/model latency), so a
    plain for-loop wastes most of the wall-clock time idle. asyncio lets us
    have many requests in flight at once.
  - Unbounded parallelism (fire all requests at once) risks tripping the
    gateway's rate limits and can blow through burst quotas. A
    `Semaphore(concurrency)` caps how many requests are in flight at any
    moment -- tune `concurrency` per model type / gateway limits.
  - Simple retry-with-backoff on transient errors, since gateways
    occasionally 429/5xx under load.

If a request ultimately fails, we return a sentinel string rather than
raising, so one bad row doesn't kill an entire batch -- the caller can
filter/inspect `__LLM_ERROR__` rows afterward.
"""
import asyncio
import logging
from typing import List, Optional

import httpx

from .connectivity import get_llm_credentials, AIRFLOW_CONN_ID_DEFAULT

logger = logging.getLogger(__name__)

ERROR_PREFIX = "__LLM_ERROR__"


class LLMClient:
    def __init__(
        self,
        model: str,
        conn_id: str = AIRFLOW_CONN_ID_DEFAULT,
        timeout: float = 60.0,
        max_retries: int = 3,
        system_prompt: Optional[str] = None,
    ):
        creds = get_llm_credentials(conn_id)
        self.base_url = creds.base_url
        self.api_key = creds.api_key
        self.extra_headers = creds.extra_headers
        self.model = model
        self.timeout = timeout
        self.max_retries = max_retries
        self.system_prompt = system_prompt

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            **self.extra_headers,
        }

    def _payload(self, prompt: str) -> dict:
        messages = []
        if self.system_prompt:
            messages.append({"role": "system", "content": self.system_prompt})
        messages.append({"role": "user", "content": prompt})
        return {"model": self.model, "messages": messages}

    async def _call_one(self, client: httpx.AsyncClient, prompt: str, sem: asyncio.Semaphore) -> str:
        async with sem:
            for attempt in range(1, self.max_retries + 1):
                try:
                    resp = await client.post(
                        f"{self.base_url}/chat/completions",
                        json=self._payload(prompt),
                        headers=self._headers(),
                        timeout=self.timeout,
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    return data["choices"][0]["message"]["content"]
                except (httpx.HTTPStatusError, httpx.TransportError, KeyError, IndexError) as e:
                    if attempt == self.max_retries:
                        logger.warning("LLM call failed after %s attempts: %s", attempt, e)
                        return f"{ERROR_PREFIX}: {e}"
                    await asyncio.sleep(2 ** attempt)  # exponential backoff

    async def run_batch(self, prompts: List[str], concurrency: int = 10) -> List[str]:
        sem = asyncio.Semaphore(concurrency)
        async with httpx.AsyncClient() as client:
            tasks = [self._call_one(client, p, sem) for p in prompts]
            return await asyncio.gather(*tasks)

    def run_batch_sync(self, prompts: List[str], concurrency: int = 10) -> List[str]:
        """Convenience wrapper for callers (e.g. Airflow PythonOperators)
        that are not themselves async."""
        return asyncio.run(self.run_batch(prompts, concurrency=concurrency))
