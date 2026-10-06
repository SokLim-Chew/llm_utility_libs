"""
Standalone credential/connectivity check -- no BigQuery involved.

Confirms:
  1. Credentials resolve (Airflow Connection 'edp_llm_gateway', or the
     EDPLLM_API_KEY / EDPLLM_GATEWAY_URL env vars).
  2. The gateway actually accepts the token and returns a completion.

Run:  python3 quick_credential_test.py
"""
import sys
sys.path.insert(0, "edpllm")

from edpllm.connectivity import get_llm_credentials
from edpllm.config import get_model_for_type
from edpllm.client import LLMClient, ERROR_PREFIX

print("Step 1: resolving credentials...")
try:
    creds = get_llm_credentials()
    print(f"  OK -- base_url={creds.base_url}, api_key=***{creds.api_key[-4:]}")
except Exception as e:
    print(f"  FAILED: {e}")
    sys.exit(1)

print("\nStep 2: calling the gateway with model_type='processing'...")
model = get_model_for_type("processing")
print(f"  Using model: {model}")

client = LLMClient(model=model)
result = client.run_batch_sync(["Reply with exactly the word: PONG"], concurrency=1)[0]

print(f"\nResponse: {result!r}")
if result.startswith(ERROR_PREFIX):
    print("\n  FAILED -- credentials resolved but the gateway call errored. "
          "Check the error text above (401/403 = bad token, "
          "404/timeout = wrong base_url).")
    sys.exit(1)
else:
    print("\n  SUCCESS -- credentials and gateway connectivity are working.")
