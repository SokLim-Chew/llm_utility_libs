"""
Standardized model-type -> model-name resolution.

We expose exactly two logical model types to pipeline authors, so nobody
has to know/care about specific model IDs (which change over time):

  - "processing": fast/cheap model for high-volume, low-complexity tasks
                   (classification, extraction, tagging, short summaries)
  - "reasoning":   higher-capability model for complex, multi-step, or
                   judgment-heavy tasks

Defaults point at Google's current Gemini lineup (this EDP runs on
BigQuery/GCP). Override without a code change via the Airflow Variable
`edpllm_model_map`, e.g.:

    {"processing": "gemini-2.5-flash-lite", "reasoning": "gemini-2.5-pro"}
"""
import json
import logging

logger = logging.getLogger(__name__)

VALID_MODEL_TYPES = ("processing", "reasoning")

DEFAULT_MODEL_MAP = {
    "processing": "gemini-2.5-flash",
    "reasoning": "gemini-2.5-pro",
}

AIRFLOW_VARIABLE_NAME = "edpllm_model_map"


def get_model_for_type(model_type: str) -> str:
    """Resolve a logical model_type ("processing"/"reasoning") to a concrete
    model name, honoring an optional Airflow Variable override."""
    if model_type not in VALID_MODEL_TYPES:
        raise ValueError(
            f"model_type must be one of {VALID_MODEL_TYPES}, got {model_type!r}"
        )

    override = {}
    try:
        from airflow.models import Variable

        raw = Variable.get(AIRFLOW_VARIABLE_NAME, default_var="{}")
        override = json.loads(raw)
    except Exception as e:
        # Not running inside Airflow, Variable not set, or bad JSON -- fall
        # back to defaults silently. This keeps the library usable outside
        # Airflow too (e.g. local testing, notebooks).
        logger.debug("No edpllm_model_map override applied: %s", e)

    model_map = {**DEFAULT_MODEL_MAP, **override}
    return model_map[model_type]
