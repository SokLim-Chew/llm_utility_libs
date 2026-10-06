"""
Lightweight unit tests that don't require live Airflow, BigQuery, or an LLM
gateway -- suitable for CI.
"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from edpllm.config import get_model_for_type, DEFAULT_MODEL_MAP
from edpllm.prompts import load_prompt_template, render_prompt


def test_default_model_map():
    assert get_model_for_type("processing") == DEFAULT_MODEL_MAP["processing"]
    assert get_model_for_type("reasoning") == DEFAULT_MODEL_MAP["reasoning"]


def test_invalid_model_type_raises():
    try:
        get_model_for_type("fast")
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_prompt_render_basic(tmp_path=None):
    tmp_dir = tempfile.mkdtemp()
    prompt_file = os.path.join(tmp_dir, "test.md")
    with open(prompt_file, "w") as f:
        f.write("Summarize: {{ content }}")

    template = load_prompt_template(prompt_file)
    rendered = render_prompt(template, content="hello world")
    assert rendered == "Summarize: hello world"


if __name__ == "__main__":
    test_default_model_map()
    test_invalid_model_type_raises()
    test_prompt_render_basic()
    print("All tests passed.")
