"""
Prompt loading.

Prompts live as markdown files in the repo (versioned, reviewable in PRs,
diffable) rather than as inline strings in DAGs. We use Jinja2 so a prompt
can:

  - reference row data:           {{ content }}
  - pull in shared/external text: {% include 'shared/system_policy.md' %}
  - do light logic if needed:     {% if some_flag %} ... {% endif %}

`{% include %}` paths are resolved relative to the prompt file's own
directory, so a repo can share boilerplate (tone-of-voice, output-format
instructions, few-shot examples, etc.) across many prompt files.
"""
import re
from pathlib import Path
from typing import Optional

from jinja2 import Environment, FileSystemLoader, StrictUndefined, Template

# Matches a trailing "_<version>" segment, e.g. "summarize_1.1" -> "1.1",
# "classify_risk_2" -> "2". Requires at least one non-version segment
# before it, so "1.1.md" alone (no name) won't match.
_VERSION_SUFFIX_RE = re.compile(r"^.+_(\d+(?:\.\d+)*)$")


def infer_prompt_version(prompt_path: str) -> Optional[str]:
    """Extracts a version suffix from a prompt filename following the
    `{name}_{version}.md` convention, e.g. "summarize_1.1.md" -> "1.1".
    Returns None if the filename stem doesn't end in a numeric/dotted
    version segment (e.g. "summarize.md", "classify_risk.md")."""
    stem = Path(prompt_path).stem
    match = _VERSION_SUFFIX_RE.match(stem)
    return match.group(1) if match else None


def version_to_field_suffix(version: str) -> str:
    """Converts a version string to something safe to append to a BigQuery
    column name. BigQuery column names cannot contain periods, so "1.1"
    becomes "1_1" (-> ai_return_field="llm_summary_1_1", not "...1.1")."""
    return version.replace(".", "_")


def load_prompt_template(prompt_path: str) -> Template:
    path = Path(prompt_path)
    if not path.exists():
        raise FileNotFoundError(f"Prompt file not found: {prompt_path}")

    env = Environment(
        loader=FileSystemLoader(str(path.parent)),
        undefined=StrictUndefined,  # fail loudly on a missing template var
        trim_blocks=True,
        lstrip_blocks=True,
    )
    return env.get_template(path.name)


def render_prompt(template: Template, **context) -> str:
    return template.render(**context)
