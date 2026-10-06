# edpllm

Lightweight, reusable utility library for LLM-powered data pipelines on the
EDP (BigQuery + Airflow). One function, `edpllm()`, replaces the
copy-pasted "connect to gateway / loop over rows / call model / write back"
boilerplate that would otherwise live in every DAG.

## Install

From the internal GitHub repo:

```bash
pip install git+https://github.com/<your-org>/edpllm.git
```

or add to a `requirements.txt` used by your Airflow image build.

## One-time setup

**Connectivity** (pick one):

- **Airflow Connection (recommended)**: create a connection named
  `edp_llm_gateway`
  - Conn Type: `HTTP`
  - Host: your LLM gateway base URL (e.g. `https://llm-gateway.mycorp.internal/v1`)
  - Password: your API token
  - Extra (optional JSON): `{"headers": {"X-Team": "data-eng"}}`

- **Environment variables**, if the key is uploaded directly to the
  Airflow server/worker:
  - `EDPLLM_API_KEY` (required)
  - `EDPLLM_GATEWAY_URL` (optional — defaults to a placeholder)

**Model selection** (optional override): by default,
`"processing"` -> `gemini-2.5-flash` and `"reasoning"` -> `gemini-2.5-pro`.
To override without a code change, set an Airflow Variable
`edpllm_model_map` to JSON, e.g.:

```json
{"processing": "gemini-2.5-flash-lite", "reasoning": "gemini-2.5-pro"}
```

## Usage

```python
from edpllm import edpllm

rows_processed = edpllm(
    prompt_path="prompts/summarize.md",      # markdown, supports Jinja2 {{ }} and {% include %}
    model_type="processing",                 # "processing" | "reasoning"
    input_table="my-project.my_dataset.articles",
    id_field="article_id",                   # unique key, needed to write results back
    processed_field="combined_text",         # single pre-joined column sent to the LLM
    ai_return_field="llm_summary",           # column created/updated with the LLM's output
    where_clause="llm_summary IS NULL",      # optional: only process unprocessed rows
    batch_size=500,
    concurrency=15,
)
```

Drop this straight into a `PythonOperator` — see `examples/example_dag.py`.

## Design notes

- **Prompts as markdown files**: versioned and reviewable like code.
  `{% include 'shared/x.md' %}` lets pipelines share boilerplate (tone,
  output format, few-shot examples) via files in the repo.
- **Single `processed_field`**: any joining/concatenation of source columns
  is expected to happen upstream (e.g. in the BQ query or a prior task),
  keeping this function's contract simple.
- **Reference/lookup data**: `reference_query` loads a small shared
  dataset (a master list, taxonomy, valid-value list) ONCE per pipeline
  run and injects it into every prompt as `{{ reference_data }}` --
  distinct from `processed_field`, which varies per row.
- **Composite/derived keys and complex reads**: pass `input_query` for a
  full custom SQL read (composite key via `CONCAT`, joins, extra
  filtering) instead of the default plain-table read.
- **Async with bounded concurrency**: LLM calls are I/O-bound, so requests
  within a batch run concurrently (`asyncio` + `Semaphore(concurrency)`)
  rather than sequentially — much faster, while still capped to avoid
  tripping gateway rate limits. Tune `concurrency` down for `"reasoning"`
  workloads (higher latency/cost per call) and up for `"processing"`.
- **Batched read + write-back**: rows are read, processed, and merged back
  into BigQuery in chunks of `batch_size`, so a mid-run failure doesn't
  lose already-completed work and memory stays bounded regardless of table
  size.
- **Write-back handles both in-place and new-table output the same way**:
  `output_table` can be the same as `input_table` (adds/updates a column
  on the existing table) or a different table entirely. If it doesn't
  exist yet, it's created from the first batch; either way, subsequent
  batches upsert (update matched rows by `id_field`, insert new ones) via
  a staging table + `MERGE`. Use `carry_fields` to preserve extra source
  columns (e.g. `description`) when writing to a new, self-contained
  output table.
- **Failure isolation**: a single row's LLM call failing (after retries)
  writes an `__LLM_ERROR__: ...` sentinel into that row rather than
  failing the whole batch — filter on this column to find rows to retry.

## Repo layout

```
edpllm/
  __init__.py
  config.py          # model_type -> model name
  connectivity.py     # Airflow Connection / env var credential resolution
  client.py           # async LLM gateway client (bounded concurrency, retries)
  prompts.py           # markdown + Jinja2 prompt loading
  bigquery_io.py      # batched reads, MERGE-based write-back
  core.py              # edpllm() main entrypoint
examples/
  example_dag.py
  prompts/summarize.md
  prompts/classify_risk.md
  prompts/shared/system_policy.md
tests/
  test_config_and_prompts.py
```
