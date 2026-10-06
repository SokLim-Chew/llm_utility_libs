"""
edpllm() -- the single reusable entrypoint for LLM data pipelines on the EDP.

    from edpllm import edpllm

    edpllm(
        prompt_path="prompts/summarize.md",
        model_type="processing",                       # or "reasoning"
        input_table="my-project.my_dataset.articles",
        id_field="article_id",
        processed_field="combined_text",
        ai_return_field="llm_summary",
    )

What it does, per batch:
  1. Read `batch_size` rows (id_field, processed_field) from BigQuery.
  2. Render the markdown prompt template once per row (content=<field value>).
  3. Send all prompts for the batch to the LLM gateway concurrently
     (bounded by `concurrency`).
  4. Write the (id_field, ai_return_field) results back to `output_table`
     (defaults to input_table) via MERGE.
  5. Move to the next batch.

Batching + per-batch write-back means a failure partway through a large
table doesn't lose already-processed work, and memory stays bounded
regardless of table size.
"""
import logging
from typing import Callable, List, Optional, Dict, Any

import pandas as pd
from google.cloud import bigquery

from .config import get_model_for_type
from .client import LLMClient
from .prompts import load_prompt_template, render_prompt, infer_prompt_version, version_to_field_suffix
from .bigquery_io import read_table_batches, write_results_merge, load_reference_text
from .connectivity import AIRFLOW_CONN_ID_DEFAULT

logger = logging.getLogger(__name__)


def edpllm(
    prompt_path: str,
    model_type: str,
    input_table: str,
    id_field: str,
    processed_field: str,
    ai_return_field: str,
    output_table: Optional[str] = None,
    input_query: Optional[str] = None,
    where_clause: Optional[str] = None,
    batch_size: int = 500,
    concurrency: int = 10,
    conn_id: str = AIRFLOW_CONN_ID_DEFAULT,
    carry_fields: Optional[List[str]] = None,
    system_prompt: Optional[str] = None,
    extra_prompt_context: Optional[Dict[str, Any]] = None,
    reference_query: Optional[str] = None,
    reference_context_key: str = "reference_data",
    reference_formatter: Optional[Callable[[pd.DataFrame], str]] = None,
    auto_version_field: bool = False,
    bq_client: Optional[bigquery.Client] = None,
) -> int:
    """
    Args:
        prompt_path: path to a markdown prompt file. Rendered with Jinja2;
            the row's `processed_field` value is available as `{{ content }}`,
            plus anything passed via `extra_prompt_context`. Supports
            `{% include 'shared/x.md' %}` for shared prompt fragments.
        reference_query: optional SQL query for a lookup/reference table
            (e.g. a master list of valid values) that's the SAME for every
            row and should be injected into every prompt. Loaded ONCE at
            pipeline start (not per-batch/per-row), then made available in
            the prompt template as `{{ reference_data }}` (or whatever
            `reference_context_key` is set to). Use this instead of
            `processed_field`/joining it into `combined_text` upstream when
            the reference data is large or shared across many rows, to
            avoid repeating it in every row of the source table.
        reference_context_key: template variable name the reference text is
            exposed under. Defaults to "reference_data".
        reference_formatter: optional function(DataFrame) -> str controlling
            how the reference query's results are turned into text. Default
            is a "- value" bulleted list of the first returned column.
        auto_version_field: if True, and `prompt_path`'s filename follows
            the `{name}_{version}.md` convention (e.g. "summarize_1.1.md"),
            automatically appends the version to `ai_return_field` (e.g.
            "llm_summary" -> "llm_summary_1_1" -- dots become underscores,
            since BigQuery column names can't contain periods). Lets each
            prompt version write to its own column without hand-computing
            the field name per task. Default False (opt-in), since a
            prompt file that just happens to end in a number for unrelated
            reasons shouldn't silently change its target column.
        model_type: "processing" or "reasoning" (see edpllm.config).
        input_table: fully-qualified BigQuery table, "project.dataset.table".
            Used to build the default read query, and as the write-back
            target if `output_table` is not set. If `input_query` is
            provided, `input_table` is only used for logging, and
            `output_table` must then be set explicitly.
        id_field: unique row identifier column, used to write results back
            to the correct row. Required. If your source table doesn't
            have a single unique column, use `input_query` to build one
            (e.g. `CONCAT(sr_id, '_', linenum) AS row_key`).
        processed_field: the single column whose value is sent to the LLM.
            Any concatenation/pre-processing of multiple source columns is
            assumed to already be done upstream, into this one column.
        ai_return_field: name of the column to write the LLM's response
            into. Created automatically if it doesn't exist.
        output_table: table to write results into. Defaults to input_table
            (in-place enrichment). Required (no default) when using
            `input_query`, since the query itself may not map to a single
            writable table.
        input_query: optional full SQL query to use instead of the default
            `SELECT {id_field}, {processed_field} FROM {input_table}`. Use
            this for a composite/derived key, a join, or any row selection
            more complex than a plain table read. Must return columns
            named exactly `id_field` and `processed_field` (alias with
            `AS` if needed). Example:

                input_query='''
                    SELECT CONCAT(sr_id, "_", linenum) AS row_key,
                           description
                    FROM `project.dataset.ma_details_sub`
                    WHERE extracted_cable IS NULL
                '''

            (with id_field="row_key", processed_field="description",
            output_table set explicitly since the query result isn't
            necessarily 1:1 with a single physical table row layout).
        where_clause: optional SQL filter, e.g. "ai_return_field IS NULL"
            to only (re)process rows that haven't been processed yet.
        batch_size: rows read/processed/written per batch.
        concurrency: max in-flight LLM requests at once, per batch.
        conn_id: Airflow connection id for the LLM gateway.
        carry_fields: extra source columns to preserve alongside the AI
            result when writing back. Not needed for in-place enrichment
            (output_table == input_table) since those columns are already
            there. Needed when output_table is a DIFFERENT/new table that
            should be self-contained -- e.g. carry_fields=["description",
            "cable_count"] so the new table has that context without
            joining back to the source. write_results_merge handles both
            "table already exists" (upsert) and "table doesn't exist yet"
            (created from the first batch) automatically -- same call
            works whether output_table is the same table or a new one.
        system_prompt: optional system-role instruction sent with every call.
        extra_prompt_context: extra template variables available in the
            prompt beyond `{{ content }}`.
        bq_client: optionally inject a bigquery.Client (e.g. for tests).

    Returns:
        Total number of rows processed.
    """
    model = get_model_for_type(model_type)
    template = load_prompt_template(prompt_path)

    if auto_version_field:
        version = infer_prompt_version(prompt_path)
        if version:
            ai_return_field = f"{ai_return_field}_{version_to_field_suffix(version)}"

    llm_client = LLMClient(model=model, conn_id=conn_id, system_prompt=system_prompt)
    client = bq_client or bigquery.Client()

    if input_query and not output_table:
        raise ValueError(
            "output_table must be set explicitly when using input_query "
            "(the custom query may not map 1:1 to a single writable table)."
        )
    output_table = output_table or input_table
    extra_prompt_context = dict(extra_prompt_context or {})

    if reference_query:
        logger.info("Loading reference data (loaded once, shared across all rows)...")
        extra_prompt_context[reference_context_key] = load_reference_text(
            reference_query, client, formatter=reference_formatter
        )

    logger.info(
        "edpllm start: model=%s input=%s output=%s field=%s -> %s",
        model, input_table, output_table, processed_field, ai_return_field,
    )

    total = 0
    for batch_df in read_table_batches(
        input_table, id_field, processed_field, batch_size, client, where_clause,
        custom_query=input_query, carry_fields=carry_fields,
    ):
        prompts = [
            render_prompt(template, content=row[processed_field], **extra_prompt_context)
            for _, row in batch_df.iterrows()
        ]
        results = llm_client.run_batch_sync(prompts, concurrency=concurrency)
        batch_df[ai_return_field] = results

        # Only write id_field + explicitly requested carry_fields +
        # ai_return_field -- not processed_field itself, which stays
        # untouched unless the caller listed it in carry_fields too.
        write_cols = [id_field] + list(carry_fields or []) + [ai_return_field]
        write_results_merge(output_table, id_field, ai_return_field, batch_df[write_cols], client)

        total += len(batch_df)
        logger.info("edpllm processed %s rows so far", total)

    logger.info("edpllm done: %s rows total", total)
    return total


# Friendlier alias for teams who prefer a more descriptive import name.
run_llm_pipeline = edpllm
