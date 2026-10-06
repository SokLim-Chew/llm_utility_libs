"""
BigQuery read/write-back helpers.

Read: streamed in batches (batch_size rows at a time) so large tables don't
have to be pulled into memory at once, and so progress can be checkpointed
batch-by-batch (if the pipeline dies halfway, you've already written the
first N batches back to the table).

Write-back: BigQuery doesn't support cheap row-by-row UPDATEs at scale, so
each batch's results are loaded into a staging table and reconciled with a
single MERGE, keyed on `id_field`. This is the standard BQ pattern for
"add/update a derived column for a set of rows".
"""
import logging
import uuid
from typing import Callable, List, Optional

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

logger = logging.getLogger(__name__)


def load_reference_text(
    query: str,
    client: bigquery.Client,
    formatter: Optional[Callable[[pd.DataFrame], str]] = None,
) -> str:
    """Runs a one-off reference/lookup query and formats it into a single
    string for prompt injection -- e.g. a master list of valid values, a
    taxonomy, or a small dimension table. Unlike `read_table_batches`, this
    is NOT batched or chunked: it's meant for small reference tables (low
    thousands of rows at most) that are the same for every row being
    processed, loaded once per pipeline run rather than once per row.

    Args:
        query: SQL query returning the reference data.
        client: an existing bigquery.Client.
        formatter: optional function that takes the result DataFrame and
            returns a string. If omitted, defaults to a "- value" bulleted
            list built from the first returned column.
    """
    df = client.query(query).result().to_dataframe()
    if formatter:
        return formatter(df)
    first_col = df.columns[0]
    return "\n".join(f"- {v}" for v in df[first_col].astype(str))


def read_table_batches(
    table: str,
    id_field: str,
    processed_field: str,
    batch_size: int,
    client: bigquery.Client,
    where_clause: str = None,
    custom_query: str = None,
    carry_fields: Optional[List[str]] = None,
):
    """Yields pandas DataFrames of up to `batch_size` rows: the id column,
    the field to send to the LLM, and any `carry_fields` to preserve
    alongside the AI result (useful when writing to a different output
    table that should be self-contained, e.g. include `description`,
    `cable_count`, etc. rather than just the id + AI result).

    If `custom_query` is given, it's used verbatim instead of the default
    `SELECT {id_field}, {processed_field}[, carry_fields...] FROM {table}`
    -- use this for a composite/derived key, a join, or filtering more
    complex than `where_clause`. The query MUST return columns named
    exactly `id_field`, `processed_field`, and any `carry_fields`.
    """
    if custom_query:
        query = custom_query
    else:
        select_cols = [id_field, processed_field] + list(carry_fields or [])
        query = f"SELECT {', '.join(select_cols)} FROM `{table}`"
        if where_clause:
            query += f" WHERE {where_clause}"

    query_job = client.query(query)
    for chunk in query_job.result(page_size=batch_size).to_dataframe_iterable():
        # to_dataframe_iterable's chunking is driven by the API page size,
        # not guaranteed to match batch_size exactly, so re-chunk locally.
        for start in range(0, len(chunk), batch_size):
            yield chunk.iloc[start : start + batch_size].reset_index(drop=True)


def write_results_merge(
    table: str,
    id_field: str,
    ai_return_field: str,
    results_df: pd.DataFrame,
    client: bigquery.Client,
):
    """Writes a batch of results (id_field + ai_return_field + any other
    columns present in `results_df`, e.g. carry_fields) back into `table`,
    handling BOTH of these cases the same way:

      - `table` already exists (typical for in-place enrichment, where
        output_table == input_table): rows are matched on `id_field` and
        UPDATEd. Unmatched rows (rare -- normally shouldn't happen for a
        true in-place update) are INSERTed too, so this stays correct even
        if `table` doesn't yet contain every id.

      - `table` does not exist yet (typical when writing to a different,
        new output table): on first write, the table is created directly
        from the batch's data (schema + rows), containing whatever columns
        `results_df` has (id_field, carry_fields, ai_return_field).
        Subsequent batches then fall into the MERGE/upsert path above.

    Either way, the caller doesn't need to know or specify which case
    applies -- pass only the columns you want written (id_field +
    optional carry_fields + ai_return_field) via `results_df`.
    """
    staging_table = f"{table}_stg_{ai_return_field}_{uuid.uuid4().hex[:8]}"

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    load_job = client.load_table_from_dataframe(results_df, staging_table, job_config=job_config)
    load_job.result()

    try:
        table_exists = True
        try:
            client.get_table(table)
        except NotFound:
            table_exists = False

        if not table_exists:
            # First write to a brand-new output table: create it directly
            # from this batch's data (schema inferred from results_df).
            copy_job = client.copy_table(staging_table, table)
            copy_job.result()
            logger.info(
                "Created new output table %s from first batch (%s rows)",
                table, len(results_df),
            )
        else:
            other_cols = [c for c in results_df.columns if c != id_field]

            # Make sure every non-key column exists on the target table
            # (harmless no-op for columns that are already there).
            for col in other_cols:
                client.query(
                    f"ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS {col} STRING"
                ).result()

            update_set = ", ".join(f"T.{c} = S.{c}" for c in other_cols)
            insert_cols = ", ".join([id_field] + other_cols)
            insert_vals = ", ".join(f"S.{c}" for c in [id_field] + other_cols)

            merge_sql = f"""
            MERGE `{table}` T
            USING `{staging_table}` S
            ON T.{id_field} = S.{id_field}
            WHEN MATCHED THEN
              UPDATE SET {update_set}
            WHEN NOT MATCHED THEN
              INSERT ({insert_cols}) VALUES ({insert_vals})
            """
            client.query(merge_sql).result()
            logger.info("Upserted %s rows into %s", len(results_df), table)
    finally:
        client.delete_table(staging_table, not_found_ok=True)
