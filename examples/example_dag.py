"""
Example DAG: two tasks using the shared edpllm utility library.

Task 1 uses the "processing" model type to summarize articles.
Task 2 uses the "reasoning" model type to classify risk on case notes.

Both tasks reuse the same connectivity setup (Airflow connection
"edp_llm_gateway") -- no team member writes API-key/gateway boilerplate.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from edpllm import edpllm

default_args = {"owner": "data-eng", "retries": 1}

with DAG(
    dag_id="example_edpllm_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    summarize_articles = PythonOperator(
        task_id="summarize_articles",
        python_callable=edpllm,
        op_kwargs=dict(
            prompt_path="/opt/airflow/dags/repo/examples/prompts/summarize.md",
            model_type="processing",
            input_table="my-project.my_dataset.articles",
            id_field="article_id",
            processed_field="combined_text",       # already concatenated upstream
            ai_return_field="llm_summary",
            where_clause="llm_summary IS NULL",    # only process new/unprocessed rows
            batch_size=500,
            concurrency=15,
        ),
    )

    classify_risk = PythonOperator(
        task_id="classify_risk",
        python_callable=edpllm,
        op_kwargs=dict(
            prompt_path="/opt/airflow/dags/repo/examples/prompts/classify_risk.md",
            model_type="reasoning",
            input_table="my-project.my_dataset.case_notes",
            id_field="case_id",
            processed_field="notes_text",
            ai_return_field="llm_risk_label",
            where_clause="llm_risk_label IS NULL",
            batch_size=200,
            concurrency=8,  # reasoning model: lower concurrency, higher latency/cost per call
        ),
    )

