from setuptools import setup, find_packages

setup(
    name="edpllm",
    version="0.1.0",
    description="Lightweight shared utility library for LLM-powered data pipelines on the EDP",
    packages=find_packages(include=["edpllm", "edpllm.*"]),
    install_requires=[
        "google-cloud-bigquery>=3.11.0",
        "db-dtypes>=1.0.0",   # needed for BigQuery -> pandas dtype conversion
        "pandas>=1.5.0",
        "httpx>=0.24.0",
        "jinja2>=3.1.0",
    ],
    python_requires=">=3.9",
)
