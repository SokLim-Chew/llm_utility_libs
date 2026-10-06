"""
Quick local smoke test: exercises prompt loading/rendering for both example
prompts against sample text, without needing BigQuery, Airflow, or a real
LLM gateway connection.

Run:  python3 local_prompt_test.py
"""
import sys
sys.path.insert(0, "edpllm")  # adjust if running from elsewhere

from edpllm.prompts import load_prompt_template, render_prompt

sample_article = """The city council voted 7-2 on Tuesday to approve a $45 million budget for
road resurfacing across twelve districts over the next three years. The
plan prioritizes streets with the highest pothole complaint volume, with
work beginning in the northern district in Q1 2027. Two council members
opposed the measure, citing concerns that the funding formula
underrepresents the eastern suburbs, where population growth has outpaced
infrastructure investment. The mayor's office says the project will create
roughly 300 temporary construction jobs and reduce average pothole repair
response time from 14 days to under 5."""

sample_case_notes = """Customer opened account 18 months ago. Payment history was on-time for the
first 14 months. Missed two consecutive payments in months 15-16, then
resumed on-time payments for months 17-18. Contacted support twice in the
last 60 days regarding billing discrepancies, both resolved same-day.
Account balance is currently 22% of credit limit. No disputes, chargebacks,
or fraud flags on file. Customer recently updated employment info to
reflect a new job at a stable, well-established employer."""

print("=" * 20, "RENDERED summarize.md", "=" * 20)
tmpl = load_prompt_template("examples/prompts/summarize.md")
print(render_prompt(tmpl, content=sample_article))

print()
print("=" * 20, "RENDERED classify_risk.md", "=" * 20)
tmpl = load_prompt_template("examples/prompts/classify_risk.md")
print(render_prompt(tmpl, content=sample_case_notes))
