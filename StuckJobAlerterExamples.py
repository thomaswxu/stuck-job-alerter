# Databricks notebook source
# MAGIC %md
# MAGIC # Stuck Job Alerter Examples
# MAGIC This notebook contains examples showing how to use the various functions available in the StuckJobAlerter class (see `stuck_job_alerter.py`). For examples using the various helper classes in this repository, refer to `README.md`.
# MAGIC
# MAGIC Note that you will need to fill out various fields in the cells for the function parameters (e.g. job run ID) for the cells to actually work.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Minimal Setup

# COMMAND ----------

# MAGIC %pip install requests==2.32.3

# COMMAND ----------

import logging
from stuck_job_alerter import JobAlerter
from utils.parsing_helpers import *

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Instantiation

# COMMAND ----------

# Manual instantiation without using Job Parameters
current_workspace_url = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .apiUrl()
    .getOrElse(None)
)
token = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .apiToken()
    .getOrElse(None)
)

manual_urls = [current_workspace_url]
manual_tokens = [token]
try:
    job_alerter = JobAlerter(logger, manual_tokens, manual_urls)
except ValueError as ve:
    print(str(ve))
except TypeError as te:
    print(str(te))

print("Using these workspace URLS:")
print(manual_urls)

# COMMAND ----------

# MAGIC %md
# MAGIC # Usage Examples

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Clusters

# COMMAND ----------

# Get info for a single cluster
sample_cluster_id = "1234-56789-abcdef"
cluster_info = job_alerter.get_cluster_info(sample_cluster_id, simplified=True)
pretty_print_json(cluster_info)

# COMMAND ----------

# Get all clusters
clusters = job_alerter.get_clusters(alive=True)
print("\nNumber of clusters:")
pretty_print_json(get_counts_in_dict_list(clusters))

# COMMAND ----------

# Print more detailed cluster information:
pretty_print_json(clusters)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Node Types

# COMMAND ----------

node_types = job_alerter.get_node_types()
pretty_print_json(node_types)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Jobs

# COMMAND ----------

# Get info for a single job
job_id = 123456789
job = job_alerter.get_job(job_id, simplified=False)
pretty_print_json(job)
tags = job_alerter.get_job_tags(job_id)
print("Tags: ")
pretty_print_json(tags)

# COMMAND ----------

jobs_lists = job_alerter.get_jobs(limit=100)
pretty_print_json(jobs_lists)
print("\nNumber of jobs: ")
pretty_print_json(get_counts_in_dict_list(jobs_lists))

# COMMAND ----------

print("Job names:")
for workspace in jobs_lists:
    print("\t" + workspace)
    for job in jobs_lists[workspace]:
        print("\t\t" + job["settings"]["name"] + " (" + str(job["job_id"]) + ")")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Job Runs

# COMMAND ----------

# Get info for a single job run
run_id = 1234567890
workspace_url = "https://myenv.cloud.databricks.com" # Ending backslash is optional
job_run = job_alerter.get_job_run(workspace_url, run_id, include_history=False, include_resolved_values=True)
pretty_print_json(job_run)

# COMMAND ----------

# Get info for multiple job runs
run_duration_threshold_hrs = 2.0
job_runs_lists = job_alerter.get_job_runs(
    active_runs_only=True, older_than_hours=run_duration_threshold_hrs,
    limit=1000, simplified_output=True, include_streaming_jobs=False)

print(f"\nJob runs older than {run_duration_threshold_hrs:.2f} hours:")
pretty_print_json(job_runs_lists)

print("\nNumber of job runs: ")
pretty_print_json(get_counts_in_dict_list(job_runs_lists))