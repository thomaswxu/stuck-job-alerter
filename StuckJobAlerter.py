# Databricks notebook source
# MAGIC %md
# MAGIC # Stuck Job Alerter
# MAGIC
# MAGIC Please refer for `README.md` for context, usage steps, and documentation.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Install Required Packages

# COMMAND ----------

# Install all required packages
%pip install requests==2.32.3

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Required Modules

# COMMAND ----------

# import datetime, json, requests
import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# COMMAND ----------

from utils.parsing_helpers import *
from utils.time_helpers import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Workflow Parameters Class

# COMMAND ----------

from workflow_parameters.job_parameters import JobParams
from workflow_parameters.task_parameters import TaskParams

# Instantiate Workflow Parameters class
job_params = JobParams(dbutils)
print(f"Secret scope name: {job_params.secret_scope_name}")
print(f"Token secret names: {job_params.token_secret_names}")
print(f"Slack webhook secret name: {job_params.slack_webhook_secret_name}")
print(f"Run Duration Threshold: {job_params.run_duration_threshold_hrs} hours")
print(f"Workspaces to check: {job_params.workspaces_to_check}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Secrets Helper Class

# COMMAND ----------

from db_secrets.secrets_helper import SecretsHelper

current_workspace_url = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .apiUrl()
    .getOrElse(None)
)
current_workspace_token = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .apiToken()
    .getOrElse(None)
)
print("Current workspace URL: " + current_workspace_url)
secrets_helper = SecretsHelper(current_workspace_url, current_workspace_token)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Main Class

# COMMAND ----------

from stuck_job_alerter import JobAlerter

# COMMAND ----------

# MAGIC %md
# MAGIC # Get Job Runs

# COMMAND ----------

# MAGIC %md
# MAGIC ### Instantiation

# COMMAND ----------

workspace_urls = job_params.workspaces_to_check
print("Workspace URLs: " + str(workspace_urls))

try:
    # Retrieve secret tokens for each workspace
    workspace_tokens = []
    for token_secret in job_params.token_secret_names:
        workspace_tokens.append(secrets_helper.get_secret(scope_name=job_params.secret_scope_name, key=token_secret))

    job_alerter = JobAlerter(logger, workspace_tokens, workspace_urls)
except ValueError as ve:
    logger.error("Failed to instantiate JobAlerter class: " + repr(ve))
except TypeError as te:
    logger.error("Failed to instantiate JobAlerter class: " + repr(te))

# COMMAND ----------

# Get info for multiple job runs
job_runs_lists = job_alerter.get_job_runs(
    active_runs_only=True, older_than_hours=job_params.run_duration_threshold_hrs,
    limit=1000, simplified_output=True, include_streaming_jobs=False)

print(f"Job runs older than {job_params.run_duration_threshold_hrs:.2f} hours:")
pretty_print_json(job_runs_lists)

print("\nNumber of job runs: ")
pretty_print_json(get_counts_in_dict_list(job_runs_lists))


# COMMAND ----------

# Simple durations view for convenience
durations_dict = {}
for url in job_runs_lists:
    durations_dict[url] = job_alerter.parse_job_run_durations(job_runs_lists[url])
print("Found job run durations (hours):")
pretty_print_json(durations_dict)

# COMMAND ----------

# MAGIC %md
# MAGIC # Slackbot Integration
# MAGIC
# MAGIC Please refer to `README.md` for further information.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Slackbot Class Instantiation

# COMMAND ----------

from slackbot.slackbot import Slackbot

# COMMAND ----------

# Retrieve the Slack webhook URL from DB Secrets
# (Assumed to be in a scope in the current workspace)
webhook = secrets_helper.get_secret(scope_name=job_params.secret_scope_name, key=job_params.slack_webhook_secret_name)
slackbot = Slackbot(webhook)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Post Alert Messages

# COMMAND ----------

workspace_payloads = slackbot.construct_workspace_payloads(job_runs_lists, job_params.run_duration_threshold_hrs)
pretty_print_json(workspace_payloads)

# COMMAND ----------

# Post messages for all workspaces
responses = slackbot.post_workspace_payloads(workspace_payloads)