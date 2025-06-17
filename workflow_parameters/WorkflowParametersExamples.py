# Databricks notebook source
# MAGIC %md
# MAGIC # Workflow Parameters Examples
# MAGIC This notebook contains various examples for how to use the helper classes for parsing Workflow job and task parameters.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup

# COMMAND ----------

from job_parameters import JobParams
from task_parameters import TaskParams

# COMMAND ----------

# MAGIC %md
# MAGIC ### Job Parameters Instantiation

# COMMAND ----------

job_params = JobParams(dbutils)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task Parameters Instantiation

# COMMAND ----------

task_params = TaskParams(dbutils)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example Usage

# COMMAND ----------

print(job_params.run_duration_threshold_hrs)
print(job_params.token_secret_names)
print(job_params.workspaces_to_check)
print(JobParams.parse_workspaces("[workspace1, workspace2, workspace3]"))
print(JobParams.parse_secret_names("[secret1, secret2, secret3]"))
