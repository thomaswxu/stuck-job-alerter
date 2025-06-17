# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Secrets Helper
# MAGIC This notebook contains helper code for admins of the Stuck Job Alerter workflow to maintain and update credential information using Databricks secrets.
# MAGIC
# MAGIC ### Relevant Documentation
# MAGIC - [Databricks Secrets](https://docs.databricks.com/aws/en/security/secrets/?language=Databricks%C2%A0workspace%C2%A0UI#manage-secret-scopes)
# MAGIC   - [Step-by-step Tutorial](https://docs.databricks.com/aws/en/security/secrets/example-secret-workflow)
# MAGIC - [Databricks Secrets via Databricks SDK](https://databricks-sdk-py.readthedocs.io/en/latest/workspace/workspace/secrets.html)
# MAGIC - [Databricks Secrets via Databricks REST API](https://docs.databricks.com/api/workspace/secrets)

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

# Install all required packages
%pip install requests==2.32.3

# COMMAND ----------

import json

# COMMAND ----------

# Helper Functions

def pretty_print_json(object_to_serialize: dict[str, str]) -> None:
    """Helper for more readable printing in JSON format."""
    print(json.dumps(object_to_serialize, indent=4, sort_keys=True))

# COMMAND ----------

# MAGIC %md
# MAGIC # REST API Class

# COMMAND ----------

# MAGIC %md
# MAGIC ### Class Import

# COMMAND ----------

from secrets_helper import SecretsHelper

# COMMAND ----------

# MAGIC %md
# MAGIC ### Usage Examples

# COMMAND ----------

# MAGIC %md
# MAGIC #####Instantiation

# COMMAND ----------

current_workspace_token = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .apiToken()
    .getOrElse(None)
)
current_workspace_url = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .apiUrl()
    .getOrElse(None)
)
print("Current workspace URL: " + current_workspace_url)
secrets_helper_REST = SecretsHelper(current_workspace_url, current_workspace_token)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### List All Scopes

# COMMAND ----------

scopes = secrets_helper_REST.get_scopes()
pretty_print_json(scopes)
print("Number of scopes: " + str(len(scopes["scopes"])))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### List All Scopes Containing Keywords

# COMMAND ----------

# Find scopes containing certain keywords
keywords = ["test", "tmp", "temp"]
scope_list = scopes["scopes"]
scope_candidates = []
for scope in scope_list:
    for keyword in keywords:
        if keyword in scope["name"]:
            scope_candidates.append(scope)
            
pretty_print_json(scope_candidates)
print("Number of scope candidates: " + str(len(scope_candidates)))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create a New Scope and Add a Secret

# COMMAND ----------

new_scope_name = "my_new_scope"
scope_creation_response = secrets_helper_REST.create_scope(new_scope_name)
pretty_print_json(scope_creation_response)
key_addition_response = secrets_helper_REST.add_secret(new_scope_name, "test", "123")
pretty_print_json(key_addition_response)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### List All Keys for a Scope

# COMMAND ----------

scope_name = new_scope_name
keys = secrets_helper_REST.get_scope_keys(scope_name)
pretty_print_json(keys)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Retrieve a Certain Key Value

# COMMAND ----------

secret_value = secrets_helper_REST.get_secret(new_scope_name, "my_secret_key")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### List Scopes in a Different Workspace

# COMMAND ----------

alt_token = secrets_helper_REST.get_secret(new_scope_name, "alt-token")
alt_example_url = "https://my-alt-env.cloud.databricks.com"

alt_secrets_helper = SecretsHelper(alt_example_url, alt_token)
pretty_print_json(alt_secrets_helper.get_scopes())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Delete a Secret or Scope

# COMMAND ----------

# Delete a scope
scope_name= ""
deletion_response = secrets_helper_REST.delete_scope(scope_name)
pretty_print_json(deletion_response)

# COMMAND ----------

# Delete a secret
scope_name = ""
secret_name = ""
deletion_response = secrets_helper_REST.delete_secret(scope_name, secret_name)
pretty_print_json(deletion_response)