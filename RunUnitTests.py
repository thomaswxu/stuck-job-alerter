# Databricks notebook source
# MAGIC %md
# MAGIC # Run Unit Tests
# MAGIC This notebook runs the unit tests for all submodules in this repository using [PyTest](https://docs.pytest.org/en/stable/), following [best practice recommendations from Databricks](https://docs.databricks.com/aws/en/notebooks/testing).
# MAGIC
# MAGIC For developers used to the terminal, think of running this notebook as the equivalent of running `pytest` in this directory.
# MAGIC
# MAGIC Note that running the individual unit test files will likely fail to import `pytest` if you do not run this notebook first.

# COMMAND ----------

# MAGIC %pip install pytest

# COMMAND ----------

import pytest
import sys
import os

# Skip writing .pyc files on a readonly filesystem.
sys.dont_write_bytecode = True

# Run pytest
retcode = pytest.main([".", "-v", "-p", "no:cacheprovider"])

# Fail the cell execution if there are any test failures.
if retcode != 0: print(f"StuckJobAlerter: The pytest invocation failed with return code {retcode}. See the cluster driver logs for details.")