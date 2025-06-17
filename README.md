# Stuck Job Alerter

### Overview

This notebook contains code that helps catch "stuck jobs," defined as Databricks (Lakeflow) Jobs that are taking longer than they are supposed to (for whatever reason). This notebook can be used as-is, or can itself be used as part of a Job.

This is achieved via a custom class wrapping the Databricks Jobs RESTful API (using the Python [Requests](https://pypi.org/project/requests/) module). 

### Functionality

- List all jobs that have been running for longer than a given time in a given list of Databricks workspaces.
- Send alert messages via Slack.
- Use Databrick secrets for credential management.

### Prerequisites

To use the `StuckJobAlerter` notebook, you must fill out the parameters associated with it (listed below). These are visible at the top of the notebook (as `dbutils` widgets) when used interactively, and are pulled from Job parameters when the notebook is used as part of a Databricks Job. Either fill these parameters out via the Databricks Jobs UI or the dbutils widgets at the top of the notebook, depending on if you are running the notebook manually or as part of a job.

- Note that the parameters values will be fetched via [Databricks Secrets](https://docs.databricks.com/aws/en/security/secrets/). A helper class has been provided for this; refer to the corresponding example notebook here: `./db_secrets/DBSecrets`

##### `run_duration_threshold_hrs`
This is the minimum current duration, in hours, for active job runs to fetch.

##### `workspaces_to_check`
These are the URLs of the Databricks workspaces to check job runs in. They should be given in a list, in the following form (no need for quotes): 
`[https://myenv.cloud.databricks.com, ...]`

- **Note:** If you run into issues regarding `(Error 403) Cert validation failed. Cross workspace access is denied due to network policies` when inputting multiple workspaces, see [this page](https://community.databricks.com/t5/data-engineering/cross-workspace-rest-api-access-denied-due-to-network-policies/td-p/92890) for how to resolve these via Private Link configuration.

##### `secret_scope_name`
This is the secret scope name in the workspace that this notebook/job is run in under which secret keys may be found corresponding to valid authorization tokens (either PAT or internal/OAuth) for the `workspaces_to_check`. 

##### `token_secret_names`
These are the secret keys under `secret_scope_name` corresponding to authorization tokens for the `workspaces_to_check`. This should be given as a list (no need for quotes), with the length and order of the list matching `workspaces_to_check`. E.g. `[token_secret_name1, token_secret_name2, ...]`

##### `slack_webhook_secret_name`
This is the name of the secret key under `secret_scope_name` containing the Slack webhook to use to send alerts.
- To use the Slack alert message functionality, you will need to set up a Slackbot; see the Slackbot section below for more information.

### Unit Tests

Run the `RunUnitTests` notebook to run all the unit tests in this repository. Refer to the documentation cells in that notebook for additional information. Note that the unit tests use [PyTest](https://docs.pytest.org/en/stable/).

### Usage

The main notebook to run is `StuckJobAlerter`. You can use this notebook interactively as-is, or you can use it in a [Databricks Job](https://docs.databricks.com/aws/en/jobs).

1. Add this repository to your Databricks workspace.
    - Either `git clone` and upload to your workspace via the UI, or use Databricks [built-in Git integration](https://docs.databricks.com/aws/en/repos).
2. In your Databricks workspace, navigate to `Workflows` on the left sidebar and create a new job.
3. Add a task of type `Notebook` and set the path to point to the `StuckJobAlerter` notebook.
    - It is recommended to add another `Notebook` task pointing to `RunUnitTests` that must succeed for the `StuckJobAlerter` task to run.
4. Add job parameters for each of the parameters in the previous section. The parameter names must match exactly.
5. Assign compute for the job to use.
    - It is recommended to use a small cluster, e.g. `m5d.large (8 GB, 2 cores, single node)`.
    - Use an LTS Databricks Runtime >= DBR 15.4.
    - No ML runtime or Photon acceleration is required.
6. (Optional) Assign permissions, scheduling, etc. for the job via the web UI.
7. Click the "Run Now" button on the top-right corner or wait for a scheduled run.

### Examples

For examples using the main `StuckJobAlerter` Python class, see the `StuckJobAlerterExamples` notebook. For examples using the helper classes, view the corresponding example notebook or unit test files in each subdirectory in this repository. Helper class functionality includes Databricks Secrets API calls, Databricks Job/Task parameter parsing, and Slackbot creation.

**Note:** The example notebooks assume that they will be run in a Databricks workspace. Running them externally may cause errors (e.g. due to missing package imports).

### Slackbot Integration

Part of the functionality of this repository is posting alert messages to Slack. Note that this requires an existing Slackbot to be set up with incoming webhook functionality (see the documentation linked below for instructions on how to set that up). Once the Slackbot is set up, the helper class found at `slackbot/slackbot.py` can be used. 

Refer to these pages for further information:

- [Slack API Webhook Messaging](https://api.slack.com/messaging/webhooks)
- [Slack Block Kit Builder](https://app.slack.com/block-kit-builder/)

### Further Resources

#### Other Documentation
- [Databricks Jobs Documentation](https://docs.databricks.com/aws/en/jobs)
- [Databricks REST API Reference](https://docs.databricks.com/api/workspace/introduction)
- [Databricks Utilities Reference](https://docs.databricks.com/aws/en/dev-tools/databricks-utils#dbutils-jobs)

#### Tokens Resources
- [Authorization Docs](https://docs.databricks.com/gcp/en/dev-tools/auth/)
  - [Authorization via Service Principal and OAuth](https://docs.databricks.com/gcp/en/dev-tools/auth/oauth-m2m)
- [Tokens REST API Docs](https://docs.databricks.com/api/workspace/tokens)

#### Maintainer
- thomas.xu@databricks.com
