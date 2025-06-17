from dataclasses import dataclass
from pyspark.dbutils import DBUtils

@dataclass
class JobParams:
    """Class for retrieval of job parameters in workflows associated with the StuckJobAlerter notebook."""
    run_duration_threshold_hrs: float
    workspaces_to_check: list[str]
    secret_scope_name: str
    token_secret_names: list[str]
    slack_webhook_secret_name: str

    def __init__(self, dbutils: DBUtils) -> None:
        # Explicitly define parameters so that they can be retrieved from the workflow.
        # Note: the strings here for the parameter names must match the ones defined in the workflow.
        dbutils.widgets.text("run_duration_threshold_hrs", defaultValue="0")
        dbutils.widgets.text("workspaces_to_check", defaultValue="[]")
        dbutils.widgets.text("secret_scope_name", defaultValue="")
        dbutils.widgets.text("token_secret_names", defaultValue="[]")
        dbutils.widgets.text("slack_webhook_secret_name", defaultValue="")

        # Retrieve actual parameter values from the workflow
        self.run_duration_threshold_hrs = float(dbutils.widgets.get("run_duration_threshold_hrs"))
        self.workspaces_to_check = self.parse_workspaces(dbutils.widgets.get("workspaces_to_check"))
        self.secret_scope_name = dbutils.widgets.get("secret_scope_name")
        self.token_secret_names = self.parse_secret_names(dbutils.widgets.get("token_secret_names"))
        self.slack_webhook_secret_name = dbutils.widgets.get("slack_webhook_secret_name")
    
    @staticmethod
    def parse_workspaces(workspaces_str: str) -> list[str]:
        """Helper to parse a list of workspaces from a string in the format '[workspace1, workspace2, ...]'."""
        return JobParams.parse_str_list(workspaces_str)

    @staticmethod
    def parse_secret_names(secrets_str: str) -> list[str]:
        """Helper to parse a list of secret names from a string in the format '[secret_name1, secret_name2, ...]'."""
        return JobParams.parse_str_list(secrets_str)
    
    @staticmethod
    def parse_str_list(str_list: str) -> list[str]:
        """Helper to parse a list of strings from a string in the format '[str1, str2, ...]'."""
        if str_list == "" or str_list == "[]":
            return []
        
        str_list_str_clean = str_list.replace(" ", "")[1:-1] # Remove all spaces and then open/close brackets
        str_list_split = str_list_str_clean.split(",")
        return [s for s in str_list_split if s != ""]