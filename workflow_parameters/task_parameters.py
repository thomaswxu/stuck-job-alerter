from dataclasses import dataclass
from pyspark.dbutils import DBUtils

@dataclass
class TaskParams:
    """
    Class for retrieval of task parameters in workflows associated with this notebook.
    
    Note that this class is currently not used by the StuckJobAlerter notebook, and is here
    as a template for optional implementation of task parameters in addition to job parameters.
    """
    test_task_param: str
    test_task_param_num: int

    def __init__(self, dbutils: DBUtils) -> None:
        # Explicitly define parameters so that they can be retrieved from the workflow.
        # Note: the strings here for the parameter names must match the ones defined in the workflow.
        dbutils.widgets.text("test_task_param", defaultValue="none")
        dbutils.widgets.text("test_task_param_num", defaultValue="-1")

        # Retrieve actual parameter values from the workflow
        # (Uncomment these lines when the above task parameters are added to the Databricks job/workflow.)
        # self.test_task_param = dbutils.widgets.get("test_task_param")
        # self.test_task_param_num = dbutils.widgets.get("test_task_param_num")
