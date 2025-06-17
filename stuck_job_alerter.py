import logging
import requests
from utils.parsing_helpers import *
from utils.time_helpers import *

class JobAlerter:
    """
    Class with various functionalities related to job alerts, especially for stuck jobs.
    Uses the Python Requests library together with the Databricks REST API.

    Originally based on the Databricks REST API Client by Miklos Christine.
    """

    def __init__(self, logger: logging.Logger, tokens: list[str]=["ABCDEFG1234"],
                 workspace_urls: list[str]=["https://myenv.cloud.databricks.com"],
                 streaming_tag: str="streaming") -> None:
        """
        Args:
            tokens: List of tokens for each workspace URL.
            workspace_urls: List of workspace URLs.
            streaming_tag: Job tag used to identify streaming jobs. Burden is on job
                           creator to populate this tag correctly.
        """
        self.__logger = logger

        # Check tokens
        if not isinstance(tokens, list):
            raise TypeError("JobAlerter: Tokens must be given as a list (of strings).")
        if len(tokens) != len(workspace_urls):
            raise ValueError(f"JobAlerter: Number of tokens ({len(tokens)}) does not match "
                             f"number of workspace URLs ({len(workspace_urls)}).")

        # Check workspace URLs
        if not isinstance(workspace_urls, list):
            raise TypeError("JobAlerter: Workspace URLS must be given in a list.")
        self.__check_workspace_urls(workspace_urls)
        
        self.__tokens = {}
        for i in range(len(tokens)):
            self.__tokens[workspace_urls[i]] = {"Authorization": "Bearer {0}".format(tokens[i])}
        self.__workspace_urls = workspace_urls
        self.__api_version = "2.2"

        # Define what fields to keep for "simplified" outputs
        # Note: not all of these fields are set for each cluster.
        self.__simple_cluster_fields = [
            "cluster_id",
            "cluster_name",
            "cluster_url", # Note: not from DB REST API
            "cluster_cores",
            "driver_node_type_id",
            "node_type_id",
            "num_workers",
            "cluster_memory_mb"]
        self.__simple_streaming_fields = ["continuous", "job_tags"]

        self.streaming_tag = streaming_tag # Necessary and sufficient job tag to identify streaming jobs
        self.unspecified_str = "Unspecified" # Used as a placeholder for unset fields

    @staticmethod
    def construct_cluster_url(cluster_id: str, workspace_url: str) -> str:
        """Construct the cluster URL using the standard format."""
        # Ensure workspace URL is in correct format
        if workspace_url[-1] != "/":
            workspace_url += "/"
        return workspace_url + "compute/clusters/" + cluster_id

    def get_node_types(self) -> dict[str, dict[str, str]]:
        """Returns a dictionary of node types for the clusters in each workspace."""
        return dict((url, self.__get(url, "/clusters/list-node-types")) for url in self.__workspace_urls)

    def get_cluster_info(self, cluster_id: str, simplified: bool=True) -> dict[str, str]:
        """
        Returns a dictionary of json objects for the cluster with the given cluster_id.
        Assumes that the cluster_id is present in one of the workspace URLs.

        Args:
            cluster_id: The cluster ID to search for.
            simplified: If True, return a simplified version of the cluster info output.
        """
        cluster_info = {}
        for url in self.__workspace_urls:
            cluster_info = self.__get(url, "/clusters/get", json_params={"cluster_id": cluster_id})
            if cluster_info:
                if "http_status_code" in cluster_info and cluster_info["http_status_code"] != 200:
                    continue

                self.__logger.info(f"JobAlerter: Cluster ID {cluster_id} found in {url}.")

                # Add cluster URL field
                cluster_info["cluster_url"] = self.construct_cluster_url(cluster_id, url)

                if simplified:
                    simplified_info = {}
                    for field in self.__simple_cluster_fields:
                        if field in cluster_info:
                            simplified_info[field] = cluster_info[field]
                    return simplified_info
                return cluster_info
        self.__logger.info("JobAlerter: Cluster ID not found in any of the known workspaces.")
        return {}

    def get_clusters(self, alive: bool=True) -> dict[str, dict[str, str]]:
        """
        Returns a dictionary of json objects for the clusters in each workspace.
        Up to 100 clusters are returned at a time. If this is insufficient, modify this
        function to support pagination like get_job_runs().
        
        Args:
            alive: If True, only list the currently running clusters.
        """
        internal_page_size_limit = 100 # Internal limit from REST API
        cluster_lists = {}
        for url in self.__workspace_urls:
            cluster_list = self.__get(url, "/clusters/list", json_params={"page_size": internal_page_size_limit})
            if alive:
                try:
                    running_clusters = list(filter(lambda x: x["state"] == "RUNNING", cluster_list["clusters"]))
                    self.__logger.debug(url)
                    for x in running_clusters:
                        self.__logger.debug("\t" + x["cluster_name"] + " : " + x["cluster_id"])
                    cluster_lists[url] = running_clusters
                except KeyError as ke:
                    self.__logger.error("JobAlerter: Failed to get clusters from " + url + ". " \
                                        "Check if the user has permission to access the clusters.")
                    cluster_lists[url] = {}
            else:
                cluster_lists[url] = cluster_list
        return cluster_lists

    def get_job_tags(self, job_id: str) -> dict[str, str]:
        """Return a dictionary of the tags associated with a job."""
        for url in self.__workspace_urls:
            job_info = self.__get(url, "/jobs/get", json_params={"job_id": job_id})
            if job_info:
                if "http_status_code" in job_info and job_info["http_status_code"] != 200:
                    continue

                self.__logger.info(f"JobAlerter: Job ID {job_id} found in {url}.")

                if "settings" in job_info:
                    if "tags" in job_info["settings"]:
                        tags = job_info["settings"]["tags"]
                        return tags
                return {}
        self.__logger.info("JobAlerter: Job ID not found in any of the known workspaces.")
        return {}

    def get_job(self, job_id: str, simplified: bool=True) -> dict[str, str]:
        """
        Returns a dictionary of json objects for the job with the given job_id.

        Args:
            job_id: The job ID to search for. Assumed to be within one of the known workspaces.
            simplified: If True, return a simplified version of the job info output.
        """
        job_info = {}
        for url in self.__workspace_urls:
            job_info = self.__get(url, "/jobs/get", json_params={"job_id": job_id})
            if job_info:
                if "http_status_code" in job_info and job_info["http_status_code"] != 200:
                    continue

                self.__logger.info(f"JobAlerter: Job ID {job_id} found in {url}.")
                if simplified:
                    # Optional section for customized output for job info fields.
                    pass
                return job_info
        self.__logger.info("JobAlerter: Job ID not found in any of the known workspaces.")
        return {}

    def job_is_continuous(self, job_id: str) -> bool:
        """
        Returns True if the job is a continuous (i.e., streaming) job.
        """
        job_info = self.get_job(job_id, simplified=False)
        if "settings" not in job_info:
            self.__logger.warning("JobAlerter: Unable to fetch necessary data for job id provided "
                                  "(no 'settings' field); cannot determine if job is continuous.")
            return False

        job_settings = job_info["settings"]
        return "continuous" in job_settings

    def get_jobs(self, limit: int=20) -> dict[str, dict[str, str]]:
        """
        Returns a dictionary of json objects for jobs (up to specified limit) in each workspace.
        
        Args:
            limit: Maximum number of jobs to return. Must be in the range [1, 100].
        """
        if limit < 1 or limit > 100:
            self.__logger.warning("JobAlerter: Limit must be in the range [1, 100].")
            return {}
        
        jobs_lists = {}
        for url in self.__workspace_urls:
            jobs = self.__get(url, "/jobs/list", json_params={"limit": limit})
            try:
                jobs_list = jobs["jobs"]
                jobs_lists[url] = jobs_list
            except KeyError as ke:
                self.__logger.error("JobAlerter: Failed to get jobs from " + url + ". " \
                                    "Check if the user has permission to access the jobs.")
                jobs_lists[url] = {}
        return jobs_lists
    
    def parse_job_run_durations(self, job_runs_list: list[dict[str, str]]) -> dict[str, float]:
        """
        Given a list of job runs, return a simple structure of only the name and durations (in hours).
        It is assumed that the job runs list provided contains the "time_from_start" and similar fields
        (e.g. it was obtained via get_job_runs()).
        """
        durations = {}
        for run in job_runs_list:
            if "time_from_start_hours" in run:
                run_name = run["run_name"]
                while run_name in durations:
                    run_name += "_" # Prevent overwriting existing keys
                durations[run_name] = run["time_from_start_hours"]
        return durations

    def get_job_run(self, workspace_url: str, run_id: int, include_history: bool=False,
                    include_resolved_values: bool=False) -> dict[str, str]:
        """Wrapper for DB REST API function to get a single job run."""
        job_run = self.__get(workspace_url, "/jobs/runs/get",
                           json_params={"run_id": run_id, "include_history": str(include_history).lower(),
                                        "include_resolved_values": str(include_resolved_values).lower()})
        return job_run
    
    def get_job_runs(self, active_runs_only: bool=True, older_than_hours: float=0.0, limit: int=20,
                     simplified_output: bool=False, expand_tasks: bool=True, add_cluster_info: bool=True,
                     include_streaming_jobs: bool=False) -> dict[str, dict[str, str]]:
        """
        Returns a dict of list of json objects (dictionaries) for current job runs in each workspace.
        Optionally adds cluster and streaming info for the runs.

        Note: job run durations are calculated based on the time since the start_time field from the REST API.
        - This method was chosen to ensure a reliable run duration calculation for all job runs, as it was observed that
        the run_duration field is not always set for all job runs. See the main README.md or contact the maintainer for
        more information.
        
        Args:
            active_runs_only: If True, return job runs that are in the "RUNNING" state. Note that this is different
                              from the "active" definition from the DB REST API docs (includes "QUEUED", "PENDING", etc.).
                              Else, return all runs.
            older_than_hours: If > 0, return only job runs that started more than this many hours ago.
            limit: Maximum number of job runs to return. A value <=0 means no limit.
            simplified_output: If True, return a simplified version of the job runs list. Else, return all fields for each run.
            expand_tasks: Whether to get cluster and task details.
            add_cluster_info: Whether to add cluster info to each job run.
            include_streaming_jobs: Whether to include streaming jobs in in returned output.
        """
        if limit <= 0:
            print("JobAlerter: Warning: No limit provided for job runs to fetch. This may take awhile.")
            # return {} # Optional: require a limit to be given.

        job_runs_lists = {}
        for url in self.__workspace_urls:
            job_runs_list = []
            try:
                job_runs_list = self.__get_job_runs_list(url, active_runs_only, expand_tasks, older_than_hours, limit)
            except KeyError as ke:
                self.__logger.error("JobAlerter: Failed to get job runs from " + url + ". " \
                                    "Check if the user has permission to access the job runs.")
                job_runs_lists[url] = []
                continue

            # Optionally augment default job run info (e.g. with cluster/streaming info)
            job_runs_list_no_streaming = []
            for run in job_runs_list:
                if add_cluster_info:
                    self.__add_cluster_info_to_run(run)

                # Add formatted duration fields
                run["time_from_start"] = ms_since(run["start_time"])
                run["time_from_start_hours"] = ms_to_hours(run["time_from_start"])

                # Add streaming info
                run["continuous"] = self.job_is_continuous(run["job_id"])
                run["job_tags"] = self.get_job_tags(run["job_id"])
                if self.streaming_tag not in run["job_tags"]:
                    job_runs_list_no_streaming.append(run)

            if not include_streaming_jobs:
                job_runs_list = job_runs_list_no_streaming

            # Optionally simplify initial job run info
            if simplified_output:
                job_runs_list = self.__simplify_job_runs_list(job_runs_list)

            job_runs_lists[url] = job_runs_list

            # Add blank fields for unset cluster info fields
            if add_cluster_info:
                for run in job_runs_lists[url]:
                    for cluster_field in self.__simple_cluster_fields:
                        if cluster_field not in run:
                            run[cluster_field] = self.unspecified_str
        return job_runs_lists

    def __get_job_runs_list(self, workspace_url: str, active_runs_only: bool=True, expand_tasks: bool=True,
                            older_than_hours: float=0.0, limit: int=20) -> list[dict[str, str]]:
        """
        Helper (e.g. for get_job_runs()) to paginate and return a list of json objects (dictionaries)
        for all job runs in given workspace across multiple pages.

        Args:
            workspace_url: The workspace URL to get job runs from.
            active_runs_only: If True, return only active job runs.
            expand_tasks: Whether to get cluster and task details.
            older_than_hours: If > 0, return only job runs that started more than this many hours ago.
            limit: Maximum number of job runs to return. A value <=0 means no limit.
        """
        REST_internal_limit = 25 # Internal limit for the jobs/runs/list call
        json_params = {"active_only": str(active_runs_only).lower(),
                       "limit": REST_internal_limit, "expand_tasks": str(expand_tasks).lower()}

        # Pagination loop to ensure all job runs are read
        job_runs_list = []
        get_more_jobs = True
        while get_more_jobs:
            # Get info for all current job runs
            job_runs = self.__get(workspace_url, "/jobs/runs/list", json_params=json_params)
            job_runs_meta = {}
            meta_fields = ["http_status_code", "next_page_token", "prev_page_token"]
            for meta_field in meta_fields:
                if meta_field in job_runs:
                    job_runs_meta[meta_field] = job_runs[meta_field]

            get_more_jobs = "next_page_token" in job_runs_meta
            if get_more_jobs:
                json_params["page_token"] = job_runs_meta["next_page_token"]

            # Filter job runs based on status, if specified.
            if active_runs_only:
                job_runs["runs"] = list(filter(lambda x: x["status"]["state"] == "RUNNING", job_runs["runs"]))

            # Filter job runs based on current run duration, if specified.
            if older_than_hours > 0:
                job_runs["runs"] = list(filter(
                    lambda x: ms_since(x["start_time"]) > hours_to_ms(older_than_hours),
                    job_runs["runs"]
                ))

            if limit > 0 and len(job_runs_list) + len(job_runs["runs"]) > limit:
                job_runs_list.extend(job_runs["runs"][:limit - len(job_runs_list)])
                get_more_jobs = False
            else:
                job_runs_list.extend(job_runs["runs"])
            if len(job_runs_list) > 0:
                self.__logger.info(f"JobAlerter: Found {len(job_runs_list)} compliant job runs so far.")
        return job_runs_list

    def __add_cluster_info_to_run(self, run: dict[str, str]) -> None:
        """Helper to augment a given job run (in place) with cluster info."""

        # Find the cluster info for the first currently running task
        if "tasks" in run:
            found_active_cluster = False
            job_cluster_key = None
            for task in run["tasks"]:
                if "job_cluster_key" in task:
                    job_cluster_key = task["job_cluster_key"]
                
                if task["status"]["state"] == "RUNNING":
                    running_task_name = task["task_key"]
                    self.__logger.debug("JobAlerter: Running task: " + running_task_name)
                    if "cluster_instance" in task:
                        cluster_id = task["cluster_instance"]["cluster_id"]
                        cluster_info = self.get_cluster_info(cluster_id, simplified=True)
                        run.update(cluster_info)
                        found_active_cluster = True
                        break

            if not found_active_cluster and job_cluster_key:
                # Run is likely queued or some similar reason, so no active cluster info is available.
                # Fallback: get cluster info for inactive/unstarted job cluster if it's set.
                if "job_clusters" in run:
                    for job_cluster in run["job_clusters"]:
                        if job_cluster["job_cluster_key"] == job_cluster_key \
                                and "new_cluster" in job_cluster \
                                and "node_type_id" in job_cluster["new_cluster"]:
                            cluster_info = {
                                "cluster_id": "Unavailable",
                                "cluster_name": job_cluster_key,
                                "node_type_id": job_cluster["new_cluster"]["node_type_id"],
                                "driver_node_type_id": job_cluster["new_cluster"]["node_type_id"]
                            }
                            run.update(cluster_info)

    def __get(self, url: str, endpoint: str, json_params: dict[str, str]={}) -> dict[str, str]:
        """Wrapper for DB REST API GET, with optional result printing. URL should have no ending backslash (/)."""
        if url not in self.__tokens:
            self.__logger.warning(f"JobAlerter: No token provided for workspace: {url}. Ensure this workspace URL "
                                   "is passed during instantiation.")
            return {}
        
        if json_params:
            raw_results = requests.get(
                url + "/api/" + self.__api_version + endpoint,
                headers=self.__tokens[url],
                params=json_params,
            )
        else:
            raw_results = requests.get(
                url + "/api/" + self.__api_version + endpoint,
                headers=self.__tokens[url]
            )
        try:
            results = raw_results.json() # Dict
        except requests.exceptions.JSONDecodeError as jde:
            self.__logger.warning("JobAlerter: Failed to decode response JSON. Check for 204 error (No Response) "
                                  "or invalid JSON in response.")
            return raw_results
        results["http_status_code"] = raw_results.status_code
        return results

    def __post(self, url: str, endpoint: str, json_params: dict[str, str]={}) -> dict[str, str]:
        """Wrapper for DB REST API POST, with optional result printing."""
        if not json_params:
            self.__logger.warning("JobAlerter: Must have a payload in json_args param.")
            return {}
        if url not in self.__tokens:
            self.__logger.warning(f"JobAlerter: No token provided for workspace: {url}. Ensure this workspace URL "
                                   "is passed during instantiation.")
            return {}
        
        raw_results = requests.post(
            url + "/api/" + self.__api_version + endpoint,
            headers=self.__tokens[url],
            json=json_params,
        )
        results = raw_results.json()

        if results:
            results["http_status_code"] = raw_results.status_code
            return results
        else:
            # If results are empty, simply return status code.
            return {"http_status_code": raw_results.status_code}

    def __check_workspace_urls(self, workspace_urls: list[str]) -> None:
        """Check whether a given list of workspace URLs are valid. If not, raise an appropriate exception."""
        workspace_urls_curated = self.__curate_workspace_urls(workspace_urls)
        if len(workspace_urls_curated) == 0:
            raise ValueError("JobAlerter: No valid workspace URLS were given. Should be in the format: https://...")

    def __curate_workspace_urls(self, workspace_urls: list[str]) -> list[str]:
        """Curate a list of workspace URLs such that only ones in the form "https://..." remain."""
        required_prefix = "https://"
        workspace_urls_curated = []
        for workspace in workspace_urls:
            if workspace != "" and workspace.startswith(required_prefix) and len(workspace) > len(required_prefix):
                # Only allow workspace urls of the form: https://...
                # (Also remove ending backslash if present)
                workspace = workspace.rstrip("/")
                workspace_urls_curated.append(workspace)
        return workspace_urls_curated
    
    def __simplify_job_runs_list(self, job_runs_list: list[dict[str, str]]):
        """Return a simplified version of a given list (of dict) of job runs."""
        simple_fields = ["run_name", "creator_user_name", "run_page_url", "format", "run_type", "status", "job_id", "run_id",
                         "start_time", "setup_duration", "execution_duration", "cleanup_duration", "run_duration"]
        custom_simple_fields = ["time_from_start", "time_from_start_hours"] # Fields not from REST API
        simple_fields.extend(custom_simple_fields)
        simple_fields.extend(self.__simple_cluster_fields)
        simple_fields.extend(self.__simple_streaming_fields)

        job_runs_simple = []
        for run in job_runs_list:
            simple_dict = {}
            for k in simple_fields:
                if k in run:
                    simple_dict[k] = run[k]

            job_runs_simple.append(simple_dict)
        return job_runs_simple
