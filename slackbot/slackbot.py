import requests

class Slackbot:
    """Class to send stuck job alert information via incoming webhook."""

    def __init__(self, webhook: str, unspecified_str: str = "Unspecified"):
        """
        Initialize using a given Slack incoming webhook URL.
        Webhook format: https://hooks.slack.com/services/ABCDEFG/1234567/xyz123foobar

        Note that there is no authentication needed to post using this URL.
        Also, note that the webhook should not be passed or stored in plain text, but
        instead stored/retrieved using Databricks Secrets or similar.
        """
        self.webhook = webhook
        self.divider_block = {"type": "divider"}
        self.unspecified_str = unspecified_str # Used to parse certain fields for the job run info blocks
        self.max_blocks_per_payload = 50 # Slack's imposed limit

    @staticmethod
    def tags_to_text(tags_dict: dict) -> str:
        if tags_dict == {}:
            return "None"
        
        tags_text = ""
        for key, value in tags_dict.items():
            if value == "":
                tags_text += f"• *{key}*\n"
            else:
                tags_text += f"• *{key}:* {value}\n"
        return tags_text[:-1]
    
    @staticmethod
    def blocks_to_payload(blocks: list[dict]) -> dict:
        """
        Construct Slack message payload from given Slack blocks.
        """
        payload = {
            "blocks": blocks
        }
        return payload

    def construct_workspace_payloads(self, job_runs_lists: dict[str, dict[str, str]], run_duration_threshold_hrs: float) -> dict[str, list[dict]]:
        """
        Construct Slack message payloads per workspace containing info for given job runs.
        Given job_runs_lists is assumed to be in the format outputted by StuckJobAlerter.
        """
        workspace_payloads = {}
        for workspace_url in job_runs_lists:
            workspace_header_blocks = self.__construct_header_blocks(workspace_url, run_duration_threshold_hrs)

            payload_list = [self.blocks_to_payload(workspace_header_blocks)] # Use list of payloads instead of one big one to avoid Slack's 50 block per payload limit
            for job_run_dict in job_runs_lists[workspace_url]:
                basic_info_block = self.__construct_basic_info_block(job_run_dict)
                cluster_info_block = self.__construct_cluster_info_block(job_run_dict)
                tags_block = self.__construct_tags_block(job_run_dict)
                duration_block = self.__construct_duration_block(job_run_dict)
                
                job_run_blocks = [basic_info_block, cluster_info_block, duration_block, tags_block, self.divider_block]
                job_run_payload = self.blocks_to_payload(job_run_blocks)
                if len(job_run_payload["blocks"]) > self.max_blocks_per_payload:
                    print(f"Slackbot [WARNING]: Job run with ID: {job_run_dict['run_id']} exceeds Slack's 50 block per payload limit. "
                           "Will likely fail to post corresponding Slack message.")

                payload_list.append(job_run_payload)

            workspace_payloads[workspace_url] = payload_list
        return workspace_payloads
    
    def post_workspace_payloads(self, workspace_payloads: dict[str, list[dict]]) -> dict[str, list[requests.Response]]:
        """
        Posts the given workspace payloads using the stored Slack webhook.
        Returns a dictionary of the corresponding responses from Slack.
        """
        workspace_responses = {}
        for workspace_url in workspace_payloads:
            responses = self.post_payloads(workspace_payloads[workspace_url])
            workspace_responses[workspace_url] = responses
        return workspace_responses
    
    def post_payloads(self, payloads: list[dict]) -> list[requests.Response]:
        """
        Posts the given payload information using the stored Slack webhook.
        Note that Slack has a limit of max 50 blocks for a single payload (i.e., Slackbot message).
        """
        responses = []
        for payload in payloads:
            response = requests.post(self.webhook, json=payload)
            responses.append(response)
        return responses

    def __construct_header_blocks(self, workspace_name: str, run_duration_threshold_hrs: float) -> list[dict]:
        """
        Construct Slack message header block containing info about the workspace and run duration threshold.
        """
        header_blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"Job runs longer than {run_duration_threshold_hrs:0.2f} hours",
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Workspace:* {workspace_name}"
                }
            },
            self.divider_block
        ]
        return header_blocks

    def __construct_basic_info_block(self, job_run_dict: dict) -> dict:
        """
        Construct Slack message block containing basic info about the job run.
        """
        basic_info_block = \
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Run name:*\n<{job_run_dict['run_page_url']}|{job_run_dict['run_name']}>"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Created by:*\n{job_run_dict['creator_user_name']}"
                }
            ]
        }
        return basic_info_block
    
    def __construct_cluster_info_block(self, job_run_dict: dict) -> dict:
        """
        Construct Slack message block containing info about the cluster used for the job run.
        """
        cluster_name_text = f"<{job_run_dict['cluster_url']}|{job_run_dict['cluster_name']}>"
        cluster_id_text = f"<{job_run_dict['cluster_url']}|{job_run_dict['cluster_id']}>"
        if job_run_dict['cluster_url'] == self.unspecified_str:
            cluster_name_text = f"{job_run_dict['cluster_name']}"
            cluster_id_text = f"{job_run_dict['cluster_id']}"

        cluster_info_text = f"*Cluster info:*\nID: {cluster_id_text}\nDriver: {job_run_dict['driver_node_type_id']}\nWorker: {job_run_dict['node_type_id']}"

        if job_run_dict['cluster_name'] == self.unspecified_str:
            cluster_name_text = "Serverless"
            cluster_id_text = job_run_dict['cluster_id']
            cluster_info_text = f"*Cluster info:*\nN/A"

        cluster_info_block = \
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Cluster name:*\n{cluster_name_text}"
                },
                {
                    "type": "mrkdwn",
                    "text": cluster_info_text
                }
            ]
        }
        return cluster_info_block
    
    def __construct_tags_block(self, job_run_dict: dict) -> dict:
        """
        Construct Slack message block containing info about the job run tags.
        """
        tags_text = self.tags_to_text(job_run_dict['job_tags'])
        tags_block = \
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Tags:*\n{tags_text}"
                }
            ]
        }
        return tags_block

    def __construct_duration_block(self, job_run_dict: dict) -> dict:
        """
        Construct Slack message block containing info about the duration of the job run.
        """
        duration_block = \
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Duration:*\n{job_run_dict['time_from_start_hours']:.2f} hours"
                }
            ]
        }
        return duration_block
