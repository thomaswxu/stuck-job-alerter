import requests
import base64

class SecretsHelper:
    """Class that implements various Databricks secrets-related functions. Mainly wraps the DB REST API."""

    def __init__(self, workspace_url, token) -> None:
        self.__workspace_url = workspace_url
        self.__token = {"Authorization": "Bearer {0}".format(token)}
        self.__api_version = "2.0"

    def get(self, endpoint: str, json_params: dict[str, str]={}) -> dict[str, str]:
        """Wrapper for DB REST API GET. URL should have no ending backslash (/)."""        
        if json_params:
            raw_results = requests.get(
                self.__workspace_url + "/api/" + self.__api_version + endpoint,
                headers=self.__token,
                params=json_params,
            )
        else:
            raw_results = requests.get(
                self.__workspace_url + "/api/" + self.__api_version + endpoint,
                headers=self.__token
            )
        try:
            results = raw_results.json() # Dict
        except requests.exceptions.JSONDecodeError as jde:
            print("SecretsHelper_REST: Failed to decode response JSON. Check for 204 error (No Response) "
                  "or invalid JSON in response.")
            return raw_results
        results["http_status_code"] = raw_results.status_code
        return results

    def post(self, endpoint: str, json_params: dict[str, str]={}) -> dict[str, str]:
        """Wrapper for DB REST API POST."""
        if not json_params:
            print("SecretsHelper_REST: Must have a payload in json_args param.")
            return {}
        
        raw_results = requests.post(
            self.__workspace_url + "/api/" + self.__api_version + endpoint,
            headers=self.__token,
            json=json_params,
        )
        results = raw_results.json()

        if results:
            results["http_status_code"] = raw_results.status_code
            return results
        else:
            # If results are empty, simply return status code.
            return {"http_status_code": raw_results.status_code}

    def get_scopes(self) -> dict:
        scopes = self.get("/secrets/scopes/list")
        return scopes

    def get_scope_keys(self, scope: str) -> dict:
        keys = self.get("/secrets/list", json_params={"scope": scope})
        return keys
    
    def create_scope(self, scope_name: str) -> dict:
        # Note: the caller of this function will be granted "MANAGE" for this scope by default.
        response = self.post("/secrets/scopes/create", json_params={"scope": scope_name})
        return response

    def delete_scope(self, scope_name: str) -> dict:
        response = self.post("/secrets/scopes/delete", json_params={"scope": scope_name})
        return response
    
    def add_secret(self, scope_name: str, key: str, value: str) -> dict:
        response = self.post("/secrets/put", json_params={"scope": scope_name, "key": key, "string_value": value})
        return response
    
    def delete_secret(self, scope_name: str, key: str) -> dict:
        response = self.post("/secrets/delete", json_params={"scope": scope_name, "key": key})
        return response
    
    def get_secret(self, scope_name: str, key: str) -> dict:
        response = self.get("/secrets/get", json_params={"scope": scope_name, "key": key})
        if 'value' in response:
            encoded_val = response['value']
            # Note: can also use dbutils.secrets.get("scope", "key") instead to avoid manual decoding
            return base64.b64decode(encoded_val).decode('utf-8')
        return response
