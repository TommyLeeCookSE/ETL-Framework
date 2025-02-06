#ETLs\utils\Connector.py
from pathlib import Path
from abc import ABC, abstractmethod
import json, requests, os, sys

class Connector(ABC):
    def __init__(self, logger, token_key):
        self.logger = logger
        self.logger.info(f"{self.__class__.__name__} initialized.")
        self.token_key = token_key

        self.token_file_path = r'misc\tokens.json'
        self.token_info = self.load_token_info()
        self.is_access_token()

    def load_token_info(self):
        """
        Loads credentials required to generate or regenerate an access token.

        Returns:
            dict: Dict of required token credentials.
        """
        self.logger.info(f"Loading token info for token key: {self.token_key}")
        try:
            with open(self.token_file_path, 'r', encoding='utf-8') as token_json_file:
                all_tokens_data = json.load(token_json_file)
                token_data = all_tokens_data[self.token_key]
                self.logger.info(f"Loaded {self.token_key} Token Data.")
                return token_data
            
        except FileNotFoundError as e:
            self.logger.warning(f"{self.token_file_path} not found, returning {[]}.")
            return {}
     
    def is_access_token(self):
        """
        Check if an access token is present from the token loading.
        If so, retreives it from cache, otherwise, calls get_access_token to generate a new token.
        """
        self.access_token = self.token_info['access_token']

        if self.access_token:
            self.logger.info("Retrieved access token from cache.")
            return
        else:
            self.get_access_token()
        
    def load_token_to_json(self):
        """
        Loads the new access token into the json cache.
        """
        self.logger.info("Loading Tokens back into JSON cache.")
        try:
            with open(self.token_file_path, 'r+', encoding='utf-8') as token_json_file:
                all_token_data = json.load(token_json_file)
                all_token_data[self.token_key] = self.token_info

                token_json_file.seek(0)
                token_json_file.truncate()

                json.dump(all_token_data,token_json_file,indent=4)

            self.logger.info("Tokens successfully loaded into JSON cache.")
        except Exception as e:
            self.logger.error(f"Error uploading tokens to json: {e}")
    
    def get_access_token(self):
        """
        Gets access token using cliend_id and client_secret.

        Returns:
            access_token (str): Access Token.
        """
        self.logger.info(f"Access Token missing or Expired... Getting a new {self.token_key} Access Token..")
        
        client_id = self.token_info['client_id']
        client_secret = self.token_info['client_secret']
        tenant_id = self.token_info['tenant_id']

        token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        payload = {
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret,
            'scope': 'https://graph.microsoft.com/.default'
        }
        
        response = requests.post(token_url, data=payload)

        if response.status_code == 200:
            self.access_token = response.json()['access_token']
            self.token_info['access_token'] = self.access_token
            self.load_token_to_json()
            return self.access_token
        else:
            self.logger.error(f'Error getting token: {response.status_code}. | {json.dumps(response)}')

    def check_response(self, info_dict: dict)-> dict:
        """
        Takes in a info_dict : 
        If retries <= max_retries, proceed, otherwise return dict with status stop to let the program know to stop
        Send the request, check status code, do checks, if sucessful code, return the object with status:stop
        Retries up till the max before returning a stop.
        Returns a response_dict which will let the program know to stop or continue

        Args:
            info_dict (dict): Dict with format {url: url, headers: headers, body: body, method: 'get/post'}
        Returns:
            response_dict (dict): Dict with format {status: 'success/fail', response: response_object}
        """
        retries = 0
        max_retries = 3
        fail_codes = [400, 401, 403, 408, 424, 500, 502, 503, 504]
        success_codes = [200, 201, 204]

        url = info_dict.get('url')
        headers = info_dict.get('headers')
        body = info_dict.get('body')
        method = info_dict.get('method')
        
        
        while retries <= max_retries:
            if method == 'get':
                response = requests.get(url, headers=headers)
            elif method == 'post' and body:
                response = requests.post(url, headers=headers, json=body)
            elif method == 'post' and not body:
                self.logger.warning(f"Missing body: {json.dumps(info_dict,indent=4)}")

            if response:
                response_json = response.json()
                self.logger.debug(f"Response: {json.dumps(response_json,indent=4)}")
                
                fail_codes = [
                    (int(item['status'])) 
                    for item in response_json['responses']  
                    if 'status' in item and int(item['status']) in fail_codes
                    ]                    

                if fail_codes and retries <= max_retries:
                    self.logger.warning(f"{retries}/{max_retries} Items have failed codes. Need to try again for {len(fail_codes)} items")
                    retries += 1
                elif fail_codes and retries >= max_retries:
                    self.logger.warning(f"{retries}/{max_retries} Limit reached, failed to upload {len(fail_codes)}")
                    response_dict = {
                        'status': 'fail',
                        'response': response_json
                    }
                    return response_dict
                else:
                    self.logger.info(f"All items successfully uploaded.")
                    response_dict = {
                        'status': 'success',
                        'response': response_json
                    }
                    return response_dict
        
        
                    
            
                