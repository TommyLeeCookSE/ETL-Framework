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