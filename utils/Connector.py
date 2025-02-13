#ETLs\utils\Connector.py
from pathlib import Path
from abc import ABC, abstractmethod
from urllib.parse import urlencode
import json, requests, os, sys

class Connector(ABC):
    def __init__(self, logger, token_key):
        self.logger = logger
        self.logger.info(f"Init: {self.__class__.__name__} initialized.")
        self.token_key = token_key

        self.token_file_path = r'misc\tokens.json'
        self.access_token = ''
        self.token_info = self.load_token_info()
        self.is_access_token()

    def load_token_info(self):
        """
        Loads credentials required to generate or regenerate an access token.

        Returns:
            dict: Dict of required token credentials.
        """
        self.logger.info(f"Load Token Info: Loading token info for token key: {self.token_key}")
        try:
            with open(self.token_file_path, 'r', encoding='utf-8') as token_json_file:
                all_tokens_data = json.load(token_json_file)
                token_data = all_tokens_data.get(self.token_key)

                if token_data:
                    self.logger.info(f"Load Token Info: Loaded {self.token_key} Token Data.")
                    return token_data
                else:
                    self.logger.warning(f"Load Token Info: {self.token_key} not found in token storage.")
                    return {}
        except FileNotFoundError as e:
            self.logger.warning(f"Load Token Info: {self.token_file_path} not found, returning {[]}.")
            return {}
     
    def is_access_token(self):
        """
        Check if an access token is present from the token loading.
        If so, retreives it from cache, otherwise, calls get_access_token to generate a new token.
        """
        self.access_token = self.token_info['access_token']

        if self.access_token:
            self.logger.info("Is Access Token: Retrieved access token from cache.")
            return
        else:
            self.get_access_token()
        
    def load_token_to_json(self):
        """
        Loads the new access token into the json cache.
        """
        self.logger.info("Load Token to Json: Loading Tokens back into JSON cache.")
        try:
            with open(self.token_file_path, 'r+', encoding='utf-8') as token_json_file:
                all_token_data = json.load(token_json_file)
                all_token_data[self.token_key] = self.token_info

                token_json_file.seek(0)
                token_json_file.truncate()

                json.dump(all_token_data,token_json_file,indent=4)

            self.logger.info("Load Token to Json: Tokens successfully loaded into JSON cache.")
        except Exception as e:
            self.logger.error(f"Load Token to Json:  Error uploading tokens to json: {e}")
    
    def get_access_token(self):
        """
        Gets access token using cliend_id and client_secret.

        Returns:
            access_token (str): Access Token.
        """
        self.logger.info(f"Get Access Token: Access Token missing or Expired... Getting a new {self.token_key} Access Token..")
        
        match self.token_key:
            case "sharepoint_tokens" | "azure_tokens":
                token_url = f"https://login.microsoftonline.com/{self.token_info['tenant_id']}/oauth2/v2.0/token"
                headers = {
                'Content-Type': 'application/x-www-form-urlencoded'
                }
                data = {
                    'grant_type': 'client_credentials',
                    'client_id': self.token_info['client_id'],
                    'client_secret': self.token_info['client_secret'],
                    'scope': 'https://graph.microsoft.com/.default'
                }

                info_dict = {
                    'url' : token_url,
                    'headers': headers,
                    'data': data,
                    'method': 'post',
                }

            case "service_desk_tokens":
                token_url = "https://accounts.zoho.com/oauth/v2/token"
                headers = {
                    "Content-Type": "application/x-www-form-urlencoded"
                }
                data = {
                    "refresh_token": self.token_info['refresh_token'],
                    "grant_type": "refresh_token",
                    "client_id": self.token_info['client_id'],
                    "client_secret": self.token_info['client_secret'],
                    "redirect_uri": "https://www.zoho.com"
                }

                info_dict ={
                    "url": token_url,
                    "headers": headers,
                    "data": data, 
                    "method": "post",
                }
                
        response_dict = self.send_response(info_dict)
        response_json = response_dict['response']

        if response_dict.get('status') == 'success':
            self.logger.info("Get Access Token: Succesfully retrieved access token.")
            self.access_token = response_json['access_token']
            self.token_info['access_token'] = self.access_token
            self.load_token_to_json()
            return self.access_token
        else:
            self.logger.error(f'Get Access Token: Error getting token: {response_json}. | {json.dumps(response_json)}')

    def response_checker(self, response: object)-> dict:
        """
        Takes in a response object from send_response checks if it's a single or multi response, checks all status codes and returns a dict that contains
        action: (retry/continue), response: response object.

        Args:
            response (obj): Response object that contains either a single response or a json of responses.

        Returns:
            checked_response_dict (dict): Dict formatted: {action: 'retry/continue', response: response_object}
        """
        self.logger.info(f"Response Checker: Checking status code...")
        self.logger.debug(f"Response Checker: {response}")
        fail_codes = [400, 401, 403, 408, 424, 500, 502, 503, 504]
        failed_codes = []
        success_codes = [200, 201, 204]
        checked_response = {
            'action': '',
            'response': ''
        }

        #Check if response is a single item, if so, will get the status code and check if there are any failed codes, otherwise, it will get the failed_codes from multiple items
        try:
            if response.status_code == 401:
                self.logger.warning(f"Response Checker: 401 detected.")
                failed_codes.append(401)
            elif hasattr(response, 'status_code'):
                self.logger.info("Response Checker: Response has a single status_code")
                status_code = response.status_code
                if status_code in fail_codes:
                    failed_codes.append(status_code)  
                response_json = response.json()
            else:
                self.logger.info("Response Checker: Response has a multiple status_codes")
                response_json = response.json()
                failed_codes = [
                (int(item['status'])) 
                for item in response_json['responses']  
                if 'status' in item and int(item['status']) in fail_codes
                ]  

        except Exception as e:
            self.logger.error(f"Response Checker: Error occured in Response Checker: {e}")

        if not failed_codes: #Check if there are no failed fail_codes
            self.logger.info(f"Response Checker: All items successfully uploaded.")
            checked_response['action'] = 'continue'
            checked_response['response'] = response_json
            return checked_response
        #Should make more sophisticated later, get each item that failed, if its 401, only refresh those items
        elif any(fail_code in fail_codes for fail_code in failed_codes): #Checks if there was any failed codes 
            self.logger.warning(f"Response Checker: Items have failed codes. Need to try again for {len(fail_codes)} items")
            if 401 in failed_codes:
                self.logger.warning(f"Response Checker: 401 detected, refreshing access_token")
                self.access_token = self.get_access_token()
                checked_response['action'] = 'retry'
                return checked_response
            
            else:
                self.logger.warning(f"Response Checker: Error codes detected but resolvable: {failed_codes}")
                checked_response['action'] = 'continue'
                checked_response['response'] = response_json
                return checked_response

    def send_response(self, info_dict: dict)-> dict:
        """
        Takes in a info_dict : 
        If retries <= max_retries, proceed, otherwise return dict with status stop to let the program know to stop
        Send the request, check status code, do checks, if sucessful code, return the object with status:stop
        Retries up till the max before returning a stop.
        Returns a response_dict which will let the program know to stop or continue

        Args:
            info_dict (dict): Dict with format {url: url, headers: headers, data/json_body: data/json_body, method: 'get/post'}
        Returns:
            response_dict (dict): Dict with format {status: 'success/fail', response: response_object}
        """
        retries = 1
        max_retries = 3

        url = info_dict.get('url')
        headers = info_dict.get('headers')
        data = info_dict.get('data')
        json_body = info_dict.get('json_body')
        method = info_dict.get('method')

        response_dict = {}

        
        while retries <= max_retries:
            try:
                if method == 'get':
                    response = requests.get(url, headers=headers)
                elif method == 'post':
                    response = requests.post(url, headers=headers, json=json_body, data=data)
                elif method == 'put':
                    data= urlencode({"input_data": data}).encode()
                    response = requests.put(url,headers=headers, data=data)
                
                checked_response = self.response_checker(response)
                    
                if checked_response['action'] == 'retry':
                    self.logger.warning(f"Send Response: {retries}/{max_retries} 401 error detected, retrying.")
                    self.get_access_token()
                    headers['Authorization'] = f'Bearer {self.access_token}'
                    retries += 1

                elif checked_response['action'] == 'continue':
                    self.logger.debug(f"Send Response: Creating response_dict.")
                    response_dict['status'] = 'success'
                    response_dict['response'] = checked_response['response']
                    self.logger.debug(f"Send Response: response_dict created.")
                    break

            except Exception as e:
                self.logger.error(f"Send Response: Error occured trying to get a response. {e}")
                response_dict = {
                    'status': 'fail',
                }
                return response_dict 
            
        self.logger.debug(f"Send Response: Returning responses.")
        return response_dict
    

        

        
        
            
                