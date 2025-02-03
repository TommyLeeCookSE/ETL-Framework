#ETLs\scripts\SharePoint_Connector.py
from utils.Connector import *
import json, requests, traceback, time
from collections import deque

class SharePoint_Connector(Connector):
    def __init__(self,logger):
        super().__init__(logger, token_key='sharepoint_tokens')
        
    def get_site_id(self) -> str:
        """
        Gets site id from cache if present, or gets site id via API call using cached information.

        Returns:
            site_id (str): SharePoint side id. (Will need to take in a domain and path to be dynamic in future.)
        """
        if self.token_info['site_id']:
            site_id = self.token_info['site_id']
            self.logger.info(f"Getting site id from cache: {site_id}")
            return site_id
        else:
            retry = 1
            max_retries = 3
            self.logger.info(f"Site ID not in cache, retrieving via API...")
            while retry <= max_retries:
                try:
                    site_domain = self.token_info['site_domain']
                    site_path = self.token_info['site_path']
                    headers = {
                    "Authorization": f"Bearer {self.access_token}"
                    }
                    api_url = f"https://graph.microsoft.com/v1.0/sites/{site_domain}:{site_path}"

                    site_response = requests.get(api_url, headers=headers)
                    if site_response.status_code == 200:
                        site_info = site_response.json()
                        site_id = site_info.get('id')
                        self.token_info['site_id'] = site_id
                        self.logger.info(f"Site ID: {site_id}")
                        self.load_token_to_json()
                        break
                    elif site_response.status_code == 401:
                        self.logger.warning(f"401 Unauthorized. Refreshing access token. Attempt {retry}/{max_retries}")
                        self.get_access_token()
                    else:
                        self.logger.error(f"Failed to connect: {site_response.status_code}: {json.dumps(site_response.json(),indent=4)}")
                        break
                    
                except requests.exceptions.RequestException as e:
                    self.logger.error(f"Error getting site ID: {e}")
                    self.logger.error(f"Stack trace: {traceback.format_exc()}")
                
                retry += 1

    def get_list_id(self, list_name: str, repeat=True) -> str:
        #TODO Need to add a retry system for this to retry the token if expired.
        """
        Gets list id from cache if present, or gets site id via API call using cached information.
        If repeat is False, will exit as the program could not find the ID.

        Args:
            list_name (str): Name of list to look for in SharePoint.
            repeat (bool): Flag to tell the program not to check again and exit.
        
            Returns:
                list_id: List ID for list_name
                False: When no list_id is found
        """
        self.logger.info(f"Getting List ID for: {list_name}")

        if list_name in self.token_info.get('list_info',{}):
            list_id = self.token_info['list_info'][list_name].get('list_id')
            if list_id:
                self.logger.info(f"List ID found in cache for {list_name}: {list_id}")
                return list_id
            else:
                self.logger.info(f"{list_name} is in cache, but List ID is missing.")
        else:
            self.logger.info(f"{list_name} is not in cache.")

                
        if not repeat :
            self.logger.warning(f"List ID not found for {list_name}, exiting.")
            return False
        
        self.logger.info(f"Retrieving new IDs...")
        try: 
            site_id = self.get_site_id()
            headers = {
                "Authorization": f"Bearer {self.access_token}"
            }
            url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists"

            response = requests.get(url, headers=headers)
            response.raise_for_status()
            raw_lists_info = response.json()

            for list_item in raw_lists_info.get('value',[]):
                cached_list_name = list_item['name']
                cached_list_id  = list_item['id']
                
                if 'list_info' not in self.token_info:
                    self.token_info['list_info'] = {}

                self.token_info['list_info'].setdefault(cached_list_name, {})['list_id'] = cached_list_id
            self.load_token_to_json()
        except Exception as e:
            self.logger.error(f"Error getting SharePoint List ID: {e}")
            return False

        self.logger.info(f"List IDs have been retrieved. Checking again for {list_name}...")

        self.get_list_id(list_name,repeat=False)

    def get_item_ids(self, list_name:str) -> dict:
        """
        Takes in a list name, queries SharePoint for all items in the list and saves to a dict and writes to a json.

        Args:
            list_name (str): Name of the SharePoint list.
        
        Returns:
            sharepoint_list_items (dict): Dict containing SharePoint list contents.
        """
        sharepoint_list_items = {}
        site_id = self.get_site_id()
        list_id = self.get_list_id(list_name)

        self.logger.info("Retrieving items from SharePoint...")

        retry = 1
        max_retries = 3

        while retry <= max_retries:
            headers = {
                'Authorization': f'Bearer {self.access_token}'
            }
            url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$expand=fields"
            while url:
                try:
                    response = requests.get(url, headers=headers)

                    if response.status_code == 200:
                        raw_data = response.json()
                        for item in raw_data['value']:
                            if(fields:= item.get('fields')):
                                sharepoint_id = fields.get('id')
                                sharepoint_list_items[sharepoint_id] = fields
                        self.logger.info(f"Retrieved {len(sharepoint_list_items)} from SharePoint.")
                        url = raw_data.get('@odata.nextLink')
                        if not url:
                            self.logger.info(f"Retrieved {len(sharepoint_list_items)} items from SharePoint.")
                            return sharepoint_list_items
                                
                    elif response.status_code == 401:
                        self.logger.warning(f"401 Unauthorized. Refreshing access token. Attempt {retry}/{max_retries}")
                        self.get_access_token()

                except Exception as e:
                    self.logger.error(f"Error getting Items from SharePoint.: {e}")
            
            retry += 1

    def batch_upload(self, batched_queue: deque):
        """
        Takes in a batched dequeue and uploads to SharePoint.

        Args:
            batched_queue (deque): Dequeue contained batches of requests up to 20.
        """

        self.logger.info("Uploading to SharePoint...")
        batch_url = "https://graph.microsoft.com/v1.0/$batch"
        self.get_access_token()
        headers = {
                        "Authorization": f"Bearer {self.access_token}",
                        "Content-Type": "application/json",
                        "Accept": "application/json"
                    }

        while batched_queue:
            self.logger.info(f"Items left to upload: {len(batched_queue)}")
            batched_item = batched_queue.pop()
            self.logger.debug(f"Batched item: {json.dumps(batched_item,indent=4)}")

            response = requests.post(batch_url, headers=headers, json=batched_item)
            response_json = response.json()

            self.logger.debug(f"Response: {json.dumps(response_json,indent=4)}")
            status_codes = []
            status_codes = [status_codes.append(int(item['status'])) for item in response_json['responses'] if 'status' in item]

            retry_after = []
            for status in status_codes:
                if status == 429:
                    self.logger.warning(f"{status} encountered.")
                    for item in response_json['responses']:
                        if item['status'] == 429:
                            retry_after.append(int(item['headers'].get('Retry-After',300)))
                elif status == 401:
                    self.logger.warning(f"{status} encountered.")
                    self.get_access_token()
                    batched_queue.append(batched_item)
                    headers = {
                        "Authorization": f"Bearer {self.access_token}",
                        "Content-Type": "application/json",
                        "Accept": "application/json"
                    }
                elif status == 400 or status == 404 or status == 409:
                    self.logger.warning(f"{status} encountered.")

            self.logger.info("Batch uploaded.")

            wait_time = max(retry_after) if retry_after else 0
            if wait_time:
                batched_queue.append(batched_item)
                self.logger.info(f"Waiting {wait_time} seconds due to a 429.")
                time.sleep(wait_time)
            
        self.logger.info("All items uploaded.")

    def delete_items(self, list_name:str):
        """
        TODO: Still in progress...
        Takes in a list name, gets the list ID, gets a list of all item IDs, then sends a delete request.

        Args:
            list_name (str): Name of list to be deleted.
        """

        self.logger.info(f"Deleting {list_name} items.")
        list_id = self.get_list_id(list_name)
        item_list = self.get_item_ids(list_name)

        #From here, need to format/batch these items. then call batch_upload

        

        


















