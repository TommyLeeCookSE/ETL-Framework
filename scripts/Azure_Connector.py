#ETLs\scripts\Azure_Connector.py
from utils.Connector import *
import json, requests, time
from collections import deque

class Azure_Connector(Connector):
    def __init__(self,logger):
        super().__init__(logger, token_key='azure_tokens')
        
    def get_users_info(self) -> dict:
        """
        Retrieves Azure User Info and combines it with License Info.

        Returns:
            user_info_dict (dict): Dict containing Azure user info.
        """
        self.logger.info("Getting Azure User Information")
        self.user_info_dict = self.get_users()
        self.get_user_license_info()
        return self.user_info_dict
  
    def get_users(self):
        """
        Gets Azure User Info and saves to a dict with the Azure ID as they key.
        """
        self.logger.info("Getting Azure User Info...")
        url = "https://graph.microsoft.com/v1.0/users?$select=displayName,mail,department,jobTitle,employeeId,id,employee_type&$expand=manager($select=displayName)"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        all_users_list = []

        while url:
            response = requests.get(url, headers=headers)

            if response.status_code==200:
                data = response.json()
                all_users_list.extend(data['value'])
                self.logger.info(f"Added {len(data['value'])} users, total users: {len(all_users_list)}")
                url = data.get('@odata.nextLink', None)
            elif response.status_code == 401:
                self.logger.warning("401 Unauthorized. Refreshing access token.")
                self.access_token = self.get_access_token()
                headers = {
                    "Authorization": f"Bearer {self.access_token}",
                    "Content-Type": "application/json"
                }
            else:
                self.logger.error(f"Failed to connect: {response.status_code}: {json.dumps(response.json(),indent=4)}")
                break

        all_users_dict = {}
        for user in all_users_list:
            user['licenses'] = []
            all_users_dict[user['id']] = user

        self.logger.info("Retrieved Azure User Info.")
        return all_users_dict

    def get_user_license_info(self):
        """
        Batches users, retrieves licenses via their Azure ID, updates the users_info_dict.
        """
        self.logger.info("Retrieving Azure User License Info...")
        batch_queue = deque()
        batch_list = []
        self.logger.info(f"Batching users for Azure query.")
        for user in self.user_info_dict:
            user_id = user 
            batch_list.append({
                'id':str(user_id),
                "method": "GET",
                "url" : f"/users/{user_id}/licenseDetails"
            })
            if len(batch_list) == 20:
                batch_body = {"requests":batch_list}
                batch_queue.append(batch_body)
                batch_list = []
        
        if len(batch_list) > 0:
            batch_body = {"requests":batch_list}
            batch_queue.append(batch_body)
            batch_list = []
        
        self.logger.info(f"Finished batching users. Total batches {len(batch_queue)}")

        while batch_queue:
            self.logger.info(f"Getting batch: {len(batch_queue)}")
            batch_url = "https://graph.microsoft.com/v1.0/$batch"
            headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
            }
            batch_body = batch_queue.pop()
            retries = 3
            for attempt in range(retries):
                response = requests.post(batch_url,headers=headers,json=batch_body)

                if response.status_code == 200:
                    results = response.json().get("responses",[])
                    licenses_to_find = ['SPE_F5_SECCOMP_GCC', 'M365_G5_GCC','WACONEDRIVESTANDARD_GOV']

                    for item in results:
                        if 'id' in item and 'body' in item:
                            id = item['id']
                            licenses_raw = item['body'].get('value',[])
                            licenses_filtered = []
                            for license in licenses_raw:
                                license_value = license['skuPartNumber']
                                if license_value.upper() in licenses_to_find:
                                    licenses_filtered.append(license_value)
                                if id in self.user_info_dict:
                                    self.user_info_dict[id]['licenses'] = licenses_filtered
                    break
                elif response.status_code == 401:
                    self.logger.warning("401 Unauthorized. Refreshing access token.")
                    self.access_token = self.get_access_token()
                    headers = {
                        "Authorization": f"Bearer {self.access_token}",
                        "Content-Type": "application/json"
                    }
                    time.sleep(1)
                else:
                    self.logger.error(f"Failed to get licenses: {response.status_code} : {json.dumps(response.json(),indent=4)}")
                    break
        self.logger.info("Retrieved Azure User License Info.")