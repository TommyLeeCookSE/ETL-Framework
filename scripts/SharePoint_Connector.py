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
            self.logger.info(f"Get Site Id: Getting site id from cache: {site_id}")
            return site_id
        else:
            self.logger.info(f"Get Site Id: Site ID not in cache, retrieving via API...")

            try:
                site_domain = self.token_info['site_domain']
                site_path = self.token_info['site_path']

                info_dict = {
                    'headers': {"Authorization": f"Bearer {self.access_token}"},
                    'url' : f"https://graph.microsoft.com/v1.0/sites/{site_domain}:{site_path}",
                    'method': 'get'
                    }
                
                site_response = self.send_response(info_dict)

                if site_response['status'] == 'success':
                    self.logger.info("Get Site Id: Succeeded.")
                    site_dict = site_response['response']
                    site_id = site_dict.get('id')
                    self.token_info['site_id'] = site_id
                    self.logger.info(f"Get Site Id:Site ID: {site_id}")
                    self.load_token_to_json()
                else:
                    self.logger.error(f"Get Site Id: Failed to connect: {json.dumps(site_response,indent=4)}")
                
                
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Get Site Id:  Error getting site ID: {e}")
                self.logger.error(f"Get Site Id:  Stack trace: {traceback.format_exc()}")
                
    def get_list_id(self, list_name: str, repeat=True) -> str:
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
        self.logger.info(f"Get List ID: Getting List ID for: {list_name}")

        if list_name in self.token_info.get('list_info',{}):
            list_id = self.token_info['list_info'][list_name].get('list_id')
            if list_id:
                self.logger.info(f"Get List ID: List ID found in cache for {list_name}: {list_id}")
                return list_id
            else:
                self.logger.info(f"Get List ID: {list_name} is in cache, but List ID is missing.")
        else:
            self.logger.info(f"Get List ID: {list_name} is not in cache.")

        if not repeat :
            self.logger.warning(f"Get List ID: List ID not found for {list_name}, exiting.")
            return False
        
        self.logger.info(f"Get List ID: Retrieving new IDs...")
        try: 
            site_id = self.get_site_id()
            info_dict = {
                'url': f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists",
                'headers' : {"Authorization": f"Bearer {self.access_token}"},
                'method': 'get'
            }

            list_id_response = self.send_response(info_dict)
            if list_id_response['status'] == 'success':
                    self.logger.info("Get Site Id: Succeeded.")
                    list_info = list_id_response['response']
                    for list_item in list_info.get('value',[]):
                        cached_list_name = list_item['name']
                        cached_list_id  = list_item['id']
                        
                        if 'list_info' not in self.token_info:
                            self.token_info['list_info'] = {}

                        self.token_info['list_info'].setdefault(cached_list_name, {})['list_id'] = cached_list_id 
            else:
                self.logger.error(f"Get Site Id: Failed to get list_id: {json.dumps(list_id_response)}")
            
            self.load_token_to_json()

        except Exception as e:
            self.logger.error(f"Error getting SharePoint List ID: {e}")
            return False

        self.logger.info(f"List IDs have been retrieved. Checking again for {list_name}...")

        #Calls the program again as it will check for the list_id again.
        self.get_list_id(list_name,repeat=False)

    def get_item_ids(self, list_name:str, params="") -> dict:
        """
        Takes in a list name, queries SharePoint for all items in the list and saves to a dict and writes to a json.

        Args:
            list_name (str): Name of the SharePoint list.
            params (str): Option parameters to filter the list.
        
        Returns:
            sharepoint_list_items (dict): Dict containing SharePoint list contents.
        """
        sharepoint_list_items = {}
        site_id = self.get_site_id()
        list_id = self.get_list_id(list_name)

        self.logger.info("Get Item Ids: Retrieving items from SharePoint...")

        
        
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$expand=fields{params}"
        while url:
            try:
                info_dict = {
                    'url' : url,
                    'headers' : {'Authorization': f'Bearer {self.access_token}'},
                    'method': 'get'
                    }
                
                item_id_response = self.send_response(info_dict)
                
                if item_id_response['status'] == 'success':
                    self.logger.info(f"Get Item IDs: Succeeded.")
                    list_info = item_id_response['response']
                    for item in list_info.get('value',{}):
                        if (fields:= item.get('fields')):
                            sharepoint_id = item.get('id')
                            sharepoint_list_items[sharepoint_id] = fields
                    self.logger.info(f"Get Item Ids: Retrieved {len(sharepoint_list_items)} from SharePoint.")
                    url = list_info.get('@odata.nextLink')
                    if not url:
                        self.logger.info(f"Get Item Ids: Retrieved {len(sharepoint_list_items)} items from SharePoint.")
                        return sharepoint_list_items
                else:
                    self.logger.warning(f"Get Item Ids: Did not correctly retrieve items: {item_id_response}")
                    break

            except Exception as e:
                self.logger.error(f"Get Item Ids: Error getting Items from SharePoint.: {e}")
        
    def batch_upload(self, batched_queue: deque):
        """
        Takes in a batched dequeue and uploads to SharePoint.

        Args:
            batched_queue (deque): Dequeue contained batches of requests up to 20.
        """

        self.logger.info("Uploading to SharePoint...")
        batch_url = "https://graph.microsoft.com/v1.0/$batch"
        self.access_token = self.get_access_token()
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
            try:
                status_codes = [status_codes.append(int(item['status'])) for item in response_json['responses'] if 'status' in item]
            except Exception as e:
                self.logger.error(f"Error getting status codes for response, error: {e}")

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

    def format_and_batch_for_upload_sharepoint(self,change_dict: dict, list_name: str) -> deque:
        """
        Takes in a dict, formats and batches them in groups of 20 for upload to SharePoint.

        Args:
            change_dict (dict): Dict holding the items that need to be updated in SharePoint.
            sharepoint_connector_o (object): Used to get information from SharePoint.
            list_name (str): The name of the SharePoint list (e.g., 'NewWorld_PO_Alert' or 'COT_Employees').
            logger (object, optional): Logger object for logging. Defaults to None.
        
        Returns:
            batch_queue (deque): Dequeue holding the formatted and batched items.
        """
        # Field mappings for each list
        field_mappings = {
            'NewWorld_PO_Alert': {
                'fields': {
                    "Title": "Title",
                    "PO_Type": "PO_Type",
                    "PO_Number": "PO_Number",
                    "Vendor_Name": "Vendor_Name",
                    "Description": "Description",
                    "PO_Amount": "PO_Amount",
                    "Expense": "Expense",
                    "Balance": "Balance",
                    "Expiration_Date": "Expiration_Date",
                    "Expired": "Expired",
                    'Days_Till_Expired' : "Days_Till_Expired",
                    "Unique_ID": "Unique_ID",
                },
                'unique_id_field': 'Unique_ID',
            },
            'COT_Employees': {
                'fields': {
                    "Email": "Email",
                    "Display_Name": "Display_Name",
                    "Department": "Department",
                    "Employee_Id": "Employee_Id",
                    "Job_Title": "Job_Title",
                    "Active": "Active",
                    "Azure_Id": "Azure_Id",
                    "Manager": "Manager",
                    "Licenses@odata.type": "licenses_data_type",
                    "Licenses": "Licenses"
                },
                'unique_id_field': 'Unique_ID',
            },
            'Asset_Pickup_History': {
                'fields': {
                    'Updated': 'Updated'
                },
                'unique_id_field': 'Unique_ID',
            },
            'ServiceDesk_Assets': {
                'fields': {
                    "name": "name",
                    "type": "type",
                    "state": "state",
                    "department": "department",
                    "asset_description": "asset_description",
                    "asset_assigned_user": "asset_assigned_user",
                    "asset_assigned_user_dept": "asset_assigned_user_dept",
                    "asset_assigned_user_email": "asset_assigned_user_email",
                    "saas_id": "saas_id",
                    "created_date": "created_date",
                    "last_updated_date": "last_updated_date",
                    "total_cost": "total_cost",
                    "lifecycle": "lifecycle",
                    "barcode": "barcode",
                    "depreciation_salvage_value": "depreciation_salvage_value",
                    "depreciation_useful_life": "depreciation_useful_life",
                    "ip_address": "ip_address",
                    "replaced_serial_number": "replaced_serial_number",
                    "service_request": "service_request",
                    "imei_number": "imei_number",
                    "cellular_provider": "cellular_provider",
                    "replacement_fund": "replacement_fund",
                    "replacement_date": "replacement_date",
                    "annual_replacement_amt": "annual_replacement_amt",
                    "acquisition_date": "acquisition_date",
                    "asset_vendor_name": "asset_vendor_name",
                    "asset_purchase_cost": "asset_purchase_cost",
                    "asset_product_type": "asset_product_type",
                    "asset_category": "asset_category",
                    "asset_manu": "asset_manu",
                    "asset_serial_no": "asset_serial_no",
                    "warranty_expiry_date": "warranty_expiry_date",
                    "missing_barcode": "missing_barcode",
                    "missing_annual_replacement_amoun": "missing_annual_replacement_amoun",
                    'in_use_date': "in_use_date",
                    'disposed_date': 'disposed_date',
                    'repl_fund': 'repl_fund'
                },
                'unique_id_field': 'Unique_ID',
            },
            'ServiceDesk_Worklogs': {
                'fields': {
                    'module': 'module',
                    'unique_id': 'Unique_ID',
                    'module_id': 'module_id',
                    'created_time': 'created_time',
                    'minutes': 'minutes',
                    'hours': 'hours',
                    'tech_name': 'tech_name',
                    'tech_email': 'tech_email',
                    'worklog_id': 'worklog_id'
                },
                'unique_id_field': 'Unique_ID',
            },
            'INFAzureLicenseUsage' : {
                'fields': {
                    'sku_id': 'sku_id',
                    'sku_name': 'sku_name',
                    'total_licenses': 'total_licenses',
                    'consumed_licenses': 'consumed_licenses',
                    'remaining_licenses': 'remaining_licenses'
                },
                'unique_id_field': 'Unique_ID',
            },
            'TFD_Credential_List' : {
                'fields': {
                    'unique_id': 'Unique_ID',
                    'credentialid': 'credentialid',
                    'categoryid': 'categoryid',
                    'userid': 'userid',
                    'credentialname': 'credentialname',
                    'startdate': 'startdate',
                    'expirationdate': 'expirationdate',
                    'days_till_expired': 'days_till_expired',
                    'status': 'status',
                },
                'unique_id_field': 'Unique_ID',
            },
            'TFD_User_List' : {
                'fields': {
                    'userid': 'userid',
                    'status': 'status',
                    'full_name': 'full_name',
                    'email': 'username',
                    'shift': 'shift',
                    'rank': 'rank',
                    'unit': 'unit',
                    'station': 'station',
                    'supervisor' : 'supervisor',
                },
                'unique_id_field': 'Unique_ID',
            },
            'TFD_Credential_Categories' : {
                'fields': {
                    'categoryid': 'categoryid',
                    'categoryname': 'categoryname'
                },
                'unique_id_field': 'Unique_ID',
            }

        }

        # Get the field mapping for the specified list
        mapping = field_mappings.get(list_name)
        if not mapping:
            raise ValueError(f"Unsupported list name: {list_name}")

        formatted_list = []
        site_id = self.get_site_id()
        list_id = self.get_list_id(list_name)

        for item in change_dict.values():
            sharepoint_id = item.get('sharepoint_id', '')
            operation = item.get('operation', '')
            unique_id = item.get(mapping['unique_id_field'], '')

            # Build list_item_data based on the field mapping
            list_item_data = {"fields": {}}
            for field, source in mapping['fields'].items():
                list_item_data['fields'][field] = item.get(source, 'MISSING')

            # Build batch request based on operation
            if operation == 'DELETE':
                batch_request = {
                    'id': unique_id,
                    'method': 'DELETE',
                    'url': f"/sites/{site_id}/lists/{list_id}/items/{sharepoint_id}",
                }
            elif operation == 'POST':
                batch_request = {
                    'id': unique_id,
                    'method': 'POST',
                    'url': f"/sites/{site_id}/lists/{list_id}/items",
                    "headers": {
                        "Content-Type": "application/json",
                        "Accept": "application/json"
                    },
                    "body": list_item_data
                }
            elif operation == 'PATCH':
                batch_request = {
                    'id': unique_id,
                    'method': 'PATCH',
                    'url': f"/sites/{site_id}/lists/{list_id}/items/{sharepoint_id}",
                    "headers": {
                        "Content-Type": "application/json",
                        "Accept": "application/json"
                    },
                    "body": list_item_data
                }
            else:
                continue

            formatted_list.append(batch_request)

        # Batch the requests into groups of 20
        batch_queue = deque()
        temp_batch_list = []

        for item in formatted_list:
            temp_batch_list.append(item)
            if len(temp_batch_list) == 20:
                batch_queue.append({"requests": temp_batch_list})
                temp_batch_list = []

        if temp_batch_list:
            batch_queue.append({"requests": temp_batch_list})

        return batch_queue