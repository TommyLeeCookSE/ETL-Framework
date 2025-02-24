from utils.Connector import *
from collections import deque
from urllib.parse import urlencode

class ServiceDesk_Connector(Connector):
    def __init__(self, logger):
        super().__init__(logger, token_key='service_desk_tokens')

        self.base_url = "https://servicedesk.torranceca.gov"
        self.max_row_count = 100
        

    def format_and_batch_for_upload_servicedesk(self,change_dict: dict, module_name: str) -> deque:
        """
        Takes in a change dict, formats items for the specific servicedesk upload type, then batches and returns a deque for uploading to servicedesk.

        Args:
            change_dict (dict): Dict containing changes to be uploaded.
            module_name (str): Asset/Worklog/Requests, used to determine what the key pairs will be.
        
            Returns:
                formatted_deque (deque): Deque holding batches of up to 20 items to be uploaded.

        """
        formatted_deque = deque()
        for key,item in change_dict.items():
            if module_name == "asset_upload":
                input_data = f'''{{
                                    "asset" : {{
                                        "serial_number": "{item.get('Serial_Number','')}",
                                        "barcode": "{int(item.get('Barcode',''))}",
                                        "udf_fields":{{
                                            "udf_char8": "{int(item.get('Request_Number',''))}",
                                            "udf_char7": "{item.get('Replaced_Serial_Number')}"
                                        }},
                                        "user":{{
                                            "name": "{item.get('User_Name','')}",
                                            "email_id": "{item.get('User','')}"
                                        }},
                                        "state":{{
                                            "name": "In Use",
                                        }}
                                    }}

                }}'''

                input_dict = {
                    'serial_number': item.get('Serial_Number',''),
                    'asset_id': None,
                    'input_data': input_data,
                    'sharepoint_id': key,
                    }
            

            self.logger.info(f"Format_and_Batch: Formatted item: {input_dict}")
            formatted_deque.append(input_dict)
        
        self.logger.info(f"Formatted {len(formatted_deque)} items.")
        
        return formatted_deque
    
    def upload_to_servicedesk(self, formatted_deque: deque):
        """
        Takes in a deque and iterates over it, uploading each item to Service Desk.

        Args:
            formatted_deque (deque): Contains a deque of dicts to be uploaded.

        """
        self.logger.info("Upload_to_Servicedesk: Beginning upload to ServiceDesk.")


        while formatted_deque:
            self.logger.info(f"Upload_to_Servicedesk: {len(formatted_deque)} Items to upload to ServiceDesk.")
            upload_item = formatted_deque.pop()
            self.logger.debug(f"Upload_to_Servicedesk: Uploading item: {upload_item}")

            input_data = upload_item.get('input_data')
            asset_id = upload_item.get('asset_id')
            sharepoint_id = upload_item.get('sharepoint_id')
            
            api_url = f'{self.base_url}/api/v3/assets/{asset_id}'
            headers = {
                "Accept": "application/vnd.manageengine.sdp.v3+json",
                "Authorization": f"Zoho-oauthtoken {self.access_token}",
                "Content-Type": "application/x-www-form-urlencoded" 
            }
            info_dict = {
                "url" : api_url,
                "headers" : headers,
                "data": input_data,
                "method": "put"
            }

            response = self.send_response(info_dict)

            if (status := response.get('status')):
                return status, sharepoint_id

    def get_assets_from_servicedesk(self, asset_id: int = None, serial_number:str = None, last_updated:int = None) -> dict:
        """
        Gets assets from servicedesk. If asset_id is specified, pulls only that asset_id. If last_updated is specified, pulls all items since that date. If nothing is specified, pulls everything.
        Checks if the asset_id is in the cache first, if not, pulls the asset list 100 items at a time, checking each item's serial number, when found, stops.
        Only pass in a asset_id or a last_updated, not both.

        Args:
            asset_id (int): asset_idr of asset to be looked up, used to get the specific item.
            serial_number (str): Serial numbe of the asset.
            last_updated (int): Gets items last updated within the timeframe specified. Unix timestamp.
        Returns:
            asset_dict (dict): Returns a dict of dicts with the asset_id as the key.
        """
        asset_dict = {}
        page = 1
        has_more_rows = True

        if asset_id or last_updated or serial_number:
            #if serial_number true, check cache to see if it's in the cache, if so and has the asset_id, return the asset_id
            #If not, get the asset_id by calling the list of assets 100 at a time, checking each item by calling it by its asset_id till it finds the serial_number
            if serial_number:
                self.logger.info(f"Get_Assets_Servicedesk: Getting asset_id by Serial Number: {serial_number}")
                has_more_rows, asset_dict = self.get_list_of_assets(page, fields_required=['name'], search_criteria={"field": "name", "condition": "eq", "value": f'{serial_number}.cot.torrnet.com'})
                self.logger.debug(f"Get_Assets: Asset_Dict: {asset_dict}")

            elif asset_id:
                self.logger.info(f"Get_Assets_Servicedesk: Getting asset by asset_id: {asset_id}")
                asset_dict = self.get_asset_by_id(asset_id)

            elif last_updated:
                self.logger.info(f"Get_Assets_Servicedesk: Getting asset_id by last_Updated: {last_updated}")
                while has_more_rows:
                    search_criteria={"field":"last_updated_time", "condition": "gt", "value": f"{last_updated}"}
                    has_more_rows, temp_asset_dict = self.get_list_of_assets(page, search_criteria=search_criteria)
                    asset_dict.update(temp_asset_dict)
                    page += 1
                    self.logger.info(f"Get_Assets: Retrieved {len(asset_dict)} items.")
                    self.logger.debug(f"Get_Assets: Has More Rows: {has_more_rows}.")

        elif not asset_id and not last_updated and not serial_number:
            self.logger.info("Get_Assets: No parameters specified, getting all assets.")
            while has_more_rows:
                self.logger.info(f"Get_Assets: Getting Page: {page} of Assets.")
                has_more_rows, temp_asset_dict = self.get_list_of_assets(page)
                asset_dict.update(temp_asset_dict)
                page += 1
                self.logger.info(f"Get_Assets: Retrieved {len(asset_dict)} items.")


        self.logger.info(f"Get_Assets: Done retrieving items")
        return asset_dict
    
    def get_list_of_assets(self, page_number:int, fields_required:list = None, search_criteria:dict = None) -> tuple:
        """
        Takes in a page number and gets a list of 100 assets, returns the assets as a list.

        Args:
            page_number (int): Current page to retrieve.
        Returns:
            has_more_rows (bool): Bool for more values.
            asset_dict (list): Dict of assets/id pairs. 
        """
        has_more_rows = False  
        asset_dict = {}

        self.logger.info(f"Get_List_Assets: Getting list of assets for page: {page_number}")

        api_url = f'{self.base_url}/api/v3/assets'
        self.logger.debug(f"Get_List_Assets: Calling {api_url}")

        headers = {
        "Accept": "application/vnd.manageengine.sdp.v3+json",
        "Authorization": f"Zoho-oauthtoken {self.access_token}",
        "Content-Type": "application/json" 
        }
        
        params = {
            "input_data": json.dumps({
                "list_info":{
                    "page": page_number, 
                    "row_count": self.max_row_count,
                    "sort_field": "id",
                    "sort_order": "asc",
                    **({"search_criteria": (search_criteria := search_criteria)} if search_criteria else {}),
                    **({"fields_required": (fields_required := fields_required)} if fields_required else {})
                }
            })        
        }
        encoded_params = urlencode(params)
        final_url = f"{api_url}?{encoded_params}"

        info_dict = {
            "url" : final_url,
            "headers" : headers,
            "params": params,
            "method": "get"
        }

        response_dict = self.send_response(info_dict)
        if (list_info := response_dict.get('response',{}).get('list_info')):
            # self.logger.info(f"Get_List_Assets: List info: {json.dumps(list_info,indent=4)}")
            has_more_rows = list_info.get('has_more_rows',False)
        else:
            self.logger.error(f"Get_List_Assets: No List Info Detected!")
            has_more_rows = False

        if (asset_list:= response_dict.get('response',{}).get('assets')):
            # self.logger.info(f"Asset_List: {json.dumps(asset_list,indent=4)}")
            asset_dict = {
                item.get('name'): {
                    'asset_id': item.get('id'), 
                    'asset_serial_number': item.get('name')
                }
                
                for item in asset_list
            }
        
        return (has_more_rows, asset_dict)

    def get_asset_by_id(self, asset_id: str) -> dict:
        """
        Takes in a str, request detailed information from it, returns a dict.
        
        Args:
            asset_id (str): id of the asset

        Returns:
            detailed_asset_dict (dict): Dict containing details information on the asset.
        """
        self.logger.info(f"Get_Asset_by_ID: Getting list of assets for asset_id: {asset_id}")

        api_url = f'{self.base_url}/api/v3/assets/{asset_id}'
        self.logger.debug(f"Get_Asset_by_ID: Calling {api_url}")

        headers = {
        "Accept": "application/vnd.manageengine.sdp.v3+json",
        "Authorization": f"Zoho-oauthtoken {self.access_token}",
        "Content-Type": "application/json" 
        }

        info_dict = {
            "url" : api_url,
            "headers" : headers,
            "method": "get"
        }

        response_dict = self.send_response(info_dict)

        asset_info = response_dict.get('response',{}).get('asset',{})
        
        return asset_info

        # self.logger.info(f"Get_Asset_by_ID: Response: {json.dumps(response_dict,indent=4)}")




    
        





