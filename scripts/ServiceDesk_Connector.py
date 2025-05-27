from utils.Connector import *
from collections import deque
from urllib.parse import urlencode

class ServiceDesk_Connector(Connector):
    def __init__(self, logger):
        super().__init__(logger, token_key='service_desk_tokens')

        self.base_url = "https://servicedesk.torranceca.gov"
        self.max_row_count = 100

        self.headers = {
            "Accept": "application/vnd.manageengine.sdp.v3+json",
            "Authorization": f"Zoho-oauthtoken {self.access_token}",
            "Content-Type": "application/x-www-form-urlencoded" 
        }

    def build_servicedesk_asset_data(self, change_item:dict, module_name)->str:
        """
        Takes in a dict that holds the item that needs to be formatted for ServiceDesk.
        Item will be formattedb based off of user type then converted to a f-string and returned.

        Args:
            change_item (dict): Dict containing parameters to upload.
        Returns:
            formatted_item (str): Formatted f-string of the change_item.
        """
        self.logger.info(f"Change Dict: {json.dumps(change_item, indent=4)}")
        if module_name == 'asset_upload':
            asset_type = change_item.get('Asset_Type')
            serial_number_key = "serial_number" if asset_type != "Monitor" else "name"
            asset_key = "asset" if asset_type != "Monitor" else "custom_asset_monitor"

            asset_data = {
                asset_key: {
                    serial_number_key: change_item.get("Serial_Number", ""),
                    "barcode": int(change_item.get("Barcode", "0")) if change_item.get("Barcode") else "",
                    "udf_fields": {
                        "udf_char8": int(change_item.get("Request_Number", "0")) if change_item.get("Request_Number") else "",
                        "udf_char7": change_item.get("Replaced_Serial_Number", "")
                    },
                    "department": {
                        "name" : change_item.get('User_Department') 
                    },
                    "state": {
                        "name": "In Use"
                    }
                }
            }
            self.logger.info(f"Asset Data: {json.dumps(asset_data, indent=4)}")

            user_type = change_item.get('User_Type',"")

            if user_type == 'User':
                asset_data[asset_key]['user'] = {
                    'name': change_item.get('User_Name',''),
                    'email_id': change_item.get('User','')
                }
            elif user_type == 'Shared Device':
                asset_data[asset_key]['location'] = change_item.get('User_Location')

        elif module_name == 'repl_fund':
            asset_data = {
                "asset": {
                    "udf_fields": {
                        'txt_repl_fund' : [change_item.get('txt_repl_fund')]
                    }
                }
            }

        formatted_item = json.dumps(asset_data,indent=4)
        self.logger.info(f"Build_Asset_Data: Asset Data: {formatted_item}")

        return formatted_item
        
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
                input_data = self.build_servicedesk_asset_data(item, module_name)

                input_dict = {
                    'serial_number': item.get('Serial_Number',''),
                    'asset_id': None,
                    'asset_type': item.get('Asset_Type'),
                    'input_data': input_data,
                    'sharepoint_id': key,
                    }
                
            elif module_name == "repl_fund":
                input_data = self.build_servicedesk_asset_data(item, module_name)
                input_dict = {
                    'asset_id' : key,
                    'input_data' : input_data,
                    'sharepoint_id' : None
                }
            
            self.logger.info(f"Format_and_Batch: Formatted item: {input_dict}")
            formatted_deque.append(input_dict)
        
        self.logger.info(f"Formatted {len(formatted_deque)} items.")
        
        return formatted_deque
    
    def upload_to_servicedesk(self, formatted_deque: deque)-> list:
        """
        Takes in a deque and iterates over it, uploading each item to Service Desk.

        Args:
            formatted_deque (deque): Contains a deque of dicts to be uploaded.
        Returns:
            response_list (list): List contains dicts that have {sharepoint_id: (str), response_item: (dict)}

        """
        self.logger.info("Upload_to_Servicedesk: Beginning upload to ServiceDesk.")

        response_list = []
        counter = 1
        num_items = len(formatted_deque)
        while formatted_deque:
            self.logger.info(f"Upload_to_Servicedesk: ({counter}/{num_items}) Items to upload to ServiceDesk.")
            upload_item = formatted_deque.pop()
            self.logger.debug(f"Upload_to_Servicedesk: Uploading item: {upload_item}")

            input_data = upload_item.get('input_data')
            asset_id = upload_item.get('asset_id')
            sharepoint_id = upload_item.get('sharepoint_id')
            asset_type = upload_item.get('asset_type')
            if asset_type != "Monitor":
                api_url = f'{self.base_url}/api/v3/assets/{asset_id}'
            else:
                api_url = f'{self.base_url}/api/v3/custom_asset_monitor/{asset_id}'

            info_dict = {
                "url" : api_url,
                "headers" : self.headers,
                "data": input_data,
                "method": "put"
            }

            response = self.send_response(info_dict)

            if (response := response):
                response_dict = {'sharepoint_id': sharepoint_id, 'response_item': response}
                response_list.append(response_dict)
            counter += 1

        return response_list
    
    def get_assets_from_servicedesk(self, asset_id: int = None, serial_number:str = None, last_updated:int = None, asset_type=None) -> dict:
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
            if serial_number and not asset_type:
                self.logger.info(f"Get_Assets_Servicedesk: Getting asset_id by Serial Number: {serial_number}")
                has_more_rows, asset_dict = self.get_list_of_assets(page, fields_required=None, search_criteria={"field": "name", "condition": "eq", "value": f'{serial_number}.cot.torrnet.com'})
                self.logger.debug(f"Get_Assets: Asset_Dict: {asset_dict}")
            
            elif serial_number and asset_type:
                self.logger.info(f"Get_Assets_Servicedesk: Getting {asset_type} asset_id by Serial Number: {serial_number}")
                field_key = "serial_number" if asset_type != "Monitor" else "name"
                has_more_rows, asset_dict = self.get_list_of_assets(page, fields_required=None, search_criteria={"field": field_key, "condition": "eq", "value": serial_number}, asset_type=asset_type)
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
    
    def get_list_of_assets(self, page_number:int, fields_required:list = None, search_criteria:dict = None, asset_type = None) -> tuple:
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
        if asset_type == "Monitor":
            api_url = f'{self.base_url}/api/v3/custom_asset_monitor'
            fields_required = ['name','barcode','user', 'department', 'state', 'udf_fields']
        else:
            api_url = f'{self.base_url}/api/v3/assets'
        self.logger.debug(f"Get_List_Assets: Calling {api_url}")
        
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
            "headers" : self.headers,
            "method": "get"
        }

        response_dict = self.send_response(info_dict)
        self.logger.debug(json.dumps(response_dict,indent=4))
        if (list_info := response_dict.get('response',{}).get('list_info')):
            # self.logger.info(f"Get_List_Assets: List info: {json.dumps(list_info,indent=4)}")
            has_more_rows = list_info.get('has_more_rows',False)
        else:
            self.logger.error(f"Get_List_Assets: No List Info Detected!")
            has_more_rows = False

        if (asset_list:= response_dict.get('response',{}).get('assets')) or (asset_list:= response_dict.get('response',{}).get('custom_asset_monitor')):
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

        info_dict = {
            "url" : api_url,
            "headers" : self.headers,
            "method": "get"
        }

        response_dict = self.send_response(info_dict)

        asset_info = response_dict.get('response',{}).get('asset',{})
        
        return asset_info

        # self.logger.info(f"Get_Asset_by_ID: Response: {json.dumps(response_dict,indent=4)}")

    def get_list_of_item_ids(self, module_name: str, page_number: int, fields_required:list = None, search_criteria:dict = None)-> dict:
        """
        Retrieves a list of item_ids for the module name passed in. Retrieves only one page at a time.

        Args:
            module_name (str): Name of the module being retrieved.
            page_number (int): Current page being retrieved.
        Returns:
            response_dict (dict): List of item ids and other pertinent info.
        """

        self.logger.info(f"Get_List_Item_Ids: Getting list for module: ({module_name.upper()}), page: {page_number}")

        api_url = f'{self.base_url}/api/v3/{module_name}'
        self.logger.debug(f"Get_List_Item_Ids: Calling {api_url}")

        params = {
            "input_data": json.dumps({
                "list_info":{
                    "page": page_number, 
                    "row_count": self.max_row_count,
                    "sort_field": "created_time",
                    "sort_order": "desc",
                    **({"search_criteria": (search_criteria := search_criteria)} if search_criteria else {}),
                    **({"fields_required": (fields_required := fields_required)} if fields_required else {})
                }
            })        
        }

        encoded_params = urlencode(params)
        final_url = f"{api_url}?{encoded_params}"

        info_dict = {
            "url" : final_url,
            "headers" : self.headers,
            "method": "get"
        }

        response_dict = self.send_response(info_dict)
        
        return response_dict if response_dict else {}
    
    def get_worklogs(self, module_name: str, worklog_dict: dict, fields_required:list = None, search_criteria:dict = None)-> dict:
        """
        Retrieves worklogs for the requested modules_id and all of its subtasks.

        Args:
            module_name (str): Name of module that is currently be retrieved.
            worklog_dict (dict): Dict containing the ids of modules and tasks.
            fields_required (list): Limits what fields are returned.
            search_criteria (dict): Ensures only certain records are returned.
        Returns:
            id_dict (dict): Dict that contains the worklogs
        """
        response_list = []

        total_items = len(worklog_dict)
        for counter, values in enumerate(worklog_dict.values(), start=1):
            module_id = values.get('module_id')
            self.logger.info(f"Get_Worklogs: Getting worklogs ({counter}/{total_items}) for module: ({module_name.upper()}) | Ticket: {module_id}")

            params = {
                "input_data": json.dumps({
                    "list_info":{
                        "row_count": self.max_row_count,
                        "sort_field": "id",
                        "sort_order": "asc",
                        **({"search_criteria": (search_criteria := search_criteria)} if search_criteria else {}),
                        **({"fields_required": (fields_required := fields_required)} if fields_required else {})
                    }
                })        
            }


            api_url = f'{self.base_url}/api/v3/{module_name}/{module_id}/worklogs'
            self.logger.debug(f"Get_Worklogs: Calling {api_url}")
            encoded_params = urlencode(params)

            final_url = f"{api_url}?{encoded_params}"

            info_dict = {
                "url" : final_url,
                "headers" : self.headers,
                "method": "get"
            }

            id_info_dict = {'module_id': module_id}

            response_list.append([id_info_dict,self.send_response(info_dict)])

        for response_pair in response_list:
            id_info = response_pair[0]
            response = response_pair[1]

            module_id = id_info.get('module_id')
            
            worklogs = response.get('response',{}).get('worklogs',[]) or response.get('response',{}).get('timesheet',[])

            for worklog in worklogs:
                owner = worklog.get('owner',{})
                tech_name = owner.get('name')
                tech_email = owner.get('email_id') or owner.get('email')
                time_spent_ms = worklog.get('time_spent',{}).get('value') or worklog.get('total_time_spent')
                if isinstance(time_spent_ms,dict):
                    hours = int(time_spent_ms.get('hours',0))
                    minutes = int(time_spent_ms.get('minutes',0))
                    time_spent_ms = (hours*60*60*1000) + (minutes*60*1000)
                start_time = worklog.get('start_time',{}).get('display_value')
                worklog_id = worklog.get('id') or module_id

                if module_id:
                    worklog_dict[module_id]['worklog_details'][worklog_id] = {
                        'worklog_id': worklog_id,
                        'tech_name': tech_name,
                        'tech_email': tech_email,
                        'time_spent_ms': time_spent_ms,
                        'start_time': start_time
                    }
        self.logger.info(f"Get_Worklogs: Created {len(worklog_dict)} worklogs.")

        return worklog_dict

    def get_list_project_tasks(self, worklog_dict, fields_required:list = None, search_criteria:dict = None)-> dict:
        """
        Retrieves list of project tasks and returns a dict with the required information.

        Args:
        worklog_doct
            worklog_dict (dict): Dict containing the ids of modules and tasks.
            fields_required (list): Limits what fields are returned.
            search_criteria (dict): Ensures only certain records are returned.
        Returns:
            id_dict (dict): Dict that contains the worklogs
        """
        response_list = []

        total_items = len(worklog_dict)
        for counter, values in enumerate(worklog_dict.values(), start=1):
            module_id = values.get('module_id')
            self.logger.info(f"Get_Worklogs: Getting list of tasks ({counter}/{total_items}) for PROJECTS |  Ticket: {module_id}")

            params = {
                "input_data": json.dumps({
                    "list_info":{
                        "row_count": self.max_row_count,
                        "sort_field": "id",
                        "sort_order": "asc",
                        **({"search_criteria": (search_criteria := search_criteria)} if search_criteria else {}),
                        **({"fields_required": (fields_required := fields_required)} if fields_required else {})
                    }
                })        
            }


            api_url = f'{self.base_url}/api/v3/projects/{module_id}/tasks'
            self.logger.debug(f"Get_Worklogs: Calling {api_url}")
            encoded_params = urlencode(params)

            final_url = f"{api_url}?{encoded_params}"

            info_dict = {
                "url" : final_url,
                "headers" : self.headers,
                "method": "get"
            }

            id_info_dict = {'module_id': module_id}

            raw_response = self.send_response(info_dict)
            tasks = raw_response.get('response',{}).get('tasks',[])
            for task in tasks:
                task_id = task.get('id')
                project_id = task.get('project',{}).get('id')
                owner = task.get('owner',{})
                if owner:
                    tech_name = owner.get('name')
                    tech_email = owner.get('email_id')
                else:
                    tech_name = None
                    tech_email = None
                created_time = task.get('created_date',{}).get('display_value')
                time_spent_ms = task.get('estimated_effort')
                title = task.get('title')

                if project_id in worklog_dict:
                    worklog_dict[project_id]['worklog_details'][task_id] = {
                        'task_id' : task_id,
                        'tech_name' : tech_name,
                        'tech_email' : tech_email,
                        'start_time' : created_time,
                        'time_spent_ms' : time_spent_ms,
                        'title' : title
                    }

        self.logger.info(f"Get_Worklogs: Created {len(worklog_dict)} worklogs.")

        return worklog_dict

    def get_worklogs_from_servicedesk(self, module_name: str, max_pages:int, fields_required:list=None, search_criteria:dict=None) -> dict:
        """
        Retrieves worklogs from ServiceDesk. Takes a module name and iterates over module_ids (up to the specified amount of max pages)
        For each list of module ids retrieved:
        Get task ids
        Get worklog details
        Get worklog details for tasks
        Return once the max_pages has been reached.

        Args:
            module_name str(): Name of module that is currently be retrieved.
            max_pages (int): Max number of pages to retrieve each page has approx 100 records.
            fields_required (list): Limits what fields are returned.
            search_criteria (dict): Ensures only certain records are returned.
        Returns:
            worklog_dict (dict): Contains worklogs in format:
            {
                module_id:{
                    worklog_details: {}
                }
            }
        """
        worklog_dict = {}
        current_page = 1
        has_more_rows = True

        while current_page <= max_pages and has_more_rows:
            response = self.get_list_of_item_ids(module_name,current_page)
            if response:
                module_id_list = response.get('response',{}).get(module_name,{})
                has_more_rows = response.get('response',{}).get('list_info',{}).get('has_more_rows',False)
                worklog_dict.update(
                    {item['id'] : 
                        {
                            "module_id": item['id'],
                            "worklog_details": {
                            },
                            "is_incident": (not item.get('is_service_request') if module_name == 'requests' else None) 
                        } 
                        for item in module_id_list
                    }
                )
                self.logger.info(f"Get_Worklogs: Retreived ({len(worklog_dict)}/{max_pages*100}) items.")
            else:
                self.logger.error(f"Get_Worklogs encountered an error retreiving a response.")
                has_more_rows = False
            
            current_page += 1
        self.logger.info(f"Get_Worklogs: Retreived total of {len(worklog_dict)} items.")
        
        if module_name != 'projects':
            worklog_dict = self.get_worklogs(module_name,worklog_dict)
            cleaned_worklog_dict = {
                key : worklogs
                for key, worklogs in worklog_dict.items() 
                if worklogs.get('worklog_details')
            }
        else:
            cleaned_worklog_dict = self.get_list_project_tasks(worklog_dict)

        self.logger.info(f"Get Worklogs: Done fetching worklogs.")
        
        return cleaned_worklog_dict
    







    
        





