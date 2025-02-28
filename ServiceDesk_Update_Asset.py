import os, sys, json, decimal, pyodbc
from datetime import datetime, date
from collections import deque

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
project_root = os.path.dirname(os.path.abspath(__file__))
os.chdir(project_root)

from utils.Logger import *
from scripts.ServiceDesk_Connector import *
from scripts.SharePoint_Connector import *
from utils.Utils import *


def main():
    try:
        script_name = Path(__file__).stem
        logger = setup_logger(script_name)

        logger.info(f"Main: Current working directory: {os.getcwd()}")
        logger.info(f"Main: {script_name} executed, doing something...")

        sharepoint_connector_o = SharePoint_Connector(logger)

        fields = ['Technician','Techician_Name', 'Request_Number', 'User_Type', 'User', 'User_Name', 'User_Department', 'User_Location', 'Asset_Type', 'Serial_Number', 'Barcode', 'Bag_Type', 'Replaced_Serial_Number', 'Updated']
        field_string = "($select="
        for item in fields:
            if item == fields[-1]:
                field_string += f'{item})'
            else:
                field_string += f'{item},'

        logger.info(f"Main: Getting Asset Pickup History.")
        raw_sharepoint_items_dict = sharepoint_connector_o.get_item_ids('Asset_Pickup_History', field_string)
        sharepoint_items_dict = {key: value for key, value in raw_sharepoint_items_dict.items() if value.get('Updated') == 'success'}

        servicedesk_connector_o = ServiceDesk_Connector(logger)
        formatted_deque = servicedesk_connector_o.format_and_batch_for_upload_servicedesk(sharepoint_items_dict, "asset_upload")

        for item in formatted_deque:
            if (serial_number:= item.get('serial_number')):
                logger.debug(f"Main: Serial Number: {serial_number}")
                temp_asset_dict = servicedesk_connector_o.get_assets_from_servicedesk(serial_number=serial_number)
                asset = next(iter(temp_asset_dict.values()))
                item['asset_id'] = asset['asset_id']
            logger.debug(f"Main: Item: {json.dumps(item,indent=4)}")

        response_list = servicedesk_connector_o.upload_to_servicedesk(formatted_deque)
        logger.debug(json.dumps(response_list,indent=4))

        upload_dict = {}
        for response_dict in response_list:
            sharepoint_id = response_dict.get('sharepoint_id')
            status = response_dict.get('response_item',{}).get('status','fail')

            upload_dict.update({
                sharepoint_id: {
                    'sharepoint_id': sharepoint_id,
                    'Updated': status,
                    'operation': 'PATCH',
                    'unique_id_field': sharepoint_id
                }
            })

        logger.debug(json.dumps(upload_dict,indent=4))


        batch_que = sharepoint_connector_o.format_and_batch_for_upload_sharepoint(upload_dict, 'Asset_Pickup_History')
        sharepoint_connector_o.batch_upload(batch_que)
    except Exception as e:
        logger.error(f"Error occured in main: {e}: Exit Code 1")
        logger.error("Traceback:", exc_info=True)
        return 1
    

main()