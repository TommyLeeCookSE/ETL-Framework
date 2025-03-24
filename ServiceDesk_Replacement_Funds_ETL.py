import os, sys, json, decimal, pyodbc
from datetime import datetime, date
from collections import deque

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
project_root = os.path.dirname(os.path.abspath(__file__))
os.chdir(project_root)

from utils.Logger import *
from scripts.ServiceDesk_Connector import *
from utils.Utils import *

servicedesk_cache_file_path = r"cache/servicedesk_asset_cache.json"

def clean_servicedesk_details(asset_dict:dict)-> dict:
    """
    Takes in an asset_dict and removes all values not required.
    
    Args:
        asset_dict (dict): Dict contains raw asset details from servicedesk
    
    Returns:
        cleaned_asset_dict (dict): Dict containing the udf_field to check as well as the udf_field to update + asset_id for upload.
    """

    cleaned_asset_dict = {}

    for key, details in asset_dict.items():
        cleaned_asset_dict[key] = {
            'serial_number': details.get('name'),
            'txt_repl_fund': details.get('udf_fields',{}).get('txt_repl_fund'), 
            'repl_fund': details.get('udf_fields',{}).get('udf_char4')}

    return cleaned_asset_dict

def adjust_repl_fund_field(asset_dict:dict)-> dict:
    """
    Takes in an asset_dict that has repl_fund and txt_repl_fund, adjusts txt_repl_fund to Yes if repl_fund exists.
    Only adds items that have to be updated to new dict, discarding already up-to-date items.

    Args:
        asset_dict (dict): Key = saas_id, values = txt_repl_fund and repl_fund.
    Returns:
        cleaned_asset_dict (dict): Dict containing the updated fields.
    """

    cleaned_asset_dict = {}
    for key, details in asset_dict.items():
        repl_fund = details['repl_fund']
        if repl_fund and repl_fund != 'Not in Replacement Fund' and repl_fund != 'Removed from PC Replacement Fund' and not details['txt_repl_fund']:
            cleaned_asset_dict[key] = {'serial_number': details.get('serial_number'), 'txt_repl_fund' : 'Yes'}

    return cleaned_asset_dict

def main():
    try:
        script_name = Path(__file__).stem
        logger = setup_logger(script_name)

        logger.info(f"Main: Current working directory: {os.getcwd()}")
        logger.info(f"Main: {script_name} executed, Updating Replacement Funds")

        #Gets the last updated data from cache
        checksum, items, dates, previous_servicedesk_cache_list = read_servicedesk_cache(servicedesk_cache_file_path)
        last_updated_iso = dates['current_time_epoch_ms']

        #Pull lists of assets from ServiceDesk since date last updated.
        servicedesk_connector_o = ServiceDesk_Connector(logger)
        current_asset_list_dict = servicedesk_connector_o.get_assets_from_servicedesk(last_updated=last_updated_iso)

        if len(current_asset_list_dict) == 0:
            logger.info(f"Main: No assets returned, exiting.")
            return

        #Pull asset details from ServiceDesk
        raw_asset_details_dict = {
            value.get('asset_id') : servicedesk_connector_o.get_assets_from_servicedesk(asset_id = value.get('asset_id'))
            for value in current_asset_list_dict.values()
        }
        #Checks if replacement fund exists and if repl_fund_txt does not, if so, mark it yes
        cleaned_asset_details = clean_servicedesk_details(raw_asset_details_dict)
        logger.info(f"Cleaned Asset Details: {json.dumps(cleaned_asset_details,indent=4)}")
        cleaned_asset_details = adjust_repl_fund_field(cleaned_asset_details)
        logger.info(json.dumps(cleaned_asset_details,indent=4))

        formatted_deque = servicedesk_connector_o.format_and_batch_for_upload_servicedesk(cleaned_asset_details, 'repl_fund')
        response = servicedesk_connector_o.upload_to_servicedesk(formatted_deque)
        logger.info(json.dumps(response,indent=4))
        # Upload to servicedesk the updated text.

       
    
        logger.info(f"Main: ETL Completed successfully: Exit Code 0")
        
    except Exception as e:
        logger.error(f"Main: Error occured in main: {e}: Exit Code 1")
        logger.error("Main: Traceback:", exc_info=True)
        return 1

main()