import os, sys, traceback
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
project_root = os.path.dirname(os.path.abspath(__file__))
os.chdir(project_root)
from utils.Logger import *
from scripts.Azure_Connector import *
from scripts.SharePoint_Connector import *
from collections import deque
from utils.Utils import *


def fix_license(wrong_license: str) -> str:
    """
    Takes in a license str, returns the correct varient.
    
    Args:
        wrong_license (str): License name in wrong format.
        
    Returns:
        correct_license (str): Corrected license name.
    """
    license_dict = {
        "M365_G5_GCC": "M365_G5",
        "SPE_F5_SECCOMP_GCC": "M365_F5",
        "WACONEDRIVESTANDARD_GOV": "OneDrive_P1_Addon",
    }
    corrected_license = license_dict[wrong_license]
    return corrected_license

def main():
    try:
        script_name = Path(__file__).stem
        logger = setup_logger(script_name)

        logger.info(f"{script_name} executed, getting user information and license information.")
        logger.info(f"Initializing Azure Connector and retirieving Access Token.")

        #Sets up azure connector
        azure_connector_o = Azure_Connector(logger)

        #Gets user info and license info
        azure_user_info_dict = azure_connector_o.get_users_info()

        #Fixes the licenses to the M365 Licenses list per Michael's requests.
        for user, value in azure_user_info_dict.items():
            licenses_data_type = 'Collection(Edm.String)'
            licenses = value.get('licenses',[])
            value['manager'] = value.get('manager', {}).get('displayName', 'N/A')
            value['licenses_data_type'] = licenses_data_type
            if licenses:
                value['licenses'] = [fix_license(license) for license in licenses]
            else:
                value['licenses'] = ['No_License'] 
            
            if value['accountEnabled']:
                value['accountEnabled'] = str(value['accountEnabled'])
            else:
                value['accountEnabled'] = "False"

        logger.debug(json.dumps(azure_user_info_dict,indent=4))
        previous_cache_file_path = project_root / 'cache' / 'azure_user_info_cache.json'
        previous_azure_user_info_list = read_from_json(previous_cache_file_path)

        current_data = cache_operation(azure_user_info_dict, previous_azure_user_info_list)
        
        status = current_data[2].get('status')

        if status == 'exit':
            logger.info("No changes deteced in checksum, ending ETL.")
            return
        else:
            logger.info("Changes detected in checksum, checking changes.")

        sharepoint_connector_o = SharePoint_Connector(logger)
        #Formats and batches items for SharePoint upload.
        logger.info("Formatting and batching items for upload.")
        batched_queue = sharepoint_connector_o.format_and_batch_for_upload_sharepoint(current_data[1], 'COT_Employees')
        #Uploads to SharePoint the batched deque
        logger.info(f"Uploading {len(batched_queue)} batches to SharePoint.")

        sharepoint_connector_o.batch_upload(batched_queue)

        #If there was any POST, then we need to get the new sharepoint IDS
        # operations = current_data[3].get('operations')
        # if operations['post'] > 0:
        
        logger.info(f"Updating cache with new info...")
        #Gets item ids from SharePoint
        sharepoint_list_items = sharepoint_connector_o.get_item_ids('COT_Employees')

        sharepoint_items_file_path = project_root / 'outputs' / 'azure_user_info_sharepoint_items.json'
        write_to_json(sharepoint_list_items,sharepoint_items_file_path)
        
        #Updates cache with the new SharePoint IDs
        logger.info("Updating cache with SharePoint info...")
        full_cache = update_cache(current_data, sharepoint_list_items,'Azure_Id')
        write_to_json(full_cache,previous_cache_file_path)
        
        
        logger.info("ETL Completed successfully: Exit Code 0")
        return 0
    except Exception as e:
        logger.error(f"Error occured in main: {e}: Exit Code 1")
        logger.error("Traceback:", exc_info=True)
        return 1


exit_code = main()
if exit_code == 0:
    pass
elif exit_code == 1:
    pass

