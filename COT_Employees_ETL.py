import os, sys, traceback
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
project_root = os.path.dirname(os.path.abspath(__file__))
os.chdir(project_root)
from utils.Logger import *
from scripts.Azure_Connector import *
from scripts.SharePoint_Connector import *
from collections import deque
from utils.Utils import *

def clean_items(azure_dict:dict) -> dict:
    """
    Takes in an azure dict, cleans and renames keys, returns a dict in sharepoint format.

    Args:
        azure_dict (dict): Dict of items pulled from azure.
    Returns:
        cleaned_azure_dict (dict): Dict with keys renamed and other data removed.
    """
    cleaned_azure_dict = {}
    for values in azure_dict.values():
        azure_id = values.get('id','N/A') or 'N/A'
        department = values.get('department','N/A') or 'N/A' 
        employee_id = values.get('employeeId','N/A') or 'N/A'
        job_title = values.get('jobTitle','N/A') or 'N/A'
        active = values.get('accountEnabled','N/A') or 'N/A'
        display_name = values.get('displayName','N/A') or 'N/A'
        licenses = values.get('licenses')
        email = values.get('mail','N/A') or 'N/A'
        manager = values.get('manager','N/A') or 'N/A'

        cleaned_azure_dict[azure_id] = {
            'Department': department,
            'Employee_Id': employee_id,
            'Job_Title' : job_title,
            'Active': active,
            'Azure_Id': azure_id,
            'Manager': manager,
            'Display_Name' : display_name,
            'Licenses': licenses,
            'Email': email
        }
    
    return cleaned_azure_dict

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
        cache_file_path = r'cache\azure_user_info_cache.json'
        script_name = Path(__file__).stem
        logger = setup_logger(script_name)

        logger.info(f"{script_name} executed, getting user information and license information.")
        logger.info(f"Initializing Azure Connector and retirieving Access Token.")

        sharepoint_connector_o = SharePoint_Connector(logger)
        cached_sharepoint_items = sharepoint_connector_o.get_item_ids('COT_Employees')
        cached_sharepoint_items = trim_sharepoint_keys(cached_sharepoint_items)
        cached_sharepoint_items = reassign_key(cached_sharepoint_items,'Azure_Id')
        logger.info(f"Cached sharepoint items: {json.dumps(cached_sharepoint_items,indent=4)}")

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

        azure_user_info_dict = clean_items(azure_user_info_dict)
        azure_user_info_dict = merge_sharepoint_ids(azure_user_info_dict, cached_sharepoint_items)

        previous_azure_user_info_list = read_from_json(cache_file_path)
        previous_azure_user_info_list[1] = cached_sharepoint_items
        current_data = cache_operation(azure_user_info_dict, previous_azure_user_info_list, delete=True, logger=logger)
        
        status = current_data[2].get('status')
    
        if status == 'exit':
            logger.info("No changes deteced in checksum, ending ETL.")
            return
        else:
            logger.info("Changes detected in checksum, checking changes.")

        for value in current_data[1].values():
            licenses_data_type = 'Collection(Edm.String)'
            value['licenses_data_type'] = licenses_data_type
        
        logger.info(f"Cached Info: {json.dumps(current_data,indent=4)}")

        sharepoint_connector_o = SharePoint_Connector(logger)
        #Formats and batches items for SharePoint upload.
        logger.info("Formatting and batching items for upload.")
        batched_queue = sharepoint_connector_o.format_and_batch_for_upload_sharepoint(current_data[1], 'COT_Employees')
        #Uploads to SharePoint the batched deque
        logger.info(f"Uploading {len(batched_queue)} batches to SharePoint.")

        sharepoint_connector_o.batch_upload(batched_queue)

        logger.info(f"Updating cache with new info...")
        
        write_to_json(current_data, cache_file_path)
        
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

