import os, sys, traceback
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
project_root = os.path.dirname(os.path.abspath(__file__))
os.chdir(project_root)
from utils.Logger import *
from scripts.Azure_Connector import *
from scripts.SharePoint_Connector import *
from collections import deque
from utils.Utils import *


def main():
    try:
        cache_file_path = r'cache\azure_arc_server_cache.json'
        script_name = Path(__file__).stem
        logger = setup_logger(script_name)

        logger.info(f"{script_name} executed, getting Azure Arc Server information.")
        logger.info(f"Initializing Azure Connector and retreiving Access Token.")
        azure_connector_o = Azure_Connector(logger)
        azure_arc_info = azure_connector_o.get_azure_arc_info()
        for item in azure_arc_info:
            logger.info(json.dumps(item,indent=4))
        #GET AZURE SERVER INFO


        # logger.info(f"Initializing SharePoint Connector and retreiving Access Token.")
        # sharepoint_connector_o = SharePoint_Connector(logger)
        # logger.info(f"Retrieving cached information.")
        # cached_info = sharepoint_connector_o.get_item_ids('INFAzureLicenseUsage')#RENAME TO ARC SERVER LIST INFO
        # logger.info(f"SharePoint values: {json.dumps(cached_info,indent=4)}")

        # formatted_licenses_dict, formatted_cached_dict = reformat_dict(cached_info, licenses_dict, 'sku_id') #CHANGE LICENSES DICT
        # logger.info(json.dumps(formatted_licenses_dict,indent=4))
        # logger.info(json.dumps(formatted_cached_dict,indent=4))

        # cached_list = read_from_json(cache_file_path)
        # cached_list[1] = formatted_cached_dict
        # cache_response = cache_operation(formatted_licenses_dict, cached_list, logger=logger)

        # logger.info(json.dumps(cache_response,indent=4))

        # status = cache_response[2].get('status')
    
        # if status == 'exit':
        #     logger.info("No changes deteced in checksum, ending ETL.")
        #     return
        # else:
        #     logger.info("Changes detected in checksum, checking changes.")

        # formatted_deque = sharepoint_connector_o.format_and_batch_for_upload_sharepoint(cache_response[1], 'INFAzureLicenseUsage')
        # logger.info(f"Main: Formatted and batched {len(formatted_deque)} items for SharePoint")
        # logger.info(f"Main: Uploading {len(formatted_deque)} batches to SharePoint.")

        # sharepoint_connector_o.batch_upload(formatted_deque)
        # logger.info("Main: Uploaded items to SharePoint.")

        # write_to_json(cache_response, cache_file_path)

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

