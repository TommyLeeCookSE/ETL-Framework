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

def determine_iteration_type(dates_dict:dict) -> str:
    """
    Takes in a dict containing iteration counts, returns full or partial.

    Args:
        dates_dict (dict): current_time_epoch_ms: int, date_last_checked_str: str, iteration: int, full_iteration_num: int

    Returns:
        iteration_type (str): partial_sync or full_sync
    """
    current_iteration = dates_dict.get('iteration')
    full_iteration = dates_dict.get('full_iteration_num')

    iteration_checker = current_iteration % full_iteration

    return "full_sync" if iteration_checker == 0 else "partial_sync"

def get_module_ids()-> dict:
    """
    Loops through each module getting all module_ids, returns a dict for the module
    """



def main():
    """

    3. Begin looping through module list and begin calling modules->task->worklogs
    4. Save each module to a dict and save the raw data to a dict to go into outputs
    5. When done begin running the cache comparison
    6. Format and upload to sharepoint
    7. Update each dict and save to the cache.
    """
    try:
        script_name = Path(__file__).stem
        logger = setup_logger(script_name)

        logger.info(f"Main: Current working directory: {os.getcwd()}")
        logger.info(f"Main: {script_name} executed, retrieving Service Desk Worklogs...")

        servicedesk_cache_file_path = r"cache/servicedesk_worklogs_cache.json"

        checksum_dict, items_dict, dates_dict, current_servicedesk_cache_list = read_servicedesk_cache(servicedesk_cache_file_path)

        iteration_type = determine_iteration_type(dates_dict)

        servicedesk_connector_o = ServiceDesk_Connector(logger)
        
        # module_list = ['requests','projects','problems','changes','releases']
        module_list = ['requests']
        worklogs_dict = {module_name : {} for module_name in module_list}

        for module_key, value in worklogs_dict.items():
            worklogs_dict[module_key] = servicedesk_connector_o.get_worklogs_from_servicedesk(module_key,1)
            logger.info(f"Main: {json.dumps(worklogs_dict,indent=4)}")
        """
        TODO: Iterate over worklog dicts, begin fill it with module_id -> task_id -> worklog details
        Format will be 
        module_id: {
            task_id: {
                worklog_details: {
                created_date: str,
                minutes: int,
                tech_name: str,
                tech_email, str
                }
            }
            worklog_details: {
                created_date: str,
                minutes: int,
                tech_name: str,
                tech_email, str}
        }
        Iterate over each module, create a new dict for each module_id, investigate each module_id fully before moving onto the next
        Return a completed module ID to be added to the dict

        Call get lists of module, utilize a max_page for partial and no max for fulls,
        Retrieve the list of module and get each item by id
        Then with the id tsaved in the dict, get the worklogs for the task ids and the module_id, when done, return the single dict

        """
        """
        TODO: Rework get_list_of_assets to get_list_of_items and get_asset_by_id to get_items_by_id
        These will be the generic fetch functions, get_assets_from_sd will be specific for assets
        Can make a get_worklogs_from_servicedesk  that will get worklogs from module-task and have it use the generic calls
        
        """


    except Exception as e:
        logger.error(f"Main: Error occured in main: {e}: Exit Code 1")
        logger.error("Main: Traceback:", exc_info=True)
        return 1

main()