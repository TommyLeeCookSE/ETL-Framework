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

def clean_and_format_data(worklogs_dict:dict, module_key:str) -> dict:
    """
    Takes in a single module_dict and module_key and convert ms to minutes, convert ms to hours, convert the start_time to mm/dd/yyyy format

    Args:
        worlogs_dict (dict): Dict that contains each individual ticket dict that contains all worklogs
        module_key (str): Used to determine which module currently in.
    Returns:
        cleaned_data_dict (dict): Dict with all values cleaned.

    """
    #for every module in module_dict.values()
    for ticket in worklogs_dict.values():
        worklog_details = ticket.get('worklog_details')
        module_id = ticket.get('module_id')
        is_incident = ticket.get('is_incident')
        for worklog in worklog_details.values():

            time_spent_ms = int(worklog.get('time_spent_ms',0))
            time_spent_minutes = time_spent_ms / 1000 / 60
            time_spent_hours = time_spent_minutes / 60

            start_time = worklog.get('start_time')
            if start_time:
                date_obj = datetime.strptime(start_time, "%b %d, %Y %I:%M %p")
                formatted_start_time = date_obj.strftime("%m/%d/%Y")
            else:
                formatted_start_time = "N/A"
            
            
            module_url = f"https://servicedesk.torranceca.gov/app/itdesk/ui/{module_key}/{module_id}/details" if module_key != "changes" else f"https://servicedesk.torranceca.gov/app/itdesk/ChangeDetails.cc?CHANGEID={module_id}&selectTab=close&subTab=details"
            module_key = "incident" if is_incident == True else module_key

            worklog['time_spent_minutes'] = time_spent_minutes
            worklog['time_spent_hours'] = time_spent_hours
            worklog['formatted_start_time'] = formatted_start_time
            worklog['module'] = module_key
            worklog['module_url'] = module_url

    return worklogs_dict

def combine_data(worklogs_dict:dict)-> dict:
    """"
    """

def main():
    try:
        script_name = Path(__file__).stem
        logger = setup_logger(script_name)

        logger.info(f"Main: Current working directory: {os.getcwd()}")
        logger.info(f"Main: {script_name} executed, retrieving Service Desk Worklogs...")

        servicedesk_cache_file_path = r"cache/servicedesk_worklogs_cache.json"

        checksum_dict, items_dict, dates_dict, current_servicedesk_cache_list = read_servicedesk_cache(servicedesk_cache_file_path)

        iteration_type = determine_iteration_type(dates_dict)

        servicedesk_connector_o = ServiceDesk_Connector(logger)
        
        module_list = ['requests','projects','problems','changes','releases']
        # module_list = ['requests','problems','releases','changes']
        worklogs_dict = {module_name : {} for module_name in module_list}

        for module_key, value in worklogs_dict.items():
            worklogs_dict[module_key] = servicedesk_connector_o.get_worklogs_from_servicedesk(module_key,1)
            logger.info(f"Main: Completed retrieving worklogs for {module_key}")
        logger.info(f"Main: {json.dumps(worklogs_dict,indent=4)}")

        for module_key, module_values in worklogs_dict.items():
            worklogs_dict[module_key] = clean_and_format_data(module_values, module_key)
            logger.info(f"Main: Completed cleaning data for {module_key}")
        logger.info("Main: Completed cleaning all modules.")
        logger.debug(json.dumps(worklogs_dict,indent=4))

        """
        TODO: For each ticket need to start combining them,cache, and then upload
        Combine tickets:
            To combine tickets, look at each worklog for each ticket, create a new worklog that is module_id_tech_email (unique id), then add the details you need but sum the time and concat the worklog ids. For date, use the latest one.
            When done, each ticket will have a worklog for each tech that is the summed total.
        Cache
        Upload
        
        """

    except Exception as e:
        logger.error(f"Main: Error occured in main: {e}: Exit Code 1")
        logger.error("Main: Traceback:", exc_info=True)
        return 1

main()