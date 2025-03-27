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
        module_id = ticket.get('module_id')
        is_incident = ticket.get('is_incident')

        time_spent_ms = int(ticket.get('time_spent_ms',0))
        time_spent_minutes = time_spent_ms / 1000 / 60
        time_spent_hours = time_spent_minutes / 60

        start_time = ticket.get('start_time')
        if start_time:
            date_obj = datetime.strptime(start_time, "%b %d, %Y %I:%M %p")
            formatted_start_time = date_obj.strftime("%m/%d/%Y")
        else:
            formatted_start_time = "N/A"
            
        
        module_url = f"https://servicedesk.torranceca.gov/app/itdesk/ui/{module_key}/{module_id}/details" if module_key != "changes" else f"https://servicedesk.torranceca.gov/app/itdesk/ChangeDetails.cc?CHANGEID={module_id}&selectTab=close&subTab=details"
        local_module_key = "incident" if is_incident else module_key

        ticket['minutes'] = round(time_spent_minutes,2)
        ticket['hours'] = round(time_spent_hours,2)
        ticket['formatted_start_time'] = formatted_start_time
        ticket['module'] = local_module_key
        ticket['module_id'] = module_url

    return worklogs_dict

def combine_data(tickets_dict:dict, module_key:str)-> dict:
    """
    Iterates over a worklog_dict, looking at each ticket and the worklogs in the ticket.
    Creates a new ticket using the module_id+tech_email to create a unique_id for the ticket. This allows tracking  worklogs per tech per ticket, minimizing worklogs.
    Combines time_spent per tech per ticket in the new ticket, updates latest_start_time to use the most current start_time and saves worklog_ids in a list.
    
    Args:
        tickets_dict (dict): Dict that contains each individual ticket dict that contains all worklogs
        module_key (str): Str stating the current module being processed.
    Returns:
        consolidated_tickets (dict): Dict with all values cleaned.
    """
    consolidated_tickets = {}
    for ticket_value in tickets_dict.values():
        module_id = ticket_value.get('module_id')
        worklog_dicts = ticket_value.get('worklog_details',{})
        is_incident = ticket_value.get('is_incident')
        for worklog in worklog_dicts.values():
            tech_name = worklog.get('tech_name',None)
            if tech_name:
                split_tech_name = tech_name.split(',')
                tech_fname = split_tech_name[1].strip()
                tech_lname = split_tech_name[0].strip()
            else:
                #All worklogs have a tech_name, only projects might be missing a tech_name, if missing, cannot make a unique_id and thus we skip it.
                continue

            tech_email = worklog.get('tech_email')
            time_spent_ms = int(worklog.get('time_spent_ms',0) or 0)
            start_time = worklog.get('start_time')
            date_obj = datetime.strptime(start_time, "%b %d, %Y %I:%M %p")
            month_start_time = date_obj.strftime("%b").upper()
            worklog_id = worklog.get('worklog_id') or worklog.get('task_id')

            if module_key == 'projects':
                unique_ticket_id = f'{module_id}_{month_start_time}_{tech_lname}_{tech_fname}'
            else:
                unique_ticket_id = f'{module_id}_{tech_lname}_{tech_fname}'

            if unique_ticket_id not in consolidated_tickets:
                consolidated_tickets[unique_ticket_id] = {
                    "Unique_ID": unique_ticket_id,
                    "module_id": module_id,
                    "tech_name": f'{tech_lname}, {tech_fname}',
                    "tech_email": tech_email or 'N/A',
                    "time_spent_ms": time_spent_ms,
                    "created_time": start_time,
                    "worklog_ids": [worklog_id],
                    "is_incident" : is_incident
                }
            else:
                consolidated_tickets[unique_ticket_id]['time_spent_ms'] += time_spent_ms
                existing_start_time = datetime.strptime(
                    consolidated_tickets[unique_ticket_id]['created_time'], "%b %d, %Y %I:%M %p"
                )
                new_start_time = datetime.strptime(start_time, "%b %d, %Y %I:%M %p")
                if new_start_time > existing_start_time:
                    consolidated_tickets[unique_ticket_id]['created_time'] = start_time

                consolidated_tickets[unique_ticket_id]['worklog_ids'].append(worklog_id)

    for tickets in consolidated_tickets.values():
        worklog_ids_list = tickets.get('worklog_ids',[])
        worklog_ids_str = str(worklog_ids_list)
        tickets['worklog_id'] = worklog_ids_str

    return consolidated_tickets

def trim_keys(worklogs_dict:dict)-> dict:
    """
    Takes in a worlogs dict and removes keys not required.
    Args:
        worklogs_dict (dict): Dict containg worklogs.
    Return:
        cleaned_worklogs_dict (doct): Dict containg worklogs with only keys required.
    """

    required_keys = ['Unique_ID', 'module', 'created_time', 'hours', 'minutes', 'module_id', 'tech_email', 'tech_name', 'worklog_id']
    cleaned_worklogs_dict = {}
    for outerkey, worklog in worklogs_dict.items():
        cleaned_worklogs_dict[outerkey] = {}
        for innerkey, value in worklog.items():
            if innerkey in required_keys:
                cleaned_worklogs_dict[outerkey][innerkey] = value
    
    return cleaned_worklogs_dict

def main():
    try:
        script_name = Path(__file__).stem
        logger = setup_logger(script_name)

        logger.info(f"Main: Current working directory: {os.getcwd()}")
        logger.info(f"Main: {script_name} executed, retrieving Service Desk Worklogs...")

        servicedesk_cache_file_path = r"cache/servicedesk_worklogs_cache.json"

        checksum_dict, items_dict, dates_dict, current_servicedesk_cache_list = read_servicedesk_cache(servicedesk_cache_file_path)

        iteration_type = determine_iteration_type(dates_dict)
        if iteration_type == 'full_sync':
            num_pages = 1000
        else:
            num_pages = 1
        servicedesk_connector_o = ServiceDesk_Connector(logger)
        
        module_list = ['requests','projects','problems','changes','releases']
        worklogs_dict = {module_name : {} for module_name in module_list}

        for module_key in worklogs_dict:
            worklogs_dict[module_key] = servicedesk_connector_o.get_worklogs_from_servicedesk(module_key,num_pages)
            logger.info(f"Main: Completed retrieving worklogs for {module_key}")
        # logger.info(f"Main: Raw Dict Values: {json.dumps(worklogs_dict,indent=4)}")

        final_worklogs_dict = {}
        for module_key, module_values in worklogs_dict.items():
            combined_data = combine_data(module_values,module_key)
            # logger.info(f"Combined data: {json.dumps(combined_data,indent=4)}")
            logger.info(f"Main: Completed combining data for {module_key}")
            cleaned_data = clean_and_format_data(combined_data, module_key)
            # logger.info(f"Cleaned data: {json.dumps(cleaned_data,indent=4)}")
            cleaned_data = trim_keys(cleaned_data)
            logger.info(f"Main: Completed cleaning data for {module_key}")
            final_worklogs_dict.update(cleaned_data)
        
        
        sharepoint_connector_o = SharePoint_Connector(logger)
        logger.info("Main: Getting SharePoint ids and updating cache.")
        sharepoint_cache = sharepoint_connector_o.get_item_ids('ServiceDesk_Worklogs')
        for item in sharepoint_cache.values():
            item['Unique_ID'] = item.pop('unique_id')
        logger.info(f"Main: {len(sharepoint_cache)} items retreieved, saving cache.")
        final_worklogs_dict, sharepoint_cache = reformat_dict(sharepoint_cache, final_worklogs_dict, 'Unique_ID')
        current_servicedesk_cache_list[1] = sharepoint_cache

        # logger.info(f"Main: Cleaned, Combined, and Current Dict Values: {json.dumps(final_worklogs_dict,indent=4)}")
        # logger.info(f"Main: Cleaned, Combined, and SharePoint Dict Values: {json.dumps(sharepoint_cache,indent=4)}")
        cached_info = cache_operation(final_worklogs_dict, current_servicedesk_cache_list, logger=logger)

        # logger.info(f"Main: Cleaned, Combined, and Cached Dict Values: {json.dumps(cached_info,indent=4)}")

        if cached_info[2].get('status') == 'exit':
            logger.info(f"No changes detected, exiting.")
            return
        
        
        batch_queue = deque()

        batch_queue = sharepoint_connector_o.format_and_batch_for_upload_sharepoint(cached_info[1],'ServiceDesk_Worklogs')
        sharepoint_connector_o.batch_upload(batch_queue)


        logger.info("Main: Updating Cache with SharePoint info")
        current_iteration = int(dates_dict.get('iteration',0))
        current_iteration += 1
        new_dates = {
            'current_time_epoch_ms' : int(time.time()*1000),
            'date_last_checked_str' : datetime.now().strftime(("%m/%d/%Y %H:%M")),
            'iteration': current_iteration,
            'full_iteration_num': 15
        }
        cached_info.append(new_dates)
        write_to_json(cached_info,servicedesk_cache_file_path)
    
        logger.info(f"Main: ETL Completed successfully: Exit Code 0")
        
    except Exception as e:
        logger.error(f"Main: Error occured in main: {e}: Exit Code 1")
        logger.error("Main: Traceback:", exc_info=True)
        return 1

main()