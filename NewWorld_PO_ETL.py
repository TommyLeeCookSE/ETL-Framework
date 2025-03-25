import os, sys, json, decimal, pyodbc
from datetime import datetime, date
from collections import deque

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
project_root = os.path.dirname(os.path.abspath(__file__))
os.chdir(project_root)

from utils.Logger import *
from scripts.SharePoint_Connector import *
from utils.Utils import *

def extract_po_information(logger)-> list:
        """
        Executes a stored procedure to retrieve PO Alert information from SQL Server nwdb01dvt in databse LOGOSDB.

        Returns:
            po_info (list): A list containing the raw unformatted data from SQL.
        """
        conn = pyodbc.connect(
            "Driver={ODBC Driver 17 for SQL Server};"
            "Server=nwdb01dvt;"
            "Database=LOGOSDB;"
            "Trusted_Connection=yes;"
            "Timeout=120;"
        )
        po_info = []
        cursor = conn.cursor()
        stored_procedure_name = "dbo.COTSP_PO_ALERT_WITH_CONTINGENCY_INFO_TOMMY"

        try:
            logger.info(f"Extracting informating from: {stored_procedure_name}")
            cursor.execute(f"EXEC {stored_procedure_name}")
            result_set_counter = 0
            while True:
                rows = cursor.fetchall()
                if rows:
                    result_set_counter += 1
                    po_info.extend(rows)
                    logger.info(f"Total items in list: {len(po_info)}")
                else:
                    if cursor.nextset():
                        logger.info("More info to extract...")
                        continue 
                    else:
                        break
            if result_set_counter == 0:
                print("No result sets returned.")
        except pyodbc.Error as e:
            print("Error executing stored procedure:", e)

        logger.info("Done extracting data.")
        cursor.close()
        conn.close()

        return po_info

def clean_values(raw_list: list, logger: object) -> list:
    """
    Takes in the raw SQL data and cleans it for SharePoint.
    Converts decimals to floats.
    Converts datetime objects to strings.

    Args:
        raw_list (list): A raw list of items from SQL.
    
    Returns:
        formatted_list (list): Returns a list with cleaned data of decimals and datetime objects.
    """
    logger.info(f"Formatting {len(raw_list)} items..")

    formatted_list = []
    for row in raw_list:
        formatted_row = []
        for item in row:
            if isinstance(item, decimal.Decimal):
                # logger.info(f"{item} is in decimal format, reformatting as float.")
                item = float(item)
            elif isinstance(item, (datetime, date)):
                # logger.info(f"{item} is in datetime format, reformatting as string.")
                item = str(item)
            formatted_row.append(item)
        formatted_list.append(formatted_row)
    
    logger.info(f"Formatted {len(formatted_list)} items.")

    return formatted_list

def save_po_information(po_list: list, logger:object) -> dict:
    """
    Takes in list and writes it to a json.

    Args:
        po_list (list): List of bronze SQL data.
    """
    
    fieldnames = [
        'Title','PO_Type','Description','PO_Number','Vendor_Name', 'VENDOR_ID', 'VENDOR_NUMBER', 'Expiration_Date', 'RESOLUTION_NUMBER', 'PO_Amount','Expense','Purchase_Request_Id','Contingency_Description', 'Contingency_Amount', 'Contingency_Used',
        'Contingency_Balance', 'Last_Month_Update_Date','Last_Month_Update','Last_Month_Updated_By','This_Month_Update_Date','This_Month_Update','This_Month_Updated_By']
    
    logger.info("Converting csv to json")
    po_dict_list = [dict(zip(fieldnames,row)) for row in po_list]
    
    logger.info(f"Returning a dict with {len(po_dict_list)} items")
    return po_dict_list

def transform_data(po_dict: dict, logger: object) -> dict:
    """
    Takes in a dict containg all the PO items that need cleaning.
    Calculates the balance, checks and sets the expiry, fixes department names, fixes empty dates, and generates unique_ids.
    
    Args:
        po_dict (dict): Dict contains semi-cleaned items.
    
    Returns:
        po_dict (dict): Dict ready to batch and upload to SharePoint.
    """
    logger.info("Transforming data...")
    for po_dict_item in po_dict:
        calculate_balance(po_dict_item)
        check_expiry(po_dict_item)
        fix_department_names(po_dict_item)
        fix_empty_dates(po_dict_item)
        unique_key_generator(po_dict_item)
    
    logger.info("Done transforming data.")
    
    return po_dict

def calculate_balance(po_dict:dict) -> None:
    """
    Takes in a po_dict, calculates the balance and updates the less_than booleans.

    Args:
        po_dict (dict): Dict containg data for a PO item.
    """
    po_dict['Balance'] = round(po_dict['PO_Amount'] - po_dict['Expense'], 2)
    balance_remaining_percent = po_dict['Balance'] / po_dict['PO_Amount']
    po_dict['Less_Than_25_Remaining'] = 'X' if balance_remaining_percent <= 0.25 else 'N/A'

def check_expiry(po_dict:dict)-> None:
    """
    Check the Expiration Date and then updates the field with an X to signify if expired or when it will expire
    
    Args:
        po_dict (dict): Dict containg data for a PO item.
    """
    today = datetime.today().date()
    expiration_str = po_dict.get('Expiration_Date', "2029-01-01 00:00:00") or "2029-01-01 00:00:00"
    expiration_date = datetime.strptime(expiration_str, "%Y-%m-%d %H:%M:%S").date()
    po_dict['Expiration_Date'] = str(expiration_date)

    days_till_expired = (expiration_date - today).days
    statuses = {
        'Expired': expiration_date < today,
        'Less_Than_30_Days_Till_Expired': 0 <= days_till_expired <= 30,
        'Less_Than_60_Days_Till_Expired': 31 <= days_till_expired <= 60,
        'Less_Than_90_Days_Till_Expired': 61 <= days_till_expired <= 90
    }
    for key, condition in statuses.items():
        po_dict[key] = 'X' if condition else 'N/A'
        
def fix_department_names(po_dict:dict) -> None:
    """
    Updates the department names to match what is used in Azure.

    Args:
        po_dict (dict): Dict containg data for a PO item.
    """
    department_names_dict = {
        "Attorney": "City Attorney",
        "City Mgr": "City Manager",
        "CommunDvl": "Community Development",
        "CommunServ": "Community Service",
        "FINANCE": "Finance",
        "GenService": "General Services",
        "HR" : "Human Resources",
        "Legal Contract" : "City Attorney",
        "Police": "Torrance PD",
        "PW": "Public Works",
    }
    department = po_dict['Title']
    if department in department_names_dict:
        po_dict['Title'] = department_names_dict[department]

def fix_empty_dates(po_dict:dict) -> None:
    """
    Sets dates that are by default, 1900-01-01 to empty so it doesn't interfere in the PowerBI report.
    Args:
        po_dict (dict): Dict containg data for a PO item.
    """

    if po_dict['Last_Month_Update_Date'] == "1900-01-01":
        po_dict['Last_Month_Update_Date'] = ""
    if po_dict['This_Month_Update_Date'] == "1900-01-01":
        po_dict['This_Month_Update_Date'] = ""

def unique_key_generator(po_dict:dict) -> None:
    """
    Generates an unique_key for each item based off the PO number and the Purchase_Request_Id
    Args:
        po_dict (dict): Dict containg data for a PO item.
    """
    po_dict['Unique_ID'] = str(str(po_dict['PO_Number']) + str(po_dict['Purchase_Request_Id']))

def convert_to_dict(po_list:list) -> dict:
    """
    Takes in a list and converts it to a dict.
    Args:
        po_list (list): List of po_info
    Returns:
        po_dict (dict): Dict of po_info
    """
    po_dict = {}
    for item in po_list:
            key = item.get('Unique_ID')
            po_dict[key] = item

    return po_dict

def remove_blanks(po_dict:dict)->dict:
    """
    Takes in a dict and iterates over each dict inside, changes blankes to N/A.
    Args:
        po_dict(dict): Dict of dicts containing PO info.
    Returns:
        po-dict(dict): Dict of dicts containging cleaned PO Info.
    """

    for outerkey, inner_dict in po_dict.items():
        for inner_key, value in inner_dict.items():
            if value in [None, ' ', '']:
                if 'Date' in inner_key:
                    po_dict[outerkey][inner_key] = "1900-01-01"
                else:
                    po_dict[outerkey][inner_key] = 'N/A'
                
    
    return po_dict
def main():
    try:
        cache_file_path = r"cache/po_info_cache.json"
        script_name = Path(__file__).stem
        logger = setup_logger(script_name)

        logger.info(f"Current working directory: {os.getcwd()}")
        logger.info(f"{script_name} executed, getting SQL Data.")

        raw_sql = extract_po_information(logger)
        raw_po_info_list = clean_values(raw_sql,logger)
        formatted_po_info_list = save_po_information(raw_po_info_list, logger)
        formatted_po_info_list = transform_data(formatted_po_info_list, logger)
        formatted_po_info = convert_to_dict(formatted_po_info_list)
        formatted_po_info = remove_blanks(formatted_po_info)
        logger.info(json.dumps(formatted_po_info,indent=4))
        sharepoint_connector_o = SharePoint_Connector(logger)
        cached_sharepoint_items = sharepoint_connector_o.get_item_ids('NewWorld_PO_Alert')

        formatted_po_info, formatted_sharepoint_dict = reformat_dict(cached_sharepoint_items,formatted_po_info, 'Unique_ID')

        logger.info("Begining caching operations.")
        previous_cache = read_from_json(cache_file_path)
        previous_cache[1] = formatted_sharepoint_dict
        current_data = cache_operation(formatted_po_info, previous_cache, delete=True, logger=logger)
        
        status = current_data[2].get('status')
        if status == 'exit':
            logger.info("No changes detected, returning.")
            return
        else:
            logger.info("Changes detected in checksum, checking changes.")
        
        logger.debug(json.dumps(current_data,indent=4))
        
        logger.info("Formatting and batching for upload.")
        batched_queue=sharepoint_connector_o.format_and_batch_for_upload_sharepoint(current_data[1],"NewWorld_PO_Alert")
        logger.info(f"Formatted and batched {len(batched_queue)} items for SharePoint")
        logger.info(f"Uploading {len(batched_queue)} batches to SharePoint.")

        sharepoint_connector_o.batch_upload(batched_queue)
        logger.info("Uploaded items to SharePoint.")

        logger.info("Updating Cache with SharePoint info")
        write_to_json(current_data,cache_file_path)
        
        logger.info(f"ETL Completed successfully: Exit Code 0")
    except Exception as e:
        logger.error(f"Error occured in main: {e}: Exit Code 1")
        logger.error("Traceback:", exc_info=True)
        return 1

main()