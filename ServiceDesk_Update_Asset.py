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

        logger.info(f"Current working directory: {os.getcwd()}")
        logger.info(f"{script_name} executed, doing something...")

        #Connects to SharePoint, gets the SharePoint list, trim it of the unrequired keys/values
        sharepoint_connector_o = SharePoint_Connector(logger)
        
        # sharepoint_items_dict = sharepoint_connector_o.get_item_ids('Asset_Pickup_History')
        # write_to_json(sharepoint_items_dict,"inputs/raw_test_data.json")
        #Cache it and check to previous cache   
        #Format for for service desk
        #Upload to service desk








    except Exception as e:
        logger.error(f"Error occured in main: {e}: Exit Code 1")
        logger.error("Traceback:", exc_info=True)
        return 1
    

main()