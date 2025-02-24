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

servicedesk_cache_file_path = r"cache/servicedesk_asset_cache.json"

def read_servicedesk_cache()-> tuple:
    """
    Reads servicedesk_asset_cache.json, returns a tuple of the items:

    Returns:
        checksum (dict)
        items (dict)
        date (dict)
    """
    servicedesk_cache = read_from_json(servicedesk_cache_file_path)
    checksum = servicedesk_cache[0]
    items = servicedesk_cache[1]
    dates = servicedesk_cache[4]

    return checksum,items,dates, servicedesk_cache

def clean_servicedesk_asset_details(raw_dict: dict) -> dict:
    """
    Takes in a raw data dict from service desk, extracts only the fields required and saves in a new dict before returning.

    Args:
        raw_dict (dict): Dict that contains the entire information pulled from Service Desk.
    Returns:
        cleaned_dict (dict): Dict that contains only the fields we are looking for from ServiceDesk.
    """
    cleaned_dict = {
        key: {
            "name": value.get("name"),
            "type": value.get("type", {}).get("name"),
            "state": value.get("state", {}).get("name"),
            "department": (department.get("name") if (department := value.get("department")) else None),
            "asset_description": value.get("product", {}).get("name"),
            "asset_assigned_user": (user.get('name') if (user := value.get('user')) else None),
            "asset_assigned_user_dept": ((user.get('department') or {}).get('name') if (user := value.get('user')) else None),
            "asset_assigned_user_email": (user.get('email_id') if (user := value.get('user')) else None),
            "saas_id": value.get("id"),
            "created_date": value.get("created_time", {}).get("display_value"),
            "last_updated_date": (last_updated.get('display_value') if (last_updated := value.get('last_updated_time')) else None),
            "total_cost": value.get("total_cost"),
            "lifecycle": (lifecycle.get('name') if (lifecycle := value.get('lifecycle')) else None),
            "barcode": value.get("barcode"),
            "depreciation_salvage_value": (product_depreciation.get("salvage_value") if (product_depreciation := value.get("product_depreciation")) else None),
            "depreciation_useful_life": (product_depreciation.get("useful_life") if (product_depreciation := value.get("product_depreciation")) else None),
            "ip_address": (network_adapters[0].get('ip_address') if (network_adapters := value.get('network_adapters', [{'ip_address: ""'}])) else None),
            "replaced_serial_number": value.get("udf_fields", {}).get("udf_char7"),
            "service_request": value.get("udf_fields", {}).get("udf_char8"),
            "imei_number": value.get("udf_fields", {}).get("udf_char9"),
            "cellular_provider": value.get("udf_fields", {}).get("udf_char14"),
            "replacement_fund": value.get("udf_fields", {}).get("udf_char4"),
            "replacement_date": (udf_date.get('display_value') if (udf_date := value.get('udf_fields',{}).get('udf_date1')) else None),
            "annual_replacement_amt": value.get("udf_fields", {}).get("udf_double1"),
            "acquisition_date": (acquistion_date.get("display_value") if (acquistion_date := value.get("acquisition_date")) else None),
            "asset_vendor_name": (vendor.get('name') if (vendor := value.get('vendor')) else None),
            "asset_purchase_cost": value.get("purchase_cost"),
            "asset_product_type": value.get("product", {}).get("product_type", {}).get("display_name"),
            "asset_category": value.get("product", {}).get("category",{}).get('name'),
            "asset_manu": value.get("product", {}).get("manufacturer"),
            "asset_serial_no": value.get("serial_number"),
            "warranty_expiry_date": (expiry.get('display_value') if (expiry := value.get('warranty_expiry')) else None),
        }
        for key,value in raw_dict.items()
    }
    
    return cleaned_dict

def main():
    """
    When called, will do the following:
    1. Will check the last time it was updated via the cache
    2. Get items from service desk, filtered to items since last updated
    3. Generate a current cache of items and compare to the previous cache
    4. Format and upload to service desk
    """
    try:
        script_name = Path(__file__).stem
        logger = setup_logger(script_name)

        logger.info(f"Main: Current working directory: {os.getcwd()}")
        logger.info(f"Main: {script_name} executed, retrieving Service Desk Assets...")

        logger.info(f"Main: Retrieving cached items...")
        checksum, items, dates, previous_servicedesk_cache_list = read_servicedesk_cache()
        logger.info(f"Main: Retrieved cached items:\nChecksum: {checksum}\nDates:{dates}")
        last_updated_iso = dates['current_time_epoch_ms']

        logger.info(f"Main: Retrieving assets from ServiceDesk.")
        servicedesk_connector_o = ServiceDesk_Connector(logger)
        current_asset_list_dict = servicedesk_connector_o.get_assets_from_servicedesk(last_updated=last_updated_iso)

        if len(current_asset_list_dict) == 0:
            logger.info(f"Main: No assets returned, exiting.")
            return

        raw_asset_details_dict = {
            value.get('asset_id') : servicedesk_connector_o.get_assets_from_servicedesk(asset_id = value.get('asset_id'))
            for value in current_asset_list_dict.values()
        }
        logger.info(f"Main: Retrieved {len(raw_asset_details_dict)} items.")
        logger.info(f"Main: {json.dumps(raw_asset_details_dict,indent=4)}")
        cleaned_asset_details_dict = clean_servicedesk_asset_details(raw_asset_details_dict)
        logger.info(f"Main: Cleaned asset details:\n{json.dumps(cleaned_asset_details_dict,indent=4)}")

        #Items are in sharpoint format, now cache and comapre checksums
        current_data = cache_operation(cleaned_asset_details_dict,previous_servicedesk_cache_list)
        status = current_data[2].get('status')
        if status == 'exit':
            logger.info("Main: No changes detected, returning.")
            return
        else:
            logger.info("Main: Changes detected in checksum, checking changes.")

        #Send to sharepoint to upload
        sharepoint_connector_o = SharePoint_Connector(logger)
        
        logger.info("Main: Formatting and batching for upload.")
        batched_queue=sharepoint_connector_o.format_and_batch_for_upload_sharepoint(current_data[1],"ServiceDesk_Assets")
        logger.info(f"Main: Formatted and batched {len(batched_queue)} items for SharePoint")
        logger.info(f"Main: Uploading {len(batched_queue)} batches to SharePoint.")

        sharepoint_connector_o.batch_upload(batched_queue)
        logger.info("Main: Uploaded items to SharePoint.")

        #Get current time in iso and string, upload to cache along with new items.
        # operations = current_data[3].get('operations')
        # if operations['post'] > 0:
        logger.info("Main: Post detected, getting SharePoint ids and updating cache.")

        sharepoint_list_items = sharepoint_connector_o.get_item_ids('ServiceDesk_Assets')

        logger.info(f"Main: {len(sharepoint_list_items)} items retreieved, saving cache.")
        
        sharpoint_items_file_path = 'outputs/service_desk_assets.json'
        write_to_json(sharepoint_list_items,sharpoint_items_file_path)
        logger.info(f"Main: Items saved to {sharpoint_items_file_path}")

        logger.info("Main: Updating Cache with SharePoint info")
        full_cache = update_cache(current_data,sharepoint_list_items,'name')
        new_dates = {
            'current_time_epoch_ms' : int(time.time()*1000),
            'date_last_checked_str' : datetime.now().strftime(("%m/%d/%Y %H:%M"))
        }
        full_cache.append(new_dates)
        write_to_json(full_cache,servicedesk_cache_file_path)
    
        logger.info(f"Main: ETL Completed successfully: Exit Code 0")
    except Exception as e:
        logger.error(f"Main: Error occured in main: {e}: Exit Code 1")
        logger.error("Main: Traceback:", exc_info=True)
        return 1

main()