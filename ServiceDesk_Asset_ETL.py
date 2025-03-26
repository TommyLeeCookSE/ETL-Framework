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
            "name": value.get("name","N/A"),
            "type": value.get("type", {}).get("name","N/A"),
            "state": value.get("state", {}).get("name","N/A"),
            "department": (department.get("name","N/A") if (department := value.get("department")) else "N/A"),
            "asset_description": value.get("product", {}).get("name","N/A"),
            "asset_assigned_user": (user.get('name',"N/A") if (user := value.get('user')) else "N/A"),
            "asset_assigned_user_dept": ((user.get('department') or {}).get('name',"N/A") if (user := value.get('user')) else "N/A"),
            "asset_assigned_user_email": (user.get('email_id',"N/A") if (user := value.get('user')) else "N/A"),
            "saas_id": value.get("id","N/A"),
            "created_date": value.get("created_time", {}).get("display_value","Jan 01, 1970"),
            "last_updated_date": (last_updated.get('display_value',"Jan 01, 1970") if (last_updated := value.get('last_updated_time')) else "Jan 01, 1970"),
            "total_cost": float(value.get("total_cost",0)),
            "lifecycle": (lifecycle.get('name',"N/A") if (lifecycle := value.get('lifecycle')) else "N/A"),
            "barcode": value.get("barcode","N/A") or "N/A",
            "depreciation_salvage_value": float((product_depreciation.get("salvage_value", 0) if (product_depreciation := value.get("product_depreciation",0)) else 0) or 0),
            "depreciation_useful_life": float((product_depreciation.get("useful_life",0) if (product_depreciation := value.get("product_depreciation",0)) else 0) or 0),
            "ip_address": (network_adapters[0].get('ip_address',"N/A") if (network_adapters := value.get('network_adapters', [{'ip_address': 'N/A'}])) else "N/A"),
            "replaced_serial_number": value.get("udf_fields", {}).get("udf_char7","N/A") or "N/A",
            "service_request": (value.get("udf_fields", {}).get("udf_char8","N/A")) or "N/A",
            "imei_number": value.get("udf_fields", {}).get("udf_char9","N/A") or "N/A",
            "cellular_provider": value.get("udf_fields", {}).get("udf_char14","N/A") or "N/A",
            "replacement_fund": value.get("udf_fields", {}).get("udf_char4","N/A") or "N/A",
            "replacement_date": ((udf_date := value.get('udf_fields', {}).get('udf_date1')) and datetime.strptime(udf_date.get('display_value'), "%b %d, %Y %I:%M %p").strftime("%b %d, %Y")) or "Jan 01, 1970",
            "annual_replacement_amt": float(value.get("udf_fields", {}).get("udf_double1",0) or 0),
            "acquisition_date": (acquistion_date.get("display_value","Jan 01, 1970") if (acquistion_date := value.get("acquisition_date")) else "Jan 01, 1970"),
            "asset_vendor_name": (vendor.get('name',"N/A") if (vendor := value.get('vendor')) else "N/A"),
            "asset_purchase_cost": float(value.get("purchase_cost",0) or 0),
            "asset_product_type": value.get("product", {}).get("product_type", {}).get("display_name","N/A"),
            "asset_category": value.get("product", {}).get("category",{}).get('name',"N/A"),
            "asset_manu": value.get("product", {}).get("manufacturer","N/A") or "N/A",
            "asset_serial_no": value.get("serial_number","N/A") or "N/A",
            "warranty_expiry_date": (expiry.get('display_value', "Jan 01, 1970") if (expiry := value.get('warranty_expiry')) else "Jan 01, 1970"),
            'repl_fund': 'Yes' if value.get("udf_fields", {}).get("txt_repl_fund","No") else "No",
            "Unique_ID": value.get('name',"N/A")
        }
        for key,value in raw_dict.items()
    }
                
    return cleaned_dict

def check_asset_status(previous_dict:dict, current_dict:dict) -> dict:
    """
    Takes in previous cache and current cache. Checks to see if the status has changed to In Use or Dispose. If so, mark In_Use Date or Disposed_Date and return the dict.

    Args:
        previous_dict (dict): Dict that holds the old cache of items.

        current_dict (dict): Dict that holds the current cache of items.
    Returns:
        updated_current_dict (dict): Dict with the updated statuses.
    """

    for key,asset in current_dict.items():
        current_status = asset.get('state','N/A') or 'N/A'
        if key in previous_dict:
            previous_status = previous_dict[key].get('state','N/A') or 'N/A'
        else:
            previous_status = 'N/A'
        previous_in_use_date = previous_dict.get('in_use_date', "Jan 01, 1970") or "Jan 01, 1970"
        previous_disposed_date = previous_dict.get('disposed_date', "Jan 01, 1970") or "Jan 01, 1970"
        if previous_in_use_date == 'N/A':
            previous_in_use_date = "Jan 01, 1970"
        if previous_disposed_date == 'N/A':
            previous_disposed_date = "Jan 01, 1970"
        
        if current_status == 'In Use' and previous_status != 'In Use':
            current_dict[key]['in_use_date'] = datetime.today().strftime('%b %d, %Y')
        elif current_status == 'Disposed' and previous_status != 'Disposed':
            current_dict[key]['disposed_date'] = datetime.today().strftime('%b %d, %Y')
        else:
            current_dict[key]['in_use_date'] = previous_in_use_date
            current_dict[key]['disposed_date'] = previous_disposed_date

    return current_dict

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
        checksum, items, dates, previous_servicedesk_cache_list = read_servicedesk_cache(servicedesk_cache_file_path)
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
        logger.info(f"Main: Raw details: {json.dumps(raw_asset_details_dict,indent=4)}")
        cleaned_asset_details_dict = clean_servicedesk_asset_details(raw_asset_details_dict)
        # logger.info(f"Main: Cleaned asset details:\n{json.dumps(cleaned_asset_details_dict,indent=4)}")

        for item in cleaned_asset_details_dict.values():
            barcode = item.get('barcode')
            if  barcode == "N/A":
                item['missing_barcode'] = 'Y'
            else:
                item['missing_barcode'] = 'N'

            annual_payment_amt = item.get('annual_replacement_amt')
            if annual_payment_amt == 0:
                item['missing_annual_replacement_amoun'] = 'Y'
            else:
                item['missing_annual_replacement_amoun'] = 'N'

        sharepoint_connector_o = SharePoint_Connector(logger)
        sharepoint_dict_items = sharepoint_connector_o.get_item_ids('ServiceDesk_Assets')

        cleaned_asset_details_dict, cleaned_sharepoint_details = reformat_dict(sharepoint_dict_items, cleaned_asset_details_dict, 'saas_id')
        cleaned_asset_details_dict = check_asset_status(cleaned_sharepoint_details, cleaned_asset_details_dict)

        previous_servicedesk_cache_list[1] = cleaned_sharepoint_details

        current_data = cache_operation(cleaned_asset_details_dict,previous_servicedesk_cache_list,logger=logger)
        status = current_data[2].get('status')
        if status == 'exit':
            logger.info("Main: No changes detected, returning.")
            return
        else:
            logger.info("Main: Changes detected in checksum, checking changes.")
            for asset in current_data[1].values():
                saas_id = asset.get('saas_id')
                asset['Unique_ID'] = saas_id
            logger.info(f"Main: Cache Details:\n{json.dumps(current_data,indent=4)}")


        logger.info("Main: Formatting and batching for upload.")
        batched_queue=sharepoint_connector_o.format_and_batch_for_upload_sharepoint(current_data[1],"ServiceDesk_Assets")
        logger.info(f"Main: Formatted and batched {len(batched_queue)} items for SharePoint")
        logger.info(f"Main: Uploading {len(batched_queue)} batches to SharePoint.")

        sharepoint_connector_o.batch_upload(batched_queue)
        logger.info("Main: Uploaded items to SharePoint.")

        logger.info("Main: Updating Cache with SharePoint info")
        new_dates = {
            'current_time_epoch_ms' : int(time.time()*1000),
            'date_last_checked_str' : datetime.now().strftime(("%m/%d/%Y %H:%M"))
        }
        current_data.append(new_dates)
        write_to_json(current_data,servicedesk_cache_file_path)
    
        logger.info(f"Main: ETL Completed successfully: Exit Code 0")
    except Exception as e:
        logger.error(f"Main: Error occured in main: {e}: Exit Code 1")
        logger.error("Main: Traceback:", exc_info=True)
        return 1

main()