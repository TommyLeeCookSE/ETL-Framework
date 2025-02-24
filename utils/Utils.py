import json,hashlib
from pathlib import Path
from typing import Tuple
from collections import deque

def cache_operation(current_dict, previous_cache_l, logger=None):
    """
    Takes in the current_dict and the previous_cache list.
    Calls generate checksum which takes in the current_dict and returns (current_dict, total_checksum)
    Change status is gathered from check_cache(previous_cache_l,current_cache_l)
    If the the checksums match, return ['continue'] which will end the program in main
    Otherwise, it check_changes(previous_cache_l,current_cache_l) will be called and return [operations,change_dict]  
    
    Args:
        current_dict (dict): Current dict with no checksums.
        previous_cache_l (list): Previous cachhe with format [{total_checksum: xxx}, {previous_cache}]
    
    Returns:
        list : Either ['continue'] or [{operations}, {current_cache}]
    """
    current_cache_l = generate_checksum(current_dict)
    
    current_cache_l = check_cache(previous_cache_l,current_cache_l)

    change_status = current_cache_l[2].get('status')
    if change_status == 'exit':
        pass
    elif change_status == 'continue':
        current_cache_l = check_changes(previous_cache_l,current_cache_l, logger)

    return current_cache_l

def generate_checksum(data: dict)-> Tuple[dict, str]:
    """
    Takes in a dict of dicts, iterates through each item in the dict and creates a checksum for each item in the inner dict.
    The checksum of each dict will be added to a temp list to store all the checksum and generate a final checksum of the entire dict after sorting.
    Returns a dict with new checksum keys and a checksum of the entire dict.

    Args: 
        data (Dict): The input dict of dicts.
    
    Returns:
        Tuple: A tuple containg the updated dict with checksums for each item and the checksum for the entire dict.
    """
    checksum_list = []
    for key, inner_dict in data.items():
        sorted_inner= json.dumps(inner_dict, sort_keys=True)
        checksum = hashlib.md5(sorted_inner.encode()).hexdigest()
        inner_dict['checksum'] = checksum
        checksum_list.append(checksum)
    
    sorted_checksum_list = sorted(checksum_list)
    final_checksum = hashlib.md5(json.dumps(sorted_checksum_list).encode()).hexdigest()
    total_checksum = {'total_checksum': final_checksum}
    
    return [total_checksum, data]

def update_cache(cache_list: list, sharepoint_dict: dict, id_key: str) -> list:
    """
    Takes in two dicts, cache_dict and sharepoint_dict and a id_key used to create the key for each dict.
    Maps sharepoint_dict to the proper format to allow cache_dict to compare keys.
    Takes the sharepoint_id from sharepoint_dict and saves it in cache.
    Returns a single dict which will be the new cache.

    Args:
        cache_dict (dict): Current cache.
        sharepoint_dict (dict): Dict that is currently storing the sharepoint information.

    Returns:
        cache_dict (dict): Dict that will be the new cache.
    """

    mapped_dict = {item[id_key]: item for item in sharepoint_dict.values()}

    for key, value in cache_list[1].items():
        if key in mapped_dict:
            value['sharepoint_id'] = mapped_dict[key]['id']
    
    return cache_list

def check_cache(previous_list: list, current_list: list) -> str:
    """
    Takes in two lists, the previous cached list and the current cached list.
    Checks the total checksum and checks if anything has changed
    If nothing has changed, return 'continue'
    If the checksum has changed, then return 'change'

    Args:
        previous_list (list): List that contains the previously saved cache in format: [{total_checksum},{items}]
        current_list (list): List that contains the current saved cache in format:     [{total_checksum},{items}]
    
    Returns
        'continue' (str) : If there are no changes.
        'change' (str) : If there are changes.
    """
    previous_checksum = previous_list[0]
    current_checksum = current_list[0]

    if previous_checksum == current_checksum:
        status = {'status':'exit'}
    else:
        status = {'status':'continue'}

    current_list.append(status)

    return current_list

def check_changes(previous_list: list, current_list: list, logger) -> list:
    """
    Checks the previous list to the current list. Iterate through each item checking for:
    1. If the key is in the other dict and if the checksum has changed. Add an operation key with the value PATCH.
    2. If the key is missing from one or the other. If missing from previous Cache, add operation key with value POST. Otherwise, DELETE.
    3. If the key is in the other dict and the checksum matches, add operation with value "KEEP".

    Args:
        previous_list (list): List that contains the previously saved cache in format: [{total_checksum},{items}]
        current_list (list): List that contains the current saved cache in format:     [{total_checksum},{items}]
    
    Returns:    
        current_list (list): List of dicts that contains the changes that need to be batched and uploaded as well as their operations.
    """
    operations = {'post':0,'patch':0,'delete':0,'none':0}
    previous_dict = previous_list[1]
    current_dict = current_list[1]

    for prev_key, prev_value in previous_dict.items():
        if prev_key not in current_dict:
            # Item was removed, mark as DELETE
            prev_value['operation'] = 'DELETE'
            prev_value['sharepoint_id'] = prev_value.get('sharepoint_id', '')
            operations['delete'] += 1
        else:
            # Ensure sharepoint_id is retained
            current_dict[prev_key]['sharepoint_id'] = prev_value.get('sharepoint_id', '')

            # Check for checksum changes
            if prev_value.get('checksum') == current_dict[prev_key].get('checksum'):
                current_dict[prev_key]['operation'] = 'NONE'
                operations['none'] += 1
            else:
                current_dict[prev_key]['operation'] = 'PATCH'
                operations['patch'] += 1

    # Identify new items (POST)
    for curr_key, curr_value in current_dict.items():
        if curr_key not in previous_dict:
            curr_value['operation'] = 'POST'
            operations['post'] += 1

    current_list.append({'operations':operations})

    return current_list

def merge_sharepoint_ids(change_dict: dict, cached_list: list, logger:object) -> dict:
    """
    Takes in a dict and downloads cached information.
    Iterates over the dict and checks the cache for matching Azure keys. If matching gets the sharepoint_id to prepare for upload.

    Args:
        change_dict (dict): Contains the users that have changes.
    
    Returns:
        change_dict (dict): Updated change dict.
    """
    for key, value in change_dict.items():
        logger.debug(f"Key: {json.dumps(key,indent=4)}")
        logger.debug(f"Value: {json.dumps(value,indent=4)}")
        value['sharepoint_id'] = cached_list[1][key].get('sharepoint_id',"")
    
    return change_dict

def write_to_json(item, file_path: str):
    """
    Writes to json taking in the item and the filepath.

    Args:
        item (any): Any item to be written.
        file_path (str): File Path where the item will be written.
    """
    with open(file_path,'w', encoding='utf-8') as json_file:
        json.dump(item, json_file,indent=4)

def read_from_json(file_path)-> dict:
    """
    Reads from json taking in the item and the filepath.

    Args:
        file_path (str): File Path where the item will be read.

    Returns:
        data (dict): Data read from json.
    """
    with open(file_path,'r',encoding='utf-8') as json_file:
        data = json.load(json_file)
    return data

def trim_sharepoint_keys(sharepoint_dict: dict, logger)-> dict:
    """
    Takes in a dict of sharepoint items, trims it to the predefined standards, returns the trimmed dict.
    
    Args:
        sharepoint_dict (dict): Dict containg raw values from SharePoint.

    Returns:
        trimmed_dict (dict): Dict with only the trimmed values.

    """
    pass
    