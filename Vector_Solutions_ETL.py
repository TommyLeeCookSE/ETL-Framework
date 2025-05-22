import os, sys, requests, json
from datetime import datetime, timedelta
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
project_root = os.path.dirname(os.path.abspath(__file__))
os.chdir(project_root)
from utils.Logger import *
from scripts.SharePoint_Connector import *
from utils.Utils import *

script_name = Path(__file__).stem
logger = setup_logger(script_name)
tokens_file_path = r"misc\tokens.json"
cred_cache_file_path = r"cache\cred_vector_solutions_cache.json"
categories_cache_file_path = r"cache\categories_cache.json"
user_cache_file_path = r"cache\user_vector_solutions_cache.json"
all_tokens = read_from_json(tokens_file_path)
vector_token = all_tokens['vector_solutions_tokens']
training_record_key = vector_token['training_records_key']
secret = vector_token['secret']
restful_token = vector_token['restful_token']
base_url = "http://devsandbox.targetsolutions.com/v1/"
headers = {
    'AccessToken': restful_token
}

##TESTED BELOW
def get_all_users()-> list:
    """
    Gets a list of all users.

    Returns:
        all_users_list (list): Contains all users.
    """
    logger.info(f"Get Active Users: Getting active users.")
    url = f'http://devsandbox.targetsolutions.com/v1/users?'
    response = requests.get(url, headers=headers)
    if response.ok:
        logger.info(f"Get All Users: Response [{response.status_code}] valid.")
        data = response.json()
        return data.get('users',[])

    return []

def check_user_status(status:str) -> bool:
    """
    Takes in a user's status (Active, Inactive, Offline) returns True if Active/Offline, False for Inactive.
    
    Args:
        status (str): Active/Inactive/Offline
    Returns:
        bool: True for Active/Offline and False for Inactive
    """
    active_criteria = ['Active', 'Offline']
    return True if status in active_criteria else False

def clean_user(user:dict)-> dict:
    """
    Takes in a user dict, cleans key:values and returns dict.
    
    Args:
        user (dict): Dict containg raw informating from api call.
    Returns:
        user (dict): Dict with keys removed.    
    """
    user = user.copy()
    user['links'] = user.get('links', {}).copy()

    for key in ['employeeid', 'usertype', 'siteid', 'email']:
        user.pop(key, None)

    user['links'].pop('resourcelink', None)

    return user

def filter_active_users(users: list) -> dict:
    """
    Takes in raw users lists, iterates over it, extracts only active/offline users, and returns in dict format.
    
    Args:
        users (list): Raw list of dicts of users
    Returns:
        active_users_dict (dict): Dict of Active/Offline users in a dict of dicts with the user_id as the key.
    """
    active_users_dict = {}
    #Iterates over list of users
    for user in users:
        status = user.get('status', '')
        if check_user_status(status):
            user_id = user.get('userid')
            active_users_dict[user_id] = clean_user(user)
        else:
            continue

    return active_users_dict

##TESTED ABOVE

def get_users_groups(link:str) -> dict:
    """
    Retrieves the groups from a user and returns a dict.

    Args:
        link (str): Link to be used as the url.
    Returns:
        data (dict): Holds the group info.
    """
    try:
        response = requests.get(link,headers=headers)
        if response.ok:
            data = response.json()
            return data.get('groups')
    except Exception as e:
        logger.error(f"Get Users: Error trying to get user info for [{link}]")

    return ""

def get_users_credentials(link:str) -> dict:
    """
    Retrieves credential info from link.

    Args:
        link (str): Url to credentials
    Returns:
        data (dict): Credential information.
    """
    try:
        response = requests.get(link,headers=headers)
        if response.ok:
            data = response.json()
            return data.get('credentials')
        else:
            logger.info(f"Get Users: No data for: [{link}]")
    except Exception as e:
        logger.error(f"Get Users: Error trying to get user info for [{link}]")
    
    return ''

def get_categories() -> dict:
    """
    """
    logger.info(f"Get All Credential Categories.")
    url = f'http://devsandbox.targetsolutions.com/v1/credentials'
    response = requests.get(url, headers=headers)
    if response.ok:
        logger.info(f"Get All Users: Response [{response.status_code}] valid.")
        data = response.json()
        return data
    
    return None

def clean_categories(credentials:dict) ->dict:
    """
    """
    cleaned_credential_category={}
    keys_to_extract = ['categoryid','categoryname','credentialid']
    for credential in credentials.get('credentials'):
        if all(key in credential for key in keys_to_extract):
            credentialid = float(credential.get('credentialid'))
            cleaned_credential_category[credentialid] = {
                key : float(credential[key]) if isinstance(credential[key], int) else credential[key]
                for key in keys_to_extract
                }
    
    return cleaned_credential_category

def add_categoryid(credential_dict:dict, categories_dict:dict)->dict:
    """
    Iterates over the credential dict, comparing credentialid to the credentialids in categories dict. If founds, assigns the categoryid to it.
    """
    for credential in credential_dict.values():
        credentialid = credential.get('credentialid')
        if credentialid in categories_dict:
            logger.info(f"{credentialid}")
            credential['categoryid'] = categories_dict[credentialid].get('categoryid')

    return credential_dict

def make_cateogoryid_unique(categories_dict:dict)->dict:
    """
    Makes the category dict unique by creating a dict with categoryid as the unique key,
    """
    return {category.get('categoryid'): {'categoryname': category.get('categoryname'), 'categoryid': category.get('categoryid')} for category in categories_dict.values()}

def clean_users_groups(groups:list)-> dict:
    """
    Takes in group list and cleans the list and converrts it to a dict of dicts instead of a list of dicts.
    Args:
        groups (list): List contains dicts of group info.
    Returns:
        user_dict (dict): Dict with the groups having been converted to dicts and trimmed of keys.
    """
    group_dict = {
        '12366': 'shift',
        '38108': 'station',
        '24618': 'unit',
        '11224': 'rank'
    }
    cleaned_groups = {}
    if groups:
        for group in groups:
            group_id = group.get('groupid')
            group_name = group.get('groupname')
            category_id = str(group.get('categoryid'))

            if str(category_id) in group_dict:
                cleaned_groups[category_id] = {group_dict[category_id]: group_name, 'category_id': category_id, 'group_id': group_id}
            
    return cleaned_groups

def check_credential_expiration(credential: dict)-> bool:
    """
    If the expirationdate is empty, return false which will mark it for deletion.
    Args:
        credential (dict): Dict with credential information.
        {
            "notes": "",
            "status": "active",
            "startdate": "01/01/2021",
            "credentialid": 345506,
            "credentialnumber": "",
            "link": "http://devsandbox.targetsolutions.com/v1/credentials/345506/assignments/345506-1862279",
            "expirationdate": "12/31/2021",
            "attachmentcount": 0,
            "siteid": 18380
        }
    Returns:
        bool: True it exists and is within last 12 months, false means delete
    """

    expiration_date = credential.get('expirationdate')
    if not expiration_date:
        return False

    exp_date = datetime.strptime(credential.get('expirationdate'), "%m/%d/%Y")
    twelve_months_ago = datetime.today() - timedelta(365)

    return exp_date >= twelve_months_ago

def clean_user_credentials(credentials:list)-> dict:
    """
    Takes in credentials list, iterates over the credentials, removes keys and converts list to a dict.
    
    Args:
        user_dict (dict): Dict containing user information and credential list.
    Returns:
        user_dict (dict): Dict containg the cleaned information. 
    """
    keys_to_keep = ['status', 'startdate', 'credentialid','credentialnumber','expirationdate']
    cleaned_credential_dict = {}
    if credentials:
        for credential in credentials:
            valid_credential = check_credential_expiration(credential)
            if not valid_credential:
                continue
            credential_id = credential.get('credentialid')
            cleaned_credential_dict[credential_id] = {}

            for item_key in keys_to_keep:
                if item_key in credential:
                    cleaned_credential_dict[credential_id][item_key] = credential[item_key]
    
    return cleaned_credential_dict

def get_credentials_list() -> dict:
    """
    Gets a list of credentials and their links.
    Returns:
        credentials_dict (dict): Dict holding credentials as they key and their info as the value.
    """
    logger.info(f"Get Credentials: Getting Credentials")
    url = 'http://devsandbox.targetsolutions.com/v1/credentials'
    response = requests.get(url, headers=headers)
    if response.ok:
        data = response.json()
        return data
    else:
        logger.warning(f"Get Credentials: Response [{response.status_code}] not valid.")

    return []

def map_credentials(credential_list: list) -> dict:
    """
    Takes in the raw credential list and returns a dict with key:value being cred_id: cred_name.
    
    Args:
        credential_list (list): List of credential dicts.
    Returns:
        credential_dict (dict): Dict of credentials with key being the id and name being the value.
    """
    credential_dict = {}

    for item in credential_list.get('credentials'):
        cred_id = item.get('credentialid')
        cred_name = item.get('credentialname')
        credential_dict[str(cred_id)] = cred_name
    
    return credential_dict

def assign_credential_names(credentials: dict, credential_dict:dict) -> dict:
    """
    Takes in users credentials which holds their credentials which only have an ID no name.
    Takes in credential_dict which holds the name of the credentials and ID.
    Joins them so user_dict holds all information.
    
    Args:
        credentials (dict): Credential dict holds user info, users have credential_dict contained.
        credential_dict (dict): Credentials with key being their Id and values being their name
    Returns:
        credentials (dict): Dict that now holds the full user info
    """
    if credentials:
        for credential in credentials.values():
            credential_id = str(credential.get('credentialid')).strip()
            if credential_id in credential_dict:
                    credential['credentialname'] = credential_dict[credential_id]
        return credentials
    
    return {}

def calculate_days_till_expired(credentials:dict)-> dict:
    """
    Takes in a dict of credentials, iterates over it and calculates the days till expired.
    Returns the dict with days_till_expired added.
    
    Args:
        credentials (dict)
    Returns:
        credentials (dict): Same dict with calculates days_till_expired added.
    """

    for credential in credentials.values():
        expiration_date = datetime.strptime(credential.get('expirationdate'), "%m/%d/%Y")
        today = datetime.today()

        credential['days_till_expired'] = float((expiration_date - today).days)

    return credentials
def get_links(user_dict:dict, credential_dict:dict) -> dict:
    """
    Iterates over the links_dict in the user_dict. Retrieves the groups and credentials and saves it to the user.
    Then cleans the group and credential to appropriate standards.
    
    Args:
        user_dict (dict): Dict containing the user info + links.
        credential_dict (dict): Dict containg the id: name of credentials.
    Returns:
        user_dict (dict): Dict containing the user info with groups and credentials.
    """
    len_users = len(user_dict)
    for index, user in enumerate(user_dict.values(),1):

        logger.info(f"Getting Links: {index}/{len_users} users.")
        links = user.get('links')
        group_link = links.get('groups')
        credential_link = links.get('credentials')

        user['groups'] = get_users_groups(group_link)
        user['groups'] = clean_users_groups(user['groups'])
        groups_to_extract = {"shift": '12366', "rank": "11224", "unit": "24618", "station": "38108"}
        for key, group_value in groups_to_extract.items():
            user[key] = user['groups'].get(group_value,{}).get(key,'').strip()
        
        credentials = get_users_credentials(credential_link)
        credentials = clean_user_credentials(credentials)
        credentials = assign_credential_names(credentials, credential_dict)
        user['credentials'] = calculate_days_till_expired(credentials)

        del user['links']
        del user['groups']
    return user_dict

def remove_non_essential(user_dict:dict)-> dict:
    """
    Removes non-essentials like IT and TPD.
    Args:
        user_dict (dict): Holds all users
    Returns:
        user_dict (dict): users without IT and TPD.
    """
    ranks_to_delete = ['IT Specialist', 'TPD', 'Fire Cadet', 'Administrative Assistant', 'Senior Administrative Analyst']
    station_to_delete = ['CRRD']
    id_to_delete = []
    for user in user_dict.values():
        rank = user.get('rank')
        station = user.get('station')
        if rank in ranks_to_delete or station in station_to_delete:
            id_to_delete.append(user.get('userid'))
    
    for id in id_to_delete:
        del user_dict[id]

    return user_dict

def create_station_hierarchy(user_dict:dict)-> dict:
    """
    Takes in user_dict and creates a station hierarchy from the users list.
    Any unit that has a captain will assign the captain to their supervisor, rest blank.

    Args:
        user_dict (dict): Holds users with their rank, title, and shift.
    Returns:
        hierarchy (dict): Returns station_hierarchy with supervisor/station.
    """

    hierarchy = {}

    for user in user_dict.values():
        email = user.get('username',"missing")
        user_id = user.get('userid')
        shift = user.get('shift')
        rank = user.get('rank')
        unit = user.get('unit')

        ignored_ranks = ["Assistant Chief", "Deputy Chief", "Fire Chief"]


        if not (shift and rank and unit):
            continue

        if shift not in hierarchy:
            hierarchy[shift] = {}
        if rank in ignored_ranks:
            if rank not in hierarchy[shift]:
                hierarchy[shift][rank] = {}
            hierarchy[shift][rank][email] = {
                'email': email, 
                'shift': shift,
                'unit': unit,
                'rank': rank,
                'user_id': user_id
            }
        
        if unit not in hierarchy[shift]:
            hierarchy[shift][unit] = {}
        
        if rank not in hierarchy[shift][unit] and rank not in ignored_ranks:
            hierarchy[shift][unit][rank] = {}
        if rank not in ignored_ranks:
            hierarchy[shift][unit][rank][email] = {
                'email': email, 
                'shift': shift,
                'unit': unit,
                'rank': rank,
                'user_id': user_id
            }
    
        
    return hierarchy

def assign_supervisor(user_dict: dict, hierarchy:dict)-> dict:
    """
    Takes in user_dict and station_hierarchy. Iterates over the Station -> Shift -> Unit and looks at the first captain.
    Then it looks at all the subs. Each sub will be assigned in the user_dict the captain's email.

    Args:
        user_dict (dict): Holds users with their information.
        station_hierarchy (dict): Holds hierarchy Station -> Shift -> Unit {captain | subs}
    Returns:
        user_dict (dict): Returns user_dict with supervisor assigned
    """
    chief_list = ['Fire Chief', 'Assistant Chief', 'Deputy Chief']
    bls_captain = user_dict[510935]
    bls_captain = {
        'email': bls_captain.get('username'),
        'shift' : bls_captain.get('shift'),
        'unit' : bls_captain.get('unit'),
        'rank' : bls_captain.get('rank'),
        'user_id' : bls_captain.get('userid')
    }
    
    for user in user_dict.values():
        rank = user.get('rank')
        if rank == 'Fire Chief':
            fire_chief = {
                'email': user.get('username'),
                'shift' : user.get('shift'),
                'unit' : user.get('unit'),
                'rank' : user.get('rank'),
                'user_id' : user.get('userid')
            }
        user['supervisor'] = 'N/A'

    for shift in hierarchy.values():
        shift_assistant_chief = next(iter(shift.get('Assistant Chief',{}).values()))
        
        for unit_key, unit_value in shift.items():
            if unit_key in chief_list:
                for a_chief in unit_value.values():
                    user_id = a_chief.get('user_id', '')
                    user_dict[user_id]['supervisor'] = fire_chief
                continue
            
            unit_captain = next(iter(unit_value.get('Captain',{}).values()), {})

            if "BLS" in unit_key and not unit_captain:
                unit_captain = bls_captain
            elif not unit_captain:
                unit_captain = shift_assistant_chief

            for rank_key, rank_value in unit_value.items():
                for user in rank_value.values():
                    user_id = user.get('user_id', '')
                    if rank_key == 'Captain':
                        user_dict[user_id]['supervisor'] = shift_assistant_chief
                    else:
                        user_dict[user_id]['supervisor'] = unit_captain

    return user_dict

def split_into_sharepoint_lists(user_dict:dict)->dict:
    """
    Splits user_dict into:
    TFD Credential | User lists
    
    Credentials List: user_id + cred_id, user_id, cred_id, cred_name, start_date, expiration_date, status
    User List: user_id, status, last+first name, email, shift, rank, unit, station, supervisor
    
    Args:
        user_dict (dict): Dict of users with finalized details
    Returns:
        sharepoint_upload_dict (dict): Dict containg the lists of items converted to sharepoint.
    """
    sharepoint_upload_dict={
        'cred_dict':{},
        'user_dict':{}
    }
    cred_dict_key_list = ['unique_id', 'credentialid', 'userid', 'credentialname', 'startdate', 'expirationdate', 'status', 'days_till_expired']
    user_dict_key_list = ['userid', 'status', 'full_name', 'username', 'shift', 'rank', 'unit', 'station', 'supervisor']
    
    for userid, user in user_dict.items():
        first_name = user.get('firstname')
        last_name = user.get('lastname')
        full_name = f'{last_name}, {first_name}'
        user['full_name'] = full_name

        temp_user_dict = {}
        for key in user_dict_key_list:
            if key == "userid":
                temp_user_dict[key] = float(user.get(key))
            elif key == "supervisor":
                temp_user_dict[key] = float(user.get('supervisor',{}).get('user_id'))
            elif key == "username":
                temp_user_dict['email'] = user.get(key)
            else:
                temp_user_dict[key] = user.get(key)
            
        sharepoint_upload_dict['user_dict'][userid] = temp_user_dict

        credentials = user.get('credentials',{})
        for credential in credentials.values():
            credential_id = credential.get('credentialid')
            user_id = user['userid']
            
            unique_key = f'{userid}_{credential_id}'
            credential['unique_id'] = unique_key
            credential['credentialid'] = float(credential_id)
            credential['userid'] = float(user_id)

            temp_dict = {
                key: credential[key] 
                for key in cred_dict_key_list}
            sharepoint_upload_dict['cred_dict'][unique_key] = temp_dict

    return sharepoint_upload_dict

def main():
    logger.info(f"Begin extraction and transformation operations.")

    categories_dict = clean_categories(get_categories())

    users = get_all_users()
    users_dict = filter_active_users(users)

    credential_list = get_credentials_list()
    credential_dict = map_credentials(credential_list)

    users_dict = get_links(users_dict, credential_dict)
    users_dict = remove_non_essential(users_dict)

    hierarchy = create_station_hierarchy(users_dict)
    users_dict = assign_supervisor(users_dict, hierarchy)

    sharepoint_upload_dict = split_into_sharepoint_lists(users_dict)
    sharepoint_upload_dict['cred_dict'] = add_categoryid(sharepoint_upload_dict['cred_dict'], categories_dict)
    sharepoint_upload_dict['cat_dict'] = make_cateogoryid_unique(categories_dict)
    logger.info(f"Completed extraction and transformation.")

    sharepoint_connector_o = SharePoint_Connector(logger)
    for filepath in [cred_cache_file_path, user_cache_file_path, categories_cache_file_path]:
        logger.info(f"Begin caching operations.")
        if 'cred' in filepath:
            unique_id = 'unique_id'
            current_dict = sharepoint_upload_dict['cred_dict']
            sp_list_name = 'TFD_Credential_List'
        elif 'user' in filepath:
            unique_id = 'userid'
            current_dict = sharepoint_upload_dict['user_dict']
            sp_list_name = 'TFD_User_List'
        elif 'categories' in filepath:
            unique_id = 'categoryid'
            current_dict = sharepoint_upload_dict['cat_dict']
            sp_list_name = 'TFD_Credential_Categories'

        cached_sharepoint_items = sharepoint_connector_o.get_item_ids(sp_list_name)
        current_formatted_dict, formatted_sharepoint_dict = reformat_dict(cached_sharepoint_items, current_dict, unique_id)
        logger.debug(f"Cached {sp_list_name} Sharepoint: {json.dumps(formatted_sharepoint_dict,indent=4)}")
        logger.debug(f"Current {sp_list_name} Dict: {json.dumps(current_formatted_dict,indent=4)}")
        previous_cache = read_from_json(filepath)
        previous_cache[1] = formatted_sharepoint_dict

        current_data = cache_operation(current_formatted_dict,previous_cache, delete=True, logger=logger)

        status = current_data[2].get('status')
        if status == 'exit':
            logger.info("No changes detected, continuing.")
            continue
        else:
            logger.info("Changes detected in checksum, checking changes.")

        logger.info("Formatting and batching for upload.")
        batched_queue=sharepoint_connector_o.format_and_batch_for_upload_sharepoint(current_data[1],sp_list_name)
        logger.info(f"Formatted and batched {len(batched_queue)} items for SharePoint")
        logger.info(f"Uploading {len(batched_queue)} batches to SharePoint.")

        sharepoint_connector_o.batch_upload(batched_queue)
        logger.info("Uploaded items to SharePoint.")

        logger.info("Updating Cache with SharePoint info")
        write_to_json(current_data,filepath)
        
        logger.info(f"ETL Completed successfully: Exit Code 0")


if __name__ == "__main__":
    main()
    # test_main()