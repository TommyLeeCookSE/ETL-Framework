#tests/test_vector_solutions.py
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from unittest.mock import patch, MagicMock
from Vector_Solutions_ETL import *


def test_check_user_status_active():
    assert check_user_status("Active") is True

def test_check_user_status_offline():
    assert check_user_status("Offline") is True

def test_check_user_status_inactive():
    assert check_user_status("Inactive") is False

def test_check_user_status_random():
    assert check_user_status("Retried") is False

@patch('Vector_Solutions_ETL.requests.get')
def test_get_all_users_success(mock_get):
    mock_response = MagicMock()
    mock_response.ok = True
    mock_response.json.return_value = {
        'users': [
             {
                "status": "Active",
                "employeeid": "23137",
                "lastname": "Aguilar",
                "usertype": "User",
                "userid": 3393969,
                "links": {
                    "credentials": "http://devsandbox.targetsolutions.com/v1/users/3393969/credentials",
                    "groups": "http://devsandbox.targetsolutions.com/v1/users/3393969/groups",
                    "resourcelink": "http://devsandbox.targetsolutions.com/v1/users/3393969"
                },
                "username": "MAguilar@TorranceCA.Gov",
                "firstname": "Maria",
                "email": [
                    {
                        "status": "Active",
                        "link": "http://devsandbox.targetsolutions.com/v1/users/3393969/emails/274327634",
                        "email": "MAguilar@TorranceCA.Gov",
                        "emailid": 274327634
                    }
                ],
                "siteid": 18380
            },
            {
                "status": "Inactive",
                "employeeid": "",
                "lastname": "Aleman",
                "usertype": "Supervisor",
                "userid": 910253,
                "links": {
                    "credentials": "http://devsandbox.targetsolutions.com/v1/users/910253/credentials",
                    "groups": "http://devsandbox.targetsolutions.com/v1/users/910253/groups",
                    "access/modules": "http://devsandbox.targetsolutions.com/v1/users/910253/access/modules",
                    "resourcelink": "http://devsandbox.targetsolutions.com/v1/users/910253"
                },
                "username": "ealeman@torranceca.gov",
                "firstname": "Ed",
                "email": [
                    {
                        "status": "Active",
                        "link": "http://devsandbox.targetsolutions.com/v1/users/910253/emails/4743566",
                        "email": "ealeman@torranceca.gov",
                        "emailid": 4743566
                    }
                ],
                "siteid": 18380
        },
        {
            "status": "Offline",
            "employeeid": "22326",
            "lastname": "Stewart",
            "usertype": "User",
            "userid": 2654341,
            "links": {
                "credentials": "http://devsandbox.targetsolutions.com/v1/users/2654341/credentials",
                "groups": "http://devsandbox.targetsolutions.com/v1/users/2654341/groups",
                "resourcelink": "http://devsandbox.targetsolutions.com/v1/users/2654341"
            },
            "username": "GStewart@TorranceCA.gov",
            "firstname": "Genevieve",
            "email": [],
            "siteid": 18380
        },
         ]
    }
    mock_get.return_value = mock_response

    users = get_all_users()

    assert len(users) == 3
    assert users[0]['userid'] == 3393969
    assert users[0]['status'] == 'Active'
    assert users[1]['userid'] == 910253
    assert users[1]['status'] == 'Inactive'
    assert users[2]['userid'] == 2654341
    assert users[2]['status'] == 'Offline'

@patch('Vector_Solutions_ETL.requests.get')
def test_get_all_users_api_failure(mock_get):
    mock_response = MagicMock()
    mock_response.ok = False
    mock_get.return_value = mock_response

    users = get_all_users()

    assert users == []

def test_clean_users_removes_keys():
    raw_user = {
        "status": "Active",
        "employeeid": "23137",
        "lastname": "Aguilar",
        "usertype": "User",
        "userid": 3393969,
        "links": {
            "credentials": "http://devsandbox.targetsolutions.com/v1/users/3393969/credentials",
            "groups": "http://devsandbox.targetsolutions.com/v1/users/3393969/groups",
            "resourcelink": "http://devsandbox.targetsolutions.com/v1/users/3393969"
        },
        "username": "MAguilar@TorranceCA.Gov",
        "firstname": "Maria",
        "email": [
            {
                "status": "Active",
                "link": "http://devsandbox.targetsolutions.com/v1/users/3393969/emails/274327634",
                "email": "MAguilar@TorranceCA.Gov",
                "emailid": 274327634
            }
        ],
        "siteid": 18380
    }

    cleaned = clean_user(raw_user)

    for k in ['employeeid', 'usertype', 'siteid', 'email']:
        assert k not in cleaned
    
    assert 'resourcelink' not in cleaned['links']

    for k in ['status', 'lastname', 'userid', 'username', 'firstname']:
        assert k in cleaned

def test_filter_active_users():
    users = [
        {"userid": 1, "status": "Active", "employeeid": "x", "links": {}, "email": [], "usertype": "User", "siteid": 1},
        {"userid": 2, "status": "Offline", "employeeid": "y", "links": {}, "email": [], "usertype": "User", "siteid": 1},
        {"userid": 3, "status": "Inactive", "employeeid": "z", "links": {}, "email": [], "usertype": "User", "siteid": 1},
    ]

    result = filter_active_users(users)
    for user in result.values():
        assert user['status'] in ['Active','Offline']
        for key in ['employeeid', 'usertype', 'siteid', 'email']:
            assert key not in user