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
    """
    When called, will do the following:
    1. Will check the last time it was updated via the cache
    2. Get items from service desk, filtered to items since last updated
    3. Generate a current cache of items and compare to the previous cache
    4. Format and upload to service desk
    """

main()