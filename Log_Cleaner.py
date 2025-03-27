import os, time, sys
from datetime import datetime, timedelta
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
project_root = os.path.dirname(os.path.abspath(__file__))
os.chdir(project_root)
from utils.Logger import *
from collections import deque
from utils.Utils import *

log_folder_path = r'logs'

script_name = Path(__file__).stem
logger = setup_logger(script_name)

def delete_old_files(folder, days_old=30):
    "Deletes logs older than 30 days."
    now = time.time()
    cutoff = now - days_old * 86400

    deleted_files = []

    for filename in os.listdir(folder):
        file_path = os.path.join(folder,filename)

        if os.path.isfile(file_path):
            if os.path.getmtime(file_path) < cutoff:
                os.remove(file_path)
                deleted_files.append(filename)

    logger.info(f"Deleted {len(deleted_files)} file(s):")
    for f in deleted_files:
        logger.info(f" - {f}")


delete_old_files(log_folder_path)