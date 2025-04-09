import os, sys, traceback, requests, csv
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
project_root = os.path.dirname(os.path.abspath(__file__))
os.chdir(project_root)
from utils.Logger import *
from utils.Utils import *

# Define the endpoint URL
script_name = Path(__file__).stem
logger = setup_logger(script_name)

tokens_file_path = r"misc\tokens.json"
all_tokens = read_from_json(tokens_file_path)
vector_token = all_tokens['vector_solutions_tokens']
training_record_key = vector_token['training_records_key']
secret = vector_token['secret']
restful_token = vector_token['restful_token']
# url = f"https://app.targetsolutions.com/tsapp/api/?action=reports.buildReport&reportType=credentials&key={training_record_key}&secret={secret}&ExpirationDateFrom=01-01-2000"
# url = f"http://devsandbox.targetsolutions.com/v1/credentials"
url = 'http://devsandbox.targetsolutions.com/v1/credentials/23776/assignments?q={"status":"inactive"}'
headers = {
    'AccessToken': restful_token
}
response = requests.get(url, headers=headers)
data = response.json()
if response.status_code == 200:
    logger.info(json.dumps(data,indent=4))
    # with open(r"outputs/training_data.csv", "w", newline="") as csv_file:
    #     csv_file.write(response.text)
    #     logger.info("CSV file saved as outputs/training_data.csv")
else:
    print(f"Failed to retrieve data: {response.status_code} - {response.text}")