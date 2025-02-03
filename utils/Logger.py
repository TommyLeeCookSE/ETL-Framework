import logging
import os
from pathlib import Path
from datetime import datetime

# Define the log directory and create it if it doesn't exist
script_dir = Path(__file__).parent.resolve()
project_root = script_dir.parent
log_directory = project_root / "logs"
log_directory.mkdir(parents=True, exist_ok=True)

def setup_logger(script_name: str):
    """
    Set up and return a logger instance specific to the script_name.

    Args: 
        script_name (str): Name of script using the logger
    
    Returns: 
        object: Configured logger instance
    """

    # Define the log file name with the specified pattern
    log_filename = log_directory / f"{datetime.now().strftime('%Y_%m_%d_%H%M')}_{script_name}.log"

    # Set up the logger
    logging.basicConfig(
        filename=log_filename,
        level=logging.DEBUG,  # Set to DEBUG to capture all levels of log messages DEBUG, INFO, WARNGING, ERROR, CRTICIAL
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Create a logger instance
    logger = logging.getLogger(script_name)

    # Add a console handler to print log messages to the console
    # Add a console handler to print log messages to the console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)  # Adjust level as needed
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(console_handler)
   
    return logger
