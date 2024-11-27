#File: gateio_folder_structure.py

import os
import logging
from gateio_logger_setup import setup_logging

def create_directory_structure(base_path):
    # Define the desired directory structure
    directories = [
        "Gateio_Files/Gateio_Article_Archive",
        "Gateio_Files/Gateio_Article_Process",
        "Gateio_Files/Gateio_JSON_Archive",
        "Gateio_Files/Gateio_JSON_Process",
        "Gateio_Files/Gateio_Logs",
        "Gateio_Files/Gateio_Subscribe"
    ]
    
    # Loop through each directory and create it if it doesn't exist
    for directory in directories:
        full_path = os.path.join(base_path, directory)
        if not os.path.exists(full_path):
            try:
                os.makedirs(full_path)
                logging.info(f"Created directory: {full_path}")
            except Exception as e:
                logging.error(f"Failed to create directory {full_path}: {e}")
        else:
            logging.info(f"Directory already exists: {full_path}")

if __name__ == "__main__":
    # Set up logging
    setup_logging()
    
    # Define the base path for the project "parsley"
    project_base_path = os.getcwd()  # Set the base path to the current working directory of the script
    
    # Create the directory structure in the "parsley" project
    create_directory_structure(project_base_path)