#File: gateio_archive_handler.py

import os
import shutil
import logging
from datetime import datetime
import logger_setup

def setup_logger():
    logger_setup.setup_logging()
    return logging.getLogger()

logger = setup_logger()

# Function to create a backup of the TSV file
def tsv_archiver(filename, backup_folder):
    try:
        os.makedirs(backup_folder, exist_ok=True)
        if os.path.exists(filename):
            current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M")
            backup_filename = os.path.join(backup_folder, f"{os.path.splitext(os.path.basename(filename))[0]}_{current_datetime}.tsv")
            shutil.copy2(filename, backup_filename)
            logger.info(f"Backup of {filename} saved to {backup_filename}")
            cleanup_old_backups(backup_folder, os.path.splitext(os.path.basename(filename))[0])
    except OSError as e:
        logger.error(f"OS error during backup process: {e}")

# Function to move files between directories
def json_archiver(source_folder, destination_folder):
    try:
        os.makedirs(destination_folder, exist_ok=True)
        for filename in os.listdir(source_folder):
            if filename.endswith('.json'):
                source_path = os.path.join(source_folder, filename)
                destination_path = os.path.join(destination_folder, filename)
                if os.path.isfile(source_path):
                    shutil.move(source_path, destination_path)
                    logger.info(f"Moved file '{filename}' from '{source_folder}' to '{destination_folder}'")
        logger.info("All JSON files moved from Process to Archived.")
    except OSError as e:
        logger.error(f"Failed to move files from {source_folder} to {destination_folder}: {e}")

# Function to cleanup old backups
def cleanup_old_backups(backup_folder, prefix, max_count=5):
    try:
        backups = sorted([f for f in os.listdir(backup_folder) if f.startswith(prefix)],
                         key=lambda x: os.path.getmtime(os.path.join(backup_folder, x)), reverse=True)
        for old_backup in backups[max_count:]:
            os.remove(os.path.join(backup_folder, old_backup))
            logger.info(f"Removed old backup: {old_backup}")
    except OSError as e:
        logger.error(f"Failed to cleanup old backups in {backup_folder}: {e}")

if __name__ == "__main__":
    try:
        # Define the source and destination folders for moving files
        source_folder = os.path.expanduser('~/parsley/Gateio_Files/Gateio_JSON_Process')
        destination_folder = os.path.expanduser('~/parsley/Gateio_Files/Gateio_JSON_Archive')
        
        # Perform the archive process
        json_archiver(source_folder, destination_folder)

        # Define the filename and backup folder for TSV files
        tsv_filename = os.path.expanduser('~/parsley/Gateio_Files/Gateio_Article_Process/gateio_article_collection.tsv')
        backup_folder = os.path.expanduser('~/parsley/Gateio_Files/Gateio_Article_Archive')
        
        # Perform the backup process for TSV files
        tsv_archiver(tsv_filename, backup_folder)
    except Exception as e:
        logger.error(f"Unexpected error in archiving process: {e}")
