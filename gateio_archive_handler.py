#File: gateio_archive_handler.py

import os
import shutil
import logging
from datetime import datetime
import gateio_logger_setup

def setup_logger():
    gateio_logger_setup.setup_logging()
    return logging.getLogger()

logger = setup_logger()

# Function to create a backup of the file
def archiver(filename, backup_folder, current_datetime):
    try:
        os.makedirs(backup_folder, exist_ok=True)
        if os.path.exists(filename):
            base_filename, ext = os.path.splitext(os.path.basename(filename))
            backup_filename = os.path.join(backup_folder, f"{base_filename}_{current_datetime}{ext}")
            shutil.copy2(filename, backup_filename)
            logger.info(f"Backup of {filename} saved to {backup_filename}")
            cleanup_old_backups(backup_folder, base_filename, ext)
    except OSError as e:
        logger.error(f"OS error during backup process: {e}")

# Function to cleanup old backups
def cleanup_old_backups(backup_folder, prefix, extension, max_count=6):
    try:
        # Get list of backup files that match the prefix and extension
        backups = [
            f for f in os.listdir(backup_folder) if f.startswith(prefix) and f.endswith(extension)
        ]
        
        # Get full paths for the files and sort by creation time
        backup_files = [(f, os.path.getctime(os.path.join(backup_folder, f))) for f in backups]
        backup_files_sorted = sorted(backup_files, key=lambda x: x[1], reverse=True)
        
        # Extract sorted file names
        sorted_backups = [f[0] for f in backup_files_sorted]

        logger.debug(f"Found backups: {sorted_backups}")

        # Delete the oldest backups, keeping only 'max_count' most recent ones
        if len(sorted_backups) > max_count:
            old_backups = sorted_backups[max_count:]
            for old_backup in old_backups:
                old_backup_path = os.path.join(backup_folder, old_backup)
                try:
                    os.remove(old_backup_path)
                    logger.info(f"Removed old backup: {old_backup_path}")
                except OSError as e:
                    logger.error(f"Failed to remove old backup {old_backup_path}: {e}")
    except OSError as e:
        logger.error(f"Failed to cleanup old backups in {backup_folder}: {e}")

if __name__ == "__main__":
    try:
        current_datetime = datetime.now().strftime("%y%m%d_%H%M%S")

        # Define the filename and backup folder for JSON files      
        json_filename = os.path.expanduser('~/parsley/Gateio_Files/Gateio_JSON_Process/gateio_structured.json')
        json_backup_folder = os.path.expanduser('~/parsley/Gateio_Files/Gateio_JSON_Archive')

        # Perform the backup process for JSON files
        archiver(json_filename, json_backup_folder, current_datetime)

        # Define the filename and backup folder for TSV files
        tsv_filename = os.path.expanduser('~/parsley/Gateio_Files/Gateio_Article_Process/gateio_article_collection.tsv')
        tsv_backup_folder = os.path.expanduser('~/parsley/Gateio_Files/Gateio_Article_Archive')
        
        # Perform the backup process for TSV files
        archiver(tsv_filename, tsv_backup_folder, current_datetime)

    except Exception as e:
        logger.error(f"Unexpected error in archiving process: {e}")
