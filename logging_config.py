import os
import logging
import shutil
from datetime import datetime, timedelta

# Define a global logger variable
logger = None

def setup_logging(log_folder="logs", archive_folder="archive", retention_days=7):
    """Set up logging to output logs to a date-specific file and console. Archives logs older than retention_days."""
    global logger  # Ensure we're modifying the global logger variable

    # Ensure the log and archive folders exist
    os.makedirs(log_folder, exist_ok=True)
    os.makedirs(archive_folder, exist_ok=True)
    
    # Get today's date for naming the log file
    today_date = datetime.now().strftime("%Y-%m-%d")
    log_file_name = f"process-{today_date}.log"  # e.g., process-2024-11-18.log
    log_file_path = os.path.join(log_folder, log_file_name)
    
    # Initialize the logger
    logger = logging.getLogger("process_logger")
    logger.setLevel(logging.INFO)
    
    # File handler for today's log
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    
    # Console handler for logging
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    
    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # Log the initial setup message
    logger.info("Logging is configured.")

    # Archive logs older than the retention period
    archive_old_logs(log_folder, archive_folder, retention_days)

def archive_old_logs(log_folder, archive_folder, retention_days):
    """Move logs older than retention_days from log_folder to archive_folder."""
    cutoff_date = datetime.now() - timedelta(days=retention_days)
    for filename in os.listdir(log_folder):
        file_path = os.path.join(log_folder, filename)
        if os.path.isfile(file_path):
            try:
                # Extract the date from the filename
                file_date_str = filename.replace("process-", "").replace(".log", "")
                file_date = datetime.strptime(file_date_str, "%Y-%m-%d")
                if file_date < cutoff_date:
                    # Move the old log file to the archive folder
                    shutil.move(file_path, os.path.join(archive_folder, filename))
                    logger.info(f"Archived log file '{filename}'")
            except (ValueError, IndexError):
                # Skip files without a valid date suffix
                continue
