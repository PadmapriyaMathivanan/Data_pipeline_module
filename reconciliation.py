import os
import json
import datetime

def setup_reconciliation_logging(log_folder='reconciliation_logs'):
    """
    Set up the folder for reconciliation logs.
    """
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)
    return log_folder

def log_reconciliation(zone_name, table_name, operation_counts, log_folder):
    """
    Log the details of database operations for reconciliation.
    
    Args:
        zone_name (str): The name of the zone (e.g., Landing, Raw, Staging).
        table_name (str): The name of the table being processed.
        operation_counts (dict): Dictionary with keys 'inserted', 'updated', and 'deleted' indicating row counts.
        log_folder (str): Path to the folder where logs are stored.
    """
    log_file = os.path.join(log_folder, f"{zone_name.lower()}_reconciliation_{datetime.date.today()}.json")
    log_entry = {
        "timestamp": datetime.datetime.now().isoformat(),
        "zone": zone_name,
        "table": table_name,
        "operations": operation_counts
    }
    
    # Read existing log or create new log
    if os.path.exists(log_file):
        with open(log_file, 'r') as file:
            logs = json.load(file)
    else:
        logs = []

    # Append new log entry
    logs.append(log_entry)
    
    # Save updated logs
    with open(log_file, 'w') as file:
        json.dump(logs, file, indent=4)
