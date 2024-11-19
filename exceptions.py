# exceptions.py

import logging

def handle_database_connection_error(error):
    logging.error(f"Database connection error: {error}")

def handle_unsupported_file_type(file_path, file_type):
    logging.warning(f"Unsupported file type '{file_type}' detected for file '{file_path}'. File skipped.")

def handle_file_copy_error(file_path, destination_folder, error):
    logging.error(f"Error copying file '{file_path}' to folder '{destination_folder}': {error}")

def handle_database_insertion_error(table_name, error):
    logging.error(f"Error inserting data into table '{table_name}': {error}")

def handle_config_loading_error(error):
    logging.error(f"Error loading configuration: {error}")

def handle_unsupported_destination_type(destination_type):
    logging.error(f"Unsupported destination type '{destination_type}' in configuration.")

def handle_database_copy_error(source_db_config, destination_db_config, table_name, error):
    """Handle errors encountered while copying data from one database to another."""
    logging.error(
        f"Error copying table '{table_name}' from source database '{source_db_config['database_name']}' "
        f"to destination database '{destination_db_config['database_name']}': {error}"
    )

def handle_unsupported_configuration(source_type, dest_type):
    """Handle unsupported source-destination configuration combinations."""
    logging.error(f"Unsupported configuration: source type '{source_type}' to destination type '{dest_type}'")


def handle_feature_engineering_error(error, table_name):
    """Logs errors related to feature engineering."""
    logging.error(f"Feature engineering error for table '{table_name}': {error}")
