import os
import json
import logging_config  # For setting up logging
import raw_zone  # Import raw_zone module
import landing_zone  # Import landing_zone module
import staging_zone  # Import staging_zone module
import exceptions  # Import custom exception handling module
from landing_zone import create_database_connection as create_landing_db_connection
from raw_zone import create_database_connection as create_raw_db_connection
from staging_zone import create_database_connection as create_staging_db_connection
import reconciliation 

def load_config():
    """Load the configuration from config.json."""
    try:
        with open('config.json', 'r') as file:
            config = json.load(file)
        return config
    except Exception as e:
        exceptions.handle_config_loading_error(e)
        raise

def process_zone(zone_config, zone_name, config):
    """Process each zone based on source and destination type."""
    source_type = zone_config['source']['type']
    destination_type = zone_config['destination']['type']
    source_path = zone_config['source'].get('path', '')
    destination_path = zone_config['destination'].get('path', '')
    
    # Get file type and delimiter
    file_type = config.get("file_type", ["csv"])[0]
    delimiter = config['delimiter'].get(file_type, ",")

    if source_type == 'folder' and destination_type == 'folder':
        # Folder to Folder
        for file_name in os.listdir(source_path):
            file_path = os.path.join(source_path, file_name)
            if zone_name == "Landing":
                landing_zone.copy_file_to_folder(file_path, destination_path)
            elif zone_name == "Raw":
                raw_zone.copy_file_to_folder(file_path, destination_path)
            logging_config.logger.info(f"File {file_name} copied to folder in {zone_name} zone.")

    elif source_type == 'folder' and destination_type == 'database':
        # Folder to Database
        if zone_name == "Landing":
            engine = create_landing_db_connection(zone_config['destination']['database'])
            for file_name in os.listdir(source_path):
                file_path = os.path.join(source_path, file_name)
                table_name = file_name.split('.')[0]
                landing_zone.load_file_to_database(file_path, table_name, engine, delimiter)
                logging_config.logger.info(f"File {file_name} loaded into database table '{table_name}' in Landing zone.")
        elif zone_name == "Raw":
            engine = create_raw_db_connection(zone_config['destination']['database'])
            for file_name in os.listdir(source_path):
                file_path = os.path.join(source_path, file_name)
                table_name = file_name.split('.')[0]
                raw_zone.append_new_data_to_table(file_path, table_name, engine, delimiter)
                logging_config.logger.info(f"File {file_name} loaded into database table '{table_name}' in Raw zone.")

    elif source_type == 'database' and destination_type == 'database':
        # Database to Database
        source_db_config = zone_config['source']['database']
        dest_db_config = zone_config['destination']['database']
        #table_name = zone_config.get('table_name', 'default_table')
        feature_config = zone_config['feature_engineering']
        
        #if zone_name == "Landing":
        #    landing_zone.copy_database_to_database(source_db_config, dest_db_config, table_name)
        #elif zone_name == "Raw":
        #    raw_zone.transfer_data_between_databases(source_db_config, dest_db_config, table_name)
        if zone_name == "Staging":
            engine = create_raw_db_connection(zone_config['destination']['database'])
            staging_zone.transfer_data_between_databases(source_db_config, dest_db_config, feature_config)
        
        logging_config.logger.info(f"Data copied from source DB to destination DB for table in {zone_name} zone.")
        
    else:
        exceptions.handle_unsupported_configuration(source_type, destination_type)



def main():
    # Load configuration and initialize logging
    config = load_config()
    logging_config.setup_logging(config.get('log_folder', 'logs'))  # Set up daily log files in a specified folder
    
    # Process Landing Zone
    process_zone(config['landing_zone'], 'Landing', config)

    # Process Raw Zone
    process_zone(config['raw_zone'], 'Raw', config)
    
    # Process Staging Zone
    process_zone(config['staging_zone'], 'Staging', config)

if __name__ == "__main__":
    main()
